"""
app/main.py  –  ISLD Pipeline エントリポイント

実行手順:
  1. ConfigLoader.load
  2. ConfigValidator.validate
  3. JobCompiler.compile → JobSpec[]
  4. SqliteIO.open
  5. ISLDCsvStreamLoader.load_if_needed (isld_pure がなければ初回ロード)
  6. TemplateRegistry.get(template).build(job) → (plan, outputs)
  7. Executor.execute(plan, outputs, ctx)
  8. (optional) --excel: CSV → 多シート Excel (ALL_*/CO_* + META)
  9. 全 jobs 完了で終了

デバッグモード:
  --only-load     : CSV→isld_pure のロードのみ実行（ジョブは実行しない）
  --dry-run       : TEMP生成+SELECT登録まで（exportしない）
  --stop-after X  : X番目のfuncステップで停止（export/cleanupスキップ）
  --excel         : ジョブ完了後に多シート Excel を生成 (config.excel_output)
  --print-plan    : 実行計画を out/plan_summary.txt に出力
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.config.loader import ConfigLoader
from app.config.validate import ConfigValidator
from app.config.compile import JobCompiler
from app.core.executor import Executor
from app.core.progress import AsciiProgress
from app.core.types import ConfigError, PlanError, SqlError
from app.funcs.library import create_default_library
from app.io.sqlite_io import SqliteIO
from app.preprocess.isld_csv_stream_loader import load_if_needed
from app.templates.registry import create_default_registry


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ISLD Pipeline")
    p.add_argument("--config", default="config.json", help="config.json のパス")
    p.add_argument("--only-load", action="store_true",
                   help="CSV→isld_pure のロードのみ実行")
    p.add_argument("--dry-run", action="store_true",
                   help="TEMP生成+SELECT登録まで（exportしない）")
    p.add_argument("--stop-after", type=str, default=None,
                   help="指定 func 名の実行後に停止 (例: enrich)")
    p.add_argument("--excel", action="store_true",
                   help="ジョブ完了後に多シート Excel を生成")
    p.add_argument("--print-plan", action="store_true",
                   help="実行計画を plan_summary.txt に出力")
    return p.parse_args()


def _build_scope_summary(job) -> str:
    """ジョブの scope を人間向けに要約する"""
    parts: list[str] = []
    s = job.scope
    if s.companies:
        parts.append(f"companies={s.companies}")
    if s.country_prefixes:
        parts.append(f"country_prefixes={s.country_prefixes}")
    elif s.countries:
        parts.append(f"countries={s.countries}")
    if s.country_mode and s.country_mode != "ALL":
        parts.append(f"country_mode={s.country_mode}")
    if s.gen_flags:
        parts.append(f"gen_flags={s.gen_flags}")
    if s.ess_flags:
        parts.append(f"ess_flags={s.ess_flags}")
    if s.version_prefixes:
        parts.append(f"version={s.version_prefixes}")
    if s.date_from or s.date_to:
        parts.append(f"date={s.date_from or ''}~{s.date_to or ''}")
    return "; ".join(parts) if parts else "(フィルタなし)"


def _print_plan(jobs, raw_config: dict, out_dir: Path) -> None:
    """実行計画を plan_summary.txt に出力"""
    out_dir.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("=" * 60)
    lines.append("ISLD Pipeline — 実行計画サマリ")
    lines.append("=" * 60)
    for job in jobs:
        lines.append(f"\n--- Job: {job.job_id} ---")
        lines.append(f"  template    : {job.template}")
        lines.append(f"  description : {job.job_description or '(なし)'}")
        lines.append(f"  scope       : {_build_scope_summary(job)}")
        lines.append(f"  unique.unit : {job.unique.unit}")
        lines.append(f"  period      : {job.time_bucket.period}")
        if job.filters_explain:
            lines.append(f"  filters     :")
            for fe in job.filters_explain:
                lines.append(f"    - {fe}")
    path = out_dir / "plan_summary.txt"
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  [plan] {path}")


def _build_excel(raw_config: dict, jobs, out_dir: Path, progress: AsciiProgress) -> None:
    """ジョブの CSV 出力を ALL_*/CO_* シート + META に統合した Excel を生成する"""
    from app.io.excel_io import build_analysis_excel

    excel_cfg = raw_config.get("excel_output", {})
    if not excel_cfg.get("enabled", False):
        progress.step("[excel] excel_output.enabled=false → スキップ")
        return

    excel_path = excel_cfg.get("path", str(out_dir / "analysis_results.xlsx"))
    companies = excel_cfg.get("companies", {})
    include_meta = excel_cfg.get("meta_sheet", True)

    # CSV マップ: job_id → CSV ファイルパスを構築
    csv_map: dict[str, Path] = {}
    for job in jobs:
        extra = job.extra or {}
        out_csv = extra.get("out_csv") or f"{job.job_id}.csv"
        csv_path = out_dir / out_csv
        if csv_path.exists():
            csv_map[job.job_id] = csv_path

    if not csv_map:
        progress.step("[excel] 出力CSVが見つかりません → スキップ")
        return

    # job meta 情報を構築
    job_meta = []
    for job in jobs:
        job_meta.append({
            "job_id": job.job_id,
            "template": job.template,
            "job_description": job.job_description,
            "scope_summary": _build_scope_summary(job),
            "unique_unit": job.unique.unit,
            "period": job.time_bucket.period,
        })

    progress.step(f"[excel] {len(csv_map)} CSV → {excel_path}")
    result = build_analysis_excel(
        csv_map=csv_map,
        excel_path=excel_path,
        companies=companies,
        job_meta=job_meta,
        include_meta=include_meta,
    )
    progress.step(f"[excel] 完了: {result}")


def main(args: argparse.Namespace | None = None) -> None:
    if args is None:
        args = parse_args()

    progress = AsciiProgress(enabled=True)

    try:
        # ── 1. Config ロード ──
        progress.start("Config")
        raw = ConfigLoader.load(args.config)
        ConfigValidator.validate(raw)
        jobs = JobCompiler.compile(raw)
        progress.finish(f"{len(jobs)} ジョブを読み込み")

        # ── 2. SQLite 接続 ──
        env = jobs[0].env  # env は全ジョブ共通
        sio = SqliteIO(env.sqlite_path)
        sio.open()

        try:
            # ── 3. CSV → isld_pure (初回のみ) ──
            load_if_needed(env, sio, progress)

            if args.only_load:
                progress.start("全体")
                progress.finish("--only-load: ロード完了、ジョブはスキップ")
                return

            # ── 4. Library / Registry 準備 ──
            library = create_default_library()
            registry = create_default_registry()
            executor = Executor(sio, library, progress)

            out_dir = Path(env.out_dir)

            # ── 4.5. --print-plan ──
            if args.print_plan:
                _print_plan(jobs, raw, out_dir)

            # ── 5. ジョブ実行 ──
            for job in jobs:
                builder = registry.get(job.template)
                plan, outputs = builder.build(job)
                executor.execute(
                    plan, outputs, job,
                    dry_run=args.dry_run,
                    stop_after=args.stop_after,
                )

            # ── 6. Excel 統合出力 ──
            if args.excel and not args.dry_run:
                progress.start("Excel統合")
                _build_excel(raw, jobs, out_dir, progress)
                progress.finish("Excel統合完了")

            progress.start("全体")
            suffix = ""
            if args.dry_run:
                suffix = " (dry-run: exportスキップ)"
            elif args.stop_after:
                suffix = f" (stop-after: {args.stop_after})"
            progress.finish(f"全ジョブ正常完了{suffix}")

        finally:
            sio.close()

    except ConfigError as e:
        print(f"\n[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
    except PlanError as e:
        print(f"\n[ERROR] {e}", file=sys.stderr)
        sys.exit(2)
    except SqlError as e:
        print(f"\n[ERROR] {e}", file=sys.stderr)
        sys.exit(3)
    except Exception as e:
        print(f"\n[UNEXPECTED] {type(e).__name__}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(99)


if __name__ == "__main__":
    main()
