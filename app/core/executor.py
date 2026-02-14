"""
core/executor.py  –  Plan 実行エンジン

PlanValidator → TEMP物理名払い出し → SQL実行 → SELECT登録 → Export → Cleanup
"""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Optional

from app.core.plan import Plan, PlanValidator
from app.core.progress import AsciiProgress
from app.core.types import (
    FuncRef,
    JobSpec,
    OutputSpec,
    SelectSpec,
    SqlError,
)
from app.funcs.base import ExecutionContext, FuncResult
from app.funcs.library import FuncLibrary
from app.io.csv_io import CsvIO
from app.io.excel_io import ExcelIO
from app.io.sqlite_io import SqliteIO


class SelectRegistry:
    """SELECT 定義をメモリ上に保持（DB には保存しない）"""

    def __init__(self) -> None:
        self._selects: dict[str, SelectSpec] = {}

    def register(self, ref_name: str, spec: SelectSpec) -> None:
        self._selects[ref_name] = spec

    def get(self, ref_name: str) -> SelectSpec:
        s = self._selects.get(ref_name)
        if s is None:
            raise KeyError(f"SelectRef '{ref_name}' は未登録です")
        return s


class Executor:
    """Plan を実行するエンジン"""

    def __init__(
        self,
        sio: SqliteIO,
        library: FuncLibrary,
        progress: AsciiProgress,
    ):
        self._sio = sio
        self._library = library
        self._progress = progress

    def execute(
        self,
        plan: Plan,
        outputs: list[OutputSpec],
        job: JobSpec,
        dry_run: bool = False,
        stop_after: str | None = None,
    ) -> None:
        """
        1 ジョブ分の Plan を実行し、outputs を書き出す。

        Parameters
        ----------
        dry_run : bool
            True なら TEMP 生成 + SELECT 登録まで (export しない)
        stop_after : str | None
            指定 func 名の実行後に停止 (例: "enrich")
        """
        run_id = uuid.uuid4().hex[:8]
        ctx = ExecutionContext(run_id=run_id, job_id=plan.job_id)
        select_registry = SelectRegistry()

        self._progress.start(f"Job: {plan.job_id}")

        # ── Plan 検証 ──
        PlanValidator.validate(plan, self._library)

        # ── Step 実行 (cleanup は export 後に遅延) ──
        deferred_cleanup: FuncResult | None = None

        for i, step in enumerate(plan.steps):
            func = self._library.get(step.func_name)
            sig = func.signature()

            self._progress.step(f"[{i+1}/{len(plan)}] {step.func_name}", step.save_as or "")

            result = func.build_sql(ctx, step.args)

            if step.func_name == "cleanup":
                # cleanup は export 後に実行するため保留
                deferred_cleanup = result
            elif sig.produces == "select":
                # SELECT → registry に登録のみ
                spec = SelectSpec(
                    ref_name=step.save_as,
                    sql=result.sql,
                    params=result.params,
                    columns=result.columns,
                    description=result.description,
                )
                select_registry.register(step.save_as, spec)
            else:
                # TEMP 作成
                self._sio.execute(result.sql, result.params)
                self._sio.commit()
                # TEMP行数をログ出力
                try:
                    phys = ctx.resolve_temp(step.save_as) if step.save_as else None
                    if phys:
                        row = self._sio.query_one(f"SELECT COUNT(*) FROM [{phys}];")
                        if row:
                            self._progress.step(f"    rows={row[0]:,}")
                except Exception:
                    pass

            # --stop-after: 指定 func で停止
            if stop_after and step.func_name == stop_after:
                self._progress.step(f"--stop-after {stop_after}: 以降のステップをスキップ")
                self._progress.finish(f"Job {plan.job_id} (stop-after)")
                return

        # ── Export ──
        if dry_run:
            self._progress.step("--dry-run: export をスキップ")
        else:
            out_dir = Path(job.env.out_dir)
            for out_spec in outputs:
                sel = select_registry.get(out_spec.select_ref)
                filename = out_spec.filename or f"{plan.job_id}_{out_spec.select_ref}.csv"
                out_path = out_dir / filename

                self._progress.step(f"Export: {filename}")

                if out_spec.format == "excel":
                    count = ExcelIO.export_select(
                        self._sio, sel, out_path, out_spec.null_policy
                    )
                else:
                    count = CsvIO.export_select(
                        self._sio, sel, out_path, out_spec.null_policy
                    )

                self._progress.step(f"  → {count:,} 行出力")

        # ── Cleanup (best-effort, export 後) ──
        if deferred_cleanup is not None:
            self._progress.step("cleanup")
            for stmt in deferred_cleanup.sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    try:
                        self._sio.execute(stmt + ";")
                    except Exception:
                        pass  # best-effort

        self._progress.finish(f"Job {plan.job_id} 完了")
