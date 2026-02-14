"""
core/plan.py  –  Plan (FuncRef 配列) とバリデーション
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.core.types import FuncRef, PlanError


@dataclass
class Plan:
    """ジョブの実行計画: FuncRef のリスト"""
    job_id: str
    steps: list[FuncRef] = field(default_factory=list)

    def add(self, func_name: str, save_as: str = "", **kwargs: Any) -> "Plan":
        self.steps.append(FuncRef(func_name=func_name, args=kwargs, save_as=save_as))
        return self

    def __len__(self) -> int:
        return len(self.steps)


class PlanValidator:
    """Plan の静的検査"""

    @staticmethod
    def validate(plan: Plan, library: "FuncLibrary") -> None:
        """
        - 全ステップの func_name が library に存在するか
        - save_as 論理名の重複がないか
        - scope → unique → enrich の順序が守られているか
        """
        from app.funcs.library import FuncLibrary

        seen_saves: set[str] = set()
        order_phases = {"scope": 0, "unique": 1, "enrich": 2}
        last_phase = -1

        for i, step in enumerate(plan.steps):
            # func 存在チェック
            if not library.has(step.func_name):
                raise PlanError(
                    f"Step {i}: func '{step.func_name}' は未登録です"
                )

            # save_as 重複チェック
            if step.save_as:
                if step.save_as in seen_saves:
                    raise PlanError(
                        f"Step {i}: save_as '{step.save_as}' が重複しています"
                    )
                seen_saves.add(step.save_as)

            # 順序チェック (scope/unique/enrich のみ)
            phase = _classify_phase(step.func_name)
            if phase is not None:
                phase_order = order_phases.get(phase, 99)
                if phase_order < last_phase:
                    raise PlanError(
                        f"Step {i}: '{step.func_name}' の順序が不正です "
                        f"(scope→unique→enrich の順を守ってください)"
                    )
                last_phase = phase_order


def _classify_phase(func_name: str) -> str | None:
    """func_name → phase 分類"""
    if func_name.startswith("scope"):
        return "scope"
    if func_name.startswith("unique"):
        return "unique"
    if func_name.startswith("enrich"):
        return "enrich"
    return None
