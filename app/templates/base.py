"""
templates/base.py  –  TemplateBuilder 基底クラス
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from app.core.plan import Plan
from app.core.types import JobSpec, OutputSpec


class TemplateBuilder(ABC):
    """テンプレートからPlan + OutputsSpec を生成する基底"""

    @abstractmethod
    def name(self) -> str:
        """テンプレート名"""
        ...

    @abstractmethod
    def build(self, job: JobSpec) -> tuple[Plan, list[OutputSpec]]:
        """
        JobSpec → (Plan, list[OutputSpec])

        Plan は scope → unique → enrich → select の固定順を守る。
        """
        ...
