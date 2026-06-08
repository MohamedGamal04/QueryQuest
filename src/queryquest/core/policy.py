"""Approval policies that decide whether the engine may execute or write back.

The engine never prompts on its own. It asks a Policy. The CLI supplies an
interactive policy (asks the human); the future autonomous website supplies
`AutoApprovePolicy`. Keeping this rich-free means `core` has no CLI dependency.

Author: mohamedgamal04
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from .models import StatementResult, WritebackTarget


class Policy(ABC):
    """Decision points the engine consults instead of prompting directly."""

    @abstractmethod
    async def approve_execution(self, statements: list[StatementResult]) -> bool:
        """Return True to run the validated statements."""

    @abstractmethod
    async def approve_writeback(self, target: WritebackTarget) -> bool:
        """Return True to persist this DML target back to its workbook."""


class AutoApprovePolicy(Policy):
    """No-human policy for autonomous use; still confines writes to excel_dir."""

    def __init__(self, excel_dir: str | Path | None) -> None:
        self._excel_dir = Path(excel_dir).resolve() if excel_dir is not None else None

    async def approve_execution(self, statements: list[StatementResult]) -> bool:
        return True

    async def approve_writeback(self, target: WritebackTarget) -> bool:
        # Defense in depth: even with no human, never write outside the workbook
        # directory the engine was pointed at.
        if self._excel_dir is None:
            return True
        resolved = target.file_path.resolve()
        return self._excel_dir == resolved or self._excel_dir in resolved.parents


class DenyAllPolicy(Policy):
    """Dry-run policy: generate and validate, but never execute or persist."""

    async def approve_execution(self, statements: list[StatementResult]) -> bool:
        return False

    async def approve_writeback(self, target: WritebackTarget) -> bool:
        return False
