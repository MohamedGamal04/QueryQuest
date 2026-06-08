"""Persist DML results back into the originating Excel workbook.

Write-back assumes a single target table per statement (DML never uses JOIN),
so I resolve the one table a statement touches and rewrite that sheet.

Author: mohamedgamal04
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .rewrite import _strip_identifier_quotes


def _extract_target_table_name(statement: str) -> str | None:
    """Best-effort extraction of target table names from DML statements."""
    update_match = re.match(r"^\s*update\s+(.+?)\s+set\b", statement, flags=re.IGNORECASE)
    if update_match:
        return _strip_identifier_quotes(update_match.group(1))

    delete_match = re.match(r"^\s*delete\s+from\s+(.+?)(?:\s+where\b|\s*$)", statement, flags=re.IGNORECASE)
    if delete_match:
        return _strip_identifier_quotes(delete_match.group(1))

    insert_match = re.match(
        r"^\s*insert\s+into\s+(.+?)(?:\s*\(|\s+values\b|\s+select\b|\s*$)",
        statement,
        flags=re.IGNORECASE,
    )
    if insert_match:
        return _strip_identifier_quotes(insert_match.group(1))

    return None


def _save_dataframe_to_workbook(file_path: Path, sheet_data: dict[str, pd.DataFrame]) -> None:
    """Rewrite workbook sheets using in-memory sheet DataFrames."""
    with pd.ExcelWriter(file_path, engine="openpyxl", mode="w") as writer:
        for current_sheet_name, current_df in sheet_data.items():
            current_df.to_excel(writer, sheet_name=current_sheet_name, index=False)
