"""Load Excel workbooks into DuckDB and build the identifier rewrite context.

Every sheet of every workbook becomes its own `workbook__sheet` table in a
single DuckDB connection, so reads (including JOINs across sheets) see all of
them at once. I also collect the name mappings the rewrite layer needs.

Author: mohamedgamal04
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TypedDict

import duckdb
import pandas as pd

from ..config import EXCEL_DIR
from ..excel.context import list_excel_files
from .rewrite import _canonical_identifier


class WorkbookRecord(TypedDict):
    """Workbook sheet metadata used for optional save-back after DML."""

    file_path: Path
    table_name: str
    sheet_name: str


class SqlRewriteContext(TypedDict):
    """Identifier mappings used to tolerate human-friendly SQL names."""

    table_identifiers: set[str]
    column_identifiers: set[str]
    table_name_map: dict[str, str]
    table_alias_map: dict[str, str]
    column_name_map: dict[str, str]
    column_alias_map: dict[str, str]


def _table_name_from_file(file_name: str) -> str:
    """Normalize a workbook filename to a SQL-safe table name."""
    stem = file_name.rsplit(".", 1)[0]
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", stem).strip("_")
    return normalized or "excel_data"


def _table_name_from_sheet(file_name: str, sheet_name: str) -> str:
    """Normalize workbook + sheet names into a SQL-safe table identifier."""
    workbook_part = _table_name_from_file(file_name)
    sheet_part = _table_name_from_file(sheet_name)
    return f"{workbook_part}__{sheet_part}"


def _column_name_from_value(value: object) -> str:
    """Normalize a column label by replacing whitespace with underscores."""
    text = str(value).strip()
    normalized = re.sub(r"\s+", "_", text)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "column"


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return DataFrame copy with normalized and unique column names using df.rename."""
    rename_map: dict[object, str] = {}
    seen: dict[str, int] = {}

    for column in df.columns:
        base_name = _column_name_from_value(column)
        count = seen.get(base_name, 0)
        seen[base_name] = count + 1
        rename_map[column] = base_name if count == 0 else f"{base_name}_{count + 1}"

    return df.rename(columns=rename_map).copy()


def _build_execution_context(
    connection: duckdb.DuckDBPyConnection,
    excel_dir: str | Path | None = None,
) -> tuple[SqlRewriteContext, dict[str, WorkbookRecord], dict[Path, dict[str, pd.DataFrame]]]:
    """Load workbooks with pandas, register DuckDB tables, and build rewrite mappings."""
    rewrite_context: SqlRewriteContext = {
        "table_identifiers": set(),
        "column_identifiers": set(),
        "table_name_map": {},
        "table_alias_map": {},
        "column_name_map": {},
        "column_alias_map": {},
    }
    workbook_records: dict[str, WorkbookRecord] = {}
    workbook_sheet_data: dict[Path, dict[str, pd.DataFrame]] = {}
    table_name_counts: dict[str, int] = {}

    selected_excel_dir = excel_dir if excel_dir is not None else EXCEL_DIR
    for file_path in list_excel_files(selected_excel_dir):
        original_table_name = file_path.stem
        workbook_table_name = _table_name_from_file(file_path.name)

        # Use simple pandas read_excel API to load all sheets in one call.
        sheet_data = pd.read_excel(file_path, sheet_name=None)
        if not sheet_data:
            continue

        normalized_sheet_data: dict[str, pd.DataFrame] = {}
        workbook_sheet_data[file_path] = normalized_sheet_data

        first_table_name: str | None = None

        for sheet_name, raw_df in sheet_data.items():
            normalized_df = _normalize_columns(raw_df)
            normalized_sheet_data[sheet_name] = normalized_df

            base_table_name = _table_name_from_sheet(file_path.name, str(sheet_name))
            table_count = table_name_counts.get(base_table_name, 0)
            table_name_counts[base_table_name] = table_count + 1
            table_name = base_table_name if table_count == 0 else f"{base_table_name}_{table_count + 1}"

            if first_table_name is None:
                first_table_name = table_name

            combined_original_name = f"{original_table_name}__{sheet_name}"

            rewrite_context["table_name_map"][combined_original_name] = table_name
            rewrite_context["table_alias_map"][_canonical_identifier(combined_original_name)] = table_name
            rewrite_context["table_alias_map"][_canonical_identifier(table_name)] = table_name
            rewrite_context["table_identifiers"].add(combined_original_name)
            rewrite_context["table_identifiers"].add(table_name)

            for raw_col, normalized_col in zip(raw_df.columns, normalized_df.columns, strict=False):
                raw_name = str(raw_col)
                normalized_name = str(normalized_col)
                rewrite_context["column_identifiers"].add(raw_name)
                rewrite_context["column_name_map"][raw_name] = normalized_name
                rewrite_context["column_alias_map"][_canonical_identifier(raw_name)] = normalized_name
                rewrite_context["column_alias_map"][_canonical_identifier(normalized_name)] = normalized_name

            source_name = f"_source_{table_name}"
            connection.register(source_name, normalized_df)
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {source_name}")

            if combined_original_name != table_name:
                connection.execute(f'DROP VIEW IF EXISTS "{combined_original_name}"')
                connection.execute(f'CREATE VIEW "{combined_original_name}" AS SELECT * FROM {table_name}')

            record: WorkbookRecord = {
                "file_path": file_path,
                "table_name": table_name,
                "sheet_name": sheet_name,
            }
            workbook_records[combined_original_name] = record
            workbook_records[table_name] = record

        if first_table_name is not None:
            rewrite_context["table_name_map"][original_table_name] = first_table_name
            rewrite_context["table_alias_map"][_canonical_identifier(original_table_name)] = first_table_name
            rewrite_context["table_alias_map"][_canonical_identifier(workbook_table_name)] = first_table_name
            rewrite_context["table_identifiers"].add(original_table_name)
            rewrite_context["table_identifiers"].add(workbook_table_name)

            first_record = workbook_records.get(first_table_name)
            if first_record is not None:
                workbook_records[original_table_name] = first_record
                workbook_records[workbook_table_name] = first_record

    return rewrite_context, workbook_records, workbook_sheet_data
