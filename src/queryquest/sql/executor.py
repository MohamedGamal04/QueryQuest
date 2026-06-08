"""Interactive SQL execution facade over the split sql/* modules.

The heavy lifting now lives in validation/rewrite/registry/execution/writeback.
This module keeps the original `execute_sql_statements` entry point (and the
symbols the tests import) so existing callers and tests keep working while the
async core engine is built on top of the same pieces.

Author: mohamedgamal04
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
from rich.console import Console
from rich.prompt import Prompt

from .preview import print_dataframe_as_table
from .registry import (
    SqlRewriteContext,
    WorkbookRecord,
    _build_execution_context,
    _column_name_from_value,
    _normalize_columns,
    _table_name_from_file,
    _table_name_from_sheet,
)
from .rewrite import (
    _canonical_identifier,
    _normalize_single_quoted_table_identifiers,
    _normalize_sql_statement,
    _prepare_statement,
    _quote_known_identifiers,
    _rewrite_to_normalized_identifiers,
    _strip_identifier_quotes,
)
from .execution import (
    _build_update_change_predicate,
    _delete_statement_to_scope_query,
    _execute_statement_safely,
    _extract_delete_table_name,
    _extract_update_set_clause,
    _extract_update_table_name,
    _extract_update_where_clause,
    _handle_sql_execution_error,
    _print_delete_preview,
    _print_sql_preview,
    _print_update_preview,
    _split_top_level_csv,
    _update_statement_to_scope_query,
)
from .validation import ALLOWED_SQL_COMMANDS, _is_dml_statement, _strip_leading_sql_noise, _validate_sql_allowlist
from .writeback import _extract_target_table_name, _save_dataframe_to_workbook

__all__ = [
    "ALLOWED_SQL_COMMANDS",
    "SqlRewriteContext",
    "WorkbookRecord",
    "execute_sql_statements",
    "_build_execution_context",
    "_build_update_change_predicate",
    "_extract_update_table_name",
    "_extract_update_where_clause",
    "_normalize_single_quoted_table_identifiers",
    "_prepare_statement",
    "_quote_known_identifiers",
    "_rewrite_to_normalized_identifiers",
    "_strip_identifier_quotes",
    "_update_statement_to_scope_query",
    "_validate_sql_allowlist",
]


def execute_sql_statements(
    sql_statements: list[str],
    console: Console | None = None,
    excel_dir: str | Path | None = None,
) -> None:
    """Execute SQL statements and optionally persist DML changes to Excel files."""
    active_console = console or Console()
    if not sql_statements:
        active_console.print("No SQL statements to execute.")
        return

    for statement in sql_statements:
        refusal_reason = _validate_sql_allowlist(statement)
        if refusal_reason is not None:
            active_console.print(
                "[red]Refused:[/red] Only SELECT, INSERT, UPDATE, and DELETE statements are allowed. "
                f"{refusal_reason}."
            )
            active_console.print(f"Skipped statement: {statement}")
            return

    _print_sql_preview(sql_statements, active_console)

    connection = duckdb.connect()
    rewrite_context, workbook_records, workbook_sheet_data = _build_execution_context(connection, excel_dir=excel_dir)

    try:
        wrote_data = False
        for statement in sql_statements:
            prepared_statement = _prepare_statement(statement, rewrite_context)

            if statement.lstrip().lower().startswith("delete"):
                _print_delete_preview(connection, prepared_statement, active_console)
                wrote_data = True
                continue

            if statement.lstrip().lower().startswith("update"):
                affected_rows = _print_update_preview(connection, prepared_statement, active_console)
                if affected_rows > 0:
                    wrote_data = True
                continue

            cursor = _execute_statement_safely(connection, prepared_statement, active_console)
            if cursor is None:
                continue

            if _is_dml_statement(statement):
                wrote_data = True

            if cursor.description is None:
                if cursor.rowcount != -1:
                    active_console.print(cursor.rowcount)
                continue

            results = cursor.df()
            print_dataframe_as_table(results, active_console)

        if not wrote_data:
            return

        should_save = Prompt.ask(
            "Save changes back to the Excel files?",
            choices=["y", "n"],
            default="n",
            console=active_console,
        ).strip()
        if should_save.lower() != "y":
            return

        saved_results: dict[str, pd.DataFrame] = {}
        workbook_updates: dict[Path, dict[str, pd.DataFrame]] = {}

        for statement in sql_statements:
            if not _is_dml_statement(statement):
                continue

            prepared_statement = _prepare_statement(statement, rewrite_context)
            target_table_name = _extract_target_table_name(prepared_statement)
            if target_table_name is None:
                continue

            workbook_record = workbook_records.get(str(target_table_name))
            if workbook_record is None:
                continue

            file_path = workbook_record["file_path"]
            if file_path not in workbook_updates:
                workbook_updates[file_path] = dict(workbook_sheet_data.get(file_path, {}))

            current_df = connection.table(workbook_record["table_name"]).df()
            workbook_updates[file_path][workbook_record["sheet_name"]] = current_df

            saved_results[workbook_record["table_name"]] = current_df.copy()

        for file_path, sheet_data in workbook_updates.items():
            if not sheet_data:
                continue
            _save_dataframe_to_workbook(file_path, sheet_data)

        if not workbook_updates:
            return

        active_console.print("Changes saved.")
        for table_name, saved_df in saved_results.items():
            preview_df = saved_df.head(20)
            print_dataframe_as_table(preview_df, active_console, title=f"Query result ({table_name})")
            if len(saved_df) > len(preview_df):
                active_console.print(f"Showing first {len(preview_df)} of {len(saved_df)} rows.")
    finally:
        connection.close()
