"""Execute model-generated SQL against Excel-backed DuckDB tables.

Flow:
1. Read workbook sheets with pandas (`pd.read_excel`).
2. Normalize first-sheet columns with `df.rename` (spaces -> underscores).
3. Register DataFrames in DuckDB and run SQL statements.
4. Optionally save DML changes back to Excel.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import TypedDict

import duckdb
import pandas as pd
from rich.console import Console
from rich.prompt import Prompt

from ..config import EXCEL_DIR
from ..excel.context import list_excel_files
from .preview import print_dataframe_as_table, print_sql_statements_table


class WorkbookRecord(TypedDict):
    """Workbook metadata used for optional save-back after DML."""

    file_path: Path
    table_name: str
    first_sheet_name: str
    sheet_data: dict[str, pd.DataFrame]


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


def _normalize_sql_statement(statement: str) -> str:
    """Normalize SQL quoting style for DuckDB parsing."""
    return statement.replace("`", '"')


def _canonical_identifier(value: str) -> str:
    """Return lowercase alphanumeric-only identifier for tolerant matching."""
    return re.sub(r"[^0-9A-Za-z]+", "", value).lower()


def _normalize_single_quoted_table_identifiers(statement: str, table_identifiers: set[str]) -> str:
    """Convert single-quoted table identifiers to double-quoted table identifiers."""

    def _replace(match: re.Match[str]) -> str:
        keyword = match.group(1)
        identifier = match.group(2)
        if identifier in table_identifiers:
            return f'{keyword} "{identifier}"'
        return match.group(0)

    pattern = re.compile(r"\b(from|join|update|into|table)\s+'([^']+)'", flags=re.IGNORECASE)
    return pattern.sub(_replace, statement)


def _rewrite_to_normalized_identifiers(
    statement: str,
    table_name_map: dict[str, str],
    table_alias_map: dict[str, str],
    column_name_map: dict[str, str],
    column_alias_map: dict[str, str],
) -> str:
    """Rewrite known table/column variants to normalized identifiers."""
    rewritten = statement

    for original_name, normalized_name in sorted(table_name_map.items(), key=lambda item: len(item[0]), reverse=True):
        rewritten = re.sub(rf'"{re.escape(original_name)}"', normalized_name, rewritten)
        rewritten = re.sub(rf"'{re.escape(original_name)}'", normalized_name, rewritten)
        if " " not in original_name:
            rewritten = re.sub(rf"\b{re.escape(original_name)}\b", normalized_name, rewritten)

    def _replace_table_alias(match: re.Match[str]) -> str:
        keyword = match.group(1)
        table_token = match.group(2)
        normalized_name = table_alias_map.get(_canonical_identifier(table_token))
        if normalized_name:
            return f"{keyword} {normalized_name}"
        return match.group(0)

    rewritten = re.sub(
        r"\b(from|join|update|into|table)\s+([A-Za-z_][A-Za-z0-9_]*)",
        _replace_table_alias,
        rewritten,
        flags=re.IGNORECASE,
    )

    for original_name, normalized_name in sorted(column_name_map.items(), key=lambda item: len(item[0]), reverse=True):
        rewritten = re.sub(rf'"{re.escape(original_name)}"', normalized_name, rewritten)
        rewritten = re.sub(rf"'{re.escape(original_name)}'", normalized_name, rewritten)

    def _replace_qualified_column(match: re.Match[str]) -> str:
        table_or_alias = match.group(1)
        column_token = match.group(2)
        normalized_name = column_alias_map.get(_canonical_identifier(column_token))
        if normalized_name:
            return f"{table_or_alias}.{normalized_name}"
        return match.group(0)

    rewritten = re.sub(r'\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*"([^"]+)"', _replace_qualified_column, rewritten)
    rewritten = re.sub(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*'([^']+)'", _replace_qualified_column, rewritten)
    rewritten = re.sub(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)", _replace_qualified_column, rewritten)

    return rewritten


def _normalize_city_join_comparisons(statement: str) -> str:
    """Normalize city=city join predicates so values like "New York" and "New York City" can match."""

    def _normalize_city_sql(token: str) -> str:
        return f"replace(lower(trim(cast({token} as varchar))), ' city', '')"

    def _replace(match: re.Match[str]) -> str:
        left_alias = match.group(1)
        left_col = match.group(2)
        right_alias = match.group(3)
        right_col = match.group(4)

        if _canonical_identifier(left_col) != "city" or _canonical_identifier(right_col) != "city":
            return match.group(0)

        left_token = f"{left_alias}.{left_col}"
        right_token = f"{right_alias}.{right_col}"
        return f"{_normalize_city_sql(left_token)} = {_normalize_city_sql(right_token)}"

    pattern = re.compile(
        r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*(\"?[A-Za-z_][A-Za-z0-9_]*\"?)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*(\"?[A-Za-z_][A-Za-z0-9_]*\"?)",
        flags=re.IGNORECASE,
    )
    return pattern.sub(_replace, statement)


def _quote_known_identifiers(statement: str, identifiers: set[str]) -> str:
    """Quote known identifiers with spaces so DuckDB parses them correctly."""
    rewritten = statement
    for identifier in sorted(identifiers, key=len, reverse=True):
        if not identifier or " " not in identifier:
            continue
        pattern = re.compile(rf'(?<!["\'])({re.escape(identifier)})(?!["\'])')
        rewritten = pattern.sub(r'"\1"', rewritten)
    return rewritten


def _strip_identifier_quotes(identifier: str) -> str:
    """Strip one layer of common identifier quotes."""
    cleaned = identifier.strip()
    if cleaned[:1] in {'`', '"', "'", '['} and cleaned[-1:] in {'`', '"', "'", ']'}:
        return cleaned[1:-1]
    return cleaned


def _is_dml_statement(statement: str) -> bool:
    """Check whether a statement mutates data and may require save confirmation."""
    return statement.lstrip().lower().startswith(("delete", "insert", "update"))


def _is_join_statement(statement: str) -> bool:
    """Check whether a SELECT uses JOIN to enable concise preview output."""
    return bool(re.search(r"\bjoin\b", statement, flags=re.IGNORECASE))


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


def _save_dataframe_to_workbook(file_path: Path, first_sheet_name: str, sheet_data: dict[str, pd.DataFrame], df: pd.DataFrame) -> None:
    """Rewrite workbook sheets, replacing only the first sheet content."""
    with pd.ExcelWriter(file_path, engine="openpyxl", mode="w") as writer:
        for current_sheet_name, current_df in sheet_data.items():
            data_to_write = df if current_sheet_name == first_sheet_name else current_df
            data_to_write.to_excel(writer, sheet_name=current_sheet_name, index=False)


def read_sql_handoff_file(file_path: str) -> list[str]:
    """Read SQL statements from the temporary JSON handoff file."""
    with open(file_path, "r", encoding="utf-8") as file:
        payload = json.loads(file.read())
    return payload.get("sql_statements", [])


def _print_sql_preview(sql_statements: list[str], console: Console) -> None:
    """Show statements that are about to execute."""
    console.print()
    print_sql_statements_table(sql_statements, console)


def _handle_sql_execution_error(error: Exception, statement: str, console: Console) -> None:
    """Render SQL execution errors without aborting the interactive CLI session."""
    if isinstance(error, duckdb.Error):
        console.print(f"[red]SQL execution failed:[/red] {error}")
    else:
        console.print(f"[red]Unexpected SQL execution error:[/red] {error}")
    console.print(f"Skipped statement: {statement}")


def _execute_statement_safely(
    connection: duckdb.DuckDBPyConnection,
    statement: str,
    console: Console,
) -> duckdb.DuckDBPyConnection | None:
    """Execute one SQL statement and return None when execution fails."""
    try:
        return connection.execute(statement)
    except Exception as error:  # Broad on purpose to keep the CLI alive.
        _handle_sql_execution_error(error, statement, console)
        return None


def _build_execution_context(connection: duckdb.DuckDBPyConnection) -> tuple[SqlRewriteContext, dict[str, WorkbookRecord]]:
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

    for file_path in list_excel_files(EXCEL_DIR):
        original_table_name = file_path.stem
        table_name = _table_name_from_file(file_path.name)

        # Use simple pandas read_excel API to load all sheets in one call.
        sheet_data = pd.read_excel(file_path, sheet_name=None)
        if not sheet_data:
            continue

        first_sheet_name = next(iter(sheet_data))
        first_sheet_raw_df = sheet_data[first_sheet_name]
        first_sheet_df = _normalize_columns(first_sheet_raw_df)
        sheet_data[first_sheet_name] = first_sheet_df

        rewrite_context["table_name_map"][original_table_name] = table_name
        rewrite_context["table_alias_map"][_canonical_identifier(original_table_name)] = table_name
        rewrite_context["table_alias_map"][_canonical_identifier(table_name)] = table_name
        rewrite_context["table_identifiers"].add(original_table_name)
        rewrite_context["table_identifiers"].add(table_name)

        for raw_col, normalized_col in zip(first_sheet_raw_df.columns, first_sheet_df.columns, strict=False):
            raw_name = str(raw_col)
            normalized_name = str(normalized_col)
            rewrite_context["column_identifiers"].add(raw_name)
            rewrite_context["column_name_map"][raw_name] = normalized_name
            rewrite_context["column_alias_map"][_canonical_identifier(raw_name)] = normalized_name
            rewrite_context["column_alias_map"][_canonical_identifier(normalized_name)] = normalized_name

        source_name = f"_source_{table_name}"
        connection.register(source_name, first_sheet_df)
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {source_name}")

        if original_table_name != table_name:
            connection.execute(f'DROP VIEW IF EXISTS "{original_table_name}"')
            connection.execute(f'CREATE VIEW "{original_table_name}" AS SELECT * FROM {table_name}')

        record: WorkbookRecord = {
            "file_path": file_path,
            "table_name": table_name,
            "first_sheet_name": first_sheet_name,
            "sheet_data": sheet_data,
        }
        workbook_records[original_table_name] = record
        workbook_records[table_name] = record

    return rewrite_context, workbook_records


def _prepare_statement(statement: str, rewrite_context: SqlRewriteContext) -> str:
    """Normalize and rewrite model SQL to table/column identifiers known by DuckDB."""
    rewritten = _normalize_sql_statement(statement)
    rewritten = _rewrite_to_normalized_identifiers(
        rewritten,
        rewrite_context["table_name_map"],
        rewrite_context["table_alias_map"],
        rewrite_context["column_name_map"],
        rewrite_context["column_alias_map"],
    )
    rewritten = _normalize_single_quoted_table_identifiers(rewritten, rewrite_context["table_identifiers"])
    rewritten = _quote_known_identifiers(rewritten, rewrite_context["table_identifiers"])
    rewritten = _quote_known_identifiers(rewritten, rewrite_context["column_identifiers"])
    # A second pass ensures identifiers quoted in the previous step still map
    # to normalized DuckDB base tables (not compatibility views).
    rewritten = _rewrite_to_normalized_identifiers(
        rewritten,
        rewrite_context["table_name_map"],
        rewrite_context["table_alias_map"],
        rewrite_context["column_name_map"],
        rewrite_context["column_alias_map"],
    )
    rewritten = _normalize_city_join_comparisons(rewritten)
    return rewritten


def execute_sql_statements(sql_statements: list[str], console: Console | None = None) -> None:
    """Execute SQL statements and optionally persist DML changes to Excel files."""
    active_console = console or Console()
    if not sql_statements:
        active_console.print("No SQL statements to execute.")
        return

    _print_sql_preview(sql_statements, active_console)

    connection = duckdb.connect()
    rewrite_context, workbook_records = _build_execution_context(connection)

    try:
        wrote_data = False
        for statement in sql_statements:
            prepared_statement = _prepare_statement(statement, rewrite_context)
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
            if _is_join_statement(statement):
                preview_df = results.head(20)
                print_dataframe_as_table(preview_df, active_console, title="Query result (JOIN preview)")
                if len(results) > len(preview_df):
                    active_console.print(f"Showing first {len(preview_df)} of {len(results)} join rows.")
            else:
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
        saved_files: set[Path] = set()

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
            if file_path in saved_files:
                continue

            current_df = connection.table(workbook_record["table_name"]).df()
            sheet_data = dict(workbook_record["sheet_data"])
            sheet_data[workbook_record["first_sheet_name"]] = current_df

            _save_dataframe_to_workbook(
                file_path,
                workbook_record["first_sheet_name"],
                sheet_data,
                current_df,
            )
            saved_files.add(file_path)
            saved_results[workbook_record["table_name"]] = current_df.copy()

        if not saved_files:
            return

        active_console.print("Changes saved.")
        for table_name, saved_df in saved_results.items():
            preview_df = saved_df.head(20)
            print_dataframe_as_table(preview_df, active_console, title=f"Query result ({table_name})")
            if len(saved_df) > len(preview_df):
                active_console.print(f"Showing first {len(preview_df)} of {len(saved_df)} rows.")
    finally:
        connection.close()


