"""Execute model-generated SQL against Excel-backed DuckDB tables.

Flow:
1. Read workbook sheets with pandas (`pd.read_excel`).
2. Normalize first-sheet columns with `df.rename` (spaces -> underscores).
3. Register DataFrames in DuckDB and run SQL statements.
4. Optionally save DML changes back to Excel.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TypedDict

import duckdb
import pandas as pd
from rich.console import Console
from rich.prompt import Prompt

from ..config import EXCEL_DIR
from ..excel.context import list_excel_files
from .preview import print_dataframe_as_table, print_sql_statements_table


ALLOWED_SQL_COMMANDS = {"select", "insert", "update", "delete"}


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


def _normalize_sql_statement(statement: str) -> str:
    """Normalize SQL quoting style for DuckDB parsing."""
    return statement.replace("`", '"')


def _strip_leading_sql_noise(statement: str) -> str:
    """Remove leading whitespace and SQL comments before checking the command."""
    text = statement.lstrip()

    while text:
        if text.startswith("--"):
            newline_index = text.find("\n")
            if newline_index == -1:
                return ""
            text = text[newline_index + 1 :].lstrip()
            continue

        if text.startswith("/*"):
            end_index = text.find("*/", 2)
            if end_index == -1:
                return ""
            text = text[end_index + 2 :].lstrip()
            continue

        break

    return text


def _validate_sql_allowlist(statement: str) -> str | None:
    """Return an error message when a statement is outside the supported allowlist."""
    stripped = _strip_leading_sql_noise(statement)
    if not stripped:
        return "empty SQL statement"

    if ";" in stripped.rstrip().rstrip(";"):
        return "multiple SQL statements are not allowed"

    match = re.match(r"([A-Za-z]+)", stripped)
    if not match:
        return "unable to determine the SQL command"

    keyword = match.group(1).lower()
    if keyword not in ALLOWED_SQL_COMMANDS:
        return f"'{keyword.upper()}' is not allowed"

    if re.search(r"\bjoin\b", stripped, flags=re.IGNORECASE):
        return "'JOIN' is not allowed"

    return None


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

    pattern = re.compile(r"\b(from|update|into|table)\s+'([^']+)'", flags=re.IGNORECASE)
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
        r"\b(from|update|into|table)\s+([A-Za-z_][A-Za-z0-9_]*)",
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


def _extract_delete_table_name(statement: str) -> str | None:
    """Extract target table name from a DELETE statement when possible."""
    match = re.match(
        r'^\s*delete\s+from\s+("[^"]+"|\[[^\]]+\]|`[^`]+`|\'[^\']+\'|[A-Za-z_][A-Za-z0-9_]*)',
        statement,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return _strip_identifier_quotes(match.group(1))


def _extract_update_table_name(statement: str) -> str | None:
    """Extract target table name from an UPDATE statement when possible."""
    match = re.match(
        r'^\s*update\s+("[^"]+"|\[[^\]]+\]|`[^`]+`|\'[^\']+\'|[A-Za-z_][A-Za-z0-9_]*)\s+set\b',
        statement,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return _strip_identifier_quotes(match.group(1))


def _extract_update_where_clause(statement: str) -> str | None:
    """Extract WHERE clause text from an UPDATE statement, when present."""
    trimmed = statement.strip().rstrip(";")
    match = re.search(r"\bwhere\b\s+(.+)$", trimmed, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    where_clause = match.group(1).strip()
    return where_clause or None


def _extract_update_set_clause(statement: str) -> str | None:
    """Extract SET clause text from an UPDATE statement."""
    trimmed = statement.strip().rstrip(";")
    match = re.search(r"\bset\b\s+(.+?)(?:\bwhere\b\s+.+)?$", trimmed, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    set_clause = match.group(1).strip()
    return set_clause or None


def _split_top_level_csv(text: str) -> list[str]:
    """Split comma-separated SQL expressions while respecting quotes/parens."""
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_single_quote = False
    in_double_quote = False
    escape = False

    for char in text:
        if in_single_quote:
            current.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == "'":
                in_single_quote = False
            continue

        if in_double_quote:
            current.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_double_quote = False
            continue

        if char == "'":
            in_single_quote = True
            current.append(char)
            continue

        if char == '"':
            in_double_quote = True
            current.append(char)
            continue

        if char == "(":
            depth += 1
            current.append(char)
            continue

        if char == ")" and depth > 0:
            depth -= 1
            current.append(char)
            continue

        if char == "," and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue

        current.append(char)

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _build_update_change_predicate(statement: str) -> str | None:
    """Build predicate matching only rows whose values will actually change."""
    set_clause = _extract_update_set_clause(statement)
    if not set_clause:
        return None

    comparisons: list[str] = []
    for assignment in _split_top_level_csv(set_clause):
        lhs, sep, rhs = assignment.partition("=")
        if not sep:
            continue
        column_expr = lhs.strip()
        value_expr = rhs.strip()
        if not column_expr or not value_expr:
            continue
        comparisons.append(f"({column_expr} IS DISTINCT FROM ({value_expr}))")

    if not comparisons:
        return None
    return " OR ".join(comparisons)


def _update_statement_to_scope_query(statement: str) -> str | None:
    """Convert an UPDATE statement into a SELECT query for rows that will change."""
    table_name = _extract_update_table_name(statement)
    if table_name is None:
        return None

    where_clause = _extract_update_where_clause(statement)
    change_predicate = _build_update_change_predicate(statement)

    predicates: list[str] = []
    if where_clause:
        predicates.append(f"({where_clause})")
    if change_predicate:
        predicates.append(f"({change_predicate})")

    if predicates:
        return f"SELECT * FROM {table_name} WHERE {' AND '.join(predicates)}"
    return f"SELECT * FROM {table_name}"


def _delete_statement_to_scope_query(statement: str) -> str | None:
    """Convert a DELETE statement into an equivalent SELECT scope query."""
    scope_query = re.sub(
        r"^\s*delete\s+from\s+",
        "SELECT * FROM ",
        statement.strip().rstrip(";"),
        count=1,
        flags=re.IGNORECASE,
    )
    if scope_query.lower().startswith("select * from "):
        return scope_query
    return None


def _print_delete_preview(
    connection: duckdb.DuckDBPyConnection,
    prepared_statement: str,
    console: Console,
) -> None:
    """Show rows targeted by DELETE before execution."""
    scope_query = _delete_statement_to_scope_query(prepared_statement)
    if scope_query is None:
        return

    table_name = _extract_delete_table_name(prepared_statement) or "target_table"
    limited_scope_query = f"SELECT * FROM ({scope_query}) AS delete_scope_preview LIMIT 20"

    try:
        before_count_row = connection.execute(
            f"SELECT COUNT(*) FROM ({scope_query}) AS delete_scope"
        ).fetchone()
        before_count = int(before_count_row[0]) if before_count_row is not None else 0
        before_df = connection.execute(
            limited_scope_query
        ).df()
        console.print(f"[yellow]Affected rows:[/yellow] {before_count} row(s) match the filter.")

        if before_count == 0:
            console.print("[yellow]DELETE skipped:[/yellow] no rows matched the filter.")
            return

        print_dataframe_as_table(before_df, console, title=f"Rows to delete ({table_name})")
    except Exception as error:
        console.print(f"[yellow]DELETE preview skipped:[/yellow] {error}")
        _execute_statement_safely(connection, prepared_statement, console)
        return

    cursor = _execute_statement_safely(connection, prepared_statement, console)
    if cursor is None:
        return


def _print_update_preview(
    connection: duckdb.DuckDBPyConnection,
    prepared_statement: str,
    console: Console,
) -> int:
    """Show before/after rows for UPDATE statements and return affected row count."""
    scope_query = _update_statement_to_scope_query(prepared_statement)
    table_name = _extract_update_table_name(prepared_statement) or "target_table"

    if scope_query is None:
        cursor = _execute_statement_safely(connection, prepared_statement, console)
        if cursor is None or cursor.description is None:
            return max(cursor.rowcount, 0) if cursor is not None and cursor.rowcount is not None else 0
        after_df = cursor.df()
        return len(after_df)

    limited_scope_query = f"SELECT * FROM ({scope_query}) AS update_scope_preview LIMIT 20"

    try:
        before_count_row = connection.execute(
            f"SELECT COUNT(*) FROM ({scope_query}) AS update_scope"
        ).fetchone()
        before_count = int(before_count_row[0]) if before_count_row is not None else 0
        before_df = connection.execute(limited_scope_query).df()
    except Exception as error:
        console.print(f"[yellow]UPDATE preview skipped:[/yellow] {error}")
        cursor = _execute_statement_safely(connection, prepared_statement, console)
        if cursor is None:
            return 0
        if cursor.description is None:
            return max(cursor.rowcount, 0) if cursor.rowcount is not None else 0
        return len(cursor.df())

    console.print(f"[yellow]Affected rows:[/yellow] {before_count} row(s) match the filter.")
    if before_count == 0:
        console.print("[yellow]UPDATE skipped:[/yellow] no rows matched the filter.")
        return 0

    print_dataframe_as_table(before_df, console, title=f"Rows before update ({table_name})")
    if before_count > len(before_df):
        console.print(f"Showing first {len(before_df)} of {before_count} matched rows before update.")

    statement_without_semicolon = prepared_statement.strip().rstrip(";")
    update_with_returning = statement_without_semicolon
    if " returning " not in statement_without_semicolon.lower():
        update_with_returning = f"{statement_without_semicolon} RETURNING *"

    cursor = _execute_statement_safely(connection, update_with_returning, console)
    if cursor is None:
        return 0

    if cursor.description is None:
        # Fallback for engines that execute UPDATE without RETURNING result sets.
        rowcount = max(cursor.rowcount, 0) if cursor.rowcount is not None else 0
        console.print(f"[yellow]Updated rows:[/yellow] {rowcount}")
        return rowcount

    after_df = cursor.df()
    after_count = len(after_df)
    print_dataframe_as_table(after_df.head(20), console, title=f"Rows after update ({table_name})")
    if after_count > 20:
        console.print(f"Showing first 20 of {after_count} rows after update.")
    return after_count


def _save_dataframe_to_workbook(file_path: Path, sheet_data: dict[str, pd.DataFrame]) -> None:
    """Rewrite workbook sheets using in-memory sheet DataFrames."""
    with pd.ExcelWriter(file_path, engine="openpyxl", mode="w") as writer:
        for current_sheet_name, current_df in sheet_data.items():
            current_df.to_excel(writer, sheet_name=current_sheet_name, index=False)


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
    return rewritten


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


