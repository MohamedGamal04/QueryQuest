"""Run prepared statements and build DELETE/UPDATE previews.

I keep the affected-row previews here so the caller can show the user exactly
what a destructive statement will touch before it commits.

Author: mohamedgamal04
"""

from __future__ import annotations

import re

import duckdb
from rich.console import Console

from .preview import print_dataframe_as_table, print_sql_statements_table
from .rewrite import _strip_identifier_quotes


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


def _print_sql_preview(sql_statements: list[str], console: Console) -> None:
    """Show statements that are about to execute."""
    console.print()
    print_sql_statements_table(sql_statements, console)


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
