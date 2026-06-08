"""Async, prompt-free orchestrator: prompt + policy -> EngineResult.

This is the reusable heart of QueryQuest. The CLI and the future website both
drive it; the only difference is the Policy they pass. All blocking work
(pandas, DuckDB, file IO) is pushed onto worker threads so the event loop stays
free for the autonomous backend.

Author: mohamedgamal04
"""

from __future__ import annotations

import asyncio

import duckdb

from ..sql.execution import _delete_statement_to_scope_query, _update_statement_to_scope_query
from ..sql.registry import WorkbookRecord, _build_execution_context
from ..sql.rewrite import _prepare_statement
from ..sql.validation import validate_statement_static, validate_statement_tables
from ..sql.writeback import _extract_target_table_name, _save_dataframe_to_workbook
from .llm import generate_sql
from .models import EngineConfig, EngineResult, StatementResult, WritebackTarget

PREVIEW_ROWS = 50


def _classify(sql: str) -> str:
    """Return the leading SQL command keyword, lowercased."""
    stripped = sql.lstrip()
    word = ""
    for char in stripped:
        if char.isalpha():
            word += char
        else:
            break
    return word.lower() or "unknown"


def _scope_preview(
    connection: duckdb.DuckDBPyConnection,
    prepared: str,
    kind: str,
) -> tuple[int, list[str], list[dict]]:
    """Count and sample the rows a DELETE/UPDATE will touch, before executing it.

    For UPDATE the scope is the rows whose values actually change; for DELETE it
    is the rows that will be removed. Must run before the statement executes.
    """
    scope = (
        _delete_statement_to_scope_query(prepared)
        if kind == "delete"
        else _update_statement_to_scope_query(prepared)
    )
    if scope is None:
        return 0, [], []
    row = connection.execute(f"SELECT COUNT(*) FROM ({scope}) AS _scope").fetchone()
    count = int(row[0]) if row is not None else 0
    if count == 0:
        return 0, [], []
    dataframe = connection.execute(f"SELECT * FROM ({scope}) AS _scope LIMIT {PREVIEW_ROWS}").df()
    return count, [str(column) for column in dataframe.columns], dataframe.to_dict("records")


def _writeback_target(
    records: dict[str, WorkbookRecord],
    prepared: str,
    affected: int,
    preview_columns: list[str] | None = None,
    preview_rows: list[dict] | None = None,
) -> WritebackTarget | None:
    """Resolve the single sheet a DML statement persists into."""
    name = _extract_target_table_name(prepared)
    if name is None:
        return None
    record = records.get(str(name))
    if record is None:
        return None
    return WritebackTarget(
        file_path=record["file_path"],
        sheet_name=record["sheet_name"],
        table_name=record["table_name"],
        affected_rows=affected,
        preview_columns=preview_columns or [],
        preview_rows=preview_rows or [],
    )


def _run_sql_session(
    config: EngineConfig,
    statement_results: list[StatementResult],
) -> list[tuple[StatementResult, WritebackTarget]]:
    """Register workbooks, run statements, fill previews. Returns DML targets.

    Runs entirely on one worker thread with one DuckDB connection, so the
    connection is never shared across threads. No data is written here.
    """
    connection = duckdb.connect()
    pairs: list[tuple[StatementResult, WritebackTarget]] = []
    try:
        rewrite_context, records, _sheet_data = _build_execution_context(connection, excel_dir=config.excel_dir)
        allowed_tables = set(rewrite_context["table_identifiers"]) | set(rewrite_context["table_name_map"].values())

        for result in statement_results:
            if result.error is not None:
                continue

            prepared = _prepare_statement(result.sql, rewrite_context)
            result.prepared_sql = prepared

            table_error = validate_statement_tables(prepared, allowed_tables)
            if table_error is not None:
                result.error = table_error
                continue

            try:
                if result.kind == "select":
                    dataframe = connection.execute(prepared).df()
                    result.columns = [str(column) for column in dataframe.columns]
                    head = dataframe.head(PREVIEW_ROWS)
                    result.rows = head.to_dict("records")
                    result.row_count = len(dataframe)
                    result.truncated = len(dataframe) > len(head)
                elif result.kind in {"update", "delete"}:
                    affected, preview_columns, preview_rows = _scope_preview(connection, prepared, result.kind)
                    connection.execute(prepared)
                    result.row_count = affected
                    target = _writeback_target(records, prepared, affected, preview_columns, preview_rows)
                    if target is not None:
                        pairs.append((result, target))
                elif result.kind == "insert":
                    cursor = connection.execute(prepared)
                    affected = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                    result.row_count = affected
                    target = _writeback_target(records, prepared, affected)
                    if target is not None:
                        pairs.append((result, target))
                else:
                    result.error = "unsupported statement"
            except Exception as error:  # Keep one bad statement from sinking the run.
                result.error = str(error)

        return pairs
    finally:
        connection.close()


def _apply_writeback(config: EngineConfig, approved_statements: list[StatementResult]) -> None:
    """Re-run approved DML against fresh workbooks and save the changed sheets."""
    connection = duckdb.connect()
    try:
        rewrite_context, records, sheet_data = _build_execution_context(connection, excel_dir=config.excel_dir)
        workbook_updates: dict = {}

        for result in approved_statements:
            prepared = _prepare_statement(result.sql, rewrite_context)
            connection.execute(prepared)
            name = _extract_target_table_name(prepared)
            record = records.get(str(name)) if name is not None else None
            if record is None:
                continue
            file_path = record["file_path"]
            if file_path not in workbook_updates:
                workbook_updates[file_path] = dict(sheet_data.get(file_path, {}))
            workbook_updates[file_path][record["sheet_name"]] = connection.table(record["table_name"]).df()

        for file_path, sheets in workbook_updates.items():
            if sheets:
                _save_dataframe_to_workbook(file_path, sheets)
    finally:
        connection.close()


class QueryEngine:
    """Drive one natural-language request to a validated, policy-gated result."""

    def __init__(self, config: EngineConfig) -> None:
        self.config = config

    async def run(self, user_prompt: str, policy) -> EngineResult:
        """Generate SQL, validate, and (if the policy allows) execute and persist."""
        result = EngineResult(prompt=user_prompt)

        raw_output, statements_sql, explanation, error = await generate_sql(self.config, user_prompt)
        result.raw_llm_output = raw_output
        result.explanation = explanation
        if error is not None:
            result.error = error
            return result

        for sql in statements_sql:
            statement = StatementResult(sql=sql, kind=_classify(sql))
            statement.error = validate_statement_static(sql)
            result.statements.append(statement)

        runnable = [statement for statement in result.statements if statement.error is None]
        if not runnable:
            return result

        if not await policy.approve_execution(runnable):
            return result

        pairs = await asyncio.to_thread(_run_sql_session, self.config, result.statements)
        result.executed = True

        approved_statements: list[StatementResult] = []
        for statement, target in pairs:
            if await policy.approve_writeback(target):
                approved_statements.append(statement)
                result.writeback_targets.append(target)

        if approved_statements:
            await asyncio.to_thread(_apply_writeback, self.config, approved_statements)
            result.wrote_back = True

        return result
