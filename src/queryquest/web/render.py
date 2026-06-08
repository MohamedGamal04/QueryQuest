"""Render an EngineResult as chat-ready markdown.

Kept pure (no Chainlit imports) so the Chainlit handler stays trivial and the
formatting is unit-testable on its own.

Author: mohamedgamal04
"""

from __future__ import annotations

import re

from ..core.models import EngineResult, StatementResult

ROW_LIMIT = 50


def _cell(value: object) -> str:
    """Render one cell, escaping pipes so the markdown table stays intact."""
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ")


def _markdown_table(columns: list[str], rows: list[dict]) -> str:
    """Build a markdown table from preview columns and row dicts."""
    if not columns:
        columns = list(rows[0].keys()) if rows else []
    if not columns:
        return "_(no columns)_"

    header = "| " + " | ".join(_cell(column) for column in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    lines = [header, separator]
    for row in rows[:ROW_LIMIT]:
        lines.append("| " + " | ".join(_cell(row.get(column)) for column in columns) + " |")
    return "\n".join(lines)


def _render_statement(statement: StatementResult) -> str:
    """Render a single statement's outcome."""
    if statement.error is not None:
        return f"❌ **Refused/failed:** {statement.error}\n\n```sql\n{statement.sql}\n```"

    if statement.kind == "select":
        if statement.row_count == 0:
            if re.search(r"\bjoin\b", statement.sql, flags=re.IGNORECASE):
                return (
                    "⚠️ The join matched no rows — the two sheets likely share no "
                    "matching key column, so there is nothing to join on."
                )
            return "ℹ️ The query ran but matched no rows. Check the filter or column values."

        table = _markdown_table(statement.columns, statement.rows)
        if statement.truncated:
            table += f"\n\n_Showing first {len(statement.rows)} of {statement.row_count} rows._"
        return table

    return f"✏️ **{statement.kind.upper()}** affected {statement.row_count} row(s)."


def format_result_markdown(result: EngineResult) -> str:
    """Turn an EngineResult into a single markdown message for the chat UI."""
    if result.error is not None:
        return f"⚠️ {result.error}"

    parts: list[str] = []
    if result.explanation:
        parts.append(result.explanation)

    for statement in result.statements:
        parts.append(_render_statement(statement))

    if result.wrote_back and result.writeback_targets:
        saved = "\n".join(
            f"💾 Saved **{target.affected_rows}** row(s) to `{target.file_path.name}` "
            f"(sheet `{target.sheet_name}`)."
            for target in result.writeback_targets
        )
        parts.append(saved)
    elif result.writeback_targets and not result.wrote_back:
        parts.append("⚠️ Changes were **not** saved to disk.")

    if not parts:
        return "_(no output)_"
    return "\n\n".join(parts)
