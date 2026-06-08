"""Identifier rewriting that lets model SQL use human-friendly names.

The model often emits table/column names with spaces, original casing, or the
raw workbook/sheet labels. I rewrite those into the normalized identifiers I
register in DuckDB so the statement actually runs.

Author: mohamedgamal04
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .registry import SqlRewriteContext


def _canonical_identifier(value: str) -> str:
    """Return lowercase alphanumeric-only identifier for tolerant matching."""
    return re.sub(r"[^0-9A-Za-z]+", "", value).lower()


def _normalize_sql_statement(statement: str) -> str:
    """Normalize SQL quoting style for DuckDB parsing."""
    return statement.replace("`", '"')


def _strip_identifier_quotes(identifier: str) -> str:
    """Strip one layer of common identifier quotes."""
    cleaned = identifier.strip()
    if cleaned[:1] in {'`', '"', "'", '['} and cleaned[-1:] in {'`', '"', "'", ']'}:
        return cleaned[1:-1]
    return cleaned


def _normalize_single_quoted_table_identifiers(statement: str, table_identifiers: set[str]) -> str:
    """Convert single-quoted table identifiers to double-quoted table identifiers."""

    def _replace(match: re.Match[str]) -> str:
        keyword = match.group(1)
        identifier = match.group(2)
        if identifier in table_identifiers:
            return f'{keyword} "{identifier}"'
        return match.group(0)

    pattern = re.compile(r"\b(from|update|into|table|join)\s+'([^']+)'", flags=re.IGNORECASE)
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
        r"\b(from|update|into|table|join)\s+([A-Za-z_][A-Za-z0-9_]*)",
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
