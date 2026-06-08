"""Validate model-generated SQL before it ever touches DuckDB.

This is the safety boundary. Because I plan to run this with no human approval
on the web, anything that is not a plain row-level SELECT/INSERT/UPDATE/DELETE
against a registered workbook table must be refused here.

Two layers:
- `validate_statement_static`: text-only checks (no DB context). Blocks comments,
  multi-statements, disallowed commands, JOIN-in-DML, and DuckDB file/admin
  access (read_csv, ATTACH, COPY, ...).
- `validate_statement_tables`: after rewriting, confirms every table the
  statement touches is one I actually registered (a positive allowlist).

Author: mohamedgamal04
"""

from __future__ import annotations

import re


ALLOWED_SQL_COMMANDS = {"select", "insert", "update", "delete"}

# DuckDB table functions that read from the filesystem or run nested SQL. These
# are the real exfiltration vector: a SELECT that wraps one bypasses the command
# allowlist. I block them whenever they appear as a function call.
FILE_FUNCTION_BLOCKLIST = {
    "read_csv",
    "read_csv_auto",
    "read_parquet",
    "read_json",
    "read_json_auto",
    "read_json_objects",
    "read_ndjson",
    "read_ndjson_objects",
    "read_text",
    "read_blob",
    "read_xlsx",
    "glob",
    "parquet_scan",
    "csv_scan",
    "sniff_csv",
    "query",
    "query_table",
}

# Admin / attach / extension keywords that must never appear, even embedded.
# NOTE: deliberately excludes SET/LOAD/RESET/CALL/IMPORT/EXPORT — those collide
# with legitimate column names or UPDATE's SET clause. Leading uses of those are
# already caught by the command allowlist.
ADMIN_TOKEN_BLOCKLIST = {
    "attach",
    "detach",
    "pragma",
    "install",
    "copy",
}


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


def _is_dml_statement(statement: str) -> bool:
    """Check whether a statement mutates data and may require save confirmation."""
    return statement.lstrip().lower().startswith(("delete", "insert", "update"))


def _scan_sql(text: str) -> tuple[str, bool, list[int]]:
    """Scan SQL once, tracking string-literal and comment state.

    Returns a tuple of:
    - `cleaned`: the statement with string-literal bodies and comments replaced
      by spaces, so later keyword/token checks never trip on data or comments.
    - `has_comment`: whether any `--` or block comment was found outside literals.
    - `semicolons`: positions of `;` that sit at the top level (not in a literal
      or comment).
    """
    cleaned: list[str] = []
    has_comment = False
    semicolons: list[int] = []

    in_single = False
    in_double = False
    escape = False
    i = 0
    length = len(text)

    while i < length:
        char = text[i]

        if in_single:
            cleaned.append(" ")
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            # Double-quoted identifiers are kept verbatim so table/column names
            # survive for the keyword and allowlist checks.
            cleaned.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_double = False
            i += 1
            continue

        # Not inside any quote: look for comments first.
        if char == "-" and i + 1 < length and text[i + 1] == "-":
            has_comment = True
            newline = text.find("\n", i)
            if newline == -1:
                break
            cleaned.append(" " * (newline - i))
            i = newline
            continue

        if char == "/" and i + 1 < length and text[i + 1] == "*":
            has_comment = True
            end = text.find("*/", i + 2)
            if end == -1:
                break
            cleaned.append(" " * (end + 2 - i))
            i = end + 2
            continue

        if char == "'":
            in_single = True
            cleaned.append(" ")
            i += 1
            continue

        if char == '"':
            in_double = True
            cleaned.append(char)
            i += 1
            continue

        if char == ";":
            semicolons.append(len(cleaned))

        cleaned.append(char)
        i += 1

    return "".join(cleaned), has_comment, semicolons


def _find_blocklisted_token(cleaned: str) -> str | None:
    """Return the first blocklisted file-function call or admin token, if any."""
    lowered = cleaned.lower()

    # File-reading functions are only dangerous as calls: `name (`.
    for match in re.finditer(r"\b([a-z_][a-z0-9_]*)\s*\(", lowered):
        name = match.group(1)
        if name in FILE_FUNCTION_BLOCKLIST:
            return name

    for match in re.finditer(r"\b([a-z_][a-z0-9_]*)\b", lowered):
        if match.group(1) in ADMIN_TOKEN_BLOCKLIST:
            return match.group(1)

    return None


def validate_statement_static(statement: str) -> str | None:
    """Text-only safety checks. Return an error message or None when accepted."""
    stripped = _strip_leading_sql_noise(statement)
    if not stripped:
        return "empty SQL statement"

    cleaned, has_comment, _ = _scan_sql(stripped)
    if has_comment:
        return "SQL comments are not allowed"

    if ";" in cleaned.rstrip().rstrip(";"):
        return "multiple SQL statements are not allowed"

    match = re.match(r"([A-Za-z]+)", stripped)
    if not match:
        return "unable to determine the SQL command"

    keyword = match.group(1).lower()
    if keyword not in ALLOWED_SQL_COMMANDS:
        return f"'{keyword.upper()}' is not allowed"

    # JOIN is allowed for reads, but DML must stay single-target so write-back
    # can resolve exactly one sheet.
    if keyword != "select" and re.search(r"\bjoin\b", cleaned, flags=re.IGNORECASE):
        return "'JOIN' is not allowed"

    blocked = _find_blocklisted_token(cleaned)
    if blocked is not None:
        return f"'{blocked.upper()}' is not allowed"

    return None


def _extract_table_position_tokens(cleaned: str) -> list[str]:
    """Collect identifiers sitting in a table position (after FROM/JOIN/etc)."""
    tokens: list[str] = []
    pattern = re.compile(
        r"\b(?:from|join|into|update|table)\s+(\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*)",
        flags=re.IGNORECASE,
    )
    for match in pattern.finditer(cleaned):
        token = match.group(1).strip()
        if token.startswith('"') and token.endswith('"'):
            token = token[1:-1]
        tokens.append(token)
    return tokens


def validate_statement_tables(prepared_statement: str, allowed_tables: set[str]) -> str | None:
    """Positive control: every table referenced must be one I registered."""
    cleaned, _, _ = _scan_sql(prepared_statement)
    allowed_lower = {name.lower() for name in allowed_tables}
    for token in _extract_table_position_tokens(cleaned):
        if token.lower() not in allowed_lower:
            return f"table '{token}' is not a registered workbook table"
    return None


def _validate_sql_allowlist(statement: str) -> str | None:
    """Backwards-compatible wrapper kept for existing callers and tests."""
    return validate_statement_static(statement)
