"""Parse model SQL JSON payloads and route statements to the SQL executor."""

import json
import re
from pathlib import Path

from rich.console import Console

from ..logger import append_log
from .executor import execute_sql_statements


def _candidate_json_strings(text: str) -> list[str]:
	"""Return possible JSON payload candidates from mixed model output text."""
	candidates: list[str] = []

	stripped = text.strip()
	if stripped:
		candidates.append(stripped)

	# Prefer fenced JSON blocks when present.
	for match in re.finditer(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.IGNORECASE | re.DOTALL):
		block = match.group(1).strip()
		if block:
			candidates.append(block)

	# Recover JSON objects embedded in explanatory text by scanning for balanced braces.
	start_indices = [index for index, char in enumerate(text) if char == "{"]
	for start in start_indices:
		depth = 0
		in_string = False
		escape = False
		for index in range(start, len(text)):
			char = text[index]
			if in_string:
				if escape:
					escape = False
				elif char == "\\":
					escape = True
				elif char == '"':
					in_string = False
				continue

			if char == '"':
				in_string = True
				continue
			if char == "{":
				depth += 1
			elif char == "}":
				depth -= 1
				if depth == 0:
					candidates.append(text[start : index + 1].strip())
					break

	# Keep order but remove duplicates.
	seen: set[str] = set()
	unique_candidates: list[str] = []
	for candidate in candidates:
		if candidate in seen:
			continue
		seen.add(candidate)
		unique_candidates.append(candidate)
	return unique_candidates


def _parse_sql_statements(candidate_text: str) -> list[str]:
	"""Parse and validate a JSON candidate, returning normalized SQL statements."""
	try:
		parsed = json.loads(candidate_text)
	except json.JSONDecodeError:
		return []

	if not isinstance(parsed, dict):
		return []

	statements = parsed.get("sql_statements")
	if not isinstance(statements, list):
		return []

	result: list[str] = []
	for item in statements:
		if isinstance(item, str) and item.strip():
			result.append(item.strip())
	return result


def extract_sql_statements(output: str) -> list[str]:
	"""Extract SQL statements from raw LLM output.

	The extractor is tolerant to surrounding explanation text and fenced blocks.
	"""
	for candidate in _candidate_json_strings(output):
		statements = _parse_sql_statements(candidate)
		if statements:
			return statements
	return []


def expose_sql_statements(
	sql_statements: list[str],
	provider: str,
	model: str,
	console: Console,
	excel_dir: str | Path | None = None,
) -> None:
	"""Execute extracted SQL statements and append execution metadata to logs."""
	payload = {
		"provider": provider,
		"model": model,
		"sql_statements": sql_statements,
	}
	console.print("Executing SQL statements inside CLI session...")
	execute_sql_statements(sql_statements, console=console, excel_dir=excel_dir)
	append_log(
		{
			"event": "sql_executed_in_cli",
			**payload,
			"count": str(len(sql_statements)),
		}
	)
