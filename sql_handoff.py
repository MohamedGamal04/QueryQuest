import json
import subprocess
import sys

from rich.console import Console

from app_config import TEMP_SCRIPT, TEMP_SQL_FILE
from logger import append_log


def extract_sql_statements(output: str) -> list[str]:
    text = output.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            text = "\n".join(lines[1:-1]).strip()
            if text.lower().startswith("json\n"):
                text = text[5:].strip()

    try:
        parsed = json.loads(text)
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


def expose_sql_statements(sql_statements: list[str], provider: str, model: str, console: Console) -> None:
    payload = {
        "provider": provider,
        "model": model,
        "sql_statements": sql_statements,
    }
    try:
        TEMP_SQL_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError:
        console.print("Could not write temporary SQL handoff file.")
        return

    if TEMP_SCRIPT.exists() and TEMP_SCRIPT.is_file():
        try:
            subprocess.run(
                [sys.executable, str(TEMP_SCRIPT), str(TEMP_SQL_FILE)],
                check=False,
            )
            console.print(f"SQL exposed to temporary script: {TEMP_SCRIPT}")
            append_log(
                {
                    "event": "sql_exposed_to_script",
                    "provider": provider,
                    "model": model,
                    "count": str(len(sql_statements)),
                }
            )
            return
        except OSError:
            console.print("Could not execute temporary SQL script. Handoff file still created.")

    console.print(f"SQL exposed to handoff file: {TEMP_SQL_FILE}")
    append_log(
        {
            "event": "sql_exposed_to_file",
            "provider": provider,
            "model": model,
            "count": str(len(sql_statements)),
        }
    )
