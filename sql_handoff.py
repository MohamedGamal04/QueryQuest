import json

from rich.console import Console

from logger import append_log
from sql_excution import execute_sql_statements

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
    console.print("Executing SQL statements inside CLI session...")
    execute_sql_statements(sql_statements, console=console)
    append_log(
        {
            "event": "sql_executed_in_cli",
            **payload,
            "count": str(len(sql_statements)),
        }
    )
