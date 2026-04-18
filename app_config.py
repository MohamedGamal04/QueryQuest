from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    base_url: str
    default_model: str
    env_key_names: tuple[str, ...]


@dataclass(frozen=True)
class CliOptions:
    setup: bool
    prompt: str | None


PROVIDERS = {
    "1": ProviderConfig(
        name="gemini",
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        default_model="gemini-2.0-flash",
        env_key_names=("GOOGLE_API_KEY", "GEMINI_API_KEY"),
    ),
    "2": ProviderConfig(
        name="groq",
        base_url="https://api.groq.com/openai/v1",
        default_model="llama-3.3-70b-versatile",
        env_key_names=("GROQ_API_KEY",),
    ),
    "3": ProviderConfig(
        name="nvidia",
        base_url="https://integrate.api.nvidia.com/v1",
        default_model="meta/llama-3.1-70b-instruct",
        env_key_names=("NVIDIA_API_KEY",),
    ),
    "4": ProviderConfig(
        name="ollama",
        base_url="http://localhost:11434/v1",
        default_model="llama3.2",
        env_key_names=("OLLAMA_API_KEY",),
    ),
}

PROVIDERS_BY_NAME = {config.name: config for config in PROVIDERS.values()}

ROOT_DIR = Path(__file__).resolve().parents[1]
STATE_FILE = Path(__file__).with_name(".provider_setup.json")
LOG_FILE = ROOT_DIR / "logs"
HISTORY_FILE = ROOT_DIR / "history.json"
TEMP_SQL_FILE = ROOT_DIR / "temporary_sql_input.json"
TEMP_SCRIPT = ROOT_DIR / "temporary_sql_runner.py"

SYSTEM_PROMPT = """### ROLE
You are QueryQuest, a precise Data Engineering Assistant. Your goal is to translate natural language requests into executable SQL statements that run on tabular data via DuckDB.

### OPERATIONS
1. **Query**: For data retrieval, analysis, or answering questions, use: SELECT [columns] FROM [table] [conditions].
2. **Delete**: For removing records, use: DELETE FROM [table] WHERE [condition].
3. **Modify/Update**: For editing existing data, use: UPDATE [table] SET [column] = [value] WHERE [condition].
4. **Insert**: For adding new records, use: INSERT INTO [table] VALUES (...).

### RULES
1. **Tool Use**: You only output SQL. Do not explain the code unless asked.
2. **Dialect**: Use standard DuckDB/PostgreSQL SQL syntax.
3. **Joins**: If a request requires data from both tables, use a JOIN on relevant keys (e.g., location or dates).
4. **Safety**: Do not perform any DROP TABLE or TRUNCATE operations.

### OUTPUT FORMAT
You MUST respond with a valid JSON object ONLY.
{
    "sql_statements": ["SQL_QUERY_1"],
    "explanation": "A brief description of what these queries will do."
}"""
