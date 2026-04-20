"""Static configuration and shared dataclasses for QueryQuest."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProviderConfig:
	"""Provider connection settings used to initialize an OpenAI-compatible client."""

	name: str
	base_url: str
	default_model: str
	env_key_names: tuple[str, ...]


@dataclass(frozen=True)
class CliOptions:
	"""Normalized CLI options parsed from command-line or interactive input."""

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
		default_model="meta/llama-3.3-70b-instruct",
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

ROOT_DIR = Path(__file__).resolve().parents[2]
STATE_FILE = ROOT_DIR / ".provider.json"
EXCEL_DIR = ROOT_DIR / "excel_files"
LOG_FILE = ROOT_DIR / "logs.txt"
EXCEL_INFO_FORMAT_VERSION = "2"

SYSTEM_PROMPT = """Role: QueryQuest, a Data Engineering Assistant.
Operations:
Read: SELECT [columns] FROM [table] WHERE [condition]
Insert rows: INSERT INTO [table] VALUES (...)
Update rows: UPDATE [table] SET [column] = [value] WHERE [condition]
Delete rows: DELETE FROM [table] WHERE [condition]
Constraints:
Strict Limits: Only use row-level SELECT, INSERT, UPDATE, DELETE.
Never use JOIN, CREATE, ALTER, DROP, TRUNCATE, PRAGMA, ATTACH, DETACH, MERGE, or any schema-changing SQL.
Scope: Use ONLY available local data.
Format: Output a JSON object only: {"sql_statements": ["..."], "explanation": "..."}. No prose outside the JSON."""