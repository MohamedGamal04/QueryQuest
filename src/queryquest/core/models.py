"""Pure data models exchanged across the core engine boundary.

These carry no rich/duckdb objects so both the CLI adapter and a future web
backend can serialize and render them however they like.

Author: mohamedgamal04
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class EngineConfig:
    """Everything the engine needs to talk to a provider and find workbooks."""

    base_url: str
    api_key: str
    model: str
    provider_name: str
    system_prompt: str
    excel_dir: Path | None = None
    excel_files_count: int = 1


@dataclass
class StatementResult:
    """Outcome of a single SQL statement: preview rows or an error."""

    sql: str
    kind: str  # select / insert / update / delete / unknown
    prepared_sql: str | None = None
    columns: list[str] = field(default_factory=list)
    rows: list[dict] = field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    error: str | None = None


@dataclass
class WritebackTarget:
    """A single sheet a DML statement wants to persist back to disk."""

    file_path: Path
    sheet_name: str
    table_name: str
    affected_rows: int
    preview_columns: list[str] = field(default_factory=list)
    preview_rows: list[dict] = field(default_factory=list)


@dataclass
class EngineResult:
    """Full result of one engine run, ready to render or return as JSON."""

    prompt: str
    raw_llm_output: str = ""
    explanation: str = ""
    statements: list[StatementResult] = field(default_factory=list)
    executed: bool = False
    wrote_back: bool = False
    writeback_targets: list[WritebackTarget] = field(default_factory=list)
    error: str | None = None
