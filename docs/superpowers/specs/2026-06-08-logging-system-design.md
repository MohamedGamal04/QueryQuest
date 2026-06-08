# QueryQuest Logging System — Design

**Date:** 2026-06-08
**Status:** Approved design, ready for implementation plan
**Author:** mohamedgamal04

## Context

QueryQuest currently has one ad-hoc logging helper, `logger.append_log()`, which
writes pretty-printed (multi-line, `indent=2`) JSON blobs to `logs.txt`. It is
called from only three places in the CLI adapter (`chat_session.py`) plus one
legacy call in `sql/handoff.py`. Two problems:

1. **The core engine logs nothing.** Every SQL generation, validation refusal,
   execution, and write-back happens silently. For the planned fully-autonomous
   web backend (no human in the loop), that is unacceptable — there is no record
   of what the agent did to the user's files.
2. **The existing format is not machine-readable.** Multi-line indented JSON
   objects appended to a `.txt` file cannot be parsed line-by-line.

We want a single, unified logging system that serves both diagnostics (what is
the app doing / why did it fail) and audit (an accountable, replayable record of
every engine action), usable by both the CLI today and the web backend later.

## Decisions (locked during brainstorming)

- **Purpose:** both diagnostic logging *and* a structured audit trail, unified.
- **Two independent streams**, not one combined file.
- **Stdlib `logging`** with named loggers; the engine just calls
  `logger.*`/`audit(...)`, and all handlers/files/levels are configured once in
  a setup module. `core/` stays free of file paths and I/O wiring.
- **Console default level: `WARNING`** so the CLI stays quiet; overridable.

## Architecture

### Two named loggers

| Logger | Purpose | Handlers | Propagate |
|---|---|---|---|
| `queryquest` | Human diagnostics | Console (`WARNING`, env-tunable) + `RotatingFileHandler` → `logs/queryquest.log` (`DEBUG`, plain format) | default |
| `queryquest.audit` | Structured action record | One handler → `logs/audit.jsonl`, JSON-line formatter | **`False`** (never reaches the diagnostic console) |

`propagate=False` on the audit logger keeps audit records out of the diagnostic
console/file; the two streams stay cleanly separated.

### Components

**`logging_setup.py` (new, top-level package module)**
- `configure_logging(diagnostic_level: str | int | None = None, log_dir: Path | None = None) -> None`
  - Idempotent: tags handlers it installs (e.g. a marker attribute) and returns
    early if already configured, so calling it twice never duplicates handlers.
  - Console level resolution order: explicit arg → env `QQ_LOG_LEVEL` →
    default `WARNING`.
  - File handlers always at `DEBUG`.
  - Creates `log_dir` (default `config.LOG_DIR`) if missing.
  - Never raises — wrapped so a logging-setup failure cannot stop the app.
- `JsonLineFormatter(logging.Formatter)` — emits exactly one JSON object per
  line: `{"timestamp", "level", "event", ...fields}`. `timestamp` is UTC ISO.
  `event` and arbitrary structured fields are read from the record (see helper).

**`logger.py` (rewritten, keeps module path)**
- `audit(event: str, **fields) -> None` — logs on `queryquest.audit` at `INFO`,
  attaching `event` and `fields` to the record (via `extra`) for the JSON
  formatter to serialize.
- `get_logger(name: str) -> logging.Logger` — thin `logging.getLogger` wrapper
  returning a child of `queryquest` for module diagnostics.
- `append_log(entry)` — **compatibility shim**: routes the old dict-based call
  to `audit(entry.get("event", "legacy"), **entry)`, so `sql/handoff.py` keeps
  working untouched and its records now land in `audit.jsonl`.

**`config.py` (extended)**
- `LOG_DIR = ROOT_DIR / "logs"`, `DIAGNOSTIC_LOG_FILE = LOG_DIR / "queryquest.log"`,
  `AUDIT_LOG_FILE = LOG_DIR / "audit.jsonl"`. The old `LOG_FILE` (`logs.txt`) is
  retired. `logs/` is gitignored.

**`core/engine.py` (instrumented)**
- Each `run()` generates a `run_id` (uuid4 hex) added to `EngineResult` so all
  records of one request correlate.
- Diagnostic logs (`get_logger(__name__)`) at key steps: generation, validation,
  execution, write-back — `debug`/`info`, `warning` on refusals/errors.
- Audit events (each carries `run_id`):
  - `run_started` — prompt, provider, model
  - `llm_generated` — statement_count, has_error
  - `statement_rejected` — sql, error (static or table-allowlist failure)
  - `execution_denied` — policy refused execution
  - `statement_executed` — kind, row_count, table (best-effort)
  - `writeback_denied` — file, sheet, rows (policy/sandbox refused)
  - `writeback_saved` — file, sheet, rows
  - `run_error` — error message
- Logging stays synchronous (stdlib handlers are fast and thread-safe); no
  `asyncio.to_thread` needed.

**`app.py` (CLI startup)**
- Calls `configure_logging()` once at the top of `main()`. Console verbosity
  honors `QQ_LOG_LEVEL`.

**`chainlit_app.py` (web startup)** *(touch only if trivial)*
- Calls `configure_logging()` in `on_chat_start` so the autonomous path also
  emits diagnostics + audit. (The web backend is exactly why audit matters.)

### Data flow

```
engine.run()
  ├─ get_logger(__name__).info/debug/warning(...)  ─► queryquest        ─► console (WARNING+) , logs/queryquest.log (DEBUG+)
  └─ audit("statement_executed", run_id=..., ...)   ─► queryquest.audit  ─► logs/audit.jsonl (one JSON object per line)
```

## Error handling

- `configure_logging` is guarded; a failure to create handlers/dirs logs a
  warning at most and never propagates.
- Stdlib handlers swallow their own write errors, preserving the current
  "logging never interrupts user flow" property.
- `audit()` and `get_logger()` callers never need try/except.

## Testing

- **`tests/test_logging_setup.py`**
  - `configure_logging()` installs the expected handlers on both loggers.
  - Idempotent: calling twice does not duplicate handlers.
  - `JsonLineFormatter` output is a single line and parses as JSON containing
    `timestamp`, `level`, `event`, and supplied fields.
  - Console level respects the `diagnostic_level` argument / `QQ_LOG_LEVEL`.
- **`tests/test_audit.py`**
  - Run the engine over a temp workbook with `AutoApprovePolicy` inside
    `assertLogs("queryquest.audit", level="INFO")`; assert the captured records
    include `run_started` and `statement_executed`, and that every record shares
    the same `run_id`.
- Existing suite stays green; `append_log` shim keeps `test_sql_handoff.py`
  passing.

## Out of scope

- Log shipping / external sinks (Datadog, ELK).
- Per-request log files or rotation tuning beyond a sane default size/count.
- Redaction of API keys (engine never logs the key; only provider name/model).
- Structured logging libraries (structlog) — stdlib is sufficient for now.
