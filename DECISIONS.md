# DECISIONS

## Approach
- Use an LLM to generate SQL from user prompts, then execute that SQL in DuckDB with `.xlsx` and `.xls` files treated as SQL-accessible tables.

## Architecture
- Adopted a CLI-first architecture focused on practical natural-language querying over local Excel files.
- Kept a modular package layout under `src/queryquest` to separate concerns across CLI parsing, setup/state, chat orchestration, Excel context, and SQL execution.
- Implemented a custom orchestration flow end-to-end (no LangChain/LlamaIndex/AutoGen/CrewAI).
- Used DuckDB over pandas DataFrames for local SQL execution performance and simple write-back behavior.

## LLM integration
- Standardized on OpenAI-compatible chat completions so providers can be swapped behind one client interface.
- Added interactive setup for provider/model/API key, then persisted configuration in `.provider.json`.
- Centralized provider defaults and system prompt policy in `config.py`.

## SQL policy and safety
- Enforced an explicit allowlist in executor runtime checks: only `SELECT`, `INSERT`, `UPDATE`, `DELETE` are accepted.
- Allow `JOIN` for `SELECT` reads only; keep it rejected in `INSERT`/`UPDATE`/`DELETE` so write-back always resolves a single target sheet.
- Reject schema-changing/admin commands and a hard blocklist of DuckDB file-access functions (`read_csv`, `read_parquet`, `glob`, `read_text`, `ATTACH`, `COPY`, `PRAGMA`, ...) so a `SELECT` cannot read arbitrary files.
- Reject multi-statement payloads and any SQL comments, using a quote-aware scanner so literal `;`/`--` inside strings no longer cause false positives.
- Positive control: after rewriting, every referenced table must be a registered `workbook__sheet` table.
- Keep the allowlist/sandbox as the hard guard; prompt instructions are advisory and not trusted alone. This matters because the autonomous web path runs with no human approval.

## Excel handling
- Build prompt context from workbook metadata and sample rows to guide SQL generation.
- Normalize filenames and column names so model SQL can be rewritten to real DuckDB identifiers.
- Register compatibility views for original workbook names to tolerate spaced/raw names in model output.
- Use the selected runtime Excel directory for both prompt context and SQL execution to avoid path drift.
- Keep write-back support for DML with explicit confirmation before persisting changes to workbook files.

## Core engine and policy
- Extracted an async, prompt-free `QueryEngine` (`core/engine.py`) that turns a prompt + a `Policy` into an `EngineResult`. It contains no `rich`/`input` and is the shared backend for the CLI and a planned fully-agentic website.
- Approval is a `Policy` (`core/policy.py`): `InteractivePolicy` asks the human (CLI); `AutoApprovePolicy` runs unattended (web) but still confines writes to the excel directory; `DenyAllPolicy` is dry-run.
- LLM access is async (`AsyncOpenAI` in `core/llm.py`); blocking pandas/DuckDB/IO run on worker threads via `asyncio.to_thread`. One DuckDB connection per thread, never shared concurrently.

## CLI UX decisions
- Render SQL statements in a preview table before execution.
- For `DELETE` and `UPDATE`, show an affected-row precheck and a preview of rows that will be changed.
- The CLI is now a thin adapter over the engine: it supplies `InteractivePolicy` and renders `EngineResult`.

## Tradeoffs
- Every sheet of each workbook is registered as its own `workbook__sheet` table; DML write-back targets the exact sheet named in the statement.
- SQL extraction is robust against fenced/embedded JSON, but fully malformed model outputs are skipped safely.
- `sql/executor.py` is split into `validation`/`rewrite`/`registry`/`execution`/`writeback`; `executor.py` remains as a compatibility facade for the legacy interactive path and tests.
- Write-back re-runs approved DML against a fresh connection rather than sharing a connection across threads — slightly redundant, but keeps each DuckDB connection thread-local and correct.

## Future improvements
- Add integration tests for Excel round-trips across multi-sheet workbooks.
- Expand support for additional SQL commands where safe and appropriate.
- Add richer schema-aware diagnostics for "table/column not found" cases.
- Build the web backend on `QueryEngine` + `AutoApprovePolicy`; add a connection pool for concurrent requests.
- Add structured user-facing error payloads for execution and validation failures.
