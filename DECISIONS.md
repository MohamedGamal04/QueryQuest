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
- Explicitly reject `JOIN` and schema-changing/admin commands by policy.
- Reject multi-statement SQL payloads in a single statement string.
- Keep the allowlist as the hard guard; prompt instructions are advisory and not trusted alone.

## Excel handling
- Build prompt context from workbook metadata and sample rows to guide SQL generation.
- Normalize filenames and column names so model SQL can be rewritten to real DuckDB identifiers.
- Register compatibility views for original workbook names to tolerate spaced/raw names in model output.
- Use the selected runtime Excel directory for both prompt context and SQL execution to avoid path drift.
- Keep write-back support for DML with explicit confirmation before persisting changes to workbook files.

## CLI UX decisions
- Render SQL statements in a preview table before execution.
- For `DELETE` and `UPDATE`, show an affected-row precheck and a preview of rows that will be changed.

## Tradeoffs
- Only the first sheet of each workbook is used as the active SQL table and write-back target.
- SQL extraction is robust against fenced/embedded JSON, but fully malformed model outputs are skipped safely.
- The executor remains intentionally centralized for delivery speed, trading off smaller component boundaries.
- Prioritized interactive CLI behavior over API/server deployment.

## Future improvements
- Split `sql/executor.py` into smaller units (registration, validation/rewrite, execution, write-back).
- Add integration tests for Excel round-trips across multi-sheet workbooks.
- Expand support for additional SQL commands where safe and appropriate.
- Add richer schema-aware diagnostics for "table/column not found" cases.
- Add structured user-facing error payloads for execution and validation failures.
