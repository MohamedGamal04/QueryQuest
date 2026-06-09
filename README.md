# QueryQuest

QueryQuest is a Python CLI assistant that converts natural language requests into SQL queries and executes them against Excel workbooks.

<img width="722" height="434" alt="qq gif" src="https://github.com/user-attachments/assets/dc38c412-3b70-4e97-a225-4e4456dc9594" />

## What It Can Do
- Read workbook context (sheet names, schema summaries, sample rows)
- Generate SQL from natural language prompts using an LLM
- Preview SQL statements in table form before execution
- Execute `SELECT`, `UPDATE`, `INSERT`, and `DELETE` via DuckDB
- Join across sheets/workbooks in `SELECT` reads (`workbook__sheet` tables)
- Confirm before writing DML changes back to Excel files
- Enforce a hard SQL sandbox (no file-read functions, no admin/DDL, no comments or multi-statements)
- Handle model responses with extra prose around JSON payloads

## Architecture
The natural-language-to-SQL logic lives in an async, prompt-free `QueryEngine`
(`core/`) driven by an approval `Policy`. The CLI supplies an interactive policy;
a future fully-agentic website can drive the same engine with `AutoApprovePolicy`.

## Supported Providers
Configured via OpenAI-compatible chat APIs:
- Gemini
- Groq
- NVIDIA NIM
- Ollama (local)

## Project Structure
```text
src/queryquest/
	app.py               # Package entrypoint
	cli.py               # CLI parsing and normalization
	config.py            # Provider and app configuration
	setup_flow.py        # Interactive provider/model/key setup
	state.py             # Persisted setup and Excel context cache
	logger.py            # JSON event logging
	chat_session.py      # Interactive CLI adapter over the engine
	excel/context.py     # Excel discovery and context building
	core/
		engine.py          # Async, prompt-free QueryEngine orchestrator
		models.py          # EngineConfig / EngineResult dataclasses
		policy.py          # Approval policies (interactive / auto / deny)
		llm.py             # Async provider access (AsyncOpenAI)
	sql/
		handoff.py         # JSON extraction from model output
		validation.py      # SQL sandbox (allowlist, blocklist, table check)
		rewrite.py         # Identifier rewriting (JOIN-aware)
		registry.py        # Excel -> DuckDB registration
		execution.py       # Statement execution + DELETE/UPDATE previews
		writeback.py       # DML write-back to workbooks
		executor.py        # Legacy interactive facade (compatibility)
		preview.py         # Rich table rendering

tests/
	test_cli.py
	test_core_engine.py
	test_policy.py
	test_sql_executor.py
	test_sql_handoff.py
	test_sql_preview.py
	test_sql_rewrite_join.py
	test_sql_validation.py
	test_state.py
```

## Requirements
- Python 3.12+
- `uv` recommended for environment/dependency management

## Setup
From the repository root:

1. Create a virtual environment (if needed):
```bash
uv venv
```

2. Install dependencies and package entrypoints:
```bash
uv sync
```

Alternative with pip (works well on Windows):
```bash
pip install -e .
```

3. Run the CLI:
```bash
qq
```

## Usage
Setup provider/model credentials:
```bash
qq --setup
```

Run interactive mode:
```bash
qq
```

Run with one prompt:
```bash
qq -p "show the top 10 highest list prices"
```

Show help:
```bash
qq -h
```

## Web (Chainlit prototype)
A Chainlit chat UI that drives the same async `QueryEngine`. Configure a
provider first with `qq --setup`, then launch:
```bash
uv run chainlit run chainlit_app.py
```
Open http://localhost:8000. How it works:
- **Attach your data** — the app works only on files you attach via the 📎 icon
  (`.xlsx`/`.xls`). They are copied into a per-session temp directory, so your
  originals are never modified. Attachments accumulate across messages.
- **Execution mode** (picker below the composer):
  - *Human-in-the-loop* — confirm before statements run, preview the rows a
    DML statement will change as a table, then confirm again before saving.
  - *Fully agentic* — runs and saves automatically; the SQL sandbox is the
    only safety boundary.
- **Results** render as interactive Dataframe elements; SELECT JOINs across
  attached sheets are supported.
- **Download** — after an UPDATE/DELETE is saved, you get a link to download
  the edited workbook.

### Deploy to Hugging Face Spaces
The repo is ready to run as a **Docker** Space (the YAML header in this README
sets `sdk: docker`, `app_port: 7860`). No server-side API key is stored — each
visitor brings their own:
1. Create a new Space → **Docker** → blank, then push this repo (or duplicate it).
2. The included [`Dockerfile`](Dockerfile) builds the app and serves it on port 7860.
3. On first message, the app asks each user to pick a provider and paste **their
   own API key** (kept only for that session). No shared key, so no cost to you.

Locally the Docker image runs the same way:
```bash
docker build -t queryquest . && docker run -p 7860:7860 queryquest
```

## Testing
Run tests from project root:
```bash
uv run python -m unittest discover -s tests -v
```

## Data Expectations
- Default workbook directory: `excel_files/`
- QueryQuest registers every sheet as a SQL table using `workbook__sheet` naming.
- Example: `inventory.xlsx` sheet `Archive Data` becomes table `inventory__Archive_Data`.
- DML (`INSERT`/`UPDATE`/`DELETE`) writes back to the exact sheet targeted by the SQL statement.

## Notes
- Logs are appended to `logs.txt`.
- Provider/model/api key setup is stored in `.provider.json`.
- The SQL extractor is robust to mixed responses (plain text + fenced JSON + embedded JSON object).
