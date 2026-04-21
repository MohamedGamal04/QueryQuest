# QueryQuest

QueryQuest is a Python CLI assistant that converts natural language requests into SQL queries and executes them against Excel workbooks.

<img width="722" height="434" alt="qq gif" src="https://github.com/user-attachments/assets/dc38c412-3b70-4e97-a225-4e4456dc9594" />

## What It Can Do
- Read workbook context (sheet names, schema summaries, sample rows)
- Generate SQL from natural language prompts using an LLM
- Preview SQL statements in table form before execution
- Execute `SELECT`, `UPDATE`, `INSERT`, and `DELETE` via DuckDB
- Confirm before writing DML changes back to Excel files
- Handle model responses with extra prose around JSON payloads

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
	chat_session.py      # Prompt loop and LLM orchestration
	excel/context.py     # Excel discovery and context building
	sql/
		handoff.py         # JSON extraction + SQL handoff
		executor.py        # SQL execution and Excel write-back
		preview.py         # Rich table rendering

tests/
	test_cli.py
	test_sql_handoff.py
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

## Testing
Run tests from project root:
```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
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
