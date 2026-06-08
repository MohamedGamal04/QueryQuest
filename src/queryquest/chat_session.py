"""Interactive CLI adapter over the async QueryEngine.

This module is deliberately thin: it collects human input, supplies an
interactive approval policy, drives the engine, and renders the result. All SQL
logic lives in the engine and the sql/* modules.

Author: mohamedgamal04
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich import box
from rich.panel import Panel
from rich.prompt import Prompt
from rich.syntax import Syntax

from .cli import is_quit_command, normalize_prompt_input
from .core.engine import QueryEngine
from .core.models import EngineConfig, EngineResult, StatementResult, WritebackTarget
from .core.policy import Policy
from .logger import append_log
from .sql.preview import print_dataframe_as_table, print_sql_statements_table


def _print_user_prompt(console: Console, prompt: str) -> None:
    """Render the user's prompt in a colorful boxed panel."""
    console.print(
        Panel(
            prompt,
            title="[bold bright_cyan]You[/bold bright_cyan]",
            border_style="bright_cyan",
            box=box.ROUNDED,
            padding=(0, 1),
        )
    )


def _print_llm_response(console: Console, output: str, provider_name: str, model_name: str) -> None:
    """Render the model response in a colorful boxed panel."""
    try:
        parsed_output = json.loads(output)
        renderable = Syntax(
            json.dumps(parsed_output, indent=2, ensure_ascii=False),
            "json",
            theme="monokai",
            word_wrap=True,
        )
    except Exception:
        renderable = output

    console.print(
        Panel(
            renderable,
            title=f"[bold bright_magenta]{provider_name}[/bold bright_magenta] [dim]({model_name})[/dim]",
            border_style="bright_magenta",
            box=box.DOUBLE,
            padding=(0, 1),
        )
    )


async def _ask(console: Console, *args, **kwargs) -> str:
    """Run a blocking rich prompt off the event loop."""
    return await asyncio.to_thread(Prompt.ask, *args, console=console, **kwargs)


class InteractivePolicy(Policy):
    """Approval policy that asks the human at the terminal."""

    def __init__(self, console: Console) -> None:
        self._console = console

    async def approve_execution(self, statements: list[StatementResult]) -> bool:
        print_sql_statements_table([statement.sql for statement in statements], self._console)
        choice = await _ask(
            self._console,
            "Execute these SQL statements in the CLI now?",
            choices=["y", "n"],
            default="n",
        )
        return choice.strip().lower() in {"y", "yes"}

    async def approve_writeback(self, target: WritebackTarget) -> bool:
        choice = await _ask(
            self._console,
            f"Save changes to {target.file_path.name} (sheet '{target.sheet_name}', "
            f"{target.affected_rows} row(s))?",
            choices=["y", "n"],
            default="n",
        )
        return choice.strip().lower() in {"y", "yes"}


def _render_result(console: Console, result: EngineResult, provider_name: str, model_name: str) -> None:
    """Render an EngineResult: raw output, per-statement previews, and errors."""
    if result.raw_llm_output:
        _print_llm_response(console, result.raw_llm_output, provider_name, model_name)

    if result.error is not None:
        console.print(f"[red]Error:[/red] {result.error}")
        return

    for statement in result.statements:
        if statement.error is not None:
            console.print(f"[red]Refused/failed:[/red] {statement.error}")
            console.print(f"Skipped statement: {statement.sql}")
            continue

        if statement.kind == "select":
            dataframe = pd.DataFrame(statement.rows, columns=statement.columns or None)
            print_dataframe_as_table(dataframe, console)
            if statement.truncated:
                console.print(f"Showing first {len(statement.rows)} of {statement.row_count} rows.")
        else:
            console.print(f"[green]{statement.kind.upper()}[/green] affected {statement.row_count} row(s).")

    if result.wrote_back:
        console.print("[green]Changes saved.[/green]")
    elif result.writeback_targets:
        console.print("[yellow]Changes not saved.[/yellow]")


def run_chat_session(
    console: Console,
    provider_name: str,
    provider_base_url: str,
    model_name: str,
    api_key: str,
    initial_prompt: str,
    system_prompt_provider: Callable[[], str],
    excel_file_count_provider: Callable[[], int],
    excel_dir: str | Path,
) -> None:
    """Run the interactive prompt loop driven by the async engine."""
    asyncio.run(
        _run_loop(
            console=console,
            provider_name=provider_name,
            provider_base_url=provider_base_url,
            model_name=model_name,
            api_key=api_key,
            initial_prompt=initial_prompt,
            system_prompt_provider=system_prompt_provider,
            excel_file_count_provider=excel_file_count_provider,
            excel_dir=excel_dir,
        )
    )


async def _run_loop(
    console: Console,
    provider_name: str,
    provider_base_url: str,
    model_name: str,
    api_key: str,
    initial_prompt: str,
    system_prompt_provider: Callable[[], str],
    excel_file_count_provider: Callable[[], int],
    excel_dir: str | Path,
) -> None:
    """Async interactive loop: read prompt, run engine, render, repeat."""
    policy = InteractivePolicy(console)
    prompt = initial_prompt

    while True:
        while not prompt:
            prompt = (await _ask(console, "You")).strip()

        if prompt.startswith("-"):
            console.print("Please prefix options with [cyan]qq[/cyan] or [cyan]QQ[/cyan] (example: [cyan]qq -q[/cyan]).")
            prompt = ""
            continue

        prompt, prompt_flag_only = normalize_prompt_input(prompt)
        if prompt_flag_only:
            console.print("Please provide prompt text after -p/--prompt.")
            prompt = ""
            continue

        if is_quit_command(prompt):
            append_log({"event": "quit", "provider": provider_name, "model": model_name})
            console.print("Goodbye.")
            return

        _print_user_prompt(console, prompt)

        excel_file_count = excel_file_count_provider()
        if excel_file_count == 0:
            # No data source: never call the model or suggest SQL.
            console.print(
                "[yellow]No Excel files available[/yellow] in the selected directory. "
                "Add a workbook and try again."
            )
            append_log(
                {
                    "event": "llm_skipped_no_files",
                    "provider": provider_name,
                    "model": model_name,
                    "input_chars": len(prompt),
                }
            )
            prompt = ""
            continue

        system_prompt = system_prompt_provider()
        config = EngineConfig(
            base_url=provider_base_url,
            api_key=api_key,
            model=model_name,
            provider_name=provider_name,
            system_prompt=system_prompt,
            excel_dir=Path(excel_dir),
            excel_files_count=excel_file_count,
        )
        engine = QueryEngine(config)
        result = await engine.run(prompt, policy)

        append_log(
            {
                "event": "llm_success" if result.error is None else "llm_error",
                "provider": provider_name,
                "model": model_name,
                "input_chars": len(prompt),
                "system_prompt_chars": len(system_prompt),
                "output_chars": len(result.raw_llm_output),
                "sql_statement_count": len(result.statements),
                "excel_file_count": excel_file_count,
                "executed": result.executed,
                "wrote_back": result.wrote_back,
                "error": result.error,
            }
        )

        _render_result(console, result, provider_name, model_name)
        prompt = ""
