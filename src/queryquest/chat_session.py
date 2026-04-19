"""Interactive chat loop that calls the LLM and optionally executes SQL outputs."""

import json
from collections.abc import Callable

from openai import APIConnectionError, AuthenticationError, NotFoundError, OpenAI, RateLimitError
from openai.types.chat import ChatCompletionMessageParam
from rich.console import Console
from rich import box
from rich.panel import Panel
from rich.prompt import Prompt
from rich.syntax import Syntax

from .cli import is_quit_command, normalize_prompt_input
from .logger import append_log
from .sql.handoff import expose_sql_statements, extract_sql_statements


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


def run_chat_session(
	console: Console,
	client: OpenAI,
	provider_name: str,
	provider_base_url: str,
	model_name: str,
	initial_prompt: str,
	system_prompt_provider: Callable[[], str],
	excel_file_count_provider: Callable[[], int],
) -> None:
	"""Run the interactive prompt loop and optionally execute generated SQL.

	The loop also records structured logs for successful calls and error paths.
	"""
	prompt = initial_prompt

	while True:
		while not prompt:
			prompt = Prompt.ask("You", console=console).strip()

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

		try:
			system_prompt = system_prompt_provider()
			excel_file_count = excel_file_count_provider()
			request_messages: list[ChatCompletionMessageParam] = [
				{"role": "system", "content": system_prompt},
				{"role": "user", "content": prompt},
			]

			if excel_file_count == 0:
				# Return a deterministic JSON response when no data source exists.
				output = json.dumps(
					{
						"sql_statements": [],
						"explanation": "No Excel files are currently available in the selected directory. The requested file or table may have been removed.",
					}
				)
				append_log(
					{
						"event": "llm_skipped_no_files",
						"provider": provider_name,
						"model": model_name,
						"input_chars": len(prompt),
						"system_prompt_chars": len(system_prompt),
						"input_messages": request_messages,
						"excel_file_count": excel_file_count,
					}
				)
			else:
				response = client.chat.completions.create(
					model=model_name,
					messages=request_messages,
				)
				output = response.choices[0].message.content or ""

			sql_statements = extract_sql_statements(output)
			if excel_file_count == 0 and sql_statements:
				# Hard guard: never allow SQL suggestions when no Excel files are currently available.
				output = json.dumps(
					{
						"sql_statements": [],
						"explanation": "No Excel files are currently available in the selected directory. The requested file or table may have been removed.",
					}
				)
				sql_statements = []

			append_log(
				{
					"event": "llm_success",
					"provider": provider_name,
					"model": model_name,
					"input_chars": len(prompt),
					"system_prompt_chars": len(system_prompt),
					"input_messages": request_messages,
					"output_chars": len(output),
					"sql_statement_count": len(sql_statements),
					"excel_file_count": excel_file_count,
				}
			)
			_print_llm_response(console, output, provider_name, model_name)

			if not sql_statements:
				append_log(
					{
						"event": "sql_expose_skipped_no_statements",
						"provider": provider_name,
						"model": model_name,
					}
				)
			else:
				execute_choice = Prompt.ask(
					"Execute these SQL statements in the CLI now?",
					choices=["y", "n"],
					default="n",
					console=console,
				).strip()
				if execute_choice.lower() in {"y", "yes"}:
					expose_sql_statements(sql_statements, provider_name, model_name, console)
				else:
					console.print("[yellow]Execution skipped.[/yellow] Continuing chat. Type [cyan]QQ -q[/cyan] to quit.")
		except NotFoundError:
			append_log(
				{
					"event": "llm_error",
					"provider": provider_name,
					"model": model_name,
					"input": prompt,
					"error": "model_not_found",
				}
			)
			console.print(
				f"Error: Model '{model_name}' not found for provider '{provider_name}'. "
				"Run with --setup to change provider/model."
			)
		except RateLimitError:
			append_log(
				{
					"event": "llm_error",
					"provider": provider_name,
					"model": model_name,
					"input": prompt,
					"error": "rate_limit",
				}
			)
			console.print(f"Error: Rate limit or quota exceeded for '{provider_name}'.")
		except AuthenticationError:
			append_log(
				{
					"event": "llm_error",
					"provider": provider_name,
					"model": model_name,
					"input": prompt,
					"error": "authentication_failed",
				}
			)
			console.print(f"Error: Authentication failed for '{provider_name}'. Check your API key.")
		except APIConnectionError:
			append_log(
				{
					"event": "llm_error",
					"provider": provider_name,
					"model": model_name,
					"input": prompt,
					"error": "api_connection_failed",
				}
			)
			console.print(
				f"Error: Could not connect to '{provider_name}' at {provider_base_url}. "
				"Check the provider URL and whether the service is running."
			)

		prompt = ""
