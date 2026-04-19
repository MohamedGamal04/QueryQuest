"""Application entrypoint for QueryQuest."""

import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI
from rich.console import Console
from rich.prompt import Prompt
from rich.panel import Panel
from rich import box

from .chat_session import run_chat_session
from .cli import normalize_prompt_input, parse_args, print_banner
from .config import EXCEL_INFO_FORMAT_VERSION, PROVIDERS_BY_NAME, SYSTEM_PROMPT
from .excel.context import (
	build_excel_files_info,
	describe_excel_snapshot_changes,
	format_excel_context,
	get_excel_snapshot,
	list_excel_files,
	normalize_excel_dir,
)
from .setup_flow import run_setup
from .state import load_state, save_state


CONSOLE = Console()
SNAPSHOT_CHECK_INTERVAL_SECONDS = 2.0
MAX_EXCEL_CONTEXT_CHARS = 5000


def main() -> None:
	"""Initialize configuration and run the interactive chat session."""
	load_dotenv()
	print_banner(CONSOLE)
	options = parse_args(sys.argv[1:], CONSOLE)

	state = load_state()
	if options.setup or state is None:
		state = run_setup(CONSOLE)

	excel_default = state.get("excel_dir") if state else None
	excel_dir_input = Prompt.ask(
		"Excel files location",
		default=excel_default or str(normalize_excel_dir()),
		console=CONSOLE,
	).strip()
	excel_dir = normalize_excel_dir(Path(excel_dir_input))

	config = PROVIDERS_BY_NAME[state["provider"]]
	CONSOLE.print(
		Panel(
			f"[bold]Provider[/bold]: [cyan]{config.name}[/cyan]\n"
			f"[bold]Model[/bold]: [bright_white]{state['model']}[/bright_white]",
			border_style="bright_cyan",
			box=box.ROUNDED,
			padding=(0, 1),
		)
	)

	excel_info_cache = {
		"signature": state.get("excel_signature") if state else None,
		"info": state.get("excel_info") if state else None,
		"format_version": state.get("excel_info_format_version") if state else None,
		"snapshot": None,
		"change_note": "",
		"last_snapshot_check_at": 0.0,
	}

	is_info_stale = excel_info_cache["format_version"] != EXCEL_INFO_FORMAT_VERSION
	if not excel_info_cache["signature"] or not excel_info_cache["info"] or is_info_stale:
		excel_info_cache["info"], excel_info_cache["signature"], excel_info_cache["snapshot"] = build_excel_files_info(excel_dir)
		excel_info_cache["format_version"] = EXCEL_INFO_FORMAT_VERSION
		save_state(
			state["provider"],
			state["api_key"],
			state["model"],
			excel_dir=str(excel_dir),
			excel_signature=excel_info_cache["signature"],
			excel_info=excel_info_cache["info"],
			excel_info_format_version=EXCEL_INFO_FORMAT_VERSION,
		)
	else:
		_, excel_info_cache["snapshot"] = get_excel_snapshot(excel_dir)
		excel_info_cache["last_snapshot_check_at"] = time.monotonic()

	def get_system_prompt() -> str:
		"""Return a system prompt enriched with current Excel metadata.

		The prompt cache is refreshed when the file snapshot signature changes.
		"""
		now = time.monotonic()
		should_refresh_snapshot = (now - excel_info_cache["last_snapshot_check_at"]) >= SNAPSHOT_CHECK_INTERVAL_SECONDS
		if should_refresh_snapshot or excel_info_cache["snapshot"] is None:
			current_signature, current_snapshot = get_excel_snapshot(excel_dir)
			excel_info_cache["last_snapshot_check_at"] = now
			if current_signature != excel_info_cache["signature"]:
				# Rebuild context when workbook set/content changed on disk.
				previous_snapshot = excel_info_cache["snapshot"]
				info, signature, snapshot = build_excel_files_info(excel_dir)
				excel_info_cache["signature"] = signature
				excel_info_cache["info"] = info
				excel_info_cache["format_version"] = EXCEL_INFO_FORMAT_VERSION
				excel_info_cache["snapshot"] = snapshot
				excel_info_cache["change_note"] = describe_excel_snapshot_changes(previous_snapshot, snapshot)
				save_state(
					state["provider"],
					state["api_key"],
					state["model"],
					excel_dir=str(excel_dir),
					excel_signature=signature,
					excel_info=info,
					excel_info_format_version=EXCEL_INFO_FORMAT_VERSION,
				)
			elif excel_info_cache["snapshot"] is None:
				excel_info_cache["snapshot"] = current_snapshot

		prompt_parts = [SYSTEM_PROMPT]
		if excel_info_cache["change_note"]:
			prompt_parts.append(excel_info_cache["change_note"])
		if excel_info_cache["info"]:
			excel_context = format_excel_context(excel_info_cache["info"])
			if len(excel_context) > MAX_EXCEL_CONTEXT_CHARS:
				excel_context = f"{excel_context[:MAX_EXCEL_CONTEXT_CHARS]}\n... [excel context truncated for speed]"
			prompt_parts.append(excel_context)
		return "\n\n".join(part for part in prompt_parts if part)

	client = OpenAI(base_url=config.base_url, api_key=state["api_key"])

	prompt, prompt_flag_only = normalize_prompt_input(options.prompt or "")
	if prompt_flag_only:
		CONSOLE.print("Please provide prompt text after -p/--prompt.")
		prompt = ""

	if options.setup and not prompt:
		CONSOLE.print(
			Panel(
				"[green]Setup complete.[/green] Type prompts below.\n"
				"Use [cyan]QQ -q[/cyan] or [cyan]QQ -quit[/cyan] to exit.",
				border_style="bright_green",
				box=box.ROUNDED,
				padding=(0, 1),
			)
		)

	run_chat_session(
		console=CONSOLE,
		client=client,
		provider_name=config.name,
		provider_base_url=config.base_url,
		model_name=state["model"],
		initial_prompt=prompt,
		system_prompt_provider=get_system_prompt,
		excel_file_count_provider=lambda: len(list_excel_files(excel_dir)),
	)


if __name__ == "__main__":
	main()
