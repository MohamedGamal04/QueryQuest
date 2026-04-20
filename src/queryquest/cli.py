"""CLI argument parsing and interactive command normalization helpers."""

from rich.console import Console
from rich.prompt import Prompt
from rich.panel import Panel
from rich.table import Table
from rich import box

from .config import CliOptions


def print_help(console: Console) -> None:
	"""Render CLI usage and available command-line options."""
	help_table = Table(box=box.ROUNDED, show_header=False, show_edge=True, border_style="bright_magenta", pad_edge=False)
	help_table.add_column("Option", style="bold cyan", no_wrap=True)
	help_table.add_column("Description", style="white")
	help_table.add_row("-h, --help", "Show this help message and exit")
	help_table.add_row("-s, --setup", "Reconfigure provider/API key/model and save settings")
	help_table.add_row("-p, --prompt", "Prompt text to send to the configured LLM")
	help_table.add_row("-q, --quit", "Exit the session")

	console.print(
		Panel(
			help_table,
			title="[bold magenta]QueryQuest CLI[/bold magenta]",
			border_style="bright_blue",
			box=box.DOUBLE,
			padding=(1, 2),
		)
	)
	console.print(
		Panel(
			"[bold]Usage[/bold]\n[cyan]QQ[/cyan] [yellow]-s|--setup[/yellow] [yellow]-p|--prompt[/yellow] <text> [yellow]-q|--quit[/yellow]",
			border_style="bright_green",
			box=box.ROUNDED,
			padding=(0, 1),
		)
	)


def print_banner(console: Console) -> None:
	"""Render a short welcome banner for the interactive CLI."""
	console.print(
		Panel(
			"[bold bright_magenta]QueryQuest[/bold bright_magenta]\n"
			"[cyan]Your personal Excel agent[/cyan]",
			border_style="bright_magenta",
			box=box.ROUNDED,
			padding=(1, 2),
		)
	)


def is_quit_command(text: str) -> bool:
	"""Return True when text matches one of the accepted quit commands."""
	value = text.strip().lower()
	return value in {
		"-q",
		"--quit",
		"qq -q",
		"qq --quit",
		"queryquest -q",
		"queryquest --quit",
	}


def normalize_prompt_input(text: str) -> tuple[str, bool]:
	"""Normalize inline prompt syntax and detect empty `-p/--prompt` usage."""
	value = text.strip()
	if value in ("-p", "--prompt"):
		return "", True
	if value.startswith("-p ") or value.startswith("--prompt "):
		parts = value.split(maxsplit=1)
		if len(parts) == 2:
			return parts[1].strip(), False
	return value, False


def parse_args(argv: list[str], console: Console) -> CliOptions:
	"""Parse command-line args, with interactive fallback for unknown/missing input."""
	if not argv:
		user_input = Prompt.ask("How can I help you today? (Tip: run [cyan]QQ -h[/cyan] for help.)", console=console).strip()

		# Allow command-style entries in interactive mode, e.g. "qq -h" or "QQ -s".
		parts = user_input.split(maxsplit=1)
		has_prefix = bool(parts and parts[0].lower() in ("qq", "queryquest"))
		if has_prefix:
			user_input = parts[1].strip() if len(parts) == 2 else ""

		if not has_prefix and user_input.startswith("-"):
			console.print("Please prefix options with [cyan]qq[/cyan] or [cyan]QQ[/cyan] (example: [cyan]qq -h[/cyan]).")
			return parse_args([], console)

		if user_input in ("-h", "--help"):
			print_help(console)
			raise SystemExit(0)
		if is_quit_command(user_input):
			raise SystemExit(0)
		if user_input in ("-s", "--setup"):
			return CliOptions(setup=True, prompt=None)
		if user_input in ("-p", "--prompt"):
			return CliOptions(setup=False, prompt=Prompt.ask("Prompt", console=console).strip())
		if user_input.startswith("-p ") or user_input.startswith("--prompt "):
			parts = user_input.split(maxsplit=1)
			if len(parts) == 2:
				return CliOptions(setup=False, prompt=parts[1].strip())

		return CliOptions(setup=False, prompt=user_input)

	setup = False
	prompt: str | None = None

	i = 0
	while i < len(argv):
		arg = argv[i]
		if arg in ("-h", "--help"):
			print_help(console)
			raise SystemExit(0)
		if arg in ("-q", "--quit"):
			raise SystemExit(0)
		if arg in ("-s", "--setup"):
			setup = True
			i += 1
			continue
		if arg in ("-p", "--prompt"):
			i += 1
			if i >= len(argv):
				console.print("Prompt value was missing. Switching to interactive mode selection.")
				return parse_args([], console)
			prompt = argv[i]
			i += 1
			continue
		console.print(f"Unknown argument '{arg}'. Switching to interactive mode selection.")
		return parse_args([], console)

	return CliOptions(setup=setup, prompt=prompt)


def main() -> None:
	"""Console entrypoint that delegates to the application bootstrap."""
	from .app import main as app_main

	app_main()
