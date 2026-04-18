from rich.console import Console
from rich.prompt import Prompt

from app_config import CliOptions


def print_help(console: Console) -> None:
    console.print("[bold]QueryQuest CLI[/bold]")
    console.print(
        "Usage: [cyan]QQ[/cyan] [yellow][-s|--setup][/yellow] "
        "[yellow][-p|--prompt[/yellow] <text>[yellow]][/yellow] [yellow][-q|--quit][/yellow]"
    )
    console.print("\nOptions:")
    console.print("  [yellow]-h[/yellow], [yellow]--help[/yellow]      Show this help message and exit")
    console.print("  [yellow]-s[/yellow], [yellow]--setup[/yellow]     Reconfigure provider/API key/model and save settings")
    console.print("  [yellow]-p[/yellow], [yellow]--prompt[/yellow]    Prompt text to send to the configured LLM")
    console.print("  [yellow]-q[/yellow], [yellow]--quit[/yellow]      Exit the session")


def is_quit_command(text: str) -> bool:
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
    value = text.strip()
    if value in ("-p", "--prompt"):
        return "", True
    if value.startswith("-p ") or value.startswith("--prompt "):
        parts = value.split(maxsplit=1)
        if len(parts) == 2:
            return parts[1].strip(), False
    return value, False


def parse_args(argv: list[str], console: Console) -> CliOptions:
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
