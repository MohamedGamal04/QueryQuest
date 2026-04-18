from getpass import getpass

from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

from app_config import PROVIDERS, ProviderConfig
from state import find_env_api_key, save_state


def select_provider(console: Console) -> ProviderConfig:
    table = Table(title="Choose Provider")
    table.add_column("Key", style="cyan", justify="right")
    table.add_column("Provider", style="green")
    table.add_row("1", "Gemini")
    table.add_row("2", "Groq")
    table.add_row("3", "Nvidia")
    table.add_row("4", "Ollama")
    console.print(table)
    choice = Prompt.ask("Enter choice", choices=["1", "2", "3", "4"], console=console)
    config = PROVIDERS.get(choice)
    if not config:
        raise RuntimeError("Invalid provider choice. Pick a number from 1 to 4.")
    return config


def resolve_api_key(config: ProviderConfig) -> str:
    env_key = find_env_api_key(config.env_key_names)
    if config.name == "ollama":
        entered = getpass("API key (optional for Ollama; press Enter to skip): ").strip()
        return entered or env_key or "ollama"

    entered = getpass("API key (hidden input): ").strip()
    if entered:
        return entered
    if env_key:
        return env_key
    env_hint = " or ".join(config.env_key_names)
    raise RuntimeError(f"Missing API key. Provide one in prompt or set {env_hint}.")


def run_setup(console: Console) -> dict[str, str]:
    config = select_provider(console)
    console.print(f"\nSelected: [bold green]{config.name}[/bold green]")
    model = Prompt.ask("Model", default=config.default_model, console=console).strip() or config.default_model
    api_key = resolve_api_key(config)
    save_state(config.name, api_key, model)
    console.print(f"Saved setup for provider '{config.name}'.")
    return {"provider": config.name, "api_key": api_key, "model": model}
