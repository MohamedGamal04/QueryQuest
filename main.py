import sys

from dotenv import load_dotenv
from openai import OpenAI
from rich.console import Console
from app_config import PROVIDERS_BY_NAME
from chat_session import run_chat_session
from cli import normalize_prompt_input, parse_args
from logger import append_log
from setup_flow import run_setup
from state import load_history, load_state, reset_history


CONSOLE = Console()


def main() -> None:
    load_dotenv()
    CONSOLE.print("HI, I'm QueryQuest your personal Excel agent")
    options = parse_args(sys.argv[1:], CONSOLE)
    
    state = load_state()
    if options.setup or state is None:
        state = run_setup(CONSOLE)

    config = PROVIDERS_BY_NAME[state["provider"]]
    CONSOLE.print(f"Using provider: {config.name} (model: {state['model']})")

    # A new CLI run starts a fresh chat history by design.
    reset_history()
    append_log({"event": "history_reset_on_new_chat", "provider": config.name, "model": state["model"]})
    history = load_history()

    client = OpenAI(base_url=config.base_url, api_key=state["api_key"])

    prompt, prompt_flag_only = normalize_prompt_input(options.prompt or "")
    if prompt_flag_only:
        CONSOLE.print("Please provide prompt text after -p/--prompt.")
        prompt = ""

    if options.setup and not prompt:
        CONSOLE.print("Setup complete. Type prompts below. Use [cyan]QQ -q[/cyan] or [cyan]QQ -quit[/cyan] to exit.")

    run_chat_session(
        console=CONSOLE,
        client=client,
        provider_name=config.name,
        provider_base_url=config.base_url,
        model_name=state["model"],
        history=history,
        initial_prompt=prompt,
    )


if __name__ == "__main__":
    main()
