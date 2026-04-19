import sys
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI
from rich.console import Console
from rich.prompt import Prompt
from app_config import EXCEL_INFO_FORMAT_VERSION, PROVIDERS_BY_NAME, SYSTEM_PROMPT
from chat_session import run_chat_session
from cli import normalize_prompt_input, parse_args
from logger import append_log
from read_excel_files import (
    build_excel_files_info,
    describe_excel_snapshot_changes,
    format_excel_context,
    list_excel_files,
    get_excel_snapshot,
    normalize_excel_dir,
)
from setup_flow import run_setup
from state import load_state, save_state


CONSOLE = Console()


def main() -> None:
    load_dotenv()
    CONSOLE.print("HI, I'm QueryQuest your personal Excel agent")
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
    CONSOLE.print(f"Using provider: {config.name} (model: {state['model']})")

    excel_info_cache = {
        "signature": state.get("excel_signature") if state else None,
        "info": state.get("excel_info") if state else None,
        "format_version": state.get("excel_info_format_version") if state else None,
        "snapshot": None,
        "change_note": "",
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

    def get_system_prompt() -> str:
        current_signature, current_snapshot = get_excel_snapshot(excel_dir)
        if current_signature != excel_info_cache["signature"]:
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
            prompt_parts.append(format_excel_context(excel_info_cache["info"]))
        return "\n\n".join(part for part in prompt_parts if part)

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
        initial_prompt=prompt,
        system_prompt_provider=get_system_prompt,
        excel_file_count_provider=lambda: len(list_excel_files(excel_dir)),
    )


if __name__ == "__main__":
    main()
