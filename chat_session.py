from openai import APIConnectionError, AuthenticationError, NotFoundError, OpenAI, RateLimitError
from rich.console import Console
from rich.prompt import Prompt

from app_config import SYSTEM_PROMPT
from cli import is_quit_command, normalize_prompt_input
from logger import append_log
from sql_handoff import expose_sql_statements, extract_sql_statements
from state import save_history


def run_chat_session(
    console: Console,
    client: OpenAI,
    provider_name: str,
    provider_base_url: str,
    model_name: str,
    history: list[dict[str, str]],
    initial_prompt: str,
) -> None:
    prompt = initial_prompt

    while True:
        while not prompt:
            prompt = Prompt.ask("User", console=console).strip()

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

        try:
            response = client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    *history, # type: ignore
                    {"role": "user", "content": prompt},
                ],
            )
            output = response.choices[0].message.content or ""
            history.extend(
                [
                    {"role": "user", "content": prompt},
                    {"role": "assistant", "content": output},
                ]
            )
            save_history(history)
            append_log(
                {
                    "event": "llm_success",
                    "provider": provider_name,
                    "model": model_name,
                    "input": prompt,
                    "output": output,
                }
            )
            console.print("\n[bold]Response:[/bold]\n")
            console.print(output)

            execute_choice = Prompt.ask(
                "do you want to excute commands :",
                choices=["y", "n"],
                default="n",
                console=console,
            ).strip()
            if execute_choice.lower() == "y" or execute_choice.lower() == "yes":
                sql_statements = extract_sql_statements(output)
                if not sql_statements:
                    console.print("No valid sql_statements found in model response JSON.")
                    append_log(
                        {
                            "event": "sql_expose_skipped_no_statements",
                            "provider": provider_name,
                            "model": model_name,
                        }
                    )
                else:
                    expose_sql_statements(sql_statements, provider_name, model_name, console)
            else:
                console.print("Continuing chat. Type [cyan]QQ -q[/cyan] to quit.")
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

        prompt = Prompt.ask("User", console=console).strip()
