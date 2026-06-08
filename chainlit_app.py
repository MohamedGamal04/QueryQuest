"""Human-in-the-loop / autonomous Chainlit prototype for QueryQuest.

Run with:  uv run chainlit run chainlit_app.py

Drives the same async QueryEngine the CLI uses, using the provider/model from
`.provider.json`. An Execution mode picker switches between human-in-the-loop
(confirm before running and before saving, with a Dataframe preview of the rows
about to change) and fully agentic (run + save automatically, sandbox-confined).

Author: mohamedgamal04
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import chainlit as cl
import pandas as pd
from dotenv import load_dotenv

from queryquest.config import EXCEL_DIR, PROVIDERS_BY_NAME, SYSTEM_PROMPT
from queryquest.core.engine import QueryEngine
from queryquest.core.models import EngineConfig, EngineResult, StatementResult, WritebackTarget
from queryquest.core.policy import AutoApprovePolicy, Policy
from queryquest.excel.context import (
    build_excel_files_info,
    format_excel_context,
    list_excel_files,
    normalize_excel_dir,
)
from queryquest.state import load_state

load_dotenv()

EXECUTION_HITL = "hitl"
EXECUTION_AUTO = "auto"


def _resolve_excel_dir(state: dict[str, str]) -> Path:
    """Pick the workbook directory: env override, saved state, then default."""
    env_dir = os.environ.get("QQ_EXCEL_DIR")
    if env_dir:
        return normalize_excel_dir(Path(env_dir))
    if state.get("excel_dir"):
        return normalize_excel_dir(Path(state["excel_dir"]))
    return normalize_excel_dir(Path(EXCEL_DIR))


def _build_system_prompt(excel_dir: Path) -> str:
    """Combine the base policy prompt with current workbook context."""
    info, _signature, _snapshot = build_excel_files_info(excel_dir)
    context = format_excel_context(info)
    return f"{SYSTEM_PROMPT}\n\n{context}" if context else SYSTEM_PROMPT


def _selected_mode(message: cl.Message, mode_id: str, default: str) -> str:
    """Read the chosen option id for a mode from an incoming message."""
    modes = getattr(message, "modes", None) or {}
    value = modes.get(mode_id)
    if value is None:
        return default
    # The value may be an option id string or a ModeOption-like object.
    return getattr(value, "id", value)


async def _confirm(question: str, yes_label: str, no_label: str) -> bool:
    """Ask a yes/no question with action buttons; return True on yes."""
    response = await cl.AskActionMessage(
        content=question,
        actions=[
            cl.Action(name="yes", payload={"value": "yes"}, label=yes_label),
            cl.Action(name="no", payload={"value": "no"}, label=no_label),
        ],
    ).send()
    return bool(response and response.get("payload", {}).get("value") == "yes")


class ChainlitPolicy(Policy):
    """Approval policy that asks the browser user to confirm each step."""

    async def approve_execution(self, statements: list[StatementResult]) -> bool:
        blocks = "\n".join(f"```sql\n{statement.sql}\n```" for statement in statements)
        return await _confirm(
            f"Run the following {len(statements)} statement(s)?\n\n{blocks}",
            "✅ Run",
            "❌ Cancel",
        )

    async def approve_writeback(self, target: WritebackTarget) -> bool:
        elements = []
        if target.preview_rows:
            dataframe = pd.DataFrame(target.preview_rows, columns=target.preview_columns or None)
            elements = [cl.Dataframe(data=dataframe, name="rows to change", display="inline")]
        await cl.Message(
            content=(
                f"**{target.affected_rows}** row(s) in `{target.file_path.name}` "
                f"(sheet `{target.sheet_name}`) will change:"
            ),
            elements=elements,
        ).send()
        return await _confirm("Save these changes to the workbook?", "💾 Save", "❌ Discard")


def _empty_select_hint(statement: StatementResult) -> str:
    """Explain a zero-row SELECT, with a join-specific note when relevant."""
    if re.search(r"\bjoin\b", statement.sql, flags=re.IGNORECASE):
        return (
            "⚠️ The join matched no rows — the two sheets likely share no matching "
            "key column, so there is nothing to join on."
        )
    return "ℹ️ The query ran but matched no rows. Check the filter or column values."


async def _send_result(result: EngineResult) -> None:
    """Render an EngineResult as Chainlit messages and Dataframe elements."""
    if result.error is not None:
        await cl.Message(content=f"⚠️ {result.error}").send()
        return

    if result.explanation:
        await cl.Message(content=result.explanation).send()

    for statement in result.statements:
        if statement.error is not None:
            await cl.Message(content=f"❌ **Refused/failed:** {statement.error}\n\n```sql\n{statement.sql}\n```").send()
        elif statement.kind == "select":
            if statement.row_count == 0:
                await cl.Message(content=_empty_select_hint(statement)).send()
            else:
                dataframe = pd.DataFrame(statement.rows, columns=statement.columns or None)
                note = f"{statement.row_count} row(s)"
                if statement.truncated:
                    note = f"Showing first {len(statement.rows)} of {statement.row_count} rows"
                await cl.Message(
                    content=note,
                    elements=[cl.Dataframe(data=dataframe, name="result", display="inline")],
                ).send()
        else:
            await cl.Message(content=f"✏️ **{statement.kind.upper()}** affected {statement.row_count} row(s).").send()

    if result.wrote_back and result.writeback_targets:
        for target in result.writeback_targets:
            await cl.Message(
                content=(
                    f"💾 Saved **{target.affected_rows}** row(s) to `{target.file_path.name}` "
                    f"(sheet `{target.sheet_name}`)."
                )
            ).send()
    elif result.writeback_targets and not result.wrote_back:
        await cl.Message(content="🚫 Changes discarded — nothing written to disk.").send()


@cl.on_chat_start
async def on_chat_start() -> None:
    """Build the engine config from .provider.json and register the Execution picker."""
    state = load_state()
    if state is None:
        await cl.Message(content="No provider configured. Run `uv run qq --setup` first, then reload.").send()
        return

    excel_dir = _resolve_excel_dir(state)
    provider = PROVIDERS_BY_NAME[state["provider"]]
    config = EngineConfig(
        base_url=provider.base_url,
        api_key=state["api_key"],
        model=state["model"],
        provider_name=provider.name,
        system_prompt=_build_system_prompt(excel_dir),
        excel_dir=excel_dir,
        excel_files_count=len(list_excel_files(excel_dir)),
    )
    cl.user_session.set("config", config)
    cl.user_session.set("excel_dir", excel_dir)

    await cl.context.emitter.set_modes(
        [
            cl.Mode(
                id="execution",
                name="Execution",
                options=[
                    cl.ModeOption(
                        id=EXECUTION_HITL,
                        name="Human-in-the-loop",
                        description="Confirm before running and before saving",
                        default=True,
                    ),
                    cl.ModeOption(
                        id=EXECUTION_AUTO,
                        name="Fully agentic",
                        description="Run and save automatically — sandbox-confined",
                    ),
                ],
            )
        ]
    )

    await cl.Message(
        content=(
            "**QueryQuest** is ready.\n\n"
            f"- Provider: `{provider.name}` · Model: `{state['model']}`\n"
            f"- Workbooks: `{excel_dir}` ({config.excel_files_count} file(s))\n"
            "- Pick an **Execution** mode below the composer: human-in-the-loop confirms each step; "
            "fully agentic runs and saves on its own."
        )
    ).send()


@cl.on_message
async def on_message(message: cl.Message) -> None:
    """Run the user's request through the engine under the chosen execution mode."""
    config: EngineConfig | None = cl.user_session.get("config")
    excel_dir = cl.user_session.get("excel_dir")
    if config is None or excel_dir is None:
        await cl.Message(content="Engine not initialized. Reload the chat after running `qq --setup`.").send()
        return

    execution = _selected_mode(message, "execution", EXECUTION_HITL)
    policy: Policy = AutoApprovePolicy(excel_dir) if execution == EXECUTION_AUTO else ChainlitPolicy()

    result = await QueryEngine(config).run(message.content, policy)
    await _send_result(result)
