"""Human-in-the-loop / autonomous Chainlit prototype for QueryQuest.

Run with:  uv run chainlit run chainlit_app.py

Drives the same async QueryEngine the CLI uses, using the provider/model from
`.provider.json`. An Execution mode picker switches between human-in-the-loop
(confirm before running and before saving, with a Dataframe preview of the rows
about to change) and fully agentic (run + save automatically, sandbox-confined).

Author: mohamedgamal04
"""

from __future__ import annotations

import re
import shutil
import tempfile
from dataclasses import replace
from pathlib import Path

import chainlit as cl
import pandas as pd
from dotenv import load_dotenv

from queryquest.config import PROVIDERS, PROVIDERS_BY_NAME, SYSTEM_PROMPT
from queryquest.core.engine import QueryEngine
from queryquest.core.models import EngineConfig, EngineResult, StatementResult, WritebackTarget
from queryquest.core.policy import AutoApprovePolicy, Policy
from queryquest.excel.context import build_excel_files_info, format_excel_context, list_excel_files
from queryquest.state import load_state

load_dotenv()

EXECUTION_HITL = "hitl"
EXECUTION_AUTO = "auto"


async def _resolve_config(work_dir: Path) -> EngineConfig | None:
    """Build an engine config from saved state, or prompt the user for one.

    Locally, `.provider.json` (from `qq --setup`) is used. When it is absent —
    e.g. a public Hugging Face Space — the user picks a provider and pastes
    their own API key, which is kept only for this session.
    """
    state = load_state()
    if state is not None:
        provider = PROVIDERS_BY_NAME[state["provider"]]
        return EngineConfig(
            base_url=provider.base_url,
            api_key=state["api_key"],
            model=state["model"],
            provider_name=provider.name,
            system_prompt=SYSTEM_PROMPT,
            excel_dir=work_dir,
            excel_files_count=0,
        )

    choice = await cl.AskActionMessage(
        content="Choose your LLM provider (you'll paste your own API key next):",
        actions=[
            cl.Action(name=key, payload={"value": key}, label=provider.name.capitalize())
            for key, provider in PROVIDERS.items()
        ],
        timeout=300,
    ).send()
    provider_key = (choice or {}).get("payload", {}).get("value")
    provider = PROVIDERS.get(provider_key) if provider_key else None
    if provider is None:
        await cl.Message(content="No provider selected. Reload to try again.").send()
        return None

    answer = await cl.AskUserMessage(
        content=(
            f"Paste your **{provider.name}** API key for `{provider.default_model}`. "
            "It is kept only for this chat session (and will show in the transcript)."
        ),
        timeout=300,
    ).send()
    api_key = ((answer or {}).get("output") or "").strip()
    if not api_key:
        await cl.Message(content=f"No API key provided for {provider.name}. Reload to try again.").send()
        return None

    return EngineConfig(
        base_url=provider.base_url,
        api_key=api_key,
        model=provider.default_model,
        provider_name=provider.name,
        system_prompt=SYSTEM_PROMPT,
        excel_dir=work_dir,
        excel_files_count=0,
    )


def _build_system_prompt(excel_dir: Path) -> str:
    """Combine the base policy prompt with current workbook context."""
    info, _signature, _snapshot = build_excel_files_info(excel_dir)
    context = format_excel_context(info)
    return f"{SYSTEM_PROMPT}\n\n{context}" if context else SYSTEM_PROMPT


def _excel_attachments(message: cl.Message) -> list[tuple[str, str]]:
    """Return (name, temp_path) for any .xlsx/.xls files attached to a message."""
    found: list[tuple[str, str]] = []
    for element in getattr(message, "elements", None) or []:
        name = getattr(element, "name", "") or ""
        path = getattr(element, "path", None)
        if path and name.lower().endswith((".xlsx", ".xls")):
            found.append((name, path))
    return found


def _ingest_attachments(message: cl.Message, config: EngineConfig) -> tuple[EngineConfig, Path, list[str]] | None:
    """Copy attached workbooks into the session temp dir and rebuild the config.

    Returns (new_config, work_dir, names) when files were attached, else None.
    Files accumulate across messages so follow-up questions see all of them.
    """
    attachments = _excel_attachments(message)
    if not attachments:
        return None

    work_dir_value = cl.user_session.get("temp_dir")
    work_dir = Path(work_dir_value) if work_dir_value else Path(tempfile.mkdtemp(prefix="qq_upload_"))
    cl.user_session.set("temp_dir", str(work_dir))

    names: list[str] = []
    for name, path in attachments:
        shutil.copy(path, work_dir / name)
        names.append(name)

    new_config = replace(
        config,
        system_prompt=_build_system_prompt(work_dir),
        excel_dir=work_dir,
        excel_files_count=len(list_excel_files(work_dir)),
    )
    return new_config, work_dir, names


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
        seen_files: set[str] = set()
        for target in result.writeback_targets:
            await cl.Message(
                content=(
                    f"💾 Saved **{target.affected_rows}** row(s) to `{target.file_path.name}` "
                    f"(sheet `{target.sheet_name}`)."
                )
            ).send()
            file_key = str(target.file_path)
            if file_key in seen_files:
                continue
            seen_files.add(file_key)
            await cl.Message(
                content=f"⬇️ Download the updated `{target.file_path.name}`:",
                elements=[cl.File(name=target.file_path.name, path=file_key, display="inline")],
            ).send()
    elif result.writeback_targets and not result.wrote_back:
        await cl.Message(content="🚫 Changes discarded — nothing written to disk.").send()


@cl.on_chat_start
async def on_chat_start() -> None:
    """Build the engine config from .provider.json and register the Execution picker."""
    # Work only on attached files: start with an empty per-session directory.
    work_dir = Path(tempfile.mkdtemp(prefix="qq_session_"))
    cl.user_session.set("temp_dir", str(work_dir))

    config = await _resolve_config(work_dir)
    if config is None:
        return
    cl.user_session.set("config", config)
    cl.user_session.set("excel_dir", work_dir)

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
            f"- Provider: `{config.provider_name}` · Model: `{config.model}`\n"
            "- 📎 **Attach `.xlsx`/`.xls` files** to a message to begin — I only work on the files you attach.\n"
            "- Pick an **Execution** mode below the composer: human-in-the-loop confirms each step; "
            "fully agentic runs and saves on its own.\n"
            "- After an edit is saved you'll get a link to download the updated file."
        )
    ).send()


@cl.on_chat_end
async def on_chat_end() -> None:
    """Clean up any per-session temp directory created for uploaded files."""
    temp_dir = cl.user_session.get("temp_dir")
    if temp_dir:
        shutil.rmtree(temp_dir, ignore_errors=True)


@cl.on_message
async def on_message(message: cl.Message) -> None:
    """Run the user's request through the engine under the chosen execution mode."""
    config: EngineConfig | None = cl.user_session.get("config")
    if config is None:
        await cl.Message(content="Engine not initialized. Reload the chat after running `qq --setup`.").send()
        return

    # Pick up any workbooks attached to this message via the composer.
    ingested = _ingest_attachments(message, config)
    if ingested is not None:
        config, work_dir, names = ingested
        cl.user_session.set("config", config)
        cl.user_session.set("excel_dir", work_dir)
        await cl.Message(
            content=f"📎 Loaded {len(names)} file(s): "
            + ", ".join(f"`{name}`" for name in names)
            + ". Now querying your uploaded data (originals untouched)."
        ).send()

    excel_dir = cl.user_session.get("excel_dir")
    if not (message.content or "").strip():
        # Files attached with no question yet — wait for the actual request.
        if ingested is None:
            await cl.Message(content="📎 Attach a workbook, then ask a question about it.").send()
        return

    if config.excel_files_count == 0:
        await cl.Message(content="📎 Attach at least one `.xlsx`/`.xls` file first — I only work on attached files.").send()
        return

    execution = _selected_mode(message, "execution", EXECUTION_HITL)
    policy: Policy = AutoApprovePolicy(excel_dir) if execution == EXECUTION_AUTO else ChainlitPolicy()

    result = await QueryEngine(config).run(message.content, policy)
    await _send_result(result)
