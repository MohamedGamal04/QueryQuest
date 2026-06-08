"""Async LLM access for the core engine.

I keep the provider call and its error handling here so the engine stays a pure
orchestrator and the CLI never touches the OpenAI client directly.

Author: mohamedgamal04
"""

from __future__ import annotations

import json

from openai import (
    APIConnectionError,
    AsyncOpenAI,
    AuthenticationError,
    NotFoundError,
    RateLimitError,
)

from ..sql.handoff import _candidate_json_strings, extract_sql_statements
from .models import EngineConfig


def _extract_explanation(output: str) -> str:
    """Pull the optional `explanation` field from the model's JSON payload."""
    for candidate in _candidate_json_strings(output):
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and isinstance(parsed.get("explanation"), str):
            return parsed["explanation"].strip()
    return ""


async def generate_sql(config: EngineConfig, user_prompt: str) -> tuple[str, list[str], str, str | None]:
    """Ask the provider for SQL. Returns (raw_output, statements, explanation, error)."""
    client = AsyncOpenAI(base_url=config.base_url, api_key=config.api_key)
    messages = [
        {"role": "system", "content": config.system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        response = await client.chat.completions.create(model=config.model, messages=messages)
    except AuthenticationError as error:
        return "", [], "", f"Authentication failed: {error}"
    except NotFoundError as error:
        return "", [], "", f"Model or endpoint not found: {error}"
    except RateLimitError as error:
        return "", [], "", f"Rate limit reached: {error}"
    except APIConnectionError as error:
        return "", [], "", f"Could not reach the provider: {error}"

    output = response.choices[0].message.content or ""
    statements = extract_sql_statements(output)
    explanation = _extract_explanation(output)
    return output, statements, explanation, None
