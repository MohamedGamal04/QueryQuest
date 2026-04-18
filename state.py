import json
import os

from app_config import HISTORY_FILE, PROVIDERS_BY_NAME, STATE_FILE


def find_env_api_key(env_names: tuple[str, ...]) -> str | None:
    for env_name in env_names:
        value = os.getenv(env_name)
        if value:
            return value
    return None


def load_state() -> dict[str, str] | None:
    if not STATE_FILE.exists():
        return None
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    provider_name = data.get("provider")
    api_key = data.get("api_key")
    model = data.get("model")
    if not isinstance(provider_name, str) or provider_name not in PROVIDERS_BY_NAME:
        return None
    if not isinstance(api_key, str) or not api_key:
        return None
    if not isinstance(model, str) or not model:
        return None
    return {"provider": provider_name, "api_key": api_key, "model": model}


def save_state(provider_name: str, api_key: str, model: str) -> None:
    payload = {"provider": provider_name, "api_key": api_key, "model": model}
    STATE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    try:
        os.chmod(STATE_FILE, 0o600)
    except OSError:
        # Best effort; some filesystems may not support chmod.
        pass


def load_history() -> list[dict[str, str]]:
    if not HISTORY_FILE.exists():
        return []
    try:
        data = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    if not isinstance(data, list):
        return []

    history: list[dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        role = item.get("role")
        content = item.get("content")
        if role in ("user", "assistant") and isinstance(content, str):
            history.append({"role": role, "content": content})
    return history


def save_history(history: list[dict[str, str]]) -> None:
    try:
        HISTORY_FILE.write_text(json.dumps(history, indent=2), encoding="utf-8")
    except OSError:
        # History persistence should not break the interactive session.
        pass


def reset_history() -> None:
    save_history([])
