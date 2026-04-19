import json
import os

from app_config import PROVIDERS_BY_NAME, STATE_FILE


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
    state: dict[str, str] = {"provider": provider_name, "api_key": api_key, "model": model}

    excel_dir = data.get("excel_dir")
    excel_signature = data.get("excel_signature")
    excel_info = data.get("excel_info")
    excel_info_format_version = data.get("excel_info_format_version")
    if isinstance(excel_dir, str) and excel_dir:
        state["excel_dir"] = excel_dir
    if isinstance(excel_signature, str) and excel_signature:
        state["excel_signature"] = excel_signature
    if isinstance(excel_info, str) and excel_info:
        state["excel_info"] = excel_info
    if isinstance(excel_info_format_version, str) and excel_info_format_version:
        state["excel_info_format_version"] = excel_info_format_version

    return state


def save_state(
    provider_name: str,
    api_key: str,
    model: str,
    *,
    excel_dir: str | None = None,
    excel_signature: str | None = None,
    excel_info: str | None = None,
    excel_info_format_version: str | None = None,
) -> None:
    payload: dict[str, str] = {"provider": provider_name, "api_key": api_key, "model": model}
    if excel_dir:
        payload["excel_dir"] = excel_dir
    if excel_signature:
        payload["excel_signature"] = excel_signature
    if excel_info:
        payload["excel_info"] = excel_info
    if excel_info_format_version:
        payload["excel_info_format_version"] = excel_info_format_version
    STATE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    try:
        os.chmod(STATE_FILE, 0o600)
    except OSError:
        # Best effort; some filesystems may not support chmod.
        pass


