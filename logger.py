import json
from collections.abc import Mapping
from datetime import datetime, timezone

from app_config import LOG_FILE


def append_log(entry: Mapping[str, object]) -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **entry,
    }
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True, indent=2) + "\n")
    except OSError:
        # Logging should never interrupt chat flow.
        pass
