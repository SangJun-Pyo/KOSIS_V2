import time
from typing import Any, Optional

import requests

from runner_core.config import DEFAULT_TIMEOUT, MAX_RETRIES, RETRY_WAIT_SEC
from runner_core.console import ANSI_YELLOW, colorize


def request_json_with_retry(url: str, params: dict, timeout: int = DEFAULT_TIMEOUT) -> Any:
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_error = e
            if attempt >= MAX_RETRIES:
                break
            print(colorize(f"[WARN] ?? ?? {attempt}/{MAX_RETRIES}: {e}", ANSI_YELLOW))
            print(f"[INFO] {RETRY_WAIT_SEC:.0f}? ? ??????.")
            time.sleep(RETRY_WAIT_SEC)

    raise RuntimeError(f"API ?? ?? after {MAX_RETRIES} attempts: {last_error}")
