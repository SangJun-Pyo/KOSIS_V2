import time
from typing import Any, Optional

import requests

from runner_core.config import DEFAULT_TIMEOUT, MAX_RETRIES, RETRY_WAIT_SEC


def request_json_with_retry(url: str, params: dict, timeout: int = DEFAULT_TIMEOUT) -> Any:
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt >= MAX_RETRIES:
                break
            print(f"[INFO] API 재시도 {attempt}/{MAX_RETRIES}: {exc}")
            print(f"[INFO] {RETRY_WAIT_SEC:.0f}초 후 다시 시도합니다.")
            time.sleep(RETRY_WAIT_SEC)

    raise RuntimeError(f"API 요청 실패 after {MAX_RETRIES} attempts: {last_error}")


def request_text_with_retry(url: str, params: dict, timeout: int = DEFAULT_TIMEOUT) -> str:
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.text
        except Exception as exc:
            last_error = exc
            if attempt >= MAX_RETRIES:
                break
            print(f"[INFO] API 재시도 {attempt}/{MAX_RETRIES}: {exc}")
            print(f"[INFO] {RETRY_WAIT_SEC:.0f}초 후 다시 시도합니다.")
            time.sleep(RETRY_WAIT_SEC)

    raise RuntimeError(f"API 요청 실패 after {MAX_RETRIES} attempts: {last_error}")
