from __future__ import annotations

import os
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
PRIMARY_ENV_FILE = ROOT_DIR / ".env"
LEGACY_ENV_FILE = ROOT_DIR / ".env.local"
ENV_FILES = (PRIMARY_ENV_FILE, LEGACY_ENV_FILE)
KEY_NAME = "KOSIS_API_KEY"


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def get_kosis_api_key() -> str:
    for env_file in ENV_FILES:
        value = _parse_env_file(env_file).get(KEY_NAME, "").strip()
        if value:
            return value
    return ""


def get_legacy_env_kosis_api_key() -> str:
    return os.getenv(KEY_NAME, "").strip()


def has_kosis_api_key() -> bool:
    return bool(get_kosis_api_key())


def mask_api_key(value: str) -> str:
    text = value.strip()
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}{'*' * (len(text) - 8)}{text[-4:]}"


def save_kosis_api_key(value: str) -> Path:
    key = value.strip()
    if not key:
        raise ValueError("KOSIS API key cannot be empty.")

    lines: list[str] = []
    replaced = False
    if PRIMARY_ENV_FILE.exists():
        for raw_line in PRIMARY_ENV_FILE.read_text(encoding="utf-8").splitlines():
            if raw_line.strip().startswith(f"{KEY_NAME}="):
                lines.append(f"{KEY_NAME}={key}")
                replaced = True
            else:
                lines.append(raw_line)
    if not replaced:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(f"{KEY_NAME}={key}")

    PRIMARY_ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return PRIMARY_ENV_FILE


def clear_kosis_api_key() -> Path:
    if not PRIMARY_ENV_FILE.exists():
        return PRIMARY_ENV_FILE

    kept_lines = [
        raw_line
        for raw_line in PRIMARY_ENV_FILE.read_text(encoding="utf-8").splitlines()
        if not raw_line.strip().startswith(f"{KEY_NAME}=")
    ]
    content = "\n".join(kept_lines).rstrip()
    PRIMARY_ENV_FILE.write_text((content + "\n") if content else "", encoding="utf-8")
    return PRIMARY_ENV_FILE
