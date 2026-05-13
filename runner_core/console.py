import os
import sys
import ctypes
from typing import Optional

ANSI_RESET = "\033[0m"
ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"
ANSI_YELLOW = "\033[33m"
ANSI_CYAN = "\033[36m"
_ANSI_READY: Optional[bool] = None


def supports_ansi() -> bool:
    global _ANSI_READY

    if _ANSI_READY is not None:
        return _ANSI_READY

    if os.getenv("NO_COLOR"):
        _ANSI_READY = False
        return False

    if os.name != "nt":
        _ANSI_READY = sys.stdout.isatty()
        return _ANSI_READY

    if not sys.stdout.isatty():
        _ANSI_READY = False
        return False

    try:
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            enabled = mode.value | 0x0004
            if kernel32.SetConsoleMode(handle, enabled):
                _ANSI_READY = True
                return True
    except Exception:
        pass

    _ANSI_READY = bool(os.getenv("WT_SESSION") or os.getenv("TERM") or os.getenv("ANSICON"))
    return _ANSI_READY


def colorize(text: str, color: str) -> str:
    if not supports_ansi():
        return text
    return f"{color}{text}{ANSI_RESET}"
