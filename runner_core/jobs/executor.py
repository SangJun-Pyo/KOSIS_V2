import json
import time
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple, Any

from runner_core.console import ANSI_CYAN, ANSI_GREEN, colorize


ProviderRunner = Callable[[dict], Tuple[Any, Any, str]]
ProviderMap = Dict[str, ProviderRunner]


def run_job(
    job_path: Path,
    provider_runners: ProviderMap,
    save_excel_func: Callable[[dict, Any, Any, str], Path],
    idx: Optional[int] = None,
    total: Optional[int] = None,
) -> None:
    with open(job_path, "r", encoding="utf-8") as f:
        job = json.load(f)

    job_name = job.get("job_name", job_path.stem)
    provider = job.get("provider", "kosis")
    started = time.time()
    prefix = "[RUN]"

    if idx is not None and total:
        pct = int((idx / total) * 100)
        prefix = f"[RUN {idx}/{total} {pct}%]"

    print(f"\n{colorize(prefix, ANSI_CYAN)} {job_name}  (provider={provider})")

    if provider not in provider_runners:
        raise RuntimeError(f"Unknown provider: {provider}")

    raw_df, pivot_df, sheet_name = provider_runners[provider](job)
    out_path = save_excel_func(job, raw_df, pivot_df, sheet_name)

    elapsed = time.time() - started
    ok_prefix = "[OK]"
    if idx is not None and total:
        pct = int((idx / total) * 100)
        ok_prefix = f"[OK  {idx}/{total} {pct}%]"
    print(f"{colorize(ok_prefix, ANSI_GREEN)} 저장 완료: {out_path} ({elapsed:.1f}s)")
