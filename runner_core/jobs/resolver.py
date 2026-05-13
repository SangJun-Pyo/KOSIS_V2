from pathlib import Path
from typing import List

from runner_core.console import ANSI_YELLOW, colorize


def resolve_job_files(args: List[str], jobs_dir: Path) -> List[Path]:
    targets = [Path(a) for a in args] if args else [jobs_dir]
    jobs: List[Path] = []

    for target in targets:
        if target.is_file():
            if target.suffix.lower() == ".json":
                jobs.append(target)
            continue

        if target.is_dir():
            jobs.extend(sorted(target.rglob("*.json")))
            continue

        print(colorize(f"[WARN] 경로를 찾을 수 없어 건너뜀: {target}", ANSI_YELLOW))

    uniq: List[Path] = []
    seen = set()
    for job in jobs:
        key = str(job.resolve())
        if key in seen:
            continue
        seen.add(key)
        uniq.append(job)
    return uniq
