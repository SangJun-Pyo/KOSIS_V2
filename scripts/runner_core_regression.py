import os
import sys
from functools import partial
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from runner_core.exporters.excel_writer import save_excel
from runner_core.jobs.executor import run_job
from runner_core.providers.data_go import run_data_go_kr_job
from runner_core.providers.kosis import run_kosis_job
from runner_core.providers.kosis_multi import run_kosis_multi_job
from runner_core.providers.kosis_sources import run_kosis_sources_job


SAMPLE_JOBS = [
    ROOT / "jobs" / "population" / "common" / "1-03_population_age5_metro_5y.json",
    ROOT / "jobs" / "population" / "common" / "1-08_population_household_status_6y.json",
    ROOT / "jobs" / "population" / "common" / "1-12_dependency_aging_population_multi.json",
]


def main() -> int:
    if not os.getenv("KOSIS_API_KEY", "").strip():
        print("[ERROR] KOSIS_API_KEY 환경변수가 없습니다.")
        return 1

    provider_runners = {
        "kosis": run_kosis_job,
        "kosis_multi": run_kosis_multi_job,
        "kosis_sources": run_kosis_sources_job,
        "data_go_kr": run_data_go_kr_job,
    }

    output_root = ROOT / "output" / "_regression"
    save_excel_func = partial(save_excel, output_root)

    failures = []
    for idx, job_path in enumerate(SAMPLE_JOBS, start=1):
        try:
            run_job(job_path, provider_runners, save_excel_func, idx, len(SAMPLE_JOBS))
        except Exception as e:
            failures.append((job_path.name, str(e)))

    if failures:
        print("[ERROR] regression failures")
        for name, error in failures:
            print(f"- {name}: {error}")
        return 1

    print("[OK] runner_core regression passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
