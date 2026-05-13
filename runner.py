import sys
import time
from functools import partial
from pathlib import Path

from runner_core.console import ANSI_RED, ANSI_YELLOW, colorize
from runner_core.exporters.excel_writer import save_excel
from runner_core.jobs.executor import run_job
from runner_core.jobs.resolver import resolve_job_files
from runner_core.providers.data_go import run_data_go_kr_job
from runner_core.providers.kosis import run_kosis_job
from runner_core.providers.kosis_multi import run_kosis_multi_job
from runner_core.providers.kosis_sources import run_kosis_sources_job

JOBS_DIR = Path("jobs")
OUTPUT_ROOT = Path("output")


def main():
    jobs = resolve_job_files(sys.argv[1:], JOBS_DIR)
    print(f"총 {len(jobs)}개 job 실행")

    provider_runners = {
        "kosis": run_kosis_job,
        "kosis_multi": run_kosis_multi_job,
        "kosis_sources": run_kosis_sources_job,
        "data_go_kr": run_data_go_kr_job,
    }
    save_excel_func = partial(save_excel, OUTPUT_ROOT)

    success = 0
    failed = 0
    all_started = time.time()

    for i, job_file in enumerate(jobs, start=1):
        try:
            run_job(job_file, provider_runners, save_excel_func, i, len(jobs))
            success += 1
        except KeyboardInterrupt:
            total_elapsed = time.time() - all_started
            print()
            print(colorize("[CANCEL] 사용자 중단으로 실행을 종료합니다.", ANSI_YELLOW))
            print(f"중단 시점 요약: 성공 {success}, 실패 {failed}, 총 {total_elapsed:.1f}s")
            return
        except Exception as e:
            failed += 1
            print(colorize(f"[ERROR] 실패: {job_file.name} -> {e}", ANSI_RED))

    total_elapsed = time.time() - all_started
    print(f"\n모든 작업 완료 (성공 {success}, 실패 {failed}, 총 {total_elapsed:.1f}s)")


if __name__ == "__main__":
    main()
