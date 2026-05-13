from __future__ import annotations

import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Queue


ROOT_DIR = Path(__file__).resolve().parent.parent
RUNNER_PATH = ROOT_DIR / "runner.py"
JOBS_DIR = ROOT_DIR / "jobs"
OUTPUT_DIR = ROOT_DIR / "output"


@dataclass
class RunState:
    status: str = "PENDING"
    selected_targets: list[str] = field(default_factory=list)
    started_at: float | None = None
    finished_at: float | None = None
    logs: list[str] = field(default_factory=list)
    return_code: int | None = None
    process: subprocess.Popen[str] | None = None
    success_count: int = 0
    failed_count: int = 0

    def push_log(self, line: str) -> None:
        text = line.rstrip()
        if not text:
            return
        self.logs.append(text)
        if "[OK" in text:
            self.success_count += 1
        if "[ERROR]" in text:
            self.failed_count += 1
        if len(self.logs) > 1000:
            self.logs = self.logs[-1000:]

    @property
    def elapsed(self) -> float:
        if self.started_at is None:
            return 0.0
        end = self.finished_at if self.finished_at is not None else time.time()
        return max(0.0, end - self.started_at)


class JobRunnerService:
    def __init__(self) -> None:
        self.state = RunState()
        self._queue: Queue[str] = Queue()
        self._reader_thread: threading.Thread | None = None

    def list_job_groups(self) -> list[str]:
        if not JOBS_DIR.exists():
            return []

        groups: list[str] = []
        for directory in sorted(JOBS_DIR.iterdir()):
            if not directory.is_dir():
                continue
            if list(directory.glob("*.json")):
                groups.append(directory.name)
        return groups

    def list_job_files(self, groups: list[str] | None = None) -> list[str]:
        if not JOBS_DIR.exists():
            return []

        files: list[str] = []
        for group in groups or self.list_job_groups():
            group_dir = JOBS_DIR / group
            if not group_dir.exists():
                continue
            files.extend(str(path.relative_to(ROOT_DIR)) for path in sorted(group_dir.glob("*.json")))
        return files

    def is_running(self) -> bool:
        process = self.state.process
        return bool(process and process.poll() is None)

    def start_run(self, targets: list[str]) -> dict[str, str]:
        if self.is_running():
            return {"ok": "false", "message": "이미 실행 중입니다."}
        if not RUNNER_PATH.exists():
            return {"ok": "false", "message": "runner.py를 찾을 수 없습니다."}
        if not targets:
            return {"ok": "false", "message": "실행할 작업이 없습니다."}

        resolved_targets = [str((ROOT_DIR / target).resolve()) for target in targets]
        self.state = RunState(
            status="RUNNING",
            selected_targets=targets.copy(),
            started_at=time.time(),
        )
        self.state.push_log("[INFO] 실행을 시작했습니다.")

        process = subprocess.Popen(
            [sys.executable, str(RUNNER_PATH), *resolved_targets],
            cwd=str(ROOT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self.state.process = process
        self._reader_thread = threading.Thread(target=self._read_output, args=(process,), daemon=True)
        self._reader_thread.start()
        return {"ok": "true", "message": "선택한 작업 실행을 시작했습니다."}

    def _read_output(self, process: subprocess.Popen[str]) -> None:
        assert process.stdout is not None
        for line in process.stdout:
            self._queue.put(line.rstrip())
        process.wait()
        self._queue.put(f"[INFO] 프로세스 종료 코드: {process.returncode}")

    def drain_logs(self) -> None:
        while True:
            try:
                line = self._queue.get_nowait()
            except Empty:
                break
            self.state.push_log(line)

        process = self.state.process
        if process and process.poll() is not None and self.state.status == "RUNNING":
            self.state.return_code = process.returncode
            self.state.finished_at = time.time()
            self.state.status = "SUCCESS" if process.returncode == 0 else "FAILED"

    def stop_run(self) -> dict[str, str]:
        process = self.state.process
        if not process or process.poll() is not None:
            return {"ok": "false", "message": "실행 중인 작업이 없습니다."}

        process.terminate()
        self.state.status = "CANCELED"
        self.state.finished_at = time.time()
        self.state.push_log("[WARN] 사용자가 실행을 중지했습니다.")
        return {"ok": "true", "message": "실행을 중지했습니다."}

    def list_artifacts(self) -> list[Path]:
        if not OUTPUT_DIR.exists():
            return []
        return sorted(OUTPUT_DIR.rglob("*.xlsx"), key=lambda path: path.stat().st_mtime, reverse=True)

    def get_status_summary(self) -> dict[str, int]:
        total = len(self.state.selected_targets)
        running = max(total - self.state.success_count - self.state.failed_count, 0) if self.is_running() else 0
        stopped = total if self.state.status == "CANCELED" else 0
        return {
            "total": total,
            "done": self.state.success_count,
            "failed": self.state.failed_count,
            "running": running,
            "stopped": stopped,
        }
