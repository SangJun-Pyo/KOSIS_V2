from __future__ import annotations

import time
from pathlib import Path

import streamlit as st

from services.job_runner_service import JobRunnerService


def init_state(default_region: str) -> None:
    st.session_state.setdefault("selected_group", "")
    st.session_state.setdefault("selected_paths", [])
    st.session_state.setdefault("selected_region", default_region)
    st.session_state.setdefault("selected_regions", [default_region])
    st.session_state.setdefault("selected_period", "2026년 상반기")
    st.session_state.setdefault("last_failed_paths", [])
    st.session_state.setdefault("matrix_state", {})
    st.session_state.setdefault("last_terminal_at", None)
    st.session_state.setdefault("run_started_at", None)


def apply_selected_region(region: str) -> None:
    st.session_state["selected_region"] = region
    st.session_state["selected_regions"] = [region]


def build_matrix(paths: list[str], regions: list[str], meta_by_path: dict[str, dict], label: str) -> dict[str, str]:
    matrix: dict[str, str] = {}
    for rel_path in paths:
        job_regions = meta_by_path.get(rel_path, {}).get("regions", [])
        targets = job_regions if job_regions else regions
        for region in targets:
            matrix[f"{rel_path}|{region}"] = label
    return matrix


def refresh_matrix_from_artifacts(artifacts: list[Path], meta_by_path: dict[str, dict], service: JobRunnerService) -> None:
    run_started_at = st.session_state.get("run_started_at")
    if not run_started_at or not st.session_state["selected_paths"]:
        return

    recent_artifacts = [path for path in artifacts if path.stat().st_mtime >= run_started_at - 1]
    recent_names = [path.name for path in recent_artifacts]

    completed_targets = set()
    for rel_path in st.session_state["selected_paths"]:
        prefix = str(meta_by_path.get(rel_path, {}).get("output_prefix") or Path(rel_path).stem)
        if any(name.startswith(prefix) for name in recent_names):
            completed_targets.add(rel_path)
            for key in list(st.session_state["matrix_state"].keys()):
                if key.startswith(f"{rel_path}|"):
                    st.session_state["matrix_state"][key] = "완료"

    if completed_targets and len(completed_targets) == len(st.session_state["selected_paths"]):
        if service.state.status in {"RUNNING", "PENDING"}:
            service.state.status = "SUCCESS"
            service.state.finished_at = time.time()
            service.state.success_count = len(completed_targets)


def sync_terminal_state(service: JobRunnerService) -> None:
    finished_at = service.state.finished_at
    if not finished_at or st.session_state["last_terminal_at"] == finished_at:
        return

    final_label = {"SUCCESS": "완료", "FAILED": "실패", "CANCELED": "중지"}.get(service.state.status)
    if final_label:
        for key in list(st.session_state["matrix_state"].keys()):
            st.session_state["matrix_state"][key] = final_label

    if service.state.status == "FAILED":
        st.session_state["last_failed_paths"] = service.state.selected_targets.copy()
    elif service.state.status == "SUCCESS":
        st.session_state["last_failed_paths"] = []

    st.session_state["last_terminal_at"] = finished_at


def collect_runtime(service: JobRunnerService, meta_by_path: dict[str, dict]) -> dict:
    service.drain_logs()
    artifacts = service.list_artifacts()
    refresh_matrix_from_artifacts(artifacts, meta_by_path, service)
    sync_terminal_state(service)
    return {
        "artifacts": artifacts,
        "summary": service.get_status_summary(),
        "running": service.is_running(),
        "last_run_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(artifacts[0].stat().st_mtime)) if artifacts else "-",
        "api_key_exists": bool(st.session_state.get("_api_key_exists")),
    }

