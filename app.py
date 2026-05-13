from __future__ import annotations

import os
import time
from pathlib import Path

import streamlit as st

from services.job_catalog_service import DEFAULT_SCOPE_REGION, REGIONS, build_job_index, filter_rows_by_region
from services.job_runner_service import JobRunnerService
from state.dashboard_state import apply_selected_region, build_matrix, collect_runtime, init_state
from ui.app_styles import inject_styles
from ui.dashboard_views import render_job_selector, render_live_bottom, render_live_right_panel, render_live_top, render_region_selector


st.set_page_config(
    page_title="KOSIS 통계지표 자동 생성 시스템",
    page_icon="📊",
    layout="wide",
)


def get_service() -> JobRunnerService:
    if "runner_service" not in st.session_state:
        st.session_state.runner_service = JobRunnerService()
    return st.session_state.runner_service


def start_selected_run(service: JobRunnerService, meta_by_path: dict[str, dict]) -> None:
    targets = st.session_state["selected_paths"]
    st.session_state["run_started_at"] = time.time()
    st.session_state["matrix_state"] = build_matrix(targets, st.session_state["selected_regions"], meta_by_path, "실행 중")
    result = service.start_run(targets)
    st.session_state["flash"] = ("success" if result["ok"] == "true" else "error", result["message"])
    st.rerun()


def rerun_failed(service: JobRunnerService, meta_by_path: dict[str, dict]) -> None:
    failed_targets = st.session_state["last_failed_paths"]
    result = service.start_run(failed_targets) if failed_targets else {"ok": "false", "message": "최근 실패 작업이 없습니다."}
    if failed_targets:
        st.session_state["run_started_at"] = time.time()
        st.session_state["matrix_state"] = build_matrix(failed_targets, st.session_state["selected_regions"], meta_by_path, "실행 중")
    st.session_state["flash"] = ("success" if result["ok"] == "true" else "error", result["message"])
    st.rerun()


def stop_run(service: JobRunnerService) -> None:
    result = service.stop_run()
    st.session_state["flash"] = ("success" if result["ok"] == "true" else "error", result["message"])
    st.rerun()


inject_styles()
init_state(DEFAULT_SCOPE_REGION)
st.session_state["_api_key_exists"] = bool(os.getenv("KOSIS_API_KEY", "").strip())

service = get_service()
groups = service.list_job_groups()
if groups and st.session_state["selected_group"] not in groups:
    st.session_state["selected_group"] = groups[0]

selected_group = st.session_state["selected_group"]
job_paths = service.list_job_files([selected_group]) if selected_group else []
all_job_rows, meta_by_path = build_job_index(Path.cwd(), job_paths)

if st.session_state["selected_region"] not in REGIONS:
    apply_selected_region(DEFAULT_SCOPE_REGION)
else:
    apply_selected_region(st.session_state["selected_region"])

filtered_job_rows = filter_rows_by_region(all_job_rows, st.session_state["selected_region"])
valid_paths = [row["path"] for row in filtered_job_rows]
st.session_state["selected_paths"] = [path for path in st.session_state["selected_paths"] if path in valid_paths]
if not st.session_state["selected_paths"] and valid_paths:
    st.session_state["selected_paths"] = valid_paths[: min(4, len(valid_paths))]

render_live_top(
    service,
    meta_by_path,
    valid_paths,
    collect_runtime,
    lambda: start_selected_run(service, meta_by_path),
    lambda: rerun_failed(service, meta_by_path),
    lambda: stop_run(service),
)

left_col, center_col, right_col = st.columns([0.95, 1.3, 1.05], gap="small")

with left_col:
    render_region_selector(
        regions=REGIONS,
        selected_region=st.session_state["selected_region"],
        all_job_rows=all_job_rows,
        on_region_change=apply_selected_region,
    )

with center_col:
    render_job_selector(
        groups=groups,
        selected_group=selected_group,
        filtered_job_rows=filtered_job_rows,
        meta_by_path=meta_by_path,
    )

with right_col:
    render_live_right_panel(service, meta_by_path, collect_runtime)

render_live_bottom(service, meta_by_path, collect_runtime)
