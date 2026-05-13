from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from services.job_catalog_service import find_meta_for_artifact


def status_chip(status: str) -> tuple[str, str]:
    mapping = {
        "PENDING": ("대기", "gray"),
        "RUNNING": ("실행 중", ""),
        "SUCCESS": ("완료", "ok"),
        "FAILED": ("실패", "fail"),
        "CANCELED": ("중지", "warn"),
    }
    return mapping.get(status, ("대기", "gray"))


def show_chip(status: str) -> None:
    label, klass = status_chip(status)
    class_name = "status-chip" if not klass else f"status-chip {klass}"
    st.markdown(f"<span class='{class_name}'>{label}</span>", unsafe_allow_html=True)


def build_checklist(artifacts: list[Path], service_status: str) -> pd.DataFrame:
    api_key_exists = bool(os.getenv("KOSIS_API_KEY", "").strip())
    rows = [
        ["선택 항목 존재 여부", "OK" if st.session_state["selected_paths"] else "NG", "정상" if st.session_state["selected_paths"] else "확인 필요"],
        ["지역 선택 여부", f"{len(st.session_state['selected_regions'])}개", "정상" if st.session_state["selected_regions"] else "확인 필요"],
        ["KOSIS_API_KEY 설정 여부", "OK" if api_key_exists else "NG", "정상" if api_key_exists else "오류"],
        ["runner.py 존재 여부", "OK" if Path("runner.py").exists() else "NG", "정상" if Path("runner.py").exists() else "오류"],
        ["jobs 폴더 존재 여부", "OK" if Path("jobs").exists() else "NG", "정상" if Path("jobs").exists() else "오류"],
        ["실행 상태", status_chip(service_status)[0], "정상" if service_status in {"PENDING", "RUNNING", "SUCCESS"} else "재확인 필요"],
        ["결과 파일 생성 여부", "OK" if artifacts else "-", "정상" if artifacts else "-"],
    ]
    return pd.DataFrame(rows, columns=["항목", "값", "상태"])


def build_result_table(artifacts: list[Path], meta_by_path: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for file_path in artifacts[:30]:
        stat = file_path.stat()
        matched_meta = find_meta_for_artifact(file_path, meta_by_path)
        rows.append(
            {
                "파일명": file_path.name,
                "지역 범위": matched_meta.get("scope_label", "-") if matched_meta else "-",
                "생성 시간": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "크기": f"{stat.st_size / 1024:.1f} KB",
            }
        )
    return pd.DataFrame(rows)


def build_log_table(logs: list[str]) -> pd.DataFrame:
    rows = []
    for line in logs[-300:]:
        level = "INFO"
        if "[ERROR]" in line:
            level = "ERROR"
        elif "[WARN]" in line:
            level = "WARN"
        elif "[OK" in line:
            level = "OK"
        rows.append({"수준": level, "메시지": line})
    return pd.DataFrame(rows)


def build_matrix_table(selected_paths: list[str], selected_regions: list[str], meta_by_path: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for rel_path in selected_paths[:10]:
        meta = meta_by_path.get(rel_path, {})
        row = {
            "항목명": meta.get("name", rel_path),
            "적용 범위": meta.get("scope_label", "-"),
        }
        job_regions = meta.get("regions", [])
        for region in selected_regions[:6]:
            row[region] = "대상 아님" if job_regions and region not in job_regions else st.session_state["matrix_state"].get(f"{rel_path}|{region}", "대기")
        rows.append(row)
    return pd.DataFrame(rows)


def render_region_map(selected_region: str, on_select) -> None:
    layout = [
        [None, None, "강원", None, None],
        ["인천", "서울", None, None, None],
        [None, "경기", "충북", None, None],
        ["충남", "세종", "대전", "경북", None],
        [None, "전북", None, "대구", None],
        ["광주", "전남", None, "경남", "울산"],
        [None, None, "부산", None, None],
        ["제주", None, None, None, None],
    ]

    st.markdown("<div class='region-map-card'>", unsafe_allow_html=True)
    st.markdown("<div class='region-map-note'>지역 버튼을 누르면 해당 지역 기준으로 작업 목록이 바뀝니다.</div>", unsafe_allow_html=True)
    st.write("")

    for row_index, row in enumerate(layout):
        cols = st.columns(len(row), gap="small")
        for col, region in zip(cols, row):
            with col:
                if region is None:
                    st.write("")
                    continue
                is_selected = selected_region == region
                if st.button(region, key=f"region_map_{row_index}_{region}", use_container_width=True, type="primary" if is_selected else "secondary"):
                    on_select(region)
                    st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)


def render_region_selector(regions: list[str], selected_region: str, on_region_change) -> None:
    with st.container(border=True):
        st.subheader("1. 지역 선택")
        dropdown_region = st.selectbox("지역 선택", regions, index=regions.index(selected_region))
        if dropdown_region != selected_region:
            on_region_change(dropdown_region)
            st.rerun()

        st.markdown(f"<div class='info-box'>선택 지역: <strong>{selected_region}</strong></div>", unsafe_allow_html=True)
        render_region_map(selected_region, on_region_change)


def render_job_selector(groups: list[str], selected_group: str, filtered_job_rows: list[dict[str, Any]], meta_by_path: dict[str, dict[str, Any]]) -> None:
    with st.container(border=True):
        st.subheader("2. 파트 및 항목 선택")
        if groups:
            st.session_state["selected_group"] = st.selectbox(
                "파트",
                groups,
                index=groups.index(selected_group) if selected_group in groups else 0,
                key="selected_group_widget",
            )
        else:
            st.warning("사용 가능한 jobs 그룹이 없습니다.")

        search = st.text_input("항목명 검색", value="", key="job_search_widget")
        visible_rows = [row for row in filtered_job_rows if search.lower() in row["name"].lower()]
        visible_paths = [row["path"] for row in visible_rows]
        st.session_state["selected_paths"] = st.multiselect(
            "항목 선택",
            options=visible_paths,
            default=[path for path in st.session_state["selected_paths"] if path in visible_paths],
            format_func=lambda path: meta_by_path.get(path, {"name": path})["name"],
            key="selected_paths_widget",
        )

        table_rows = [
            {
                "항목명": row["name"],
                "원천 데이터": row["source"],
                "지역 범위": row.get("scope_label", "-"),
                "최근 상태": "선택" if row["path"] in st.session_state["selected_paths"] else "대기",
            }
            for row in visible_rows[:30]
        ]
        table_df = pd.DataFrame(table_rows) if table_rows else pd.DataFrame(columns=["항목명", "원천 데이터", "지역 범위", "최근 상태"])
        st.dataframe(table_df, use_container_width=True, hide_index=True)


@st.fragment(run_every="2s")
def render_live_top(service, meta_by_path: dict[str, dict[str, Any]], valid_paths: list[str], collect_runtime, on_start, on_rerun_failed, on_stop) -> None:
    runtime = collect_runtime(service, meta_by_path)
    artifacts = runtime["artifacts"]
    summary = runtime["summary"]
    running = runtime["running"]
    last_run_time = runtime["last_run_time"]
    api_key_exists = runtime["api_key_exists"]

    with st.container(border=True):
        left, right = st.columns([1.4, 2.8], gap="small")
        with left:
            st.title("KOSIS 통계지표 자동 생성 시스템")
            st.markdown("<p class='muted'>KOSIS 원천 엑셀을 정리하고 산식을 적용해 지표를 검증합니다.</p>", unsafe_allow_html=True)
        with right:
            c1, c2, c3, c4, c5, c6 = st.columns([1.05, 0.85, 1.0, 1.25, 1.15, 1.0], gap="small")
            with c1:
                st.caption("기준 기간")
                st.session_state["selected_period"] = st.selectbox(
                    "기준 기간",
                    ["2026년 상반기", "2026년 하반기"],
                    index=0 if st.session_state["selected_period"] == "2026년 상반기" else 1,
                    label_visibility="collapsed",
                    key="selected_period_widget",
                )
            with c2:
                st.caption("시스템 상태")
                show_chip(service.state.status)
            with c3:
                st.caption("최근 실행")
                st.write(last_run_time)
            with c4:
                if st.button("선택 항목 처리 시작", type="primary", use_container_width=True, disabled=running, key="start_run_button"):
                    on_start()
            with c5:
                if st.button("실패 작업 다시 실행", use_container_width=True, disabled=running, key="rerun_failed_button"):
                    on_rerun_failed()
            with c6:
                if st.button("실행 중지", use_container_width=True, disabled=not running, key="stop_run_button"):
                    on_stop()

    if running:
        refresh_col1, refresh_col2 = st.columns([1.0, 4.0], gap="small")
        with refresh_col1:
            if st.button("진행 상태 새로고침", use_container_width=True, key="manual_refresh_button"):
                st.rerun()
        with refresh_col2:
            st.caption("실행 중에는 상태 영역만 자동 갱신됩니다.")

    if "flash" in st.session_state:
        level, message = st.session_state.pop("flash")
        (st.success if level == "success" else st.error)(message)

    if not api_key_exists:
        st.error("KOSIS_API_KEY가 설정되지 않아 실제 수집 실행은 실패합니다.")
    else:
        st.info(f"현재 선택 지역은 {st.session_state['selected_region']}입니다. 지역을 바꾸면 해당 지역으로 분류된 JSON 항목만 표시됩니다.")

    with st.container(border=True):
        m1, m2, m3, m4, m5, m6 = st.columns(6, gap="small")
        matrix_values = list(st.session_state["matrix_state"].values())
        done_count = max(summary["done"], sum(1 for value in matrix_values if value == "완료"))
        failed_count = max(summary["failed"], sum(1 for value in matrix_values if value == "실패"))
        stopped_count = max(summary["stopped"], sum(1 for value in matrix_values if value == "중지"))
        m1.metric("전체 작업", len(valid_paths), f"선택 {len(st.session_state['selected_paths'])}건")
        m2.metric("완료", done_count, "실행 로그 기준")
        m3.metric("실패", failed_count, "실행 로그 기준")
        m4.metric("선택 지역", len(st.session_state["selected_regions"]), "필터 기준")
        m5.metric("중지", stopped_count, "중지 작업 포함")
        m6.metric("결과 파일", len(artifacts), "output 하위 xlsx")


@st.fragment(run_every="2s")
def render_live_right_panel(service, meta_by_path: dict[str, dict[str, Any]], collect_runtime) -> None:
    runtime = collect_runtime(service, meta_by_path)
    artifacts = runtime["artifacts"]
    running = runtime["running"]

    with st.container(border=True):
        st.subheader("3. 선택 작업 상세")
        if st.session_state["selected_paths"]:
            first_path = st.session_state["selected_paths"][0]
            first_meta = meta_by_path.get(first_path, {})
            st.markdown(
                f"""
<div class="kv-grid">
  <div class="kv-key">작업명</div><div class="kv-value">{first_meta.get('name', first_path)} / {st.session_state['selected_period']}</div>
  <div class="kv-key">원천 통계원</div><div class="kv-value">{first_meta.get('source', 'KOSIS')}</div>
  <div class="kv-key">실행 파일</div><div class="kv-value"><code>{first_path}</code></div>
  <div class="kv-key">대상 지역</div><div class="kv-value">{first_meta.get('scope_label', '-')}</div>
  <div class="kv-key">검증 상태</div><div class="kv-value">{status_chip(service.state.status)[0]}</div>
</div>
""",
                unsafe_allow_html=True,
            )
        else:
            st.info("선택된 작업이 없습니다.")

        progress_value = 55 if running else 100 if service.state.status in {"SUCCESS", "FAILED", "CANCELED"} else 0
        st.progress(progress_value, text="작업 진행 상태")
        st.markdown(
            f"<div class='info-box'>선택 항목 {len(st.session_state['selected_paths'])}건과 지역 {len(st.session_state['selected_regions'])}개를 기준으로 실행 상태를 계산하고 있습니다.</div>",
            unsafe_allow_html=True,
        )
        st.divider()
        st.subheader("4. 검증 체크리스트")
        st.dataframe(build_checklist(artifacts, service.state.status), use_container_width=True, hide_index=True)


@st.fragment(run_every="2s")
def render_live_bottom(service, meta_by_path: dict[str, dict[str, Any]], collect_runtime) -> None:
    runtime = collect_runtime(service, meta_by_path)
    artifacts = runtime["artifacts"]

    bottom_left, bottom_right = st.columns([1.8, 1.1], gap="small")
    with bottom_left:
        with st.container(border=True):
            st.subheader("5. 작업 현황")
            matrix_df = build_matrix_table(st.session_state["selected_paths"], st.session_state["selected_regions"], meta_by_path)
            if matrix_df.empty:
                matrix_df = pd.DataFrame(columns=["항목명", "적용 범위", "인천"])
            st.dataframe(matrix_df, use_container_width=True, hide_index=True)

    with bottom_right:
        with st.container(border=True):
            st.subheader("6. 결과 파일")
            result_df = build_result_table(artifacts, meta_by_path)
            if result_df.empty:
                result_df = pd.DataFrame(columns=["파일명", "지역 범위", "생성 시간", "크기"])
            st.dataframe(result_df, use_container_width=True, hide_index=True)
            if artifacts:
                with open(artifacts[0], "rb") as file_obj:
                    st.download_button("최신 파일 다운로드", file_obj.read(), file_name=artifacts[0].name, use_container_width=True, key="download_latest_button")

    with st.container(border=True):
        st.subheader("7. 실행 로그")
        log_query = st.text_input("로그 검색", value="", key="log_search")
        logs = service.state.logs
        if log_query.strip():
            logs = [line for line in logs if log_query.lower() in line.lower()]
        log_df = build_log_table(logs)
        if log_df.empty:
            log_df = pd.DataFrame([{"수준": "-", "메시지": "(로그 없음)"}])
        st.dataframe(log_df, use_container_width=True, hide_index=True, height=260)
