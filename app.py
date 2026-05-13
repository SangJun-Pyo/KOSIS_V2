from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from services.job_runner_service import JobRunnerService


st.set_page_config(
    page_title="KOSIS 통계지표 자동 생성 시스템",
    page_icon="📊",
    layout="wide",
)

st.markdown(
    """
<style>
[data-testid="stHeader"] { background: transparent; }
[data-testid="stToolbar"] { display: none !important; }
#MainMenu, footer { visibility: hidden; }
.block-container { max-width: 1680px; padding-top: 14px; padding-bottom: 28px; }
html, body, [class*="css"] { color: #1f2937; }
.muted { color: #6b7280; font-size: 14px; line-height: 1.7; }
.status-chip {
  display: inline-flex; align-items: center; min-height: 28px; padding: 4px 12px;
  border-radius: 999px; border: 1px solid #dbeafe; background: #eff6ff;
  color: #1d4ed8; font-size: 13px; font-weight: 700;
}
.status-chip.ok { background: #ecfdf3; border-color: #bbf7d0; color: #15803d; }
.status-chip.warn { background: #fff7ed; border-color: #fed7aa; color: #c2410c; }
.status-chip.fail { background: #fef2f2; border-color: #fecaca; color: #b91c1c; }
.status-chip.gray { background: #f8fafc; border-color: #e5e7eb; color: #475569; }
.info-box {
  background: #f8fbff; border: 1px solid #dbeafe; border-radius: 12px;
  padding: 14px 16px; color: #1d4ed8; line-height: 1.7;
  word-break: break-word; overflow-wrap: anywhere;
}
.kv-grid { display: grid; grid-template-columns: 140px 1fr; gap: 10px 14px; align-items: start; }
.kv-key { color: #475569; font-size: 13px; font-weight: 700; }
.kv-value { color: #0f172a; font-size: 14px; line-height: 1.7; word-break: break-word; overflow-wrap: anywhere; }
</style>
""",
    unsafe_allow_html=True,
)

DEFAULT_SCOPE_REGION = "인천"
REGIONS = ["서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종", "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"]
REGION_KEYWORDS = {
    "서울": ["seoul", "서울"], "부산": ["busan", "부산"], "대구": ["daegu", "대구"], "인천": ["incheon", "인천"],
    "광주": ["gwangju", "광주"], "대전": ["daejeon", "대전"], "울산": ["ulsan", "울산"], "세종": ["sejong", "세종"],
    "경기": ["gyeonggi", "경기"], "강원": ["gangwon", "강원"], "충북": ["chungbuk", "충북"], "충남": ["chungnam", "충남"],
    "전북": ["jeonbuk", "전북"], "전남": ["jeonnam", "전남"], "경북": ["gyeongbuk", "경북"], "경남": ["gyeongnam", "경남"], "제주": ["jeju", "제주"],
}


def get_service() -> JobRunnerService:
    if "runner_service" not in st.session_state:
        st.session_state.runner_service = JobRunnerService()
    return st.session_state.runner_service


def init_state() -> None:
    st.session_state.setdefault("selected_group", "")
    st.session_state.setdefault("selected_paths", [])
    st.session_state.setdefault("selected_region", DEFAULT_SCOPE_REGION)
    st.session_state.setdefault("selected_regions", [DEFAULT_SCOPE_REGION])
    st.session_state.setdefault("selected_period", "2026년 상반기")
    st.session_state.setdefault("last_failed_paths", [])
    st.session_state.setdefault("matrix_state", {})
    st.session_state.setdefault("last_terminal_at", None)
    st.session_state.setdefault("run_started_at", None)


def infer_regions(text: str) -> list[str]:
    lowered = text.lower()
    matched: list[str] = []
    for region, keywords in REGION_KEYWORDS.items():
        if any(keyword.lower() in lowered for keyword in keywords):
            matched.append(region)
    return matched


def prettify_name(raw_name: str) -> str:
    text = raw_name.replace("_", " ").replace("-", " ").strip()
    return " ".join(text.split()) or raw_name


def parse_job_meta(root_dir: Path, rel_path: str) -> dict:
    path = root_dir / rel_path
    stem = path.stem
    row = {
        "path": rel_path,
        "name": prettify_name(stem),
        "source": "KOSIS",
        "provider": "kosis",
        "regions": [DEFAULT_SCOPE_REGION],
        "status": "대기",
        "output_prefix": stem,
    }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return row

    raw_name = str(payload.get("job_name") or stem)
    row["name"] = prettify_name(stem) if "?" in raw_name else raw_name
    row["source"] = str(payload.get("source_name") or payload.get("source") or "KOSIS")
    row["provider"] = str(payload.get("provider") or "kosis")
    row["output_prefix"] = str(payload.get("output_prefix") or stem)
    region_text = " ".join([rel_path, raw_name, row["source"], row["output_prefix"], json.dumps(payload, ensure_ascii=False)])
    matched_regions = infer_regions(region_text)
    row["regions"] = matched_regions or [DEFAULT_SCOPE_REGION]
    return row


def filter_rows_by_region(rows: list[dict], selected_region: str) -> list[dict]:
    return [row for row in rows if not row.get("regions") or selected_region in row.get("regions", [])]


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


def build_checklist(service: JobRunnerService, artifacts: list[Path]) -> pd.DataFrame:
    api_key_exists = bool(os.getenv("KOSIS_API_KEY", "").strip())
    rows = [
        ["선택 항목 존재 여부", "OK" if st.session_state["selected_paths"] else "NG", "정상" if st.session_state["selected_paths"] else "확인 필요"],
        ["지역 선택 여부", f"{len(st.session_state['selected_regions'])}개", "정상" if st.session_state["selected_regions"] else "확인 필요"],
        ["KOSIS_API_KEY 설정 여부", "OK" if api_key_exists else "NG", "정상" if api_key_exists else "오류"],
        ["runner.py 존재 여부", "OK" if Path("runner.py").exists() else "NG", "정상" if Path("runner.py").exists() else "오류"],
        ["jobs 폴더 존재 여부", "OK" if Path("jobs").exists() else "NG", "정상" if Path("jobs").exists() else "오류"],
        ["실행 상태", status_chip(service.state.status)[0], "정상" if service.state.status in {"PENDING", "RUNNING", "SUCCESS"} else "점검 필요"],
        ["결과 파일 생성 여부", "OK" if artifacts else "-", "정상" if artifacts else "-"],
    ]
    return pd.DataFrame(rows, columns=["항목", "값", "상태"])


def build_result_table(artifacts: list[Path]) -> pd.DataFrame:
    rows = []
    for file_path in artifacts[:30]:
        stat = file_path.stat()
        rows.append({
            "파일명": file_path.name,
            "생성 시간": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "크기": f"{stat.st_size / 1024:.1f} KB",
        })
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


def build_matrix_table(selected_paths: list[str], selected_regions: list[str], meta_by_path: dict[str, dict]) -> pd.DataFrame:
    rows = []
    for rel_path in selected_paths[:10]:
        meta = meta_by_path.get(rel_path, {})
        row = {"항목명": meta.get("name", rel_path)}
        job_regions = meta.get("regions", [])
        for region in selected_regions[:6]:
            row[region] = "대상 아님" if job_regions and region not in job_regions else st.session_state["matrix_state"].get(f"{rel_path}|{region}", "대기")
        rows.append(row)
    return pd.DataFrame(rows)


def collect_runtime(service: JobRunnerService, meta_by_path: dict[str, dict]) -> dict:
    service.drain_logs()
    artifacts = service.list_artifacts()
    refresh_matrix_from_artifacts(artifacts, meta_by_path, service)
    sync_terminal_state(service)
    return {
        "artifacts": artifacts,
        "summary": service.get_status_summary(),
        "running": service.is_running(),
        "last_run_time": datetime.fromtimestamp(artifacts[0].stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if artifacts else "-",
        "api_key_exists": bool(os.getenv("KOSIS_API_KEY", "").strip()),
    }


init_state()
service = get_service()

groups = service.list_job_groups()
if groups and st.session_state["selected_group"] not in groups:
    st.session_state["selected_group"] = groups[0]

selected_group = st.session_state["selected_group"]
all_group_jobs = service.list_job_files([selected_group]) if selected_group else []
all_job_rows = [parse_job_meta(Path.cwd(), rel_path) for rel_path in all_group_jobs]

if st.session_state["selected_region"] not in REGIONS:
    st.session_state["selected_region"] = DEFAULT_SCOPE_REGION
st.session_state["selected_regions"] = [st.session_state["selected_region"]]

filtered_job_rows = filter_rows_by_region(all_job_rows, st.session_state["selected_region"])
meta_by_path = {row["path"]: row for row in all_job_rows}
valid_paths = [row["path"] for row in filtered_job_rows]
st.session_state["selected_paths"] = [path for path in st.session_state["selected_paths"] if path in valid_paths]
if not st.session_state["selected_paths"] and valid_paths:
    st.session_state["selected_paths"] = valid_paths[: min(4, len(valid_paths))]


def start_selected_run() -> None:
    targets = st.session_state["selected_paths"]
    st.session_state["run_started_at"] = time.time()
    st.session_state["matrix_state"] = build_matrix(targets, st.session_state["selected_regions"], meta_by_path, "실행 중")
    result = service.start_run(targets)
    st.session_state["flash"] = ("success" if result["ok"] == "true" else "error", result["message"])
    st.rerun()


def rerun_failed() -> None:
    failed_targets = st.session_state["last_failed_paths"]
    result = service.start_run(failed_targets) if failed_targets else {"ok": "false", "message": "최근 실패 작업이 없습니다."}
    if failed_targets:
        st.session_state["run_started_at"] = time.time()
        st.session_state["matrix_state"] = build_matrix(failed_targets, st.session_state["selected_regions"], meta_by_path, "실행 중")
    st.session_state["flash"] = ("success" if result["ok"] == "true" else "error", result["message"])
    st.rerun()


def stop_run() -> None:
    result = service.stop_run()
    st.session_state["flash"] = ("success" if result["ok"] == "true" else "error", result["message"])
    st.rerun()


@st.fragment(run_every="2s")
def render_live_top(meta_by_path: dict[str, dict], valid_paths: list[str]) -> None:
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
                st.session_state["selected_period"] = st.selectbox("기준 기간", ["2026년 상반기", "2026년 하반기"], index=0 if st.session_state["selected_period"] == "2026년 상반기" else 1, label_visibility="collapsed", key="selected_period_widget")
            with c2:
                st.caption("시스템 상태")
                show_chip(service.state.status)
            with c3:
                st.caption("최근 실행")
                st.write(last_run_time)
            with c4:
                if st.button("선택 항목 처리 시작", type="primary", use_container_width=True, disabled=running, key="start_run_button"):
                    start_selected_run()
            with c5:
                if st.button("실패 작업 다시 실행", use_container_width=True, disabled=running, key="rerun_failed_button"):
                    rerun_failed()
            with c6:
                if st.button("실행 중지", use_container_width=True, disabled=not running, key="stop_run_button"):
                    stop_run()

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
        st.error("KOSIS_API_KEY가 설정되지 않아 실제 수집 실행이 실패합니다.")
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
def render_live_right_panel(meta_by_path: dict[str, dict]) -> None:
    runtime = collect_runtime(service, meta_by_path)
    artifacts = runtime["artifacts"]
    running = runtime["running"]

    with st.container(border=True):
        st.subheader("3. 선택 작업 상세")
        if st.session_state["selected_paths"]:
            first_path = st.session_state["selected_paths"][0]
            first_meta = meta_by_path.get(first_path, {})
            region_text = ", ".join(first_meta.get("regions", [])) if first_meta.get("regions") else "전국/공통"
            st.markdown(
                f"""
<div class="kv-grid">
  <div class="kv-key">작업명</div><div class="kv-value">{first_meta.get('name', first_path)} / {st.session_state['selected_period']}</div>
  <div class="kv-key">원천 통계원</div><div class="kv-value">{first_meta.get('source', 'KOSIS')}</div>
  <div class="kv-key">실행 파일</div><div class="kv-value"><code>{first_path}</code></div>
  <div class="kv-key">대상 지역</div><div class="kv-value">{region_text}</div>
  <div class="kv-key">검증 상태</div><div class="kv-value">{status_chip(service.state.status)[0]}</div>
</div>
""",
                unsafe_allow_html=True,
            )
        else:
            st.info("선택된 작업이 없습니다.")

        progress_value = 55 if running else 100 if service.state.status in {"SUCCESS", "FAILED", "CANCELED"} else 0
        st.progress(progress_value, text="작업 진행 상태")
        st.markdown(f"<div class='info-box'>선택 항목 {len(st.session_state['selected_paths'])}건과 지역 {len(st.session_state['selected_regions'])}개를 기준으로 실행 준비 상태를 계산하고 있습니다.</div>", unsafe_allow_html=True)
        st.divider()
        st.subheader("4. 검증 체크리스트")
        st.dataframe(build_checklist(service, artifacts), use_container_width=True, hide_index=True)


@st.fragment(run_every="2s")
def render_live_bottom(meta_by_path: dict[str, dict]) -> None:
    runtime = collect_runtime(service, meta_by_path)
    artifacts = runtime["artifacts"]

    bottom_left, bottom_right = st.columns([1.8, 1.1], gap="small")
    with bottom_left:
        with st.container(border=True):
            st.subheader("5. 작업 현황")
            matrix_df = build_matrix_table(st.session_state["selected_paths"], st.session_state["selected_regions"], meta_by_path)
            if matrix_df.empty:
                matrix_df = pd.DataFrame(columns=["항목명", "서울", "부산", "대구", "인천"])
            st.dataframe(matrix_df, use_container_width=True, hide_index=True)

    with bottom_right:
        with st.container(border=True):
            st.subheader("6. 결과 파일")
            result_df = build_result_table(artifacts)
            if result_df.empty:
                result_df = pd.DataFrame(columns=["파일명", "생성 시간", "크기"])
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


render_live_top(meta_by_path, valid_paths)

left_col, center_col, right_col = st.columns([0.95, 1.3, 1.05], gap="small")

with left_col:
    with st.container(border=True):
        st.subheader("1. 지역 선택")
        st.session_state["selected_region"] = st.selectbox("지역 선택", REGIONS, index=REGIONS.index(st.session_state["selected_region"]), key="selected_region_widget")
        st.session_state["selected_regions"] = [st.session_state["selected_region"]]
        matched_count = sum(1 for row in all_job_rows if st.session_state["selected_region"] in row.get("regions", []))
        st.markdown(f"<div class='info-box'>선택 지역: <strong>{st.session_state['selected_region']}</strong><br>현재 이 지역으로 분류된 작업은 <strong>{matched_count}건</strong>입니다.</div>", unsafe_allow_html=True)
        region_rows = [{"지역": region, "매칭 작업 수": sum(1 for row in all_job_rows if region in row.get('regions', []))} for region in REGIONS]
        st.dataframe(pd.DataFrame(region_rows), use_container_width=True, hide_index=True, height=500)

with center_col:
    with st.container(border=True):
        st.subheader("2. 파트 및 항목 선택")
        if groups:
            st.session_state["selected_group"] = st.selectbox("파트", groups, index=groups.index(selected_group) if selected_group in groups else 0, key="selected_group_widget")
        else:
            st.warning("사용 가능한 jobs 그룹이 없습니다.")
        search = st.text_input("항목명 검색", value="", key="job_search_widget")
        filtered_rows = [row for row in filtered_job_rows if search.lower() in row["name"].lower()]
        visible_paths = [row["path"] for row in filtered_rows]
        st.session_state["selected_paths"] = st.multiselect("항목 선택", options=visible_paths, default=[path for path in st.session_state["selected_paths"] if path in visible_paths], format_func=lambda path: meta_by_path.get(path, {"name": path})["name"], key="selected_paths_widget")
        table_rows = [{"항목명": row["name"], "원천 데이터": row["source"], "대상 지역": ", ".join(row["regions"]) if row["regions"] else "전국/공통", "최근 상태": "선택" if row["path"] in st.session_state["selected_paths"] else "대기"} for row in filtered_rows[:30]]
        table_df = pd.DataFrame(table_rows) if table_rows else pd.DataFrame(columns=["항목명", "원천 데이터", "대상 지역", "최근 상태"])
        st.dataframe(table_df, use_container_width=True, hide_index=True)

with right_col:
    render_live_right_panel(meta_by_path)

render_live_bottom(meta_by_path)
