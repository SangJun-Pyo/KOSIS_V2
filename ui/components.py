from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable, List

import streamlit as st


def inject_app_css() -> None:
    st.markdown(
        """
<style>
:root {
  --bg: #0f172a;
  --panel: #17233b;
  --panel2: #1d2d4a;
  --line: #2f456a;
  --text: #ecf3ff;
  --muted: #a8bddb;
  --primary: #38bdf8;
  --ok: #22c55e;
  --warn: #f59e0b;
  --err: #ef4444;
}

.stApp {
  background: radial-gradient(circle at top left, #1b2d4b 0%, var(--bg) 50%);
}

[data-testid="stHeader"] {
  background: transparent;
}

/* Streamlit 기본 배포/메뉴 노출 숨김 */
[data-testid="stToolbar"] { display: none !important; }
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }

.k-wrap, .k-wrap * {
  color: var(--text);
  word-break: break-word;
  overflow-wrap: anywhere;
  line-height: 1.7;
}

.k-main {
  max-width: 1320px;
  margin: 0 auto;
}

.k-head {
  border: 1px solid var(--line);
  background: linear-gradient(180deg, var(--panel) 0%, #111d33 100%);
  border-radius: 16px;
  padding: 18px 20px;
  min-height: 84px;
  margin-bottom: 14px;
}

.k-title {
  font-size: clamp(1.25rem, 2.2vw, 1.95rem);
  font-weight: 800;
  margin: 0 0 6px 0;
}

.k-sub {
  color: var(--muted);
  font-size: 0.96rem;
  margin: 0;
}

.k-step {
  border: 1px solid var(--line);
  background: linear-gradient(180deg, var(--panel) 0%, var(--panel2) 100%);
  border-radius: 14px;
  padding: 14px 16px;
  min-height: 132px;
}

.k-step-no {
  display: inline-block;
  border-radius: 999px;
  border: 1px solid var(--line);
  padding: 2px 9px;
  font-size: 0.8rem;
  color: var(--primary);
  min-height: 24px;
}

.k-step-title {
  margin: 8px 0 6px 0;
  font-size: 1.05rem;
  font-weight: 700;
}

.k-step-desc {
  margin: 0;
  font-size: 0.9rem;
  color: var(--muted);
}

.k-kpi {
  border: 1px solid var(--line);
  border-radius: 12px;
  background: #122039;
  padding: 12px 14px;
  min-height: 92px;
}

.k-kpi-label {
  color: var(--muted);
  font-size: 0.86rem;
}

.k-kpi-value {
  font-size: 1.45rem;
  font-weight: 800;
  margin-top: 2px;
}

.k-panel {
  border: 1px solid var(--line);
  border-radius: 14px;
  background: rgba(20, 33, 56, 0.85);
  padding: 14px;
  min-height: 170px;
}

.k-panel-title {
  font-size: 1.02rem;
  font-weight: 700;
  margin: 0 0 10px 0;
}

.k-muted {
  color: var(--muted);
  font-size: 0.9rem;
}

.k-log-wrap {
  border: 1px solid var(--line);
  border-radius: 12px;
  padding: 8px;
  background: #0a1324;
}

@media (max-width: 1024px) {
  .k-step { min-height: 120px; }
}

@media (max-width: 768px) {
  .k-head { padding: 14px; }
  .k-title { font-size: 1.25rem; }
}
</style>
        """,
        unsafe_allow_html=True,
    )


def render_header(status: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.markdown(
        f"""
<div class="k-wrap k-main">
  <div class="k-head">
    <h1 class="k-title">KOSIS 데이터 수집 운영 시스템</h1>
    <p class="k-sub">처음 사용하는 분도 쉽게 사용할 수 있도록, 3단계 실행 흐름으로 구성했습니다. · 현재 상태: {status} · {now}</p>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )


def render_steps_guide() -> None:
    cols = st.columns(3, gap="small")
    steps = [
        ("1", "수집 범위 선택", "전체 수집 또는 카테고리별 수집을 선택합니다."),
        ("2", "실행 버튼 클릭", "실행 시작 버튼을 누르면 자동으로 순차 처리됩니다."),
        ("3", "결과 확인 및 다운로드", "완료 후 결과 파일을 바로 내려받을 수 있습니다."),
    ]
    for col, (no, title, desc) in zip(cols, steps):
        with col:
            st.markdown(
                f"""
<div class="k-wrap k-step">
  <span class="k-step-no">단계 {no}</span>
  <div class="k-step-title">{title}</div>
  <p class="k-step-desc">{desc}</p>
</div>
                """,
                unsafe_allow_html=True,
            )


def render_kpi(status: str, group_count: int, job_count: int, file_count: int) -> None:
    cols = st.columns(4, gap="small")
    items = [("현재 상태", status), ("카테고리", str(group_count)), ("선택 작업 수", str(job_count)), ("결과 파일 수", str(file_count))]
    for col, (label, value) in zip(cols, items):
        with col:
            st.markdown(
                f"""
<div class="k-wrap k-kpi">
  <div class="k-kpi-label">{label}</div>
  <div class="k-kpi-value">{value}</div>
</div>
                """,
                unsafe_allow_html=True,
            )


def render_long_data_test() -> None:
    st.markdown("<div class='k-wrap k-panel-title'>긴 문장 표시 점검</div>", unsafe_allow_html=True)
    title = "인천광역시_고용및노동_중장기추세분석_매우긴작업지시명_2026년상반기통합수집검증보고용_최종안_검토본"
    desc = (
        "이 문구는 실제 운영 환경에서 긴 한글 제목, 긴 설명, 긴 URL이 함께 들어왔을 때도 레이아웃이 깨지지 않는지 점검하기 위한 샘플입니다. "
        "참고주소: https://kosis.kr/openapi/very/long/url/path/for/layout-break-word-validation-case"
    )
    st.markdown(
        f"""
<div class="k-wrap k-panel">
  <div><strong>{title}</strong></div>
  <div class="k-muted">{desc}</div>
</div>
        """,
        unsafe_allow_html=True,
    )


def render_artifacts(files: Iterable[Path]) -> None:
    rows = []
    for p in files:
        s = p.stat()
        rows.append(
            {
                "파일명": p.name,
                "수정시각": datetime.fromtimestamp(s.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "크기(KB)": round(s.st_size / 1024, 1),
                "경로": str(p),
            }
        )
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("현재 결과 파일이 없습니다.")


def render_logs(logs: List[str]) -> None:
    st.markdown("<div class='k-wrap k-log-wrap'>", unsafe_allow_html=True)
    st.code("\n".join(logs[-200:]) if logs else "(로그 없음)", language="text")
    st.markdown("</div>", unsafe_allow_html=True)
