from __future__ import annotations

import streamlit as st

from services.job_runner_service import JobRunnerService


def get_service() -> JobRunnerService:
    if "job_runner_service" not in st.session_state:
        st.session_state.job_runner_service = JobRunnerService()
    return st.session_state.job_runner_service
