import os
from typing import Any, Tuple

import pandas as pd

from runner_core.api.kosis_client import fetch_kosis_df
from runner_core.views.builders import build_table_views

KOSIS_API_KEY = os.getenv("KOSIS_API_KEY", "").strip()


def run_kosis_job(job: dict) -> Tuple[pd.DataFrame, Any, str]:
    df = fetch_kosis_df(job, KOSIS_API_KEY)
    pivot_views = build_table_views(df, job)
    first_sheet = next(iter(pivot_views), "TABLE_VIEW")
    return df, pivot_views if pivot_views else None, first_sheet
