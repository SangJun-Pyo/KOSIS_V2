from typing import Any, Tuple

import pandas as pd

from runner_core.api.data_go_client import fetch_data_go_df
from runner_core.views.builders import build_table_views


def run_data_go_kr_job(job: dict) -> Tuple[pd.DataFrame, Any, str]:
    df = fetch_data_go_df(job)
    pivot_views = build_table_views(df, job)
    first_sheet = next(iter(pivot_views), "TABLE_VIEW")
    return df, pivot_views if pivot_views else None, first_sheet
