from typing import Any

import pandas as pd
from runner_core.periods import available_periods, resolve_period_list


def apply_row_filters(df: pd.DataFrame, filters: Any) -> pd.DataFrame:
    if not isinstance(filters, dict) or not filters:
        return df

    d = df.copy()
    for col, allowed in filters.items():
        if col not in d.columns:
            raise RuntimeError(f"filter column missing: {col}")
        allowed_vals = allowed if isinstance(allowed, list) else [allowed]
        if col == "PRD_DE":
            allowed_vals = resolve_period_list(allowed_vals, available_periods(d[col].astype(str).tolist()))
        else:
            allowed_vals = [str(v) for v in allowed_vals]
        d = d[d[col].astype(str).isin(allowed_vals)]
    return d


def apply_value_maps(df: pd.DataFrame, spec: dict) -> pd.DataFrame:
    d = df.copy()
    replace_values = spec.get("replace_values", {})
    if isinstance(replace_values, dict):
        for col, mapping in replace_values.items():
            if col in d.columns and isinstance(mapping, dict):
                d[col] = d[col].astype(str).replace({str(k): v for k, v in mapping.items()})
    return d
