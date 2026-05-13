import os
from typing import Any, Dict, Tuple

import pandas as pd

from runner_core.api.kosis_client import fetch_kosis_df
from runner_core.preprocess.filters import apply_row_filters
from runner_core.views.builders import build_source_views

KOSIS_API_KEY = os.getenv("KOSIS_API_KEY", "").strip()


def run_kosis_sources_job(job: dict) -> Tuple[Any, Any, str]:
    sources = job.get("sources", [])
    if not isinstance(sources, list) or not sources:
        raise RuntimeError("kosis_sources requires non-empty sources list")

    src_raw_frames: Dict[str, pd.DataFrame] = {}
    for src in sources:
        if not isinstance(src, dict):
            raise RuntimeError("Each source must be a dict")
        src_name = str(src.get("name", "")).strip()
        if not src_name:
            raise RuntimeError("Each source needs a non-empty name")
        src_raw_frames[src_name] = fetch_kosis_df(src, KOSIS_API_KEY)

    pivot_views = build_source_views(src_raw_frames, job)
    sheet_name = next(iter(pivot_views), "TABLE_VIEW")

    raw_out: Any = {} if not bool(job.get("include_source_raw", True)) else src_raw_frames
    raw_sheets = job.get("raw_sheets", [])
    if isinstance(raw_sheets, list) and raw_sheets:
        sheet_map: Dict[str, pd.DataFrame] = {}
        for i, spec in enumerate(raw_sheets, start=1):
            if not isinstance(spec, dict):
                continue
            src_name = str(spec.get("source", "")).strip()
            if not src_name or src_name not in src_raw_frames:
                continue
            name = str(spec.get("sheet_name", f"RAW_{i}")).strip() or f"RAW_{i}"
            d = src_raw_frames[src_name].copy()
            d = apply_row_filters(d, spec.get("filters", {}))

            cols = spec.get("columns")
            if isinstance(cols, list) and cols:
                keep = [c for c in cols if c in d.columns]
                if keep:
                    d = d[keep].copy()
            sheet_map[name] = d
        if sheet_map:
            raw_out = sheet_map

    return raw_out, pivot_views if pivot_views else None, sheet_name
