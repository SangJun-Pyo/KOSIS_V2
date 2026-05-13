import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from runner_core.api.kosis_client import fetch_kosis_df
from runner_core.views.builders import build_table_views

KOSIS_API_KEY = os.getenv("KOSIS_API_KEY", "").strip()


def run_kosis_multi_job(job: dict) -> Tuple[Any, Any, str]:
    sources = job.get("sources", [])
    if not isinstance(sources, list) or not sources:
        raise RuntimeError("kosis_multi requires non-empty sources list")

    merge_keys = job.get("merge_keys", ["C1", "C1_NM", "PRD_DE"])
    if not isinstance(merge_keys, list) or not merge_keys:
        raise RuntimeError("kosis_multi.merge_keys must be a non-empty list")

    metrics = job.get("metrics", [])
    if not isinstance(metrics, list) or not metrics:
        raise RuntimeError("kosis_multi.metrics must be a non-empty list")

    src_raw_frames: Dict[str, pd.DataFrame] = {}
    for src in sources:
        if not isinstance(src, dict):
            raise RuntimeError("Each source must be a dict")
        src_name = str(src.get("name", "")).strip()
        if not src_name:
            raise RuntimeError("Each source needs a non-empty name")
        df = fetch_kosis_df(src, KOSIS_API_KEY)
        need = [c for c in merge_keys if c not in df.columns]
        if need:
            raise RuntimeError(f"source '{src_name}' missing merge keys: {need}")
        if "DT" not in df.columns:
            raise RuntimeError(f"source '{src_name}' missing DT column")
        src_raw_frames[src_name] = df.copy()

    merged: Optional[pd.DataFrame] = None
    for metric in metrics:
        if not isinstance(metric, dict):
            raise RuntimeError("Each metric must be a dict")
        mid = str(metric.get("id", "")).strip()
        if not mid:
            raise RuntimeError("Each metric needs id")
        src_name = str(metric.get("source", "")).strip()
        if not src_name:
            continue
        if src_name not in src_raw_frames:
            raise RuntimeError(f"Unknown source in metrics: {src_name}")

        src_df = src_raw_frames[src_name].copy()
        src_filter = metric.get("source_filter", {})
        if src_filter:
            if not isinstance(src_filter, dict):
                raise RuntimeError(f"metric '{mid}' source_filter must be a dict")
            for col, val in src_filter.items():
                if col not in src_df.columns:
                    raise RuntimeError(f"metric '{mid}' source_filter column missing: {col}")
                src_df = src_df[src_df[col].astype(str) == str(val)]

        pick = list(dict.fromkeys(merge_keys + ["DT"]))
        d = src_df[pick].copy()
        d["DT"] = pd.to_numeric(d["DT"], errors="coerce")

        agg = str(metric.get("agg", "first")).strip().lower()
        if agg == "sum":
            d = d.groupby(merge_keys, as_index=False, dropna=False, observed=True)["DT"].sum()
        else:
            d = d.groupby(merge_keys, as_index=False, dropna=False, observed=True)["DT"].first()

        d = d.rename(columns={"DT": mid})
        if merged is None:
            merged = d
        else:
            merged = merged.merge(d, on=merge_keys, how="outer")

    if merged is None:
        raise RuntimeError("No source-based metrics found")

    for metric in metrics:
        mid = str(metric.get("id", "")).strip()
        formula = metric.get("formula")
        if not mid or not formula:
            continue
        try:
            merged[mid] = merged.eval(str(formula))
        except Exception as e:
            raise RuntimeError(f"Failed to evaluate formula for metric '{mid}': {e}") from e
        if metric.get("round") is not None:
            merged[mid] = merged[mid].round(int(metric["round"]))

    for metric in metrics:
        mid = str(metric.get("id", "")).strip()
        if mid and mid in merged.columns:
            merged[mid] = pd.to_numeric(merged[mid], errors="coerce")

    long_parts: List[pd.DataFrame] = []
    for metric in metrics:
        mid = str(metric.get("id", "")).strip()
        if not mid or mid not in merged.columns:
            continue
        label = str(metric.get("label", mid))
        part = merged[merge_keys + [mid]].copy()
        part["METRIC"] = label
        part = part.rename(columns={mid: "VALUE"})
        long_parts.append(part)

    if not long_parts:
        raise RuntimeError("No metric columns available after merge/formula")

    raw_df = pd.concat(long_parts, ignore_index=True)
    pivot_views = build_table_views(raw_df, job)
    sheet_name = next(iter(pivot_views), "TABLE_VIEW")

    raw_out: Any = raw_df
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

            flt = spec.get("filters", {})
            if isinstance(flt, dict):
                for col, val in flt.items():
                    if col in d.columns:
                        vals = val if isinstance(val, list) else [val]
                        d = d[d[col].astype(str).isin([str(v) for v in vals])]

            cols = spec.get("columns")
            if isinstance(cols, list) and cols:
                keep = [c for c in cols if c in d.columns]
                if keep:
                    d = d[keep].copy()

            sheet_map[name] = d

        if sheet_map:
            raw_out = sheet_map

    return raw_out, pivot_views if pivot_views else None, sheet_name
