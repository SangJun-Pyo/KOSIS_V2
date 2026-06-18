from decimal import Decimal, ROUND_HALF_UP
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from runner_core.preprocess.filters import apply_row_filters
from runner_core.periods import available_periods, resolve_cagr_label, resolve_period_list, resolve_period_value, period_template


def _format_rounded_value(val: Any, digits: int | None) -> Any:
    num = pd.to_numeric(val, errors="coerce")
    if digits is None or pd.isna(num):
        return val
    quant = Decimal("1") if digits == 0 else Decimal("1").scaleb(-digits)
    rounded = Decimal(str(float(num))).quantize(quant, rounding=ROUND_HALF_UP)
    if digits == 0:
        return int(rounded)
    return float(rounded)


def _format_period_label(period: str) -> str:
    text = str(period).strip()
    if re_match := re.match(r"^(\d{4})$", text):
        return f"{re_match.group(1)}년"
    if re_match := re.match(r"^(\d{4})\.([1-4])/4$", text):
        return f"{re_match.group(1)}년 {re_match.group(2)}분기"
    return text


def _should_include_calc(
    key: str,
    include_keys: list[str] | None = None,
    exclude_keys: list[str] | None = None,
) -> bool:
    key = str(key)
    if include_keys:
        return key in {str(x) for x in include_keys}
    if exclude_keys:
        return key not in {str(x) for x in exclude_keys}
    return True


def make_metric_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["C1_NM", "ITM_ID", "ITM_NM", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"summary pivot columns missing: {missing}")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    if not years:
        raise RuntimeError("summary pivot requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()

    item_ids = pivot_cfg.get("item_ids", [])
    if item_ids:
        d = d[d["ITM_ID"].astype(str).isin([str(x) for x in item_ids])].copy()

    item_order = [str(x) for x in item_ids] if item_ids else list(dict.fromkeys(d["ITM_ID"].astype(str)))
    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(d["C1_NM"].astype(str)))

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    pv = d.pivot_table(
        index=["ITM_ID", "ITM_NM", "C1_NM"],
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    ).reset_index()

    for y in years:
        if y not in pv.columns:
            pv[y] = pd.NA

    cagr_label = pivot_cfg.get("cagr_label")
    start_year = years[0]
    end_year = years[-1]
    cagr_label = resolve_cagr_label(cagr_label, years)

    periods = max(int(end_year) - int(start_year), 1)
    share_year = resolve_period_value(pivot_cfg.get("share_year", end_year), available)
    share_item_ids = {str(x) for x in pivot_cfg.get("share_item_ids", [])}
    include_share = bool(pivot_cfg.get("include_share", True))
    if include_share and not share_item_ids:
        share_item_ids = {str(x) for x in pv["ITM_ID"].astype(str) if "(명)" in str(pv.loc[pv["ITM_ID"].astype(str) == str(x), "ITM_NM"].iloc[0])}
    annual_change_label = str(pivot_cfg.get("annual_change_label", "")).strip()
    annual_change_item_ids = {str(x) for x in pivot_cfg.get("annual_change_item_ids", [])}

    rows: List[Dict[str, Any]] = []
    for item_id in item_order:
        block = pv[pv["ITM_ID"].astype(str) == item_id].copy()
        if block.empty:
            continue

        item_name = str(block["ITM_NM"].iloc[0])
        block["__region_order"] = block["C1_NM"].astype(str).map({name: i for i, name in enumerate(region_order)})
        block["__region_order"] = block["__region_order"].fillna(9999)
        block = block.sort_values("__region_order", kind="stable")

        national_series = block[block["C1_NM"].astype(str) == "전국"]
        national_val = None
        if not national_series.empty and share_year in national_series.columns:
            national_val = pd.to_numeric(national_series.iloc[0][share_year], errors="coerce")

        first_row = True
        for _, rec in block.iterrows():
            row: Dict[str, Any] = {
                "구분": item_name if first_row else "",
                "지역": "계" if str(rec["C1_NM"]) == "전국" else str(rec["C1_NM"]),
            }
            for y in years:
                row[f"{y}년"] = rec.get(y)

            start_val = pd.to_numeric(rec.get(start_year), errors="coerce")
            end_val = pd.to_numeric(rec.get(end_year), errors="coerce")
            if (
                pd.notna(start_val)
                and pd.notna(end_val)
                and start_val not in (0, 0.0)
                and start_val > 0
                and end_val > 0
            ):
                row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
            else:
                row[cagr_label] = pd.NA

            if include_share and str(item_id) in share_item_ids and pd.notna(national_val) and national_val not in (0, 0.0):
                row["비중"] = round((pd.to_numeric(rec.get(share_year), errors="coerce") / national_val) * 100, 1)
            else:
                row["비중"] = pd.NA

            if annual_change_label:
                if str(item_id) in annual_change_item_ids and pd.notna(start_val) and pd.notna(end_val):
                    row[annual_change_label] = round((end_val - start_val) / periods, 2)
                else:
                    row[annual_change_label] = pd.NA

            rows.append(row)
            first_row = False

    cols = ["구분", "지역"] + [f"{y}년" for y in years]
    if include_share:
        cols.append("비중")
    if annual_change_label:
        cols.append(annual_change_label)
    cols.append(cagr_label)
    return pd.DataFrame(rows, columns=cols)

def make_metric_block_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["ITM_ID", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"metric block summary columns missing: {missing}")

    row_col = str(pivot_cfg.get("row_col", "C2_NM"))
    if row_col not in df.columns:
        raise RuntimeError(f"metric block summary row column missing: {row_col}")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    if not years:
        raise RuntimeError("metric block summary requires non-empty years")

    item_ids = [str(x) for x in pivot_cfg.get("item_ids", [])]
    if not item_ids:
        raise RuntimeError("metric block summary requires item_ids")

    item_labels = {str(k): str(v) for k, v in pivot_cfg.get("item_labels", {}).items()}
    row_order = [str(x) for x in pivot_cfg.get("row_order", [])]
    cagr_label = resolve_cagr_label(pivot_cfg.get("cagr_label"), years)
    periods = max(int(years[-1]) - int(years[0]), 1)

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()
    d = d[d["ITM_ID"].astype(str).isin(item_ids)].copy()
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")

    pv = d.pivot_table(
        index=row_col,
        columns=["ITM_ID", "PRD_DE"],
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )

    if not row_order:
        row_order = list(dict.fromkeys(d[row_col].astype(str)))

    tuples = []
    for item_id in item_ids:
        top = item_labels.get(item_id, item_id)
        for year in years:
            tuples.append((top, f"{year}년"))
        tuples.append((top, cagr_label))

    out = pd.DataFrame(index=row_order, columns=pd.MultiIndex.from_tuples(tuples), dtype="object")
    for row_name in row_order:
        for item_id in item_ids:
            top = item_labels.get(item_id, item_id)
            for year in years:
                out.loc[row_name, (top, f"{year}년")] = pv.loc[row_name, (item_id, year)] if row_name in pv.index and (item_id, year) in pv.columns else pd.NA
            start_val = pd.to_numeric(out.loc[row_name, (top, f"{years[0]}년")], errors="coerce")
            end_val = pd.to_numeric(out.loc[row_name, (top, f"{years[-1]}년")], errors="coerce")
            if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0:
                out.loc[row_name, (top, cagr_label)] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
            else:
                out.loc[row_name, (top, cagr_label)] = pd.NA

    out.index.name = str(pivot_cfg.get("row_label", "구분"))
    return out

def make_paired_metric_timeseries_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"paired metric timeseries columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM")).strip()
    if region_col not in df.columns:
        raise RuntimeError(f"paired metric timeseries region column missing: {region_col}")

    metrics = pivot_cfg.get("metrics", [])
    if not isinstance(metrics, list) or not metrics:
        raise RuntimeError("paired metric timeseries requires non-empty metrics")

    area_label = str(pivot_cfg.get("area_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(df[region_col].astype(str)))

    result = pd.DataFrame(index=pd.Index(region_order, name=area_label))
    work = df.copy()
    work["PRD_DE"] = work["PRD_DE"].astype(str)
    work["DT"] = pd.to_numeric(work["DT"], errors="coerce")

    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        label = str(metric.get("label", "")).strip()
        years = resolve_period_list([str(y) for y in metric.get("years", [])], available_periods(work["PRD_DE"].astype(str).tolist()))
        if not label or not years:
            continue

        cagr_label = resolve_cagr_label(metric.get("cagr_label"), years)
        periods = max(int(str(years[-1])[:4]) - int(str(years[0])[:4]), 1)
        block = apply_row_filters(work, metric.get("filters", {}))
        block = block[block["PRD_DE"].isin(years)].copy()

        pv = block.pivot_table(
            index=region_col,
            columns="PRD_DE",
            values="DT",
            aggfunc="first",
            sort=False,
            observed=True,
        )
        for year in years:
            if year not in pv.columns:
                pv[year] = pd.NA
        pv = pv.reindex(region_order)

        for year in years:
            result[(label, f"{year}년")] = pv[year]

        start_vals = pd.to_numeric(pv[years[0]], errors="coerce")
        end_vals = pd.to_numeric(pv[years[-1]], errors="coerce")
        cagr_vals = pd.Series(pd.NA, index=pv.index, dtype="object")
        mask = start_vals.notna() & end_vals.notna() & (start_vals > 0) & (end_vals > 0)
        cagr_vals.loc[mask] = ((((end_vals.loc[mask] / start_vals.loc[mask]) ** (1 / periods)) - 1) * 100).round(1)
        result[(label, cagr_label)] = cagr_vals

    result.index = [national_alias if str(x) == national_name else str(x) for x in result.index]
    result.columns = pd.MultiIndex.from_tuples(result.columns)
    return result

def make_paired_metric_latest_compare_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"paired metric latest compare columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM")).strip()
    if region_col not in df.columns:
        raise RuntimeError(f"paired metric latest compare region column missing: {region_col}")

    metrics = pivot_cfg.get("metrics", [])
    if not isinstance(metrics, list) or not metrics:
        raise RuntimeError("paired metric latest compare requires non-empty metrics")

    area_label = str(pivot_cfg.get("area_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(df[region_col].astype(str)))

    result = pd.DataFrame(index=pd.Index(region_order, name=area_label))
    work = df.copy()
    work["PRD_DE"] = work["PRD_DE"].astype(str)
    work["DT"] = pd.to_numeric(work["DT"], errors="coerce")

    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        label = str(metric.get("label", "")).strip()
        years = resolve_period_list([str(y) for y in metric.get("years", [])], available_periods(work["PRD_DE"].astype(str).tolist()))
        if not label or len(years) != 2:
            continue

        change_label = str(metric.get("change_label", "전년대비"))
        pct_label = str(metric.get("pct_change_label", "증감률"))
        include_pct = bool(metric.get("include_pct_change", False))

        block = apply_row_filters(work, metric.get("filters", {}))
        block = block[block["PRD_DE"].isin(years)].copy()
        pv = block.pivot_table(
            index=region_col,
            columns="PRD_DE",
            values="DT",
            aggfunc="first",
            sort=False,
            observed=True,
        )
        for year in years:
            if year not in pv.columns:
                pv[year] = pd.NA
        pv = pv.reindex(region_order)

        start_vals = pd.to_numeric(pv[years[0]], errors="coerce")
        end_vals = pd.to_numeric(pv[years[1]], errors="coerce")
        result[(label, f"{years[0]}년")] = start_vals
        result[(label, f"{years[1]}년")] = end_vals
        result[(label, change_label)] = (end_vals - start_vals).round(1)

        if include_pct:
            pct_vals = pd.Series(pd.NA, index=pv.index, dtype="object")
            mask = start_vals.notna() & end_vals.notna() & (start_vals != 0)
            pct_vals.loc[mask] = (((end_vals.loc[mask] / start_vals.loc[mask]) - 1) * 100).round(1)
            result[(label, pct_label)] = pct_vals

    result.index = [national_alias if str(x) == national_name else str(x) for x in result.index]
    result.columns = pd.MultiIndex.from_tuples(result.columns)
    return result


def make_multi_metric_region_compare_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"multi_metric_region_compare columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM")).strip()
    if not region_col or region_col not in df.columns:
        raise RuntimeError("multi_metric_region_compare requires valid region_col")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    if len(years) != 2:
        raise RuntimeError("multi_metric_region_compare requires exactly two years")

    metrics = pivot_cfg.get("metrics", [])
    if not isinstance(metrics, list) or not metrics:
        raise RuntimeError("multi_metric_region_compare requires non-empty metrics")

    work = df.copy()
    work["PRD_DE"] = work["PRD_DE"].astype(str)
    work = work[work["PRD_DE"].isin(years)].copy()

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in work.columns:
            allowed = vals if isinstance(vals, list) else [vals]
            work = work[work[col].astype(str).isin([str(v) for v in allowed])].copy()

    work["DT"] = pd.to_numeric(work["DT"], errors="coerce")
    work[region_col] = work[region_col].astype(str)

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(work[region_col].astype(str)))

    year_label_map = {str(k): str(v) for k, v in pivot_cfg.get("year_label_map", {}).items()}
    row_label = str(pivot_cfg.get("row_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))
    row_alias_map = {str(k): str(v) for k, v in pivot_cfg.get("row_alias_map", {}).items()}

    metric_frames: Dict[str, pd.DataFrame] = {}
    metric_specs: List[dict] = []
    for idx, metric in enumerate(metrics, start=1):
        if not isinstance(metric, dict):
            continue
        label = str(metric.get("label", "")).strip() or f"지표{idx}"
        block = apply_row_filters(work, metric.get("filters", {}))
        pv = block.pivot_table(
            index=region_col,
            columns="PRD_DE",
            values="DT",
            aggfunc="first",
            sort=False,
            observed=True,
        )
        for year in years:
            if year not in pv.columns:
                pv[year] = pd.NA
        metric_frames[label] = pv[years]
        metric_specs.append(metric)

    row_entries: List[tuple[str, Dict[str, pd.Series]]] = []
    for region in region_order:
        region_values = {
            str(metric.get("label", "")).strip() or f"지표{idx + 1}": metric_frames[
                str(metric.get("label", "")).strip() or f"지표{idx + 1}"
            ].loc[region]
            if region in metric_frames[str(metric.get("label", "")).strip() or f"지표{idx + 1}"].index
            else pd.Series(index=years, dtype="float64")
            for idx, metric in enumerate(metric_specs)
        }
        row_entries.append((region, region_values))

    subtotals = pivot_cfg.get("subtotals", [])
    if not subtotals and pivot_cfg.get("subtotal"):
        subtotals = [pivot_cfg["subtotal"]]
    for subtotal in subtotals:
        if not isinstance(subtotal, dict):
            continue
        members = [str(x) for x in subtotal.get("members", [])]
        agg = str(subtotal.get("agg", "mean")).strip().lower()
        label = str(subtotal.get("label", "소계"))
        subtotal_values: Dict[str, pd.Series] = {}
        for metric in metric_specs:
            metric_label = str(metric.get("label", "")).strip()
            pv = metric_frames[metric_label]
            valid_members = [member for member in members if member in pv.index]
            if not valid_members:
                subtotal_values[metric_label] = pd.Series(index=years, dtype="float64")
                continue
            block = pv.loc[valid_members, years].apply(pd.to_numeric, errors="coerce")
            if agg in {"sum"}:
                subtotal_values[metric_label] = block.sum(axis=0, min_count=1)
            else:
                subtotal_values[metric_label] = block.mean(axis=0)
        row_entries.append((label, subtotal_values))

    rows: List[Dict[tuple[str, str] | str, Any]] = []
    for row_key, metric_values in row_entries:
        display_name = national_alias if row_key == national_name else row_alias_map.get(row_key, row_key)
        row: Dict[tuple[str, str] | str, Any] = {row_label: display_name}
        for metric in metric_specs:
            metric_label = str(metric.get("label", "")).strip()
            value_round = metric.get("value_round")
            value_round = int(value_round) if value_round is not None else None
            change_label = str(metric.get("change_label", "전년대비 증감률")).strip()
            change_round = int(metric.get("change_round", 1))
            change_type = str(metric.get("change_type", "pct")).strip().lower()
            share_label = str(metric.get("share_label", "")).strip()
            share_round = int(metric.get("share_round", 1))
            share_year = resolve_period_value(metric.get("share_year", years[0]), available)
            share_base_region = str(metric.get("share_base_region", national_name)).strip()

            series = metric_values.get(metric_label, pd.Series(index=years, dtype="float64"))
            start_val = pd.to_numeric(series.get(years[0]), errors="coerce")
            end_val = pd.to_numeric(series.get(years[1]), errors="coerce")
            row[(metric_label, year_label_map.get(years[0], f"{years[0]}년"))] = _format_rounded_value(start_val, value_round)
            row[(metric_label, year_label_map.get(years[1], f"{years[1]}년"))] = _format_rounded_value(end_val, value_round)

            if share_label:
                base_series = metric_frames.get(metric_label)
                base_val = pd.NA
                if base_series is not None and share_base_region in base_series.index:
                    base_val = pd.to_numeric(base_series.loc[share_base_region, share_year], errors="coerce")
                share_source = start_val if share_year == years[0] else end_val
                if pd.notna(share_source) and pd.notna(base_val) and base_val not in (0, 0.0):
                    row[(metric_label, share_label)] = _format_rounded_value((share_source / base_val) * 100, share_round)
                else:
                    row[(metric_label, share_label)] = pd.NA

            if change_type == "diff":
                change_val = end_val - start_val if pd.notna(start_val) and pd.notna(end_val) else pd.NA
            else:
                change_val = (
                    ((end_val / start_val) - 1) * 100
                    if pd.notna(start_val) and pd.notna(end_val) and start_val not in (0, 0.0)
                    else pd.NA
                )
            row[(metric_label, change_label)] = _format_rounded_value(change_val, change_round)
        rows.append(row)

    columns: List[tuple[str, str] | str] = [row_label]
    for metric in metric_specs:
        metric_label = str(metric.get("label", "")).strip()
        columns.append((metric_label, year_label_map.get(years[0], f"{years[0]}년")))
        columns.append((metric_label, year_label_map.get(years[1], f"{years[1]}년")))
        share_label = str(metric.get("share_label", "")).strip()
        if share_label:
            columns.append((metric_label, share_label))
        columns.append((metric_label, str(metric.get("change_label", "전년대비 증감률")).strip()))

    out = pd.DataFrame(rows)
    out = out.reindex(columns=columns)

    multi_cols = []
    for col in out.columns:
        if isinstance(col, tuple):
            multi_cols.append(col)
        else:
            multi_cols.append((row_label, ""))
    out.columns = pd.MultiIndex.from_tuples(multi_cols)
    return out

def make_age_distribution_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"age distribution summary columns missing: {missing}")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    if not years:
        raise RuntimeError("age distribution summary requires years")

    age_col = str(pivot_cfg.get("age_col", "C3_NM")).strip()
    age_code_col = str(pivot_cfg.get("age_code_col", "C3")).strip()
    if age_col not in df.columns or age_code_col not in df.columns:
        raise RuntimeError("age distribution summary requires valid age columns")

    work = df.copy()
    work["PRD_DE"] = work["PRD_DE"].astype(str)
    work["DT"] = pd.to_numeric(work["DT"], errors="coerce")
    work = work[work["PRD_DE"].isin(years)].copy()

    detail_filters = pivot_cfg.get("detail_filters", {})
    detail_order = [str(x) for x in pivot_cfg.get("detail_order", [])]
    bucket_defs = pivot_cfg.get("bucket_defs", [])
    if not detail_order or not isinstance(bucket_defs, list) or not bucket_defs:
        raise RuntimeError("age distribution summary requires detail_order and bucket_defs")

    detail_df = apply_row_filters(work, detail_filters)
    detail_pv = detail_df.pivot_table(
        index=age_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )

    left_label = str(pivot_cfg.get("left_label", "구분"))
    summary_label = str(pivot_cfg.get("summary_label", "요약 구분"))
    value_label = str(pivot_cfg.get("value_label", "인구수"))
    share_label = str(pivot_cfg.get("share_label", "비중"))

    left_rows: List[Dict[str, Any]] = []
    for label in detail_order:
        row: Dict[str, Any] = {left_label: label}
        for year in years:
            row[f"{year}년"] = detail_pv.loc[label, year] if label in detail_pv.index and year in detail_pv.columns else pd.NA
        left_rows.append(row)
    left = pd.DataFrame(left_rows, columns=[left_label] + [f"{y}년" for y in years])

    summary_filters = pivot_cfg.get("summary_filters", {})
    summary_df = apply_row_filters(work, summary_filters)
    total_codes = [str(x) for x in pivot_cfg.get("total_codes", [])]
    total_label = str(pivot_cfg.get("total_label", "계"))
    share_year = resolve_period_value(pivot_cfg.get("share_year", years[-1]), available)
    cagr_label = resolve_cagr_label(pivot_cfg.get("cagr_label"), years)
    periods = max(int(str(years[-1])[:4]) - int(str(years[0])[:4]), 1)

    total_block = summary_df[summary_df[age_code_col].astype(str).isin(total_codes)].copy()
    total_pv = total_block.groupby(["PRD_DE"], as_index=False, dropna=False, observed=True)["DT"].sum()
    total_map = {str(r["PRD_DE"]): r["DT"] for _, r in total_pv.iterrows()}

    right_rows: List[Dict[str, Any]] = []
    total_row: Dict[str, Any] = {summary_label: total_label}
    for year in years:
        total_row[f"{year}년 {value_label}"] = total_map.get(year, pd.NA)
    total_row[share_label] = 100.0 if pd.notna(total_map.get(share_year)) else pd.NA
    start_val = pd.to_numeric(total_row.get(f"{years[0]}년 {value_label}"), errors="coerce")
    end_val = pd.to_numeric(total_row.get(f"{years[-1]}년 {value_label}"), errors="coerce")
    total_row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1) if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0 else pd.NA
    right_rows.append(total_row)

    total_share_base = pd.to_numeric(total_row.get(f"{share_year}년 {value_label}"), errors="coerce")
    for bucket in bucket_defs:
        if not isinstance(bucket, dict):
            continue
        label = str(bucket.get("label", "")).strip()
        codes = [str(x) for x in bucket.get("codes", [])]
        if not label or not codes:
            continue
        block = summary_df[summary_df[age_code_col].astype(str).isin(codes)].copy()
        pv = block.groupby(["PRD_DE"], as_index=False, dropna=False, observed=True)["DT"].sum()
        val_map = {str(r["PRD_DE"]): r["DT"] for _, r in pv.iterrows()}
        row: Dict[str, Any] = {summary_label: label}
        for year in years:
            row[f"{year}년 {value_label}"] = val_map.get(year, pd.NA)
        latest_val = pd.to_numeric(row.get(f"{share_year}년 {value_label}"), errors="coerce")
        row[share_label] = round((latest_val / total_share_base) * 100, 1) if pd.notna(latest_val) and pd.notna(total_share_base) and total_share_base not in (0, 0.0) else pd.NA
        start_val = pd.to_numeric(row.get(f"{years[0]}년 {value_label}"), errors="coerce")
        end_val = pd.to_numeric(row.get(f"{years[-1]}년 {value_label}"), errors="coerce")
        row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1) if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0 else pd.NA
        right_rows.append(row)

    right = pd.DataFrame(
        right_rows,
        columns=[summary_label] + [f"{y}년 {value_label}" for y in years] + [share_label, cagr_label],
    )

    max_len = max(len(left), len(right))
    left = left.reindex(range(max_len))
    right = right.reindex(range(max_len))
    spacer = pd.DataFrame({"": [pd.NA] * max_len})
    return pd.concat([left, spacer, right], axis=1)


def make_gender_age_compare_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["ITM_ID", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"gender_age_compare_summary columns missing: {missing}")

    row_col = str(pivot_cfg.get("row_col", "C2_NM")).strip()
    if not row_col or row_col not in df.columns:
        raise RuntimeError("gender_age_compare_summary requires valid row_col")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    year_a = resolve_period_value(pivot_cfg.get("year_a", ""), available).strip()
    year_b = resolve_period_value(pivot_cfg.get("year_b", ""), available).strip()
    total_item_id = str(pivot_cfg.get("total_item_id", "")).strip()
    male_item_id = str(pivot_cfg.get("male_item_id", "")).strip()
    female_item_id = str(pivot_cfg.get("female_item_id", "")).strip()
    if not all([year_a, year_b, total_item_id, male_item_id, female_item_id]):
        raise RuntimeError("gender_age_compare_summary requires years and total/male/female item ids")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["ITM_ID"] = d["ITM_ID"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d = d[d["PRD_DE"].isin([year_a, year_b])].copy()

    filters = pivot_cfg.get("filters", {})
    d = apply_row_filters(d, filters)

    pv = d.pivot_table(
        index=row_col,
        columns=["PRD_DE", "ITM_ID"],
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )

    row_order = [str(x) for x in pivot_cfg.get("row_order", [])]
    if not row_order:
        row_order = list(dict.fromkeys(d[row_col].astype(str)))
    row_alias_map = {str(k): str(v) for k, v in pivot_cfg.get("row_alias_map", {}).items()}

    share_base_key = str(pivot_cfg.get("share_base_key", row_order[0] if row_order else "")).strip()
    if not share_base_key or share_base_key not in pv.index:
        raise RuntimeError("gender_age_compare_summary requires a valid share_base_key")

    year_a_label = resolve_period_value(pivot_cfg.get("year_a_label", f"{year_a}년"), available)
    year_b_label = resolve_period_value(pivot_cfg.get("year_b_label", f"{year_b}년"), available)
    cagr_label = resolve_cagr_label(pivot_cfg.get("cagr_label"), [year_a, year_b])
    value_round = int(pivot_cfg.get("value_round", 0))
    ratio_round = int(pivot_cfg.get("ratio_round", 1))
    share_round = int(pivot_cfg.get("share_round", 1))
    periods = max(int(year_b[:4]) - int(year_a[:4]), 1)

    total_a_base = pd.to_numeric(pv.loc[share_base_key, (year_a, total_item_id)], errors="coerce") if (year_a, total_item_id) in pv.columns else pd.NA
    total_b_base = pd.to_numeric(pv.loc[share_base_key, (year_b, total_item_id)], errors="coerce") if (year_b, total_item_id) in pv.columns else pd.NA

    rows: List[Dict[str, Any]] = []
    for row_key in row_order:
        if row_key not in pv.index:
            continue
        total_a = pd.to_numeric(pv.loc[row_key, (year_a, total_item_id)], errors="coerce") if (year_a, total_item_id) in pv.columns else pd.NA
        total_b = pd.to_numeric(pv.loc[row_key, (year_b, total_item_id)], errors="coerce") if (year_b, total_item_id) in pv.columns else pd.NA
        male_a = pd.to_numeric(pv.loc[row_key, (year_a, male_item_id)], errors="coerce") if (year_a, male_item_id) in pv.columns else pd.NA
        male_b = pd.to_numeric(pv.loc[row_key, (year_b, male_item_id)], errors="coerce") if (year_b, male_item_id) in pv.columns else pd.NA
        female_a = pd.to_numeric(pv.loc[row_key, (year_a, female_item_id)], errors="coerce") if (year_a, female_item_id) in pv.columns else pd.NA
        female_b = pd.to_numeric(pv.loc[row_key, (year_b, female_item_id)], errors="coerce") if (year_b, female_item_id) in pv.columns else pd.NA

        row: Dict[str, Any] = {
            "구분": row_alias_map.get(row_key, row_key),
            f"{year_a_label} 인구수": _format_rounded_value(total_a, value_round),
            f"{year_a_label} 성비": round((male_a / female_a) * 100, ratio_round) if pd.notna(male_a) and pd.notna(female_a) and female_a not in (0, 0.0) else pd.NA,
            f"{year_a_label} 비중": round((total_a / total_a_base) * 100, share_round) if pd.notna(total_a) and pd.notna(total_a_base) and total_a_base not in (0, 0.0) else pd.NA,
            f"{year_b_label} 인구수": _format_rounded_value(total_b, value_round),
            f"{year_b_label} 성비": round((male_b / female_b) * 100, ratio_round) if pd.notna(male_b) and pd.notna(female_b) and female_b not in (0, 0.0) else pd.NA,
            f"{year_b_label} 비중": round((total_b / total_b_base) * 100, share_round) if pd.notna(total_b) and pd.notna(total_b_base) and total_b_base not in (0, 0.0) else pd.NA,
            cagr_label: round((((total_b / total_a) ** (1 / periods)) - 1) * 100, 1) if pd.notna(total_a) and pd.notna(total_b) and total_a > 0 and total_b > 0 else pd.NA,
        }
        rows.append(row)

    out = pd.DataFrame(rows)
    ordered_cols = [
        "구분",
        f"{year_a_label} 인구수",
        f"{year_a_label} 성비",
        f"{year_a_label} 비중",
        f"{year_b_label} 인구수",
        f"{year_b_label} 성비",
        f"{year_b_label} 비중",
        cagr_label,
    ]
    return out[ordered_cols]


def make_population_pyramid_compare_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["ITM_ID", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"population_pyramid_compare columns missing: {missing}")

    row_col = str(pivot_cfg.get("row_col", "C2_NM")).strip()
    if not row_col or row_col not in df.columns:
        raise RuntimeError("population_pyramid_compare requires valid row_col")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    year_a = resolve_period_value(pivot_cfg.get("year_a", ""), available).strip()
    year_b = resolve_period_value(pivot_cfg.get("year_b", ""), available).strip()
    male_item_id = str(pivot_cfg.get("male_item_id", "")).strip()
    female_item_id = str(pivot_cfg.get("female_item_id", "")).strip()
    if not all([year_a, year_b, male_item_id, female_item_id]):
        raise RuntimeError("population_pyramid_compare requires years and male/female item ids")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["ITM_ID"] = d["ITM_ID"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d = d[d["PRD_DE"].isin([year_a, year_b])].copy()

    filters = pivot_cfg.get("filters", {})
    d = apply_row_filters(d, filters)

    pv = d.pivot_table(
        index=row_col,
        columns=["PRD_DE", "ITM_ID"],
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )

    row_order = [str(x) for x in pivot_cfg.get("row_order", [])]
    if not row_order:
        row_order = list(dict.fromkeys(d[row_col].astype(str)))
    row_alias_map = {str(k): str(v) for k, v in pivot_cfg.get("row_alias_map", {}).items()}
    value_round = int(pivot_cfg.get("value_round", 0))

    year_a_label = resolve_period_value(pivot_cfg.get("year_a_label", f"{year_a}년"), available)
    year_b_label = resolve_period_value(pivot_cfg.get("year_b_label", f"{year_b}년"), available)
    male_label = str(pivot_cfg.get("male_label", "남자인구수"))
    female_label = str(pivot_cfg.get("female_label", "여자인구수"))
    row_label = str(pivot_cfg.get("row_label", "구분"))

    rows: List[Dict[str, Any]] = []
    for row_key in row_order:
        if row_key not in pv.index:
            continue
        male_a = pd.to_numeric(pv.loc[row_key, (year_a, male_item_id)], errors="coerce") if (year_a, male_item_id) in pv.columns else pd.NA
        female_a = pd.to_numeric(pv.loc[row_key, (year_a, female_item_id)], errors="coerce") if (year_a, female_item_id) in pv.columns else pd.NA
        male_b = pd.to_numeric(pv.loc[row_key, (year_b, male_item_id)], errors="coerce") if (year_b, male_item_id) in pv.columns else pd.NA
        female_b = pd.to_numeric(pv.loc[row_key, (year_b, female_item_id)], errors="coerce") if (year_b, female_item_id) in pv.columns else pd.NA
        label = row_alias_map.get(row_key, row_key)
        rows.append(
            {
                f"{year_a_label} {row_label}": label,
                f"{year_a_label} {male_label}": _format_rounded_value(-male_a if pd.notna(male_a) else pd.NA, value_round),
                f"{year_a_label} {female_label}": _format_rounded_value(female_a, value_round),
                f"{year_b_label} {row_label}": label,
                f"{year_b_label} {male_label}": _format_rounded_value(-male_b if pd.notna(male_b) else pd.NA, value_round),
                f"{year_b_label} {female_label}": _format_rounded_value(female_b, value_round),
            }
        )

    cols = [
        f"{year_a_label} {row_label}",
        f"{year_a_label} {male_label}",
        f"{year_a_label} {female_label}",
        f"{year_b_label} {row_label}",
        f"{year_b_label} {male_label}",
        f"{year_b_label} {female_label}",
    ]
    return pd.DataFrame(rows, columns=cols)

def make_single_metric_share_summary_pivot(
    df: pd.DataFrame,
    pivot_cfg: dict,
    share_base_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"single_metric_share_summary columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM"))
    if region_col not in df.columns:
        raise RuntimeError(f"single_metric_share_summary region column missing: {region_col}")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    if not years:
        raise RuntimeError("single_metric_share_summary requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            d = d[d[col].astype(str).isin([str(v) for v in vals])].copy()

    preserve_text_values = bool(pivot_cfg.get("preserve_text_values", False))
    raw_pv = None
    if preserve_text_values:
        raw_pv = d.pivot_table(
            index=region_col,
            columns="PRD_DE",
            values="DT",
            aggfunc="first",
            sort=False,
            observed=True,
        )
        for year in years:
            if year not in raw_pv.columns:
                raw_pv[year] = pd.NA
        raw_pv = raw_pv[years]

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[region_col] = d[region_col].astype(str)

    pv = d.pivot_table(
        index=region_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )
    for year in years:
        if year not in pv.columns:
            pv[year] = pd.NA
    pv = pv[years]

    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))
    area_label = str(pivot_cfg.get("area_label", "구분"))
    share_year = resolve_period_value(pivot_cfg.get("share_year", years[-1]), available)
    include_share = bool(pivot_cfg.get("include_share", True))
    value_round = pivot_cfg.get("value_round")
    value_round = int(value_round) if value_round is not None else None
    value_round_map = {str(k): int(v) for k, v in pivot_cfg.get("value_round_map", {}).items()}
    include_cagr = bool(pivot_cfg.get("include_cagr", True))
    cagr_label = resolve_cagr_label(pivot_cfg.get("cagr_label"), years)
    periods = max(int(years[-1]) - int(years[0]), 1)
    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(pv.index.astype(str))
    region_alias_map = {str(k): str(v) for k, v in pivot_cfg.get("region_alias_map", {}).items()}

    share_base_filters = pivot_cfg.get("share_base_filters", {})
    share_base_year = resolve_period_value(pivot_cfg.get("share_base_year", share_year), available)
    share_exclude_regions = {str(x) for x in pivot_cfg.get("share_exclude_regions", [])}
    share_exclude_placeholder = pivot_cfg.get("share_exclude_placeholder", pd.NA)
    if include_share and isinstance(share_base_filters, dict) and share_base_filters:
        base_df = share_base_df.copy() if share_base_df is not None else df.copy()
        base_df["PRD_DE"] = base_df["PRD_DE"].astype(str)
        base_df = base_df[base_df["PRD_DE"] == share_base_year].copy()
        for col, vals in share_base_filters.items():
            if col in base_df.columns:
                base_df = base_df[base_df[col].astype(str).isin([str(v) for v in vals])].copy()
        share_base_series = pd.to_numeric(base_df.get("DT"), errors="coerce")
        national_val = share_base_series.iloc[0] if len(share_base_series) == 1 else share_base_series.sum(min_count=1)
    else:
        national_val = pd.to_numeric(pv.loc[national_name, share_year], errors="coerce") if national_name in pv.index else pd.NA

    rows: List[Dict[str, Any]] = []
    for region in region_order:
        if region not in pv.index and not (preserve_text_values and raw_pv is not None and region in raw_pv.index):
            continue
        rec = pv.loc[region] if region in pv.index else pd.Series(index=years, dtype="object")
        row: Dict[str, Any] = {
            area_label: national_alias if region == national_name else region_alias_map.get(region, region)
        }
        digits = value_round_map.get(str(region), value_round)
        for year in years:
            value = _format_rounded_value(rec.get(year), digits)
            if preserve_text_values and raw_pv is not None and pd.isna(value) and region in raw_pv.index:
                row[f"{year}년"] = raw_pv.loc[region].get(year)
            else:
                row[f"{year}년"] = value

        latest_val = pd.to_numeric(rec.get(share_year), errors="coerce")
        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
        if include_share:
            if region in share_exclude_regions:
                row["비중"] = share_exclude_placeholder
            else:
                row["비중"] = (
                    round((latest_val / national_val) * 100, 1)
                    if pd.notna(latest_val) and pd.notna(national_val) and national_val not in (0, 0.0)
                    else pd.NA
                )
        row[cagr_label] = (
            round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
            if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
            else pd.NA
        )
        rows.append(row)

    subtotals = pivot_cfg.get("subtotals", [])
    if not subtotals and pivot_cfg.get("subtotal"):
        subtotals = [pivot_cfg["subtotal"]]
    for subtotal in subtotals:
        members = [str(x) for x in subtotal.get("members", []) if str(x) in pv.index]
        if not members:
            continue
        agg = str(subtotal.get("agg", "sum")).strip().lower()
        subtotal_round = subtotal.get("value_round")
        subtotal_round = int(subtotal_round) if subtotal_round is not None else value_round
        subtotal_frame = pv.loc[members, years].apply(pd.to_numeric, errors="coerce")
        if agg in {"mean", "avg", "average"}:
            subtotal_vals = subtotal_frame.mean(axis=0)
        else:
            subtotal_vals = subtotal_frame.sum(axis=0, min_count=1)
        row = {area_label: str(subtotal.get("label", "소계"))}
        for year in years:
            row[f"{year}년"] = _format_rounded_value(subtotal_vals.get(year), subtotal_round)
        latest_val = pd.to_numeric(subtotal_vals.get(share_year), errors="coerce")
        start_val = pd.to_numeric(subtotal_vals.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(subtotal_vals.get(years[-1]), errors="coerce")
        if include_share:
            row["비중"] = (
                round((latest_val / national_val) * 100, 1)
                if pd.notna(latest_val) and pd.notna(national_val) and national_val not in (0, 0.0)
                else pd.NA
            )
        if include_cagr:
            row[cagr_label] = (
                round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
                if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
                else pd.NA
            )
        rows.append(row)

    columns = [area_label] + [f"{y}년" for y in years]
    if include_share:
        columns.append("비중")
    if include_cagr:
        columns.append(cagr_label)
    out = pd.DataFrame(rows, columns=columns)
    has_nonint_subtotal = any(
        isinstance(subtotal, dict) and subtotal.get("value_round") not in (None, 0)
        for subtotal in subtotals
    )
    if value_round == 0 and not preserve_text_values and not value_round_map:
        for year in years:
            col = f"{year}년"
            if col not in out.columns:
                continue
            if not has_nonint_subtotal:
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
                continue
            values = pd.to_numeric(out[col], errors="coerce")
            converted: List[Any] = []
            for val in values.tolist():
                if pd.isna(val):
                    converted.append(pd.NA)
                elif float(val).is_integer():
                    converted.append(int(val))
                else:
                    converted.append(float(val))
            out[col] = pd.Series(converted, dtype="object")
    return out


def make_single_metric_year_share_summary_pivot(
    df: pd.DataFrame,
    pivot_cfg: dict,
) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"single_metric_year_share_summary columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM"))
    if region_col not in df.columns:
        raise RuntimeError(f"single_metric_year_share_summary region column missing: {region_col}")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    if not years:
        raise RuntimeError("single_metric_year_share_summary requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            d = d[d[col].astype(str).isin([str(v) for v in vals])].copy()

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[region_col] = d[region_col].astype(str)

    pv = d.pivot_table(
        index=region_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )
    for year in years:
        if year not in pv.columns:
            pv[year] = pd.NA
    pv = pv[years]

    area_label = str(pivot_cfg.get("area_label", "구분"))
    share_base_key = str(pivot_cfg.get("share_base_key", "")).strip()
    if not share_base_key:
        raise RuntimeError("single_metric_year_share_summary requires share_base_key")

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(pv.index.astype(str))
    region_alias_map = {str(k): str(v) for k, v in pivot_cfg.get("region_alias_map", {}).items()}
    year_label_map = {str(k): str(v) for k, v in pivot_cfg.get("year_label_map", {}).items()}
    value_round = pivot_cfg.get("value_round")
    value_round = int(value_round) if value_round is not None else None
    value_round_map = {str(k): int(v) for k, v in pivot_cfg.get("value_round_map", {}).items()}
    share_label = str(pivot_cfg.get("share_label", "비중"))
    share_as_percent = bool(pivot_cfg.get("share_as_percent", False))
    share_round = int(pivot_cfg.get("share_round", 4 if not share_as_percent else 1))
    cagr_label = resolve_cagr_label(pivot_cfg.get("cagr_label"), years)
    periods = max(int(years[-1]) - int(years[0]), 1)

    if share_base_key not in pv.index:
        raise RuntimeError(f"single_metric_year_share_summary share_base_key missing: {share_base_key}")
    share_base_series = pd.to_numeric(pv.loc[share_base_key, years], errors="coerce")

    rows: List[Dict[str, Any]] = []
    value_keys = {year: f"__value_{year}" for year in years}
    share_keys = {year: f"__share_{year}" for year in years}
    for region in region_order:
        if region not in pv.index:
            continue
        rec = pv.loc[region]
        row: Dict[str, Any] = {area_label: region_alias_map.get(region, region)}
        digits = value_round_map.get(str(region), value_round)
        for year in years:
            value = pd.to_numeric(rec.get(year), errors="coerce")
            base_val = pd.to_numeric(share_base_series.get(year), errors="coerce")
            row[value_keys[year]] = _format_rounded_value(value, digits)
            row[share_keys[year]] = (
                round((value / base_val) * (100 if share_as_percent else 1), share_round)
                if pd.notna(value) and pd.notna(base_val) and base_val not in (0, 0.0)
                else pd.NA
            )

        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
        row[cagr_label] = (
            round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
            if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
            else pd.NA
        )
        rows.append(row)

    subtotals = pivot_cfg.get("subtotals", [])
    if not subtotals and pivot_cfg.get("subtotal"):
        subtotals = [pivot_cfg["subtotal"]]
    for subtotal in subtotals:
        members = [str(x) for x in subtotal.get("members", []) if str(x) in pv.index]
        if not members:
            continue
        agg = str(subtotal.get("agg", "sum")).strip().lower()
        subtotal_round = subtotal.get("value_round")
        subtotal_round = int(subtotal_round) if subtotal_round is not None else value_round
        subtotal_frame = pv.loc[members, years].apply(pd.to_numeric, errors="coerce")
        subtotal_vals = subtotal_frame.mean(axis=0) if agg in {"mean", "avg", "average"} else subtotal_frame.sum(axis=0, min_count=1)
        row: Dict[str, Any] = {area_label: str(subtotal.get("label", "소계"))}
        for year in years:
            value = pd.to_numeric(subtotal_vals.get(year), errors="coerce")
            base_val = pd.to_numeric(share_base_series.get(year), errors="coerce")
            row[value_keys[year]] = _format_rounded_value(value, subtotal_round)
            row[share_keys[year]] = (
                round((value / base_val) * (100 if share_as_percent else 1), share_round)
                if pd.notna(value) and pd.notna(base_val) and base_val not in (0, 0.0)
                else pd.NA
            )
        start_val = pd.to_numeric(subtotal_vals.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(subtotal_vals.get(years[-1]), errors="coerce")
        row[cagr_label] = (
            round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
            if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
            else pd.NA
        )
        rows.append(row)

    internal_columns = [area_label]
    display_columns = [area_label]
    year_display_columns: List[str] = []
    for year in years:
        display_label = year_label_map.get(year, f"{year}년")
        internal_columns.extend([value_keys[year], share_keys[year]])
        display_columns.extend([display_label, share_label])
        year_display_columns.append(display_label)
    internal_columns.append(cagr_label)
    display_columns.append(cagr_label)

    out = pd.DataFrame(rows, columns=internal_columns)
    out.columns = display_columns
    if value_round == 0 and not value_round_map:
        for col in year_display_columns:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    return out

def make_group_metric_share_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"group_metric_share_summary columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM")).strip()
    category_col = str(pivot_cfg.get("category_col", "C2_NM")).strip()
    if region_col not in df.columns or category_col not in df.columns:
        raise RuntimeError("group_metric_share_summary requires valid region/category columns")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    if not years:
        raise RuntimeError("group_metric_share_summary requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            d = d[d[col].astype(str).isin([str(v) for v in vals])].copy()

    preserve_text_values = bool(pivot_cfg.get("preserve_text_values", False))
    raw_pv = None
    if preserve_text_values:
        raw_pv = d.pivot_table(
            index=region_col,
            columns="PRD_DE",
            values="DT",
            aggfunc="first",
            sort=False,
            observed=True,
        )
        for year in years:
            if year not in raw_pv.columns:
                raw_pv[year] = pd.NA
        raw_pv = raw_pv[years]

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[region_col] = d[region_col].astype(str)
    d[category_col] = d[category_col].astype(str)

    pv = d.pivot_table(
        index=[region_col, category_col],
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )
    for year in years:
        if year not in pv.columns:
            pv[year] = pd.NA
    pv = pv[years]

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(d[region_col].astype(str)))
    category_order = [str(x) for x in pivot_cfg.get("category_order", [])]
    if not category_order:
        category_order = list(dict.fromkeys(d[category_col].astype(str)))

    area_label = str(pivot_cfg.get("area_label", "지역"))
    category_label = str(pivot_cfg.get("category_label", "구분"))
    share_year = resolve_period_value(pivot_cfg.get("share_year", years[-1]), available)
    include_share = bool(pivot_cfg.get("include_share", True))
    share_base_category = str(pivot_cfg.get("share_base_category", "계"))
    value_round = pivot_cfg.get("value_round")
    value_round = int(value_round) if value_round is not None else None
    cagr_label = resolve_cagr_label(pivot_cfg.get("cagr_label"), years)
    periods = max(int(years[-1]) - int(years[0]), 1)
    region_alias_map = {str(k): str(v) for k, v in pivot_cfg.get("region_alias_map", {}).items()}

    rows: List[Dict[str, Any]] = []
    for region in region_order:
        share_base = pd.NA
        if (region, share_base_category) in pv.index:
            share_base = pd.to_numeric(pv.loc[(region, share_base_category), share_year], errors="coerce")

        first = True
        for category in category_order:
            if (region, category) not in pv.index:
                continue
            rec = pv.loc[(region, category)]
            row: Dict[str, Any] = {
                area_label: region_alias_map.get(region, region) if first else "",
                category_label: category,
            }
            for year in years:
                row[f"{year}년"] = _format_rounded_value(rec.get(year), value_round)
            latest_val = pd.to_numeric(rec.get(share_year), errors="coerce")
            start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
            end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
            if include_share:
                row["비중"] = (
                    round((latest_val / share_base) * 100, 1)
                    if pd.notna(latest_val) and pd.notna(share_base) and share_base not in (0, 0.0)
                    else pd.NA
                )
            row[cagr_label] = (
                round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
                if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
                else pd.NA
            )
            rows.append(row)
            first = False

    columns = [area_label, category_label] + [f"{y}년" for y in years]
    if include_share:
        columns.append("비중")
    columns.append(cagr_label)
    return pd.DataFrame(rows, columns=columns)


def make_row_timeseries_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"row_timeseries columns missing: {missing}")

    row_key_col = str(pivot_cfg.get("row_key_col", "")).strip()
    row_label_col = str(pivot_cfg.get("row_label_col", row_key_col)).strip()
    if not row_key_col or row_key_col not in df.columns:
        raise RuntimeError("row_timeseries requires valid row_key_col")
    if not row_label_col or row_label_col not in df.columns:
        raise RuntimeError("row_timeseries requires valid row_label_col")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    if not years:
        raise RuntimeError("row_timeseries requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            d = d[d[col].astype(str).isin([str(v) for v in vals])].copy()

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[row_key_col] = d[row_key_col].astype(str)
    d[row_label_col] = d[row_label_col].astype(str)

    row_order = [str(x) for x in pivot_cfg.get("row_order", [])]
    if not row_order:
        row_order = list(dict.fromkeys(d[row_key_col].astype(str)))

    pv = d.pivot_table(
        index=row_key_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )
    for year in years:
        if year not in pv.columns:
            pv[year] = pd.NA
    pv = pv[years]

    if row_key_col == row_label_col:
        label_map = {key: key for key in d[row_key_col].drop_duplicates().astype(str).tolist()}
    else:
        label_map = (
            d[[row_key_col, row_label_col]]
            .drop_duplicates()
            .set_index(row_key_col)[row_label_col]
            .to_dict()
        )

    group_label = str(pivot_cfg.get("group_label", "구분"))
    item_label = str(pivot_cfg.get("item_label", "항목"))
    group_value = str(pivot_cfg.get("group_value", "")).strip()
    repeat_group_label = bool(pivot_cfg.get("repeat_group_label", False))
    include_group_label = bool(group_label)
    value_round = pivot_cfg.get("value_round")
    value_round = int(value_round) if value_round is not None else None
    value_round_map = {str(k): int(v) for k, v in pivot_cfg.get("value_round_map", {}).items()}
    year_label_map = {str(k): str(v) for k, v in pivot_cfg.get("year_label_map", {}).items()}
    include_share = bool(pivot_cfg.get("include_share", False))
    share_year = resolve_period_value(pivot_cfg.get("share_year", years[-1]), available)
    share_base_key = str(pivot_cfg.get("share_base_key", row_order[0] if row_order else "")).strip()
    share_label = str(pivot_cfg.get("share_label", "비중"))
    share_round = int(pivot_cfg.get("share_round", 1))
    include_cagr = bool(pivot_cfg.get("include_cagr", False))
    cagr_label = resolve_cagr_label(pivot_cfg.get("cagr_label"), years)
    cagr_include_keys = [str(x) for x in pivot_cfg.get("cagr_include_keys", [])]
    cagr_exclude_keys = [str(x) for x in pivot_cfg.get("cagr_exclude_keys", [])]
    periods = max(int(years[-1][:4]) - int(years[0][:4]), 1)

    share_base = pd.NA
    if include_share and share_base_key and share_base_key in pv.index and share_year in pv.columns:
        share_base = pd.to_numeric(pv.loc[share_base_key, share_year], errors="coerce")

    rows: List[Dict[str, Any]] = []
    first = True
    for key in row_order:
        if key not in pv.index:
            continue
        rec = pv.loc[key]
        row: Dict[str, Any] = {item_label: label_map.get(key, key)}
        if include_group_label:
            row[group_label] = group_value if (repeat_group_label or first) else ""
        digits = value_round_map.get(str(key), value_round)
        for year in years:
            row[year_label_map.get(year, year)] = _format_rounded_value(rec.get(year), digits)
        latest_val = pd.to_numeric(rec.get(share_year), errors="coerce")
        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
        if include_share:
            row[share_label] = (
                round((latest_val / share_base) * 100, share_round)
                if pd.notna(latest_val) and pd.notna(share_base) and share_base not in (0, 0.0)
                else pd.NA
            )
        if include_cagr:
            if _should_include_calc(key, cagr_include_keys, cagr_exclude_keys):
                row[cagr_label] = (
                    round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
                    if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
                    else pd.NA
                )
            else:
                row[cagr_label] = pd.NA
        rows.append(row)
        first = False

    year_columns = [year_label_map.get(year, year) for year in years]
    columns = ([group_label] if include_group_label else []) + [item_label] + year_columns
    if include_share:
        columns.append(share_label)
    if include_cagr:
        columns.append(cagr_label)
    out = pd.DataFrame(rows, columns=columns)
    for col in year_columns:
        if col not in out.columns:
            continue
        values = pd.to_numeric(out[col], errors="coerce")
        converted: List[Any] = []
        for raw_val, num_val in zip(out[col].tolist(), values.tolist()):
            if pd.isna(num_val):
                converted.append(raw_val)
            elif float(num_val).is_integer():
                converted.append(int(num_val))
            else:
                converted.append(float(num_val))
        out[col] = pd.Series(converted, dtype="object")
    return out


def make_category_timeseries_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"category_timeseries_summary columns missing: {missing}")

    category_col = str(pivot_cfg.get("category_col", "")).strip()
    if not category_col or category_col not in df.columns:
        raise RuntimeError("category_timeseries_summary requires valid category_col")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    if not years:
        raise RuntimeError("category_timeseries_summary requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[category_col] = d[category_col].astype(str)

    category_order = [str(x) for x in pivot_cfg.get("category_order", [])]
    if not category_order:
        category_order = list(dict.fromkeys(d[category_col].astype(str)))

    pv = d.pivot_table(
        index=category_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )
    for year in years:
        if year not in pv.columns:
            pv[year] = pd.NA
    pv = pv[years]

    row_label = str(pivot_cfg.get("row_label", "구분"))
    category_alias_map = {str(k): str(v) for k, v in pivot_cfg.get("category_alias_map", {}).items()}
    include_share = bool(pivot_cfg.get("include_share", False))
    share_year = resolve_period_value(pivot_cfg.get("share_year", years[-1]), available)
    share_base_key = str(pivot_cfg.get("share_base_key", category_order[0] if category_order else "")).strip()
    share_label = str(pivot_cfg.get("share_label", "비중"))
    include_cagr = bool(pivot_cfg.get("include_cagr", True))
    cagr_label = resolve_cagr_label(pivot_cfg.get("cagr_label"), years)
    cagr_include_keys = [str(x) for x in pivot_cfg.get("cagr_include_keys", [])]
    cagr_exclude_keys = [str(x) for x in pivot_cfg.get("cagr_exclude_keys", [])]
    annual_change_label = str(pivot_cfg.get("annual_change_label", "")).strip()
    annual_change_include_keys = [str(x) for x in pivot_cfg.get("annual_change_include_keys", [])]
    annual_change_exclude_keys = [str(x) for x in pivot_cfg.get("annual_change_exclude_keys", [])]
    annual_change_round = int(pivot_cfg.get("annual_change_round", 2))
    periods = max(int(years[-1][:4]) - int(years[0][:4]), 1)
    value_round = pivot_cfg.get("value_round")
    value_round = int(value_round) if value_round is not None else None
    year_label_map = {str(k): str(v) for k, v in pivot_cfg.get("year_label_map", {}).items()}
    if not year_label_map:
        year_label_map = {year: _format_period_label(year) for year in years}

    share_base = pd.NA
    if include_share and share_base_key and share_base_key in pv.index and share_year in pv.columns:
        share_base = pd.to_numeric(pv.loc[share_base_key, share_year], errors="coerce")

    rows: List[Dict[str, Any]] = []
    for key in category_order:
        if key not in pv.index:
            continue
        rec = pv.loc[key]
        row: Dict[str, Any] = {row_label: category_alias_map.get(key, key)}
        for year in years:
            row[year_label_map.get(year, year)] = _format_rounded_value(rec.get(year), value_round)

        latest_val = pd.to_numeric(rec.get(share_year), errors="coerce")
        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
        if include_share:
            row[share_label] = (
                round((latest_val / share_base) * 100, 1)
                if pd.notna(latest_val) and pd.notna(share_base) and share_base not in (0, 0.0)
                else pd.NA
            )
        if include_cagr:
            if _should_include_calc(key, cagr_include_keys, cagr_exclude_keys):
                row[cagr_label] = (
                    round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
                    if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
                    else pd.NA
                )
            else:
                row[cagr_label] = pd.NA
        if annual_change_label:
            if _should_include_calc(key, annual_change_include_keys, annual_change_exclude_keys):
                row[annual_change_label] = (
                    round((end_val - start_val) / periods, annual_change_round)
                    if pd.notna(start_val) and pd.notna(end_val)
                    else pd.NA
                )
            else:
                row[annual_change_label] = pd.NA
        rows.append(row)

    columns = [row_label] + [year_label_map.get(year, year) for year in years]
    if include_share:
        columns.append(share_label)
    if include_cagr:
        columns.append(cagr_label)
    if annual_change_label:
        columns.append(annual_change_label)
    return pd.DataFrame(rows, columns=columns)


def make_hierarchy_timeseries_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"hierarchy_timeseries_summary columns missing: {missing}")

    group_col = str(pivot_cfg.get("group_col", "")).strip()
    detail_col = str(pivot_cfg.get("detail_col", "")).strip()
    order_col = str(pivot_cfg.get("order_col", "")).strip()
    if not group_col or group_col not in df.columns:
        raise RuntimeError("hierarchy_timeseries_summary requires valid group_col")
    if not detail_col or detail_col not in df.columns:
        raise RuntimeError("hierarchy_timeseries_summary requires valid detail_col")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    if not years:
        raise RuntimeError("hierarchy_timeseries_summary requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[group_col] = d[group_col].astype(str)
    d[detail_col] = d[detail_col].fillna("").astype(str)
    if order_col and order_col in d.columns:
        d[order_col] = pd.to_numeric(d[order_col], errors="coerce")

    index_cols = [group_col, detail_col]
    if order_col and order_col in d.columns:
        index_cols.append(order_col)
    index_cols = list(dict.fromkeys(index_cols))
    pv = d.pivot_table(
        index=index_cols,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )
    for year in years:
        if year not in pv.columns:
            pv[year] = pd.NA
    pv = pv[years].reset_index()

    if order_col and order_col in pv.columns:
        pv = pv.sort_values([order_col, group_col, detail_col], kind="stable")
    else:
        pv = pv.sort_values([group_col, detail_col], kind="stable")

    group_label = str(pivot_cfg.get("group_label", "구분"))
    detail_label = str(pivot_cfg.get("detail_label", "세부"))
    include_share = bool(pivot_cfg.get("include_share", False))
    share_year = resolve_period_value(pivot_cfg.get("share_year", years[-1]), available)
    share_base_key = str(pivot_cfg.get("share_base_key", "계")).strip()
    share_label = str(pivot_cfg.get("share_label", "비중"))
    include_cagr = bool(pivot_cfg.get("include_cagr", True))
    cagr_label = resolve_cagr_label(pivot_cfg.get("cagr_label"), years)
    cagr_include_keys = [str(x) for x in pivot_cfg.get("cagr_include_keys", [])]
    cagr_exclude_keys = [str(x) for x in pivot_cfg.get("cagr_exclude_keys", [])]
    annual_change_label = str(pivot_cfg.get("annual_change_label", "")).strip()
    annual_change_include_keys = [str(x) for x in pivot_cfg.get("annual_change_include_keys", [])]
    annual_change_exclude_keys = [str(x) for x in pivot_cfg.get("annual_change_exclude_keys", [])]
    annual_change_round = int(pivot_cfg.get("annual_change_round", 2))
    periods = max(int(years[-1][:4]) - int(years[0][:4]), 1)
    value_round = pivot_cfg.get("value_round")
    value_round = int(value_round) if value_round is not None else None
    year_label_map = {str(k): str(v) for k, v in pivot_cfg.get("year_label_map", {}).items()}
    if not year_label_map:
        year_label_map = {year: _format_period_label(year) for year in years}

    base_val = pd.NA
    if include_share:
        base_rows = pv[(pv[group_col] == share_base_key) & (pv[detail_col].astype(str).isin(["", "소계"]))]
        if base_rows.empty:
            base_rows = pv[pv[group_col] == share_base_key]
        if not base_rows.empty:
            base_val = pd.to_numeric(base_rows.iloc[0].get(share_year), errors="coerce")

    rows: List[Dict[str, Any]] = []
    last_group = None
    for _, rec in pv.iterrows():
        row: Dict[str, Any] = {
            group_label: rec[group_col] if rec[group_col] != last_group else "",
            detail_label: rec[detail_col],
        }
        for year in years:
            row[year_label_map.get(year, year)] = _format_rounded_value(rec.get(year), value_round)

        latest_val = pd.to_numeric(rec.get(share_year), errors="coerce")
        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
        calc_key = rec[detail_col] if str(rec[detail_col]).strip() else rec[group_col]
        if include_share:
            row[share_label] = (
                round((latest_val / base_val) * 100, 1)
                if pd.notna(latest_val) and pd.notna(base_val) and base_val not in (0, 0.0)
                else pd.NA
            )
        if include_cagr:
            if _should_include_calc(calc_key, cagr_include_keys, cagr_exclude_keys):
                row[cagr_label] = (
                    round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
                    if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
                    else pd.NA
                )
            else:
                row[cagr_label] = pd.NA
        if annual_change_label:
            if _should_include_calc(calc_key, annual_change_include_keys, annual_change_exclude_keys):
                row[annual_change_label] = (
                    round((end_val - start_val) / periods, annual_change_round)
                    if pd.notna(start_val) and pd.notna(end_val)
                    else pd.NA
                )
            else:
                row[annual_change_label] = pd.NA
        rows.append(row)
        last_group = rec[group_col]

    columns = [group_label, detail_label] + [year_label_map.get(year, year) for year in years]
    if include_share:
        columns.append(share_label)
    if include_cagr:
        columns.append(cagr_label)
    if annual_change_label:
        columns.append(annual_change_label)
    return pd.DataFrame(rows, columns=columns)


def make_latest_metric_share_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"latest_metric_share_summary columns missing: {missing}")

    category_col = str(pivot_cfg.get("category_col", "")).strip()
    if not category_col or category_col not in df.columns:
        raise RuntimeError("latest_metric_share_summary requires valid category_col")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    period = resolve_period_value(pivot_cfg.get("period", ""), available).strip()
    if not period:
        raise RuntimeError("latest_metric_share_summary requires period")

    metrics = pivot_cfg.get("metrics", [])
    if not isinstance(metrics, list) or not metrics:
        raise RuntimeError("latest_metric_share_summary requires non-empty metrics")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"] == period].copy()

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            allowed = vals if isinstance(vals, list) else [vals]
            d = d[d[col].astype(str).isin([str(v) for v in allowed])].copy()

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[category_col] = d[category_col].astype(str)

    category_order = [str(x) for x in pivot_cfg.get("category_order", [])]
    if not category_order:
        category_order = list(dict.fromkeys(d[category_col].astype(str)))

    row_label = str(pivot_cfg.get("row_label", "구분"))
    category_alias_map = {str(k): str(v) for k, v in pivot_cfg.get("category_alias_map", {}).items()}

    base_category = str(pivot_cfg.get("share_base_category", "")).strip()
    share_as_percent = bool(pivot_cfg.get("share_as_percent", True))

    rows: List[Dict[str, Any]] = []
    for category in category_order:
        row: Dict[str, Any] = {row_label: category_alias_map.get(category, category)}
        block_for_row = d[d[category_col].astype(str) == category].copy()
        if block_for_row.empty:
            continue

        for metric in metrics:
            if not isinstance(metric, dict):
                continue
            value_label = str(metric.get("value_label", "")).strip()
            share_label = str(metric.get("share_label", "")).strip()
            if not value_label:
                continue

            block = d.copy()
            metric_filters = metric.get("filters", {})
            if isinstance(metric_filters, dict):
                for col, vals in metric_filters.items():
                    if col in block.columns:
                        allowed = vals if isinstance(vals, list) else [vals]
                        block = block[block[col].astype(str).isin([str(v) for v in allowed])].copy()

            metric_series = block.groupby(category_col, as_index=True, dropna=False, observed=True)["DT"].first()
            value = metric_series.get(category, pd.NA)
            value_round = metric.get("value_round")
            value_round = int(value_round) if value_round is not None else None
            row[value_label] = _format_rounded_value(value, value_round)

            if share_label:
                base_val = metric_series.get(base_category, pd.NA) if base_category else pd.NA
                share_round = metric.get("share_round")
                share_round = int(share_round) if share_round is not None else 1
                num = pd.to_numeric(value, errors="coerce")
                den = pd.to_numeric(base_val, errors="coerce")
                if pd.notna(num) and pd.notna(den) and den not in (0, 0.0):
                    share = (num / den) * (100 if share_as_percent else 1)
                    row[share_label] = _format_rounded_value(share, share_round)
                else:
                    row[share_label] = pd.NA

        rows.append(row)

    columns = [row_label]
    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        value_label = str(metric.get("value_label", "")).strip()
        share_label = str(metric.get("share_label", "")).strip()
        if value_label:
            columns.append(value_label)
        if share_label:
            columns.append(share_label)
    return pd.DataFrame(rows, columns=columns)


def make_latest_metric_matrix_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"latest_metric_matrix columns missing: {missing}")

    row_col = str(pivot_cfg.get("row_col", "")).strip()
    metric_col = str(pivot_cfg.get("metric_col", "")).strip()
    if not row_col or row_col not in df.columns:
        raise RuntimeError("latest_metric_matrix requires valid row_col")
    if not metric_col or metric_col not in df.columns:
        raise RuntimeError("latest_metric_matrix requires valid metric_col")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    period = resolve_period_value(pivot_cfg.get("period", ""), available).strip()
    if not period:
        raise RuntimeError("latest_metric_matrix requires period")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"] == period].copy()

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            allowed = vals if isinstance(vals, list) else [vals]
            d = d[d[col].astype(str).isin([str(v) for v in allowed])].copy()

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[row_col] = d[row_col].astype(str)
    d[metric_col] = d[metric_col].astype(str)

    row_order = [str(x) for x in pivot_cfg.get("row_order", [])]
    if not row_order:
        row_order = list(dict.fromkeys(d[row_col].astype(str)))
    metric_order = [str(x) for x in pivot_cfg.get("metric_order", [])]
    if not metric_order:
        metric_order = list(dict.fromkeys(d[metric_col].astype(str)))

    pv = d.pivot_table(
        index=row_col,
        columns=metric_col,
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )

    row_alias_map = {str(k): str(v) for k, v in pivot_cfg.get("row_alias_map", {}).items()}
    metric_label_map = {str(k): str(v) for k, v in pivot_cfg.get("metric_label_map", {}).items()}
    value_round_map = {str(k): int(v) for k, v in pivot_cfg.get("value_round_map", {}).items()}
    row_label = str(pivot_cfg.get("row_label", "구분"))

    rows: List[Dict[str, Any]] = []
    for row_key in row_order:
        if row_key not in pv.index:
            continue
        rec = pv.loc[row_key]
        row: Dict[str, Any] = {row_label: row_alias_map.get(row_key, row_key)}
        for metric_key in metric_order:
            label = metric_label_map.get(metric_key, metric_key)
            digits = value_round_map.get(metric_key)
            row[label] = _format_rounded_value(rec.get(metric_key), digits)
        rows.append(row)

    columns = [row_label] + [metric_label_map.get(metric_key, metric_key) for metric_key in metric_order]
    return pd.DataFrame(rows, columns=columns)


def make_dual_label_timeseries_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"dual_label_timeseries_summary columns missing: {missing}")

    row_key_col = str(pivot_cfg.get("row_key_col", "")).strip()
    if not row_key_col or row_key_col not in df.columns:
        raise RuntimeError("dual_label_timeseries_summary requires valid row_key_col")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    if not years:
        raise RuntimeError("dual_label_timeseries_summary requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            allowed = vals if isinstance(vals, list) else [vals]
            d = d[d[col].astype(str).isin([str(v) for v in allowed])].copy()

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[row_key_col] = d[row_key_col].astype(str)

    pv = d.pivot_table(
        index=row_key_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )
    for year in years:
        if year not in pv.columns:
            pv[year] = pd.NA
    pv = pv[years]

    entries = pivot_cfg.get("entries", [])
    if not isinstance(entries, list) or not entries:
        raise RuntimeError("dual_label_timeseries_summary requires non-empty entries")

    group_label = str(pivot_cfg.get("group_label", "구분"))
    detail_label = str(pivot_cfg.get("detail_label", "세부업종"))
    share_year = resolve_period_value(pivot_cfg.get("share_year", years[-1]), available)
    share_base_key = str(pivot_cfg.get("share_base_key", "")).strip()
    include_share = bool(pivot_cfg.get("include_share", True))
    share_label = str(pivot_cfg.get("share_label", "비중"))
    include_cagr = bool(pivot_cfg.get("include_cagr", True))
    value_round = pivot_cfg.get("value_round")
    value_round = int(value_round) if value_round is not None else None
    cagr_label = resolve_cagr_label(pivot_cfg.get("cagr_label"), years)
    cagr_include_keys = [str(x) for x in pivot_cfg.get("cagr_include_keys", [])]
    cagr_exclude_keys = [str(x) for x in pivot_cfg.get("cagr_exclude_keys", [])]
    annual_change_label = str(pivot_cfg.get("annual_change_label", "")).strip()
    annual_change_include_keys = [str(x) for x in pivot_cfg.get("annual_change_include_keys", [])]
    annual_change_exclude_keys = [str(x) for x in pivot_cfg.get("annual_change_exclude_keys", [])]
    annual_change_round = int(pivot_cfg.get("annual_change_round", 2))
    periods = max(int(years[-1]) - int(years[0]), 1)

    base_val = pd.to_numeric(pv.loc[share_base_key, share_year], errors="coerce") if share_base_key and share_base_key in pv.index else pd.NA

    rows: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("key", "")).strip()
        if not key or key not in pv.index:
            continue
        rec = pv.loc[key]
        row: Dict[str, Any] = {
            group_label: entry.get("group", ""),
            detail_label: entry.get("detail", ""),
        }
        for year in years:
            row[f"{year}년"] = _format_rounded_value(rec.get(year), value_round)

        latest_val = pd.to_numeric(rec.get(share_year), errors="coerce")
        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
        calc_key = key
        if include_share:
            row[share_label] = (
                round((latest_val / base_val) * 100, 1)
                if pd.notna(latest_val) and pd.notna(base_val) and base_val not in (0, 0.0)
                else pd.NA
            )
        if include_cagr:
            if _should_include_calc(calc_key, cagr_include_keys, cagr_exclude_keys):
                row[cagr_label] = (
                    round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
                    if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
                    else pd.NA
                )
            else:
                row[cagr_label] = pd.NA
        if annual_change_label:
            if _should_include_calc(calc_key, annual_change_include_keys, annual_change_exclude_keys):
                row[annual_change_label] = (
                    round((end_val - start_val) / periods, annual_change_round)
                    if pd.notna(start_val) and pd.notna(end_val)
                    else pd.NA
                )
            else:
                row[annual_change_label] = pd.NA
        rows.append(row)

    columns = [group_label, detail_label] + [f"{y}년" for y in years]
    if include_share:
        columns.append(share_label)
    if include_cagr:
        columns.append(cagr_label)
    if annual_change_label:
        columns.append(annual_change_label)
    return pd.DataFrame(rows, columns=columns)


def make_region_year_metric_matrix_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"region_year_metric_matrix columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "")).strip()
    metric_col = str(pivot_cfg.get("metric_col", "")).strip()
    if not region_col or region_col not in df.columns:
        raise RuntimeError("region_year_metric_matrix requires valid region_col")
    if not metric_col or metric_col not in df.columns:
        raise RuntimeError("region_year_metric_matrix requires valid metric_col")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    years = resolve_period_list([str(y) for y in pivot_cfg.get("years", [])], available)
    metric_order = [str(x) for x in pivot_cfg.get("metric_order", [])]
    if not years or not metric_order:
        raise RuntimeError("region_year_metric_matrix requires years and metric_order")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            allowed = vals if isinstance(vals, list) else [vals]
            d = d[d[col].astype(str).isin([str(v) for v in allowed])].copy()

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[region_col] = d[region_col].astype(str)
    d[metric_col] = d[metric_col].astype(str)

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(d[region_col].astype(str)))

    pv = d.pivot_table(
        index=region_col,
        columns=["PRD_DE", metric_col],
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )

    for year in years:
        for metric in metric_order:
            if (year, metric) not in pv.columns:
                pv[(year, metric)] = pd.NA
    pv = pv.reindex(columns=pd.MultiIndex.from_tuples([(year, metric) for year in years for metric in metric_order]))

    rows = []
    for region in region_order:
        if region not in pv.index:
            continue
        rows.append((region, pv.loc[region]))

    subtotals = pivot_cfg.get("subtotals", [])
    for subtotal in subtotals:
        if not isinstance(subtotal, dict):
            continue
        members = [str(x) for x in subtotal.get("members", []) if str(x) in pv.index]
        if not members:
            continue
        agg = str(subtotal.get("agg", "mean")).strip().lower()
        block = pv.loc[members]
        if agg in {"sum"}:
            series = block.sum(axis=0, min_count=1)
        else:
            series = block.mean(axis=0)
        rows.append((str(subtotal.get("label", "소계")), series))

    row_label = str(pivot_cfg.get("row_label", "구분"))
    out = pd.DataFrame([series for _, series in rows], index=[name for name, _ in rows])
    out.index.name = row_label

    year_label_map = {str(k): str(v) for k, v in pivot_cfg.get("year_label_map", {}).items()}
    metric_label_map = {str(k): str(v) for k, v in pivot_cfg.get("metric_label_map", {}).items()}
    out.columns = pd.MultiIndex.from_tuples(
        [
            (year_label_map.get(str(year), str(year)), metric_label_map.get(str(metric), str(metric)))
            for year, metric in out.columns
        ]
    )

    value_round_map = {str(k): int(v) for k, v in pivot_cfg.get("value_round_map", {}).items()}
    for metric in metric_order:
        digits = value_round_map.get(metric)
        if digits is None:
            continue
        col_label = metric_label_map.get(metric, metric)
        for year in years:
            year_label = year_label_map.get(year, year)
            if (year_label, col_label) in out.columns:
                out[(year_label, col_label)] = out[(year_label, col_label)].map(lambda v: _format_rounded_value(v, digits))

    return out.reset_index()


def make_dual_label_latest_compare_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"dual_label_latest_compare columns missing: {missing}")

    row_key_col = str(pivot_cfg.get("row_key_col", "")).strip()
    metric_col = str(pivot_cfg.get("metric_col", "")).strip()
    if not row_key_col or row_key_col not in df.columns:
        raise RuntimeError("dual_label_latest_compare requires valid row_key_col")
    if not metric_col or metric_col not in df.columns:
        raise RuntimeError("dual_label_latest_compare requires valid metric_col")

    available = available_periods(df["PRD_DE"].astype(str).tolist())
    period = resolve_period_value(pivot_cfg.get("period", ""), available).strip()
    if not period:
        raise RuntimeError("dual_label_latest_compare requires period")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"] == period].copy()

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            allowed = vals if isinstance(vals, list) else [vals]
            d = d[d[col].astype(str).isin([str(v) for v in allowed])].copy()

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[row_key_col] = d[row_key_col].astype(str)
    d[metric_col] = d[metric_col].astype(str)

    entries = pivot_cfg.get("entries", [])
    if not isinstance(entries, list) or not entries:
        raise RuntimeError("dual_label_latest_compare requires non-empty entries")

    metric_order = [str(x) for x in pivot_cfg.get("metric_order", [])]
    if not metric_order:
        metric_order = list(dict.fromkeys(d[metric_col].astype(str)))

    pv = d.pivot_table(
        index=row_key_col,
        columns=metric_col,
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )

    group_label = str(pivot_cfg.get("group_label", "구분"))
    detail_label = str(pivot_cfg.get("detail_label", "세부"))
    metric_label_map = {str(k): str(v) for k, v in pivot_cfg.get("metric_label_map", {}).items()}
    value_round_map = {str(k): int(v) for k, v in pivot_cfg.get("value_round_map", {}).items()}
    share_spec = pivot_cfg.get("share")
    share_label = ""
    share_num_metric = ""
    share_den_metric = ""
    share_round = 1
    share_percent = True
    if isinstance(share_spec, dict):
        share_label = str(share_spec.get("label", "")).strip()
        share_num_metric = str(share_spec.get("numerator_metric", "")).strip()
        share_den_metric = str(share_spec.get("denominator_metric", "")).strip()
        share_round = int(share_spec.get("round", 1))
        share_percent = bool(share_spec.get("as_percent", True))

    rows: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("key", "")).strip()
        if not key or key not in pv.index:
            continue
        rec = pv.loc[key]
        row: Dict[str, Any] = {
            group_label: entry.get("group", ""),
            detail_label: entry.get("detail", ""),
        }
        for metric_key in metric_order:
            label = metric_label_map.get(metric_key, metric_key)
            digits = value_round_map.get(metric_key)
            row[label] = _format_rounded_value(rec.get(metric_key), digits)

        if share_label and share_num_metric and share_den_metric:
            num = pd.to_numeric(rec.get(share_num_metric), errors="coerce")
            den = pd.to_numeric(rec.get(share_den_metric), errors="coerce")
            if pd.notna(num) and pd.notna(den) and den not in (0, 0.0):
                share_val = (num / den) * (100 if share_percent else 1)
                row[share_label] = _format_rounded_value(share_val, share_round)
            else:
                row[share_label] = pd.NA

        rows.append(row)

    columns = [group_label, detail_label] + [metric_label_map.get(metric_key, metric_key) for metric_key in metric_order]
    if share_label:
        columns.append(share_label)
    return pd.DataFrame(rows, columns=columns)

