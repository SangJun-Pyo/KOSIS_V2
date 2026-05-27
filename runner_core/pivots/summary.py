from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from runner_core.preprocess.filters import apply_row_filters


def _format_rounded_value(val: Any, digits: int | None) -> Any:
    num = pd.to_numeric(val, errors="coerce")
    if digits is None or pd.isna(num):
        return val
    quant = Decimal("1") if digits == 0 else Decimal("1").scaleb(-digits)
    rounded = Decimal(str(float(num))).quantize(quant, rounding=ROUND_HALF_UP)
    if digits == 0:
        return int(rounded)
    return float(rounded)


def make_metric_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["C1_NM", "ITM_ID", "ITM_NM", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"summary pivot columns missing: {missing}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
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
    if not cagr_label:
        cagr_label = f"CAGR('{start_year[2:]}~'{end_year[2:]})"

    periods = max(int(end_year) - int(start_year), 1)
    share_year = str(pivot_cfg.get("share_year", end_year))
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

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("metric block summary requires non-empty years")

    item_ids = [str(x) for x in pivot_cfg.get("item_ids", [])]
    if not item_ids:
        raise RuntimeError("metric block summary requires item_ids")

    item_labels = {str(k): str(v) for k, v in pivot_cfg.get("item_labels", {}).items()}
    row_order = [str(x) for x in pivot_cfg.get("row_order", [])]
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
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
        years = [str(y) for y in metric.get("years", [])]
        if not label or not years:
            continue

        cagr_label = str(metric.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
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
        years = [str(y) for y in metric.get("years", [])]
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

def make_age_distribution_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"age distribution summary columns missing: {missing}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
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

    left_rows: List[Dict[str, Any]] = []
    for label in detail_order:
        row: Dict[str, Any] = {"구분": label}
        for year in years:
            row[f"{year}년"] = detail_pv.loc[label, year] if label in detail_pv.index and year in detail_pv.columns else pd.NA
        left_rows.append(row)
    left = pd.DataFrame(left_rows, columns=["구분"] + [f"{y}년" for y in years])

    summary_filters = pivot_cfg.get("summary_filters", {})
    summary_df = apply_row_filters(work, summary_filters)
    total_codes = [str(x) for x in pivot_cfg.get("total_codes", [])]
    total_label = str(pivot_cfg.get("total_label", "계"))
    share_year = str(pivot_cfg.get("share_year", years[-1]))
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    periods = max(int(str(years[-1])[:4]) - int(str(years[0])[:4]), 1)

    total_block = summary_df[summary_df[age_code_col].astype(str).isin(total_codes)].copy()
    total_pv = total_block.groupby(["PRD_DE"], as_index=False, dropna=False, observed=True)["DT"].sum()
    total_map = {str(r["PRD_DE"]): r["DT"] for _, r in total_pv.iterrows()}

    right_rows: List[Dict[str, Any]] = []
    total_row: Dict[str, Any] = {"요약 구분": total_label}
    for year in years:
        total_row[f"{year}년 인구수"] = total_map.get(year, pd.NA)
    total_row["비중"] = 100.0 if pd.notna(total_map.get(share_year)) else pd.NA
    start_val = pd.to_numeric(total_row.get(f"{years[0]}년 인구수"), errors="coerce")
    end_val = pd.to_numeric(total_row.get(f"{years[-1]}년 인구수"), errors="coerce")
    total_row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1) if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0 else pd.NA
    right_rows.append(total_row)

    total_share_base = pd.to_numeric(total_row.get(f"{share_year}년 인구수"), errors="coerce")
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
        row: Dict[str, Any] = {"요약 구분": label}
        for year in years:
            row[f"{year}년 인구수"] = val_map.get(year, pd.NA)
        latest_val = pd.to_numeric(row.get(f"{share_year}년 인구수"), errors="coerce")
        row["비중"] = round((latest_val / total_share_base) * 100, 1) if pd.notna(latest_val) and pd.notna(total_share_base) and total_share_base not in (0, 0.0) else pd.NA
        start_val = pd.to_numeric(row.get(f"{years[0]}년 인구수"), errors="coerce")
        end_val = pd.to_numeric(row.get(f"{years[-1]}년 인구수"), errors="coerce")
        row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1) if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0 else pd.NA
        right_rows.append(row)

    right = pd.DataFrame(
        right_rows,
        columns=["요약 구분"] + [f"{y}년 인구수" for y in years] + ["비중", cagr_label],
    )

    max_len = max(len(left), len(right))
    left = left.reindex(range(max_len))
    right = right.reindex(range(max_len))
    spacer = pd.DataFrame({"": [pd.NA] * max_len})
    return pd.concat([left, spacer, right], axis=1)

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

    years = [str(y) for y in pivot_cfg.get("years", [])]
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
    share_year = str(pivot_cfg.get("share_year", years[-1]))
    include_share = bool(pivot_cfg.get("include_share", True))
    value_round = pivot_cfg.get("value_round")
    value_round = int(value_round) if value_round is not None else None
    value_round_map = {str(k): int(v) for k, v in pivot_cfg.get("value_round_map", {}).items()}
    include_cagr = bool(pivot_cfg.get("include_cagr", True))
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    periods = max(int(years[-1]) - int(years[0]), 1)
    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(pv.index.astype(str))

    share_base_filters = pivot_cfg.get("share_base_filters", {})
    share_base_year = str(pivot_cfg.get("share_base_year", share_year))
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
        row: Dict[str, Any] = {area_label: national_alias if region == national_name else region}
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


def make_group_metric_share_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"group_metric_share_summary columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM")).strip()
    category_col = str(pivot_cfg.get("category_col", "C2_NM")).strip()
    if region_col not in df.columns or category_col not in df.columns:
        raise RuntimeError("group_metric_share_summary requires valid region/category columns")

    years = [str(y) for y in pivot_cfg.get("years", [])]
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
    share_year = str(pivot_cfg.get("share_year", years[-1]))
    include_share = bool(pivot_cfg.get("include_share", True))
    share_base_category = str(pivot_cfg.get("share_base_category", "계"))
    value_round = pivot_cfg.get("value_round")
    value_round = int(value_round) if value_round is not None else None
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
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

    years = [str(y) for y in pivot_cfg.get("years", [])]
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
        rows.append(row)
        first = False

    year_columns = [year_label_map.get(year, year) for year in years]
    columns = ([group_label] if include_group_label else []) + [item_label] + year_columns
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


def make_latest_metric_share_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"latest_metric_share_summary columns missing: {missing}")

    category_col = str(pivot_cfg.get("category_col", "")).strip()
    if not category_col or category_col not in df.columns:
        raise RuntimeError("latest_metric_share_summary requires valid category_col")

    period = str(pivot_cfg.get("period", "")).strip()
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

    period = str(pivot_cfg.get("period", "")).strip()
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

    years = [str(y) for y in pivot_cfg.get("years", [])]
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
    share_year = str(pivot_cfg.get("share_year", years[-1]))
    share_base_key = str(pivot_cfg.get("share_base_key", "")).strip()
    include_share = bool(pivot_cfg.get("include_share", True))
    include_cagr = bool(pivot_cfg.get("include_cagr", True))
    value_round = pivot_cfg.get("value_round")
    value_round = int(value_round) if value_round is not None else None
    cagr_label = str(pivot_cfg.get("cagr_label", "CAGR"))
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
        if include_share:
            row["비중"] = (
                round((latest_val / base_val) * 100, 1)
                if pd.notna(latest_val) and pd.notna(base_val) and base_val not in (0, 0.0)
                else pd.NA
            )
        if include_cagr:
            row[cagr_label] = (
                round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
                if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
                else pd.NA
            )
        rows.append(row)

    columns = [group_label, detail_label] + [f"{y}년" for y in years]
    if include_share:
        columns.append("비중")
    if include_cagr:
        columns.append(cagr_label)
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

    years = [str(y) for y in pivot_cfg.get("years", [])]
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

    period = str(pivot_cfg.get("period", "")).strip()
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

