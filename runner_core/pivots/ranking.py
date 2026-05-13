from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from runner_core.preprocess.filters import apply_row_filters


def _coerce_rank_value(value: Any) -> Any:
    rank_num = pd.to_numeric(value, errors="coerce")
    if pd.isna(rank_num):
        return pd.NA
    return int(rank_num)

def make_latest_rank_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["C1_NM", "ITM_ID", "ITM_NM", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"latest rank pivot columns missing: {missing}")

    year = str(pivot_cfg.get("year", "")).strip()
    if not year:
        raise RuntimeError("latest rank pivot requires year")

    item_ids = [str(x) for x in pivot_cfg.get("item_ids", [])]
    if not item_ids:
        raise RuntimeError("latest rank pivot requires item_ids")

    rank_item_id = str(pivot_cfg.get("rank_item_id", "")).strip()
    if not rank_item_id:
        raise RuntimeError("latest rank pivot requires rank_item_id")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[(d["PRD_DE"] == year) & (d["ITM_ID"].astype(str).isin(item_ids))].copy()
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")

    pv = d.pivot_table(
        index="C1_NM",
        columns="ITM_ID",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    ).reset_index()

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(d["C1_NM"].astype(str)))

    for item_id in item_ids:
        if item_id not in pv.columns:
            pv[item_id] = pd.NA

    label_map = pivot_cfg.get("column_labels", {})
    rank_label = str(pivot_cfg.get("rank_label", "순위"))
    area_label = str(pivot_cfg.get("area_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))

    rank_series = pv.loc[pv["C1_NM"].astype(str) != national_name, ["C1_NM", rank_item_id]].copy()
    rank_series["__rank"] = pd.to_numeric(rank_series[rank_item_id], errors="coerce").rank(method="min", ascending=False)
    rank_map = dict(zip(rank_series["C1_NM"].astype(str), rank_series["__rank"]))

    pv["__order"] = pv["C1_NM"].astype(str).map({name: i for i, name in enumerate(region_order)})
    pv["__order"] = pv["__order"].fillna(9999)
    pv = pv.sort_values("__order", kind="stable")

    rows: List[Dict[str, Any]] = []
    for _, rec in pv.iterrows():
        row: Dict[str, Any] = {
            area_label: national_alias if str(rec["C1_NM"]) == national_name else str(rec["C1_NM"])
        }
        for item_id in item_ids:
            row[str(label_map.get(item_id, item_id))] = rec.get(item_id)
        row[rank_label] = pd.NA if str(rec["C1_NM"]) == national_name else _coerce_rank_value(rank_map.get(str(rec["C1_NM"])))
        rows.append(row)

    cols = [area_label] + [str(label_map.get(item_id, item_id)) for item_id in item_ids] + [rank_label]
    return pd.DataFrame(rows, columns=cols)

def make_rank_timeseries_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"rank timeseries columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM"))
    if region_col not in df.columns:
        raise RuntimeError(f"rank timeseries region column missing: {region_col}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("rank timeseries requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d = d[d["PRD_DE"].isin(years)].copy()

    pv = d.pivot_table(
        index=region_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    ).reset_index()

    for y in years:
        if y not in pv.columns:
            pv[y] = pd.NA

    area_label = str(pivot_cfg.get("area_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))
    rank_label = str(pivot_cfg.get("rank_label", "순위"))
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    rank_year = str(pivot_cfg.get("rank_year", years[-1]))

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(d[region_col].astype(str)))

    pv["__order"] = pv[region_col].astype(str).map({name: i for i, name in enumerate(region_order)})
    pv["__order"] = pv["__order"].fillna(9999)
    pv = pv.sort_values("__order", kind="stable")

    rank_series = pv.loc[pv[region_col].astype(str) != national_name, [region_col, rank_year]].copy()
    rank_series["__rank"] = pd.to_numeric(rank_series[rank_year], errors="coerce").rank(method="min", ascending=False)
    rank_map = dict(zip(rank_series[region_col].astype(str), rank_series["__rank"]))

    periods = max(int(years[-1]) - int(years[0]), 1)
    rows: List[Dict[str, Any]] = []
    for _, rec in pv.iterrows():
        row: Dict[str, Any] = {
            area_label: national_alias if str(rec[region_col]) == national_name else str(rec[region_col])
        }
        for y in years:
            row[f"{y}년"] = rec.get(y)

        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
        if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0:
            row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
        else:
            row[cagr_label] = pd.NA

        row[rank_label] = pd.NA if str(rec[region_col]) == national_name else _coerce_rank_value(rank_map.get(str(rec[region_col])))
        rows.append(row)

    cols = [area_label] + [f"{y}년" for y in years] + [rank_label, cagr_label]
    return pd.DataFrame(rows, columns=cols)

def make_rank_and_metric_block_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"rank_and_metric_block summary columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C2_NM")).strip()
    if region_col not in df.columns:
        raise RuntimeError(f"rank_and_metric_block region column missing: {region_col}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("rank_and_metric_block requires non-empty years")

    rank_metric = pivot_cfg.get("rank_metric", {})
    metric_blocks = pivot_cfg.get("metric_blocks", [])
    if not isinstance(rank_metric, dict) or not isinstance(metric_blocks, list) or not metric_blocks:
        raise RuntimeError("rank_and_metric_block requires rank_metric and metric_blocks")

    area_label = str(pivot_cfg.get("area_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))
    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(df[region_col].astype(str)))

    work = df.copy()
    work["PRD_DE"] = work["PRD_DE"].astype(str)
    work["DT"] = pd.to_numeric(work["DT"], errors="coerce")
    work = work[work["PRD_DE"].isin(years)].copy()

    out = pd.DataFrame(index=pd.Index(region_order, name=area_label))

    rank_label = str(rank_metric.get("rank_label", "순위"))
    rank_cagr_label = str(rank_metric.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    rank_title = str(rank_metric.get("label", "지표"))
    rank_year = str(rank_metric.get("rank_year", years[-1]))
    rank_block = apply_row_filters(work, rank_metric.get("filters", {}))
    rank_pv = rank_block.pivot_table(
        index=region_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )
    for year in years:
        if year not in rank_pv.columns:
            rank_pv[year] = pd.NA
    rank_pv = rank_pv.reindex(region_order)

    periods = max(int(str(years[-1])[:4]) - int(str(years[0])[:4]), 1)
    rank_series = rank_pv.loc[rank_pv.index != national_name, rank_year]
    rank_map = rank_series.rank(method="min", ascending=False)

    for year in years:
        out[(rank_title, f"{year}년")] = rank_pv[year]
    out[(rank_title, rank_label)] = [pd.NA if idx == national_name else int(rank_map.get(idx, 0)) for idx in out.index]

    start_vals = pd.to_numeric(rank_pv[years[0]], errors="coerce")
    end_vals = pd.to_numeric(rank_pv[years[-1]], errors="coerce")
    cagr_vals = pd.Series(pd.NA, index=rank_pv.index, dtype="object")
    mask = start_vals.notna() & end_vals.notna() & (start_vals > 0) & (end_vals > 0)
    cagr_vals.loc[mask] = ((((end_vals.loc[mask] / start_vals.loc[mask]) ** (1 / periods)) - 1) * 100).round(1)
    out[(rank_title, rank_cagr_label)] = cagr_vals.reindex(out.index)

    for block in metric_blocks:
        if not isinstance(block, dict):
            continue
        title = str(block.get("label", "")).strip()
        if not title:
            continue
        cagr_label = str(block.get("cagr_label", rank_cagr_label))
        block_df = apply_row_filters(work, block.get("filters", {}))
        pv = block_df.pivot_table(
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
            out[(title, f"{year}년")] = pv[year]
        start_vals = pd.to_numeric(pv[years[0]], errors="coerce")
        end_vals = pd.to_numeric(pv[years[-1]], errors="coerce")
        cagr_vals = pd.Series(pd.NA, index=pv.index, dtype="object")
        mask = start_vals.notna() & end_vals.notna() & (start_vals > 0) & (end_vals > 0)
        cagr_vals.loc[mask] = ((((end_vals.loc[mask] / start_vals.loc[mask]) ** (1 / periods)) - 1) * 100).round(1)
        out[(title, cagr_label)] = cagr_vals.reindex(out.index)

    out.index = [national_alias if str(x) == national_name else str(x) for x in out.index]
    out.columns = pd.MultiIndex.from_tuples(out.columns)
    return out

