from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from runner_core.preprocess.filters import apply_row_filters


def _coerce_rank_value(value: Any) -> Any:
    rank_num = pd.to_numeric(value, errors="coerce")
    if pd.isna(rank_num):
        return pd.NA
    return int(rank_num)


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

def make_year_gender_mix_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["ITM_NM", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"year gender mix columns missing: {missing}")

    index_col = str(pivot_cfg.get("index_col", "ITM_NM"))
    sex_col = str(pivot_cfg.get("sex_col", "C2_NM"))
    region_col = str(pivot_cfg.get("region_col", "C1_NM"))
    if index_col not in df.columns or sex_col not in df.columns or region_col not in df.columns:
        raise RuntimeError("year gender mix required columns missing")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    detail_year = str(pivot_cfg.get("detail_year", years[-1] if years else ""))
    total_label = str(pivot_cfg.get("total_label", "계"))
    detail_labels = [str(x) for x in pivot_cfg.get("detail_labels", ["남자", "여자"])]
    item_order = [str(x) for x in pivot_cfg.get("item_order", [])]
    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    keep_years = list(dict.fromkeys(years + [detail_year]))
    d = d[d["PRD_DE"].isin(keep_years)].copy()

    rows = item_order or list(dict.fromkeys(d[index_col].astype(str)))
    regions = region_order or list(dict.fromkeys(d[region_col].astype(str)))

    col_tuples: List[Tuple[str, str]] = []
    for region in regions:
        for year in years:
            col_tuples.append((region, year))
        for label in detail_labels:
            col_tuples.append((region, f"{detail_year} {label}"))

    out = pd.DataFrame(index=rows, columns=pd.MultiIndex.from_tuples(col_tuples), dtype="object")
    for region in regions:
        block = d[d[region_col].astype(str) == region].copy()
        for item in rows:
            item_df = block[block[index_col].astype(str) == item].copy()
            for year in years:
                val = item_df[(item_df["PRD_DE"] == year) & (item_df[sex_col].astype(str) == total_label)]["DT"]
                out.loc[item, (region, year)] = val.iloc[0] if not val.empty else pd.NA
            for label in detail_labels:
                val = item_df[(item_df["PRD_DE"] == detail_year) & (item_df[sex_col].astype(str) == label)]["DT"]
                out.loc[item, (region, f"{detail_year} {label}")] = val.iloc[0] if not val.empty else pd.NA

    out.index.name = str(pivot_cfg.get("row_label", "항목"))
    return out

def make_latest_profile_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["ITM_ID", "ITM_NM", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"latest profile summary columns missing: {missing}")

    year = str(pivot_cfg.get("year", "")).strip()
    if not year:
        raise RuntimeError("latest profile summary requires year")

    sex_col = str(pivot_cfg.get("sex_col", "C2_NM"))
    total_label = str(pivot_cfg.get("total_label", "계"))
    male_label = str(pivot_cfg.get("male_label", "남자"))
    female_label = str(pivot_cfg.get("female_label", "여자"))
    item_order = [str(x) for x in pivot_cfg.get("item_order", [])]
    total_item_id = str(pivot_cfg.get("total_item_id", "")).strip()
    if not total_item_id:
        raise RuntimeError("latest profile summary requires total_item_id")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d = d[d["PRD_DE"] == year].copy()

    rows: List[Dict[str, Any]] = []
    total_series = d[(d["ITM_ID"].astype(str) == total_item_id) & (d[sex_col].astype(str) == total_label)]["DT"]
    grand_total = pd.to_numeric(total_series.iloc[0], errors="coerce") if not total_series.empty else pd.NA

    for item_id in item_order:
        block = d[d["ITM_ID"].astype(str) == item_id].copy()
        if block.empty:
            continue
        item_name = str(block["ITM_NM"].iloc[0])
        total_val = pd.to_numeric(block[block[sex_col].astype(str) == total_label]["DT"].iloc[0], errors="coerce") if not block[block[sex_col].astype(str) == total_label].empty else pd.NA
        male_val = pd.to_numeric(block[block[sex_col].astype(str) == male_label]["DT"].iloc[0], errors="coerce") if not block[block[sex_col].astype(str) == male_label].empty else pd.NA
        female_val = pd.to_numeric(block[block[sex_col].astype(str) == female_label]["DT"].iloc[0], errors="coerce") if not block[block[sex_col].astype(str) == female_label].empty else pd.NA

        row = {"항목": item_name, "계": total_val}
        row["비중"] = round((total_val / grand_total) * 100, 1) if pd.notna(total_val) and pd.notna(grand_total) and grand_total not in (0, 0.0) else pd.NA
        row["남자 인구수"] = male_val
        row["여자 인구수"] = female_val
        row["성비"] = round((male_val / female_val) * 100, 1) if pd.notna(male_val) and pd.notna(female_val) and female_val not in (0, 0.0) else pd.NA
        rows.append(row)

    return pd.DataFrame(rows, columns=["항목", "계", "비중", "남자 인구수", "여자 인구수", "성비"])

def make_timeseries_profile_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["ITM_ID", "ITM_NM", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"timeseries profile summary columns missing: {missing}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("timeseries profile summary requires years")

    sex_col = str(pivot_cfg.get("sex_col", "C2_NM"))
    total_label = str(pivot_cfg.get("total_label", "계"))
    male_label = str(pivot_cfg.get("male_label", "남자"))
    female_label = str(pivot_cfg.get("female_label", "여자"))
    item_order = [str(x) for x in pivot_cfg.get("item_order", [])]
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    periods = max(int(years[-1]) - int(years[0]), 1)

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d = d[d["PRD_DE"].isin(years)].copy()

    rows: List[Dict[str, Any]] = []
    latest_year = years[-1]
    for item_id in item_order:
        block = d[d["ITM_ID"].astype(str) == item_id].copy()
        if block.empty:
            continue
        item_name = str(block["ITM_NM"].iloc[0])
        row: Dict[str, Any] = {"구분": item_name}
        for year in years:
            val = block[(block["PRD_DE"] == year) & (block[sex_col].astype(str) == total_label)]["DT"]
            row[f"{year}년 총인구수"] = val.iloc[0] if not val.empty else pd.NA
        male_val = block[(block["PRD_DE"] == latest_year) & (block[sex_col].astype(str) == male_label)]["DT"]
        female_val = block[(block["PRD_DE"] == latest_year) & (block[sex_col].astype(str) == female_label)]["DT"]
        male_num = pd.to_numeric(male_val.iloc[0], errors="coerce") if not male_val.empty else pd.NA
        female_num = pd.to_numeric(female_val.iloc[0], errors="coerce") if not female_val.empty else pd.NA
        row["성비"] = round((male_num / female_num) * 100, 1) if pd.notna(male_num) and pd.notna(female_num) and female_num not in (0, 0.0) else pd.NA
        start_val = pd.to_numeric(row.get(f"{years[0]}년 총인구수"), errors="coerce")
        end_val = pd.to_numeric(row.get(f"{years[-1]}년 총인구수"), errors="coerce")
        row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1) if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0 else pd.NA
        rows.append(row)

    cols = ["구분"] + [f"{y}년 총인구수" for y in years] + ["성비", cagr_label]
    return pd.DataFrame(rows, columns=cols)

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

def make_ratio_timeseries_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"ratio timeseries columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM"))
    if region_col not in df.columns:
        raise RuntimeError(f"ratio timeseries region column missing: {region_col}")

    groupby = pivot_cfg.get("groupby", [region_col, "PRD_DE"])
    if not isinstance(groupby, list) or not groupby:
        raise RuntimeError("ratio timeseries requires non-empty groupby")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("ratio timeseries requires non-empty years")

    numerator_filters = pivot_cfg.get("numerator_filters", {})
    denominator_filters = pivot_cfg.get("denominator_filters", {})

    work = df.copy()
    work["PRD_DE"] = work["PRD_DE"].astype(str)
    work["DT"] = pd.to_numeric(work["DT"], errors="coerce").fillna(0)
    work = work[work["PRD_DE"].isin(years)].copy()

    num = apply_row_filters(work, numerator_filters)
    den = apply_row_filters(work, denominator_filters)

    num = num.groupby(groupby, as_index=False, dropna=False, observed=True)["DT"].sum().rename(columns={"DT": "NUM"})
    den = den.groupby(groupby, as_index=False, dropna=False, observed=True)["DT"].sum().rename(columns={"DT": "DEN"})
    merged = num.merge(den, on=groupby, how="outer")
    merged["VALUE"] = merged["NUM"] / merged["DEN"]

    pv = merged.pivot_table(
        index=region_col,
        columns="PRD_DE",
        values="VALUE",
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
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    stage_label = str(pivot_cfg.get("stage_label", "소멸위험 5단계"))
    latest_year = str(pivot_cfg.get("latest_year", years[-1]))

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(work[region_col].astype(str)))

    pv["__order"] = pv[region_col].astype(str).map({name: i for i, name in enumerate(region_order)})
    pv["__order"] = pv["__order"].fillna(9999)
    pv = pv.sort_values("__order", kind="stable")

    rows: List[Dict[str, Any]] = []
    for _, rec in pv.iterrows():
        row_name = national_alias if str(rec[region_col]) == national_name else str(rec[region_col])
        row: Dict[str, Any] = {area_label: row_name}
        for y in years:
            val = pd.to_numeric(rec.get(y), errors="coerce")
            row[f"{y}년"] = round(val, 2) if pd.notna(val) else pd.NA

        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
        if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0:
            row[cagr_label] = round((((end_val / start_val) ** (1 / max(int(years[-1]) - int(years[0]), 1))) - 1) * 100, 1)
        else:
            row[cagr_label] = pd.NA

        latest_val = pd.to_numeric(rec.get(latest_year), errors="coerce")
        if pd.isna(latest_val):
            row[stage_label] = pd.NA
        elif latest_val < 0.2:
            row[stage_label] = "고위험"
        elif latest_val < 0.5:
            row[stage_label] = "위험진입"
        elif latest_val < 1.0:
            row[stage_label] = "주의단계"
        elif latest_val < 1.5:
            row[stage_label] = "보통"
        else:
            row[stage_label] = "저위험"

        rows.append(row)

    subtotal = pivot_cfg.get("subtotal", {})
    if isinstance(subtotal, dict):
        members = [str(x) for x in subtotal.get("members", [])]
        label = str(subtotal.get("label", "")).strip()
        if members and label:
            sub = pv[pv[region_col].astype(str).isin(members)].copy()
            if not sub.empty:
                row = {area_label: label}
                for y in years:
                    vals = pd.to_numeric(sub[y], errors="coerce")
                    row[f"{y}년"] = round(vals.mean(), 2) if vals.notna().any() else pd.NA
                start_val = pd.to_numeric(row.get(f"{years[0]}년"), errors="coerce")
                end_val = pd.to_numeric(row.get(f"{years[-1]}년"), errors="coerce")
                if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0:
                    row[cagr_label] = round((((end_val / start_val) ** (1 / max(int(years[-1]) - int(years[0]), 1))) - 1) * 100, 1)
                else:
                    row[cagr_label] = pd.NA
                latest_val = pd.to_numeric(row.get(f"{latest_year}년"), errors="coerce")
                if pd.isna(latest_val):
                    row[stage_label] = pd.NA
                elif latest_val < 0.2:
                    row[stage_label] = "고위험"
                elif latest_val < 0.5:
                    row[stage_label] = "위험진입"
                elif latest_val < 1.0:
                    row[stage_label] = "주의단계"
                elif latest_val < 1.5:
                    row[stage_label] = "보통"
                else:
                    row[stage_label] = "저위험"
                rows.append(row)

    cols = [area_label] + [f"{y}년" for y in years] + [cagr_label, stage_label]
    return pd.DataFrame(rows, columns=cols)

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

def make_single_metric_share_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
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
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    periods = max(int(years[-1]) - int(years[0]), 1)
    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(pv.index.astype(str))

    national_val = pd.to_numeric(pv.loc[national_name, share_year], errors="coerce") if national_name in pv.index else pd.NA

    rows: List[Dict[str, Any]] = []
    for region in region_order:
        if region not in pv.index:
            continue
        rec = pv.loc[region]
        row: Dict[str, Any] = {area_label: national_alias if region == national_name else region}
        for year in years:
            row[f"{year}년"] = rec.get(year)

        latest_val = pd.to_numeric(rec.get(share_year), errors="coerce")
        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
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
        subtotal_frame = pv.loc[members, years].apply(pd.to_numeric, errors="coerce")
        if agg in {"mean", "avg", "average"}:
            subtotal_vals = subtotal_frame.mean(axis=0)
        else:
            subtotal_vals = subtotal_frame.sum(axis=0, min_count=1)
        row = {area_label: str(subtotal.get("label", "소계"))}
        for year in years:
            row[f"{year}년"] = subtotal_vals.get(year)
        latest_val = pd.to_numeric(subtotal_vals.get(share_year), errors="coerce")
        start_val = pd.to_numeric(subtotal_vals.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(subtotal_vals.get(years[-1]), errors="coerce")
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

    return pd.DataFrame(rows, columns=[area_label] + [f"{y}년" for y in years] + ["비중", cagr_label])

