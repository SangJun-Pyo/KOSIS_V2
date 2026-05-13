from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from runner_core.preprocess.filters import apply_row_filters


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

