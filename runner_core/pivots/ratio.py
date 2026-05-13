from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from runner_core.preprocess.filters import apply_row_filters


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

