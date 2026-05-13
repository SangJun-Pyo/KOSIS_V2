from typing import Optional

import pandas as pd

def make_default_pivot(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if "DT" not in df.columns or "PRD_DE" not in df.columns:
        return None
    if "C1_NM" not in df.columns:
        return None

    d = df.copy()
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d["PRD_DE"] = d["PRD_DE"].astype(str)

    pv = (
        d.pivot_table(
            index="C1_NM",
            columns="PRD_DE",
            values="DT",
            aggfunc="first",
            observed=True,
        )
        .sort_index()
    )

    def fmt_prd(x: str) -> str:
        x = str(x)
        return f"{x[:4]}.{x[4:]}" if len(x) >= 6 else x

    pv.columns = [fmt_prd(c) for c in pv.columns]
    return pv.reset_index()

def make_custom_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    idx = pivot_cfg.get("index", [])
    cols = pivot_cfg.get("columns", [])
    val = pivot_cfg.get("values", "DT")

    if not isinstance(idx, list) or not isinstance(cols, list):
        raise RuntimeError("pivot.index / pivot.columns 는 반드시 리스트여야 합니다.")

    need = set(idx + cols + [val])
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise RuntimeError(f"Pivot columns missing: {missing}")

    d = df.copy()

    # Optional row filters before pivot, e.g. {"PRD_DE": ["2015", "2025"]}
    filters = pivot_cfg.get("filters", {})
    if filters:
        if not isinstance(filters, dict):
            raise RuntimeError("pivot.filters must be a dict")
        for col, allowed in filters.items():
            if col not in d.columns:
                raise RuntimeError(f"pivot.filters column missing: {col}")
            allowed_vals = allowed if isinstance(allowed, list) else [allowed]
            allowed_vals = [str(v) for v in allowed_vals]
            d = d[d[col].astype(str).isin(allowed_vals)]

    if val in d.columns:
        d[val] = pd.to_numeric(d[val], errors="coerce")
    if "PRD_DE" in d.columns:
        d["PRD_DE"] = d["PRD_DE"].astype(str)

    sort_opt = bool(pivot_cfg.get("sort", True))
    pv = d.pivot_table(index=idx, columns=cols, values=val, aggfunc="first", sort=sort_opt, observed=True)

    preserve_multi = bool(pivot_cfg.get("preserve_multiindex_columns", False))

    # 컬럼 평탄화
    if isinstance(pv.columns, pd.MultiIndex):
        if not preserve_multi:
            pv.columns = ["_".join(map(str, c)).strip() for c in pv.columns.values]
    else:
        pv.columns = [str(c) for c in pv.columns]

    label_map = pivot_cfg.get("column_label_map", {})
    if isinstance(label_map, dict) and label_map:
        if isinstance(pv.columns, pd.MultiIndex):
            pass
        else:
            pv.columns = [str(label_map.get(str(c), c)) for c in pv.columns]

    # (옵션) 월 포맷
    if pivot_cfg.get("flatten_columns_year", False):
        def fmt_prd2(x: str) -> str:
            x = str(x)
            return f"{x[:4]}.{x[4:]}" if len(x) >= 6 and x.isdigit() else x
        pv.columns = [fmt_prd2(c) for c in pv.columns]

    if preserve_multi:
        return pv

    return pv.reset_index()

