import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Optional

import pandas as pd


def map_age_to_10y_bucket(label: Any) -> Optional[str]:
    s = str(label).strip()
    if not s:
        return None
    if s in ("??", "??", "?"):
        return "??"

    nums = [int(x) for x in re.findall(r"\d+", s)]
    if "??" in s:
        if nums and nums[0] <= 9:
            return "9???"
    if "??" in s or "+" in s:
        if nums and nums[0] >= 100:
            return "100???"
        if nums:
            start = (nums[0] // 10) * 10
            if start == 0:
                return "9???"
            if start >= 100:
                return "100???"
            return f"{start}-{start+9}?"

    if len(nums) >= 2:
        start = (nums[0] // 10) * 10
        if start == 0:
            return "9???"
        if start >= 100:
            return "100???"
        return f"{start}-{start+9}?"

    if len(nums) == 1:
        n = nums[0]
        if n <= 9:
            return "9???"
        if n >= 100:
            return "100???"
        start = (n // 10) * 10
        return f"{start}-{start+9}?"

    return None


def apply_preprocess(df: pd.DataFrame, job: dict) -> pd.DataFrame:
    cfg = job.get("preprocess", {})
    if not isinstance(cfg, dict) or not cfg:
        return df

    d = df.copy()

    region_cfg = cfg.get("dedupe_region_names")
    if region_cfg:
        if not isinstance(region_cfg, dict):
            raise RuntimeError("preprocess.dedupe_region_names must be a dict")
        code_col = str(region_cfg.get("code_col", "C1"))
        name_col = str(region_cfg.get("name_col", "C1_NM"))
        label_col = str(region_cfg.get("label_col", f"{name_col}_LABEL"))
        if code_col not in d.columns or name_col not in d.columns:
            raise RuntimeError(f"dedupe_region_names columns missing: {code_col}, {name_col}")

        def normalize_region_name(v: Any) -> str:
            s = str(v).strip()
            s = re.sub(r"\s{2,}", "", s)
            return s

        def shorten_region_name(v: str) -> str:
            s = normalize_region_name(v)
            for suffix in ["?????", "?????", "???", "???", "?????", "?????", "?"]:
                if s.endswith(suffix):
                    return s[: -len(suffix)]
            return s

        d[label_col] = d[name_col].map(normalize_region_name)
        parent_map: Dict[str, str] = {}
        for _, rec in d[[code_col, name_col]].drop_duplicates().iterrows():
            code = str(rec[code_col]).strip().replace("'", "")
            name = normalize_region_name(rec[name_col])
            if len(code) == 2 and code != "00":
                parent_map[code] = name

        counts = d[label_col].value_counts(dropna=False)

        def build_label(rec: pd.Series) -> str:
            code = str(rec[code_col]).strip().replace("'", "")
            name = str(rec[label_col])
            if code in ("", "00") or len(code) <= 2 or counts.get(name, 0) <= 1:
                return name
            parent_name = parent_map.get(code[:2], "")
            if not parent_name:
                return name
            return f"{shorten_region_name(parent_name)}_{name}"

        d[label_col] = d.apply(build_label, axis=1)

    age_cfg = cfg.get("age_bucket_10y")
    if age_cfg:
        if not isinstance(age_cfg, dict):
            raise RuntimeError("preprocess.age_bucket_10y must be a dict")
        src = age_cfg.get("source", "C2_NM")
        if src not in d.columns:
            raise RuntimeError(f"preprocess source column missing: {src}")

        d[src] = d[src].map(map_age_to_10y_bucket)
        if age_cfg.get("drop_unknown", True):
            d = d[d[src].notna()].copy()

        order = [
            "??",
            "9???",
            "10-19?",
            "20-29?",
            "30-39?",
            "40-49?",
            "50-59?",
            "60-69?",
            "70-79?",
            "80-89?",
            "90-99?",
            "100???",
        ]
        d[src] = pd.Categorical(d[src], categories=order, ordered=True)

        if "DT" in d.columns:
            d["DT"] = pd.to_numeric(d["DT"], errors="coerce").fillna(0)
            group_cols = [c for c in d.columns if c != "DT"]
            d = d.groupby(group_cols, as_index=False, dropna=False, sort=False, observed=True)["DT"].sum()
            d[src] = pd.Categorical(d[src], categories=order, ordered=True)

            sort_cols = [c for c in ["C1_NM", src, "PRD_DE", "ITM_NM"] if c in d.columns]
            if sort_cols:
                d = d.sort_values(sort_cols, kind="stable")

    quarter_avg_cfg = cfg.get("quarter_to_year_average")
    if quarter_avg_cfg:
        if not isinstance(quarter_avg_cfg, dict):
            raise RuntimeError("preprocess.quarter_to_year_average must be a dict")
        src = str(quarter_avg_cfg.get("source", "PRD_DE"))
        if src not in d.columns:
            raise RuntimeError(f"preprocess source column missing: {src}")
        if "DT" not in d.columns:
            raise RuntimeError("quarter_to_year_average requires DT column")

        keep_years = [str(x) for x in quarter_avg_cfg.get("keep_years", []) if str(x).strip()]
        d[src] = d[src].astype(str)
        d = d[d[src].str.match(r"^\d{6}$", na=False)].copy()
        d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
        d["__YEAR__"] = d[src].str[:4]
        if keep_years:
            d = d[d["__YEAR__"].isin(keep_years)].copy()

        key_cols = quarter_avg_cfg.get("group_cols")
        if isinstance(key_cols, list) and key_cols:
            group_cols = [str(c) for c in key_cols if str(c) in d.columns]
        else:
            preferred = ["ORG_ID", "TBL_ID", "ITM_ID", "ITM_NM", "C1", "C1_NM", "C2", "C2_NM", "UNIT_NM", "TBL_NM"]
            group_cols = [c for c in preferred if c in d.columns]
        d = d.groupby(group_cols + ["__YEAR__"], as_index=False, dropna=False, sort=False, observed=True)["DT"].mean()
        d = d.rename(columns={"__YEAR__": src})

    scale_dt_cfg = cfg.get("scale_dt")
    if scale_dt_cfg:
        if "DT" not in d.columns:
            raise RuntimeError("scale_dt requires DT column")
        if isinstance(scale_dt_cfg, dict):
            factor = float(scale_dt_cfg.get("factor", 1.0))
        else:
            factor = float(scale_dt_cfg)
        d["DT"] = pd.to_numeric(d["DT"], errors="coerce") * factor

    compose_cfg = cfg.get("compose_column")
    if compose_cfg:
        if not isinstance(compose_cfg, dict):
            raise RuntimeError("preprocess.compose_column must be a dict")
        target = str(compose_cfg.get("target", "")).strip()
        template = str(compose_cfg.get("template", "")).strip()
        if not target or not template:
            raise RuntimeError("compose_column requires target and template")

        def build_value(rec: pd.Series) -> str:
            out = template
            for key in re.findall(r"\{([^{}]+)\}", template):
                out = out.replace("{" + key + "}", str(rec.get(key, "")))
            return out

        d[target] = d.apply(build_value, axis=1)

    category_map_agg_cfg = cfg.get("category_map_aggregate")
    if not category_map_agg_cfg and cfg.get("category_map_sum"):
        category_map_agg_cfg = dict(cfg["category_map_sum"])
        category_map_agg_cfg.setdefault("agg", "sum")
    if category_map_agg_cfg:
        if not isinstance(category_map_agg_cfg, dict):
            raise RuntimeError("preprocess.category_map_aggregate must be a dict")
        src = str(category_map_agg_cfg.get("source", "")).strip()
        if not src or src not in d.columns:
            raise RuntimeError(f"preprocess source column missing: {src}")
        mapping = category_map_agg_cfg.get("mapping", {})
        if not isinstance(mapping, dict) or not mapping:
            raise RuntimeError("category_map_aggregate requires non-empty mapping")
        target = str(category_map_agg_cfg.get("target", src)).strip() or src
        keep_unmapped = bool(category_map_agg_cfg.get("keep_unmapped", False))
        if "DT" not in d.columns:
            raise RuntimeError("category_map_aggregate requires DT column")
        agg = str(category_map_agg_cfg.get("agg", "sum")).strip().lower()

        d[target] = d[src].astype(str).map({str(k): str(v) for k, v in mapping.items()})
        if keep_unmapped:
            d[target] = d[target].fillna(d[src].astype(str))
        else:
            d = d[d[target].notna()].copy()

        key_cols = category_map_agg_cfg.get("group_cols")
        if isinstance(key_cols, list) and key_cols:
            group_cols = [str(c) for c in key_cols if str(c) in d.columns and str(c) not in {"DT", src}]
        else:
            group_cols = [c for c in d.columns if c not in {"DT", src, "LST_CHN_DE"}]
            if target != src:
                group_cols = [c for c in group_cols if c != target] + [target]

        d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
        round_before_aggregate = category_map_agg_cfg.get("round_before_aggregate")
        if round_before_aggregate is not None:
            digits = int(round_before_aggregate)
            quant = Decimal("1") if digits == 0 else Decimal("1").scaleb(-digits)

            def _round_half_up(val: Any) -> Any:
                if pd.isna(val):
                    return val
                return float(Decimal(str(float(val))).quantize(quant, rounding=ROUND_HALF_UP))

            d["DT"] = d["DT"].map(_round_half_up)
        grouped = d.groupby(group_cols, as_index=False, dropna=False, sort=False, observed=True)["DT"]
        if agg in {"mean", "avg", "average"}:
            d = grouped.mean()
        else:
            d = grouped.sum()

    return d


def flatten_for_block(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.columns, pd.MultiIndex):
        return df.reset_index(drop=False) if df.index.name or isinstance(df.index, pd.MultiIndex) else df.copy()
    out = df.copy().reset_index()
    out.columns = ["_".join([str(x) for x in col if str(x) != ""]).strip("_") if isinstance(col, tuple) else str(col) for col in out.columns]
    return out


def substitute_template(value: Any, mapping: Dict[str, Any]) -> Any:
    if isinstance(value, str):
        out = value
        for k, v in mapping.items():
            out = out.replace(f"{{{{{k}}}}}", str(v))
        return out
    if isinstance(value, list):
        return [substitute_template(v, mapping) for v in value]
    if isinstance(value, dict):
        return {k: substitute_template(v, mapping) for k, v in value.items()}
    return value
