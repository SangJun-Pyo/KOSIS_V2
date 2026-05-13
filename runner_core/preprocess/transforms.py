import re
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
