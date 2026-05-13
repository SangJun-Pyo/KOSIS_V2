from typing import Dict, List, Optional, Tuple

import pandas as pd

from runner_core.pivots.base import make_custom_pivot, make_default_pivot
from runner_core.pivots.profile import (
    make_latest_profile_summary_pivot,
    make_timeseries_profile_summary_pivot,
    make_year_gender_mix_pivot,
)
from runner_core.pivots.ranking import (
    make_latest_rank_pivot,
    make_rank_and_metric_block_summary_pivot,
    make_rank_timeseries_pivot,
)
from runner_core.pivots.ratio import make_ratio_timeseries_pivot
from runner_core.pivots.summary import (
    make_age_distribution_summary_pivot,
    make_metric_block_summary_pivot,
    make_metric_summary_pivot,
    make_paired_metric_latest_compare_pivot,
    make_paired_metric_timeseries_summary_pivot,
    make_single_metric_share_summary_pivot,
)
from runner_core.preprocess.filters import apply_row_filters, apply_value_maps
from runner_core.preprocess.transforms import apply_preprocess, flatten_for_block, substitute_template

def build_single_source_view(df: pd.DataFrame, spec: dict) -> pd.DataFrame:
    d = apply_row_filters(df, spec.get("filters", {}))
    if isinstance(spec.get("preprocess"), dict):
        d = apply_preprocess(d, {"preprocess": spec["preprocess"]})
    d = apply_value_maps(d, spec)
    kind = str(spec.get("kind", "pivot")).strip().lower()

    if kind == "pivot":
        return make_custom_pivot(d, spec)
    if kind == "sum_pivot":
        group_cols = spec.get("groupby", [])
        if not isinstance(group_cols, list) or not group_cols:
            raise RuntimeError("sum_pivot requires non-empty groupby")
        work = d.copy()
        work["DT"] = pd.to_numeric(work["DT"], errors="coerce").fillna(0)
        work = work.groupby(group_cols, as_index=False, dropna=False, observed=True)["DT"].sum()
        pivot_spec = dict(spec)
        pivot_spec.pop("filters", None)
        pivot_spec.pop("groupby", None)
        pivot_spec.pop("replace_values", None)
        return make_custom_pivot(work, pivot_spec)
    if kind == "rank_timeseries":
        return make_rank_timeseries_pivot(d, spec)
    if kind == "ratio_timeseries":
        return make_ratio_timeseries_pivot(d, spec)
    if kind == "metric_summary":
        return make_metric_summary_pivot(d, spec)
    if kind == "metric_block_summary":
        return make_metric_block_summary_pivot(d, spec)
    if kind == "year_gender_mix":
        return make_year_gender_mix_pivot(d, spec)
    if kind == "latest_profile_summary":
        return make_latest_profile_summary_pivot(d, spec)
    if kind == "timeseries_profile_summary":
        return make_timeseries_profile_summary_pivot(d, spec)
    if kind == "latest_rank":
        return make_latest_rank_pivot(d, spec)
    if kind == "paired_metric_timeseries_summary":
        return make_paired_metric_timeseries_summary_pivot(d, spec)
    if kind == "paired_metric_latest_compare":
        return make_paired_metric_latest_compare_pivot(d, spec)
    if kind == "rank_and_metric_block_summary":
        return make_rank_and_metric_block_summary_pivot(d, spec)
    if kind == "age_distribution_summary":
        return make_age_distribution_summary_pivot(d, spec)
    if kind == "single_metric_share_summary":
        return make_single_metric_share_summary_pivot(d, spec)
    raise RuntimeError(f"unknown view kind: {kind}")

def make_stack_blocks_view(source_frames: Dict[str, pd.DataFrame], spec: dict) -> pd.DataFrame:
    blocks = spec.get("blocks", [])
    if not isinstance(blocks, list) or not blocks:
        raise RuntimeError("stack_blocks requires non-empty blocks")

    rendered: List[pd.DataFrame] = []
    for i, block in enumerate(blocks, start=1):
        if not isinstance(block, dict):
            continue
        src_name = str(block.get("source", "")).strip()
        if not src_name or src_name not in source_frames:
            raise RuntimeError(f"stack_blocks source missing: {src_name}")

        block_df = build_single_source_view(source_frames[src_name].copy(), block)
        if bool(block.get("flatten", True)):
            block_df = flatten_for_block(block_df)

        title = str(block.get("title", "")).strip()
        if title:
            first_col = block_df.columns[0] if len(block_df.columns) else "구분"
            rendered.append(pd.DataFrame([{first_col: title}]))

        rendered.append(block_df)

        blank_rows = int(block.get("blank_rows", 1))
        if blank_rows > 0 and i < len(blocks):
            rendered.append(pd.DataFrame([{} for _ in range(blank_rows)]))

    if not rendered:
        raise RuntimeError("stack_blocks produced no blocks")

    return pd.concat(rendered, ignore_index=True, sort=False)

def build_source_views(source_frames: Dict[str, pd.DataFrame], job: dict) -> Dict[str, pd.DataFrame]:
    views: Dict[str, pd.DataFrame] = {}
    specs = job.get("views", [])
    if not isinstance(specs, list) or not specs:
        return views

    expanded_specs: List[dict] = []
    for spec in specs:
        if not isinstance(spec, dict):
            continue
        repeat_cfg = spec.get("repeat_over")
        if isinstance(repeat_cfg, dict) and isinstance(repeat_cfg.get("items"), list):
            base = dict(spec)
            base.pop("repeat_over", None)
            for item in repeat_cfg.get("items", []):
                if not isinstance(item, dict):
                    continue
                expanded = substitute_template(base, {str(k): v for k, v in item.items()})
                if isinstance(expanded, dict):
                    expanded_specs.append(expanded)
        else:
            expanded_specs.append(spec)

    for i, spec in enumerate(expanded_specs, start=1):
        if not isinstance(spec, dict):
            continue
        sheet_name = str(spec.get("sheet_name", f"TABLE_VIEW_{i}")).strip() or f"TABLE_VIEW_{i}"
        kind = str(spec.get("kind", "pivot")).strip().lower()

        try:
            if kind == "stack_blocks":
                views[sheet_name] = make_stack_blocks_view(source_frames, spec)
            else:
                src_names = []
                if isinstance(spec.get("sources"), list):
                    src_names = [str(x).strip() for x in spec.get("sources", []) if str(x).strip()]
                else:
                    src_name = str(spec.get("source", "")).strip()
                    if src_name:
                        src_names = [src_name]

                if not src_names:
                    print("[WARN] view source missing")
                    continue

                missing = [name for name in src_names if name not in source_frames]
                if missing:
                    print(f"[WARN] view source missing: {', '.join(missing)}")
                    continue

                base_df = pd.concat([source_frames[name].copy() for name in src_names], ignore_index=True)
                views[sheet_name] = build_single_source_view(base_df, spec)
        except Exception as e:
            print(f"[WARN] source view 생성 실패 ({sheet_name}): {e}")

    return views

def build_table_views(df: pd.DataFrame, job: dict) -> Dict[str, pd.DataFrame]:
    views: Dict[str, pd.DataFrame] = {}
    pivot_src = apply_preprocess(df, job)

    pivot_cfg = job.get("pivot")
    primary_sheet_name = "TABLE_VIEW"
    if pivot_cfg and pivot_cfg.get("sheet_name"):
        primary_sheet_name = pivot_cfg["sheet_name"]

    try:
        if pivot_cfg:
            primary_df = make_custom_pivot(pivot_src, pivot_cfg)
        else:
            primary_df = make_default_pivot(pivot_src)
        if primary_df is not None:
            views[primary_sheet_name] = primary_df
    except Exception as e:
        print("[WARN] TABLE_VIEW 생성 실패:", e)

    extra_pivots = job.get("extra_pivots", [])
    if isinstance(extra_pivots, list):
        for i, cfg in enumerate(extra_pivots, start=1):
            if not isinstance(cfg, dict):
                continue
            sheet_name = str(cfg.get("sheet_name", f"TABLE_VIEW_{i}")).strip() or f"TABLE_VIEW_{i}"
            kind = str(cfg.get("kind", "pivot")).strip().lower()
            try:
                if kind == "metric_summary":
                    views[sheet_name] = make_metric_summary_pivot(df, cfg)
                elif kind == "latest_rank":
                    views[sheet_name] = make_latest_rank_pivot(df, cfg)
                else:
                    views[sheet_name] = make_custom_pivot(pivot_src, cfg)
            except Exception as e:
                print(f"[WARN] 추가 피벗 생성 실패 ({sheet_name}): {e}")

    return views

def build_table_view(df: pd.DataFrame, job: dict) -> Tuple[Optional[pd.DataFrame], str]:
    views = build_table_views(df, job)
    if not views:
        return None, "TABLE_VIEW"
    first_sheet = next(iter(views))
    return views[first_sheet], first_sheet

# -----------------------------

