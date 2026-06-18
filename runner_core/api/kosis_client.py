from typing import Any, Dict, Iterable
from datetime import datetime
from xml.etree import ElementTree as ET

import pandas as pd

from runner_core.api.http_client import request_json_with_retry, request_text_with_retry
from runner_core.config import KOSIS_BASE_URL


def _resolve_request_period_param(value: Any, prd_se: str, table_year: int | None = None) -> str:
    text = str(value).strip()
    now = datetime.now()
    quarter = ((now.month - 1) // 3) + 1
    if table_year is not None:
        text = text.replace("{year}", str(table_year))

    if text in {"__CURRENT_YEAR__", "__LATEST_YEAR__"}:
        return str(now.year)
    if text == "__PREVIOUS_YEAR__":
        return str(now.year - 1)
    if text in {"__CURRENT_PERIOD__", "__LATEST_PERIOD__"}:
        prd = str(prd_se or "").upper()
        if prd == "M":
            return f"{now.year}{now.month:02d}"
        if prd == "Q":
            return f"{now.year}.{quarter}/4"
        return str(now.year)
    return text


def _resolve_tblid_text(value: Any, year: int | None = None) -> str:
    text = str(value).strip()
    now_year = datetime.now().year
    resolved_year = year if year is not None else now_year
    return (
        text.replace("__CURRENT_YEAR__", str(now_year))
        .replace("__LATEST_YEAR__", str(now_year))
        .replace("{current_year}", str(now_year))
        .replace("{year}", str(resolved_year))
    )


def _build_tblid_candidates(job_like: dict) -> list[tuple[str, int | None]]:
    candidates: list[tuple[str, int | None]] = []
    if isinstance(job_like.get("tblId_candidates"), Iterable) and not isinstance(job_like.get("tblId_candidates"), (str, bytes)):
        for item in job_like.get("tblId_candidates", []):
            text = _resolve_tblid_text(item)
            candidate = (text, None)
            if text and candidate not in candidates:
                candidates.append(candidate)
        if candidates:
            return candidates

    template = str(job_like.get("tblId_template", "")).strip()
    if template:
        offsets = job_like.get("tblId_year_fallbacks", [0, -1, -2])
        now_year = datetime.now().year
        for offset in offsets if isinstance(offsets, list) else [offsets]:
            try:
                year = now_year + int(offset)
            except Exception:
                continue
            text = _resolve_tblid_text(template, year=year)
            candidate = (text, year)
            if text and candidate not in candidates:
                candidates.append(candidate)
        if candidates:
            return candidates

    tbl_id = _resolve_tblid_text(job_like["tblId"])
    return [(tbl_id, None)] if tbl_id else []


def build_kosis_params(
    job_like: dict,
    api_key: str,
    tbl_id_override: str | None = None,
    table_year: int | None = None,
) -> Dict[str, str]:
    if not api_key:
        raise RuntimeError("KOSIS API key was not found in .env or .env.local.")

    prd_se = str(job_like.get("prdSe", "M")).strip()
    params: Dict[str, str] = {
        "method": "getList",
        "apiKey": api_key,
        "orgId": str(job_like["orgId"]).strip(),
        "tblId": str(tbl_id_override or job_like["tblId"]).strip(),
        "prdSe": prd_se,
        "format": job_like.get("format", "json"),
        "jsonVD": job_like.get("jsonVD", "Y"),
    }

    if job_like.get("newEstPrdCnt") is not None and str(job_like.get("newEstPrdCnt")).strip() != "":
        params["newEstPrdCnt"] = str(job_like["newEstPrdCnt"]).strip()

    if job_like.get("startPrdDe") is not None and str(job_like.get("startPrdDe")).strip() != "":
        params["startPrdDe"] = _resolve_request_period_param(job_like["startPrdDe"], prd_se, table_year=table_year)

    if job_like.get("endPrdDe") is not None and str(job_like.get("endPrdDe")).strip() != "":
        params["endPrdDe"] = _resolve_request_period_param(job_like["endPrdDe"], prd_se, table_year=table_year)

    if job_like.get("itmId"):
        params["itmId"] = str(job_like["itmId"]).strip()

    if job_like.get("type"):
        params["type"] = str(job_like["type"]).strip()

    if job_like.get("version"):
        params["version"] = str(job_like["version"]).strip()

    for i in range(1, 9):
        key = f"objL{i}"
        if job_like.get(key):
            params[key] = str(job_like[key]).strip()

    return params


def _parse_sdmx_df(xml_text: str) -> pd.DataFrame:
    root = ET.fromstring(xml_text)
    if root.tag.endswith("error"):
        err_msg = root.findtext("errMsg") or "KOSIS SDMX error"
        raise RuntimeError(err_msg)

    rows = []
    for series in root.iter():
        if not series.tag.endswith("Series"):
            continue
        attrs = dict(series.attrib)
        item_id = str(attrs.get("ITEM", "")).strip()
        c1 = str(attrs.get("C_zone2017", "")).strip()
        c2 = str(attrs.get("C_IND2017", "")).strip()
        c3 = str(attrs.get("C_size2024", "")).strip()
        unit = str(attrs.get("UNIT", "")).strip()
        for obs in series:
            if not obs.tag.endswith("Obs"):
                continue
            obs_attrs = dict(obs.attrib)
            rows.append(
                {
                    "ITM_ID": item_id,
                    "ITM_NM": item_id,
                    "C1": c1,
                    "C1_NM": c1,
                    "C2": c2,
                    "C2_NM": c2,
                    "C3": c3,
                    "C3_NM": c3,
                    "UNIT_NM": unit,
                    "PRD_DE": str(obs_attrs.get("TIME_PERIOD", "")).strip(),
                    "DT": obs_attrs.get("OBS_VALUE"),
                    "LST_CHN_DE": obs_attrs.get("LST_CHN_DE"),
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("KOSIS SDMX returned 0 rows")
    return df


def fetch_kosis_df(job_like: dict, api_key: str) -> pd.DataFrame:
    fmt = str(job_like.get("format", "json")).strip().lower()
    candidates = _build_tblid_candidates(job_like)
    last_error: Exception | None = None

    for tbl_id, tbl_year in candidates:
        try:
            params = build_kosis_params(job_like, api_key, tbl_id_override=tbl_id, table_year=tbl_year)
            if fmt == "sdmx":
                df = _parse_sdmx_df(request_text_with_retry(KOSIS_BASE_URL, params=params))
            else:
                data = request_json_with_retry(KOSIS_BASE_URL, params=params)
                if not isinstance(data, list):
                    raise RuntimeError(f"KOSIS API returned non-list: {data}")
                df = pd.DataFrame(data)
                if df.empty:
                    raise RuntimeError("KOSIS returned 0 rows")

            if "TBL_ID" not in df.columns:
                df["TBL_ID"] = tbl_id
            df["REQUEST_TBL_ID"] = tbl_id
            return df
        except Exception as exc:
            last_error = exc
            continue

    if last_error is not None:
        raise last_error
    raise RuntimeError("KOSIS tblId candidate resolution produced no usable candidate")
