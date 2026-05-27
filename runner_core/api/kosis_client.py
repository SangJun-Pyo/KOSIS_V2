from typing import Any, Dict
from xml.etree import ElementTree as ET

import pandas as pd

from runner_core.api.http_client import request_json_with_retry, request_text_with_retry
from runner_core.config import KOSIS_BASE_URL


def build_kosis_params(job_like: dict, api_key: str) -> Dict[str, str]:
    if not api_key:
        raise RuntimeError("KOSIS API key was not found in .env or .env.local.")

    params: Dict[str, str] = {
        "method": "getList",
        "apiKey": api_key,
        "orgId": str(job_like["orgId"]).strip(),
        "tblId": str(job_like["tblId"]).strip(),
        "prdSe": job_like.get("prdSe", "M"),
        "format": job_like.get("format", "json"),
        "jsonVD": job_like.get("jsonVD", "Y"),
    }

    if job_like.get("newEstPrdCnt") is not None and str(job_like.get("newEstPrdCnt")).strip() != "":
        params["newEstPrdCnt"] = str(job_like["newEstPrdCnt"]).strip()

    if job_like.get("startPrdDe") is not None and str(job_like.get("startPrdDe")).strip() != "":
        params["startPrdDe"] = str(job_like["startPrdDe"]).strip()

    if job_like.get("endPrdDe") is not None and str(job_like.get("endPrdDe")).strip() != "":
        params["endPrdDe"] = str(job_like["endPrdDe"]).strip()

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
    params = build_kosis_params(job_like, api_key)
    fmt = str(job_like.get("format", "json")).strip().lower()
    if fmt == "sdmx":
        return _parse_sdmx_df(request_text_with_retry(KOSIS_BASE_URL, params=params))

    data = request_json_with_retry(KOSIS_BASE_URL, params=params)
    if not isinstance(data, list):
        raise RuntimeError(f"KOSIS API returned non-list: {data}")
    df = pd.DataFrame(data)
    if df.empty:
        raise RuntimeError("KOSIS returned 0 rows")
    return df
