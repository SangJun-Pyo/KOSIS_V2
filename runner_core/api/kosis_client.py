from typing import Any, Dict

import pandas as pd

from runner_core.api.http_client import request_json_with_retry
from runner_core.config import KOSIS_BASE_URL


def build_kosis_params(job_like: dict, api_key: str) -> Dict[str, str]:
    if not api_key:
        raise RuntimeError("KOSIS_API_KEY ????? ????.")

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

    for i in range(1, 9):
        key = f"objL{i}"
        if job_like.get(key):
            params[key] = str(job_like[key]).strip()

    return params


def fetch_kosis_df(job_like: dict, api_key: str) -> pd.DataFrame:
    params = build_kosis_params(job_like, api_key)
    data = request_json_with_retry(KOSIS_BASE_URL, params=params)
    if not isinstance(data, list):
        raise RuntimeError(f"KOSIS API returned non-list: {data}")
    df = pd.DataFrame(data)
    if df.empty:
        raise RuntimeError("KOSIS returned 0 rows")
    return df
