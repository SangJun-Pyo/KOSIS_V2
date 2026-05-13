import os
from typing import Any, List

import pandas as pd

from runner_core.api.http_client import request_json_with_retry


def deep_get(obj: Any, path: str) -> Any:
    cur = obj
    if not path:
        return cur
    for part in path.split('.'):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list):
            if part.isdigit():
                idx = int(part)
                cur = cur[idx] if 0 <= idx < len(cur) else None
            else:
                return None
        else:
            return None
    return cur


def normalize_to_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def extract_items(data: Any, item_path: str) -> List[Any]:
    if not item_path:
        if isinstance(data, list):
            return data
        raise RuntimeError("data_go_kr job?? item_path? ?????. (?: response.body.items.item)")

    items = deep_get(data, item_path)
    items = normalize_to_list(items)
    if not items:
        raise RuntimeError(f"data.go.kr items 0?(item_path={item_path})")
    return items


def fetch_data_go_df(job: dict) -> pd.DataFrame:
    base_url = job.get("base_url")
    if not base_url:
        raise RuntimeError("data_go_kr job?? base_url? ?????.")

    params = job.get("params", {})
    if not isinstance(params, dict):
        raise RuntimeError("data_go_kr job? params? dict?? ???.")

    params = dict(params)
    svc_env = os.getenv("DATA_GO_KR_SERVICE_KEY", "").strip()
    for k, v in list(params.items()):
        if isinstance(v, str) and v.strip() == "{{DATA_GO_KR_SERVICE_KEY}}":
            if not svc_env:
                raise RuntimeError("???? DATA_GO_KR_SERVICE_KEY? ????.")
            params[k] = svc_env

    data = request_json_with_retry(base_url, params=params)
    if not isinstance(data, (dict, list)):
        raise RuntimeError("data.go.kr ??? JSON? ????. (job params? type/json ?? ?? ??)")

    items = extract_items(data, str(job.get("item_path", "")))
    if isinstance(items[0], dict):
        return pd.DataFrame(items)
    return pd.DataFrame({"value": items})
