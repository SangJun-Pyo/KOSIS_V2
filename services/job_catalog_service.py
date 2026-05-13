from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_SCOPE_REGION = "인천"
REGIONS = [
    "서울",
    "부산",
    "대구",
    "인천",
    "광주",
    "대전",
    "울산",
    "세종",
    "경기",
    "강원",
    "충북",
    "충남",
    "전북",
    "전남",
    "경북",
    "경남",
    "제주",
]

REGION_KEYWORDS = {
    "서울": ["seoul", "서울"],
    "부산": ["busan", "부산"],
    "대구": ["daegu", "대구"],
    "인천": ["incheon", "인천"],
    "광주": ["gwangju", "광주"],
    "대전": ["daejeon", "대전"],
    "울산": ["ulsan", "울산"],
    "세종": ["sejong", "세종"],
    "경기": ["gyeonggi", "경기"],
    "강원": ["gangwon", "강원"],
    "충북": ["chungbuk", "충북"],
    "충남": ["chungnam", "충남"],
    "전북": ["jeonbuk", "전북"],
    "전남": ["jeonnam", "전남"],
    "경북": ["gyeongbuk", "경북"],
    "경남": ["gyeongnam", "경남"],
    "제주": ["jeju", "제주"],
}


def infer_regions(text: str) -> list[str]:
    lowered = text.lower()
    matched: list[str] = []
    for region, keywords in REGION_KEYWORDS.items():
        if any(keyword.lower() in lowered for keyword in keywords):
            matched.append(region)
    return matched


def prettify_name(raw_name: str) -> str:
    text = raw_name.replace("_", " ").replace("-", " ").strip()
    return " ".join(text.split()) or raw_name


def normalize_regions(raw_regions: Any) -> list[str]:
    if not raw_regions:
        return []

    values = raw_regions if isinstance(raw_regions, list) else [raw_regions]
    normalized: list[str] = []
    for value in values:
        text = str(value).strip()
        if text in REGIONS and text not in normalized:
            normalized.append(text)
            continue

        matched = infer_regions(text)
        for region in matched:
            if region not in normalized:
                normalized.append(region)
    return normalized


def summarize_scope(regions: list[str], scope_all_regions: bool = False) -> str:
    if scope_all_regions or len(regions) == len(REGIONS):
        return "전지역"
    if not regions:
        return DEFAULT_SCOPE_REGION
    return ", ".join(regions)


def parse_job_meta(root_dir: Path, rel_path: str) -> dict[str, Any]:
    path = root_dir / rel_path
    stem = path.stem
    rel_parts = Path(rel_path).parts
    top_group = rel_parts[1] if len(rel_parts) >= 2 and rel_parts[0] == "jobs" else path.parent.name
    row: dict[str, Any] = {
        "path": rel_path,
        "name": prettify_name(stem),
        "source": "KOSIS",
        "provider": "kosis",
        "regions": [DEFAULT_SCOPE_REGION],
        "scope_all_regions": False,
        "scope_label": DEFAULT_SCOPE_REGION,
        "status": "대기",
        "output_prefix": stem,
        "group": top_group,
    }

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return row

    raw_name = str(payload.get("job_name") or stem).strip()
    row["name"] = raw_name or prettify_name(stem)
    row["source"] = str(payload.get("source_name") or payload.get("source") or "KOSIS")
    row["provider"] = str(payload.get("provider") or "kosis")
    row["output_prefix"] = str(payload.get("output_prefix") or stem)

    scope_type = str(payload.get("scope_type") or "").strip().lower()
    scope_all_regions = bool(payload.get("scope_all_regions"))
    explicit_regions = normalize_regions(payload.get("scope_regions"))

    if scope_type == "common":
        row["regions"] = REGIONS.copy()
        row["scope_all_regions"] = True
    elif scope_type == "incheon":
        row["regions"] = ["인천"]
    elif scope_type == "multi_region" and explicit_regions:
        row["regions"] = explicit_regions
    elif scope_all_regions:
        row["regions"] = REGIONS.copy()
        row["scope_all_regions"] = True
    elif explicit_regions:
        row["regions"] = explicit_regions
    else:
        region_text = " ".join(
            [
                rel_path,
                raw_name,
                row["source"],
                row["output_prefix"],
                json.dumps(payload, ensure_ascii=False),
            ]
        )
        inferred_regions = infer_regions(region_text)
        row["regions"] = inferred_regions or [DEFAULT_SCOPE_REGION]
        row["scope_all_regions"] = len(row["regions"]) == len(REGIONS)

    row["scope_label"] = summarize_scope(row["regions"], row["scope_all_regions"])
    return row


def filter_rows_by_region(rows: list[dict[str, Any]], selected_region: str) -> list[dict[str, Any]]:
    return [row for row in rows if not row.get("regions") or selected_region in row.get("regions", [])]


def build_job_index(root_dir: Path, relative_paths: list[str]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    rows = [parse_job_meta(root_dir, rel_path) for rel_path in relative_paths]
    return rows, {row["path"]: row for row in rows}


def find_meta_for_artifact(artifact: Path, meta_by_path: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    artifact_stem = artifact.stem
    matches: list[tuple[int, dict[str, Any]]] = []
    for meta in meta_by_path.values():
        prefix = str(meta.get("output_prefix") or Path(meta["path"]).stem)
        if artifact_stem.startswith(prefix):
            matches.append((len(prefix), meta))

    if not matches:
        return None

    matches.sort(key=lambda item: item[0], reverse=True)
    return matches[0][1]
