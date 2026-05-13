from __future__ import annotations

import json
from pathlib import Path


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


def parse_job_meta(root_dir: Path, rel_path: str) -> dict:
    path = root_dir / rel_path
    stem = path.stem
    row = {
        "path": rel_path,
        "name": prettify_name(stem),
        "source": "KOSIS",
        "provider": "kosis",
        "regions": [DEFAULT_SCOPE_REGION],
        "status": "대기",
        "output_prefix": stem,
    }

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return row

    raw_name = str(payload.get("job_name") or stem)
    row["name"] = prettify_name(stem) if "?" in raw_name else raw_name
    row["source"] = str(payload.get("source_name") or payload.get("source") or "KOSIS")
    row["provider"] = str(payload.get("provider") or "kosis")
    row["output_prefix"] = str(payload.get("output_prefix") or stem)

    region_text = " ".join([rel_path, raw_name, row["source"], row["output_prefix"], json.dumps(payload, ensure_ascii=False)])
    matched_regions = infer_regions(region_text)
    row["regions"] = matched_regions or [DEFAULT_SCOPE_REGION]
    return row


def filter_rows_by_region(rows: list[dict], selected_region: str) -> list[dict]:
    return [row for row in rows if not row.get("regions") or selected_region in row.get("regions", [])]


def build_job_index(root_dir: Path, relative_paths: list[str]) -> tuple[list[dict], dict[str, dict]]:
    rows = [parse_job_meta(root_dir, rel_path) for rel_path in relative_paths]
    return rows, {row["path"]: row for row in rows}

