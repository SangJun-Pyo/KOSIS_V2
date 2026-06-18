import re
from typing import Any, Iterable


def period_sort_key(value: Any) -> tuple:
    text = str(value).strip()
    if re.fullmatch(r"\d{4}", text):
        return (0, int(text), 0, 0)
    if re.fullmatch(r"\d{6}", text):
        return (1, int(text[:4]), int(text[4:6]), 0)
    match = re.fullmatch(r"(\d{4})\.(\d)/4", text)
    if match:
        return (2, int(match.group(1)), int(match.group(2)), 0)
    return (9, text, 0, 0)


def available_periods(values: Iterable[Any]) -> list[str]:
    return sorted({str(v).strip() for v in values if str(v).strip()}, key=period_sort_key)


def available_years(values: Iterable[Any]) -> list[str]:
    return [v for v in available_periods(values) if re.fullmatch(r"\d{4}", v)]


def _latest_n(items: list[str], count: int) -> list[str]:
    if count <= 0:
        return items
    return items[-count:]


def _latest_n_aligned_periods(available: list[str], count: int) -> list[str]:
    if not available:
        return []
    latest = available[-1]
    if re.fullmatch(r"\d{4}", latest):
        years = available_years(available)
        return _latest_n(years, count)

    quarter_match = re.fullmatch(r"(\d{4})(\.[1-4]/4)", latest)
    if quarter_match:
        suffix = quarter_match.group(2)
        matched = [period for period in available if str(period).endswith(suffix)]
        return _latest_n(matched, count)

    month_match = re.fullmatch(r"(\d{4})(\d{2})", latest)
    if month_match:
        suffix = month_match.group(2)
        matched = [period for period in available if re.fullmatch(rf"\d{{4}}{suffix}", str(period))]
        return _latest_n(matched, count)

    return _latest_n(available, count)


def resolve_period_token(value: Any, available: list[str]) -> Any:
    text = str(value).strip()
    if not text.startswith("__") or not text.endswith("__"):
        return value

    years = available_years(available)

    if text == "__LATEST_PERIOD__":
        return available[-1] if available else value
    if text == "__EARLIEST_PERIOD__":
        return available[0] if available else value
    if text == "__LATEST_YEAR__":
        return years[-1] if years else (available[-1] if available else value)
    if text == "__EARLIEST_YEAR__":
        return years[0] if years else (available[0] if available else value)
    if text == "__LATEST_YEAR_LABEL__":
        latest = years[-1] if years else (available[-1] if available else "")
        return f"{latest}년" if latest else value
    if text == "__LATEST_PERIOD_LABEL__":
        latest = available[-1] if available else ""
        return f"{latest}년" if re.fullmatch(r"\d{4}", latest) else latest or value

    match = re.fullmatch(r"__LATEST_(\d+)_YEARS__", text)
    if match:
        return _latest_n(years, int(match.group(1)))
    match = re.fullmatch(r"__LATEST_(\d+)_PERIODS__", text)
    if match:
        return _latest_n(available, int(match.group(1)))
    match = re.fullmatch(r"__LATEST_(\d+)_ALIGNED_PERIODS__", text)
    if match:
        return _latest_n_aligned_periods(available, int(match.group(1)))

    return value


def resolve_period_list(values: list[Any], available: list[str]) -> list[str]:
    resolved: list[str] = []
    for value in values:
        token_value = resolve_period_token(value, available)
        if isinstance(token_value, list):
            resolved.extend([str(v) for v in token_value])
        else:
            resolved.append(str(token_value))

    deduped: list[str] = []
    seen = set()
    for item in resolved:
        if item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped


def resolve_period_value(value: Any, available: list[str]) -> str:
    token_value = resolve_period_token(value, available)
    if isinstance(token_value, list):
        return str(token_value[-1]) if token_value else str(value)
    return str(token_value)


def resolve_cagr_label(label: Any, years: list[str]) -> str:
    if not years:
        return str(label or "CAGR")
    default = f"CAGR('{years[0][2:]}~'{years[-1][2:]})"
    if label in (None, "", "__AUTO_CAGR_LABEL__"):
        return default
    return str(label)


def period_template(text: Any, available: list[str], years: list[str] | None = None) -> str:
    raw = str(text or "")
    if not raw:
        return raw

    years = years or available_years(available)
    latest_period = available[-1] if available else ""
    latest_year = years[-1] if years else latest_period
    start_year = years[0] if years else ""
    end_year = years[-1] if years else ""
    mapping = {
        "latest_period": latest_period,
        "latest_year": latest_year,
        "start_year": start_year,
        "end_year": end_year,
        "start_yy": start_year[2:] if len(start_year) >= 4 else start_year,
        "end_yy": end_year[2:] if len(end_year) >= 4 else end_year,
    }
    out = raw
    for key, value in mapping.items():
        out = out.replace("{" + key + "}", str(value))
    return out
