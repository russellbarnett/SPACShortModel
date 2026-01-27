from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

QuarterPoint = Tuple[str, float]  # (period_end_date, value)


def safe_float(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except Exception:
        return None
    if x != x:  # NaN
        return None
    return x


def safe_div(a: float, b: float) -> Optional[float]:
    try:
        if b == 0:
            return None
        return a / b
    except Exception:
        return None


def pct_change(prev: float, curr: float) -> Optional[float]:
    denom = abs(prev)
    if denom == 0:
        return None
    return ((curr - prev) / denom) * 100.0


def _normalize_us_gaap(obj: Any) -> Dict[str, Any]:
    """
    Accept any of these shapes and return the us-gaap dict:
      1) full SEC companyfacts JSON:  {"facts": {"us-gaap": {...}}}
      2) facts dict:                  {"us-gaap": {...}}
      3) us-gaap dict already:        {...tag->tag_obj...}
    """
    if not isinstance(obj, dict):
        return {}

    # Full payload shape
    if "facts" in obj and isinstance(obj.get("facts"), dict):
        facts = obj.get("facts", {})
        if isinstance(facts.get("us-gaap"), dict):
            return facts["us-gaap"]

    # Facts-only shape
    if isinstance(obj.get("us-gaap"), dict):
        return obj["us-gaap"]

    # Already us-gaap shape (heuristic: contains tag dicts with "units")
    # We just return it as-is.
    return obj


def _is_quarterish(item: Dict[str, Any]) -> bool:
    form = str(item.get("form") or "")
    fp = str(item.get("fp") or "")

    # Allow amended forms too
    ok_form = (
        form.startswith("10-Q")
        or form.startswith("10-K")
        or form.startswith("20-F")
    )

    # Prefer quarterly points if fp is Q*
    # But keep FY/10-K points too (some series use FY for Q4-ish)
    ok_fp = fp.startswith("Q") or fp == "FY" or fp == ""

    return ok_form and ok_fp


def _extract_points_from_tag_obj(tag_obj: Dict[str, Any]) -> List[QuarterPoint]:
    units = tag_obj.get("units", {})
    if not isinstance(units, dict) or not units:
        return []

    preferred_units = ["USD", "USD/shares", "shares", "pure"]
    unit_key = None

    for k in preferred_units:
        if k in units and isinstance(units[k], list) and units[k]:
            unit_key = k
            break

    if unit_key is None:
        for k, arr in units.items():
            if isinstance(arr, list) and arr:
                unit_key = k
                break

    if unit_key is None:
        return []

    raw = units.get(unit_key, [])
    if not isinstance(raw, list):
        return []

    by_end: Dict[str, Dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue

        if not _is_quarterish(item):
            continue

        end = item.get("end")
        val = safe_float(item.get("val"))
        if not end or val is None:
            continue

        existing = by_end.get(end)
        if existing is None:
            by_end[end] = item
            continue

        filed_new = str(item.get("filed") or "")
        filed_old = str(existing.get("filed") or "")
        if filed_new and (not filed_old or filed_new > filed_old):
            by_end[end] = item

    points: List[QuarterPoint] = []
    for end, item in by_end.items():
        val = safe_float(item.get("val"))
        if val is None:
            continue
        points.append((end, float(val)))

    points.sort(key=lambda x: x[0])
    return points


def quarterly_points(facts_or_tag_obj: Dict[str, Any], tag: Optional[str] = None) -> List[QuarterPoint]:
    """
    Two calling styles:
      quarterly_points(any_facts_shape, "Revenues")
      quarterly_points(tag_obj)

    This function is intentionally defensive because different parts of the pipeline
    may pass different slices of the SEC JSON.
    """
    if tag is None:
        if not isinstance(facts_or_tag_obj, dict):
            return []
        return _extract_points_from_tag_obj(facts_or_tag_obj)

    us_gaap = _normalize_us_gaap(facts_or_tag_obj)
    tag_obj = us_gaap.get(tag)
    if not isinstance(tag_obj, dict):
        return []
    return _extract_points_from_tag_obj(tag_obj)


def latest_n_quarters(series: Sequence[QuarterPoint], n: int) -> List[QuarterPoint]:
    if not series or n <= 0:
        return []
    return list(series[-n:])


def pick_tag_series(facts: Dict[str, Any], tags: Sequence[str]) -> Tuple[str, List[QuarterPoint]]:
    for t in tags:
        s = quarterly_points(facts, t)
        if len(s) >= 2:
            return t, s
    raise KeyError(f"No series found for tags: {list(tags)}")


def last_end_date(*series_list: Sequence[QuarterPoint]) -> str:
    last = ""
    for s in series_list:
        if s:
            last = max(last, s[-1][0])
    return last