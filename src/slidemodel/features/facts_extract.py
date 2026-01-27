from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


QuarterPoint = Tuple[str, float]  # (end_date, value)


def safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


def safe_div(num: float, den: float) -> Optional[float]:
    try:
        if den == 0:
            return None
        return float(num) / float(den)
    except Exception:
        return None


def pct_change(a: float, b: float) -> Optional[float]:
    """
    Percent change from a -> b, returned as percent (not fraction).
    """
    try:
        if a == 0:
            return None
        return ((float(b) - float(a)) / float(a)) * 100.0
    except Exception:
        return None


def _us_gaap_facts(facts: Dict[str, Any]) -> Dict[str, Any]:
    """
    SEC companyfacts JSON has:
      facts -> us-gaap -> TAG -> units -> UNIT -> [ {end, fy, fp, form, val, ...}, ... ]
    We only care about facts["facts"]["us-gaap"].
    """
    if not isinstance(facts, dict):
        return {}
    return facts.get("facts", {}).get("us-gaap", {}) or {}


def quarterly_points(facts_or_tag_obj: Dict[str, Any], tag: Optional[str] = None) -> List[QuarterPoint]:
    """
    Returns a list of (end_date, value) points for a given tag.
    - If called as quarterly_points(facts, "Revenues") -> looks up tag in facts.
    - If called as quarterly_points(tag_obj) (tag=None) -> treats arg as tag object (facts["facts"]["us-gaap"][TAG]).
    Filters to quarterly filings when possible.
    """
    if tag is None:
        tag_obj = facts_or_tag_obj
    else:
        tag_obj = _us_gaap_facts(facts_or_tag_obj).get(tag)

    if not isinstance(tag_obj, dict):
        return []

    units = tag_obj.get("units", {})
    if not isinstance(units, dict) or not units:
        return []

    # Prefer USD if present, else first unit key.
    unit_key = "USD" if "USD" in units else next(iter(units.keys()))
    rows = units.get(unit_key, [])
    if not isinstance(rows, list) or not rows:
        return []

    pts: List[QuarterPoint] = []

    # Prefer 10-Q/quarterly, but if there are no quarterly rows we’ll fall back to whatever exists.
    quarterly_rows = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        form = str(r.get("form", "")).upper()
        fp = str(r.get("fp", "")).upper()
        # fp usually "Q1/Q2/Q3/Q4" or "FY". 10-Q tends to be Q1-Q3.
        if form in {"10-Q", "10-K"} and fp.startswith("Q"):
            quarterly_rows.append(r)

    use_rows = quarterly_rows if quarterly_rows else rows

    for r in use_rows:
        if not isinstance(r, dict):
            continue
        end = str(r.get("end", "")).strip()
        val = safe_float(r.get("val"))
        if not end or val is None:
            continue
        pts.append((end, float(val)))

    # Deduplicate by date keeping last, then sort by end date asc.
    by_end: Dict[str, float] = {}
    for end, val in pts:
        by_end[end] = val
    out = sorted(by_end.items(), key=lambda x: x[0])
    return [(d, v) for d, v in out]


def latest_n_quarters(series: Sequence[QuarterPoint], n: int) -> List[QuarterPoint]:
    if not series:
        return []
    if n <= 0:
        return []
    return list(series[-n:])


def pick_tag_series(facts: Dict[str, Any], tags: Sequence[str]) -> Tuple[str, List[QuarterPoint]]:
    """
    Try tags in order, return the first tag that has >= 2 quarterly points.
    """
    for t in tags:
        s = quarterly_points(facts, t)
        if len(s) >= 2:
            return t, s
    raise KeyError(f"No series found for tags: {list(tags)}")


def last_end_date(*series_list: Sequence[QuarterPoint]) -> str:
    """
    Return the max end date across series (best-effort).
    """
    last = ""
    for s in series_list:
        if s:
            last = max(last, s[-1][0])
    return last