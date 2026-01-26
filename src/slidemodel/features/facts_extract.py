from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass
class SeriesPoint:
    end: str
    val: float
    form: str


def _iter_unit_points(tag_obj: Dict[str, Any]) -> Iterable[SeriesPoint]:
    """
    SEC companyfacts structure:
      facts['us-gaap'][TAG]['units'][UNIT] = [{end, val, form, fy, fp, ...}, ...]
    We prefer USD for income statement lines.
    """
    units = (tag_obj or {}).get("units", {}) or {}

    # Prefer USD; if not present, fall back to any unit.
    preferred_units = []
    if "USD" in units:
        preferred_units.append("USD")
    preferred_units.extend([u for u in units.keys() if u != "USD"])

    for unit in preferred_units:
        for p in units.get(unit, []) or []:
            end = p.get("end")
            val = p.get("val")
            form = p.get("form")
            if not end or val is None or form is None:
                continue
            try:
                fval = float(val)
            except Exception:
                continue
            yield SeriesPoint(end=str(end), val=fval, form=str(form))


def _quarterly_series_from_tag(tag_obj: Dict[str, Any]) -> List[SeriesPoint]:
    """
    Return latest quarterly-like datapoints, de-duped by 'end', sorted ascending by end.
    Keep 10-Q and 10-K. (10-K includes annual, but still useful if quarterly isn't there.)
    """
    pts = list(_iter_unit_points(tag_obj))

    # Keep only primary filings
    pts = [p for p in pts if p.form in ("10-Q", "10-K")]

    # De-dupe by end date: keep the last occurrence
    by_end: Dict[str, SeriesPoint] = {}
    for p in pts:
        by_end[p.end] = p

    out = list(by_end.values())
    out.sort(key=lambda x: x.end)
    return out


def pick_tag_series(facts: Dict[str, Any], tags: List[str]) -> Tuple[str, List[SeriesPoint]]:
    """
    Try tags in order and return (chosen_tag, quarterly_series_points).
    Raise KeyError if none found.
    """
    usgaap = (facts or {}).get("facts", {}).get("us-gaap", {}) or {}

    for t in tags:
        obj = usgaap.get(t)
        if not obj:
            continue
        series = _quarterly_series_from_tag(obj)
        if len(series) >= 2:
            return t, series

    raise KeyError(f"No series found for tags: {tags}")


def series_values(series: List[SeriesPoint]) -> List[float]:
    return [p.val for p in series]


def series_ends(series: List[SeriesPoint]) -> List[str]:
    return [p.end for p in series]