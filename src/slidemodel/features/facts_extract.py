from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple

REVENUE_TAGS = [
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
]

GROSS_PROFIT_TAGS = [
    "GrossProfit",
]

def _get_usd_facts(facts: Dict[str, Any], tag: str) -> Optional[List[Dict[str, Any]]]:
    try:
        return facts["facts"]["us-gaap"][tag]["units"]["USD"]
    except KeyError:
        return None

def pick_tag_series(facts: Dict[str, Any], tags: List[str]) -> Tuple[str, List[Dict[str, Any]]]:
    for tag in tags:
        series = _get_usd_facts(facts, tag)
        if series:
            return tag, series
    raise KeyError(f"No series found for tags: {tags}")

def quarterly_points(series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Keep quarterly (10-Q / 10-K quarterly) points: form=10-Q or form=10-K, fp=Q1/Q2/Q3/Q4
    out = []
    for p in series:
        form = p.get("form")
        fp = p.get("fp")
        if form not in ("10-Q", "10-K"):
            continue
        if fp not in ("Q1", "Q2", "Q3", "Q4"):
            continue
        if p.get("fy") is None or p.get("end") is None:
            continue
        out.append(p)
    # Sort by period end date
    out.sort(key=lambda x: x["end"])
    return out

def latest_n_quarters(points: List[Dict[str, Any]], n: int = 8) -> List[Dict[str, Any]]:
    return points[-n:] if len(points) >= n else points
