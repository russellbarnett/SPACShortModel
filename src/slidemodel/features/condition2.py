from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional

from slidemodel.features.facts_extract import (
    quarterly_points,
    latest_n_quarters,
)

OPEX_TAGS = [
    "OperatingExpenses",
    "OperatingCostsAndExpenses",  # some filers
]

SGA_TAGS = [
    "SellingGeneralAndAdministrativeExpense",
]

RD_TAGS = [
    "ResearchAndDevelopmentExpense",
]

REVENUE_TAGS = [
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
]

def _get_usd_series(facts: Dict[str, Any], tag: str) -> Optional[List[Dict[str, Any]]]:
    try:
        return facts["facts"]["us-gaap"][tag]["units"]["USD"]
    except KeyError:
        return None

def _pick_series(facts: Dict[str, Any], tags: List[str]) -> Tuple[str, List[Dict[str, Any]]]:
    for tag in tags:
        s = _get_usd_series(facts, tag)
        if s:
            return tag, s
    raise KeyError(f"No series found for tags: {tags}")

def _values(points: List[Dict[str, Any]]) -> List[Tuple[str, float]]:
    out = []
    for p in points:
        out.append((p["end"], float(p["val"])))
    return out

def _qoq_growth(vals: List[float]) -> List[float]:
    g = []
    for i in range(1, len(vals)):
        prev = vals[i-1]
        cur = vals[i]
        if prev == 0:
            g.append(0.0)
        else:
            g.append((cur - prev) / prev)
    return g

def condition_2_from_facts(facts: Dict[str, Any]) -> Dict[str, Any]:
    # Revenue
    rev_tag, rev_series = _pick_series(facts, REVENUE_TAGS)
    rev_q = latest_n_quarters(quarterly_points(rev_series), n=8)
    rev = _values(rev_q)
    rev_map = {d: v for d, v in rev}

    # OpEx: prefer OperatingExpenses; if absent, use SG&A + R&D
    opex_tag = None
    opex_map: Dict[str, float] = {}

    for tag in OPEX_TAGS:
        s = _get_usd_series(facts, tag)
        if s:
            opex_tag = tag
            pts = latest_n_quarters(quarterly_points(s), n=8)
            opex_map = {d: v for d, v in _values(pts)}
            break

    derived = False
    if not opex_map:
        # derive from SGA + R&D if available
        sga_tag, sga_series = _pick_series(facts, SGA_TAGS)
        rd_tag, rd_series = _pick_series(facts, RD_TAGS)
        sga_pts = latest_n_quarters(quarterly_points(sga_series), n=8)
        rd_pts = latest_n_quarters(quarterly_points(rd_series), n=8)
        sga_map = {d: v for d, v in _values(sga_pts)}
        rd_map = {d: v for d, v in _values(rd_pts)}
        opex_tag = f"{sga_tag}+{rd_tag}"
        derived = True
        # inner join on dates
        for d in set(sga_map.keys()).intersection(rd_map.keys()):
            opex_map[d] = sga_map[d] + rd_map[d]

    # Align by date with revenue
    dates = sorted(set(rev_map.keys()).intersection(opex_map.keys()))
    rev_vals = [rev_map[d] for d in dates]
    opex_vals = [opex_map[d] for d in dates]

    rev_growth = _qoq_growth(rev_vals)
    opex_growth = _qoq_growth(opex_vals)

    opex_up_2q = False
    neg_leverage_2q = False

    # Need at least 3 quarters of values for 2-step checks (growth needs 3 points)
    if len(opex_vals) >= 3:
        o2, o1, o0 = opex_vals[-3], opex_vals[-2], opex_vals[-1]
        opex_up_2q = (o0 > o1) and (o1 > o2)

    if len(rev_growth) >= 2 and len(opex_growth) >= 2:
        # compare last two growth periods
        r1, r0 = rev_growth[-2], rev_growth[-1]
        o1, o0 = opex_growth[-2], opex_growth[-1]
        neg_leverage_2q = (r0 < o0) and (r1 < o1)

    condition_2 = opex_up_2q and neg_leverage_2q

    return {
        "revenue_tag": rev_tag,
        "opex_tag": opex_tag,
        "opex_derived": derived,
        "dates": dates,
        "revenue": rev_vals,
        "opex": opex_vals,
        "revenue_growth_qoq": rev_growth,
        "opex_growth_qoq": opex_growth,
        "opex_up_2q": opex_up_2q,
        "negative_leverage_2q": neg_leverage_2q,
        "condition_2": condition_2,
    }
