from __future__ import annotations
from typing import Dict, Any, List, Tuple

from slidemodel.features.facts_extract import (
    pick_tag_series,
    quarterly_points,
    latest_n_quarters,
    REVENUE_TAGS,
    GROSS_PROFIT_TAGS,
)

def _values(points: List[Dict[str, Any]]) -> List[Tuple[str, float]]:
    # returns list of (period_end, value)
    out = []
    for p in points:
        out.append((p["end"], float(p["val"])))
    return out

def qoq_growth(vals: List[float]) -> List[float]:
    g = []
    for i in range(1, len(vals)):
        prev = vals[i-1]
        cur = vals[i]
        if prev == 0:
            g.append(0.0)
        else:
            g.append((cur - prev) / prev)
    return g

def condition_1_from_facts(facts: Dict[str, Any]) -> Dict[str, Any]:
    rev_tag, rev_series = pick_tag_series(facts, REVENUE_TAGS)
    gp_tag, gp_series = pick_tag_series(facts, GROSS_PROFIT_TAGS)

    rev_q = latest_n_quarters(quarterly_points(rev_series), n=8)
    gp_q = latest_n_quarters(quarterly_points(gp_series), n=8)

    rev = _values(rev_q)
    gp = _values(gp_q)

    # Align by period end date (simple inner join)
    gp_map = {d: v for d, v in gp}
    aligned = [(d, r, gp_map[d]) for d, r in rev if d in gp_map]
    dates = [d for d, _, _ in aligned]
    rev_vals = [r for _, r, _ in aligned]
    gp_vals = [g for _, _, g in aligned]

    gm_vals = [(gp_vals[i] / rev_vals[i]) if rev_vals[i] else 0.0 for i in range(len(rev_vals))]
    growth = qoq_growth(rev_vals)

    # Need at least 3 quarters to evaluate 2-step deceleration
    revenue_deceleration = False
    margin_failure = False

    if len(growth) >= 3:
        # growth has length N-1 relative to rev_vals
        # last three growth points correspond to last four quarters
        g2, g1, g0 = growth[-3], growth[-2], growth[-1]
        revenue_deceleration = (g0 < g1) and (g1 < g2)

    if len(gm_vals) >= 3:
        m2, m1, m0 = gm_vals[-3], gm_vals[-2], gm_vals[-1]
        margin_failure = (m0 <= m1) and (m1 <= m2)

    condition_1 = revenue_deceleration and margin_failure

    return {
        "revenue_tag": rev_tag,
        "gross_profit_tag": gp_tag,
        "dates": dates,
        "revenue": rev_vals,
        "gross_margin": gm_vals,
        "revenue_growth_qoq": growth,
        "revenue_deceleration_2q": revenue_deceleration,
        "margin_failure_2q": margin_failure,
        "condition_1": condition_1,
    }
