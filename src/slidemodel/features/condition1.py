from __future__ import annotations

from typing import Any, Dict, List, Tuple

from slidemodel.features.facts_extract import (
    QuarterPoint,
    last_end_date,
    pct_change,
    pick_tag_series,
    safe_div,
)

REVENUE_TAGS = [
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
]

# Prefer actual GrossProfit when it exists.
GROSS_PROFIT_TAGS = [
    "GrossProfit",
]

# Fallback when GrossProfit is missing: use OperatingIncomeLoss as a “profit proxy”.
# This will catch operating loss/margin stress patterns for early-stage companies.
OPERATING_INCOME_TAGS = [
    "OperatingIncomeLoss",
]


def _two_quarter_deceleration(rev_series: List[QuarterPoint]) -> bool:
    """
    Deceleration: last 2 sequential quarterly revenue growth rates both down.
    This is deliberately simple and robust.
    """
    if len(rev_series) < 4:
        return False

    r0 = rev_series[-4][1]
    r1 = rev_series[-3][1]
    r2 = rev_series[-2][1]
    r3 = rev_series[-1][1]

    g1 = pct_change(r0, r1)
    g2 = pct_change(r1, r2)
    g3 = pct_change(r2, r3)

    if g1 is None or g2 is None or g3 is None:
        return False

    # “2Q decel” means growth is stepping down twice in a row.
    return (g3 < g2) and (g2 < g1)


def _two_quarter_margin_failure(rev: List[QuarterPoint], profit: List[QuarterPoint]) -> bool:
    """
    Margin failure: profit proxy deteriorates relative to revenue in last 2 quarters.
    Works with GrossProfit or OperatingIncomeLoss.
    """
    if len(rev) < 2 or len(profit) < 2:
        return False

    r_prev, r_last = rev[-2][1], rev[-1][1]
    p_prev, p_last = profit[-2][1], profit[-1][1]

    m_prev = safe_div(p_prev, r_prev)
    m_last = safe_div(p_last, r_last)
    if m_prev is None or m_last is None:
        return False

    # “Failure” means margin is falling and is negative or heading toward it.
    return (m_last < m_prev) and (m_last < 0.05)


def condition_1_from_facts(facts: Dict[str, Any]) -> Dict[str, Any]:
    rev_tag, rev_series = pick_tag_series(facts, REVENUE_TAGS)

    used_fallback = False
    try:
        profit_tag, profit_series = pick_tag_series(facts, GROSS_PROFIT_TAGS)
    except KeyError:
        profit_tag, profit_series = pick_tag_series(facts, OPERATING_INCOME_TAGS)
        used_fallback = True

    revenue_deceleration_2q = _two_quarter_deceleration(rev_series)
    margin_failure_2q = _two_quarter_margin_failure(rev_series, profit_series)

    condition_1 = bool(revenue_deceleration_2q or margin_failure_2q)

    return {
        "revenue_tag": rev_tag,
        "profit_tag": profit_tag,
        "profit_is_operating_fallback": used_fallback,
        "revenue_deceleration_2q": revenue_deceleration_2q,
        "margin_failure_2q": margin_failure_2q,
        "condition_1": condition_1,
        "last_date": last_end_date(rev_series, profit_series),
    }