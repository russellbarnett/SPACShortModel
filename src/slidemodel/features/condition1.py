from __future__ import annotations

from typing import Any, Dict, List, Tuple

from slidemodel.features.facts_extract import (
    QuarterPoint,
    last_end_date,
    pct_change,
    pick_tag_series,
)

# Revenue tags (expanded)
REVENUE_TAGS = [
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "OperatingRevenues",
    "RevenuesAndOtherIncome",
    "TotalRevenuesAndOtherIncome",
    "RevenuesNetOfInterestExpense",
]

# Profit tags (gross profit preferred)
GROSS_PROFIT_TAGS = [
    "GrossProfit",
]

# Operating profit / loss fallback
OPERATING_INCOME_TAGS = [
    "OperatingIncomeLoss",
]


def _last_two(series: List[QuarterPoint]) -> Tuple[Tuple[str, float], Tuple[str, float]]:
    if len(series) < 2:
        raise ValueError("need at least 2 points")
    return series[-2], series[-1]


def _two_quarter_revenue_deceleration(rev_series: List[QuarterPoint]) -> bool:
    """
    True if growth rate decelerates for 2 consecutive quarters.
    Needs 3 points to compute two growth rates.
    """
    if len(rev_series) < 3:
        return False

    (_, r1), (_, r2), (_, r3) = rev_series[-3], rev_series[-2], rev_series[-1]

    g1 = pct_change(r1, r2)
    g2 = pct_change(r2, r3)

    if g1 is None or g2 is None:
        return False

    return g2 < g1


def _two_quarter_margin_failure(
    rev_series: List[QuarterPoint],
    profit_series: List[QuarterPoint],
) -> bool:
    """
    If we have revenue + a profit series, check if "profit margin" worsens 2 quarters in a row.
    Margin = profit / revenue (best-effort).
    """
    if len(rev_series) < 3 or len(profit_series) < 3:
        return False

    # Align on last 3 dates by index (good enough for our usage)
    r = [v for _, v in rev_series[-3:]]
    p = [v for _, v in profit_series[-3:]]

    def margin(profit: float, revenue: float) -> float:
        if revenue == 0:
            return 0.0
        return profit / revenue

    m1 = margin(p[0], r[0])
    m2 = margin(p[1], r[1])
    m3 = margin(p[2], r[2])

    return (m2 < m1) and (m3 < m2)


def _two_quarter_operating_loss_worsening(oi_series: List[QuarterPoint]) -> bool:
    """
    Fallback when revenue is missing: trigger if OperatingIncomeLoss is negative
    and getting more negative over the last 2 quarters.
    """
    if len(oi_series) < 2:
        return False

    (_, a), (_, b) = _last_two(oi_series)

    # OperatingIncomeLoss: negative is bad, more negative is worse
    return (a < 0) and (b < a)


def condition_1_from_facts(facts: Dict[str, Any]) -> Dict[str, Any]:
    revenue_missing = False
    used_profit_fallback = False

    # Revenue series
    try:
        rev_tag, rev_series = pick_tag_series(facts, REVENUE_TAGS)
    except KeyError:
        revenue_missing = True
        rev_tag = ""
        rev_series = []

    # Profit series: GrossProfit preferred, else OperatingIncomeLoss
    try:
        profit_tag, profit_series = pick_tag_series(facts, GROSS_PROFIT_TAGS)
    except KeyError:
        profit_tag, profit_series = pick_tag_series(facts, OPERATING_INCOME_TAGS)
        used_profit_fallback = True

    # Signals
    if not revenue_missing:
        revenue_deceleration_2q = _two_quarter_revenue_deceleration(rev_series)
        margin_failure_2q = _two_quarter_margin_failure(rev_series, profit_series)
        operating_loss_fallback = False
        operating_loss_worsening_2q = False
    else:
        # No revenue: use operating-loss worsening as the C1 signal
        revenue_deceleration_2q = False
        margin_failure_2q = False
        operating_loss_fallback = True
        operating_loss_worsening_2q = _two_quarter_operating_loss_worsening(profit_series)

    condition_1 = bool(revenue_deceleration_2q or margin_failure_2q or operating_loss_worsening_2q)

    return {
        "revenue_tag": rev_tag,
        "profit_tag": profit_tag,
        "revenue_missing": revenue_missing,
        "profit_is_operating_fallback": used_profit_fallback,
        "revenue_deceleration_2q": revenue_deceleration_2q,
        "margin_failure_2q": margin_failure_2q,
        "operating_loss_fallback": operating_loss_fallback,
        "operating_loss_worsening_2q": operating_loss_worsening_2q,
        "condition_1": condition_1,
        "last_date": last_end_date(rev_series, profit_series),
    }