from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from .facts_extract import pick_tag_series, series_values, series_ends


# Revenue tags (keep your original intent, but widen slightly)
REVENUE_TAGS = [
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
]

# Gross profit tags (many ex-SPACs won't have this)
GROSS_PROFIT_TAGS = [
    "GrossProfit",
]

# Operating income as fallback "margin" proxy
OPERATING_INCOME_TAGS = [
    "OperatingIncomeLoss",
]


@dataclass
class Condition1Result:
    revenue_tag: str
    margin_tag: str
    revenue_deceleration_2q: bool
    margin_failure_2q: bool
    condition_1: bool
    last_date: str


def _pct_change(new: float, old: float) -> float:
    if old == 0:
        return 0.0
    return (new - old) / abs(old)


def _two_quarter_checks(values: List[float], threshold_drop: float) -> bool:
    """
    Simple rule: last 2 qtrs show negative change beyond threshold.
    Example: revenue deceleration: threshold_drop = -0.05 means drop >5%
    """
    if len(values) < 3:
        return False
    q0, q1, q2 = values[-3], values[-2], values[-1]
    d1 = _pct_change(q1, q0)
    d2 = _pct_change(q2, q1)
    return (d1 <= threshold_drop) and (d2 <= threshold_drop)


def condition_1_from_facts(facts: Dict[str, Any]) -> Condition1Result:
    """
    Condition 1: Revenue + margin pressure.

    Margin preference order:
      1) GrossProfit / Revenue, if GrossProfit available
      2) OperatingIncomeLoss / Revenue, if gross profit is unavailable
    """

    rev_tag, rev_series = pick_tag_series(facts, REVENUE_TAGS)
    rev_vals = series_values(rev_series)
    last_date = series_ends(rev_series)[-1]

    # Revenue deceleration: revenue shrinking two quarters in a row (tweak threshold if you want)
    revenue_deceleration_2q = _two_quarter_checks(rev_vals, threshold_drop=-0.02)

    # Try gross profit route first
    margin_tag = ""
    margin_vals: List[float] = []

    try:
        gp_tag, gp_series = pick_tag_series(facts, GROSS_PROFIT_TAGS)
        gp_vals = series_values(gp_series)

        # Align lengths by taking the last N that match
        n = min(len(gp_vals), len(rev_vals))
        gp_vals = gp_vals[-n:]
        r_vals = rev_vals[-n:]

        # gross margin
        margin_vals = [(gp_vals[i] / r_vals[i]) if r_vals[i] else 0.0 for i in range(n)]
        margin_tag = gp_tag

    except KeyError:
        # Fallback to operating income margin
        op_tag, op_series = pick_tag_series(facts, OPERATING_INCOME_TAGS)
        op_vals = series_values(op_series)

        n = min(len(op_vals), len(rev_vals))
        op_vals = op_vals[-n:]
        r_vals = rev_vals[-n:]

        margin_vals = [(op_vals[i] / r_vals[i]) if r_vals[i] else 0.0 for i in range(n)]
        margin_tag = op_tag

    # Margin failure: margin getting worse two quarters in a row
    margin_failure_2q = _two_quarter_checks(margin_vals, threshold_drop=-0.01)

    condition_1 = bool(revenue_deceleration_2q or margin_failure_2q)

    return Condition1Result(
        revenue_tag=rev_tag,
        margin_tag=margin_tag,
        revenue_deceleration_2q=revenue_deceleration_2q,
        margin_failure_2q=margin_failure_2q,
        condition_1=condition_1,
        last_date=last_date,
    )