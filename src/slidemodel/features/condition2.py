from __future__ import annotations

from typing import Any, Dict

from slidemodel.features.facts_extract import (
    latest_n_quarters,
    pick_tag_series,
    safe_div,
)

CAPEX_TAGS = [
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PaymentsToAcquireProductiveAssets",
    "PaymentsToAcquireRealEstate",
]


def condition_2_from_facts(facts: Dict[str, Any]) -> Dict[str, Any]:
    """
    Capex spike: last quarter capex vs average prior 4 quarters.
    If we don’t have enough data, we return condition_2=False (not a crash).
    """
    tag, series = pick_tag_series(facts, CAPEX_TAGS)

    s = latest_n_quarters(series, 6)
    if len(s) < 6:
        return {
            "capex_tag": tag,
            "capex_spike": False,
            "condition_2": False,
            "reason": "insufficient_quarters",
        }

    vals = [v for _, v in s]
    last = float(vals[-1])
    baseline = sum(float(x) for x in vals[-5:-1]) / 4.0

    ratio = safe_div(last, baseline)
    spike = False
    if ratio is not None:
        spike = ratio >= 1.75

    return {
        "capex_tag": tag,
        "capex_spike": spike,
        "condition_2": bool(spike),
        "ratio": ratio,
    }