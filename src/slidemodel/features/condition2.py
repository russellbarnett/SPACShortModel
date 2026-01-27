from __future__ import annotations

from typing import Any, Dict, List

from slidemodel.features.facts_extract import (
    QuarterPoint,
    latest_n_quarters,
    pick_tag_series,
    safe_div,
)

CAPEX_TAGS = [
    # Common capex / PPE acquisition tags
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PaymentsToAcquireProductiveAssets",
    "PaymentsToAcquireRealEstate",
    "PaymentsToAcquirePropertyPlantAndEquipmentAndEquipment",
    "PaymentsToAcquirePropertyPlantAndEquipmentNet",
    "CapitalExpenditures",
]


def condition_2_from_facts(facts: Dict[str, Any]) -> Dict[str, Any]:
    """
    Capex spike condition:
      Compare most recent quarter capex vs avg of prior 4 quarters.
      Trigger when >= 1.75x.

    If capex series is missing, return capex_missing=True and condition_2=False.
    """
    try:
        tag, series = pick_tag_series(facts, CAPEX_TAGS)
    except KeyError:
        return {
            "capex_tag": "",
            "capex_missing": True,
            "capex_spike": False,
            "condition_2": False,
        }

    s: List[QuarterPoint] = latest_n_quarters(series, 6)
    if len(s) < 6:
        return {
            "capex_tag": tag,
            "capex_missing": False,
            "capex_spike": False,
            "condition_2": False,
        }

    vals = [float(v) for _, v in s]
    last = float(vals[-1])
    baseline = sum(float(x) for x in vals[-5:-1]) / 4.0

    ratio = safe_div(last, baseline)
    spike = False
    if ratio is not None:
        spike = ratio >= 1.75

    return {
        "capex_tag": tag,
        "capex_missing": False,
        "capex_spike": spike,
        "condition_2": bool(spike),
    }