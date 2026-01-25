from __future__ import annotations
from typing import Dict, Any, List, Optional

from slidemodel.features.facts_extract import quarterly_points, latest_n_quarters

CAPEX_TAGS = ["PaymentsToAcquirePropertyPlantAndEquipment"]
RND_TAGS = ["ResearchAndDevelopmentExpense"]
OCF_TAGS = ["NetCashProvidedByUsedInOperatingActivities"]

def _get_usd_series(facts: Dict[str, Any], tag: str) -> Optional[List[Dict[str, Any]]]:
    try:
        return facts["facts"]["us-gaap"][tag]["units"]["USD"]
    except KeyError:
        return None

def _pick_series(facts: Dict[str, Any], tags: List[str]):
    for tag in tags:
        s = _get_usd_series(facts, tag)
        if s:
            return tag, s
    raise KeyError(f"No series found for tags: {tags}")

def _vals(series: List[Dict[str, Any]]) -> List[float]:
    pts = latest_n_quarters(quarterly_points(series), n=8)
    return [float(p["val"]) for p in pts]

def _flat_or_up(v: List[float]) -> bool:
    return len(v) >= 3 and (v[-1] >= v[-2] >= v[-3])

def _burn_persists(v: List[float]) -> bool:
    return len(v) >= 3 and (v[-1] <= v[-2] <= v[-3])

def condition_3_from_facts(
    facts: Dict[str, Any],
    revenue_deceleration_2q: bool,
    margin_failure_2q: bool,
) -> Dict[str, Any]:
    capex_tag, capex_series = _pick_series(facts, CAPEX_TAGS)
    capex = _vals(capex_series)

    rnd_tag = None
    rnd: List[float] = []
    try:
        rnd_tag, rnd_series = _pick_series(facts, RND_TAGS)
        rnd = _vals(rnd_series)
    except KeyError:
        pass

    ocf_tag, ocf_series = _pick_series(facts, OCF_TAGS)
    ocf = _vals(ocf_series)

    discretionary_continues = _flat_or_up(capex) or _flat_or_up(rnd)
    cash_burn_persists = _burn_persists(ocf)
    no_slope_improvement = revenue_deceleration_2q and margin_failure_2q

    return {
        "capex_tag": capex_tag,
        "rnd_tag": rnd_tag,
        "ocf_tag": ocf_tag,
        "capex_continues_2q": _flat_or_up(capex),
        "rnd_continues_2q": _flat_or_up(rnd) if rnd else False,
        "cash_burn_persists_2q": cash_burn_persists,
        "no_slope_improvement": no_slope_improvement,
        "condition_3": bool(discretionary_continues and cash_burn_persists and no_slope_improvement),
    }
