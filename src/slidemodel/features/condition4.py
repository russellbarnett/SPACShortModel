from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional

KEYWORDS = [
    # expansion / new markets
    "international expansion",
    "entering new markets",
    "new market",
    "expanding into",
    "launch in",
    # out of category / new lines
    "new product line",
    "product expansion",
    "new category",
    "outside the",
    # restructures framed as “strategic”
    "strategic review",
    "repositioning",
    "transformation",
    "new initiative",
    "growth initiative",
    # capacity moves
    "new facility",
    "manufacturing expansion",
    "capacity expansion",
]

def find_keyword_snippet(text: str, window: int = 220) -> Optional[Tuple[str, str]]:
    t = text.lower()
    for kw in KEYWORDS:
        idx = t.find(kw)
        if idx != -1:
            start = max(0, idx - window)
            end = min(len(text), idx + len(kw) + window)
            snippet = text[start:end].strip()
            return kw, snippet
    return None

def condition_4_from_text(
    filing_accession: str,
    filing_filename: str,
    text: str,
    revenue_deceleration_2q: bool,
    margin_failure_2q: bool,
) -> Dict[str, Any]:
    hit = find_keyword_snippet(text)
    initiative = bool(hit)
    no_slope_improvement = revenue_deceleration_2q and margin_failure_2q

    return {
        "filing_accession": filing_accession,
        "filing_filename": filing_filename,
        "initiative_detected": initiative,
        "keyword": hit[0] if hit else None,
        "snippet": hit[1] if hit else None,
        "no_slope_improvement": no_slope_improvement,
        "condition_4": bool(initiative and no_slope_improvement),
    }
