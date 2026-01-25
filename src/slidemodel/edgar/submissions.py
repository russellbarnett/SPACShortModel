from __future__ import annotations
import os
import time
import requests
from typing import Any, Dict, List, Tuple
from datetime import datetime, timedelta

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

def _headers() -> Dict[str, str]:
    ua = os.getenv("SEC_USER_AGENT")
    if not ua:
        raise RuntimeError("Missing SEC_USER_AGENT env var.")
    return {
        "User-Agent": ua,
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
    }

def fetch_submissions(cik: str, throttle_seconds: float = 0.25) -> Dict[str, Any]:
    cik_padded = cik.zfill(10)
    url = SUBMISSIONS_URL.format(cik=cik_padded)
    time.sleep(throttle_seconds)
    r = requests.get(url, headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def recent_8k_accessions_with_dates(
    submissions: Dict[str, Any],
    days: int = 90,
    limit: int = 50,
) -> List[Tuple[str, str]]:
    cutoff = datetime.utcnow() - timedelta(days=days)
    recent = submissions.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    dates = recent.get("filingDate", [])

    out: List[Tuple[str, str]] = []
    for form, acc, d in zip(forms[:limit], accessions[:limit], dates[:limit]):
        if form != "8-K":
            continue
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
        except Exception:
            continue
        if dt >= cutoff:
            out.append((acc, d))
    return out
