from __future__ import annotations
import os
import time
import requests
from typing import Any, Dict

SEC_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

def _headers() -> Dict[str, str]:
    # SEC requires a descriptive User-Agent with contact info.
    # Set SEC_USER_AGENT in your environment, e.g.:
    # export SEC_USER_AGENT="Russell Barnett russell@example.com"
    ua = os.getenv("SEC_USER_AGENT")
    if not ua:
        raise RuntimeError("Missing SEC_USER_AGENT env var. Set it to 'Name email' for SEC requests.")
    return {
        "User-Agent": ua,
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
    }

def fetch_company_facts(cik: str, throttle_seconds: float = 0.25) -> Dict[str, Any]:
    cik_padded = cik.zfill(10)
    url = SEC_FACTS_URL.format(cik=cik_padded)
    time.sleep(throttle_seconds)
    resp = requests.get(url, headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()
