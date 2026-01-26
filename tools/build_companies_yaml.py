#!/usr/bin/env python3
"""
Build src/slidemodel/config/companies.yaml from a ticker list.

Fixes common issues:
- Handles ticker aliases (ex: CNOO -> GOEV)
- Supports manual CIK overrides when SEC ticker map doesn't include a ticker
- Produces a deterministic YAML

Usage:
  python3 tools/build_companies_yaml.py
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Tuple

import requests
import yaml

# ----------------------------
# 1) EDIT THIS LIST
# ----------------------------
TICKERS: List[str] = [
    # Your watchlist tickers go here (20ish)
    "BYND",
    "SKIN",
    "DNUT",
    "OWLT",
    "PLBY",
    "SHCR",
    "AVPT",
    "HIMS",
    "SOFI",
    "STEM",
    "QS",
    "DNA",
    "IONQ",
    "LILM",
    "ASTS",
    "EVGO",
    "LCID",
    "RKLB",
    "APPH",
    "BOWL",
    # The three that broke you:
    "ETWO",   # E2open
    "CNOO",   # alias -> GOEV (Canoo)
    "HYZN",   # Hyzon
]

# ----------------------------
# 2) OPTIONAL: ALIASES + OVERRIDES
# ----------------------------

# If you type something that is not actually a ticker, normalize it here.
TICKER_ALIASES: Dict[str, str] = {
    "CNOO": "GOEV",  # Canoo ticker is GOEV
}

# If the SEC ticker map doesn't contain a ticker, force it here.
# Values should be 10-digit strings with leading zeros.
MANUAL_CIK_OVERRIDES: Dict[str, str] = {
    # ETWO (E2open) CIK 1800347  -> "0001800347"
    "ETWO": "0001800347",
    # HYZN (Hyzon) CIK 1716583 -> "0001716583"
    "HYZN": "0001716583",
    # GOEV (Canoo) CIK 1750153 -> "0001750153"
    "GOEV": "0001750153",
}

# ----------------------------
# 3) OUTPUT PATH
# ----------------------------
OUT_PATH = Path("src/slidemodel/config/companies.yaml")

# You can tweak these defaults if you want.
DEFAULT_BUCKET = "watchlist"
DEFAULT_IN_SCOPE = True


def normalize_ticker(t: str) -> str:
    t = (t or "").strip().upper()
    return TICKER_ALIASES.get(t, t)


def pad_cik(cik: str | int) -> str:
    s = str(cik).strip()
    s = s.lstrip("0")
    if not s.isdigit():
        raise ValueError(f"CIK must be numeric, got: {cik}")
    return s.zfill(10)


def load_sec_ticker_map() -> Dict[str, str]:
    """
    Pulls SEC's official company_tickers.json and returns {TICKER -> CIK(10-digit)}.
    Requires a SEC-friendly User-Agent.
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    ua = os.environ.get("SEC_USER_AGENT") or os.environ.get("SEC_USERAGENT")

    if not ua:
        raise SystemExit(
            "ERROR: SEC_USER_AGENT env var is not set.\n\n"
            "Fix:\n"
            "  export SEC_USER_AGENT='Your Name your@email.com'\n"
            "Then rerun:\n"
            "  python3 tools/build_companies_yaml.py\n"
        )

    headers = {"User-Agent": ua, "Accept-Encoding": "gzip, deflate", "Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    raw = r.json()
    out: Dict[str, str] = {}
    # SEC format: { "0": {"cik_str": 320193, "ticker": "AAPL", "title": "..."} , ... }
    for _, row in raw.items():
        ticker = str(row.get("ticker", "")).upper().strip()
        cik = row.get("cik_str")
        if ticker and cik is not None:
            out[ticker] = pad_cik(cik)
    return out


def build_companies_yaml(tickers: List[str]) -> Tuple[dict, List[str]]:
    sec_map = load_sec_ticker_map()

    missing: List[str] = []
    companies: List[dict] = []

    # de-dupe while preserving order
    seen = set()
    ordered: List[str] = []
    for t in tickers:
        nt = normalize_ticker(t)
        if nt and nt not in seen:
            seen.add(nt)
            ordered.append(nt)

    for t in ordered:
        cik = None

        if t in MANUAL_CIK_OVERRIDES:
            cik = pad_cik(MANUAL_CIK_OVERRIDES[t])
        elif t in sec_map:
            cik = sec_map[t]

        if not cik:
            missing.append(t)
            continue

        companies.append(
            {
                "company_id": t,
                "ticker": t,
                "cik": cik,
                "bucket": DEFAULT_BUCKET,
                "in_scope": bool(DEFAULT_IN_SCOPE),
            }
        )

    doc = {"companies": companies}
    return doc, missing


def main() -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    doc, missing = build_companies_yaml(TICKERS)

    if missing:
        print("\nERROR: Could not find CIK for these tickers in SEC map and no override was provided:")
        for t in missing:
            print(f" - {t}")
        print("\nFix options:")
        print("  1) Remove them from TICKERS")
        print("  2) Add a correct alias in TICKER_ALIASES")
        print("  3) Add a manual CIK in MANUAL_CIK_OVERRIDES (10-digit, zero-padded)")
        raise SystemExit(2)

    OUT_PATH.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    print(f"Wrote {OUT_PATH.resolve()} with {len(doc['companies'])} companies.")


if __name__ == "__main__":
    main()