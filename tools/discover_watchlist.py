#!/usr/bin/env python3
"""
Option 2: Auto-discover a watchlist from a candidate universe, score it using SEC fundamentals,
and write src/slidemodel/config/companies.yaml.

How it works:
1) Reads tickers from tools/universe.txt (one per line).
2) Maps ticker -> CIK using SEC "company_tickers_exchange.json".
3) Pulls SEC Company Facts for each CIK.
4) Scores:
   - Revenue deceleration (2Q)
   - Gross margin pressure (2Q)
   - High capex intensity (CapEx / Revenue)
   - Negative free cash flow (OCF - CapEx)
5) Selects top N as "watch" (in_scope: true), optionally writes rest as ignore (in_scope: false).
"""

from __future__ import annotations

import os
import sys
import time
import math
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_PATH = REPO_ROOT / "tools" / "universe.txt"
OUT_YAML_PATH = REPO_ROOT / "src" / "slidemodel" / "config" / "companies.yaml"

SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers_exchange.json"  #  [oai_citation:1‡SEC](https://www.sec.gov/file/company-tickers-exchange?utm_source=chatgpt.com)
SEC_FACTS_URL_TMPL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

DEFAULT_SLEEP_SEC = 0.15  # be polite to SEC

# XBRL tags (common ones)
REVENUE_TAGS = ["Revenues", "SalesRevenueNet", "RevenueFromContractWithCustomerExcludingAssessedTax"]
GROSS_PROFIT_TAGS = ["GrossProfit"]
OCF_TAGS = ["NetCashProvidedByUsedInOperatingActivities"]
CAPEX_TAGS = ["PaymentsToAcquirePropertyPlantAndEquipment"]

@dataclass
class ScoreDetail:
    rev_decel_2q: bool = False
    margin_down_2q: bool = False
    capex_intense: bool = False
    neg_fcf: bool = False
    score: int = 0
    notes: List[str] = None

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _ua() -> str:
    ua = os.getenv("SEC_USER_AGENT", "").strip()
    if not ua:
        print("\nERROR: SEC_USER_AGENT is not set.\n"
              "Set it like:\n"
              "  export SEC_USER_AGENT=\"Russell Barnett russell@example.com\"\n", file=sys.stderr)
        sys.exit(2)
    return ua

def load_universe(path: Path) -> List[str]:
    if not path.exists():
        print(f"\nERROR: Missing universe file: {path}\nCreate it with one ticker per line.\n", file=sys.stderr)
        sys.exit(2)

    tickers: List[str] = []
    for line in path.read_text().splitlines():
        t = line.strip().upper()
        if not t or t.startswith("#"):
            continue
        tickers.append(t)
    tickers = sorted(set(tickers))
    if not tickers:
        print(f"\nERROR: universe file is empty: {path}\n", file=sys.stderr)
        sys.exit(2)
    return tickers

def fetch_ticker_map(session: requests.Session) -> Dict[str, str]:
    """
    Returns {TICKER: 10-digit-zero-padded-CIK}
    """
    r = session.get(SEC_TICKER_MAP_URL, timeout=30)
    r.raise_for_status()
    data = r.json()

    # The SEC payload is a list of lists: [cik, name, ticker, exchange]
    out: Dict[str, str] = {}
    for row in data.get("data", []):
        if not isinstance(row, list) or len(row) < 3:
            continue
        cik_int = row[0]
        ticker = str(row[2]).upper()
        if not ticker:
            continue
        try:
            cik = str(int(cik_int)).zfill(10)
        except Exception:
            continue
        out[ticker] = cik
    return out

def fetch_company_facts(session: requests.Session, cik10: str) -> dict:
    url = SEC_FACTS_URL_TMPL.format(cik=cik10)
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def pick_quarterly_series(facts: dict, tags: List[str]) -> Optional[List[Tuple[str, float]]]:
    """
    Returns list of (end_date, value) sorted asc, quarterly (fp=Q).
    """
    usgaap = (facts.get("facts", {}) or {}).get("us-gaap", {}) or {}

    for tag in tags:
        node = usgaap.get(tag)
        if not node:
            continue
        units = node.get("units", {}) or {}
        # Most common unit for financial statement values is USD.
        usd = units.get("USD")
        if not isinstance(usd, list):
            continue

        pts = []
        for p in usd:
            # Filter to quarterly-ish points
            if p.get("fp") not in ("Q1", "Q2", "Q3", "Q4"):
                continue
            end = p.get("end")
            val = p.get("val")
            if not end:
                continue
            try:
                v = float(val)
            except Exception:
                continue
            pts.append((end, v))

        # Deduplicate by end date, keep last seen
        dedup: Dict[str, float] = {}
        for end, v in pts:
            dedup[end] = v
        series = sorted(dedup.items(), key=lambda x: x[0])
        if len(series) >= 4:
            return series

    return None

def pct_change(a: float, b: float) -> float:
    # from a -> b
    if a == 0:
        return math.inf if b != 0 else 0.0
    return (b - a) / abs(a)

def score_company(facts: dict) -> ScoreDetail:
    notes: List[str] = []

    rev = pick_quarterly_series(facts, REVENUE_TAGS)
    gp = pick_quarterly_series(facts, GROSS_PROFIT_TAGS)
    ocf = pick_quarterly_series(facts, OCF_TAGS)
    capex = pick_quarterly_series(facts, CAPEX_TAGS)

    sd = ScoreDetail(notes=notes)

    # Revenue deceleration over last 2 quarter-to-quarter changes
    if rev and len(rev) >= 6:
        r1 = pct_change(rev[-3][1], rev[-2][1])
        r2 = pct_change(rev[-2][1], rev[-1][1])
        if math.isfinite(r1) and math.isfinite(r2) and r2 < r1:
            sd.rev_decel_2q = True
            notes.append("rev_decel_2q")

    # Margin pressure: GP/Rev down 2 quarters in a row
    if rev and gp and len(rev) >= 6 and len(gp) >= 6:
        # Align by end date
        rmap = dict(rev)
        gpmap = dict(gp)
        ends = sorted(set(rmap.keys()) & set(gpmap.keys()))
        if len(ends) >= 6:
            e3, e2, e1 = ends[-3], ends[-2], ends[-1]
            m3 = gpmap[e3] / rmap[e3] if rmap[e3] else None
            m2 = gpmap[e2] / rmap[e2] if rmap[e2] else None
            m1 = gpmap[e1] / rmap[e1] if rmap[e1] else None
            if all(x is not None and math.isfinite(x) for x in [m3, m2, m1]):
                if (m2 < m3) and (m1 < m2):
                    sd.margin_down_2q = True
                    notes.append("margin_down_2q")

    # Capex intensity: capex / revenue high (rough heuristic)
    if rev and capex:
        rmap = dict(rev)
        cmap = dict(capex)
        ends = sorted(set(rmap.keys()) & set(cmap.keys()))
        if len(ends) >= 2:
            e = ends[-1]
            if rmap[e] != 0:
                intensity = abs(cmap[e]) / abs(rmap[e])
                if math.isfinite(intensity) and intensity >= 0.12:
                    sd.capex_intense = True
                    notes.append(f"capex_intense({intensity:.2f})")

    # Negative FCF: OCF - Capex < 0
    if ocf and capex:
        omap = dict(ocf)
        cmap = dict(capex)
        ends = sorted(set(omap.keys()) & set(cmap.keys()))
        if len(ends) >= 2:
            e = ends[-1]
            fcf = omap[e] - abs(cmap[e])
            if math.isfinite(fcf) and fcf < 0:
                sd.neg_fcf = True
                notes.append("neg_fcf")

    sd.score = sum([sd.rev_decel_2q, sd.margin_down_2q, sd.capex_intense, sd.neg_fcf])
    return sd

def main() -> int:
    ua = _ua()

    tickers = load_universe(UNIVERSE_PATH)

    session = requests.Session()
    session.headers.update({
        "User-Agent": ua,
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    })

    print(f"Universe: {len(tickers)} tickers")

    ticker_map = fetch_ticker_map(session)
    missing = [t for t in tickers if t not in ticker_map]
    if missing:
        print("\nERROR: Could not map these tickers to CIK using SEC mapping:")
        for t in missing:
            print(f" - {t}")
        print("\nFix: remove them from tools/universe.txt or confirm the ticker is correct.\n")
        return 2

    scored: List[Tuple[str, str, ScoreDetail]] = []
    for i, t in enumerate(tickers, 1):
        cik = ticker_map[t]
        try:
            facts = fetch_company_facts(session, cik)
            sd = score_company(facts)
            scored.append((t, cik, sd))
        except requests.HTTPError as e:
            print(f"{t}: skipped (HTTP error): {e}")
        except Exception as e:
            print(f"{t}: skipped (error): {e}")

        if i % 25 == 0:
            print(f"Processed {i}/{len(tickers)}")
        time.sleep(DEFAULT_SLEEP_SEC)

    # Sort by score desc, then ticker
    scored.sort(key=lambda x: (-x[2].score, x[0]))

    # You decide these numbers.
    WATCH_N = int(os.getenv("WATCH_N", "20"))
    INCLUDE_IGNORES = os.getenv("INCLUDE_IGNORES", "0") == "1"

    watch = scored[:WATCH_N]
    ignore = scored[WATCH_N:] if INCLUDE_IGNORES else []

    out = {"companies": []}
    for t, cik, sd in watch:
        out["companies"].append({
            "company_id": t,
            "ticker": t,
            "cik": cik,
            "bucket": "auto_discovered",
            "in_scope": True,
        })

    for t, cik, sd in ignore:
        out["companies"].append({
            "company_id": t,
            "ticker": t,
            "cik": cik,
            "bucket": "auto_discovered",
            "in_scope": False,
        })

    OUT_YAML_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_YAML_PATH.write_text(
        "# Auto-generated by tools/discover_watchlist.py\n"
        f"# Generated at: {_now_iso()}\n"
        + yaml.safe_dump(out, sort_keys=False),
        encoding="utf-8",
    )

    print(f"\nWrote {OUT_YAML_PATH}")
    print(f"Watch: {len(watch)}  Ignore: {len(ignore)}  Total scored: {len(scored)}")
    print("\nTop picks:")
    for t, cik, sd in watch[:10]:
        print(f" - {t} score={sd.score} ({', '.join(sd.notes or [])})")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())