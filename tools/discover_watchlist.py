#!/usr/bin/env python3
"""
discover_watchlist.py

Reads tools/universe.txt (one ticker per line), pulls SEC CIK mapping,
fetches SEC company facts, scores each company for "watch" based on:
- negative / low operating cash flow (proxy for burn)
- capex intensity (capex / revenue)
- margin pressure (gross margin trend)
- revenue deceleration (recent vs prior)

Writes src/slidemodel/config/companies.yaml with top WATCH_N as in_scope=true.
Everything else becomes in_scope=false (ignore).

Env vars:
  SEC_USER_AGENT (required): "Name email@domain.com"
  WATCH_N (optional): default 20
"""

from __future__ import annotations

import os
import sys
import time
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import yaml


ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_PATH = ROOT / "tools" / "universe.txt"
OUT_YAML = ROOT / "src" / "slidemodel" / "config" / "companies.yaml"

SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

DEFAULT_WATCH_N = 20

# If tickers changed or have weird SEC mapping, put alias here.
TICKER_ALIASES: Dict[str, str] = {
    # Example:
    # "OLDTICKER": "NEWTICKER",
}

# If SEC mapping fails, you can hardcode a CIK here (10-digit, zero padded)
MANUAL_CIK_OVERRIDES: Dict[str, str] = {
    # Example:
    # "SOMETICKER": "0000123456",
}

# Buckets are just labels for UI grouping. You can tweak these.
DEFAULT_BUCKET = "watchlist_auto"


@dataclass
class ScoreResult:
    ticker: str
    cik: str
    score: float
    reasons: List[str]


def die(msg: str, code: int = 1) -> None:
    print(f"\nERROR: {msg}\n", file=sys.stderr)
    sys.exit(code)


def require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        die(
            f"Missing env var {name}.\n"
            f"Set it like:\n"
            f'  export {name}="Russell Barnett russellbarnett.co@gmail.com"\n'
        )
    return v


def load_universe(path: Path) -> List[str]:
    if not path.exists():
        die(f"universe file not found: {path}")

    raw = [ln.strip().upper() for ln in path.read_text().splitlines()]
    tickers = [t for t in raw if t and not t.startswith("#")]

    if not tickers:
        die(f"universe file is empty: {path}")

    # apply aliases
    tickers = [TICKER_ALIASES.get(t, t) for t in tickers]
    # dedupe preserving order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def sec_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }
    )
    return s


def get_with_retries(
    s: requests.Session,
    url: str,
    tries: int = 5,
    base_sleep: float = 0.6,
) -> requests.Response:
    last_exc = None
    for i in range(tries):
        try:
            r = s.get(url, timeout=30)
            # common throttles
            if r.status_code in (403, 429, 500, 502, 503, 504):
                sleep_for = base_sleep * (2 ** i) + (0.05 * i)
                time.sleep(sleep_for)
                continue
            return r
        except Exception as e:
            last_exc = e
            time.sleep(base_sleep * (2 ** i))
    if last_exc:
        raise last_exc
    raise RuntimeError("request failed")


def load_sec_ticker_map(s: requests.Session) -> Dict[str, str]:
    """
    Returns dict: TICKER -> CIK (10-digit zero padded)
    """
    r = get_with_retries(s, SEC_TICKER_MAP_URL)
    r.raise_for_status()

    data = r.json()
    out: Dict[str, str] = {}

    # company_tickers.json is a dict keyed by number: {"0": {...}, "1": {...}}
    for _, row in data.items():
        t = str(row.get("ticker", "")).upper().strip()
        cik_int = row.get("cik_str", None)
        if not t or cik_int is None:
            continue
        try:
            cik_num = int(cik_int)
        except Exception:
            continue
        out[t] = f"{cik_num:010d}"

    return out


def pick_usd_series(facts: dict, tag: str) -> List[Tuple[str, float]]:
    """
    Return list of (end_date, value) sorted by date asc for a given US-GAAP tag in USD.
    """
    try:
        units = facts["facts"]["us-gaap"][tag]["units"]
    except Exception:
        return []

    # Prefer USD units
    series = units.get("USD", [])
    out = []
    for pt in series:
        end = pt.get("end")
        val = pt.get("val")
        if end and isinstance(val, (int, float)):
            out.append((end, float(val)))
    out.sort(key=lambda x: x[0])
    return out


def last_n(series: List[Tuple[str, float]], n: int) -> List[Tuple[str, float]]:
    if len(series) <= n:
        return series[:]
    return series[-n:]


def pct_change(a: float, b: float) -> Optional[float]:
    # from a -> b
    if a is None or b is None:
        return None
    if a == 0:
        return None
    return (b - a) / abs(a) * 100.0


def safe_div(a: float, b: float) -> Optional[float]:
    if b == 0:
        return None
    return a / b


def score_company(ticker: str, cik: str, facts: dict) -> Optional[ScoreResult]:
    """
    Higher score means "more watch-worthy".
    Returns None if we can't compute anything useful.
    """

    reasons: List[str] = []
    score = 0.0

    # Revenue tags vary. Try common ones.
    revenue = []
    for tag in ["Revenues", "SalesRevenueNet", "RevenueFromContractWithCustomerExcludingAssessedTax"]:
        revenue = pick_usd_series(facts, tag)
        if revenue:
            break

    gross_profit = pick_usd_series(facts, "GrossProfit")
    capex = pick_usd_series(facts, "PaymentsToAcquirePropertyPlantAndEquipment")
    op_cf = pick_usd_series(facts, "NetCashProvidedByUsedInOperatingActivities")

    # If nothing, skip
    if not (revenue or gross_profit or capex or op_cf):
        return None

    # Use last 4 quarters-ish points if available (SEC series can include annual too, so we just use last points)
    rev_pts = last_n(revenue, 4)
    gp_pts = last_n(gross_profit, 4)
    capex_pts = last_n(capex, 4)
    ocf_pts = last_n(op_cf, 4)

    # Revenue deceleration: compare recent growth vs prior growth
    if len(rev_pts) >= 3:
        r0 = rev_pts[-3][1]
        r1 = rev_pts[-2][1]
        r2 = rev_pts[-1][1]
        g1 = pct_change(r0, r1)
        g2 = pct_change(r1, r2)
        if g1 is not None and g2 is not None:
            if g2 < g1:
                score += 1.0
                reasons.append("revenue decelerating")

    # Gross margin pressure: GP / Rev trend down
    if len(rev_pts) >= 2 and len(gp_pts) >= 2:
        # Align by taking last points only
        r1 = rev_pts[-2][1]
        r2 = rev_pts[-1][1]
        gp1 = gp_pts[-2][1]
        gp2 = gp_pts[-1][1]
        m1 = safe_div(gp1, r1)
        m2 = safe_div(gp2, r2)
        if m1 is not None and m2 is not None:
            if m2 < m1:
                drop = (m1 - m2)
                # scale: a 5pt drop is meaningful
                score += min(2.0, drop * 20.0)
                reasons.append("gross margin down")

    # Capex intensity: capex / revenue
    if len(capex_pts) >= 1 and len(rev_pts) >= 1:
        cx = abs(capex_pts[-1][1])
        rv = abs(rev_pts[-1][1])
        ratio = safe_div(cx, rv)
        if ratio is not None:
            if ratio > 0.10:
                score += min(2.0, ratio * 10.0)
                reasons.append("high capex intensity")

    # Operating cash flow negative
    if len(ocf_pts) >= 1:
        ocf = ocf_pts[-1][1]
        if ocf < 0:
            score += 2.0
            reasons.append("negative operating cash flow")

    # If we computed but score is still 0, keep it but low
    return ScoreResult(ticker=ticker, cik=cik, score=score, reasons=reasons)


def write_companies_yaml(watch: List[ScoreResult], ignore: List[str]) -> None:
    companies = []

    # Watch list in scope
    for r in watch:
        companies.append(
            {
                "company_id": r.ticker,
                "ticker": r.ticker,
                "cik": r.cik,
                "bucket": DEFAULT_BUCKET,
                "in_scope": True,
            }
        )

    # Ignore list out of scope
    for t in ignore:
        companies.append(
            {
                "company_id": t,
                "ticker": t,
                "cik": "",
                "bucket": DEFAULT_BUCKET,
                "in_scope": False,
            }
        )

    OUT_YAML.parent.mkdir(parents=True, exist_ok=True)
    OUT_YAML.write_text(yaml.safe_dump({"companies": companies}, sort_keys=False))
    print(f"Wrote {OUT_YAML}")
    print(f"Watch: {len(watch)}  Ignore: {len(ignore)}")


def main() -> None:
    user_agent = require_env("SEC_USER_AGENT")
    watch_n = int(os.environ.get("WATCH_N", str(DEFAULT_WATCH_N)))

    tickers = load_universe(UNIVERSE_PATH)
    print(f"Universe: {len(tickers)} tickers")

    s = sec_session(user_agent)

    # Load SEC mapping
    sec_map = load_sec_ticker_map(s)

    # Build CIK list
    missing = []
    ticker_to_cik: Dict[str, str] = {}
    for t in tickers:
        if t in MANUAL_CIK_OVERRIDES:
            ticker_to_cik[t] = MANUAL_CIK_OVERRIDES[t]
            continue
        cik = sec_map.get(t)
        if not cik:
            missing.append(t)
        else:
            ticker_to_cik[t] = cik

    if missing:
        die(
            "Could not map these tickers to CIK using SEC mapping and no override was provided:\n - "
            + "\n - ".join(missing)
            + "\n\nFix: remove them from tools/universe.txt, add an alias, or add MANUAL_CIK_OVERRIDES."
        )

    results: List[ScoreResult] = []
    for t in tickers:
        cik = ticker_to_cik[t]
        url = SEC_FACTS_URL.format(cik=cik)

        # polite pacing: SEC expects you not to hammer them
        time.sleep(0.15)

        try:
            r = get_with_retries(s, url)
            if r.status_code == 404:
                # this should be rare; treat as a hard skip
                print(f"{t}: skipped (404 on companyfacts)")
                continue
            r.raise_for_status()
            facts = r.json()
        except Exception as e:
            # You were seeing these. With headers set correctly, this should mostly stop.
            print(f"{t}: skipped (HTTP error): {e}")
            continue

        scored = score_company(t, cik, facts)
        if not scored:
            print(f"{t}: skipped (insufficient data)")
            continue

        results.append(scored)

    if not results:
        print("\nNo companies scored. This usually means requests are blocked or universe is bad.")
        print("Double-check SEC_USER_AGENT is set and valid.")
        return

    # Sort best first
    results.sort(key=lambda x: x.score, reverse=True)

    watch = results[:watch_n]
    watch_set = {r.ticker for r in watch}
    ignore = [t for t in tickers if t not in watch_set]

    write_companies_yaml(watch, ignore)

    print("\nTop picks:")
    for r in watch[:min(10, len(watch))]:
        why = ", ".join(r.reasons) if r.reasons else "low score but included"
        print(f"  {r.ticker}  score={r.score:.2f}  ({why})")


if __name__ == "__main__":
    main()