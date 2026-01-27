from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml


# ============================================================
# Paths
# ============================================================

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB = REPO_ROOT / "data" / "slidemodel.sqlite3"
DEFAULT_COMPANIES_YAML = REPO_ROOT / "src" / "slidemodel" / "config" / "companies.yaml"
DEFAULT_DOCS_DIR = REPO_ROOT / "docs"
DEFAULT_OUT_JSON = DEFAULT_DOCS_DIR / "dashboard.json"


# ============================================================
# Time + SEC helpers
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sec_user_agent() -> str:
    ua = os.environ.get("SEC_USER_AGENT", "").strip()
    if not ua:
        raise RuntimeError(
            'SEC_USER_AGENT must be set. Example:\n'
            'export SEC_USER_AGENT="Russell Barnett russellbarnett.co@gmail.com"'
        )
    return ua


def fetch_company_name(cik10: str, session: requests.Session, ua: str) -> Optional[str]:
    cik10 = str(cik10).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
    try:
        r = session.get(url, headers={"User-Agent": ua}, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
        name = data.get("entityName")
        return name.strip() if isinstance(name, str) else None
    except Exception:
        return None


# ============================================================
# Quote helper (no API key): Stooq
# ============================================================

def _stooq_symbol(ticker: str) -> str:
    """
    Stooq uses e.g. aapl.us for US tickers.
    """
    t = (ticker or "").strip().lower()
    return f"{t}.us" if t else ""


def fetch_live_quotes_for_tickers(
    tickers: List[str],
    session: requests.Session,
    timeout: int = 20,
) -> Dict[str, Dict[str, Any]]:
    """
    Returns dict keyed by ticker:
      {
        "AAPL": {"price": 123.45, "change": -1.2, "change_pct": -0.96, "as_of": "...Z", "source": "stooq"}
      }

    This is "latest available" from Stooq, not millisecond streaming.
    """
    out: Dict[str, Dict[str, Any]] = {}
    as_of = utc_now_iso()

    for t in tickers:
        sym = _stooq_symbol(t)
        if not sym:
            continue

        # CSV endpoint (no key)
        # Example:
        # https://stooq.com/q/l/?s=aapl.us&f=sd2t2ohlcv&h&e=csv
        url = f"https://stooq.com/q/l/?s={sym}&f=sd2t2ohlcv&h&e=csv"

        try:
            r = session.get(url, timeout=timeout)
            if r.status_code != 200:
                continue
            text = (r.text or "").strip()
            lines = text.splitlines()
            if len(lines) < 2:
                continue

            # header: Symbol,Date,Time,Open,High,Low,Close,Volume
            header = [x.strip() for x in lines[0].split(",")]
            row = [x.strip() for x in lines[1].split(",")]
            if len(row) != len(header):
                continue

            d = dict(zip(header, row))

            close_str = d.get("Close")
            open_str = d.get("Open")
            if not close_str or close_str == "N/A":
                continue

            close = float(close_str)
            open_ = float(open_str) if open_str and open_str != "N/A" else None

            change = None
            change_pct = None
            if open_ is not None and open_ != 0:
                change = close - open_
                change_pct = (change / open_) * 100.0

            out[t.upper()] = {
                "price": close,
                "change": change,
                "change_pct": change_pct,
                "as_of": as_of,
                "source": "stooq",
            }

            # be polite
            time.sleep(0.08)

        except Exception:
            continue

    return out


# ============================================================
# Companies config
# ============================================================

@dataclass
class Company:
    company_id: str
    ticker: str
    cik: str
    bucket: str
    in_scope: bool
    name: str = ""


def load_companies(path: Path) -> List[Company]:
    raw = yaml.safe_load(path.read_text())
    rows = raw.get("companies", []) if isinstance(raw, dict) else []

    out: List[Company] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        out.append(
            Company(
                company_id=str(r.get("company_id", "")).strip(),
                ticker=str(r.get("ticker", "")).strip(),
                cik=str(r.get("cik", "")).strip(),
                bucket=str(r.get("bucket", "")).strip(),
                in_scope=bool(r.get("in_scope", False)),
                name=str(r.get("name", "")).strip(),
            )
        )
    return out


# ============================================================
# Database helpers
# ============================================================

def db_connect(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    return con


def table_info_columns(con: sqlite3.Connection, table: str) -> List[str]:
    cur = con.execute(f"PRAGMA table_info({table})")
    return [r["name"] for r in cur.fetchall()]


def load_latest_state(con: sqlite3.Connection) -> List[Dict[str, Any]]:
    cols = table_info_columns(con, "model_state")

    base_cols = [
        "company_id",
        "state",
        "as_of",
        "condition_1",
        "condition_2",
        "condition_3",
        "condition_4",
    ]

    select_cols = [c for c in base_cols if c in cols]

    # Optional columns (only select if present)
    optional = ["price_1m"]
    for c in optional:
        if c in cols:
            select_cols.append(c)

    if not select_cols:
        return []

    sql = f"SELECT {', '.join(select_cols)} FROM model_state ORDER BY company_id"
    rows = con.execute(sql).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)

        # Parse JSON columns if they exist and are strings
        if "price_1m" in d and isinstance(d["price_1m"], str) and d["price_1m"].strip():
            try:
                d["price_1m"] = json.loads(d["price_1m"])
            except Exception:
                d["price_1m"] = None

        out.append(d)

    return out


def load_events(con: sqlite3.Connection, limit: int = 100) -> List[Dict[str, Any]]:
    # Only attempt if table exists
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='state_events'")
    if cur.fetchone() is None:
        return []

    sql = """
        SELECT company_id, as_of, prev_state, new_state
        FROM state_events
        ORDER BY as_of DESC
        LIMIT ?
    """
    return [dict(r) for r in con.execute(sql, (limit,)).fetchall()]


# ============================================================
# Export
# ============================================================

def export_dashboard(
    db_path: Path = DEFAULT_DB,
    companies_yaml: Path = DEFAULT_COMPANIES_YAML,
    out_json: Path = DEFAULT_OUT_JSON,
) -> Path:
    companies = load_companies(companies_yaml)

    ua = sec_user_agent()
    session = requests.Session()

    # Backfill names (best-effort)
    for c in companies:
        if not c.name and c.cik:
            name = fetch_company_name(c.cik, session, ua)
            if name:
                c.name = name
            time.sleep(0.12)

    con = db_connect(db_path)
    try:
        latest_state = load_latest_state(con)
        events = load_events(con)
    finally:
        con.close()

    # Monitor-only tickers for "live" quote fetch
    monitor_tickers: List[str] = []
    for row in latest_state:
        if str(row.get("state", "")).upper() == "MONITOR":
            cid = row.get("company_id")
            # map company_id -> ticker from config
            tick = next((c.ticker for c in companies if c.company_id == cid), None)
            if tick:
                monitor_tickers.append(tick)

    live_quotes = fetch_live_quotes_for_tickers(monitor_tickers, session=session)

    payload = {
        "generated_at": utc_now_iso(),
        "companies": [
            {
                "company_id": c.company_id,
                "ticker": c.ticker,
                "name": c.name,
                "cik": c.cik,
                "bucket": c.bucket,
                "in_scope": c.in_scope,
            }
            for c in companies
        ],
        "latest_state": latest_state,
        "events": events,
        "live_quotes": live_quotes,  # <- new
    }

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2))
    return out_json


# ============================================================
# CLI entrypoint
# ============================================================

def run(data_dir: str = "data", out_dir: str = "docs") -> None:
    db = REPO_ROOT / data_dir / "slidemodel.sqlite3"
    out = Path(out_dir) / "dashboard.json"
    export_dashboard(db_path=db, out_json=out)
    print(f"Wrote {out}")


def main() -> None:
    run()


if __name__ == "__main__":
    main()