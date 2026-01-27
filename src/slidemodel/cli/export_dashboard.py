from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml


# ----------------------------
# Paths
# ----------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB = REPO_ROOT / "data" / "slidemodel.sqlite3"
DEFAULT_COMPANIES_YAML = REPO_ROOT / "src" / "slidemodel" / "config" / "companies.yaml"
DEFAULT_DOCS_DIR = REPO_ROOT / "docs"
DEFAULT_OUT_JSON = DEFAULT_DOCS_DIR / "dashboard.json"


# ----------------------------
# SEC helper
# ----------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sec_user_agent() -> str:
    ua = os.environ.get("SEC_USER_AGENT", "").strip()
    if not ua:
        raise RuntimeError(
            "SEC_USER_AGENT is not set. Example:\n"
            'export SEC_USER_AGENT="Russell Barnett russellbarnett.co@gmail.com"'
        )
    return ua


def fetch_entity_name_from_companyfacts(cik10: str, session: requests.Session, ua: str) -> Optional[str]:
    """
    Best-effort company name from SEC companyfacts endpoint.
    Returns None if it cannot be fetched.
    """
    cik10 = str(cik10).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
    try:
        r = session.get(url, headers={"User-Agent": ua}, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
        name = data.get("entityName")
        if isinstance(name, str) and name.strip():
            return name.strip()
        return None
    except Exception:
        return None


# ----------------------------
# DB reading
# ----------------------------

@dataclass
class Company:
    company_id: str
    ticker: str
    cik: str
    bucket: str
    in_scope: bool
    name: str = ""


def load_companies_yaml(path: Path) -> List[Company]:
    obj = yaml.safe_load(path.read_text())
    rows = obj.get("companies", []) if isinstance(obj, dict) else []
    out: List[Company] = []
    for c in rows:
        if not isinstance(c, dict):
            continue
        out.append(
            Company(
                company_id=str(c.get("company_id", "")).strip(),
                ticker=str(c.get("ticker", "")).strip(),
                cik=str(c.get("cik", "")).strip(),
                bucket=str(c.get("bucket", "")).strip(),
                in_scope=bool(c.get("in_scope", False)),
                name=str(c.get("name", "")).strip(),
            )
        )
    return out


def db_connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    return con


def load_latest_state(con: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    We assume a table model_state with at least:
    company_id, state, as_of, condition_1..condition_4, price_1m
    price_1m may be NULL or JSON.
    """
    cur = con.cursor()
    # Keep it permissive: select known fields, ignore if missing by trying alternatives.
    # If your schema changes later, this avoids hard-crashing.
    cols = [
        "company_id",
        "state",
        "as_of",
        "condition_1",
        "condition_2",
        "condition_3",
        "condition_4",
        "price_1m",
    ]
    sql = f"SELECT {', '.join(cols)} FROM model_state ORDER BY company_id"
    rows = cur.execute(sql).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        # Parse price_1m JSON if present
        p = d.get("price_1m")
        if isinstance(p, str) and p.strip():
            try:
                d["price_1m"] = json.loads(p)
            except Exception:
                d["price_1m"] = None
        else:
            d["price_1m"] = None
        out.append(d)
    return out


def load_events(con: sqlite3.Connection, limit: int = 100) -> List[Dict[str, Any]]:
    """
    We assume table state_events with:
    company_id, as_of, prev_state, new_state
    """
    cur = con.cursor()
    sql = """
      SELECT company_id, as_of, prev_state, new_state
      FROM state_events
      ORDER BY as_of DESC
      LIMIT ?
    """
    rows = cur.execute(sql, (limit,)).fetchall()
    return [dict(r) for r in rows]


# ----------------------------
# Export
# ----------------------------

def export_dashboard(
    db_path: Path = DEFAULT_DB,
    companies_yaml: Path = DEFAULT_COMPANIES_YAML,
    out_json: Path = DEFAULT_OUT_JSON,
) -> Path:
    companies = load_companies_yaml(companies_yaml)

    # Fill missing company names via SEC (best-effort)
    ua = _sec_user_agent()
    session = requests.Session()
    for c in companies:
        if c.name:
            continue
        if not c.cik:
            continue
        name = fetch_entity_name_from_companyfacts(c.cik, session=session, ua=ua)
        if name:
            c.name = name
        # be polite to SEC. 20 companies is fine, but don't hammer.
        time.sleep(0.12)

    con = db_connect(db_path)
    try:
        latest_state = load_latest_state(con)
        events = load_events(con, limit=100)
    finally:
        con.close()

    # Join config into a "companies" section for the UI
    companies_out: List[Dict[str, Any]] = []
    for c in companies:
        companies_out.append(
            {
                "company_id": c.company_id,
                "ticker": c.ticker,
                "cik": c.cik,
                "bucket": c.bucket,
                "in_scope": bool(c.in_scope),
                "name": c.name,  # <- what the UI will display
            }
        )

    payload = {
        "generated_at": _utc_now_iso(),
        "companies": companies_out,
        "latest_state": latest_state,
        "events": events,
    }

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2, sort_keys=False))
    return out_json


def main() -> None:
    out = export_dashboard()
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()