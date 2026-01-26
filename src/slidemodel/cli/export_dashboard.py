from __future__ import annotations

import csv
import json
import time
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from slidemodel.storage.db import connect


def _rows(conn, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _to_int01(v: Any) -> int:
    try:
        return 1 if int(v) == 1 else 0
    except Exception:
        return 0


def _safe_float(v: Any) -> Optional[float]:
    try:
        x = float(v)
        if x != x:  # NaN
            return None
        return x
    except Exception:
        return None


def _fetch_stooq_daily_csv(symbol: str) -> Optional[str]:
    """
    Stooq endpoint:
      https://stooq.com/q/d/l/?s=tsla.us&i=d
    Returns CSV as text, or None on failure.
    """
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    headers = {
        "User-Agent": "SPACShortModel/1.0 (dashboard export; contact: you)",
        "Accept": "text/csv,*/*;q=0.8",
    }
    try:
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code != 200:
            return None
        txt = (r.text or "").strip()
        if not txt or "No data" in txt:
            return None
        return txt
    except Exception:
        return None


def _parse_stooq_closes_last_30d(csv_text: str) -> Optional[Dict[str, Any]]:
    """
    Parse Stooq daily CSV, take last ~30 calendar days of closes (or last 22 trading rows fallback).
    Returns:
      {
        closes: [...],
        pct_change: ...,
        start: ...,
        end: ...,
        source: "stooq"
      }
    """
    try:
        f = StringIO(csv_text)
        reader = csv.DictReader(f)
        rows: List[Tuple[str, float]] = []
        for row in reader:
            d = row.get("Date")
            c = _safe_float(row.get("Close"))
            if not d or c is None:
                continue
            rows.append((d, float(c)))

        if len(rows) < 2:
            return None

        today = datetime.now(timezone.utc).date()
        cutoff = today - timedelta(days=35)

        filtered: List[Tuple[str, float]] = []
        for d, c in rows:
            try:
                dd = datetime.strptime(d, "%Y-%m-%d").date()
            except Exception:
                continue
            if dd >= cutoff:
                filtered.append((d, c))

        if len(filtered) < 2:
            filtered = rows[-22:] if len(rows) >= 22 else rows

        if len(filtered) < 2:
            return None

        closes = [c for _, c in filtered]
        start_close = closes[0]
        end_close = closes[-1]
        if start_close == 0:
            return None

        pct_change = (end_close - start_close) / start_close * 100.0

        return {
            "closes": closes,
            "pct_change": round(pct_change, 2),
            "start": filtered[0][0],
            "end": filtered[-1][0],
            "source": "stooq",
        }
    except Exception:
        return None


def _price_1m_for_ticker(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Try common Stooq symbol patterns:
      - {ticker}.us (most US equities)
      - {ticker} (sometimes works)
    """
    t = (ticker or "").strip().lower()
    if not t:
        return None

    for sym in (f"{t}.us", t):
        txt = _fetch_stooq_daily_csv(sym)
        if not txt:
            continue
        parsed = _parse_stooq_closes_last_30d(txt)
        if parsed:
            return parsed
    return None


def _stdev(xs: List[float]) -> Optional[float]:
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    var = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return var ** 0.5


def _price_metrics(price_1m: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Computes simple 1m metrics from closes:
      - return_1m_pct
      - drawdown_1m_pct (negative number, worst peak-to-trough)
      - vol_1m_daily_pct (stdev of daily % returns)
    """
    if not price_1m or not isinstance(price_1m, dict):
        return {"return_1m_pct": None, "drawdown_1m_pct": None, "vol_1m_daily_pct": None}

    closes = price_1m.get("closes")
    if not isinstance(closes, list) or len(closes) < 2:
        return {"return_1m_pct": None, "drawdown_1m_pct": None, "vol_1m_daily_pct": None}

    cs = [float(x) for x in closes if isinstance(x, (int, float))]
    if len(cs) < 2:
        return {"return_1m_pct": None, "drawdown_1m_pct": None, "vol_1m_daily_pct": None}

    ret = price_1m.get("pct_change")
    ret = float(ret) if isinstance(ret, (int, float)) else None

    peak = cs[0]
    worst_dd = 0.0  # negative
    for c in cs:
        if c > peak:
            peak = c
        if peak > 0:
            dd = (c - peak) / peak * 100.0
            if dd < worst_dd:
                worst_dd = dd

    daily = []
    for i in range(1, len(cs)):
        if cs[i - 1] == 0:
            continue
        daily.append((cs[i] - cs[i - 1]) / cs[i - 1] * 100.0)

    vol = _stdev(daily)

    return {
        "return_1m_pct": round(ret, 2) if ret is not None else None,
        "drawdown_1m_pct": round(worst_dd, 2),
        "vol_1m_daily_pct": round(vol, 2) if vol is not None else None,
    }


def _pressure_score(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pressure score (0–10) with grade label.
    Uses:
      - condition flags (weighted)
      - 1m return, drawdown, daily vol (if available)
    """
    in_scope = _to_int01(row.get("in_scope")) == 1
    if not in_scope:
        return {
            "pressure_score": None,
            "pressure_grade": "OUT_OF_SCOPE",
            "triggered_conditions": [],
        }

    c1 = 1 if _to_int01(row.get("condition_1")) == 1 else 0
    c2 = 1 if _to_int01(row.get("condition_2")) == 1 else 0
    c3 = 1 if _to_int01(row.get("condition_3")) == 1 else 0
    c4 = 1 if _to_int01(row.get("condition_4")) == 1 else 0

    triggers = []
    if c1: triggers.append("C1")
    if c2: triggers.append("C2")
    if c3: triggers.append("C3")
    if c4: triggers.append("C4")

    score = 0
    score += 3 * c1
    score += 2 * c2
    score += 2 * c3
    score += 3 * c4

    pm = row.get("price_metrics") or {}
    r1 = pm.get("return_1m_pct")
    dd = pm.get("drawdown_1m_pct")
    vol = pm.get("vol_1m_daily_pct")

    # Price overlays (simple, interpretable)
    if isinstance(r1, (int, float)):
        if r1 <= -30:
            score += 3
        elif r1 <= -15:
            score += 2
        elif r1 <= -7:
            score += 1

    if isinstance(dd, (int, float)):
        if dd <= -35:
            score += 2
        elif dd <= -20:
            score += 1

    if isinstance(vol, (int, float)):
        # Daily vol in % terms; 4% daily is hot.
        if vol >= 4.0:
            score += 1

    if score < 0:
        score = 0
    if score > 10:
        score = 10

    if score <= 2:
        grade = "STABLE"
    elif score <= 5:
        grade = "WATCH"
    elif score <= 8:
        grade = "ELEVATED"
    else:
        grade = "CRITICAL"

    return {
        "pressure_score": score,
        "pressure_grade": grade,
        "triggered_conditions": triggers,
    }


def run(data_dir: str = "data", out_dir: str = "docs") -> None:
    conn = connect(Path(data_dir))

    companies = _rows(
        conn,
        """
        SELECT company_id, ticker, cik, bucket, in_scope
        FROM companies
        ORDER BY company_id ASC
        """,
    )

    latest_state = _rows(
        conn,
        """
        SELECT
            ms.company_id,
            c.ticker,
            c.bucket,
            c.in_scope,
            ms.as_of,
            ms.state,
            ms.condition_1,
            ms.condition_2,
            ms.condition_3,
            ms.condition_4,
            ms.details_json
        FROM model_state ms
        JOIN (
            SELECT company_id, MAX(as_of) AS max_as_of
            FROM model_state
            GROUP BY company_id
        ) x
          ON ms.company_id = x.company_id AND ms.as_of = x.max_as_of
        JOIN companies c
          ON c.company_id = ms.company_id
        ORDER BY c.ticker ASC
        """,
    )

    events = _rows(
        conn,
        """
        SELECT id, company_id, as_of, prev_state, new_state, event_json
        FROM state_events
        ORDER BY id DESC
        LIMIT 200
        """,
    )

    # Add 1-month price data + metrics + pressure score.
    for r in latest_state:
        ticker = str(r.get("ticker") or "").strip()
        r["price_1m"] = None
        r["price_metrics"] = {"return_1m_pct": None, "drawdown_1m_pct": None, "vol_1m_daily_pct": None}

        if ticker:
            price = _price_1m_for_ticker(ticker)
            r["price_1m"] = price
            r["price_metrics"] = _price_metrics(price)

            # Be polite to free endpoint
            time.sleep(0.15)

        # Pressure grading
        r.update(_pressure_score(r))

    def _count_where(pred):
        return sum(1 for rr in latest_state if pred(rr))

    summary = {
        "companies_total": len(companies),
        "latest_rows": len(latest_state),
        "in_scope_total": sum(1 for c in companies if _to_int01(c.get("in_scope")) == 1),
        "states": {
            "MONITOR": _count_where(lambda rr: rr.get("state") == "MONITOR"),
            "OUT_OF_SCOPE": _count_where(lambda rr: rr.get("state") == "OUT_OF_SCOPE"),
            "NO_DATA": _count_where(lambda rr: rr.get("state") == "NO_DATA"),
            "ERROR": _count_where(lambda rr: rr.get("state") == "ERROR"),
        },
        "prices_1m_available": _count_where(
            lambda rr: isinstance(rr.get("price_1m"), dict) and rr["price_1m"].get("closes")
        ),
        "pressure_grades": {
            "STABLE": _count_where(lambda rr: rr.get("pressure_grade") == "STABLE"),
            "WATCH": _count_where(lambda rr: rr.get("pressure_grade") == "WATCH"),
            "ELEVATED": _count_where(lambda rr: rr.get("pressure_grade") == "ELEVATED"),
            "CRITICAL": _count_where(lambda rr: rr.get("pressure_grade") == "CRITICAL"),
            "OUT_OF_SCOPE": _count_where(lambda rr: rr.get("pressure_grade") == "OUT_OF_SCOPE"),
        },
    }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "companies": companies,
        "latest_state": latest_state,
        "events": events,
    }

    outp = Path(out_dir)
    outp.mkdir(parents=True, exist_ok=True)
    (outp / "dashboard.json").write_text(json.dumps(payload, indent=2))
    print(f"Wrote {(outp / 'dashboard.json').resolve()}")