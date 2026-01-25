from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any

DB_NAME = "slidemodel.sqlite3"

def db_path(data_dir: Path) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / DB_NAME

def connect(data_dir: Path) -> sqlite3.Connection:
    path = db_path(data_dir)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn

def init_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS companies (
            company_id TEXT PRIMARY KEY,
            ticker TEXT NOT NULL,
            cik TEXT NOT NULL,
            bucket TEXT NOT NULL,
            in_scope INTEGER NOT NULL DEFAULT 1
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS model_state (
            company_id TEXT NOT NULL,
            as_of TEXT NOT NULL,
            state TEXT NOT NULL,
            condition_1 INTEGER NOT NULL,
            condition_2 INTEGER NOT NULL,
            condition_3 INTEGER NOT NULL,
            condition_4 INTEGER NOT NULL,
            details_json TEXT,
            PRIMARY KEY (company_id, as_of),
            FOREIGN KEY (company_id) REFERENCES companies(company_id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS state_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id TEXT NOT NULL,
            as_of TEXT NOT NULL,
            prev_state TEXT NOT NULL,
            new_state TEXT NOT NULL,
            event_json TEXT,
            FOREIGN KEY (company_id) REFERENCES companies(company_id)
        );
        """
    )

    conn.commit()

def upsert_company(conn: sqlite3.Connection, company: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO companies(company_id, ticker, cik, bucket, in_scope)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET
            ticker=excluded.ticker,
            cik=excluded.cik,
            bucket=excluded.bucket,
            in_scope=excluded.in_scope
        """,
        (
            company["company_id"],
            company["ticker"],
            company["cik"],
            company["bucket"],
            int(company.get("in_scope", True)),
        ),
    )
    conn.commit()

def get_latest_state(conn: sqlite3.Connection, company_id: str) -> Optional[str]:
    row = conn.execute(
        """
        SELECT state FROM model_state
        WHERE company_id = ?
        ORDER BY as_of DESC
        LIMIT 1
        """,
        (company_id,),
    ).fetchone()
    return row["state"] if row else None

def write_state(conn: sqlite3.Connection, record: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO model_state(
            company_id, as_of, state, condition_1, condition_2, condition_3, condition_4, details_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["company_id"],
            record["as_of"],
            record["state"],
            int(record["condition_1"]),
            int(record["condition_2"]),
            int(record["condition_3"]),
            int(record["condition_4"]),
            record.get("details_json"),
        ),
    )
    conn.commit()

def write_event(conn: sqlite3.Connection, event: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO state_events(company_id, as_of, prev_state, new_state, event_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            event["company_id"],
            event["as_of"],
            event["prev_state"],
            event["new_state"],
            event.get("event_json"),
        ),
    )
    conn.commit()
