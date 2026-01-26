from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import yaml

from slidemodel.edgar.sec_client import fetch_company_facts
from slidemodel.edgar.submissions import (
    fetch_submissions,
    recent_8k_accessions_with_dates,
)
from slidemodel.edgar.filing_text import fetch_8k_text
from slidemodel.features.condition1 import condition_1_from_facts
from slidemodel.features.condition2 import condition_2_from_facts
from slidemodel.features.condition3 import condition_3_from_facts
from slidemodel.features.condition4 import condition_4_from_text
from slidemodel.models.signals import ConditionFlags, EvaluationInput
from slidemodel.notify.slack import notify
from slidemodel.state.machine import next_state
from slidemodel.storage.db import (
    connect,
    get_latest_state,
    init_schema,
    upsert_company,
    write_event,
    write_state,
)


def run(
    data_dir: str = "data",
    config_path: str = "src/slidemodel/config/companies.yaml",
) -> None:
    cfg = yaml.safe_load(Path(config_path).read_text())
    companies = cfg["companies"]

    conn = connect(Path(data_dir))
    init_schema(conn)

    today = date.today().isoformat()

    for c in companies:
        upsert_company(conn, c)

        # --- EARLY EXIT: don't even fetch SEC data if out of scope ---
        if not bool(c.get("in_scope", True)):
            print(f"{c['ticker']} skipped (out of scope)")
            continue

        # --- Fetch SEC facts and compute conditions ---
        try:
            facts = fetch_company_facts(c["cik"])

            c1 = condition_1_from_facts(facts)
            c2 = condition_2_from_facts(facts)
            c3 = condition_3_from_facts(
                facts,
                revenue_deceleration_2q=bool(c1.get("revenue_deceleration_2q")),
                margin_failure_2q=bool(c1.get("margin_failure_2q")),
            )
        except KeyError as e:
            # Missing/unsupported series in SEC company facts for this ticker.
            # We don't want one bad mapping to kill the entire batch.
            print(f"{c['ticker']} skipped (insufficient SEC series): {e}")
            continue
        except Exception as e:
            # Catch-all: keep the batch running; surface the exception.
            print(
                f"{c['ticker']} skipped (error fetching/evaluating): "
                f"{type(e).__name__}: {e}"
            )
            continue

        # Default Condition 4 (no initiative detected)
        c4 = {
            "initiative_detected": False,
            "keyword": None,
            "snippet": None,
            "filing_accession": None,
            "filing_filename": None,
            "condition_4": False,
        }

        # Guardrail: only scan 8-Ks if Condition 1 is already true
        if bool(c1.get("condition_1")):
            try:
                submissions = fetch_submissions(c["cik"])
                accessions = recent_8k_accessions_with_dates(submissions, days=90)

                for accession, filed_date in accessions:
                    filename, text = fetch_8k_text(c["cik"], accession)
                    candidate = condition_4_from_text(
                        filing_accession=accession,
                        filing_filename=filename,
                        text=text,
                        revenue_deceleration_2q=bool(
                            c1.get("revenue_deceleration_2q")
                        ),
                        margin_failure_2q=bool(c1.get("margin_failure_2q")),
                    )
                    if candidate.get("initiative_detected"):
                        c4 = candidate
                        break
            except Exception as e:
                # Condition 4 is optional; don't fail evaluation if text fetch/parsing breaks.
                print(
                    f"{c['ticker']} note: condition_4 scan failed: "
                    f"{type(e).__name__}: {e}"
                )

        flags = ConditionFlags(
            condition_1=bool(c1.get("condition_1")),
            condition_2=bool(c2.get("condition_2")),
            condition_3=bool(c3.get("condition_3")),
            condition_4=bool(c4.get("condition_4")),
        )

        prev = get_latest_state(conn, c["company_id"]) or "MONITOR"

        evaluation = EvaluationInput(
            in_scope=True,
            has_sufficient_data=True,
            prev_state=prev,
            flags=flags,
        )

        new_state = next_state(evaluation).value

        write_state(
            conn,
            {
                "company_id": c["company_id"],
                "as_of": today,
                "state": new_state,
                "condition_1": flags.condition_1,
                "condition_2": flags.condition_2,
                "condition_3": flags.condition_3,
                "condition_4": flags.condition_4,
                "details_json": json.dumps(
                    {"c1": c1, "c2": c2, "c3": c3, "c4": c4}
                ),
            },
        )

        if new_state != prev:
            notify(
                f"{c['ticker']} {prev} → {new_state} | "
                f"c1={int(flags.condition_1)} c2={int(flags.condition_2)} "
                f"c3={int(flags.condition_3)} c4={int(flags.condition_4)}"
            )

            write_event(
                conn,
                {
                    "company_id": c["company_id"],
                    "as_of": today,
                    "prev_state": prev,
                    "new_state": new_state,
                    "event_json": json.dumps({"trigger": "condition_update"}),
                },
            )
            print(f"{c['ticker']} STATE CHANGE: {prev} -> {new_state}")
        else:
            print(
                f"{c['ticker']} state unchanged: {new_state} "
                f"(c1={int(flags.condition_1)} "
                f"c2={int(flags.condition_2)} "
                f"c3={int(flags.condition_3)} "
                f"c4={int(flags.condition_4)})"
            )