from __future__ import annotations
import yaml
from pathlib import Path
from datetime import date

from slidemodel.edgar.sec_client import fetch_company_facts
from slidemodel.features.condition1 import condition_1_from_facts

def run(config_path: str = "src/slidemodel/config/companies.yaml") -> None:
    cfg = yaml.safe_load(Path(config_path).read_text())
    companies = cfg["companies"]

    for c in companies:
        if c.get("ticker") != "BYND":
            continue
        facts = fetch_company_facts(c["cik"])
        out = condition_1_from_facts(facts)
        print("BYND Condition 1 result:")
        print("revenue_tag:", out["revenue_tag"])
        print("gross_profit_tag:", out["gross_profit_tag"])
        print("revenue_deceleration_2q:", out["revenue_deceleration_2q"])
        print("margin_failure_2q:", out["margin_failure_2q"])
        print("condition_1:", out["condition_1"])
        print("last_date:", out["dates"][-1] if out["dates"] else None)
