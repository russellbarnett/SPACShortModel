from __future__ import annotations

import argparse

from slidemodel.cli import evaluate_once
from slidemodel.cli import export_dashboard
from slidemodel.storage.db import connect, init_schema
from pathlib import Path


def _cmd_init_db(args: argparse.Namespace) -> None:
    conn = connect(Path(args.data_dir))
    init_schema(conn)
    print(f"Initialized sqlite db in {Path(args.data_dir).resolve()}")


def _cmd_run_once(args: argparse.Namespace) -> None:
    evaluate_once.run(data_dir=args.data_dir, config_path=args.config)


def _cmd_evaluate(args: argparse.Namespace) -> None:
    # If you have a separate batch evaluator module, call it here.
    # For now, reuse evaluate_once.run which loops over companies.yaml in your code.
    evaluate_once.run(data_dir=args.data_dir, config_path=args.config)


def _cmd_export_dashboard(args: argparse.Namespace) -> None:
    export_dashboard.run(data_dir=args.data_dir, out_dir=args.out_dir)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="slidemodel")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-db", help="Initialize local sqlite database")
    p_init.add_argument("--data-dir", default="data")
    p_init.set_defaults(func=_cmd_init_db)

    p_once = sub.add_parser("run-once", help="Evaluate companies once, update db")
    p_once.add_argument("--data-dir", default="data")
    p_once.add_argument("--config", default="src/slidemodel/config/companies.yaml")
    p_once.set_defaults(func=_cmd_run_once)

    p_eval = sub.add_parser("evaluate", help="Evaluate companies, update db, emit state change events")
    p_eval.add_argument("--data-dir", default="data")
    p_eval.add_argument("--config", default="src/slidemodel/config/companies.yaml")
    p_eval.set_defaults(func=_cmd_evaluate)

    p_dash = sub.add_parser("export-dashboard", help="Export dashboard JSON to docs/ (GitHub Pages)")
    p_dash.add_argument("--data-dir", default="data")
    p_dash.add_argument("--out-dir", default="docs")
    p_dash.set_defaults(func=_cmd_export_dashboard)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()