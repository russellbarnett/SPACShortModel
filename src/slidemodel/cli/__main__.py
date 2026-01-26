import argparse
from pathlib import Path

from dotenv import load_dotenv

from slidemodel.storage.db import connect, init_schema
from slidemodel.cli.run_once import run as run_once
from slidemodel.cli.evaluate_once import run as evaluate


def main() -> None:
    # Load environment variables from .env (Slack webhook, SEC user agent, etc.)
    load_dotenv()

    parser = argparse.ArgumentParser(prog="slidemodel")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # init-db
    p_init = sub.add_parser(
        "init-db",
        help="Initialize local sqlite database",
    )
    p_init.add_argument("--data-dir", default="data")

    # run-once
    p_run = sub.add_parser(
        "run-once",
        help="Fetch one company (BYND) and compute conditions",
    )
    p_run.add_argument(
        "--config",
        default="src/slidemodel/config/companies.yaml",
    )

    # evaluate
    p_eval = sub.add_parser(
        "evaluate",
        help="Evaluate companies, update db, emit state change events",
    )
    p_eval.add_argument("--data-dir", default="data")
    p_eval.add_argument(
        "--config",
        default="src/slidemodel/config/companies.yaml",
    )

    args = parser.parse_args()

    if args.cmd == "init-db":
        conn = connect(Path(args.data_dir))
        init_schema(conn)
        print(f"Initialized DB at {Path(args.data_dir).resolve()}/slidemodel.sqlite3")

    elif args.cmd == "run-once":
        run_once(args.config)

    elif args.cmd == "evaluate":
        evaluate(args.data_dir, args.config)


if __name__ == "__main__":
    main()