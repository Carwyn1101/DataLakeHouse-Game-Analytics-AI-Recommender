"""CLI for collector staging. The published pipeline is outside this workflow."""

import argparse
import sys

from .contracts import ContractError
from .staging import collect_batch, enrich_batch, read_staging_input, staging_root
from .steam import HttpClient, SteamCollector


def main(argv=None):
    parser = argparse.ArgumentParser(description="Collect or enrich game CSVs in isolated staging.")
    parser.add_argument("--stage-dir", required=True, help="Dedicated directory outside the repository and Airflow mount")
    commands = parser.add_subparsers(dest="command", required=True)

    collect = commands.add_parser("collect", help="Collect raw games, reviews, popularity and tags for explicit App IDs")
    collect.add_argument("--batch-id", required=True)
    collect.add_argument("--app-id", action="append", default=[], help="Repeat for multiple Steam App IDs")
    collect.add_argument("--ids-file", help="UTF-8 file inside staging, one App ID per line")
    collect.add_argument("--language", default="all", help="Steam review language")
    collect.add_argument("--timeout", type=float, default=15.0)
    collect.add_argument("--attempts", type=int, default=3)
    collect.add_argument("--max-retry-delay", type=float, default=60.0)
    collect.add_argument("--steamspy-interval", type=float, default=1.1)

    enrich = commands.add_parser("enrich", help="Join a staged 15-column games file to a staged tags sidecar")
    enrich.add_argument("--batch-id", required=True)
    enrich.add_argument("--games", required=True)
    enrich.add_argument("--tags", required=True)
    args = parser.parse_args(argv)

    try:
        if args.command == "collect":
            ids = args.app_id
            if args.ids_file:
                root = staging_root(args.stage_dir)
                content = read_staging_input(root, args.ids_file).decode("utf-8-sig")
                ids = ids + [line.strip() for line in content.splitlines() if line.strip()]
            client = HttpClient(timeout=args.timeout, attempts=args.attempts,
                                max_delay=args.max_retry_delay, steamspy_interval=args.steamspy_interval)
            collect_batch(args.stage_dir, args.batch_id, ids, SteamCollector(client), language=args.language)
        else:
            enrich_batch(args.stage_dir, args.batch_id, args.games, args.tags)
    except ContractError as error:
        print(f"Staging stopped: {error}", file=sys.stderr)
        return 1
    except (OSError, UnicodeError):
        print("Staging I/O failed. Check the isolated inputs and existing batch state.", file=sys.stderr)
        return 1
    print("Staging batch complete and integrity checked.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
