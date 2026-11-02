"""CLI parsing and application composition."""

import argparse
from collections.abc import Sequence

from .core.configuration import load_environment
from .core.doctor import InspectEnvironment
from .core.exceptions import UserAbort, UserError
from .core.ui import print_environment_report, print_error


def build_parser() -> argparse.ArgumentParser:
    package = __package__.split(".")[0]
    parser = argparse.ArgumentParser(
        prog=f"python -m {package}", description="A minimal Python CLI application."
    )
    parser.add_argument("--doctor", action="store_true", help="Show the .env path and effective configuration.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.doctor:
        parser.print_help()
        return 0
    try:
        config = load_environment()
        print_environment_report(InspectEnvironment(config).execute())
    except UserError as exc:
        print_error(str(exc))
        return 1
    except (UserAbort, KeyboardInterrupt):
        print_error("Cancelled.")
        return 130
    return 0
