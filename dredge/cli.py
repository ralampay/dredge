"""CLI lifecycle: parse, validate, load configuration, and dispatch lazily."""
import argparse
from collections.abc import Sequence

from .cli_config import build_runtime_config
from .cli_options import OPTIONS
from .cli_routing import option_groups, resolve_runner, route_choices, validate_route
from .core.configuration import load_environment
from .core.doctor import InspectEnvironment
from .core.exceptions import UserAbort, UserError
from .core.ui import print_environment_report, print_error


def build_parser() -> argparse.ArgumentParser:
    package = __package__.split(".")[0]
    parser = argparse.ArgumentParser(prog=f"python -m {package}",
                                     description="Data discovery and ingestion pipeline tools.")
    choices = route_choices()
    for option in OPTIONS:
        option.add_to(parser, **({"choices": choices[option.destination]}
                                if option.destination in choices else {}))
    for title, options in option_groups():
        group = parser.add_argument_group(title)
        for option in options:
            option.add_to(group)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    route = validate_route(parser, vars(args))
    if not args.doctor and route is None:
        parser.print_help()
        return 0
    try:
        config = load_environment()
        if args.doctor:
            print_environment_report(InspectEnvironment(config).execute())
        else:
            report = resolve_runner(route)(build_runtime_config(args))
            return 1 if report.partial else 0
    except UserError as exc:
        print_error(str(exc))
        return 1
    except (UserAbort, KeyboardInterrupt):
        print_error("Cancelled.")
        return 130
    return 0
