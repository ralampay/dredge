"""Immutable route metadata and lazy, project-owned entry point resolution."""
from dataclasses import dataclass
from importlib import import_module

from .cli_options import OPTIONS as GLOBAL_OPTIONS


@dataclass(frozen=True)
class RouteDescriptor:
    module: str
    mode: str
    actions: tuple[str, ...]
    runner: str
    option_inventories: tuple[tuple[str, str], ...]
    validators: tuple[str, ...]


ROUTES = (
    RouteDescriptor(
        module="discover", mode="s3", actions=("crawl",),
        runner=".modules.discover.runner:run_discovery",
        option_inventories=(
            ("Schema discovery", ".modules.discover.cli_options:OPTIONS"),
            ("S3 source", ".modes.s3.cli_options:OPTIONS"),
        ),
        validators=(".modules.discover.cli_options:validate_options",
                    ".modes.s3.cli_options:validate_options"),
    ),
)


def load_reference(reference):
    module, name = reference.split(":")
    return getattr(import_module(module, package=__package__), name)


def resolve_route(module, mode):
    return next((route for route in ROUTES if (route.module, route.mode) == (module, mode)), None)


def resolve_runner(route):
    return load_reference(route.runner)


def option_groups():
    """Yield each inventory once when several routes share a module or mode."""
    seen = set()
    for route in ROUTES:
        for title, reference in route.option_inventories:
            if reference not in seen:
                seen.add(reference)
                yield title, load_reference(reference)


def route_choices():
    return {
        "module": tuple(sorted({route.module for route in ROUTES})),
        "mode": tuple(sorted({route.mode for route in ROUTES})),
        "action": tuple(sorted({action for route in ROUTES for action in route.actions})),
    }


def validate_route(parser, config):
    supplied = {name for name, value in config.items() if value is not None and value is not False}
    if config.get("doctor"):
        if supplied != {"doctor"}:
            parser.error("--doctor cannot be combined with crawl options")
        return None
    if not supplied:
        return None
    for name in ("module", "mode", "action"):
        if not config.get(name):
            parser.error(f"--{name} is required")
    route = resolve_route(config["module"], config["mode"])
    if route is None:
        parser.error("Unsupported --module and --mode combination")
    if config["action"] not in route.actions:
        parser.error(f"Unsupported action for {route.module}/{route.mode}: {config['action']}")
    allowed = {option.destination for option in GLOBAL_OPTIONS}
    for _, reference in route.option_inventories:
        allowed.update(option.destination for option in load_reference(reference))
    unexpected = supplied - allowed
    if unexpected:
        parser.error("Options do not apply to this route: " + ", ".join(
            "--" + name.replace("_", "-") for name in sorted(unexpected)))
    for reference in route.validators:
        load_reference(reference)(parser, config)
    return route
