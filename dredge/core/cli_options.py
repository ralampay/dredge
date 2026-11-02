"""Immutable argument metadata; importing options never loads workflows."""
import argparse
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping


@dataclass(frozen=True)
class CliOption:
    flags: tuple[str, ...]
    settings: Mapping[str, Any]

    def __post_init__(self):
        object.__setattr__(self, "settings", MappingProxyType(dict(self.settings)))

    @property
    def destination(self) -> str:
        return self.settings.get("dest", self.flags[0].lstrip("-").replace("-", "_"))

    def add_to(self, parser, **overrides):
        parser.add_argument(*self.flags, **{**self.settings, **overrides})


def positive(value):
    """Shared argparse converter for positive integer limits."""
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError('must be a positive integer') from exc
    if number <= 0:
        raise argparse.ArgumentTypeError('must be a positive integer')
    return number
