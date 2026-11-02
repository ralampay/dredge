"""Inspect loaded configuration independently of terminal presentation."""

from dataclasses import dataclass
from pathlib import Path

from .configuration import EnvironmentConfig


SECRET_MARKERS = (
    "SECRET", "TOKEN", "PASSWORD", "PASSWD", "API_KEY", "PRIVATE_KEY", "CREDENTIAL",
)


@dataclass(frozen=True)
class DoctorEntry:
    name: str
    display_value: str
    source: str


@dataclass(frozen=True)
class EnvironmentReport:
    path: Path
    exists: bool
    entries: tuple[DoctorEntry, ...]


class InspectEnvironment:
    def __init__(self, config: EnvironmentConfig):
        self.config = config

    def execute(self) -> EnvironmentReport:
        entries = []
        for entry in self.config.entries:
            if entry.value is None:
                value = "<unset>"
            elif entry.value == "":
                value = "<empty>"
            elif any(marker in entry.name.upper() for marker in SECRET_MARKERS):
                value = "********"
            else:
                value = entry.value
            entries.append(DoctorEntry(entry.name, value, entry.source))
        return EnvironmentReport(self.config.path, self.config.exists, tuple(entries))
