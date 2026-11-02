"""Structured discovery results."""
from dataclasses import dataclass


@dataclass
class DiscoveryReport:
    document: dict

    @property
    def partial(self) -> bool:
        return self.document["status"] == "partial"
