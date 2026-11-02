"""Structured discovery results."""
from dataclasses import dataclass


@dataclass
class DiscoveryReport:
    document: dict

    @property
    def partial(self) -> bool:
        return self.document["status"] == "partial"


@dataclass(frozen=True)
class CrawlResolutionProgress:
    phase: str
    detail: object = None


@dataclass(frozen=True)
class CrawlResolutionResult:
    document: dict
    rules: list[dict]
    partial: bool = False
