"""Structured results for terminal-independent imagery commands."""
from dataclasses import dataclass


@dataclass(frozen=True)
class ImageryResult:
    document: dict
    partial: bool = False
