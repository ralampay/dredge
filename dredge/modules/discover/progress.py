"""Terminal-independent discovery progress events and read accounting."""
from dataclasses import dataclass

# Compatibility import for callers of the initial discovery progress API.
from ...core.sources import CountedSource


@dataclass(frozen=True)
class DiscoveryProgress:
    phase: str = 'listing'
    listed: int = 0
    matched: int = 0
    datasets: int = 0
    total_files: int = 0
    completed_files: int = 0
    failed_files: int = 0
    current_key: str = ''
    bytes_read: int = 0
