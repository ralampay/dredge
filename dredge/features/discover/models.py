"""Compatibility imports for discovery requests, results, and source contracts."""
from ...core.sources import ObjectInfo, ObjectSource, SourceAccessError
from ...modules.discover.requests import CrawlOptions
from ...modules.discover.models import DiscoveryReport

__all__ = ["ObjectInfo", "ObjectSource", "SourceAccessError", "CrawlOptions", "DiscoveryReport"]
