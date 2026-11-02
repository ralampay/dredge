"""Compatibility imports; implementation lives in modules.discover.readers."""
from ...modules.discover.readers import InspectObject
from ...formats.records import SampleLimit, RangeReader, ExpandedReader, UTF8Reader, json_records, text_records, invalid_constant

__all__ = ['SampleLimit', 'RangeReader', 'ExpandedReader', 'UTF8Reader', 'json_records', 'text_records', 'invalid_constant', 'InspectObject']
