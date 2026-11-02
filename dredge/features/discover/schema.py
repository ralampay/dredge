"""Compatibility imports; implementation lives in modules.discover.schema."""
from ...modules.discover.schema import (infer, merge, conflicts, csv_value, arrow_schema)

__all__ = ['infer', 'merge', 'conflicts', 'csv_value', 'arrow_schema']
