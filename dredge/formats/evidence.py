"""Bounded schema evidence: counts and types only, never sampled values."""
from collections import Counter
from datetime import date, datetime
from decimal import Decimal

from .temporal import temporal_kind


def observed_type(value):
    if value is None:
        return 'null'
    if isinstance(value, bool):
        return 'boolean'
    if isinstance(value, int):
        return 'integer'
    if isinstance(value, (float, Decimal)):
        return 'float'
    if isinstance(value, datetime):
        return 'timestamp'
    if isinstance(value, date):
        return 'date'
    if isinstance(value, dict):
        return 'struct'
    if isinstance(value, list):
        return 'list'
    return 'string'


class Evidence:
    def __init__(self):
        self.present = 0
        self.types = Counter()
        self.temporal = Counter()
        self.children = {}
        self.items = None

    def observe(self, value):
        self.present += 1
        self.types[observed_type(value)] += 1
        candidate = temporal_kind(value)
        if candidate:
            self.temporal[candidate] += 1
        if isinstance(value, dict):
            for name, item in value.items():
                self.children.setdefault(name, Evidence()).observe(item)
        elif isinstance(value, list):
            if self.items is None:
                self.items = Evidence()
            for item in value:
                self.items.observe(item)

    def combine(self, other):
        self.present += other.present
        self.types.update(other.types)
        self.temporal.update(other.temporal)
        for name, child in other.children.items():
            self.children.setdefault(name, Evidence()).combine(child)
        if other.items:
            if self.items is None:
                self.items = Evidence()
            self.items.combine(other.items)


def annotate(schema, evidence, *, origin, temporal_types, source_kind, path='', parent_count=None, positions=None):
    """Attach assumptions after schema merging, so metadata cannot affect merging."""
    node = dict(schema)
    inference = {'origin': origin}
    if evidence is not None:
        non_null = evidence.present - evidence.types['null']
        inference.update(present_count=evidence.present, null_count=evidence.types['null'],
                         missing_count=max(0, (parent_count or 0) - evidence.present),
                         observed_types=dict(sorted(evidence.types.items())))
        if schema['type'] == 'string' and temporal_types == 'auto' and non_null:
            for rule, count in evidence.temporal.items():
                if count == non_null:
                    node.update(type='date' if rule == 'iso-date' else 'timestamp', input_format=rule)
                    if node['type'] == 'timestamp':
                        node.update(unit='us', timezone='UTC')
                    inference['assumption'] = 'all_observed_non_null_strings_match_' + rule
                    break
            else:
                if evidence.temporal:
                    inference['diagnostic'] = 'mixed_temporal_and_other_strings'
    node['inference'] = inference
    if 'name' in schema:
        node['source'] = {'kind': source_kind, 'path': path}
        if positions is not None and schema['name'] in positions:
            node['source']['positions'] = sorted(positions[schema['name']])
    if schema['type'] == 'struct':
        node['fields'] = [annotate(field, evidence.children.get(field['name'], Evidence()) if evidence else None,
                                  origin=origin, temporal_types=temporal_types, source_kind=source_kind,
                                  path=path + '/' + field['name'].replace('~', '~0').replace('/', '~1'),
                                  parent_count=evidence.types['struct'] if evidence else None, positions=positions if not path else None)
                          for field in schema['fields']]
    elif schema['type'] == 'list':
        node['items'] = annotate(schema['items'], evidence.items if evidence else None, origin=origin,
                                 temporal_types=temporal_types, source_kind=source_kind, path=path + '/*')
    return node
