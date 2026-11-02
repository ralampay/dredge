"""Reusable explicit rules applied to a fresh discovery baseline."""
from copy import deepcopy
from pathlib import Path

from ...core.exceptions import UserError
from ...formats.resolution import path_key, path_label, validate_rules, check_shape


def load_rules(path):
    import yaml
    try:
        with Path(path).open(encoding='utf-8') as stream:
            document = yaml.safe_load(stream)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise UserError('Cannot load resolution rules; check the path and YAML syntax.') from exc
    validate_rules_document(document)
    return document


def validate_rules_document(document):
    if (not isinstance(document, dict) or set(document) != {'rules_version', 'datasets'} or
            type(document['rules_version']) is not int or document['rules_version'] != 1):
        raise UserError('Resolution rules require rules_version: 1 and datasets.')
    datasets = document['datasets']
    if not isinstance(datasets, list) or not datasets:
        raise UserError('Resolution rules must contain at least one dataset.')
    seen = set()
    for entry in datasets:
        if (not isinstance(entry, dict) or set(entry) != {'id', 'rules'} or
                not isinstance(entry['id'], str) or not entry['id'] or entry['id'] in seen):
            raise UserError('Resolution dataset IDs must be nonempty and unique.')
        seen.add(entry['id'])
        if not isinstance(entry['rules'], list) or not entry['rules']:
            raise UserError('Each resolution dataset requires at least one rule.')
        executable = []
        for rule in entry['rules']:
            if not isinstance(rule, dict) or rule.get('op') not in ('assert_type', 'extract_number'):
                raise UserError('Reusable rules support assert_type and extract_number; use --unresolved-as json for fallback.')
            if rule.get('op') == 'assert_type':
                if set(rule) != {'op', 'path', 'schema'}:
                    raise UserError('Type assertion rules require op, path, and schema.')
                schema = rule['schema']
                if not isinstance(schema, dict) or 'type' not in schema or {'name', 'nullable'} & set(schema):
                    raise UserError('Assertion schema must declare a type and preserve the target name/nullability.')
                from ...core.schema_manifest import validate_node
                candidate = {**schema, 'nullable': False}
                validate_node(candidate, version=3)
                check_shape(candidate)
                executable.append({k:v for k,v in rule.items() if k != 'schema'})
            else:
                executable.append(rule)
        validate_rules(executable)


def _target(schema, path):
    node = schema
    for kind, value in path_key(path):
        if kind == 'field' and node['type'] == 'struct':
            node = next((f for f in node['fields'] if f['name'] == value), None)
        elif kind == 'items' and node['type'] == 'list':
            node = node['items']
        else:
            node = None
        if node is None:
            raise UserError('Resolution path is missing or incompatible: ' + path_label(path))
    return node


def _compatible(actual, requested):
    if isinstance(requested, dict):
        return isinstance(actual, dict) and all(k in actual and _compatible(actual[k], v) for k,v in requested.items())
    if isinstance(requested, list):
        return isinstance(actual, list) and len(actual) == len(requested) and all(_compatible(a,b) for a,b in zip(actual, requested))
    return actual == requested


def apply_rules(baseline, rules):
    """Return an edited draft, selected IDs, and applied/satisfied diagnostics."""
    validate_rules_document(rules)
    draft = deepcopy(baseline.document)
    by_id = {d['id']: d for d in draft['datasets']}
    selected, results = [], []
    for entry in rules['datasets']:
        identity = entry['id']
        if identity not in by_id:
            raise UserError('Resolution dataset is absent from fresh discovery: ' + identity)
        selected.append(identity)
        dataset = by_id[identity]
        operations = []
        for rule in entry['rules']:
            node = _target(dataset['schema'], rule['path'])
            requested = rule.get('schema', {'type': 'float'})
            status = 'satisfied'
            needs_resolution = ((node['type'] == 'unknown' and rule['op'] == 'assert_type') or
                                (node['type'] == 'union' and rule['op'] == 'extract_number'))
            if needs_resolution:
                retained = {k:v for k,v in node.items() if k in ('name', 'nullable', 'source', 'inference')}
                node.clear()
                node.update({**retained, **deepcopy(requested)})
                operations.append({k:deepcopy(v) for k,v in rule.items() if k != 'schema'})
                status = 'applied'
            elif not _compatible(node, requested) or (rule['op'] == 'extract_number' and node.get('storage_type', 'double') != 'double'):
                raise UserError('Resolution rule conflicts with fresh discovery at ' + path_label(rule['path']))
            results.append({'dataset_id': identity, 'path': deepcopy(rule['path']), 'op': rule['op'], 'status': status})
        dataset['resolutions'] = operations
    return draft, selected, results
