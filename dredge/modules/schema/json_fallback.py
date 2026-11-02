"""Build explicit encoding operations from unresolved discovery evidence."""
from copy import deepcopy

from ...formats.json_fallback import fallback_target


def apply_json_fallback(baseline):
    draft = deepcopy(baseline.document)
    statuses = []
    for dataset in draft['datasets']:
        rules = []
        def visit(node, path):
            if path and fallback_target(node):
                rule = {'op': 'encode_json', 'path': path}
                rules.append(rule)
                statuses.append({'dataset_id': dataset['id'], **deepcopy(rule), 'status': 'applied'})
                retained = {key:value for key,value in node.items() if key in ('name', 'nullable', 'source', 'inference')}
                node.clear()
                node.update({**retained, 'type': 'string'})
                return
            for field in node.get('fields', []):
                visit(field, path + [{'field': field['name']}])
            if 'items' in node:
                visit(node['items'], path + [{'items': True}])
        visit(dataset['schema'], [])
        dataset['resolutions'] = rules
    return draft, [d['id'] for d in draft['datasets']], statuses
