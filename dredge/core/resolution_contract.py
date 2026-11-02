"""Compilation and verification of version-4 resolution artifacts."""
from copy import deepcopy
import hashlib
import json

from .exceptions import UserError
from ..formats.resolution import compile_schema


def document_digest(document):
    return hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest()


def compile_draft(baseline, draft, selected_ids=()):
    from .schema_manifest import schema_fingerprint
    from ..formats.arrow_contract import output_schema
    if baseline['schema_version'] not in (2, 3):
        raise UserError('Resolution requires an original version-2/3 discovery baseline.')
    if not isinstance(draft, dict) or not isinstance(draft.get('datasets'), list):
        raise UserError('Edited schema must be a discovery document.')
    if {k:v for k,v in draft.items() if k != 'datasets'} != {k:v for k,v in baseline.items() if k != 'datasets'}:
        raise UserError('Only dataset field schemas and resolutions may be edited.')
    originals = baseline['datasets']
    edits = draft['datasets']
    if len(originals) != len(edits) or any(not isinstance(d, dict) for d in edits):
        raise UserError('Edited schema must preserve all datasets and their order.')
    ids = [d['id'] for d in originals]
    selected = list(dict.fromkeys(selected_ids or ids))
    if not selected or set(selected) - set(ids):
        raise UserError('Select one or more dataset IDs present in the baseline.')
    result = deepcopy(baseline)
    result['schema_version'] = 4
    for before, edited, dataset in zip(originals, edits, result['datasets']):
        allowed = {'schema', 'resolutions'}
        if {k:v for k,v in before.items() if k not in allowed} != {k:v for k,v in edited.items() if k not in allowed}:
            raise UserError('Dataset identity, parser, selector, evidence, and readiness must remain unchanged.')
        if before['id'] in selected:
            dataset['schema'], dataset['resolutions'] = compile_schema(before['schema'], edited.get('schema'), edited.get('resolutions', []))
            other_blockers = [b for b in before['ingestion']['blockers'] if b != 'unresolved_types']
            if other_blockers:
                raise UserError('Resolution cannot clear discovery blockers: ' + ', '.join(other_blockers))
            output_schema(dataset)
            dataset['ingestion']['blockers'] = []
            dataset['ingestion']['ready'] = True
        elif edited != before:
            raise UserError('Edits to unselected datasets are not allowed.')
        dataset['schema_fingerprint'] = schema_fingerprint(dataset['schema'], dataset['ingestion']['parser'],
                                                          version=4, resolutions=dataset.get('resolutions', []))
    return result, selected


def validate_resolution_document(document):
    from .schema_manifest import SchemaManifest, require
    envelope = document.get('resolution')
    require(isinstance(envelope, dict) and set(envelope) == {'version', 'baseline', 'baseline_digest', 'validation'}
            and type(envelope.get('version')) is int and envelope['version'] == 1, 'resolution provenance required')
    baseline = envelope.get('baseline')
    require(isinstance(baseline, dict) and baseline.get('schema_version') in (2, 3), 'original baseline required')
    SchemaManifest(baseline)
    require(envelope.get('baseline_digest') == document_digest(baseline), 'baseline digest mismatch')
    validations = envelope.get('validation')
    require(isinstance(validations, list) and bool(validations), 'resolution validation required')
    require(all(isinstance(v, dict) and isinstance(v.get('dataset_id'), str) for v in validations), 'invalid validation entries')
    selected = [v['dataset_id'] for v in validations]
    require(len(set(selected)) == len(selected), 'duplicate validation entries')
    draft = deepcopy(baseline)
    require(len(document['datasets']) == len(draft['datasets']), 'resolved dataset count differs from baseline')
    for candidate, edited in zip(document['datasets'], draft['datasets']):
        require(candidate['id'] == edited['id'], 'resolved dataset identity/order changed')
        edited['schema'] = deepcopy(candidate['schema'])
        if candidate['id'] in selected:
            edited['resolutions'] = deepcopy(candidate.get('resolutions', []))
    expected, _ = compile_draft(baseline, draft, selected)
    require({k:v for k,v in document.items() if k != 'resolution'} == expected,
            'resolved contract differs from permitted baseline edits')
    by_id = {d['id']: d for d in document['datasets']}
    for report in validations:
        dataset = by_id[report['dataset_id']]
        require(report.get('status') == 'passed' and report.get('schema_fingerprint') == dataset['schema_fingerprint'],
                'successful validation must match the executable fingerprint')
        limits = report.get('limits')
        require(isinstance(limits, dict) and all(type(limits.get(k)) is int and limits[k] > 0
                for k in ('sample_files', 'sample_records', 'sample_bytes')), 'invalid validation limits')
        objects = report.get('objects')
        require(isinstance(objects, list) and 0 < len(objects) <= limits['sample_files'], 'validation object evidence required')
        keys = set()
        records = 0
        for obj in objects:
            require(isinstance(obj, dict) and isinstance(obj.get('key'), str) and obj['key'] not in keys,
                    'invalid or duplicate validation object')
            keys.add(obj['key'])
            require(isinstance(obj.get('etag'), str) and type(obj.get('reused_discovery_sample')) is bool,
                    'validation source references required')
            require(type(obj.get('records')) is int and 0 <= obj['records'] <= limits['sample_records'], 'invalid validation record count')
            require(obj['reused_discovery_sample'] == (obj['key'] in {s['key'] for s in dataset['samples']}),
                    'invalid discovery sample provenance')
            records += obj['records']
        require(records > 0 and report.get('validated_records') == records, 'validation requires successful records')
        coverage = report.get('rule_coverage')
        require(isinstance(coverage, list) and len(coverage) == len(dataset.get('resolutions', []))
                and all(type(n) is int and n > 0 for n in coverage), 'every resolution requires validation coverage')
        require(isinstance(report.get('validated_at'), str), 'validation timestamp required')
