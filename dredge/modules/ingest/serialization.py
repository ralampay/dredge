"""Version-1 output manifests and portable dataset paths."""
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json

from ...formats.schema import arrow_schema


def dataset_path(dataset_id: str) -> str:
    """Treat IDs as opaque identifiers, never as user-controlled paths."""
    if len(dataset_id) == 64 and all(c in '0123456789abcdef' for c in dataset_id):
        directory = dataset_id
    else:
        directory = hashlib.sha256(dataset_id.encode()).hexdigest()
    return 'datasets/' + directory


def create_manifest(schema_document, options):
    return {
        'dataset_version': 1,
        'status': 'complete',
        'created_at': datetime.now(timezone.utc).isoformat(),
        'source': schema_document['source'],
        'source_snapshot': False,
        'schema_digest': hashlib.sha256(json.dumps(schema_document, sort_keys=True).encode()).hexdigest(),
        'schema_digest_encoding': 'sha256 of json.dumps(schema, sort_keys=True), UTF-8',
        'settings': {**asdict(options), 'compression': 'snappy'},
        'datasets': [],
    }


def dataset_entry(dataset, schema, partitions, files, rows):
    relative = dataset_path(dataset['id'])
    return {
        'id': dataset['id'], 'name': dataset['name'], 'path': relative,
        'data_path': relative + '/data', 'notebook': relative + '/preview.ipynb',
        'schema_fingerprint': dataset['schema_fingerprint'],
        'resolutions': dataset.get('resolutions', []),
        'fields': arrow_schema(schema)['fields'], 'partitions': partitions,
        'rows': rows, 'bytes': sum(item['bytes'] for item in files), 'files': files,
    }


def write_manifests(directory, schema_document, output_document):
    import yaml
    (directory / 'schema.yaml').write_text(yaml.safe_dump(schema_document, sort_keys=False), encoding='utf-8')
    (directory / 'manifest.json').write_text(json.dumps(output_document, indent=2) + '\n', encoding='utf-8')
