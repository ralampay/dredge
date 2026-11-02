"""Versioned, schema-bound ingestion plans and atomic YAML persistence."""
from dataclasses import asdict, dataclass
import hashlib
import json

from ...core.exceptions import UserError
from .time_filters import TimeFilter
from .plan_io import read_plan_document, write_plan, load_plan  # compatibility exports


def binding(dataset):
    contract = {'fingerprint': dataset['schema_fingerprint'], 'selector': dataset['ingestion']['selector']}
    return hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True)
class FilterPlan:
    bucket: str
    dataset_id: str
    schema_binding: str
    predicate: TimeFilter
    inspect_metadata: bool = False

    @classmethod
    def create(cls, bucket, dataset, predicate, inspect_metadata=False):
        predicate.validate_dataset(dataset)
        if inspect_metadata and (predicate.kind != 'record' or dataset['format'] != 'parquet'):
            raise UserError('Parquet metadata inspection requires a Parquet record-time filter.')
        if inspect_metadata and any(rule['path'][0].get('field') == predicate.field
                                    for rule in dataset.get('resolutions', [])):
            raise UserError('Metadata pruning is unavailable for a field with schema resolution transforms.')
        return cls(bucket, dataset['id'], binding(dataset), predicate, inspect_metadata)

    def validate(self, bucket, dataset):
        if self.bucket != bucket or self.dataset_id != dataset['id'] or self.schema_binding != binding(dataset):
            raise UserError('Plan bucket, dataset, or schema binding changed; create a new plan.')
        self.create(bucket, dataset, self.predicate, self.inspect_metadata)

    def document(self):
        return {'plan_version': 1, 'bucket': self.bucket, 'dataset_id': self.dataset_id,
                'schema_binding': self.schema_binding, 'filter': asdict(self.predicate),
                'inspect_parquet_metadata': self.inspect_metadata}

    @classmethod
    def load(cls, path):
        return cls.from_document(read_plan_document(path))

    @classmethod
    def from_document(cls, document):
        keys = {'plan_version', 'bucket', 'dataset_id', 'schema_binding', 'filter',
                'inspect_parquet_metadata', 'created_at', 'estimate'}
        if (not isinstance(document, dict) or set(document) - keys
                or type(document.get('plan_version')) is not int or document['plan_version'] != 1
                or type(document.get('inspect_parquet_metadata')) is not bool
                or any(not isinstance(document.get(k), str) or not document[k]
                       for k in ('bucket', 'dataset_id', 'schema_binding'))
                or not isinstance(document.get('filter'), dict)
                or set(document['filter']) != {'kind', 'field', 'start', 'end', 'timezone'}):
            raise UserError('Invalid or unsupported ingestion plan contract.')
        return cls(document['bucket'], document['dataset_id'], document['schema_binding'],
                   TimeFilter(**document['filter']), document['inspect_parquet_metadata'])
