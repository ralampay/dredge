"""Schema-independent version-2 source-selection plans."""
from dataclasses import dataclass

from ...core.exceptions import UserError
from ...core.partitioning import HiveFilter


@dataclass(frozen=True)
class SourcePlan:
    bucket: str
    prefix: str = ''
    partitions: HiveFilter = HiveFilter()

    def __post_init__(self):
        if not isinstance(self.bucket, str) or not self.bucket or '/' in self.bucket or ':' in self.bucket:
            raise UserError('Source plan requires a bucket name.')
        if not isinstance(self.prefix, str):
            raise UserError('Source plan prefix must be a literal string.')

    @classmethod
    def from_config(cls, config):
        names = tuple(config['partition_date'].split(',')) if config.get('partition_date') else ()
        return cls(config['bucket'], config.get('prefix', ''),
                   HiveFilter(tuple(config.get('partition', ())), names, config.get('time_from'), config.get('time_to')))

    def validate(self, bucket):
        if self.bucket != bucket:
            raise UserError('Source plan bucket does not match --bucket.')

    def document(self):
        calendar = ({'names': list(self.partitions.date_names), 'from': self.partitions.start,
                     'to': self.partitions.end} if self.partitions.date_names else None)
        return {'plan_version': 2, 'bucket': self.bucket, 'prefix': self.prefix,
                'partitions': list(self.partitions.partitions), 'calendar': calendar}

    @classmethod
    def from_document(cls, document):
        required = {'plan_version', 'bucket', 'prefix', 'partitions', 'calendar'}
        if (not isinstance(document, dict) or not required <= document.keys()
                or document.keys() - required - {'created_at', 'estimate'}
                or type(document['plan_version']) is not int or document['plan_version'] != 2
                or not isinstance(document['partitions'], list)):
            raise UserError('Invalid version-2 source plan contract.')
        calendar = document['calendar']
        if calendar is not None and (not isinstance(calendar, dict) or set(calendar) != {'names', 'from', 'to'}
                                     or not isinstance(calendar['names'], list) or len(calendar['names']) != 3):
            raise UserError('Invalid source plan calendar predicate.')
        return cls(document['bucket'], document['prefix'],
                   HiveFilter(tuple(document['partitions']), tuple(calendar['names']) if calendar else (),
                              calendar['from'] if calendar else None, calendar['to'] if calendar else None))
