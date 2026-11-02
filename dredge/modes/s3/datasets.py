"""Consume the versioned S3 selector without depending on discovery workflows."""
from fnmatch import fnmatchcase

from ...core.partitioning import group_key
from ...core.exceptions import UserError
from ...formats.detection import classify


class S3DatasetAdapter:
    def __init__(self, source, dataset):
        self.source = source
        self.selector = dataset['ingestion']['selector']
        if getattr(source, 'bucket', self.selector['bucket']) != self.selector['bucket']:
            raise UserError('Schema source bucket does not match the supplied S3 source.')

    @property
    def prefix(self):
        return self.selector['prefix']

    def matches(self, obj):
        s = self.selector
        if not obj.size or obj.key.endswith('/') or not obj.key.startswith(s['prefix']):
            return False
        if s['include'] and not any(fnmatchcase(obj.key, p) for p in s['include']):
            return False
        if any(fnmatchcase(obj.key, p) for p in s['exclude']):
            return False
        if classify(obj.key, s['format_policy'])[0] != s['format']:
            return False
        location, partitions = group_key(obj.key, s['partition_style'])
        return (location + '/' if location else '') == s['location_pattern'] and partitions == s['partitions']

    def objects(self):
        for obj in self.source.objects(self.selector['prefix']):
            if self.matches(obj):
                yield obj

    def partition_values(self, obj):
        segments = obj.key.split('/')[:-1]
        return [{**p, 'value': segments[p['segment']].split('=', 1)[1] if p['layout'] == 'hive'
                 else segments[p['segment']]} for p in self.selector['partitions']]
