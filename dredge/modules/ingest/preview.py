"""Metadata-only source sizing with shared dataset selectors."""
from dataclasses import asdict

from .models import IngestResult, PreviewDataset
from .planning import blockers, pair_selections


class PreviewIngestion:
    def __init__(self, source, datasets, selections, on_progress=None):
        self.source, self.datasets, self.selections = source, datasets, selections
        self.on_progress = on_progress

    def execute(self):
        pairs = pair_selections(self.datasets, self.selections)
        rows = [PreviewDataset(d['id'], d['name'], blockers(d)) for d, _ in pairs]
        # Collapse overlapping prefixes as well as identical ones, so totals
        # never count a source object twice and listing remains streaming.
        prefixes = []
        for prefix in sorted({selection.prefix for _, selection in pairs}):
            if not any(prefix.startswith(parent) for parent in prefixes):
                prefixes.append(prefix)
        total_objects = total_bytes = listed = 0
        for prefix in prefixes:
            for obj in self.source.objects(prefix):
                listed += 1
                matched = False
                for row, selection in zip(rows, self.selections):
                    if selection.matches(obj):
                        row.objects += 1
                        row.bytes += obj.size
                        row.largest = max(row.largest, obj.size)
                        matched = True
                if matched:
                    total_objects += 1
                    total_bytes += obj.size
                if self.on_progress and listed % 1000 == 0:
                    self.on_progress(listed)
        return IngestResult({'dry_run': True, 'datasets': [asdict(row) for row in rows],
                             'objects': total_objects, 'bytes': total_bytes, 'listed': listed},
                            partial=any(row.blockers for row in rows))
