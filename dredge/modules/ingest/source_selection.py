"""Listing-only source previews and guarded schema-based materialization."""
from ...core.exceptions import UserError
from ...core.schema_io import validate_output as validate_plan_output
from .plan_io import write_plan
from .ingest_datasets import IngestDatasets
from .models import IngestResult
from .planning import blockers, pair_selections
from .publication import validate_output


def assignments(obj, selections):
    return [index for index, selection in enumerate(selections) if selection.matches(obj)]


class PreviewSourceSelection:
    def __init__(self, selection, plan, datasets=None, selections=(), output=None, on_progress=None):
        self.selection, self.plan = selection, plan
        self.datasets, self.selections = datasets, selections
        self.output, self.on_progress = output, on_progress

    def execute(self):
        if self.output is not None:
            validate_plan_output(self.output)
        rows = []
        if self.datasets is not None:
            pair_selections(self.datasets, self.selections)
            rows = [dict(id=d['id'], name=d['name'], objects=0, bytes=0, blockers=[])
                    for d in self.datasets]
        estimate = dict(objects=0, bytes=0, consumable_objects=0, consumable_bytes=0,
                        unmatched_objects=0, unmatched_bytes=0, ambiguous_objects=0, ambiguous_bytes=0)
        examples = {'unmatched': [], 'ambiguous': []}
        for obj in self.selection.objects():
            estimate['objects'] += 1
            estimate['bytes'] += obj.size
            if self.datasets is not None:
                indices = assignments(obj, self.selections)
                category = 'consumable' if len(indices) == 1 else 'unmatched' if not indices else 'ambiguous'
                estimate[category + '_objects'] += 1
                estimate[category + '_bytes'] += obj.size
                if category == 'consumable':
                    row = rows[indices[0]]
                    row['objects'] += 1
                    row['bytes'] += obj.size
                elif len(examples[category]) < 5:
                    examples[category].append(obj.key)
            if self.on_progress:
                self.on_progress(estimate['objects'])
        for row, dataset in zip(rows, self.datasets or ()):
            if row['objects']:
                row['blockers'] = blockers(dataset)
        document = {'source_plan': self.plan.document(), 'source_estimate': estimate,
                    'schema_checked': self.datasets is not None, 'datasets': rows,
                    'examples': examples, 'saved': self.output is not None}
        if self.output is not None:
            write_plan(self.plan, estimate, self.output)
        partial = bool(estimate['unmatched_objects'] or estimate['ambiguous_objects'] or any(r['blockers'] for r in rows))
        return IngestResult(document, str(self.output) if self.output is not None else None, partial)


class AssignedSelection:
    """Intersect source predicates and schema selectors; reject inventory drift.

    Each streaming listing is rechecked against every schema selector. New
    unassigned/ambiguous objects cannot disappear silently between preflight and
    materialization. No unbounded in-memory object inventory is retained.
    """
    def __init__(self, source_selection, selections, index, active):
        self.source_selection, self.selections = source_selection, selections
        self.index, self.active = index, active

    @property
    def prefix(self):
        return self.source_selection.prefix

    def matches(self, obj):
        return self.source_selection.matches(obj) and self.selections[self.index].matches(obj)

    def objects(self):
        for obj in self.source_selection.objects():
            indices = assignments(obj, self.selections)
            if len(indices) != 1 or indices[0] not in self.active:
                raise UserError('Selected source inventory changed or cannot be assigned uniquely; rerun dry-run.')
            if indices[0] == self.index:
                yield obj

    def partition_values(self, obj):
        return self.selections[self.index].partition_values(obj)


class IngestSelectedObjects:
    def __init__(self, manifest, source, datasets, selections, selection, plan, output, options, on_progress=None, check_cancel=None):
        self.manifest, self.source, self.datasets, self.selections = manifest, source, datasets, selections
        self.selection, self.plan, self.output, self.options = selection, plan, output, options
        self.on_progress = on_progress
        self.check_cancel = check_cancel

    def execute(self):
        if self.check_cancel:
            self.check_cancel()
        self.options.validate()
        validate_output(self.output)
        self.plan.validate(self.manifest.document['source']['bucket'])
        preview = PreviewSourceSelection(self.selection, self.plan, self.datasets, self.selections,
                                         on_progress=self.on_progress).execute()
        if preview.partial:
            estimate = preview.document['source_estimate']
            issues = [f"{estimate['unmatched_objects']} unmatched objects", f"{estimate['ambiguous_objects']} ambiguous objects"]
            issues.extend(row['id'] + ': ' + ', '.join(row['blockers'])
                          for row in preview.document['datasets'] if row['blockers'])
            raise UserError('Source selection cannot be ingested: ' + '; '.join(issues) + '. Narrow the selection or update the schema.')
        active = {i for i, row in enumerate(preview.document['datasets']) if row['objects']}
        datasets = [dataset for i, dataset in enumerate(self.datasets) if i in active]
        selections = [AssignedSelection(self.selection, self.selections, i, active)
                      for i in range(len(self.selections)) if i in active]
        return IngestDatasets(self.manifest, self.source, datasets, selections, self.output,
                              self.options, self.on_progress, selection_plan=self.plan.document(),
                              check_cancel=self.check_cancel).execute()
