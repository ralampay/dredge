"""Compile explicit edits, validate source samples, and publish a resolved schema."""
from copy import deepcopy

from ...core.exceptions import UserError
from ...core.schema_manifest import SchemaManifest
from ...core.schema_io import validate_output, write_report
from ...core.resolution_contract import compile_draft, document_digest
from .models import ResolutionResult
from .requests import ValidationLimits
from .validation import ValidateResolution


class BuildResolvedSchema:
    def __init__(self, baseline, draft, source, selection_factory, *, dataset_ids=(), limits=None, on_progress=None):
        self.baseline, self.draft, self.source = baseline, draft, source
        self.selection_factory = selection_factory
        self.dataset_ids = dataset_ids
        self.limits = limits or ValidationLimits()
        self.on_progress = on_progress

    def execute(self):
        self.limits.validate()
        document, selected = compile_draft(self.baseline.document, self.draft, self.dataset_ids)
        reports = []
        for dataset in document['datasets']:
            if dataset['id'] in selected:
                selection = self.selection_factory(self.source, dataset)
                reports.append(ValidateResolution(dataset, self.source, selection, self.limits, self.on_progress).execute())
        document['resolution'] = {'version': 1, 'baseline': deepcopy(self.baseline.document),
                                  'baseline_digest': document_digest(self.baseline.document), 'validation': reports}
        SchemaManifest(document)
        return ResolutionResult(document)


class ResolveSchema:
    """Standalone resolution retains its fresh-output publication policy."""
    def __init__(self, baseline, draft, source, selection_factory, output, *, dataset_ids=(), limits=None, on_progress=None):
        self.output = output
        self.builder = BuildResolvedSchema(baseline, draft, source, selection_factory,
                                           dataset_ids=dataset_ids, limits=limits, on_progress=on_progress)

    def execute(self):
        validate_output(self.output)
        result = self.builder.execute()
        write_report(result, self.output)
        return result


def check_bucket(baseline, bucket):
    if baseline.document['source']['bucket'] != bucket:
        raise UserError('Schema baseline bucket does not match --bucket.')
