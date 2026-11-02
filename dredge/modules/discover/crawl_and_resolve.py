"""Compose discovery and validated resolution without terminal dependencies."""
from pathlib import Path

from ...core.exceptions import UserError
from ...core.schema_manifest import SchemaManifest
from ...core.schema_io import validate_output, write_report
from ...core.resolution_contract import compile_draft
from ..schema.commands import BuildResolvedSchema
from ..schema.requests import ValidationLimits
from ..schema.rules import apply_rules, validate_rules_document
from ..schema.json_fallback import apply_json_fallback
from ..ingest.planning import require_ready
from .commands import CrawlSchemas
from .models import CrawlResolutionProgress, CrawlResolutionResult


def validate_paths(request):
    paths = [Path(request.crawl.output)]
    if request.rules_path:
        paths.append(Path(request.rules_path))
    if request.discovery_output:
        paths.append(Path(request.discovery_output))
    for index, path in enumerate(paths):
        for other in paths[:index]:
            if path.resolve() == other.resolve() or (path.exists() and other.exists() and path.samefile(other)):
                raise UserError('Rules, final output, and discovery output must use distinct paths.')
    validate_output(request.crawl.output, request.crawl.overwrite)
    if request.discovery_output:
        validate_output(request.discovery_output, request.crawl.overwrite)


class CrawlAndResolveSchemas:
    def __init__(self, source, selection_factory, request, rules, on_progress=None):
        self.source, self.selection_factory = source, selection_factory
        self.request, self.rules, self.on_progress = request, rules, on_progress

    def _emit(self, phase, detail=None):
        if self.on_progress:
            self.on_progress(CrawlResolutionProgress(phase, detail))

    def _discovery_progress(self, event):
        if event.phase != 'writing':
            self._emit('discovery', event)

    def execute(self):
        request = self.request
        limits = ValidationLimits(request.sample_files, request.sample_records, request.sample_bytes)
        limits.validate()
        if request.unresolved_as not in (None, 'json') or (request.unresolved_as and request.rules_path):
            raise UserError('Choose either reusable rules or --unresolved-as json.')
        if request.unresolved_as != 'json':
            validate_rules_document(self.rules)
        validate_paths(request)
        report = CrawlSchemas(self.source, request.crawl.options, self._discovery_progress).execute()
        if request.discovery_output:
            self._emit('discovery_output', request.discovery_output)
            write_report(report, request.discovery_output, request.crawl.overwrite)
        if report.partial:
            raise UserError('Discovery is partial; final resolved output was not published.')
        baseline = SchemaManifest(report.document)
        self._emit('rules')
        if request.unresolved_as == 'json':
            draft, selected, statuses = apply_json_fallback(baseline)
        else:
            draft, selected, statuses = apply_rules(baseline, self.rules)
        compiled, _ = compile_draft(baseline.document, draft, selected)
        require_ready(compiled['datasets'])
        self._emit('rules_applied', statuses)
        self._emit('validation')
        resolved = BuildResolvedSchema(baseline, draft, self.source, self.selection_factory,
                                       dataset_ids=selected, limits=limits,
                                       on_progress=lambda event: self._emit('validation', event)).execute()
        self._emit('publication', request.crawl.output)
        write_report(resolved, request.crawl.output, request.crawl.overwrite)
        return CrawlResolutionResult(resolved.document, statuses)
