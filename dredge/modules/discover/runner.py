"""Thin discovery composition boundary; workflows remain reusable commands."""
from ...core.exceptions import UserError
from .requests import CrawlRequest


def run_discovery(config):
    from ...modes.s3.clients import open_source
    from ...modes.s3.configuration import S3Settings
    from .commands import CrawlSchemas
    from .presentation import present, RichDiscoveryProgress
    from .serialization import validate_output, write_report

    if (config.get("module"), config.get("mode"), config.get("action")) != ("discover", "s3", "crawl"):
        raise UserError("Discovery currently supports mode s3 and action crawl.")
    if config.get('resolve_with') or config.get('unresolved_as'):
        return _run_combined(config)
    request = CrawlRequest.from_config(config)
    connection = S3Settings.from_config(config)
    validate_output(request.output, request.overwrite)
    with RichDiscoveryProgress() as progress:
        with open_source(connection) as source:
            report = CrawlSchemas(source, request.options, on_progress=progress).execute()
            write_report(report, request.output, request.overwrite)
    present(report, request.output, field_limit=config.get('report_fields', 20))
    return report


def _run_combined(config):
    from ...modes.s3.clients import open_source
    from ...modes.s3.configuration import S3Settings
    from ...modes.s3.datasets import S3DatasetAdapter
    from ..schema.rules import load_rules
    from ..schema.requests import ValidationLimits
    from .requests import CrawlResolutionRequest
    from .crawl_and_resolve import CrawlAndResolveSchemas, validate_paths
    from .presentation import CrawlResolutionDisplay, present_combined

    request = CrawlResolutionRequest.from_config(config)
    ValidationLimits(request.sample_files, request.sample_records, request.sample_bytes).validate()
    validate_paths(request)
    rules = load_rules(request.rules_path) if request.rules_path else None
    with CrawlResolutionDisplay() as progress:
        with open_source(S3Settings.from_config(config)) as source:
            report = CrawlAndResolveSchemas(source, S3DatasetAdapter, request, rules, progress).execute()
    present_combined(report, request.crawl.output, field_limit=config.get('report_fields', 20))
    return report
