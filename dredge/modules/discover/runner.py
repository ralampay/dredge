"""Thin discovery composition boundary; workflows remain reusable commands."""
from ...core.exceptions import UserError
from .requests import CrawlRequest


def run_discovery(config):
    from ...modes.s3.clients import open_source
    from ...modes.s3.configuration import S3Settings
    from .commands import CrawlSchemas
    from .presentation import present
    from .serialization import validate_output, write_report

    if (config.get("module"), config.get("mode"), config.get("action")) != ("discover", "s3", "crawl"):
        raise UserError("Discovery currently supports mode s3 and action crawl.")
    request = CrawlRequest.from_config(config)
    connection = S3Settings.from_config(config)
    validate_output(request.output, request.overwrite)
    with open_source(connection) as source:
        report = CrawlSchemas(source, request.options).execute()
        write_report(report, request.output, request.overwrite)
    present(report, request.output)
    return report
