"""Discovery-only option declarations and pre-execution validation."""
import argparse
import codecs

from ...core.cli_options import CliOption, positive


def delimiter(value):
    if len(value) != 1 or value in "\r\n\0":
        raise argparse.ArgumentTypeError("must be one non-newline character")
    return value


def encoding(value):
    try:
        codecs.lookup(value)
    except LookupError as exc:
        raise argparse.ArgumentTypeError("unknown text encoding") from exc
    return value


def report_fields(value):
    if value == 'all':
        return value
    return positive(value)


OPTIONS = (
    CliOption(('--unresolved-as',), {'choices': ('json',), 'help': 'Encode unknown/conflicting fields as JSON strings; validate and publish without a rules file.'}),
    CliOption(('--resolve-with',), {'help': 'Reusable resolution rules YAML; crawl, validate, and publish a resolved schema.'}),
    CliOption(('--discovery-output',), {'help': 'Optional raw discovery YAML saved before resolution.'}),
    CliOption(('--resolve-sample-files',), {'type': positive, 'help': 'Resolution validation files per dataset (default: 50).'}),
    CliOption(('--resolve-sample-records',), {'type': positive, 'help': 'Resolution validation records per file (default: 1000).'}),
    CliOption(('--resolve-sample-bytes',), {'type': positive, 'help': 'Resolution transferred/decompressed byte budget per file (default: 8388608).'}),
    CliOption(('--report-fields',), {'type': report_fields, 'help': 'Maximum schema rows per dataset in the Rich report (default: 20); use all for every field.'}),
    CliOption(('--include',), {'action': 'append', 'help': 'Include full-key glob; repeatable.'}),
    CliOption(('--exclude',), {'action': 'append', 'help': 'Exclude full-key glob; repeatable; wins over include.'}),
    CliOption(('--format',), {'choices': ('auto', 'csv', 'json', 'jsonl', 'parquet', 's3-access-log'), 'help': 'Format override (default: auto).'}),
    CliOption(('--partition-style',), {'choices': ('auto', 'hive', 'none'), 'help': 'Directory grouping (default: auto).'}),
    CliOption(('--csv-delimiter',), {'type': delimiter, 'help': 'CSV delimiter (default: comma).'}),
    CliOption(('--csv-header',), {'choices': ('auto', 'present', 'absent'), 'help': 'CSV header policy (default: auto).'}),
    CliOption(('--temporal-types',), {'choices': ('auto', 'off'), 'help': 'Infer ISO date/time strings (default: auto).'}),
    CliOption(('--encoding',), {'type': encoding, 'help': 'Text encoding (default: utf-8-sig).'}),
    CliOption(('--on-error',), {'choices': ('fail', 'continue'), 'help': 'Object error policy (default: fail).'}),
    CliOption(('--overwrite',), {'action': 'store_true', 'help': 'Replace an existing regular report file.'}),
)


def validate_options(parser, config):
    for name in ('resolve_with', 'discovery_output'):
        if config.get(name) is not None and not config[name]:
            parser.error('--' + name.replace('_', '-') + ' requires a nonempty path')
    resolution_options = ('discovery_output', 'resolve_sample_files', 'resolve_sample_records', 'resolve_sample_bytes')
    if config.get('resolve_with') and config.get('unresolved_as'):
        parser.error('--resolve-with and --unresolved-as cannot be combined')
    if not (config.get('resolve_with') or config.get('unresolved_as')) and any(config.get(k) is not None for k in resolution_options):
        parser.error('Discovery output and resolution sampling flags require --resolve-with or --unresolved-as json')
    if not config.get("output"):
        parser.error("--output is required for discovery")
    if config.get("format") in ("json", "jsonl", "parquet", "s3-access-log") and (
        config.get("csv_header") or config.get("csv_delimiter")
    ):
        parser.error("CSV options cannot be combined with a non-CSV --format")
    if config.get("format") == "parquet" and (config.get("encoding") or config.get("sample_records")):
        parser.error("--encoding and --sample-records apply only to text formats")
