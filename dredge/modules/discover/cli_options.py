"""Discovery-only option declarations and pre-execution validation."""
import argparse
import codecs

from ...core.cli_options import CliOption


def positive(value):
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


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


OPTIONS = (
    CliOption(('--output',), {'help': 'Local YAML report path.'}),
    CliOption(('--include',), {'action': 'append', 'help': 'Include full-key glob; repeatable.'}),
    CliOption(('--exclude',), {'action': 'append', 'help': 'Exclude full-key glob; repeatable; wins over include.'}),
    CliOption(('--format',), {'choices': ('auto', 'csv', 'json', 'jsonl', 'parquet', 's3-access-log'), 'help': 'Format override (default: auto).'}),
    CliOption(('--partition-style',), {'choices': ('auto', 'hive', 'none'), 'help': 'Directory grouping (default: auto).'}),
    CliOption(('--sample-files',), {'type': positive, 'help': 'Files per dataset (default: 10).'}),
    CliOption(('--sample-records',), {'type': positive, 'help': 'Text records per file (default: 1000).'}),
    CliOption(('--sample-bytes',), {'type': positive, 'help': 'Transferred/decompressed bytes per file (default: 8388608).'}),
    CliOption(('--csv-delimiter',), {'type': delimiter, 'help': 'CSV delimiter (default: comma).'}),
    CliOption(('--csv-header',), {'choices': ('present', 'absent'), 'help': 'CSV header policy (default: present).'}),
    CliOption(('--encoding',), {'type': encoding, 'help': 'Text encoding (default: utf-8-sig).'}),
    CliOption(('--on-error',), {'choices': ('fail', 'continue'), 'help': 'Object error policy (default: fail).'}),
    CliOption(('--overwrite',), {'action': 'store_true', 'help': 'Replace an existing regular report file.'}),
)


def validate_options(parser, config):
    if not config.get("output"):
        parser.error("--output is required for discovery")
    if config.get("format") in ("json", "jsonl", "parquet", "s3-access-log") and (
        config.get("csv_header") or config.get("csv_delimiter")
    ):
        parser.error("CSV options cannot be combined with a non-CSV --format")
    if config.get("format") == "parquet" and (config.get("encoding") or config.get("sample_records")):
        parser.error("--encoding and --sample-records apply only to text formats")
