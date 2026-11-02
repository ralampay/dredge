"""S3 option declarations and validation; no SDK imports."""
from ...core.cli_options import CliOption


OPTIONS = (
    CliOption(('--bucket',), {'help': 'S3 bucket name (not an s3:// URI).'}),
    CliOption(('--profile',), {'help': 'AWS named profile; otherwise use SDK defaults.'}),
    CliOption(('--region',), {'help': 'AWS region override.'}),
    CliOption(('--prefix',), {'help': 'Literal S3 prefix, recursively listed.'}),
)


def validate_options(parser, config):
    bucket = config.get("bucket")
    if not bucket:
        parser.error("--bucket is required for S3")
    if "/" in bucket or ":" in bucket:
        parser.error("--bucket must be a bucket name, not a URI or path")
