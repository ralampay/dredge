"""Athena execution context flags."""
from ...core.cli_options import CliOption

OPTIONS = (
    CliOption(('--catalog',), {'help': 'Athena catalog NAME, not Glue catalog ID (default: AwsDataCatalog).'}),
    CliOption(('--workgroup',), {'help': 'Existing Athena workgroup (default: primary).'}),
    CliOption(('--results-location',), {'help': 'Optional s3://bucket/prefix/ for query results; workgroup enforcement applies.'}),
)
