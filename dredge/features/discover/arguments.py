"""Compatibility argument registration; canonical inventories own all flags."""
from ...modules.discover.cli_options import OPTIONS as DISCOVERY_OPTIONS, delimiter, encoding, positive
from ...core.sampling_options import OPTIONS as SAMPLING_OPTIONS
from ...core.output_options import OPTIONS as OUTPUT_OPTIONS
from ...modes.aws.cli_options import OPTIONS as AWS_OPTIONS
from ...modes.s3.cli_options import OPTIONS as S3_OPTIONS


def add_arguments(parser):
    group = parser.add_argument_group("S3 schema discovery")
    for option in (*OUTPUT_OPTIONS, *DISCOVERY_OPTIONS, *SAMPLING_OPTIONS, *S3_OPTIONS, *AWS_OPTIONS):
        option.add_to(group)


__all__ = ["add_arguments", "delimiter", "encoding", "positive"]
