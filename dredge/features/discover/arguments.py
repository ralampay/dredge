"""Compatibility argument registration; canonical inventories own all flags."""
from ...modules.discover.cli_options import OPTIONS as DISCOVERY_OPTIONS, delimiter, encoding, positive
from ...modes.s3.cli_options import OPTIONS as S3_OPTIONS


def add_arguments(parser):
    group = parser.add_argument_group("S3 schema discovery")
    for option in (*DISCOVERY_OPTIONS, *S3_OPTIONS):
        option.add_to(group)


__all__ = ["add_arguments", "delimiter", "encoding", "positive"]
