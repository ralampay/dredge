"""Optional processing controls."""
from ....core.cli_options import CliOption

OPTIONS = (
    CliOption(('--processing-config',), {'help': 'Version-1 raster pipeline YAML; no catalog overrides.'}),
    CliOption(('--allow-hyp3-submit',), {'action': 'store_true', 'help': 'Explicitly authorize new HyP3 RTC jobs for this invocation.'}),
)
