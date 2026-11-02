"""Global options; route choices are supplied by routing descriptors."""
from .core.cli_options import CliOption


OPTIONS = (
    CliOption(("--doctor",), {"action": "store_true", "help": "Show the .env path and effective configuration."}),
    CliOption(("--module",), {"help": "Application capability."}),
    CliOption(("--mode",), {"help": "Source adapter."}),
    CliOption(("--action",), {"help": "Action within the module."}),
)
