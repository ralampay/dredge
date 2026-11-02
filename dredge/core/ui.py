"""Reusable Rich presentation, prompts, and progress helpers."""

from rich.console import Console
from rich.progress import Progress
from rich.prompt import Confirm, FloatPrompt, IntPrompt, Prompt
from rich.table import Table
from rich.text import Text

from .doctor import EnvironmentReport
from .exceptions import UserAbort


def print_info(message: str) -> None:
    Console().print(Text(message, style="cyan"))


def print_success(message: str) -> None:
    Console().print(Text(message, style="bold green"))


def print_warning(message: str) -> None:
    Console().print(Text(message, style="yellow"))


def print_error(message: str) -> None:
    Console(stderr=True).print(Text(message, style="bold red"))


def prompt_text(message: str, default: str | None = None) -> str:
    return Prompt.ask(Text(message), default=default)


def prompt_int(message: str, default: int | None = None) -> int:
    return IntPrompt.ask(Text(message), default=default)


def prompt_float(message: str, default: float | None = None) -> float:
    return FloatPrompt.ask(Text(message), default=default)


def confirm_action(message: str, default: bool = False, abort: bool = False) -> bool:
    confirmed = Confirm.ask(Text(message), default=default)
    if abort and not confirmed:
        raise UserAbort()
    return confirmed


def create_progress() -> Progress:
    """Return a progress display to use as a context manager."""
    return Progress()


def print_environment_report(report: EnvironmentReport, console: Console | None = None) -> None:
    console = console if console is not None else Console()
    console.print(Text("Environment doctor", style="bold cyan"))
    console.print(Text(f".env path: {report.path}"))
    console.print(Text(f"File status: {'found' if report.exists else 'missing (optional)'}"))
    if not report.entries:
        console.print("No .env variables to report.")
        return
    table = Table("Variable", "Effective value", "Source")
    for entry in report.entries:
        table.add_row(Text(entry.name), Text(entry.display_value), Text(entry.source))
    console.print(table)
    console.print("Secret-looking keys are masked. <empty> is an empty string; <unset> has no value.")
