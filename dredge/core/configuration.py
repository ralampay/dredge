"""Load the repository's .env without overriding shell configuration."""

from collections.abc import MutableMapping
from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import dotenv_values

from .exceptions import UserError


@dataclass(frozen=True)
class EnvironmentEntry:
    name: str
    value: str | None
    source: str


@dataclass(frozen=True)
class EnvironmentConfig:
    path: Path
    exists: bool
    entries: tuple[EnvironmentEntry, ...]


def default_env_path() -> Path:
    return Path(__file__).resolve().parents[2] / ".env"


def load_environment(
    path: Path | None = None,
    environment: MutableMapping[str, str] | None = None,
) -> EnvironmentConfig:
    """Read dotenv values literally; existing environment values take priority.

    Variable interpolation is disabled so the report and loaded configuration
    always agree, including when a shell variable overrides a file value.
    """
    path = (path if path is not None else default_env_path()).resolve()
    environment = os.environ if environment is None else environment
    try:
        with path.open(encoding="utf-8") as stream:
            values = dotenv_values(stream=stream, interpolate=False)
    except FileNotFoundError:
        return EnvironmentConfig(path, False, ())
    except (OSError, UnicodeError) as exc:
        raise UserError(f"Cannot read {path}. Check its permissions and UTF-8 encoding: {exc}") from exc

    entries = []
    for name, value in values.items():
        if name in environment:
            entries.append(EnvironmentEntry(name, environment[name], "shell override"))
        else:
            entries.append(EnvironmentEntry(name, value, ".env"))
            if value is not None:
                environment[name] = value
    return EnvironmentConfig(path, True, tuple(entries))
