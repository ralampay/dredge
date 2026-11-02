# Dredge

A toolkit for data ingestion and pipeline tasks. Run it from the repository root with `python -m dredge`.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m dredge --help
python -m dredge --doctor
```

Edit `.env` to configure your application. On Windows, activate the virtual
environment with `.venv\Scripts\Activate.ps1` in PowerShell.
Initialize a Git repository with `git init` when ready.

## Structure

```text
dredge/
  __init__.py
  __main__.py          # Module entry point
  cli.py               # Argument parsing and dispatch
  core/
    __init__.py
    configuration.py   # Explicit .env loading
    doctor.py          # InspectEnvironment command and report types
    exceptions.py      # UserError and UserAbort
    ui.py              # Rich rendering, prompts, and progress
tests/
  test_cli.py
.env.dist
.gitignore
requirements.txt
README.md
```

## Configuration and doctor

The application reads `.env` beside the `dredge/` directory, independently of
the current working directory. Python still needs to find the package: normally
run commands from the repository root, or add that root to `PYTHONPATH`.

Configuration is loaded explicitly during command execution, never when importing
a module. Existing shell variables take precedence, including empty strings.
Quoted values and comments use python-dotenv syntax; `${VARIABLE}` interpolation
is disabled, so such strings remain literal. No settings are required.

`--doctor` reports the resolved path, whether the file exists, and each file key's
effective value and source. It does not list unrelated shell variables. Missing
or empty files are valid. `KEY=` appears as `<empty>`; a bare `KEY` appears as
`<unset>` unless supplied by the shell. Bare keys are not added to the environment.

Values are masked when the key contains `SECRET`, `TOKEN`, `PASSWORD`, `PASSWD`,
`API_KEY`, `PRIVATE_KEY`, or `CREDENTIAL` (case-insensitive). This is name-based
masking, not automatic secret detection; use recognizable names for credentials.
Configuration text is displayed literally, including Rich markup characters.
Keep local values in the ignored `.env`; commit only harmless examples in `.env.dist`.

No arguments or `--help` prints usage and exits with status 0. Doctor also returns
0 for missing configuration, 1 for an unreadable file, and 130 for cancellation.
Invalid CLI arguments return 2.

## Add application commands

See [AGENTS.md](AGENTS.md) for business-logic, module ownership, and validation
conventions when extending this template.

Add options in `build_parser()` and dispatch them from `main()` in `cli.py`.
Before invoking a command, call `load_environment()` once and construct its
dependencies there. Pass configuration inward instead of reading global environment
variables deep in workflow code.

Put each workflow in a focused command class, for example
`dredge/commands/greet.py` (add `commands/__init__.py`):

```python
class Greet:
    def __init__(self, name: str):
        self.name = name

    def execute(self) -> str:
        return f"Hello, {self.name}!"
```

The CLI constructs `Greet`, calls `execute()`, and passes the result to
`print_info()`. Keep command logic independent of argument parsing and terminal
output so it can also be called from tests or another Python application. Raise
`UserError` for actionable failures; `main()` displays these without a traceback.

`core.ui` includes `print_info`, `print_success`, `print_warning`, `print_error`,
`prompt_text`, `prompt_int`, `prompt_float`, `confirm_action`, and `create_progress`.
For example:

```python
from dredge.core.ui import create_progress

with create_progress() as progress:
    task = progress.add_task("Processing", total=len(items))
    for item in items:
        process(item)
        progress.advance(task)
```

`rich` supplies tables, styling, prompts, and progress; `python-dotenv` reads
configuration. Parsing uses Python's built-in `argparse`, and tests use `unittest`,
so no additional framework is needed. Requirements are intentionally unpinned;
pin versions when your application needs reproducible deployments.

## Run tests

```bash
python -m unittest discover -s tests -v
```

Tests use temporary configuration files and do not depend on your local `.env`.
