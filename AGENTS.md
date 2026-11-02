# Python CLI development conventions

This repository is a modular Python 3.10+ CLI for data discovery and pipeline tasks. Preserve the
`python -m dredge` entry point and keep application logic modular, portable,
and usable without a terminal. If the package is renamed, apply these conventions
to the new package name and update examples and documentation.

## Development workflow

Follow `INSPECT -> PLAN -> IMPLEMENT -> VALIDATE -> REVIEW -> REFINE` for meaningful
changes.

- Read the relevant implementation, README, and tests before editing.
- Reuse existing boundaries and conventions; implement the smallest coherent change.
- Validate requested behavior and important failures, then review correctness,
  responsibility boundaries, portability, and unnecessary complexity.
- Fix material issues and rerun affected checks before finishing.
- Keep changes within the requested scope. Do not add speculative frameworks,
  registries, base classes, dependencies, or placeholder modules.

## Business logic: command pattern

Implement non-trivial business workflows as dedicated command classes.

- Name commands with a verb-first intent, such as `ExportRecords` or `CreateReport`.
- Inject inputs, configuration, and replaceable collaborators through `__init__`.
- Use `execute()` as the default public entry point.
- Return values or structured result objects; let presentation code render them.
- Keep commands callable from tests and other Python modules without CLI setup.
- Use plain functions for small stateless helpers and pure transformations.

Example shape (the repository interface belongs near the feature that owns it):

```python
from typing import Protocol


class RecordRepository(Protocol):
    def read_records(self) -> list[str]:
        ...


class ExportRecords:
    def __init__(self, repository: RecordRepository, separator: str = "\n"):
        self.repository = repository
        self.separator = separator

    def execute(self) -> str:
        return self.separator.join(self.repository.read_records())
```

Commands coordinate a use case: validate preconditions, call collaborators in the
required order, and return a result. Move substantial parsing, persistence,
serialization, or provider integration into focused components. Split complex
workflows into private methods or collaborating commands rather than growing a
single large `execute()` method.

Do not parse CLI arguments, prompt, print, render Rich objects, or read global
environment variables inside business commands. Prefer composition over inheritance;
extract shared capabilities only when there is actual shared behavior.

## Module ownership and dependency direction

Keep dependencies flowing inward:

```text
CLI / caller
    -> runner or composition root
        -> command
            -> focused service or project-owned interface
                -> infrastructure adapter
```

- `dredge/__main__.py`: only invoke `main()` and propagate its exit code.
- `dredge/cli.py`: own the top-level lifecycle: parse, validate routes, load
  configuration once, invoke the selected runner, and translate errors/results to
  exit codes. Keep module-specific composition and presentation out of this file.
- `dredge/cli_options.py`, `cli_config.py`, and `cli_routing.py`: declare global
  options, normalize parsed inputs, and define immutable route descriptors with
  lazy runner/option/validator references. Derive parser choices and dispatch from
  descriptors; do not add module-specific dispatch branches to `cli.py`.
- `dredge/core/`: cross-cutting infrastructure that is genuinely shared, such as
  configuration, exceptions, doctor reporting, and terminal UI. Do not put domain
  logic here merely to simplify imports.
- `dredge/modules/<module>/`: own application workflows such as discovery and
  future ingestion. Keep commands, typed requests/results, readers, serialization,
  CLI option inventories, thin runners, and presentation near their workflow.
- `dredge/modes/<mode>/`: own source integration such as S3. Keep mode-specific
  flags, typed connection settings, client construction/lifecycle, and adapters
  here so multiple modules can reuse them. Modes must not import modules.
- `dredge/core/sources.py`: neutral object-source contracts consumed by commands
  and mode adapters. Core contracts must not import concrete integrations.
- `dredge/formats/`: shared parsing and type/schema adapters used by discovery
  and ingestion; do not import application modules or source modes here.
- `dredge/core/schema_manifest.py` and `core/partitioning.py`: shared artifact
  validation and partition-selection semantics.
- `dredge/features/discover/`: compatibility re-exports only. Do not add business
  logic here or make canonical code depend on compatibility modules.
- `tests/`: portable tests of public behavior using temporary data and injected
  collaborators.

Within an application module, use files named by responsibility: `commands.py` for small
related commands, a dedicated verb-named module for a larger command,
`presentation.py` for rendering, `requests.py` for typed inputs, and `models.py`
for structured results. Source integrations belong in the appropriate mode;
other workflow-specific integrations can stay near their owning module. Create only modules needed by current behavior. Add `__init__.py` to
new packages and use relative imports within the application.

Follow [ARCHITECTURE.md](ARCHITECTURE.md) for the canonical module/mode boundaries.
Declare CLI flags once using `CliOption` inventories. Importing options, routing,
or runners must not initialize provider clients or load heavyweight readers.
Preserve compatibility re-exports when moving documented Python APIs.

Keep any `runner.py` thin: normalize parsed inputs, construct collaborators, select
a command, invoke it, and forward its result to presentation. Do not put business
logic or provider operations in runners. Business modules must not import CLI or
presentation modules. Avoid circular imports and generic utility containers.

## Configuration and external dependencies

- Centralize environment access in the configuration/composition boundary. Call
  `load_environment()` once per invocation that needs configuration and pass values
  explicitly into workflows.
- Preserve repository-relative `.env` resolution, shell precedence, literal dotenv
  values without interpolation, and valid missing/empty-file behavior unless the
  requested change explicitly alters these contracts.
- Keep imports free of configuration loading, network calls, resource creation,
  or other runtime side effects.
- Keep secrets out of committed files and error messages. Preserve doctor's masking
  and its restriction to keys declared in `.env`.
- Use constructor parameters or focused configuration objects instead of mutable
  global state, hard-coded machine paths, or hidden singleton dependencies.
- Isolate third-party integrations behind narrow adapters or injected callables
  when replacement or testing warrants a boundary. Do not introduce an interface
  for every helper automatically.
- Add runtime dependencies to `requirements.txt` only when justified and document
  their purpose. Prefer existing Rich capabilities and standard-library argparse.

## Presentation and errors

- Reuse `core.ui` for shared output, prompts, confirmations, and progress displays.
  Module-specific rendering belongs in its presentation module.
- Render user/configuration strings literally; do not interpret them as Rich markup.
- Raise `UserError` for actionable input, configuration, or runtime failures.
  Raise `UserAbort` for intentional interactive cancellation.
- Translate lower-level failures at the boundary with enough context to explain
  what failed and what the user can do. Preserve causes with `raise ... from exc`.
- Do not swallow exceptions or broadly catch unexpected programming errors merely
  to hide a traceback. Reusable components must not print errors directly.
- Preserve exit codes: 0 for success/help, 1 for user-facing runtime failures,
  2 for invalid CLI arguments, and 130 for cancellation.

## Validation and documentation

- Use `unittest`; run `python -m unittest discover -s tests -v` for relevant changes.
- Test commands through `execute()` with lightweight fakes and temporary files.
  Verify behavior rather than mirroring implementation details.
- Cover important success and failure paths, CLI dispatch, and configuration
  precedence when affected. Do not depend on a developer's `.env`, credentials,
  network services, or machine-specific directory layout.
- Smoke-test `python -m dredge --help` and `python -m dredge --doctor` when changing
  the entry point, parsing, or configuration.
- Documentation-only changes need a consistency review, not new tests.
- Update README usage and extension instructions when public behavior or module
  ownership changes. Keep `.env.dist` examples harmless and current.
- Review the final diff for unrelated edits, duplication, debug output, dead code,
  and unnecessary abstractions. Report what changed and what was verified.
