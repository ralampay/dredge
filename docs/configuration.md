# Configuration and doctor

[Documentation home](../README.md)

Run CLI examples from the repository root.

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
