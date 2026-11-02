# Dredge architecture

Dredge uses the same responsibility boundaries as MLX: declarative CLI options,
lazy routing, typed inputs, thin composition runners, command workflows, isolated
provider integration, and separate presentation. Dredge adds a module dimension
because discovery and ingestion can use the same source mode.

## Ownership and dependency direction

```text
CLI -> route descriptor -> module runner
                            |-> typed request + mode connection settings
                            |-> mode client factory -> source adapter
                            |-> command -> neutral source contract + focused readers
                            |-> report serializer
                            `-> presentation -> core.ui
```

- `modules.discover` owns schema discovery, sampling, schema inspection, ingestion
  contract construction, report serialization, and presentation.
- `modules.ingest` owns record-consumption and held-out-validation commands. It
  receives source and selector collaborators without importing concrete modes.
- `formats` owns shared record parsers, format detection, schema transformations,
  and record type validation. Both discovery and ingestion consume these adapters.
- `core.schema_manifest` validates the version-2 report contract, while
  `core.partitioning` provides shared directory grouping semantics.
- `modes.s3` owns AWS configuration, resource lifecycle, and object access. It
  imports neutral contracts and never imports discovery or another module.
- `core.sources` defines `ObjectInfo`, `ObjectSource`, and `SourceAccessError`.
  These contracts contain no provider, CLI, or presentation dependencies.
- Other `core` components own genuinely cross-cutting configuration, exceptions,
  doctor behavior, UI, and CLI metadata. Domain logic does not move into core
  merely to shorten imports.

Commands receive collaborators and typed inputs through constructors and expose
`execute()`. They do not import concrete source modes, CLI parsing, runners, or
presentation. Readers own format-specific third-party integration. Source mode
adapters translate provider failures and close response streams; client factories
close their clients even when a workflow fails or is cancelled.

## CLI and composition contracts

`cli_options.OPTIONS` declares global flags. Module and mode `cli_options.OPTIONS`
inventories declare their own flags using immutable `core.cli_options.CliOption`
metadata. Inventory imports must stay lightweight. Keep optional values at `None`
(or `False` for opt-in switches) until composition so validation can distinguish
supplied options from workflow defaults.

`cli_routing.ROUTES` is an immutable tuple of frozen `RouteDescriptor` values.
Each identifies a `(module, mode)` pair, supported actions, a runner reference,
option inventories, and validation hooks. References name project-owned modules;
users cannot supply arbitrary import paths. Parser choices and help derive from
these inventories; shared inventories are registered once. Unsupported routes,
actions, and options fail before configuration loading or resource creation.

`cli_config.build_runtime_config` normalizes parsed arguments. A selected runner
converts configuration into typed requests, validates output, constructs mode
resources, invokes commands, publishes results, and calls presentation. It returns
a structured result with a boolean `partial` property, which the CLI maps to exit
code 1 for partial execution and 0 otherwise. Action defaults are not inferred.

For discovery, `CrawlRequest` holds `CrawlOptions`, output, and overwrite policy.
`S3Settings` holds bucket/profile/region. `CrawlSchemas(source, options).execute()`
returns `DiscoveryReport` without CLI or terminal setup. The report serializer
owns atomic publication, and `presentation.present` owns the literal summary.
The existing bucket/prefix fields in `CrawlOptions` remain to preserve its Python
interface and the source provenance in the version-2 YAML contract.

Configuration loads exactly once in `cli.main` for an invocation that needs it.
SDK credential resolution happens inside the S3 composition boundary after that
load. Commands do not access environment variables. No arguments and help return
usage without loading configuration; doctor bypasses workflow dispatch. Help,
doctor, and runner imports do not import Boto3, PyArrow, or discovery readers.

Exit codes remain 0 for success/help, 1 for runtime failure/partial output, 2 for
usage errors, and 130 for cancellation. Dotenv semantics and exit codes remain stable. Version-2 manifests add executable
selectors, parser policies, readiness, and fingerprints; version-1 reports require
recrawling before ingestion.

## Extension and compatibility

To add a module, implement its current workflow under `modules/<name>` and add
its route descriptor(s). To add a mode, implement the required capabilities under
`modes/<name>` and connect it in the relevant runner. The ingestion consumer reuses S3 selectors directly through injected collaborators. API sources may expose different capabilities; do not force
them into an object-range interface merely to reuse a name.

Only create files needed by actual behavior. No command inheritance, generic
configuration bridge, dynamic external plugin discovery, or unimplemented module
is required. Shared format adapters now serve both discovery and record consumption.
The S3 access-log adapter uses the declared AWS field contract; other text formats
use observed schema inference. Data values and credentials never enter manifests.
The live validation script is opt-in composition only; reusable validation and
record-reading behavior stays in commands.

The old `features.discover` modules contain compatibility re-exports of canonical
objects, plus an argument-registration wrapper using the same option inventories.
They contain no duplicate workflows. New code must not import them. Remove these
wrappers only through an explicit compatibility decision.

Tests cover both public behavior and dependency boundaries, including lazy imports,
route-driven extension, incompatible flags, source lifecycle, and object identity
through compatibility imports. Use synthetic data and injected/stubbed sources;
never require a live bucket or local credentials for the test suite.
