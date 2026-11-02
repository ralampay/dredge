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
- `modules.ingest` owns record consumption, held-out validation, metadata-only
  previews, Parquet dataset materialization, notebooks, and publication. It
  receives source and selector collaborators without importing concrete modes.
- `formats` owns shared record parsers, format detection, schema transformations,
  and record type validation. Both discovery and ingestion consume these adapters.
- `core.schema_manifest` validates version-2/3 discovery and version-4 resolved contracts, while
  `core.partitioning` provides shared directory grouping semantics.
- `modes.s3` owns AWS configuration, resource lifecycle, and object access. It
  imports neutral contracts and never imports discovery or another module.
- `core.sources` defines `ObjectInfo`, `ObjectSource`, `DatasetSelection`,
  `SourceAccessError`, and shared `CountedSource` transfer accounting.
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
owns atomic publication, and `presentation.present` owns the Rich field tables.
An optional `on_progress` callback receives immutable `DiscoveryProgress` events
for listing, selected-file inspection, and bytes read. These events contain no
terminal dependencies. The runner wires them to `RichDiscoveryProgress`, which
owns live rendering, redirected phase summaries, and cleanup.
The existing bucket/prefix fields in `CrawlOptions` remain to preserve its Python
interface and the source provenance in the version-3 YAML contract.

Configuration loads exactly once in `cli.main` for an invocation that needs it.
SDK credential resolution happens inside the S3 composition boundary after that
load. Commands do not access environment variables. No arguments and help return
usage without loading configuration; doctor bypasses workflow dispatch. Help,
doctor, and runner imports do not import Boto3, PyArrow, or discovery readers.

Exit codes remain 0 for success/help, 1 for runtime failure/partial output, 2 for
usage errors, and 130 for cancellation. Dotenv semantics and exit codes remain stable. Version-3 manifests add ordered field sources and inference evidence to the
executable selector/parser contract. Version-2 contracts remain readable; version-1
reports require recrawling. Fingerprints for version 3 exclude observational metadata.

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

## Assumed field structure

Shared `formats.csv_headers`, `formats.temporal`, and `formats.evidence` own header
detection, strict temporal interpretation, and bounded value-free evidence.
Discovery merges raw types and evidence before attaching field annotations and
final temporal assumptions. This prevents sample order or per-file decisions from
silently converting mixed date/string data. CSV header decisions are collected per
object and reconciled at the dataset boundary. Consumers receive resolved parser
settings and declared temporal conversions; they do not repeat header inference.

Field order follows the deterministic discovery sample order and each format's
source order. Null/missing evidence describes observations; it is not a universal
data constraint. Known log types retain format-contract provenance and Parquet
metadata remains distinct from sampled record evidence.

## Ingestion output

`IngestDatasets.execute()` coordinates strict preflight, streamed record validation,
bounded Parquet writing, notebook generation, and staged publication. It receives
source and selector collaborators. Both commands validate that every dataset has
exactly one selector and that selected IDs are unique before touching the source.
Selectors expose a neutral prefix/matching/iteration interface; commands do not
inspect mode-specific selector dictionaries. `PreviewIngestion.execute()` uses the same
selectors to count matching stored bytes using listings only; shared preflight
reports blocked output types without downloading records. Both return
`IngestResult`; preview blockers set `partial` to preserve CLI exit semantics.

`formats.arrow_contract` maps executable schemas to explicit Arrow schemas and
checks lossy numeric/temporal conversions. `modules.ingest.writer` owns batches
and file rotation; `serialization` owns versioned manifests and safe dataset paths;
`publication` owns local output lifecycle; `notebook` emits
standard notebook JSON. These components do not render terminal output.
Presentation receives immutable progress events and renders Rich tables. Shared
`core.output_options` declares the output flag once for both routes.

Output manifest version 1 is independent of input schema versions 2, 3, and 4. It
records relative dataset/data/notebook paths, field contracts, partition mappings,
file inventory, sizes, and counts. Each dataset contains flat Parquet shards in
`data/`, isolated from its two-cell example notebook. Publication requires a fresh
destination; no append, replacement, or resume semantics are implied.

Aborted writers close resources without reporting successful files. If writer
cleanup also fails, the original failure or cancellation is preserved with the
cleanup error chained. Staging and destination-lock cleanup are registered
independently so a staging-removal failure does not strand the lock. Existing
record and progress imports remain compatibility aliases when ownership moves.

## Explicit schema resolution

`modules.schema` owns the resolve workflow, its typed requests/results, bounded
source validation, and Rich presentation. `ResolveSchema.execute()` accepts a
validated baseline, untrusted edited document, source, and selector factory. It
compiles only permitted edits, validates selected datasets, and publishes only
when all pass. Provider setup stays in the runner. The edited document is never
accepted directly as an ingestion contract.

`formats.resolution` owns typed-path traversal, schema-edit compilation, and pure
record transformations. Both validation and ingestion call these transformations
through `ReadDatasetRecords`; no transformation logic is duplicated in runners or
writers. `core.resolution_contract` verifies version-4 artifact consistency,
baseline provenance, permitted edits, fingerprints, and validation coverage.
`core.schema_io` owns atomic YAML publication, with discovery's old serializer
imports preserved as compatibility exports. `core.sampling` shares bounded,
deterministic sampling with existing held-out ingestion validation; only the
resolution workflow enables discovery-sample fallback.

Schema version 4 embeds an immutable copy of the original version-2/3 baseline.
Executable fingerprints include resolution operations; validation reports bind
to those fingerprints. Selected datasets require successful bounded validation
and meaningful coverage for every operation. Unselected datasets retain their
original schema/readiness, with version-4 fingerprints. The loader reconstructs
permitted edits from the baseline and compares the resulting contract before
allowing consumption. These bindings detect inconsistent edits, not maliciously
forged validation evidence; manifests are trusted local artifacts, not signatures.

Shared schema-input and sampling option inventories keep flags declared once.
The new `schema/s3/resolve` route remains lazy and requires no CLI dispatch branch.
Legacy version-2/3 ingestion and old discovery import paths remain supported.

## Combined discovery and resolution

`CrawlAndResolveSchemas.execute()` composes the existing discovery command,
reusable rule application, and `BuildResolvedSchema.execute()`. The builder
returns a validated contract without writing files; standalone `ResolveSchema`
retains its original fresh-output publication policy. The combined command owns
optional baseline publication and atomic final replacement, so failed resolution
never replaces an existing final contract. Its runner only loads configuration,
constructs one source session and collaborators, and connects presentation.

`modules.schema.rules` owns the version-1 reusable rules document, literal path
matching, type-parameter compatibility, and conversion to an edited draft. It
reuses the existing resolution compiler and transformations. Already compatible
fields produce satisfied diagnostics without executable transformations. Output
must be ingestion-ready across all discovered datasets, including those without
rules. Discovery and validation retain independent sampling settings.

Combined progress and result objects live in discovery models; phase rendering
switches between existing Rich discovery and resolution displays. Optional raw
output and final output are separate artifacts, with independent publication
points. Preflight rejects identical paths and existing-file aliases to prevent
replacing the rules input. Existing standalone commands and versioned artifact
contracts remain unchanged.

## Automatic JSON fallback

Discovery's opt-in `--unresolved-as json` uses the same combined workflow and
version-4 contract as explicit resolution, without requiring a rules file.
`modules.schema.json_fallback` builds an edited draft and nonoverlapping
`encode_json` operations from fresh discovery. Unknown/union fields and empty structs are selected;
arrays containing unresolved descendants are encoded as a whole. Known sibling
fields remain unchanged. `formats.json_fallback` owns eligibility checks and
JSON serialization; shared resolution compilation verifies that operations only
target eligible baseline nodes and preserve names/nullability.

Ingestion and validation share the same transformation execution. JSON fallback
coverage counts successfully validated records even for null or absent nullable
values; it is an encoding policy, not a claim that an unknown source type was
observed. Assertions and numeric extraction retain their non-null evidence
requirements. Baselines, fingerprints, schema comparison, and mandatory bounded
source validation continue to protect the resolved artifact. Renderers and
notebooks expose JSON encoding explicitly; no provider-specific fallback logic
lives in runners or writers.

## Time-filtered ingestion plans

Ingestion's `plan` action either inspects schema capabilities offline or runs
`PlanIngestion.execute()` with an injected source and selector. `filter_plan` owns
the version-1 YAML contract, schema/selector bindings, and atomic publication;
`time_filters` owns explicit temporal predicates and the provider-neutral
`FilteredSelection` decorator. `get --plan` refreshes inventory and reuses these
predicates. The runner only composes commands and presentation.

`formats.parquet_filter` selects conservative row groups from native temporal
statistics. Planning and streaming reads use the same function; record predicates
remain authoritative. Parquet range reads avoid generic stream buffering so
excluded column chunks are not prefetched. Record parsing, validation, writing,
and existing no-plan workflows retain their separate responsibilities. Plans do
not equate partition dates with event dates or imply a source snapshot.

## Schema-independent source plans

Version-2 source plans describe literal prefixes and Hive partition predicates;
they have no schema dependency. `core.partitioning.HiveFilter` handles literal
key/value matching and calendar ranges. `modes.s3.datasets.S3ObjectSelection`
passes the literal prefix to S3 and filters directory segments without reading
contents. Version-1 schema-bound time plans remain supported; `plan_io` owns
version dispatch and atomic persistence for both contracts.

`PreviewSourceSelection` streams selected objects and optionally checks schema
assignments. `IngestSelectedObjects` rejects unmatched, ambiguous, or blocked
assignments before handing intersected selectors to `IngestDatasets`. Assignment
checks repeat during streaming materialization to catch newly unmatched objects.
Only matching datasets are written; empty selections produce an empty collection.
The writer receives the source plan as provenance, with no new parsing responsibility.
Source-plan execution accepts a newly generated schema without changing the plan;
compatibility is checked against that schema at execution time.
