"""Rich rendering of ingestion events and completed results."""
from rich.console import Console
from rich.progress import SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.text import Text

from ...core.ui import create_progress


def size_label(value):
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB'):
        if value < 1024 or unit == 'TiB':
            return f'{value:,.1f} {unit}'
        value /= 1024


class IngestionProgress:
    def __init__(self, dry_run=False, console=None, metadata=False):
        self.console = console if console is not None else Console(stderr=True)
        self.dry_run = dry_run
        self.metadata = metadata
        self.display = create_progress(SpinnerColumn(), TextColumn('{task.description}', markup=False),
                                       TimeElapsedColumn(), console=self.console, transient=True,
                                       refresh_per_second=4)
        self.task = None

    def __enter__(self):
        label = 'Listing source sizes (no content downloads)' if self.dry_run else 'Ingesting records'
        if self.dry_run and self.metadata:
            label = 'Estimating source sizes (Parquet footer downloads enabled)'
        if self.console.is_terminal:
            self.display.start()
            self.task = self.display.add_task(label, total=None)
        else:
            self.console.print(Text(label))
        return self

    def __call__(self, event):
        if self.task is None:
            return
        if isinstance(event, int):
            label = f'Estimating source sizes: {event:,} objects examined'
        else:
            label = (f'{event.objects:,} objects completed | {event.records:,} records processed | '
                     f'{size_label(event.bytes_read)} read | {event.files:,} files written')
        self.display.update(self.task, description=label)

    def __exit__(self, exc_type, exc, traceback):
        if self.task is not None:
            self.display.stop()
        if exc_type is not None:
            self.console.print(Text('Ingestion stopped before successful completion.', style='yellow'))
        return False


def present(result, console=None):
    console = console if console is not None else Console()
    document = result.document
    if 'source_estimate' in document:
        present_source_estimate(result, console)
        return
    if 'filter_fields' in document:
        present_filter_fields(document, console)
        return
    if 'estimate' in document and 'filter_plan' in document:
        present_filter_estimate(result, console)
        return
    if document.get('dry_run'):
        table = Table('Dataset / ID', 'Status', 'Objects', 'Stored source size', 'Largest object')
        for row in document['datasets']:
            table.add_row(Text(row['name'] + '\n' + row['id'], overflow='fold'),
                          Text('BLOCKED' if row['blockers'] else 'READY'), str(row['objects']),
                          size_label(row['bytes']), size_label(row['largest']))
        console.print(table)
        for row in document['datasets']:
            for blocker in row['blockers']:
                console.print(Text(row['id'] + ': ' + blocker, style='yellow'))
        console.print(Text(f"Total: {document['objects']:,} unique objects; {size_label(document['bytes'])} stored source size."))
        console.print(Text('No object contents downloaded and no output created. Stored size includes compression; '
                           'it does not estimate memory, Parquet output size, or exact transferred bytes. '
                           'Listings use AWS requests and are not a snapshot of a future run.'))
    else:
        console.print(Text('Ingestion complete', style='bold green'))
        table = Table('Dataset', 'Rows', 'Files', 'Output size')
        for dataset in document['datasets']:
            table.add_row(Text(dataset['name']), str(dataset['rows']), str(len(dataset['files'])), size_label(dataset['bytes']))
        console.print(table)
        console.print(Text(f'Output: {result.output}'))
        console.print(Text('Each dataset has a preview.ipynb notebook. Its pandas example loads all shards into memory.'))


def present_filter_fields(document, console):
    for dataset in document['filter_fields']:
        console.print(Text(dataset['name'] + '\n' + dataset['id'], style='bold'))
        table = Table('Time field (literal top-level name)', 'Type', 'Source timezone')
        for field in dataset['fields']:
            table.add_row(Text(field['name']), Text(field['type']), Text(field.get('timezone') or 'unspecified'))
        console.print(table)
        console.print(Text('Partitions: ' + ', '.join(p['name'] for p in dataset['partitions'])))
    console.print(Text('Choose --time-field or --partition-date year,month,day. Folder dates are not assumed to be event times. '
                       'Ranges are start-inclusive/end-exclusive; naive timestamps default to UTC. No S3 requests made.'))


def present_filter_estimate(result, console):
    document = result.document
    plan, estimate = document['filter_plan'], document['estimate']
    predicate = plan['filter']
    console.print(Text(f"{predicate['kind']} time filter: {predicate['field']} | "
                       f"[{predicate['start']}, {predicate['end']}) | timezone: {predicate['timezone']}"))
    table = Table('Measure', 'Value')
    for key, title in [('candidate_objects', 'Candidate objects'), ('excluded_objects', 'Excluded objects'),
                       ('selected_objects', 'Selected objects'), ('candidate_bytes', 'Candidate stored bytes'),
                       ('selected_bytes', 'Selected stored bytes'), ('estimated_data_bytes', 'Estimated data read bytes'),
                       ('metadata_bytes', 'Planning metadata bytes fetched')]:
        value = estimate[key]
        table.add_row(title, size_label(value) if 'bytes' in key else f'{value:,}')
    if plan['inspect_parquet_metadata']:
        table.add_row('Selected / total row groups', f"{estimate['selected_row_groups']:,} / {estimate['row_groups']:,}")
        table.add_row('Retained groups with unknown statistics', str(estimate['uncertain_row_groups']))
    console.print(table)
    if plan['inspect_parquet_metadata']:
        console.print(Text('Data estimate sums selected compressed column chunks. Footer reads, buffering and retries add transfer. '
                           'Metadata inspection was explicitly enabled by this plan.'))
    else:
        console.print(Text('No object contents downloaded. The estimate sums selected object sizes.'))
    if predicate['kind'] == 'record' and not plan['inspect_parquet_metadata']:
        console.print(Text('Record-time selection requires scanning candidate files during ingestion; matching row counts are unknown.'))
    console.print(Text('Output size is unknown until ingestion. Inventory and estimates refresh on each run; this is not a source snapshot.'))
    for blocker in document['blockers']:
        console.print(Text(blocker, style='yellow'))
    if document['saved']:
        console.print(Text('Plan saved: ' + result.output))


def present_source_estimate(result, console):
    document = result.document
    plan, estimate = document['source_plan'], document['source_estimate']
    console.print(Text('Source prefix: s3://' + plan['bucket'] + '/' + plan['prefix']))
    console.print(Text('Partition predicates: ' + (', '.join(plan['partitions']) or 'none')))
    if plan['calendar']:
        calendar = plan['calendar']
        console.print(Text(f"Calendar range: [{calendar['from']}, {calendar['to']}) on " + ','.join(calendar['names'])))
    table = Table('Selection', 'Objects', 'Stored bytes')
    table.add_row('Selected', f"{estimate['objects']:,}", size_label(estimate['bytes']))
    if document['schema_checked']:
        for category in ('consumable', 'unmatched', 'ambiguous'):
            table.add_row(category.capitalize(), f"{estimate[category + '_objects']:,}", size_label(estimate[category + '_bytes']))
    console.print(table)
    for row in document['datasets']:
        if row['objects']:
            status = ', '.join(row['blockers']) or 'READY'
            console.print(Text(f"{row['id']}: {row['objects']:,} objects; {status}"))
    for category, keys in document['examples'].items():
        for key in keys:
            console.print(Text(category.capitalize() + ': ' + key, style='yellow'))
    console.print(Text('No object contents downloaded. Stored bytes are not Parquet output size or exact network transfer. '
                       'Inventory is refreshed on each run; zero-byte files are counted and directory markers are excluded.'))
    if not document['schema_checked']:
        console.print(Text('Schema compatibility has not been checked. Actual ingestion requires --schema.'))
    else:
        console.print(Text('Consumable means exactly one selector matches; listed schema blockers still prevent ingestion.'))
    if document['saved']:
        console.print(Text('Plan saved: ' + result.output))
