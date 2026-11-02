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
    def __init__(self, dry_run=False, console=None):
        self.console = console if console is not None else Console(stderr=True)
        self.dry_run = dry_run
        self.display = create_progress(SpinnerColumn(), TextColumn('{task.description}', markup=False),
                                       TimeElapsedColumn(), console=self.console, transient=True,
                                       refresh_per_second=4)
        self.task = None

    def __enter__(self):
        label = 'Listing source sizes (no content downloads)' if self.dry_run else 'Ingesting records'
        if self.console.is_terminal:
            self.display.start()
            self.task = self.display.add_task(label, total=None)
        else:
            self.console.print(Text(label))
        return self

    def __call__(self, event):
        if self.task is None:
            return
        if self.dry_run:
            label = f'Listing source sizes: {event:,} objects examined'
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
