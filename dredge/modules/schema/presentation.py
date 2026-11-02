"""Rich resolution progress and validation summaries."""
from rich.console import Console
from rich.progress import SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.text import Text

from ...core.ui import create_progress


class ResolutionProgressDisplay:
    def __init__(self, console=None):
        self.console = console if console is not None else Console(stderr=True)
        self.display = create_progress(SpinnerColumn(), TextColumn('{task.description}', markup=False),
                                       TimeElapsedColumn(), console=self.console, transient=True)
        self.task = None

    def __enter__(self):
        if self.console.is_terminal:
            self.display.start()
            self.task = self.display.add_task('Listing and validating bounded source samples', total=None)
        else:
            self.console.print(Text('Listing and validating bounded source samples'))
        return self

    def __call__(self, event):
        label = f'{event.dataset_id}: {event.objects} objects, {event.records} records validated'
        if self.task is not None:
            self.display.update(self.task, description=label)

    def __exit__(self, exc_type, exc, traceback):
        if self.task is not None:
            self.display.stop()
        return False


def present(result, output, console=None):
    console = console if console is not None else Console()
    table = Table('Dataset ID', 'Validated objects', 'Reused samples', 'Records', 'Rules covered')
    for report in result.document['resolution']['validation']:
        table.add_row(Text(report['dataset_id'], overflow='fold'), str(len(report['objects'])),
                      str(sum(o['reused_discovery_sample'] for o in report['objects'])),
                      str(report['validated_records']), str(len(report['rule_coverage'])))
    console.print(table)
    console.print(Text(f'Resolved schema: {output}', style='green'))
    console.print(Text('Bounded validation passed. Ingestion will still validate every record; unsampled data may differ.'))
