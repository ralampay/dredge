"""Rich query progress with explicit remote-cancellation status."""
from rich.console import Console
from rich.progress import SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.text import Text

from ....core.ui import create_progress
from ..presentation import size_label


class QueryDisplay:
    def __init__(self, console=None):
        self.console = console or Console(stderr=True)
        self.progress = create_progress(SpinnerColumn(), TextColumn('{task.description}', markup=False),
                                        TimeElapsedColumn(), console=self.console, transient=True)
        self.task, self.last_state, self.query_id = None, None, None

    def __enter__(self):
        if self.console.is_terminal:
            self.progress.start()
            self.task = self.progress.add_task('Preparing Athena query', total=None)
        return self

    def __call__(self, event):
        if event.query_id and self.query_id != event.query_id:
            self.query_id = event.query_id
            self.console.print(Text('Athena query ID: ' + event.query_id))
        label = (f'{event.state} | {event.elapsed:.0f}s | {size_label(event.bytes_scanned)} scanned | '
                 f'{size_label(event.bytes_written)} exported')
        if self.task is not None:
            self.progress.update(self.task, description=label)
        elif self.last_state != event.state:
            self.console.print(Text(label))
        if event.detail:
            self.console.print(Text(event.detail))
        self.last_state = event.state

    def __exit__(self, *exc):
        if self.task is not None:
            self.progress.stop()
        return False


def present(result, console=None):
    console = console or Console()
    document = result.document
    console.print(Text(f"Athena export complete: {result.output}", style='bold green'))
    console.print(Text(f"Query {document['query_id']} | {len(document['columns'])} columns | "
                       f"{size_label(document['bytes'])} CSV | "
                       f"{size_label(document['statistics']['data_scanned_bytes'])} scanned"))
    console.print(Text('Files: data.csv, manifest.json, preview.ipynb. The notebook reads at most five rows.'))
