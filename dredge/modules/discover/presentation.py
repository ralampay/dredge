"""Rich progress and compact schema reports; no discovery/provider operations."""
from rich.console import Console
from rich.progress import BarColumn, SpinnerColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.table import Table
from rich.text import Text

from ...core.ui import create_progress
from .models import DiscoveryReport


def size_label(value):
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB'):
        if value < 1024 or unit == 'TiB':
            return f'{value:,.1f} {unit}' if unit != 'B' else f'{value:,} B'
        value /= 1024


class RichDiscoveryProgress:
    """Render injected workflow events; redirected output gets phase lines only."""
    def __init__(self, console=None):
        self.console = console if console is not None else Console(stderr=True)
        self.live = self.console.is_terminal
        self.phase = None
        self.event = None
        self.display = create_progress(
            SpinnerColumn(), TextColumn('{task.description}', markup=False), BarColumn(bar_width=18),
            TextColumn('{task.fields[detail]}', markup=False), TimeElapsedColumn(),
            TextColumn('ETA', markup=False), TimeRemainingColumn(),
            console=self.console, transient=True, refresh_per_second=4,
        )
        self.task = None

    def __enter__(self):
        if self.live:
            self.display.start()
        return self

    def __call__(self, event):
        self.event = event
        changed = event.phase != self.phase
        self.phase = event.phase
        if event.phase == 'listing':
            description = 'Listing S3 objects'
            detail = f'{event.listed:,} listed; total unknown'
            total, completed = None, 0
        elif event.phase == 'inspecting':
            description = 'Inspecting samples'
            remaining = max(0, event.total_files - event.completed_files)
            detail = (f'{event.completed_files:,}/{event.total_files:,} files; {remaining:,} left; '
                      f'{size_label(event.bytes_read)} read')
            if event.failed_files:
                detail += f'; {event.failed_files:,} failed'
            total, completed = event.total_files, event.completed_files
        elif event.phase == 'writing':
            description, detail = 'Writing schema', f'{event.completed_files:,} samples processed'
            total, completed = None, 0
        else:
            description, detail = 'Discovery failed', f'{event.completed_files:,}/{event.total_files:,} samples processed'
            total, completed = None, 0
        if self.live:
            if self.task is None:
                self.task = self.display.add_task(description, total=total, detail=detail)
            elif changed:
                self.display.reset(self.task, total=total, completed=completed, description=description, detail=detail)
            else:
                self.display.update(self.task, completed=completed, description=description, detail=detail)
        elif changed:
            self.console.print(Text(f'{description}: {detail}'))
        if changed and event.phase == 'inspecting':
            self.console.print(Text(
                f'Listed {event.listed:,} objects; {event.matched:,} match across {event.datasets:,} datasets. '
                f'Inspecting {event.total_files:,} selected files. ETA estimates sample work, not a full data scan.'
            ))

    def __exit__(self, exc_type, exc, traceback):
        if self.live:
            self.display.stop()
        if exc_type is not None:
            self.console.print(Text('Discovery stopped before successful completion.', style='yellow'))
        return False


def type_label(node):
    kind = node['type']
    if kind == 'list':
        return 'list<' + type_label(node['items']) + '>'
    if kind == 'struct':
        names = ', '.join(field['name'] for field in node['fields'][:3])
        return 'struct<' + names + (', …' if len(node['fields']) > 3 else '') + '>'
    if kind == 'union':
        return ' | '.join(type_label(item) for item in node['alternatives'])
    if kind == 'decimal':
        return f"decimal({node['precision']},{node['scale']})"
    if kind == 'timestamp':
        return 'timestamp' + (f" [{node['timezone']}]" if node.get('timezone') else '')
    return kind


def field_issue(node):
    if node['type'] == 'unknown':
        return 'Unresolved type'
    if node['type'] == 'union':
        return 'Conflicting types'
    if node['type'] == 'list':
        issue = field_issue(node['items'])
        return 'Element: ' + issue if issue else ''
    return ''


def field_rows(schema, prefix=''):
    for field in schema.get('fields', []):
        name = prefix + field['name']
        note = field_issue(field)
        inference = field.get('inference', {})
        if not note and inference.get('diagnostic'):
            note = inference['diagnostic'].replace('_', ' ')
        if not note and field.get('input_format'):
            note = 'Inferred ISO date' if field['input_format'] == 'iso-date' else 'Inferred ISO time'
        source = field.get('source', {})
        origin = {'csv-header': 'Header', 'generated-position': 'Position', 'json-key': 'JSON',
                  'parquet-field': 'Parquet', 'format-field': 'Format'}.get(source.get('kind'), source.get('kind', ''))
        if source.get('assumed'):
            origin += ' (assumed)'
        yield (name, type_label(field), 'yes' if field['nullable'] else 'no', origin, note)
        if field['type'] == 'struct':
            yield from field_rows(field, name + '.')
        elif field['type'] == 'list' and field['items']['type'] == 'struct':
            yield from field_rows(field['items'], name + '[].')


BLOCKERS = {
    'unresolved_types': 'Resolve the unknown/conflicting types highlighted below.',
    'sample_failures': 'Some sampled files could not be inspected; review the YAML failures.',
    'no_record_schema': 'No usable record schema was found.',
    'ambiguous_csv_header_use_explicit_override': 'CSV header is ambiguous; set --csv-header present or absent.',
    'inconsistent_csv_header_presence': 'Sampled CSV files disagree on header presence.',
}


def present(report: DiscoveryReport, output: str, *, field_limit=20, console=None):
    console = console if console is not None else Console()
    document, counts = report.document, report.document['counts']
    ready = sum(d['ingestion']['ready'] for d in document['datasets'])
    console.print(Text('Discovery complete' if not report.partial else 'Discovery completed with failures', style='bold cyan'))
    console.print(Text(
        f"{counts['listed']:,} objects listed | {counts['sampled_objects']:,} sampled | "
        f"{counts['sampled_records']:,} text records | {size_label(counts['sampled_bytes'])} sampled\n"
        f"{len(document['datasets']):,} datasets | {ready:,} ingestion-ready | "
        f"{counts['failed_objects']:,} sample failures"
    ))
    if counts['unsupported']:
        console.print(Text(f"Skipped {counts['unsupported']:,} unsupported objects; use --format for custom names.", style='yellow'))
    for dataset in document['datasets']:
        contract = dataset['ingestion']
        console.print()
        console.print(Text(dataset['name'], style='bold'))
        status = 'READY' if contract['ready'] else 'BLOCKED'
        console.print(Text(
            f"{status} | {dataset['format']} | {dataset['matched_objects']:,} objects | "
            f"{len(dataset['schema'].get('fields', [])):,} top-level fields | {len(dataset['samples']):,} sampled files",
            style='green' if contract['ready'] else 'yellow'))
        if dataset['partitions']:
            console.print(Text('Partitions: ' + ', '.join(p['name'] for p in dataset['partitions'])))
        for blocker in contract['blockers']:
            console.print(Text(BLOCKERS.get(blocker, blocker), style='yellow'))
        json_fields = set()
        for rule in dataset.get('resolutions', []):
            if rule['op'] == 'encode_json':
                name = ''
                for segment in rule['path']:
                    name += ('.' if name else '') + segment['field'] if 'field' in segment else '[]'
                json_fields.add(name)
        rows = [(name, kind, nullable, origin, 'JSON-encoded' if name in json_fields else note)
                for name, kind, nullable, origin, note in field_rows(dataset['schema'])]
        if field_limit != 'all' and len(rows) > field_limit:
            # Surface blockers even when they occur late in a wide schema.
            critical = [row for row in rows if row[4].startswith(('Unresolved', 'Conflicting', 'Element:'))]
            notable = [row for row in rows if row[4] and row not in critical]
            ordinary = [row for row in rows if not row[4]]
            displayed = (critical + notable + ordinary)[:field_limit]
        else:
            displayed = rows
        table = Table('Field', 'Assumed type', 'Null?', 'Source', 'Notes', box=None,
                      show_edge=False, padding=(0, 1), expand=False)
        for row in displayed:
            table.add_row(*(Text(value, style='yellow' if row[4] else '') for value in row))
        console.print(table)
        if len(displayed) < len(rows):
            console.print(Text(f'Showing {len(displayed)} of {len(rows)} schema entries (notable fields first). '
                               'Use --report-fields all to show every field.', style='dim'))
    console.print()
    console.print(Text(f'Full contract: {output}', style='cyan'))
    console.print(Text('Types and nullability are assumptions from sampled data or format metadata; unsampled data may differ.', style='dim'))


class CrawlResolutionDisplay:
    """Delegate phase rendering while allowing only one live display at a time."""
    def __init__(self, console=None):
        self.console = console if console is not None else Console(stderr=True)
        self.active = None
        self.phase = None

    def __enter__(self):
        return self

    def _stop(self, exc_type=None, exc=None, traceback=None):
        if self.active is not None:
            active, self.active = self.active, None
            active.__exit__(exc_type, exc, traceback)

    def __call__(self, event):
        from ..schema.presentation import ResolutionProgressDisplay
        if event.phase != self.phase:
            self._stop()
            self.phase = event.phase
            if event.phase == 'discovery':
                self.active = RichDiscoveryProgress(self.console)
            elif event.phase == 'validation':
                self.active = ResolutionProgressDisplay(self.console)
            elif event.phase == 'rules':
                self.console.print(Text('Applying schema resolution policy'))
            elif event.phase == 'discovery_output':
                self.console.print(Text(f'Writing raw discovery: {event.detail}'))
            elif event.phase == 'publication':
                self.console.print(Text(f'Publishing validated schema: {event.detail}'))
            if self.active:
                self.active.__enter__()
        if self.active and event.detail is not None:
            self.active(event.detail)

    def __exit__(self, exc_type, exc, traceback):
        self._stop(exc_type, exc, traceback)
        if exc_type is not None:
            self.console.print(Text('Crawl and resolution stopped; final output was not published.', style='yellow'))
        return False


def present_combined(report, output, *, field_limit=20, console=None):
    from ...formats.resolution import path_label
    from ..schema.presentation import present as present_resolution
    console = console if console is not None else Console()
    table = Table('Dataset ID', 'Rule', 'Field', 'Status', box=None)
    for item in report.rules:
        table.add_row(Text(item['dataset_id'], overflow='fold'), Text(item['op']),
                      Text(path_label(item['path'])), Text(item['status']))
    console.print(table)
    present_resolution(report, output, console)
    present(report, output, field_limit=field_limit, console=console)
