"""Literal Rich rendering for catalog navigation and table fields."""
from rich.console import Console
from rich.table import Table
from rich.text import Text


def present(report, console=None):
    console = console or Console()
    document = report.document
    if document['action'] == 'describe':
        for item in document['items']:
            console.print(Text(f"{item['database']}.{item['name']} | {item.get('type') or 'table'}", style='bold'))
            console.print(Text('Location: ' + (item.get('location') or 'not specified')))
            table = Table('Field', 'Glue type', 'Partition key', 'Comment')
            for column in item['columns']:
                table.add_row(Text(column['name']), Text(column['type']), 'yes' if column['partition'] else 'no',
                              Text(column.get('comment') or ''))
            console.print(table)
    else:
        columns = ('id', 'name', 'description') if document['action'] == 'catalogs' else ('name', 'description', 'location')
        table = Table(*(name.replace('_', ' ').title() for name in columns))
        for item in document['items']:
            table.add_row(*(Text(str(item.get(name) or '')) for name in columns))
        console.print(table)
        console.print(Text(f"{len(document['items']):,} {document['action']} visible in this account/region."))
    if report.output:
        console.print(Text('Metadata saved: ' + report.output))
