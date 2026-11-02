"""Generate a portable two-cell example without a Jupyter dependency."""
import json


def type_label(node):
    kind = node['type']
    if kind == 'list':
        return 'list[' + type_label(node['items']) + ']'
    if kind == 'decimal':
        return f"decimal({node['precision']}, {node['scale']})"
    if kind == 'timestamp':
        return f"timestamp ({node['unit']}, {node.get('timezone') or 'no timezone'})"
    return kind


def field_lines(node, prefix=''):
    for field in node.get('fields', []):
        name = prefix + field['name']
        label = type_label(field)
        escaped = name.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('|', '&#124;').replace('\n', '<br>')
        yield f"| {escaped} | {label} | {'yes' if field['nullable'] else 'no'} |"
        if field['type'] == 'struct':
            yield from field_lines(field, name + '.')
        elif field['type'] == 'list' and field['items']['type'] == 'struct':
            yield from field_lines(field['items'], name + '[].')


def write_notebook(path, dataset, schema, partitions):
    from ...formats.schema import arrow_schema
    heading = dataset['name'].replace('<', '&lt;').replace('>', '&gt;')
    markdown = f'# Dataset: {heading}\n\nDataset ID: `{dataset["id"]}`\n\n'
    markdown += ('Install `pandas`, `pyarrow`, and Jupyter. Run this notebook with its containing directory as the working directory. '
                 'The code reads every Parquet shard in `data/`, including all source partition values, and displays the first five rows. '
                 'It loads the entire dataset into memory; for large datasets use Arrow batch iteration and convert one batch at a time.\n\n'
                 'Expected headers (dotted paths describe nested fields, not additional top-level columns):\n\n'
                 '| Header | Type | Nullable |\n| --- | --- | --- |\n')
    markdown += '\n'.join(field_lines(arrow_schema(schema)))
    if partitions:
        markdown += '\n\nPartition columns:\n' + '\n'.join(
            f'- `{p["column"]}`: source segment {p["segment"]}, name {json.dumps(p["name"])}, layout `{p["layout"]}`.' for p in partitions)
    if dataset.get('resolutions'):
        markdown += '\n\nExplicit validated resolutions applied before ingestion:\n\n```json\n' + json.dumps(dataset['resolutions'], indent=2) + '\n```\n'
    if any(rule['op'] == 'encode_json' for rule in dataset.get('resolutions', [])):
        markdown += ('\nJSON fallback columns contain JSON-encoded strings; source nulls remain null. '
                     'Decode a non-null cell with `json.loads(value)`. For example: '
                     '`df[\"column\"].map(lambda value: json.loads(value) if pd.notna(value) else None)` '
                     '(after `import json`). Empty arrays are stored as `[]`, not null.\n')
    code = 'import pandas as pd\n\ndf = pd.read_parquet("data", engine="pyarrow")\ndf.head()'
    notebook = {'nbformat': 4, 'nbformat_minor': 5,
                'metadata': {'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'},
                             'language_info': {'name': 'python'}},
                'cells': [dict(cell_type='markdown', id='dataset-contract', metadata={}, source=markdown),
                          dict(cell_type='code', id='read-dataset', metadata={}, source=code,
                               execution_count=None, outputs=[])]}
    path.write_text(json.dumps(notebook, indent=2) + '\n', encoding='utf-8')
