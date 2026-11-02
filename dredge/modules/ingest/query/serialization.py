"""CSV dataset metadata and a bounded two-cell pandas notebook."""
from datetime import datetime, timezone
import json


def write_artifacts(stage, request, execution, exported):
    document = {'query_dataset_version': 1, 'status': 'complete',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'source': {'mode': 'athena', 'catalog': request.catalog, 'database': request.database,
                           'workgroup': request.workgroup},
                'query': request.sql, **execution, **exported, 'data_path': 'data.csv', 'notebook': 'preview.ipynb',
                'csv': {'encoding': 'utf-8', 'header': True,
                        'null_semantics': 'Empty CSV cells may represent SQL NULL or empty strings; pandas infers types.'}}
    (stage / 'manifest.json').write_text(json.dumps(document, indent=2) + '\n', encoding='utf-8')
    markdown = ('# Athena query result\n\nQuery ID: `' + execution['query_id'] + '`\n\n'
                'Run this notebook from its containing directory. Install pandas and Jupyter. '
                'The preview reads at most five rows, not the entire CSV. For larger processing, use '
                '`pd.read_csv("data.csv", chunksize=10000)`. CSV does not preserve native SQL types; '
                'empty values can represent NULL or empty strings. Pandas infers types and may rename duplicate headers.\n\n'
                '| Column | Athena type | Nullable |\n| --- | --- | --- |\n')
    def escape(value):
        return str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('|', '&#124;').replace('\n', '<br>')
    markdown += '\n'.join('| ' + ' | '.join(escape(c.get(k, 'UNKNOWN')) for k in ('name', 'type', 'nullable')) + ' |'
                          for c in exported['columns'])
    notebook = {'nbformat': 4, 'nbformat_minor': 5,
                'metadata': {'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'},
                             'language_info': {'name': 'python'}},
                'cells': [{'cell_type': 'markdown', 'id': 'query-contract', 'metadata': {}, 'source': markdown},
                          {'cell_type': 'code', 'id': 'preview', 'metadata': {}, 'execution_count': None, 'outputs': [],
                           'source': 'import pandas as pd\n\ndf = pd.read_csv("data.csv", nrows=5)\ndf'}]}
    (stage / 'preview.ipynb').write_text(json.dumps(notebook, indent=2) + '\n', encoding='utf-8')
    return document
