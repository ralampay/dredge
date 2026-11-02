"""Read-only live validation; credentials and record values never enter reports."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

# Allow execution from a source checkout without installing the CLI package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dredge.core.configuration import load_environment
from dredge.core.exceptions import UserError
from dredge.core.schema_manifest import SchemaManifest
from dredge.core.ui import print_info, print_error
from dredge.modes.s3.clients import open_source
from dredge.modes.s3.configuration import S3Settings
from dredge.modes.s3.datasets import S3DatasetAdapter
from dredge.modules.ingest.validation import ValidateDatasetRecords


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--schema', required=True)
    parser.add_argument('--profile')
    parser.add_argument('--region')
    parser.add_argument('--output', required=True)
    parser.add_argument('--sample-files', type=int, default=50)
    parser.add_argument('--sample-records', type=int, default=1000)
    parser.add_argument('--sample-bytes', type=int, default=8 * 1024 * 1024)
    args = parser.parse_args()
    if min(args.sample_files, args.sample_records, args.sample_bytes) <= 0:
        parser.error('sampling limits must be positive')
    try:
        load_environment()
        manifest = SchemaManifest.load(args.schema)
        info = manifest.document['source']
        settings = S3Settings(info['bucket'], args.profile, args.region or info.get('region'))
        results = []
        with open_source(settings) as source:
            for dataset in manifest.document['datasets']:
                result = ValidateDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset),
                                                sample_files=args.sample_files, sample_records=args.sample_records,
                                                sample_bytes=args.sample_bytes).execute()
                results.append(result)
                print_info(f"{dataset['name']}: {result['status']}; {result['validated_objects']} held-out objects, "
                           f"{result['validated_records']} records validated.")
        report = {'validated_at': datetime.now(timezone.utc).isoformat(), 'schema': str(Path(args.schema)),
                  'source': info, 'read_only': True, 'datasets': results}
        Path(args.output).write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        return 0 if results and all(r['status'] == 'passed' for r in results) else 1
    except (UserError, OSError) as exc:
        print_error(str(exc))
        return 1
    except KeyboardInterrupt:
        print_error('Cancelled.')
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
