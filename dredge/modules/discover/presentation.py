"""Discovery summaries, separate from commands and source integration."""
from ...core.ui import print_info, print_warning
from .models import DiscoveryReport


def present(report: DiscoveryReport, output: str):
    print_info(f"Wrote {len(report.document['datasets'])} dataset(s) to {output}; "
               f"status: {report.document['status']}; "
               f"matched objects: {report.document['counts']['matched']}.")

    ready = sum(dataset['ingestion']['ready'] for dataset in report.document['datasets'])
    print_info(f"Ingestion-ready datasets: {ready}; sampled objects: "
               f"{report.document['counts']['sampled_objects']}/{report.document['counts']['matched']}.")
    if report.document['counts']['unsupported']:
        print_warning(f"Skipped {report.document['counts']['unsupported']} objects with unsupported names/formats. "
                      "Use --format for custom-named supported data.")
