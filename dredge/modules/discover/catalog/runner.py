"""Compose Glue discovery without embedding SDK operations."""


def run_catalog_discovery(config):
    from dataclasses import replace
    from ....core.schema_io import validate_output
    from ....modes.aws.configuration import AWSSettings
    from ....modes.aws.clients import open_clients
    from ....modes.glue.adapters import GlueCatalog
    from .requests import CatalogRequest
    from .commands import BrowseCatalog
    from .presentation import present

    request = CatalogRequest.from_config(config)
    request.validate()
    if request.output:
        validate_output(request.output)
    with open_clients(AWSSettings.from_config(config), 'glue') as clients:
        report = BrowseCatalog(GlueCatalog(clients['glue']),
                               replace(request, region=clients['glue'].meta.region_name)).execute()
    present(report)
    return report
