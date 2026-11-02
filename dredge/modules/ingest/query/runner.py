"""Compose Athena execution, CSV export, and cancellation."""


def run_query(config):
    from ....core.cancellation import handle_interrupts
    from ....core.ui import announce_cancellation
    from ....modes.aws.clients import open_clients
    from ....modes.aws.configuration import AWSSettings
    from ....modes.athena.adapters import AthenaQueries
    from ..publication import validate_output
    from .commands import ExportAthenaQuery
    from .requests import QueryRequest
    from .presentation import QueryDisplay, present

    request = QueryRequest.from_config(config)
    request.validate()
    validate_output(request.output)
    with handle_interrupts(announce_cancellation) as cancellation, QueryDisplay() as display:
        with open_clients(AWSSettings.from_config(config), 'athena', 's3') as clients:
            result = ExportAthenaQuery(AthenaQueries(clients['athena'], clients['s3']), request,
                                       cancellation.check, display).execute()
    present(result)
    return result
