"""Lazy provider composition for the ASF ingestion route."""


def run_imagery(config):
    from ....core.cancellation import handle_interrupts
    from ....core.ui import announce_cancellation
    from ....core.exceptions import UserError
    from ....modes.asf.configuration import EarthdataSettings
    from ....modes.asf.adapters import AsfCatalog, AsfDownloader
    from .requests import ImageryRequest
    from .commands import IngestScenes
    from .presentation import present, progress

    request = ImageryRequest.from_config(config)
    try:
        import asf_search
    except ImportError as exc:
        raise UserError('ASF ingestion requires the dependencies in requirements.txt.') from exc
    settings = EarthdataSettings.from_environment()
    with handle_interrupts(announce_cancellation) as cancellation:
        catalog = AsfCatalog(asf_search, check_cancel=cancellation.check)
        downloader = AsfDownloader(asf_search, settings, check_cancel=cancellation.check)
        processor = None
        client = None
        try:
            if not request.dry_run and request.processing.get('rtc', {}).get('enabled'):
                from .process_scenes import ProcessScenes
                from ....modes.hyp3.adapters import HyP3Service
                from ....modes.hyp3.clients import create_client
                downloader.authenticate()
                client = create_client(settings)
                processor = ProcessScenes(HyP3Service(client, check_cancel=cancellation.check), request,
                                          check_cancel=cancellation.check, on_progress=progress)
            result = IngestScenes(catalog, downloader, request, processor, progress, cancellation.check).execute()
            cancellation.check()
        except OSError as exc:
            raise UserError('Cannot read or publish imagery artifacts; check disk space and filesystem permissions.') from exc
        finally:
            downloader.close()
            if client is not None:
                client.session.close()
    present(result)
    return result
