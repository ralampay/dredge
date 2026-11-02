"""Resumable scene ingestion independent of providers and terminal setup."""
from dataclasses import asdict
from datetime import datetime, timezone

from ....core.exceptions import UserError
from ....core.imagery import AuthenticationError, Scene
from .models import ImageryResult
from .preview import PreviewScenes
from .publication import ManagedOutput, PublicationError, fingerprint, safe_path


class IngestScenes:
    def __init__(self, catalog, downloader, request, processor=None, on_progress=lambda message: None,
                 check_cancel=lambda: None):
        self.catalog, self.downloader, self.request = catalog, downloader, request
        self.processor, self.progress, self.check_cancel = processor, on_progress, check_cancel

    def execute(self):
        preview = PreviewScenes(self.catalog, self.request).execute()
        if self.request.dry_run:
            return preview
        self.downloader.authenticate()
        with ManagedOutput(self.request.output, self.request.query.bbox).open() as store:
            if self.processor:
                self.processor.prepare(store)
            store.document['query'] = asdict(self.request.query)
            store.document['updated_at'] = datetime.now(timezone.utc).isoformat()
            store.document['status'] = 'incomplete'
            store.save()
            failed = False
            for entry in preview.document['scenes']:
                self.check_cancel()
                identifier = entry['identifier']
                self.progress('Processing scene ' + identifier)
                saved = store.document['scenes'].setdefault(identifier, {**entry, 'artifacts': []})
                try:
                    self._ingest_scene(store, entry, saved)
                    if saved['status'] != 'complete':
                        failed = True
                except (AuthenticationError, PublicationError):
                    raise
                except UserError as exc:
                    saved.update(status='failed', error=str(exc))
                    store.save()
                    failed = True
                    if self.request.on_error == 'fail':
                        raise
                store.save()
            failed = failed or any(s.get('status') != 'complete' for s in store.document['scenes'].values())
            store.document['status'] = 'partial' if failed else 'complete'
            store.save()
            document = {**preview.document, 'status': store.document['status'],
                        'scenes': list(store.document['scenes'].values()),
                        'tiff_count': sum(a['path'].lower().endswith('.tif') for s in store.document['scenes'].values()
                                          for a in s.get('artifacts', [])), 'output': str(store.root)}
            return ImageryResult(document, failed)

    def _ingest_scene(self, store, entry, saved):
        scene = Scene(**{name: entry[name] for name in Scene.__dataclass_fields__})
        identity = fingerprint({'id': scene.identifier, 'url': scene.url, 'size': scene.size_bytes})
        raw = saved.get('raw')
        valid = raw is not None and store.verified(raw)
        if raw and (not valid or saved.get('source_fingerprint') != identity) and not self.request.overwrite:
            if safe_path(store.root, raw['path']).exists():
                raise UserError('Existing raw artifact conflicts; use --overwrite.')
        if not valid or saved.get('source_fingerprint') != identity:
            with store.workspace() as workspace:
                temporary = workspace / 'product.zip'
                self.downloader.download(scene, temporary)
                self.check_cancel()
                raw = store.publish(temporary, 'raw/' + identity + '.zip', self.request.overwrite)
            saved.update(entry)
            saved.update(raw=raw, source_fingerprint=identity, status='downloaded', local_path=raw['path'])
            store.save()
        saved.pop('error', None)
        if self.processor:
            self.processor.process(scene, saved, store)
        else:
            saved['status'] = 'complete'
