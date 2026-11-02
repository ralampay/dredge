"""Optional RTC orchestration and reusable raster processing."""
import json
import time

from ....core.exceptions import UserError
from ....formats.raster.grid import ReferenceGrid, create_grid
from ....formats.raster.operations import align_raster
from ....formats.raster.tiling import TileRaster
from .publication import atomic_json, extract_archive, fingerprint, safe_path


class ProcessScenes:
    def __init__(self, service, request, check_cancel=lambda: None, sleep=time.sleep, clock=time.monotonic,
                 on_progress=lambda message: None):
        self.service, self.request, self.check_cancel = service, request, check_cancel
        self.sleep, self.clock, self.progress = sleep, clock, on_progress
        self.config = request.processing
        self.grid = None

    def prepare(self, store):
        settings = self.config.get('reference_grid')
        if settings:
            path = safe_path(store.root, 'reference-grid.json')
            if path.exists():
                try:
                    self.grid = ReferenceGrid.from_document(json.loads(path.read_text()))
                except (OSError, ValueError) as exc:
                    raise UserError('Cannot read persisted reference grid.') from exc
                from pyproj import CRS
                from pyproj.exceptions import CRSError
                try:
                    same_crs = CRS.from_user_input(self.grid.crs).equals(CRS.from_user_input(settings['crs']))
                except CRSError as exc:
                    raise UserError('Invalid configured reference CRS.') from exc
                if (not same_crs or
                        self.grid.bbox != self.request.query.bbox or
                        self.grid.resolution_m != settings['resolution_m'] or
                        self.grid.resampling != settings.get('resampling', 'bilinear') or
                        self.grid.nodata != settings.get('nodata', -9999)):
                    raise UserError('Reference grid differs; use the original grid settings or a new destination.')
            else:
                self.grid = create_grid(self.request.query.bbox, settings)
                atomic_json(path, self.grid.document())

    def process(self, scene, saved, store):
        options = self.config.get('rtc', {})
        identity = fingerprint({'scene': scene.granule, 'resolution': options.get('resolution', 30),
                                'radiometry': options.get('radiometry', 'gamma0'), 'scale': 'power'})
        settings_hash = fingerprint({**self.config, 'rtc': {k: v for k, v in options.items()
                                            if k not in ('timeout_seconds', 'poll_seconds', 'max_extract_bytes')}})
        if saved.get('processing_fingerprint') == settings_hash and saved.get('status') == 'complete':
            if all(store.verified(a) for a in saved.get('artifacts', [])):
                return
            if not self.request.overwrite and any(safe_path(store.root, a['path']).exists() and not store.verified(a)
                                                 for a in saved.get('artifacts', [])):
                raise UserError('Derived artifacts conflict; use --overwrite.')
        elif saved.get('processing_fingerprint') not in (None, settings_hash) and not self.request.overwrite:
            raise UserError('Processing configuration changed; use --overwrite for managed derived artifacts.')
        saved['processing_fingerprint'] = settings_hash
        state = self._resolve_job(scene, saved, store, identity, options)
        job = self._await_job(state, saved, store, options)
        if job is None:
            return
        artifacts = self._publish_products(scene, job, store, identity, settings_hash, options)
        saved.update(artifacts=artifacts, status='complete')
        store.save()

    def _resolve_job(self, scene, saved, store, identity, options):
        jobs = saved.setdefault('jobs', {})
        state = jobs.setdefault(identity, {})
        name = 'dredge-' + identity[:40]
        if not state.get('job_id'):
            matches = self.service.find(name)
            if len(matches) > 1:
                raise UserError('Multiple matching HyP3 jobs; resolve the saved submission before proceeding.')
            if matches:
                job = matches[0]
            else:
                if state.get('submission_intent'):
                    raise UserError('Unresolved HyP3 submission intent; inspect the service before clearing it. No duplicate job was submitted.')
                if not self.request.allow_hyp3_submit:
                    raise UserError('New RTC jobs require --allow-hyp3-submit on this invocation.')
                if scene.acquisition_mode != 'IW' or scene.product_type not in ('GRD_HD', 'GRD_HS'):
                    raise UserError('HyP3 RTC requires supported IW GRD-H scenes.')
                state['submission_intent'] = True
                store.save()
                job = self.service.submit(scene, name, options)
            state.update(job_id=job['job_id'], name=name)
            store.save()
        return state

    def _await_job(self, state, saved, store, options):
        deadline = self.clock() + options.get('timeout_seconds', 1800)
        while True:
            self.check_cancel()
            job = self.service.get(state['job_id'])
            # Signed product URLs are fetched afresh rather than persisted.
            state.update(status=job['status_code'], parameters=job.get('job_parameters', {}))
            saved['status'] = 'processing'
            store.save()
            if job['status_code'] == 'FAILED':
                raise UserError('HyP3 RTC job failed; inspect the recorded job ID.')
            if job['status_code'] == 'SUCCEEDED':
                return job
            if self.clock() >= deadline:
                saved['status'] = 'pending'
                store.save()
                return
            self.progress('Waiting for HyP3 job ' + state['job_id'])
            self.sleep(min(30, options.get('poll_seconds', 30), max(0, deadline - self.clock())))

    def _publish_products(self, scene, job, store, identity, settings_hash, options):
        artifacts = []
        with store.workspace() as workspace:
            for index, file in enumerate(job.get('files', [])):
                self.check_cancel()
                archive = workspace / f'rtc-{index}.zip'
                self.service.download(file, archive)
                extracted = workspace / f'extracted-{index}'
                extracted.mkdir()
                files = extract_archive(archive, extracted, options.get('max_extract_bytes', 50 * 1024**3))
                # Preserve complete RTC package including provider provenance.
                artifacts.append(store.publish(archive, f'derived/{identity}/rtc-{index}.zip', self.request.overwrite))
                rasters = [p for p in files if p.suffix.lower() in ('.tif', '.tiff') and
                           p.stem.upper().endswith(('_VV', '_VH', '_HH', '_HV'))]
                if not rasters:
                    raise UserError('RTC package contains no recognized polarization GeoTIFFs.')
                for raster in sorted(rasters):
                    name_hash = fingerprint([scene.identifier, index, str(raster.relative_to(extracted))])
                    relative = f'derived/{settings_hash}/{name_hash}.tif'
                    if self.grid:
                        aligned = workspace / (name_hash + '.tif')
                        align_raster(raster, aligned, self.grid, self.config.get('normalization'), self.check_cancel)
                    else:
                        aligned = raster
                    artifact = store.publish(aligned, relative, self.request.overwrite)
                    artifact.update(source_scene=scene.identifier, job_id=job['job_id'],
                                    processing=self.config, grid='reference-grid.json' if self.grid else None)
                    artifacts.append(artifact)
                    tile_settings = self.config.get('tiling', {})
                    if tile_settings.get('enabled'):
                        tile_dir = workspace / ('tiles-' + name_hash)
                        tiles = TileRaster(safe_path(store.root, relative), tile_dir,
                                           **{k: v for k, v in tile_settings.items() if k != 'enabled'},
                                           classes=self.config.get('classes'), source_fingerprint=artifact['sha256'],
                                           check_cancel=self.check_cancel).execute()
                        for tile in tiles:
                            path = f'tiles/{settings_hash}/{name_hash}/' + tile['path']
                            record = store.publish(tile_dir / tile['path'], path, self.request.overwrite)
                            artifacts.append({**tile, **record, 'source': relative})
        if not artifacts:
            raise UserError('HyP3 succeeded without downloadable artifacts.')
        return artifacts
