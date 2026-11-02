"""ASF search/download integration; no raster processing or workflow imports."""
from datetime import datetime, timezone
import time
from urllib.parse import urlsplit

from ...core.exceptions import UserError
from ...core.imagery import AuthenticationError, Scene


def retry(operation, sleep=time.sleep, check_cancel=lambda: None):
    from requests.exceptions import RequestException
    from asf_search.exceptions import ASFSearchError, ASFDownloadError
    for attempt in range(3):
        check_cancel()
        try:
            return operation()
        except (RequestException, ASFSearchError, ASFDownloadError) as exc:
            response = getattr(exc, 'response', None)
            status = getattr(response, 'status_code', None)
            if status in (401, 403):
                raise AuthenticationError('Earthdata authentication failed; check credentials and account authorization.') from exc
            if attempt == 2 or (status is not None and status < 500 and status not in (408, 429)):
                raise UserError('ASF request failed; check connectivity and service availability.') from exc
            sleep(2 ** attempt)


def map_scene(product):
    p = product.properties
    try:
        identifier = str(p['fileID'])
        acquired = datetime.fromisoformat(p['startTime'].replace('Z', '+00:00'))
        if acquired.tzinfo is None:
            raise ValueError
        size = p.get('bytes')
        size = int(size) if size is not None else None
        if size is not None and size < 0:
            raise ValueError
        url = p['url']
        if urlsplit(url).scheme != 'https':
            raise ValueError
        return Scene(identifier, str(p.get('sceneName') or identifier), str(p.get('platform', 'unknown')),
                     str(p.get('processingLevel', 'unknown')), acquired.astimezone(timezone.utc).isoformat(),
                     str(p.get('polarization', 'unknown')),
                     {k: p.get(k) for k in ('orbit', 'pathNumber', 'flightDirection')},
                     product.geometry, url, size, str(p.get('beamModeType', 'unknown')))
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise UserError('ASF returned invalid scene metadata.') from exc


class AsfCatalog:
    def __init__(self, sdk, sleep=time.sleep, check_cancel=lambda: None):
        self.sdk, self.sleep, self.check_cancel = sdk, sleep, check_cancel

    def search(self, query):
        scenes = {}
        for w, s, e, n in query.boxes():
            options = dict(platform=self.sdk.PLATFORM.SENTINEL1,
                           processingLevel=['GRD_HD', 'GRD_HS', 'GRD_MD', 'GRD_MS', 'GRD_FD'],
                           intersectsWith=f'POLYGON(({w} {s},{e} {s},{e} {n},{w} {n},{w} {s}))',
                           start=query.start, end=query.end)
            for key, value in (('polarization', query.polarization), ('flightDirection', query.flight_direction),
                               ('relativeOrbit', query.relative_orbit), ('beamMode', query.acquisition_mode)):
                if value is not None:
                    options[key] = value
            expected = retry(lambda: self.sdk.search_count(**options), self.sleep, self.check_cancel)
            def collect():
                matched = {}
                complete = True
                for page in self.sdk.search_generator(**options):
                    self.check_cancel()
                    complete = getattr(page, 'searchComplete', True)
                    for product in page:
                        scene = map_scene(product)
                        matched[scene.identifier] = scene
                if not complete or len(matched) != expected:
                    raise UserError('ASF catalog changed or returned incomplete results; repeat the search for a complete preview.')
                return matched
            matched = retry(collect, self.sleep, self.check_cancel)
            scenes.update({key: scene for key, scene in matched.items() if query.start <= scene.acquired <= query.end})
        return sorted(scenes.values(), key=lambda scene: (scene.acquired, scene.identifier))


class AsfDownloader:
    def __init__(self, sdk, settings, sleep=time.sleep, check_cancel=lambda: None):
        self.sdk, self.settings, self.sleep, self.check_cancel = sdk, settings, sleep, check_cancel
        self.session = None

    def authenticate(self):
        if self.session is None:
            if not self.settings.username or not self.settings.password:
                raise AuthenticationError('Downloads require EARTHDATA_USERNAME and EARTHDATA_PASSWORD.')
            from asf_search.exceptions import ASFAuthenticationError
            from requests.exceptions import RequestException
            self.session = self.sdk.ASFSession()
            try:
                self.session.auth_with_creds(self.settings.username, self.settings.password)
            except (ASFAuthenticationError, RequestException) as exc:
                self.close()
                raise AuthenticationError('Earthdata authentication failed; check credentials and account authorization.') from exc

    def download(self, scene, destination):
        self.authenticate()
        def transfer():
            destination.unlink(missing_ok=True)
            with self.session.get(scene.url, stream=True, timeout=(10, 60)) as response:
                response.raise_for_status()
                with destination.open('wb') as output:
                    for chunk in response.iter_content(1024 * 1024):
                        self.check_cancel()
                        output.write(chunk)
        retry(transfer, self.sleep, self.check_cancel)
        if not destination.is_file() or destination.stat().st_size == 0:
            raise UserError('ASF download produced an empty or missing artifact.')
        if scene.size_bytes is not None and destination.stat().st_size != scene.size_bytes:
            raise UserError('ASF download size does not match catalog metadata; retry the download.')

    def close(self):
        if self.session is not None:
            self.session.close()
            self.session = None
