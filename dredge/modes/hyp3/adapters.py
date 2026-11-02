"""Optional HyP3 boundary: sanitized failures, no implicit job submission."""
import time
from urllib.parse import urlsplit

from ...core.exceptions import UserError
from ...core.imagery import AuthenticationError


class HyP3Service:
    def __init__(self, client, sleep=time.sleep, check_cancel=lambda: None):
        self.client, self.sleep, self.check_cancel = client, sleep, check_cancel

    def _call(self, operation, retry=True):
        from requests.exceptions import RequestException
        from hyp3_sdk.exceptions import HyP3SDKError, AuthenticationError as SDKAuthenticationError
        for attempt in range(3 if retry else 1):
            self.check_cancel()
            try:
                return operation()
            except SDKAuthenticationError as exc:
                raise AuthenticationError('HyP3 authentication failed; check service access.') from exc
            except (RequestException, HyP3SDKError) as exc:
                response = getattr(exc, 'response', None)
                if response is None:
                    response = getattr(exc.__context__, 'response', None)
                status = getattr(response, 'status_code', None)
                if status in (401, 403):
                    raise AuthenticationError('HyP3 authentication failed; check Earthdata credentials and service access.') from exc
                if not retry or attempt == 2 or (status and status < 500 and status not in (408, 429)):
                    raise UserError('HyP3 request failed; inspect saved job state before resubmitting.') from exc
                self.sleep(2 ** attempt)

    def find(self, name):
        return [job.to_dict() for job in self._call(lambda: self.client.find_jobs(name=name, job_type='RTC_GAMMA'))]

    def submit(self, scene, name, options):
        if scene.acquisition_mode != 'IW' or scene.product_type not in ('GRD_HD', 'GRD_HS'):
            raise UserError('HyP3 RTC requires supported IW GRD-H scenes; refine the catalog selection.')
        batch = self._call(lambda: self.client.submit_rtc_job(
            granule=scene.granule, name=name, resolution=options.get('resolution', 30),
            radiometry=options.get('radiometry', 'gamma0'), scale='power'), retry=False)
        jobs = list(batch)
        if len(jobs) != 1:
            raise UserError('HyP3 returned an unexpected submission; reconcile saved intent before retrying.')
        return jobs[0].to_dict()

    def get(self, identifier):
        return self._call(lambda: self.client.get_job_by_id(identifier)).to_dict()

    def download(self, file, destination):
        import requests
        if urlsplit(file['url']).scheme != 'https':
            raise UserError('HyP3 product URL must use HTTPS.')
        def transfer():
            with requests.get(file['url'], stream=True, timeout=(10, 60)) as response:
                response.raise_for_status()
                with destination.open('wb') as output:
                    for chunk in response.iter_content(1024 * 1024):
                        self.check_cancel()
                        output.write(chunk)
        self._call(transfer)
        if not destination.is_file() or destination.stat().st_size == 0:
            raise UserError('HyP3 returned an empty product.')
        if file.get('size') is not None and destination.stat().st_size != int(file['size']):
            raise UserError('HyP3 download size mismatch.')
