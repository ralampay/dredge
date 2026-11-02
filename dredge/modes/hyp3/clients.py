"""Lazy optional HyP3 client creation and sanitized authentication errors."""
from ...core.exceptions import UserError
from ...core.imagery import AuthenticationError


def create_client(settings):
    try:
        import hyp3_sdk
    except ImportError as exc:
        raise UserError('RTC requires optional dependencies: pip install -r requirements-hyp3.txt') from exc
    from hyp3_sdk.exceptions import AuthenticationError as SDKAuthenticationError, HyP3SDKError
    from requests.exceptions import RequestException
    try:
        return hyp3_sdk.HyP3(username=settings.username, password=settings.password, prompt=False)
    except SDKAuthenticationError as exc:
        raise AuthenticationError('HyP3 authentication failed; check Earthdata credentials and service access.') from exc
    except (HyP3SDKError, RequestException) as exc:
        raise UserError('Cannot initialize HyP3; check service connectivity.') from exc
