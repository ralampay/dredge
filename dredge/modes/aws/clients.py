"""Lazy service-client lifecycle with bounded SDK retries."""
from contextlib import ExitStack, contextmanager

from ...core.exceptions import UserError


@contextmanager
def open_clients(settings, *services):
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError

    with ExitStack() as stack:
        clients = {}
        try:
            session = boto3.Session(profile_name=settings.profile, region_name=settings.region)
            for service in services:
                client = session.client(service, config=Config(connect_timeout=10, read_timeout=30,
                    retries={'mode': 'standard', 'total_max_attempts': 3}))
                stack.callback(client.close)
                clients[service] = client
        except (BotoCoreError, ClientError) as exc:
            raise UserError('Cannot initialize AWS access. Check profile, region, and credentials.') from exc
        yield clients
