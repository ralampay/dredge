"""Construct and close AWS resources only when a source is requested."""
from contextlib import contextmanager

from ...core.exceptions import UserError
from .configuration import S3Settings


@contextmanager
def open_source(settings: S3Settings):
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError

    from .adapters import S3Source

    try:
        session = boto3.Session(profile_name=settings.profile, region_name=settings.region)
        client = session.client("s3", config=Config(
            retries={"mode": "standard", "max_attempts": 3}, connect_timeout=10, read_timeout=60))
    except (BotoCoreError, ClientError) as exc:
        raise UserError("Cannot initialize AWS access. Check the named profile, credentials, and region.") from exc
    try:
        yield S3Source(client, settings.bucket)
    finally:
        client.close()
