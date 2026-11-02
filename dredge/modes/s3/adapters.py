"""S3 access; clients are constructed by the caller, never at import time."""
from botocore.exceptions import (
    BotoCoreError, ClientError, CredentialRetrievalError, NoCredentialsError,
    PartialCredentialsError, TokenRetrievalError, UnauthorizedSSOTokenError, SSOTokenLoadError,
)

from ...core.exceptions import UserError
from ...core.sources import ObjectInfo, SourceAccessError


class S3Source:
    def __init__(self, client, bucket: str):
        self.client = client
        self.bucket = bucket

    def objects(self, prefix: str):
        try:
            pages = self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=prefix
            )
            for page in pages:
                for item in page.get("Contents", []):
                    yield ObjectInfo(item["Key"], item["Size"], item.get("ETag", ""),
                                     str(item.get("LastModified", "")))
        except (BotoCoreError, ClientError) as exc:
            raise UserError("Cannot list S3 objects. Check the bucket, region, credentials, and s3:ListBucket permission.") from exc

    def read_range(self, obj: ObjectInfo, start: int, end: int) -> bytes:
        if end < start:
            return b""
        args = dict(Bucket=self.bucket, Key=obj.key, Range=f"bytes={start}-{end}")
        if obj.etag:
            args["IfMatch"] = obj.etag
        try:
            response = self.client.get_object(**args)
            body = response["Body"]
            try:
                data = body.read(end - start + 1)
            finally:
                body.close()
            if len(data) != end - start + 1:
                raise UserError(f"Incomplete read of {obj.key!r}. Retry the crawl.")
            return data
        except (NoCredentialsError, PartialCredentialsError, CredentialRetrievalError,
                TokenRetrievalError, UnauthorizedSSOTokenError, SSOTokenLoadError) as exc:
            raise SourceAccessError("AWS credentials are unavailable or expired. Refresh your AWS session and retry.") from exc
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {
                "ExpiredToken", "InvalidToken", "InvalidAccessKeyId", "SignatureDoesNotMatch",
                "TokenRefreshRequired", "InvalidClientTokenId", "RequestExpired"
            }:
                raise SourceAccessError("AWS credentials were rejected. Refresh your AWS session and retry.") from exc
            raise UserError(f"Cannot read {obj.key!r}. Check read/KMS permissions, storage class, and whether the object changed.") from exc
        except (BotoCoreError, OSError) as exc:
            raise UserError(f"Cannot read {obj.key!r}. Check read/KMS permissions, storage class, and whether the object changed.") from exc
