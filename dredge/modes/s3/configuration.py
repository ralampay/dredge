"""Explicit S3 connection configuration."""
from dataclasses import dataclass


@dataclass(frozen=True)
class S3Settings:
    bucket: str
    profile: str | None = None
    region: str | None = None

    @classmethod
    def from_config(cls, config):
        return cls(bucket=config["bucket"], profile=config.get("profile"), region=config.get("region"))
