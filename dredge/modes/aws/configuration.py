"""Explicit SDK connection configuration for AWS services."""
from dataclasses import dataclass


@dataclass(frozen=True)
class AWSSettings:
    profile: str | None = None
    region: str | None = None

    @classmethod
    def from_config(cls, config):
        return cls(config.get('profile'), config.get('region'))
