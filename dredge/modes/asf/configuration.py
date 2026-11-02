"""Earthdata settings resolved only at composition time."""
from dataclasses import dataclass, field
import os


@dataclass(frozen=True)
class EarthdataSettings:
    username: str = field(default='', repr=False)
    password: str = field(default='', repr=False)

    @classmethod
    def from_environment(cls):
        return cls(os.environ.get('EARTHDATA_USERNAME', ''), os.environ.get('EARTHDATA_PASSWORD', ''))
