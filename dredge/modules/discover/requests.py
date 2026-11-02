"""Typed discovery requests, independent of CLI and provider libraries."""
from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any


@dataclass(frozen=True)
class CrawlOptions:
    bucket: str
    prefix: str = ""
    region: str | None = None
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    format: str = "auto"
    partition_style: str = "auto"
    sample_files: int = 10
    sample_records: int = 1000
    sample_bytes: int = 8 * 1024 * 1024
    csv_delimiter: str = ","
    csv_header: str = "present"
    encoding: str = "utf-8-sig"
    on_error: str = "fail"



@dataclass(frozen=True)
class CrawlRequest:
    options: CrawlOptions
    output: str
    overwrite: bool = False

    @classmethod
    def from_config(cls, config: Mapping[str, Any]):
        values = {field.name: config[field.name] for field in fields(CrawlOptions)
                  if config.get(field.name) is not None}
        for name in ("include", "exclude"):
            if name in values:
                values[name] = tuple(values[name])
        return cls(CrawlOptions(**values), config["output"], config.get("overwrite", False))
