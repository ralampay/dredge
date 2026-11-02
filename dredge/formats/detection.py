"""Version-1 key-based format classification, verified by content parsing."""
from pathlib import PurePosixPath
import re


EXTENSIONS = {".csv": "csv", ".json": "json", ".jsonl": "jsonl",
              ".ndjson": "jsonl", ".parquet": "parquet"}


def classify(key: str, override: str):
    compressed = key.lower().endswith(".gz")
    name = key[:-3] if compressed else key
    fmt = EXTENSIONS.get(PurePosixPath(name.lower()).suffix) if override == "auto" else override
    if compressed and fmt == "parquet":
        return None, None
    if fmt is None and re.search(r"\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}-[A-Fa-f0-9]{16}$", name):
        fmt = "s3-access-log"
    return fmt, "gzip" if compressed else "none"


