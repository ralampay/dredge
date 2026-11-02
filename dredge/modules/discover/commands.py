"""Application workflow for bounded, reproducible schema discovery."""
from dataclasses import asdict
from fnmatch import fnmatchcase
import hashlib
import json
import codecs

from ...core.exceptions import UserError
from .grouping import classify, group_key
from ...core.sources import ObjectSource, SourceAccessError
from .requests import CrawlOptions
from .models import DiscoveryReport
from .readers import InspectObject
from .manifest import attach_contract
from .schema import conflicts, merge


class CrawlSchemas:
    def __init__(self, source: ObjectSource, options: CrawlOptions):
        self.source, self.options = source, options

    def execute(self) -> DiscoveryReport:
        self._validate()
        groups, counts = self._enumerate()
        datasets, diagnostics = [], []
        inspector = InspectObject(self.source, self.options)
        for identity, group in sorted(groups.items()):
            schema = {"type": "unknown", "nullable": False}
            samples, failures = [], []
            for _, obj, compression in sorted(group.pop("candidates")):
                try:
                    current, coverage = inspector.execute(obj, group["format"], compression)
                    schema = merge(schema, current)
                    samples.append({"key": obj.key, "etag": obj.etag, "modified": obj.modified,
                                    "compression": compression, **coverage})
                except SourceAccessError:
                    raise
                except UserError as exc:
                    if self.options.on_error == "fail":
                        raise
                    failures.append({"key": obj.key, "message": str(exc)})
            group.update(id=hashlib.sha256(identity.encode()).hexdigest(), schema=schema,
                         samples=samples, failures=failures, conflicts=conflicts(schema),
                         sample_files_limited=group["matched_objects"] > self.options.sample_files)
            attach_contract(group, self.options)
            datasets.append(group)
            diagnostics.extend(failures)
        samples = [sample for dataset in datasets for sample in dataset["samples"]]
        counts.update(sampled_objects=len(samples), failed_objects=len(diagnostics),
                      sampled_records=sum(s["records"] for s in samples),
                      sampled_bytes=sum(s["bytes"] for s in samples))
        document = {"schema_version": 2, "status": "partial" if diagnostics else "complete",
                    "source": {"mode": "s3", "bucket": self.options.bucket, "prefix": self.options.prefix, "region": self.options.region},
                    "settings": asdict(self.options), "counts": counts,
                    "diagnostics": diagnostics, "datasets": datasets}
        return DiscoveryReport(document)

    def _validate(self):
        o = self.options
        if not o.bucket or any(value <= 0 for value in (o.sample_files, o.sample_records, o.sample_bytes)):
            raise UserError("A bucket and positive sampling limits are required.")
        if o.format not in ("auto", "csv", "json", "jsonl", "parquet", "s3-access-log") or o.partition_style not in ("auto", "hive", "none"):
            raise UserError("Invalid format or partition style.")
        if o.on_error not in ("fail", "continue") or o.csv_header not in ("present", "absent"):
            raise UserError("Invalid error or CSV header policy.")
        if len(o.csv_delimiter) != 1 or o.csv_delimiter in "\r\n\0":
            raise UserError("CSV delimiter must be one non-newline character.")
        try:
            codecs.lookup(o.encoding)
        except LookupError as exc:
            raise UserError("Unknown text encoding.") from exc

    def _enumerate(self):
        groups = {}
        counts = dict(listed=0, filtered=0, markers=0, empty=0, unsupported=0, matched=0)
        for obj in self.source.objects(self.options.prefix):
            counts["listed"] += 1
            if ((self.options.include and not any(fnmatchcase(obj.key, p) for p in self.options.include))
                    or any(fnmatchcase(obj.key, p) for p in self.options.exclude)):
                counts["filtered"] += 1
                continue
            if obj.key.endswith("/"):
                counts["markers"] += 1
                continue
            if obj.size == 0:
                counts["empty"] += 1
                continue
            fmt, compression = classify(obj.key, self.options.format)
            if fmt is None:
                counts["unsupported"] += 1
                continue
            counts["matched"] += 1
            location, partitions = group_key(obj.key, self.options.partition_style)
            identity = json.dumps([location, partitions, fmt], sort_keys=True)
            if identity not in groups:
                groups[identity] = {"location_pattern": location + "/" if location else "",
                                    "partitions": partitions, "format": fmt, "matched_objects": 0,
                                    "candidates": [], "compression": []}
            group = groups[identity]
            group["matched_objects"] += 1
            if compression not in group["compression"]:
                group["compression"].append(compression)
                group["compression"].sort()
            candidates = group["candidates"]
            candidates.append((hashlib.sha256(obj.key.encode()).hexdigest(), obj, compression))
            candidates.sort(key=lambda candidate: candidate[0])
            del candidates[self.options.sample_files:]
        return groups, counts
