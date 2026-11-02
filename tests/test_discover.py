import gzip
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import boto3
from botocore.response import StreamingBody
from botocore.stub import Stubber
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from dredge.core.exceptions import UserError
from dredge.modes.s3.adapters import S3Source
from dredge.modules.discover.commands import CrawlSchemas
from dredge.modules.discover.grouping import group_key
from dredge.modules.discover.requests import CrawlOptions
from dredge.core.sources import ObjectInfo, SourceAccessError
from dredge.modules.discover.serialization import write_report
from dredge.cli import main


class MemorySource:
    def __init__(self, data):
        self.data = data
        self.reads = []

    def objects(self, prefix):
        for key, value in self.data.items():
            if key.startswith(prefix):
                yield ObjectInfo(key, len(value), '"etag"')

    def read_range(self, obj, start, end):
        self.reads.append((obj.key, start, end))
        return self.data[obj.key][start:end + 1]


def crawl(data, **options):
    return CrawlSchemas(MemorySource(data), CrawlOptions(bucket="test", **options)).execute()


def fields(report):
    return {f["name"]: f for f in report.document["datasets"][0]["schema"]["fields"]}


class DiscoveryTests(unittest.TestCase):
    def test_nested_json_merge_and_conflicts(self):
        report = crawl({"data/a.json": b'[{"id":1,"x":{"a":true},"items":[]},'
                        b'{"id":2.5,"x":{"a":"yes"},"items":[1],"extra":null}]'})
        schema = fields(report)
        self.assertEqual(schema["id"]["type"], "float")
        self.assertTrue(schema["extra"]["nullable"])
        self.assertEqual(schema["items"]["items"]["type"], "integer")
        self.assertEqual(report.document["datasets"][0]["conflicts"][0]["path"], "/x/a")

    def test_formats_and_encoding(self):
        for key, data, options in [
            ("x.csv", b'id,label\n001,"a,b"\n002,hello\n', {}),
            ("x.json", b'{"id":"001","label":"a,b"}', {}),
            ("x.ndjson", b'{"id":"001","label":"a,b"}\n', {}),
            ("x.jsonl.gz", gzip.compress(b'{"id":"001","label":"a,b"}\n'), {}),
            ("x.json", '{"id":"001","label":"café"}'.encode("utf-16"), {"encoding": "utf-16"}),
        ]:
            with self.subTest(key=key, options=options):
                result = fields(crawl({key: data}, **options))
                self.assertEqual(result["id"]["type"], "string")
                self.assertEqual(result["label"]["type"], "string")

    def test_csv_headers_and_nulls(self):
        report = crawl({"x.csv": b"1;true\n2;\n"}, csv_delimiter=";", csv_header="absent")
        self.assertEqual(fields(report)["column_1"]["type"], "integer")
        self.assertTrue(fields(report)["column_2"]["nullable"])
        for data in (b"x,x\n1,2\n", b"x,y\n1\n", b'x\n"unclosed'):
            with self.subTest(data=data), self.assertRaises(UserError):
                crawl({"x.csv": data})

    def test_date_and_hive_grouping(self):
        data = {"events/2026/11/02/a.json": b'{"a":1}',
                "events/2026/11/03/b.json": b'{"b":2}',
                "other/day=02/x.csv": b"id\n1\n"}
        result = crawl(data)
        self.assertEqual(len(result.document["datasets"]), 2)
        self.assertEqual(len(crawl(data, partition_style="none").document["datasets"]), 3)
        self.assertEqual(group_key("x/2026-11-02/a.json", "auto")[0], "x/{date}")
        self.assertEqual(group_key("x/2026-02-30/a.json", "auto")[0], "x/2026-02-30")
        self.assertEqual(group_key("x/2026/11/02/a.json", "hive")[0], "x/2026/11/02")

    def test_deterministic_sample_and_filters(self):
        data = {f"events/day={i}/a.json": b'{"x":1}' for i in range(30)}
        a = crawl(data, sample_files=3)
        b = crawl(dict(reversed(list(data.items()))), sample_files=3)
        self.assertEqual(a.document, b.document)
        self.assertEqual(len(a.document["datasets"][0]["samples"]), 3)
        self.assertTrue(a.document["datasets"][0]["sample_files_limited"])
        report = crawl({"a.json": b"{}", "b.json": b"{}", "empty.csv": b"", "dir/": b"x", "x.bin": b"x"},
                       include=("*.json",), exclude=("b*",))
        self.assertEqual(report.document["counts"]["matched"], 1)
        self.assertEqual(crawl({}).document["datasets"], [])

    def test_record_and_byte_limits(self):
        for name, data in [("x.jsonl", b'{"x":1}\n' * 1000),
                           ("x.csv", b"x\n" + b"1\n" * 1000),
                           ("x.json", b"[" + b'{"x":1},' * 999 + b'{"x":1}]')]:
            with self.subTest(name=name):
                source = MemorySource({name: data})
                result = CrawlSchemas(source, CrawlOptions(bucket="test", sample_bytes=128)).execute()
                sample = result.document["datasets"][0]["samples"][0]
                self.assertGreater(sample["records"], 0)
                self.assertIn("bytes", sample["limits"])
                self.assertLessEqual(sum(end-start+1 for _, start, end in source.reads), 128)
                result = crawl({name: data}, sample_records=2)
                sample = result.document["datasets"][0]["samples"][0]
                self.assertEqual(sample["records"], 2)
                self.assertIn("records", sample["limits"])
        result = crawl({"x.jsonl.gz": gzip.compress(b'{"x":1}\n' * 1000)}, sample_bytes=128)
        self.assertIn("bytes", result.document["datasets"][0]["samples"][0]["limits"])
        with self.assertRaisesRegex(UserError, "No complete record"):
            crawl({"x.json": b'{"long":"' + b"a" * 1000 + b'"}'}, sample_bytes=64)

    def test_malformed_and_partial(self):
        for data in (b"[1]", b"1", b'{"x":', b"{} trailing", b"[{}", b"[{},2]"):
            with self.subTest(data=data), self.assertRaises(UserError):
                crawl({"x.json": data})
        result = crawl({"a.json": b"{}", "b.json": b"bad private record"}, on_error="continue")
        self.assertTrue(result.partial)
        self.assertEqual(len(result.document["diagnostics"]), 1)
        self.assertNotIn("private record", str(result.document))

    def test_parquet_logical_types_and_ranges(self):
        schema = pa.schema([pa.field("amount", pa.decimal128(10, 2)),
                            pa.field("time", pa.timestamp("us", tz="UTC")),
                            pa.field("nested", pa.list_(pa.int32()))])
        output = io.BytesIO()
        pq.write_table(pa.Table.from_pylist([], schema=schema), output)
        report = crawl({"x.parquet": output.getvalue()})
        result = fields(report)
        self.assertEqual(result["amount"]["precision"], 10)
        self.assertEqual(result["time"]["timezone"], "UTC")
        self.assertEqual(result["nested"]["items"]["type"], "integer")
        with self.assertRaises(UserError):
            crawl({"x.parquet": output.getvalue()}, sample_bytes=8)

    def test_parquet_reads_footer_without_data_pages(self):
        output = io.BytesIO()
        pq.write_table(pa.table({"id": list(range(10000))}), output, compression=None)
        data = output.getvalue()
        source = MemorySource({"large.parquet": data})
        report = CrawlSchemas(source, CrawlOptions(bucket="test")).execute()
        footer_start = len(data) - 8 - int.from_bytes(data[-8:-4], "little")
        for _, start, end in source.reads:
            self.assertTrue(end < 4 or start >= footer_start)
        self.assertEqual(fields(report)["id"]["storage_type"], "int64")
        self.assertLess(sum(end - start + 1 for _, start, end in source.reads), len(data))

    def test_source_failures_are_never_silently_complete(self):
        source = MemorySource({"a.json": b"{}"})
        with patch.object(source, "read_range", side_effect=SourceAccessError("Refresh credentials")):
            with self.assertRaises(SourceAccessError):
                CrawlSchemas(source, CrawlOptions(bucket="test", on_error="continue")).execute()
        with patch.object(source, "objects", side_effect=UserError("Cannot list")):
            with self.assertRaises(UserError):
                CrawlSchemas(source, CrawlOptions(bucket="test", on_error="continue")).execute()
        with patch.object(source, "read_range", side_effect=UserError("Cannot read")):
            result = CrawlSchemas(source, CrawlOptions(bucket="test", on_error="continue")).execute()
            self.assertTrue(result.partial)
            self.assertEqual(result.document["counts"]["failed_objects"], 1)

    def test_yaml_publication_and_overwrite(self):
        report = crawl({"[bold].json": b'{"[bold]":1}'})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "schema.yml"
            write_report(report, path)
            self.assertEqual(yaml.safe_load(path.read_text())["schema_version"], 2)
            with self.assertRaises(UserError):
                write_report(report, path)
            write_report(report, path, overwrite=True)
            self.assertEqual(list(Path(directory).iterdir()), [path])
            with patch("yaml.safe_dump", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
                write_report(report, path, overwrite=True)
            self.assertEqual(list(Path(directory).iterdir()), [path])
            self.assertEqual(yaml.safe_load(path.read_text())["schema_version"], 2)


class AdapterTests(unittest.TestCase):
    def test_pagination_conditional_reads_and_close(self):
        client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
        stub = Stubber(client)
        stub.add_response("list_objects_v2", {"IsTruncated": True, "NextContinuationToken": "next",
                          "Contents": [{"Key": "a", "Size": 2, "ETag": '"etag"'}] +
                          [{"Key": f"a{i}", "Size": 2} for i in range(999)]}, {"Bucket": "test", "Prefix": ""})
        stub.add_response("list_objects_v2", {"IsTruncated": False, "Contents": [{"Key": "b", "Size": 3}]},
                          {"Bucket": "test", "Prefix": "", "ContinuationToken": "next"})
        stream = io.BytesIO(b"ab")
        stub.add_response("get_object", {"Body": StreamingBody(stream, 2)},
                          {"Bucket": "test", "Key": "a", "Range": "bytes=0-1", "IfMatch": '"etag"'})
        with stub:
            source = S3Source(client, "test")
            objects = list(source.objects(""))
            self.assertEqual(len(objects), 1001)
            self.assertEqual(source.read_range(objects[0], 0, 1), b"ab")
            self.assertTrue(stream.closed)
            stub.assert_no_pending_responses()
        client.close()

    def test_access_errors(self):
        client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
        with Stubber(client) as stub:
            stub.add_client_error("list_objects_v2", "AccessDenied")
            with self.assertRaisesRegex(UserError, "Cannot list"):
                list(S3Source(client, "test").objects(""))
        client.close()

    def test_read_credentials_and_changed_objects(self):
        client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
        with Stubber(client) as stub:
            stub.add_client_error("get_object", "ExpiredToken")
            stub.add_client_error("get_object", "PreconditionFailed")
            source = S3Source(client, "test")
            with self.assertRaises(SourceAccessError):
                source.read_range(ObjectInfo("x", 2), 0, 1)
            with self.assertRaisesRegex(UserError, "object changed"):
                source.read_range(ObjectInfo("x", 2), 0, 1)
        client.close()


class DiscoveryCliTests(unittest.TestCase):
    def test_dispatch_configuration_and_output(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "schema.yml"
            with patch("dredge.cli.load_environment") as load, patch("boto3.Session") as session, \
                    patch("dredge.modes.s3.adapters.S3Source", return_value=MemorySource({"x.json": b'{"x":1}'})):
                result = main(["--module", "discover", "--mode", "s3", "--action", "crawl", "--bucket", "test",
                               "--profile", "example", "--region", "us-east-1", "--output", str(path)])
                self.assertEqual(result, 0)
                load.assert_called_once()
                session.assert_called_once_with(profile_name="example", region_name="us-east-1")
                self.assertEqual(yaml.safe_load(path.read_text())["counts"]["matched"], 1)

    def test_invalid_combinations(self):
        base = ["--module", "discover", "--mode", "s3", "--action", "crawl", "--bucket", "test", "--output", "x.yml"]
        for args in (["--bucket", "test"], ["--doctor", "--module", "discover"],
                     base + ["--sample-files", "0"], base + ["--format", "json", "--csv-header", "absent"],
                     base + ["--encoding", "not-an-encoding"]):
            with self.subTest(args=args), patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit) as context:
                main(args)
            self.assertEqual(context.exception.code, 2)

    def test_partial_and_cancellation_exit_codes(self):
        base = ["--module", "discover", "--mode", "s3", "--action", "crawl", "--bucket", "test"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "partial.yml"
            with patch("dredge.cli.load_environment"), patch("boto3.Session"), \
                    patch("dredge.modes.s3.adapters.S3Source", return_value=MemorySource({"x.json": b"bad"})), \
                    patch("dredge.modules.discover.presentation.print_info"):
                self.assertEqual(main(base + ["--output", str(path), "--on-error", "continue"]), 1)
                self.assertEqual(yaml.safe_load(path.read_text())["status"], "partial")
            with patch("dredge.cli.load_environment", side_effect=KeyboardInterrupt), patch("dredge.cli.print_error"):
                self.assertEqual(main(base + ["--output", str(path)]), 130)
