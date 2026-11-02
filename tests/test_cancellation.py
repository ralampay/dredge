"""Real SIGINT regression tests for Python-backed Parquet reads and cleanup."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from dredge.core.cancellation import Cancellation, handle_interrupts
from dredge.core.exceptions import UserAbort
from dredge.modules.ingest.publication import staged_output


ROOT = Path(__file__).resolve().parents[1]


class CancellationTests(unittest.TestCase):
    def test_request_is_sticky_and_notification_is_once(self):
        notifications = []
        cancellation = Cancellation(lambda: notifications.append('cancel'))
        cancellation.check()
        cancellation.request()
        cancellation.request()
        self.assertEqual(notifications, ['cancel'])
        with self.assertRaises(UserAbort):
            cancellation.check()

    def test_handlers_restore_on_success_and_failure(self):
        original = signal.getsignal(signal.SIGINT)
        for fail in (False, True):
            try:
                with handle_interrupts() as cancellation:
                    self.assertNotEqual(signal.getsignal(signal.SIGINT), original)
                    if fail:
                        raise RuntimeError('failure')
            except RuntimeError:
                pass
            self.assertEqual(signal.getsignal(signal.SIGINT), original)

    def test_worker_thread_callers_do_not_install_process_handlers(self):
        results = []
        def work():
            with handle_interrupts() as cancellation:
                results.append(cancellation.requested)
        with patch('dredge.core.cancellation.signal.signal') as install:
            worker = threading.Thread(target=work)
            worker.start()
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
            install.assert_not_called()
        self.assertEqual(results, [False])

    def test_cancel_before_publication_removes_stage_and_lock(self):
        cancellation = Cancellation()
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'output'
            with self.assertRaises(UserAbort):
                with staged_output(output, check_cancel=cancellation.check) as stage:
                    (stage / 'partial').write_text('data')
                    cancellation.request()
            self.assertEqual(list(Path(parent).iterdir()), [])


CHILD = r'''
from contextlib import contextmanager
import io
from pathlib import Path
import socket
import sys
import threading
import time
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

sys.path.insert(0, 'tests')
from test_ingestion import setup
from dredge.cli import main
from dredge.modules.ingest.writer import ParquetDatasetWriter

parent, phase = Path(sys.argv[1]), sys.argv[2]
# Large enough that footer reads cannot consume all data columns.
buffer = io.BytesIO()
pq.write_table(pa.table({'id': list(range(100)), 'payload': ['x' * 16384] * 100}),
               buffer, compression=None, use_dictionary=False)
manifest, source, datasets, selections = setup({'data/a.parquet': buffer.getvalue()})
schema = parent / 'schema.yml'
schema.write_text(yaml.safe_dump(manifest.document))
left, right = socket.socketpair()
armed = True

def wait_for_interrupt():
    global armed
    if armed:
        armed = False
        assert threading.current_thread() is threading.main_thread(), 'S3 read ran on an Arrow worker'
        (parent / 'ready').touch()
        try:
            left.recv(1)  # Real blocking socket operation, interrupted by SIGINT.
        finally:
            (parent / 'read_closed').touch()

original_read = source.read_range
original_objects = source.objects
original_append = ParquetDatasetWriter.append
original_close = ParquetDatasetWriter._close

def read(obj, start, end):
    if phase == 'read' and end < obj.size - 1 and start < obj.size - 65536:
        wait_for_interrupt()
    return original_read(obj, start, end)

def objects(prefix):
    if phase == 'listing':
        wait_for_interrupt()
    yield from original_objects(prefix)

def append(writer, record):
    original_append(writer, record)
    if phase == 'write':
        wait_for_interrupt()

def close(writer, **kwargs):
    if phase == 'write' and kwargs.get('record_success') is False:
        (parent / 'cleaning').touch()
        time.sleep(0.5)  # Parent sends another SIGINT while cleanup is active.
    original_close(writer, **kwargs)
    if phase == 'write' and kwargs.get('record_success') is False:
        (parent / 'writer_closed').touch()

@contextmanager
def open_source(settings):
    try:
        yield source
    finally:
        left.close()
        right.close()
        (parent / 'client_closed').touch()

source.read_range, source.objects = read, objects
args = ['--module', 'ingest', '--mode', 's3', '--action', 'get', '--bucket', 'test',
        '--schema', str(schema), '--prefix', 'data/', '--output', str(parent / 'output'), '--batch-rows', '1']
if phase == 'listing':
    args.append('--dry-run')
with patch('dredge.cli.load_environment', return_value=None), \
     patch('dredge.modes.s3.clients.open_source', open_source), \
     patch.object(ParquetDatasetWriter, 'append', append), \
     patch.object(ParquetDatasetWriter, '_close', close):
    status = main(args)
sys.exit(status)
'''


@unittest.skipUnless(os.name == 'posix', 'Subprocess SIGINT test requires POSIX signals')
class ProcessCancellationTests(unittest.TestCase):
    def wait_for_marker(self, process, path):
        deadline = time.monotonic() + 15
        while not path.exists():
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                self.fail(f'Process exited before {path.name}: {stdout}\n{stderr}')
            if time.monotonic() >= deadline:
                self.fail('Timed out waiting for ' + path.name)
            time.sleep(0.02)

    def test_real_sigint_listing_parquet_read_and_writer_cleanup(self):
        for phase in ('listing', 'read', 'write'):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                parent = Path(directory)
                process = subprocess.Popen([sys.executable, '-c', CHILD, directory, phase], cwd=ROOT,
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    self.wait_for_marker(process, parent / 'ready')
                    process.send_signal(signal.SIGINT)
                    if phase == 'write':
                        self.wait_for_marker(process, parent / 'cleaning')
                        process.send_signal(signal.SIGINT)
                    stdout, stderr = process.communicate(timeout=10)
                    self.assertEqual(process.returncode, 130, stdout + stderr)
                    self.assertEqual(stderr.count('Cancellation requested.'), 1, stderr)
                    self.assertIn('Cancelled.', stderr)
                    self.assertNotIn('Traceback', stderr)
                    self.assertNotIn('Ingestion complete', stdout)
                    self.assertTrue((parent / 'client_closed').exists())
                    self.assertTrue((parent / 'read_closed').exists())
                    if phase == 'write':
                        self.assertTrue((parent / 'writer_closed').exists())
                    self.assertFalse((parent / 'output').exists())
                    self.assertEqual(list(parent.glob('.output.ingest*')), [])
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate(timeout=5)
