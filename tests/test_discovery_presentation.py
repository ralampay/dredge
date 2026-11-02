"""Progress accounting and literal, compact terminal output."""
from contextlib import redirect_stderr
import io
import unittest

from rich.console import Console

from dredge.cli import build_parser
from dredge.core.exceptions import UserError
from dredge.modules.discover.commands import CrawlSchemas
from dredge.modules.discover.models import DiscoveryReport
from dredge.modules.discover.presentation import RichDiscoveryProgress, present
from dredge.modules.discover.progress import DiscoveryProgress
from dredge.modules.discover.requests import CrawlOptions
from test_discover import MemorySource, crawl


class DiscoveryProgressTests(unittest.TestCase):
    def test_events_count_selected_work_and_read_bytes(self):
        source = MemorySource({f'file-{i}.json': b'{"x":1}' for i in range(15)})
        events = []
        report = CrawlSchemas(source, CrawlOptions(bucket='test', sample_files=3), on_progress=events.append).execute()
        self.assertEqual(events[0].phase, 'listing')
        self.assertEqual(events[-1].phase, 'writing')
        self.assertEqual(events[-1].total_files, 3)
        self.assertEqual(events[-1].completed_files, 3)
        self.assertEqual(events[-1].listed, 15)
        self.assertEqual(events[-1].matched, 15)
        self.assertEqual(events[-1].bytes_read, sum(end - start + 1 for _, start, end in source.reads))
        self.assertTrue(any(event.current_key for event in events))
        self.assertEqual(report.document, crawl(source.data, sample_files=3).document)
        self.assertEqual([event.completed_files for event in events], sorted(event.completed_files for event in events))

    def test_listing_heartbeats_and_empty_bucket(self):
        events = []
        data = {f'{i}.bin': b'x' for i in range(2001)}
        CrawlSchemas(MemorySource(data), CrawlOptions(bucket='test'), on_progress=events.append).execute()
        self.assertEqual([e.listed for e in events if e.phase == 'listing'], [0, 1000, 2000])
        self.assertEqual(events[-1].total_files, 0)
        self.assertEqual(events[-1].listed, 2001)
        empty = []
        CrawlSchemas(MemorySource({}), CrawlOptions(bucket='test'), on_progress=empty.append).execute()
        self.assertEqual(empty[-1].completed_files, 0)

    def test_failure_and_continue_accounting(self):
        events = []
        data = {'bad.json': b'bad', 'good.json': b'{"x":1}'}
        report = CrawlSchemas(MemorySource(data), CrawlOptions(bucket='test', on_error='continue'), events.append).execute()
        self.assertTrue(report.partial)
        self.assertEqual(events[-1].completed_files, 2)
        self.assertEqual(events[-1].failed_files, 1)
        failed = []
        with self.assertRaises(UserError):
            CrawlSchemas(MemorySource({'bad.json': b'bad'}), CrawlOptions(bucket='test'), failed.append).execute()
        self.assertEqual(failed[-1].phase, 'failed')
        self.assertNotIn('writing', [e.phase for e in failed])

    def test_redirected_output_has_phase_lines_without_escape_codes(self):
        output = io.StringIO()
        with RichDiscoveryProgress(Console(file=output, force_terminal=False)) as reporter:
            reporter(DiscoveryProgress())
            reporter(DiscoveryProgress(listed=1000))
            reporter(DiscoveryProgress(phase='inspecting', listed=2000, matched=2000, datasets=2, total_files=20))
            reporter(DiscoveryProgress(phase='inspecting', total_files=20, completed_files=1, bytes_read=1024))
            reporter(DiscoveryProgress(phase='writing', total_files=20, completed_files=20))
        text = output.getvalue()
        self.assertEqual(text.count('Listing S3 objects:'), 1)
        self.assertIn('total unknown', text)
        self.assertIn('20 left', text)
        self.assertNotIn('\x1b', text)

    def test_terminal_render_and_exception_cleanup(self):
        output = io.StringIO()
        reporter = RichDiscoveryProgress(Console(file=output, force_terminal=True, width=120))
        with self.assertRaises(KeyboardInterrupt):
            with reporter:
                reporter(DiscoveryProgress())
                reporter(DiscoveryProgress(phase='inspecting', total_files=20, completed_files=3, bytes_read=1024))
                raise KeyboardInterrupt()
        self.assertFalse(reporter.display.live.is_started)
        self.assertIn('stopped before successful completion', output.getvalue())


class CompactReportTests(unittest.TestCase):
    def test_blockers_are_shown_before_inferred_temporal_fields(self):
        report = crawl({'a.json': b'{"date":"2026-11-02","normal":1,"attachments":[]}'})
        output = io.StringIO()
        present(report, 'schema.yml', field_limit=1, console=Console(file=output, width=120, color_system=None))
        text = output.getvalue()
        self.assertIn('attachments', text)
        self.assertIn('list<unknown>', text)
        self.assertIn('BLOCKED', text)
        self.assertNotIn('Inferred ISO date', text)
        self.assertIn('Showing 1 of 3', text)
        self.assertIn('--report-fields all', text)

    def test_all_fields_and_literal_user_content(self):
        report = crawl({'[bold].json': b'{"[red]name[/red]":1,"second":true}'})
        output = io.StringIO()
        present(report, '[bold]schema.yml', field_limit='all', console=Console(file=output, width=100, color_system=None))
        text = output.getvalue()
        self.assertIn('[red]name[/red]', text)
        self.assertIn('[bold]schema.yml', text)
        self.assertIn('second', text)
        self.assertIn('integer', text)
        self.assertNotIn('Showing ', text)

    def test_report_fields_cli_validation(self):
        parser = build_parser()
        self.assertEqual(parser.parse_args(['--report-fields', 'all']).report_fields, 'all')
        self.assertEqual(parser.parse_args(['--report-fields', '5']).report_fields, 5)
        for value in ('0', '-1', 'bad'):
            with self.subTest(value=value), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                parser.parse_args(['--report-fields', value])
            self.assertEqual(error.exception.code, 2)
