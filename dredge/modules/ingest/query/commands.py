"""Execute one query, stream its CSV result, and publish a dataset atomically."""
import time
import uuid

from ....core.exceptions import UserAbort, UserError
from ..models import IngestResult
from ..publication import staged_output, validate_output
from .models import QueryProgress, QueryRepository
from .results import ExportQueryResults
from .serialization import write_artifacts


class ExportAthenaQuery:
    def __init__(self, repository: QueryRepository, request, check_cancel=None, on_progress=None,
                 clock=time.monotonic, sleep=time.sleep):
        self.repository, self.request = repository, request
        self.check_cancel = check_cancel or (lambda: None)
        self.on_progress, self.clock, self.sleep = on_progress, clock, sleep
        self.query_id, self.terminal, self.started = None, False, None
        self.scanned = 0

    def _emit(self, state, detail='', written=0):
        if self.on_progress:
            self.on_progress(QueryProgress(state, self.query_id,
                self.clock() - self.started if self.started is not None else 0, self.scanned, written, detail))

    def execute(self):
        self.request.validate()
        validate_output(self.request.output)
        self.check_cancel()
        configuration = self.repository.prepare(self.request.workgroup, self.request.results_location)
        self.query_id, self.terminal, self.scanned = None, False, 0
        self.started = self.clock()
        try:
            with staged_output(self.request.output, check_cancel=self.check_cancel) as stage:
                try:
                    self.check_cancel()
                    self._emit('SUBMITTING')
                    try:
                        self.query_id = self.repository.start(self.request.sql, self.request.catalog,
                            self.request.database, self.request.workgroup, configuration, uuid.uuid4().hex)
                    except (KeyboardInterrupt, UserAbort, UserError):
                        self._emit('SUBMISSION_UNCONFIRMED', 'No query ID received. If submission reached Athena, '
                                   'it may still be running; check the workgroup query history.')
                        raise
                    self._emit('SUBMITTED')
                    execution = self._wait()
                    self._emit('EXPORTING')
                    exported = ExportQueryResults(self.repository, execution, stage / 'data.csv', self.check_cancel,
                                                   lambda count: self._emit('EXPORTING', written=count)).execute()
                    self.check_cancel()
                    document = write_artifacts(stage, self.request, execution, exported)
                finally:
                    if self.query_id and not self.terminal:
                        self._cancel_remote()
        except OSError as exc:
            raise UserError('Cannot write the Athena export; check local disk space and permissions.') from exc
        return IngestResult(document, self.request.output)

    def _wait(self):
        while True:
            self.check_cancel()
            if self.clock() - self.started >= self.request.timeout:
                raise UserError(f'Athena query {self.query_id} exceeded the {self.request.timeout}-second wait timeout.')
            execution = self.repository.execution(self.query_id)
            state = execution['state']
            self.scanned = execution['statistics'].get('data_scanned_bytes', 0)
            self._emit(state)
            if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
                self.terminal = True
                if state != 'SUCCEEDED':
                    raise UserError(f"Athena query {self.query_id} {state.lower()}: {execution.get('reason') or 'no reason reported'}")
                return execution
            if state not in ('QUEUED', 'RUNNING'):
                raise UserError(f'Athena query {self.query_id} returned an unknown state: {state}')
            self.sleep(min(2, max(0, self.request.timeout - (self.clock() - self.started))))

    def _cancel_remote(self):
        # Do not call check_cancel here: cancellation cleanup must finish.
        self._emit('STOPPING', 'Requesting remote query cancellation.')
        try:
            self.repository.stop(self.query_id)
            deadline = self.clock() + 30
            while self.clock() < deadline:
                state = self.repository.execution(self.query_id)['state']
                if state in ('CANCELLED', 'FAILED', 'SUCCEEDED'):
                    self.terminal = True
                    self._emit('STOPPED', f'Remote query state confirmed: {state}.')
                    return
                self.sleep(min(2, max(0, deadline - self.clock())))
            self._emit('STOP_UNCONFIRMED', 'Cancellation requested but not confirmed; check this query ID in Athena.')
        except UserError as exc:
            self._emit('STOP_UNCONFIRMED', f'Could not confirm remote cancellation: {exc} Query may still be running.')
