"""Invocation-scoped SIGINT handling without import-time signal changes."""
from contextlib import contextmanager
import signal
import threading

from .exceptions import UserAbort


class Cancellation:
    """Sticky cancellation state, also usable by callers without a terminal."""
    def __init__(self, on_request=None):
        self.requested = False
        self.on_request = on_request

    def request(self):
        if not self.requested:
            self.requested = True
            if self.on_request is not None:
                self.on_request()

    def check(self):
        if self.requested:
            raise UserAbort('Cancellation requested.')

    def _interrupt(self, signum, frame):
        if self.requested:
            # Do not interrupt writer/client/staging cleanup a second time.
            return
        self.request()
        # Interrupt Python socket waits as well as regular Python work. Parquet
        # callbacks must run on this thread, not Arrow's column worker threads.
        raise KeyboardInterrupt()


@contextmanager
def handle_interrupts(on_request=None):
    cancellation = Cancellation(on_request)
    main_thread = threading.current_thread() is threading.main_thread()
    previous = signal.getsignal(signal.SIGINT) if main_thread else None
    if main_thread:
        signal.signal(signal.SIGINT, cancellation._interrupt)
    try:
        yield cancellation
    finally:
        if main_thread:
            signal.signal(signal.SIGINT, previous)
