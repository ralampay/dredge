"""Local staged publication; completed runs are never overwritten."""
from contextlib import ExitStack, contextmanager
from pathlib import Path
import shutil
import tempfile

from ...core.exceptions import UserError


def validate_output(output):
    path = Path(output).absolute()
    if path.exists() or path.is_symlink():
        raise UserError('Ingestion output already exists; choose a new output directory.')
    if not path.parent.is_dir():
        raise UserError('Output parent directory must already exist.')
    return path


@contextmanager
def staged_output(output, check_cancel=None):
    path = validate_output(output)
    # Exclusive sibling lock serializes dredge publishers for this destination.
    lock = path.parent / ('.' + path.name + '.ingest.lock')
    try:
        handle = lock.open('x')
    except FileExistsError as exc:
        raise UserError('Another ingestion owns this output destination.') from exc
    with ExitStack() as cleanup:
        # ExitStack still releases the lock if removing staging fails.
        cleanup.callback(lock.unlink)
        handle.close()
        validate_output(path)
        stage = Path(tempfile.mkdtemp(prefix='.' + path.name + '.ingest-', dir=path.parent))
        with ExitStack() as staging_cleanup:
            staging_cleanup.callback(shutil.rmtree, stage)
            yield stage
            if check_cancel:
                check_cancel()
            validate_output(path)
            stage.rename(path)
            staging_cleanup.pop_all()
