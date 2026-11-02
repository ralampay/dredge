"""Locked resumable imagery storage with atomic files and manifests."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tempfile
import zipfile

from ....core.exceptions import UserError


class PublicationError(UserError):
    """Fatal managed-output integrity failure."""


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def checksum(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def atomic_json(path, document):
    path = Path(path)
    if path.is_symlink():
        raise PublicationError('Refusing to replace a symlink.')
    descriptor, temporary = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            json.dump(document, stream, indent=2, allow_nan=False)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def safe_path(root, relative):
    if not isinstance(relative, str) or ':' in relative or '\x00' in relative:
        raise PublicationError('Invalid managed artifact path.')
    parts = PurePosixPath(relative)
    if not relative or parts.is_absolute() or '..' in parts.parts or '\\' in relative:
        raise PublicationError('Artifact path escapes the managed output.')
    path = root
    for part in parts.parts:
        path = path / part
        if path.is_symlink():
            raise PublicationError('Managed artifacts must not be symlinks.')
    return path


class ManagedOutput:
    def __init__(self, output, bbox):
        self.root = Path(output).absolute()
        self.bbox = list(bbox)
        self.document = None

    @contextmanager
    def open(self):
        if self.root.is_symlink() or not self.root.parent.is_dir():
            raise PublicationError('Output requires an existing parent directory and must not be a symlink.')
        lock = self.root.parent / ('.' + self.root.name + '.imagery.lock')
        try:
            handle = lock.open('x')
        except FileExistsError as exc:
            raise PublicationError('Another imagery process owns this destination; inspect stale locks after an interrupted process.') from exc
        try:
            handle.close()
            if self.root.exists():
                if not self.root.is_dir():
                    raise PublicationError('Imagery output must be a managed directory.')
                try:
                    path = safe_path(self.root, 'manifest.json')
                    self.document = json.loads(path.read_text(encoding='utf-8'))
                except (OSError, ValueError) as exc:
                    raise PublicationError('Existing output has no valid imagery manifest; choose a new destination.') from exc
                if not isinstance(self.document, dict) or self.document.get('imagery_version') != 1 or self.document.get('bbox') != self.bbox:
                    raise PublicationError('Output belongs to a different area or manifest format; choose a new destination.')
                if (not isinstance(self.document.get('scenes'), dict) or
                        any(not isinstance(v, dict) for v in self.document['scenes'].values())):
                    raise PublicationError('Invalid imagery scene inventory.')
            else:
                self.root.mkdir()
                self.document = {'imagery_version': 1, 'bbox': self.bbox, 'status': 'incomplete', 'scenes': {}}
                self.save()
            yield self
        finally:
            lock.unlink(missing_ok=True)

    def save(self):
        atomic_json(safe_path(self.root, 'manifest.json'), self.document)

    def verified(self, artifact):
        if (not isinstance(artifact, dict) or not isinstance(artifact.get('path'), str) or
                not isinstance(artifact.get('bytes'), int) or not isinstance(artifact.get('sha256'), str)):
            raise PublicationError('Invalid managed artifact inventory.')
        path = safe_path(self.root, artifact['path'])
        return path.is_file() and path.stat().st_size == artifact['bytes'] and checksum(path) == artifact['sha256']

    @contextmanager
    def workspace(self):
        with tempfile.TemporaryDirectory(prefix='.imagery-stage-', dir=self.root) as directory:
            yield Path(directory)

    def publish(self, source, relative, overwrite=False):
        target = safe_path(self.root, relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        digest = checksum(source)
        if target.exists():
            if not target.is_file():
                raise PublicationError('Managed artifact destination is not a regular file.')
            if checksum(target) == digest:
                return {'path': relative, 'sha256': digest, 'bytes': target.stat().st_size}
            if not overwrite:
                raise PublicationError('Managed artifact conflicts; use --overwrite to replace it.')
        os.replace(source, target)
        return {'path': relative, 'sha256': digest, 'bytes': target.stat().st_size}


def extract_archive(archive, destination, max_bytes=50 * 1024**3):
    """Validate the complete ZIP inventory before writing any member."""
    try:
        with zipfile.ZipFile(archive) as zipped:
            members = zipped.infolist()
            seen = set()
            total = 0
            for member in members:
                path = safe_path(destination, member.filename)
                mode = member.external_attr >> 16
                if stat.S_ISLNK(mode) or (stat.S_IFMT(mode) and not (stat.S_ISREG(mode) or stat.S_ISDIR(mode))):
                    raise UserError('Archive contains a link or special file.')
                if path in seen:
                    raise UserError('Archive contains duplicate paths.')
                seen.add(path)
                total += member.file_size
                if total > max_bytes:
                    raise UserError('Archive exceeds configured extraction limit.')
            for member in members:
                path = safe_path(destination, member.filename)
                if member.is_dir():
                    path.mkdir(parents=True, exist_ok=True)
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with zipped.open(member) as source, path.open('xb') as target:
                        shutil.copyfileobj(source, target, 1024 * 1024)
            return [safe_path(destination, m.filename) for m in members if not m.is_dir()]
    except (zipfile.BadZipFile, OSError) as exc:
        raise UserError('Cannot safely extract the product archive.') from exc
