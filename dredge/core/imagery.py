"""Provider-neutral catalog inputs and scene metadata (no SDK imports)."""
from dataclasses import dataclass
from datetime import datetime, time, timezone
import math

from .exceptions import UserError


def parse_bbox(value):
    try:
        bounds = tuple(float(v) for v in (value.split(',') if isinstance(value, str) else value))
        if len(bounds) != 4 or not all(math.isfinite(v) for v in bounds):
            raise ValueError
        west, south, east, north = bounds
        if not (-180 <= west <= 180 and -180 <= east <= 180 and -90 <= south < north <= 90):
            raise ValueError
        if west == east or (west == 180 and east == -180):
            raise ValueError
        return bounds
    except (TypeError, ValueError) as exc:
        raise UserError('Bounds must be west,south,east,north in WGS84 with nonzero extent.') from exc


def date_bound(value, end=False):
    try:
        if len(value) == 10:
            parsed = datetime.combine(datetime.strptime(value, '%Y-%m-%d').date(), time.max if end else time.min, timezone.utc)
        else:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                raise ValueError
        return parsed.astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError) as exc:
        raise UserError('Dates must be ISO calendar dates or timestamps with a timezone.') from exc


@dataclass(frozen=True)
class SceneQuery:
    bbox: tuple[float, float, float, float]
    start: str
    end: str
    polarization: str | None = None
    flight_direction: str | None = None
    relative_orbit: int | None = None
    acquisition_mode: str | None = None

    def __post_init__(self):
        object.__setattr__(self, 'bbox', parse_bbox(self.bbox))
        object.__setattr__(self, 'start', date_bound(self.start))
        object.__setattr__(self, 'end', date_bound(self.end, True))
        if self.start > self.end:
            raise UserError('Start date must not follow end date.')
        if self.relative_orbit is not None and not 1 <= self.relative_orbit <= 175:
            raise UserError('Sentinel-1 relative orbit must be between 1 and 175.')
        if self.flight_direction not in (None, 'ASCENDING', 'DESCENDING'):
            raise UserError('Flight direction must be ASCENDING or DESCENDING.')
        if self.polarization not in (None, 'VV', 'VH', 'HH', 'HV', 'VV+VH', 'HH+HV'):
            raise UserError('Unsupported Sentinel-1 polarization.')
        if self.acquisition_mode not in (None, 'IW', 'EW', 'SM', 'WV'):
            raise UserError('Unsupported acquisition mode.')

    @classmethod
    def from_config(cls, config):
        return cls(config['bbox'], config['start_date'], config['end_date'],
                   **{k: config.get(k) for k in ('polarization', 'flight_direction', 'relative_orbit', 'acquisition_mode')})

    def boxes(self):
        w, s, e, n = self.bbox
        if w < e:
            return (self.bbox,)
        return tuple(b for b in ((w, s, 180, n), (-180, s, e, n)) if b[0] < b[2])


@dataclass(frozen=True)
class Scene:
    identifier: str
    granule: str
    platform: str
    product_type: str
    acquired: str
    polarization: str
    orbit: dict
    footprint: dict | None
    url: str
    size_bytes: int | None
    acquisition_mode: str


class AuthenticationError(UserError):
    """Fatal authentication failure, never a skippable scene failure."""
