"""Domain helpers: UTC time handling, event validation, canonical hashing.

Everything is event time based. All timestamps are timezone-aware datetimes
in UTC; naive (timezone-less) timestamps are rejected so an event can never
be grouped by the receiver's local clock.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

WATERMARK_DELAY = timedelta(seconds=30)
FINALIZE_GRACE = timedelta(seconds=60)
WINDOW_SIZE = timedelta(seconds=60)

# Stored timestamp format: UTC, microsecond precision, trailing Z, which also
# sorts correctly lexicographically.
TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class EventValidationError(ValueError):
    """Raised when an event payload cannot be turned into a valid event."""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def format_ts(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("refusing to format a naive datetime as UTC")
    return dt.astimezone(timezone.utc).strftime(TS_FORMAT)


def parse_event_time(value: Any) -> datetime:
    """Parse an event time into a timezone-aware UTC datetime.

    Accepts:
      * ISO 8601 strings, including a trailing ``Z`` or an explicit offset;
      * integers/floats interpreted as Unix epoch seconds.

    Rejects naive datetimes/strings (no zone), unparseable values and
    out-of-range epochs.
    """
    if isinstance(value, bool):
        raise EventValidationError(f"event_time must be a string or number, got bool: {value!r}")
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            raise EventValidationError(f"event_time epoch is not finite: {value!r}")
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise EventValidationError(f"event_time epoch out of range: {value!r}") from exc
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise EventValidationError("event_time datetime is naive (timezone required)")
        return value.astimezone(timezone.utc)
    if not isinstance(value, str) or not value.strip():
        raise EventValidationError(f"event_time must be a non-empty ISO 8601 string: {value!r}")
    text = value.strip()
    iso = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        parsed = datetime.fromisoformat(iso)
    except ValueError as exc:
        raise EventValidationError(f"unparseable event_time: {value!r}") from exc
    if parsed.tzinfo is None:
        raise EventValidationError(f"event_time is naive, an explicit timezone is required: {value!r}")
    return parsed.astimezone(timezone.utc)


def parse_watermark_target(value: str) -> datetime:
    """Parse the argument of an explicit watermark advance (ISO string or epoch)."""
    text = value.strip()
    try:
        return parse_event_time(float(text))
    except ValueError:
        pass
    return parse_event_time(text)


def validate_reading(value: Any) -> float:
    """Readings must be finite JSON numbers (booleans are not readings)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EventValidationError(f"reading must be a JSON number, got {type(value).__name__}: {value!r}")
    result = float(value)
    if not math.isfinite(result):
        raise EventValidationError(f"reading must be finite, got: {value!r}")
    return result


def validate_identifier(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EventValidationError(f"{field_name} must be a non-empty string: {value!r}")
    return value.strip()


def canonical_hash(payload: Any) -> str:
    """Deterministic hash of an event payload for duplicate detection.

    JSON dumps with sorted keys make key reordering irrelevant; whitespace
    differences in the raw JSONL line therefore do not defeat deduplication.
    """
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def minute_floor(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def window_end_for(window_minute: datetime) -> datetime:
    return window_minute + WINDOW_SIZE


@dataclass
class ParsedEvent:
    event_id: str
    device_id: str
    event_time: datetime
    reading: float
    raw_payload: Any
    content_hash: str = ""

    def __post_init__(self) -> None:
        self.event_id = validate_identifier(self.event_id, "event_id")
        self.device_id = validate_identifier(self.device_id, "device_id")
        self.event_time = parse_event_time(self.event_time)
        self.reading = validate_reading(self.reading)
        if not self.content_hash:
            self.content_hash = canonical_hash(self.raw_payload)

    @property
    def window_minute(self) -> datetime:
        return minute_floor(self.event_time)


@dataclass
class ProcessResult:
    """Outcome of feeding one JSONL line to the processor."""

    status: str  # accepted | duplicate | conflict | late | invalid
    reason: str = ""
    event_id: str | None = None
    window_minute: str | None = None
    finalized_windows: list[tuple[str, str]] = field(default_factory=list)
    watermark_before: str | None = None
    watermark_after: str | None = None
    window_version: int | None = None


def parse_event(payload: Any) -> ParsedEvent:
    """Validate a decoded JSON object into a :class:`ParsedEvent`."""
    if not isinstance(payload, dict):
        raise EventValidationError("event payload must be a JSON object")
    missing = [name for name in ("event_id", "device_id", "event_time", "reading") if name not in payload]
    if missing:
        raise EventValidationError(f"missing required field(s): {', '.join(missing)}")
    return ParsedEvent(
        event_id=payload["event_id"],
        device_id=payload["device_id"],
        event_time=payload["event_time"],
        reading=payload["reading"],
        raw_payload=payload,
    )
