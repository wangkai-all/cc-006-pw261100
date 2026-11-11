"""Core event-time processing engine.

All state changes for one input line happen in a single SQLite transaction,
so progress, deduplication records and metrics can never diverge.

Event-time ordering rules implemented here:

* the watermark is ``max(max_valid_event_time - 30s, explicit_override)``;
* an event is first classified against the *current* watermark, and only
  afterwards may the watermark advance;
* a (device, minute) window finalizes once the watermark reaches
  ``window_end + 60s``;
* corrections arriving before finalization update the metric and bump its
  version; events arriving after finalization go to ``late_events`` and the
  finalized metric is never modified;
* invalid events and conflicts never move the watermark.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any

from .models import (
    FINALIZE_GRACE,
    WATERMARK_DELAY,
    EventValidationError,
    ProcessResult,
    format_ts,
    parse_event,
    parse_event_time,
    utcnow,
    window_end_for,
)
from .schema import DB_LOCK


class EventProcessor:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    # ------------------------------------------------------------------ state

    def _get_state_row(self) -> sqlite3.Row:
        row = self.conn.execute("SELECT * FROM state WHERE id = 1").fetchone()
        if row is None:  # pragma: no cover - init_db always inserts it
            self.conn.execute(
                "INSERT INTO state (id, max_event_ts, watermark_override) VALUES (1, NULL, NULL)"
            )
            row = self.conn.execute("SELECT * FROM state WHERE id = 1").fetchone()
        return row

    @staticmethod
    def _parse_stored_ts(value: str | None) -> datetime | None:
        return parse_event_time(value) if value is not None else None

    def get_state(self) -> dict[str, Any]:
        with DB_LOCK:
            row = self._get_state_row()
            wm = self.get_effective_watermark()
            return {
                "max_event_ts": row["max_event_ts"],
                "watermark_override": row["watermark_override"],
                "effective_watermark": format_ts(wm) if wm else None,
            }

    def get_effective_watermark(self) -> datetime | None:
        with DB_LOCK:
            row = self._get_state_row()
            candidates = []
            max_ts = self._parse_stored_ts(row["max_event_ts"])
            if max_ts is not None:
                candidates.append(max_ts - WATERMARK_DELAY)
            override = self._parse_stored_ts(row["watermark_override"])
            if override is not None:
                candidates.append(override)
            return max(candidates) if candidates else None

    def get_max_event_ts(self) -> datetime | None:
        with DB_LOCK:
            row = self._get_state_row()
            return self._parse_stored_ts(row["max_event_ts"])

    def _log_watermark(self, old: datetime | None, new: datetime, reason: str) -> None:
        self.conn.execute(
            "INSERT INTO watermark_log (old_watermark, new_watermark, reason, created_at) "
            "VALUES (?, ?, ?, ?)",
            (format_ts(old) if old else None, format_ts(new), reason, format_ts(utcnow())),
        )

    # ------------------------------------------------------------- entry point

    def process_line(self, raw_line: str, source_file: str | None = None,
                     line_no: int | None = None, on_commit=None) -> ProcessResult:
        """Process one raw JSON line.

        ``on_commit`` is an optional zero-argument callable run inside the
        same transaction as the resulting state change. The tailer uses it to
        persist its byte offset atomically with the line just processed, so
        progress can never diverge from the metrics on crash.
        """
        text = raw_line.strip()
        if not text:
            if on_commit is not None:
                with DB_LOCK, self.conn:
                    on_commit()
            # Blank lines are skipped quietly by the tailer; keep a guard here.
            return ProcessResult(status="invalid", reason="empty line")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            return self._record_invalid(source_file, line_no,
                                        f"malformed JSON: {exc.msg}", text, on_commit)
        try:
            event = parse_event(payload)
        except EventValidationError as exc:
            return self._record_invalid(source_file, line_no, str(exc), text, on_commit)
        return self.process_event(event, source_file=source_file, line_no=line_no,
                                  on_commit=on_commit)

    def _record_invalid(self, source_file: str | None, line_no: int | None,
                        reason: str, raw_text: str, on_commit=None) -> ProcessResult:
        with DB_LOCK, self.conn:
            self.conn.execute(
                "INSERT INTO invalid_events (source_file, line_no, reason, raw_payload, detected_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (source_file, line_no, reason, raw_text[:10000], format_ts(utcnow())),
            )
            if on_commit is not None:
                on_commit()
        return ProcessResult(status="invalid", reason=reason)

    def process_event(self, event, source_file: str | None = None,
                      line_no: int | None = None, on_commit=None) -> ProcessResult:
        """Process a validated :class:`~device_metrics.models.ParsedEvent`."""
        del source_file, line_no  # invalid-only bookkeeping, not needed here
        now = format_ts(utcnow())
        with DB_LOCK, self.conn:
            wm_before = self.get_effective_watermark()
            result = ProcessResult(
                status="accepted",
                event_id=event.event_id,
                window_minute=format_ts(event.window_minute),
                watermark_before=format_ts(wm_before) if wm_before else None,
            )

            # ---- 1. identity check across accepted AND quarantined events ---
            existing = self.conn.execute(
                "SELECT content_hash, duplicate_count FROM events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            existing_late = None
            if existing is None:
                existing_late = self.conn.execute(
                    "SELECT content_hash FROM late_events WHERE event_id = ? "
                    "ORDER BY late_id LIMIT 1", (event.event_id,),
                ).fetchone()

            if existing is not None:
                if existing["content_hash"] == event.content_hash:
                    # Identical repeat: count it, change nothing else.
                    self.conn.execute(
                        "UPDATE events SET duplicate_count = ?, last_seen = ? WHERE event_id = ?",
                        (existing["duplicate_count"] + 1, now, event.event_id),
                    )
                    result.status = "duplicate"
                    result.reason = "identical event_id and content already accepted"
                    result.watermark_after = format_ts(wm_before) if wm_before else None
                    if on_commit is not None:
                        on_commit()
                    return result
                accepted_hash = existing["content_hash"]
            elif existing_late is not None:
                if existing_late["content_hash"] == event.content_hash:
                    # Same quarantined late event delivered again: ignore,
                    # never store it twice and never touch finalized metrics.
                    result.status = "duplicate"
                    result.reason = "identical late event already quarantined"
                    result.watermark_after = format_ts(wm_before) if wm_before else None
                    if on_commit is not None:
                        on_commit()
                    return result
                accepted_hash = existing_late["content_hash"]
            else:
                accepted_hash = None

            if accepted_hash is not None:
                # Same id, different content: keep the first reading, log conflict.
                self.conn.execute(
                    "INSERT INTO conflicts (event_id, device_id, event_time, reading, "
                    "content_hash, raw_payload, accepted_hash, detected_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.event_id,
                        _safe_str(getattr(event, "device_id", None)),
                        format_ts(event.event_time),
                        event.reading,
                        event.content_hash,
                        json.dumps(event.raw_payload, ensure_ascii=False)[:10000],
                        accepted_hash,
                        now,
                    ),
                )
                result.status = "conflict"
                result.reason = "event_id already exists with different content; first reading kept"
                result.watermark_after = format_ts(wm_before) if wm_before else None
                if on_commit is not None:
                    on_commit()
                return result  # conflicts never move the watermark

            # ---- 2. classify with the *current* watermark -------------------
            window_minute = event.window_minute
            w_end = window_end_for(window_minute)
            window_row = self.conn.execute(
                "SELECT finalized FROM windows WHERE device_id = ? AND window_minute = ?",
                (event.device_id, format_ts(window_minute)),
            ).fetchone()
            already_finalized = bool(window_row and window_row["finalized"])

            if already_finalized or (wm_before is not None and event.event_time <= wm_before):
                # Arrived too late: store separately, leave finalized/aggregated
                # data untouched, do not move the watermark.
                self.conn.execute(
                    "INSERT INTO late_events (event_id, device_id, event_time, reading, "
                    "window_minute, content_hash, raw_payload, watermark_at_arrival, received_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.event_id,
                        event.device_id,
                        format_ts(event.event_time),
                        event.reading,
                        format_ts(window_minute),
                        event.content_hash,
                        json.dumps(event.raw_payload, ensure_ascii=False)[:10000],
                        format_ts(wm_before) if wm_before else None,
                        now,
                    ),
                )
                result.status = "late"
                result.reason = (
                    "window already finalized" if already_finalized
                    else "event_time at or behind the current watermark"
                )
                result.watermark_after = format_ts(wm_before) if wm_before else None
                if on_commit is not None:
                    on_commit()
                return result

            # ---- 3. accept into the per-minute window -----------------------
            version = self._upsert_window(event, window_minute, w_end, now)
            result.window_version = version

            self.conn.execute(
                "INSERT INTO events (event_id, device_id, event_time, reading, content_hash, "
                "window_minute, duplicate_count, first_seen, last_seen) "
                "VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)",
                (
                    event.event_id,
                    event.device_id,
                    format_ts(event.event_time),
                    event.reading,
                    event.content_hash,
                    format_ts(window_minute),
                    now,
                    now,
                ),
            )

            # ---- 4. only now may the watermark advance ----------------------
            wm_after = self._advance_natural_watermark(event.event_time)
            result.watermark_after = format_ts(wm_after) if wm_after else None

            # ---- 5. finalize every window whose deadline the watermark passed
            result.finalized_windows = self._finalize_due_windows(wm_after)
            if on_commit is not None:
                on_commit()
            return result

    # ------------------------------------------------------------- window work

    def _upsert_window(self, event, window_minute: datetime,
                       w_end: datetime, now: str) -> int:
        key = (event.device_id, format_ts(window_minute))
        row = self.conn.execute(
            "SELECT * FROM windows WHERE device_id = ? AND window_minute = ?", key
        ).fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO windows (device_id, window_minute, window_end, count, sum_reading, "
                "max_reading, version, finalized, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, 1, 0, ?, ?)",
                (key[0], key[1], format_ts(w_end), event.reading, event.reading, now, now),
            )
            self._insert_version(event.device_id, window_minute, 1, 1,
                                 event.reading, event.reading, "created", event.event_id, now)
            return 1

        new_count = row["count"] + 1
        new_sum = row["sum_reading"] + event.reading
        new_max = max(row["max_reading"], event.reading)
        new_version = row["version"] + 1
        self.conn.execute(
            "UPDATE windows SET count = ?, sum_reading = ?, max_reading = ?, version = ?, "
            "updated_at = ? WHERE device_id = ? AND window_minute = ?",
            (new_count, new_sum, new_max, new_version, now, key[0], key[1]),
        )
        self._insert_version(event.device_id, window_minute, new_version, new_count,
                             new_sum / new_count, new_max, "updated", event.event_id, now)
        return new_version

    def _insert_version(self, device_id: str, window_minute: datetime, version: int,
                        count: int, avg: float, mx: float, reason: str,
                        event_id: str | None, now: str) -> None:
        self.conn.execute(
            "INSERT INTO window_versions (device_id, window_minute, version, count, "
            "avg_reading, max_reading, change_reason, event_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (device_id, format_ts(window_minute), version, count, avg, mx,
             reason, event_id, now),
        )

    def _advance_natural_watermark(self, event_time: datetime) -> datetime | None:
        """Move max_event_ts (and hence the watermark) forward if this event beats it."""
        current_max = self.get_max_event_ts()
        if current_max is not None and event_time <= current_max:
            return self.get_effective_watermark()
        old_wm = self.get_effective_watermark()
        self.conn.execute("UPDATE state SET max_event_ts = ? WHERE id = 1",
                          (format_ts(event_time),))
        new_wm = self.get_effective_watermark()
        if new_wm is not None and (old_wm is None or new_wm > old_wm):
            self._log_watermark(old_wm, new_wm, "natural:max_event_ts")
        return new_wm

    def _finalize_due_windows(self, watermark: datetime | None) -> list[tuple[str, str]]:
        if watermark is None:
            return []
        due = self.conn.execute(
            "SELECT device_id, window_minute, window_end FROM windows "
            "WHERE finalized = 0 ORDER BY window_minute, device_id"
        ).fetchall()
        finalized: list[tuple[str, str]] = []
        now = format_ts(utcnow())
        for row in due:
            w_end = parse_event_time(row["window_end"])
            if watermark >= w_end + FINALIZE_GRACE:
                self.conn.execute(
                    "UPDATE windows SET finalized = 1, finalized_at = ? "
                    "WHERE device_id = ? AND window_minute = ?",
                    (now, row["device_id"], row["window_minute"]),
                )
                finalized.append((row["device_id"], row["window_minute"]))
        return finalized

    def advance_watermark(self, target: datetime) -> dict[str, Any]:
        """Explicit, monotonic watermark advance (demo / idle-stream entry point).

        The override can only move forward. Afterwards any windows whose
        deadline has been reached are finalized in the same transaction.
        """
        if target.tzinfo is None:
            raise ValueError("watermark target must be timezone-aware UTC")
        target = target.astimezone()
        with DB_LOCK, self.conn:
            current = self.get_effective_watermark()
            if current is not None and target <= current:
                return {
                    "advanced": False,
                    "reason": "target is not beyond the current effective watermark",
                    "effective_watermark": format_ts(current),
                    "finalized_windows": [],
                }
            self.conn.execute(
                "UPDATE state SET watermark_override = ? WHERE id = 1",
                (format_ts(target),),
            )
            new_wm = self.get_effective_watermark()
            self._log_watermark(current, new_wm, "explicit:advance-watermark")
            finalized = self._finalize_due_windows(new_wm)
            return {
                "advanced": True,
                "effective_watermark": format_ts(new_wm),
                "previous_watermark": format_ts(current) if current else None,
                "finalized_windows": finalized,
            }

    # ------------------------------------------------------------- read views

    def list_windows(self) -> list[dict[str, Any]]:
        with DB_LOCK:
            rows = self.conn.execute(
                "SELECT * FROM windows ORDER BY window_minute DESC, device_id"
            ).fetchall()
            return [self._window_dict(r) for r in rows]

    def list_versions(self, device_id: str | None = None,
                      window_minute: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM window_versions WHERE 1=1"
        params: list[Any] = []
        if device_id is not None:
            sql += " AND device_id = ?"
            params.append(device_id)
        if window_minute is not None:
            sql += " AND window_minute = ?"
            params.append(window_minute)
        sql += " ORDER BY window_minute, device_id, version"
        with DB_LOCK:
            return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def list_late_events(self) -> list[dict[str, Any]]:
        with DB_LOCK:
            return [dict(r) for r in self.conn.execute(
                "SELECT * FROM late_events ORDER BY received_at DESC, late_id DESC").fetchall()]

    def list_conflicts(self) -> list[dict[str, Any]]:
        with DB_LOCK:
            return [dict(r) for r in self.conn.execute(
                "SELECT * FROM conflicts ORDER BY conflict_id DESC").fetchall()]

    def list_invalid(self) -> list[dict[str, Any]]:
        with DB_LOCK:
            return [dict(r) for r in self.conn.execute(
                "SELECT * FROM invalid_events ORDER BY invalid_id DESC").fetchall()]

    def list_events(self) -> list[dict[str, Any]]:
        with DB_LOCK:
            return [dict(r) for r in self.conn.execute(
                "SELECT event_id, device_id, event_time, reading, window_minute, "
                "duplicate_count, first_seen, last_seen FROM events "
                "ORDER BY first_seen DESC, event_id").fetchall()]

    def watermark_log(self) -> list[dict[str, Any]]:
        with DB_LOCK:
            return [dict(r) for r in self.conn.execute(
                "SELECT * FROM watermark_log ORDER BY log_id").fetchall()]

    @staticmethod
    def _window_dict(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["avg_reading"] = row["sum_reading"] / row["count"] if row["count"] else None
        data["finalized"] = bool(row["finalized"])
        return data


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)
