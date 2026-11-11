"""JSONL tailer: continuously appends events into the processing engine.

Reading progress (byte offset per file) is committed in the same SQLite
transaction as the event itself, so a restart resumes exactly where the last
successfully processed line ended -- no double processing and no skipped
lines.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from dataclasses import dataclass

from .processor import EventProcessor
from .schema import DB_LOCK

log = logging.getLogger("device_metrics.tailer")


@dataclass
class TailStats:
    cycles: int = 0
    lines_processed: int = 0


class JsonlTailer(threading.Thread):
    """Poll one or more JSONL files and feed new lines to the processor."""

    def __init__(self, conn: sqlite3.Connection, processor: EventProcessor,
                 files: list[str], poll_interval: float = 0.2):
        super().__init__(name="jsonl-tailer", daemon=True)
        # A dedicated connection: SQLite connections must not be shared across
        # threads. WAL allows this writer to run while the server reads.
        self._own_conn = False
        self.conn = conn
        self.processor = processor
        self.files = list(files)
        self.poll_interval = poll_interval
        # Note: Thread already defines a _stop() method, so the shutdown flag
        # gets its own name to avoid shadowing it and breaking join().
        self._stop_event = threading.Event()
        self.stats = TailStats()

    def stop(self) -> None:
        self._stop_event.set()

    def stopped(self) -> bool:
        return self._stop_event.is_set()

    def _get_offset(self, path: str) -> int:
        with DB_LOCK:
            row = self.conn.execute(
                "SELECT read_offset FROM files WHERE path = ?", (path,)
            ).fetchone()
        return int(row["read_offset"]) if row else 0

    def _set_offset(self, path: str, offset: int) -> None:
        self.conn.execute(
            "INSERT INTO files (path, read_offset) VALUES (?, ?) "
            "ON CONFLICT(path) DO UPDATE SET read_offset = excluded.read_offset",
            (path, offset),
        )

    def run_once(self) -> int:
        """Process every currently available line. Returns lines processed."""
        processed = 0
        for path in self.files:
            processed += self._drain_file(path)
        self.stats.cycles += 1
        return processed

    def _drain_file(self, path: str) -> int:
        if not os.path.exists(path):
            return 0
        offset = self._get_offset(path)
        # File was truncated/rotated: start over from the beginning.
        if offset > os.path.getsize(path):
            log.info("file %s shrank (rotated?), restarting from offset 0", path)
            offset = 0
        processed = 0
        with open(path, "rb") as fh:
            fh.seek(offset)
            while True:
                line_start = fh.tell()
                raw = fh.readline()
                if not raw:
                    break  # no more data for now
                if not raw.endswith(b"\n"):
                    # Partial line (writer still flushing): wait for the rest.
                    fh.seek(line_start)
                    break
                text = raw.decode("utf-8", errors="replace")
                line_no = None  # line numbers are best-effort bookkeeping only
                next_offset = fh.tell()
                with DB_LOCK:
                    # Offset update runs inside the processor's transaction:
                    # progress and metrics commit atomically.
                    result = self.processor.process_line(
                        text, source_file=path, line_no=line_no,
                        on_commit=lambda p=path, o=next_offset: self._set_offset(p, o),
                    )
                processed += 1
                self.stats.lines_processed += 1
                log.info("processed %s -> %s (%s)", path, result.status, result.reason or "ok")
        return processed

    def run(self) -> None:
        log.info("tailer started, watching: %s", ", ".join(self.files))
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception:  # keep the service alive through transient I/O errors
                log.exception("tailer cycle failed; will retry")
            self._stop_event.wait(self.poll_interval)
        log.info("tailer stopped after %d lines", self.stats.lines_processed)
