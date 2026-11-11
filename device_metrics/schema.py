"""SQLite schema definition and connection helpers.

All processing state lives in a single SQLite database so that progress,
deduplication records, conflicts, late events and metrics commit atomically
and a restart simply resumes from the persisted state.
"""

from __future__ import annotations

import sqlite3
import threading

# Serializes every database access. The service shares one connection between
# the tailer thread and the HTTP handler threads (check_same_thread=False);
# WAL handles durability while this lock makes that sharing safe.
DB_LOCK = threading.RLock()

SCHEMA_STATEMENTS = [
    #
    # Singleton processing state: watermark plus its constituent parts.
    #
    # max_event_ts       : largest event_time among *valid* events accepted so far.
    #                      Invalid/illegal events never move it.
    # watermark_override : explicit watermark advance ("advance-watermark"
    #                      demo entry point). The effective watermark is
    #                      max(max_event_ts - 30s, watermark_override).
    #
    """
    CREATE TABLE IF NOT EXISTS state (
        id                 INTEGER PRIMARY KEY CHECK (id = 1),
        max_event_ts       TEXT,
        watermark_override TEXT
    )
    """,
    """
    INSERT OR IGNORE INTO state (id, max_event_ts, watermark_override)
    VALUES (1, NULL, NULL)
    """,
    #
    # JSONL ingestion progress (one row per input file path).
    #
    """
    CREATE TABLE IF NOT EXISTS files (
        path        TEXT PRIMARY KEY,
        read_offset INTEGER NOT NULL DEFAULT 0
    )
    """,
    #
    # Accepted events, keyed by event_id -> this is the deduplication store.
    # content_hash is a canonical hash of the whole event payload so identical
    # repeats are detected. duplicate_count tracks how often the same event id
    # with the same content was seen again.
    #
    """
    CREATE TABLE IF NOT EXISTS events (
        event_id       TEXT PRIMARY KEY,
        device_id      TEXT NOT NULL,
        event_time     TEXT NOT NULL,
        reading        REAL NOT NULL,
        content_hash   TEXT NOT NULL,
        window_minute  TEXT NOT NULL,
        duplicate_count INTEGER NOT NULL DEFAULT 1,
        first_seen     TEXT NOT NULL,
        last_seen      TEXT NOT NULL
    )
    """,
    #
    # Same event id but different content: the first accepted reading wins and
    # the conflicting payload is recorded here without changing any metric.
    #
    """
    CREATE TABLE IF NOT EXISTS conflicts (
        conflict_id  INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id     TEXT NOT NULL,
        device_id    TEXT,
        event_time   TEXT,
        reading      REAL,
        content_hash TEXT,
        raw_payload  TEXT NOT NULL,
        accepted_hash TEXT NOT NULL,
        detected_at  TEXT NOT NULL
    )
    """,
    #
    # Events that failed validation (bad time, illegal reading, malformed
    # JSON, missing fields ...). They are recorded but never move the
    # watermark and never update metrics.
    #
    """
    CREATE TABLE IF NOT EXISTS invalid_events (
        invalid_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        source_file  TEXT,
        line_no      INTEGER,
        reason       TEXT NOT NULL,
        raw_payload  TEXT NOT NULL,
        detected_at  TEXT NOT NULL
    )
    """,
    #
    # One aggregate row per (device, minute window). version starts at 1 and
    # increments on every accepted update. finalized flips to 1 when the
    # watermark reaches window_end + grace; after that the row is immutable.
    #
    """
    CREATE TABLE IF NOT EXISTS windows (
        device_id     TEXT NOT NULL,
        window_minute TEXT NOT NULL,
        window_end    TEXT NOT NULL,
        count         INTEGER NOT NULL,
        sum_reading   REAL NOT NULL,
        max_reading   REAL NOT NULL,
        version       INTEGER NOT NULL,
        finalized     INTEGER NOT NULL DEFAULT 0,
        finalized_at  TEXT,
        created_at    TEXT NOT NULL,
        updated_at    TEXT NOT NULL,
        PRIMARY KEY (device_id, window_minute)
    )
    """,
    #
    # Append-only history of every window metric version. Lets the web page
    # show how late (but on-time) corrections evolved the result before
    # finalization.
    #
    """
    CREATE TABLE IF NOT EXISTS window_versions (
        version_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id     TEXT NOT NULL,
        window_minute TEXT NOT NULL,
        version       INTEGER NOT NULL,
        count         INTEGER NOT NULL,
        avg_reading   REAL NOT NULL,
        max_reading   REAL NOT NULL,
        change_reason TEXT NOT NULL,
        event_id      TEXT,
        created_at    TEXT NOT NULL,
        UNIQUE (device_id, window_minute, version)
    )
    """,
    #
    # Events that arrived after their window had already been finalized.
    # Stored separately; finalized metrics are never silently overwritten.
    #
    """
    CREATE TABLE IF NOT EXISTS late_events (
        late_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id      TEXT NOT NULL,
        device_id     TEXT NOT NULL,
        event_time    TEXT NOT NULL,
        reading       REAL NOT NULL,
        window_minute TEXT NOT NULL,
        content_hash  TEXT NOT NULL,
        raw_payload   TEXT NOT NULL,
        watermark_at_arrival TEXT NOT NULL,
        received_at   TEXT NOT NULL
    )
    """,
    #
    # Audit trail of effective-watermark changes (natural and explicit).
    #
    """
    CREATE TABLE IF NOT EXISTS watermark_log (
        log_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        old_watermark TEXT,
        new_watermark TEXT NOT NULL,
        reason       TEXT NOT NULL,
        created_at   TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_windows_open ON windows (finalized, window_end)",
    "CREATE INDEX IF NOT EXISTS idx_events_window ON events (device_id, window_minute)",
    "CREATE INDEX IF NOT EXISTS idx_versions_window ON window_versions (device_id, window_minute)",
    "CREATE INDEX IF NOT EXISTS idx_late_window ON late_events (device_id, window_minute)",
    "CREATE INDEX IF NOT EXISTS idx_late_event_id ON late_events (event_id)",
    "CREATE INDEX IF NOT EXISTS idx_conflicts_event_id ON conflicts (event_id)",
]


def connect(db_path: str) -> sqlite3.Connection:
    """Open the database with WAL and the pragmas the service relies on."""
    conn = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL lets readers see committed rows while the tailer writes; DB_LOCK
    # serializes actual access on the shared connection.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create every table/index if missing. Safe to run on every startup."""
    with conn:
        for stmt in SCHEMA_STATEMENTS:
            conn.execute(stmt)
