"""Unit/integration tests for the event-time processing engine.

Run from the repository root::

    python3 -I run_tests.py
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from device_metrics.models import ParsedEvent, parse_event  # noqa: E402
from device_metrics.processor import EventProcessor  # noqa: E402
from device_metrics.schema import connect, init_db  # noqa: E402

UTC = timezone.utc


def ts(second: int, minute: int = 0, hour: int = 10, day: int = 11) -> datetime:
    return datetime(2026, 11, day, hour, minute, second, tzinfo=UTC)


def make_event(event_id: str, device: str, when: datetime, reading: float) -> ParsedEvent:
    return ParsedEvent(
        event_id=event_id,
        device_id=device,
        event_time=when,
        reading=reading,
        raw_payload={
            "event_id": event_id,
            "device_id": device,
            "event_time": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "reading": reading,
        },
    )


class ProcessorTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.conn = connect(self.db_path)
        init_db(self.conn)
        self.proc = EventProcessor(self.conn)

    def tearDown(self) -> None:
        self.conn.close()
        self.tmp.cleanup()

    def window(self, device: str, minute: datetime):
        rows = [w for w in self.proc.list_windows()
                if w["device_id"] == device and w["window_minute"].startswith(minute.strftime("%Y-%m-%dT%H:%M"))]
        self.assertEqual(len(rows), 1, rows)
        return rows[0]


class WatermarkOrderingTests(ProcessorTestBase):
    def test_watermark_is_max_event_time_minus_delay_and_classifies_first(self):
        # First event at 10:00:40 -> watermark 10:00:10.
        r1 = self.proc.process_event(make_event("e1", "d1", ts(40), 1.0))
        self.assertEqual(r1.status, "accepted")
        self.assertTrue(r1.watermark_after.startswith("2026-11-11T10:00:10"))

        # Event at exactly the watermark (10:00:10) is classified against the
        # CURRENT watermark first -> late, never accepted, watermark unchanged.
        r2 = self.proc.process_event(make_event("e2", "d1", ts(10), 2.0))
        self.assertEqual(r2.status, "late")
        self.assertEqual(r2.watermark_after, r1.watermark_after)
        self.assertEqual(len(self.proc.list_late_events()), 1)

        # An event behind the watermark is late even though it is "newer" to us.
        r3 = self.proc.process_event(make_event("e3", "d1", ts(9), 3.0))
        self.assertEqual(r3.status, "late")

    def test_watermark_never_moves_backwards_on_old_events(self):
        self.proc.process_event(make_event("e1", "d1", ts(50), 1.0))
        before = self.proc.get_state()["effective_watermark"]
        self.proc.process_event(make_event("e2", "d1", ts(5), 2.0))  # late
        after = self.proc.get_state()["effective_watermark"]
        self.assertEqual(before, after)


class DedupConflictTests(ProcessorTestBase):
    def test_identical_event_id_and_content_counts_once(self):
        a = make_event("e1", "d1", ts(5), 10.0)
        self.proc.process_event(a)
        repeat = self.proc.process_event(make_event("e1", "d1", ts(5), 10.0))
        self.assertEqual(repeat.status, "duplicate")
        events = self.proc.list_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["duplicate_count"], 2)
        w = self.window("d1", ts(0))
        self.assertEqual(w["count"], 1)
        self.assertEqual(w["version"], 1)

    def test_same_id_different_content_records_conflict_keeps_first(self):
        self.proc.process_event(make_event("e1", "d1", ts(5), 10.0))
        conflict = self.proc.process_event(make_event("e1", "d1", ts(5), 99.0))
        self.assertEqual(conflict.status, "conflict")
        conflicts = self.proc.list_conflicts()
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["reading"], 99.0)
        w = self.window("d1", ts(0))
        self.assertEqual(w["count"], 1)
        self.assertEqual(w["max_reading"], 10.0)  # tampered reading rejected
        # Conflicts must not move the watermark.
        self.assertEqual(self.proc.get_state()["max_event_ts"][:19],
                         ts(5).strftime("%Y-%m-%dT%H:%M:%S"))

    def test_key_reordering_is_still_a_duplicate(self):
        payload = {"event_id": "e1", "device_id": "d1",
                   "event_time": "2026-11-11T10:00:05Z", "reading": 10.0}
        first = parse_event(payload)
        self.proc.process_event(first)
        reordered = parse_event({"reading": 10.0, "event_time": "2026-11-11T10:00:05Z",
                                 "device_id": "d1", "event_id": "e1"})
        result = self.proc.process_event(reordered)
        self.assertEqual(result.status, "duplicate")


class WindowAggregationTests(ProcessorTestBase):
    def test_out_of_order_events_group_by_event_time_minute(self):
        self.proc.process_event(make_event("e2", "d1", ts(20), 12.0))
        self.proc.process_event(make_event("e1", "d1", ts(5), 10.0))
        self.proc.process_event(make_event("e3", "d1", ts(10, minute=1), 20.0))
        windows = self.proc.list_windows()
        self.assertEqual(len(windows), 2)
        w0 = self.window("d1", ts(0))
        self.assertEqual((w0["count"], w0["max_reading"]), (2, 12.0))
        self.assertAlmostEqual(w0["avg_reading"], 11.0)
        w1 = self.window("d1", ts(0, minute=1))
        self.assertEqual(w1["count"], 1)

    def test_late_correction_before_finalization_bumps_version(self):
        self.proc.process_event(make_event("e1", "d2", ts(40), 20.0))  # wm 10:00:10
        self.proc.process_event(make_event("e2", "d2", ts(50), 22.0))  # wm 10:00:20
        # 10:00:15 is behind the current watermark (10:00:20) -> classified late.
        behind = self.proc.process_event(make_event("e3", "d2", ts(15), 5.0))
        self.assertEqual(behind.status, "late")

        # But a correction whose event time stays ahead of the watermark is
        # accepted and creates a new metric version.
        ok = self.proc.process_event(make_event("e4", "d2", ts(25), 8.0))
        self.assertEqual(ok.status, "accepted")
        self.assertEqual(ok.window_version, 3)
        w = self.window("d2", ts(0))
        self.assertEqual(w["version"], 3)
        versions = [v for v in self.proc.list_versions() if v["device_id"] == "d2"]
        self.assertEqual([v["version"] for v in versions], [1, 2, 3])


class FinalizationTests(ProcessorTestBase):
    def _feed_window(self):
        self.proc.process_event(make_event("e1", "d1", ts(5), 10.0))
        self.proc.process_event(make_event("e2", "d1", ts(55), 30.0))

    def test_window_finalizes_when_watermark_reaches_end_plus_grace(self):
        self._feed_window()
        # Window end = 10:01:00, deadline = 10:02:00.
        # First establish a natural watermark (10:00:55 - 30s = 10:00:25),
        # then prove the explicit override only moves forward.
        res = self.proc.advance_watermark(ts(0))
        self.assertFalse(res["advanced"])  # behind the current watermark
        self.assertFalse(self.window("d1", ts(0))["finalized"])

        res = self.proc.advance_watermark(ts(0, minute=2))
        self.assertTrue(res["advanced"])
        self.assertEqual(len(res["finalized_windows"]), 1)
        self.assertTrue(self.window("d1", ts(0))["finalized"])

    def test_post_finalization_events_are_quarantined_and_metric_immutable(self):
        self._feed_window()
        self.proc.advance_watermark(ts(0, minute=2))
        before = self.window("d1", ts(0))

        late = self.proc.process_event(make_event("e3", "d1", ts(30), 99.0))
        self.assertEqual(late.status, "late")
        after = self.window("d1", ts(0))
        self.assertEqual(
            (after["count"], after["sum_reading"], after["max_reading"], after["version"]),
            (before["count"], before["sum_reading"], before["max_reading"], before["version"]),
        )
        self.assertEqual(len(self.proc.list_late_events()), 1)

        # Repeated late event id is deduplicated too.
        again = self.proc.process_event(make_event("e3", "d1", ts(30), 99.0))
        self.assertEqual(again.status, "duplicate")
        self.assertEqual(len(self.proc.list_late_events()), 1)

    def test_explicit_watermark_is_monotonic(self):
        self.proc.advance_watermark(ts(0, minute=5))
        res = self.proc.advance_watermark(ts(0, minute=4))
        self.assertFalse(res["advanced"])
        self.assertTrue(self.proc.get_state()["effective_watermark"].startswith(
            "2026-11-11T10:05:00"))


class InvalidEventTests(ProcessorTestBase):
    def test_invalid_payloads_are_recorded_but_never_move_watermark(self):
        r1 = self.proc.process_line("{not json")
        self.assertEqual(r1.status, "invalid")
        r2 = self.proc.process_line(
            '{"event_id":"x","device_id":"d","event_time":"2026-11-11T10:00:00","reading":1.0}')
        self.assertEqual(r2.status, "invalid")  # naive timestamp
        r3 = self.proc.process_line(
            '{"event_id":"y","device_id":"d","event_time":"2026-11-11T10:00:00Z","reading":"hot"}')
        self.assertEqual(r3.status, "invalid")  # non-numeric reading
        self.assertIsNone(self.proc.get_state()["max_event_ts"])
        self.assertEqual(len(self.proc.list_invalid()), 3)

    def test_epoch_and_iso_z_and_offset_times_are_equivalent_utc(self):
        epoch = datetime(2026, 11, 11, 10, 0, 30, tzinfo=UTC).timestamp()
        e1 = parse_event({"event_id": "a", "device_id": "d",
                          "event_time": epoch, "reading": 1.0})
        e2 = parse_event({"event_id": "b", "device_id": "d",
                          "event_time": "2026-11-11T10:00:30Z", "reading": 2.0})
        e3 = parse_event({"event_id": "c", "device_id": "d",
                          "event_time": "2026-11-11T12:00:30+02:00", "reading": 3.0})
        self.assertEqual(e1.event_time, e2.event_time)
        self.assertEqual(e2.event_time, e3.event_time)


class PersistenceTests(ProcessorTestBase):
    def test_state_survives_reconnect(self):
        self.proc.process_event(make_event("e1", "d1", ts(40), 10.0))
        self.proc.process_event(make_event("e2", "d1", ts(50), 20.0))
        self.proc.advance_watermark(ts(0, minute=2))
        self.conn.commit()

        conn2 = connect(self.db_path)
        proc2 = EventProcessor(conn2)
        windows = proc2.list_windows()
        self.assertEqual(len(windows), 1)
        self.assertTrue(windows[0]["finalized"])
        self.assertEqual(windows[0]["count"], 2)
        self.assertTrue(proc2.get_state()["effective_watermark"]
                        .startswith("2026-11-11T10:02:00"))
        # Dedup store is intact: same id is a duplicate after restart.
        dup = proc2.process_event(make_event("e1", "d1", ts(40), 10.0))
        self.assertEqual(dup.status, "duplicate")
        conn2.close()


class MultiWindowFinalizationTests(ProcessorTestBase):
    def test_one_watermark_advance_finalizes_all_due_windows_across_devices(self):
        self.proc.process_event(make_event("a1", "d1", ts(5), 1.0))
        self.proc.process_event(make_event("b1", "d2", ts(50), 2.0))
        self.proc.process_event(make_event("a2", "d1", ts(10, minute=1), 3.0))
        res = self.proc.advance_watermark(ts(0, minute=2))  # deadline of the 10:00 window
        self.assertEqual(len(res["finalized_windows"]), 2)
        windows = self.proc.list_windows()
        finalized = {w["device_id"] for w in windows if w["finalized"]}
        self.assertEqual(finalized, {"d1", "d2"})
        # The 10:01 window (deadline 10:03:00) stays open.
        self.assertFalse(self.window("d1", ts(0, minute=1))["finalized"])

    def test_nan_payload_line_is_invalid(self):
        line = ('{"event_id":"n","device_id":"d",'
                '"event_time":"2026-11-11T10:00:00Z","reading":NaN}')
        result = self.proc.process_line(line)
        self.assertEqual(result.status, "invalid")
        self.assertIsNone(self.proc.get_state()["max_event_ts"])


class TailerTests(ProcessorTestBase):
    def test_tailer_resumes_from_persisted_offset(self):
        from device_metrics.tailer import JsonlTailer

        path = os.path.join(self.tmp.name, "events.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write('{"event_id":"e1","device_id":"d","event_time":"2026-11-11T10:00:05Z","reading":1.0}\n')
            fh.write('{"event_id":"e2","device_id":"d","event_time":"2026-11-11T10:00:06Z","reading":2.0}\n')

        tailer = JsonlTailer(self.conn, self.proc, [path])
        self.assertEqual(tailer.run_once(), 2)
        self.assertEqual(len(self.proc.list_events()), 2)

        # Second cycle with no new lines processes nothing.
        self.assertEqual(tailer.run_once(), 0)

        # Simulate restart: fresh tailer instance, same database.
        tailer2 = JsonlTailer(self.conn, self.proc, [path])
        self.assertEqual(tailer2.run_once(), 0)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write('{"event_id":"e3","device_id":"d","event_time":"2026-11-11T10:00:07Z","reading":3.0}\n')
        self.assertEqual(tailer2.run_once(), 1)
        self.assertEqual(len(self.proc.list_events()), 3)

    def test_offset_is_committed_in_the_same_transaction_as_the_event(self):
        from device_metrics.tailer import JsonlTailer

        path = os.path.join(self.tmp.name, "atomic.jsonl")
        line = ('{"event_id":"e1","device_id":"d",'
                '"event_time":"2026-11-11T10:00:05Z","reading":1.0}\n')
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(line)

        tailer = JsonlTailer(self.conn, self.proc, [path])
        tailer.run_once()
        # Reconnect exactly like a process restart would.
        conn2 = connect(self.db_path)
        try:
            offset = conn2.execute("SELECT read_offset FROM files WHERE path = ?",
                                   (path,)).fetchone()["read_offset"]
            self.assertEqual(offset, len(line.encode("utf-8")))
            self.assertEqual(
                conn2.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"], 1)
        finally:
            conn2.close()


class ConcurrencyTests(ProcessorTestBase):
    def test_concurrent_tailer_writes_and_dashboard_reads(self):
        """The shared connection must survive cross-thread reads under DB_LOCK."""
        import threading

        from device_metrics.tailer import JsonlTailer

        path = os.path.join(self.tmp.name, "concurrent.jsonl")
        tailer = JsonlTailer(self.conn, self.proc, [path], poll_interval=0.02)
        tailer.start()
        errors: list[Exception] = []

        def reader() -> None:
            try:
                for _ in range(200):
                    self.proc.list_windows()
                    self.proc.get_state()
                    self.proc.list_versions()
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()
        with open(path, "a", encoding="utf-8") as fh:
            for i in range(40):
                fh.write('{"event_id":%s,"device_id":"d",'
                         '"event_time":"2026-11-11T10:00:%02dZ","reading":%d}\n'
                         % (f'"c{i}"', i % 60, i))
        # Wait until the polling tailer has drained every byte.
        import time as _time
        deadline = _time.monotonic() + 5
        while _time.monotonic() < deadline:
            offset = self.conn.execute(
                "SELECT read_offset FROM files WHERE path = ?", (path,)
            ).fetchone()
            if offset and offset["read_offset"] == os.path.getsize(path):
                break
            _time.sleep(0.05)
        for t in threads:
            t.join(timeout=10)
        tailer.stop()
        tailer.join(timeout=5)
        self.assertFalse(tailer.is_alive())
        self.assertEqual(errors, [])
        # Every line was processed exactly once; some land in late_events as
        # the watermark advances, so count both stores plus the file offset.
        total = (len(self.proc.list_events()) + len(self.proc.list_late_events()))
        self.assertEqual(total, 40)
        offset = self.conn.execute(
            "SELECT read_offset FROM files WHERE path = ?", (path,)).fetchone()["read_offset"]
        self.assertEqual(offset, os.path.getsize(path))


if __name__ == "__main__":
    unittest.main(verbosity=2)
