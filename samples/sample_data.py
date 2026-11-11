"""Repeatable out-of-order JSONL sample data.

All event times are fixed UTC timestamps (no wall clock dependence), so the
demo behaves identically on every run. Events are split into three batches:

* ``BATCH_1``  -- out-of-order arrival across two minute windows, including an
                  identical duplicate;
* ``BATCH_2``  -- a duplicate, an id/content conflict, an invalid time, an
                  illegal reading and a *correctly timed* late correction
                  (arrives late but before finalization, so it updates the
                  metric and creates a new version);
* ``BATCH_3``  -- a watermark-pushing event and events that arrive after
                  finalization, which must be quarantined in ``late_events``.

Watermark mechanics used by the demo:

* watermark        = max(valid event time) - 30s
* window deadline  = window end + 60s grace
"""

from __future__ import annotations

import json
from dataclasses import dataclass

# Fixed base time; the whole sample is deterministic and repeatable.
BASE = "2026-11-11T10:00:00Z"


def _ts(seconds_from_base: int) -> str:
    """ISO UTC timestamp `seconds_from_base` seconds after BASE."""
    from datetime import datetime, timedelta, timezone

    base_dt = datetime(2026, 11, 11, 10, 0, 0, tzinfo=timezone.utc)
    return (base_dt + timedelta(seconds=seconds_from_base)).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class EventLine:
    batch: int
    description: str
    payload: dict

    def jsonl(self) -> str:
        return json.dumps(self.payload, ensure_ascii=False)


# A device reading helper keeps the table compact and readable.
def _evt(event_id: str, device: str, secs: int, reading) -> dict:
    return {
        "event_id": event_id,
        "device_id": device,
        "event_time": _ts(secs),
        "reading": reading,
    }


BATCH_1: list[EventLine] = [
    # --- deliberately out of order relative to event_time ---
    EventLine(1, "device-A 10:00:20 读数 11.0（先到）", _evt("e-a1", "device-A", 20, 11.0)),
    EventLine(1, "device-A 10:00:05 读数 10.0（更早的事件反而后到，乱序）",
              _evt("e-a0", "device-A", 5, 10.0)),
    EventLine(1, "device-B 10:00:40 读数 20.0", _evt("e-b1", "device-B", 40, 20.0)),
    EventLine(1, "device-A 10:01:10 读数 12.0（跨进下一分钟窗口）",
              _evt("e-a2", "device-A", 70, 12.0)),
    EventLine(1, "e-a1 完全重复（同 ID 同内容，只计算一次）",
              _evt("e-a1", "device-A", 20, 11.0)),
    EventLine(1, "device-B 10:00:55 读数 22.0（乱序，落在 10:00 窗口）",
              _evt("e-b2", "device-B", 55, 22.0)),
]

BATCH_2: list[EventLine] = [
    EventLine(2, "e-b1 又一次完全重复（重复计数 +1，指标不变）",
              _evt("e-b1", "device-B", 40, 20.0)),
    EventLine(2, "同 ID e-a0 但读数被篡改 -> 冲突，保留首次接受的 10.0",
              _evt("e-a0", "device-A", 5, 99.0)),
    EventLine(2, "非法时间（无时区的 naive 时间）-> 无效，不推动水位线",
              {"event_id": "e-bad-time", "device_id": "device-B",
               "event_time": "2026-11-11T10:00:30", "reading": 5.0}),
    EventLine(2, "非法读数 NaN -> 无效，不推动水位线",
              {"event_id": "e-bad-read", "device_id": "device-A",
               "event_time": _ts(30), "reading": float("nan")}),
    EventLine(2, "迟到但仍在水位线之前到达的修正：device-B 10:00:45 读数 18.5"
                 "（事件时间晚于当前水位线 10:00:40、窗口未定稿，应计入并产生新版本）",
              _evt("e-b0", "device-B", 45, 18.5)),
]

BATCH_3: list[EventLine] = [
    # Pushes max event time to 10:02:30 -> natural watermark 10:02:00.
    EventLine(3, "推进型事件 10:02:30 读数 30.0（自然水位线 -> 10:02:00，定稿 10:00 窗口）",
              _evt("e-c1", "device-C", 150, 30.0)),
    # 10:00 window is finalized by now: quarantined, metric untouched.
    EventLine(3, "10:00 窗口定稿后才到的 device-A 读数 -> 迟到隔离区",
              _evt("e-late1", "device-A", 50, 77.0)),
    EventLine(3, "10:00 窗口定稿后才到的 device-B 读数 -> 迟到隔离区",
              _evt("e-late2", "device-B", 15, 88.0)),
    # Also a repeated late id to prove late storage keeps its own identity.
    EventLine(3, "迟到事件重复送达（同 ID 同内容，仍只记录首次，重复计数逻辑不适用隔离区）",
              _evt("e-late1", "device-A", 50, 77.0)),
]

BATCHES = {1: BATCH_1, 2: BATCH_2, 3: BATCH_3}
ALL_BATCHES = BATCH_1 + BATCH_2 + BATCH_3


def write_batch(path: str, batch: int) -> None:
    """Append one batch as JSONL lines (creating the file if needed)."""
    with open(path, "a", encoding="utf-8") as fh:
        for line in BATCHES[batch]:
            fh.write(line.jsonl() + "\n")


def write_all(path: str) -> None:
    for batch in sorted(BATCHES):
        write_batch(path, batch)


# Key timestamps the demo drives the watermark to.
def iso(seconds_from_base: int) -> str:
    return _ts(seconds_from_base)


# Natural watermark after batch 1: max event time 10:01:10 - 30s = 10:00:40.
WM_AFTER_BATCH1 = iso(40)
# Explicit target that finalizes the 10:00 window (end 10:01:00 + 60s = 10:02:00).
FINALIZE_1000_TARGET = iso(120)
# Batch 3's natural watermark lands exactly on the same boundary.
WM_AFTER_BATCH3 = iso(120)
# Final demo target: also finalizes the 10:01 window.
FINALIZE_1001_TARGET = iso(180)
