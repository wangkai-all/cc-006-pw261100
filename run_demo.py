#!/usr/bin/env python3
"""Repeatable end-to-end demonstration of the device metrics service.

The script is fully deterministic: all event times are fixed UTC timestamps
and the watermark is advanced *explicitly* through the service's
``advance-watermark`` entry point (HTTP API + CLI both work), so no real-time
waiting is needed to observe window finalization.

Stages
------
1. start the service on an empty database;
2. append batch 1 (out of order, duplicate, cross-minute);
3. append batch 2 (duplicate, conflict, invalid events, on-time late
   correction -> new metric version);
4. explicitly advance the watermark to the 10:00 window's finalization
   deadline and observe finalization;
5. append batch 3 (watermark-pushing event, post-finalization late events);
6. restart the service process to prove progress/dedup/metrics are
   persisted and processing resumes;
7. advance the watermark once more so every window is finalized;
8. print a final summary; the dashboard stays available until Ctrl-C.

Run (from the repository root)::

    python3 -I run_demo.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
# -I (isolated mode) strips the script dir from sys.path; add it back so the
# local package and the samples directory import without any env vars.
sys.path.insert(0, str(ROOT))

from samples.sample_data import (  # noqa: E402
    BATCHES,
    FINALIZE_1000_TARGET,
    FINALIZE_1001_TARGET,
    WM_AFTER_BATCH1,
)
from device_metrics.models import parse_watermark_target  # noqa: E402

RUNTIME = ROOT / "runtime"
DB_PATH = RUNTIME / "metrics.db"
INPUT_PATH = RUNTIME / "events.jsonl"
HOST = "127.0.0.1"
PORT = int(os.environ.get("DEMO_PORT", "8765"))
BASE_URL = f"http://{HOST}:{PORT}"
PYTHON = sys.executable


class Reporter:
    def __init__(self) -> None:
        self.step_no = 0

    def step(self, title: str) -> None:
        self.step_no += 1
        print(f"\n{'=' * 78}\n步骤 {self.step_no}：{title}\n{'=' * 78}")

    @staticmethod
    def info(text: str) -> None:
        print(f"  · {text}")

    @staticmethod
    def show_lines(batch: int) -> None:
        for line in BATCHES[batch]:
            print(f"    + {line.description}\n        {line.jsonl()}")


def wait_for_server(timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{BASE_URL}/healthz", timeout=1) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError) as exc:
            last_err = exc
            time.sleep(0.25)
    raise RuntimeError(f"server did not become ready: {last_err}")


def api_get(path: str):
    with urllib.request.urlopen(f"{BASE_URL}{path}", timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def api_post(path: str):
    req = urllib.request.Request(f"{BASE_URL}{path}", method="POST")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def start_server() -> subprocess.Popen:
    cmd = [
        PYTHON, "-I", str(ROOT / "run_server.py"),
        "--db", str(DB_PATH),
        "--input", str(INPUT_PATH),
        "--host", HOST,
        "--port", str(PORT),
    ]
    proc = subprocess.Popen(
        cmd, cwd=str(ROOT),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        wait_for_server()
    except RuntimeError:
        if proc.poll() is not None:
            print(proc.stdout.read(), file=sys.stderr)
        raise
    return proc


def stop_server(proc: subprocess.Popen) -> str:
    proc.terminate()
    try:
        out, _ = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate(timeout=5)
    return out


def append_batch(batch: int) -> None:
    from samples.sample_data import write_batch
    write_batch(str(INPUT_PATH), batch)
    # Give the polling tailer a moment to drain the new lines.
    time.sleep(0.8)


def print_windows(title: str) -> None:
    print(f"\n  --- {title} ---")
    windows = api_get("/api/windows")
    if not windows:
        print("    （暂无窗口）")
    for w in windows:
        state = "已定稿" if w["finalized"] else "开放中"
        print(f"    {w['window_minute']}  {w['device_id']:<9} "
              f"count={w['count']} avg={w['avg_reading']:.4f} "
              f"max={w['max_reading']:.4f} version=v{w['version']} [{state}]")


def print_state() -> None:
    state = api_get("/api/state")
    print(f"    有效事件最大时间 : {state['max_event_ts']}")
    print(f"    当前有效水位线  : {state['effective_watermark']}")
    print(f"    显式覆盖值      : {state['watermark_override']}")


def print_counts() -> None:
    events = api_get("/api/events")
    late = api_get("/api/late")
    conflicts = api_get("/api/conflicts")
    invalid = api_get("/api/invalid")
    versions = api_get("/api/versions")
    print(f"    已接受事件(去重后) : {len(events)}  指标版本记录: {len(versions)}")
    print(f"    迟到隔离事件       : {len(late)}  冲突: {len(conflicts)}  无效: {len(invalid)}")


def main() -> int:
    RUNTIME.mkdir(exist_ok=True)
    # Fresh, repeatable run: wipe every runtime artifact first.
    for path in (DB_PATH, INPUT_PATH):
        if path.exists():
            path.unlink()
    for suffix in ("-wal", "-shm"):
        p = Path(str(DB_PATH) + suffix)
        if p.exists():
            p.unlink()

    reporter = Reporter()

    # ---- stage 1: start -----------------------------------------------------
    reporter.step(f"启动服务（空库，监听 {BASE_URL}，输入文件 {INPUT_PATH}）")
    proc = start_server()
    reporter.info("服务已启动，页面地址：%s/ （每 5 秒自动刷新）" % BASE_URL)

    try:
        # ---- stage 2: batch 1 ------------------------------------------------
        reporter.step("写入第 1 批：乱序、跨分钟、完全重复")
        reporter.show_lines(1)
        append_batch(1)
        print_state()
        print_windows("批次 1 后的窗口（注意按事件时间分钟归组，与到达顺序无关）")
        print_counts()
        state = api_get("/api/state")
        assert parse_watermark_target(state["effective_watermark"]) == \
            parse_watermark_target(WM_AFTER_BATCH1), state
        reporter.info(f"水位线 = 最大有效事件时间 - 30s = {WM_AFTER_BATCH1}，与计算一致 ✔")

        # ---- stage 3: batch 2 ------------------------------------------------
        reporter.step("写入第 2 批：重复、ID 冲突、非法时间、非法读数、宽限内迟到修正")
        reporter.show_lines(2)
        append_batch(2)
        print_windows("批次 2 后的窗口（device-B/10:00 因迟到修正升版，数量与均值变化）")
        print_counts()
        conflicts = api_get("/api/conflicts")
        invalid = api_get("/api/invalid")
        assert len(conflicts) == 1 and conflicts[0]["event_id"] == "e-a0"
        assert len(invalid) == 2, invalid
        b_window = next(w for w in api_get("/api/windows")
                        if w["device_id"] == "device-B" and w["window_minute"].startswith("2026-11-11T10:00"))
        assert b_window["count"] == 3 and b_window["version"] == 3, b_window
        reporter.info("冲突保留首次读数 10.0；两条无效事件被拒绝且未推动水位线 ✔")

        # ---- stage 4: explicit watermark advance -> finalize 10:00 ----------
        reporter.step(f"显式推进水位线到 {FINALIZE_1000_TARGET}（= 10:00 窗口结束 + 60s 定稿线）")
        result = api_post(f"/api/advance-watermark?to={FINALIZE_1000_TARGET}")
        print(f"    API 返回：{json.dumps(result, ensure_ascii=False)}")
        print_windows("推进后的窗口（10:00 两个窗口定稿，10:01 仍开放）")
        finalized = {(w["device_id"], w["window_minute"][:16])
                     for w in api_get("/api/windows") if w["finalized"]}
        assert ("device-A", "2026-11-11T10:00") in finalized
        assert ("device-B", "2026-11-11T10:00") in finalized

        # ---- stage 5: batch 3 -> late events after finalization -------------
        reporter.step("写入第 3 批：推进型事件 + 定稿后迟到数据")
        reporter.show_lines(3)
        append_batch(3)
        print_state()
        print_windows("批次 3 后的窗口（已定稿窗口的指标保持不变）")
        print_counts()
        late = api_get("/api/late")
        assert len(late) == 2, late  # the repeated late id is deduplicated
        assert {e["event_id"] for e in late} == {"e-late1", "e-late2"}
        a_window = next(w for w in api_get("/api/windows")
                        if w["device_id"] == "device-A" and w["window_minute"].startswith("2026-11-11T10:00"))
        assert a_window["count"] == 2 and a_window["finalized"], a_window
        reporter.info("迟到事件进入隔离区，已定稿的 count/avg/max/version 均未被改动 ✔")

        # ---- stage 6: restart ------------------------------------------------
        reporter.step("重启服务进程，验证处理进度、去重记录与指标全部持久化、自动续跑")
        logs = stop_server(proc)
        proc = None
        reporter.info("旧进程已停止。尾部服务日志：")
        for line in logs.splitlines()[-8:]:
            print(f"    | {line}")
        proc = start_server()
        time.sleep(0.6)
        # The persisted file offset means old lines are never replayed.
        # Appending batch 1 again only processes the six NEW lines, each of
        # which is a known duplicate: counters rise by exactly one per copy,
        # no new event id is accepted and no late/conflict/invalid row appears.
        events_before = {e["event_id"]: e["duplicate_count"] for e in api_get("/api/events")}
        side_before = (len(api_get("/api/late")), len(api_get("/api/conflicts")),
                       len(api_get("/api/invalid")))
        append_batch(1)
        events_after = {e["event_id"]: e["duplicate_count"] for e in api_get("/api/events")}
        assert set(events_after) == set(events_before)  # nothing new accepted
        expected_delta = {"e-a1": 2, "e-a0": 1, "e-b1": 1, "e-a2": 1, "e-b2": 1}
        for event_id, delta in expected_delta.items():
            assert events_after[event_id] - events_before[event_id] == delta, \
                (event_id, events_before, events_after)
        side_after = (len(api_get("/api/late")), len(api_get("/api/conflicts")),
                      len(api_get("/api/invalid")))
        assert side_after == side_before
        reporter.info("重启后偏移续读：旧行零重放，新重复行各计一次，无新事件被接受 ✔")

        # ---- stage 7: final watermark advance --------------------------------
        reporter.step(f"演示结束入口：再次显式推进水位线到 {FINALIZE_1001_TARGET}，定稿 10:01 窗口")
        result = api_post(f"/api/advance-watermark?to={FINALIZE_1001_TARGET}")
        print(f"    API 返回：{json.dumps(result, ensure_ascii=False)}")
        print_windows("最终窗口状态（10:01 已定稿；10:02 截止线 10:04:00 未到，仍开放）")
        windows = api_get("/api/windows")
        finalized = {w["window_minute"][:16] for w in windows if w["finalized"]}
        assert "2026-11-11T10:00" in finalized and "2026-11-11T10:01" in finalized
        assert "2026-11-11T10:02" not in finalized  # only due windows are finalized
        reporter.info("定稿只作用于到期窗口，10:02 窗口保持开放，等待后续事件或推进 ✔")

        # ---- stage 8: summary ------------------------------------------------
        reporter.step("最终汇总")
        print_state()
        print_counts()
        print("\n  指标版本历史（每个窗口的每次修正都可追溯）：")
        for v in api_get("/api/versions"):
            print(f"    {v['window_minute']} {v['device_id']:<9} v{v['version']} "
                  f"count={v['count']} avg={v['avg_reading']:.4f} max={v['max_reading']:.4f} "
                  f"({v['change_reason']}, event={v['event_id']})")
        print("\n  迟到隔离区数据（定稿后到达，未回改指标）：")
        for e in api_get("/api/late"):
            print(f"    {e['event_id']} {e['device_id']} t={e['event_time']} "
                  f"reading={e['reading']} (到达时水位线 {e['watermark_at_arrival']})")
        print(f"\n  数据库文件：{DB_PATH}")
        print(f"  控制台页面：{BASE_URL}/   （服务保持运行，Ctrl-C 退出）")
        print("  也可用 CLI 推进水位线："
              f"python3 -I run_cli.py --db {DB_PATH} advance-watermark --to <ISO时间>")

        # DEMO_EXIT=1 makes the script return after a successful run (used for
        # automated verification); default is to keep serving the dashboard.
        if os.environ.get("DEMO_EXIT") == "1":
            reporter.info("DEMO_EXIT=1，演示全部断言通过，自动结束。")
            return 0
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        reporter.info("收到 Ctrl-C，退出演示。")
        return 0
    finally:
        if proc is not None:
            stop_server(proc)


if __name__ == "__main__":
    raise SystemExit(main())
