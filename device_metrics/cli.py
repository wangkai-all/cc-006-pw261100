"""Command line entry point.

Examples
--------
Start the service (tail JSONL files + serve the dashboard)::

    python3 -m device_metrics.cli serve --db runtime/metrics.db \
        --input runtime/events.jsonl --host 127.0.0.1 --port 8000

Explicitly advance the watermark (also reachable via the HTTP API / page)::

    python3 -m device_metrics.cli advance-watermark --db runtime/metrics.db \
        --to 2026-11-11T10:03:00Z

Inspect state without the web server::

    python3 -m device_metrics.cli show --db runtime/metrics.db windows
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys

from .models import parse_watermark_target
from .processor import EventProcessor
from .schema import connect, init_db
from .server import make_server
from .tailer import JsonlTailer


def _build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default="runtime/metrics.db", help="SQLite database path")
    # Same flag repeated on subparsers, but with SUPPRESS so an omitted --db
    # after the subcommand does not clobber a value parsed before it.
    sub_common = argparse.ArgumentParser(add_help=False)
    sub_common.add_argument("--db", default=argparse.SUPPRESS, help="SQLite database path")

    parser = argparse.ArgumentParser(
        prog="device_metrics",
        description="Event-time device metrics service (JSONL in, SQLite + dashboard out).",
        parents=[common],
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", parents=[sub_common],
                           help="tail JSONL input(s) and run the HTTP dashboard")
    serve.add_argument("--input", action="append", required=True,
                       help="JSONL file to watch (repeatable)")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--poll-interval", type=float, default=0.2)

    advance = sub.add_parser("advance-watermark", parents=[sub_common],
                             help="explicitly advance the watermark and finalize due windows")
    advance.add_argument("--to", required=True,
                         help="target watermark as ISO 8601 UTC or epoch seconds")

    show = sub.add_parser("show", parents=[sub_common], help="print stored data as JSON")
    show.add_argument("what",
                      choices=["state", "windows", "versions", "late", "conflicts",
                               "invalid", "events", "watermark-log"])
    return parser


def _connect_processor(db_path: str) -> tuple:
    conn = connect(db_path)
    init_db(conn)
    return conn, EventProcessor(conn)


def cmd_serve(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("device_metrics.cli")
    conn, processor = _connect_processor(args.db)
    tailer = JsonlTailer(conn, processor, args.input, poll_interval=args.poll_interval)
    tailer.start()
    httpd = make_server(args.host, args.port, conn, processor)

    def _shutdown(signum, frame):
        # httpd.shutdown() must not be called from the serve_forever thread,
        # so the handler raises and the finally block below does the cleanup.
        raise KeyboardInterrupt

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _shutdown)

    log.info("dashboard: http://%s:%d/  (db=%s, inputs=%s)",
             args.host, args.port, args.db, ", ".join(args.input))
    try:
        httpd.serve_forever(poll_interval=0.3)
    except KeyboardInterrupt:
        log.info("shutdown signal received")
    finally:
        tailer.stop()
        tailer.join(timeout=5)
        httpd.server_close()
        conn.close()
        log.info("stopped; state persisted in %s, restart resumes automatically", args.db)
    return 0


def cmd_advance(args: argparse.Namespace) -> int:
    conn, processor = _connect_processor(args.db)
    try:
        target = parse_watermark_target(args.to)
        result = processor.advance_watermark(target)
        result["finalized_windows"] = [
            {"device_id": d, "window_minute": w} for d, w in result["finalized_windows"]
        ]
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    finally:
        conn.close()


def cmd_show(args: argparse.Namespace) -> int:
    conn, processor = _connect_processor(args.db)
    try:
        data = {
            "state": processor.get_state,
            "windows": processor.list_windows,
            "versions": processor.list_versions,
            "late": processor.list_late_events,
            "conflicts": processor.list_conflicts,
            "invalid": processor.list_invalid,
            "events": processor.list_events,
            "watermark-log": processor.watermark_log,
        }[args.what]()
        print(json.dumps(data, ensure_ascii=False, indent=2, default=str))
        return 0
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "serve":
        return cmd_serve(args)
    if args.command == "advance-watermark":
        return cmd_advance(args)
    if args.command == "show":
        return cmd_show(args)
    return 2  # pragma: no cover


if __name__ == "__main__":
    sys.exit(main())
