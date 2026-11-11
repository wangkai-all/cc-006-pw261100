"""Small stdlib-only HTTP service: dashboard page plus JSON APIs.

Endpoints
---------
GET  /                      HTML dashboard (windows, versions, late events ...)
GET  /api/state             watermark / processing state
GET  /api/windows           per-device per-minute metrics
GET  /api/versions          full metric version history
GET  /api/late              events that missed their finalized window
GET  /api/conflicts         same-id/different-content conflict records
GET  /api/invalid           rejected events with reasons
GET  /api/events            accepted events with duplicate counters
GET  /api/watermark-log     watermark movement audit trail
POST /api/advance-watermark?to=<iso8601|epoch>
                            explicit, monotonic watermark advance
GET  /healthz               liveness probe
"""

from __future__ import annotations

import html
import json
import logging
import sqlite3
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .models import EventValidationError, parse_watermark_target
from .processor import EventProcessor

log = logging.getLogger("device_metrics.server")


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "DeviceMetrics/1.0"

    # Injected by make_server:
    processor: EventProcessor = None  # type: ignore[assignment]
    db_path: str = ""  # type: ignore[assignment]

    def log_message(self, fmt: str, *args) -> None:  # quieter default logging
        log.info("%s - %s", self.address_string(), fmt % args)

    # ---------------------------------------------------------------- helpers

    def _send_json(self, payload, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, body: str, status: int = 200) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    # ---------------------------------------------------------------- routing

    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        parsed = urlparse(self.path)
        route = parsed.path
        try:
            if route == "/":
                return self._send_html(render_dashboard(self.processor))
            if route == "/healthz":
                return self._send_json({"ok": True})
            if route == "/api/state":
                return self._send_json(self.processor.get_state())
            if route == "/api/windows":
                return self._send_json(self.processor.list_windows())
            if route == "/api/versions":
                return self._send_json(self.processor.list_versions())
            if route == "/api/late":
                return self._send_json(self.processor.list_late_events())
            if route == "/api/conflicts":
                return self._send_json(self.processor.list_conflicts())
            if route == "/api/invalid":
                return self._send_json(self.processor.list_invalid())
            if route == "/api/events":
                return self._send_json(self.processor.list_events())
            if route == "/api/watermark-log":
                return self._send_json(self.processor.watermark_log())
            self._send_json({"error": "not found", "route": route}, status=404)
        except sqlite3.Error as exc:
            log.exception("database error on %s", route)
            self._send_json({"error": f"database error: {exc}"}, status=500)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/advance-watermark":
            self._send_json({"error": "not found", "route": parsed.path}, status=404)
            return
        try:
            query = parse_qs(parsed.query)
            target_text = query.get("to", [None])[0]
            if not target_text:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length).decode("utf-8") if length else ""
                if raw:
                    target_text = parse_qs(raw).get("to", [None])[0]
            if not target_text:
                self._send_json({"error": "missing 'to' query parameter (ISO 8601 UTC or epoch)"},
                                status=400)
                return
            target = parse_watermark_target(target_text)
            result = self.processor.advance_watermark(target)
            result["finalized_windows"] = [
                {"device_id": d, "window_minute": w} for d, w in result["finalized_windows"]
            ]
            self._send_json(result)
        except EventValidationError as exc:
            self._send_json({"error": str(exc)}, status=400)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=400)
        except sqlite3.Error as exc:
            log.exception("database error while advancing watermark")
            self._send_json({"error": f"database error: {exc}"}, status=500)


def make_server(host: str, port: int, conn: sqlite3.Connection,
                processor: EventProcessor) -> ThreadingHTTPServer:
    handler = type("BoundDashboardHandler", (DashboardHandler,),
                   {"processor": processor, "db_path": conn.execute(
                       "PRAGMA database_list").fetchall()[0]["file"] or ":memory:"})
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.processor = processor  # type: ignore[attr-defined]
    return httpd


# ------------------------------------------------------------------ dashboard

def _esc(value) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _badge(finalized: bool) -> str:
    if finalized:
        return '<span class="badge badge-final">已定稿 FINALIZED</span>'
    return '<span class="badge badge-open">开放 OPEN</span>'


def render_dashboard(processor: EventProcessor) -> str:
    state = processor.get_state()
    windows = processor.list_windows()
    versions = processor.list_versions()
    late = processor.list_late_events()
    conflicts = processor.list_conflicts()
    invalid = processor.list_invalid()
    events = processor.list_events()
    wm_log = processor.watermark_log()

    window_rows = "".join(
        f"<tr><td>{_esc(w['window_minute'])}</td><td>{_esc(w['device_id'])}</td>"
        f"<td class='num'>{w['count']}</td>"
        f"<td class='num'>{w['avg_reading']:.4f}</td>"
        f"<td class='num'>{w['max_reading']:.4f}</td>"
        f"<td class='num'>v{w['version']}</td><td>{_badge(w['finalized'])}</td>"
        f"<td>{_esc(w['finalized_at'] or '')}</td></tr>"
        for w in windows
    ) or _empty_row(8)

    version_rows = "".join(
        f"<tr><td>{_esc(v['window_minute'])}</td><td>{_esc(v['device_id'])}</td>"
        f"<td class='num'>v{v['version']}</td><td class='num'>{v['count']}</td>"
        f"<td class='num'>{v['avg_reading']:.4f}</td><td class='num'>{v['max_reading']:.4f}</td>"
        f"<td>{_esc(v['change_reason'])}</td><td>{_esc(v['event_id'] or '')}</td>"
        f"<td>{_esc(v['created_at'])}</td></tr>"
        for v in versions
    ) or _empty_row(9)

    late_rows = "".join(
        f"<tr><td>{_esc(e['window_minute'])}</td><td>{_esc(e['device_id'])}</td>"
        f"<td>{_esc(e['event_id'])}</td><td>{_esc(e['event_time'])}</td>"
        f"<td class='num'>{e['reading']:.4f}</td><td>{_esc(e['watermark_at_arrival'] or '')}</td>"
        f"<td>{_esc(e['received_at'])}</td></tr>"
        for e in late
    ) or _empty_row(7)

    conflict_rows = "".join(
        f"<tr><td>{_esc(c['event_id'])}</td><td>{_esc(c['device_id'] or '')}</td>"
        f"<td>{_esc(c['event_time'] or '')}</td><td class='num'>{_conflict_reading(c)}</td>"
        f"<td><code>{_esc(c['content_hash'][:12])}</code></td>"
        f"<td><code>{_esc(c['accepted_hash'][:12])}</code></td>"
        f"<td>{_esc(c['detected_at'])}</td></tr>"
        for c in conflicts
    ) or _empty_row(7)

    invalid_rows = "".join(
        f"<tr><td>{_esc(i['source_file'] or '')}</td><td>{_esc(i['reason'])}</td>"
        f"<td><code>{_esc(i['raw_payload'])}</code></td><td>{_esc(i['detected_at'])}</td></tr>"
        for i in invalid
    ) or _empty_row(4)

    event_rows = "".join(
        f"<tr><td>{_esc(e['event_id'])}</td><td>{_esc(e['device_id'])}</td>"
        f"<td>{_esc(e['event_time'])}</td><td class='num'>{e['reading']:.4f}</td>"
        f"<td>{_esc(e['window_minute'])}</td><td class='num'>{e['duplicate_count']}</td></tr>"
        for e in events
    ) or _empty_row(6)

    log_rows = "".join(
        f"<tr><td>{_esc(r['old_watermark'] or '∅')}</td><td>{_esc(r['new_watermark'])}</td>"
        f"<td>{_esc(r['reason'])}</td><td>{_esc(r['created_at'])}</td></tr>"
        for r in wm_log
    ) or _empty_row(4)

    suggested = ""
    wm = state.get("effective_watermark")
    if wm:
        try:
            suggested = (parse_watermark_target(wm).timestamp()) + 60
            suggested = str(int(suggested))
        except EventValidationError:
            suggested = ""

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>设备分钟指标 · 事件时间水位线控制台</title>
<style>
  :root {{ --bg:#0f1420; --panel:#171e2e; --line:#26314a; --fg:#e6ebf5;
           --muted:#8fa0bd; --accent:#5aa7ff; --green:#3fbf7f; --amber:#e0a13c; --red:#e06a6a; }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; background:var(--bg); color:var(--fg);
         font:14px/1.5 -apple-system, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; }}
  header {{ padding:20px 28px; border-bottom:1px solid var(--line);
            display:flex; flex-wrap:wrap; gap:18px; align-items:center; }}
  h1 {{ font-size:18px; margin:0; }}
  .cards {{ display:flex; gap:14px; flex-wrap:wrap; margin-left:auto; }}
  .card {{ background:var(--panel); border:1px solid var(--line); border-radius:10px;
           padding:10px 16px; min-width:170px; }}
  .card .k {{ color:var(--muted); font-size:12px; }}
  .card .v {{ font-size:15px; font-weight:600; margin-top:2px; word-break:break-all; }}
  main {{ padding:22px 28px 60px; display:grid; gap:22px; }}
  section {{ background:var(--panel); border:1px solid var(--line); border-radius:12px;
             padding:16px 18px; overflow-x:auto; }}
  h2 {{ font-size:15px; margin:0 0 12px; display:flex; gap:10px; align-items:center; }}
  h2 .hint {{ color:var(--muted); font-weight:400; font-size:12px; }}
  table {{ border-collapse:collapse; width:100%; font-size:13px; }}
  th, td {{ text-align:left; padding:7px 10px; border-bottom:1px solid var(--line);
            white-space:nowrap; }}
  th {{ color:var(--muted); font-weight:600; }}
  td.num, th.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
  code {{ color:var(--amber); }}
  .badge {{ padding:2px 9px; border-radius:999px; font-size:12px; font-weight:600; }}
  .badge-open {{ background:rgba(90,167,255,.15); color:var(--accent); }}
  .badge-final {{ background:rgba(63,191,127,.15); color:var(--green); }}
  .empty {{ color:var(--muted); text-align:center; }}
  form {{ display:flex; gap:10px; flex-wrap:wrap; align-items:center; }}
  input[name=to] {{ background:#0c1120; border:1px solid var(--line); color:var(--fg);
                    border-radius:8px; padding:8px 12px; width:320px; }}
  button {{ background:var(--accent); color:#08111f; border:0; border-radius:8px;
            padding:8px 18px; font-weight:700; cursor:pointer; }}
  button:hover {{ filter:brightness(1.1); }}
  .api {{ color:var(--muted); font-size:12px; margin-top:10px; }}
  .api code {{ color:var(--accent); }}
</style>
</head>
<body>
<header>
  <h1>设备数据处理服务 · 分钟窗口控制台</h1>
  <div class="cards">
    <div class="card"><div class="k">有效事件最大时间</div>
      <div class="v">{_esc(state.get('max_event_ts') or '∅')}</div></div>
    <div class="card"><div class="k">当前有效水位线 (max-30s / 显式推进)</div>
      <div class="v">{_esc(state.get('effective_watermark') or '∅')}</div></div>
    <div class="card"><div class="k">显式覆盖值</div>
      <div class="v">{_esc(state.get('watermark_override') or '∅')}</div></div>
    <div class="card"><div class="k">窗口数 (开放/已定稿)</div>
      <div class="v">{sum(1 for w in windows if not w['finalized'])} / {sum(1 for w in windows if w['finalized'])}</div></div>
  </div>
</header>
<main>
  <section>
    <h2>显式推进水位线 <span class="hint">演示结束入口：定稿条件 = 水位线 ≥ 窗口结束 + 60s；只允许单调向前</span></h2>
    <form id="wm-form">
      <input name="to" placeholder="ISO 8601 UTC，例如 2026-11-11T10:03:00Z；或 epoch 秒"
             value="{_esc(suggested)}">
      <button type="submit">推进水位线并定稿到期窗口</button>
      <span id="wm-result" class="api"></span>
    </form>
    <div class="api">也可用 API：<code>curl -X POST 'http://HOST/api/advance-watermark?to=2026-11-11T10:03:00Z'</code>
      ；页面每 5 秒自动刷新</div>
    <script>
    document.getElementById('wm-form').addEventListener('submit', async function (ev) {{
      ev.preventDefault();
      const out = document.getElementById('wm-result');
      const to = new FormData(this).get('to');
      out.textContent = '推进中…';
      try {{
        const resp = await fetch('/api/advance-watermark?to=' + encodeURIComponent(to),
                                 {{ method: 'POST' }});
        const data = await resp.json();
        if (!resp.ok) {{ out.textContent = '失败：' + (data.error || resp.status); return; }}
        const fins = (data.finalized_windows || [])
            .map(w => w.device_id + ' ' + w.window_minute).join('；') || '无新定稿窗口';
        out.textContent = '有效水位线 → ' + data.effective_watermark + '；新定稿：' + fins
            + (data.advanced ? '' : '（' + data.reason + '）');
        setTimeout(() => location.reload(), 1200);
      }} catch (err) {{ out.textContent = '请求失败：' + err; }}
    }});
    </script>
  </section>

  <section>
    <h2>窗口指标 <span class="hint">按设备 + 事件发生分钟（UTC），非接收时间</span></h2>
    <table><thead><tr>
      <th>分钟窗口(UTC)</th><th>设备</th><th class="num">有效读数</th>
      <th class="num">平均值</th><th class="num">最高值</th><th class="num">版本</th>
      <th>状态</th><th>定稿时间</th></tr></thead><tbody>{window_rows}</tbody></table>
  </section>

  <section>
    <h2>指标版本历史 <span class="hint">定稿前每次迟到修正都会新增版本；定稿后不再变化</span></h2>
    <table><thead><tr>
      <th>分钟窗口</th><th>设备</th><th class="num">版本</th><th class="num">数量</th>
      <th class="num">平均值</th><th class="num">最高值</th><th>变更原因</th>
      <th>触发事件</th><th>记录时间</th></tr></thead><tbody>{version_rows}</tbody></table>
  </section>

  <section>
    <h2>迟到数据（定稿后到达，单独保存）<span class="hint">共 {len(late)} 条，不回改已定稿指标</span></h2>
    <table><thead><tr>
      <th>所属窗口</th><th>设备</th><th>事件 ID</th><th>事件时间(UTC)</th>
      <th class="num">读数</th><th>到达时水位线</th><th>接收时间</th></tr></thead>
      <tbody>{late_rows}</tbody></table>
  </section>

  <section>
    <h2>冲突记录 <span class="hint">相同事件 ID 不同内容：保留第一次接受的读数</span></h2>
    <table><thead><tr>
      <th>事件 ID</th><th>设备</th><th>事件时间</th><th class="num">冲突读数</th>
      <th>冲突内容哈希</th><th>已接受哈希</th><th>检测时间</th></tr></thead>
      <tbody>{conflict_rows}</tbody></table>
  </section>

  <section>
    <h2>无效事件 <span class="hint">不推动水位线、不进入指标</span></h2>
    <table><thead><tr><th>来源文件</th><th>拒绝原因</th><th>原始内容</th><th>记录时间</th></tr></thead>
      <tbody>{invalid_rows}</tbody></table>
  </section>

  <section>
    <h2>已接受事件与去重计数</h2>
    <table><thead><tr><th>事件 ID</th><th>设备</th><th>事件时间</th>
      <th class="num">读数</th><th>所属窗口</th><th class="num">重复次数</th></tr></thead>
      <tbody>{event_rows}</tbody></table>
  </section>

  <section>
    <h2>水位线变更审计</h2>
    <table><thead><tr><th>旧水位线</th><th>新水位线</th><th>原因</th><th>时间</th></tr></thead>
      <tbody>{log_rows}</tbody></table>
  </section>
</main>
</body>
</html>"""


def _empty_row(cols: int) -> str:
    return f'<tr><td colspan="{cols}" class="empty">（暂无数据）</td></tr>'


def _conflict_reading(conflict: dict) -> str:
    reading = conflict["reading"]
    return "" if reading is None else f"{reading:.4f}"
