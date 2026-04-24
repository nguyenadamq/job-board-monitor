import os
from datetime import datetime, timezone

from aiohttp import web

from status_monitor import STATUS_DB_PATH, get_dashboard_snapshot


DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8080"))


def format_ts(ts: int | None) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Job Tracker Monitor</title>
  <style>
    :root {
      color-scheme: light;
      --paper:#f6f1e8;
      --card:#fffdf8;
      --ink:#201d1a;
      --muted:#6e675f;
      --ok:#25643b;
      --err:#a63232;
      --accent:#0d5c63;
      --accent-soft:#d8eeef;
      --border:#dfd3c0;
      --shadow:0 10px 28px rgba(36, 24, 12, 0.06);
    }
    * { box-sizing:border-box; }
    body {
      margin:0;
      font-family: Georgia, "Times New Roman", serif;
      background:
        radial-gradient(circle at top left, #efe4d0 0%, transparent 32%),
        linear-gradient(180deg, #efe6d9 0%, var(--paper) 38%, #fbf8f3 100%);
      color:var(--ink);
    }
    main { max-width:1280px; margin:0 auto; padding:28px 24px 48px; }
    h1, h2, h3 { margin:0; }
    .hero { margin-bottom:22px; }
    .sub { color:var(--muted); margin-top:8px; }
    .service-grid {
      display:grid;
      grid-template-columns:repeat(auto-fit,minmax(240px,1fr));
      gap:16px;
      margin-bottom:18px;
    }
    .card, .service-card, .section-card, .board-card {
      background:var(--card);
      border:1px solid var(--border);
      border-radius:18px;
      box-shadow:var(--shadow);
    }
    .service-card { padding:18px; }
    .service-head {
      display:flex;
      justify-content:space-between;
      align-items:flex-start;
      gap:12px;
      margin-bottom:10px;
    }
    .pill {
      display:inline-flex;
      align-items:center;
      border-radius:999px;
      padding:4px 10px;
      font-size:12px;
      font-weight:700;
      letter-spacing:0.04em;
      text-transform:uppercase;
    }
    .pill-ok { background:#e6f4e8; color:var(--ok); }
    .pill-error { background:#fae4e1; color:var(--err); }
    .muted { color:var(--muted); }
    .mini-code {
      margin-top:10px;
      font-family:Consolas, monospace;
      font-size:12px;
      color:#4b463f;
      background:#f7f2ea;
      border-radius:12px;
      padding:10px;
      white-space:pre-wrap;
    }
    .graph-grid {
      display:grid;
      grid-template-columns:repeat(auto-fit,minmax(320px,1fr));
      gap:18px;
      margin:20px 0;
    }
    .graph-card {
      padding:18px;
      min-height:280px;
    }
    .graph-meta {
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:12px;
      margin:10px 0 14px;
      color:var(--muted);
      font-size:13px;
    }
    .chart-wrap {
      background:linear-gradient(180deg, #fbf7f1 0%, #f5efe6 100%);
      border:1px solid var(--border);
      border-radius:16px;
      padding:12px;
    }
    .legend {
      display:flex;
      gap:14px;
      font-size:13px;
      margin-top:10px;
      color:var(--muted);
      flex-wrap:wrap;
    }
    .dot {
      display:inline-block;
      width:10px;
      height:10px;
      border-radius:999px;
      margin-right:6px;
    }
    .dot-err { background:var(--err); }
    .dot-ok { background:var(--accent); }
    .section-card {
      padding:18px;
      margin-bottom:18px;
    }
    table {
      width:100%;
      border-collapse:collapse;
      font-size:14px;
    }
    th, td {
      text-align:left;
      padding:10px 8px;
      border-bottom:1px solid var(--border);
      vertical-align:top;
    }
    th {
      font-size:12px;
      text-transform:uppercase;
      letter-spacing:0.06em;
      color:var(--muted);
    }
    code {
      font-family:Consolas, monospace;
      font-size:12px;
      background:#f7f2ea;
      padding:2px 6px;
      border-radius:8px;
      word-break:break-word;
    }
    .boards {
      display:grid;
      grid-template-columns:1fr;
      gap:16px;
    }
    .board-card {
      overflow:hidden;
    }
    .board-toggle {
      width:100%;
      border:none;
      background:transparent;
      padding:18px;
      cursor:pointer;
      text-align:left;
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:12px;
    }
    .board-title {
      display:flex;
      flex-direction:column;
      gap:4px;
    }
    .board-summary {
      display:flex;
      gap:10px;
      align-items:center;
      flex-wrap:wrap;
      color:var(--muted);
      font-size:13px;
    }
    .chev {
      font-size:20px;
      color:var(--accent);
      transition:transform 0.2s ease;
    }
    .board-card.open .chev { transform:rotate(90deg); }
    .board-body {
      display:none;
      padding:0 18px 18px;
      border-top:1px solid var(--border);
      background:linear-gradient(180deg, rgba(13,92,99,0.03), rgba(13,92,99,0));
    }
    .board-card.open .board-body { display:block; }
    .nested-list {
      display:grid;
      grid-template-columns:repeat(auto-fit,minmax(260px,1fr));
      gap:12px;
      margin-top:16px;
    }
    .nested-card {
      background:#fffaf2;
      border:1px solid var(--border);
      border-radius:14px;
      padding:14px;
    }
    .nested-card h3 {
      font-size:15px;
      margin-bottom:8px;
      word-break:break-word;
    }
    .detail {
      color:var(--muted);
      font-size:13px;
      line-height:1.45;
      margin-top:8px;
      white-space:pre-wrap;
      word-break:break-word;
    }
    .empty {
      color:var(--muted);
      padding:18px 0 4px;
    }
    @media (max-width: 700px) {
      main { padding:20px 14px 36px; }
      .graph-grid, .service-grid, .nested-list { grid-template-columns:1fr; }
      .board-toggle, .section-card, .service-card, .graph-card { padding:15px; }
    }
  </style>
</head>
<body>
  <main>
    <div class="hero">
      <h1>Job Tracker Monitor</h1>
      <div class="sub">Live monitor for Ashby, Lever, and Greenhouse. Trends refresh every 5 seconds and the board cards expand into source-level error details.</div>
    </div>

    <div id="services" class="service-grid"></div>

    <div class="graph-grid" id="graphs"></div>

    <div class="section-card">
      <h2>Boards With Error Details</h2>
      <div class="sub">Expand a board to inspect the exact companies or slugs currently returning errors.</div>
      <div id="board-errors" class="boards"></div>
    </div>

    <div class="section-card">
      <h2>Recent Events</h2>
      <table>
        <thead><tr><th>When</th><th>Board</th><th>Source</th><th>Status</th><th>Detail</th></tr></thead>
        <tbody id="recent-events"></tbody>
      </table>
    </div>
  </main>
  <script>
    const openBoards = new Set();

    function pill(status) {
      const lowered = String(status || "").toLowerCase();
      const error = lowered === "error" || lowered.startsWith("error") || lowered.startsWith("exception") || lowered.startsWith("timeout") || lowered.startsWith("http_") || lowered.startsWith("gql_error");
      return `<span class="pill ${error ? "pill-error" : "pill-ok"}">${esc(status)}</span>`;
    }

    function esc(value) {
      return String(value ?? "").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");
    }

    function chartSvg(points) {
      const width = 520;
      const height = 200;
      const pad = 18;
      const maxY = Math.max(1, ...points.map(point => Math.max(point.error_count, point.ok_count)));
      const x = (index) => points.length <= 1 ? width / 2 : pad + (index * (width - pad * 2) / (points.length - 1));
      const y = (value) => height - pad - ((value / maxY) * (height - pad * 2));
      const line = (key) => points.map((point, index) => `${index === 0 ? "M" : "L"} ${x(index).toFixed(2)} ${y(point[key]).toFixed(2)}`).join(" ");
      const area = points.map((point, index) => `${index === 0 ? "M" : "L"} ${x(index).toFixed(2)} ${y(point.error_count).toFixed(2)}`).join(" ")
        + ` L ${x(points.length - 1).toFixed(2)} ${(height - pad).toFixed(2)} L ${x(0).toFixed(2)} ${(height - pad).toFixed(2)} Z`;
      const grid = Array.from({length: 4}, (_, index) => {
        const value = Math.round((maxY / 4) * (index + 1));
        const yy = y(value);
        return `<line x1="${pad}" y1="${yy}" x2="${width - pad}" y2="${yy}" stroke="#ddd3c3" stroke-dasharray="4 5" />
                <text x="${pad}" y="${yy - 4}" fill="#8a8178" font-size="11">${value}</text>`;
      }).join("");
      const labels = points.filter((_, index) => index === 0 || index === points.length - 1 || index === Math.floor(points.length / 2)).map((point, index, arr) => {
        const sourceIndex = index === 0 ? 0 : index === arr.length - 1 ? points.length - 1 : Math.floor(points.length / 2);
        const xx = x(sourceIndex);
        return `<text x="${xx}" y="${height}" text-anchor="middle" fill="#8a8178" font-size="11">${esc(point.label)}</text>`;
      }).join("");
      return `
        <svg viewBox="0 0 ${width} ${height + 8}" width="100%" height="220" role="img" aria-label="error trend chart">
          ${grid}
          <path d="${area}" fill="rgba(166,50,50,0.12)"></path>
          <path d="${line("error_count")}" fill="none" stroke="#a63232" stroke-width="3" stroke-linecap="round"></path>
          <path d="${line("ok_count")}" fill="none" stroke="#0d5c63" stroke-width="2.5" stroke-linecap="round"></path>
          ${points.map((point, index) => `<circle cx="${x(index)}" cy="${y(point.error_count)}" r="3.5" fill="#a63232"></circle>`).join("")}
          ${points.map((point, index) => `<circle cx="${x(index)}" cy="${y(point.ok_count)}" r="3" fill="#0d5c63"></circle>`).join("")}
          ${labels}
        </svg>
      `;
    }

    function renderGraphs(historyByService) {
      const entries = Object.entries(historyByService);
      if (!entries.length) {
        document.getElementById("graphs").innerHTML = '<div class="graph-card card"><h2>Error Trends</h2><div class="empty">No cycle history yet.</div></div>';
        return;
      }
      document.getElementById("graphs").innerHTML = entries.map(([service, points]) => {
        const last = points[points.length - 1] || {};
        const chartPoints = points.slice(-20).map(point => ({
          ...point,
          label: point.last_cycle_short || "-"
        }));
        return `
          <div class="graph-card card">
            <h2>${esc(service)} Error Trend</h2>
            <div class="graph-meta">
              <span>Last ${chartPoints.length} cycles</span>
              <span>Latest errors: <strong>${esc(last.error_count ?? 0)}</strong></span>
            </div>
            <div class="chart-wrap">${chartSvg(chartPoints)}</div>
            <div class="legend">
              <span><span class="dot dot-err"></span>Error sources</span>
              <span><span class="dot dot-ok"></span>Healthy sources</span>
            </div>
          </div>
        `;
      }).join("");
    }

    function renderBoardErrors(services, errorGroups) {
      const boards = services.map(service => {
        const errors = errorGroups[service.service] || [];
        const isOpen = openBoards.has(service.service);
        return `
          <div class="board-card ${isOpen ? "open" : ""}" data-board="${esc(service.service)}">
            <button class="board-toggle" type="button" onclick="toggleBoard('${esc(service.service)}')">
              <div class="board-title">
                <h2>${esc(service.service)}</h2>
                <div class="board-summary">
                  ${pill(service.status)}
                  <span>${esc(errors.length)} active source error${errors.length === 1 ? "" : "s"}</span>
                  <span>Last cycle ${esc(service.last_cycle_display)}</span>
                </div>
              </div>
              <span class="chev">›</span>
            </button>
            <div class="board-body">
              ${errors.length ? `
                <div class="nested-list">
                  ${errors.map(row => `
                    <div class="nested-card">
                      <h3><code>${esc(row.source)}</code></h3>
                      <div>${pill(row.status)}</div>
                      <div class="detail">${esc(row.detail || "-")}</div>
                      <div class="detail">Last error: ${esc(row.last_error_display)}</div>
                      <div class="detail">Consecutive errors: ${esc(row.consecutive_errors)}</div>
                    </div>
                  `).join("")}
                </div>
              ` : '<div class="empty">No active source errors on this board right now.</div>'}
            </div>
          </div>
        `;
      }).join("");
      document.getElementById("board-errors").innerHTML = boards || '<div class="empty">No board data yet.</div>';
    }

    function renderServices(services) {
      document.getElementById("services").innerHTML = services.map(service => `
        <div class="service-card">
          <div class="service-head">
            <div>
              <h2>${esc(service.service)}</h2>
              <div class="muted">Last cycle ${esc(service.last_cycle_display)}</div>
            </div>
            ${pill(service.status)}
          </div>
          <div class="muted">Duration ${esc(service.cycle_duration_ms)} ms</div>
          <div class="mini-code">${esc(JSON.stringify(service.summary))}</div>
        </div>
      `).join("") || '<div class="service-card">No cycle data yet.</div>';
    }

    function renderRecentEvents(events) {
      document.getElementById("recent-events").innerHTML = events.map(row => `
        <tr>
          <td>${esc(row.created_display)}</td>
          <td>${esc(row.service)}</td>
          <td><code>${esc(row.source)}</code></td>
          <td>${pill(row.status)}</td>
          <td>${esc(row.detail || "-")}</td>
        </tr>
      `).join("") || '<tr><td colspan="5">No events yet.</td></tr>';
    }

    function toggleBoard(service) {
      if (openBoards.has(service)) {
        openBoards.delete(service);
      } else {
        openBoards.add(service);
      }
      const card = document.querySelector(`[data-board="${CSS.escape(service)}"]`);
      if (card) {
        card.classList.toggle("open");
      }
    }

    async function refresh() {
      const res = await fetch("/api/status");
      const data = await res.json();
      renderServices(data.services);
      renderGraphs(data.history_by_service);
      renderBoardErrors(data.services, data.error_groups);
      renderRecentEvents(data.recent_events);
    }

    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>"""


async def index(_: web.Request) -> web.Response:
    return web.Response(text=HTML, content_type="text/html")


async def api_status(_: web.Request) -> web.Response:
    snapshot = get_dashboard_snapshot()
    for service in snapshot["services"]:
        service["last_cycle_display"] = format_ts(service.get("last_cycle_ts"))
    for row in snapshot["active_errors"]:
        row["last_error_display"] = format_ts(row.get("last_error_ts"))
    for row in snapshot["recent_events"]:
        row["created_display"] = format_ts(row.get("created_ts"))
    for history_rows in snapshot["history_by_service"].values():
        for row in history_rows:
            row["last_cycle_display"] = format_ts(row.get("last_cycle_ts"))
            row["last_cycle_short"] = datetime.fromtimestamp(
                row["last_cycle_ts"], tz=timezone.utc
            ).astimezone().strftime("%H:%M:%S")
    snapshot["status_db"] = str(STATUS_DB_PATH)
    return web.json_response(snapshot)


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/api/status", api_status)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host=DASHBOARD_HOST, port=DASHBOARD_PORT)
