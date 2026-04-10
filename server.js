import express from "express";
import dotenv from "dotenv";
import twilio from "twilio";
import { createClient } from "@supabase/supabase-js";
import OpenAI from "openai";

dotenv.config();

console.log("OPENAI KEY LOADED:", !!process.env.OPENAI_API_KEY);

const app = express();
const port = process.env.PORT || 3000;
const DASHBOARD_ORG_ID = 1;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error("Missing SUPABASE_URL or usable Supabase key in .env");
}

const openai = process.env.OPENAI_API_KEY
  ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
  : null;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

app.use(
  express.urlencoded({
    extended: true,
    verify: (req, _res, buf) => {
      req.rawBody = buf.toString("utf8");
    },
  }),
);

const RATE_LIMIT_WINDOW_MS = 60 * 1000;
const RATE_LIMIT_MAX_REQUESTS = 30;
const rateLimitStore = new Map();
const APP_TIMEZONE = "Asia/Kolkata";
const APP_TIMEZONE_OFFSET = "+05:30";
const DEFAULT_SHIFT_START_TEXT = "10:30 AM";
const LATE_APPROVAL_NOTICE_HOURS = 3;

// Attendance day settings
const ATTENDANCE_DAY_START_HOUR = 6; // 6:00 AM IST
const LONG_SHIFT_THRESHOLD_MIN = 10 * 60; // 10 hours
const LONG_BREAK_THRESHOLD_MIN = 2 * 60; // 2 hours
const HALF_DAY_THRESHOLD_MIN = 4 * 60; // optional future use

function normalizeText(text) {
  return String(text || "")
    .trim()
    .toLowerCase();
}

app.use(express.json());

function sendTwiml(res, message) {
  try {
    console.log("sendTwiml called");
    console.log("sendTwiml message preview:", String(message).slice(0, 300));

    const twiml = new twilio.twiml.MessagingResponse();

    if (message && String(message).trim()) {
      twiml.message(String(message));
    }

    const xml = twiml.toString();
    console.log("sendTwiml xml preview:", xml.slice(0, 300));

    res.writeHead(200, { "Content-Type": "text/xml" });
    res.end(xml);

    console.log("sendTwiml response sent");
    return;
  } catch (err) {
    console.error("sendTwiml failed:", err);
    try {
      res.status(500).send("Internal Server Error");
    } catch (e) {
      console.error("Failed sending 500 response:", e);
    }
  }
}

function sendEmptyTwiml(res) {
  res.status(200).type("text/xml").send("<Response></Response>");
}

function sendApiSuccess(res, data) {
  return res.status(200).json({ ok: true, data });
}

function sendApiError(res, status, message) {
  return res.status(status).json({ ok: false, error: message });
}

function safeParseJson(text) {
  if (!text) return null;

  let cleaned = text.trim();
  cleaned = cleaned.replace(/^```json\s*/i, "").replace(/^```\s*/i, "");
  cleaned = cleaned.replace(/\s*```$/, "");

  try {
    return JSON.parse(cleaned);
  } catch {
    console.error("Failed to parse AI JSON:", cleaned);
    return null;
  }
}

function parseDeadlineCommand(text) {
  const raw = normalizeText(text);
  const match = raw.match(/^deadline\s+(\d+)\s+(.+)$/i);
  if (!match) return null;

  return {
    taskId: Number(match[1]),
    dateText: match[2].trim(),
  };
}

function renderStage0BugBoardPage(data) {
  const summary = data?.summary || {};
  const columns = data?.columns || [];
  const users = data?.users || [];

  const columnHtml = columns
    .map((column) => {
      const cardsHtml = (column.items || []).length
        ? column.items
            .map((bug) => {
              return `
                <div class="bug-card" data-id="${escapeHtml(bug.id)}">
                  <div class="bug-top">
                    <div class="bug-id">#${escapeHtml(bug.id)}</div>
                    <div class="bug-badges">
                      <span class="${bugSeverityBadgeClass(bug.severity)}">${escapeHtml(bug.severity)}</span>
                      <span class="${bugStatusBadgeClass(bug.status)}">${escapeHtml(bug.status)}</span>
                    </div>
                  </div>

                  <div class="bug-title">${escapeHtml(bug.title)}</div>

                  ${
                    bug.description
                      ? `<div class="bug-desc">${escapeHtml(bug.description)}</div>`
                      : ""
                  }

                  <div class="bug-meta">
                    <div><strong>Assignee:</strong> ${escapeHtml(bug.assigned_to_name || "-")}</div>
                    <div><strong>Created by:</strong> ${escapeHtml(bug.created_by_name || "-")}</div>
                    <div><strong>Created:</strong> ${escapeHtml(bug.created_at_text || "-")}</div>
                  </div>

                  ${
                    bug.source_message_sid ||
                    bug.source_phone_number ||
                    bug.source_message_text
                      ? `
                        <div class="bug-source">
                          ${bug.source_message_sid ? `<div><strong>SID:</strong> ${escapeHtml(bug.source_message_sid)}</div>` : ""}
                          ${bug.source_phone_number ? `<div><strong>Phone:</strong> ${escapeHtml(bug.source_phone_number)}</div>` : ""}
                          ${bug.source_message_text ? `<div><strong>Message:</strong> ${escapeHtml(bug.source_message_text)}</div>` : ""}
                        </div>
                      `
                      : ""
                  }

                  <div class="bug-actions" style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
                    <select onchange="updateBug(${bug.id}, { board_column: this.value })"
                      style="padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                      ${STAGE0_BUG_COLUMNS.map(
                        (col) => `
                        <option value="${escapeHtml(col)}" ${bug.board_column === col ? "selected" : ""}>${escapeHtml(col)}</option>
                      `,
                      ).join("")}
                    </select>

                    <select onchange="updateBug(${bug.id}, { severity: this.value })"
                      style="padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                      ${["P0", "P1", "P2"]
                        .map(
                          (sev) => `
                        <option value="${sev}" ${bug.severity === sev ? "selected" : ""}>${sev}</option>
                      `,
                        )
                        .join("")}
                    </select>

                    <select onchange="updateBug(${bug.id}, { status: this.value })"
                      style="padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                      ${["open", "in_progress", "blocked", "done"]
                        .map(
                          (st) => `
                        <option value="${st}" ${bug.status === st ? "selected" : ""}>${st}</option>
                      `,
                        )
                        .join("")}
                    </select>

                    <select onchange="updateBug(${bug.id}, { assigned_to_user_id: this.value || null })"
                      style="padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                      <option value="">Unassigned</option>
                      ${users
                        .map(
                          (u) => `
                        <option value="${u.id}" ${String(bug.assigned_to_user_id || "") === String(u.id) ? "selected" : ""}>${escapeHtml(u.name)}</option>
                      `,
                        )
                        .join("")}
                    </select>
                  </div>
                </div>
              `;
            })
            .join("")
        : `<div class="empty-col">No bugs here</div>`;

      return `
        <div class="board-col">
          <div class="board-col-head">
            <div class="board-col-title">${escapeHtml(column.name)}</div>
            <div class="board-col-count">${escapeHtml(column.count)}</div>
          </div>
          <div class="board-col-body">
            ${cardsHtml}
          </div>
        </div>
      `;
    })
    .join("");

  return `
    <html>
      <head>
        <title>Stage 0 Bug Board</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}

          .wrap { max-width: 1600px; margin: 0 auto; padding: 24px 18px 36px; }
          .topbar, .panel, .stat-card, .board-col, .bug-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
          }
          .topbar {
            display: flex; justify-content: space-between; align-items: center;
            gap: 16px; flex-wrap: wrap; margin-bottom: 20px; padding: 18px 20px;
          }
          .eyebrow {
            font-size: 11px; letter-spacing: 0.16em; text-transform: uppercase;
            color: var(--primary); font-weight: 700; margin-bottom: 8px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }
          h1 { margin: 0; font-size: 30px; letter-spacing: -0.04em; }
          .subtitle { color: var(--muted); margin-top: 8px; font-size: 14px; }
          .links { display: flex; gap: 10px; flex-wrap: wrap; }
          .links a {
            color: var(--text); text-decoration: none; padding: 10px 14px;
            border-radius: 12px; border: 1px solid color-mix(in srgb, var(--secondary) 30%, transparent);
            background: var(--secondary-soft); font-weight: 600;
          }
          .stats {
            display: grid; grid-template-columns: repeat(7, minmax(0, 1fr));
            gap: 12px; margin-bottom: 20px;
          }
          .stat-card { padding: 14px; }
          .stat-label {
            color: var(--muted); font-size: 12px; text-transform: uppercase;
            letter-spacing: 0.08em; font-weight: 700;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }
          .stat-value { margin-top: 10px; font-size: 28px; font-weight: 700; }
          .board {
            display: grid;
            grid-template-columns: repeat(7, minmax(250px, 1fr));
            gap: 14px;
            align-items: start;
            overflow-x: auto;
          }
          .board-col { min-height: 300px; display: flex; flex-direction: column; }
          .board-col-head {
            padding: 14px 14px 10px;
            border-bottom: 1px solid rgba(255,255,255,0.08);
            display: flex; align-items: center; justify-content: space-between; gap: 10px;
          }
          .board-col-title { font-size: 14px; font-weight: 700; }
          .board-col-count {
            min-width: 28px; height: 28px; border-radius: 999px;
            display: grid; place-items: center;
            background: var(--primary-soft);
            border: 1px solid rgba(255,255,255,0.08);
            font-size: 12px; font-weight: 700;
          }
          .board-col-body { padding: 12px; display: flex; flex-direction: column; gap: 12px; }
          .bug-card { padding: 12px; }
          .bug-top {
            display: flex; align-items: center; justify-content: space-between;
            gap: 8px; margin-bottom: 10px;
          }
          .bug-id {
            color: var(--muted); font-size: 12px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }
          .bug-badges { display: flex; gap: 8px; flex-wrap: wrap; }
          .bug-title { font-weight: 700; margin-bottom: 8px; line-height: 1.35; }
          .bug-desc {
            color: var(--muted); font-size: 13px; line-height: 1.5;
            margin-bottom: 10px; white-space: pre-wrap;
          }
          .bug-meta, .bug-source { color: var(--muted); font-size: 12px; line-height: 1.5; }
          .bug-source {
            margin-top: 10px; padding-top: 10px;
            border-top: 1px dashed rgba(255,255,255,0.08);
          }
          .empty-col {
            color: var(--muted); text-align: center; padding: 20px 12px;
            border: 1px dashed rgba(255,255,255,0.12); border-radius: 12px;
          }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Stage 0 Stability</div>
              <h1>Bug Board</h1>
              <div class="subtitle">Parsing, idempotency, Twilio, DB failures, dashboard/logs, infra, unknown issues.</div>
            </div>
            <div class="links">
              <a href="/dashboard">Dashboard</a>
              <a href="/tasks">Tasks</a>
              <a href="/attendance">Attendance</a>
              <a href="/logs">Logs</a>
              <a href="/bugs">Bug Board</a>
              <a href="/reports">Reports</a>
            </div>
          </div>

          <div class="stats">
            <div class="stat-card"><div class="stat-label">Total</div><div class="stat-value">${escapeHtml(summary.total ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">P0</div><div class="stat-value">${escapeHtml(summary.p0 ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">P1</div><div class="stat-value">${escapeHtml(summary.p1 ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">P2</div><div class="stat-value">${escapeHtml(summary.p2 ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">Open</div><div class="stat-value">${escapeHtml(summary.open ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">In Progress</div><div class="stat-value">${escapeHtml(summary.in_progress ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">Blocked</div><div class="stat-value">${escapeHtml(summary.blocked ?? 0)}</div></div>
          </div>

          <div class="panel" style="margin-bottom: 18px; padding: 16px;">
            <h2 style="margin-top:0;">Create bug</h2>
            <div style="display:grid; grid-template-columns: 2fr 1.2fr 1fr 1fr; gap:10px; margin-bottom:10px;">
              <input id="bugTitle" placeholder="Bug title" style="padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);" />
              <select id="bugColumn" style="padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                ${STAGE0_BUG_COLUMNS.map((x) => `<option value="${escapeHtml(x)}">${escapeHtml(x)}</option>`).join("")}
              </select>
              <select id="bugSeverity" style="padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                <option value="P0">P0</option>
                <option value="P1">P1</option>
                <option value="P2">P2</option>
              </select>
              <button onclick="createBug()" style="padding:10px 14px; border-radius:10px; border:1px solid var(--line); background:var(--primary-soft); color:var(--text); font-weight:700;">Create</button>
            </div>

            <textarea id="bugDescription" placeholder="Description" style="width:100%; min-height:90px; padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);"></textarea>

            <div style="display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-top:10px;">
              <input id="bugSourceSid" placeholder="Source Message SID (optional)" style="padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);" />
              <input id="bugSourcePhone" placeholder="Source Phone (optional)" style="padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);" />
            </div>
            <textarea id="bugSourceText" placeholder="Source message text (optional)" style="width:100%; min-height:70px; margin-top:10px; padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);"></textarea>
          </div>

          <div class="board">
            ${columnHtml}
          </div>
        </div>

        <script>
          async function createBug() {
            const title = document.getElementById("bugTitle").value.trim();
            const description = document.getElementById("bugDescription").value.trim();
            const board_column = document.getElementById("bugColumn").value;
            const severity = document.getElementById("bugSeverity").value;
            const source_message_sid = document.getElementById("bugSourceSid").value.trim();
            const source_phone_number = document.getElementById("bugSourcePhone").value.trim();
            const source_message_text = document.getElementById("bugSourceText").value.trim();

            if (!title) {
              alert("Title is required");
              return;
            }

            const res = await fetch("/api/bugs", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                title,
                description,
                board_column,
                severity,
                source_message_sid,
                source_phone_number,
                source_message_text
              })
            });

            const json = await res.json();
            if (!json.ok) {
              alert(json.error || "Failed to create bug");
              return;
            }

            location.reload();
          }

          async function updateBug(id, patch) {
            const res = await fetch("/api/bugs/" + id, {
              method: "PATCH",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(patch)
            });

            const json = await res.json();
            if (!json.ok) {
              alert(json.error || "Failed to update bug");
              return;
            }

            location.reload();
          }
        </script>
      </body>
    </html>
  `;
}

function parseFlexibleDate(input) {
  const raw = String(input || "")
    .toLowerCase()
    .trim();

  if (raw === "today") {
    const d = new Date();
    d.setHours(0, 0, 0, 0);
    return d;
  }

  if (raw === "tomorrow") {
    const d = new Date();
    d.setDate(d.getDate() + 1);
    d.setHours(0, 0, 0, 0);
    return d;
  }

  const plusDaysMatch = raw.match(/^\+(\d+)\s+day(s)?$/i);
  if (plusDaysMatch) {
    const days = Number(plusDaysMatch[1]);
    if (!Number.isNaN(days) && days >= 0) {
      const d = new Date();
      d.setDate(d.getDate() + days);
      d.setHours(0, 0, 0, 0);
      return d;
    }
  }

  // YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const [year, month, day] = raw.split("-").map(Number);
    const d = new Date(year, month - 1, day);
    if (!Number.isNaN(d.getTime())) {
      d.setHours(0, 0, 0, 0);
      return d;
    }
  }

  // "5 Apr"
  const parts = raw.split(/\s+/);
  if (parts.length === 2) {
    const day = parseInt(parts[0], 10);
    const monthStr = parts[1].slice(0, 3);

    const months = {
      jan: 0,
      feb: 1,
      mar: 2,
      apr: 3,
      may: 4,
      jun: 5,
      jul: 6,
      aug: 7,
      sep: 8,
      oct: 9,
      nov: 10,
      dec: 11,
    };

    const month = months[monthStr];
    if (!Number.isNaN(day) && month !== undefined) {
      const now = new Date();
      const d = new Date(now.getFullYear(), month, day);
      d.setHours(0, 0, 0, 0);
      return d;
    }
  }

  return null;
}

function isManagerOrAdmin(user) {
  return user?.role === "admin" || user?.role === "manager";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

const UI_THEME = {
  bg0: "#151a2e",
  bg1: "#1b2238",
  bg2: "#242c47",

  text: "#f3f6ff",
  textStrong: "#ffffff",
  muted: "#c4cce0",

  border: "rgba(255,255,255,0.12)",
  borderStrong: "rgba(255,255,255,0.20)",

  panel: "rgba(31, 39, 63, 0.88)",
  panelStrong: "rgba(26, 33, 55, 0.94)",
  panelSoft: "rgba(38, 47, 74, 0.88)",

  shadowSoft: "0 0 0 1px rgba(255,255,255,0.03), 0 10px 30px rgba(0,0,0,0.22)",
  shadowCard: "0 0 0 1px rgba(255,255,255,0.04), 0 8px 24px rgba(0,0,0,0.18)",

  primary: "#8b7cf6",
  secondary: "#56c7d9",
  accent: "#f3b562",
  accent2: "#f28bc1",
  success: "#58c98a",
  danger: "#ef6b73",
  info: "#6ea8ff",
  neutral: "#aab6cf",

  primarySoft: "rgba(139,124,246,0.16)",
  secondarySoft: "rgba(86,199,217,0.16)",
  accentSoft: "rgba(243,181,98,0.16)",
  accent2Soft: "rgba(242,139,193,0.16)",
  successSoft: "rgba(88,201,138,0.16)",
  dangerSoft: "rgba(239,107,115,0.16)",
  infoSoft: "rgba(110,168,255,0.16)",
  neutralSoft: "rgba(170,182,207,0.16)",

  radiusXl: "22px",
  radiusLg: "18px",
  radiusMd: "14px",
};

function buildBasePageCss() {
  return `
    * { box-sizing: border-box; }

    body {
      margin: 0;
      color: var(--text);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, var(--primary-soft), transparent 28%),
        radial-gradient(circle at top right, var(--secondary-soft), transparent 20%),
        linear-gradient(180deg, var(--bg-1) 0%, var(--bg-0) 100%);
    }

    body::before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background:
        linear-gradient(
          to bottom,
          rgba(255,255,255,0.025) 0px,
          rgba(255,255,255,0.025) 1px,
          transparent 1px,
          transparent 4px
        );
      background-size: 100% 4px;
      opacity: 0.08;
    }
    
.task-row-overdue {
  background-color: rgba(239, 107, 115, 0.08);
}

.task-row-blocked {
  background-color: rgba(243, 181, 98, 0.10);
}

.task-row-blocked.task-row-overdue {
  background-color: rgba(239, 107, 115, 0.14);
}

.task-row-overdue td:first-child {
  border-left: 4px solid #ef6b73;
}

.task-row-blocked td:first-child {
  border-left: 4px solid #f3b562;
}

    .muted { color: var(--muted); }
    .empty-cell { text-align: center; color: var(--muted); padding: 18px; }
  `;
}

function buildThemeCss(theme = UI_THEME) {
  return `
    :root {
      --bg-0: ${theme.bg0};
      --bg-1: ${theme.bg1};
      --bg-2: ${theme.bg2};

      --panel: ${theme.panel};
      --panel-strong: ${theme.panelStrong};
      --panel-soft: ${theme.panelSoft};

      --text: ${theme.text};
      --text-strong: ${theme.textStrong};
      --muted: ${theme.muted};

      --line: ${theme.border};
      --line-strong: ${theme.borderStrong};

      --primary: ${theme.primary};
      --secondary: ${theme.secondary};
      --accent: ${theme.accent};
      --accent-2: ${theme.accent2};

      --success: ${theme.success};
      --danger: ${theme.danger};
      --info: ${theme.info};
      --neutral: ${theme.neutral};

      --primary-soft: ${theme.primarySoft};
      --secondary-soft: ${theme.secondarySoft};
      --accent-soft: ${theme.accentSoft};
      --accent-2-soft: ${theme.accent2Soft};
      --success-soft: ${theme.successSoft};
      --danger-soft: ${theme.dangerSoft};
      --info-soft: ${theme.infoSoft};
      --neutral-soft: ${theme.neutralSoft};

      --shadow-soft: ${theme.shadowSoft};
      --shadow-card: ${theme.shadowCard};

      --radius-xl: ${theme.radiusXl};
      --radius-lg: ${theme.radiusLg};
      --radius-md: ${theme.radiusMd};
    }
  `;
}

function formatDateListForHumans(dateList) {
  if (!dateList || !dateList.length) return "None";

  return dateList
    .map((dateStr) => {
      const date = new Date(`${dateStr}T00:00:00${APP_TIMEZONE_OFFSET}`);
      return date.toLocaleDateString("en-IN", {
        timeZone: APP_TIMEZONE,
        day: "numeric",
        month: "short",
      });
    })
    .join(", ");
}

function formatDateTime(isoString) {
  if (!isoString) return "-";
  const d = new Date(isoString);
  if (Number.isNaN(d.getTime())) return String(isoString);

  return (
    d.toLocaleString("en-IN", {
      timeZone: APP_TIMEZONE,
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    }) + " IST"
  );
}

function formatDateOnly(dateString) {
  if (!dateString) return "-";

  const d = new Date(`${dateString}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return String(dateString);

  return d.toLocaleDateString("en-IN", {
    timeZone: APP_TIMEZONE,
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function formatTimeOnly(isoString) {
  if (!isoString) return "-";
  const d = new Date(isoString);
  if (Number.isNaN(d.getTime())) return String(isoString);

  return (
    d.toLocaleString("en-IN", {
      timeZone: APP_TIMEZONE,
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    }) + " IST"
  );
}

function badgeClass(value) {
  const v = normalizeText(value);

  if (["high", "urgent"].includes(v)) return "badge badge-danger";
  if (["medium"].includes(v)) return "badge badge-warn";
  if (["low"].includes(v)) return "badge badge-ok";

  if (["done", "logout"].includes(v)) return "badge badge-muted";
  if (["blocked", "break"].includes(v)) return "badge badge-danger";
  if (["in_progress", "back", "login"].includes(v)) return "badge badge-info";
  if (["open", "pending"].includes(v)) return "badge badge-warn";
  if (["cancelled"].includes(v)) return "badge badge-muted";

  return "badge badge-muted";
}

function stripOrdinalSuffixes(text) {
  return String(text || "").replace(/\b(\d{1,2})(st|nd|rd|th)\b/gi, "$1");
}

function monthNameToNumber(monthText) {
  const months = {
    january: 1,
    jan: 1,
    february: 2,
    feb: 2,
    march: 3,
    mar: 3,
    april: 4,
    apr: 4,
    may: 5,
    june: 6,
    jun: 6,
    july: 7,
    jul: 7,
    august: 8,
    aug: 8,
    september: 9,
    sep: 9,
    sept: 9,
    october: 10,
    oct: 10,
    november: 11,
    nov: 11,
    december: 12,
    dec: 12,
  };

  return months[normalizeText(monthText)] || null;
}

function parseLateForOtherCommand(text) {
  const raw = normalizeText(text);

  const match = raw.match(
    /^late\s+(.+?)\s+(\d{1,2}(:\d{2})?\s*(am|pm))(?:\s+(.+))?$/i,
  );

  if (!match) return null;

  return {
    target_name: match[1].trim(),
    time_text: match[2].trim().replace(/\s+/g, " "),
    note: match[5]?.trim() || null,
  };
}

function parseFlexibleDateText(input) {
  const text = normalizeText(stripOrdinalSuffixes(input || ""));
  const todayDb = getTodayDateStringInTimeZone(APP_TIMEZONE);

  if (!text) return null;

  if (text === "today") {
    return todayDb;
  }

  if (text === "tomorrow") {
    return addDaysToDateString(todayDb, 1);
  }

  const weekdays = {
    sunday: 0,
    monday: 1,
    tuesday: 2,
    wednesday: 3,
    thursday: 4,
    friday: 5,
    saturday: 6,
  };

  if (text in weekdays) {
    const todayDate = new Date(`${todayDb}T00:00:00Z`);
    const currentDay = todayDate.getUTCDay();
    const targetDay = weekdays[text];

    let diff = targetDay - currentDay;
    if (diff <= 0) diff += 7;

    return addDaysToDateString(todayDb, diff);
  }

  let match = text.match(/^(\d{1,2})\s+([a-z]+)$/i);
  if (match) {
    const day = Number(match[1]);
    const month = monthNameToNumber(match[2]);

    if (month && day >= 1 && day <= 31) {
      return formatDateForDbFromParts(
        getCurrentYearInTimeZone(APP_TIMEZONE),
        month,
        day,
      );
    }
  }

  match = text.match(/^([a-z]+)\s+(\d{1,2})$/i);
  if (match) {
    const month = monthNameToNumber(match[1]);
    const day = Number(match[2]);

    if (month && day >= 1 && day <= 31) {
      return formatDateForDbFromParts(
        getCurrentYearInTimeZone(APP_TIMEZONE),
        month,
        day,
      );
    }
  }

  return null;
}

function parseUnsupportedTimedSelfAttendance(text) {
  const raw = normalizeText(text);
  const match = raw.match(
    /^(login|logout|back)\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))$/i,
  );
  if (!match) return null;

  return {
    action: match[1].toLowerCase(),
    time_text: match[2].trim().replace(/\s+/g, " "),
  };
}

function buildUnknownCommandHelp(user, body) {
  const msg = String(body || "").trim();
  const isManager = isManagerOrAdmin(user);

  return [
    `❌ I did not understand: "${msg}"`,
    "",
    "Try one of these:",
    "Attendance:",
    "login",
    "logout",
    "break",
    "back",
    "late 11:00 am",
    "",
    "Tasks:",
    "show task 2",
    "progress 2 50 finished API work",
    "done 2 tested and verified",
    "edit task 2 blocker waiting on aj",
    "extra work helped aj debug org id issue",
    isManager ? "delete 2" : null,
    "",
    "Need full list?",
    "help attendance",
    "help tasks",
    isManager ? "help manager" : null,
  ]
    .filter(Boolean)
    .join("\n");
}

function parseCancelTaskCommand(text) {
  const raw = normalizeText(text);

  if (!raw.startsWith("delete") && !raw.startsWith("cancel")) {
    return null;
  }

  let match = raw.match(/^(cancel|delete)\s+task\s+(\d+)$/i);
  if (match) {
    return {
      action: match[1].toLowerCase(),
      taskId: Number(match[2]),
    };
  }

  match = raw.match(/^(cancel|delete)\s+(\d+)$/i);
  if (match) {
    return {
      action: match[1].toLowerCase(),
      taskId: Number(match[2]),
    };
  }

  return {
    error:
      "❌ Could not understand delete/cancel command\nUse:\ndelete 169\ncancel 169\ndelete task 169",
  };
}

function parseDeadline(deadlineText) {
  return parseFlexibleDateText(deadlineText);
}

function parseLocalDateTimeForToday(timeText) {
  const raw = String(timeText || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");

  const match = raw.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)$/i);
  if (!match) return null;

  let hour = Number(match[1]);
  const minute = match[2] == null ? 0 : Number(match[2]);
  const ampm = match[3].toLowerCase();

  if (hour < 1 || hour > 12 || minute < 0 || minute > 59) return null;

  if (ampm === "pm" && hour !== 12) hour += 12;
  if (ampm === "am" && hour === 12) hour = 0;

  const todayDb = getTodayDateStringInTimeZone(APP_TIMEZONE);
  const iso = `${todayDb}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00${APP_TIMEZONE_OFFSET}`;
  const d = new Date(iso);

  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function getShiftStartIsoForToday() {
  return parseLocalDateTimeForToday(DEFAULT_SHIFT_START_TEXT);
}

function isLateApproved(informedAtIso, shiftStartIso) {
  const informedAt = new Date(informedAtIso);
  const shiftStartAt = new Date(shiftStartIso);

  if (
    Number.isNaN(informedAt.getTime()) ||
    Number.isNaN(shiftStartAt.getTime())
  ) {
    return false;
  }

  const diffHours =
    (shiftStartAt.getTime() - informedAt.getTime()) / (1000 * 60 * 60);

  return diffHours >= LATE_APPROVAL_NOTICE_HOURS;
}

function getFirstLoginEvent(userEvents) {
  return userEvents.find((e) => e.action === "login") || null;
}

function getOpenBreakFromEvents(events) {
  let currentBreak = null;

  for (const ev of events) {
    if (ev.action === "break") currentBreak = ev;
    if (ev.action === "back" || ev.action === "logout") currentBreak = null;
  }

  return currentBreak;
}

function parseMarkAttendanceCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(
    /^mark\s+(.+?)\s+(login|logout|back)\s+(\d{1,2}:\d{2}\s*(?:am|pm))$/i,
  );
  if (match) {
    return {
      target_name: match[1].trim(),
      action: match[2].toLowerCase(),
      duration_min: null,
      time_text: match[3].trim().replace(/\s+/g, " "),
    };
  }

  match = raw.match(/^mark\s+(.+?)\s+(login|logout|back)$/i);
  if (match) {
    return {
      target_name: match[1].trim(),
      action: match[2].toLowerCase(),
      duration_min: null,
      time_text: null,
    };
  }

  match = raw.match(
    /^mark\s+(.+?)\s+break\s+(\d+)\s+(\d{1,2}:\d{2}\s*(?:am|pm))$/i,
  );
  if (match) {
    return {
      target_name: match[1].trim(),
      action: "break",
      duration_min: Number(match[2]),
      time_text: match[3].trim().replace(/\s+/g, " "),
    };
  }

  match = raw.match(/^mark\s+(.+?)\s+break\s+(\d{1,2}:\d{2}\s*(?:am|pm))$/i);
  if (match) {
    return {
      target_name: match[1].trim(),
      action: "break",
      duration_min: null,
      time_text: match[2].trim().replace(/\s+/g, " "),
    };
  }

  match = raw.match(/^mark\s+(.+?)\s+break\s+(\d+)$/i);
  if (match) {
    return {
      target_name: match[1].trim(),
      action: "break",
      duration_min: Number(match[2]),
      time_text: null,
    };
  }

  match = raw.match(/^mark\s+(.+?)\s+break$/i);
  if (match) {
    return {
      target_name: match[1].trim(),
      action: "break",
      duration_min: null,
      time_text: null,
    };
  }

  return null;
}

function parseDirectManagerAttendanceCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(
    /^(login|logout|back)\s+(.+?)\s+(\d{1,2}:\d{2}\s*(?:am|pm))$/i,
  );
  if (match) {
    return {
      target_name: match[2].trim(),
      action: match[1].toLowerCase(),
      duration_min: null,
      time_text: match[3].trim().replace(/\s+/g, " "),
      reason: null,
    };
  }

  match = raw.match(/^(login|logout|back)\s+(.+)$/i);
  if (match) {
    const maybeName = match[2].trim();

    if (
      !/^(today|tomorrow|\d{1,2}:\d{2}\s*(?:am|pm)|for\b|because\b)/i.test(
        maybeName,
      )
    ) {
      return {
        target_name: maybeName,
        action: match[1].toLowerCase(),
        duration_min: null,
        time_text: null,
        reason: null,
      };
    }
  }

  match = raw.match(/^break\s+(.+?)\s+(\d+)\s+(\d{1,2}:\d{2}\s*(?:am|pm))$/i);
  if (match) {
    return {
      target_name: match[1].trim(),
      action: "break",
      duration_min: Number(match[2]),
      time_text: match[3].trim().replace(/\s+/g, " "),
      reason: null,
    };
  }

  match = raw.match(/^break\s+(.+?)\s+(\d+)$/i);
  if (match) {
    return {
      target_name: match[1].trim(),
      action: "break",
      duration_min: Number(match[2]),
      time_text: null,
      reason: null,
    };
  }

  match = raw.match(/^break\s+(.+?)\s+(\d{1,2}:\d{2}\s*(?:am|pm))$/i);
  if (match) {
    return {
      target_name: match[1].trim(),
      action: "break",
      duration_min: null,
      time_text: match[2].trim().replace(/\s+/g, " "),
      reason: null,
    };
  }

  match = raw.match(/^break\s+(.+)$/i);
  if (match) {
    const maybeName = match[1].trim();

    if (
      !/^\d+$/.test(maybeName) &&
      !/^(personal|lunch|tea|coffee|washroom|restroom|urgent|family|meeting)\b/i.test(
        maybeName,
      )
    ) {
      return {
        target_name: maybeName,
        action: "break",
        duration_min: null,
        time_text: null,
        reason: null,
      };
    }
  }

  return null;
}

// function parseSimpleTaskCommand(text) {
//   const raw = normalizeText(text);

//   let match = raw.match(
//     /^task\s+(.+?)\s+(low|medium|high|urgent)\s+(.+?)\s+by\s+(.+)$/i,
//   );

//   if (match) {
//     return {
//       assignee_name: match[1].trim(),
//       priority: match[2].toLowerCase(),
//       title: match[3].trim(),
//       deadline_text: match[4].trim(),
//     };
//   }

//   match = raw.match(/^task\s+(.+?)\s+(.+?)\s+by\s+(.+)$/i);
//   if (!match) return null;

//   return {
//     assignee_name: match[1].trim(),
//     priority: null,
//     title: match[2].trim(),
//     deadline_text: match[3].trim(),
//   };
// }

function parseTaskIdCommand(text, commandWord) {
  const msg = normalizeText(text);
  const regex = new RegExp(`^${commandWord}\\s+(\\d+)$`);
  const match = msg.match(regex);

  if (!match) return null;
  return Number(match[1]);
}

function parseWhoIsOffTodayCommand(text) {
  const msg = normalizeText(text);
  return msg === "who is off today" || msg === "who all are on leave today";
}

function parseShowTaskCommand(text) {
  const msg = normalizeText(text);
  const match = msg.match(/^show\s+task\s+(\d+)$/);
  if (!match) return null;
  return Number(match[1]);
}

function parseWhoAmICommand(text) {
  return normalizeText(text) === "who am i";
}

function parseStatusCommand(text) {
  return normalizeText(text) === "status";
}

function parseProgressCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(/^progress\s+task\s+(\d+)\s+(\d{1,3}%?)\s+(.+)$/i);
  if (match) {
    return {
      taskId: Number(match[1]),
      progress: parseProgressPercentToken(match[2]),
      note: match[3].trim(),
    };
  }

  match = raw.match(/^progress\s+(\d+)\s+(\d{1,3}%?)\s+(.+)$/i);
  if (!match) return null;

  return {
    taskId: Number(match[1]),
    progress: parseProgressPercentToken(match[2]),
    note: match[3].trim(),
  };
}

function parseAdvancedCreateTaskCommand(text) {
  const raw = normalizeText(text);

  if (!raw.startsWith("create task ")) {
    return null;
  }

  const match = raw.match(
    /^create task\s+(.+?)\s+business\s+(.+?)\s+area\s+(.+?)\s+owner\s+(.+?)\s+priority\s+(low|medium|high|urgent)\s+due\s+(.+)$/i,
  );

  if (!match) {
    return {
      error:
        "❌ Could not create task\nUse:\ncreate task <title> business <business> area <area> owner <a, b> priority <low|medium|high|urgent> due <date>\nExample:\ncreate task fix landing page business joolian area marketing owner aj priority high due tomorrow",
    };
  }

  const title = match[1].trim();
  const business = match[2].trim();
  const area = match[3].trim();
  const owners = parseOwnerNames(match[4]);
  const priority = match[5].toLowerCase();
  const deadline = parseDeadline(match[6].trim());

  if (!title) return { error: "❌ Task title is missing." };
  if (!business) return { error: "❌ Business is missing." };
  if (!area) return { error: "❌ Area is missing." };
  if (!owners.length) return { error: "❌ At least one owner is required." };
  if (!deadline) {
    return {
      error: `❌ Could not understand due date "${match[6].trim()}"\nTry: today, tomorrow, friday, 11 april, or april 11`,
    };
  }

  return {
    title,
    business,
    area,
    owner_names: owners,
    priority,
    deadline,
  };
}

function parseEditTaskCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(/^edit\s+task\s+(\d+)\s+title\s+(.+)$/i);
  if (match) {
    return { taskId: Number(match[1]), field: "title", value: match[2].trim() };
  }

  match = raw.match(/^edit\s+task\s+(\d+)\s+detail\s+(.+)$/i);
  if (match) {
    return {
      taskId: Number(match[1]),
      field: "detail",
      value: match[2].trim(),
    };
  }

  match = raw.match(
    /^edit\s+task\s+(\d+)\s+priority\s+(low|medium|high|urgent)$/i,
  );
  if (match) {
    return {
      taskId: Number(match[1]),
      field: "priority",
      value: match[2].toLowerCase(),
    };
  }

  match = raw.match(/^edit\s+task\s+(\d+)\s+business\s+(.+)$/i);
  if (match) {
    return {
      taskId: Number(match[1]),
      field: "business",
      value: match[2].trim(),
    };
  }

  match = raw.match(/^edit\s+task\s+(\d+)\s+area\s+(.+)$/i);
  if (match) {
    return { taskId: Number(match[1]), field: "area", value: match[2].trim() };
  }

  match = raw.match(/^edit\s+task\s+(\d+)\s+deadline\s+(.+)$/i);
  if (match) {
    return {
      taskId: Number(match[1]),
      field: "deadline",
      value: match[2].trim(),
    };
  }

  match = raw.match(
    /^edit\s+task\s+(\d+)\s+status\s+(open|pending|in_progress|done|cancelled)$/i,
  );
  if (match) {
    return {
      taskId: Number(match[1]),
      field: "status",
      value: match[2].toLowerCase(),
    };
  }

  match = raw.match(/^edit\s+task\s+(\d+)\s+blocker\s+(.+)$/i);
  if (match) {
    return {
      taskId: Number(match[1]),
      field: "blocker_note",
      value: match[2].trim(),
    };
  }

  match = raw.match(/^edit\s+task\s+(\d+)\s+owner\s+(.+)$/i);
  if (match) {
    return { taskId: Number(match[1]), field: "owner", value: match[2].trim() };
  }

  match = raw.match(
    /^edit\s+task\s+(\d+)\s+clear\s+(detail|blocker|business|area|deadline)$/i,
  );
  if (match) {
    return {
      taskId: Number(match[1]),
      field: `clear_${match[2].toLowerCase()}`,
      value: null,
    };
  }

  return null;
}

function parseDoneCommand(text) {
  const raw = normalizeText(text);
  const match = raw.match(/^done\s+(\d+)\s+(.+)$/i);
  if (!match) return null;

  return {
    taskId: Number(match[1]),
    note: match[2].trim(),
  };
}

function parseTasksByNameCommand(text) {
  const raw = normalizeText(text);
  const match = raw.match(/^tasks\s+(.+)$/i);
  if (!match) return null;

  return {
    assignee_name: match[1].trim(),
  };
}

function parseWhoIsOnBreakCommand(text) {
  return normalizeText(text) === "who is on break";
}

function parseSummaryTodayCommand(text) {
  const msg = normalizeText(text);
  return msg === "summary today" || msg === "attendance summary today";
}

function parseNowCommand(text) {
  const msg = normalizeText(text);
  return msg === "now" || msg === "now summary";
}

function parseUndoLastTaskChangeCommand(text) {
  return normalizeText(text) === "undo last task change";
}

function parseOffDayCommand(text) {
  const raw = normalizeText(text);

  const match = raw.match(/^(off|leave)\s+(.+)$/i);
  if (!match) return null;

  const offDateText = match[2]
    .trim()
    .replace(/^on\s+/i, "")
    .trim();

  return {
    target_name: null,
    off_date_text: offDateText,
  };
}

function parseOffDayForOtherCommand(text) {
  const raw = normalizeText(text);

  const match = raw.match(
    /^(off|leave)\s+(.+?)\s+(?:on\s+)?(today|tomorrow|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)$/i,
  );
  if (!match) return null;

  const targetName = match[2].trim();
  if (/^on$/i.test(targetName)) return null;

  return {
    target_name: targetName,
    off_date_text: match[3].trim(),
  };
}

function parseAttendanceCommand(text) {
  const raw = normalizeText(text);

  if (/^login$/i.test(raw)) {
    return {
      action: "login",
      expected_duration_min: null,
      reason: null,
    };
  }

  if (/^back$/i.test(raw)) {
    return {
      action: "back",
      expected_duration_min: null,
      reason: null,
    };
  }

  if (/^logout$/i.test(raw)) {
    return {
      action: "logout",
      expected_duration_min: null,
      reason: null,
    };
  }

  let match = raw.match(/^logout\s+(.+)$/i);
  if (match) {
    return {
      action: "logout",
      expected_duration_min: null,
      reason: match[1].trim(),
    };
  }

  if (/^break$/i.test(raw)) {
    return {
      action: "break",
      expected_duration_min: null,
      reason: null,
    };
  }

  match = raw.match(/^break\s+(\d+)$/i);
  if (match) {
    return {
      action: "break",
      expected_duration_min: Number(match[1]),
      reason: null,
    };
  }

  match = raw.match(/^break\s+(\d+)\s+(.+)$/i);
  if (match) {
    return {
      action: "break",
      expected_duration_min: Number(match[1]),
      reason: match[2].trim(),
    };
  }

  match = raw.match(/^break\s+(.+)$/i);
  if (match) {
    return {
      action: "break",
      expected_duration_min: null,
      reason: match[1].trim(),
    };
  }

  return null;
}

function parseLateCommand(text) {
  const raw = normalizeText(text);

  const match = raw.match(
    /^late\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))(?:\s+(.+))?$/i,
  );
  if (!match) return null;

  return {
    time_text: match[1].trim().replace(/\s+/g, " "),
    note: match[2]?.trim() || null,
  };
}

function formatTaskLine(task) {
  return `#${task.task_no || task.id}${task.priority ? ` | ${task.priority}` : ""} | ${task.status} | ${task.title} | due ${task.deadline ?? "no deadline"} | ${task.progress}%`;
}

const MIN_TASK_NOTE_LENGTH = 20;

function validateDetailedTaskNote(note) {
  const cleanNote = String(note || "").trim();

  if (!cleanNote) {
    return {
      ok: false,
      message: "Please write detailed notes (at least 20 characters).",
    };
  }

  if (cleanNote.length < MIN_TASK_NOTE_LENGTH) {
    return {
      ok: false,
      message: "Please write detailed notes (at least 20 characters).",
    };
  }

  return {
    ok: true,
    cleanNote,
  };
}

function validateAttendanceTransition(lastAction, nextAction, subjectName) {
  const isYou = subjectName === "You";

  if (nextAction === "login") {
    if (lastAction === "login" || lastAction === "back") {
      return `❌ ${isYou ? "You are" : `${subjectName} is`} already logged in\nNo action was taken`;
    }

    if (lastAction === "break") {
      return `❌ Could not log in\nReason: ${isYou ? "you are currently on break, use 'back' first" : `${subjectName} is currently on break, use 'back' first`}`;
    }
  }

  if (nextAction === "break") {
    if (lastAction === "break") {
      return `❌ Could not start break\nReason: ${isYou ? "you are already on break" : `${subjectName} is already on break`}`;
    }

    if (lastAction !== "login" && lastAction !== "back") {
      return `❌ Could not start break\nReason: ${isYou ? "you must be logged in first" : `${subjectName} must be logged in first`}`;
    }
  }

  if (nextAction === "back") {
    if (lastAction !== "break") {
      return `❌ Could not return from break\nReason: ${isYou ? "you are not currently on break" : `${subjectName} is not currently on break`}`;
    }
  }

  if (nextAction === "logout") {
    if (lastAction === "break") {
      return `❌ Could not log out\nReason: ${isYou ? "you are currently on break, use 'back' first" : `${subjectName} is currently on break, use 'back' first`}`;
    }

    if (lastAction !== "login" && lastAction !== "back") {
      return `❌ Could not log out\nReason: ${isYou ? "you are not currently logged in" : `${subjectName} is not currently logged in`}`;
    }
  }

  return null;
}

function looksLikeTask(text) {
  const msg = normalizeText(text);

  if (!msg) return false;
  if (msg.startsWith("task ")) return true;
  if (msg.startsWith("assign ")) return true;
  if (msg.startsWith("create task ")) return true;

  const hasStrongAssignment =
    /\bto\s+[a-z]/i.test(msg) || /\bfor\s+[a-z]/i.test(msg);

  const hasTaskVerb =
    /\b(assign|follow up|complete|finish|review|test|check|call|send|prepare)\b/i.test(
      msg,
    );

  const hasTimeSignal =
    /\b(today|tomorrow|monday|tuesday|wednesday|thursday|friday|saturday|sunday|by)\b/i.test(
      msg,
    );

  const hasPriority = /\b(low|medium|high|urgent)\b/i.test(msg);

  return (
    (hasTaskVerb && hasStrongAssignment && (hasTimeSignal || hasPriority)) ||
    msg.startsWith("task ") ||
    msg.startsWith("assign ")
  );
}

function buildAbsoluteUrl(req) {
  const proto = req.headers["x-forwarded-proto"] || req.protocol || "http";
  const host = req.headers["x-forwarded-host"] || req.get("host");
  return `${proto}://${host}${req.originalUrl}`;
}

function validateTwilioRequest(req) {
  const authToken = process.env.TWILIO_AUTH_TOKEN;
  if (!authToken) {
    console.warn(
      "TWILIO_AUTH_TOKEN missing; skipping Twilio signature validation.",
    );
    return true;
  }

  const signature = req.get("X-Twilio-Signature");
  if (!signature) {
    return false;
  }

  const url = buildAbsoluteUrl(req);
  return twilio.validateRequest(authToken, signature, url, req.body);
}

function checkRateLimit(key) {
  const now = Date.now();
  const existing = rateLimitStore.get(key);

  if (!existing || now > existing.resetAt) {
    rateLimitStore.set(key, {
      count: 1,
      resetAt: now + RATE_LIMIT_WINDOW_MS,
    });
    return true;
  }

  if (existing.count >= RATE_LIMIT_MAX_REQUESTS) {
    return false;
  }

  existing.count += 1;
  return true;
}

function requireDashboardAuth(req, res, next) {
  const username = process.env.DASHBOARD_USERNAME;
  const password = process.env.DASHBOARD_PASSWORD;

  if (!username || !password) {
    console.warn("Dashboard auth env vars missing; dashboard is unprotected.");
    return next();
  }

  const header = req.get("Authorization") || "";
  if (!header.startsWith("Basic ")) {
    res.set("WWW-Authenticate", 'Basic realm="WeSolveHR Dashboard"');
    return res.status(401).send("Authentication required");
  }

  const base64 = header.slice(6);
  let decoded = "";
  try {
    decoded = Buffer.from(base64, "base64").toString("utf8");
  } catch {
    res.set("WWW-Authenticate", 'Basic realm="WeSolveHR Dashboard"');
    return res.status(401).send("Invalid auth header");
  }

  const separatorIndex = decoded.indexOf(":");
  const inputUser = separatorIndex >= 0 ? decoded.slice(0, separatorIndex) : "";
  const inputPass =
    separatorIndex >= 0 ? decoded.slice(separatorIndex + 1) : "";

  if (inputUser !== username || inputPass !== password) {
    res.set("WWW-Authenticate", 'Basic realm="WeSolveHR Dashboard"');
    return res.status(401).send("Invalid credentials");
  }

  return next();
}

const STAGE0_BUG_COLUMNS = [
  "Parsing",
  "Duplicate / idempotency",
  "Webhook / Twilio",
  "DB / save failure",
  "Dashboard / logs",
  "Infra / regional access",
  "Unknown",
];

const STAGE0_BUG_SEVERITIES = ["P0", "P1", "P2"];
const STAGE0_BUG_STATUSES = ["open", "in_progress", "blocked", "done"];

function isValidStage0BugColumn(value) {
  return STAGE0_BUG_COLUMNS.includes(String(value || "").trim());
}

function isValidStage0BugSeverity(value) {
  return STAGE0_BUG_SEVERITIES.includes(String(value || "").trim());
}

function isValidStage0BugStatus(value) {
  return STAGE0_BUG_STATUSES.includes(String(value || "").trim());
}

function bugSeveritySortWeight(severity) {
  if (severity === "P0") return 0;
  if (severity === "P1") return 1;
  return 2;
}

function bugSeverityBadgeClass(severity) {
  if (severity === "P0") return "badge badge-danger";
  if (severity === "P1") return "badge badge-warn";
  return "badge badge-info";
}

function bugStatusBadgeClass(status) {
  if (status === "done") return "badge badge-ok";
  if (status === "blocked") return "badge badge-danger";
  if (status === "in_progress") return "badge badge-info";
  return "badge badge-warn";
}

async function canReadTask(user, task) {
  if (!user || !task) return false;

  if (user.org_id !== task.org_id) return false;
  if (isManagerOrAdmin(user)) return true;
  if (task.created_by_user_id === user.id) return true;

  const ownerIds = await getTaskOwnerIds(task.id, user.org_id);
  return ownerIds.includes(user.id);
}

async function canModifyTask(user, task) {
  if (!user || !task) return false;

  if (user.org_id !== task.org_id) return false;

  if (task.status === "cancelled") {
    return isManagerOrAdmin(user);
  }

  if (isManagerOrAdmin(user)) return true;

  const ownerIds = await getTaskOwnerIds(task.id, user.org_id);
  return ownerIds.includes(user.id);
}

async function parseTaskWithAI(text) {
  if (!openai) return null;

  try {
    const prompt = `
Extract task details from this message.

Message: "${text}"

Return JSON ONLY in this exact shape:
{
  "assignee_name": "",
  "priority": "low",
  "title": "",
  "deadline_text": ""
}

Rules:
- priority may be one of: low, medium, high, urgent, or empty string
- keep title short and clean
- if deadline is missing, use empty string
- if assignee is missing, use empty string
- if priority is missing, use empty string
`;

    const response = await openai.chat.completions.create({
      model: "gpt-4.1-mini",
      messages: [{ role: "user", content: prompt }],
      temperature: 0,
    });

    const content = response.choices?.[0]?.message?.content || "";
    console.log("AI raw response:", content);

    const parsed = safeParseJson(content);
    if (!parsed) return null;

    const priority = normalizeText(parsed.priority || "");
    if (priority && !["low", "medium", "high", "urgent"].includes(priority)) {
      return null;
    }

    return {
      assignee_name: String(parsed.assignee_name || "").trim(),
      priority: priority || null,
      title: String(parsed.title || "").trim(),
      deadline_text: String(parsed.deadline_text || "").trim(),
    };
  } catch (e) {
    console.error("AI parsing failed:", e);
    return null;
  }
}

async function getActiveUserByPhone(phoneNumber) {
  const { data, error } = await supabase
    .from("users")
    .select("id, org_id, name, phone_number, role, is_active")
    .eq("phone_number", phoneNumber)
    .eq("is_active", true)
    .maybeSingle();

  if (error) {
    console.error("User lookup error:", error);
    return { user: null, error };
  }

  return { user: data || null, error: null };
}

async function getLastAction(userId, orgId) {
  const { data, error } = await supabase
    .from("attendance_events")
    .select("action")
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("Error fetching last action:", error);
    return null;
  }

  return data?.action || null;
}

async function getLastActionAtOrBefore(userId, orgId, occurredAtIso = null) {
  let query = supabase
    .from("attendance_events")
    .select("action, created_at")
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .order("created_at", { ascending: false })
    .limit(1);

  if (occurredAtIso) {
    query = query.lte("created_at", occurredAtIso);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    console.error("Error fetching last action at time:", error);
    return null;
  }

  return data?.action || null;
}

async function insertMessageParsingLog({
  orgId = null,
  messageSid,
  phoneNumber,
  rawText,
  normalizedText,
  intentDetected,
  parserUsed,
  parsedJson,
  validationPassed,
  validationError,
  actionTaken,
}) {
  const { error } = await supabase.from("message_parsing_logs").insert([
    {
      org_id: orgId,
      message_sid: messageSid || null,
      phone_number: phoneNumber || null,
      raw_text: rawText || null,
      normalized_text: normalizedText || null,
      intent_detected: intentDetected || null,
      parser_used: parserUsed || null,
      parsed_json: parsedJson || null,
      validation_passed: !!validationPassed,
      validation_error: validationError || null,
      action_taken: actionTaken || null,
    },
  ]);

  if (error) {
    console.error("insertMessageParsingLog error:", error);
  }
}

async function findUsersByName(name, orgId) {
  const trimmed = String(name || "").trim();

  const { data, error } = await supabase
    .from("users")
    .select("id, org_id, name, phone_number, role, is_active")
    .eq("org_id", orgId)
    .ilike("name", trimmed)
    .eq("is_active", true);

  if (error) {
    console.error("User name lookup error:", error);
    return [];
  }

  if (data?.length) return data;

  const { data: fuzzyData, error: fuzzyError } = await supabase
    .from("users")
    .select("id, org_id, name, phone_number, role, is_active")
    .eq("org_id", orgId)
    .ilike("name", `%${trimmed}%`)
    .eq("is_active", true);

  if (fuzzyError) {
    console.error("User fuzzy lookup error:", fuzzyError);
    return [];
  }

  return fuzzyData || [];
}

async function findUniqueUserByName(name, orgId) {
  const users = await findUsersByName(name, orgId);
  if (users.length !== 1) return null;
  return users[0];
}

async function findUsersByNames(names, orgId) {
  const matchedUsers = [];
  const missingNames = [];

  for (const name of names) {
    const user = await findUniqueUserByName(name, orgId);
    if (user) matchedUsers.push(user);
    else missingNames.push(name);
  }

  return { matchedUsers, missingNames };
}

async function getTaskOwnerIds(taskId, orgId) {
  const { data, error } = await supabase
    .from("task_owners")
    .select("user_id")
    .eq("task_id", taskId)
    .eq("org_id", orgId);

  if (error) {
    console.error("getTaskOwnerIds error:", error);
    return [];
  }

  return (data || []).map((x) => x.user_id);
}

async function getTaskOwnerNames(taskId, orgId) {
  const { data, error } = await supabase
    .from("task_owners")
    .select(
      `
      user_id,
      users!task_owners_user_id_fkey(name)
    `,
    )
    .eq("task_id", taskId)
    .eq("org_id", orgId);

  if (error) {
    console.error("getTaskOwnerNames error:", error);
    return [];
  }

  return (data || []).map((x) => x.users?.name).filter(Boolean);
}

async function getTaskAssignedCount(userId, orgId) {
  const { data, error } = await supabase
    .from("task_owners")
    .select(
      `
      task_id,
      tasks!inner(id, org_id, status)
    `,
    )
    .eq("user_id", userId)
    .eq("org_id", orgId);

  if (error) {
    console.error("Assigned task count error:", error);
    return 0;
  }

  return (data || []).filter(
    (row) =>
      row.tasks &&
      row.tasks.org_id === orgId &&
      !["done", "archived", "cancelled"].includes(row.tasks.status),
  ).length;
}

async function getTaskById(taskId, orgId) {
  const numericTaskNo = Number(taskId);

  let query = supabase
    .from("tasks")
    .select(
      `
      id,
      org_id,
      task_no,
      title,
      detail,
      priority,
      status,
      progress,
      deadline,
      blocker_note,
      business,
      area,
      assigned_to_user_id,
      created_by_user_id,
      last_updated_by_user_id
    `,
    )
    .eq("org_id", orgId);

  if (!Number.isNaN(numericTaskNo) && Number.isFinite(numericTaskNo)) {
    query = query.eq("task_no", numericTaskNo);
  } else {
    query = query.eq("id", taskId);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    console.error("Get task by id error:", error);
    return { task: null, error };
  }

  if (!data) {
    return { task: null, error: null };
  }

  const ownerNames = await getTaskOwnerNames(data.id, orgId);

  return {
    task: {
      ...data,
      owner_names: ownerNames,
    },
    error: null,
  };
}

async function insertTaskHistory(
  taskId,
  changedByUserId,
  changeType,
  fieldName,
  oldValue,
  newValue,
  orgId,
) {
  const { error } = await supabase.from("task_history").insert([
    {
      org_id: orgId,
      task_id: taskId,
      changed_by_user_id: changedByUserId,
      change_type: changeType,
      field_name: fieldName,
      old_value: oldValue,
      new_value: newValue,
    },
  ]);

  if (error) {
    console.error("Task history insert error:", error);
  }
}

function minutesBetween(earlierIso, laterIso = new Date().toISOString()) {
  if (!earlierIso) return 0;
  const start = new Date(earlierIso);
  const end = new Date(laterIso);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return 0;
  return Math.max(0, Math.round((end.getTime() - start.getTime()) / 60000));
}

function getTotalBreakMinutesSoFar(events) {
  let total = 0;
  let openBreak = null;

  for (const ev of events || []) {
    if (ev.action === "break") {
      openBreak = ev;
      continue;
    }

    if (ev.action === "back" && openBreak) {
      total += minutesBetween(openBreak.created_at, ev.created_at);
      openBreak = null;
    }
  }

  if (openBreak) {
    total += minutesBetween(openBreak.created_at, new Date().toISOString());
  }

  return total;
}

function formatDurationMinutes(totalMinutes) {
  const mins = Math.max(0, Number(totalMinutes || 0));
  const hours = Math.floor(mins / 60);
  const rem = mins % 60;
  if (hours === 0) return `${rem} min`;
  if (rem === 0) return `${hours}h`;
  return `${hours}h ${rem}m`;
}

async function getLatestAttendanceEvent(userId, orgId) {
  const { data, error } = await supabase
    .from("attendance_events")
    .select(
      "id, org_id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
    )
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("Latest attendance event error:", error);
    return null;
  }

  return data || null;
}

async function getLatestBreakEvent(userId, orgId) {
  const { data, error } = await supabase
    .from("attendance_events")
    .select(
      "id, org_id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
    )
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .eq("action", "break")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("Latest break event error:", error);
    return null;
  }

  return data || null;
}

async function getAttendanceEventsForAttendanceDay(
  attendanceDateString,
  orgId,
) {
  const { startUtc, endUtc } = getAttendanceDayUtcRange(attendanceDateString);

  const { data, error } = await supabase
    .from("attendance_events")
    .select(
      "id, org_id, user_id, action, created_at, duration_min, expected_duration_min, reason, note, acted_by_phone, target_phone",
    )
    .eq("org_id", orgId)
    .gte("created_at", startUtc)
    .lt("created_at", endUtc)
    .order("created_at", { ascending: true });

  if (error) {
    throw error;
  }

  return data || [];
}

async function getAttendanceEventsForUserOnAttendanceDay(
  userId,
  attendanceDateString,
  orgId,
) {
  const { startUtc, endUtc } = getAttendanceDayUtcRange(attendanceDateString);

  const { data, error } = await supabase
    .from("attendance_events")
    .select(
      "id, org_id, user_id, action, created_at, duration_min, expected_duration_min, reason, note, acted_by_phone, target_phone",
    )
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .gte("created_at", startUtc)
    .lt("created_at", endUtc)
    .order("created_at", { ascending: true });

  if (error) {
    throw error;
  }

  return data || [];
}

async function getLatestBreakEventAtOrBefore(
  userId,
  orgId,
  occurredAtIso = null,
) {
  let query = supabase
    .from("attendance_events")
    .select(
      "id, org_id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
    )
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .eq("action", "break")
    .order("created_at", { ascending: false })
    .limit(1);

  if (occurredAtIso) {
    query = query.lte("created_at", occurredAtIso);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    console.error("Latest break event at or before error:", error);
    return null;
  }

  return data || null;
}

async function getLatestAttendanceEventByAction(
  userId,
  orgId,
  action,
  attendanceDateString = null,
) {
  let query = supabase
    .from("attendance_events")
    .select(
      "id, org_id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
    )
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .eq("action", action)
    .order("created_at", { ascending: false })
    .limit(1);

  if (attendanceDateString) {
    const { startUtc, endUtc } = getAttendanceDayUtcRange(attendanceDateString);
    query = query.gte("created_at", startUtc).lt("created_at", endUtc);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    console.error("Latest attendance event by action error:", error);
    return null;
  }

  return data || null;
}

async function deleteAttendanceEventById(eventId, orgId) {
  const { error } = await supabase
    .from("attendance_events")
    .delete()
    .eq("id", eventId)
    .eq("org_id", orgId);

  return error;
}

async function deleteAttendanceEventsForUserOnAttendanceDay(
  userId,
  attendanceDateString,
  orgId,
) {
  const { startUtc, endUtc } = getAttendanceDayUtcRange(attendanceDateString);

  const { error } = await supabase
    .from("attendance_events")
    .delete()
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .gte("created_at", startUtc)
    .lt("created_at", endUtc);

  return error;
}

async function deleteLateArrivalForUserOnDate(
  userId,
  attendanceDateString,
  orgId,
) {
  const { error } = await supabase
    .from("late_arrivals")
    .delete()
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .eq("late_date", attendanceDateString);

  return error;
}

async function deletePlannedOffForUserOnDate(
  userId,
  attendanceDateString,
  orgId,
) {
  const { error } = await supabase
    .from("planned_time_off")
    .delete()
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .eq("off_date", attendanceDateString);

  return error;
}

async function isAttendanceDayLocked(userId, attendanceDateString, orgId) {
  const { data, error } = await supabase
    .from("attendance_day_locks")
    .select("id, is_locked")
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .eq("attendance_date", attendanceDateString)
    .maybeSingle();

  if (error) {
    console.error("Attendance day lock lookup error:", error);
    return false;
  }

  return !!data?.is_locked;
}

async function setAttendanceDayLock(
  userId,
  attendanceDateString,
  isLocked,
  actedByUserId,
  orgId,
  note = null,
) {
  const { error } = await supabase.from("attendance_day_locks").upsert(
    [
      {
        org_id: orgId,
        user_id: userId,
        attendance_date: attendanceDateString,
        is_locked: isLocked,
        locked_by_user_id: actedByUserId,
        note,
        updated_at: new Date().toISOString(),
      },
    ],
    { onConflict: "user_id,attendance_date" },
  );

  return error;
}

function buildAttendanceTimelineLines(events) {
  if (!events?.length) return ["No attendance events found"];

  return events.map((ev) => {
    let line = `${formatTimeOnly(ev.created_at)} → ${ev.action}`;

    if (ev.action === "break" && ev.expected_duration_min) {
      line += ` (${ev.expected_duration_min} min expected)`;
    }

    if (ev.reason) {
      line += ` | ${ev.reason}`;
    }

    if (ev.note) {
      line += ` | ${ev.note}`;
    }

    return line;
  });
}

function analyzeAttendanceIssues(events) {
  const issues = [];
  let loginCount = 0;
  let breakOpen = null;
  let hasLogout = false;

  for (const ev of events || []) {
    if (ev.action === "login") {
      loginCount += 1;
      if (loginCount > 1) {
        issues.push(
          `Multiple login entries found (latest at ${formatTimeOnly(ev.created_at)})`,
        );
      }
    }

    if (ev.action === "break") {
      if (breakOpen) {
        issues.push(
          `Break started again without back at ${formatTimeOnly(ev.created_at)}`,
        );
      }
      breakOpen = ev;
    }

    if (ev.action === "back") {
      if (!breakOpen) {
        issues.push(
          `Back recorded without a matching break at ${formatTimeOnly(ev.created_at)}`,
        );
      } else {
        const breakMinutes = minutesBetween(
          breakOpen.created_at,
          ev.created_at,
        );
        if (breakMinutes >= LONG_BREAK_THRESHOLD_MIN) {
          issues.push(
            `Long break detected: ${formatDurationMinutes(breakMinutes)} ending at ${formatTimeOnly(ev.created_at)}`,
          );
        }
      }
      breakOpen = null;
    }

    if (ev.action === "logout") {
      hasLogout = true;
      if (breakOpen) {
        issues.push(
          `Logout happened while still on break at ${formatTimeOnly(ev.created_at)}`,
        );
        breakOpen = null;
      }
    }
  }

  if (breakOpen) {
    issues.push(
      `Break without return since ${formatTimeOnly(breakOpen.created_at)}`,
    );
  }

  const summary = getAttendanceSummaryFromEvents(events || []);
  if (summary.longShiftFlag) {
    issues.push(
      `Long shift detected: ${formatDurationMinutes(summary.workedMinutes)}`,
    );
  }

  const hasWorkStart = (events || []).some(
    (x) => x.action === "login" || x.action === "back",
  );

  if (hasWorkStart && !hasLogout) {
    issues.push("No logout recorded");
  }

  return issues;
}

async function getTodayAttendanceEventsForAllUsers(orgId) {
  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
  return getAttendanceEventsForAttendanceDay(attendanceDate, orgId);
}

function computeWorkedMinutesFromEvents(events) {
  let total = 0;
  let workStart = null;

  for (const event of events) {
    if (event.action === "login" || event.action === "back") {
      if (!workStart) {
        workStart = event.created_at;
      }
      continue;
    }

    if ((event.action === "break" || event.action === "logout") && workStart) {
      total += minutesBetween(workStart, event.created_at);
      workStart = null;
    }
  }

  if (workStart) {
    total += minutesBetween(workStart);
  }

  return total;
}

function getLastLogoutEvent(events) {
  let lastLogout = null;
  for (const ev of events || []) {
    if (ev.action === "logout") {
      lastLogout = ev;
    }
  }
  return lastLogout;
}

function getAttendanceSummaryFromEvents(events, options = {}) {
  const shiftStartIso = options.shiftStartIso || getShiftStartIsoForToday();

  let workedMinutes = 0;
  let breakMinutes = 0;
  let workStart = null;
  let openBreak = null;
  let longestBreakMin = 0;
  let breakCount = 0;

  for (const ev of events || []) {
    if (ev.action === "login" || ev.action === "back") {
      if (!workStart) {
        workStart = ev.created_at;
      }
      if (ev.action === "back" && openBreak) {
        const oneBreak = minutesBetween(openBreak.created_at, ev.created_at);
        breakMinutes += oneBreak;
        if (oneBreak > longestBreakMin) longestBreakMin = oneBreak;
        openBreak = null;
      }
      continue;
    }

    if (ev.action === "break") {
      if (workStart) {
        workedMinutes += minutesBetween(workStart, ev.created_at);
        workStart = null;
      }
      openBreak = ev;
      breakCount += 1;
      continue;
    }

    if (ev.action === "logout") {
      if (workStart) {
        workedMinutes += minutesBetween(workStart, ev.created_at);
        workStart = null;
      }
      if (openBreak) {
        const oneBreak = minutesBetween(openBreak.created_at, ev.created_at);
        breakMinutes += oneBreak;
        if (oneBreak > longestBreakMin) longestBreakMin = oneBreak;
        openBreak = null;
      }
    }
  }

  if (workStart) {
    workedMinutes += minutesBetween(workStart);
  }

  if (openBreak) {
    const oneBreak = minutesBetween(openBreak.created_at);
    breakMinutes += oneBreak;
    if (oneBreak > longestBreakMin) longestBreakMin = oneBreak;
  }

  const firstLogin = getFirstLoginEvent(events);
  const lastLogout = getLastLogoutEvent(events);
  const latest = events?.length ? events[events.length - 1] : null;

  const lateMinutes = firstLogin
    ? Math.max(
        0,
        Math.round(
          (new Date(firstLogin.created_at) - new Date(shiftStartIso)) / 60000,
        ),
      )
    : 0;

  return {
    firstLogin,
    lastLogout,
    latest,
    workedMinutes,
    breakMinutes,
    breakCount,
    longestBreakMin,
    currentStatus: latest?.action || "no_update",
    lateMinutes,
    longShiftFlag: workedMinutes > LONG_SHIFT_THRESHOLD_MIN,
    longBreakFlag: longestBreakMin >= LONG_BREAK_THRESHOLD_MIN,
    possibleHalfDay:
      workedMinutes > 0 && workedMinutes < HALF_DAY_THRESHOLD_MIN,
  };
}

async function logIncomingMessage(user, reqBody, body, from) {
  const incoming = {
    org_id: user?.org_id ?? DASHBOARD_ORG_ID,
    user_id: user?.id ?? null,
    phone_number: from,
    wa_id: reqBody.WaId || null,
    profile_name: reqBody.ProfileName || null,
    direction: "inbound",
    message_text: body,
    message_type: reqBody.MessageType || "unknown",
    media_count: Number(reqBody.NumMedia || 0),
    twilio_message_sid: reqBody.MessageSid || null,
    payload: reqBody,
  };

  const { error } = await supabase.from("message_logs").insert([incoming]);

  if (error) {
    if (error.code === "23505") {
      console.warn(
        "Duplicate MessageSid detected; skipping message_logs insert.",
      );
      return { duplicate: true, error: null };
    }

    console.error("Supabase insert error:", error);
    return { duplicate: false, error };
  }

  console.log("Message saved to Supabase");
  return { duplicate: false, error: null };
}

async function beginInboundProcessing(
  messageSid,
  phoneNumber,
  normalizedText,
  orgId = null,
) {
  const row = {
    org_id: orgId,
    message_sid: messageSid,
    phone_number: phoneNumber || null,
    normalized_text: normalizedText || null,
    status: "processing",
    updated_at: new Date().toISOString(),
  };

  const { data, error } = await supabase
    .from("inbound_message_processing")
    .insert([row])
    .select("*")
    .maybeSingle();

  if (error) {
    if (error.code === "23505") {
      return { duplicate: true, row: null, error: null };
    }
    return { duplicate: false, row: null, error };
  }

  return { duplicate: false, row: data, error: null };
}

async function completeInboundProcessing(
  messageSid,
  resultType,
  resultRefId = null,
  orgId = null,
) {
  let query = supabase
    .from("inbound_message_processing")
    .update({
      status: "completed",
      result_type: resultType || null,
      result_ref_id: resultRefId || null,
      updated_at: new Date().toISOString(),
    })
    .eq("message_sid", messageSid);

  if (orgId != null) {
    query = query.eq("org_id", orgId);
  }

  const { error } = await query;
  if (error) console.error("completeInboundProcessing error:", error);
}

async function failInboundProcessing(messageSid, errorMessage, orgId = null) {
  let query = supabase
    .from("inbound_message_processing")
    .update({
      status: "failed",
      error_message: errorMessage || "unknown_error",
      updated_at: new Date().toISOString(),
    })
    .eq("message_sid", messageSid);

  if (orgId != null) {
    query = query.eq("org_id", orgId);
  }

  const { error } = await query;
  if (error) console.error("failInboundProcessing error:", error);
}

async function handleExtraWork(res, user, command, messageSid = null) {
  const note = String(command?.note || "").trim();

  if (!note) {
    return sendTwiml(
      res,
      "Please add a note.\nExample: extra work helped aj debug org id issue",
    );
  }

  const reportDate = getReportDateString();

  const { error } = await insertDailyReportNote({
    orgId: user.org_id,
    userId: user.id,
    reportDate,
    note,
    createdByUserId: user.id,
    sourceMessageSid: messageSid,
  });

  if (error) {
    if (error.code === "23505") {
      return sendTwiml(
        res,
        `✅ Extra work already saved for today\nNote: ${note}`,
      );
    }

    console.error("handleExtraWork error:", error);
    return sendTwiml(res, "Failed to save extra work.");
  }

  return sendTwiml(res, `✅ Extra work saved for today\nNote: ${note}`);
}

async function handleEmployeeSummary(res, actingUser, command) {
  const targetUser = command.target_name
    ? await findUniqueUserByName(command.target_name, actingUser.org_id)
    : actingUser;

  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${command.target_name}".`,
    );
  }

  try {
    const monthly = await getEmployeeMonthlyAttendanceSummary(
      targetUser.id,
      actingUser.org_id,
    );

    const lines = [
      `📊 Employee summary: ${targetUser.name}`,
      "",
      `Present days this month: ${monthly.presentDays}`,
      `Total leave entries this month: ${monthly.leaveDays}`,
      `Past leave dates: ${formatDateListForHumans(monthly.pastLeaveDates)}`,
      `Upcoming planned leave dates: ${formatDateListForHumans(monthly.upcomingLeaveDates)}`,
      `Late joins this month: ${monthly.lateJoins}`,
      `Approved late: ${monthly.approvedLate}`,
      `Late with prior info but not approved: ${monthly.unapprovedLate}`,
      `Late without prior info: ${monthly.uninformedLate}`,
      `Average login time/day: ${monthly.avgLoginTimeText}`,
      `Average break time/day: ${formatDurationMinutes(monthly.avgBreakMin)}`,
      `Long shift flags: ${monthly.longShiftCount}`,
      `Long break flags: ${monthly.longBreakCount}`,
      `Possible half days: ${monthly.possibleHalfDays}`,
      `Manager corrections: ${monthly.managerCorrectionCount}`,
    ];

    return sendTwiml(res, lines.join("\n"));
  } catch (error) {
    console.error("Employee summary error:", error);
    return sendTwiml(res, "Failed to fetch employee summary.");
  }
}

async function handleDeadlineUpdate(res, user, taskId, dateText) {
  if (!isManagerOrAdmin(user)) {
    return sendTwiml(res, "Only managers can change deadlines.");
  }

  const parsedDate = parseDeadline(dateText);
  if (!parsedDate) {
    return sendTwiml(
      res,
      "Invalid date. Try: deadline 12 5 Apr OR deadline 12 tomorrow",
    );
  }

  const isoDate = parsedDate;

  const { task, error } = await getTaskById(taskId, user.org_id);

  if (error) return sendTwiml(res, "Failed to fetch task.");
  if (!task) return sendTwiml(res, `Task #${taskId} not found.`);

  const oldDeadline = task.deadline;

  const { error: updateError } = await supabase
    .from("tasks")
    .update({
      deadline: isoDate,
      last_updated_by_user_id: user.id,
      updated_at: new Date().toISOString(),
    })
    .eq("id", task.id);

  if (updateError) {
    console.error(updateError);
    return sendTwiml(res, "Failed to update deadline.");
  }

  await insertTaskHistory(
    task.id,
    user.id,
    "deadline_change",
    "deadline",
    { deadline: oldDeadline },
    { deadline: isoDate },
    user.org_id,
  );

  return sendTwiml(
    res,
    `📅 Deadline updated for Task ${taskRef(task)}\nNew deadline: ${isoDate}`,
  );
}

async function handleEditTask(res, user, editCommand) {
  const { task, error } = await getTaskById(editCommand.taskId, user.org_id);

  if (error) {
    return sendTwiml(res, "Failed to fetch that task.");
  }

  if (!task) {
    return sendTwiml(res, `Task #${editCommand.taskId} not found.`);
  }

  if (!(await canModifyTask(user, task))) {
    return sendTwiml(res, "You are not allowed to edit that task.");
  }

  const patch = {
    last_updated_by_user_id: user.id,
    updated_at: new Date().toISOString(),
  };

  let oldValue = {};
  let newValue = {};
  let successMessage = "";

  if (editCommand.field === "title") {
    if (!editCommand.value) return sendTwiml(res, "Title cannot be empty.");
    oldValue = { title: task.title };
    newValue = { title: editCommand.value };
    patch.title = editCommand.value;
    successMessage = `✏️ Task ${taskRef(task)} title updated\nNew title: ${editCommand.value}`;
  } else if (editCommand.field === "detail") {
    if (!editCommand.value) return sendTwiml(res, "Detail cannot be empty.");
    oldValue = { detail: task.detail };
    newValue = { detail: editCommand.value };
    patch.detail = editCommand.value;
    successMessage = `✏️ Task ${taskRef(task)} detail updated\nNew detail: ${editCommand.value}`;
  } else if (editCommand.field === "priority") {
    oldValue = { priority: task.priority };
    newValue = { priority: editCommand.value };
    patch.priority = editCommand.value;
    successMessage = `✏️ Task ${taskRef(task)} priority updated\nNew priority: ${editCommand.value}`;
  } else if (editCommand.field === "business") {
    if (!editCommand.value) return sendTwiml(res, "Business cannot be empty.");
    oldValue = { business: task.business };
    newValue = { business: editCommand.value };
    patch.business = editCommand.value;
    successMessage = `✏️ Task ${taskRef(task)} business updated\nNew business: ${editCommand.value}`;
  } else if (editCommand.field === "area") {
    if (!editCommand.value) return sendTwiml(res, "Area cannot be empty.");
    oldValue = { area: task.area };
    newValue = { area: editCommand.value };
    patch.area = editCommand.value;
    successMessage = `✏️ Task ${taskRef(task)} area updated\nNew area: ${editCommand.value}`;
  } else if (editCommand.field === "deadline") {
    const parsedDate = parseDeadline(editCommand.value);
    if (!parsedDate) {
      return sendTwiml(
        res,
        `I could not understand the deadline "${editCommand.value}". Use today, tomorrow, friday, 11 april, or april 11.`,
      );
    }

    oldValue = { deadline: task.deadline };
    newValue = { deadline: parsedDate };
    patch.deadline = parsedDate;
    successMessage = `📅 Task ${taskRef(task)} deadline updated\nNew deadline: ${parsedDate}`;
  } else if (editCommand.field === "status") {
    if (editCommand.value === "cancelled" && !isManagerOrAdmin(user)) {
      return sendTwiml(
        res,
        "Only managers/admins can set status to cancelled.",
      );
    }

    oldValue = {
      status: task.status,
      progress: task.progress,
      blocker_note: task.blocker_note,
    };

    newValue = { status: editCommand.value };

    patch.status = editCommand.value;

    if (editCommand.value === "done") {
      patch.progress = 100;
      newValue.progress = 100;
    }

    if (editCommand.value === "open" && task.progress === 100) {
      patch.progress = 0;
      newValue.progress = 0;
    }

    if (task.blocker_note) {
      patch.blocker_note = null;
      newValue.blocker_note = null;
    }

    successMessage = `✏️ Task ${taskRef(task)} status updated\nNew status: ${editCommand.value}`;
  } else if (editCommand.field === "progress") {
    return sendTwiml(
      res,
      "❌ Progress can only be updated using the progress command\nUse: progress <task_id> <percent> <detailed note>",
    );
  } else if (editCommand.field === "blocker_note") {
    if (!editCommand.value) {
      return sendTwiml(res, "Blocker note cannot be empty.");
    }

    oldValue = { blocker_note: task.blocker_note, status: task.status };
    newValue = { blocker_note: editCommand.value, status: "blocked" };

    patch.blocker_note = editCommand.value;
    patch.status = "blocked";

    successMessage = `⛔ Task ${taskRef(task)} blocker updated\nBlocker: ${editCommand.value}`;
  } else if (editCommand.field === "clear_detail") {
    oldValue = { detail: task.detail };
    newValue = { detail: null };
    patch.detail = null;
    successMessage = `✏️ Task ${taskRef(task)} detail cleared`;
  } else if (editCommand.field === "clear_blocker") {
    oldValue = { blocker_note: task.blocker_note, status: task.status };
    newValue = {
      blocker_note: null,
      status: task.progress > 0 ? "in_progress" : "open",
    };

    patch.blocker_note = null;
    patch.status = task.progress > 0 ? "in_progress" : "open";

    successMessage = `✏️ Task ${taskRef(task)} blocker cleared`;
  } else if (editCommand.field === "clear_business") {
    oldValue = { business: task.business };
    newValue = { business: null };
    patch.business = null;
    successMessage = `✏️ Task ${taskRef(task)} business cleared`;
  } else if (editCommand.field === "clear_area") {
    oldValue = { area: task.area };
    newValue = { area: null };
    patch.area = null;
    successMessage = `✏️ Task ${taskRef(task)} area cleared`;
  } else if (editCommand.field === "clear_deadline") {
    oldValue = { deadline: task.deadline };
    newValue = { deadline: null };
    patch.deadline = null;
    successMessage = `✏️ Task ${taskRef(task)} deadline cleared`;
  } else if (editCommand.field === "owner") {
    if (!isManagerOrAdmin(user)) {
      return sendTwiml(res, "Only managers/admins can change task owners.");
    }

    const ownerNames = parseOwnerNames(editCommand.value);
    if (!ownerNames.length) {
      return sendTwiml(res, "Please provide at least one owner name.");
    }

    const { matchedUsers, missingNames } = await findUsersByNames(
      ownerNames,
      user.org_id,
    );

    if (missingNames.length) {
      return sendTwiml(
        res,
        `❌ Could not find these users: ${missingNames.join(", ")}`,
      );
    }

    const oldOwnerNames = task.owner_names || [];

    const { error: deleteError } = await supabase
      .from("task_owners")
      .delete()
      .eq("task_id", task.id)
      .eq("org_id", user.org_id);

    if (deleteError) {
      console.error("Task owner delete error:", deleteError);
      return sendTwiml(res, "Failed to update task owners.");
    }

    const ownerRows = matchedUsers.map((owner) => ({
      org_id: user.org_id,
      task_id: task.id,
      user_id: owner.id,
    }));

    const { error: insertError } = await supabase
      .from("task_owners")
      .insert(ownerRows);

    if (insertError) {
      console.error("Task owner insert error:", insertError);
      return sendTwiml(res, "Failed to update task owners.");
    }

    const { error: taskUpdateError } = await supabase
      .from("tasks")
      .update({
        assigned_to_user_id: matchedUsers[0]?.id || null,
        last_updated_by_user_id: user.id,
        updated_at: new Date().toISOString(),
      })
      .eq("id", task.id);

    if (taskUpdateError) {
      console.error("Task assigned_to update error:", taskUpdateError);
      return sendTwiml(res, "Failed to finish owner update.");
    }

    await insertTaskHistory(
      task.id,
      user.id,
      "owner_change",
      "owner",
      { owners: oldOwnerNames },
      { owners: matchedUsers.map((x) => x.name) },
      user.org_id,
    );

    return sendTwiml(
      res,
      `👥 Task ${taskRef(task)} owners updated\nNew owners: ${matchedUsers.map((x) => x.name).join(", ")}`,
    );
  } else {
    return sendTwiml(res, "That task field cannot be edited.");
  }

  const { error: updateError } = await supabase
    .from("tasks")
    .update(patch)
    .eq("id", task.id);

  if (updateError) {
    console.error("Edit task update error:", updateError);
    return sendTwiml(res, "Failed to edit that task.");
  }

  await insertTaskHistory(
    task.id,
    user.id,
    "edit",
    editCommand.field,
    oldValue,
    newValue,
    user.org_id,
  );

  return sendTwiml(res, successMessage);
}

async function handleTimelineAttendance(res, actingUser, command) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to view attendance timeline.");
  }

  const targetUser = await findUniqueUserByName(
    command.target_name,
    actingUser.org_id,
  );
  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${command.target_name}".`,
    );
  }

  const attendanceDate = parseFlexibleDateText(command.date_text);
  if (!attendanceDate) {
    return sendTwiml(
      res,
      `I could not understand the date "${command.date_text}".`,
    );
  }

  try {
    const events = await getAttendanceEventsForUserOnAttendanceDay(
      targetUser.id,
      attendanceDate,
      actingUser.org_id,
    );

    const lines = [
      `🧾 Timeline: ${targetUser.name}`,
      `Date: ${attendanceDate}`,
      "",
      ...buildAttendanceTimelineLines(events),
    ];

    return sendTwiml(res, lines.join("\n"));
  } catch (error) {
    console.error("Timeline attendance error:", error);
    return sendTwiml(res, "Failed to fetch attendance timeline.");
  }
}

async function handleAuditAttendance(res, actingUser, command) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to audit attendance.");
  }

  const targetUser = await findUniqueUserByName(
    command.target_name,
    actingUser.org_id,
  );
  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${command.target_name}".`,
    );
  }

  const attendanceDate = parseFlexibleDateText(command.date_text);
  if (!attendanceDate) {
    return sendTwiml(
      res,
      `I could not understand the date "${command.date_text}".`,
    );
  }

  try {
    const events = await getAttendanceEventsForUserOnAttendanceDay(
      targetUser.id,
      attendanceDate,
      actingUser.org_id,
    );

    const issues = analyzeAttendanceIssues(events);

    const lines = [
      `🔍 Attendance audit: ${targetUser.name}`,
      `Date: ${attendanceDate}`,
      "",
      issues.length
        ? issues.map((x) => `• ${x}`).join("\n")
        : "✅ No obvious attendance issues found",
    ];

    return sendTwiml(res, lines.join("\n"));
  } catch (error) {
    console.error("Audit attendance error:", error);
    return sendTwiml(res, "Failed to audit attendance.");
  }
}

async function handleUndoAttendance(res, actingUser, command) {
  const isSelf = command.mode === "self";
  const targetUser = isSelf
    ? actingUser
    : await findUniqueUserByName(command.target_name, actingUser.org_id);

  if (!isSelf && !isManagerOrAdmin(actingUser)) {
    return sendTwiml(
      res,
      "You are not allowed to undo other people's attendance.",
    );
  }

  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${command.target_name}".`,
    );
  }

  try {
    const latestEvent = await getLatestAttendanceEvent(
      targetUser.id,
      actingUser.org_id,
    );
    if (!latestEvent) {
      return sendTwiml(
        res,
        `No attendance event found to undo for ${targetUser.name}.`,
      );
    }

    const attendanceDate = getAttendanceDayDateStringFromDate(
      new Date(latestEvent.created_at),
    );
    const locked = await isAttendanceDayLocked(
      targetUser.id,
      attendanceDate,
      actingUser.org_id,
    );

    if (locked) {
      return sendTwiml(
        res,
        `❌ Attendance is locked for ${targetUser.name} on ${attendanceDate}`,
      );
    }

    const deleteError = await deleteAttendanceEventById(
      latestEvent.id,
      actingUser.org_id,
    );
    if (deleteError) {
      console.error("Undo attendance delete error:", deleteError);
      return sendTwiml(res, "Failed to undo attendance.");
    }

    await insertAttendanceAudit(
      targetUser.id,
      actingUser.id,
      "undo_attendance",
      latestEvent,
      null,
      `Deleted latest attendance event (${latestEvent.action})`,
      actingUser.org_id,
    );

    return sendTwiml(
      res,
      `↩ Attendance undone for ${targetUser.name}\nRemoved: ${latestEvent.action} at ${formatTimeOnly(latestEvent.created_at)}`,
    );
  } catch (error) {
    console.error("Undo attendance error:", error);
    return sendTwiml(res, "Failed to undo attendance.");
  }
}

async function handleResetAttendance(res, actingUser, command) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to reset attendance.");
  }

  const targetUser = await findUniqueUserByName(
    command.target_name,
    actingUser.org_id,
  );
  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${command.target_name}".`,
    );
  }

  const attendanceDate = parseFlexibleDateText(command.date_text);
  if (!attendanceDate) {
    return sendTwiml(
      res,
      `I could not understand the date "${command.date_text}".`,
    );
  }

  const locked = await isAttendanceDayLocked(
    targetUser.id,
    attendanceDate,
    actingUser.org_id,
  );
  if (locked) {
    return sendTwiml(
      res,
      `❌ Attendance is locked for ${targetUser.name} on ${attendanceDate}`,
    );
  }

  try {
    const oldEvents = await getAttendanceEventsForUserOnAttendanceDay(
      targetUser.id,
      attendanceDate,
      actingUser.org_id,
    );

    const [attendanceError, lateError, offError] = await Promise.all([
      deleteAttendanceEventsForUserOnAttendanceDay(
        targetUser.id,
        attendanceDate,
        actingUser.org_id,
      ),
      deleteLateArrivalForUserOnDate(
        targetUser.id,
        attendanceDate,
        actingUser.org_id,
      ),
      deletePlannedOffForUserOnDate(
        targetUser.id,
        attendanceDate,
        actingUser.org_id,
      ),
    ]);

    if (attendanceError || lateError || offError) {
      console.error("Reset attendance errors:", {
        attendanceError,
        lateError,
        offError,
      });
      return sendTwiml(res, "Failed to reset attendance.");
    }

    await insertAttendanceAudit(
      targetUser.id,
      actingUser.id,
      "reset_attendance_day",
      {
        attendance_date: attendanceDate,
        old_events: oldEvents,
      },
      {
        attendance_date: attendanceDate,
        reset: true,
      },
      `Attendance reset by ${actingUser.name}`,
      actingUser.org_id,
    );

    return sendTwiml(
      res,
      `⚠ Attendance reset for ${targetUser.name}\nDate: ${attendanceDate}\nAll attendance + late + leave entries cleared for that date`,
    );
  } catch (error) {
    console.error("Reset attendance fatal error:", error);
    return sendTwiml(res, "Failed to reset attendance.");
  }
}

async function handleForceAttendance(res, actingUser, command) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to force attendance changes.");
  }

  const targetUser = await findUniqueUserByName(
    command.target_name,
    actingUser.org_id,
  );
  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${command.target_name}".`,
    );
  }

  const occurredAtIso = command.time_text
    ? parseLocalDateTimeForToday(command.time_text)
    : new Date().toISOString();

  if (command.time_text && !occurredAtIso) {
    return sendTwiml(
      res,
      `Could not understand the time "${command.time_text}". Use format like 2:30 PM.`,
    );
  }

  if (new Date(occurredAtIso) > new Date()) {
    return sendTwiml(res, "❌ Future attendance corrections are not allowed");
  }

  const attendanceDate = getAttendanceDayDateStringFromDate(
    new Date(occurredAtIso),
  );
  const locked = await isAttendanceDayLocked(
    targetUser.id,
    attendanceDate,
    actingUser.org_id,
  );

  if (locked) {
    return sendTwiml(
      res,
      `❌ Attendance is locked for ${targetUser.name} on ${attendanceDate}`,
    );
  }

  let durationMin = null;
  let note = `Force ${command.action} by ${actingUser.name}`;

  if (command.action === "back") {
    const lastBreak = await getLatestBreakEventAtOrBefore(
      targetUser.id,
      actingUser.org_id,
      occurredAtIso,
    );
    if (lastBreak) {
      durationMin = minutesBetween(lastBreak.created_at, occurredAtIso);
      note += ` | Actual break: ${durationMin} min`;
    }
  }

  const attendanceRow = {
    org_id: actingUser.org_id,
    user_id: targetUser.id,
    target_phone: targetUser.phone_number,
    acted_by_phone: actingUser.phone_number,
    action: command.action,
    duration_min: durationMin,
    expected_duration_min: null,
    reason: null,
    note,
    created_at: occurredAtIso,
  };

  const { error } = await supabase
    .from("attendance_events")
    .insert([attendanceRow]);

  if (error) {
    console.error("Force attendance insert error:", error);
    return sendTwiml(res, "Failed to force attendance change.");
  }

  await insertAttendanceAudit(
    targetUser.id,
    actingUser.id,
    `force_${command.action}`,
    null,
    attendanceRow,
    note,
    actingUser.org_id,
  );

  return sendTwiml(
    res,
    `⚠ Forced ${command.action} for ${targetUser.name}${command.time_text ? ` at ${command.time_text}` : ""}`,
  );
}

async function handleFixAttendance(res, actingUser, command) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to fix attendance.");
  }

  const targetUser = await findUniqueUserByName(
    command.target_name,
    actingUser.org_id,
  );
  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${command.target_name}".`,
    );
  }

  const correctedIso = parseLocalDateTimeForToday(command.time_text);
  if (!correctedIso) {
    return sendTwiml(
      res,
      `Could not understand the time "${command.time_text}". Use format like 2:30 PM.`,
    );
  }

  if (new Date(correctedIso) > new Date()) {
    return sendTwiml(res, "❌ Future attendance corrections are not allowed");
  }

  const attendanceDate = getAttendanceDayDateStringFromDate(
    new Date(correctedIso),
  );
  const locked = await isAttendanceDayLocked(
    targetUser.id,
    attendanceDate,
    actingUser.org_id,
  );

  if (locked) {
    return sendTwiml(
      res,
      `❌ Attendance is locked for ${targetUser.name} on ${attendanceDate}`,
    );
  }

  const latestActionEvent = await getLatestAttendanceEventByAction(
    targetUser.id,
    actingUser.org_id,
    command.action,
    attendanceDate,
  );

  if (!latestActionEvent) {
    return sendTwiml(
      res,
      `No ${command.action} event found for ${targetUser.name} on ${attendanceDate}.`,
    );
  }

  const oldValue = { ...latestActionEvent };

  const patch = {
    created_at: correctedIso,
    note: `${latestActionEvent.note ? latestActionEvent.note + " | " : ""}Fixed by ${actingUser.name}`,
  };

  let durationMin = latestActionEvent.duration_min;

  if (command.action === "back") {
    const lastBreak = await getLatestBreakEventAtOrBefore(
      targetUser.id,
      actingUser.org_id,
      correctedIso,
    );
    if (lastBreak) {
      durationMin = minutesBetween(lastBreak.created_at, correctedIso);
      patch.duration_min = durationMin;
    }
  }

  const { error } = await supabase
    .from("attendance_events")
    .update(patch)
    .eq("id", latestActionEvent.id);

  if (error) {
    console.error("Fix attendance update error:", error);
    return sendTwiml(res, "Failed to fix attendance.");
  }

  await insertAttendanceAudit(
    targetUser.id,
    actingUser.id,
    `fix_${command.action}`,
    oldValue,
    {
      ...oldValue,
      ...patch,
    },
    `Fixed ${command.action} time by ${actingUser.name}`,
    actingUser.org_id,
  );

  return sendTwiml(
    res,
    `🛠 Fixed ${command.action} for ${targetUser.name}\nNew time: ${command.time_text}`,
  );
}

async function handleRemoveAttendance(res, actingUser, command) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to remove attendance events.");
  }

  const targetUser = await findUniqueUserByName(
    command.target_name,
    actingUser.org_id,
  );
  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${command.target_name}".`,
    );
  }

  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const locked = await isAttendanceDayLocked(
    targetUser.id,
    attendanceDate,
    actingUser.org_id,
  );

  if (locked) {
    return sendTwiml(
      res,
      `❌ Attendance is locked for ${targetUser.name} on ${attendanceDate}`,
    );
  }

  const latestActionEvent = await getLatestAttendanceEventByAction(
    targetUser.id,
    actingUser.org_id,
    command.action,
    attendanceDate,
  );

  if (!latestActionEvent) {
    return sendTwiml(
      res,
      `No ${command.action} event found for ${targetUser.name} today.`,
    );
  }

  const deleteError = await deleteAttendanceEventById(
    latestActionEvent.id,
    actingUser.org_id,
  );

  if (deleteError) {
    console.error("Remove attendance delete error:", deleteError);
    return sendTwiml(res, "Failed to remove attendance event.");
  }

  await insertAttendanceAudit(
    targetUser.id,
    actingUser.id,
    `remove_${command.action}`,
    latestActionEvent,
    null,
    `Removed latest ${command.action} event by ${actingUser.name}`,
    actingUser.org_id,
  );

  return sendTwiml(
    res,
    `🧹 Removed latest ${command.action} for ${targetUser.name}\nWas at: ${formatTimeOnly(latestActionEvent.created_at)}`,
  );
}

async function handleAutoFixAttendance(res, actingUser, command) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to auto-fix attendance.");
  }

  const targetUser = await findUniqueUserByName(
    command.target_name,
    actingUser.org_id,
  );
  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${command.target_name}".`,
    );
  }

  const attendanceDate = parseFlexibleDateText(command.date_text);
  if (!attendanceDate) {
    return sendTwiml(
      res,
      `I could not understand the date "${command.date_text}".`,
    );
  }

  const locked = await isAttendanceDayLocked(
    targetUser.id,
    attendanceDate,
    actingUser.org_id,
  );

  if (locked) {
    return sendTwiml(
      res,
      `❌ Attendance is locked for ${targetUser.name} on ${attendanceDate}`,
    );
  }

  try {
    const events = await getAttendanceEventsForUserOnAttendanceDay(
      targetUser.id,
      attendanceDate,
      actingUser.org_id,
    );

    if (!events.length) {
      return sendTwiml(
        res,
        `No attendance events found for ${targetUser.name} on ${attendanceDate}.`,
      );
    }

    const latest = events[events.length - 1];
    const applied = [];

    if (latest.action === "break") {
      const forcedBackRow = {
        org_id: actingUser.org_id,
        user_id: targetUser.id,
        target_phone: targetUser.phone_number,
        acted_by_phone: actingUser.phone_number,
        action: "back",
        duration_min: minutesBetween(latest.created_at),
        expected_duration_min: null,
        reason: null,
        note: `Auto-fix back by ${actingUser.name}`,
      };

      const { error: insertBackError } = await supabase
        .from("attendance_events")
        .insert([forcedBackRow]);

      if (!insertBackError) {
        applied.push("closed open break with back");
      }
    }

    const refreshedEvents = await getAttendanceEventsForUserOnAttendanceDay(
      targetUser.id,
      attendanceDate,
      actingUser.org_id,
    );

    const refreshedLatest = refreshedEvents[refreshedEvents.length - 1];

    if (
      refreshedLatest &&
      (refreshedLatest.action === "login" || refreshedLatest.action === "back")
    ) {
      const forcedLogoutRow = {
        org_id: actingUser.org_id,
        user_id: targetUser.id,
        target_phone: targetUser.phone_number,
        acted_by_phone: actingUser.phone_number,
        action: "logout",
        duration_min: null,
        expected_duration_min: null,
        reason: null,
        note: `Auto-fix logout by ${actingUser.name}`,
      };

      const { error: insertLogoutError } = await supabase
        .from("attendance_events")
        .insert([forcedLogoutRow]);

      if (!insertLogoutError) {
        applied.push("closed open session with logout");
      }
    }

    await insertAttendanceAudit(
      targetUser.id,
      actingUser.id,
      "auto_fix_attendance_day",
      { attendance_date: attendanceDate, before: events },
      { attendance_date: attendanceDate, actions_applied: applied },
      `Auto-fix by ${actingUser.name}`,
      actingUser.org_id,
    );

    return sendTwiml(
      res,
      `🛠 Auto-fix complete for ${targetUser.name}\nDate: ${attendanceDate}\n${
        applied.length
          ? applied.map((x) => `• ${x}`).join("\n")
          : "No changes were needed"
      }`,
    );
  } catch (error) {
    console.error("Auto-fix attendance error:", error);
    return sendTwiml(res, "Failed to auto-fix attendance.");
  }
}

async function handleLockAttendanceDay(res, actingUser, command) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to lock or unlock attendance.");
  }

  const targetUser = await findUniqueUserByName(
    command.target_name,
    actingUser.org_id,
  );
  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${command.target_name}".`,
    );
  }

  const attendanceDate = parseFlexibleDateText(command.date_text);
  if (!attendanceDate) {
    return sendTwiml(
      res,
      `I could not understand the date "${command.date_text}".`,
    );
  }

  const isLock = command.mode === "lock";
  const error = await setAttendanceDayLock(
    targetUser.id,
    attendanceDate,
    isLock,
    actingUser.id,
    actingUser.org_id,
    `${command.mode} by ${actingUser.name}`,
  );

  if (error) {
    console.error("Attendance day lock error:", error);
    return sendTwiml(res, `Failed to ${command.mode} attendance day.`);
  }

  await insertAttendanceAudit(
    targetUser.id,
    actingUser.id,
    `${command.mode}_attendance_day`,
    null,
    {
      attendance_date: attendanceDate,
      is_locked: isLock,
    },
    `${command.mode} attendance by ${actingUser.name}`,
    actingUser.org_id,
  );

  return sendTwiml(
    res,
    `${isLock ? "🔒" : "🔓"} Attendance ${isLock ? "locked" : "unlocked"} for ${targetUser.name}\nDate: ${attendanceDate}`,
  );
}

async function handleHelp(res, user, topic = "") {
  try {
    const isManager = isManagerOrAdmin(user);
    const normalizedTopic = normalizeText(topic || "");

    if (normalizedTopic === "tasks") {
      return sendTwiml(
        res,
        [
          "📋 Task Help",
          "",
          "Create tasks:",
          "create task present progress on rasset business rasset area general owner ruhab priority high due today",
          "create task finalize parents pitch business joolian area parents owner zoya, niharika, aj priority high due 4 apr",
          "",
          "View tasks:",
          "my tasks",
          "tasks Ruhab",
          "show task 2",
          "",
          "Update tasks:",
          "progress 2 50% 20 mails sent no positive response",
          "done 2 tested and verified",
          "deadline 2 11 april",
          "undo last task change",
          "edit task 2 title final parents pitch v2",
          "edit task 2 detail call parents and collect objections",
          "edit task 2 priority urgent",
          "edit task 2 business joolian",
          "edit task 2 area parents",
          "edit task 2 deadline tomorrow",
          "edit task 2 status pending",
          "edit task 2 status in_progress",
          "edit task 2 progress 70",
          "edit task 2 blocker waiting on backend fix",
          "edit task 2 clear blocker",
          "extra work helped aj debug org id issue",
          "",
          "Manager/Admin only:",
          "cancel task 2",
          "delete task 2",
          "edit task 2 owner zoya, aj",
          "",
          "Notes:",
          "• Use task number like show task 2",
          "• Progress note is required",
          "• Priority: low, medium, high, urgent",
          "• Advanced create-task supports business, area, and multiple owners",
        ].join("\n"),
      );
    }

    if (normalizedTopic === "attendance") {
      return sendTwiml(
        res,
        [
          "🕒 Attendance Help",
          "",
          "Your own attendance:",
          "login",
          "logout",
          "break",
          "back",
          "late 11:00 am",
          "late unsure",
          "status",
          "summary today",
          "now",
          "who am i",
          "",
          "Leave / off:",
          "off today",
          "leave tomorrow",
          "off 11 april",
          "",
          "Manager/Admin only:",
          "login Zoya",
          "logout Aj 6:30 pm",
          "break Ruhab",
          "back Mahesh",
          "employee summary Aj",
          "timeline Mahesh",
          "who is on break",
          "off Zoya tomorrow",
          "leave Aj 11 april",
          "",
          "Notes:",
          "• Use real times like 11:00 am",
          "• Use today / tomorrow / weekday / date",
        ].join("\n"),
      );
    }

    if (normalizedTopic === "manager") {
      if (!isManager) {
        return sendTwiml(
          res,
          "❌ Only managers/admins can use this help section.",
        );
      }

      return sendTwiml(
        res,
        [
          "🧑‍💼 Manager/Admin Help",
          "",
          "Attendance for others:",
          "login Zoya",
          "logout Aj 6:30 pm",
          "break Ruhab",
          "back Mahesh",
          "late Zoya 11:00 am",
          "late Ruhab unsure",
          "employee summary Aj",
          "timeline Mahesh",
          "who is on break",
          "off Zoya tomorrow",
          "",
          "Task management:",
          "tasks Ruhab",
          "show task 2",
          "Task examples:",
          "progress 2 50% 20 mails sent no positive response",
          "edit task 2 blocker waiting on dependency",
          "edit task 2 clear blocker",
          "done 2 tested and verified",
          "cancel task 2",
          "delete task 2",
          "",
          "Notes:",
          "• Managers can view and manage other users' tasks",
          "• Shared tasks may appear under each owner's task list",
        ].join("\n"),
      );
    }

    const lines = [
      "🤖 WeSolveHR Help",
      "",
      "Common commands:",
      "login",
      "logout",
      "break",
      "back",
      "status",
      "my tasks",
      "show task 2",
      "summary today",
      "",
      "Task examples:",
      "task Ruhab high present progress on Rasset by today",
      "progress 2 50% 20 mails sent no positive response",
      "edit task 2 blocker waiting on dependency",
      "edit task 2 clear blocker",
      "done 2 tested and verified",
      "",
      "Advanced task example:",
      "create task finalize parents pitch business joolian area parents owner zoya, niharika, aj priority high due 4 apr",
      "",
      "More help:",
      "help attendance",
      "help tasks",
      isManager ? "help manager" : "ask your manager for manager commands",
    ];

    return sendTwiml(res, lines.join("\n"));
  } catch (err) {
    console.error("handleHelp failed:", err);
    return sendTwiml(res, "❌ Help failed");
  }
}

async function handleMyTasks(res, user) {
  const { data, error } = await supabase
    .from("task_owners")
    .select(
      `
      task_id,
      tasks!inner(id, task_no, title, priority, status, progress, deadline)
    `,
    )
    .eq("user_id", user.id)
    .eq("org_id", user.org_id);

  if (error) {
    console.error("My tasks query error:", error);
    return sendTwiml(res, "Failed to fetch your tasks.");
  }

  const tasks = (data || [])
    .map((x) => x.tasks)
    .filter((t) => t && !["done", "archived", "cancelled"].includes(t.status));

  if (!tasks.length) {
    return sendTwiml(res, "You have no open tasks.");
  }

  const lines = tasks
    .slice(0, 8)
    .map(
      (task) =>
        `#${task.task_no || task.id}${task.priority ? ` | ${task.priority}` : ""} | ${task.status} | ${task.title} | due ${task.deadline ?? "no deadline"} | ${task.progress}%`,
    );

  const suffix = tasks.length > 8 ? `\n...and ${tasks.length - 8} more.` : "";

  return sendTwiml(res, `Your open tasks:\n${lines.join("\n")}${suffix}`);
}

async function handleShowTask(res, user, taskId) {
  const { task, error } = await getTaskById(taskId, user.org_id);

  if (error) {
    return sendTwiml(res, "Failed to fetch that task.");
  }

  if (!task) {
    return sendTwiml(res, `Task #${taskId} not found.`);
  }

  if (!(await canReadTask(user, task))) {
    return sendTwiml(res, "You are not allowed to view that task.");
  }

  const assignedTo = task.owner_names?.length
    ? task.owner_names.join(", ")
    : "Unknown";
  const detail = task.detail ? `\nDetail: ${task.detail}` : "";
  const blocker = task.blocker_note ? `\nBlocker: ${task.blocker_note}` : "";

  return sendTwiml(
    res,
    `Task #${task.task_no || task.id}\nOwners: ${assignedTo}\nPriority: ${task.priority}\nStatus: ${task.status}\nProgress: ${task.progress}%\nTitle: ${task.title}\nDeadline: ${task.deadline ?? "no deadline"}${detail}${blocker}`,
  );
}

async function handleDoneTask(res, user, taskId, note) {
  const { task, error } = await getTaskById(taskId, user.org_id);

  if (error) {
    return sendTwiml(res, "Failed to fetch that task.");
  }

  if (!task) {
    return sendTwiml(res, `Task #${taskId} not found.`);
  }

  if (!(await canModifyTask(user, task))) {
    return sendTwiml(res, "You are not allowed to modify that task.");
  }

  const noteCheck = validateDetailedTaskNote(note);
  if (!noteCheck.ok) {
    return sendTwiml(res, noteCheck.message);
  }

  const cleanNote = noteCheck.cleanNote;

  if (task.status === "done") {
    return sendTwiml(res, `Task ${taskRef(task)} is already marked done.`);
  }

  const { error: updateError } = await supabase
    .from("tasks")
    .update({
      status: "done",
      progress: 100,
      last_updated_by_user_id: user.id,
      updated_at: new Date().toISOString(),
    })
    .eq("id", task.id);

  if (updateError) {
    console.error("Done task update error:", updateError);
    return sendTwiml(res, "Failed to mark the task done.");
  }

  await insertTaskHistory(
    task.id,
    user.id,
    "status_change",
    "status",
    { status: task.status, progress: task.progress, note: null },
    { status: "done", progress: 100, note: cleanNote },
    user.org_id,
  );

  return sendTwiml(
    res,
    `✅ Task ${taskRef(task)} marked done\nTitle: ${task.title}\nNote: ${cleanNote}`,
  );
}

async function handleProgressTask(res, user, taskId, progressValue, note) {
  const { task, error } = await getTaskById(taskId, user.org_id);

  if (error) {
    return sendTwiml(res, "Failed to fetch that task.");
  }

  if (!task) {
    return sendTwiml(res, `Task #${taskId} not found.`);
  }

  if (!(await canModifyTask(user, task))) {
    return sendTwiml(res, "You are not allowed to modify that task.");
  }

  if (
    progressValue === null ||
    progressValue === undefined ||
    Number.isNaN(Number(progressValue))
  ) {
    return sendTwiml(
      res,
      "Progress must be a number between 0 and 100.\nExample: progress 12 50 finished API testing and verified responses",
    );
  }

  const numericProgress = Number(progressValue);

  if (numericProgress < 0 || numericProgress > 100) {
    return sendTwiml(res, "Progress must be between 0 and 100.");
  }

  const noteCheck = validateDetailedTaskNote(note);
  if (!noteCheck.ok) {
    return sendTwiml(res, noteCheck.message);
  }

  const cleanNote = noteCheck.cleanNote;

  const newStatus =
    numericProgress === 100
      ? "done"
      : task.status === "open" || task.status === "pending"
        ? "in_progress"
        : task.status;

  const { error: updateError } = await supabase
    .from("tasks")
    .update({
      progress: numericProgress,
      status: newStatus,
      last_updated_by_user_id: user.id,
      updated_at: new Date().toISOString(),
    })
    .eq("id", task.id);

  if (updateError) {
    console.error("Progress task update error:", updateError);
    return sendTwiml(res, "Failed to update task progress.");
  }

  await insertTaskHistory(
    task.id,
    user.id,
    "progress_change",
    "progress",
    { progress: task.progress, status: task.status, note: null },
    { progress: numericProgress, status: newStatus, note: cleanNote },
    user.org_id,
  );

  return sendTwiml(
    res,
    `📈 Task ${taskRef(task)} progress updated to ${numericProgress}%\nTitle: ${task.title}\nNote: ${cleanNote}`,
  );
}

async function handleShowOverdue(res, user) {
  if (!isManagerOrAdmin(user)) {
    return sendTwiml(res, "You are not allowed to view overdue tasks.");
  }

  const { data, error } = await supabase
    .from("overdue_tasks_view")
    .select("*")
    .eq("org_id", user.org_id)
    .order("days_overdue", { ascending: false });

  if (error) {
    console.error("Overdue tasks query error:", error);
    return sendTwiml(res, "Failed to fetch overdue tasks.");
  }

  if (!data || data.length === 0) {
    return sendTwiml(res, "There are no overdue tasks.");
  }

  const lines = data
    .slice(0, 8)
    .map(
      (task) =>
        `#${task.id} | ${task.assigned_to ?? "Unknown"} | ${task.priority} | ${task.title} | due ${task.deadline} | ${task.days_overdue} day(s) overdue`,
    );

  const suffix = data.length > 8 ? `\n...and ${data.length - 8} more.` : "";
  return sendTwiml(res, `Overdue tasks:\n${lines.join("\n")}${suffix}`);
}

async function handleWhoAmI(res, user) {
  const openTaskCount = await getTaskAssignedCount(user.id, user.org_id);

  return sendTwiml(
    res,
    `You are ${user.name} | role: ${user.role} | phone: ${user.phone_number} | open tasks: ${openTaskCount}`,
  );
}

async function handleStatus(res, user) {
  try {
    const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
    const { startUtc, endUtc } = getCurrentAttendanceDayRange();

    const [latestEvent, eventsResult, lateRows] = await Promise.all([
      getLatestAttendanceEvent(user.id, user.org_id),
      supabase
        .from("attendance_events")
        .select(
          "id, org_id, user_id, action, created_at, expected_duration_min, reason, note",
        )
        .eq("user_id", user.id)
        .eq("org_id", user.org_id)
        .gte("created_at", startUtc)
        .lt("created_at", endUtc)
        .order("created_at", { ascending: true }),
      getLateArrivalRowsForDate(attendanceDate, user.org_id),
    ]);

    if (eventsResult.error) {
      console.error("Status events query error:", eventsResult.error);
      return sendTwiml(res, "Failed to fetch your status.");
    }

    const userEvents = eventsResult.data || [];
    const summary = getAttendanceSummaryFromEvents(userEvents);

    const myLate = (lateRows || []).find((x) => x.user_id === user.id) || null;
    const firstLogin = summary.firstLogin;

    const lines = [
      `👤 ${user.name}`,
      `Status: ${summary.currentStatus === "no_update" ? "No update" : summary.currentStatus}`,
    ];

    if (latestEvent?.created_at) {
      lines.push(`Since: ${formatTimeOnly(latestEvent.created_at)}`);
    }

    if (latestEvent?.action === "break" && latestEvent?.expected_duration_min) {
      lines.push(`Expected break: ${latestEvent.expected_duration_min} min`);
    }

    if (latestEvent?.action === "break" && latestEvent?.reason) {
      lines.push(`Reason: ${latestEvent.reason}`);
    }

    if (latestEvent?.action === "logout" && latestEvent?.reason) {
      lines.push(`Logout reason: ${latestEvent.reason}`);
    }

    if (myLate && !firstLogin) {
      const isTimeUnsure =
        !myLate.expected_login_at ||
        String(myLate.note || "").includes("TIME_UNSURE");

      if (isTimeUnsure) {
        lines.push("Expected login: Time unsure");
      } else {
        lines.push(
          `Expected login: ${formatTimeOnly(myLate.expected_login_at)}`,
        );
      }

      lines.push(
        `Late status: ${myLate.is_approved ? "Approved" : "Not approved"}`,
      );
    }

    if (summary.longShiftFlag) {
      lines.push(
        `⚠ Long shift flag: ${formatDurationMinutes(summary.workedMinutes)}`,
      );
    }

    if (summary.longBreakFlag) {
      lines.push(
        `⚠ Long break flag: longest break ${formatDurationMinutes(summary.longestBreakMin)}`,
      );
    }

    lines.push("");
    lines.push("Today:");
    lines.push(`Worked: ${formatDurationMinutes(summary.workedMinutes)}`);
    lines.push(`Break: ${formatDurationMinutes(summary.breakMinutes)}`);

    return sendTwiml(res, lines.join("\n"));
  } catch (error) {
    console.error("Status fatal error:", error);
    return sendTwiml(res, "Failed to fetch your status.");
  }
}

async function handleLateCommand(res, user, lateCommand) {
  const expectedLoginAtIso = parseLocalDateTimeForToday(lateCommand.time_text);

  if (!expectedLoginAtIso) {
    return sendTwiml(
      res,
      `Could not understand the time "${lateCommand.time_text}". Use format like 11:00 AM.`,
    );
  }

  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const locked = await isAttendanceDayLocked(
    user.id,
    attendanceDate,
    user.org_id,
  );

  if (locked) {
    return sendTwiml(
      res,
      `❌ Your attendance is locked for ${attendanceDate}\nPlease contact admin`,
    );
  }

  const { error, approved } = await upsertLateArrival(
    user.id,
    expectedLoginAtIso,
    lateCommand.note,
    user.id,
    user.org_id,
  );

  if (error) {
    console.error("Late arrival upsert error:", error);
    return sendTwiml(res, "Failed to save your late update.");
  }

  return sendTwiml(
    res,
    `🕒 Late marked (${approved ? "Approved" : "Not approved"})\nExpected login: ${formatTimeOnly(expectedLoginAtIso)}`,
  );
}

async function handleLateUnsureCommand(res, actingUser, lateUnsureCommand) {
  let targetUser = actingUser;

  if (lateUnsureCommand.target_name) {
    if (!isManagerOrAdmin(actingUser)) {
      return sendTwiml(res, "Only managers can mark late for others.");
    }

    targetUser = await findUniqueUserByName(
      lateUnsureCommand.target_name,
      actingUser.org_id,
    );
    if (!targetUser) {
      return sendTwiml(
        res,
        `I could not uniquely find an active user named "${lateUnsureCommand.target_name}".`,
      );
    }
  }

  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const locked = await isAttendanceDayLocked(
    targetUser.id,
    attendanceDate,
    actingUser.org_id,
  );

  if (locked) {
    return sendTwiml(
      res,
      `❌ Attendance is locked for ${targetUser.name} on ${attendanceDate}`,
    );
  }

  const shiftStartIso = getShiftStartIsoForToday();
  const informedAtIso = new Date().toISOString();
  const approved = isLateApproved(informedAtIso, shiftStartIso);

  const note =
    lateUnsureCommand.note ||
    (lateUnsureCommand.target_name
      ? `Marked by ${actingUser.name}`
      : "Time unsure");

  const { error } = await supabase.from("late_arrivals").upsert(
    [
      {
        org_id: actingUser.org_id,
        user_id: targetUser.id,
        late_date: attendanceDate,
        expected_login_at: shiftStartIso,
        informed_at: informedAtIso,
        shift_start_at: shiftStartIso,
        is_approved: approved,
        created_by_user_id: actingUser.id,
        note: `TIME_UNSURE | ${note}`,
      },
    ],
    { onConflict: "user_id,late_date" },
  );

  if (error) {
    console.error("Late unsure upsert error:", {
      message: error.message,
      details: error.details,
      hint: error.hint,
      code: error.code,
    });
    return sendTwiml(res, "Failed to mark late unsure.");
  }

  if (lateUnsureCommand.target_name) {
    return sendTwiml(
      res,
      `🕒 Late marked (${approved ? "Approved" : "Not approved"})\n${targetUser.name}: time unsure`,
    );
  }

  return sendTwiml(
    res,
    `🕒 Late marked (${approved ? "Approved" : "Not approved"})\nYour join time is marked as unsure`,
  );
}

async function handleMarkedAttendance(res, actingUser, markCommand) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to mark attendance for others.");
  }

  const targetUser = await findUniqueUserByName(
    markCommand.target_name,
    actingUser.org_id,
  );

  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${markCommand.target_name}".`,
    );
  }

  const occurredAtIso = markCommand.time_text
    ? parseLocalDateTimeForToday(markCommand.time_text)
    : new Date().toISOString();

  if (markCommand.time_text && !occurredAtIso) {
    return sendTwiml(
      res,
      `Could not understand the time "${markCommand.time_text}". Use format like 2:30 PM.`,
    );
  }

  if (new Date(occurredAtIso) > new Date()) {
    return sendTwiml(
      res,
      "❌ Future attendance corrections are not allowed\nPlease mark it after that time happens",
    );
  }

  const attendanceDate = getAttendanceDayDateStringFromDate(
    new Date(occurredAtIso),
  );

  const locked = await isAttendanceDayLocked(
    targetUser.id,
    attendanceDate,
    actingUser.org_id,
  );
  if (locked) {
    return sendTwiml(
      res,
      `❌ Attendance is locked for ${targetUser.name} on ${attendanceDate}`,
    );
  }

  const lastAction = await getLastActionAtOrBefore(
    targetUser.id,
    actingUser.org_id,
    occurredAtIso,
  );

  const oldValue = {
    last_action: lastAction,
    attendance_date: attendanceDate,
  };

  const validationError = validateAttendanceTransition(
    lastAction,
    markCommand.action,
    targetUser.name,
  );

  if (validationError) {
    return sendTwiml(res, validationError);
  }

  let note = `Marked by ${actingUser.name}`;

  if (markCommand.time_text) {
    note += ` | Effective time: ${markCommand.time_text}`;
  }

  let actualBreakMinutes = null;

  if (markCommand.action === "back") {
    const lastBreak = await getLatestBreakEventAtOrBefore(
      targetUser.id,
      actingUser.org_id,
      occurredAtIso,
    );

    if (lastBreak) {
      actualBreakMinutes = minutesBetween(lastBreak.created_at, occurredAtIso);
      note += ` | Actual break: ${actualBreakMinutes} min`;
    }
  }

  const attendanceRow = {
    org_id: actingUser.org_id,
    user_id: targetUser.id,
    target_phone: targetUser.phone_number,
    acted_by_phone: actingUser.phone_number,
    action: markCommand.action,
    duration_min:
      markCommand.action === "back"
        ? actualBreakMinutes
        : (markCommand.duration_min ?? null),
    expected_duration_min: markCommand.duration_min ?? null,
    reason: markCommand.reason ?? null,
    note,
    created_at: occurredAtIso,
  };

  const { error } = await supabase
    .from("attendance_events")
    .insert([attendanceRow]);

  if (error) {
    console.error("Marked attendance insert error:", error);
    return sendTwiml(res, "Failed to save marked attendance.");
  }

  await insertAttendanceAudit(
    targetUser.id,
    actingUser.id,
    `mark_attendance_${markCommand.action}`,
    oldValue,
    {
      action: markCommand.action,
      attendance_date: attendanceDate,
      duration_min: attendanceRow.duration_min,
      expected_duration_min: attendanceRow.expected_duration_min,
      reason: attendanceRow.reason,
      note,
      created_at: occurredAtIso,
    },
    `Marked by ${actingUser.name}`,
    actingUser.org_id,
  );

  if (markCommand.action === "break") {
    return sendTwiml(
      res,
      `${targetUser.name}: break started${
        markCommand.duration_min
          ? ` for ${markCommand.duration_min} minutes`
          : ""
      } by ${actingUser.name}${
        markCommand.time_text ? ` at ${markCommand.time_text}` : ""
      }.`,
    );
  }

  if (markCommand.action === "back") {
    return sendTwiml(
      res,
      `${targetUser.name}: back marked by ${actingUser.name}${
        markCommand.time_text ? ` at ${markCommand.time_text}` : ""
      }. Break duration was ${formatDurationMinutes(actualBreakMinutes || 0)}.`,
    );
  }

  return sendTwiml(
    res,
    `${targetUser.name}: ${markCommand.action} marked by ${actingUser.name}${
      markCommand.time_text ? ` at ${markCommand.time_text}` : ""
    }.`,
  );
}

async function handleSelfOffDay(res, user, offCommand) {
  const offDate = parseFlexibleDateText(offCommand.off_date_text);

  if (!offDate) {
    return sendTwiml(
      res,
      `I could not understand the off date "${offCommand.off_date_text}". Use today, tomorrow, 11 april, or april 11.`,
    );
  }
  const locked = await isAttendanceDayLocked(user.id, offDate, user.org_id);
  if (locked) {
    return sendTwiml(
      res,
      `❌ Leave could not be changed because ${offDate} is locked`,
    );
  }
  const error = await createPlannedOffDay(
    user.id,
    offDate,
    user.id,
    user.org_id,
  );
  if (error) {
    console.error("Create self off day error:", error);
    return sendTwiml(res, "Failed to save your day off.");
  }

  return sendTwiml(res, `🌴 Leave saved for ${offDate}\nName: ${user.name}`);
}

async function handleOffDayForOther(res, actingUser, offCommand) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to mark day off for others.");
  }

  const targetUser = await findUniqueUserByName(
    offCommand.target_name,
    actingUser.org_id,
  );

  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${offCommand.target_name}".`,
    );
  }

  const offDate = parseFlexibleDateText(offCommand.off_date_text);
  if (!offDate) {
    return sendTwiml(
      res,
      `I could not understand the off date "${offCommand.off_date_text}". Use today, tomorrow, 11 april, or april 11.`,
    );
  }

  const locked = await isAttendanceDayLocked(
    targetUser.id,
    offDate,
    actingUser.org_id,
  );

  if (locked) {
    return sendTwiml(
      res,
      `❌ Leave could not be changed because ${offDate} is locked for ${targetUser.name}`,
    );
  }

  const error = await createPlannedOffDay(
    targetUser.id,
    offDate,
    actingUser.id,
    actingUser.org_id,
  );

  if (error) {
    console.error("Create off day for other error:", error);
    return sendTwiml(res, "Failed to save day off.");
  }

  await insertAttendanceAudit(
    targetUser.id,
    actingUser.id,
    "mark_leave_for_other",
    null,
    {
      off_date: offDate,
    },
    `Leave marked by ${actingUser.name}`,
    actingUser.org_id,
  );

  return sendTwiml(
    res,
    `🌴 Leave saved for ${offDate}\nName: ${targetUser.name}\nMarked by: ${actingUser.name}`,
  );
}

async function handleSelfAttendance(res, user, attendanceCommand) {
  const lastAction = await getLastAction(user.id, user.org_id);
  const validationError = validateAttendanceTransition(
    lastAction,
    attendanceCommand.action,
    "You",
  );

  if (validationError) {
    return sendTwiml(res, validationError);
  }

  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const locked = await isAttendanceDayLocked(
    user.id,
    attendanceDate,
    user.org_id,
  );

  if (locked) {
    return sendTwiml(
      res,
      `❌ Your attendance is locked for ${attendanceDate}\nPlease contact admin`,
    );
  }

  const attendanceRow = {
    org_id: user.org_id,
    user_id: user.id,
    target_phone: user.phone_number,
    acted_by_phone: user.phone_number,
    action: attendanceCommand.action,
    duration_min: attendanceCommand.expected_duration_min ?? null,
    expected_duration_min: attendanceCommand.expected_duration_min ?? null,
    reason: attendanceCommand.reason ?? null,
    note: null,
  };

  const { error: attendanceError } = await supabase
    .from("attendance_events")
    .insert([attendanceRow]);

  if (attendanceError) {
    console.error("Attendance insert error:", attendanceError);
    return sendTwiml(
      res,
      "❌ Could not update attendance status\nPlease try again",
    );
  }

  if (attendanceCommand.action === "break") {
    const lines = ["☕ Break started"];

    if (attendanceCommand.expected_duration_min) {
      lines.push(`Expected: ${attendanceCommand.expected_duration_min} min`);
    }

    if (attendanceCommand.reason) {
      lines.push(`Reason: ${attendanceCommand.reason}`);
    }

    return sendTwiml(res, lines.join("\n"));
  }

  if (attendanceCommand.action === "back") {
    const lastBreak = await getLatestBreakEvent(user.id, user.org_id);
    const actualMinutes = lastBreak ? minutesBetween(lastBreak.created_at) : 0;

    return sendTwiml(
      res,
      `✅ Back to work\nBreak duration: ${formatDurationMinutes(actualMinutes)}`,
    );
  }

  if (attendanceCommand.action === "login") {
    try {
      const today = getAttendanceDayDateStringFromDate(new Date());
      const plannedOffRows = await getPlannedOffRowsForDate(today, user.org_id);
      const otherNames = (plannedOffRows || [])
        .filter((x) => x.user_id !== user.id)
        .map((x) => x.users?.name || "Unknown");

      const shiftStartIso = getShiftStartIsoForToday();
      const loginIso = new Date().toISOString();
      const delayMin = Math.max(
        0,
        Math.round((new Date(loginIso) - new Date(shiftStartIso)) / 60000),
      );

      const lateRows = await getLateArrivalRowsForDate(today, user.org_id);
      const myLate = lateRows.find((x) => x.user_id === user.id) || null;

      let lateLine = "";
      if (delayMin > 0) {
        if (myLate) {
          lateLine = `\n🕒 Joined late: ${delayMin} min (${myLate.is_approved ? "approved prior notice" : "not approved"})`;
        } else {
          lateLine = `\n🕒 Joined late: ${delayMin} min (no prior intimation)`;
        }
      }

      const leaveLine = otherNames.length
        ? `\n🌴 On leave today: ${otherNames.join(", ")}`
        : `\n🌴 On leave today: None`;

      return sendTwiml(
        res,
        `✅ Logged in successfully\nWelcome, ${user.name}${lateLine}${leaveLine}`,
      );
    } catch (error) {
      console.error("Login leave lookup error:", error);
      return sendTwiml(res, `✅ Logged in successfully\nWelcome, ${user.name}`);
    }
  }

  if (attendanceCommand.action === "logout") {
    const lines = ["✅ Logged out successfully\nSee you next time"];
    if (attendanceCommand.reason) {
      lines.push(`Reason: ${attendanceCommand.reason}`);
    }
    return sendTwiml(res, lines.join("\n"));
  }

  return sendTwiml(res, `✅ ${attendanceCommand.action} marked successfully`);
}

async function createPlannedOffDay(
  userId,
  offDate,
  createdByUserId,
  orgId,
  note = null,
) {
  const { error } = await supabase.from("planned_time_off").upsert(
    [
      {
        org_id: orgId,
        user_id: userId,
        off_date: offDate,
        note,
        created_by_user_id: createdByUserId,
      },
    ],
    { onConflict: "user_id,off_date" },
  );

  return error;
}

async function getPlannedOffRowsForDate(dateString, orgId) {
  const { data, error } = await supabase
    .from("planned_time_off")
    .select(
      `
      id,
      org_id,
      user_id,
      off_date,
      note,
      users!planned_time_off_user_id_fkey(name)
    `,
    )
    .eq("off_date", dateString)
    .eq("org_id", orgId);

  if (error) {
    throw error;
  }

  return data || [];
}

async function getLateArrivalRowsForDate(dateString, orgId) {
  const { data, error } = await supabase
    .from("late_arrivals")
    .select(
      `
      id,
      org_id,
      user_id,
      late_date,
      expected_login_at,
      informed_at,
      shift_start_at,
      is_approved,
      note,
      users!late_arrivals_user_id_fkey(name)
    `,
    )
    .eq("late_date", dateString)
    .eq("org_id", orgId);

  if (error) {
    throw error;
  }

  return data || [];
}

async function upsertLateArrival(
  userId,
  expectedLoginAtIso,
  note = null,
  createdByUserId = null,
  orgId,
) {
  const todayDb = getAttendanceDayDateStringFromDate(new Date());
  const shiftStartIso = getShiftStartIsoForToday();
  const informedAtIso = new Date().toISOString();
  const approved = isLateApproved(informedAtIso, shiftStartIso);

  const { error } = await supabase.from("late_arrivals").upsert(
    [
      {
        org_id: orgId,
        user_id: userId,
        late_date: todayDb,
        expected_login_at: expectedLoginAtIso,
        informed_at: informedAtIso,
        shift_start_at: shiftStartIso,
        is_approved: approved,
        created_by_user_id: createdByUserId,
        note,
      },
    ],
    { onConflict: "user_id,late_date" },
  );

  return { error, approved };
}

async function handleCreateTaskAdvanced(res, user, taskCommand) {
  if (taskCommand.error) {
    return sendTwiml(res, `❌ ${taskCommand.error}`);
  }

  const { matchedUsers, missingNames } = await findUsersByNames(
    taskCommand.owner_names,
    user.org_id,
  );

  if (missingNames.length) {
    return sendTwiml(
      res,
      `❌ Could not find these users: ${missingNames.join(", ")}`,
    );
  }

  const taskRow = {
    created_by_user_id: user.id,
    last_updated_by_user_id: user.id,
    title: taskCommand.title,
    detail: null,
    priority: taskCommand.priority || "medium",
    status: "open",
    org_id: user.org_id,
    progress: 0,
    deadline: taskCommand.deadline,
    blocker_note: null,
    business: taskCommand.business,
    area: taskCommand.area,
    updated_at: new Date().toISOString(),
  };

  const { data: createdTask, error: taskError } = await supabase
    .from("tasks")
    .insert([taskRow])
    .select("id, task_no, title, priority, deadline, business, area")
    .single();

  if (taskError) {
    console.error("Advanced task insert error:", taskError);
    return sendTwiml(
      res,
      `❌ Could not create task\nReason: ${taskError.message || "system could not save it"}`,
    );
  }

  const ownerRows = matchedUsers.map((owner) => ({
    org_id: user.org_id,
    task_id: createdTask.id,
    user_id: owner.id,
  }));

  const { error: ownerInsertError } = await supabase
    .from("task_owners")
    .insert(ownerRows);

  if (ownerInsertError) {
    console.error("Task owners insert error:", ownerInsertError);

    await supabase.from("tasks").delete().eq("id", createdTask.id);

    return sendTwiml(
      res,
      "❌ Task could not be completed because owners failed to save. Nothing was created.",
    );
  }

  await insertTaskHistory(
    createdTask.id,
    user.id,
    "task_created",
    "task",
    null,
    {
      title: createdTask.title,
      priority: createdTask.priority,
      deadline: createdTask.deadline,
      business: createdTask.business,
      area: createdTask.area,
      owners: matchedUsers.map((x) => x.name),
    },
    user.org_id,
  );

  return sendTwiml(
    res,
    [
      `✅ Task #${createdTask.task_no || createdTask.id} created`,
      `Owners: ${matchedUsers.map((x) => x.name).join(", ")}`,
      `Priority: ${createdTask.priority}`,
      `Title: ${createdTask.title}`,
      `Due: ${createdTask.deadline || "no due date"}`,
      createdTask.business ? `Business: ${createdTask.business}` : null,
      createdTask.area ? `Area: ${createdTask.area}` : null,
    ]
      .filter(Boolean)
      .join("\n"),
  );
}

// async function handleCreateTask(res, user, taskCommand) {
//   if (!taskCommand.assignee_name) {
//     return sendTwiml(
//       res,
//       "I understood this as a task, but could not identify the assignee.",
//     );
//   }

//   if (!taskCommand.title) {
//     return sendTwiml(
//       res,
//       "I understood this as a task, but could not identify the title.",
//     );
//   }

// const assignee = await findUniqueUserByName(taskCommand.assignee_name, user.org_id);
//   if (!assignee) {
//     return sendTwiml(
//       res,
//       `I could not uniquely find an active user named "${taskCommand.assignee_name}".`,
//     );
//   }

//   if (!isManagerOrAdmin(user) && assignee.id !== user.id) {
//     return sendTwiml(
//       res,
//       "You are not allowed to assign tasks to other people.",
//     );
//   }

//   const deadline = parseDeadline(taskCommand.deadline_text);

//   if (!deadline) {
//     return sendTwiml(
//       res,
//       `I could not understand the deadline "${taskCommand.deadline_text}". Use today, tomorrow, friday, 11 april, or april 11.`,
//     );
//   }

//   const taskRow = {
//     assigned_to_user_id: assignee.id,
//     org_id: user.org_id,
//     created_by_user_id: user.id,
//     last_updated_by_user_id: user.id,
//     title: taskCommand.title,
//     detail: null,
//     priority: taskCommand.priority || "medium",
//     status: "open",
//     progress: 0,
//     deadline,
//     blocker_note: null,
//     updated_at: new Date().toISOString(),
//   };

//   const { data: createdTask, error: taskError } = await supabase
//     .from("tasks")
//     .insert([taskRow])
//     .select("id, task_no, title, priority, deadline")
//     .single();

//   if (taskError) {
//     console.error("Task insert error:", taskError);
//     return sendTwiml(
//       res,
//       "❌ Could not create task\nReason: system could not save it\nTry: please send the task again once",
//     );
//   }

// const { error: ownerUpsertError } = await supabase
//   .from("task_owners")
//   .upsert([
//     {
//       org_id: user.org_id,
//       task_id: createdTask.id,
//       user_id: assignee.id,
//     },
//   ]);

// if (ownerUpsertError) {
//   console.error("Simple task owner upsert error:", ownerUpsertError);

//   await supabase
//     .from("tasks")
//     .delete()
//     .eq("id", createdTask.id);

//   return sendTwiml(
//     res,
//     "❌ Task could not be completed because owner save failed. Nothing was created.",
//   );
//   }

//   await insertTaskHistory(
//     createdTask.id,
//     user.id,
//     "task_created",
//     "task",
//     null,
//     {
//       title: createdTask.title,
//       priority: createdTask.priority,
//       deadline: createdTask.deadline,
//       assigned_to_user_id: assignee.id,
//     },
//     user.org_id
//   );

//   return sendTwiml(
//     res,
//     `✅ Task #${createdTask.task_no || createdTask.id} created\nAssigned to ${assignee.name}\nPriority: ${createdTask.priority}\nTitle: ${createdTask.title}\nDue: ${createdTask.deadline || "no deadline"}`,
//   );
// }

async function handleBlockTask(res, user, taskId, reason) {
  const cleanNote = String(reason || "").trim();

  if (!cleanNote) {
    return sendTwiml(
      res,
      "Please add a reason.\nExample: block 12 waiting on backend fix",
    );
  }

  const { task, error } = await getTaskById(taskId, user.org_id);

  if (error) {
    return sendTwiml(
      res,
      "❌ Could not open that task\nReason: system could not fetch task details",
    );
  }

  if (!task) {
    return sendTwiml(
      res,
      `❌ Task #${taskId} was not found\nTry: check the task number and send again`,
    );
  }

  if (!(await canModifyTask(user, task))) {
    return sendTwiml(res, "You are not allowed to block that task.");
  }

  if (task.status === "done" || task.status === "archived") {
    return sendTwiml(
      res,
      `Task ${taskRef(task)} cannot be blocked because it is ${task.status}.`,
    );
  }

  if (task.status === "blocked") {
    return sendTwiml(res, `Task ${taskRef(task)} is already blocked.`);
  }

  const { error: updateError } = await supabase
    .from("tasks")
    .update({
      status: "blocked",
      blocker_note: cleanNote,
      last_updated_by_user_id: user.id,
      updated_at: new Date().toISOString(),
    })
    .eq("id", task.id);

  if (updateError) {
    console.error("Block task update error:", updateError);
    return sendTwiml(res, "Failed to block the task.");
  }

  await insertTaskHistory(
    task.id,
    user.id,
    "status_change",
    "status",
    { status: task.status, blocker_note: task.blocker_note, note: null },
    { status: "blocked", blocker_note: cleanNote, note: cleanNote },
    user.org_id,
  );

  return sendTwiml(
    res,
    `⛔ Task ${taskRef(task)} blocked
Title: ${task.title}
Reason: ${cleanNote}`,
  );
}

async function handleUnblockTask(res, user, taskId, note) {
  const cleanNote = String(note || "").trim();

  if (!cleanNote) {
    return sendTwiml(
      res,
      "Please add a note.\nExample: unblock 12 backend fix merged",
    );
  }

  const { task, error } = await getTaskById(taskId, user.org_id);

  if (error) {
    return sendTwiml(res, "Failed to fetch that task.");
  }

  if (!task) {
    return sendTwiml(res, `Task #${taskId} not found.`);
  }

  if (!(await canModifyTask(user, task))) {
    return sendTwiml(res, "You are not allowed to unblock that task.");
  }

  if (task.status !== "blocked") {
    return sendTwiml(res, `Task ${taskRef(task)} is not blocked.`);
  }

  const nextStatus = task.progress > 0 ? "in_progress" : "open";

  const { error: updateError } = await supabase
    .from("tasks")
    .update({
      status: nextStatus,
      blocker_note: null,
      last_updated_by_user_id: user.id,
      updated_at: new Date().toISOString(),
    })
    .eq("id", task.id);

  if (updateError) {
    console.error("Unblock task update error:", updateError);
    return sendTwiml(res, "Failed to unblock the task.");
  }

  await insertTaskHistory(
    task.id,
    user.id,
    "status_change",
    "status",
    { status: task.status, blocker_note: task.blocker_note, note: null },
    { status: nextStatus, blocker_note: null, note: cleanNote },
    user.org_id,
  );

  return sendTwiml(
    res,
    `✅ Task ${taskRef(task)} unblocked
Title: ${task.title}
Note: ${cleanNote}`,
  );
}

async function handleTasksByName(res, actingUser, assigneeName) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to view other people's tasks.");
  }

  const targetUser = await findUniqueUserByName(
    assigneeName,
    actingUser.org_id,
  );

  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${assigneeName}".`,
    );
  }

  const { data, error } = await supabase
    .from("task_owners")
    .select(
      `
      task_id,
      tasks!inner(id, task_no, title, priority, status, progress, deadline)
    `,
    )
    .eq("user_id", targetUser.id)
    .eq("org_id", actingUser.org_id);

  if (error) {
    console.error("Tasks by name query error:", error);
    return sendTwiml(res, "Failed to fetch tasks.");
  }

  const tasks = (data || [])
    .map((x) => x.tasks)
    .filter((t) => t && !["done", "archived", "cancelled"].includes(t.status));

  if (!tasks.length) {
    return sendTwiml(res, `${targetUser.name} has no open tasks.`);
  }

  const lines = tasks
    .slice(0, 8)
    .map(
      (task) =>
        `#${task.task_no || task.id} | ${task.priority} | ${task.status} | ${task.title} | due ${task.deadline ?? "no deadline"} | ${task.progress}%`,
    );

  const suffix = tasks.length > 8 ? `\n...and ${tasks.length - 8} more.` : "";

  return sendTwiml(
    res,
    `${targetUser.name}'s open tasks:\n${lines.join("\n")}${suffix}`,
  );
}

async function handleWhoIsOnBreak(res, actingUser) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to view team break status.");
  }

  const { data: users, error: usersError } = await supabase
    .from("users")
    .select("id, name")
    .eq("org_id", actingUser.org_id)
    .eq("is_active", true)
    .order("name", { ascending: true });

  if (usersError) {
    console.error("Who is on break users query error:", usersError);
    return sendTwiml(res, "Failed to fetch break status.");
  }

  const { data: events, error: eventsError } = await supabase
    .from("attendance_events")
    .select("user_id, action, created_at")
    .eq("org_id", actingUser.org_id)
    .order("created_at", { ascending: false });

  if (eventsError) {
    console.error("Who is on break events query error:", eventsError);
    return sendTwiml(res, "Failed to fetch break status.");
  }

  const latestByUser = new Map();

  for (const event of events || []) {
    if (!latestByUser.has(event.user_id)) {
      latestByUser.set(event.user_id, event);
    }
  }

  const onBreak = (users || [])
    .filter((u) => latestByUser.get(u.id)?.action === "break")
    .map((u) => {
      const ev = latestByUser.get(u.id);
      return `${u.name} | on break for ${formatDurationMinutes(minutesBetween(ev.created_at))}`;
    });

  if (onBreak.length === 0) {
    return sendTwiml(res, "Nobody is currently on break.");
  }

  return sendTwiml(res, `Currently on break:\n${onBreak.join("\n")}`);
}

async function handleWhoIsOffToday(res, actingUser) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "❌ You are not allowed to view leave status.");
  }

  try {
    const today = getAttendanceDayDateStringFromDate(new Date());
    const plannedOffRows = await getPlannedOffRowsForDate(
      today,
      actingUser.org_id,
    );
    const plannedOff = plannedOffRows || [];

    if (plannedOff.length === 0) {
      return sendTwiml(res, "🌴 Nobody is on leave today");
    }

    const names = plannedOff.map((x) => x.users?.name || "Unknown");

    return sendTwiml(res, `🌴 On leave today:\n${names.join("\n")}`);
  } catch (error) {
    console.error("Who is off today error:", error);
    return sendTwiml(res, "❌ Failed to fetch today's leave list");
  }
}
async function handleNowSummary(res, actingUser) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "❌ You are not allowed to view team summary.");
  }

  try {
    const attendanceDate = getAttendanceDayDateStringFromDate(new Date());

    const [usersResult, events, plannedOffRows, lateRows] = await Promise.all([
      supabase
        .from("users")
        .select("id, name")
        .eq("org_id", actingUser.org_id)
        .eq("is_active", true)
        .order("name", { ascending: true }),
      getTodayAttendanceEventsForAllUsers(actingUser.org_id),
      getPlannedOffRowsForDate(attendanceDate, actingUser.org_id),
      getLateArrivalRowsForDate(attendanceDate, actingUser.org_id),
    ]);

    if (usersResult.error) {
      console.error("Now summary users error:", usersResult.error);
      return sendTwiml(res, "❌ Failed to fetch now summary.");
    }

    const users = usersResult.data || [];
    const plannedOff = plannedOffRows || [];

    const eventsByUser = new Map();
    for (const ev of events || []) {
      if (!eventsByUser.has(ev.user_id)) {
        eventsByUser.set(ev.user_id, []);
      }
      eventsByUser.get(ev.user_id).push(ev);
    }

    const lateByUser = new Map();
    for (const row of lateRows || []) {
      lateByUser.set(row.user_id, row);
    }

    const plannedOffUserIds = new Set(plannedOff.map((x) => x.user_id));

    const workingNow = [];
    const onBreakNow = [];
    const expectedLater = [];
    const onLeaveToday = plannedOff.map((x) => x.users?.name || "Unknown");
    const loggedOutToday = [];
    const noUpdateYet = [];
    const quickCheckIns = [];
    const workingLongerThanUsual = [];

    for (const user of users) {
      if (plannedOffUserIds.has(user.id)) continue;

      const userEvents = eventsByUser.get(user.id) || [];
      const latest = userEvents[userEvents.length - 1] || null;
      const summary = getAttendanceSummaryFromEvents(userEvents);

      if (summary.longShiftFlag) {
        workingLongerThanUsual.push(
          `${user.name} (${formatDurationMinutes(summary.workedMinutes)})`,
        );
      }

      if (!latest) {
        const lateInfo = lateByUser.get(user.id);

        if (lateInfo) {
          const isTimeUnsure =
            !lateInfo.expected_login_at ||
            String(lateInfo.note || "").includes("TIME_UNSURE");

          if (isTimeUnsure) {
            expectedLater.push(`${user.name} (late, time unsure)`);
          } else {
            expectedLater.push(
              `${user.name} (till ${formatTimeOnly(lateInfo.expected_login_at)})`,
            );

            if (new Date() > new Date(lateInfo.expected_login_at)) {
              quickCheckIns.push(
                `${user.name} has not logged in yet after the informed time (${formatTimeOnly(lateInfo.expected_login_at)})`,
              );
            }
          }
        } else {
          noUpdateYet.push(user.name);
        }

        continue;
      }

      if (latest.action === "break") {
        const breakTime = formatTimeOnly(latest.created_at);
        const expectedMin = latest.expected_duration_min || null;
        const totalBreakMinSoFar = getTotalBreakMinutesSoFar(userEvents);
        const breakAgeMin = minutesBetween(latest.created_at);

        let label = `${user.name} (since ${breakTime} | ${formatDurationMinutes(breakAgeMin)}`;

        if (expectedMin) {
          label += ` | expected ${expectedMin} min`;
        }

        label += ` | total today ${formatDurationMinutes(totalBreakMinSoFar)})`;

        onBreakNow.push(label);

        if (expectedMin && breakAgeMin > expectedMin + 15) {
          quickCheckIns.push(
            `${user.name} has been on break longer than expected (${breakAgeMin} min vs expected ${expectedMin} min)`,
          );
        }

        continue;
      }

      if (latest.action === "logout") {
        const time = formatTimeOnly(latest.created_at);

        let label = `${user.name} (${time})`;

        if (latest.reason) {
          label += ` - ${latest.reason}`;
        }

        loggedOutToday.push(label);
        continue;
      }

      if (latest.action === "login" || latest.action === "back") {
        workingNow.push(
          `${user.name} (${formatDurationMinutes(summary.workedMinutes)})`,
        );
        continue;
      }

      noUpdateYet.push(user.name);
    }

    for (const userName of noUpdateYet) {
      quickCheckIns.push(`${userName} has not updated attendance yet`);
    }

    const lines = [
      "📋 Live team snapshot",
      "",
      `Total team: ${users.length} | Working: ${workingNow.length} | Break: ${onBreakNow.length} | Leave: ${onLeaveToday.length} | Logged out: ${loggedOutToday.length} | Expected later: ${expectedLater.length} | No update yet: ${noUpdateYet.length}`,
      "",
      `✅ Working now\n${workingNow.length ? workingNow.join("\n") : "None"}`,
      "",
      `☕ On break\n${onBreakNow.length ? onBreakNow.join("\n") : "None"}`,
      "",
      `🕒 Expected later\n${expectedLater.length ? expectedLater.join("\n") : "None"}`,
      "",
      `🌴 On leave today\n${onLeaveToday.length ? onLeaveToday.join("\n") : "None"}`,
      "",
      `🏁 Logged out today\n${loggedOutToday.length ? loggedOutToday.join("\n") : "None"}`,
      "",
      `❓ No update yet\n${noUpdateYet.length ? noUpdateYet.join("\n") : "None"}`,
    ];

    if (quickCheckIns.length) {
      lines.push("");
      lines.push(
        `💬 Quick check-ins\n${quickCheckIns.map((x) => `• ${x}`).join("\n")}`,
      );
    }

    if (workingLongerThanUsual.length) {
      lines.push("");
      lines.push(
        `⏱ Working longer than usual\n${workingLongerThanUsual.join("\n")}`,
      );
    }

    return sendTwiml(res, lines.join("\n"));
  } catch (error) {
    console.error("Now summary error:", error);
    return sendTwiml(res, "❌ Failed to fetch now summary.");
  }
}

async function handleSummaryToday(res, actingUser) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to view team summary.");
  }

  try {
    const today = getAttendanceDayDateStringFromDate(new Date());
    const shiftStartIso = getShiftStartIsoForToday();

    const [usersResult, events, plannedOffRows, lateRows] = await Promise.all([
      supabase
        .from("users")
        .select("id, name, role")
        .eq("org_id", actingUser.org_id)
        .eq("is_active", true)
        .order("name", { ascending: true }),
      getTodayAttendanceEventsForAllUsers(actingUser.org_id),
      getPlannedOffRowsForDate(today, actingUser.org_id),
      getLateArrivalRowsForDate(today, actingUser.org_id),
    ]);

    if (usersResult.error) {
      console.error("Summary users error:", usersResult.error);
      return sendTwiml(res, "Failed to fetch today's summary.");
    }

    const users = usersResult.data || [];
    const plannedOff = plannedOffRows || [];
    const plannedOffUserIds = new Set(plannedOff.map((x) => x.user_id));

    const eventsByUser = new Map();
    for (const ev of events || []) {
      if (!eventsByUser.has(ev.user_id)) {
        eventsByUser.set(ev.user_id, []);
      }
      eventsByUser.get(ev.user_id).push(ev);
    }

    const lateByUser = new Map();
    for (const row of lateRows || []) {
      lateByUser.set(row.user_id, row);
    }

    const approvedLate = [];
    const unapprovedLate = [];
    const uninformedLate = [];
    const exceededLate = [];
    const onBreakNow = [];
    const loggedOutToday = [];
    const noUpdateToday = [];
    const workedToday = [];

    for (const user of users) {
      if (plannedOffUserIds.has(user.id)) continue;

      const userEvents = eventsByUser.get(user.id) || [];
      const latest = userEvents[userEvents.length - 1] || null;
      const firstLogin = getFirstLoginEvent(userEvents);
      const lateInfo = lateByUser.get(user.id) || null;
      const workedMin = computeWorkedMinutesFromEvents(userEvents);

      if (workedMin > 0) {
        workedToday.push(`${user.name} (${formatDurationMinutes(workedMin)})`);
      }

      if (latest?.action === "break") {
        onBreakNow.push(user.name);
      }

      if (latest?.action === "logout") {
        loggedOutToday.push(user.name);
      }

      if (!firstLogin) {
        if (lateInfo) {
          const isTimeUnsure =
            !lateInfo.expected_login_at ||
            String(lateInfo.note || "").includes("TIME_UNSURE");

          if (isTimeUnsure) {
            if (lateInfo.is_approved) {
              approvedLate.push(`${user.name} (late, time unsure)`);
            } else {
              unapprovedLate.push(`${user.name} (late, time unsure)`);
            }
          } else if (new Date() > new Date(lateInfo.expected_login_at)) {
            exceededLate.push(
              `${user.name} (said ${formatTimeOnly(lateInfo.expected_login_at)})`,
            );
          } else {
            noUpdateToday.push(
              `${user.name} (late till ${formatTimeOnly(lateInfo.expected_login_at)})`,
            );
          }
        } else if (new Date() > new Date(shiftStartIso)) {
          noUpdateToday.push(user.name);
        }
        continue;
      }

      const loginDelayMin = Math.max(
        0,
        Math.round(
          (new Date(firstLogin.created_at) - new Date(shiftStartIso)) / 60000,
        ),
      );
      const LATE_GRACE_MIN = 10;
      if (loginDelayMin > LATE_GRACE_MIN) {
        const wasTimeUnsure =
          lateInfo &&
          (!lateInfo.expected_login_at ||
            String(lateInfo.note || "").includes("TIME_UNSURE"));

        if (lateInfo && lateInfo.is_approved) {
          approvedLate.push(
            wasTimeUnsure
              ? `${user.name} (${formatTimeOnly(firstLogin.created_at)}, ${loginDelayMin}m late, was unsure)`
              : `${user.name} (${formatTimeOnly(firstLogin.created_at)}, ${loginDelayMin}m late)`,
          );
        } else if (lateInfo && !lateInfo.is_approved) {
          unapprovedLate.push(
            wasTimeUnsure
              ? `${user.name} (${formatTimeOnly(firstLogin.created_at)}, ${loginDelayMin}m late, was unsure)`
              : `${user.name} (${formatTimeOnly(firstLogin.created_at)}, ${loginDelayMin}m late)`,
          );
        } else {
          uninformedLate.push(
            `${user.name} (${formatTimeOnly(firstLogin.created_at)}, ${loginDelayMin}m late)`,
          );
        }
      }
    }

    const leaveNames = plannedOff.map((x) => x.users?.name || "Unknown");

    const lines = [
      "📋 Today summary",
      "",
      `🟢 Approved late: ${approvedLate.length ? approvedLate.join(", ") : "None"}`,
      `🟡 Late not approved: ${unapprovedLate.length ? unapprovedLate.join(", ") : "None"}`,
      `🔴 Uninformed late: ${uninformedLate.length ? uninformedLate.join(", ") : "None"}`,
      `⚠️ Exceeded informed late time: ${exceededLate.length ? exceededLate.join(", ") : "None"}`,
      `☕ On break now: ${onBreakNow.length ? onBreakNow.join(", ") : "None"}`,
      `🏁 Logged out: ${loggedOutToday.length ? loggedOutToday.join(", ") : "None"}`,
      `🌴 Leave: ${leaveNames.length ? leaveNames.join(", ") : "None"}`,
      `❓ No update: ${noUpdateToday.length ? noUpdateToday.join(", ") : "None"}`,
      "",
      `⏱ Worked today: ${workedToday.length ? workedToday.join(", ") : "None"}`,
    ];

    return sendTwiml(res, lines.join("\n"));
  } catch (error) {
    console.error("Summary today fatal error:", error);
    return sendTwiml(res, "Failed to fetch today's summary.");
  }
}

async function insertAttendanceAudit(
  targetUserId,
  actedByUserId,
  actionType,
  oldValue,
  newValue,
  note = null,
  orgId,
) {
  const { error } = await supabase.from("attendance_audit").insert([
    {
      org_id: orgId,
      target_user_id: targetUserId,
      acted_by_user_id: actedByUserId,
      action_type: actionType,
      old_value: oldValue,
      new_value: newValue,
      note,
    },
  ]);

  if (error) {
    console.error("Attendance audit insert error:", error);
  }
}

async function getTaskByDbId(taskDbId, orgId) {
  const { data, error } = await supabase
    .from("tasks")
    .select(
      `
      id,
      org_id,
      task_no,
      title,
      detail,
      priority,
      status,
      progress,
      deadline,
      blocker_note,
      business,
      area,
      assigned_to_user_id,
      created_by_user_id,
      last_updated_by_user_id
    `,
    )
    .eq("id", taskDbId)
    .eq("org_id", orgId)
    .maybeSingle();

  if (error) {
    console.error("Get task by db id error:", error);
    return { task: null, error };
  }

  if (!data) {
    return { task: null, error: null };
  }

  const ownerNames = await getTaskOwnerNames(data.id, orgId);

  return {
    task: {
      ...data,
      owner_names: ownerNames,
    },
    error: null,
  };
}

async function handleUndoLastTaskChange(res, user) {
  if (!isManagerOrAdmin(user)) {
    return sendTwiml(res, "Undo is only available to managers/admins.");
  }

  const { data: rows, error } = await supabase
    .from("task_history")
    .select(
      "id, task_id, changed_by_user_id, change_type, old_value, new_value",
    )
    .eq("changed_by_user_id", user.id)
    .eq("org_id", user.org_id)
    .order("id", { ascending: false })
    .limit(10);

  if (error) {
    console.error("Undo task history fetch error:", error);
    return sendTwiml(res, "Failed to fetch your last task change.");
  }

  const history = (rows || []).find(
    (row) =>
      row.change_type === "status_change" ||
      row.change_type === "progress_change",
  );

  if (!history) {
    return sendTwiml(res, "No reversible task change found.");
  }

  const { task, error: taskError } = await getTaskByDbId(
    history.task_id,
    user.org_id,
  );

  if (taskError || !task) {
    return sendTwiml(res, "Failed to fetch the task for undo.");
  }

  if (!(await canModifyTask(user, task)) && !isManagerOrAdmin(user)) {
    return sendTwiml(res, "You are not allowed to undo that task change.");
  }

  const oldValue = history.old_value || {};
  const hasUndoableField =
    oldValue.status !== undefined ||
    oldValue.progress !== undefined ||
    oldValue.blocker_note !== undefined;

  if (!hasUndoableField) {
    return sendTwiml(res, "Your last task change cannot be safely undone.");
  }

  const patch = {
    last_updated_by_user_id: user.id,
    updated_at: new Date().toISOString(),
  };

  if (oldValue.status !== undefined) patch.status = oldValue.status;
  if (oldValue.progress !== undefined) patch.progress = oldValue.progress;
  if (oldValue.blocker_note !== undefined)
    patch.blocker_note = oldValue.blocker_note;

  const { error: updateError } = await supabase
    .from("tasks")
    .update(patch)
    .eq("id", history.task_id);

  if (updateError) {
    console.error("Undo task update error:", updateError);
    return sendTwiml(res, "Failed to undo your last task change.");
  }

  await insertTaskHistory(
    history.task_id,
    user.id,
    "undo",
    "task",
    history.new_value,
    history.old_value,
    user.org_id,
  );

  return sendTwiml(
    res,
    `Reverted your last task change on task ${taskRef(task)}.`,
  );
}

function getPartsInTimeZone(date = new Date(), timeZone = APP_TIMEZONE) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  const parts = formatter.formatToParts(date);
  const out = {};

  for (const part of parts) {
    if (part.type !== "literal") {
      out[part.type] = part.value;
    }
  }

  return {
    year: Number(out.year),
    month: Number(out.month),
    day: Number(out.day),
    hour: Number(out.hour),
    minute: Number(out.minute),
    second: Number(out.second),
  };
}

function getAttendanceDayDateStringFromDate(date = new Date()) {
  const parts = getPartsInTimeZone(date, APP_TIMEZONE);

  let attendanceDate = formatDateForDbFromParts(
    parts.year,
    parts.month,
    parts.day,
  );

  if (parts.hour < ATTENDANCE_DAY_START_HOUR) {
    attendanceDate = addDaysToDateString(attendanceDate, -1);
  }

  return attendanceDate;
}

function taskRef(task) {
  return "#" + (task?.task_no || task?.id || "");
}

function getAttendanceDayUtcRange(attendanceDateString) {
  const nextDate = addDaysToDateString(attendanceDateString, 1);

  const startUtc = new Date(
    `${attendanceDateString}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
  ).toISOString();

  const endUtc = new Date(
    `${nextDate}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
  ).toISOString();

  return {
    startUtc,
    endUtc,
    attendanceDate: attendanceDateString,
  };
}

function getCurrentAttendanceDayRange() {
  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
  return getAttendanceDayUtcRange(attendanceDate);
}

function parseEmployeeSummaryCommand(text) {
  const raw = normalizeText(text);

  if (/^employee\s+summary$/i.test(raw)) {
    return {
      target_name: null,
    };
  }

  const match = raw.match(/^employee\s+summary\s+(.+)$/i);
  if (!match) return null;

  return {
    target_name: match[1].trim(),
  };
}

function parseOwnerNames(ownerText) {
  if (!ownerText) return [];
  return ownerText
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function parseProgressPercentToken(token) {
  const raw = String(token || "").trim();
  const match = raw.match(/^(\d{1,3})%?$/);
  if (!match) return null;

  const value = Number(match[1]);
  if (value < 0 || value > 100) return null;
  return value;
}

function parseLateUnsureCommand(text) {
  const raw = normalizeText(text);

  if (/^late\s+unsure$/i.test(raw)) {
    return {
      target_name: null,
      note: null,
    };
  }

  const match = raw.match(/^late\s+(.+?)\s+unsure(?:\s+(.+))?$/i);
  if (!match) return null;

  return {
    target_name: match[1].trim(),
    note: match[2]?.trim() || null,
  };
}

function parseTimelineCommand(text) {
  const raw = normalizeText(text);
  let match = raw.match(
    /^timeline\s+(.+?)\s+(today|tomorrow|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)$/i,
  );

  if (match) {
    return {
      target_name: match[1].trim(),
      date_text: match[2].trim(),
    };
  }

  match = raw.match(/^timeline\s+(.+)$/i);
  if (!match) return null;

  return {
    target_name: match[1].trim(),
    date_text: "today",
  };
}

function parseAuditAttendanceCommand(text) {
  const raw = normalizeText(text);
  let match = raw.match(
    /^audit\s+(.+?)\s+(today|tomorrow|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)$/i,
  );

  if (match) {
    return {
      target_name: match[1].trim(),
      date_text: match[2].trim(),
    };
  }

  match = raw.match(/^audit\s+(.+)$/i);
  if (!match) return null;

  return {
    target_name: match[1].trim(),
    date_text: "today",
  };
}

function parseUndoAttendanceCommand(text) {
  const raw = normalizeText(text);

  if (/^undo\s+my\s+attendance$/i.test(raw)) {
    return {
      mode: "self",
      target_name: null,
    };
  }

  const match = raw.match(/^undo\s+attendance\s+(.+)$/i);
  if (!match) return null;

  return {
    mode: "other",
    target_name: match[1].trim(),
  };
}

function parseResetAttendanceCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(
    /^reset\s+(.+?)\s+(today|tomorrow|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)$/i,
  );
  if (!match) return null;

  return {
    target_name: match[1].trim(),
    date_text: match[2].trim(),
  };
}

function parseForceAttendanceCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(
    /^force\s+(logout|back)\s+(.+?)\s+(\d{1,2}:\d{2}\s*(?:am|pm))$/i,
  );
  if (match) {
    return {
      action: match[1].toLowerCase(),
      target_name: match[2].trim(),
      time_text: match[3].trim().replace(/\s+/g, " "),
    };
  }

  match = raw.match(/^force\s+(logout|back)\s+(.+)$/i);
  if (!match) return null;

  return {
    action: match[1].toLowerCase(),
    target_name: match[2].trim(),
    time_text: null,
  };
}

function parseFixAttendanceCommand(text) {
  const raw = normalizeText(text);

  const match = raw.match(
    /^fix\s+(.+?)\s+(login|logout|break|back)\s+(\d{1,2}:\d{2}\s*(?:am|pm))$/i,
  );
  if (!match) return null;

  return {
    target_name: match[1].trim(),
    action: match[2].toLowerCase(),
    time_text: match[3].trim().replace(/\s+/g, " "),
  };
}

function parseRemoveAttendanceCommand(text) {
  const raw = normalizeText(text);

  const match = raw.match(/^remove\s+(.+?)\s+(login|logout|break|back)$/i);
  if (!match) return null;

  return {
    target_name: match[1].trim(),
    action: match[2].toLowerCase(),
  };
}

function parseAutoFixAttendanceCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(
    /^auto\s+fix\s+(.+?)\s+(today|tomorrow|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)$/i,
  );
  if (match) {
    return {
      target_name: match[1].trim(),
      date_text: match[2].trim(),
    };
  }

  match = raw.match(/^auto\s+fix\s+(.+)$/i);
  if (!match) return null;

  return {
    target_name: match[1].trim(),
    date_text: "today",
  };
}

function parseLockAttendanceCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(
    /^(lock|unlock)\s+(.+?)\s+(today|tomorrow|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)$/i,
  );
  if (!match) return null;

  return {
    mode: match[1].toLowerCase(),
    target_name: match[2].trim(),
    date_text: match[3].trim(),
  };
}

function parseIsoToAttendanceDateString(isoString) {
  if (!isoString) return null;
  const d = new Date(isoString);
  if (Number.isNaN(d.getTime())) return null;
  return getAttendanceDayDateStringFromDate(d);
}

function formatDateForDbFromParts(year, month, day) {
  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function formatLocalDateForDb(date) {
  const parts = getPartsInTimeZone(date, APP_TIMEZONE);
  return formatDateForDbFromParts(parts.year, parts.month, parts.day);
}

function getTodayDateStringInTimeZone(timeZone = APP_TIMEZONE) {
  const parts = getPartsInTimeZone(new Date(), timeZone);
  return formatDateForDbFromParts(parts.year, parts.month, parts.day);
}

function addDaysToDateString(dateString, days) {
  const base = new Date(`${dateString}T00:00:00Z`);
  base.setUTCDate(base.getUTCDate() + days);

  return formatDateForDbFromParts(
    base.getUTCFullYear(),
    base.getUTCMonth() + 1,
    base.getUTCDate(),
  );
}

function getMonthDateRangeForTimeZone(
  date = new Date(),
  timeZone = APP_TIMEZONE,
) {
  const parts = getPartsInTimeZone(date, timeZone);
  const startDate = formatDateForDbFromParts(parts.year, parts.month, 1);

  const nextMonthYear = parts.month === 12 ? parts.year + 1 : parts.year;
  const nextMonth = parts.month === 12 ? 1 : parts.month + 1;
  const nextMonthStart = formatDateForDbFromParts(nextMonthYear, nextMonth, 1);

  return {
    startDate,
    endDateExclusive: nextMonthStart,
  };
}

function getCurrentYearInTimeZone(timeZone = APP_TIMEZONE) {
  return getPartsInTimeZone(new Date(), timeZone).year;
}

function getUtcRangeForTodayInTimeZone(timeZone = APP_TIMEZONE) {
  const todayDb = getTodayDateStringInTimeZone(timeZone);
  const tomorrowDb = addDaysToDateString(todayDb, 1);

  const startUtc = new Date(
    `${todayDb}T00:00:00${APP_TIMEZONE_OFFSET}`,
  ).toISOString();
  const endUtc = new Date(
    `${tomorrowDb}T00:00:00${APP_TIMEZONE_OFFSET}`,
  ).toISOString();

  return { startUtc, endUtc, todayDb };
}

function parseExtraWorkCommand(text) {
  const raw = String(text || "").trim();
  const match = raw.match(/^extra work\s+(.+)$/i);
  if (!match) return null;

  const note = String(match[1] || "").trim();
  if (!note) return null;

  return { note };
}

function getReportDateString(date = new Date()) {
  return getAttendanceDayDateStringFromDate(date);
}

function getReportDayUtcRange(reportDate) {
  const nextDate = addDaysToDateString(reportDate, 1);

  return {
    startUtc: new Date(
      `${reportDate}T00:00:00${APP_TIMEZONE_OFFSET}`,
    ).toISOString(),
    endUtc: new Date(
      `${nextDate}T00:00:00${APP_TIMEZONE_OFFSET}`,
    ).toISOString(),
  };
}

async function insertDailyReportNote({
  orgId,
  userId,
  reportDate,
  note,
  createdByUserId,
  sourceMessageSid = null,
}) {
  const normalizedNote = normalizeText(note).replace(/\s+/g, " ");

  const row = {
    org_id: orgId,
    user_id: userId,
    report_date: reportDate,
    note,
    normalized_note: normalizedNote,
    source_type: "manual",
    source_message_sid: sourceMessageSid,
    created_by_user_id: createdByUserId,
  };

  const { data, error } = await supabase
    .from("daily_report_notes")
    .insert([row])
    .select("id, org_id, user_id, report_date, note, created_at")
    .maybeSingle();

  return { data, error };
}

async function getDailyReportNotes({ orgId, reportDate, userId = null }) {
  let query = supabase
    .from("daily_report_notes")
    .select("id, org_id, user_id, report_date, note, created_at")
    .eq("org_id", orgId)
    .eq("report_date", reportDate)
    .order("created_at", { ascending: true });

  if (userId) {
    query = query.eq("user_id", userId);
  }

  const { data, error } = await query;

  if (error) {
    console.error("getDailyReportNotes error:", error);
    return [];
  }

  return data || [];
}

async function getUserOpenBlockedCounts(orgId, userId) {
  const { data, error } = await supabase
    .from("task_owners")
    .select(
      `
      task_id,
      tasks!inner(id, org_id, status)
    `,
    )
    .eq("org_id", orgId)
    .eq("user_id", userId);

  if (error) {
    console.error("getUserOpenBlockedCounts error:", error);
    return { open: 0, blocked: 0 };
  }

  let open = 0;
  let blocked = 0;

  for (const row of data || []) {
    const task = row.tasks;
    if (!task || task.org_id !== orgId) continue;

    const status = String(task.status || "").toLowerCase();

    if (!["done", "archived", "cancelled"].includes(status)) {
      open += 1;
    }

    if (status === "blocked") {
      blocked += 1;
    }
  }

  return { open, blocked };
}

function formatShortDate(dateString) {
  if (!dateString) return "-";

  const d = new Date(`${dateString}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return String(dateString);

  return d.toLocaleDateString("en-IN", {
    timeZone: APP_TIMEZONE,
    day: "numeric",
    month: "short",
  });
}

function escapeHtmlAttr(value) {
  return escapeHtml(value).replace(/'/g, "&#39;");
}

function summarizeProgressDelta(taskNarratives) {
  let totalDelta = 0;

  for (const item of taskNarratives || []) {
    const from = Number(item.fromProgress);
    const to = Number(item.toProgress);

    if (!Number.isNaN(from) && !Number.isNaN(to) && to > from) {
      totalDelta += to - from;
    }
  }

  return totalDelta;
}

function buildCompactUserMeta(userReport) {
  const touched = (userReport.taskNarratives || []).length;
  const delta = summarizeProgressDelta(userReport.taskNarratives || []);
  const blocked = Number(userReport.summary?.blocked || 0);
  const hasExtra = (userReport.extraWork || []).length > 0;

  const parts = [];
  parts.push(`${touched} touched`);
  if (delta > 0) parts.push(`+${delta}%`);
  if (blocked > 0) parts.push(`${blocked} blocked`);
  if (hasExtra) parts.push("extra");

  return parts.join(" · ");
}

function extractCompactChangeChips(entries) {
  const chipMap = new Map();

  for (const entry of entries || []) {
    const fieldName = String(entry.field_name || "").toLowerCase();
    const changeType = String(entry.change_type || "").toLowerCase();
    const oldValue = entry.old_value || {};
    const newValue = entry.new_value || {};

    if (fieldName === "deadline" || changeType === "deadline_change") {
      chipMap.set("deadline", {
        key: "deadline",
        label: "deadline",
        detail: `${formatShortDate(oldValue.deadline)} → ${formatShortDate(newValue.deadline)}`,
      });
    }

    if (fieldName === "owner" || changeType === "owner_change") {
      const oldOwners = Array.isArray(oldValue.owners)
        ? oldValue.owners.join(", ")
        : "-";
      const newOwners = Array.isArray(newValue.owners)
        ? newValue.owners.join(", ")
        : "-";

      chipMap.set("owner", {
        key: "owner",
        label: "owner",
        detail: `${oldOwners} → ${newOwners}`,
      });
    }

    if (fieldName === "status" || changeType === "status_change") {
      chipMap.set("status", {
        key: "status",
        label: "status",
        detail: `${oldValue.status || "-"} → ${newValue.status || "-"}`,
      });
    }

    if (fieldName === "priority") {
      chipMap.set("priority", {
        key: "priority",
        label: "priority",
        detail: `${oldValue.priority || "-"} → ${newValue.priority || "-"}`,
      });
    }
  }

  return Array.from(chipMap.values());
}

function classifyReportUsers(users) {
  const full = [];
  const partial = [];
  const missing = [];
  const onLeave = [];

  for (const user of users || []) {
    if (user.isOnLeave) {
      onLeave.push(user.userName);
      continue;
    }

    const touched = (user.taskNarratives || []).length;
    const extra = (user.extraWork || []).length;

    if (touched > 0 && extra > 0) {
      full.push(user.userName);
    } else if (touched > 0 || extra > 0) {
      partial.push(user.userName);
    } else {
      missing.push(user.userName);
    }
  }

  return { full, partial, missing, onLeave };
}

function linkifyTaskSentence(sentence, taskNo, taskId) {
  const safeSentence = escapeHtml(sentence || "");
  const clickable = `<button type="button" class="task-inline-link" onclick="openTaskDetail(${Number(taskNo)}, ${Number(taskId)})">#${escapeHtml(taskNo)}</button>`;
  return safeSentence.replace(/^Task #\d+/, `Task ${clickable}`);
}

function buildTaskNarrativeFromHistoryEntries(entries, taskTitle, taskNoOrId) {
  if (!entries || !entries.length) return null;

  let firstProgress = null;
  let lastProgress = null;
  let finalStatus = null;
  let blockerAdded = null;
  let blockerCleared = false;
  const notes = [];

  for (const entry of entries) {
    const oldValue = entry.old_value || {};
    const newValue = entry.new_value || {};
    const changeType = String(entry.change_type || "");
    const fieldName = String(entry.field_name || "");

    if (oldValue.progress != null && firstProgress == null) {
      firstProgress = oldValue.progress;
    }

    if (newValue.progress != null) {
      lastProgress = newValue.progress;
    }

    if (newValue.status) {
      finalStatus = String(newValue.status).toLowerCase();
    }

    if (
      (fieldName === "status" || fieldName === "blocker_note") &&
      newValue.blocker_note
    ) {
      blockerAdded = newValue.blocker_note;
    }

    if (
      oldValue.blocker_note &&
      (newValue.blocker_note == null || newValue.blocker_note === "")
    ) {
      blockerCleared = true;
    }

    const possibleNote = newValue.note || oldValue.note || null;

    if (possibleNote && !notes.includes(possibleNote)) {
      notes.push(possibleNote);
    }

    if (
      changeType === "edit" &&
      fieldName === "blocker_note" &&
      newValue.blocker_note
    ) {
      if (!notes.includes(newValue.blocker_note)) {
        notes.push(newValue.blocker_note);
      }
    }
  }

  let sentence = `Task #${taskNoOrId} — ${taskTitle}: `;

  if (
    firstProgress != null &&
    lastProgress != null &&
    firstProgress !== lastProgress
  ) {
    sentence += `Worked on this from ${firstProgress}% to ${lastProgress}%`;
  } else if (finalStatus === "done") {
    sentence += "Completed this task";
  } else if (blockerAdded) {
    sentence += "Worked on this and got blocked";
  } else if (blockerCleared) {
    sentence += "Cleared blocker and resumed progress";
  } else {
    sentence += "Updated this task";
  }

  if (finalStatus === "done" && notes.length) {
    sentence += ` by ${notes[0]}`;
  } else if (blockerAdded) {
    sentence += ` waiting on ${blockerAdded}`;
  } else if (notes.length) {
    sentence += ` and ${notes[0]}`;
  }

  sentence += ".";

  return {
    sentence,
    fromProgress: firstProgress,
    toProgress: lastProgress,
    finalStatus,
    blockerAdded,
    blockerCleared,
    notePreview: notes[0] || null,
  };
}

async function getDailyTaskNarratives({ orgId, reportDate, userId = null }) {
  const { startUtc, endUtc } = getReportDayUtcRange(reportDate);

  let query = supabase
    .from("task_history")
    .select(
      `
      id,
      org_id,
      task_id,
      changed_by_user_id,
      change_type,
      field_name,
      old_value,
      new_value,
      created_at
    `,
    )
    .eq("org_id", orgId)
    .gte("created_at", startUtc)
    .lt("created_at", endUtc)
    .order("created_at", { ascending: true });

  if (userId) {
    query = query.eq("changed_by_user_id", userId);
  }

  const { data: historyRows, error: historyError } = await query;

  if (historyError) {
    console.error("getDailyTaskNarratives history error:", historyError);
    return [];
  }

  const history = (historyRows || []).filter((row) => {
    const changeType = String(row.change_type || "");
    return [
      "task_created",
      "progress_change",
      "status_change",
      "edit",
      "owner_change",
      "deadline_change",
    ].includes(changeType);
  });

  if (!history.length) return [];

  const taskIds = [...new Set(history.map((x) => x.task_id).filter(Boolean))];
  if (!taskIds.length) return [];

  const { data: taskRows, error: taskError } = await supabase
    .from("tasks")
    .select("id, task_no, title")
    .eq("org_id", orgId)
    .in("id", taskIds);

  if (taskError) {
    console.error("getDailyTaskNarratives task fetch error:", taskError);
    return [];
  }

  const taskMap = new Map((taskRows || []).map((task) => [task.id, task]));
  const grouped = new Map();

  for (const row of history) {
    const task = taskMap.get(row.task_id);
    if (!task) continue;

    const key = `${row.changed_by_user_id}::${row.task_id}`;

    if (!grouped.has(key)) {
      grouped.set(key, {
        userId: row.changed_by_user_id,
        taskId: row.task_id,
        taskNo: task.task_no || task.id,
        title: task.title,
        entries: [],
      });
    }

    grouped.get(key).entries.push(row);
  }

  const out = [];

  for (const group of grouped.values()) {
    const narrative = buildTaskNarrativeFromHistoryEntries(
      group.entries,
      group.title,
      group.taskNo,
    );

    if (!narrative) continue;

    out.push({
      userId: group.userId,
      taskId: group.taskId,
      taskNo: group.taskNo,
      title: group.title,
      sentence: narrative.sentence,
      fromProgress: narrative.fromProgress,
      toProgress: narrative.toProgress,
      finalStatus: narrative.finalStatus,
      notePreview: narrative.notePreview,
      compactChanges: extractCompactChangeChips(group.entries),
    });
  }

  out.sort((a, b) => {
    if (a.userId !== b.userId) return a.userId - b.userId;
    return a.taskNo - b.taskNo;
  });

  return out;
}

function emptyUserDailyReport(user) {
  return {
    userId: user.id,
    userName: user.name,
    taskNarratives: [],
    extraWork: [],
    summary: {
      open: 0,
      blocked: 0,
    },
  };
}

async function getDailyNarrativeReport({ orgId, reportDate, userId = null }) {
  let usersQuery = supabase
    .from("users")
    .select("id, name, role")
    .eq("org_id", orgId)
    .eq("is_active", true)
    .order("name", { ascending: true });

  if (userId) {
    usersQuery = usersQuery.eq("id", userId);
  }

  const { data: users, error: usersError } = await usersQuery;
  if (usersError) {
    throw usersError;
  }

  const [taskNarratives, extraNotes, plannedOffRows] = await Promise.all([
    getDailyTaskNarratives({ orgId, reportDate, userId }),
    getDailyReportNotes({ orgId, reportDate, userId }),
    getPlannedOffRowsForDate(reportDate, orgId),
  ]);

  const leaveSet = new Set((plannedOffRows || []).map((x) => x.user_id));

  const narrativesByUser = new Map();
  for (const item of taskNarratives) {
    if (!narrativesByUser.has(item.userId))
      narrativesByUser.set(item.userId, []);
    narrativesByUser.get(item.userId).push(item);
  }

  const notesByUser = new Map();
  for (const note of extraNotes) {
    if (!notesByUser.has(note.user_id)) notesByUser.set(note.user_id, []);
    notesByUser.get(note.user_id).push(note.note);
  }

  const resultUsers = [];
  for (const user of users || []) {
    const row = emptyUserDailyReport(user);
    row.taskNarratives = narrativesByUser.get(user.id) || [];
    row.extraWork = notesByUser.get(user.id) || [];
    row.summary = await getUserOpenBlockedCounts(orgId, user.id);
    row.isOnLeave = leaveSet.has(user.id);
    row.compactMeta = buildCompactUserMeta(row);
    resultUsers.push(row);
  }

  return {
    reportDate,
    users: resultUsers,
    compliance: classifyReportUsers(resultUsers),
  };
}

async function getMultiDayNarrativeReport({
  orgId,
  userId,
  days = 7,
  endDate = null,
}) {
  const safeDays = Math.max(1, Math.min(31, Number(days || 7)));
  const finalDate = endDate || getReportDateString();

  const dailyReports = [];

  for (let i = 0; i < safeDays; i += 1) {
    const reportDate = addDaysToDateString(finalDate, -i);

    const daily = await getDailyNarrativeReport({
      orgId,
      reportDate,
      userId,
    });

    dailyReports.push(daily);
  }

  return {
    mode: "multi_day_user",
    userId,
    endDate: finalDate,
    days: safeDays,
    dailyReports,
  };
}

function renderReportsPage(data) {
  const reportDate = data?.reportDate || getReportDateString();
  const users = data?.users || [];
  const compliance = data?.compliance || {
    full: [],
    partial: [],
    missing: [],
    onLeave: [],
  };

  const cardsHtml = users.length
    ? users
        .map((user) => {
          const taskHtml = (user.taskNarratives || []).length
            ? user.taskNarratives
                .map((item) => {
                  const chipsHtml = (item.compactChanges || []).length
                    ? `
                      <div class="change-chips">
                        ${item.compactChanges
                          .map(
                            (chip) => `
                              <span
                                class="change-chip"
                                title="${escapeHtmlAttr(chip.detail || chip.label)}"
                              >
                                ${escapeHtml(chip.label)}
                              </span>
                            `,
                          )
                          .join("")}
                      </div>
                    `
                    : "";

                  return `
                    <li class="report-task-item">
                      <div class="task-line">
                        ${linkifyTaskSentence(item.sentence, item.taskNo, item.taskId)}
                      </div>
                      ${chipsHtml}
                    </li>
                  `;
                })
                .join("")
            : `<li class="muted">No task updates today</li>`;

          const extraHtml = (user.extraWork || []).length
            ? user.extraWork
                .map((note) => `<li>${escapeHtml(note)}</li>`)
                .join("")
            : `<li class="muted">No extra work notes</li>`;

          return `
            <div class="report-card" data-user-name="${escapeHtml(String(user.userName || "").toLowerCase())}">
              <div class="report-card-head">
                <div>
                  <div class="report-name">
                    <a href="/attendance/${escapeHtml(user.userId)}">${escapeHtml(user.userName)}</a>
                  </div>
                  <div class="report-date">${escapeHtml(formatDateOnly(reportDate))}</div>
                  <div class="micro-meta">${escapeHtml(user.compactMeta || "0 touched")}</div>
                </div>
                <div class="summary-pill">
                  Open: ${escapeHtml(user.summary?.open ?? 0)} | Blocked: ${escapeHtml(user.summary?.blocked ?? 0)}
                </div>
              </div>

              <div class="report-section">
                <div class="section-title">Task updates</div>
                <ul class="report-list">${taskHtml}</ul>
              </div>

              <div class="report-section">
                <div class="section-title">Extra work</div>
                <ul class="report-list">${extraHtml}</ul>
              </div>
            </div>
          `;
        })
        .join("")
    : `
      <div class="panel" style="padding:18px;">
        <div class="muted">No users found.</div>
      </div>
    `;

  return `
    <html>
      <head>
        <title>Reports</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}

          .wrap { max-width: 1400px; margin: 0 auto; padding: 24px 18px 36px; }
          .topbar, .panel, .report-card, .status-chip-box, .modal-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
          }
          .topbar {
            display:flex; justify-content:space-between; align-items:center;
            gap:16px; flex-wrap:wrap; margin-bottom:20px; padding:18px 20px;
          }
          .eyebrow {
            font-size:11px; letter-spacing:0.16em; text-transform:uppercase;
            color:var(--primary); font-weight:700; margin-bottom:8px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }
          h1 { margin:0; font-size:30px; letter-spacing:-0.04em; }
          .subtitle { color:var(--muted); margin-top:8px; font-size:14px; }
          .links { display:flex; gap:10px; flex-wrap:wrap; }
          .links a {
            color: var(--text);
            text-decoration: none;
            padding: 10px 14px;
            border-radius: 12px;
            border: 1px solid color-mix(in srgb, var(--secondary) 30%, transparent);
            background: var(--secondary-soft);
            font-weight: 600;
          }

          .reports-grid {
            display:grid;
            grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
            gap:16px;
          }

          .report-name a {
            color: var(--text);
            text-decoration: none;
          }

          .report-name a:hover {
            color: var(--text-strong);
            text-decoration: underline;
          }

          .report-card { padding:16px; }
          .report-card-head {
            display:flex;
            justify-content:space-between;
            align-items:flex-start;
            gap:12px;
            margin-bottom:14px;
          }
          .report-name { font-size:20px; font-weight:800; }
          .report-date { color:var(--muted); font-size:13px; margin-top:4px; }
          .micro-meta {
            margin-top:6px;
            font-size:12px;
            color:var(--muted);
            font-weight:700;
          }
          .summary-pill {
            white-space:nowrap;
            padding:10px 12px;
            border-radius:12px;
            background:var(--primary-soft);
            border:1px solid rgba(255,255,255,0.08);
            font-weight:700;
            font-size:13px;
          }
          .report-section + .report-section {
            margin-top:16px;
            padding-top:16px;
            border-top:1px solid rgba(255,255,255,0.08);
          }
          .section-title {
            font-size:12px;
            text-transform:uppercase;
            letter-spacing:0.1em;
            color:var(--muted);
            font-weight:800;
            margin-bottom:10px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }
          .report-list {
            margin:0;
            padding-left:18px;
            line-height:1.6;
          }
          .report-list li + li { margin-top:8px; }

          .report-task-item {
            margin-bottom: 10px;
          }

          .task-line {
            display:block;
          }

          .task-inline-link {
            padding:0;
            margin:0;
            border:none;
            background:none;
            color:var(--secondary);
            font-weight:800;
            cursor:pointer;
            font-size:inherit;
          }

          .task-inline-link:hover {
            text-decoration:underline;
          }

          .change-chips {
            display:flex;
            gap:6px;
            flex-wrap:wrap;
            margin-top:6px;
          }

          .change-chip {
            display:inline-flex;
            align-items:center;
            padding:2px 8px;
            border-radius:999px;
            background:rgba(255,255,255,0.06);
            border:1px solid rgba(255,255,255,0.08);
            color:var(--muted);
            font-size:11px;
            font-weight:700;
            line-height:1.5;
          }

          .status-grid {
            display:grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap:12px;
            margin-bottom:16px;
          }

          .status-chip-box {
            padding:12px 14px;
          }

          .status-chip-title {
            font-size:11px;
            text-transform:uppercase;
            letter-spacing:0.12em;
            color:var(--muted);
            font-weight:800;
            margin-bottom:8px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

          .status-chip-count {
            font-size:22px;
            font-weight:800;
            margin-bottom:4px;
          }

          .status-chip-names {
            font-size:13px;
            color:var(--muted);
            line-height:1.5;
          }

          .modal-backdrop {
            position: fixed;
            inset: 0;
            background: rgba(4, 8, 20, 0.72);
            display: none;
            align-items: center;
            justify-content: center;
            padding: 24px;
            z-index: 9999;
          }

          .modal-backdrop.open {
            display: flex;
          }

          .modal-card {
            width: min(860px, 100%);
            max-height: 88vh;
            overflow: auto;
            padding: 18px;
          }

          .modal-head {
            display:flex;
            justify-content:space-between;
            gap:12px;
            align-items:flex-start;
            margin-bottom:14px;
          }

          .modal-title {
            font-size:24px;
            font-weight:800;
            margin:0;
          }

          .modal-close {
            border:none;
            background:rgba(255,255,255,0.08);
            color:var(--text);
            border-radius:10px;
            padding:8px 10px;
            cursor:pointer;
            font-weight:700;
          }

          .modal-meta-grid {
            display:grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap:10px;
            margin-bottom:14px;
          }

          .modal-meta-box {
            border:1px solid rgba(255,255,255,0.08);
            border-radius:12px;
            padding:10px 12px;
            background:rgba(255,255,255,0.04);
          }

          .modal-meta-label {
            font-size:11px;
            text-transform:uppercase;
            letter-spacing:0.1em;
            color:var(--muted);
            font-weight:800;
            margin-bottom:4px;
          }

          .history-list {
            display:flex;
            flex-direction:column;
            gap:10px;
          }

          .history-item {
            border:1px solid rgba(255,255,255,0.08);
            border-radius:12px;
            padding:10px 12px;
            background:rgba(255,255,255,0.04);
          }

          .history-top {
            display:flex;
            justify-content:space-between;
            gap:8px;
            flex-wrap:wrap;
            margin-bottom:4px;
            font-size:13px;
          }

          .history-detail {
            color:var(--muted);
            font-size:13px;
            line-height:1.5;
            white-space:pre-wrap;
            word-break:break-word;
          }

          @media (max-width: 700px) {
            .wrap { padding:16px 12px 28px; }
            h1 { font-size:24px; }
            .report-card-head { flex-direction:column; }
            .summary-pill { white-space:normal; }
          }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Daily Reporting</div>
              <h1>WeSolveHR // Reports</h1>
              <div class="subtitle">Today only. Task narratives + extra work + open/blocked snapshot.</div>
            </div>
            <div class="links">
              <a href="/dashboard">Dashboard</a>
              <a href="/tasks">Tasks</a>
              <a href="/attendance">Attendance</a>
              <a href="/logs">Logs</a>
              <a href="/bugs">Bug Board</a>
              <a href="/reports">Reports</a>
            </div>
          </div>

          <div class="panel" style="padding:14px 16px; margin-bottom:16px;">
            <strong>Date:</strong> ${escapeHtml(formatDateOnly(reportDate))}
          </div>

          <div class="status-grid">
            <div class="status-chip-box">
              <div class="status-chip-title">Fully updated</div>
              <div class="status-chip-count">${escapeHtml(compliance.full.length)}</div>
              <div class="status-chip-names">${escapeHtml(compliance.full.join(", ") || "None")}</div>
            </div>
            <div class="status-chip-box">
              <div class="status-chip-title">Partially updated</div>
              <div class="status-chip-count">${escapeHtml(compliance.partial.length)}</div>
              <div class="status-chip-names">${escapeHtml(compliance.partial.join(", ") || "None")}</div>
            </div>
            <div class="status-chip-box">
              <div class="status-chip-title">Missing</div>
              <div class="status-chip-count">${escapeHtml(compliance.missing.length)}</div>
              <div class="status-chip-names">${escapeHtml(compliance.missing.join(", ") || "None")}</div>
            </div>
            <div class="status-chip-box">
              <div class="status-chip-title">On leave</div>
              <div class="status-chip-count">${escapeHtml(compliance.onLeave.length)}</div>
              <div class="status-chip-names">${escapeHtml(compliance.onLeave.join(", ") || "None")}</div>
            </div>
          </div>

          <div class="panel" style="padding:14px 16px; margin-bottom:16px;">
            <input
              id="reportSearch"
              type="text"
              placeholder="Search user name"
              oninput="filterReports()"
              style="width:100%; padding:12px 14px; border-radius:12px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);"
            />
          </div>

          <div class="reports-grid">
            ${cardsHtml}
          </div>
        </div>

        <div id="taskModal" class="modal-backdrop" onclick="closeTaskModal(event)">
          <div class="modal-card" onclick="event.stopPropagation()">
            <div class="modal-head">
              <div>
                <div class="eyebrow">Task detail</div>
                <h2 id="modalTitle" class="modal-title">Loading...</h2>
              </div>
              <button class="modal-close" onclick="closeTaskModal()">Close</button>
            </div>

            <div id="modalBody">
              <div class="muted">Loading task details...</div>
            </div>
          </div>
        </div>

        <script>
          function filterReports() {
            const input = document.getElementById("reportSearch");
            const query = String(input?.value || "").trim().toLowerCase();
            const cards = document.querySelectorAll(".report-card");

            for (const card of cards) {
              const userName = String(card.getAttribute("data-user-name") || "");
              card.style.display = !query || userName.includes(query) ? "" : "none";
            }
          }

          function closeTaskModal(event) {
            if (event && event.target && event.target.id !== "taskModal") return;
            document.getElementById("taskModal").classList.remove("open");
          }

          function renderHistoryDetail(item) {
            const oldText = JSON.stringify(item.oldValue || {});
            const newText = JSON.stringify(item.newValue || {});
            return "Field: " + (item.fieldName || "-") + "\\nOld: " + oldText + "\\nNew: " + newText;
          }

          async function openTaskDetail(taskNo) {
            const modal = document.getElementById("taskModal");
            const title = document.getElementById("modalTitle");
            const body = document.getElementById("modalBody");

            title.textContent = "Task #" + taskNo;
            body.innerHTML = '<div class="muted">Loading task details...</div>';
            modal.classList.add("open");

            try {
              const res = await fetch("/api/reports/task/" + taskNo);
              const json = await res.json();

              if (!json.ok) {
                body.innerHTML = '<div class="muted">' + (json.error || "Failed to load task") + '</div>';
                return;
              }

              const task = json.data || {};
              title.textContent = "#" + (task.taskNo || task.id) + " — " + (task.title || "Untitled");

              const historyHtml = (task.history || []).length
                ? task.history.map((item) => {
                    return (
                      '<div class="history-item">' +
                        '<div class="history-top">' +
                          '<strong>' + (item.changeType || "-") + '</strong>' +
                          '<span>' + (item.at || "-") + ' • ' + (item.by || "-") + '</span>' +
                        '</div>' +
                        '<div class="history-detail">' + renderHistoryDetail(item) + '</div>' +
                      '</div>'
                    );
                  }).join("")
                : '<div class="muted">No recent history</div>';

              body.innerHTML =
                '<div class="modal-meta-grid">' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Owners</div><div>' + ((task.owners || []).join(", ") || "-") + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Status</div><div>' + (task.status || "-") + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Priority</div><div>' + (task.priority || "-") + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Progress</div><div>' + (task.progress ?? "-") + '%</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Deadline</div><div>' + (task.deadline || "-") + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Business / Area</div><div>' + ((task.business || "-") + ' / ' + (task.area || "-")) + '</div></div>' +
                '</div>' +

                '<div class="report-section">' +
                  '<div class="section-title">Detail</div>' +
                  '<div>' + (task.detail || '<span class="muted">No detail</span>') + '</div>' +
                '</div>' +

                '<div class="report-section">' +
                  '<div class="section-title">Blocker</div>' +
                  '<div>' + (task.blockerNote || '<span class="muted">No blocker</span>') + '</div>' +
                '</div>' +

                '<div class="report-section">' +
                  '<div class="section-title">Recent history</div>' +
                  '<div class="history-list">' + historyHtml + '</div>' +
                '</div>';
            } catch (error) {
              body.innerHTML = '<div class="muted">Failed to load task details</div>';
            }
          }
        </script>
      </body>
    </html>
  `;
}

function renderMultiDayUserReportsPage(data) {
  const days = data?.days || 7;
  const dailyReports = data?.dailyReports || [];
  const firstUser =
    dailyReports?.[0]?.users?.[0] ||
    dailyReports?.find((d) => (d.users || []).length)?.users?.[0] ||
    null;

  const pageTitle = firstUser
    ? `${firstUser.userName} — Last ${days} Days`
    : `Last ${days} Days Report`;

  const dayCardsHtml = dailyReports
    .map((daily) => {
      const reportDate = daily.reportDate;
      const user = (daily.users || [])[0];

      if (!user) {
        return `
          <div class="report-card">
            <div class="report-card-head">
              <div>
                <div class="report-name">${escapeHtml(formatDateOnly(reportDate))}</div>
                <div class="report-date muted">No report data</div>
              </div>
            </div>
            <div class="report-section">
              <div class="muted">No updates found for this day.</div>
            </div>
          </div>
        `;
      }

      const taskHtml = (user.taskNarratives || []).length
        ? user.taskNarratives
            .map((item) => {
              const chipsHtml = (item.compactChanges || []).length
                ? `
                  <div class="change-chips">
                    ${item.compactChanges
                      .map(
                        (chip) => `
                          <span
                            class="change-chip"
                            title="${escapeHtmlAttr(chip.detail || chip.label)}"
                          >
                            ${escapeHtml(chip.label)}
                          </span>
                        `,
                      )
                      .join("")}
                  </div>
                `
                : "";

              return `
                <li class="report-task-item">
                  <div class="task-line">
                    ${linkifyTaskSentence(item.sentence, item.taskNo, item.taskId)}
                  </div>
                  ${chipsHtml}
                </li>
              `;
            })
            .join("")
        : `<li class="muted">No task updates</li>`;

      const extraHtml = (user.extraWork || []).length
        ? user.extraWork.map((note) => `<li>${escapeHtml(note)}</li>`).join("")
        : `<li class="muted">No extra work notes</li>`;

      return `
        <div class="report-card">
          <div class="report-card-head">
            <div>
              <div class="report-name">${escapeHtml(formatDateOnly(reportDate))}</div>
              <div class="report-date">${escapeHtml(user.userName)}</div>
              <div class="micro-meta">${escapeHtml(user.compactMeta || "0 touched")}</div>
            </div>
            <div class="summary-pill">
              Open: ${escapeHtml(user.summary?.open ?? 0)} | Blocked: ${escapeHtml(user.summary?.blocked ?? 0)}
            </div>
          </div>

          <div class="report-section">
            <div class="section-title">Task updates</div>
            <ul class="report-list">${taskHtml}</ul>
          </div>

          <div class="report-section">
            <div class="section-title">Extra work</div>
            <ul class="report-list">${extraHtml}</ul>
          </div>
        </div>
      `;
    })
    .join("");

  return `
    <html>
      <head>
        <title>${escapeHtml(pageTitle)}</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}

          .wrap { max-width: 1200px; margin: 0 auto; padding: 24px 18px 36px; }
          .topbar, .panel, .report-card, .modal-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
          }
          .topbar {
            display:flex; justify-content:space-between; align-items:center;
            gap:16px; flex-wrap:wrap; margin-bottom:20px; padding:18px 20px;
          }
          .eyebrow {
            font-size:11px; letter-spacing:0.16em; text-transform:uppercase;
            color:var(--primary); font-weight:700; margin-bottom:8px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }
          h1 { margin:0; font-size:30px; letter-spacing:-0.04em; }
          .subtitle { color:var(--muted); margin-top:8px; font-size:14px; }
          .links { display:flex; gap:10px; flex-wrap:wrap; }
          .links a {
            color: var(--text);
            text-decoration: none;
            padding: 10px 14px;
            border-radius: 12px;
            border: 1px solid color-mix(in srgb, var(--secondary) 30%, transparent);
            background: var(--secondary-soft);
            font-weight: 600;
          }

          .reports-stack {
            display:flex;
            flex-direction:column;
            gap:16px;
          }

          .report-card { padding:16px; }
          .report-card-head {
            display:flex;
            justify-content:space-between;
            align-items:flex-start;
            gap:12px;
            margin-bottom:14px;
          }
          .report-name { font-size:20px; font-weight:800; }
          .report-date { color:var(--muted); font-size:13px; margin-top:4px; }
          .micro-meta {
            margin-top:6px;
            font-size:12px;
            color:var(--muted);
            font-weight:700;
          }
          .summary-pill {
            white-space:nowrap;
            padding:10px 12px;
            border-radius:12px;
            background:var(--primary-soft);
            border:1px solid rgba(255,255,255,0.08);
            font-weight:700;
            font-size:13px;
          }
          .report-section + .report-section {
            margin-top:16px;
            padding-top:16px;
            border-top:1px solid rgba(255,255,255,0.08);
          }
          .section-title {
            font-size:12px;
            text-transform:uppercase;
            letter-spacing:0.1em;
            color:var(--muted);
            font-weight:800;
            margin-bottom:10px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }
          .report-list {
            margin:0;
            padding-left:18px;
            line-height:1.6;
          }
          .report-list li + li { margin-top:8px; }

          .report-task-item { margin-bottom: 10px; }
          .task-line { display:block; }

          .task-inline-link {
            padding:0;
            margin:0;
            border:none;
            background:none;
            color:var(--secondary);
            font-weight:800;
            cursor:pointer;
            font-size:inherit;
          }
          .task-inline-link:hover { text-decoration:underline; }

          .change-chips {
            display:flex;
            gap:6px;
            flex-wrap:wrap;
            margin-top:6px;
          }

          .change-chip {
            display:inline-flex;
            align-items:center;
            padding:2px 8px;
            border-radius:999px;
            background:rgba(255,255,255,0.06);
            border:1px solid rgba(255,255,255,0.08);
            color:var(--muted);
            font-size:11px;
            font-weight:700;
            line-height:1.5;
          }

          .modal-backdrop {
            position: fixed;
            inset: 0;
            background: rgba(4, 8, 20, 0.72);
            display: none;
            align-items: center;
            justify-content: center;
            padding: 24px;
            z-index: 9999;
          }
          .modal-backdrop.open { display: flex; }
          .modal-card {
            width: min(860px, 100%);
            max-height: 88vh;
            overflow: auto;
            padding: 18px;
          }
          .modal-head {
            display:flex;
            justify-content:space-between;
            gap:12px;
            align-items:flex-start;
            margin-bottom:14px;
          }
          .modal-title {
            font-size:24px;
            font-weight:800;
            margin:0;
          }
          .modal-close {
            border:none;
            background:rgba(255,255,255,0.08);
            color:var(--text);
            border-radius:10px;
            padding:8px 10px;
            cursor:pointer;
            font-weight:700;
          }
          .modal-meta-grid {
            display:grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap:10px;
            margin-bottom:14px;
          }
          .modal-meta-box {
            border:1px solid rgba(255,255,255,0.08);
            border-radius:12px;
            padding:10px 12px;
            background:rgba(255,255,255,0.04);
          }
          .modal-meta-label {
            font-size:11px;
            text-transform:uppercase;
            letter-spacing:0.1em;
            color:var(--muted);
            font-weight:800;
            margin-bottom:4px;
          }
          .history-list {
            display:flex;
            flex-direction:column;
            gap:10px;
          }
          .history-item {
            border:1px solid rgba(255,255,255,0.08);
            border-radius:12px;
            padding:10px 12px;
            background:rgba(255,255,255,0.04);
          }
          .history-top {
            display:flex;
            justify-content:space-between;
            gap:8px;
            flex-wrap:wrap;
            margin-bottom:4px;
            font-size:13px;
          }
          .history-detail {
            color:var(--muted);
            font-size:13px;
            line-height:1.5;
            white-space:pre-wrap;
            word-break:break-word;
          }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Multi-Day Reporting</div>
              <h1>${escapeHtml(pageTitle)}</h1>
              <div class="subtitle">Last ${escapeHtml(days)} attendance-days, one section per day.</div>
            </div>
            <div class="links">
              <a href="/dashboard">Dashboard</a>
              <a href="/tasks">Tasks</a>
              <a href="/attendance">Attendance</a>
              <a href="/logs">Logs</a>
              <a href="/bugs">Bug Board</a>
              <a href="/reports">Reports</a>
            </div>
          </div>

          <div class="reports-stack">
            ${dayCardsHtml}
          </div>
        </div>

        <div id="taskModal" class="modal-backdrop" onclick="closeTaskModal(event)">
          <div class="modal-card" onclick="event.stopPropagation()">
            <div class="modal-head">
              <div>
                <div class="eyebrow">Task detail</div>
                <h2 id="modalTitle" class="modal-title">Loading...</h2>
              </div>
              <button class="modal-close" onclick="closeTaskModal()">Close</button>
            </div>

            <div id="modalBody">
              <div class="muted">Loading task details...</div>
            </div>
          </div>
        </div>

        <script>
          function closeTaskModal(event) {
            if (event && event.target && event.target.id !== "taskModal") return;
            document.getElementById("taskModal").classList.remove("open");
          }

          function renderHistoryDetail(item) {
            const oldText = JSON.stringify(item.oldValue || {});
            const newText = JSON.stringify(item.newValue || {});
            return "Field: " + (item.fieldName || "-") + "\\nOld: " + oldText + "\\nNew: " + newText;
          }

          async function openTaskDetail(taskNo) {
            const modal = document.getElementById("taskModal");
            const title = document.getElementById("modalTitle");
            const body = document.getElementById("modalBody");

            title.textContent = "Task #" + taskNo;
            body.innerHTML = '<div class="muted">Loading task details...</div>';
            modal.classList.add("open");

            try {
              const res = await fetch("/api/reports/task/" + taskNo);
              const json = await res.json();

              if (!json.ok) {
                body.innerHTML = '<div class="muted">' + (json.error || "Failed to load task") + '</div>';
                return;
              }

              const task = json.data || {};
              title.textContent = "#" + (task.taskNo || task.id) + " — " + (task.title || "Untitled");

              const historyHtml = (task.history || []).length
                ? task.history.map((item) => {
                    return (
                      '<div class="history-item">' +
                        '<div class="history-top">' +
                          '<strong>' + (item.changeType || "-") + '</strong>' +
                          '<span>' + (item.at || "-") + ' • ' + (item.by || "-") + '</span>' +
                        '</div>' +
                        '<div class="history-detail">' + renderHistoryDetail(item) + '</div>' +
                      '</div>'
                    );
                  }).join("")
                : '<div class="muted">No recent history</div>';

              body.innerHTML =
                '<div class="modal-meta-grid">' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Owners</div><div>' + ((task.owners || []).join(", ") || "-") + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Status</div><div>' + (task.status || "-") + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Priority</div><div>' + (task.priority || "-") + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Progress</div><div>' + (task.progress ?? "-") + '%</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Deadline</div><div>' + (task.deadline || "-") + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Business / Area</div><div>' + ((task.business || "-") + ' / ' + (task.area || "-")) + '</div></div>' +
                '</div>' +
                '<div class="report-section">' +
                  '<div class="section-title">Detail</div>' +
                  '<div>' + (task.detail || '<span class="muted">No detail</span>') + '</div>' +
                '</div>' +
                '<div class="report-section">' +
                  '<div class="section-title">Blocker</div>' +
                  '<div>' + (task.blockerNote || '<span class="muted">No blocker</span>') + '</div>' +
                '</div>' +
                '<div class="report-section">' +
                  '<div class="section-title">Recent history</div>' +
                  '<div class="history-list">' + historyHtml + '</div>' +
                '</div>';
            } catch (error) {
              body.innerHTML = '<div class="muted">Failed to load task details</div>';
            }
          }
        </script>
      </body>
    </html>
  `;
}

function buildDateForCurrentYear(month, day) {
  const year = getCurrentYearInTimeZone(APP_TIMEZONE);
  const d = new Date(Date.UTC(year, month - 1, day));
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

async function getLatestAttendanceByUser(orgId) {
  const today = getAttendanceDayDateStringFromDate(new Date());
  const [usersResult, events, plannedOffRows] = await Promise.all([
    supabase
      .from("users")
      .select("id, org_id, name, role, phone_number")
      .eq("org_id", orgId)
      .eq("is_active", true)
      .order("name", { ascending: true }),
    getTodayAttendanceEventsForAllUsers(orgId),
    getPlannedOffRowsForDate(today, orgId),
  ]);

  if (usersResult.error) {
    throw usersResult.error;
  }

  const users = usersResult.data || [];
  const plannedOffUserIds = new Set(
    (plannedOffRows || []).map((x) => x.user_id),
  );

  const eventsByUser = new Map();
  for (const ev of events || []) {
    if (!eventsByUser.has(ev.user_id)) {
      eventsByUser.set(ev.user_id, []);
    }
    eventsByUser.get(ev.user_id).push(ev);
  }

  return users.map((user) => {
    const userEvents = eventsByUser.get(user.id) || [];
    const last = userEvents[userEvents.length - 1] || null;

    const hasLoginOrBack = userEvents.some(
      (x) => x.action === "login" || x.action === "back",
    );

    let derivedStatus = "unknown";
    if (plannedOffUserIds.has(user.id)) {
      derivedStatus = "planned_off";
    } else if (last?.action) {
      derivedStatus = last.action;
    } else {
      derivedStatus = "no_login";
    }

    return {
      id: user.id,
      name: user.name,
      role: user.role,
      phone_number: user.phone_number,
      status: derivedStatus,
      last_action_at: last?.created_at || null,
      duration_min:
        derivedStatus === "break" && last?.created_at
          ? minutesBetween(last.created_at)
          : null,
      worked_min_today: computeWorkedMinutesFromEvents(userEvents),
      has_login_today: hasLoginOrBack,
    };
  });
}

async function getEmployeeMonthlyAttendanceSummary(userId, orgId) {
  const { startDate, endDateExclusive } = getMonthDateRangeForTimeZone(
    new Date(),
    APP_TIMEZONE,
  );
  const LATE_GRACE_MIN = 10;

  const todayAttendanceDate = getAttendanceDayDateStringFromDate(new Date());

  const startUtc = new Date(
    `${startDate}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
  ).toISOString();

  const endUtc = new Date(
    `${endDateExclusive}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
  ).toISOString();

  const [eventsResult, leaveResult, lateResult, auditResult] =
    await Promise.all([
      supabase
        .from("attendance_events")
        .select(
          "id, user_id, action, created_at, expected_duration_min, reason, note",
        )
        .eq("org_id", orgId)
        .eq("user_id", userId)
        .gte("created_at", startUtc)
        .lt("created_at", endUtc)
        .order("created_at", { ascending: true }),

      supabase
        .from("planned_time_off")
        .select("id, off_date")
        .eq("user_id", userId)
        .eq("org_id", orgId)
        .gte("off_date", startDate)
        .lt("off_date", endDateExclusive)
        .order("off_date", { ascending: true }),

      supabase
        .from("late_arrivals")
        .select("id, late_date, is_approved")
        .eq("user_id", userId)
        .eq("org_id", orgId)
        .gte("late_date", startDate)
        .lte("late_date", todayAttendanceDate),

      supabase
        .from("attendance_audit")
        .select("id, action_type, created_at")
        .eq("org_id", orgId)
        .eq("target_user_id", userId)
        .gte("created_at", startUtc)
        .lt("created_at", endUtc),
    ]);

  if (eventsResult.error) throw eventsResult.error;
  if (leaveResult.error) throw leaveResult.error;
  if (lateResult.error) throw lateResult.error;
  if (auditResult.error) throw auditResult.error;

  const events = eventsResult.data || [];
  const leaveRows = leaveResult.data || [];
  const lateRows = lateResult.data || [];
  const auditRows = auditResult.data || [];

  const eventsByAttendanceDay = new Map();

  for (const ev of events) {
    const attendanceDate = parseIsoToAttendanceDateString(ev.created_at);
    if (!attendanceDate) continue;

    if (attendanceDate > todayAttendanceDate) continue;

    if (!eventsByAttendanceDay.has(attendanceDate)) {
      eventsByAttendanceDay.set(attendanceDate, []);
    }
    eventsByAttendanceDay.get(attendanceDate).push(ev);
  }

  let presentDays = 0;
  let lateJoins = 0;
  let approvedLate = 0;
  let unapprovedLate = 0;
  let uninformedLate = 0;
  let totalLoginMinuteOfDay = 0;
  let loginDaysCount = 0;
  let totalBreakMinutes = 0;
  let breakDaysCount = 0;
  let longShiftCount = 0;
  let longBreakCount = 0;
  let possibleHalfDays = 0;

  for (const [attendanceDate, dayEvents] of eventsByAttendanceDay.entries()) {
    const summary = getAttendanceSummaryFromEvents(dayEvents);
    const lateInfo =
      lateRows.find((x) => x.late_date === attendanceDate) || null;

    if (summary.firstLogin) {
      presentDays += 1;

      const firstLoginParts = getPartsInTimeZone(
        new Date(summary.firstLogin.created_at),
        APP_TIMEZONE,
      );
      totalLoginMinuteOfDay +=
        firstLoginParts.hour * 60 + firstLoginParts.minute;
      loginDaysCount += 1;
    }

    totalBreakMinutes += summary.breakMinutes;
    if (summary.firstLogin) {
      breakDaysCount += 1;
    }

    if (summary.lateMinutes > LATE_GRACE_MIN) {
      lateJoins += 1;

      if (lateInfo && lateInfo.is_approved) {
        approvedLate += 1;
      } else if (lateInfo && !lateInfo.is_approved) {
        unapprovedLate += 1;
      } else {
        uninformedLate += 1;
      }
    }

    if (summary.longShiftFlag) longShiftCount += 1;
    if (summary.longBreakFlag) longBreakCount += 1;
    if (summary.possibleHalfDay) possibleHalfDays += 1;
  }

  const avgLoginMin = loginDaysCount
    ? Math.round(totalLoginMinuteOfDay / loginDaysCount)
    : null;

  const avgBreakMin = breakDaysCount
    ? Math.round(totalBreakMinutes / breakDaysCount)
    : 0;

  const avgLoginTimeText =
    avgLoginMin == null
      ? "-"
      : `${String(((Math.floor(avgLoginMin / 60) + 11) % 12) + 1)}:${String(
          avgLoginMin % 60,
        ).padStart(
          2,
          "0",
        )} ${Math.floor(avgLoginMin / 60) >= 12 ? "PM" : "AM"} IST`;

  const pastLeaveDates = leaveRows
    .filter((x) => x.off_date <= todayAttendanceDate)
    .map((x) => x.off_date);

  const upcomingLeaveDates = leaveRows
    .filter((x) => x.off_date > todayAttendanceDate)
    .map((x) => x.off_date);

  return {
    presentDays,
    leaveDays: leaveRows.length,
    pastLeaveDates,
    upcomingLeaveDates,
    lateJoins,
    approvedLate,
    unapprovedLate,
    uninformedLate,
    avgLoginTimeText,
    avgBreakMin,
    longShiftCount,
    longBreakCount,
    possibleHalfDays,
    managerCorrectionCount: auditRows.length,
  };
}

async function getEmployeeAttendanceOverview(userId, orgId) {
  const todayAttendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const { startUtc, endUtc } = getCurrentAttendanceDayRange();
  const { startDate, endDateExclusive } = getMonthDateRangeForTimeZone(
    new Date(),
    APP_TIMEZONE,
  );

  const [
    userResult,
    todayEventsResult,
    monthlyEventsResult,
    leaveResult,
    lateResult,
    auditResult,
    monthlySummary,
  ] = await Promise.all([
    supabase
      .from("users")
      .select("id, name, role, phone_number")
      .eq("id", userId)
      .eq("org_id", orgId)
      .eq("is_active", true)
      .maybeSingle(),

    supabase
      .from("attendance_events")
      .select(
        "id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
      )
      .eq("user_id", userId)
      .eq("org_id", orgId)
      .gte("created_at", startUtc)
      .lt("created_at", endUtc)
      .order("created_at", { ascending: true }),

    supabase
      .from("attendance_events")
      .select(
        "id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
      )
      .eq("user_id", userId)
      .eq("org_id", orgId)
      .gte(
        "created_at",
        new Date(
          `${startDate}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
        ).toISOString(),
      )
      .lt(
        "created_at",
        new Date(
          `${endDateExclusive}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
        ).toISOString(),
      )
      .order("created_at", { ascending: true }),

    supabase
      .from("planned_time_off")
      .select("id, off_date, note")
      .eq("user_id", userId)
      .eq("org_id", orgId)
      .gte("off_date", startDate)
      .lt("off_date", endDateExclusive)
      .order("off_date", { ascending: true }),

    supabase
      .from("late_arrivals")
      .select(
        "id, late_date, expected_login_at, informed_at, shift_start_at, is_approved, note",
      )
      .eq("user_id", userId)
      .eq("org_id", orgId)
      .gte("late_date", startDate)
      .lt("late_date", endDateExclusive)
      .order("late_date", { ascending: true }),

    supabase
      .from("attendance_audit")
      .select(
        "id, action_type, old_value, new_value, note, created_at, acted_by_user_id",
      )
      .eq("target_user_id", userId)
      .eq("org_id", orgId)
      .gte(
        "created_at",
        new Date(
          `${startDate}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
        ).toISOString(),
      )
      .lt(
        "created_at",
        new Date(
          `${endDateExclusive}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
        ).toISOString(),
      )
      .order("created_at", { ascending: false }),
    getEmployeeMonthlyAttendanceSummary(userId, orgId),
  ]);

  if (userResult.error) throw userResult.error;
  if (todayEventsResult.error) throw todayEventsResult.error;
  if (monthlyEventsResult.error) throw monthlyEventsResult.error;
  if (leaveResult.error) throw leaveResult.error;
  if (lateResult.error) throw lateResult.error;
  if (auditResult.error) throw auditResult.error;

  const user = userResult.data;
  if (!user) {
    throw new Error("Employee not found");
  }

  const todayEvents = todayEventsResult.data || [];
  const monthlyEvents = monthlyEventsResult.data || [];
  const leaveRows = leaveResult.data || [];
  const lateRows = lateResult.data || [];
  const auditRows = auditResult.data || [];

  const todaySummary = getAttendanceSummaryFromEvents(todayEvents);
  const leaveToday =
    leaveRows.find((x) => x.off_date === todayAttendanceDate) || null;
  const lateToday =
    lateRows.find((x) => x.late_date === todayAttendanceDate) || null;

  const eventsByAttendanceDay = new Map();

  for (const ev of monthlyEvents) {
    const attendanceDate = parseIsoToAttendanceDateString(ev.created_at);
    if (!attendanceDate) continue;

    if (!eventsByAttendanceDay.has(attendanceDate)) {
      eventsByAttendanceDay.set(attendanceDate, []);
    }
    eventsByAttendanceDay.get(attendanceDate).push(ev);
  }

  const history = [];

  const allAttendanceDates = new Set([
    ...Array.from(eventsByAttendanceDay.keys()),
    ...leaveRows.map((x) => x.off_date),
  ]);

  const sortedAttendanceDates = Array.from(allAttendanceDates).sort((a, b) =>
    a < b ? 1 : -1,
  );

  for (const attendanceDate of sortedAttendanceDates) {
    const dayEvents = eventsByAttendanceDay.get(attendanceDate) || [];
    const daySummary = getAttendanceSummaryFromEvents(dayEvents);
    const dayLate =
      lateRows.find((x) => x.late_date === attendanceDate) || null;
    const dayLeave =
      leaveRows.find((x) => x.off_date === attendanceDate) || null;
    const dayAuditCount = auditRows.filter((x) => {
      const auditDate = parseIsoToAttendanceDateString(x.created_at);
      return auditDate === attendanceDate;
    }).length;

    history.push({
      attendance_date: attendanceDate,
      status: dayLeave ? "leave" : daySummary.currentStatus,
      first_login_text: daySummary.firstLogin
        ? formatTimeOnly(daySummary.firstLogin.created_at)
        : "-",
      last_logout_text: daySummary.lastLogout
        ? formatTimeOnly(daySummary.lastLogout.created_at)
        : "-",
      worked_text: formatDurationMinutes(daySummary.workedMinutes),
      break_text: formatDurationMinutes(daySummary.breakMinutes),
      late_text:
        daySummary.lateMinutes > 10 ? `${daySummary.lateMinutes} min` : "No",
      late_approved:
        daySummary.lateMinutes > 10
          ? dayLate
            ? dayLate.is_approved
              ? "approved"
              : "not approved"
            : "no prior info"
          : "-",
      leave_text: dayLeave ? "Yes" : "No",
      flags:
        [
          daySummary.longShiftFlag ? "Long shift" : null,
          daySummary.longBreakFlag ? "Long break" : null,
          daySummary.possibleHalfDay ? "Half day" : null,
        ]
          .filter(Boolean)
          .join(", ") || "-",
      corrections: dayAuditCount,
      timeline: dayEvents.map((ev) => ({
        id: ev.id,
        action: ev.action,
        created_at: ev.created_at,
        time_text: formatTimeOnly(ev.created_at),
        reason: ev.reason || null,
        note: ev.note || null,
        expected_duration_min: ev.expected_duration_min || null,
      })),
    });
  }
  return {
    employee: user,
    today: {
      attendance_date: todayAttendanceDate,
      current_status: leaveToday ? "leave" : todaySummary.currentStatus,
      first_login_text: todaySummary.firstLogin
        ? formatTimeOnly(todaySummary.firstLogin.created_at)
        : "-",
      last_logout_text: todaySummary.lastLogout
        ? formatTimeOnly(todaySummary.lastLogout.created_at)
        : "-",
      worked_text: formatDurationMinutes(todaySummary.workedMinutes),
      break_text: formatDurationMinutes(todaySummary.breakMinutes),
      break_count: todaySummary.breakCount,
      late_text:
        todaySummary.lateMinutes > 10
          ? `${todaySummary.lateMinutes} min`
          : "No",
      late_status:
        todaySummary.lateMinutes > 10
          ? lateToday
            ? lateToday.is_approved
              ? "approved"
              : "not approved"
            : "no prior info"
          : "-",
      leave_today: !!leaveToday,
      long_shift_flag: todaySummary.longShiftFlag,
      long_break_flag: todaySummary.longBreakFlag,
      possible_half_day: todaySummary.possibleHalfDay,
      events: todayEvents.map((ev) => ({
        id: ev.id,
        action: ev.action,
        time_text: formatTimeOnly(ev.created_at),
        reason: ev.reason || null,
        note: ev.note || null,
        expected_duration_min: ev.expected_duration_min || null,
      })),
    },
    monthly: monthlySummary,
    recent_audit: auditRows.slice(0, 20).map((row) => ({
      id: row.id,
      action_type: row.action_type,
      note: row.note || "-",
      created_at_text: row.created_at ? formatDateTime(row.created_at) : "-",
    })),
    history,
  };
}

function renderEmployeeAttendancePage(data) {
  const employee = data?.employee || {};
  const today = data?.today || {};
  const monthly = data?.monthly || {};
  const history = data?.history || [];
  const recentAudit = data?.recent_audit || [];

  const todayTimelineRows = (today.events || []).length
    ? today.events
        .map(
          (ev) => `
            <tr>
              <td>${escapeHtml(ev.time_text || "-")}</td>
              <td>${escapeHtml(ev.action || "-")}</td>
              <td>${escapeHtml(
                ev.expected_duration_min
                  ? `${ev.expected_duration_min} min`
                  : "-",
              )}</td>
              <td>${escapeHtml(ev.reason || "-")}</td>
              <td>${escapeHtml(ev.note || "-")}</td>
            </tr>
          `,
        )
        .join("")
    : `
      <tr>
        <td colspan="5" class="empty-cell">No attendance events today.</td>
      </tr>
    `;

  const historyRows = history.length
    ? history
        .map(
          (row) => `
            <tr>
              <td>${escapeHtml(row.attendance_date)}</td>
              <td>${escapeHtml(row.status)}</td>
              <td>${escapeHtml(row.first_login_text)}</td>
              <td>${escapeHtml(row.last_logout_text)}</td>
              <td>${escapeHtml(row.worked_text)}</td>
              <td>${escapeHtml(row.break_text)}</td>
              <td>${escapeHtml(row.late_text)}</td>
              <td>${escapeHtml(row.late_approved)}</td>
              <td>${escapeHtml(row.leave_text)}</td>
              <td>${escapeHtml(row.flags)}</td>
              <td>${escapeHtml(String(row.corrections || 0))}</td>
            </tr>
          `,
        )
        .join("")
    : `
      <tr>
        <td colspan="11" class="empty-cell">No history found for this month.</td>
      </tr>
    `;

  const auditRows = recentAudit.length
    ? recentAudit
        .map(
          (row) => `
            <tr>
              <td>${escapeHtml(row.created_at_text)}</td>
              <td>${escapeHtml(row.action_type)}</td>
              <td>${escapeHtml(row.note)}</td>
            </tr>
          `,
        )
        .join("")
    : `
      <tr>
        <td colspan="3" class="empty-cell">No recent audit entries.</td>
      </tr>
    `;

  return `
    <html>
      <head>
        <title>Employee Attendance</title>
        <style>
    ${buildThemeCss()}   ${buildBasePageCss()}

          .wrap {
            max-width: 1380px;
            margin: 0 auto;
            padding: 24px 18px 36px;
          }

.topbar, .panel, .card {
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  border-radius: var(--radius-lg);
box-shadow: var(--shadow-soft), 0 0 18px color-mix(in srgb, var(--primary) 18%, transparent);
}

          .topbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            flex-wrap: wrap;
            margin-bottom: 20px;
            padding: 18px 20px;
          }

.eyebrow {
  font-size: 11px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--primary);
  font-weight: 700;
  margin-bottom: 8px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}

          h1 {
            margin: 0;
            font-size: 30px;
            letter-spacing: -0.04em;
          }

          .subtitle {
            color: var(--muted);
            margin-top: 8px;
            font-size: 14px;
          }

          .links {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
          }

.links a {
  color: var(--text);
  text-decoration: none;
  padding: 10px 14px;
  border-radius: 12px;
  border: 1px solid color-mix(in srgb, var(--secondary) 30%, transparent);
  background: var(--secondary-soft);
  font-weight: 600;
}

.links a:hover {
  color: var(--text-strong);
  border-color: color-mix(in srgb, var(--secondary) 55%, transparent);
}

          .cards {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 16px;
            margin-bottom: 18px;
          }

          .card {
            padding: 16px;
          }

          .card-label {
            color: var(--muted);
            font-size: 12px;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            font-weight: 700;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

.card h2 {
  margin: 10px 0 0;
  font-size: 28px;
  color: var(--accent-2);
}

          .grid-2 {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 18px;
            margin-bottom: 18px;
          }

          .panel {
            padding: 18px;
            margin-bottom: 18px;
          }

          h2 {
            margin: 0 0 12px;
            font-size: 19px;
          }

          .kv {
            display: grid;
            grid-template-columns: 220px 1fr;
            gap: 10px;
            row-gap: 12px;
          }

          .kv .k {
            color: var(--muted);
          }

.table-wrap {
  overflow-x: auto;
  border: 1px solid var(--line);
  border-radius: var(--radius-md);
  background: rgba(255,255,255,0.03);
}

th, td {
  padding: 12px;
  border-bottom: 1px solid rgba(255,255,255,0.06);
  text-align: left;
  vertical-align: top;
}

th {
  color: var(--muted);
  font-size: 11px;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  background: rgba(255,255,255,0.04);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}

          .empty-cell {
            text-align: center;
            color: var(--muted);
            padding: 18px;
          }

          @media (max-width: 1000px) {
            .cards, .grid-2 { grid-template-columns: 1fr; }
            .kv { grid-template-columns: 1fr; }
          }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Employee Attendance Detail</div>
              <h1>${escapeHtml(employee.name || "Employee")}</h1>
<div class="subtitle">
  ${escapeHtml(employee.role || "-")} • ${escapeHtml(employee.phone_number || "-")}
</div>
              <a href="/reports?userId=${employee.id}" class="btn-secondary">Today</a>
<a href="/reports?userId=${employee.id}&days=7" class="btn-secondary">Last 7 days</a>
            </div>
            <div class="links">
              <a href="/attendance">Attendance</a>
              <a href="/dashboard">Dashboard</a>
              <a href="/tasks">Tasks</a>
              <a href="/logs">Logs</a>
              <a href="/bugs">Bug Board</a>
              <a href="/reports">Reports</a>
            </div>
          </div>

          <div class="cards">
            <div class="card"><div class="card-label">Current Status</div><h2>${escapeHtml(today.current_status || "-")}</h2></div>
            <div class="card"><div class="card-label">Worked Today</div><h2>${escapeHtml(today.worked_text || "-")}</h2></div>
            <div class="card"><div class="card-label">Break Today</div><h2>${escapeHtml(today.break_text || "-")}</h2></div>
            <div class="card"><div class="card-label">Late Today</div><h2>${escapeHtml(today.late_text || "No")}</h2></div>
          </div>

          <div class="grid-2">
            <div class="panel">
              <h2>Today details</h2>
              <div class="kv">
                <div class="k">Attendance date</div><div>${escapeHtml(today.attendance_date || "-")}</div>
                <div class="k">First login</div><div>${escapeHtml(today.first_login_text || "-")}</div>
                <div class="k">Last logout</div><div>${escapeHtml(today.last_logout_text || "-")}</div>
                <div class="k">Break count</div><div>${escapeHtml(String(today.break_count || 0))}</div>
                <div class="k">Late status</div><div>${escapeHtml(today.late_status || "-")}</div>
                <div class="k">Leave today</div><div>${today.leave_today ? "Yes" : "No"}</div>
                <div class="k">Flags</div>
                <div>
                  ${
                    [
                      today.long_shift_flag ? "Long shift" : null,
                      today.long_break_flag ? "Long break" : null,
                      today.possible_half_day ? "Half day" : null,
                    ]
                      .filter(Boolean)
                      .join(", ") || "None"
                  }
                </div>
              </div>
            </div>

            <div class="panel">
              <h2>Monthly summary</h2>
              <div class="kv">
                <div class="k">Present days</div><div>${escapeHtml(String(monthly.presentDays || 0))}</div>
                <div class="k">Total leave entries</div><div>${escapeHtml(String(monthly.leaveDays || 0))}</div>
                <div class="k">Past leave dates</div><div>${escapeHtml(formatDateListForHumans(monthly.pastLeaveDates || []))}</div>
                <div class="k">Upcoming leave dates</div><div>${escapeHtml(formatDateListForHumans(monthly.upcomingLeaveDates || []))}</div>
                <div class="k">Late joins</div><div>${escapeHtml(String(monthly.lateJoins || 0))}</div>
                <div class="k">Approved late</div><div>${escapeHtml(String(monthly.approvedLate || 0))}</div>
                <div class="k">Late not approved</div><div>${escapeHtml(String(monthly.unapprovedLate || 0))}</div>
                <div class="k">Late without prior info</div><div>${escapeHtml(String(monthly.uninformedLate || 0))}</div>
                <div class="k">Average login time</div><div>${escapeHtml(monthly.avgLoginTimeText || "-")}</div>
                <div class="k">Average break time</div><div>${escapeHtml(formatDurationMinutes(monthly.avgBreakMin || 0))}</div>
                <div class="k">Long shift flags</div><div>${escapeHtml(String(monthly.longShiftCount || 0))}</div>
                <div class="k">Long break flags</div><div>${escapeHtml(String(monthly.longBreakCount || 0))}</div>
                <div class="k">Possible half days</div><div>${escapeHtml(String(monthly.possibleHalfDays || 0))}</div>
                <div class="k">Manager corrections</div><div>${escapeHtml(String(monthly.managerCorrectionCount || 0))}</div>
              </div>
            </div>
          </div>

          <div class="panel">
            <h2>Today timeline</h2>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Action</th>
                    <th>Expected Duration</th>
                    <th>Reason</th>
                    <th>Note</th>
                  </tr>
                </thead>
                <tbody>${todayTimelineRows}</tbody>
              </table>
            </div>
          </div>

          <div class="panel">
            <h2>Attendance history this month</h2>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>Status</th>
                    <th>Login</th>
                    <th>Logout</th>
                    <th>Worked</th>
                    <th>Break</th>
                    <th>Late</th>
                    <th>Late status</th>
                    <th>Leave</th>
                    <th>Flags</th>
                    <th>Corrections</th>
                  </tr>
                </thead>
                <tbody>${historyRows}</tbody>
              </table>
            </div>
          </div>

          <div class="panel">
            <h2>Recent attendance audit</h2>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Action Type</th>
                    <th>Note</th>
                  </tr>
                </thead>
                <tbody>${auditRows}</tbody>
              </table>
            </div>
          </div>
        </div>

        <script>
loadUsers().then(loadTasks);

        </script>
      </body>
    </html>
  `;
}

async function getDashboardData(orgId) {
  const todayAttendanceDate = getCurrentAttendanceDayRange().attendanceDate;

  const [openTasksResult, overdueResult, blockedResult, attendanceRows] =
    await Promise.all([
      supabase
        .from("tasks")
        .select(
          `
          id,
          org_id,
          task_no,
          title,
          priority,
          status,
          deadline,
          blocker_note
        `,
        )
        .eq("org_id", orgId)
        .or("status.is.null,status.not.in.(done,archived,cancelled,deleted)")
        .order("deadline", { ascending: true, nullsFirst: false })
        .limit(100),

      supabase
        .from("tasks")
        .select(
          `
          id,
          org_id,
          task_no,
          title,
          priority,
          status,
          deadline,
          blocker_note
        `,
        )
        .eq("org_id", orgId)
        .lt("deadline", todayAttendanceDate)
        .or("status.is.null,status.not.in.(done,archived,cancelled,deleted)")
        .order("deadline", { ascending: true })
        .limit(100),

      supabase
        .from("tasks")
        .select(
          `
          id,
          org_id,
          task_no,
          title,
          priority,
          status,
          deadline,
          blocker_note
        `,
        )
        .eq("org_id", orgId)
        .not("blocker_note", "is", null)
        .or("status.is.null,status.not.in.(done,archived,cancelled,deleted)")
        .order("deadline", { ascending: true, nullsFirst: false })
        .limit(100),

      getLatestAttendanceByUser(orgId),
    ]);

  if (openTasksResult.error) throw openTasksResult.error;
  if (overdueResult.error) throw overdueResult.error;
  if (blockedResult.error) throw blockedResult.error;

  const openTasks = openTasksResult.data || [];
  const overdueTasks = overdueResult.data || [];
  const blockedTasks = blockedResult.data || [];
  const attendance = attendanceRows || [];

  const allTasks = [...openTasks, ...overdueTasks, ...blockedTasks];
  const uniqueTaskIds = [...new Set(allTasks.map((t) => t.id).filter(Boolean))];

  let ownersByTaskId = {};

  if (uniqueTaskIds.length) {
    const { data: ownerRows, error: ownerError } = await supabase
      .from("task_owners")
      .select(
        `
        task_id,
        user_id,
        users!task_owners_user_id_fkey(id, name)
      `,
      )
      .eq("org_id", orgId)
      .in("task_id", uniqueTaskIds);

    if (ownerError) {
      console.error("getDashboardData task_owners error:", ownerError);
      throw ownerError;
    }

    for (const row of ownerRows || []) {
      if (!ownersByTaskId[row.task_id]) ownersByTaskId[row.task_id] = [];
      ownersByTaskId[row.task_id].push(row.users?.name || "Unknown");
    }
  }

  function attachOwners(task) {
    return {
      ...task,
      owner_names: ownersByTaskId[task.id] || [],
      assignee_name: (ownersByTaskId[task.id] || []).join(", ") || "Unknown",
    };
  }

  return {
    summary: {
      open_tasks: openTasks.length,
      overdue_tasks: overdueTasks.length,
      blocked_tasks: blockedTasks.length,
      active_today_count: attendance.filter((x) => x.has_login_today).length,
    },
    open_tasks: openTasks.map(attachOwners),
    overdue_tasks: overdueTasks.map(attachOwners),
    blocked_tasks: blockedTasks.map(attachOwners),
    attendance,
  };
}

app.use("/api", requireDashboardAuth);

function renderDashboardPage(data) {
  const summary = data?.summary || {};
  const attendance = data?.attendance || [];
  const openTasks = data?.open_tasks || [];
  const blockedTasks = data?.blocked_tasks || [];
  const overdueTasks = data?.overdue_tasks || [];

  const onBreakCount = attendance.filter((x) => x.status === "break").length;
  const plannedOffCount = attendance.filter(
    (x) => x.status === "planned_off",
  ).length;
  const noLoginCount = attendance.filter((x) => x.status === "no_login").length;
  const teamCount = attendance.length;

  const summaryCards = [
    {
      label: "Open Tasks",
      value: summary.open_tasks ?? openTasks.length ?? 0,
      note: "Active work requiring ownership",
      cardClass: "info",
    },
    {
      label: "Overdue",
      value: summary.overdue_tasks ?? overdueTasks.length ?? 0,
      note: "Past deadline and needing action",
      cardClass: "danger",
    },
    {
      label: "Blocked",
      value: summary.blocked_tasks ?? blockedTasks.length ?? 0,
      note: "Waiting on help or dependency",
      cardClass: "warn",
    },
    {
      label: "Active Today",
      value: summary.active_today_count ?? 0,
      note: "Logged in at least once today",
      cardClass: "success",
    },
    {
      label: "On Break",
      value: onBreakCount,
      note: "Live break status snapshot",
      cardClass: "muted",
    },
    {
      label: "Planned Off",
      value: plannedOffCount,
      note: "Approved off / leave state",
      cardClass: "cyan",
    },
    {
      label: "No Login",
      value: noLoginCount,
      note: "No attendance marked yet",
      cardClass: "danger",
    },
    {
      label: "Team Size",
      value: teamCount,
      note: "Active people in dashboard",
      cardClass: "info",
    },
  ];

  const summaryCardsHtml = summaryCards
    .map(
      (card) => `
        <div class="stat-card ${card.cardClass}">
          <div class="stat-label">${escapeHtml(card.label)}</div>
          <div class="stat-value">${escapeHtml(card.value)}</div>
          <div class="stat-note">${escapeHtml(card.note)}</div>
        </div>
      `,
    )
    .join("");

  const attendanceRows = attendance.length
    ? attendance
        .map((row) => {
          const statusClass = badgeClass(row.status || "unknown");
          return `
            <tr>
              <td><div class="primary-text">${escapeHtml(row.name || "-")}</div></td>
              <td><span class="muted">${escapeHtml(row.role || "-")}</span></td>
              <td><span class="${statusClass}">${escapeHtml(row.status || "unknown")}</span></td>
              <td>${escapeHtml(row.break_duration_text || row.breakDurationText || "-")}</td>
              <td>${escapeHtml(row.worked_today_text || row.workedTodayText || "-")}</td>
              <td>${escapeHtml(row.last_action_at ? formatTimeOnly(row.last_action_at) : "-")}</td>
            </tr>
          `;
        })
        .join("")
    : `<tr><td colspan="6" class="empty-cell">No attendance data found</td></tr>`;

  const blockedTaskRows = blockedTasks.length
    ? blockedTasks
        .map(
          (task) => `
            <tr class="task-row-blocked">
              <td>#${escapeHtml(task.task_no || task.id)}</td>
              <td>${escapeHtml(task.title || "-")}</td>
              <td>${escapeHtml(task.assignee_name || "-")}</td>
              <td>${escapeHtml(task.priority || "-")}</td>
              <td>${escapeHtml(task.deadline || "-")}</td>
              <td>${escapeHtml(task.blocker_note || "-")}</td>
            </tr>
          `,
        )
        .join("")
    : `<tr><td colspan="6" class="empty-cell">No blocked tasks</td></tr>`;

  const overdueTaskRows = overdueTasks.length
    ? overdueTasks
        .map(
          (task) => `
            <tr class="task-row-overdue">
              <td>#${escapeHtml(task.task_no || task.id)}</td>
              <td>${escapeHtml(task.title || "-")}</td>
              <td>${escapeHtml(task.assignee_name || "-")}</td>
              <td>${escapeHtml(task.priority || "-")}</td>
              <td>${escapeHtml(task.deadline || "-")}</td>
              <td>${escapeHtml(task.status || "-")}</td>
            </tr>
          `,
        )
        .join("")
    : `<tr><td colspan="6" class="empty-cell">No overdue tasks</td></tr>`;

  return `
    <html>
      <head>
        <title>WeSolveHR Dashboard</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}

          .page {
            max-width: 1480px;
            margin: 0 auto;
            padding: 24px 18px 36px;
          }

          .topbar, .panel, .stat-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
          }

          .topbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            flex-wrap: wrap;
            margin-bottom: 20px;
            padding: 18px 20px;
          }

          .eyebrow {
            font-size: 11px;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            color: var(--primary);
            font-weight: 700;
            margin-bottom: 8px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

          h1 {
            margin: 0;
            font-size: 30px;
            letter-spacing: -0.04em;
          }

          .subtitle {
            color: var(--muted);
            margin-top: 8px;
            font-size: 14px;
          }

          .links {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
          }

          .links a {
            color: var(--text);
            text-decoration: none;
            padding: 10px 14px;
            border-radius: 12px;
            border: 1px solid color-mix(in srgb, var(--secondary) 30%, transparent);
            background: var(--secondary-soft);
            font-weight: 600;
          }

          .stats {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 12px;
            margin-bottom: 20px;
          }

          .stat-card {
            padding: 14px;
          }

          .stat-label {
            color: var(--muted);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 700;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

          .stat-value {
            margin-top: 10px;
            font-size: 28px;
            font-weight: 700;
          }

          .stat-note {
            margin-top: 8px;
            color: var(--muted);
            font-size: 13px;
          }

          .panel {
            padding: 16px;
            margin-bottom: 18px;
          }

          .panel h2 {
            margin: 0 0 12px 0;
            font-size: 18px;
          }

          table {
            width: 100%;
            border-collapse: collapse;
          }

          th, td {
            text-align: left;
            padding: 12px 10px;
            border-bottom: 1px solid rgba(255,255,255,0.08);
            vertical-align: top;
          }

          th {
            color: var(--muted);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
          }

          .grid-2 {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 18px;
          }

          .primary-text {
            font-weight: 600;
          }
        </style>
      </head>
      <body>
        <div class="page">
          <div class="topbar">
            <div>
              <div class="eyebrow">WeSolveHR // Live Operations</div>
              <h1>Dashboard</h1>
              <div class="subtitle">Tasks, attendance, blockers, and operating visibility</div>
            </div>
            <div class="links">
              <a href="/dashboard">Dashboard</a>
              <a href="/tasks">Tasks</a>
              <a href="/attendance">Attendance</a>
              <a href="/logs">Logs</a>
              <a href="/bugs">Bug Board</a>
              <a href="/reports">Reports</a>
            </div>
          </div>

          <div class="stats">
            ${summaryCardsHtml}
          </div>

          <div class="panel">
            <h2>Current Attendance</h2>
            <table>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Role</th>
                  <th>Status</th>
                  <th>Break</th>
                  <th>Worked Today</th>
                  <th>Last Activity</th>
                </tr>
              </thead>
              <tbody>
                ${attendanceRows}
              </tbody>
            </table>
          </div>

          <div class="grid-2">
            <div class="panel">
              <h2>Blocked Tasks</h2>
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Title</th>
                    <th>Owner</th>
                    <th>Priority</th>
                    <th>Deadline</th>
                    <th>Blocker</th>
                  </tr>
                </thead>
                <tbody>
                  ${blockedTaskRows}
                </tbody>
              </table>
            </div>

            <div class="panel">
              <h2>Overdue Tasks</h2>
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Title</th>
                    <th>Owner</th>
                    <th>Priority</th>
                    <th>Deadline</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  ${overdueTaskRows}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </body>
    </html>
  `;
}

app.get("/health/live", (_req, res) => {
  return res.status(200).json({ ok: true, status: "live" });
});

app.get("/reports", requireDashboardAuth, async (req, res) => {
  try {
    const userId = req.query.userId ? Number(req.query.userId) : null;
    const days = req.query.days ? Number(req.query.days) : 1;
    const reportDate =
      String(req.query.date || "").trim() || getReportDateString();

    if (userId && days > 1) {
      const data = await getMultiDayNarrativeReport({
        orgId: DASHBOARD_ORG_ID,
        userId,
        days,
        endDate: reportDate,
      });

      return res.status(200).send(renderMultiDayUserReportsPage(data));
    }

    const data = await getDailyNarrativeReport({
      orgId: DASHBOARD_ORG_ID,
      reportDate,
      userId,
    });

    return res.status(200).send(renderReportsPage(data));
  } catch (error) {
    console.error("Reports page error:", error);
    return res.status(500).send("Failed to load reports page");
  }
});

app.get("/api/reports/task/:taskNo", requireDashboardAuth, async (req, res) => {
  try {
    const taskNo = Number(req.params.taskNo);
    if (!taskNo) {
      return sendApiError(res, 400, "Invalid task number");
    }

    const { task, error } = await getTaskById(taskNo, DASHBOARD_ORG_ID);

    if (error) {
      console.error("Report task detail fetch error:", error);
      return sendApiError(res, 500, "Failed to fetch task");
    }

    if (!task) {
      return sendApiError(res, 404, "Task not found");
    }

    const ownerNames = await getTaskOwnerNames(task.id, DASHBOARD_ORG_ID);

    const { data: historyRows, error: historyError } = await supabase
      .from("task_history")
      .select(
        `
        id,
        task_id,
        changed_by_user_id,
        change_type,
        field_name,
        old_value,
        new_value,
        created_at
      `,
      )
      .eq("org_id", DASHBOARD_ORG_ID)
      .eq("task_id", task.id)
      .order("created_at", { ascending: false })
      .limit(15);

    if (historyError) {
      console.error("Report task history fetch error:", historyError);
      return sendApiError(res, 500, "Failed to fetch task history");
    }

    const changedByIds = [
      ...new Set(
        (historyRows || []).map((x) => x.changed_by_user_id).filter(Boolean),
      ),
    ];
    let userMap = new Map();

    if (changedByIds.length) {
      const { data: userRows } = await supabase
        .from("users")
        .select("id, name")
        .eq("org_id", DASHBOARD_ORG_ID)
        .in("id", changedByIds);

      userMap = new Map((userRows || []).map((u) => [u.id, u.name]));
    }

    const history = (historyRows || []).map((row) => ({
      id: row.id,
      at: formatDateTime(row.created_at),
      by:
        userMap.get(row.changed_by_user_id) ||
        `User ${row.changed_by_user_id || "-"}`,
      changeType: row.change_type,
      fieldName: row.field_name,
      oldValue: row.old_value || {},
      newValue: row.new_value || {},
    }));

    return sendApiSuccess(res, {
      id: task.id,
      taskNo: task.task_no || task.id,
      title: task.title,
      detail: task.detail,
      status: task.status,
      priority: task.priority,
      progress: task.progress,
      deadline: task.deadline,
      blockerNote: task.blocker_note,
      business: task.business,
      area: task.area,
      owners: ownerNames,
      history,
    });
  } catch (error) {
    console.error("Report task detail fatal error:", error);
    return sendApiError(res, 500, "Failed to fetch task detail");
  }
});

app.get("/health/ready", async (_req, res) => {
  try {
    const { error } = await supabase.from("users").select("id").limit(1);
    if (error) {
      return res
        .status(500)
        .json({ ok: false, status: "db_error", error: error.message });
    }

    return res.status(200).json({
      ok: true,
      status: "ready",
      openai: !!process.env.OPENAI_API_KEY,
      twilioAuth: !!process.env.TWILIO_AUTH_TOKEN,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      status: "error",
      error: error?.message || String(error),
    });
  }
});

app.get("/attendance/:userId", requireDashboardAuth, async (req, res) => {
  try {
    const userId = Number(req.params.userId);
    if (!userId) {
      return res.status(400).send("Invalid user id");
    }

    const data = await getEmployeeAttendanceOverview(userId, DASHBOARD_ORG_ID);
    return res.status(200).send(renderEmployeeAttendancePage(data));
  } catch (error) {
    console.error("Employee attendance page error:", error);
    return res.status(500).send(`
      <html>
        <head>
          <title>Employee Attendance Error</title>
          <style>
            body {
              font-family: Arial, sans-serif;
              background: #0f172a;
              color: white;
              padding: 40px;
            }
            .box {
              max-width: 800px;
              margin: 0 auto;
              padding: 24px;
              border-radius: 16px;
              background: rgba(255,255,255,0.06);
              border: 1px solid rgba(255,255,255,0.1);
            }
            pre {
              white-space: pre-wrap;
              word-break: break-word;
              color: #fca5a5;
            }
            a { color: #93c5fd; }
          </style>
        </head>
        <body>
          <div class="box">
            <h1>Employee attendance failed to load</h1>
            <pre>${escapeHtml(error?.message || String(error))}</pre>
            <p><a href="/attendance">Back to attendance</a></p>
          </div>
        </body>
      </html>
    `);
  }
});

async function getDashboardSummaryData(orgId) {
  const { startUtc, endUtc, attendanceDate } = getCurrentAttendanceDayRange();
  const today = attendanceDate;

  const [
    openTasksResult,
    overdueTasksResult,
    blockedTasksResult,
    activeTodayResult,
    usersResult,
    recentAttendanceResult,
  ] = await Promise.all([
    supabase
      .from("tasks")
      .select("*", { count: "exact", head: true })
      .eq("org_id", orgId)
      .not("status", "in", '("done","archived","cancelled")'),

    supabase
      .from("tasks")
      .select("*", { count: "exact", head: true })
      .eq("org_id", orgId)
      .lt("deadline", today)
      .not("status", "in", '("done","archived","cancelled")'),

    supabase
      .from("tasks")
      .select("*", { count: "exact", head: true })
      .eq("org_id", orgId)
      .eq("status", "blocked"),

    supabase
      .from("attendance_events")
      .select("user_id", { count: "exact" })
      .eq("org_id", orgId)
      .gte("created_at", startUtc)
      .lt("created_at", endUtc),

    supabase
      .from("users")
      .select("id, name, role")
      .eq("org_id", orgId)
      .eq("is_active", true)
      .order("name", { ascending: true }),

    supabase
      .from("attendance_events")
      .select("user_id, action, created_at")
      .eq("org_id", orgId)
      .order("created_at", { ascending: false })
      .limit(300),
  ]);

  if (openTasksResult.error) throw openTasksResult.error;
  if (overdueTasksResult.error) throw overdueTasksResult.error;
  if (blockedTasksResult.error) throw blockedTasksResult.error;
  if (activeTodayResult.error) throw activeTodayResult.error;
  if (usersResult.error) throw usersResult.error;
  if (recentAttendanceResult.error) throw recentAttendanceResult.error;

  const latestByUser = new Map();
  for (const row of recentAttendanceResult.data || []) {
    if (!latestByUser.has(row.user_id)) {
      latestByUser.set(row.user_id, row);
    }
  }

  let onBreakNow = 0;
  for (const row of latestByUser.values()) {
    if (row.action === "break") onBreakNow += 1;
  }

  return {
    open_tasks: openTasksResult.count || 0,
    overdue_tasks: overdueTasksResult.count || 0,
    blocked_tasks: blockedTasksResult.count || 0,
    active_users_today: new Set(
      (activeTodayResult.data || []).map((x) => x.user_id),
    ).size,
    on_break_now: onBreakNow,
  };
}

async function getAttendancePageData(orgId) {
  const { startUtc, endUtc } = getCurrentAttendanceDayRange();
  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());

  const [
    { data: users, error: usersError },
    { data: events, error: eventsError },
    plannedOffRows,
  ] = await Promise.all([
    supabase
      .from("users")
      .select("id, name, role")
      .eq("org_id", orgId)
      .eq("is_active", true)
      .order("name", { ascending: true }),

    supabase
      .from("attendance_events")
      .select(
        "id, user_id, action, duration_min, expected_duration_min, reason, note, created_at",
      )
      .eq("org_id", orgId)
      .gte("created_at", startUtc)
      .lt("created_at", endUtc)
      .order("created_at", { ascending: false }),

    getPlannedOffRowsForDate(attendanceDate, orgId),
  ]);

  if (usersError) throw usersError;
  if (eventsError) throw eventsError;

  const plannedOffUserIds = new Set(
    (plannedOffRows || []).map((x) => x.user_id),
  );

  const latestByUser = new Map();
  for (const event of events || []) {
    if (!latestByUser.has(event.user_id)) {
      latestByUser.set(event.user_id, event);
    }
  }

  const currentStatus = (users || []).map((user) => {
    const latest = latestByUser.get(user.id);
    const status = plannedOffUserIds.has(user.id)
      ? "leave"
      : latest?.action || "unknown";

    return {
      user_id: user.id,
      name: user.name,
      role: user.role,
      status,
      last_event_at: latest?.created_at || null,
      last_event_at_text: latest?.created_at
        ? formatDateTime(latest.created_at)
        : plannedOffUserIds.has(user.id)
          ? "On leave today"
          : "-",
    };
  });

  const activeTodayUserIds = new Set((events || []).map((e) => e.user_id));

  const loggedInCount = currentStatus.filter(
    (x) => x.status === "login" || x.status === "back",
  ).length;

  const onBreakCount = currentStatus.filter((x) => x.status === "break").length;

  return {
    summary: {
      logged_in_count: loggedInCount,
      on_break_count: onBreakCount,
      active_today_count: activeTodayUserIds.size,
    },
    current_status: currentStatus,
    recent_events: (events || []).slice(0, 50).map((row) => ({
      ...row,
      created_at_text: row.created_at ? formatDateTime(row.created_at) : "-",
    })),
  };
}

async function getTasksPageData(filters = {}, orgId) {
  const search = String(filters.search || "").trim();
  const assignee = String(filters.assignee || "").trim();
  const business = String(filters.business || "")
    .trim()
    .toLowerCase();
  const area = String(filters.area || "")
    .trim()
    .toLowerCase();
  const status = String(filters.status || "").trim();
  const priority = String(filters.priority || "").trim();
  const blocked = String(filters.blocked || "") === "true";
  const overdue = String(filters.overdue || "") === "true";

  let query = supabase
    .from("tasks")
    .select(
      `
      id,
      org_id,
      task_no,
      title,
      business,
      area,
      status,
      progress,
      priority,
      deadline,
      blocker_note
      `,
    )
    .eq("org_id", orgId)
    .order("deadline", { ascending: true, nullsFirst: false });

  if (priority) query = query.eq("priority", priority);
  if (business) query = query.eq("business", business);
  if (area) query = query.eq("area", area);

  if (blocked) {
    query = query.eq("status", "blocked");
  } else if (status) {
    query = query.eq("status", status);
  }

  if (overdue) {
    const today = new Date().toISOString().slice(0, 10);
    query = query
      .lt("deadline", today)
      .not("status", "in", '("done","archived","cancelled")');
  }

  if (search) {
    if (/^\d+$/.test(search)) {
      query = query.or(
        `task_no.eq.${Number(search)},id.eq.${Number(search)},title.ilike.%${search}%`,
      );
    } else {
      query = query.ilike("title", `%${search}%`);
    }
  }

  const { data: tasks, error } = await query;

  if (error) {
    console.error("getTasksPageData error:", error);
    throw error;
  }

  if (!tasks || !tasks.length) return [];

  const taskIds = tasks.map((t) => t.id);

  const { data: ownerRows, error: ownerError } = await supabase
    .from("task_owners")
    .select(
      `
    task_id,
    user_id,
    users!task_owners_user_id_fkey(id, name)
    `,
    )
    .eq("org_id", orgId)
    .in("task_id", taskIds);

  if (ownerError) {
    console.error("getTasksPageData task_owners error:", ownerError);
    throw ownerError;
  }

  const ownersByTaskId = {};
  for (const row of ownerRows || []) {
    if (!ownersByTaskId[row.task_id]) ownersByTaskId[row.task_id] = [];
    ownersByTaskId[row.task_id].push({
      user_id: row.user_id,
      name: row.users?.name || "",
    });
  }

  let rows = tasks.map((task) => {
    const owners = ownersByTaskId[task.id] || [];
    return {
      ...task,
      owner_names: owners.map((x) => x.name).filter(Boolean),
      assignee_name: owners
        .map((x) => x.name)
        .filter(Boolean)
        .join(", "),
    };
  });

  if (assignee) {
    rows = rows.filter((task) =>
      (ownersByTaskId[task.id] || []).some(
        (owner) => String(owner.user_id) === assignee,
      ),
    );
  }

  return rows;
}

async function getTaskDetailData(taskId, orgId) {
  const { data: task, error: taskError } = await supabase
    .from("tasks")
    .select(
      `
      id,
      org_id,
      task_no,
      title,
      detail,
      priority,
      status,
      progress,
      deadline,
      blocker_note,
      business,
      area,
      created_by_user_id,
      last_updated_by_user_id,
      created_at,
      updated_at
    `,
    )
    .eq("id", taskId)
    .eq("org_id", orgId)
    .maybeSingle();

  if (taskError) throw taskError;
  if (!task) return null;

  const { data: ownerRows, error: ownerError } = await supabase
    .from("task_owners")
    .select(
      `
      task_id,
      user_id,
      users!task_owners_user_id_fkey(id, name)
    `,
    )
    .eq("task_id", taskId)
    .eq("org_id", orgId);

  if (ownerError) throw ownerError;

  const ownerNames = (ownerRows || [])
    .map((row) => row.users?.name)
    .filter(Boolean);

  const ownerIds = (ownerRows || []).map((row) => row.user_id).filter(Boolean);

  const { data: history, error: historyError } = await supabase
    .from("task_history")
    .select(
      `
      id,
      change_type,
      field_name,
      old_value,
      new_value,
      created_at,
      changed_by_user_id,
      changer:users!task_history_changed_by_user_id_fkey(name)
    `,
    )
    .eq("task_id", taskId)
    .eq("org_id", orgId)
    .order("created_at", { ascending: false });

  if (historyError) throw historyError;

  return {
    id: task.id,
    org_id: task.org_id,
    task_no: task.task_no,
    title: task.title,
    detail: task.detail,
    owner_names: ownerNames,
    assignee_name: ownerNames.join(", ") || "Unknown",
    priority: task.priority,
    status: task.status,
    progress: task.progress,
    deadline: task.deadline,
    blocker_note: task.blocker_note,
    business: task.business,
    area: task.area,
    owner_user_ids: ownerIds,
    created_by_user_id: task.created_by_user_id,
    last_updated_by_user_id: task.last_updated_by_user_id,
    created_at: task.created_at,
    updated_at: task.updated_at,
    task_history: (history || []).map((item) => ({
      ...item,
      changed_by_name: item.changer?.name || "Unknown",
      note:
        item?.new_value?.note ||
        item?.new_value?.blocker_note ||
        item?.old_value?.note ||
        item?.old_value?.blocker_note ||
        null,
    })),
  };
}

async function getLogsPageData(orgId) {
  const { data, error } = await supabase
    .from("message_logs")
    .select(
      `
      id,
      org_id,
      user_id,
      phone_number,
      profile_name,
      message_text,
      twilio_message_sid,
      created_at,
      direction
    `,
    )
    .eq("org_id", orgId)
    .eq("direction", "inbound")
    .order("created_at", { ascending: false })
    .limit(100);

  if (error) throw error;

  return (data || []).map((row) => ({
    id: row.id,
    sender: row.profile_name || row.phone_number || "Unknown",
    body: row.message_text,
    message_sid: row.twilio_message_sid,
    created_at: row.created_at,
    created_at_text: row.created_at ? formatDateTime(row.created_at) : "-",
  }));
}

async function getStage0BugBoardData(orgId) {
  const { data, error } = await supabase
    .from("stage0_bug_board")
    .select(
      `
      id,
      org_id,
      title,
      description,
      board_column,
      severity,
      status,
      source_message_sid,
      source_phone_number,
      source_message_text,
      created_by_user_id,
      assigned_to_user_id,
      created_at,
      updated_at,
      creator:users!stage0_bug_board_created_by_user_id_fkey(name),
      assignee:users!stage0_bug_board_assigned_to_user_id_fkey(name)
    `,
    )
    .eq("org_id", orgId)
    .order("created_at", { ascending: false });

  if (error) throw error;

  const rows = (data || []).map((row) => ({
    id: row.id,
    title: row.title || "",
    description: row.description || "",
    board_column: row.board_column || "Unknown",
    severity: row.severity || "P2",
    status: row.status || "open",
    source_message_sid: row.source_message_sid || "",
    source_phone_number: row.source_phone_number || "",
    source_message_text: row.source_message_text || "",
    created_by_name: row.creator?.name || "-",
    assigned_to_name: row.assignee?.name || "-",
    assigned_to_user_id: row.assigned_to_user_id || null,
    created_at: row.created_at,
    updated_at: row.updated_at,
    created_at_text: row.created_at ? formatDateTime(row.created_at) : "-",
    updated_at_text: row.updated_at ? formatDateTime(row.updated_at) : "-",
  }));

  const grouped = {};
  for (const column of STAGE0_BUG_COLUMNS) grouped[column] = [];

  for (const row of rows) {
    if (!grouped[row.board_column]) grouped[row.board_column] = [];
    grouped[row.board_column].push(row);
  }

  return {
    summary: {
      total: rows.length,
      p0: rows.filter((x) => x.severity === "P0").length,
      p1: rows.filter((x) => x.severity === "P1").length,
      p2: rows.filter((x) => x.severity === "P2").length,
      open: rows.filter((x) => x.status === "open").length,
      in_progress: rows.filter((x) => x.status === "in_progress").length,
      blocked: rows.filter((x) => x.status === "blocked").length,
    },
    columns: STAGE0_BUG_COLUMNS.map((name) => ({
      name,
      count: (grouped[name] || []).length,
      items: (grouped[name] || []).sort((a, b) => {
        if (
          bugSeveritySortWeight(a.severity) !==
          bugSeveritySortWeight(b.severity)
        ) {
          return (
            bugSeveritySortWeight(a.severity) -
            bugSeveritySortWeight(b.severity)
          );
        }
        return new Date(b.created_at || 0) - new Date(a.created_at || 0);
      }),
    })),
  };
}

app.get("/", (_req, res) => {
  res.type("html").send(`
    <html>
      <head>
        <title>WeSolveHR</title>
        <style>
          body {
            font-family: Arial, sans-serif;
            background: #0f172a;
            color: white;
            display: grid;
            place-items: center;
            height: 100vh;
            margin: 0;
          }
          .box {
            text-align: center;
            padding: 32px;
            border-radius: 16px;
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.1);
          }
          a {
            display: inline-block;
            margin-top: 16px;
            color: white;
            text-decoration: none;
            padding: 10px 16px;
            border-radius: 10px;
            background: #2563eb;
          }
        </style>
      </head>
      <body>
        <div class="box">
          <h1>WeSolveHR Server</h1>
          <p>Webhook + Dashboard is running.</p>
          <a href="/dashboard">Open Dashboard</a>
        </div>
      </body>
    </html>
  `);
});

app.get("/dashboard", requireDashboardAuth, async (_req, res) => {
  try {
    const data = await getDashboardData(DASHBOARD_ORG_ID);
    res.type("html").send(renderDashboardPage(data));
  } catch (error) {
    console.error("Dashboard error:", error);
    res.status(500).type("html").send(`
      <html>
        <head>
          <title>Dashboard Error</title>
          <style>
            body {
              font-family: Arial, sans-serif;
              background: #0f172a;
              color: white;
              padding: 40px;
            }
            .box {
              max-width: 800px;
              margin: 0 auto;
              padding: 24px;
              border-radius: 16px;
              background: rgba(255,255,255,0.06);
              border: 1px solid rgba(255,255,255,0.1);
            }
            pre {
              white-space: pre-wrap;
              word-break: break-word;
              color: #fca5a5;
            }
            a { color: #93c5fd; }
          </style>
        </head>
        <body>
          <div class="box">
            <h1>Dashboard failed to load</h1>
            <p>Check server logs and the details below.</p>
            <pre>${escapeHtml(error?.message || String(error))}</pre>
            <p><a href="/dashboard">Try again</a></p>
          </div>
        </body>
      </html>
    `);
  }
});

app.get("/bugs", requireDashboardAuth, async (_req, res) => {
  try {
    const data = await getStage0BugBoardData(DASHBOARD_ORG_ID);
    res.status(200).type("html").send(renderStage0BugBoardPage(data));
  } catch (error) {
    console.error("Bug board page error:", error);
    res.status(500).type("html").send(`
      <html>
        <head><title>Bug Board Error</title></head>
        <body>
          <pre>${escapeHtml(error?.stack || error?.message || String(error))}</pre>
        </body>
      </html>
    `);
  }
});

app.get("/api/users", requireDashboardAuth, async (_req, res) => {
  try {
    const { data, error } = await supabase
      .from("users")
      .select("id, org_id, name, role, is_active")
      .eq("org_id", DASHBOARD_ORG_ID)
      .eq("is_active", true)
      .order("name", { ascending: true });

    if (error) {
      console.error("API /api/users error:", error);
      return sendApiError(res, 500, "Failed to load users");
    }

    return sendApiSuccess(res, data || []);
  } catch (error) {
    console.error("API /api/users fatal error:", error);
    return sendApiError(res, 500, "Failed to load users");
  }
});

app.get("/api/attendance", requireDashboardAuth, async (_req, res) => {
  try {
    const data = await getAttendancePageData(DASHBOARD_ORG_ID);
    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("API /api/attendance error:", error);
    return sendApiError(
      res,
      500,
      error?.message || "Failed to load attendance",
    );
  }
});

app.get("/api/tasks", requireDashboardAuth, async (req, res) => {
  try {
    const filters = {
      search: req.query.search || "",
      assignee: req.query.assignee || "",
      business: req.query.business || "",
      area: req.query.area || "",
      status: req.query.status || "",
      priority: req.query.priority || "",
      blocked: String(req.query.blocked || "") === "true",
      overdue: String(req.query.overdue || "") === "true",
    };

    const data = await getTasksPageData(filters, DASHBOARD_ORG_ID);
    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("API /api/tasks error:", error);
    return sendApiError(res, 500, error?.message || "Failed to load tasks");
  }
});

app.patch("/api/bugs/:id", requireDashboardAuth, async (req, res) => {
  try {
    const bugId = Number(req.params.id);
    if (!bugId) {
      return sendApiError(res, 400, "Invalid bug id");
    }

    const {
      title,
      description,
      board_column,
      severity,
      status,
      assigned_to_user_id,
      source_message_sid,
      source_phone_number,
      source_message_text,
    } = req.body || {};

    const { data: existingBug, error: existingBugError } = await supabase
      .from("stage0_bug_board")
      .select("id, org_id")
      .eq("id", bugId)
      .eq("org_id", DASHBOARD_ORG_ID)
      .maybeSingle();

    if (existingBugError) {
      console.error("Bug lookup before patch error:", existingBugError);
      return sendApiError(res, 500, "Failed to fetch bug");
    }

    if (!existingBug) {
      return sendApiError(res, 404, "Bug not found");
    }

    const patch = {
      updated_at: new Date().toISOString(),
    };

    if (title !== undefined) {
      const cleanTitle = String(title).trim();
      if (!cleanTitle) {
        return sendApiError(res, 400, "Title cannot be empty");
      }
      patch.title = cleanTitle;
    }

    if (description !== undefined) {
      patch.description =
        description == null ? null : String(description).trim();
    }

    if (board_column !== undefined) {
      if (!isValidStage0BugColumn(board_column)) {
        return sendApiError(res, 400, "Invalid board_column");
      }
      patch.board_column = String(board_column).trim();
    }

    if (severity !== undefined) {
      if (!isValidStage0BugSeverity(severity)) {
        return sendApiError(res, 400, "Invalid severity");
      }
      patch.severity = String(severity).trim();
    }

    if (status !== undefined) {
      if (!isValidStage0BugStatus(status)) {
        return sendApiError(res, 400, "Invalid status");
      }
      patch.status = String(status).trim();
    }

    if (source_message_sid !== undefined) {
      patch.source_message_sid = source_message_sid
        ? String(source_message_sid).trim()
        : null;
    }

    if (source_phone_number !== undefined) {
      patch.source_phone_number = source_phone_number
        ? String(source_phone_number).trim()
        : null;
    }

    if (source_message_text !== undefined) {
      patch.source_message_text = source_message_text
        ? String(source_message_text).trim()
        : null;
    }

    if (assigned_to_user_id !== undefined) {
      if (!assigned_to_user_id) {
        patch.assigned_to_user_id = null;
      } else {
        const numericUserId = Number(assigned_to_user_id);

        if (!numericUserId) {
          return sendApiError(res, 400, "Invalid assigned_to_user_id");
        }

        const { data: assigneeUser, error: assigneeError } = await supabase
          .from("users")
          .select("id, org_id, is_active")
          .eq("id", numericUserId)
          .eq("org_id", DASHBOARD_ORG_ID)
          .eq("is_active", true)
          .maybeSingle();

        if (assigneeError) {
          console.error("Bug assignee lookup error:", assigneeError);
          return sendApiError(res, 500, "Failed to validate assignee");
        }

        if (!assigneeUser) {
          return sendApiError(
            res,
            400,
            "Assigned user not found, inactive, or belongs to another org",
          );
        }

        patch.assigned_to_user_id = numericUserId;
      }
    }

    const { data, error } = await supabase
      .from("stage0_bug_board")
      .update(patch)
      .eq("id", bugId)
      .eq("org_id", DASHBOARD_ORG_ID)
      .select("*")
      .single();

    if (error) {
      console.error("API /api/bugs/:id PATCH error:", error);
      return sendApiError(res, 500, "Failed to update bug");
    }

    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("API /api/bugs/:id PATCH fatal error:", error);
    return sendApiError(res, 500, "Failed to update bug");
  }
});

app.get("/tasks", requireDashboardAuth, async (_req, res) => {
  res.status(200).send(`
    <html>
      <head>
        <title>Tasks</title>
        <style>
        
        ${buildThemeCss()}
${buildBasePageCss()}

          .wrap {
            max-width: 1380px;
            margin: 0 auto;
            padding: 24px 18px 36px;
            position: relative;
            z-index: 1;
          }

.topbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
  margin-bottom: 20px;
  padding: 18px 20px;
  border-radius: var(--radius-lg);
  border: 1px solid var(--line);
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  box-shadow: var(--shadow-soft);
}

          .eyebrow {
            font-size: 11px;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            color: var(--primary);
            font-weight: 700;
            margin-bottom: 8px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

          h1 {
            margin: 0;
            font-size: 30px;
            letter-spacing: -0.04em;
          }

          .subtitle {
            color: var(--muted);
            margin-top: 8px;
            font-size: 14px;
          }

          .actions {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
          }
          
.task-row-overdue {
  background-color: rgba(239, 107, 115, 0.08);
}

.task-row-blocked {
  background-color: rgba(243, 181, 98, 0.10);
}

.task-row-blocked.task-row-overdue {
  background-color: rgba(239, 107, 115, 0.14);
}

.task-row-overdue td:first-child {
  border-left: 4px solid #ef6b73;
}

.task-row-blocked td:first-child {
  border-left: 4px solid #f3b562;
}

tbody tr:hover {
  background: color-mix(in srgb, var(--primary) 10%, transparent);
}

tbody tr.task-row-overdue:hover,
tbody tr.task-row-blocked:hover,
tbody tr.task-row-blocked.task-row-overdue:hover {
  background: color-mix(in srgb, var(--danger) 24%, var(--primary) 6%);
}

.actions a {
  color: var(--text);
  text-decoration: none;
  padding: 10px 14px;
  border-radius: 12px;
  border: 1px solid color-mix(in srgb, var(--secondary) 30%, transparent);
  background: var(--secondary-soft);
  font-weight: 600;
}

.actions a:hover {
  color: var(--text-strong);
  border-color: color-mix(in srgb, var(--secondary) 55%, transparent);
}

.panel {
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-soft);
  padding: 18px;
  margin-bottom: 18px;
}

input, select, button {
  padding: 11px 12px;
  border-radius: 12px;
  border: 1px solid var(--line);
  background: rgba(255,255,255,0.04);
  color: var(--text);
  font-size: 14px;
}

input::placeholder {
  color: var(--muted);
}

button {
  cursor: pointer;
  font-weight: 700;
  color: var(--text-strong);
  background: var(--primary-soft);
  border-color: color-mix(in srgb, var(--primary) 30%, transparent);
}

          button {
            cursor: pointer;
            font-weight: 700;
            color: var(--primary);
          }

          button:hover {
            border-color: var(--line-strong);
          }

          label {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            color: var(--muted);
            font-size: 14px;
            white-space: nowrap;
          }

          #statusText {
            color: var(--muted);
            margin: 8px 2px 14px;
            font-size: 14px;
          }

.wrap {
  max-width: 1800px;
  margin: 0 auto;
  padding: 20px 18px 32px;
}

.panel.task-table-panel {
  padding: 0;
  overflow: hidden;
}

.table-wrap {
  overflow-x: auto;
  border: 1px solid var(--line);
  border-radius: var(--radius-md);
  background: rgba(255,255,255,0.03);
}

table {
  width: 100%;
  min-width: 1500px;
  border-collapse: collapse;
}

th, td {
  padding: 12px 14px;
  border-bottom: 1px solid rgba(255,255,255,0.06);
  text-align: left;
  vertical-align: top;
}

th:nth-child(1), td:nth-child(1) { min-width: 80px; }   /* ID */
th:nth-child(2), td:nth-child(2) { min-width: 300px; }  /* Title */
th:nth-child(3), td:nth-child(3) { min-width: 120px; }  /* Business */
th:nth-child(4), td:nth-child(4) { min-width: 160px; }  /* Area */
th:nth-child(5), td:nth-child(5) { min-width: 150px; }  /* Assignee */
th:nth-child(6), td:nth-child(6) { min-width: 120px; }  /* Status */
th:nth-child(7), td:nth-child(7) { min-width: 100px; }  /* Progress */
th:nth-child(8), td:nth-child(8) { min-width: 110px; }  /* Priority */
th:nth-child(9), td:nth-child(9) { min-width: 130px; }  /* Deadline */
th:nth-child(10), td:nth-child(10) { min-width: 260px; } /* Blocker */

tbody tr:hover {
  background: color-mix(in srgb, var(--primary) 10%, transparent);
}

          @media (max-width: 1100px) {
            .controls {
              grid-template-columns: 1fr 1fr;
            }
          }

          @media (max-width: 700px) {
            .wrap { padding: 16px 12px 28px; }
            h1 { font-size: 24px; }
            .controls { grid-template-columns: 1fr; }
          }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Task Operations</div>
              <h1>WeSolveHR // Tasks Console</h1>
              <div class="subtitle">Filter and inspect work across the team without changing backend behavior</div>
            </div>
            <div class="actions">
              <a href="/dashboard">Dashboard</a>
              <a href="/attendance">Attendance</a>
              <a href="/logs">Logs</a>
              <a href="/bugs">Bug Board</a>
              <a href="/reports">Reports</a>
            </div>
          </div>

<div class="panel task-table-panel">
  <div class="controls">
  <input id="search" placeholder="Search task title or ID" />
  <select id="assignee"><option value="">All assignees</option></select>

  <select id="business">
    <option value="">All business</option>
    <option value="joolian">Joolian</option>
    <option value="wesolve">WeSolve</option>
    <option value="rasset">Rasset</option>
    <option value="general">General</option>
  </select>

<select id="area">
  <option value="">All areas</option>
  <option value="pricing">Pricing</option>
  <option value="marketing">Marketing</option>
  <option value="prospect fu">Prospect FU</option>
  <option value="pm">PM</option>
  <option value="escalation">Escalation</option>
  <option value="contractors hiring">Contractors Hiring</option>
  <option value="product dev">Product Dev</option>
  <option value="pitch practice">Pitch Practice</option>
  <option value="b2c leads gen">B2C Leads Gen</option>
  <option value="b2b leads gen">B2B Leads Gen</option>
  <option value="website dev">Website Dev</option>
  <option value="competitors calling">Competitors Calling</option>
  <option value="prospects calling">Prospects Calling</option>
  <option value="research">Research</option>
  <option value="strategy">Strategy</option>
</select>

  <select id="status">
    <option value="">All active status</option>
    <option value="open">Open</option>
    <option value="in_progress">In progress</option>
    <option value="blocked">Blocked</option>
  </select>

  <select id="priority">
    <option value="">All priority</option>
    <option value="low">Low</option>
    <option value="medium">Medium</option>
    <option value="high">High</option>
    <option value="urgent">Urgent</option>
  </select>

  <label><input type="checkbox" id="blocked" /> Blocked only</label>
  <label><input type="checkbox" id="overdue" /> Overdue only</label>
  <button onclick="loadTasks()">Apply</button>
</div>
</div>

<div class="panel">
  <div id="statusText">Loading tasks...</div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
    <th>ID</th>
    <th>Title</th>
    <th>Business</th>
    <th>Area</th>
    <th>Assignee</th>
    <th>Status</th>
    <th>Progress</th>
    <th>Priority</th>
    <th>Deadline</th>
    <th>Blocker</th>
        </tr>
      </thead>
      <tbody id="taskRows"></tbody>
    </table>
  </div>
</div>

<div class="panel">
  <h2>Task Detail</h2>
  <div id="taskDetailEmpty" style="color:#8db6a0;">
    Click any task row to view full history.
  </div>
  <div id="taskDetail" style="display:none;"></div>
</div>
<script>
        
        function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function formatTime(ts) {
  try {
    const d = new Date(ts);
    return d.toLocaleString('en-IN', {
      timeZone: 'Asia/Kolkata',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    }) + ' IST';
  } catch {
    return ts;
  }
}

function formatJsonValue(value) {
  if (value == null) return '-';
  if (typeof value === 'object') {
    return escapeHtml(JSON.stringify(value));
  }
  return escapeHtml(String(value));
}

async function openTaskDetail(taskId) {
  try {
    const res = await fetch('/api/tasks/' + taskId);
    const json = await res.json();

    if (!json.ok) {
      document.getElementById('taskDetailEmpty').style.display = 'block';
      document.getElementById('taskDetailEmpty').textContent = 'Could not load task detail';
      document.getElementById('taskDetail').style.display = 'none';
      document.getElementById('taskDetail').innerHTML = '';
      return;
    }

    const task = json.data;

    const ownerText =
      task.owner_names && Array.isArray(task.owner_names) && task.owner_names.length
        ? task.owner_names.join(', ')
        : (task.assignee_name || '-');

    const historyRows = (task.task_history || []).map(function(item) {
      const time = formatTime(item.created_at);
      const by = item.changed_by_name || 'Unknown';
      const note = item.note
        ? '<div style="margin-top:4px;color:#9fe3c1;">' + escapeHtml(item.note) + '</div>'
        : '';

      let mainText = '';

      if (item.change_type === 'progress_change') {
        const oldP = item.old_value?.progress ?? 0;
        const newP = item.new_value?.progress ?? 0;
        mainText = '📈 Progress: ' + oldP + '% → ' + newP + '%';
      } else if (item.change_type === 'status_change') {
        const oldS = item.old_value?.status ?? '-';
        const newS = item.new_value?.status ?? '-';
        mainText = '🔄 Status: ' + oldS + ' → ' + newS;
      } else if (item.change_type === 'task_created') {
        mainText = '🆕 Task created';
      } else if (item.change_type === 'undo') {
        mainText = '↩️ Undo action';
      } else {
        mainText = escapeHtml(item.change_type || 'Unknown change');
      }

      return (
        '<div style="padding:10px;border-bottom:1px solid #1e2a24;">' +
          '<div style="font-size:13px;color:#8db6a0;">' + escapeHtml(time) + ' — ' + escapeHtml(by) + '</div>' +
          '<div style="font-size:15px;margin-top:4px;">' + mainText + '</div>' +
          note +
        '</div>'
      );
    }).join('');

    document.getElementById('taskDetailEmpty').style.display = 'none';
    document.getElementById('taskDetail').style.display = 'block';
    document.getElementById('taskDetail').innerHTML =
      '<div style="margin-bottom:16px;">' +
        '<div style="font-size:22px; font-weight:800;">Task #' + (task.task_no || task.id) + ' — ' + escapeHtml(task.title || '') + '</div>' +
        '<div style="margin-top:8px; color:#8db6a0; line-height:1.7;">' +
          '<div><strong>Owners:</strong> ' + escapeHtml(ownerText) + '</div>' +
          '<div><strong>Business:</strong> ' + escapeHtml(task.business || '-') + '</div>' +
          '<div><strong>Area:</strong> ' + escapeHtml(task.area || '-') + '</div>' +
          '<div><strong>Status:</strong> ' + escapeHtml(task.status || '-') + '</div>' +
          '<div><strong>Progress:</strong> ' + (task.progress ?? 0) + '%</div>' +
          '<div><strong>Priority:</strong> ' + escapeHtml(task.priority || '-') + '</div>' +
          '<div><strong>Deadline:</strong> ' + escapeHtml(task.deadline || '-') + '</div>' +
        '</div>' +
        (task.detail
          ? '<div style="margin-top:10px;"><strong>Detail:</strong> ' + escapeHtml(task.detail) + '</div>'
          : '') +
        (task.blocker_note
          ? '<div style="margin-top:10px;"><strong>Current blocker:</strong> ' + escapeHtml(task.blocker_note) + '</div>'
          : '') +
      '</div>' +
      '<div style="margin-top:16px;border:1px solid #1e2a24;border-radius:8px;">' +
        (historyRows || '<div style="padding:12px;">No history yet</div>') +
      '</div>';
  } catch (error) {
    console.error('openTaskDetail error:', error);
    document.getElementById('taskDetailEmpty').style.display = 'block';
    document.getElementById('taskDetailEmpty').textContent = 'Could not load task detail';
    document.getElementById('taskDetail').style.display = 'none';
    document.getElementById('taskDetail').innerHTML = '';
  }
}

      
          async function loadUsers() {
            const res = await fetch('/api/users');
            const json = await res.json();
            const select = document.getElementById('assignee');
            if (!json.ok) return;
            for (const user of json.data) {
              const opt = document.createElement('option');
              opt.value = user.id;
              opt.textContent = user.name;
              select.appendChild(opt);
            }
          }

          async function loadTasks() {
            const params = new URLSearchParams();
const search = document.getElementById('search').value.trim();
const assignee = document.getElementById('assignee').value;
const business = document.getElementById('business').value;
const area = document.getElementById('area').value;
const status = document.getElementById('status').value;
const priority = document.getElementById('priority').value;
const blocked = document.getElementById('blocked').checked;
const overdue = document.getElementById('overdue').checked;


if (search) params.set('search', search);
if (assignee) params.set('assignee', assignee);
if (business) params.set('business', business);
if (area) params.set('area', area);
if (status) params.set('status', status);
if (priority) params.set('priority', priority);
if (blocked) params.set('blocked', 'true');
if (overdue) params.set('overdue', 'true');

            document.getElementById('statusText').textContent = 'Loading tasks...';

            const res = await fetch('/api/tasks?' + params.toString());
            const json = await res.json();

if (!json.ok) {
  document.getElementById('statusText').textContent =
    'Could not load tasks: ' + (json.error || 'unknown error');
  document.getElementById('taskRows').innerHTML = '';
  console.error('loadTasks api error:', json);
  return;
}


            const rows = json.data || [];
console.log('tasks rows:', rows);
document.getElementById('statusText').textContent = rows.length ? '' : 'No tasks found';

document.getElementById('taskRows').innerHTML = rows.map(function(task) {
  const status = String(task.status || '').toLowerCase();

  const isBlocked = status === 'blocked';

  const isOverdue =
    !!task.deadline &&
    status !== 'done' &&
    status !== 'cancelled' &&
    new Date(task.deadline + 'T23:59:59') < new Date();

  const rowClasses = [
    isBlocked ? 'task-row-blocked' : '',
    isOverdue ? 'task-row-overdue' : ''
  ].filter(Boolean).join(' ');

  return (
    '<tr class="' + rowClasses + '" onclick="openTaskDetail(' + task.id + ')">' +
      '<td>#' + (task.task_no || task.id) + '</td>' +
      '<td>' + escapeHtml(task.title || '') + '</td>' +
      '<td>' + escapeHtml(task.business || '-') + '</td>' +
      '<td>' + escapeHtml(task.area || '-') + '</td>' +
      '<td>' + escapeHtml(
  task.assignee_name ||
  (Array.isArray(task.owner_names) ? task.owner_names.join(', ') : task.owner_names) ||
  '-'
) + '</td>' +
      '<td>' + escapeHtml(task.status || '') + '</td>' +
      '<td>' + (task.progress ?? 0) + '%</td>' +
      '<td>' + escapeHtml(task.priority || '') + '</td>' +
      '<td>' + escapeHtml(task.deadline || '-') + '</td>' +
      '<td>' + escapeHtml(task.blocker_note || '-') + '</td>' +
    '</tr>'
  );
}).join('');
          }

          loadUsers().then(loadTasks);
            setInterval(() => {
    window.location.reload();
  }, 60000);

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      location.reload();
    }
  });
        </script>
      </body>
    </html>
  `);
});

app.get("/attendance", requireDashboardAuth, async (_req, res) => {
  res.status(200).send(`
    <html>
      <head>
        <title>Attendance</title>
        <style>
    ${buildThemeCss()}   ${buildBasePageCss()}

          .wrap {
            max-width: 1320px;
            margin: 0 auto;
            padding: 24px 18px 36px;
            position: relative;
            z-index: 1;
          }

.topbar, .panel, .card {
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-soft);
}

          .topbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            flex-wrap: wrap;
            margin-bottom: 20px;
            padding: 18px 20px;
          }

          .eyebrow {
            font-size: 11px;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            color: var(--primary);
            font-weight: 700;
            margin-bottom: 8px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

          h1 {
            margin: 0;
            font-size: 30px;
            letter-spacing: -0.04em;
          }

          .subtitle {
            color: var(--muted);
            margin-top: 8px;
            font-size: 14px;
          }

          .links {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
          }

.links a {
  color: var(--text);
  text-decoration: none;
  padding: 10px 14px;
  border-radius: 12px;
  border: 1px solid color-mix(in srgb, var(--secondary) 30%, transparent);
  background: var(--secondary-soft);
  font-weight: 600;
}

.links a:hover {
  color: var(--text-strong);
  border-color: color-mix(in srgb, var(--secondary) 55%, transparent);
}

          .cards {
            display:grid;
            grid-template-columns: repeat(3, 1fr);
            gap:16px;
            margin-bottom:18px;
          }

          .card {
            padding:16px;
          }

          .card-label {
            color: var(--muted);
            font-size: 12px;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            font-weight: 700;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

          .card h2 {
            margin: 10px 0 0;
            font-size: 34px;
            color: var(--primary);
          }

          .panel {
            padding: 18px;
            margin-bottom: 18px;
          }

          h2 {
            margin: 0 0 12px;
            font-size: 19px;
          }

          .table-wrap {
            overflow-x: auto;
            border: 1px solid rgba(74, 222, 128, 0.08);
            border-radius: 14px;
            background: rgba(5, 14, 11, 0.65);
          }

          table {
            width:100%;
            border-collapse:collapse;
          }

          th, td {
            padding:12px;
            border-bottom:1px solid rgba(74, 222, 128, 0.08);
            text-align:left;
          }

          th {
            color: #9fdab7;
            font-size: 11px;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            background: rgba(8, 22, 17, 0.98);
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

          tbody tr:hover {
            background: rgba(97,255,161,0.045);
          }

          @media (max-width: 900px) {
            .cards { grid-template-columns: 1fr; }
          }

          @media (max-width: 700px) {
            .wrap { padding: 16px 12px 28px; }
            h1 { font-size: 24px; }
          }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Attendance Monitoring</div>
              <h1>WeSolveHR // Attendance Console</h1>
              <div class="subtitle">Live attendance state, recent activity, and operational visibility</div>
            </div>
            <div class="links">
              <a href="/dashboard">Dashboard</a>
              <a href="/tasks">Tasks</a>
              <a href="/logs">Logs</a>
              <a href="/bugs">Bug Board</a>
              <a href="/reports">Reports</a>
            </div>
          </div>

          <div class="cards">
            <div class="card"><div class="card-label">Logged In</div><h2 id="loggedIn">-</h2></div>
            <div class="card"><div class="card-label">On Break</div><h2 id="onBreak">-</h2></div>
            <div class="card"><div class="card-label">Active Today</div><h2 id="activeToday">-</h2></div>
          </div>

          <div class="panel">
            <h2>Current Status</h2>
            <div class="muted">Click any row to open full employee attendance details.</div>
            <div class="table-wrap">
              <table>
<thead><tr><th>Name</th><th>Role</th><th>Status</th><th>Last Activity</th></tr></thead>                <tbody id="currentStatusRows"></tbody>
              </table>
            </div>
          </div>

          <div class="panel">
            <h2>Recent Events</h2>
            <div class="table-wrap">
              <table>
                <thead><tr><th>Time</th><th>User ID</th><th>Action</th><th>Duration</th><th>Note</th></tr></thead>
                <tbody id="recentEventRows"></tbody>
              </table>
            </div>
          </div>
        </div>

        <script>

async function loadAttendance() {
  const res = await fetch('/api/attendance');
  const json = await res.json();

  if (!json.ok) {
    document.getElementById('loggedIn').textContent = 'ERR';
    document.getElementById('onBreak').textContent = 'ERR';
    document.getElementById('activeToday').textContent = 'ERR';
    document.getElementById('currentStatusRows').innerHTML =
      '<tr><td colspan="4">Could not load attendance: ' + (json.error || 'unknown error') + '</td></tr>';
    document.getElementById('recentEventRows').innerHTML = '';
    console.error('loadAttendance api error:', json);
    return;
  }

  const data = json.data;

  document.getElementById('loggedIn').textContent =
    data.summary.logged_in_count;

  document.getElementById('onBreak').textContent =
    data.summary.on_break_count;

  document.getElementById('activeToday').textContent =
    data.summary.active_today_count;

  document.getElementById('currentStatusRows').innerHTML =
    (data.current_status || [])
      .map(function (row) {
        return (
          '<tr style="cursor:pointer" data-id="' + (row.user_id || '') + '">' +
            '<td>' + (row.name || '') + '</td>' +
            '<td>' + (row.role || '') + '</td>' +
            '<td>' + (row.status || '') + '</td>' +
            '<td>' + (row.last_event_at_text || '-') + '</td>' +
          '</tr>'
        );
      })
      .join('');

  document.getElementById('recentEventRows').innerHTML =
    (data.recent_events || [])
      .map(function (row) {
        return (
          '<tr>' +
            '<td>' + (row.created_at_text || '') + '</td>' +
            '<td>' + (row.user_id || '') + '</td>' +
            '<td>' + (row.action || '') + '</td>' +
            '<td>' + (row.duration_min || '-') + '</td>' +
            '<td>' + (row.note || '-') + '</td>' +
          '</tr>'
        );
      })
      .join('');
}

          loadAttendance();
          document.getElementById("currentStatusRows").addEventListener("click", function (e) {
  const tr = e.target.closest("tr[data-id]");
  if (!tr) return;

  const id = tr.getAttribute("data-id");
  if (!id) return;

  window.location.href = "/attendance/" + id;
});
            setInterval(() => {
    window.location.reload();
  }, 60000);

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      location.reload();
    }
  });
        </script>
      </body>
    </html>
  `);
});

app.get("/logs", requireDashboardAuth, async (_req, res) => {
  res.status(200).send(`
    <html>
      <head>
        <title>Logs</title>
        <style>
          ${buildThemeCss()}   ${buildBasePageCss()}

          .wrap {
            max-width: 1320px;
            margin: 0 auto;
            padding: 24px 18px 36px;
            position: relative;
            z-index: 1;
          }

.topbar, .panel {
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-soft);
}

          .topbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            flex-wrap: wrap;
            margin-bottom: 20px;
            padding: 18px 20px;
          }

.eyebrow {
  font-size: 11px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--primary);
  font-weight: 700;
  margin-bottom: 8px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}

          h1 {
            margin: 0;
            font-size: 30px;
            letter-spacing: -0.04em;
          }

          .subtitle {
            color: var(--muted);
            margin-top: 8px;
            font-size: 14px;
          }

          .links {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
          }
.links a {
  color: var(--text);
  text-decoration: none;
  padding: 10px 14px;
  border-radius: 12px;
  border: 1px solid color-mix(in srgb, var(--secondary) 30%, transparent);
  background: var(--secondary-soft);
  font-weight: 600;
}

.links a:hover {
  color: var(--text-strong);
  border-color: color-mix(in srgb, var(--secondary) 55%, transparent);
}

          .panel {
            padding: 18px;
          }

.table-wrap {
  overflow-x: auto;
  border: 1px solid var(--line);
  border-radius: var(--radius-md);
  background: rgba(255,255,255,0.03);
}

table {
  width: 100%;
  border-collapse: collapse;
}

th, td {
  padding: 12px;
  border-bottom: 1px solid rgba(255,255,255,0.06);
  text-align: left;
  vertical-align: top;
}

th {
  color: var(--muted);
  font-size: 11px;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  background: rgba(255,255,255,0.04);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}

          tbody tr:hover {
            background: rgba(97,255,161,0.045);
          }

          .msg {
            white-space: pre-wrap;
          }

          @media (max-width: 700px) {
            .wrap { padding: 16px 12px 28px; }
            h1 { font-size: 24px; }
          }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Message Logging</div>
              <h1>WeSolveHR // Logs Console</h1>
              <div class="subtitle">Inbound command visibility for tracing, debugging, and audit review</div>
            </div>
            <div class="links">
              <a href="/dashboard">Dashboard</a>
              <a href="/tasks">Tasks</a>
              <a href="/attendance">Attendance</a>
              <a href="/bugs">Bug Board</a>
              <a href="/reports">Reports</a>
            </div>
          </div>

          <div class="panel">
            <div class="table-wrap">
              <table>
                <thead><tr><th>Time</th><th>Sender</th><th>Message</th><th>Message SID</th></tr></thead>
                <tbody id="logRows"></tbody>
              </table>
            </div>
          </div>
        </div>

        <script>
          async function loadLogs() {
            const res = await fetch('/api/logs');
            const json = await res.json();
            if (!json.ok) return;

            document.getElementById('logRows').innerHTML = (json.data || []).map(row => \`
              <tr>
                <td>\${row.created_at_text || ''}</td>
                <td>\${row.sender || ''}</td>
                <td class="msg">\${row.body || ''}</td>
                <td>\${row.message_sid || '-'}</td>
              </tr>
            \`).join('');
          }

          loadLogs();

    setInterval(() => {
    window.location.reload();
  }, 60000);

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      location.reload();
    }
  });
        </script>
      </body>
    </html>
  `);
});

app.post("/whatsapp", async (req, res) => {
  let messageSid = null;

  try {
    if (!validateTwilioRequest(req)) {
      console.warn("Rejected request due to invalid Twilio signature.");
      return res.status(403).send("Invalid Twilio signature");
    }

    console.log("Incoming message:", req.body);

    const from = req.body.From || null;
    const body = String(req.body.Body || "").trim();
    messageSid = req.body.MessageSid || null;
    const normalizedBody = normalizeText(body).replace(/\s+/g, " ");

    const rateLimitKey = from || req.ip || "unknown";
    const inboundMessageSid =
      req.body.MessageSid || req.body.SmsMessageSid || null;
    const requestTag = `[wa:${inboundMessageSid || "no-sid"}]`;

    console.log(`${requestTag} Incoming message`, {
      from,
      body,
      profileName: req.body.ProfileName || null,
    });

    if (!checkRateLimit(rateLimitKey)) {
      console.warn("Rate limit exceeded for:", rateLimitKey);
      return sendTwiml(
        res,
        "Too many requests. Please wait a minute and try again.",
      );
    }

    const { user, error: userError } = await getActiveUserByPhone(from);
    const resolvedOrgId = user?.org_id ?? DASHBOARD_ORG_ID;

    async function logParse({
      intentDetected,
      parserUsed,
      parsedJson = null,
      validationPassed = true,
      validationError = null,
      actionTaken = null,
    }) {
      await insertMessageParsingLog({
        orgId: resolvedOrgId,
        messageSid,
        phoneNumber: from,
        rawText: body,
        normalizedText: normalizedBody,
        intentDetected,
        parserUsed,
        parsedJson,
        validationPassed,
        validationError,
        actionTaken,
      });
    }

    async function runInboundAction({
      successType,
      successRefId = null,
      failureType = "command_failed",
      action,
    }) {
      try {
        const result = await action();
        await completeInboundProcessing(
          messageSid,
          successType,
          successRefId,
          resolvedOrgId,
        );
        return result;
      } catch (error) {
        console.error(`runInboundAction failed [${failureType}]:`, error);
        await failInboundProcessing(messageSid, failureType, resolvedOrgId);
        throw error;
      }
    }

    if (userError) {
      return sendTwiml(
        res,
        "❌ Could not verify your account right now\nReason: user lookup failed\nTry: please message again in a minute",
      );
    }

    const processingStart = await beginInboundProcessing(
      messageSid,
      from,
      normalizedBody,
      resolvedOrgId,
    );

    if (processingStart.error) {
      console.error("Inbound processing start error:", processingStart.error);
      return sendTwiml(res, "❌ System error while processing message");
    }

    if (processingStart.duplicate) {
      return sendTwiml(
        res,
        "Duplicate message detected. No action was repeated.",
      );
    }

    const logResult = await logIncomingMessage(user, req.body, body, from);

    if (logResult.error) {
      console.error("Incoming message log failed:", logResult.error);
      await failInboundProcessing(
        messageSid,
        "message_log_failed",
        resolvedOrgId,
      );
      return sendTwiml(
        res,
        "❌ Could not process your message right now\nReason: message logging failed\nTry: please send it again in a minute",
      );
    }

    if (logResult.duplicate) {
      await completeInboundProcessing(
        messageSid,
        "duplicate_message_log",
        null,
        resolvedOrgId,
      );
      return sendTwiml(
        res,
        "⚠️ We already received this message. If your attendance did not update, send 'status'.",
      );
    }

    if (!user) {
      await failInboundProcessing(messageSid, "unknown_user", resolvedOrgId);
      return sendTwiml(
        res,
        "❌ Your number is not registered in this system\nPlease contact admin to get added",
      );
    }

    console.log(`Mapped sender to user: ${user.name} (${user.role})`);

    // ------------------------------------------------------------------
    // Basic / utility commands
    // ------------------------------------------------------------------
    if (normalizedBody === "help attendance") {
      await logParse({
        intentDetected: "help_attendance",
        parserUsed: "normalizedBody === help attendance",
        parsedJson: { normalizedBody },
        validationPassed: true,
        actionTaken: "show_help_attendance",
      });

      return runInboundAction({
        successType: "help_shown",
        failureType: "help_failed",
        action: () =>
          sendTwiml(
            res,
            [
              "🕒 Attendance Help",
              "",
              "Your commands:",
              "login",
              "logout",
              "break",
              "back",
              "status",
              "now",
              "leave today",
              "leave tomorrow",
              "late 11:00 am",
              "",
              "Examples:",
              "login",
              "break",
              "back",
              "logout",
              "status",
              "now",
              "leave today",
              "late 10:45 am",
              "",
              "Notes:",
              "• Use actual clock time for late",
              "• Do not use: late 30 min",
            ].join("\n"),
          ),
      });
    }

    if (normalizedBody === "help tasks") {
      return runInboundAction({
        successType: "help_shown",
        failureType: "help_failed",
        action: () =>
          sendTwiml(
            res,
            [
              "📋 Task Help",
              "",
              "Create:",
              "task Ruhab high present progress on Rasset by today",
              "",
              "View:",
              "my tasks",
              "tasks Ruhab",
              "show task 2",
              "",
              "Update:",
              "progress 2 50% 20 mails sent no positive response",
              "edit task 2 blocker waiting on dependency",
              "edit task 2 clear blocker",
              "done 2 tested and verified",
              "undo last task change",
              "",
              "Manager/Admin only:",
              "cancel task 2",
              "delete task 2",
              "",
              "Notes:",
              "• Use task ID for updates",
              "• Priority: low, medium, high",
            ].join("\n"),
          ),
      });
    }

    if (normalizedBody === "help manager") {
      if (!isManagerOrAdmin(user)) {
        await failInboundProcessing(
          messageSid,
          "help_forbidden",
          resolvedOrgId,
        );
        return sendTwiml(
          res,
          "❌ Only managers/admins can use this help section.",
        );
      }

      return runInboundAction({
        successType: "help_shown",
        failureType: "help_failed",
        action: () =>
          sendTwiml(
            res,
            [
              "🧑‍💼 Manager/Admin Help",
              "",
              "Attendance for others:",
              "login Zoya",
              "logout Aj 6:30 pm",
              "break Ruhab",
              "back Mahesh",
              "",
              "People views:",
              "employee summary Aj",
              "timeline Mahesh",
              "tasks Ruhab",
              "",
              "Task management:",
              "task Ruhab high present progress on Rasset by today",
              "cancel task 2",
              "delete task 2",
              "edit task 2 title final parents pitch v2",
              "edit task 2 deadline tomorrow",
              "edit task 2 owner zoya, aj",
              "edit task 2 status blocked",
              "",
              "Notes:",
              "• Use clear unique names",
              "• Past-time marking is allowed where supported",
            ].join("\n"),
          ),
      });
    }

    if (normalizedBody === "help" || normalizedBody === "commands") {
      console.log("HELP matched", {
        rawBody: body,
        normalizedBody,
        user: user?.name,
        from,
      });

      return runInboundAction({
        successType: "help_shown",
        failureType: "help_failed",
        action: () => handleHelp(res, user),
      });
    }

    if (normalizedBody === "my tasks") {
      return runInboundAction({
        successType: "read_only_query",
        failureType: "read_only_query_failed",
        action: () => handleMyTasks(res, user),
      });
    }

    if (normalizedBody === "show overdue") {
      return runInboundAction({
        successType: "read_only_query",
        failureType: "read_only_query_failed",
        action: () => handleShowOverdue(res, user),
      });
    }

    const showTaskId = parseShowTaskCommand(body);
    if (showTaskId) {
      return runInboundAction({
        successType: "read_only_query",
        failureType: "read_only_query_failed",
        action: () => handleShowTask(res, user, showTaskId),
      });
    }

    const doneCommand = parseDoneCommand(body);
    if (doneCommand) {
      return runInboundAction({
        successType: "task_updated",
        failureType: "task_update_failed",
        action: () =>
          handleDoneTask(res, user, doneCommand.taskId, doneCommand.note),
      });
    }

    const employeeSummaryCommand = parseEmployeeSummaryCommand(body);
    if (employeeSummaryCommand) {
      return runInboundAction({
        successType: "attendance_query",
        failureType: "attendance_query_failed",
        action: () => handleEmployeeSummary(res, user, employeeSummaryCommand),
      });
    }

    // ------------------------------------------------------------------
    // Admin cleanup / correction commands
    // ------------------------------------------------------------------
    const timelineCommand = parseTimelineCommand(body);
    if (timelineCommand) {
      return runInboundAction({
        successType: "attendance_query",
        failureType: "attendance_query_failed",
        action: () => handleTimelineAttendance(res, user, timelineCommand),
      });
    }

    const auditAttendanceCommand = parseAuditAttendanceCommand(body);
    if (auditAttendanceCommand) {
      return runInboundAction({
        successType: "attendance_query",
        failureType: "attendance_query_failed",
        action: () => handleAuditAttendance(res, user, auditAttendanceCommand),
      });
    }

    const deadlineCommand = parseDeadlineCommand(body);
    if (deadlineCommand) {
      await logParse({
        intentDetected: "deadline_update",
        parserUsed: "parseDeadlineCommand",
        parsedJson: deadlineCommand,
        validationPassed: true,
        actionTaken: "handleDeadlineUpdate",
      });

      return runInboundAction({
        successType: "task_updated",
        failureType: "task_update_failed",
        action: () =>
          handleDeadlineUpdate(
            res,
            user,
            deadlineCommand.taskId,
            deadlineCommand.dateText,
          ),
      });
    }

    const undoAttendanceCommand = parseUndoAttendanceCommand(body);
    if (undoAttendanceCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () => handleUndoAttendance(res, user, undoAttendanceCommand),
      });
    }

    const resetAttendanceCommand = parseResetAttendanceCommand(body);
    if (resetAttendanceCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () => handleResetAttendance(res, user, resetAttendanceCommand),
      });
    }

    const forceAttendanceCommand = parseForceAttendanceCommand(body);
    if (forceAttendanceCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () => handleForceAttendance(res, user, forceAttendanceCommand),
      });
    }

    const fixAttendanceCommand = parseFixAttendanceCommand(body);
    if (fixAttendanceCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () => handleFixAttendance(res, user, fixAttendanceCommand),
      });
    }

    const removeAttendanceCommand = parseRemoveAttendanceCommand(body);
    if (removeAttendanceCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () =>
          handleRemoveAttendance(res, user, removeAttendanceCommand),
      });
    }

    const autoFixAttendanceCommand = parseAutoFixAttendanceCommand(body);
    if (autoFixAttendanceCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () =>
          handleAutoFixAttendance(res, user, autoFixAttendanceCommand),
      });
    }

    const lockAttendanceCommand = parseLockAttendanceCommand(body);
    if (lockAttendanceCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () => handleLockAttendanceDay(res, user, lockAttendanceCommand),
      });
    }

    // ------------------------------------------------------------------
    // Task progress / identity / status
    // ------------------------------------------------------------------
    const progressCommand = parseProgressCommand(body);
    if (progressCommand) {
      return runInboundAction({
        successType: "task_updated",
        failureType: "task_update_failed",
        action: () =>
          handleProgressTask(
            res,
            user,
            progressCommand.taskId,
            progressCommand.progress,
            progressCommand.note,
          ),
      });
    }

    if (parseWhoAmICommand(body)) {
      return runInboundAction({
        successType: "read_only_query",
        failureType: "read_only_query_failed",
        action: () => handleWhoAmI(res, user),
      });
    }

    if (parseStatusCommand(body)) {
      return runInboundAction({
        successType: "read_only_query",
        failureType: "read_only_query_failed",
        action: () => handleStatus(res, user),
      });
    }

    const lateUnsureCommand = parseLateUnsureCommand(body);
    if (lateUnsureCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () => handleLateUnsureCommand(res, user, lateUnsureCommand),
      });
    }

    const lateForOther = parseLateForOtherCommand(body);
    if (lateForOther) {
      if (!isManagerOrAdmin(user)) {
        await failInboundProcessing(
          messageSid,
          "attendance_update_forbidden",
          resolvedOrgId,
        );
        return sendTwiml(res, "Only managers can mark late for others.");
      }

      const targetUser = await findUniqueUserByName(
        lateForOther.target_name,
        user.org_id,
      );

      if (!targetUser) {
        await failInboundProcessing(
          messageSid,
          "attendance_target_not_found",
          resolvedOrgId,
        );
        return sendTwiml(
          res,
          `I could not uniquely find an active user named "${lateForOther.target_name}".`,
        );
      }

      const lateIso = parseLocalDateTimeForToday(lateForOther.time_text);

      if (!lateIso) {
        await failInboundProcessing(
          messageSid,
          "attendance_bad_time",
          resolvedOrgId,
        );
        return sendTwiml(
          res,
          `Could not understand the time "${lateForOther.time_text}". Use format like 11:00 AM.`,
        );
      }

      const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
      const locked = await isAttendanceDayLocked(
        targetUser.id,
        attendanceDate,
        user.org_id,
      );

      if (locked) {
        await failInboundProcessing(
          messageSid,
          "attendance_day_locked",
          resolvedOrgId,
        );
        return sendTwiml(
          res,
          `❌ Attendance is locked for ${targetUser.name} on ${attendanceDate}`,
        );
      }

      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: async () => {
          const shiftStartIso = getShiftStartIsoForToday();
          const approved = isLateApproved(
            new Date().toISOString(),
            shiftStartIso,
          );
          const informedAtIso = new Date().toISOString();

          const { error } = await supabase.from("late_arrivals").upsert(
            [
              {
                org_id: user.org_id,
                user_id: targetUser.id,
                late_date: attendanceDate,
                expected_login_at: lateIso,
                informed_at: informedAtIso,
                shift_start_at: shiftStartIso,
                is_approved: approved,
                created_by_user_id: user.id,
                note: lateForOther.note || `Marked by ${user.name}`,
              },
            ],
            { onConflict: "user_id,late_date" },
          );

          if (error) {
            console.error(error);
            return sendTwiml(res, "Failed to mark late.");
          }

          return sendTwiml(
            res,
            `⏰ Late marked\n${targetUser.name} will join at ${lateForOther.time_text}`,
          );
        },
      });
    }

    const lateCommand = parseLateCommand(body);
    if (lateCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () => handleLateCommand(res, user, lateCommand),
      });
    }

    const unsupportedTimedSelfAttendance =
      parseUnsupportedTimedSelfAttendance(body);
    if (unsupportedTimedSelfAttendance) {
      await logParse({
        intentDetected: "attendance_timed_self_not_supported",
        parserUsed: "parseUnsupportedTimedSelfAttendance",
        parsedJson: unsupportedTimedSelfAttendance,
        validationPassed: false,
        validationError: "timed_self_attendance_not_supported",
        actionTaken: "reply_timed_self_attendance_not_supported",
      });

      await failInboundProcessing(
        messageSid,
        "timed_self_attendance_not_supported",
        resolvedOrgId,
      );

      return sendTwiml(
        res,
        `❌ ${unsupportedTimedSelfAttendance.action} with time is not supported for self-update yet\nYou can use:\n${unsupportedTimedSelfAttendance.action}\n\nOr ask admin:\nmark ${user.name} ${unsupportedTimedSelfAttendance.action} ${unsupportedTimedSelfAttendance.time_text}`,
      );
    }

    const markAttendanceCommand = parseMarkAttendanceCommand(body);
    if (markAttendanceCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () => handleMarkedAttendance(res, user, markAttendanceCommand),
      });
    }

    const directManagerAttendanceCommand =
      parseDirectManagerAttendanceCommand(body);
    if (directManagerAttendanceCommand) {
      if (!isManagerOrAdmin(user)) {
        await failInboundProcessing(
          messageSid,
          "attendance_update_forbidden",
          resolvedOrgId,
        );
        return sendTwiml(res, "Only managers can mark attendance for others.");
      }

      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () =>
          handleMarkedAttendance(res, user, {
            target_name: directManagerAttendanceCommand.target_name,
            action: directManagerAttendanceCommand.action,
            duration_min: directManagerAttendanceCommand.duration_min,
            time_text: directManagerAttendanceCommand.time_text,
            reason: directManagerAttendanceCommand.reason,
          }),
      });
    }

    const attendanceCommand = parseAttendanceCommand(body);
    if (attendanceCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_handler_failed",
        action: () => handleSelfAttendance(res, user, attendanceCommand),
      });
    }

    // ------------------------------------------------------------------
    // Task blocking / team visibility
    // ------------------------------------------------------------------
    const cancelCmd = parseCancelTaskCommand(body);
    if (cancelCmd) {
      if (cancelCmd.error) {
        await logParse({
          intentDetected: "delete_or_cancel_task",
          parserUsed: "parseCancelTaskCommand",
          parsedJson: cancelCmd,
          validationPassed: false,
          validationError: cancelCmd.error,
          actionTaken: "delete_or_cancel_validation_failed",
        });

        await failInboundProcessing(
          messageSid,
          "task_delete_bad_format",
          resolvedOrgId,
        );
        return sendTwiml(res, cancelCmd.error);
      }

      if (!isManagerOrAdmin(user)) {
        await failInboundProcessing(
          messageSid,
          "task_update_forbidden",
          resolvedOrgId,
        );
        return sendTwiml(res, "❌ Only managers/admins can cancel tasks");
      }

      return runInboundAction({
        successType: "task_updated",
        failureType: "task_update_failed",
        action: async () => {
          const { task, error } = await getTaskById(
            cancelCmd.taskId,
            user.org_id,
          );

          if (error || !task) {
            return sendTwiml(res, "❌ Task not found");
          }

          if (task.status === "cancelled") {
            return sendTwiml(res, "⚠️ Task already cancelled");
          }

          const oldStatus = task.status;

          const { error: updateError } = await supabase
            .from("tasks")
            .update({
              status: "cancelled",
              last_updated_by_user_id: user.id,
              updated_at: new Date().toISOString(),
            })
            .eq("id", task.id);

          if (updateError) {
            console.error(updateError);
            return sendTwiml(res, "❌ Failed to cancel task");
          }

          await insertTaskHistory(
            task.id,
            user.id,
            "status_change",
            "status",
            oldStatus,
            "cancelled",
            user.org_id,
          );

          return sendTwiml(
            res,
            `🗑️ Task ${taskRef(task)} cancelled successfully`,
          );
        },
      });
    }

    const tasksByNameCommand = parseTasksByNameCommand(body);
    if (tasksByNameCommand) {
      return runInboundAction({
        successType: "read_only_query",
        failureType: "read_only_query_failed",
        action: () =>
          handleTasksByName(res, user, tasksByNameCommand.assignee_name),
      });
    }

    if (parseWhoIsOnBreakCommand(body)) {
      return runInboundAction({
        successType: "attendance_query",
        failureType: "attendance_query_failed",
        action: () => handleWhoIsOnBreak(res, user),
      });
    }

    if (parseNowCommand(body)) {
      return runInboundAction({
        successType: "attendance_query",
        failureType: "attendance_query_failed",
        action: () => handleNowSummary(res, user),
      });
    }

    if (parseSummaryTodayCommand(body)) {
      return runInboundAction({
        successType: "attendance_query",
        failureType: "attendance_query_failed",
        action: () => handleSummaryToday(res, user),
      });
    }

    if (parseUndoLastTaskChangeCommand(body)) {
      return runInboundAction({
        successType: "task_updated",
        failureType: "task_update_failed",
        action: () => handleUndoLastTaskChange(res, user),
      });
    }

    if (parseWhoIsOffTodayCommand(body)) {
      return runInboundAction({
        successType: "attendance_query",
        failureType: "attendance_query_failed",
        action: () => handleWhoIsOffToday(res, user),
      });
    }

    // ------------------------------------------------------------------
    // Leave commands
    // ------------------------------------------------------------------
    const offDayCommand = parseOffDayCommand(body);
    if (offDayCommand) {
      const normalizedRaw = String(body || "").trim();

      if (/^(leave|off)\s+on\s+/i.test(normalizedRaw)) {
        return runInboundAction({
          successType: "attendance_updated",
          failureType: "attendance_update_failed",
          action: () => handleSelfOffDay(res, user, offDayCommand),
        });
      }

      if (
        /^(leave|off)\s+(today|tomorrow|on\s+today|on\s+tomorrow|on\s+[a-z]+\s+\d{1,2}|on\s+\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+|[a-z]+\s+\d{1,2})$/i.test(
          normalizedRaw,
        )
      ) {
        return runInboundAction({
          successType: "attendance_updated",
          failureType: "attendance_update_failed",
          action: () => handleSelfOffDay(res, user, offDayCommand),
        });
      }
    }

    const offDayForOtherCommand = parseOffDayForOtherCommand(body);
    if (offDayForOtherCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () => handleOffDayForOther(res, user, offDayForOtherCommand),
      });
    }

    // ------------------------------------------------------------------
    // Task creation / parsing
    // ------------------------------------------------------------------
    const editTaskCommand = parseEditTaskCommand(body);
    if (editTaskCommand) {
      await logParse({
        intentDetected: "edit_task",
        parserUsed: "parseEditTaskCommand",
        parsedJson: editTaskCommand,
        validationPassed: true,
        actionTaken: "handleEditTask",
      });

      return runInboundAction({
        successType: "task_updated",
        failureType: "task_update_failed",
        action: () => handleEditTask(res, user, editTaskCommand),
      });
    }

    const extraWorkCommand = parseExtraWorkCommand(body);
    if (extraWorkCommand) {
      await logParse({
        intentDetected: "extra_work",
        parserUsed: "parseExtraWorkCommand",
        parsedJson: extraWorkCommand,
        validationPassed: true,
        actionTaken: "handleExtraWork",
      });

      return runInboundAction({
        successType: "extra_work_saved",
        failureType: "extra_work_save_failed",
        action: () => handleExtraWork(res, user, extraWorkCommand, messageSid),
      });
    }

    const advancedCreateTaskCommand = parseAdvancedCreateTaskCommand(body);
    if (advancedCreateTaskCommand) {
      await logParse({
        intentDetected: "create_task_advanced",
        parserUsed: "parseAdvancedCreateTaskCommand",
        parsedJson: advancedCreateTaskCommand,
        validationPassed: !advancedCreateTaskCommand.error,
        validationError: advancedCreateTaskCommand.error || null,
        actionTaken: advancedCreateTaskCommand.error
          ? "advanced_create_validation_failed"
          : "handleCreateTaskAdvanced",
      });

      return runInboundAction({
        successType: "task_created",
        failureType: "task_create_failed",
        action: () =>
          handleCreateTaskAdvanced(res, user, advancedCreateTaskCommand),
      });
    }

    let taskCommand = parseSimpleTaskCommand(body);
    let aiParsingAttempted = false;

    if (taskCommand) {
      await logParse({
        intentDetected: "create_task_simple",
        parserUsed: "parseSimpleTaskCommand",
        parsedJson: taskCommand,
        validationPassed: true,
        actionTaken: "handleCreateTask",
      });
    }

    if (!taskCommand && looksLikeTask(body)) {
      aiParsingAttempted = true;
      taskCommand = await parseTaskWithAI(body);

      await logParse({
        intentDetected: "create_task_ai_attempt",
        parserUsed: "parseTaskWithAI",
        parsedJson: taskCommand,
        validationPassed: !!taskCommand,
        validationError: taskCommand ? null : "ai_task_parse_failed",
        actionTaken: taskCommand ? "handleCreateTask" : "reply_ai_parse_failed",
      });
    }

    console.log("Body received for task parsing:", body);
    console.log("Final task command:", taskCommand);

    if (taskCommand) {
      return runInboundAction({
        successType: "task_created",
        failureType: "task_create_failed",
        action: () => handleCreateTask(res, user, taskCommand),
      });
    }

    if (aiParsingAttempted && !taskCommand) {
      await failInboundProcessing(
        messageSid,
        "task_parse_failed",
        resolvedOrgId,
      );
      return sendTwiml(
        res,
        "I could not parse that task automatically right now. Please use this format: task Ruhab high VPN testing by tomorrow",
      );
    }

    console.log("Unknown command fallback", {
      rawBody: body,
      normalizedBody,
      user: user?.name,
      from,
    });

    await logParse({
      intentDetected: "unknown_command",
      parserUsed: "none",
      parsedJson: null,
      validationPassed: false,
      validationError: "unknown_command",
      actionTaken: "reply_unknown_command_help",
    });

    await failInboundProcessing(messageSid, "unknown_command", resolvedOrgId);
    return sendTwiml(res, buildUnknownCommandHelp(user, body));
  } catch (error) {
    if (messageSid) {
      const resolvedOrgId =
        typeof req !== "undefined" && req.body && req.body.From
          ? ((await getActiveUserByPhone(req.body.From))?.user?.org_id ??
            DASHBOARD_ORG_ID)
          : DASHBOARD_ORG_ID;

      await failInboundProcessing(
        messageSid,
        "webhook_exception",
        resolvedOrgId,
      );
    }

    console.error("Unhandled /whatsapp error:", error);
    return sendTwiml(res, "Something went wrong.");
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
