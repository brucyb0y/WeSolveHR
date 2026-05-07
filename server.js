import express from "express";
import dotenv from "dotenv";
import twilio from "twilio";
import { createClient } from "@supabase/supabase-js";
import OpenAI from "openai";
import { toFile } from "openai/uploads";
import bcrypt from "bcrypt";
import session from "express-session";
import crypto from "crypto";
import multer from "multer";
import XLSX from "xlsx";
import fs from "fs";
import os from "os";
import path from "path";
import ffmpeg from "fluent-ffmpeg";
import ffmpegStatic from "ffmpeg-static";

dotenv.config();

console.log("OPENAI KEY LOADED:", !!process.env.OPENAI_API_KEY);

const app = express();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024,
  },
});
const port = process.env.PORT || 3000;
const DASHBOARD_ORG_ID = 1;

const LEAD_BUSINESSES = [
  { business: "rasset", label: "Rasset", table: "rasset_leads", active: true },
  {
    business: "joolian",
    label: "Joolian",
    table: "joolian_leads",
    active: true,
  },
];

function getActiveLeadBusinesses() {
  return LEAD_BUSINESSES.filter((x) => x.active);
}

function getBusinessConfig(business) {
  const key = String(business || "")
    .trim()
    .toLowerCase();
  return getActiveLeadBusinesses().find((x) => x.business === key) || null;
}

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
ffmpeg.setFfmpegPath(ffmpegStatic);

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

function normalizePhoneForLogin(input) {
  if (!input) return "";

  let value = String(input).trim();

  // Remove whatsapp: if someone pastes it
  value = value.replace(/^whatsapp:/i, "");

  // Remove spaces, dashes, brackets, dots etc, but keep digits and +
  value = value.replace(/[^\d+]/g, "");

  // Convert 00... to +...
  if (value.startsWith("00")) {
    value = `+${value.slice(2)}`;
  }

  // If user entered full country code but no plus, add it
  if (value && !value.startsWith("+")) {
    value = `+${value}`;
  }

  return value;
}

function normalizeLeadPhone(input) {
  if (!input) return "";

  let digits = String(input)
    .replace(/^whatsapp:/i, "")
    .replace(/\D/g, "");

  if (!digits) return "";

  if (digits.startsWith("00")) {
    digits = digits.slice(2);
  }

  if (digits.length === 10) {
    digits = "91" + digits;
  }

  if (digits.length === 11 && digits.startsWith("0")) {
    digits = "91" + digits.slice(1);
  }

  return digits;
}

app.use(express.json());

app.use(
  session({
    secret: process.env.SESSION_SECRET || "wesolve-secret",
    resave: false,
    saveUninitialized: false,
    cookie: {
      secure: false,
      httpOnly: true,
      maxAge: 1000 * 60 * 60 * 24 * 30, // 30 days
    },
  }),
);

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

async function getBusinessLeadsData(
  orgId,
  business,
  selectedTab = "all",
  search = "",
  page = 1,
  filters = {},
) {
  const normalizedBusiness = getBusinessCanonicalName(business);
  const tableName = getBusinessLeadTableName(normalizedBusiness);

  const safePage = Math.max(1, Number(page) || 1);
  const pageSize = 25;
  const from = (safePage - 1) * pageSize;
  const to = from + pageSize - 1;
  const q = String(search || "").trim();
  const industryFilter = String(filters.industry || "").trim();
  const capabilityFilter = String(filters.capability || "").trim();
  const entityTypeFilter = String(filters.entity_type || "").trim();
  const statusFilter = String(filters.status || "").trim();
  const cityFilter = String(filters.city || "").trim();
  const stateFilter = String(filters.state || "").trim();
  const assignedToFilter = String(filters.assigned_to || "").trim();
  const qualifiedFilter = String(filters.qualified || "").trim();
  const worthTalkingFilter = String(filters.worth_talking || "").trim();
  const hasCallTranscriptionFilter = String(
    filters.has_call_transcription || "",
  ).trim();
  const { data: voiceRows, error: voiceError } = await supabase
    .from("lead_voice_uploads")
    .select("*")
    .eq("org_id", orgId)
    .eq("business", normalizedBusiness)
    .order("created_at", { ascending: false });

  if (voiceError) throw voiceError;

  let businessRows = [];
  let totalBusinessCount = 0;
  let b2bBusinessCount = 0;
  let b2cBusinessCount = 0;
  if (tableName) {
    let query = supabase
      .from(tableName)
      .select("*", { count: "exact" })
      .eq("org_id", orgId)
      .order("created_at", { ascending: false });

    if (tableName === "rasset_leads") {
      query = query.or("is_deleted.is.null,is_deleted.eq.false");
    }

    if (q) {
      const commonSearchFields = [
        `industry_primary.ilike.%${q}%`,
        `manufacturing_capabilities.ilike.%${q}%`,
        `entity_type.ilike.%${q}%`,
        `raw_industry.ilike.%${q}%`,
        `assigned_to.ilike.%${q}%`,
        `lead_source.ilike.%${q}%`,
        `import_source.ilike.%${q}%`,
        `phone.ilike.%${q}%`,
        `business_name.ilike.%${q}%`,
        `contact_name.ilike.%${q}%`,
        `email.ilike.%${q}%`,
        `city.ilike.%${q}%`,
        `industry.ilike.%${q}%`,
        `notes.ilike.%${q}%`,
        `latest_transcript.ilike.%${q}%`,
        `company.ilike.%${q}%`,
        `website.ilike.%${q}%`,
        `pin_code.ilike.%${q}%`,
        `location.ilike.%${q}%`,
        `country.ilike.%${q}%`,
        `owner_name.ilike.%${q}%`,
        `number_of_employees.ilike.%${q}%`,
        `company_size.ilike.%${q}%`,
        `lead_stage.ilike.%${q}%`,
        ...(q.toLowerCase() === "qualified" ? ["qualified.eq.true"] : []),
        ...(q.toLowerCase() === "l2 done" ||
        q.toLowerCase() === "l2" ||
        q.toLowerCase() === "l2_done"
          ? ["l2_done.eq.true"]
          : []),
        ...(q.toLowerCase() === "prospect" ? ["lead_stage.eq.prospect"] : []),
      ];

      const joolianOnlySearchFields =
        tableName === "joolian_leads"
          ? [
              `age_group.ilike.%${q}%`,
              `activity_category.ilike.%${q}%`,
              `sub_activity_category.ilike.%${q}%`,
              `type_of_business.ilike.%${q}%`,
              `pricing_approx.ilike.%${q}%`,
            ]
          : [];

      query = query.or(
        [...commonSearchFields, ...joolianOnlySearchFields].join(","),
      );
    }
    if (tableName === "rasset_leads") {
      if (industryFilter) {
        query = query.or(
          `industry.ilike.%${industryFilter}%,industry_primary.ilike.%${industryFilter}%,raw_industry.ilike.%${industryFilter}%`,
        );
      }

      if (capabilityFilter) {
        query = query.ilike(
          "manufacturing_capabilities",
          `%${capabilityFilter}%`,
        );
      }

      if (entityTypeFilter) {
        query = query.eq("entity_type", entityTypeFilter);
      }

      if (statusFilter) {
        query = query.eq("status", statusFilter);
      }

      if (cityFilter) {
        query = query.ilike("city", `%${cityFilter}%`);
      }

      if (stateFilter) {
        query = query.ilike("state", `%${stateFilter}%`);
      }

      if (assignedToFilter) {
        query = query.ilike("assigned_to", `%${assignedToFilter}%`);
      }

      if (hasCallTranscriptionFilter === "yes") {
        query = query
          .not("latest_transcript", "is", null)
          .neq("latest_transcript", "");
      }

      if (hasCallTranscriptionFilter === "no") {
        query = query.or("latest_transcript.is.null,latest_transcript.eq.");
      }

      if (qualifiedFilter === "yes") {
        query = query.eq("qualified", true);
      }

      if (qualifiedFilter === "no") {
        query = query.eq("qualified", false);
      }

      if (worthTalkingFilter === "yes") {
        query = query.eq("worth_talking", true);
      }

      if (worthTalkingFilter === "no") {
        query = query.eq("worth_talking", false);
      }
    }
    const { data, error, count } = await query;

    if (error) throw error;
    businessRows = data || [];
    totalBusinessCount = count || businessRows.length;
  }

  if (tableName === "rasset_leads") {
    const { count: b2bCount } = await supabase
      .from(tableName)
      .select("id", { count: "exact", head: true })
      .eq("org_id", orgId)
      .or("is_deleted.is.null,is_deleted.eq.false")
      .eq("lead_category", "b2b");

    const { count: b2cCount } = await supabase
      .from(tableName)
      .select("id", { count: "exact", head: true })
      .eq("org_id", orgId)
      .or("is_deleted.is.null,is_deleted.eq.false")
      .eq("lead_category", "b2c");

    b2bBusinessCount = b2bCount || 0;
    b2cBusinessCount = b2cCount || 0;
  } else {
    b2bBusinessCount = businessRows.filter(
      (x) => x.lead_category === "b2b",
    ).length;
    b2cBusinessCount = businessRows.filter(
      (x) => x.lead_category === "b2c",
    ).length;
  }

  const voice = voiceRows || [];

  const voiceInboxRows = voice.filter((x) =>
    [
      "pending_transcription",
      "transcribing",
      "pending_review",
      "rejected",
    ].includes(x.status),
  );

  const filteredBusinessRows = businessRows.filter((x) => {
    if (selectedTab === "b2b") return x.lead_category === "b2b";
    if (selectedTab === "b2c") return x.lead_category === "b2c";
    if (selectedTab === "in_progress") return x.status === "in_progress";
    if (selectedTab === "completed") return x.status === "completed";
    return true;
  });

  const pagedBusinessRows = filteredBusinessRows.slice(from, to + 1);

  const rows =
    selectedTab === "voice_inbox" ? voiceInboxRows : pagedBusinessRows;

  return {
    business: normalizedBusiness,
    selectedTab,
    search: q,
    page: safePage,
    pageSize,
    rows,
    voiceRows: voice,
    businessRows,
    tableName,
    counts: {
      all: totalBusinessCount,
      b2b: b2bBusinessCount,
      b2c: b2cBusinessCount,
      in_progress: businessRows.filter((x) => x.status === "in_progress")
        .length,
      completed: businessRows.filter((x) => x.status === "completed").length,
      voice_inbox: voiceInboxRows.length,
      total: totalBusinessCount,
      pending_review: voice.filter((x) => x.status === "pending_review").length,
    },
    pagination: {
      total: filteredBusinessRows.length,
      page: safePage,
      pageSize,
      hasPrev: safePage > 1,
      hasNext: to + 1 < filteredBusinessRows.length,
    },
    filters: {
      industry: industryFilter,
      capability: capabilityFilter,
      entity_type: entityTypeFilter,
      status: statusFilter,
      city: cityFilter,
      state: stateFilter,
      assigned_to: assignedToFilter,
      qualified: qualifiedFilter,
      worth_talking: worthTalkingFilter,
      has_call_transcription: hasCallTranscriptionFilter,
    },
  };
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

function parseChangePasswordCommand(text) {
  const raw = normalizeText(text);
  const match = raw.match(/^change password\s+(.+)$/i);

  if (!match) return null;

  return {
    newPassword: match[1].trim(),
  };
}

function renderLoginPage(errorMessage = "") {
  return `
    <html>
      <head>
        <title>Login | WeSolveHR</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}
          ${buildTopNavCss()}

          .login-shell {
            min-height: 100vh;
            display: flex;
            flex-direction: column;
          }

          .login-wrap {
            flex: 1;
            max-width: 1200px;
            margin: 0 auto;
            width: 100%;
            display: grid;
            grid-template-columns: 1.1fr 0.95fr;
            gap: 28px;
            align-items: center;
            padding: 32px 18px 48px;
          }

          .hero-card,
          .login-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-xl);
            box-shadow: var(--shadow-soft);
          }

          .hero-card { padding: 34px; }
          .eyebrow {
            font-size: 11px;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            color: var(--primary);
            font-weight: 700;
            margin-bottom: 12px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

          .hero-card h1 {
            margin: 0 0 14px;
            font-size: 42px;
            line-height: 1.05;
            letter-spacing: -0.05em;
          }

          .hero-card p {
            margin: 0;
            color: var(--muted);
            font-size: 16px;
            line-height: 1.7;
          }

          .login-card {
            padding: 28px;
            max-width: 460px;
            width: 100%;
            margin-left: auto;
          }

          .login-card h2 {
            margin: 0 0 8px;
            font-size: 28px;
            letter-spacing: -0.04em;
          }

          .login-subtitle {
            color: var(--muted);
            margin-bottom: 22px;
            font-size: 14px;
          }

          .form-group { margin-bottom: 16px; }
          .label {
            display: block;
            margin-bottom: 8px;
            font-size: 13px;
            font-weight: 700;
            color: var(--text);
          }

          .input {
            width: 100%;
            padding: 14px 14px;
            border-radius: 14px;
            border: 1px solid var(--line);
            background: rgba(255,255,255,0.04);
            color: var(--text);
            font-size: 15px;
            outline: none;
          }

          .input:focus {
            border-color: color-mix(in srgb, var(--primary) 55%, transparent);
            box-shadow: 0 0 0 3px rgba(139,124,246,0.14);
          }

          .login-btn {
            width: 100%;
            padding: 14px 16px;
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 14px;
            background: var(--primary-soft);
            color: var(--text-strong);
            font-size: 15px;
            font-weight: 800;
            cursor: pointer;
          }

          .login-error {
            margin-bottom: 16px;
            padding: 12px 14px;
            border-radius: 14px;
            border: 1px solid rgba(239,107,115,0.28);
            background: rgba(239,107,115,0.12);
            color: #ffd7da;
            font-size: 14px;
            line-height: 1.5;
          }

          .helper {
            margin-top: 14px;
            color: var(--muted);
            font-size: 13px;
            line-height: 1.6;
          }

          .minimal-top {
            border-bottom: 1px solid rgba(255,255,255,0.08);
            backdrop-filter: blur(18px);
            background: rgba(13, 18, 33, 0.82);
          }

          .minimal-top-inner {
            max-width: 1600px;
            margin: 0 auto;
            padding: 14px 18px;
            display: flex;
            align-items: center;
            justify-content: space-between;
          }

          .brand {
            font-size: 20px;
            font-weight: 800;
            letter-spacing: -0.03em;
            color: var(--text);
          }

          .brand-sub {
            color: var(--muted);
            font-size: 13px;
          }

          @media (max-width: 980px) {
            .login-wrap { grid-template-columns: 1fr; }
            .login-card { margin-left: 0; max-width: 100%; }
          }
          
          .loading-overlay {
  position: fixed;
  inset: 0;
  background: rgba(9, 12, 24, 0.55);
  backdrop-filter: blur(6px);
  display: none;
  align-items: center;
  justify-content: center;
  z-index: 9999;
}

.loading-overlay.show {
  display: flex;
}

.loading-card {
  min-width: 240px;
  padding: 22px 24px;
  border-radius: 18px;
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  box-shadow: var(--shadow-soft);
  text-align: center;
}

.loading-spinner {
  width: 34px;
  height: 34px;
  margin: 0 auto 12px;
  border-radius: 999px;
  border: 3px solid rgba(255,255,255,0.14);
  border-top-color: var(--primary);
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.login-btn:disabled {
  opacity: 0.75;
  cursor: wait;
}

        </style>
      </head>
      <body>
        <div class="login-shell">
          <div class="minimal-top">
            <div class="minimal-top-inner">
              <div class="brand">WeSolveHR</div>
              <div class="brand-sub">Personal workspace login</div>
            </div>
          </div>

          <div class="login-wrap">
            <div class="hero-card">
              <div class="eyebrow">Team Operations</div>
              <h1>Welcome back</h1>
              <p>
                Log in to access your personal workspace, attendance details,
                leave balance, recent feedback, and appraisal history.
              </p>
            </div>

            <div class="login-card">
              <h2>Sign in</h2>
              <div class="login-subtitle">Use your phone number and password to continue.</div>

              ${errorMessage ? `<div class="login-error">${escapeHtml(errorMessage)}</div>` : ""}

<form method="POST" action="/login" id="loginForm">
  <div class="form-group">
<label>Phone number</label>
<input
  class="input"
  type="text"
  name="phone"
  placeholder="e.g. +12133081594 or +919891517965"
  autocomplete="tel"
/>
<p class="helper" style="margin-top:8px;">
  Enter your full phone number with country code.
</p>
  </div>

  <div class="form-group">
    <label class="label">Password</label>
    <input
      class="input"
      type="password"
      name="password"
      placeholder="Enter password"
      autocomplete="current-password"
    />
  </div>

  <button class="login-btn" id="loginSubmitBtn" type="submit">Login</button>
</form>

<div id="loginLoadingOverlay" class="loading-overlay">
  <div class="loading-card">
    <div class="loading-spinner"></div>
    <div style="font-weight:700;">Logging you in...</div>
  </div>
</div>

<script>
  (function () {
    const form = document.getElementById("loginForm");
    const overlay = document.getElementById("loginLoadingOverlay");
    const btn = document.getElementById("loginSubmitBtn");

    if (!form) return;

    form.addEventListener("submit", function () {
      if (btn) {
        btn.disabled = true;
        btn.textContent = "Logging in...";
      }
      if (overlay) {
        overlay.classList.add("show");
      }
    });
  })();
</script>

              <div class="helper">
                First-time users can use the default password assigned by admin. Use your full phone number with country code.
              </div>
            </div>
          </div>
        </div>
      </body>
    </html>
  `;
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
  ${buildTopNavCss()}

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
        ${renderTopNav("bugs")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Stage 0 Stability</div>
              <h1>Bug Board</h1>
              <div class="subtitle">Parsing, idempotency, Twilio, DB failures, dashboard/logs, infra, unknown issues.</div>
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

function renderUserTaskWorkspacePage(data) {
  const user = data?.user;
  const counts = data?.counts || {};
  const selectedTab = data?.selectedTab || "pending";
  const tabs = data?.tabs || {};

  const selectedItems = tabs[selectedTab] || [];

  const chip = (key, label, count) => `
    <a
      href="/tasks/user/${user.id}?tab=${key}"
      class="workspace-chip ${selectedTab === key ? "active" : ""}"
    >
      ${label} (${count || 0})
    </a>
  `;

  const taskCardsHtml = selectedItems.length
    ? selectedItems
        .map(
          (task) => `
        <div class="workspace-task-card">
          <div class="workspace-task-top">
            <div>
<a
  href="#"
  class="workspace-task-id-link"
  onclick="event.preventDefault(); event.stopPropagation(); openUserWorkspaceTaskDetail(${Number(task.task_no || task.id)})"
>
                #${escapeHtml(task.task_no || task.id)}
              </a>
            </div>
            <div class="${badgeClass(task.status)}">${escapeHtml(task.status || "")}</div>
          </div>

          <div class="workspace-task-title">${escapeHtml(task.title || "")}</div>

          <div class="workspace-task-meta">
            <div><strong>Business:</strong> ${escapeHtml(task.business || "-")}</div>
            <div><strong>Area:</strong> ${escapeHtml(task.area || "-")}</div>
            <div><strong>Owners:</strong> ${escapeHtml((task.owner_names || []).join(", ") || "-")}</div>
            <div><strong>Priority:</strong> ${escapeHtml(task.priority || "-")}</div>
            <div><strong>Progress:</strong> ${escapeHtml(task.progress ?? 0)}%</div>
            <div><strong>Deadline:</strong> ${escapeHtml(task.deadline || "-")}</div>
            <div><strong>Blocker:</strong> ${escapeHtml(task.blocker_note || "-")}</div>
            <div><strong>Latest update:</strong> ${escapeHtml(task.latest_update_text || "No updates yet")}</div>
            <div><strong>Updated by:</strong> ${escapeHtml(task.latest_updated_by || "-")}</div>
            <div><strong>Updated at:</strong> ${escapeHtml(task.latest_update_at ? formatDateTime(task.latest_update_at) : "-")}</div>
          </div>

          ${
            Array.isArray(task.mini_history) && task.mini_history.length
              ? `
                <div class="workspace-mini-timeline">
                  <div class="workspace-mini-timeline-title">Recent flow</div>
                  ${task.mini_history
                    .map(
                      (item) => `
                    <div class="workspace-mini-timeline-item">
                      <div class="workspace-mini-timeline-time">
                        ${escapeHtml(formatDateTime(item.created_at))}
                      </div>
                      <div class="workspace-mini-timeline-text">
                        ${escapeHtml(renderUserWorkspaceHistoryLine(item))}
                        ${item.changed_by_name ? `<span class="workspace-mini-timeline-by">by ${escapeHtml(item.changed_by_name)}</span>` : ""}
                      </div>
                    </div>
                  `,
                    )
                    .join("")}
                </div>
              `
              : ""
          }
        </div>
      `,
        )
        .join("")
    : `<div class="panel" style="padding:16px;">No items found in this tab.</div>`;

  const historyCardsHtml = selectedItems.length
    ? selectedItems
        .map(
          (item) => `
        <div class="workspace-task-card">
          <div class="workspace-task-top">
            <div>
              <a
                href="#"
                class="workspace-task-id-link"
                onclick="event.preventDefault(); event.stopPropagation(); openUserWorkspaceTaskDetail(${Number(item.task_no || item.task_id)})"
              >
                Task #${escapeHtml(item.task_no || item.task_id)}
              </a>
            </div>
            <div class="muted">${escapeHtml(formatDateTime(item.created_at))}</div>
          </div>

          <div class="workspace-task-title">${escapeHtml(renderUserWorkspaceHistoryLine(item))}</div>

<div class="workspace-task-meta">
  <div><strong>Updated by:</strong> ${escapeHtml(item.changed_by_name || "-")}</div>
  <div><strong>Type:</strong> ${escapeHtml(item.change_type || "-")}</div>
  <div><strong>Field:</strong> ${escapeHtml(item.field_name || "-")}</div>
</div>
        </div>
      `,
        )
        .join("")
    : `<div class="panel" style="padding:16px;">No progress updates found.</div>`;

  return `
    <html>
      <head>
        <title>${escapeHtml(user?.name || "User")} Tasks</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}
          ${buildTopNavCss()}

          .wrap {
            max-width: 1600px;
            margin: 0 auto;
            padding: 24px 18px 36px;
            position: relative;
            z-index: 1;
          }

          .topbar, .panel, .stat-card, .workspace-task-card {
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
          }

          .stat-value {
            margin-top: 10px;
            font-size: 28px;
            font-weight: 700;
          }

          .workspace-chip-row {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-bottom: 18px;
          }

          .workspace-chip {
            display: inline-flex;
            align-items: center;
            padding: 10px 14px;
            border-radius: 999px;
            text-decoration: none;
            color: var(--text);
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.10);
            font-weight: 700;
          }

          .workspace-chip.active {
            background: var(--primary-soft);
            border-color: color-mix(in srgb, var(--primary) 55%, transparent);
            color: var(--text-strong);
          }

          .workspace-list {
            display: grid;
            gap: 12px;
          }

          .workspace-task-card {
            padding: 14px;
          }

          .workspace-task-top {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            margin-bottom: 10px;
          }

          .workspace-task-id {
            color: var(--muted);
            font-size: 12px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

          .workspace-task-title {
            font-size: 18px;
            font-weight: 700;
            margin-bottom: 10px;
          }

          .workspace-task-meta {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 8px 16px;
            color: var(--muted);
            font-size: 14px;
          }
          
                    @media (max-width: 1100px) {
            .workspace-task-meta {
              grid-template-columns: repeat(2, minmax(0, 1fr));
            }

            .workspace-mini-timeline-item {
              grid-template-columns: 1fr;
              gap: 4px;
            }
          }

          @media (max-width: 720px) {
            .workspace-task-meta {
              grid-template-columns: 1fr;
            }
          }
          
                    .workspace-task-id-link {
            color: var(--primary);
            text-decoration: none;
            font-size: 13px;
            font-weight: 800;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

          .workspace-task-id-link:hover {
            text-decoration: underline;
            color: var(--text-strong);
          }

          .workspace-mini-timeline {
            margin-top: 14px;
            padding-top: 12px;
            border-top: 1px solid rgba(255,255,255,0.08);
          }

          .workspace-mini-timeline-title {
            font-size: 12px;
            font-weight: 800;
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 10px;
          }

          .workspace-mini-timeline-item {
            display: grid;
            grid-template-columns: 180px 1fr;
            gap: 12px;
            margin-bottom: 8px;
            align-items: start;
          }

          .workspace-mini-timeline-time {
            font-size: 12px;
            color: var(--muted);
          }

          .workspace-mini-timeline-text {
            font-size: 13px;
            color: var(--text);
            line-height: 1.5;
          }

          .workspace-mini-timeline-by {
            margin-left: 8px;
            color: var(--muted);
            font-size: 12px;
          }

          .back-link {
            display: inline-flex;
            align-items: center;
            text-decoration: none;
            color: var(--text);
            padding: 10px 14px;
            border-radius: 12px;
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.10);
            font-weight: 700;
          }
          
                    .task-modal {
            position: fixed;
            inset: 0;
            background: rgba(3, 8, 20, 0.68);
            backdrop-filter: blur(8px);
            display: none;
            align-items: center;
            justify-content: center;
            padding: 18px;
            z-index: 9999;
          }

          .task-modal.open {
            display: flex;
          }

          .task-modal-card {
            width: min(980px, 100%);
            max-height: 88vh;
            overflow: auto;
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: 22px;
            box-shadow: var(--shadow-soft);
            padding: 18px;
          }

          .task-modal-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            margin-bottom: 14px;
          }

          .task-modal-close {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.12);
            border-radius: 12px;
            color: var(--text);
            padding: 10px 12px;
            cursor: pointer;
            font: inherit;
          }

          .modal-meta-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 12px;
            margin-bottom: 16px;
          }

          .modal-meta-box {
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 14px;
            padding: 12px;
          }

          .modal-meta-label {
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--muted);
            margin-bottom: 6px;
            font-weight: 700;
          }

          .modal-section {
            margin-top: 16px;
          }

          .modal-section h3 {
            margin: 0 0 10px;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--muted);
          }

          .history-item {
            padding: 12px 0;
            border-top: 1px solid rgba(255,255,255,0.08);
          }

          .history-top {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            margin-bottom: 6px;
            font-size: 13px;
          }

          .history-detail {
            white-space: pre-wrap;
            color: var(--text);
            font-size: 13px;
            line-height: 1.5;
          }

          @media (max-width: 900px) {
            .modal-meta-grid {
              grid-template-columns: 1fr;
            }
          }
          
        </style>
      </head>
      <body>
        ${renderTopNav("tasks")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">User Task Workspace</div>
              <h1>${escapeHtml(user?.name || "Unknown user")}</h1>
              <div class="subtitle">Focused task workspace for one user</div>
            </div>
            <a class="back-link" href="/tasks">← Back to Tasks</a>
          </div>

          <div class="stats">
            <div class="stat-card">
              <div class="stat-label">Pending</div>
              <div class="stat-value">${counts.pending || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Blocked</div>
              <div class="stat-value">${counts.blocked || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Done today</div>
              <div class="stat-value">${counts.done_today || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Deleted</div>
              <div class="stat-value">${counts.deleted || 0}</div>
            </div>
          </div>

          <div class="workspace-chip-row">
            ${chip("pending", "Pending", counts.pending)}
            ${chip("blocked", "Blocked", counts.blocked)}
            ${chip("blocked_on_me", "Blocked on me", counts.blocked_on_me)}
            ${chip("done_today", "Done today", counts.done_today)}
            ${chip("deleted", "Deleted", counts.deleted)}
            ${chip("progress_updates", "Progress updates", counts.progress_updates)}
          </div>

          <div class="workspace-list">
            ${selectedTab === "progress_updates" ? historyCardsHtml : taskCardsHtml}
          </div>

                    <div id="taskModal" class="task-modal" onclick="closeUserWorkspaceTaskDetail(event)">
            <div class="task-modal-card" onclick="event.stopPropagation()">
              <div class="task-modal-head">
                <div id="taskModalTitle" style="font-size:22px; font-weight:800;">Task detail</div>
                <button type="button" class="task-modal-close" onclick="closeUserWorkspaceTaskDetail()">Close</button>
              </div>
              <div id="taskModalBody" class="muted">Loading...</div>
            </div>
          </div>
        </div>
        <script>
        
                  function escapeHtmlClient(value) {
            return String(value ?? "")
              .replace(/&/g, "&amp;")
              .replace(/</g, "&lt;")
              .replace(/>/g, "&gt;")
              .replace(/"/g, "&quot;");
          }


          function renderUserWorkspaceTaskHistoryDetail(item) {
            const oldValue = item.oldValue || {};
            const newValue = item.newValue || {};

            if (item.changeType === "progress_change") {
              return "Progress: " + (oldValue.progress ?? 0) + "% → " + (newValue.progress ?? 0) + "%" +
                (newValue.note ? "\\nNote: " + newValue.note : "");
            }

            if (item.changeType === "status_change") {
              return "Status: " + (oldValue.status || "-") + " → " + (newValue.status || "-") +
                (newValue.note ? "\\nNote: " + newValue.note : "");
            }

            if (item.changeType === "owner_change") {
              const oldOwners = Array.isArray(oldValue.owners) ? oldValue.owners.join(", ") : "-";
              const newOwners = Array.isArray(newValue.owners) ? newValue.owners.join(", ") : "-";
              return "Owners: " + oldOwners + " → " + newOwners;
            }

            if (item.changeType === "deadline_change") {
              return "Deadline: " + (oldValue.deadline || "-") + " → " + (newValue.deadline || "-");
            }

            if (item.fieldName === "title") {
              return "Title: " + (oldValue.title || "-") + " → " + (newValue.title || "-");
            }

            if (item.fieldName === "detail") {
              return "Detail updated";
            }

            if (item.fieldName === "priority") {
              return "Priority: " + (oldValue.priority || "-") + " → " + (newValue.priority || "-");
            }

            if (item.fieldName === "business") {
              return "Business: " + (oldValue.business || "-") + " → " + (newValue.business || "-");
            }

            if (item.fieldName === "area") {
              return "Area: " + (oldValue.area || "-") + " → " + (newValue.area || "-");
            }

            if (item.fieldName === "blocker_note") {
              return [
                "Blocker: " + (newValue.blocker_note || "-"),
                newValue.note ? "Note: " + newValue.note : null
              ].filter(Boolean).join("\\n");
            }

            if (item.fieldName) {
              return (item.fieldName || "Field") + ": " +
                JSON.stringify(oldValue) + " → " + JSON.stringify(newValue);
            }

            return JSON.stringify(newValue || {});
          }

          async function openUserWorkspaceTaskDetail(taskNo) {
            const modal = document.getElementById("taskModal");
            const title = document.getElementById("taskModalTitle");
            const body = document.getElementById("taskModalBody");

            if (!modal || !title || !body) return;

            title.textContent = "Task #" + taskNo;
            body.innerHTML = '<div class="muted">Loading task details...</div>';
            modal.classList.add("open");

            try {
              const res = await fetch("/api/reports/task/" + taskNo);
              const json = await res.json();

              if (!json.ok) {
                body.innerHTML =
                  '<div class="muted">' + escapeHtmlClient(json.error || "Failed to load task") + '</div>';
                return;
              }

              const task = json.data || {};
              title.textContent = "#" + (task.taskNo || task.id) + " — " + escapeHtmlClient(task.title || "Untitled");

              const historyHtml = (task.history || []).length
                ? task.history.map(function(item) {
                    return (
                      '<div class="history-item">' +
                        '<div class="history-top">' +
                          '<strong>' + escapeHtmlClient(item.changeType || "-") + '</strong>' +
                          '<span>' + escapeHtmlClient(item.at || "-") + ' • ' + escapeHtmlClient(item.by || "-") + '</span>' +
                        '</div>' +
                        '<div class="history-detail">' + escapeHtmlClient(renderUserWorkspaceTaskHistoryDetail(item)) + '</div>' +
                      '</div>'
                    );
                  }).join("")
                : '<div class="muted">No recent history</div>';

              body.innerHTML =
                '<div class="modal-meta-grid">' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Owners</div><div>' + escapeHtmlClient(((task.owners || []).join(", ") || "-")) + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Status</div><div>' + escapeHtmlClient(task.status || "-") + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Priority</div><div>' + escapeHtmlClient(task.priority || "-") + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Progress</div><div>' + escapeHtmlClient(String(task.progress ?? 0)) + '%</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Deadline</div><div>' + escapeHtmlClient(task.deadline || "-") + '</div></div>' +
                  '<div class="modal-meta-box"><div class="modal-meta-label">Business / Area</div><div>' + escapeHtmlClient((task.business || "-") + " / " + (task.area || "-")) + '</div></div>' +
                '</div>' +

                ((task.detail || task.blockerNote) ? (
                  '<div class="modal-section">' +
                    '<h3>Details</h3>' +
                    (task.detail
                      ? '<div class="modal-meta-box" style="margin-bottom:10px;"><div class="modal-meta-label">Detail</div><div>' + escapeHtmlClient(task.detail) + '</div></div>'
                      : ''
                    ) +
                    (task.blockerNote
                      ? '<div class="modal-meta-box"><div class="modal-meta-label">Blocker</div><div>' + escapeHtmlClient(task.blockerNote) + '</div></div>'
                      : ''
                    ) +
                  '</div>'
                ) : '') +

                '<div class="modal-section">' +
                  '<h3>History</h3>' +
                  historyHtml +
                '</div>';
            } catch (error) {
              body.innerHTML =
                '<div class="muted">' + escapeHtmlClient(error?.message || "Failed to load task") + '</div>';
            }
          }

          function closeUserWorkspaceTaskDetail(event) {
            if (event && event.target && event.target.id !== "taskModal") return;
            const modal = document.getElementById("taskModal");
            if (modal) modal.classList.remove("open");
          }

          setInterval(() => {
            window.location.reload();
          }, 60000);

          document.addEventListener("visibilitychange", () => {
            if (document.visibilityState === "visible") {
              window.location.reload();
            }
          });

          document.addEventListener("keydown", function (event) {
            if (event.key === "Escape") {
              closeUserWorkspaceTaskDetail();
            }
          });
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

function getPostLoginRedirectPath(user) {
  if (isManagerOrAdmin(user)) {
    return "/dashboard";
  }

  return "/my-dashboard";
}

function isManagerOrAdmin(user) {
  return user?.role === "admin" || user?.role === "manager";
}

function generateClientViewToken() {
  return crypto.randomBytes(24).toString("hex");
}

function normalizeSlug(input) {
  return String(input || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function ensureArray(value) {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
}

async function insertClientActivityLog({
  orgId,
  clientId,
  actorUserId,
  action,
  entityType = null,
  entityId = null,
  oldValue = null,
  newValue = null,
}) {
  const { error } = await supabase.from("client_activity_logs").insert([
    {
      org_id: orgId,
      client_id: clientId,
      actor_user_id: actorUserId || null,
      action,
      entity_type: entityType,
      entity_id: entityId,
      old_value: oldValue,
      new_value: newValue,
    },
  ]);

  if (error) {
    console.error("insertClientActivityLog error:", error);
  }
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

function buildTopNavCss() {
  return `
    .top-nav {
      position: sticky;
      top: 0;
      z-index: 100;
      backdrop-filter: blur(18px);
      background: rgba(13, 18, 33, 0.88);
      border-bottom: 1px solid rgba(255,255,255,0.08);
    }

    .top-nav-inner {
      width: 100%;
      margin: 0 auto;
      padding: 10px 14px;
      display: grid;
      grid-template-columns: 56px minmax(0, 1fr) auto;
      align-items: center;
      gap: 12px;
      min-height: 64px;
    }

    .brand {
      width: 42px;
      height: 42px;
      border-radius: 14px;
      display: grid;
      place-items: center;
      font-size: 24px;
      font-weight: 900;
      letter-spacing: -0.06em;
      color: var(--text-strong);
      background: var(--primary-soft);
      border: 1px solid color-mix(in srgb, var(--primary) 55%, transparent);
      text-decoration: none;
      white-space: nowrap;
    }

    .nav-links {
      display: flex;
      align-items: center;
      justify-content: flex-start;
      gap: 8px;
      flex-wrap: nowrap;
      min-width: 0;
      overflow: visible;
    }

    .nav-links a,
    .nav-links button {
      color: var(--text);
      text-decoration: none;
      padding: 9px 13px;
      border-radius: 13px;
      border: 1px solid color-mix(in srgb, var(--secondary) 30%, transparent);
      background: var(--secondary-soft);
      font-weight: 750;
      transition: all 0.15s ease;
      font: inherit;
      cursor: pointer;
      white-space: nowrap;
      flex: 0 0 auto;
      min-height: 40px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }

    .nav-links a:hover,
    .nav-links button:hover {
      color: var(--text-strong);
      border-color: color-mix(in srgb, var(--secondary) 55%, transparent);
      transform: translateY(-1px);
    }

    .nav-links a.active {
      background: var(--primary-soft);
      border-color: color-mix(in srgb, var(--primary) 55%, transparent);
      color: var(--text-strong);
    }

    .nav-icon-link {
      width: 42px;
      padding: 0 !important;
      font-size: 18px;
      font-weight: 900;
    }

    .nav-links a.logout-link {
      background: rgba(255,255,255,0.05);
      border-color: rgba(255,255,255,0.12);
    }
    
.nav-dropdown-wrap {
  position: relative;
  flex: 0 0 auto;
  padding-bottom: 8px;
  margin-bottom: -8px;
}

.nav-dropdown-menu {
  display: none;
  position: absolute;
  top: 100%;
  left: 0;
  min-width: 190px;
  padding: 8px;
  border-radius: 14px;
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  box-shadow: var(--shadow-soft);
  z-index: 9999;
}

.nav-dropdown-wrap:hover .nav-dropdown-menu {
  display: block;
}

.nav-dropdown-menu a {
  display: block !important;
  width: 100%;
  margin: 0;
  padding: 10px 11px !important;
  border-radius: 10px !important;
  background: transparent !important;
  border: 0 !important;
  text-align: left;
}

.nav-dropdown-menu a:hover {
  background: rgba(255,255,255,0.07) !important;
  transform: none !important;
}

    .top-nav-status {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      flex-wrap: nowrap;
      min-width: 0;
    }

    .top-nav-pill {
      padding: 8px 11px;
      border-radius: 999px;
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.10);
      color: var(--text);
      font-size: 12px;
      font-weight: 800;
      white-space: nowrap;
      line-height: 1;
      min-height: 34px;
      display: inline-flex;
      align-items: center;
      max-width: 190px;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .top-nav-pill.loading,
    .top-nav-pill.muted {
      color: var(--muted);
    }

    .quick-action-btn {
      background: var(--primary-soft) !important;
      border-color: color-mix(in srgb, var(--primary) 55%, transparent) !important;
      color: var(--text-strong) !important;
    }

    @media (max-width: 1180px) {
      .nav-text-optional {
        display: none !important;
      }

      .nav-links {
        gap: 7px;
      }
    }

    @media (max-width: 900px) {
      .top-nav-inner {
        grid-template-columns: 48px minmax(0, 1fr);
      }

      .top-nav-status {
        grid-column: 1 / -1;
        justify-content: flex-start;
        overflow-x: auto;
      }

      .nav-links {
        overflow-x: auto;
        scrollbar-width: none;
      }

      .nav-links::-webkit-scrollbar {
        display: none;
      }
    }

    @media (max-width: 640px) {
      .top-nav-inner {
        padding: 9px 10px;
      }

      .nav-links a {
        padding: 8px 10px;
        font-size: 13px;
      }

      .nav-icon-link {
        width: 38px;
      }
    }

    /* keep existing quick action modal styles */
    .quick-action-overlay {
      position: fixed;
      inset: 0;
      background: rgba(3, 8, 20, 0.68);
      backdrop-filter: blur(8px);
      z-index: 9998;
      display: none;
      align-items: center;
      justify-content: center;
      padding: 18px;
    }

    .quick-action-overlay.open { display: flex; }

    .quick-action-modal {
      width: min(920px, 100%);
      max-height: 88vh;
      overflow: auto;
      background: linear-gradient(180deg, var(--panel), var(--panel-strong));
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: var(--shadow-soft);
      padding: 18px;
    }
  `;
}

function renderQuickActionModal() {
  return `
    <button type="button" class="quick-action-btn" onclick="openQuickActionModal()">
      Quick Action
    </button>

    <div id="quickActionOverlay" class="quick-action-overlay" onclick="closeQuickActionModal(event)">
      <div class="quick-action-modal" onclick="event.stopPropagation()">
        <div class="quick-action-head">
          <div class="quick-action-title">Quick Action</div>
          <button type="button" class="quick-action-close" onclick="closeQuickActionModal()">Close</button>
        </div>

        <input
          id="quickActionInput"
          class="quick-action-input"
          placeholder="Type a command like: show task 12"
          autocomplete="off"
        />

        <div class="quick-action-grid">
          <div class="quick-action-panel">
            <h3>Suggested commands</h3>
            <div class="quick-action-chips">
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('show task 2')">show task 2</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('my tasks')">my tasks</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('status')">status</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('login')">login</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('break')">break</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('back')">back</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('logout')">logout</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('progress 2 50 finished API testing properly')">progress 2 50 ...</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('done 2 tested and verified properly')">done 2 ...</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('wait 23 on aj for API response')">wait 23 on aj ...</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('clear wait 23 aj responded')">clear wait 23 ...</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('who is on break')">who is on break</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('who is off today')">who is off today</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('summary today')">summary today</button>
              <button type="button" class="quick-action-chip" onclick="setQuickActionCommand('now')">now</button>
            </div>
          </div>

          <div class="quick-action-panel">
            <h3>Preview</h3>
            <div id="quickActionPreview" class="quick-action-preview muted">Start typing to see a quick preview.</div>
          </div>
        </div>

        <div class="quick-action-panel" style="margin-top:16px;">
          <h3>Result</h3>
          <div id="quickActionResult" class="quick-action-result muted">Nothing run yet.</div>
        </div>

        <div class="quick-action-actions">
          <button type="button" class="quick-action-secondary" onclick="clearQuickAction()">Clear</button>
          <button type="button" class="quick-action-submit" onclick="runQuickAction()">Run command</button>
        </div>
      </div>
    </div>

    <script>
      function openQuickActionModal() {
        const overlay = document.getElementById("quickActionOverlay");
        const input = document.getElementById("quickActionInput");
        if (!overlay || !input) return;
        overlay.classList.add("open");
        setTimeout(() => input.focus(), 0);
      }

      function closeQuickActionModal(event) {
        if (event && event.target && event.target.id !== "quickActionOverlay") return;
        const overlay = document.getElementById("quickActionOverlay");
        if (overlay) overlay.classList.remove("open");
      }

      function setQuickActionCommand(value) {
        const input = document.getElementById("quickActionInput");
        if (!input) return;
        input.value = value;
        updateQuickActionPreview();
        input.focus();
      }

      function clearQuickAction() {
        const input = document.getElementById("quickActionInput");
        const preview = document.getElementById("quickActionPreview");
        const result = document.getElementById("quickActionResult");
        if (input) input.value = "";
        if (preview) {
          preview.textContent = "Start typing to see a quick preview.";
          preview.classList.add("muted");
        }
        if (result) {
          result.textContent = "Nothing run yet.";
          result.classList.add("muted");
        }
      }

      function updateQuickActionPreview() {
        const input = document.getElementById("quickActionInput");
        const preview = document.getElementById("quickActionPreview");
        if (!input || !preview) return;

        const value = String(input.value || "").trim();

        if (!value) {
          preview.textContent = "Start typing to see a quick preview.";
          preview.classList.add("muted");
          return;
        }

        preview.classList.remove("muted");

        if (/^show\\s+task\\s+\\d+$/i.test(value)) {
          preview.textContent = "Will fetch and display one task.";
          return;
        }

        if (/^my\\s+tasks$/i.test(value)) {
          preview.textContent = "Will fetch your open tasks.";
          return;
        }

        if (/^status$/i.test(value)) {
          preview.textContent = "Will fetch your current attendance status.";
          return;
        }

        if (/^(login|break|back|logout)$/i.test(value)) {
          preview.textContent = "Will update your attendance.";
          return;
        }

        if (/^progress\\s+\\d+\\s+\\d{1,3}%?\\s+.+$/i.test(value)) {
          preview.textContent = "Will update task progress and save task history.";
          return;
        }

        if (/^done\\s+\\d+\\s+.+$/i.test(value)) {
          preview.textContent = "Will mark task done and set progress to 100%.";
          return;
        }

        if (/^wait\\s+\\d+\\s+on\\s+.+\\s+for\\s+.+$/i.test(value)) {
          preview.textContent = "Will mark the task blocked and set waiting-on user.";
          return;
        }

        if (/^clear\\s+wait\\s+\\d+(?:\\s+.+)?$/i.test(value)) {
          preview.textContent = "Will clear waiting/blocker state and reopen the task.";
          return;
        }

        if (/^(who\\s+is\\s+on\\s+break|who\\s+is\\s+off\\s+today|summary\\s+today|now)$/i.test(value)) {
          preview.textContent = "Will run a team summary command.";
          return;
        }

        preview.textContent = "Will send this command to the server.";
      }

      async function runQuickAction() {
        const input = document.getElementById("quickActionInput");
        const result = document.getElementById("quickActionResult");

        if (!input || !result) return;

        const command = String(input.value || "").trim();
        if (!command) {
          result.textContent = "Please type a command first.";
          result.classList.remove("muted");
          return;
        }

        result.textContent = "Running...";
        result.classList.remove("muted");

        try {
          const res = await fetch("/api/command-palette/run", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ command })
          });

          const json = await res.json();

          if (!json.ok) {
            result.textContent = json.error || "Failed to run command.";
            return;
          }

          result.textContent = json.data?.message || "Done.";

          if (json.data?.reload) {
            setTimeout(() => window.location.reload(), 700);
          }
        } catch (error) {
          result.textContent = error?.message || "Something went wrong.";
        }
      }

      document.addEventListener("keydown", function (event) {
        const isMac = navigator.platform.toUpperCase().includes("MAC");
        const hotkey = (isMac ? event.metaKey : event.ctrlKey) && event.key.toLowerCase() === "k";

        if (hotkey) {
          event.preventDefault();
          openQuickActionModal();
        }

        if (event.key === "Escape") {
          closeQuickActionModal();
        }
      });

      document.addEventListener("input", function (event) {
        if (event.target && event.target.id === "quickActionInput") {
          updateQuickActionPreview();
        }
      });
    </script>
  `;
}

function renderClientsListPage({ clients = [], summary = {} } = {}) {
  const rowsHtml = clients.length
    ? clients
        .map((client) => {
          const serviceNames = (client.service_names || []).join(", ") || "-";

          return `
            <tr>
              <td>
                <div style="font-weight:800;">
                  <a href="/clients/${client.id}" style="color:var(--text-strong); text-decoration:none;">
                    ${escapeHtml(client.name)}
                  </a>
                </div>
                <div class="muted">${escapeHtml(client.company_name || "-")}</div>
              </td>
              <td>${escapeHtml(serviceNames)}</td>
              <td>${escapeHtml(client.project_manager_name || "-")}</td>
              <td><span class="badge badge-info">${escapeHtml(client.status || "-")}</span></td>
              <td><span class="${client.health_status === "at_risk" ? "badge badge-danger" : client.health_status === "watch" ? "badge badge-warn" : "badge badge-ok"}">${escapeHtml(client.health_status || "-")}</span></td>
              <td>${escapeHtml(client.open_work_count || 0)}</td>
              <td>${escapeHtml(client.waiting_count || 0)}</td>
              <td>${escapeHtml(client.last_update_text || "-")}</td>
<td>
  <button class="action-kebab" type="button" onclick="toggleClientActionsMenu(event, ${Number(client.id)})">⋯</button>

  <div id="clientActionsMenu-${Number(client.id)}" class="floating-actions-menu">
    <a href="/clients/${client.id}/edit">Edit Client</a>
    <a href="/clients/${client.id}/reset">Reset Workspace</a>
    <a href="/clients/${client.id}">Open Workspace</a>
  </div>
</td>
            </tr>
          `;
        })
        .join("")
    : `
      <tr>
        <td colspan="9" class="empty">
          No clients yet. Click “New Client” to start.
        </td>
      </tr>
    `;

  return `
    <html>
      <head>
        <title>Clients | WeSolveHR</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}
          ${buildTopNavCss()}

          .wrap {
            max-width: 1600px;
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

          .action-btn {
            display: inline-flex;
            align-items: center;
            text-decoration: none;
            color: var(--text-strong);
            padding: 11px 14px;
            border-radius: 12px;
            background: var(--primary-soft);
            border: 1px solid color-mix(in srgb, var(--primary) 55%, transparent);
            font-weight: 800;
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
          }

          .stat-value {
            margin-top: 10px;
            font-size: 28px;
            font-weight: 800;
          }

          table {
            width: 100%;
            border-collapse: collapse;
          }

          th, td {
            padding: 14px;
            border-bottom: 1px solid rgba(255,255,255,0.08);
            text-align: left;
            font-size: 14px;
            vertical-align: top;
          }

          th {
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-size: 12px;
          }

          .empty {
            padding: 28px;
            text-align: center;
            color: var(--muted);
          }

          .small-link {
            display:inline-flex;
            margin-right:8px;
            color:var(--primary);
            font-weight:800;
            text-decoration:none;
          }
          
                
.action-kebab {
  font-size: 18px;
  background: transparent;
  border: none;
  cursor: pointer;
  color: var(--text);
}

.floating-actions-menu {
  display: none !important;   /* <-- important fix */
  position: fixed;
  min-width: 180px;
  z-index: 9999;
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  border-radius: 14px;
  box-shadow: var(--shadow-soft);
  padding: 8px;
}

.floating-actions-menu.open {
  display: block !important;
}

.floating-actions-menu a {
  display: block;
  padding: 10px 11px;
  border-radius: 10px;
  color: var(--text);
  text-decoration: none;
  font-weight: 700;
  white-space: nowrap;
}

.floating-actions-menu a:hover {
  background: rgba(255,255,255,0.07);
}

          .badge {
            display:inline-flex;
            padding:6px 9px;
            border-radius:999px;
            font-size:12px;
            font-weight:800;
          }

          .badge-ok {
            background: var(--success-soft);
            color: var(--text-strong);
          }

          .badge-warn {
            background: var(--accent-soft);
            color: var(--text-strong);
          }

          .badge-danger {
            background: var(--danger-soft);
            color: var(--text-strong);
          }

          .badge-info {
            background: var(--info-soft);
            color: var(--text-strong);
          }

          @media (max-width: 900px) {
            .stats {
              grid-template-columns: 1fr;
            }

            .panel {
              overflow-x: auto;
            }
          }
        </style>
      </head>
      <body>
        ${renderTopNav("clients")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Client Workspace</div>
              <h1>Clients</h1>
              <div class="subtitle">Internal consulting CRM layer for client work, updates, actions, documents, and progress.</div>
            </div>

            <a class="action-btn" href="/clients/new">+ New Client</a>
          </div>

          <div class="stats">
            <div class="stat-card">
              <div class="stat-label">Total Clients</div>
              <div class="stat-value">${summary.total || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Active</div>
              <div class="stat-value">${summary.active || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Waiting on Client</div>
              <div class="stat-value">${summary.waiting || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">At Risk</div>
              <div class="stat-value">${summary.atRisk || 0}</div>
            </div>
          </div>

          <div class="panel">
            <table>
              <thead>
                <tr>
                  <th>Client</th>
                  <th>Services</th>
                  <th>Project Manager</th>
                  <th>Status</th>
                  <th>Health</th>
                  <th>Open Work</th>
                  <th>Waiting</th>
                  <th>Last Update</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                ${rowsHtml}
              </tbody>
            </table>
          </div>
        </div>
        <script>
  function toggleClientActionsMenu(event, clientId) {
    event.stopPropagation();

    document.querySelectorAll(".floating-actions-menu.open").forEach(function(menu) {
      if (menu.id !== "clientActionsMenu-" + clientId) {
        menu.classList.remove("open");
      }
    });

    const menu = document.getElementById("clientActionsMenu-" + clientId);
    if (!menu) return;

    const rect = event.currentTarget.getBoundingClientRect();

    menu.style.top = rect.bottom + 6 + "px";
    menu.style.left = Math.max(12, rect.right - 180) + "px";
    menu.classList.toggle("open");
  }

  document.addEventListener("click", function() {
    document.querySelectorAll(".floating-actions-menu.open").forEach(function(menu) {
      menu.classList.remove("open");
    });
  });

  document.addEventListener("keydown", function(event) {
    if (event.key === "Escape") {
      document.querySelectorAll(".floating-actions-menu.open").forEach(function(menu) {
        menu.classList.remove("open");
      });
    }
  });
</script>
      </body>
    </html>
  `;
}

function renderNewClientPage({ users = [] }) {
  return `
    <html>
      <head>
        <title>New Client | WeSolveHR</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}
          ${buildTopNavCss()}

          .wrap {
            max-width: 1200px;
            margin: 0 auto;
            padding: 24px 18px 36px;
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

          .panel {
            padding: 20px;
            margin-bottom: 16px;
          }

          .eyebrow {
            font-size: 11px;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            color: var(--primary);
            font-weight: 700;
            margin-bottom: 8px;
          }

          h1 {
            margin: 0;
            font-size: 30px;
            letter-spacing: -0.04em;
          }

          h2 {
            margin: 0 0 14px;
            font-size: 18px;
          }

          .subtitle {
            color: var(--muted);
            margin-top: 8px;
            font-size: 14px;
          }

          .grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 14px;
          }

          .field {
            display: flex;
            flex-direction: column;
            gap: 8px;
          }

          label {
            font-size: 13px;
            font-weight: 800;
          }

          input, select, textarea {
            width: 100%;
            padding: 12px 13px;
            border-radius: 12px;
            border: 1px solid var(--line);
            background: rgba(255,255,255,0.04);
            color: var(--text);
            font: inherit;
          }

          textarea {
            min-height: 90px;
            resize: vertical;
          }

          .hint {
            color: var(--muted);
            font-size: 12px;
            line-height: 1.5;
          }

          .actions {
            display: flex;
            gap: 10px;
            justify-content: flex-end;
            margin-top: 18px;
          }

          .btn {
            padding: 11px 14px;
            border-radius: 12px;
            border: 1px solid rgba(255,255,255,0.12);
            color: var(--text);
            background: rgba(255,255,255,0.05);
            font-weight: 800;
            text-decoration: none;
            cursor: pointer;
          }

          .btn-primary {
            background: var(--primary-soft);
            color: var(--text-strong);
            border-color: color-mix(in srgb, var(--primary) 55%, transparent);
          }
          



          @media (max-width: 800px) {
            .grid {
              grid-template-columns: 1fr;
            }
          }
        </style>
      </head>
      <body>
        ${renderTopNav("clients")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Create Client</div>
              <h1>New Client</h1>
              <div class="subtitle">This page is the starting shell. Database save will come after DB tables are added.</div>
            </div>
            <a class="btn" href="/clients">← Back to Clients</a>
          </div>

          <form method="POST" action="/api/clients">
            <div class="panel">
              <h2>Basic Info</h2>
              <div class="grid">
                <div class="field">
                  <label>Client Name</label>
                  <input name="name" placeholder="Example: Everloop" />
                </div>

                <div class="field">
                  <label>Company Name</label>
                  <input name="company_name" placeholder="Example: Everloop AI Inc." />
                </div>

                <div class="field">
                  <label>Slug</label>
                  <input name="slug" placeholder="example: SomeAi" />
                  <div class="hint">This will be used later in the client view link.</div>
                </div>
                
                <div class="field">
  <label>Google Drive Folder Link</label>
  <input
    name="google_drive_folder_url"
    placeholder="Paste main Google Drive client folder link"
    required
  />
  <div class="hint">
    Create one Google Drive folder for this client and paste the folder link here.
  </div>
</div>

                <div class="field">
                  <label>Status</label>
                  <select name="status">
                    <option value="active">Active</option>
                    <option value="paused">Paused</option>
                    <option value="onboarding">Onboarding</option>
                    <option value="completed">Completed</option>
                    <option value="inactive">Inactive</option>
                  </select>
                </div>

                <div class="field">
                  <label>Health</label>
                  <select name="health_status">
                    <option value="healthy">Healthy</option>
                    <option value="watch">Watch</option>
                    <option value="at_risk">At Risk</option>
                  </select>
                </div>

                <div class="field">
                  <label>Start Date</label>
                  <input type="date" name="start_date" />
                </div>
              </div>

              <div class="field" style="margin-top:14px;">
                <label>Description</label>
                <textarea name="description" placeholder="Short internal description of this client relationship..."></textarea>
              </div>
            </div>

            <div class="panel">
              <h2>Services</h2>
              <div class="grid">
                <label><input type="checkbox" name="services" value="Tech" /> Tech</label>
                <label><input type="checkbox" name="services" value="Sales" /> Sales</label>
                <label><input type="checkbox" name="services" value="Marketing" /> Marketing</label>
                <label><input type="checkbox" name="services" value="GTM" /> GTM</label>
                <label><input type="checkbox" name="services" value="Design" /> Design</label>
                <label><input type="checkbox" name="services" value="QA" /> QA</label>
                <label><input type="checkbox" name="services" value="Operations" /> Operations</label>
                <label><input type="checkbox" name="services" value="Support" /> Support</label>
              </div>
            </div>

            <div class="panel">
              <h2>Primary Client Contact</h2>
              <div class="grid">
                <div class="field">
                  <label>Name</label>
                  <input name="contact_name" placeholder="Client contact name" />
                </div>
        

                <div class="field">
                  <label>Email</label>
                  <input name="contact_email" placeholder="email@example.com" />
                </div>

                <div class="field">
                  <label>Phone</label>
                  <input name="contact_phone" placeholder="+1..." />
                </div>

                <div class="field">
                  <label>Role</label>
                  <input name="contact_role" placeholder="Founder / CEO / PM" />
                </div>
              </div>
            </div>
            
            <div class="panel">
  <h2>Additional Client Contacts</h2>
  <div class="grid">
    <div class="field">
      <label>Contact 2 Name</label>
      <input name="contact_2_name" placeholder="Optional" />
    </div>

    <div class="field">
      <label>Contact 2 Email</label>
      <input name="contact_2_email" placeholder="Optional" />
    </div>

    <div class="field">
      <label>Contact 2 Phone</label>
      <input name="contact_2_phone" placeholder="Optional" />
    </div>

    <div class="field">
      <label>Contact 2 Role</label>
      <input name="contact_2_role" placeholder="Founder / PM / Finance / Marketing" />
    </div>
  </div>
</div>

<div class="panel">
  <h2>Internal Ownership</h2>
  <div class="grid">
    <div class="field">
      <label>Account Manager</label>
<select name="account_manager_user_id">
  <option value="">Select account manager</option>
  ${users.map((u) => `<option value="${u.id}">${u.name}</option>`).join("")}
</select>
      <div class="hint">Later this will load active users from WeSolveHR.</div>
    </div>

    <div class="field">
      <label>Project Manager</label>
<select name="project_manager_user_id">
  <option value="">Select project manager</option>
  ${users.map((u) => `<option value="${u.id}">${u.name}</option>`).join("")}
</select>
      <div class="hint">Later this will load active users from WeSolveHR.</div>
    </div>
  </div>
</div>

<div class="panel">
  <div class="actions">
    <a class="btn" href="/clients">Cancel</a>
    <button class="btn btn-primary" type="submit">Create Client</button>
  </div>
</div>
</form>
        </div>
      </body>
    </html>
  `;
}

function renderClientWorkspacePage({
  client,
  contacts = [],
  services = [],
  workItems = [],
  updates = [],
  actions = [],
  contributors = [],
  milestones = [],
  documents = [],
  users = [],
  selectedTab = "overview",
  activityLogs = [],
}) {
  const activeTab = [
    "overview",
    "work",
    "updates",
    "actions",
    "contributors",
    "milestones",
    "documents",
  ].includes(selectedTab)
    ? selectedTab
    : "overview";

  const tabLink = (key, label) => `
    <a class="tab ${activeTab === key ? "active" : ""}" href="/clients/${Number(client.id)}?tab=${key}">
      ${label}
    </a>
  `;

  const getUserName = (userId) =>
    users.find((u) => String(u.id) === String(userId))?.name || "-";

  const getWorkItemTitle = (workItemId) =>
    workItems.find((w) => String(w.id) === String(workItemId))?.title || "";
  const getMilestoneTitle = (milestoneId) =>
    milestones.find((m) => String(m.id) === String(milestoneId))?.title || "";
  const manualUpdateEvents = updates.map((u) => ({
    type: "manual_update",
    at: u.created_at,
    title: u.title || "Client update",
    text: u.update_text || "",
    by: getUserName(u.created_by_user_id),
    relatedWorkItemTitle: u.related_work_item_id
      ? getWorkItemTitle(u.related_work_item_id)
      : "",
  }));

  const activityEvents = activityLogs.map((log) => {
    const actionLabel = String(log.action || "").replaceAll("_", " ");
    const newValue = log.new_value || {};
    const oldValue = log.old_value || {};

    let text = actionLabel;

    if (log.action === "work_item_created") {
      text = `Created work item: ${newValue.title || "-"}`;
    }

    if (log.action === "work_item_updated") {
      if (oldValue.status !== newValue.status) {
        text = `Status changed: ${oldValue.status || "-"} → ${newValue.status || "-"}`;
      } else {
        text = `Updated work item: ${newValue.title || "-"}`;
      }
    }

    if (log.action === "work_item_archived") {
      text = `Archived work item: ${oldValue.title || newValue.title || "-"}`;
    }

    return {
      type: "activity",
      at: log.created_at,
      title: actionLabel,
      text,
      by: getUserName(log.actor_user_id),
      relatedWorkItemTitle:
        log.entity_type === "client_work_items"
          ? getWorkItemTitle(log.entity_id)
          : "",
    };
  });

  const timelineEvents = [...manualUpdateEvents, ...activityEvents]
    .filter((x) => x.at)
    .sort((a, b) => new Date(b.at).getTime() - new Date(a.at).getTime());

  return `
    <html>
      <head>
        <title>${escapeHtml(client?.name || "Client")} | WeSolveHR</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}
          ${buildTopNavCss()}

          .wrap { max-width: 1600px; margin: 0 auto; padding: 24px 18px 36px; }

          .topbar, .panel, .stat-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
          }

          .topbar {
            display:flex; justify-content:space-between; align-items:center;
            gap:16px; flex-wrap:wrap; margin-bottom:20px; padding:18px 20px;
          }

          h1 { margin:0; font-size:30px; letter-spacing:-0.04em; }
          .subtitle, .meta { color:var(--muted); font-size:13px; line-height:1.5; }
          .subtitle { margin-top:8px; font-size:14px; }

          .eyebrow {
            font-size:11px; letter-spacing:0.16em; text-transform:uppercase;
            color:var(--primary); font-weight:700; margin-bottom:8px;
          }

          .btn {
            display:inline-flex; align-items:center; text-decoration:none; color:var(--text);
            padding:10px 13px; border-radius:12px; background:rgba(255,255,255,0.05);
            border:1px solid rgba(255,255,255,0.12); font-weight:800; cursor:pointer;
          }

          .btn-primary {
            background:var(--primary-soft); color:var(--text-strong);
            border-color:color-mix(in srgb, var(--primary) 55%, transparent);
          }

          .stats {
            display:grid; grid-template-columns:repeat(4, minmax(0, 1fr));
            gap:12px; margin-bottom:20px;
          }

          .grid-2 {
            display:grid; grid-template-columns:1fr 1fr;
            gap:16px; margin-bottom:20px;
          }

          .stat-card, .panel { padding:18px; }
          .panel {
  margin-bottom: 16px;
  width: 100%;
}

.tab-content-wrap {
  width: 100%;
}
          .panel h2 { margin:0 0 14px; font-size:18px; }

          .stat-label {
            color:var(--muted); font-size:12px; text-transform:uppercase;
            letter-spacing:0.08em; font-weight:700;
          }

          .stat-value { margin-top:10px; font-size:26px; font-weight:800; }

.tabs {
  width: 100%;
  display: grid;
  grid-template-columns: repeat(7, minmax(0, 1fr));
  gap: 0;
  margin: 0 0 18px;
  background: rgba(255,255,255,0.035);
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: 16px;
  padding: 6px;
}

.tab {
  min-height: 44px;
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  padding: 9px 10px;
  border-radius: 12px;
  font-size: 13px;
  font-weight: 800;
  text-decoration: none;
  color: var(--muted);
  border: 1px solid transparent;
  white-space: nowrap;
}

.tab:hover {
  color: var(--text-strong);
  background: rgba(255,255,255,0.045);
}

.tab.active {
  background: var(--primary-soft);
  border-color: color-mix(in srgb, var(--primary) 55%, transparent);
  color: var(--text-strong);
}

@media (max-width: 1100px) {
  .tabs {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }
}

@media (max-width: 700px) {
  .tabs {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
          
          .work-table {
  width: 100%;
  border-collapse: collapse;
}

.work-table th,
.work-table td {
  padding: 12px;
  border-bottom: 1px solid rgba(255,255,255,0.08);
  text-align: left;
  vertical-align: top;
  font-size: 13px;
}

.work-table th {
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 11px;
}

.badge {
  display:inline-flex;
  padding:6px 9px;
  border-radius:999px;
  font-size:12px;
  font-weight:800;
}

.badge-ok {
  background: var(--success-soft);
  color: var(--text-strong);
}

.badge-info {
  background: var(--info-soft);
  color: var(--text-strong);
}

.badge-warn {
  background: var(--accent-soft);
  color: var(--text-strong);
}

.badge-muted {
  background: rgba(255,255,255,0.08);
  color: var(--text);
}

.work-summary-chips {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-top: 10px;
}

.summary-chip {
  display: inline-flex;
  padding: 8px 11px;
  border-radius: 999px;
  background: rgba(255,255,255,0.06);
  border: 1px solid rgba(255,255,255,0.10);
  color: var(--text);
  font-size: 12px;
  font-weight: 800;
}

.section-head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
  margin-bottom: 14px;
}

.section-title {
  margin: 0;
  font-size: 18px;
  font-weight: 900;
}

.section-subtitle {
  margin-top: 4px;
  color: var(--muted);
  font-size: 13px;
}

.standard-list {
  display: grid;
  gap: 12px;
}

.standard-card {
  padding: 15px;
  border-radius: 16px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.035);
}

.standard-card-top {
  display: flex;
  justify-content: space-between;
  gap: 14px;
  align-items: flex-start;
}

.standard-card-title {
  font-size: 16px;
  font-weight: 900;
  margin-bottom: 5px;
}

.standard-card-meta {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px 14px;
  color: var(--muted);
  font-size: 13px;
  margin-top: 12px;
}

.standard-card-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-top: 14px;
}

@media (max-width: 1000px) {
  .standard-card-meta {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 700px) {
  .section-head,
  .standard-card-top {
    flex-direction: column;
  }

  .standard-card-meta {
    grid-template-columns: 1fr;
  }
}

.work-card-list {
  display: grid;
  gap: 12px;
}

.work-card {
  padding: 14px;
  border-radius: 16px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.035);
}

.work-card-top {
  display: flex;
  justify-content: space-between;
  gap: 14px;
  align-items: flex-start;
  margin-bottom: 12px;
}

.work-card-title {
  font-size: 16px;
  font-weight: 900;
  margin-bottom: 6px;
}

.work-card-meta {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px 14px;
  color: var(--muted);
  font-size: 13px;
  margin-top: 10px;
}

.work-card-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-top: 14px;
}

@media (max-width: 900px) {
  .work-card-top {
    flex-direction: column;
  }

  .work-card-meta {
    grid-template-columns: 1fr;
  }
}

.badge-danger {
  background: var(--danger-soft);
  color: var(--text-strong);
}

.work-modal {
  display: none;
  position: fixed;
  z-index: 1000;
  inset: 0;
  background: rgba(0,0,0,0.4);
  justify-content: center;
  align-items: center;
}

.work-modal.open {
  display: flex;
}

.work-modal-card {
  width: min(900px, 100%);
  max-height: 88vh;
  overflow: auto;
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  border-radius: 22px;
  box-shadow: var(--shadow-soft);
  padding: 18px;
}

.form-grid {
  display:grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.form-field {
  display:flex;
  flex-direction:column;
  gap:8px;
}

.form-field input,
.form-field select,
.form-field textarea {
  width:100%;
  padding:12px;
  border-radius:12px;
  border:1px solid var(--line);
  background:rgba(255,255,255,0.04);
  color:var(--text);
  font:inherit;
}

.form-field textarea {
  min-height:90px;
}

          .item { padding:12px 0; border-top:1px solid rgba(255,255,255,0.08); }
          .item:first-child { border-top:0; }
          .item-title { font-weight:800; margin-bottom:6px; }

          a { color: var(--primary); }

          @media (max-width: 900px) {
            .stats, .grid-2 { grid-template-columns:1fr; }
          }
        </style>
      </head>

      <body>
        ${renderTopNav("clients")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Client Workspace</div>
              <h1>${escapeHtml(client.name)}</h1>
              <div class="subtitle">
                ${escapeHtml(client.company_name || "-")} · ${escapeHtml(client.status || "-")} · ${escapeHtml(client.health_status || "-")}
              </div>
            </div>

<div style="display:flex; gap:10px; flex-wrap:wrap;">
  <a class="btn" href="/clients">← Clients</a>
  ${
    client.google_drive_folder_url
      ? `<a class="btn" href="${escapeHtml(client.google_drive_folder_url)}" target="_blank" rel="noopener noreferrer">📁 Open Drive</a>`
      : ""
  }
  <button class="btn" type="button" onclick="generateClientViewLink()">Generate Client Link</button>
  <a class="btn btn-primary" href="/clients/${client.id}/edit">Edit Client</a>
  <a class="btn" href="/clients/${client.id}/reset">Reset</a>
</div>
</div>

          <div class="stats">
            <div class="stat-card"><div class="stat-label">Services</div><div class="stat-value">${services.length}</div></div>
            <div class="stat-card"><div class="stat-label">Open Work</div><div class="stat-value">${workItems.length}</div></div>
            <div class="stat-card"><div class="stat-label">Actions Needed</div><div class="stat-value">${actions.length}</div></div>
            <div class="stat-card"><div class="stat-label">Contacts</div><div class="stat-value">${contacts.length}</div></div>
          </div>

<div class="tabs">
  ${tabLink("overview", "Overview")}
  ${tabLink("work", "Work Items")}
  ${tabLink("updates", "Updates")}
  ${tabLink("actions", "Actions Needed")}
  ${tabLink("contributors", "Contributors")}
  ${tabLink("milestones", "Milestones")}
  ${tabLink("documents", "Documents")}
</div>
<div class="tab-content-wrap">

${
  activeTab === "overview"
    ? `
      <div class="grid-2">
        <div class="panel">
          <h2>Overview</h2>
          <div class="meta"><strong>Description:</strong> ${escapeHtml(client.description || "-")}</div>
          <div class="meta"><strong>Start Date:</strong> ${escapeHtml(client.start_date || "-")}</div>
          <div class="meta"><strong>Slug:</strong> ${escapeHtml(client.slug || "-")}</div>
          <div class="meta"><strong>Account Manager:</strong> ${escapeHtml(client.account_manager_name || "-")}</div>
          <div class="meta"><strong>Project Manager:</strong> ${escapeHtml(client.project_manager_name || "-")}</div>
          <div class="meta">
  <strong>Last Activity:</strong>
  ${
    timelineEvents.length
      ? `${escapeHtml(formatDateTime(timelineEvents[0].at))} · ${escapeHtml(timelineEvents[0].text)}`
      : "-"
  }
</div>
          <div class="meta">
            <strong>Google Drive Folder:</strong>
            ${
              client.google_drive_folder_url
                ? `<a href="${escapeHtml(client.google_drive_folder_url)}" target="_blank" rel="noopener noreferrer">📁 Open Client Folder</a>`
                : `<span style="color: var(--danger);">Not set</span>`
            }
          </div>
        </div>

        <div class="panel">
          <h2>Services</h2>
          ${
            services.length
              ? services
                  .map(
                    (s) =>
                      `<div class="item"><div class="item-title">${escapeHtml(s.name)}</div></div>`,
                  )
                  .join("")
              : `<div class="meta">No services selected.</div>`
          }
        </div>
      </div>

      <div class="grid-2">
        <div class="panel">
          <h2>Client Contacts</h2>
          ${
            contacts.length
              ? contacts
                  .map(
                    (c) => `
                <div class="item">
                  <div class="item-title">${escapeHtml(c.name || "-")} ${c.is_primary ? "· Primary" : ""}</div>
                  <div class="meta">${escapeHtml(c.role || "-")}</div>
                  <div class="meta">${escapeHtml(c.email || "-")} · ${escapeHtml(c.phone || "-")}</div>
                </div>
              `,
                  )
                  .join("")
              : `<div class="meta">No contacts added.</div>`
          }
        </div>

        <div class="panel">
          <h2>Recent Updates</h2>
          ${
            updates.length
              ? updates
                  .map(
                    (u) => `
                <div class="item">
                  <div class="item-title">${escapeHtml(u.title || "Update")}</div>
                  <div class="meta">${escapeHtml(u.update_text || "")}</div>
                </div>
              `,
                  )
                  .join("")
              : `<div class="meta">No updates yet.</div>`
          }
        </div>
      </div>
    `
    : ""
}

${
  activeTab === "work"
    ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Work Items</h2>
            <div class="work-summary-chips">
              <span class="summary-chip">All ${workItems.length}</span>
              <span class="summary-chip">Todo ${workItems.filter((w) => w.status === "todo").length}</span>
              <span class="summary-chip">In Progress ${workItems.filter((w) => w.status === "in_progress").length}</span>
              <span class="summary-chip">Done ${workItems.filter((w) => w.status === "done").length}</span>
              <span class="summary-chip">Blocked ${
                workItems.filter((w) => {
                  if (!w.dependency_work_item_id) return false;
                  const dep = workItems.find(
                    (x) => String(x.id) === String(w.dependency_work_item_id),
                  );
                  return dep && dep.status !== "done";
                }).length
              }</span>
            </div>
          </div>

          <button class="btn btn-primary" type="button" onclick="openWorkItemModal()">+ Add Work Item</button>
        </div>

        <div class="work-card-list">
          ${
            workItems.length
              ? workItems
                  .map((w) => {
                    const ownerName =
                      users.find(
                        (u) => String(u.id) === String(w.owner_user_id),
                      )?.name || "-";

                    const dep = w.dependency_work_item_id
                      ? workItems.find(
                          (x) =>
                            String(x.id) === String(w.dependency_work_item_id),
                        )
                      : null;

                    const isBlockedByDependency = dep && dep.status !== "done";

                    const statusClass =
                      w.status === "done"
                        ? "badge badge-ok"
                        : w.status === "in_progress"
                          ? "badge badge-info"
                          : isBlockedByDependency
                            ? "badge badge-warn"
                            : "badge badge-muted";

                    const dependencyText = dep
                      ? isBlockedByDependency
                        ? `Blocked by #${escapeHtml(dep.id)} · ${escapeHtml(dep.title)}`
                        : `Dependency complete: #${escapeHtml(dep.id)} · ${escapeHtml(dep.title)}`
                      : "No dependency";

                    return `
                    <div class="work-card">
                      <div class="work-card-top">
                        <div>
                          <div class="work-card-title">${escapeHtml(w.title || "Untitled")}</div>
                          <div class="meta">${escapeHtml(w.description || "No description")}</div>
                        </div>

                        <div style="display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end;">
                          <span class="${statusClass}">
                            ${isBlockedByDependency && w.status !== "done" ? "blocked" : escapeHtml(w.status || "todo")}
                          </span>
                          <span class="badge badge-muted">${escapeHtml(w.priority || "medium")}</span>
                        </div>
                      </div>

                      <div class="work-card-meta">
                        <div><strong>Owner:</strong> ${escapeHtml(ownerName)}</div>
                        <div><strong>Due:</strong> ${escapeHtml(w.due_date || "-")}</div>
                        <div><strong>Depends:</strong> ${dependencyText}</div>
                        <div><strong>Milestone:</strong> ${escapeHtml(w.milestone_id ? getMilestoneTitle(w.milestone_id) : "-")}</div>
                        <div><strong>Last updated:</strong> ${escapeHtml(w.updated_at ? formatDateTime(w.updated_at) : "-")}</div>
                      </div>

                      <div class="work-card-actions">
                        <button class="btn" type="button" onclick="openWorkItemDetail(${Number(w.id)})">Open / Edit</button>
                        <button class="btn" type="button" onclick="quickUpdateWorkItem(${Number(w.id)}, 'in_progress')">Start</button>
                        <button class="btn" type="button" onclick="quickUpdateWorkItem(${Number(w.id)}, 'done')">Done</button>
                        <button class="btn" type="button" onclick="archiveWorkItem(${Number(w.id)})">Archive</button>
                      </div>
                    </div>
                  `;
                  })
                  .join("")
              : `<div class="meta">No work items yet. Add the first work item for this client.</div>`
          }
        </div>
      </div>
    `
    : ""
}

${
  activeTab === "updates"
    ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Updates / Progress Timeline</h2>
            <div class="meta">Manual client updates + automatic work-item activity.</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openClientUpdateModal()">+ Add Update</button>
        </div>

        <div class="work-summary-chips">
          <span class="summary-chip">Manual Updates ${updates.length}</span>
          <span class="summary-chip">Activity Logs ${activityLogs.length}</span>
          <span class="summary-chip">Timeline ${timelineEvents.length}</span>
        </div>

        <div style="margin-top:16px;">
          ${
            timelineEvents.length
              ? timelineEvents
                  .map(
                    (event) => `
                <div class="item">
                  <div class="item-title">
                    ${escapeHtml(event.title)}
                    ${event.relatedWorkItemTitle ? ` · ${escapeHtml(event.relatedWorkItemTitle)}` : ""}
                  </div>
                  <div class="meta">${escapeHtml(event.text)}</div>
                  <div class="meta">
                    ${escapeHtml(event.at ? formatDateTime(event.at) : "-")}
                    · by ${escapeHtml(event.by || "-")}
                    · ${escapeHtml(event.type === "manual_update" ? "Manual update" : "System activity")}
                  </div>
                </div>
              `,
                  )
                  .join("")
              : `<div class="meta">No updates or activity yet.</div>`
          }
        </div>
      </div>
    `
    : ""
}

${
  activeTab === "actions"
    ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Actions Needed</h2>
            <div class="meta">Track simple client or WeSolve follow-ups.</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openActionModal()">+ Add Action</button>
        </div>

        ${
          actions.length
            ? actions
                .map(
                  (a) => `
              <div class="work-card">
                <div class="work-card-top">
                  <div>
                    <div class="work-card-title">${escapeHtml(a.title)}</div>
                    <div class="meta">${escapeHtml(a.notes || "")}</div>
                  </div>
                  <div style="display:flex; gap:8px; flex-wrap:wrap;">
                    <span class="badge badge-info">${escapeHtml(a.status || "Open")}</span>
                    <span class="badge badge-muted">${escapeHtml(a.priority || "Medium")}</span>
                  </div>
                </div>

                <div class="work-card-meta">
                  <div><strong>Owner:</strong> ${escapeHtml(a.owner_type || "-")} ${a.owner_name ? "· " + escapeHtml(a.owner_name) : ""}</div>
                  <div><strong>Due:</strong> ${escapeHtml(a.due_date || "-")}</div>
                  <div><strong>Updated:</strong> ${escapeHtml(a.updated_at ? formatDateTime(a.updated_at) : "-")}</div>
                </div>

                <div class="work-card-actions">
                  <button class="btn" type="button" onclick="openActionEditModal(${Number(a.id)})">Edit</button>
                  <button class="btn" type="button" onclick="archiveAction(${Number(a.id)})">Archive</button>
                </div>
              </div>
            `,
                )
                .join("")
            : `<div class="meta">No actions yet.</div>`
        }
      </div>
    `
    : ""
}

${
  activeTab === "contributors"
    ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Contributors</h2>
            <div class="meta">Internal team, contractors, and client contacts. No attendance required.</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openContributorModal()">+ Add Contributor</button>
        </div>

        ${
          contributors.length
            ? contributors
                .map(
                  (p) => `
              <div class="work-card">
                <div class="work-card-top">
                  <div>
                    <div class="work-card-title">${escapeHtml(p.name)}</div>
                    <div class="meta">${escapeHtml(p.role || "-")}</div>
                  </div>
                  <div style="display:flex; gap:8px; flex-wrap:wrap;">
                    <span class="badge badge-info">${escapeHtml(p.person_type || "-")}</span>
                    <span class="badge badge-muted">${escapeHtml(p.status || "Active")}</span>
                  </div>
                </div>

                <div class="work-card-meta">
                  <div><strong>Email:</strong> ${escapeHtml(p.email || "-")}</div>
                  <div><strong>Phone:</strong> ${escapeHtml(p.phone || "-")}</div>
                  <div><strong>Can update work:</strong> ${p.can_update_work ? "Yes" : "No"}</div>
                  <div><strong>Can view client dashboard:</strong> ${p.can_view_client_dashboard ? "Yes" : "No"}</div>
                </div>

                ${p.notes ? `<div class="meta" style="margin-top:10px;">${escapeHtml(p.notes)}</div>` : ""}

                <div class="work-card-actions">
                  <button class="btn" type="button" onclick="openContributorEditModal(${Number(p.id)})">Edit</button>
                  <button class="btn" type="button" onclick="archiveContributor(${Number(p.id)})">Archive</button>
                </div>
              </div>
            `,
                )
                .join("")
            : `<div class="meta">No contributors yet.</div>`
        }
      </div>
    `
    : ""
}

${
  activeTab === "milestones"
    ? `
      <div class="panel">
        <div class="section-head">
          <div>
            <h2 class="section-title">Milestones</h2>
            <div class="section-subtitle">Project checkpoints connected to work items.</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openMilestoneModal()">+ Add Milestone</button>
        </div>

        <div class="standard-list">
          ${
            milestones.length
              ? milestones
                  .map((m) => {
                    const linkedCount = workItems.filter(
                      (w) => String(w.milestone_id || "") === String(m.id),
                    ).length;

                    return `
                    <div class="standard-card">
                      <div class="standard-card-top">
                        <div>
                          <div class="standard-card-title">${escapeHtml(m.title || "Milestone")}</div>
                          <div class="meta">${escapeHtml(m.notes || "")}</div>
                        </div>
                        <div style="display:flex; gap:8px; flex-wrap:wrap;">
                          <span class="badge badge-info">${escapeHtml(m.status || "planned")}</span>
                          <span class="badge badge-muted">${linkedCount} work items</span>
                        </div>
                      </div>

                      <div class="standard-card-meta">
                        <div><strong>Due:</strong> ${escapeHtml(m.due_date || "-")}</div>
                        <div><strong>Updated:</strong> ${escapeHtml(m.updated_at ? formatDateTime(m.updated_at) : "-")}</div>
                      </div>

                      <div class="standard-card-actions">
                        <button class="btn" type="button" onclick="openMilestoneEditModal(${Number(m.id)})">Edit</button>
                        <button class="btn" type="button" onclick="closeMilestone(${Number(m.id)})">Close</button>
                        <button class="btn" type="button" onclick="archiveMilestone(${Number(m.id)})">Archive</button>
                      </div>
                    </div>
                  `;
                  })
                  .join("")
              : `<div class="meta">No milestones yet.</div>`
          }
        </div>
      </div>
    `
    : ""
}

${
  activeTab === "documents"
    ? `
      <div class="panel">
        <h2>Documents</h2>
        <div class="meta" style="margin-bottom:12px;">
          Main document system is Google Drive.
          ${
            client.google_drive_folder_url
              ? `<a href="${escapeHtml(client.google_drive_folder_url)}" target="_blank" rel="noopener noreferrer">Open Client Folder</a>`
              : `<span style="color: var(--danger);">Google Drive folder not set</span>`
          }
        </div>

        ${
          documents.length
            ? documents
                .map(
                  (d) => `
              <div class="item">
                <div class="item-title">${escapeHtml(d.title || d.name || "Document")}</div>
                <div class="meta">${escapeHtml(d.url || "-")}</div>
              </div>
            `,
                )
                .join("")
            : `<div class="meta">No separate documents tracked. Use the Google Drive folder.</div>`
        }
      </div>
    `
    : ""
}
</div>
<div id="milestoneModal" class="work-modal" onclick="closeMilestoneModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="milestoneModalTitle" style="font-size:22px; font-weight:800;">Add Milestone</div>
      <button class="btn" type="button" onclick="closeMilestoneModal()">Close</button>
    </div>

    <input id="milestoneId" type="hidden" />

    <div class="form-grid">
      <div class="form-field">
        <label>Title</label>
        <input id="milestoneTitle" placeholder="Example: MVP Launch" />
      </div>

      <div class="form-field">
        <label>Due Date</label>
        <input id="milestoneDueDate" type="date" />
      </div>

      <div class="form-field">
        <label>Status</label>
        <select id="milestoneStatus">
          <option value="planned">Planned</option>
          <option value="in_progress">In Progress</option>
          <option value="done">Done</option>
          <option value="closed">Closed</option>
        </select>
      </div>

      <div class="form-field" style="grid-column:1 / -1;">
        <label>Notes</label>
        <textarea id="milestoneNotes"></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeMilestoneModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveMilestone()">Save Milestone</button>
    </div>
  </div>
</div>
<div id="workItemModal" class="work-modal" onclick="closeWorkItemModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div style="font-size:22px; font-weight:800;">Add Work Item</div>
      <button class="btn" type="button" onclick="closeWorkItemModal()">Close</button>
    </div>

    <div class="form-grid">
      <div class="form-field">
        <label>Title</label>
        <input id="workTitle" placeholder="Example: Build landing page" />
      </div>

      <div class="form-field">
        <label>Owner</label>
        <select id="workOwner">
          <option value="">Select owner</option>
          ${users.map((u) => `<option value="${u.id}">${escapeHtml(u.name)}</option>`).join("")}
        </select>
      </div>

      <div class="form-field">
        <label>Priority</label>
        <select id="workPriority">
          <option value="medium">Medium</option>
          <option value="high">High</option>
          <option value="low">Low</option>
        </select>
      </div>

      <div class="form-field">
        <label>Due Date</label>
        <input id="workDueDate" type="date" />
      </div>

      <div class="form-field">
        <label>Depends On</label>
        <select id="workDependency">
          <option value="">No dependency</option>
          ${workItems.map((w) => `<option value="${w.id}">#${w.id} · ${escapeHtml(w.title)}</option>`).join("")}
        </select>
      </div>
      
      <div class="form-field">
  <label>Milestone</label>
  <select id="workMilestone">
    <option value="">No milestone</option>
    ${milestones.map((m) => `<option value="${m.id}">${escapeHtml(m.title)}</option>`).join("")}
  </select>
</div>

      <div class="form-field" style="grid-column:1 / -1;">
        <label>Description</label>
        <textarea id="workDescription" placeholder="Add details, expected outcome, blockers, etc."></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeWorkItemModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="createWorkItem(${Number(client.id)})">Create Work Item</button>
    </div>
  </div>
</div>

<div id="workItemDetailModal" class="work-modal" onclick="closeWorkItemDetail(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="workItemDetailTitle" style="font-size:22px; font-weight:800;">Work Item</div>
      <button class="btn" type="button" onclick="closeWorkItemDetail()">Close</button>
    </div>
    <div id="workItemDetailBody" class="meta">Loading...</div>
  </div>
</div>

<div id="clientUpdateModal" class="work-modal" onclick="closeClientUpdateModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div style="font-size:22px; font-weight:800;">Add Client Update</div>
      <button class="btn" type="button" onclick="closeClientUpdateModal()">Close</button>
    </div>

    <div class="form-grid">
      <div class="form-field">
        <label>Title</label>
        <input id="clientUpdateTitle" placeholder="Example: Weekly progress update" />
      </div>

      <div class="form-field">
        <label>Related Work Item</label>
        <select id="clientUpdateWorkItem">
          <option value="">No related work item</option>
          ${workItems.map((w) => `<option value="${w.id}">#${w.id} · ${escapeHtml(w.title)}</option>`).join("")}
        </select>
      </div>

      <div class="form-field">
        <label>Update Type</label>
        <select id="clientUpdateType">
          <option value="general">General</option>
          <option value="progress">Progress</option>
          <option value="blocker">Blocker</option>
          <option value="client_call">Client Call</option>
          <option value="delivery">Delivery</option>
        </select>
      </div>

      <div class="form-field">
        <label>Visibility</label>
        <select id="clientUpdateVisibility">
          <option value="internal">Internal only</option>
          <option value="client">Client visible later</option>
        </select>
      </div>

      <div class="form-field" style="grid-column:1 / -1;">
        <label>Update</label>
        <textarea id="clientUpdateText" placeholder="Write what happened, what changed, next step, blocker, etc."></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeClientUpdateModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="createClientUpdate(${Number(client.id)})">Save Update</button>
    </div>
  </div>
</div>

<div id="actionModal" class="work-modal" onclick="closeActionModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="actionModalTitle" style="font-size:22px; font-weight:800;">Add Action</div>
      <button class="btn" type="button" onclick="closeActionModal()">Close</button>
    </div>

    <input id="actionId" type="hidden" />

    <div class="form-grid">
      <div class="form-field">
        <label>Title</label>
        <input id="actionTitle" placeholder="Need logo from client" />
      </div>

      <div class="form-field">
        <label>Owner Type</label>
        <select id="actionOwnerType">
          <option value="WeSolve">WeSolve</option>
          <option value="Client">Client</option>
        </select>
      </div>

      <div class="form-field">
        <label>Owner Name</label>
        <input id="actionOwnerName" placeholder="Aj / Malikah / Client" />
      </div>

      <div class="form-field">
        <label>Due Date</label>
        <input id="actionDueDate" type="date" />
      </div>

      <div class="form-field">
        <label>Status</label>
        <select id="actionStatus">
          <option>Open</option>
          <option>In Progress</option>
          <option>Waiting</option>
          <option>Done</option>
        </select>
      </div>

      <div class="form-field">
        <label>Priority</label>
        <select id="actionPriority">
          <option>Low</option>
          <option selected>Medium</option>
          <option>High</option>
          <option>Urgent</option>
        </select>
      </div>

      <div class="form-field" style="grid-column:1 / -1;">
        <label>Notes</label>
        <textarea id="actionNotes"></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeActionModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveAction()">Save Action</button>
    </div>
  </div>
</div>

<div id="contributorModal" class="work-modal" onclick="closeContributorModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="contributorModalTitle" style="font-size:22px; font-weight:800;">Add Contributor</div>
      <button class="btn" type="button" onclick="closeContributorModal()">Close</button>
    </div>

    <input id="contributorId" type="hidden" />

    <div class="form-grid">
      <div class="form-field">
        <label>Person Type</label>
        <select id="contributorPersonType">
          <option>Internal</option>
          <option selected>Contractor</option>
          <option>Client</option>
        </select>
      </div>

      <div class="form-field">
        <label>Name</label>
        <input id="contributorName" placeholder="Name" />
      </div>

      <div class="form-field">
        <label>Email</label>
        <input id="contributorEmail" placeholder="email@example.com" />
      </div>

      <div class="form-field">
        <label>Phone</label>
        <input id="contributorPhone" placeholder="+91..." />
      </div>

      <div class="form-field">
        <label>Role</label>
        <input id="contributorRole" placeholder="Developer / Designer / Client Contact" />
      </div>

      <div class="form-field">
        <label>Status</label>
        <select id="contributorStatus">
          <option>Active</option>
          <option>Inactive</option>
        </select>
      </div>

      <div class="form-field">
        <label>
          <input id="contributorCanUpdateWork" type="checkbox" style="width:auto;" />
          Can update work
        </label>
      </div>

      <div class="form-field">
        <label>
          <input id="contributorCanViewClientDashboard" type="checkbox" style="width:auto;" />
          Can view client dashboard
        </label>
      </div>

      <div class="form-field" style="grid-column:1 / -1;">
        <label>Notes</label>
        <textarea id="contributorNotes"></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeContributorModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveContributor()">Save Contributor</button>
    </div>
  </div>
</div>

        
<script>
  const WORK_ITEM_USERS = ${JSON.stringify(users.map((u) => ({ id: u.id, name: u.name })))};
const WORK_ITEMS = ${JSON.stringify(
    workItems.map((w) => ({
      id: w.id,
      title: w.title,
      status: w.status,
      owner_user_id: w.owner_user_id,
      dependency_work_item_id: w.dependency_work_item_id,
      milestone_id: w.milestone_id,
      priority: w.priority,
      due_date: w.due_date,
      description: w.description,
      created_at: w.created_at,
      updated_at: w.updated_at,
    })),
  )};
  
  const CLIENT_ACTIONS = ${JSON.stringify(actions)};
const CLIENT_CONTRIBUTORS = ${JSON.stringify(contributors)};
const CLIENT_ID = ${Number(client.id)};
const CLIENT_MILESTONES = ${JSON.stringify(milestones)};

async function generateClientViewLink() {
  const res = await fetch("/api/clients/" + CLIENT_ID + "/client-view-link", {
    method: "POST",
    headers: { "Content-Type": "application/json" }
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to create client link");
    return;
  }

  const url = json.data.url;

  try {
    await navigator.clipboard.writeText(url);
    alert("Client view link copied:\\n" + url);
  } catch (e) {
    prompt("Copy this client view link:", url);
  }
}


function openMilestoneModal() {
  document.getElementById("milestoneModalTitle").textContent = "Add Milestone";
  document.getElementById("milestoneId").value = "";
  document.getElementById("milestoneTitle").value = "";
  document.getElementById("milestoneDueDate").value = "";
  document.getElementById("milestoneStatus").value = "planned";
  document.getElementById("milestoneNotes").value = "";
  document.getElementById("milestoneModal").classList.add("open");
}

function openMilestoneEditModal(id) {
  const milestone = CLIENT_MILESTONES.find(function(m) {
    return Number(m.id) === Number(id);
  });

  if (!milestone) {
    alert("Milestone not found");
    return;
  }

  document.getElementById("milestoneModalTitle").textContent = "Edit Milestone";
  document.getElementById("milestoneId").value = milestone.id;
  document.getElementById("milestoneTitle").value = milestone.title || "";
  document.getElementById("milestoneDueDate").value = milestone.due_date || "";
  document.getElementById("milestoneStatus").value = milestone.status || "planned";
  document.getElementById("milestoneNotes").value = milestone.notes || "";
  document.getElementById("milestoneModal").classList.add("open");
}

function closeMilestoneModal(event) {
  if (event && event.target && event.target.id !== "milestoneModal") return;
  document.getElementById("milestoneModal").classList.remove("open");
}

async function saveMilestone() {
  const id = document.getElementById("milestoneId").value;

  const payload = {
    title: document.getElementById("milestoneTitle").value.trim(),
    due_date: document.getElementById("milestoneDueDate").value || null,
    status: document.getElementById("milestoneStatus").value,
    notes: document.getElementById("milestoneNotes").value.trim()
  };

  if (!payload.title) {
    alert("Milestone title is required");
    return;
  }

  const url = id
    ? "/api/clients/" + CLIENT_ID + "/milestones/" + id
    : "/api/clients/" + CLIENT_ID + "/milestones";

  const method = id ? "PUT" : "POST";

  const res = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to save milestone");
    return;
  }

  window.location.reload();
}

async function closeMilestone(id) {
  if (!confirm("Close this milestone?")) return;

  const res = await fetch("/api/clients/" + CLIENT_ID + "/milestones/" + id, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ status: "closed" })
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to close milestone");
    return;
  }

  window.location.reload();
}

async function archiveMilestone(id) {
  if (!confirm("Archive this milestone? Work items will remain, but the milestone will be hidden.")) return;

  const res = await fetch("/api/clients/" + CLIENT_ID + "/milestones/" + id + "/archive", {
    method: "POST",
    headers: { "Content-Type": "application/json" }
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to archive milestone");
    return;
  }

  window.location.reload();
}

function openActionModal() {
  document.getElementById("actionModalTitle").textContent = "Add Action";
  document.getElementById("actionId").value = "";
  document.getElementById("actionTitle").value = "";
  document.getElementById("actionOwnerType").value = "WeSolve";
  document.getElementById("actionOwnerName").value = "";
  document.getElementById("actionDueDate").value = "";
  document.getElementById("actionStatus").value = "Open";
  document.getElementById("actionPriority").value = "Medium";
  document.getElementById("actionNotes").value = "";
  document.getElementById("actionModal").classList.add("open");
}

function openActionEditModal(id) {
  const action = CLIENT_ACTIONS.find(function(a) {
    return Number(a.id) === Number(id);
  });

  if (!action) {
    alert("Action not found");
    return;
  }

  document.getElementById("actionModalTitle").textContent = "Edit Action";
  document.getElementById("actionId").value = action.id;
  document.getElementById("actionTitle").value = action.title || "";
  document.getElementById("actionOwnerType").value = action.owner_type || "WeSolve";
  document.getElementById("actionOwnerName").value = action.owner_name || "";
  document.getElementById("actionDueDate").value = action.due_date || "";
  document.getElementById("actionStatus").value = action.status || "Open";
  document.getElementById("actionPriority").value = action.priority || "Medium";
  document.getElementById("actionNotes").value = action.notes || "";
  document.getElementById("actionModal").classList.add("open");
}

function closeActionModal(event) {
  if (event && event.target && event.target.id !== "actionModal") return;
  document.getElementById("actionModal").classList.remove("open");
}

async function saveAction() {
  const id = document.getElementById("actionId").value;

  const payload = {
    title: document.getElementById("actionTitle").value.trim(),
    owner_type: document.getElementById("actionOwnerType").value,
    owner_name: document.getElementById("actionOwnerName").value.trim(),
    due_date: document.getElementById("actionDueDate").value || null,
    status: document.getElementById("actionStatus").value,
    priority: document.getElementById("actionPriority").value,
    notes: document.getElementById("actionNotes").value.trim()
  };

  if (!payload.title) {
    alert("Action title is required");
    return;
  }

  const url = id
    ? "/api/clients/" + CLIENT_ID + "/actions/" + id
    : "/api/clients/" + CLIENT_ID + "/actions";

  const method = id ? "PUT" : "POST";

  const res = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  const json = await res.json();

  if (!json.success && !json.ok) {
    alert(json.error || "Failed to save action");
    return;
  }

  window.location.reload();
}

async function archiveAction(id) {
  if (!confirm("Archive this action?")) return;

  const res = await fetch("/api/clients/" + CLIENT_ID + "/actions/" + id + "/archive", {
    method: "POST",
    headers: { "Content-Type": "application/json" }
  });

  const json = await res.json();

  if (!json.success && !json.ok) {
    alert(json.error || "Failed to archive action");
    return;
  }

  window.location.reload();
}

function openContributorModal() {
  document.getElementById("contributorModalTitle").textContent = "Add Contributor";
  document.getElementById("contributorId").value = "";
  document.getElementById("contributorPersonType").value = "Contractor";
  document.getElementById("contributorName").value = "";
  document.getElementById("contributorEmail").value = "";
  document.getElementById("contributorPhone").value = "";
  document.getElementById("contributorRole").value = "";
  document.getElementById("contributorStatus").value = "Active";
  document.getElementById("contributorCanUpdateWork").checked = true;
  document.getElementById("contributorCanViewClientDashboard").checked = false;
  document.getElementById("contributorNotes").value = "";
  document.getElementById("contributorModal").classList.add("open");
}

function openContributorEditModal(id) {
  const person = CLIENT_CONTRIBUTORS.find(function(p) {
    return Number(p.id) === Number(id);
  });

  if (!person) {
    alert("Contributor not found");
    return;
  }

  document.getElementById("contributorModalTitle").textContent = "Edit Contributor";
  document.getElementById("contributorId").value = person.id;
  document.getElementById("contributorPersonType").value = person.person_type || "Contractor";
  document.getElementById("contributorName").value = person.name || "";
  document.getElementById("contributorEmail").value = person.email || "";
  document.getElementById("contributorPhone").value = person.phone || "";
  document.getElementById("contributorRole").value = person.role || "";
  document.getElementById("contributorStatus").value = person.status || "Active";
  document.getElementById("contributorCanUpdateWork").checked = !!person.can_update_work;
  document.getElementById("contributorCanViewClientDashboard").checked = !!person.can_view_client_dashboard;
  document.getElementById("contributorNotes").value = person.notes || "";
  document.getElementById("contributorModal").classList.add("open");
}

function closeContributorModal(event) {
  if (event && event.target && event.target.id !== "contributorModal") return;
  document.getElementById("contributorModal").classList.remove("open");
}

async function saveContributor() {
  const id = document.getElementById("contributorId").value;

  const payload = {
    person_type: document.getElementById("contributorPersonType").value,
    name: document.getElementById("contributorName").value.trim(),
    email: document.getElementById("contributorEmail").value.trim(),
    phone: document.getElementById("contributorPhone").value.trim(),
    role: document.getElementById("contributorRole").value.trim(),
    status: document.getElementById("contributorStatus").value,
    can_update_work: document.getElementById("contributorCanUpdateWork").checked,
    can_view_client_dashboard: document.getElementById("contributorCanViewClientDashboard").checked,
    notes: document.getElementById("contributorNotes").value.trim()
  };

  if (!payload.name || !payload.role) {
    alert("Name and role are required");
    return;
  }

  const url = id
    ? "/api/clients/" + CLIENT_ID + "/contributors/" + id
    : "/api/clients/" + CLIENT_ID + "/contributors";

  const method = id ? "PUT" : "POST";

  const res = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  const json = await res.json();

  if (!json.success && !json.ok) {
    alert(json.error || "Failed to save contributor");
    return;
  }

  window.location.reload();
}

async function archiveContributor(id) {
  if (!confirm("Archive this contributor?")) return;

  const res = await fetch("/api/clients/" + CLIENT_ID + "/contributors/" + id + "/archive", {
    method: "POST",
    headers: { "Content-Type": "application/json" }
  });

  const json = await res.json();

  if (!json.success && !json.ok) {
    alert(json.error || "Failed to archive contributor");
    return;
  }

  window.location.reload();
}

  function showLoadingModal(message) {
    const modal = document.getElementById("workItemDetailModal");
    const title = document.getElementById("workItemDetailTitle");
    const body = document.getElementById("workItemDetailBody");

    title.textContent = "Opening this page";
    body.innerHTML =
      '<div style="padding:20px; text-align:center;">' +
        '<div style="font-size:18px; font-weight:800; margin-bottom:8px;">Please wait...</div>' +
        '<div class="meta">' + escapeHtmlClient(message || "Loading details...") + '</div>' +
      '</div>';

    modal.classList.add("open");
  }
  
  function openClientUpdateModal() {
  document.getElementById("clientUpdateTitle").value = "";
  document.getElementById("clientUpdateWorkItem").value = "";
  document.getElementById("clientUpdateType").value = "general";
  document.getElementById("clientUpdateVisibility").value = "internal";
  document.getElementById("clientUpdateText").value = "";

  document.getElementById("clientUpdateModal").classList.add("open");
}

function closeClientUpdateModal(event) {
  if (event && event.target && event.target.id !== "clientUpdateModal") return;
  document.getElementById("clientUpdateModal").classList.remove("open");
}

async function createClientUpdate(clientId) {
  const updateText = document.getElementById("clientUpdateText").value.trim();

  if (!updateText) {
    alert("Update text is required");
    return;
  }

  const payload = {
    title: document.getElementById("clientUpdateTitle").value.trim(),
    update_text: updateText,
    update_type: document.getElementById("clientUpdateType").value,
    related_work_item_id: document.getElementById("clientUpdateWorkItem").value || null,
    is_client_visible: document.getElementById("clientUpdateVisibility").value === "client"
  };

  showLoadingModal("Saving client update...");

  const res = await fetch("/api/clients/" + clientId + "/updates", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to save update");
    closeWorkItemDetail();
    return;
  }

  window.location.reload();
}

function openWorkItemModal() {
  document.getElementById("workTitle").value = "";
  document.getElementById("workOwner").value = "";
  document.getElementById("workPriority").value = "medium";
  document.getElementById("workDueDate").value = "";
  document.getElementById("workDependency").value = "";
  document.getElementById("workDescription").value = "";

  document.getElementById("workItemModal").classList.add("open");
}

  function closeWorkItemModal(event) {
    if (event && event.target && event.target.id !== "workItemModal") return;
    document.getElementById("workItemModal").classList.remove("open");
  }

  async function createWorkItem(clientId) {
    const title = document.getElementById("workTitle").value.trim();

    if (!title) {
      alert("Title is required");
      return;
    }

    showLoadingModal("Creating work item...");

const payload = {
  client_id: clientId,
  title,
  description: document.getElementById("workDescription").value.trim(),
  owner_user_id: document.getElementById("workOwner").value || null,
  priority: document.getElementById("workPriority").value,
  due_date: document.getElementById("workDueDate").value || null,
  dependency_work_item_id: document.getElementById("workDependency").value || null,
  milestone_id: document.getElementById("workMilestone").value || null
};

    const res = await fetch("/api/client-work-items", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const json = await res.json();

    if (!json.ok) {
      alert("Create failed: " + (json.error || "Unknown error"));
      console.error("Create work item failed:", json);
      closeWorkItemDetail();
      return;
    }

    window.location.reload();
  }

  async function quickUpdateWorkItem(id, status) {
    showLoadingModal("Updating work item status...");

    const res = await fetch("/api/client-work-items/" + id, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status })
    });

    const json = await res.json();

    if (!json.ok) {
      alert(json.error || "Failed to update work item");
      closeWorkItemDetail();
      return;
    }

    window.location.reload();
  }

  async function archiveWorkItem(id) {
    if (!confirm("Archive this work item? It will be hidden but not permanently deleted.")) {
      return;
    }

    showLoadingModal("Archiving work item...");

    const res = await fetch("/api/client-work-items/" + id, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ archive: true })
    });

    const json = await res.json();

    if (!json.ok) {
      alert(json.error || "Failed to archive work item");
      closeWorkItemDetail();
      return;
    }

    window.location.reload();
  }

  async function openWorkItemDetail(id) {
    showLoadingModal("Opening work item details...");

    const res = await fetch("/api/client-work-items/" + id);
    const json = await res.json();

    if (!json.ok) {
      document.getElementById("workItemDetailBody").innerHTML =
        escapeHtmlClient(json.error || "Failed to load work item");
      return;
    }

    const w = json.data;

    const ownerOptions = WORK_ITEM_USERS.map(function(u) {
      return '<option value="' + u.id + '" ' +
        (String(w.owner_user_id || "") === String(u.id) ? "selected" : "") +
        '>' + escapeHtmlClient(u.name) + '</option>';
    }).join("");

    const dependencyOptions = WORK_ITEMS
      .filter(function(item) {
        return Number(item.id) !== Number(w.id);
      })
      .map(function(item) {
        return '<option value="' + item.id + '" ' +
          (String(w.dependency_work_item_id || "") === String(item.id) ? "selected" : "") +
          '>#' + item.id + ' · ' + escapeHtmlClient(item.title) + ' (' + escapeHtmlClient(item.status || "todo") + ')</option>';
      })
      .join("");
      
      const milestoneOptions = ${JSON.stringify(
        milestones.map((m) => ({ id: m.id, title: m.title })),
      )}.map(function(m) {
  return '<option value="' + m.id + '" ' +
    (String(w.milestone_id || "") === String(m.id) ? "selected" : "") +
    '>' + escapeHtmlClient(m.title) + '</option>';
}).join("");

    document.getElementById("workItemDetailTitle").textContent =
      "#" + w.id + " — Edit Work Item";

    document.getElementById("workItemDetailBody").innerHTML =
      '<div class="form-grid">' +

        '<div class="form-field">' +
          '<label>Title</label>' +
          '<input id="editWorkTitle" value="' + escapeHtmlClient(w.title || "") + '" />' +
        '</div>' +

        '<div class="form-field">' +
          '<label>Status</label>' +
          '<select id="editWorkStatus">' +
            '<option value="todo" ' + ((w.status || "todo") === "todo" ? "selected" : "") + '>Todo</option>' +
            '<option value="in_progress" ' + (w.status === "in_progress" ? "selected" : "") + '>In Progress</option>' +
            '<option value="done" ' + (w.status === "done" ? "selected" : "") + '>Done</option>' +
          '</select>' +
        '</div>' +

        '<div class="form-field">' +
          '<label>Priority</label>' +
          '<select id="editWorkPriority">' +
            '<option value="low" ' + (w.priority === "low" ? "selected" : "") + '>Low</option>' +
            '<option value="medium" ' + ((w.priority || "medium") === "medium" ? "selected" : "") + '>Medium</option>' +
            '<option value="high" ' + (w.priority === "high" ? "selected" : "") + '>High</option>' +
          '</select>' +
        '</div>' +

        '<div class="form-field">' +
          '<label>Due Date</label>' +
          '<input id="editWorkDueDate" type="date" value="' + escapeHtmlClient(w.due_date || "") + '" />' +
        '</div>' +

        '<div class="form-field">' +
          '<label>Owner</label>' +
          '<select id="editWorkOwner">' +
            '<option value="">No owner</option>' +
            ownerOptions +
          '</select>' +
        '</div>' +

        '<div class="form-field">' +
          '<label>Depends On</label>' +
          '<select id="editWorkDependency">' +
            '<option value="">No dependency</option>' +
            dependencyOptions +
          '</select>' +
        '</div>' +
        
        '<div class="form-field">' +
  '<label>Milestone</label>' +
  '<select id="editWorkMilestone">' +
    '<option value="">No milestone</option>' +
    milestoneOptions +
  '</select>' +
'</div>' +

        '<div class="form-field" style="grid-column:1 / -1;">' +
          '<label>Description</label>' +
          '<textarea id="editWorkDescription">' + escapeHtmlClient(w.description || "") + '</textarea>' +
        '</div>' +

      '</div>' +

      '<div style="margin-top:14px; padding-top:14px; border-top:1px solid rgba(255,255,255,0.10);">' +
        '<div><strong>Created:</strong> ' + escapeHtmlClient(w.created_at || "-") + '</div>' +
        '<div><strong>Last Updated:</strong> ' + escapeHtmlClient(w.updated_at || "-") + '</div>' +
      '</div>' +

      '<div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px; flex-wrap:wrap;">' +
        '<button class="btn" type="button" onclick="closeWorkItemDetail()">Cancel</button>' +
        '<button class="btn" type="button" onclick="archiveWorkItem(' + Number(w.id) + ')">Archive</button>' +
        '<button class="btn btn-primary" type="button" onclick="saveWorkItemChanges(' + Number(w.id) + ')">Save Changes</button>' +
      '</div>';
  }

  async function saveWorkItemChanges(id) {
    const title = document.getElementById("editWorkTitle").value.trim();

    if (!title) {
      alert("Title is required");
      return;
    }

    showLoadingModal("Saving work item changes...");


const payload = {
  title,
  status: document.getElementById("editWorkStatus").value,
  priority: document.getElementById("editWorkPriority").value,
  owner_user_id: document.getElementById("editWorkOwner").value || null,
  due_date: document.getElementById("editWorkDueDate").value || null,
  dependency_work_item_id: document.getElementById("editWorkDependency").value || null,
  milestone_id: document.getElementById("editWorkMilestone").value || null,
  description: document.getElementById("editWorkDescription").value.trim()
};

    const res = await fetch("/api/client-work-items/" + id, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const json = await res.json();

    if (!json.ok) {
      alert(json.error || "Failed to update work item");
      closeWorkItemDetail();
      return;
    }

    window.location.reload();
  }

function closeWorkItemDetail(e) {
  if (!e || e.target.id === "workItemDetailModal") {
    document.getElementById("workItemDetailModal").classList.remove("open");
  }
}

document.addEventListener("keydown", function(event) {
  if (event.key === "Escape") {
    document.querySelectorAll(".work-modal.open").forEach(function(modal) {
      modal.classList.remove("open");
    });
  }
});

</script>
      </body>
    </html>
  `;
}

function renderClientViewOnlyPage({
  client,
  services = [],
  workItems = [],
  updates = [],
  actions = [],
  documents = [],
}) {
  const openWorkItems = workItems.filter((w) => w.status !== "done");
  const doneWorkItems = workItems.filter((w) => w.status === "done");
  const clientActions = actions.filter((a) => a.owner_type === "Client");

  const serviceNames =
    services
      .map((s) => s.name)
      .filter(Boolean)
      .join(", ") || "-";

  return `
    <html>
      <head>
        <title>${escapeHtml(client.name || "Client")} | Project View</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}

          .wrap {
            max-width: 1280px;
            margin: 0 auto;
            padding: 28px 18px 42px;
          }

          .topbar, .panel, .stat-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
          }

          .topbar {
            padding: 22px;
            margin-bottom: 18px;
            display: flex;
            justify-content: space-between;
            gap: 16px;
            align-items: flex-start;
            flex-wrap: wrap;
          }

          .eyebrow {
            font-size: 11px;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            color: var(--primary);
            font-weight: 800;
            margin-bottom: 8px;
          }

          h1 {
            margin: 0;
            font-size: 30px;
            letter-spacing: -0.04em;
          }

          h2 {
            margin: 0;
            font-size: 18px;
          }

          .subtitle, .meta {
            color: var(--muted);
            font-size: 14px;
            line-height: 1.55;
          }

          .stats {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 12px;
            margin-bottom: 18px;
          }

          .stat-card {
            padding: 16px;
          }

          .stat-label {
            color: var(--muted);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 800;
          }

          .stat-value {
            margin-top: 10px;
            font-size: 28px;
            font-weight: 900;
          }

          .client-view-tabs {
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 0;
            margin-bottom: 18px;
            background: rgba(255,255,255,0.035);
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 16px;
            padding: 6px;
            position: sticky;
            top: 0;
            z-index: 20;
            backdrop-filter: blur(14px);
          }

          .client-view-tab {
            min-height: 42px;
            border-radius: 12px;
            border: 1px solid transparent;
            background: transparent;
            color: var(--muted);
            font: inherit;
            font-size: 13px;
            font-weight: 900;
            cursor: pointer;
          }

          .client-view-tab.active {
            background: var(--primary-soft);
            border-color: color-mix(in srgb, var(--primary) 55%, transparent);
            color: var(--text-strong);
          }

          .panel {
            padding: 18px;
            margin-bottom: 16px;
          }

          .panel-head {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            align-items: center;
            margin-bottom: 14px;
          }

          .table-wrap {
            overflow-x: auto;
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 14px;
          }

          table {
            width: 100%;
            border-collapse: collapse;
            min-width: 820px;
          }

          th, td {
            padding: 13px 14px;
            border-bottom: 1px solid rgba(255,255,255,0.08);
            text-align: left;
            vertical-align: top;
            font-size: 13px;
          }

          th {
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-size: 11px;
            font-weight: 900;
            background: rgba(255,255,255,0.035);
          }

          tr:last-child td {
            border-bottom: 0;
          }

          .badge {
            display: inline-flex;
            padding: 6px 9px;
            border-radius: 999px;
            background: rgba(255,255,255,0.08);
            color: var(--text);
            font-size: 12px;
            font-weight: 800;
            white-space: nowrap;
          }

          .tab-panel {
            display: none;
          }

          .tab-panel.active {
            display: block;
          }

          a {
            color: var(--primary);
            font-weight: 800;
          }

          @media (max-width: 850px) {
            .stats {
              grid-template-columns: 1fr 1fr;
            }

            .client-view-tabs {
              grid-template-columns: repeat(2, minmax(0, 1fr));
              position: relative;
            }
          }
        </style>
      </head>

      <body>
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Client Project View</div>
              <h1>${escapeHtml(client.name || "-")}</h1>
              <div class="subtitle">${escapeHtml(client.company_name || "")}</div>
            </div>

            ${
              client.google_drive_folder_url
                ? `<a href="${escapeHtml(client.google_drive_folder_url)}" target="_blank" rel="noopener noreferrer">Open Google Drive Folder</a>`
                : ""
            }
          </div>

          <div class="stats">
            <div class="stat-card">
              <div class="stat-label">Services</div>
              <div class="stat-value">${services.length}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Open Work</div>
              <div class="stat-value">${openWorkItems.length}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Completed</div>
              <div class="stat-value">${doneWorkItems.length}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Client Actions</div>
              <div class="stat-value">${clientActions.length}</div>
            </div>
          </div>

          <div class="client-view-tabs">
            <button class="client-view-tab active" onclick="showClientViewTab('overview', this)">Overview</button>
            <button class="client-view-tab" onclick="showClientViewTab('work', this)">Work Progress</button>
            <button class="client-view-tab" onclick="showClientViewTab('updates', this)">Updates</button>
            <button class="client-view-tab" onclick="showClientViewTab('actions', this)">Actions Needed</button>
            <button class="client-view-tab" onclick="showClientViewTab('documents', this)">Documents</button>
          </div>

          <div id="clientViewTab-overview" class="tab-panel active">
            <div class="panel">
              <div class="panel-head">
                <h2>Overview</h2>
              </div>

              <div class="table-wrap">
                <table>
                  <tbody>
                    <tr>
                      <th>Client</th>
                      <td>${escapeHtml(client.name || "-")}</td>
                    </tr>
                    <tr>
                      <th>Company</th>
                      <td>${escapeHtml(client.company_name || "-")}</td>
                    </tr>
                    <tr>
                      <th>Status</th>
                      <td><span class="badge">${escapeHtml(client.status || "-")}</span></td>
                    </tr>
                    <tr>
                      <th>Services</th>
                      <td>${escapeHtml(serviceNames)}</td>
                    </tr>
                    <tr>
                      <th>Description</th>
                      <td>${escapeHtml(client.description || "Project progress and updates.")}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="clientViewTab-work" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Work Progress</h2>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Work Item</th>
                      <th>Status</th>
                      <th>Priority</th>
                      <th>Due Date</th>
                      <th>Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      workItems.length
                        ? workItems
                            .map(
                              (w) => `
                          <tr>
                            <td><strong>${escapeHtml(w.title || "Work item")}</strong></td>
                            <td><span class="badge">${escapeHtml(w.status || "todo")}</span></td>
                            <td>${escapeHtml(w.priority || "-")}</td>
                            <td>${escapeHtml(w.due_date || "-")}</td>
                            <td>${escapeHtml(w.description || "-")}</td>
                          </tr>
                        `,
                            )
                            .join("")
                        : `<tr><td colspan="5" class="meta">No work items shared yet.</td></tr>`
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="clientViewTab-updates" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Latest Updates</h2>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Title</th>
                      <th>Type</th>
                      <th>Update</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      updates.length
                        ? updates
                            .map(
                              (u) => `
                          <tr>
                            <td>${escapeHtml(u.created_at ? formatDateTime(u.created_at) : "-")}</td>
                            <td><strong>${escapeHtml(u.title || "Update")}</strong></td>
                            <td>${escapeHtml(u.update_type || "-")}</td>
                            <td>${escapeHtml(u.update_text || "")}</td>
                          </tr>
                        `,
                            )
                            .join("")
                        : `<tr><td colspan="4" class="meta">No client-visible updates yet.</td></tr>`
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="clientViewTab-actions" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Actions Needed From Client</h2>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Action</th>
                      <th>Status</th>
                      <th>Priority</th>
                      <th>Due Date</th>
                      <th>Notes</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      clientActions.length
                        ? clientActions
                            .map(
                              (a) => `
                          <tr>
                            <td><strong>${escapeHtml(a.title || "Action")}</strong></td>
                            <td><span class="badge">${escapeHtml(a.status || "Open")}</span></td>
                            <td>${escapeHtml(a.priority || "Medium")}</td>
                            <td>${escapeHtml(a.due_date || "-")}</td>
                            <td>${escapeHtml(a.notes || "-")}</td>
                          </tr>
                        `,
                            )
                            .join("")
                        : `<tr><td colspan="5" class="meta">No client actions pending.</td></tr>`
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="clientViewTab-documents" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Documents</h2>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Document</th>
                      <th>Link</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      client.google_drive_folder_url
                        ? `
                          <tr>
                            <td><strong>Main Google Drive Folder</strong></td>
                            <td><a href="${escapeHtml(client.google_drive_folder_url)}" target="_blank" rel="noopener noreferrer">Open Folder</a></td>
                          </tr>
                        `
                        : ""
                    }

                    ${
                      documents.length
                        ? documents
                            .map(
                              (d) => `
                          <tr>
                            <td><strong>${escapeHtml(d.title || d.name || "Document")}</strong></td>
                            <td>${d.url ? `<a href="${escapeHtml(d.url)}" target="_blank" rel="noopener noreferrer">Open</a>` : "-"}</td>
                          </tr>
                        `,
                            )
                            .join("")
                        : ""
                    }

                    ${
                      !client.google_drive_folder_url && !documents.length
                        ? `<tr><td colspan="2" class="meta">No shared documents available.</td></tr>`
                        : ""
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>

        <script>
          function showClientViewTab(key, btn) {
            document.querySelectorAll(".tab-panel").forEach(function(panel) {
              panel.classList.remove("active");
            });

            document.querySelectorAll(".client-view-tab").forEach(function(tab) {
              tab.classList.remove("active");
            });

            const panel = document.getElementById("clientViewTab-" + key);
            if (panel) panel.classList.add("active");
            if (btn) btn.classList.add("active");
          }
        </script>
      </body>
    </html>
  `;
}

function renderTopNav(active = "") {
  const items = [
    { href: "/dashboard", label: "Dashboard", short: "Home", key: "dashboard" },
    { href: "/tasks", label: "Tasks", short: "Tasks", key: "tasks" },
    {
      href: "/attendance",
      label: "Attendance",
      short: "Attend",
      key: "attendance",
    },
    { href: "/logs", label: "Logs", short: "?", key: "logs", icon: true },
    { href: "/reports", label: "Reports", short: "Reports", key: "reports" },
    {
      href: "/leads",
      label: "Leads",
      short: "Leads",
      key: "leads",
      dropdown: getActiveLeadBusinesses().map((b) => ({
        href: `/leads/${b.business}`,
        label: `${b.label} Leads`,
      })),
    },
    { href: "/clients", label: "Clients", short: "Clients", key: "clients" },
    {
      href: "/account",
      label: "My Account",
      short: "Me",
      key: "account",
      optional: true,
    },
    { href: "/logout", label: "Logout", short: "⏻", key: "logout", icon: true },
  ];

  return `
    <div class="top-nav">
      <div class="top-nav-inner">
        <a class="brand" href="/dashboard" title="WeSolveHR">W</a>

        <div class="nav-links">
          ${items
            .map((item) => {
              const classes = [
                active === item.key ||
                (item.key === "leads" && active === "lead-detail")
                  ? "active"
                  : "",
                item.key === "logout" ? "logout-link" : "",
                item.icon ? "nav-icon-link" : "",
                item.optional ? "nav-text-optional" : "",
              ]
                .filter(Boolean)
                .join(" ");

              const dropdownHtml =
                item.dropdown && item.dropdown.length
                  ? `
      <div class="nav-dropdown-wrap">
        <a
          href="${item.href}"
          class="${classes}"
          title="${escapeHtml(item.label)}"
          aria-label="${escapeHtml(item.label)}"
        >
          ${escapeHtml(item.short || item.label)}
        </a>
        <div class="nav-dropdown-menu">
          ${item.dropdown
            .map(
              (child) => `
                <a href="${child.href}">
                  ${escapeHtml(child.label)}
                </a>
              `,
            )
            .join("")}
        </div>
      </div>
    `
                  : `
      <a
        href="${item.href}"
        class="${classes}"
        title="${escapeHtml(item.label)}"
        aria-label="${escapeHtml(item.label)}"
      >
        ${escapeHtml(item.short || item.label)}
      </a>
    `;

              return dropdownHtml;
            })
            .join("")}
        </div>

        <div class="top-nav-status" id="topNavStatus">
          <span class="top-nav-pill loading">Off: ...</span>
          <span class="top-nav-pill loading">Break: ...</span>
        </div>
      </div>
    </div>

    <script>
      (function () {
        const mount = document.getElementById("topNavStatus");
        if (!mount) return;

        fetch("/api/top-nav-summary")
          .then((r) => r.json())
          .then((json) => {
            if (!json.ok || !json.data) {
              mount.innerHTML =
                '<span class="top-nav-pill muted">Off: -</span>' +
                '<span class="top-nav-pill muted">Break: -</span>';
              return;
            }

            const data = json.data;

            const offTitle = data.offNames && data.offNames.length
              ? data.offNames.join(", ")
              : "Nobody off today";

            const breakTitle = data.breakNames && data.breakNames.length
              ? data.breakNames.join(", ")
              : "Nobody on break";

            const offLabel =
              (data.offCount || 0) === 0
                ? "Off: 0"
                : (data.offCount || 0) <= 2
                  ? "Off: " + data.offNames.join(", ")
                  : "Off: " + data.offCount;

            const breakLabel =
              (data.breakCount || 0) === 0
                ? "Break: 0"
                : (data.breakCount || 0) <= 2
                  ? "Break: " + data.breakNames.join(", ")
                  : "Break: " + data.breakCount;

            mount.innerHTML =
              '<span class="top-nav-pill" title="' + escapeHtmlClient(offTitle) + '">' + escapeHtmlClient(offLabel) + '</span>' +
              '<span class="top-nav-pill" title="' + escapeHtmlClient(breakTitle) + '">' + escapeHtmlClient(breakLabel) + '</span>';
          })
          .catch(() => {
            mount.innerHTML =
              '<span class="top-nav-pill muted">Off: -</span>' +
              '<span class="top-nav-pill muted">Break: -</span>';
          });
      })();

      function escapeHtmlClient(value) {
        return String(value ?? "")
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;");
      }
    </script>
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

function parseLeadUploadCommand(text) {
  const raw = String(text || "").trim();

  const match = raw.match(
    /^lead\s+(\S+)\s+upload\s+(\+?\d[\d\s().-]{7,}\d)(?:\s+name\s+(.+))?$/i,
  );

  if (!match) return null;

  const business = getBusinessCanonicalName(match[1]);
  const leadPhone = normalizePhoneForLogin(match[2]);
  const spokeToName = String(match[3] || "").trim() || null;

  if (!getBusinessLeadTableName(business)) {
    const allowedBusinesses = getActiveLeadBusinesses()
      .map((b) => b.business)
      .join(", ");

    return {
      error: `❌ Unsupported business. Use one of: ${allowedBusinesses}`,
    };
  }

  if (!leadPhone) {
    return {
      error:
        "❌ Missing lead phone. Example: lead joolian upload +12129816238 name Jaya",
    };
  }

  return {
    business,
    lead_phone: leadPhone,
    spoke_to_name: spokeToName,
  };
}

function getTwilioMediaFromRequest(req) {
  const numMedia = Number(req.body.NumMedia || 0);

  if (!numMedia || numMedia < 1) return null;

  const mediaUrl = req.body.MediaUrl0 || null;
  const mediaContentType = req.body.MediaContentType0 || null;

  if (!mediaUrl) return null;

  return {
    media_url: mediaUrl,
    media_content_type: mediaContentType,
  };
}

function isAudioMedia(mediaContentType) {
  const value = String(mediaContentType || "").toLowerCase();

  return (
    value.startsWith("audio/") ||
    value.includes("ogg") ||
    value.includes("mpeg") ||
    value.includes("mp4") ||
    value.includes("amr")
  );
}

async function createLeadUploadSession({
  orgId,
  senderPhone,
  business,
  leadPhone,
  userId,
  spokeToName,
}) {
  const expiresAt = new Date(Date.now() + 10 * 60 * 1000).toISOString();

  await supabase
    .from("lead_upload_sessions")
    .update({ status: "expired" })
    .eq("org_id", orgId)
    .eq("sender_phone", senderPhone)
    .eq("status", "waiting_for_voice");

  const { data, error } = await supabase
    .from("lead_upload_sessions")
    .insert([
      {
        org_id: orgId,
        sender_phone: senderPhone,
        business,
        lead_phone: leadPhone,
        spoke_to_name: spokeToName || null,
        status: "waiting_for_voice",
        expires_at: expiresAt,
        created_by_user_id: userId || null,
      },
    ])
    .select()
    .single();

  if (error) {
    console.error("createLeadUploadSession error:", error);
    throw error;
  }

  return data;
}

async function getLeadsOverviewData(orgId) {
  const { data: voiceRows, error: voiceError } = await supabase
    .from("lead_voice_uploads")
    .select(
      "id, business, lead_phone, sender_phone, status, media_content_type, created_at",
    )
    .eq("org_id", orgId)
    .order("created_at", { ascending: false });

  if (voiceError) throw voiceError;

  const businessTables = getActiveLeadBusinesses();

  const businesses = [];

  for (const item of businessTables) {
    let query = supabase
      .from(item.table)
      .select("id, status", { count: "exact" })
      .eq("org_id", orgId);

    if (item.table === "rasset_leads") {
      query = query.or("is_deleted.is.null,is_deleted.eq.false");
    }

    const { data, error, count } = await query;

    if (!error) {
      const rows = data || [];
      businesses.push({
        business: item.business,
        label: item.label || item.business,
        total: count || rows.length,
        leads: rows.filter(
          (x) => !["in_progress", "completed"].includes(x.status),
        ).length,
        in_progress: rows.filter((x) => x.status === "in_progress").length,
        completed: rows.filter((x) => x.status === "completed").length,
        voice_uploads: (voiceRows || []).filter(
          (x) => x.business === item.business,
        ).length,
      });
    }
  }

  return {
    summary: {
      total: businesses.reduce((sum, x) => sum + x.total, 0),
      leads: businesses.reduce((sum, x) => sum + x.leads, 0),
      in_progress: businesses.reduce((sum, x) => sum + x.in_progress, 0),
      completed: businesses.reduce((sum, x) => sum + x.completed, 0),
      voice_uploads: (voiceRows || []).length,
    },
    businesses,
    recent: (voiceRows || []).slice(0, 20),
  };
}

async function getActiveLeadUploadSession({ orgId, senderPhone }) {
  const nowIso = new Date().toISOString();

  const { data, error } = await supabase
    .from("lead_upload_sessions")
    .select("*")
    .eq("org_id", orgId)
    .eq("sender_phone", senderPhone)
    .eq("status", "waiting_for_voice")
    .gt("expires_at", nowIso)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("getActiveLeadUploadSession error:", error);
    throw error;
  }

  return data || null;
}

async function markLeadUploadSessionCompleted(sessionId) {
  const { error } = await supabase
    .from("lead_upload_sessions")
    .update({ status: "completed" })
    .eq("id", sessionId);

  if (error) {
    console.error("markLeadUploadSessionCompleted error:", error);
    throw error;
  }
}

async function saveLeadVoiceUpload({
  orgId,
  business,
  leadPhone,
  senderPhone,
  uploadedByUserId,
  twilioMessageSid,
  mediaUrl,
  mediaContentType,
  spokeToName,
}) {
  const { data, error } = await supabase
    .from("lead_voice_uploads")
    .insert([
      {
        org_id: orgId,
        business,
        lead_phone: leadPhone,
        sender_phone: senderPhone,
        uploaded_by_user_id: uploadedByUserId || null,
        twilio_message_sid: twilioMessageSid || null,
        media_url: mediaUrl,
        media_content_type: mediaContentType || null,
        spoke_to_name: spokeToName || null,
        status: "pending_transcription",
      },
    ])
    .select()
    .single();

  if (error) {
    console.error("saveLeadVoiceUpload error:", error);
    throw error;
  }
  await ensureBusinessLeadExistsForVoiceUpload({
    orgId,
    business,
    leadPhone,
    senderPhone,
    uploadedByUserId,
    spokeToName,
  });

  setImmediate(async () => {
    try {
      await transcribeLeadVoiceUploadById({
        leadVoiceId: data.id,
        orgId,
      });
    } catch (error) {
      console.error("Auto transcription failed:", error);

      await supabase
        .from("lead_voice_uploads")
        .update({
          status: "pending_transcription",
          transcription_error: String(error.message || error),
          updated_at: new Date().toISOString(),
        })
        .eq("org_id", orgId)
        .eq("id", data.id);
    }
  });

  return data;
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

function parseFeedbackCommand(text) {
  const raw = normalizeText(text);

  const patterns = [
    { type: "feedback", regex: /^feedback\s+(.+?)\s+(.+)$/i },
    { type: "appreciation", regex: /^appreciation\s+(.+?)\s+(.+)$/i },
    { type: "coaching", regex: /^coaching\s+(.+?)\s+(.+)$/i },
    { type: "one_on_one", regex: /^1on1\s+(.+?)\s+(.+)$/i },
  ];

  for (const p of patterns) {
    const match = raw.match(p.regex);

    if (match) {
      return {
        type: p.type,
        target_name: match[1].trim(),
        note: match[2].trim(),
      };
    }
  }

  return null;
}

function parseAppraisalCommand(text) {
  const raw = normalizeText(text);

  const match = raw.match(
    /^appraisal\s+(.+?)\s+rating\s+(\d+)\s+strengths\s+(.+?)\s+improve\s+(.+?)\s+comment\s+(.+)$/i,
  );

  if (!match) return null;

  return {
    target_name: match[1].trim(),
    rating: Number(match[2]),
    strengths: match[3].trim(),
    improvement_areas: match[4].trim(),
    manager_comment: match[5].trim(),
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
    "wait 23 on aj for API response",
    "clear wait 23 aj responded",
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

async function ensureBusinessLeadExistsForVoiceUpload({
  orgId,
  business,
  leadPhone,
  senderPhone,
  uploadedByUserId,
  spokeToName,
}) {
  const normalizedBusiness = getBusinessCanonicalName(business);
  const tableName = getBusinessLeadTableName(normalizedBusiness);

  if (!tableName || !leadPhone) return null;

  const { data: existing, error: existingError } = await supabase
    .from(tableName)
    .select("id")
    .eq("org_id", orgId)
    .eq("phone", leadPhone)
    .maybeSingle();

  if (existingError) throw existingError;
  if (existing) return existing;

  const { data, error } = await supabase
    .from(tableName)
    .insert([
      {
        org_id: orgId,
        phone: leadPhone,
        lead_category: normalizedBusiness === "rasset" ? "b2b" : "b2c",
        status: "new",
        lead_stage: null,
        lead_source: "voice",
        notes:
          "Auto-created from voice upload by " + (senderPhone || "unknown"),
        latest_transcript: null,
        last_spoke_to_name: spokeToName || null,
        last_call_uploaded_by_phone: senderPhone || null,
        last_call_uploaded_by_user_id: uploadedByUserId || null,
        updated_at: new Date().toISOString(),
      },
    ])
    .select()
    .single();

  if (error) throw error;

  return data;
}

async function getUserWorkProfile(userId, orgId) {
  const { data, error } = await supabase
    .from("user_work_profiles")
    .select(
      "user_id, employment_type, shift_start_time, shift_end_time, working_hours",
    )
    .eq("user_id", userId)
    .maybeSingle();

  if (error) {
    console.error("getUserWorkProfile error:", error);
    return null;
  }

  return data || null;
}

function parseTimeValueToTodayIso(timeValue) {
  if (!timeValue) return null;

  const raw = String(timeValue).trim();

  // supports "21:30:00" or "21:30"
  const m24 = raw.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (m24) {
    const hour = Number(m24[1]);
    const minute = Number(m24[2]);
    const second = Number(m24[3] || 0);

    if (
      Number.isNaN(hour) ||
      Number.isNaN(minute) ||
      Number.isNaN(second) ||
      hour < 0 ||
      hour > 23 ||
      minute < 0 ||
      minute > 59 ||
      second < 0 ||
      second > 59
    ) {
      return null;
    }

    const todayDb = getTodayDateStringInTimeZone(APP_TIMEZONE);
    const iso = `${todayDb}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:${String(second).padStart(2, "0")}${APP_TIMEZONE_OFFSET}`;
    const d = new Date(iso);

    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString();
  }

  // fallback for old style like "10:30 AM"
  return parseLocalDateTimeForToday(raw);
}

async function getAttendanceInsightsData(orgId) {
  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());

  const { startDate: weekStartDate, endDateExclusive: weekEndDateExclusive } =
    getWeekDateRangeForAttendance(APP_TIMEZONE);

  const monthStartDate = attendanceDate.slice(0, 8) + "01";
  const monthEndDateExclusive = addDaysToDateString(attendanceDate, 1);

  const [weeklyAgg, monthlyAgg] = await Promise.all([
    getAttendanceInsightsForRange(orgId, weekStartDate, weekEndDateExclusive),
    getAttendanceInsightsForRange(orgId, monthStartDate, monthEndDateExclusive),
  ]);

  return {
    weekly: buildWeeklyInsightsFromAgg(weeklyAgg),
    monthly: buildMonthlyInsightsFromAgg(monthlyAgg),
  };
}

async function getShiftStartIsoForUserToday(userId, orgId) {
  const workProfile = await getUserWorkProfile(userId, orgId);

  if (workProfile?.shift_start_time) {
    const customIso = parseTimeValueToTodayIso(workProfile.shift_start_time);
    if (customIso) return customIso;
  }

  return parseLocalDateTimeForToday(DEFAULT_SHIFT_START_TEXT);
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

  // supports:
  // login khateeba 3 pm
  // login khateeba today 3 pm
  // login khateeba mukhtar 3 pm
  // logout khateeba 6:30 pm
  // back khateeba today 4 pm
  let match = raw.match(
    /^(login|logout|back)\s+(.+?)(?:\s+(today))?\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))$/i,
  );

  if (match) {
    return {
      target_name: match[2].trim().replace(/\s+/g, " "),
      action: match[1].toLowerCase(),
      duration_min: null,
      time_text: match[4].trim().replace(/\s+/g, " "),
      reason: null,
    };
  }

  // supports:
  // login khateeba
  // logout khateeba mukhtar
  match = raw.match(/^(login|logout|back)\s+(.+)$/i);
  if (match) {
    const maybeName = match[2]
      .replace(/\b(today|yesterday|tomorrow)\b/gi, "")
      .replace(/\b\d{1,2}(?::\d{2})?\s*(am|pm)\b/gi, "")
      .replace(/\s+/g, " ")
      .trim();

    if (maybeName) {
      return {
        target_name: maybeName,
        action: match[1].toLowerCase(),
        duration_min: null,
        time_text: null,
        reason: null,
      };
    }
  }

  // supports:
  // break khateeba 30 3 pm
  // break khateeba 30 3:15 pm
  match = raw.match(
    /^break\s+(.+?)(?:\s+(today))?\s+(\d+)\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))$/i,
  );

  if (match) {
    return {
      target_name: match[1].trim().replace(/\s+/g, " "),
      action: "break",
      duration_min: Number(match[3]),
      time_text: match[4].trim().replace(/\s+/g, " "),
      reason: null,
    };
  }

  // supports:
  // break khateeba 30
  match = raw.match(/^break\s+(.+?)\s+(\d+)$/i);
  if (match) {
    return {
      target_name: match[1].trim().replace(/\s+/g, " "),
      action: "break",
      duration_min: Number(match[2]),
      time_text: null,
      reason: null,
    };
  }

  // supports:
  // break khateeba 3 pm
  // break khateeba today 3 pm
  match = raw.match(
    /^break\s+(.+?)(?:\s+(today))?\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))$/i,
  );

  if (match) {
    return {
      target_name: match[1].trim().replace(/\s+/g, " "),
      action: "break",
      duration_min: null,
      time_text: match[3].trim().replace(/\s+/g, " "),
      reason: null,
    };
  }

  // supports:
  // break khateeba
  match = raw.match(/^break\s+(.+)$/i);
  if (match) {
    const maybeName = match[1]
      .replace(/\b(today|yesterday|tomorrow)\b/gi, "")
      .replace(/\b\d{1,2}(?::\d{2})?\s*(am|pm)\b/gi, "")
      .replace(/\s+/g, " ")
      .trim();

    if (maybeName) {
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

function parseWaitTaskCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(/^wait\s+(\d+)\s+on\s+(.+?)\s+for\s+(.+)$/i);
  if (match) {
    return {
      taskId: Number(match[1]),
      waiting_on_name: match[2].trim(),
      reason: match[3].trim(),
    };
  }

  match = raw.match(/^waiting\s+(\d+)\s+on\s+(.+?)\s+for\s+(.+)$/i);
  if (match) {
    return {
      taskId: Number(match[1]),
      waiting_on_name: match[2].trim(),
      reason: match[3].trim(),
    };
  }

  match = raw.match(/^blocked\s+(\d+)\s+on\s+(.+?)\s+for\s+(.+)$/i);
  if (match) {
    return {
      taskId: Number(match[1]),
      waiting_on_name: match[2].trim(),
      reason: match[3].trim(),
    };
  }

  return null;
}

function parseClearWaitTaskCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(/^clear\s+wait\s+(\d+)(?:\s+(.+))?$/i);
  if (match) {
    return {
      taskId: Number(match[1]),
      note: match[2]?.trim() || "Cleared wait",
    };
  }

  match = raw.match(/^unwait\s+(\d+)(?:\s+(.+))?$/i);
  if (match) {
    return {
      taskId: Number(match[1]),
      note: match[2]?.trim() || "Cleared wait",
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

function parseWorkDayOverrideCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(
    /^day\s+on\s+(today|tomorrow|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)\s+(.+)$/i,
  );

  if (match) {
    return {
      date_text: match[1].trim(),
      target_name: match[2].trim(),
      mode: "full_day",
    };
  }

  match = raw.match(
    /^day\s+half\s+(today|tomorrow|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)\s+(.+)$/i,
  );

  if (match) {
    return {
      date_text: match[1].trim(),
      target_name: match[2].trim(),
      mode: "half_day",
    };
  }

  return null;
}

function parseCompanyOffCommand(text) {
  const raw = normalizeText(text);

  const match = raw.match(
    /^company\s+off\s+(today|tomorrow|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)$/i,
  );

  if (!match) return null;

  return {
    off_date_text: match[1].trim(),
  };
}

function parseCompanyWorkDayOverrideCommand(text) {
  const raw = normalizeText(text);

  let match = raw.match(
    /^company\s+day\s+on\s+(today|tomorrow|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)$/i,
  );

  if (match) {
    return {
      date_text: match[1].trim(),
      mode: "full_day",
    };
  }

  match = raw.match(
    /^company\s+day\s+half\s+(today|tomorrow|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)$/i,
  );

  if (match) {
    return {
      date_text: match[1].trim(),
      mode: "half_day",
    };
  }

  return null;
}

function parseAttendanceCommand(text) {
  let raw = text.trim();

  const actionMatch = raw.match(/^(login|logout|break|back)\b/i);
  if (!actionMatch) return null;

  const action = actionMatch[1].toLowerCase();

  let rest = raw.replace(/^(login|logout|break|back)\b/i, "").trim();

  // remove date words from name area
  rest = rest.replace(/\b(today|yesterday|tomorrow)\b/gi, "").trim();

  // extract time like 3 pm, 3pm, 3:30 pm
  const timeMatch = rest.match(/\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b/i);

  let timeText = null;
  if (timeMatch) {
    timeText = timeMatch[0];
    rest = rest.replace(timeMatch[0], "").trim();
  }

  const employeeName = rest.replace(/\s+/g, " ").trim();

  return {
    action,
    employeeName,
    timeText,
  };
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

async function requireUserLogin(req, res, next) {
  const userId = req.session?.userId;

  if (!userId) {
    return res.redirect("/login");
  }

  const { data: user } = await supabase
    .from("users")
    .select("*")
    .eq("id", userId)
    .maybeSingle();

  if (!user) {
    req.session.destroy(() => {});
    if (isApiRoute) {
      return sendApiError(res, 401, "Session expired");
    }
    return res.redirect("/login");
  }

  req.loggedInUser = user;
  next();
}

async function requireDashboardAuth(req, res, next) {
  try {
    const isApiRoute = req.path.startsWith("/api/");
    // 1) Prefer real user session if present
    const sessionUserId = req.session?.userId;

    if (sessionUserId) {
      const { data: user, error } = await supabase
        .from("users")
        .select("*")
        .eq("id", sessionUserId)
        .eq("is_active", true)
        .maybeSingle();

      if (error) {
        console.error("requireDashboardAuth session lookup error:", error);
        return res.status(500).send("Failed to validate logged in user");
      }

      if (user) {
        req.loggedInUser = user;
        return next();
      }

      // session exists but user no longer valid -> clear it
      req.session.destroy(() => {});
      return res.redirect("/login");
    }

    // 2) Fallback to existing dashboard basic auth
    const username = process.env.DASHBOARD_USERNAME;
    const password = process.env.DASHBOARD_PASSWORD;

    if (!username || !password) {
      console.warn(
        "Dashboard auth env vars missing; dashboard is unprotected.",
      );
      return next();
    }

    const header = req.get("Authorization") || "";
    if (!header.startsWith("Basic ")) {
      if (isApiRoute) {
        return sendApiError(res, 401, "Not authenticated");
      }
      return res.redirect("/login");
    }

    const base64 = header.slice(6);
    let decoded = "";

    try {
      decoded = Buffer.from(base64, "base64").toString("utf8");
    } catch {
      if (isApiRoute) {
        return sendApiError(res, 401, "Not authenticated");
      }
      return res.redirect("/login");
    }

    const separatorIndex = decoded.indexOf(":");
    const inputUser =
      separatorIndex >= 0 ? decoded.slice(0, separatorIndex) : "";
    const inputPass =
      separatorIndex >= 0 ? decoded.slice(separatorIndex + 1) : "";
    if (inputUser !== username || inputPass !== password) {
      if (isApiRoute) {
        return sendApiError(res, 401, "Not authenticated");
      }
      return res.redirect("/login");
    }

    // Optional: attach one admin user for legacy dashboard flows if needed
    const { data: fallbackAdmin, error: fallbackAdminError } = await supabase
      .from("users")
      .select("*")
      .eq("org_id", DASHBOARD_ORG_ID)
      .eq("is_active", true)
      .in("role", ["admin", "manager"])
      .order("id", { ascending: true })
      .limit(1)
      .maybeSingle();

    if (fallbackAdminError) {
      console.error(
        "requireDashboardAuth fallback admin lookup error:",
        fallbackAdminError,
      );
    } else if (fallbackAdmin) {
      req.loggedInUser = fallbackAdmin;
    }

    return next();
  } catch (error) {
    console.error("requireDashboardAuth fatal error:", error);
    return res.status(500).send("Authentication failed");
  }
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

function getBusinessCanonicalName(input) {
  const key = String(input || "")
    .trim()
    .toLowerCase();

  const aliases = {
    rasset: "rasset",
    rassetai: "rasset",
    "rasset.ai": "rasset",
    joolian: "joolian",
    joolianai: "joolian",
    "joolian.ai": "joolian",
  };

  return aliases[key] || key;
}

function getBusinessLeadTableName(business) {
  const normalized = getBusinessCanonicalName(business);
  return getBusinessConfig(normalized)?.table || null;
}

function guessFilenameFromContentType(contentType) {
  const value = String(contentType || "").toLowerCase();

  if (value.includes("ogg")) return "lead-voice.ogg";
  if (value.includes("mpeg") || value.includes("mp3")) return "lead-voice.mp3";
  if (value.includes("mp4")) return "lead-voice.mp4";
  if (value.includes("wav")) return "lead-voice.wav";
  if (value.includes("amr")) return "lead-voice.amr";

  return "lead-voice.ogg";
}

async function downloadTwilioMediaToBuffer(mediaUrl) {
  const accountSid = process.env.TWILIO_ACCOUNT_SID;
  const authToken = process.env.TWILIO_AUTH_TOKEN;

  if (!accountSid || !authToken) {
    throw new Error("Missing TWILIO_ACCOUNT_SID or TWILIO_AUTH_TOKEN");
  }

  const auth = Buffer.from(`${accountSid}:${authToken}`).toString("base64");

  const response = await fetch(mediaUrl, {
    headers: {
      Authorization: `Basic ${auth}`,
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to download Twilio media: ${response.status}`);
  }

  const arrayBuffer = await response.arrayBuffer();
  return Buffer.from(arrayBuffer);
}

async function splitAudioBufferIntoMp3Chunks({
  buffer,
  contentType,
  chunkSeconds = 120,
}) {
  const tempDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "lead-audio-"),
  );

  const inputExt =
    guessFilenameFromContentType(contentType).split(".").pop() || "ogg";
  const inputPath = path.join(tempDir, `input.${inputExt}`);
  const outputPattern = path.join(tempDir, "chunk-%03d.mp3");

  await fs.promises.writeFile(inputPath, buffer);

  await new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .outputOptions([
        "-f segment",
        `-segment_time ${chunkSeconds}`,
        "-reset_timestamps 1",
        "-ac 1",
        "-ar 16000",
        "-b:a 64k",
      ])
      .output(outputPattern)
      .on("end", resolve)
      .on("error", reject)
      .run();
  });

  const files = (await fs.promises.readdir(tempDir))
    .filter((name) => name.startsWith("chunk-") && name.endsWith(".mp3"))
    .sort();

  const chunks = [];

  for (let i = 0; i < files.length; i += 1) {
    const filePath = path.join(tempDir, files[i]);
    const chunkBuffer = await fs.promises.readFile(filePath);

    chunks.push({
      index: i + 1,
      startSeconds: i * chunkSeconds,
      endSeconds: (i + 1) * chunkSeconds,
      buffer: chunkBuffer,
      contentType: "audio/mpeg",
      fileName: files[i],
    });
  }

  return {
    tempDir,
    chunks,
  };
}

async function cleanupTempDir(tempDir) {
  if (!tempDir) return;

  try {
    await fs.promises.rm(tempDir, {
      recursive: true,
      force: true,
    });
  } catch (error) {
    console.error("cleanupTempDir error:", error);
  }
}

function formatChunkTime(seconds) {
  const safeSeconds = Math.max(0, Number(seconds) || 0);
  const mins = Math.floor(safeSeconds / 60);
  const secs = safeSeconds % 60;
  return `${String(mins).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
}

async function transcribeAudioBuffer({ buffer, contentType, fileName }) {
  if (!openai) {
    throw new Error("OpenAI client is not configured");
  }

  const safeFileName = fileName || guessFilenameFromContentType(contentType);

  const transcription = await openai.audio.transcriptions.create({
    model: "gpt-4o-transcribe",
    file: await toFile(buffer, safeFileName, {
      type: contentType || "audio/ogg",
    }),
  });

  return transcription.text || "";
}

async function transcribeAudioBufferInChunks({
  buffer,
  contentType,
  chunkSeconds = 120,
}) {
  let tempDir = null;

  try {
    const splitResult = await splitAudioBufferIntoMp3Chunks({
      buffer,
      contentType,
      chunkSeconds,
    });

    tempDir = splitResult.tempDir;
    const chunks = splitResult.chunks || [];

    if (!chunks.length) {
      return await transcribeAudioBuffer({
        buffer,
        contentType,
      });
    }

    const transcriptParts = [];

    for (const chunk of chunks) {
      console.log("Transcribing audio chunk:", {
        chunk: chunk.index,
        start: formatChunkTime(chunk.startSeconds),
        end: formatChunkTime(chunk.endSeconds),
        size: chunk.buffer.length,
      });

      const text = await transcribeAudioBuffer({
        buffer: chunk.buffer,
        contentType: chunk.contentType,
        fileName: chunk.fileName,
      });

      transcriptParts.push(
        `[${formatChunkTime(chunk.startSeconds)}-${formatChunkTime(
          chunk.endSeconds,
        )}]\n${text || "[no speech detected]"}`,
      );
    }

    return transcriptParts.join("\n\n");
  } finally {
    await cleanupTempDir(tempDir);
  }
}

async function cleanAndTranslateLeadTranscript(rawTranscript) {
  if (!openai) {
    throw new Error("OpenAI client is not configured");
  }

  const prompt = `
You are cleaning, translating, and extracting key details from a lead call transcript.

Return JSON only with EXACT structure:
{
  "detected_language": "hindi|english|hinglish|unknown",
  "cleaned_transcript": "Full cleaned readable transcript preserving all meaningful spoken content",
  "translated_text": "Full English translation preserving all meaningful spoken content. If already English, use the cleaned transcript.",
  "transcription_confidence": "low|medium|high",
  "important_points": [
    "Important factual point from the call"
  ],
  "pain_points": [
    "Pain point mentioned by the lead"
  ],
  "follow_up_questions": [
    "Question salesperson should ask next"
  ]
}

CRITICAL OUTPUT RULES:
- Return valid JSON only. No markdown. No explanation.
- Do NOT summarize the transcript.
- Do NOT lose meaningful spoken content.
- Do NOT invent names, roles, facts, intent, or conclusions.
- Preserve the full call in cleaned_transcript and translated_text.
- If the transcript is already English, translated_text should be polished readable English but must not lose meaning.
- Remove obvious transcription junk only when it is clearly noise.
- Keep the conversation readable as a continuous transcript.

CONTENT PRESERVATION RULES:
- Keep repeated questions if they were repeated.
- Keep short replies like "yes", "no", "okay", "sure", "hello", "I'm sorry".
- Keep emails, phone numbers, locations, company names, business names, machine names, activity names, pricing, age groups, membership details, and payment details exactly if mentioned.
- Use [unclear] only where the words are unclear.
- Do not add phrases like "you mentioned" unless actually spoken.
- Do not convert cleaned_transcript or translated_text into bullet points.
- Do not add analysis inside cleaned_transcript or translated_text.

BUSINESS EXTRACTION RULES:
- important_points and pain_points may be summarized.
- follow_up_questions should be practical next questions based only on the call.
- For manufacturing/Rasset calls, pay attention to manpower issues, payment or money-stuck issues, raw material sourcing, urgent order capacity, spare parts, technician availability, machine breakdown frequency, maintenance schedule, and production dependency.
- For Joolian/activity-provider calls, pay attention to age groups, activity type, membership/pricing model, group enrollment possibility, availability, scheduling, owner/manager availability, contact person, email, payment requirement, and whether custom classes are possible.
`;

  const completion = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      { role: "system", content: prompt },
      { role: "user", content: rawTranscript || "" },
    ],
    response_format: { type: "json_object" },
  });

  const parsed =
    safeParseJson(completion.choices?.[0]?.message?.content || "{}") || {};

  return {
    detected_language: parsed.detected_language || "unknown",
    cleaned_transcript: parsed.cleaned_transcript || rawTranscript || "",
    translated_text:
      parsed.translated_text ||
      parsed.cleaned_transcript ||
      rawTranscript ||
      "",
    transcription_confidence: parsed.transcription_confidence || "medium",
    conversation_rows: Array.isArray(parsed.conversation_rows)
      ? parsed.conversation_rows
      : [],
    important_points: Array.isArray(parsed.important_points)
      ? parsed.important_points
      : [],
    pain_points: Array.isArray(parsed.pain_points) ? parsed.pain_points : [],
    follow_up_questions: Array.isArray(parsed.follow_up_questions)
      ? parsed.follow_up_questions
      : [],
  };
}

async function transcribeLeadVoiceUploadById({ leadVoiceId, orgId }) {
  const { data: lead, error: fetchError } = await supabase
    .from("lead_voice_uploads")
    .select("*")
    .eq("id", leadVoiceId)
    .eq("org_id", orgId)
    .maybeSingle();

  if (fetchError) throw fetchError;
  if (!lead) throw new Error("Lead voice upload not found");

  if (!lead.media_url) {
    throw new Error("Lead voice upload has no media_url");
  }

  await supabase
    .from("lead_voice_uploads")
    .update({
      status: "transcribing",
      updated_at: new Date().toISOString(),
    })
    .eq("id", leadVoiceId)
    .eq("org_id", orgId);

  const buffer = await downloadTwilioMediaToBuffer(lead.media_url);

  const rawTranscript = await transcribeAudioBufferInChunks({
    buffer,
    contentType: lead.media_content_type,
    chunkSeconds: 120,
  });

  const cleaned = await cleanAndTranslateLeadTranscript(rawTranscript);

  const { data: updated, error: updateError } = await supabase
    .from("lead_voice_uploads")
    .update({
      raw_transcript: rawTranscript,
      cleaned_transcript: cleaned.cleaned_transcript,
      translated_text: cleaned.translated_text,
      detected_language: cleaned.detected_language,
      conversation_rows: cleaned.conversation_rows || [],
      transcription_model: "gpt-4o-transcribe",
      transcription_chunk_seconds: 120,
      transcription_chunked: true,
      status: "pending_review",
      updated_at: new Date().toISOString(),
    })
    .eq("id", leadVoiceId)
    .eq("org_id", orgId)
    .select()
    .single();

  if (updateError) throw updateError;

  return updated;
}

async function updateLeadVoiceTranscript({
  leadVoiceId,
  orgId,
  cleanedTranscript,
  translatedText,
  reviewNotes,
}) {
  const { data, error } = await supabase
    .from("lead_voice_uploads")
    .update({
      cleaned_transcript: cleanedTranscript,
      translated_text: translatedText || cleanedTranscript,
      review_notes: reviewNotes || null,
      status: "pending_review",
      updated_at: new Date().toISOString(),
    })
    .eq("id", leadVoiceId)
    .eq("org_id", orgId)
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function rejectLeadVoiceUpload({ leadVoiceId, orgId, userId, reason }) {
  const { data, error } = await supabase
    .from("lead_voice_uploads")
    .update({
      status: "rejected",
      review_notes: reason || null,
      reviewed_by_user_id: userId || null,
      reviewed_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    })
    .eq("id", leadVoiceId)
    .eq("org_id", orgId)
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function approveLeadVoiceUpload({
  leadVoiceId,
  orgId,
  userId,
  verifiedBy,
  verifiedAt,
}) {
  const { data: lead, error: fetchError } = await supabase
    .from("lead_voice_uploads")
    .select("*")
    .eq("id", leadVoiceId)
    .eq("org_id", orgId)
    .maybeSingle();

  if (fetchError) throw fetchError;
  if (!lead) throw new Error("Lead voice upload not found");

  const business = getBusinessCanonicalName(lead.business);
  const tableName = getBusinessLeadTableName(business);

  if (!tableName) {
    throw new Error(
      `No business lead table configured for business: ${lead.business}`,
    );
  }

  const transcriptForLead =
    lead.translated_text ||
    lead.cleaned_transcript ||
    lead.raw_transcript ||
    "";

  const { data: existingLead, error: existingError } = await supabase
    .from(tableName)
    .select("*")
    .eq("org_id", orgId)
    .eq("phone", lead.lead_phone)
    .maybeSingle();

  if (existingError) throw existingError;

  let businessLead;

  if (existingLead) {
    const { data, error } = await supabase
      .from(tableName)
      .update({
        source_voice_upload_id: lead.id,
        latest_transcript: transcriptForLead,

        last_spoke_to_name: lead.spoke_to_name || null,
        last_call_uploaded_by_phone: lead.sender_phone || null,
        last_call_uploaded_by_user_id: lead.uploaded_by_user_id || null,

        updated_at: new Date().toISOString(),
      })
      .eq("id", existingLead.id)
      .eq("org_id", orgId)
      .select()
      .single();

    if (error) throw error;
    businessLead = data;
  } else {
    const basePayload = {
      org_id: orgId,
      phone: lead.lead_phone,
      lead_category: business === "rasset" ? "b2b" : "b2c",
      lead_source: "voice",
      source_voice_upload_id: lead.id,
      latest_transcript: transcriptForLead,
      status: "new",
      last_spoke_to_name: lead.spoke_to_name || null,
      last_call_uploaded_by_phone: lead.sender_phone || null,
      last_call_uploaded_by_user_id: lead.uploaded_by_user_id || null,
    };

    if (tableName === "rasset_leads") {
      basePayload.problem_summary = transcriptForLead;
    }

    if (tableName === "joolian_leads") {
      basePayload.interest_summary = transcriptForLead;
    }

    if (tableName === "matrimonials_leads") {
      basePayload.requirement_summary = transcriptForLead;
    }

    const { data, error } = await supabase
      .from(tableName)
      .insert([basePayload])
      .select()
      .single();

    if (error) throw error;
    businessLead = data;
  }

  const { data: updatedVoice, error: updateVoiceError } = await supabase
    .from("lead_voice_uploads")
    .update({
      status: "reviewed",
      reviewed_by_user_id: userId || null,
      reviewed_at: new Date().toISOString(),
      linked_table_name: tableName,
      linked_lead_id: businessLead.id,
      updated_at: new Date().toISOString(),
      verified_by: verifiedBy || null,
      verified_at: verifiedAt || new Date().toISOString(),
    })
    .eq("id", leadVoiceId)
    .eq("org_id", orgId)
    .select()
    .single();

  if (updateVoiceError) throw updateVoiceError;

  return {
    voice: updatedVoice,
    businessLead,
    tableName,
  };
}

async function updateBusinessLeadStatus({ business, leadId, orgId, status }) {
  const tableName = getBusinessLeadTableName(business);

  if (!tableName) {
    throw new Error(
      `No business lead table configured for business: ${business}`,
    );
  }

  if (!["new", "in_progress", "completed"].includes(status)) {
    throw new Error("Invalid status");
  }

  const { data, error } = await supabase
    .from(tableName)
    .update({
      status,
      updated_at: new Date().toISOString(),
    })
    .eq("id", leadId)
    .eq("org_id", orgId)
    .select()
    .single();

  if (error) throw error;
  return data;
}

function buildBusinessLeadPayloadFromBody(body) {
  return {
    phone: normalizeLeadPhone(body.phone || "") || null,
    lead_category: normalizeText(body.lead_category || "b2b"),
    lead_source: normalizeText(body.lead_source || "manual"),
    lead_stage: normalizeText(body.lead_stage || "prospect"),

    company: String(body.company || body.business_name || "").trim() || null,
    business_name:
      String(body.business_name || body.company || "").trim() || null,
    contact_name: String(body.contact_name || "").trim() || null,
    owner_name: String(body.owner_name || "").trim() || null,

    email: String(body.email || "").trim() || null,
    website: String(body.website || "").trim() || null,
    google_maps_url: String(body.google_maps_url || "").trim() || null,
    yelp_url: String(body.yelp_url || "").trim() || null,

    address: String(body.address || "").trim() || null,
    city: String(body.city || "").trim() || null,
    state: String(body.state || "").trim() || null,
    pin_code: String(body.pin_code || "").trim() || null,
    location: String(body.location || body.address || "").trim() || null,
    country: String(body.country || "").trim() || null,

    industry: String(body.industry || "").trim() || null,
    year_of_establishment:
      String(body.year_of_establishment || "").trim() || null,
    number_of_employees: String(body.number_of_employees || "").trim() || null,
    company_size: String(body.company_size || "").trim() || null,

    notes: String(body.notes || "").trim() || null,
    status: normalizeText(body.status || "new"),
    enrichment_status: normalizeText(body.enrichment_status || "not_enriched"),
    enrichment_notes: String(body.enrichment_notes || "").trim() || null,
    qualification_done:
      body.qualification_done === true || body.qualification_done === "true",
    worth_talking: body.worth_talking === true || body.worth_talking === "true",
    l2_done: body.l2_done === true || body.l2_done === "true",
    qualified: body.qualified === true || body.qualified === "true",
  };
}

function validateBusinessLeadPayload(payload) {
  if (
    !payload.phone &&
    !payload.company &&
    !payload.business_name &&
    !payload.website &&
    !payload.google_maps_url &&
    !payload.yelp_url
  ) {
    return "Enter at least phone, company, website, Google Maps link, or Yelp link.";
  }

  if (!["b2b", "b2c"].includes(payload.lead_category)) {
    return "Lead category must be b2b or b2c.";
  }

  if (!["new", "in_progress", "completed"].includes(payload.status)) {
    return "Status must be new, in_progress, or completed.";
  }

  return null;
}

async function createBusinessLead({ orgId, business, body }) {
  const tableName = getBusinessLeadTableName(business);

  if (!tableName) {
    throw new Error(`No lead table configured for ${business}`);
  }

  const payload = buildBusinessLeadPayloadFromBody(body);
  const validationError = validateBusinessLeadPayload(payload);

  if (validationError) {
    const err = new Error(validationError);
    err.statusCode = 400;
    throw err;
  }

  const insertPayload = {
    org_id: orgId,
    ...payload,
    latest_transcript:
      String(body.latest_transcript || body.notes || "").trim() || null,
    updated_at: new Date().toISOString(),
  };

  const { data, error } = await supabase
    .from(tableName)
    .insert([insertPayload])
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function updateBusinessLead({ orgId, business, leadId, body }) {
  const tableName = getBusinessLeadTableName(business);

  if (!tableName) {
    throw new Error(`No lead table configured for ${business}`);
  }

  const payload = buildBusinessLeadPayloadFromBody(body);
  const validationError = validateBusinessLeadPayload(payload);

  if (validationError) {
    const err = new Error(validationError);
    err.statusCode = 400;
    throw err;
  }

  const updatePayload = {
    ...payload,
    latest_transcript: String(body.latest_transcript || "").trim() || null,
    updated_at: new Date().toISOString(),
  };

  const { data, error } = await supabase
    .from(tableName)
    .update(updatePayload)
    .eq("org_id", orgId)
    .eq("id", leadId)
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function getBusinessLeadById({ orgId, business, leadId }) {
  const tableName = getBusinessLeadTableName(business);

  if (!tableName) {
    throw new Error(`No lead table configured for ${business}`);
  }

  const { data, error } = await supabase
    .from(tableName)
    .select("*")
    .eq("org_id", orgId)
    .eq("id", leadId)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function enrichLeadFromUrl({ url, googleMapsUrl = "" }) {
  const websiteUrl = String(url || "").trim();
  const mapUrl = String(googleMapsUrl || "").trim();

  if (!websiteUrl && !mapUrl) {
    return {
      success: false,
      message: "Please provide website or Google Map link.",
      data: {},
    };
  }

  const data = {
    website: websiteUrl || null,
    google_maps_url: mapUrl || null,
    lead_source: websiteUrl ? "website" : "google_map",
    lead_category: "b2b",
    lead_stage: null,
    enrichment_status: "partial",
  };

  let websiteText = "";

  if (websiteUrl) {
    try {
      const response = await fetch(websiteUrl, {
        headers: {
          "User-Agent": "Mozilla/5.0 WeSolveHR Lead Enrichment",
        },
      });

      if (response.ok) {
        const html = await response.text();

        const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
        const descMatch = html.match(
          /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["'][^>]*>/i,
        );

        const title = titleMatch
          ? titleMatch[1].replace(/\s+/g, " ").trim()
          : "";

        const description = descMatch
          ? descMatch[1].replace(/\s+/g, " ").trim()
          : "";

        const emailMatch = html.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
        const phoneMatch = html.match(/(\+?\d[\d\s().-]{8,}\d)/);

        websiteText = html
          .replace(/<script[\s\S]*?<\/script>/gi, " ")
          .replace(/<style[\s\S]*?<\/style>/gi, " ")
          .replace(/<[^>]+>/g, " ")
          .replace(/\s+/g, " ")
          .slice(0, 12000);

        data.company = title || null;
        data.business_name = title || null;
        data.notes = description || null;
        data.email = emailMatch?.[0] || null;
        data.phone = phoneMatch?.[0]
          ? normalizePhoneForLogin(phoneMatch[0])
          : null;
      } else {
        data.enrichment_notes = `Website fetch failed with status ${response.status}`;
      }
    } catch (error) {
      data.enrichment_notes = `Website fetch failed: ${error.message}`;
    }
  }

  if (openai && websiteText) {
    try {
      const completion = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content: `
Extract Rasset B2B lead fields from website text.
Return JSON only:
{
  "company": "",
  "website": "",
  "email": "",
  "industry": "",
  "pin_code": "",
  "city": "",
  "location": "",
  "phone": "",
  "year_of_establishment": "",
  "owner_name": "",
  "number_of_employees": "",
  "company_size": "",
  "country": "",
  "notes": "",
  "confidence_notes": ""
}
Rules:
- Do not invent.
- If unknown, use empty string.
- Prefer exact data from website text.
`,
          },
          {
            role: "user",
            content: `Website: ${websiteUrl}\nGoogle Map: ${mapUrl}\n\nText:\n${websiteText}`,
          },
        ],
      });

      const parsed =
        safeParseJson(completion.choices?.[0]?.message?.content || "{}") || {};

      for (const key of [
        "company",
        "website",
        "email",
        "industry",
        "pin_code",
        "city",
        "location",
        "phone",
        "year_of_establishment",
        "owner_name",
        "number_of_employees",
        "company_size",
        "country",
        "notes",
      ]) {
        if (parsed[key]) data[key] = parsed[key];
      }

      data.business_name = data.company || data.business_name || null;
      data.phone = data.phone ? normalizePhoneForLogin(data.phone) : null;
      data.enrichment_status = "enriched";
      data.enrichment_notes =
        parsed.confidence_notes || data.enrichment_notes || null;
    } catch (error) {
      data.enrichment_status = "partial";
      data.enrichment_notes = `AI extraction failed: ${error.message}`;
    }
  }

  if (mapUrl) {
    data.google_maps_url = mapUrl;
  }

  return {
    success: true,
    message:
      data.enrichment_status === "enriched"
        ? "Website enriched. Please review before saving."
        : "Partial data fetched. Please review manually.",
    data,
  };
}

function normalizeExcelHeader(value) {
  return normalizeText(value)
    .replace(/\./g, "")
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]/g, "");
}

function mapExcelRowToRassetLead(row) {
  const normalized = {};

  for (const [key, value] of Object.entries(row || {})) {
    normalized[normalizeExcelHeader(key)] = value;
  }

  const get = (...keys) => {
    for (const key of keys) {
      const value = normalized[normalizeExcelHeader(key)];
      if (
        value !== undefined &&
        value !== null &&
        String(value).trim() !== ""
      ) {
        return String(value).trim();
      }
    }
    return "";
  };

  return {
    company: get("Company", "company", "business_name"),
    business_name: get("Company", "company", "business_name"),
    website: get("website", "Website"),
    email: get("Email", "email"),
    industry: get("Industry", "industry"),
    pin_code: get("Pin code", "pincode", "pin_code", "zip"),
    city: get("city", "City"),
    location: get("Location", "address", "Address"),
    phone: normalizePhoneForLogin(get("Phone", "phone", "mobile")),
    year_of_establishment: get(
      "year of estb.",
      "year_of_establishment",
      "established",
    ),
    owner_name: get("ownwer", "owner", "owner_name"),
    number_of_employees: get(
      "No of Employe",
      "No of Employee",
      "employees",
      "number_of_employees",
    ),
    company_size: get("Company Size", "company_size"),
    google_maps_url: get("Google Map", "google_map", "google_maps_url"),
    country: get("country", "Country"),

    lead_category: normalizeText(
      get("lead_category", "type of lead", "lead type") || "b2b",
    ),
    lead_stage: normalizeText(get("lead_stage", "stage") || "prospect"),
    lead_source: normalizeText(get("lead_source", "source") || "excel"),
    status: normalizeText(get("status") || "new"),
    notes: get("notes", "Notes"),
    enrichment_status: "imported",
  };
}

async function createLeadImportLog({
  orgId,
  business,
  fileName,
  uploadedByUserId,
  uploadedByName,
  totalRows,
}) {
  const { data, error } = await supabase
    .from("lead_import_logs")
    .insert([
      {
        org_id: orgId,
        business,
        file_name: fileName || null,
        uploaded_by_user_id: uploadedByUserId || null,
        uploaded_by_name: uploadedByName || null,
        total_rows: totalRows || 0,
        status: "processing",
      },
    ])
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function addLeadImportRowLog({
  importId,
  orgId,
  business,
  rowNumber,
  phone,
  company,
  website,
  status,
  message,
  existingLeadId,
}) {
  const { error } = await supabase.from("lead_import_log_rows").insert([
    {
      import_id: importId,
      org_id: orgId,
      business,
      row_number: rowNumber || null,
      phone: phone || null,
      company: company || null,
      website: website || null,
      status,
      message: message || null,
      existing_lead_id: existingLeadId || null,
    },
  ]);

  if (error) {
    console.error("addLeadImportRowLog error:", error);
  }
}

async function finishLeadImportLog({
  importId,
  results,
  status = "completed",
  errorMessage = null,
}) {
  const { error } = await supabase
    .from("lead_import_logs")
    .update({
      inserted_count: results.inserted || 0,
      updated_count: results.updated || 0,
      duplicate_count: results.duplicates || 0,
      skipped_count: results.skipped || 0,
      error_count: (results.errors || []).length,
      status,
      error_message: errorMessage,
      completed_at: new Date().toISOString(),
    })
    .eq("id", importId);

  if (error) {
    console.error("finishLeadImportLog error:", error);
  }
}

async function importRassetLeadsFromExcel({
  orgId,
  buffer,
  fileName,
  uploadedByUserId,
  uploadedByName,
}) {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];

  if (!sheetName) {
    throw new Error("Excel file has no sheets");
  }

  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });

  const importLog = await createLeadImportLog({
    orgId,
    business: "rasset",
    fileName,
    uploadedByUserId,
    uploadedByName,
    totalRows: rows.length,
  });

  const results = {
    import_id: importLog.id,
    total: rows.length,
    inserted: 0,
    updated: 0,
    duplicates: 0,
    skipped: 0,
    errors: [],
  };

  try {
    for (let i = 0; i < rows.length; i += 1) {
      const rowNumber = i + 2;

      try {
        const payload = mapExcelRowToRassetLead(rows[i]);

        payload.phone = normalizeLeadPhone(payload.phone);

        if (
          !payload.company &&
          !payload.website &&
          !payload.phone &&
          !payload.google_maps_url
        ) {
          results.skipped += 1;

          await addLeadImportRowLog({
            importId: importLog.id,
            orgId,
            business: "rasset",
            rowNumber,
            phone: payload.phone,
            company: payload.company,
            website: payload.website,
            status: "skipped",
            message: "Empty row skipped",
          });

          continue;
        }

        if (!payload.phone) {
          results.errors.push({
            row: rowNumber,
            error: "Missing phone number",
          });

          await addLeadImportRowLog({
            importId: importLog.id,
            orgId,
            business: "rasset",
            rowNumber,
            phone: "",
            company: payload.company,
            website: payload.website,
            status: "error",
            message: "Missing phone number",
          });

          continue;
        }

        const { data: existingByPhone, error: phoneError } = await supabase
          .from("rasset_leads")
          .select("id, phone, company, business_name")
          .eq("org_id", orgId)
          .eq("phone", payload.phone)
          .or("is_deleted.is.null,is_deleted.eq.false")
          .maybeSingle();

        if (phoneError) throw phoneError;

        if (existingByPhone) {
          results.duplicates += 1;

          await addLeadImportRowLog({
            importId: importLog.id,
            orgId,
            business: "rasset",
            rowNumber,
            phone: payload.phone,
            company: payload.company,
            website: payload.website,
            status: "duplicate",
            message: "Duplicate phone number skipped",
            existingLeadId: existingByPhone.id,
          });

          continue;
        }

        const { data: insertedLead, error: insertError } = await supabase
          .from("rasset_leads")
          .insert([
            {
              org_id: orgId,
              ...payload,
            },
          ])
          .select("id")
          .single();

        if (insertError) throw insertError;

        results.inserted += 1;

        await addLeadImportRowLog({
          importId: importLog.id,
          orgId,
          business: "rasset",
          rowNumber,
          phone: payload.phone,
          company: payload.company,
          website: payload.website,
          status: "success",
          message: "Lead inserted",
          existingLeadId: insertedLead?.id || null,
        });
      } catch (error) {
        results.errors.push({
          row: rowNumber,
          error: error.message || String(error),
        });

        await addLeadImportRowLog({
          importId: importLog.id,
          orgId,
          business: "rasset",
          rowNumber,
          status: "error",
          message: error.message || String(error),
        });
      }
    }

    await finishLeadImportLog({
      importId: importLog.id,
      results,
      status: "completed",
    });

    return results;
  } catch (error) {
    await finishLeadImportLog({
      importId: importLog.id,
      results,
      status: "failed",
      errorMessage: error.message || String(error),
    });

    throw error;
  }
}

function mapExcelRowToJoolianB2BLead(row) {
  const normalized = {};

  for (const [key, value] of Object.entries(row || {})) {
    normalized[normalizeExcelHeader(key)] = value;
  }

  const get = (...keys) => {
    for (const key of keys) {
      const value = normalized[normalizeExcelHeader(key)];
      if (
        value !== undefined &&
        value !== null &&
        String(value).trim() !== ""
      ) {
        return String(value).trim();
      }
    }
    return "";
  };

  const company = get(
    "AP Name",
    "Activity Provider Name",
    "Company",
    "Business Name",
  );
  const owner = get("Owner / Founder", "Owner", "Founder", "Contact Name");
  const activityCategory = get(
    "Activity category",
    "Activity Category",
    "Category",
  );
  const subActivityCategory = get(
    "Sub Activity Category",
    "Sub Activity",
    "Sub Category",
  );
  const typeOfBusiness = get("Type of Business", "Business Type");
  const ageGroup = get("Age Group", "Ages");
  const specialNormalBoth = get(
    "Special/Normal/both",
    "Special Normal Both",
    "Special Normal",
  );
  const isHardRare = get("Is hard/Rare", "Hard Rare", "Rare", "Is Rare");
  const pricing = get("Pricing (Approx)", "Pricing", "Approx Pricing");
  const otherDetails = get("Other important details", "Other Details", "Notes");

  const smartNotes = [
    ageGroup ? `Age group: ${ageGroup}` : "",
    activityCategory ? `Category: ${activityCategory}` : "",
    subActivityCategory ? `Sub-category: ${subActivityCategory}` : "",
    typeOfBusiness ? `Business type: ${typeOfBusiness}` : "",
    specialNormalBoth ? `Special/Normal: ${specialNormalBoth}` : "",
    isHardRare ? `Hard/Rare: ${isHardRare}` : "",
    pricing ? `Pricing: ${pricing}` : "",
    otherDetails ? `Details: ${otherDetails}` : "",
  ]
    .filter(Boolean)
    .join("\n");

  return {
    company,
    business_name: company,
    contact_name: owner,
    owner_name: owner,

    phone: normalizePhoneForLogin(get("Phone Number", "Phone", "Mobile")),
    email: get("Email", "Email Address"),

    city: get("City"),
    pin_code: get("Zip code", "Zip", "Postal Code"),
    country: get("Country") || "USA",

    google_maps_url: get("Google map link", "Google Map", "Google Maps URL"),
    yelp_url: get("Yelp link", "Yelp URL", "Yelp"),
    website: get("Website", "Website URL"),

    industry: activityCategory,
    company_size: typeOfBusiness,

    age_group: ageGroup,
    activity_category: activityCategory,
    sub_activity_category: subActivityCategory,
    type_of_business: typeOfBusiness,
    special_normal_both: specialNormalBoth,
    is_hard_rare: isHardRare,
    pricing_approx: pricing,
    year_of_establishment: get("Year Established", "Year of Establishment"),

    notes: smartNotes,
    enrichment_status: "imported",

    lead_category: "b2b",
    lead_stage: normalizeText(get("lead_stage", "stage") || "prospect"),
    lead_source: "excel",
    status: normalizeText(get("status") || "new"),
  };
}

async function importJoolianB2BLeadsFromExcel({ orgId, buffer }) {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];

  if (!sheetName) {
    throw new Error("Excel file has no sheets");
  }

  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });

  const results = {
    total: rows.length,
    inserted: 0,
    updated: 0,
    skipped: 0,
    errors: [],
  };

  for (let i = 0; i < rows.length; i += 1) {
    try {
      const payload = mapExcelRowToJoolianB2BLead(rows[i]);

      if (
        !payload.company &&
        !payload.phone &&
        !payload.website &&
        !payload.google_maps_url
      ) {
        results.skipped += 1;
        continue;
      }

      let existing = null;

      if (payload.phone) {
        const { data, error } = await supabase
          .from("joolian_leads")
          .select("*")
          .eq("org_id", orgId)
          .eq("phone", payload.phone)
          .maybeSingle();

        if (error) throw error;
        existing = data;
      }

      if (!existing && payload.website) {
        const { data, error } = await supabase
          .from("joolian_leads")
          .select("*")
          .eq("org_id", orgId)
          .eq("website", payload.website)
          .maybeSingle();

        if (error) throw error;
        existing = data;
      }

      if (!existing && payload.company) {
        const { data, error } = await supabase
          .from("joolian_leads")
          .select("*")
          .eq("org_id", orgId)
          .ilike("company", payload.company)
          .maybeSingle();

        if (error) throw error;
        existing = data;
      }

      if (existing) {
        const { error } = await supabase
          .from("joolian_leads")
          .update({
            ...payload,
            updated_at: new Date().toISOString(),
          })
          .eq("org_id", orgId)
          .eq("id", existing.id);

        if (error) throw error;
        results.updated += 1;
      } else {
        const { error } = await supabase.from("joolian_leads").insert([
          {
            org_id: orgId,
            ...payload,
          },
        ]);

        if (error) throw error;
        results.inserted += 1;
      }
    } catch (error) {
      results.errors.push({
        row: i + 2,
        error: error.message,
      });
    }
  }

  return results;
}

async function deleteBusinessLead({ orgId, business, leadId }) {
  const normalizedBusiness = getBusinessCanonicalName(business);
  const tableName = getBusinessLeadTableName(normalizedBusiness);

  if (!tableName) {
    throw new Error("Unsupported business");
  }

  const { data: lead, error: fetchError } = await supabase
    .from(tableName)
    .select("*")
    .eq("org_id", orgId)
    .eq("id", leadId)
    .maybeSingle();

  if (fetchError) throw fetchError;
  if (!lead) throw new Error("Lead not found");

  const leadPhone = normalizePhoneForLogin(lead.phone || "");

  if (leadPhone) {
    const { error: voiceDeleteError } = await supabase
      .from("lead_voice_uploads")
      .delete()
      .eq("org_id", orgId)
      .eq("business", normalizedBusiness)
      .eq("lead_phone", leadPhone);

    if (voiceDeleteError) throw voiceDeleteError;
  }

  const { error: leadDeleteError } = await supabase
    .from(tableName)
    .delete()
    .eq("org_id", orgId)
    .eq("id", leadId);

  if (leadDeleteError) throw leadDeleteError;

  return {
    deleted: true,
    business: normalizedBusiness,
    tableName,
    leadId,
    leadPhone,
  };
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

async function getAllActiveUsersInOrg(orgId) {
  const { data, error } = await supabase
    .from("users")
    .select("id, name, phone_number, role, is_active")
    .eq("org_id", orgId)
    .eq("is_active", true)
    .order("name", { ascending: true });

  if (error) {
    console.error("getAllActiveUsersInOrg error:", error);
    return { users: [], error };
  }

  return { users: data || [], error: null };
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
      blocked_reason,
      business,
      area,
      assigned_to_user_id,
      waiting_on_user_id,
      waiting_since,
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

  let waitingOnName = "";

  if (data.waiting_on_user_id) {
    const { data: waitingUser, error: waitingUserError } = await supabase
      .from("users")
      .select("id, name")
      .eq("org_id", orgId)
      .eq("id", data.waiting_on_user_id)
      .maybeSingle();

    if (waitingUserError) {
      console.error("getTaskById waiting user error:", waitingUserError);
    } else {
      waitingOnName = waitingUser?.name || "";
    }
  }

  return {
    task: {
      ...data,
      owner_names: ownerNames,
      waiting_on_name: waitingOnName,
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

function analyzeAttendanceIssues(events, options = {}) {
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

  const summary = getAttendanceSummaryFromEvents(events || [], options);
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
      `Total working days this month: ${monthly.totalWorkingDays}`,
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

    oldValue = {
      blocker_note: task.blocker_note,
      blocked_reason: task.blocked_reason || null,
      waiting_on_user_id: task.waiting_on_user_id || null,
      waiting_since: task.waiting_since || null,
      status: task.status,
    };

    newValue = {
      blocker_note: editCommand.value,
      blocked_reason: editCommand.value,
      waiting_on_user_id: null,
      waiting_since: new Date().toISOString(),
      status: "blocked",
    };

    patch.blocker_note = editCommand.value;
    patch.blocked_reason = editCommand.value;
    patch.waiting_on_user_id = null;
    patch.waiting_since = new Date().toISOString();
    patch.status = "blocked";

    successMessage = `⛔ Task ${taskRef(task)} blocker updated\nBlocker: ${editCommand.value}`;
  } else if (editCommand.field === "clear_detail") {
    oldValue = { detail: task.detail };
    newValue = { detail: null };
    patch.detail = null;
    successMessage = `✏️ Task ${taskRef(task)} detail cleared`;
  } else if (editCommand.field === "clear_blocker") {
    oldValue = {
      blocker_note: task.blocker_note,
      blocked_reason: task.blocked_reason || null,
      waiting_on_user_id: task.waiting_on_user_id || null,
      waiting_since: task.waiting_since || null,
      status: task.status,
    };

    newValue = {
      blocker_note: null,
      blocked_reason: null,
      waiting_on_user_id: null,
      waiting_since: null,
      status: task.progress > 0 ? "in_progress" : "open",
    };

    patch.blocker_note = null;
    patch.blocked_reason = null;
    patch.waiting_on_user_id = null;
    patch.waiting_since = null;
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

    const shiftStartIso = await getShiftStartIsoForUserToday(
      targetUser.id,
      actingUser.org_id,
    );
    const issues = analyzeAttendanceIssues(events, { shiftStartIso });

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
          "Create:",
          "create task finalize parents pitch business joolian area parents owner zoya, niharika, aj priority high due 4 apr",
          "",
          "View:",
          "my tasks",
          "tasks Ruhab",
          "show task 2",
          "",
          "Update:",
          "progress 2 50% 20 mails sent no positive response",
          "done 2 tested and verified properly",
          "wait 23 on aj for API response",
          "waiting 23 on niharika for design confirmation",
          "clear wait 23 aj responded",
          "edit task 2 blocker waiting on backend fix",
          "edit task 2 clear blocker",
          "edit task 2 title final parents pitch v2",
          "",
          isManager ? "Manager only:" : null,
          isManager ? "cancel task 2" : null,
          isManager ? "delete task 2" : null,
          isManager ? "edit task 2 owner zoya, aj" : null,
        ]
          .filter(Boolean)
          .join("\n"),
      );
    }

    if (normalizedTopic === "attendance") {
      return sendTwiml(
        res,
        [
          "🕒 Attendance Help",
          "",
          "Self:",
          "login",
          "break",
          "back",
          "logout",
          "late 11:00 am",
          "late unsure",
          "status",
          "who am i",
          "",
          "Leave:",
          "off today",
          "leave tomorrow",
          "off 11 april",
          "",
          isManager ? "Manager extras:" : null,
          isManager ? "login Zoya" : null,
          isManager ? "logout Aj 6:30 pm" : null,
          isManager ? "break Ruhab" : null,
          isManager ? "back Mahesh" : null,
          isManager ? "late Zoya 11:00 am" : null,
          isManager ? "late Ruhab unsure" : null,
          isManager ? "off Zoya tomorrow" : null,
          isManager ? "employee summary Aj" : null,
          isManager ? "timeline Mahesh" : null,
          isManager ? "who is on break" : null,
          isManager ? "who is off today" : null,
          isManager ? "summary today" : null,
          isManager ? "now" : null,
          isManager ? "day on sunday Zoya" : null,
          isManager ? "company off today" : null,
          isManager ? "company off tomorrow" : null,
          isManager ? "company off 15 april" : null,
          isManager ? "company day on today" : null,
          isManager ? "company day on 18 april" : null,
          isManager ? "company day half sunday" : null,
          isManager ? "day on saturday Aj" : null,
          isManager ? "day half sunday Ruhab" : null,
          isManager ? "day on 11 april Mahesh" : null,
        ]
          .filter(Boolean)
          .join("\n"),
      );
    }

    if (normalizedTopic === "manager") {
      if (!isManager) {
        return sendTwiml(res, "❌ Only managers/admins can use help manager");
      }

      return sendTwiml(
        res,
        [
          "🧑‍💼 Manager Help",
          "",
          "Attendance:",
          "login Zoya",
          "logout Aj 6:30 pm",
          "break Ruhab",
          "back Mahesh",
          "late Zoya 11:00 am",
          "late Ruhab unsure",
          "off Zoya tomorrow",
          "employee summary Aj",
          "timeline Mahesh",
          "who is on break",
          "who is off today",
          "summary today",
          "day on sunday Zoya",
          "company off today",
          "company off tomorrow",
          "company off 15 april",
          "day on saturday Aj",
          "day half sunday Ruhab",
          "day on 11 april Mahesh",
          "company day on today",
          "company day on 18 april",
          "company day half sunday",
          "now",
          "",
          "Tasks:",
          "tasks Ruhab",
          "show task 2",
          "progress 2 50% 20 mails sent no positive response",
          "done 2 tested and verified properly",
          "edit task 2 blocker waiting on dependency",
          "edit task 2 clear blocker",
          "cancel task 2",
          "delete task 2",
          "edit task 2 owner zoya, aj",
        ].join("\n"),
      );
    }

    return sendTwiml(
      res,
      [
        "🤖 WeSolveHR Help",
        "",
        "Attendance:",
        "login | break | back | logout",
        "late 11:00 am | late unsure",
        "status | who am i",
        "",
        "Tasks:",
        "my tasks | show task 2",
        "progress 2 50% detailed note",
        "done 2 detailed note",
        "",
        "Create:",
        "create task <title> business <business> area <area> owner <names> priority <level> due <date>",
        "",
        "More:",
        "help attendance",
        "help tasks",
        isManager ? "help manager" : null,
      ]
        .filter(Boolean)
        .join("\n"),
    );
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
  const waitingOn =
    task.status === "blocked" && task.waiting_on_name
      ? `\nWaiting on: ${task.waiting_on_name}`
      : "";
  const blockerReason =
    task.blocked_reason || task.blocker_note
      ? `\nReason: ${task.blocked_reason || task.blocker_note}`
      : "";

  return sendTwiml(
    res,
    `Task #${task.task_no || task.id}
Owners: ${assignedTo}
Priority: ${task.priority}
Status: ${task.status}
Progress: ${task.progress}%
Title: ${task.title}
Deadline: ${task.deadline ?? "no deadline"}${detail}${waitingOn}${blockerReason}`,
  );
}

async function handleFeedbackCommand(res, actingUser, feedbackCommand) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "Only managers/admins can add feedback.");
  }

  const targetUser = await findUniqueUserByName(
    feedbackCommand.target_name,
    actingUser.org_id,
  );

  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${feedbackCommand.target_name}".`,
    );
  }

  const { error } = await supabase.from("employee_feedback").insert([
    {
      org_id: actingUser.org_id,
      user_id: targetUser.id,
      created_by_user_id: actingUser.id,
      type: feedbackCommand.type,
      note: feedbackCommand.note,
    },
  ]);

  if (error) {
    console.error("Feedback insert error:", error);
    return sendTwiml(res, "❌ Failed to save feedback.");
  }

  return sendTwiml(
    res,
    `✅ ${feedbackCommand.type} saved for ${targetUser.name}.`,
  );
}

async function handleAppraisalCommand(res, actingUser, appraisalCommand) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "Only managers/admins can add appraisals.");
  }

  const targetUser = await findUniqueUserByName(
    appraisalCommand.target_name,
    actingUser.org_id,
  );

  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${appraisalCommand.target_name}".`,
    );
  }

  const { error } = await supabase.from("employee_feedback").insert([
    {
      org_id: actingUser.org_id,
      user_id: targetUser.id,
      created_by_user_id: actingUser.id,
      type: "appraisal",
      rating: appraisalCommand.rating,
      strengths: appraisalCommand.strengths,
      improvement_areas: appraisalCommand.improvement_areas,
      manager_comment: appraisalCommand.manager_comment,
    },
  ]);

  if (error) {
    console.error("Appraisal insert error:", error);
    return sendTwiml(res, "❌ Failed to save appraisal.");
  }

  return sendTwiml(
    res,
    `✅ Appraisal saved for ${targetUser.name}. Rating: ${appraisalCommand.rating}`,
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
    const shiftStartIso = await getShiftStartIsoForUserToday(
      user.id,
      user.org_id,
    );
    const summary = getAttendanceSummaryFromEvents(userEvents, {
      shiftStartIso,
    });

    const myLate = (lateRows || []).find((x) => x.user_id === user.id) || null;
    const firstLogin = summary.firstLogin;

    const lines = [
      `👤 ${user.name}`,
      `Status: ${summary.currentStatus === "no_update" ? "No update" : summary.currentStatus}`,
      `Expected shift start: ${shiftStartIso ? formatTimeOnly(shiftStartIso) : "-"}`,
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

  const shiftStartIso = await getShiftStartIsoForUserToday(
    targetUser.id,
    actingUser.org_id,
  );
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

async function handleCompanyOffDay(res, actingUser, command) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to mark company-wide leave.");
  }

  const offDate = parseFlexibleDateText(command.off_date_text);

  if (!offDate) {
    return sendTwiml(
      res,
      `I could not understand the off date "${command.off_date_text}". Use today, tomorrow, 11 april, or april 11.`,
    );
  }

  const { users, error: usersError } = await getAllActiveUsersInOrg(
    actingUser.org_id,
  );

  if (usersError) {
    console.error("handleCompanyOffDay get users error:", usersError);
    return sendTwiml(res, "Failed to fetch team users.");
  }

  if (!users.length) {
    return sendTwiml(res, "No active users found in the company.");
  }

  const lockedUsers = [];
  const unlockedUsers = [];

  for (const user of users) {
    const locked = await isAttendanceDayLocked(
      user.id,
      offDate,
      actingUser.org_id,
    );

    if (locked) {
      lockedUsers.push(user);
    } else {
      unlockedUsers.push(user);
    }
  }

  if (!unlockedUsers.length) {
    return sendTwiml(
      res,
      `❌ Company-wide leave could not be applied because all users are locked for ${offDate}.`,
    );
  }

  const rows = unlockedUsers.map((user) => ({
    org_id: actingUser.org_id,
    user_id: user.id,
    off_date: offDate,
    note: `Company-wide leave marked by ${actingUser.name}`,
    created_by_user_id: actingUser.id,
  }));

  const { error } = await supabase
    .from("planned_time_off")
    .upsert(rows, { onConflict: "user_id,off_date" });

  if (error) {
    console.error("handleCompanyOffDay upsert error:", error);
    return sendTwiml(res, "Failed to save company-wide leave.");
  }

  for (const user of unlockedUsers) {
    await insertAttendanceAudit(
      user.id,
      actingUser.id,
      "mark_company_leave",
      null,
      { off_date: offDate },
      `Company-wide leave marked by ${actingUser.name}`,
      actingUser.org_id,
    );
  }

  const lines = [
    `🌴 Company-wide leave saved`,
    `Date: ${offDate}`,
    `Applied to: ${unlockedUsers.length} user(s)`,
  ];

  if (lockedUsers.length) {
    lines.push(
      `Skipped locked users: ${lockedUsers.map((x) => x.name).join(", ")}`,
    );
  }

  return sendTwiml(res, lines.join("\n"));
}

async function handleWorkDayOverride(res, actingUser, command) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(
      res,
      "You are not allowed to change work-day expectation.",
    );
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

  const overrideDate = parseFlexibleDateText(command.date_text);
  if (!overrideDate) {
    return sendTwiml(
      res,
      `I could not understand the date "${command.date_text}". Use today, tomorrow, 11 april, or april 11.`,
    );
  }

  const error = await upsertWorkDayOverride({
    orgId: actingUser.org_id,
    userId: targetUser.id,
    overrideDate,
    mode: command.mode,
    createdByUserId: actingUser.id,
    note: `Marked by ${actingUser.name}`,
  });

  if (error) {
    console.error("handleWorkDayOverride error:", error);
    return sendTwiml(res, "Failed to save work-day override.");
  }

  return sendTwiml(
    res,
    `✅ Work-day override saved
Name: ${targetUser.name}
Date: ${overrideDate}
Mode: ${command.mode}`,
  );
}

async function handleCompanyWorkDayOverride(res, actingUser, command) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(
      res,
      "You are not allowed to change company-wide work-day expectation.",
    );
  }

  const overrideDate = parseFlexibleDateText(command.date_text);
  if (!overrideDate) {
    return sendTwiml(
      res,
      `I could not understand the date "${command.date_text}". Use today, tomorrow, 11 april, or april 11.`,
    );
  }

  const { users, error: usersError } = await getAllActiveUsersInOrg(
    actingUser.org_id,
  );

  if (usersError) {
    console.error("handleCompanyWorkDayOverride get users error:", usersError);
    return sendTwiml(res, "Failed to fetch team users.");
  }

  if (!users.length) {
    return sendTwiml(res, "No active users found in the company.");
  }

  const lockedUsers = [];
  const unlockedUsers = [];

  for (const user of users) {
    const locked = await isAttendanceDayLocked(
      user.id,
      overrideDate,
      actingUser.org_id,
    );

    if (locked) {
      lockedUsers.push(user);
    } else {
      unlockedUsers.push(user);
    }
  }

  if (!unlockedUsers.length) {
    return sendTwiml(
      res,
      `❌ Company-wide work-day override could not be applied because all users are locked for ${overrideDate}.`,
    );
  }

  const rows = unlockedUsers.map((user) => ({
    org_id: actingUser.org_id,
    user_id: user.id,
    override_date: overrideDate,
    mode: command.mode,
    note: `Company-wide ${command.mode} marked by ${actingUser.name}`,
    created_by_user_id: actingUser.id,
  }));

  const { error } = await supabase
    .from("work_day_expectation_overrides")
    .upsert(rows, { onConflict: "org_id,user_id,override_date" });

  if (error) {
    console.error("handleCompanyWorkDayOverride upsert error:", error);
    return sendTwiml(res, "Failed to save company-wide work-day override.");
  }

  for (const user of unlockedUsers) {
    await insertAttendanceAudit(
      user.id,
      actingUser.id,
      "mark_company_work_day_override",
      null,
      { override_date: overrideDate, mode: command.mode },
      `Company-wide ${command.mode} marked by ${actingUser.name}`,
      actingUser.org_id,
    );
  }

  const lines = [
    `✅ Company-wide work-day override saved`,
    `Date: ${overrideDate}`,
    `Mode: ${command.mode}`,
    `Applied to: ${unlockedUsers.length} user(s)`,
  ];

  if (lockedUsers.length) {
    lines.push(
      `Skipped locked users: ${lockedUsers.map((x) => x.name).join(", ")}`,
    );
  }

  return sendTwiml(res, lines.join("\n"));
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

      const shiftStartIso = await getShiftStartIsoForUserToday(
        user.id,
        user.org_id,
      );
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

      const { data: todayOverride, error: todayOverrideError } = await supabase
        .from("work_day_expectation_overrides")
        .select("mode")
        .eq("org_id", user.org_id)
        .eq("user_id", user.id)
        .eq("override_date", today)
        .maybeSingle();

      if (todayOverrideError) {
        console.error("Login override lookup error:", todayOverrideError);
      }

      const fullDayReminder =
        todayOverride?.mode === "full_day"
          ? `\n⚠️ Don't forget: today is a full working day`
          : "";

      return sendTwiml(
        res,
        `✅ Logged in successfully\nWelcome, ${user.name}${fullDayReminder}${lateLine}${leaveLine}`,
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

async function getLogsPageData(orgId, filters = {}) {
  const q = String(filters.q || "").trim();
  const user = String(filters.user || "").trim();
  const outcome = String(filters.outcome || "").trim();
  const day = String(filters.day || "").trim(); // YYYY-MM-DD
  const month = String(filters.month || "").trim(); // YYYY-MM

  let startDate = null;
  let endDate = null;

  if (day) {
    startDate = `${day}T00:00:00+05:30`;
    endDate = `${day}T23:59:59+05:30`;
  } else if (month) {
    const [year, monthNum] = month.split("-").map(Number);
    const nextMonth = monthNum === 12 ? 1 : monthNum + 1;
    const nextYear = monthNum === 12 ? year + 1 : year;
    startDate = `${month}-01T00:00:00+05:30`;
    endDate = `${nextYear}-${String(nextMonth).padStart(2, "0")}-01T00:00:00+05:30`;
  }

  let logsQuery = supabase
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
    .order("created_at", { ascending: false })
    .limit(250);

  if (orgId != null) logsQuery = logsQuery.eq("org_id", orgId);
  if (startDate) logsQuery = logsQuery.gte("created_at", startDate);
  if (endDate) logsQuery = logsQuery.lt("created_at", endDate);

  if (user) {
    logsQuery = logsQuery.or(
      `profile_name.ilike.%${user}%,phone_number.ilike.%${user}%`,
    );
  }

  if (q) {
    logsQuery = logsQuery.or(
      `message_text.ilike.%${q}%,twilio_message_sid.ilike.%${q}%`,
    );
  }

  const { data: logs, error: logsError } = await logsQuery;
  if (logsError) throw logsError;

  const messageSids = [
    ...new Set(
      (logs || []).map((row) => row.twilio_message_sid).filter(Boolean),
    ),
  ];

  let processingMap = new Map();

  if (messageSids.length) {
    let processingQuery = supabase
      .from("inbound_message_processing")
      .select(
        `
        message_sid,
        org_id,
        status,
        result_type,
        result_ref_id,
        error_message,
        updated_at
      `,
      )
      .in("message_sid", messageSids);

    if (orgId != null) processingQuery = processingQuery.eq("org_id", orgId);

    const { data: processingRows, error: processingError } =
      await processingQuery;
    if (processingError) throw processingError;

    processingMap = new Map(
      (processingRows || []).map((row) => [row.message_sid, row]),
    );
  }

  let rows = (logs || []).map((row) => {
    const proc = processingMap.get(row.twilio_message_sid) || null;

    return {
      id: row.id,
      sender: row.profile_name || row.phone_number || "Unknown",
      body: row.message_text || "",
      message_sid: row.twilio_message_sid,
      created_at: row.created_at,
      created_at_text: row.created_at ? formatDateTime(row.created_at) : "-",
      direction: row.direction || "-",
      org_id: row.org_id,
      outcome_status: proc?.status || "unknown",
      outcome_result_type: proc?.result_type || "-",
      outcome_error: proc?.error_message || "",
      outcome_updated_at: proc?.updated_at
        ? formatDateTime(proc.updated_at)
        : "-",
    };
  });

  if (outcome) {
    rows = rows.filter(
      (row) =>
        String(row.outcome_status || "").toLowerCase() ===
        outcome.toLowerCase(),
    );
  }

  const todayIST = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());

  const thisMonthIST = todayIST.slice(0, 7);

  const byPersonToday = {};
  const byPersonMonth = {};

  rows.forEach((row) => {
    const rowDay = new Intl.DateTimeFormat("en-CA", {
      timeZone: "Asia/Kolkata",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date(row.created_at));

    const rowMonth = rowDay.slice(0, 7);
    const name = row.sender || "Unknown";

    if (rowDay === todayIST) {
      byPersonToday[name] = (byPersonToday[name] || 0) + 1;
    }

    if (rowMonth === thisMonthIST) {
      byPersonMonth[name] = (byPersonMonth[name] || 0) + 1;
    }
  });

  const people = [...new Set(rows.map((r) => r.sender).filter(Boolean))].sort();

  return {
    rows,
    people,
    stats: {
      total: rows.length,
      completed: rows.filter((r) => r.outcome_status === "completed").length,
      failed: rows.filter((r) => r.outcome_status === "failed").length,
      unknown: rows.filter((r) => r.outcome_status === "unknown").length,
      byPersonToday,
      byPersonMonth,
    },
  };
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

async function createCompanyWidePlannedOffDay(
  orgId,
  offDate,
  createdByUserId,
  note = null,
) {
  const { users, error: usersError } = await getAllActiveUsersInOrg(orgId);

  if (usersError) {
    return { error: usersError, count: 0, users: [] };
  }

  if (!users.length) {
    return { error: null, count: 0, users: [] };
  }

  const rows = users.map((user) => ({
    org_id: orgId,
    user_id: user.id,
    off_date: offDate,
    note,
    created_by_user_id: createdByUserId,
  }));

  const { error } = await supabase
    .from("planned_time_off")
    .upsert(rows, { onConflict: "user_id,off_date" });

  if (error) {
    return { error, count: 0, users: [] };
  }

  return {
    error: null,
    count: users.length,
    users,
  };
}

async function upsertWorkDayOverride({
  orgId,
  userId,
  overrideDate,
  mode,
  createdByUserId,
  note = null,
}) {
  const { error } = await supabase
    .from("work_day_expectation_overrides")
    .upsert(
      [
        {
          org_id: orgId,
          user_id: userId,
          override_date: overrideDate,
          mode,
          note,
          created_by_user_id: createdByUserId,
        },
      ],
      { onConflict: "org_id,user_id,override_date" },
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
  const shiftStartIso = await getShiftStartIsoForUserToday(userId, orgId);
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

function parseOwnerNames(ownerText) {
  if (!ownerText) return [];
  return ownerText
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

async function findUsersByNames(names, orgId) {
  const uniqueNames = [
    ...new Set((names || []).map((x) => String(x).trim()).filter(Boolean)),
  ];

  const matchedUsers = [];
  const missingNames = [];

  for (const name of uniqueNames) {
    const user = await findUniqueUserByName(name, orgId);
    if (!user) {
      missingNames.push(name);
    } else {
      matchedUsers.push(user);
    }
  }

  return { matchedUsers, missingNames };
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

async function handleWaitTask(res, user, waitCommand) {
  const cleanReason = String(waitCommand?.reason || "").trim();

  if (!cleanReason) {
    return sendTwiml(
      res,
      "Please add a reason.\nExample: wait 23 on Aj for API response",
    );
  }

  const { task, error } = await getTaskById(waitCommand.taskId, user.org_id);

  if (error) {
    return sendTwiml(res, "Failed to fetch that task.");
  }

  if (!task) {
    return sendTwiml(res, `Task #${waitCommand.taskId} not found.`);
  }

  if (!(await canModifyTask(user, task))) {
    return sendTwiml(res, "You are not allowed to modify that task.");
  }

  const waitingUser = await findUniqueUserByName(
    waitCommand.waiting_on_name,
    user.org_id,
  );

  if (!waitingUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${waitCommand.waiting_on_name}".`,
    );
  }

  const oldValue = {
    status: task.status,
    blocker_note: task.blocker_note,
    waiting_on_user_id: task.waiting_on_user_id || null,
    blocked_reason: task.blocked_reason || null,
    waiting_since: task.waiting_since || null,
  };

  const nowIso = new Date().toISOString();

  const patch = {
    status: "blocked",
    blocker_note: `Waiting on ${waitingUser.name} for ${cleanReason}`,
    waiting_on_user_id: waitingUser.id,
    blocked_reason: cleanReason,
    waiting_since: nowIso,
    last_updated_by_user_id: user.id,
    updated_at: nowIso,
  };

  const { error: updateError } = await supabase
    .from("tasks")
    .update(patch)
    .eq("id", task.id);

  if (updateError) {
    console.error("Wait task update error:", updateError);
    return sendTwiml(res, "Failed to mark task as waiting.");
  }

  await insertTaskHistory(
    task.id,
    user.id,
    "status_change",
    "waiting_on",
    oldValue,
    {
      status: "blocked",
      blocker_note: patch.blocker_note,
      waiting_on_user_id: waitingUser.id,
      waiting_on_name: waitingUser.name,
      blocked_reason: cleanReason,
      waiting_since: nowIso,
      note: cleanReason,
    },
    user.org_id,
  );

  return sendTwiml(
    res,
    `⏸ Task ${taskRef(task)} is now waiting
Title: ${task.title}
Waiting on: ${waitingUser.name}
Reason: ${cleanReason}`,
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

  const oldValue = {
    status: task.status,
    blocker_note: task.blocker_note,
    waiting_on_user_id: task.waiting_on_user_id || null,
    blocked_reason: task.blocked_reason || null,
    waiting_since: task.waiting_since || null,
  };

  const patch = {
    status: nextStatus,
    blocker_note: null,
    waiting_on_user_id: null,
    blocked_reason: null,
    waiting_since: null,
    last_updated_by_user_id: user.id,
    updated_at: new Date().toISOString(),
  };

  const { error: updateError } = await supabase
    .from("tasks")
    .update(patch)
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
    oldValue,
    {
      status: nextStatus,
      blocker_note: null,
      waiting_on_user_id: null,
      blocked_reason: null,
      waiting_since: null,
      note: cleanNote,
    },
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
      const shiftStartIso = await getShiftStartIsoForUserToday(user.id, orgId);
      const summary = getAttendanceSummaryFromEvents(userEvents, {
        shiftStartIso,
      });

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
      const userShiftStartIso = await getShiftStartIsoForUserToday(
        user.id,
        actingUser.org_id,
      );
      const summary = getAttendanceSummaryFromEvents(userEvents, {
        shiftStartIso: userShiftStartIso,
      });
      const firstLogin = summary.firstLogin;
      const lateInfo = lateByUser.get(user.id) || null;
      const workedMin = summary.workedMinutes;

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
        } else if (new Date() > new Date(userShiftStartIso)) {
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
    oldValue.blocker_note !== undefined ||
    oldValue.waiting_on_user_id !== undefined ||
    oldValue.blocked_reason !== undefined ||
    oldValue.waiting_since !== undefined;

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
  if (oldValue.waiting_on_user_id !== undefined)
    patch.waiting_on_user_id = oldValue.waiting_on_user_id;
  if (oldValue.blocked_reason !== undefined)
    patch.blocked_reason = oldValue.blocked_reason;
  if (oldValue.waiting_since !== undefined)
    patch.waiting_since = oldValue.waiting_since;

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
    hourCycle: "h23",
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
    hour: Number(out.hour) === 24 ? 0 : Number(out.hour),
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

function getAttendanceMonthNavigation(monthQuery) {
  const todayAttendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const currentMonth = todayAttendanceDate.slice(0, 7);

  const selectedMonth = /^\d{4}-\d{2}$/.test(String(monthQuery || ""))
    ? String(monthQuery)
    : currentMonth;

  const [year, month] = selectedMonth.split("-").map(Number);

  const startDate = formatDateForDbFromParts(year, month, 1);

  const nextMonthYear = month === 12 ? year + 1 : year;
  const nextMonthNumber = month === 12 ? 1 : month + 1;
  const endDateExclusive = formatDateForDbFromParts(
    nextMonthYear,
    nextMonthNumber,
    1,
  );

  const prevMonthYear = month === 1 ? year - 1 : year;
  const prevMonthNumber = month === 1 ? 12 : month - 1;
  const prevMonth = `${prevMonthYear}-${String(prevMonthNumber).padStart(2, "0")}`;

  const nextMonth = `${nextMonthYear}-${String(nextMonthNumber).padStart(2, "0")}`;

  return {
    selectedMonth,
    currentMonth,
    prevMonth,
    nextMonth,
    startDate,
    endDateExclusive,
  };
}

function getCurrentYearInTimeZone(timeZone = APP_TIMEZONE) {
  return getPartsInTimeZone(new Date(), timeZone).year;
}

function getWeekdayNameFromDateString(dateString) {
  const d = new Date(`${dateString}T00:00:00${APP_TIMEZONE_OFFSET}`);
  return d
    .toLocaleDateString("en-US", {
      timeZone: APP_TIMEZONE,
      weekday: "long",
    })
    .toLowerCase();
}

function getDefaultWorkExpectationForDate(reportDate) {
  const weekday = getWeekdayNameFromDateString(reportDate);

  if (weekday === "sunday") {
    return {
      expectedToWork: false,
      workDayWeight: 0,
      workMode: "off",
      source: "default",
      label: "Sunday off",
    };
  }

  if (weekday === "saturday") {
    return {
      expectedToWork: true,
      workDayWeight: 0.5,
      workMode: "half_day",
      source: "default",
      label: "Saturday half day",
    };
  }

  return {
    expectedToWork: true,
    workDayWeight: 1,
    workMode: "full_day",
    source: "default",
    label: "Working day",
  };
}

async function getMissingReportDatesForUserInRange({
  orgId,
  userId,
  startDate,
  endDateExclusive,
}) {
  const missingDates = [];
  const todayAttendanceDate = getAttendanceDayDateStringFromDate(new Date());

  let currentDate = startDate;

  while (currentDate < endDateExclusive && currentDate <= todayAttendanceDate) {
    const daily = await getDailyNarrativeReport({
      orgId,
      reportDate: currentDate,
      userId,
    });

    const row = (daily.users || [])[0] || null;

    if (row && row.reportStatus === "missing") {
      missingDates.push(currentDate);
    }

    currentDate = addDaysToDateString(currentDate, 1);
  }

  return missingDates;
}

function resolveWorkExpectation({ reportDate, isOnLeave, overrideMode }) {
  if (overrideMode === "half_day") {
    return {
      expectedToWork: true,
      workDayWeight: 0.5,
      workMode: "half_day",
      source: "override",
      label: "Override: half day",
    };
  }

  if (overrideMode === "full_day") {
    return {
      expectedToWork: true,
      workDayWeight: 1,
      workMode: "full_day",
      source: "override",
      label: "Override: full day",
    };
  }

  if (isOnLeave) {
    return {
      expectedToWork: false,
      workDayWeight: 0,
      workMode: "off",
      source: "leave",
      label: "On leave",
    };
  }

  return getDefaultWorkExpectationForDate(reportDate);
}

function getReportCardStatus({
  reportDate,
  isOnLeave,
  expectedToWork,
  workMode,
  hasTaskUpdates,
  hasExtraWork,
}) {
  if (!expectedToWork) {
    return {
      status: isOnLeave ? "leave" : "off",
      cardClass: isOnLeave ? "report-card-leave" : "report-card-off",
      reason: isOnLeave ? "On leave" : "Not expected to work",
    };
  }

  if (!hasTaskUpdates && !hasExtraWork) {
    return {
      status: "missing",
      cardClass: "report-card-missing",
      reason:
        workMode === "half_day"
          ? "Expected half day, but no task or extra work update"
          : "Expected full day, but no task or extra work update",
    };
  }

  if (!hasTaskUpdates || !hasExtraWork) {
    return {
      status: "partial",
      cardClass: "report-card-partial",
      reason:
        workMode === "half_day"
          ? "Half-day update is partial"
          : "Day update is partial",
    };
  }

  return {
    status: "full",
    cardClass: "report-card-full",
    reason: "Updated",
  };
}

function formatWorkDayWeight(weight) {
  if (Number(weight) === 1) return "1";
  if (Number(weight) === 0.5) return "0.5";
  return "0";
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
  return getAttendanceDayUtcRange(reportDate);
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

async function getWorkDayOverrideRowsForDate({
  orgId,
  reportDate,
  userId = null,
}) {
  let query = supabase
    .from("work_day_expectation_overrides")
    .select(
      "id, org_id, user_id, override_date, mode, note, created_by_user_id",
    )
    .eq("org_id", orgId)
    .eq("override_date", reportDate);

  if (userId) {
    query = query.eq("user_id", userId);
  }

  const { data, error } = await query;

  if (error) {
    console.error("getWorkDayOverrideRowsForDate error:", error);
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

async function getOpenBlockedCountsForUsers(orgId, userIds = []) {
  const safeUserIds = Array.from(
    new Set((userIds || []).map((x) => Number(x)).filter(Boolean)),
  );

  if (!safeUserIds.length) {
    return new Map();
  }

  const { data, error } = await supabase
    .from("task_owners")
    .select(
      `
      user_id,
      task_id,
      tasks!inner(id, org_id, status)
    `,
    )
    .eq("org_id", orgId)
    .in("user_id", safeUserIds);

  if (error) {
    console.error("getOpenBlockedCountsForUsers error:", error);
    return new Map();
  }

  const counts = new Map();

  for (const userId of safeUserIds) {
    counts.set(userId, { open: 0, blocked: 0 });
  }

  for (const row of data || []) {
    const task = row.tasks;
    if (!task || task.org_id !== orgId) continue;
    if (["done", "archived", "cancelled"].includes(task.status)) continue;

    const current = counts.get(row.user_id) || { open: 0, blocked: 0 };
    current.open += 1;
    if (task.status === "blocked") current.blocked += 1;
    counts.set(row.user_id, current);
  }

  return counts;
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
  const off = [];

  for (const user of users || []) {
    if (user.reportStatus === "leave") {
      onLeave.push(user.userName);
      continue;
    }

    if (user.reportStatus === "off") {
      off.push(user.userName);
      continue;
    }

    if (user.reportStatus === "full") {
      full.push(user.userName);
      continue;
    }

    if (user.reportStatus === "partial") {
      partial.push(user.userName);
      continue;
    }

    missing.push(user.userName);
  }

  return { full, partial, missing, onLeave, off };
}

function linkifyTaskSentence(sentence, taskNo, taskId) {
  const safeSentence = escapeHtml(sentence || "");
  const clickable = `<button type="button" class="task-inline-link" onclick="openTaskDetail(${Number(taskNo)})">#${escapeHtml(taskNo)}</button>`;
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
    isOnLeave: false,
    expectedToWork: false,
    workDayWeight: 0,
    workMode: "off",
    workRuleSource: "default",
    reportStatus: "off",
    reportCardClass: "report-card-off",
    reportReason: "Not expected to work",
  };
}

async function getDailyNarrativeReport({
  orgId,
  reportDate,
  userId = null,
  includeUsers = true,
}) {
  let users = [];

  if (includeUsers) {
    let usersQuery = supabase
      .from("users")
      .select("id, name, role")
      .eq("org_id", orgId)
      .eq("is_active", true)
      .order("name", { ascending: true });

    if (userId) {
      usersQuery = usersQuery.eq("id", userId);
    }

    const { data: userRows, error: usersError } = await usersQuery;
    if (usersError) {
      throw usersError;
    }

    users = userRows || [];
  }

  const [taskNarratives, extraNotes, plannedOffRows, overrideRows] =
    await Promise.all([
      getDailyTaskNarratives({ orgId, reportDate, userId }),
      getDailyReportNotes({ orgId, reportDate, userId }),
      getPlannedOffRowsForDate(reportDate, orgId),
      getWorkDayOverrideRowsForDate({ orgId, reportDate, userId }),
    ]);

  if (!includeUsers) {
    const emptyUsers = [];
    return {
      reportDate,
      users: emptyUsers,
      compliance: classifyReportUsers(emptyUsers),
    };
  }

  const leaveSet = new Set((plannedOffRows || []).map((x) => x.user_id));

  const narrativesByUser = new Map();
  for (const item of taskNarratives) {
    if (!narrativesByUser.has(item.userId)) {
      narrativesByUser.set(item.userId, []);
    }
    narrativesByUser.get(item.userId).push(item);
  }

  const notesByUser = new Map();
  for (const note of extraNotes) {
    if (!notesByUser.has(note.user_id)) {
      notesByUser.set(note.user_id, []);
    }
    notesByUser.get(note.user_id).push(note.note);
  }

  const overridesByUser = new Map();
  for (const row of overrideRows || []) {
    overridesByUser.set(row.user_id, row);
  }

  const countsByUser = await getOpenBlockedCountsForUsers(
    orgId,
    (users || []).map((u) => u.id),
  );

  const resultUsers = [];

  for (const user of users || []) {
    const row = emptyUserDailyReport(user);

    row.taskNarratives = narrativesByUser.get(user.id) || [];
    row.extraWork = notesByUser.get(user.id) || [];
    row.summary = countsByUser.get(user.id) || { open: 0, blocked: 0 };
    row.isOnLeave = leaveSet.has(user.id);

    const overrideMode = overridesByUser.get(user.id)?.mode || null;

    const expectation = resolveWorkExpectation({
      reportDate,
      isOnLeave: row.isOnLeave,
      overrideMode,
    });

    row.expectedToWork = expectation.expectedToWork;
    row.workDayWeight = expectation.workDayWeight;
    row.workMode = expectation.workMode;
    row.workRuleSource = expectation.source;

    const hasTaskUpdates = row.taskNarratives.length > 0;
    const hasExtraWork = row.extraWork.length > 0;

    const cardStatus = getReportCardStatus({
      reportDate,
      isOnLeave: row.isOnLeave,
      expectedToWork: row.expectedToWork,
      workMode: row.workMode,
      hasTaskUpdates,
      hasExtraWork,
    });

    row.reportStatus = cardStatus.status;
    row.reportCardClass = cardStatus.cardClass;
    row.reportReason = cardStatus.reason;

    row.compactMeta = `${buildCompactUserMeta(row)} · day ${formatWorkDayWeight(row.workDayWeight)}`;

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

function renderLeadsOverviewPage(data) {
  const summary = data?.summary || {};
  const businesses = data?.businesses || [];
  const recent = data?.recent || [];

  const businessRowsHtml = businesses.length
    ? businesses
        .map((b) => {
          const attention =
            Number(b.in_progress || 0) > 0 || Number(b.leads || 0) > 0;
          return `
            <tr class="business-row" onclick="window.location.href='/leads/${encodeURIComponent(b.business)}'">
              <td>
                <div class="business-name">${escapeHtml(b.label || b.business)}</div>
                <div class="business-subtitle">${escapeHtml(b.business)}</div>
              </td>
              <td><strong>${escapeHtml(b.total || 0)}</strong></td>
              <td>${escapeHtml(b.leads || 0)}</td>
              <td>
                <span class="${Number(b.in_progress || 0) > 0 ? "badge badge-warn" : "badge badge-muted"}">
                  ${escapeHtml(b.in_progress || 0)}
                </span>
              </td>
              <td>
                <span class="badge badge-ok">${escapeHtml(b.completed || 0)}</span>
              </td>
              <td>
                <span class="${attention ? "attention-dot active" : "attention-dot"}"></span>
                ${attention ? "Needs review" : "Clean"}
              </td>
              <td class="open-cell">Open →</td>
            </tr>
          `;
        })
        .join("")
    : `<tr><td colspan="7" class="empty-cell">No businesses found.</td></tr>`;

  const recentRowsHtml = recent.length
    ? recent
        .map(
          (lead) => `
            <tr>
              <td>${escapeHtml(formatDateTime(lead.created_at))}</td>
              <td><a href="/leads/${encodeURIComponent(lead.business)}">${escapeHtml(lead.business)}</a></td>
              <td>${escapeHtml(lead.lead_phone)}</td>
              <td>${escapeHtml(lead.sender_phone)}</td>
              <td><span class="${badgeClass(lead.status)}">${escapeHtml(lead.status)}</span></td>
            </tr>
          `,
        )
        .join("")
    : `<tr><td colspan="5" class="empty-cell">No recent voice uploads.</td></tr>`;

  return `
    <html>
      <head>
        <title>Leads | WeSolveHR</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}
          ${buildTopNavCss()}

          .wrap {
            max-width: 1600px;
            margin: 0 auto;
            padding: 18px 16px 32px;
          }

          .topbar, .panel, .stat-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
          }

          .topbar {
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:16px;
            flex-wrap:wrap;
            margin-bottom:14px;
            padding:18px 20px;
          }

          .eyebrow {
            font-size:11px;
            letter-spacing:0.16em;
            text-transform:uppercase;
            color:var(--primary);
            font-weight:800;
            margin-bottom:8px;
          }

          h1 {
            margin:0;
            font-size:32px;
            letter-spacing:-0.04em;
          }

          .subtitle {
            color:var(--muted);
            margin-top:8px;
            font-size:14px;
          }

          .stats {
            display:grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap:10px;
            margin-bottom:14px;
          }

          .stat-card {
            padding:12px 14px;
          }

          .stat-label {
            color:var(--muted);
            font-size:11px;
            text-transform:uppercase;
            letter-spacing:0.10em;
            font-weight:800;
          }

          .stat-value {
            margin-top:6px;
            font-size:34px;
            line-height:1;
            font-weight:950;
          }

          .toolbar {
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
            margin-bottom:12px;
            flex-wrap:wrap;
          }

          .toolbar-title {
            font-size:18px;
            font-weight:900;
          }

          .toolbar-actions {
            display:flex;
            gap:8px;
            flex-wrap:wrap;
          }

          .filter-chip {
            padding:8px 11px;
            border-radius:999px;
            border:1px solid rgba(255,255,255,0.12);
            background:rgba(255,255,255,0.05);
            color:var(--text);
            font-size:12px;
            font-weight:800;
          }

          .panel {
            padding:16px;
            margin-bottom:14px;
          }

          table {
            width:100%;
            border-collapse:collapse;
          }

          th, td {
            padding:12px 14px;
            border-bottom:1px solid rgba(255,255,255,0.08);
            text-align:left;
            font-size:14px;
            vertical-align:middle;
          }

          th {
            color:var(--muted);
            text-transform:uppercase;
            letter-spacing:0.08em;
            font-size:11px;
            font-weight:900;
          }

          .business-row {
            cursor:pointer;
            transition: background 0.12s ease, transform 0.12s ease;
          }

          .business-row:hover {
            background:rgba(255,255,255,0.045);
          }

          .business-name {
            font-size:16px;
            font-weight:950;
            color:var(--text-strong);
            text-transform:capitalize;
          }

          .business-subtitle {
            margin-top:3px;
            color:var(--muted);
            font-size:12px;
          }

          .open-cell {
            color:var(--primary);
            font-weight:900;
            white-space:nowrap;
          }

          .badge {
            display:inline-flex;
            padding:6px 9px;
            border-radius:999px;
            font-size:12px;
            font-weight:900;
          }

          .badge-ok {
            background:var(--success-soft);
            color:var(--text-strong);
          }

          .badge-warn {
            background:var(--accent-soft);
            color:var(--text-strong);
          }

          .badge-muted {
            background:rgba(255,255,255,0.08);
            color:var(--text);
          }

          .attention-dot {
            width:8px;
            height:8px;
            border-radius:999px;
            display:inline-block;
            margin-right:7px;
            background:rgba(255,255,255,0.25);
          }

          .attention-dot.active {
            background:var(--accent);
            box-shadow:0 0 0 4px rgba(243,181,98,0.12);
          }

          .recent-panel {
            margin-top:14px;
          }

          @media (max-width: 900px) {
            .stats {
              grid-template-columns: repeat(2, minmax(0, 1fr));
            }

            .panel {
              overflow-x:auto;
            }

            table {
              min-width:760px;
            }
          }

          @media (max-width: 600px) {
            .stats {
              grid-template-columns:1fr;
            }
          }
        </style>
      </head>

      <body>
        ${renderTopNav("leads")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Voice Upload Inbox</div>
              <h1>Leads Overview</h1>
              <div class="subtitle">Voice leads received from WhatsApp, grouped by business.</div>
            </div>
          </div>

          <div class="stats">
            <div class="stat-card"><div class="stat-label">Total</div><div class="stat-value">${summary.total || 0}</div></div>
            <div class="stat-card"><div class="stat-label">Leads</div><div class="stat-value">${summary.leads || 0}</div></div>
            <div class="stat-card"><div class="stat-label">In Progress</div><div class="stat-value">${summary.in_progress || 0}</div></div>
            <div class="stat-card"><div class="stat-label">Completed</div><div class="stat-value">${summary.completed || 0}</div></div>
          </div>

          <div class="panel">
            <div class="toolbar">
              <div>
                <div class="toolbar-title">Businesses</div>
                <div class="subtitle">Click any row to open that business lead inbox.</div>
              </div>

              <div class="toolbar-actions">
                <span class="filter-chip">All: ${businesses.length}</span>
                <span class="filter-chip">Needs Review: ${businesses.filter((b) => Number(b.in_progress || 0) > 0 || Number(b.leads || 0) > 0).length}</span>
              </div>
            </div>

            <table>
              <thead>
                <tr>
                  <th>Business</th>
                  <th>Total</th>
                  <th>Leads</th>
                  <th>In Progress</th>
                  <th>Completed</th>
                  <th>Status</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>${businessRowsHtml}</tbody>
            </table>
          </div>

          <div class="panel recent-panel">
            <div class="toolbar">
              <div class="toolbar-title">Recent Voice Uploads</div>
            </div>

            <table>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Business</th>
                  <th>Lead Phone</th>
                  <th>Uploaded By</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>${recentRowsHtml}</tbody>
            </table>
          </div>
        </div>
      </body>
    </html>
  `;
}

function renderBusinessLeadsPage(data) {
  const business = data.business;
  const rows = data.rows || [];
  const selectedTab = data.selectedTab || "all";
  const counts = data.counts || {};
  const search = data.search || "";
  const pagination = data.pagination || {};
  const filters = data.filters || {};
  const filterQuery = new URLSearchParams({
    tab: selectedTab,
    search: search || "",
    industry: filters.industry || "",
    capability: filters.capability || "",
    entity_type: filters.entity_type || "",
    status: filters.status || "",
    city: filters.city || "",
    state: filters.state || "",
    assigned_to: filters.assigned_to || "",
    qualified: filters.qualified || "",
    worth_talking: filters.worth_talking || "",
  }).toString();
  const tabLink = (key, label, count) => `
    <a class="tab ${selectedTab === key ? "active" : ""}"
       href="/leads/${encodeURIComponent(business)}?tab=${key}&search=${encodeURIComponent(search)}">
      ${label} (${count || 0})
    </a>
  `;

  const leadRowsHtml =
    selectedTab !== "voice_inbox"
      ? rows.length
        ? rows
            .map(
              (lead) => `
              <tr>
                <td class="lead-name-cell">
                  <div class="lead-company-name">
                    ${escapeHtml(lead.company || lead.business_name || lead.company_name || "Lead #" + lead.id)}
                    ${lead.factory_setup === "multiple_sites" ? `<span class="mini-chip">Multi-site</span>` : ""}
                  </div>

                  <div class="muted lead-contact-line">
                    ${escapeHtml([lead.contact_name || lead.owner_name, lead.phone].filter(Boolean).join(" · ") || "-")}
                  </div>

                  ${
                    lead.last_spoke_to_name
                      ? `
                        <div style="font-size:12px; margin-top:4px;">
                          <strong>Spoke to:</strong> ${escapeHtml(lead.last_spoke_to_name)}
                        </div>
                      `
                      : ""
                  }
                </td>

<td>
  <div class="lead-chip-row">
    ${
      lead.manufacturing_capabilities
        ? String(lead.manufacturing_capabilities)
            .split(",")
            .filter(Boolean)
            .slice(0, 4)
            .map(
              (x) => `<span class="lead-chip">${escapeHtml(x.trim())}</span>`,
            )
            .join("")
        : `<span class="muted">No capabilities</span>`
    }
  </div>
</td>

                <td>
                  <div><strong>${escapeHtml(lead.industry_primary || lead.industry || "-")}</strong></div>
                  <div class="muted">${escapeHtml(lead.raw_industry || "")}</div>

                  <div class="lead-chip-row">
                    ${lead.entity_type ? `<span class="lead-chip">${escapeHtml(lead.entity_type)}</span>` : ""}
                    ${lead.company_size ? `<span class="lead-chip">${escapeHtml(lead.company_size)}</span>` : ""}
                    ${lead.assigned_to ? `<span class="lead-chip">${escapeHtml(lead.assigned_to)}</span>` : ""}
                  </div>
                </td>

                <td>
                  <div>${escapeHtml([lead.city, lead.state, lead.country].filter(Boolean).join(", ") || "-")}</div>
                  <div class="muted">${escapeHtml(lead.pin_code || lead.location || "")}</div>
                </td>

                <td style="text-align:left; padding:10px 12px; min-width:135px;">
                  <div style="display:flex; flex-direction:column; gap:7px; align-items:flex-start;">
                    <label style="display:flex; align-items:center; gap:8px; cursor:pointer; white-space:nowrap;">
                      <input
                        type="checkbox"
                        style="margin:0; width:auto;"
                        ${lead.l2_done ? "checked" : ""}
                        onclick="toggleLeadCheckbox(event, '${escapeHtml(business)}', ${Number(lead.id)}, 'l2_done', this.checked)"
                      />
                      <span>L2 Done</span>
                    </label>

                  </div>
                </td>

                <td>
                  ${
                    business === "joolian"
                      ? `
      <div>${escapeHtml(lead.activity_category || lead.industry || "-")}</div>
      <div class="muted">${escapeHtml(lead.sub_activity_category || "")}</div>
      <div class="muted">Ages: ${escapeHtml(lead.age_group || "-")}</div>
      <div class="muted">Type: ${escapeHtml(lead.type_of_business || lead.company_size || "-")}</div>
      <div class="muted">Price: ${escapeHtml(lead.pricing_approx || "-")}</div>
    `
                      : `
      <div>${escapeHtml(lead.industry || "-")}</div>
      <div class="muted">Emp: ${escapeHtml(lead.number_of_employees || "-")}</div>
      <div class="muted">Machines: ${escapeHtml(lead.machine_count || "-")}</div>
    `
                  }                </td>

                <td>
                  <button class="btn" type="button" onclick="openCallSummaryModal('${escapeHtml(business)}', '${escapeHtml(lead.phone || "")}')">
                    Calls
                  </button>
                </td>

                <td class="actions-cell">
<button class="kebab-btn" type="button" onclick="toggleLeadActions(event, ${Number(lead.id)})">...</button>
                  <div id="leadActions-${Number(lead.id)}" class="lead-actions-menu">
                    <button type="button" onclick="openLeadEditModal(${Number(lead.id)})">Edit</button>
                    <button type="button" onclick="updateBusinessLeadStatus('${escapeHtml(business)}', ${Number(lead.id)}, 'new')">Mark New</button>
                    <button type="button" onclick="updateBusinessLeadStatus('${escapeHtml(business)}', ${Number(lead.id)}, 'in_progress')">Mark In Progress</button>
                    <button type="button" onclick="updateBusinessLeadStatus('${escapeHtml(business)}', ${Number(lead.id)}, 'completed')">Mark Completed</button>
                    <button type="button" class="danger-menu-item" onclick="deleteBusinessLead('${escapeHtml(business)}', ${Number(lead.id)})">Delete</button>
                  </div>
                </td>
              </tr>
            `,
            )
            .join("")
        : `<tr><td colspan="7" class="empty-cell">No leads found.</td></tr>`
      : "";

  const renderConversationRows = (lead) => {
    const rows = Array.isArray(lead.conversation_rows)
      ? lead.conversation_rows
      : [];

    if (!rows.length) {
      return `
      <textarea id="translated-${Number(lead.id)}" class="compact-transcript-textarea">${escapeHtml(lead.translated_text || "")}</textarea>
    `;
    }

    return `
    <div class="conversation-thread">
      ${rows
        .map((row, index) => {
          const speaker =
            row.speaker || row.person || row.role || `Person ${index + 1}`;
          const text = row.text || row.message || row.content || "";

          return `
            <div class="conversation-row">
              <div class="speaker-pill">${escapeHtml(speaker)}</div>
              <div class="conversation-text">${escapeHtml(text)}</div>
            </div>
          `;
        })
        .join("")}
    </div>

    <textarea id="translated-${Number(lead.id)}" class="compact-transcript-textarea hidden-transcript">${escapeHtml(lead.translated_text || "")}</textarea>
  `;
  };

  const voiceInboxHtml =
    selectedTab === "voice_inbox"
      ? rows.length
        ? `
        <div style="display:flex; justify-content:flex-end; margin-bottom:12px;">
          <button class="btn btn-danger" type="button" onclick="deleteSelectedVoiceUploads()">
            Delete Selected Voice Messages
          </button>
        </div>

${rows
  .map(
    (lead) => `
    <div class="lead-card compact-voice-card" id="lead-card-${Number(lead.id)}">

<!-- HEADER -->
<div class="voice-header">
  <div class="voice-main">
    <div class="voice-title-row">
      <label class="voice-check" title="Select for bulk delete">
        <input type="checkbox" class="voice-delete-checkbox" value="${Number(lead.id)}">
      </label>

      <div>
        <div class="voice-title">Voice Lead #${escapeHtml(lead.id)}</div>
        <div class="voice-meta">
          ${escapeHtml(formatDateTime(lead.created_at))}
          · ${escapeHtml(lead.lead_phone)}
          · ${escapeHtml(lead.sender_phone)}
        </div>
      </div>
    </div>
  </div>

  <div class="voice-side">
    <span class="${badgeClass(lead.status)}">${escapeHtml(lead.status)}</span>

    <button class="kebab-btn" type="button" onclick="toggleVoiceActions(event, ${Number(lead.id)})">⋯</button>

    <div id="voiceActions-${Number(lead.id)}" class="lead-actions-menu">
      <button type="button" onclick="deleteVoiceUpload(${Number(lead.id)})">Delete Voice</button>

      ${
        lead.status === "pending_review"
          ? `
            <button type="button" onclick="saveTranscript(${Number(lead.id)})">Save</button>
            <button type="button" onclick="approveLead(${Number(lead.id)})">Approve</button>
            <button type="button" onclick="rejectLead(${Number(lead.id)})">Reject</button>
            <button type="button" onclick="deleteVoiceTranscript(${Number(lead.id)})">Delete Transcript</button>
          `
          : ""
      }

      ${
        lead.status === "rejected"
          ? `<button type="button" onclick="saveTranscript(${Number(lead.id)})">Edit & Reopen</button>`
          : ""
      }
    </div>
  </div>
</div>
      <!-- AUDIO -->
      <div class="voice-audio">
        <audio controls preload="none">
          <source src="/api/lead-voice-uploads/${Number(lead.id)}/audio" type="${escapeHtml(lead.media_content_type || "audio/mpeg")}">
        </audio>
      </div>
      
<details class="voice-transcript">
  <summary>
    <span>🗣 Transcript</span>
    <span class="transcript-preview">
      ${escapeHtml((lead.translated_text || "").slice(0, 160))}
      ${(lead.translated_text || "").length > 160 ? "..." : ""}
    </span>
  </summary>

  <textarea id="translated-${Number(lead.id)}" class="transcript-textarea">${escapeHtml(lead.translated_text || "")}</textarea>
</details>

    </div>
  `,
  )
  .join("")}

      `
        : `<div class="panel">No voice leads need review.</div>`
      : "";

  return `
    <html>
      <head>
        <title>${escapeHtml(business)} Leads | WeSolveHR</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}
          ${buildTopNavCss()}

.lead-status-select {
  width: 92px;
  max-width: 92px;
  padding: 8px;
  border-radius: 10px;
  font-size: 12px;
}
.advanced-filter-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 10px;
}

.advanced-filter-grid input,
.advanced-filter-grid select {
  width: 100%;
  padding: 11px 12px;
  border-radius: 12px;
  border: 1px solid var(--line);
  background: rgba(255,255,255,0.04);
  color: var(--text);
  font: inherit;
}

.lead-chip-row {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  margin-top: 8px;
}

.lead-chip {
  display: inline-flex;
  padding: 5px 8px;
  border-radius: 999px;
  background: rgba(255,255,255,0.07);
  border: 1px solid rgba(255,255,255,0.10);
  font-size: 11px;
  font-weight: 800;
  color: var(--text);
}

.lead-chip.primary {
  background: var(--primary-soft);
  border-color: color-mix(in srgb, var(--primary) 45%, transparent);
}

.lead-chip.good {
  background: var(--success-soft);
}

.lead-chip.warn {
  background: var(--accent-soft);
}

.lead-chip.danger {
  background: var(--danger-soft);
}

@media (max-width: 1200px) {
  .advanced-filter-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 760px) {
  .advanced-filter-grid {
    grid-template-columns: 1fr;
  }
}

          .wrap { max-width: 1600px; margin: 0 auto; padding: 24px 18px 36px; }
          .topbar, .panel, .stat-card, .lead-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
          }
          .topbar {
            display:flex; justify-content:space-between; align-items:center;
            gap:16px; flex-wrap:wrap; margin-bottom:20px; padding:18px 20px;
          }
          .eyebrow { font-size:11px; letter-spacing:0.16em; text-transform:uppercase; color:var(--primary); font-weight:700; margin-bottom:8px; }
          h1 { margin:0; font-size:30px; letter-spacing:-0.04em; text-transform:capitalize; }
          .subtitle { color:var(--muted); margin-top:8px; font-size:14px; }

          .stats { display:grid; grid-template-columns:repeat(4, minmax(0,1fr)); gap:12px; margin-bottom:20px; }
          .stat-card { padding:14px; }
          .stat-label { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:0.08em; font-weight:700; }
          .stat-value { margin-top:10px; font-size:28px; font-weight:800; }

          .tabs {
            display:grid; grid-template-columns:repeat(6, minmax(0,1fr)); gap:0;
            margin-bottom:18px; background:rgba(255,255,255,0.035);
            border:1px solid rgba(255,255,255,0.10); border-radius:16px; padding:6px;
          }
          .tab {
            min-height:44px; display:flex; align-items:center; justify-content:center;
            text-align:center; padding:9px 10px; border-radius:12px; font-size:13px;
            font-weight:800; text-decoration:none; color:var(--muted); border:1px solid transparent;
          }
          .tab.active {
            background:var(--primary-soft);
            border-color:color-mix(in srgb, var(--primary) 55%, transparent);
            color:var(--text-strong);
          }
          
          
.compact-voice-card {
  padding: 16px 18px;
}

.voice-header {
  display: flex;
  justify-content: space-between;
  gap: 18px;
  align-items: flex-start;
}

.voice-main {
  min-width: 0;
  flex: 1;
}

.voice-title-row {
  display: flex;
  align-items: flex-start;
  gap: 12px;
}

.voice-check {
  margin-top: 4px;
  display: inline-flex;
}

.voice-title {
  font-size: 17px;
  font-weight: 900;
}

.voice-meta {
  margin-top: 4px;
  color: var(--muted);
  font-size: 13px;
}

.voice-side {
  display: flex;
  align-items: center;
  gap: 8px;
  position: relative;
  flex-shrink: 0;
}

.kebab-btn {
  width: 38px;
  height: 38px;
  border-radius: 12px;
  border: 1px solid rgba(255,255,255,0.12);
  background: rgba(255,255,255,0.06);
  color: var(--text);
  font-size: 20px;
  font-weight: 900;
  cursor: pointer;
}

.voice-audio {
  margin-top: 12px;
}

.voice-audio audio {
  width: 100%;
  max-width: 420px;
  height: 38px;
}

.voice-transcript {
  margin-top: 10px;
  padding-top: 8px;
  border-top: 1px solid rgba(255,255,255,0.08);
}

.voice-transcript summary {
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
  color: var(--text);
  font-size: 13px;
  font-weight: 800;
}

.transcript-preview {
  color: var(--muted);
  font-size: 12px;
  font-weight: 500;
  min-width: 0;
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
}

.transcript-textarea {
  width: 100%;
  min-height: 180px;
  max-height: 320px;
  margin-top: 10px;
  padding: 12px 13px;
  border-radius: 14px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.045);
  color: var(--text);
  font: 13px/1.5 Inter, ui-sans-serif, system-ui;
  resize: vertical;
}

@media (max-width: 760px) {
  .voice-card-header {
    flex-direction: column;
  }

  .conversation-row {
    grid-template-columns: 1fr;
  }

  .speaker-pill {
    width: fit-content;
  }
}
          
          .lead-name-cell {
  min-width: 520px;
  max-width: 620px;
}

.lead-company-name {
  font-weight: 900;
  line-height: 1.35;
  white-space: normal;
}

.lead-contact-line {
  margin-top: 4px;
  line-height: 1.35;
  white-space: normal;
}

.mini-chip {
  display: inline-flex;
  margin-left: 8px;
  padding: 3px 7px;
  border-radius: 999px;
  background: var(--info-soft);
  font-size: 11px;
  font-weight: 800;
}

.actions-cell {
  position: relative;
  width: 70px;
}

.kebab-btn {
  padding: 8px 11px;
  border-radius: 10px;
  border: 1px solid var(--line);
  background: rgba(255,255,255,0.05);
  color: var(--text);
  font-weight: 900;
  cursor: pointer;
}

.lead-actions-menu {
  display: none;
  position: fixed;
  min-width: 180px;
  z-index: 9999;
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  border-radius: 14px;
  box-shadow: var(--shadow-soft);
  padding: 8px;
}

.lead-actions-menu.open {
  display: block !important;
}

.lead-actions-menu button {
  display: block;
  width: 100%;
  text-align: left;
  padding: 10px 11px;
  border: 0;
  border-radius: 10px;
  color: var(--text);
  background: transparent;
  font-weight: 800;
  cursor: pointer;
}

.lead-actions-menu button:hover {
  background: rgba(255,255,255,0.07);
}

.danger-menu-item {
  color: #ffd7da !important;
}

          .panel { padding:18px; margin-bottom:16px; }
          .lead-list { display:grid; gap:14px; }
          .lead-card { padding:16px; }
          .lead-card-top {
            display:flex; justify-content:space-between; gap:12px; align-items:flex-start;
            margin-bottom:12px;
          }
          .lead-title { font-size:18px; font-weight:900; margin-bottom:6px; }
          .lead-actions { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:14px; }
          .transcript-grid {
            display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:12px;
          }
          .form-grid {
            display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:12px;
          }
          .form-field { display:flex; flex-direction:column; gap:8px; }
          .form-field label { font-size:13px; font-weight:800; }
          input, select, textarea {
            width:100%; padding:12px; border-radius:12px;
            border:1px solid var(--line); background:rgba(255,255,255,0.04);
            color:var(--text); font:inherit;
          }
          textarea { min-height:100px; }

          table { width:100%; border-collapse:collapse; }
          th, td { padding:12px; border-bottom:1px solid rgba(255,255,255,0.08); text-align:left; font-size:14px; vertical-align:top; }
          th { color:var(--muted); text-transform:uppercase; letter-spacing:0.08em; font-size:12px; }

          .btn {
            display:inline-flex; align-items:center; justify-content:center;
            text-decoration:none; color:var(--text); padding:10px 13px;
            border-radius:12px; background:rgba(255,255,255,0.05);
            border:1px solid rgba(255,255,255,0.12); font-weight:800; cursor:pointer;
          }
          
          .lead-status-select {
  width: 92px;
  max-width: 92px;
  padding: 8px;
  border-radius: 10px;
  font-size: 12px;
}

.call-summary-card {
  padding: 14px;
  margin-bottom: 12px;
  border-radius: 16px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.035);
}

        
        .upload-wrapper {
  margin-bottom: 12px;
}

.upload-btn {
  background: linear-gradient(135deg, #6b7cff, #8a5cff);
  color: white;
  border: none;
  padding: 10px 14px;
  border-radius: 10px;
  font-weight: 800;
  cursor: pointer;
  font-size: 13px;
}

.upload-btn:hover {
  opacity: 0.9;
}

.upload-box {
  margin-top: 10px;
  padding: 12px;
  border-radius: 12px;
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
}

.hidden {
  display: none;
}

.upload-note {
  margin-top: 8px;
  font-size: 12px;
  color: #aaa;
}

          .btn-primary {
            background:var(--primary-soft); color:var(--text-strong);
            border-color:color-mix(in srgb, var(--primary) 55%, transparent);
          }
          .btn-success {
            background:var(--success-soft); color:var(--text-strong);
            border-color:color-mix(in srgb, var(--success) 55%, transparent);
          }
          .btn-danger {
            background:var(--danger-soft); color:var(--text-strong);
            border-color:color-mix(in srgb, var(--danger) 55%, transparent);
          }

          .modal {
            display:none; position:fixed; inset:0; z-index:9999;
            background:rgba(3,8,20,0.70); backdrop-filter:blur(8px);
            align-items:center; justify-content:center; padding:18px;
          }
          .modal.open { display:flex; }
          .modal-card {
            width:min(980px,100%); max-height:90vh; overflow:auto;
            background:linear-gradient(180deg, var(--panel), var(--panel-strong));
            border:1px solid var(--line); border-radius:22px;
            box-shadow:var(--shadow-soft); padding:18px;
          }

          .search-row {
            display:grid; grid-template-columns:1fr auto auto; gap:10px; align-items:center;
          }

          .pagination {
            display:flex; justify-content:flex-end; gap:10px; margin-top:14px;
          }

          @media (max-width: 1000px) {
            .stats, .tabs, .transcript-grid, .form-grid, .search-row { grid-template-columns:1fr; }
            .panel { overflow-x:auto; }
          }
        </style>
      </head>
      <body>
        ${renderTopNav("leads")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Business Lead CRM</div>
              <h1>${escapeHtml(business)} Leads</h1>
              <div class="subtitle">All leads, B2B/B2C split, manual onboarding, search, and voice inbox.</div>
            </div>
            <div style="display:flex; gap:10px; flex-wrap:wrap;">
              <a class="btn" href="/leads">← Leads Overview</a>
              <button class="btn btn-primary" type="button" onclick="openLeadCreateModal()">+ Add Lead</button>
            </div>
          </div>

          <div class="stats">
            <div class="stat-card"><div class="stat-label">All Leads</div><div class="stat-value">${counts.all || 0}</div></div>
            <div class="stat-card"><div class="stat-label">B2B</div><div class="stat-value">${counts.b2b || 0}</div></div>
            <div class="stat-card"><div class="stat-label">B2C</div><div class="stat-value">${counts.b2c || 0}</div></div>
            <div class="stat-card"><div class="stat-label">Voice Inbox</div><div class="stat-value">${counts.voice_inbox || 0}</div></div>
          </div>

          <div class="tabs">
            ${tabLink("all", "All Leads", counts.all)}
            ${tabLink("b2b", "B2B", counts.b2b)}
            ${tabLink("b2c", "B2C", counts.b2c)}
            ${tabLink("in_progress", "In Progress", counts.in_progress)}
            ${tabLink("completed", "Completed", counts.completed)}
            ${tabLink("voice_inbox", "Voice Inbox", counts.voice_inbox)}
          </div>

          ${
            selectedTab !== "voice_inbox"
              ? `
                <div class="panel">
                  <form method="GET" action="/leads/${encodeURIComponent(business)}">
                    <input type="hidden" name="tab" value="${escapeHtml(selectedTab)}" />

                    ${
                      business === "rasset"
                        ? `
                          <div class="advanced-filter-grid">
                            <input
                              name="search"
                              value="${escapeHtml(search)}"
                              placeholder="Search company, phone, city, CNC, laser, owner, notes..."
                            />

                            <input
                              name="industry"
                              value="${escapeHtml(filters.industry || "")}"
                              placeholder="Industry e.g. Auto Components"
                            />

                            <input
                              name="capability"
                              value="${escapeHtml(filters.capability || "")}"
                              placeholder="Capability e.g. CNC Machining"
                            />

                            <select name="entity_type">
                              <option value="">All Entity Types</option>
                              ${[
                                "Factory",
                                "Service Provider",
                                "Trading Company",
                                "Supplier",
                                "Training Institute",
                              ]
                                .map(
                                  (x) =>
                                    `<option value="${escapeHtml(x)}" ${filters.entity_type === x ? "selected" : ""}>${escapeHtml(x)}</option>`,
                                )
                                .join("")}
                            </select>

                            <select name="status">
                              <option value="">All Status</option>
                              ${[
                                "new",
                                "working",
                                "busy",
                                "unreachable",
                                "invalid",
                                "unsure",
                                "in_progress",
                                "completed",
                              ]
                                .map(
                                  (x) =>
                                    `<option value="${escapeHtml(x)}" ${filters.status === x ? "selected" : ""}>${escapeHtml(x)}</option>`,
                                )
                                .join("")}
                            </select>

                            <input
                              name="city"
                              value="${escapeHtml(filters.city || "")}"
                              placeholder="City"
                            />

                            <input
                              name="state"
                              value="${escapeHtml(filters.state || "")}"
                              placeholder="State"
                            />

                            <input
                              name="assigned_to"
                              value="${escapeHtml(filters.assigned_to || "")}"
                              placeholder="Assigned to"
                            />

                            <select name="qualified">
                              <option value="">Qualified?</option>
                              <option value="yes" ${filters.qualified === "yes" ? "selected" : ""}>Qualified</option>
                              <option value="no" ${filters.qualified === "no" ? "selected" : ""}>Not Qualified</option>
                            </select>

                            <select name="worth_talking">
                              <option value="">Worth Talking?</option>
                              <option value="yes" ${filters.worth_talking === "yes" ? "selected" : ""}>Worth Talking</option>
                              <option value="no" ${filters.worth_talking === "no" ? "selected" : ""}>Not Worth Talking</option>
                            </select>
                          </div>
                        `
                        : `
                          <div class="search-row">
                            <input
                              name="search"
                              value="${escapeHtml(search)}"
                              placeholder="Search phone, business, contact, city, notes..."
                            />
                            <button class="btn btn-primary" type="submit">Search</button>
                            <a class="btn" href="/leads/${encodeURIComponent(business)}?tab=${escapeHtml(selectedTab)}">Clear</a>
                          </div>
                        `
                    }

                    ${
                      business === "rasset"
                        ? `
                          <div style="display:flex; gap:10px; margin-top:12px; flex-wrap:wrap;">
                            <button class="btn btn-primary" type="submit">Search / Filter</button>
                            <a class="btn" href="/leads/${encodeURIComponent(business)}?tab=${escapeHtml(selectedTab)}">Clear</a>
                          </div>
                        `
                        : ""
                    }
                  </form>
                </div>

${
  business === "rasset"
    ? `
  <div style="margin-bottom:12px;">
    
<button class="btn btn-primary" onclick="toggleUploadBox('rassetUploadBox')" 
  title="Upload Excel with Company, Email, Phone, Industry, Location, etc.">
  ＋ Import Rasset Excel
</button>

<a class="btn" href="/leads/rasset/imports">Import Logs</a>

    <div id="rassetUploadBox" style="display:none; margin-top:10px;">
<div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
  <input id="rassetExcelFile" type="file" accept=".xlsx,.xls,.csv" />
  <button class="btn btn-primary" type="button" onclick="uploadRassetExcel()">Upload</button>
</div>
      <div class="muted" style="margin-top:6px; font-size:12px;">
        Supports: Company, Website, Email, Industry, City, Phone, Owner, Employees, Size, Country
      </div>
    </div>

  </div>
`
    : ""
}



${
  business === "joolian"
    ? `
  <div style="margin-bottom:12px;">
    
    <button class="btn btn-primary" onclick="toggleUploadBox('joolianUploadBox')" 
      title="Upload Excel with AP details, category, pricing, etc.">
      ＋ Import Joolian Excel
    </button>

    <div id="joolianUploadBox" style="display:none; margin-top:10px;">
<div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
  <input id="joolianB2BExcelFile" type="file" accept=".xlsx,.xls,.csv" />
  <button class="btn btn-primary" type="button" onclick="uploadJoolianB2BExcel()">Upload</button>
</div>

      <div class="muted" style="margin-top:6px; font-size:12px;">
        Supports: AP Name, Phone, Email, City, Category, Pricing, Owner, etc.
      </div>
    </div>

  </div>
`
    : ""
}

                <div class="panel">
                  <table>
                    <thead>
                      <tr>
<th>Company / Contact</th>
<th>Category / Capability</th>
<th>Industry / Entity</th>
<th>Location</th>
<th>Lead Quality</th>
<th>Call Summary</th>
<th>Actions</th>
                      </tr>
                    </thead>
                    <tbody>${leadRowsHtml}</tbody>
                  </table>

                  <div class="pagination">
${
  pagination.hasPrev
    ? `<a class="btn" href="/leads/${encodeURIComponent(business)}?${filterQuery}&page=${Number(pagination.page) - 1}">← Previous</a>`
    : ""
}
<span class="btn">Page ${escapeHtml(pagination.page || 1)}</span>
${
  pagination.hasNext
    ? `<a class="btn" href="/leads/${encodeURIComponent(business)}?${filterQuery}&page=${Number(pagination.page) + 1}">Next →</a>`
    : ""
}
                  </div>
                </div>
              `
              : `<div class="lead-list">${voiceInboxHtml}</div>`
          }
        </div>
        
        <div id="callSummaryModal" class="modal" onclick="closeCallSummaryModal(event)">
  <div class="modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="callSummaryTitle" style="font-size:22px; font-weight:900;">Call Summaries</div>
      <button class="btn" type="button" onclick="closeCallSummaryModal()">Close</button>
    </div>

    <div id="callSummaryBody" class="muted">Loading...</div>
  </div>
</div>

        <div id="leadModal" class="modal" onclick="closeLeadModal(event)">
          <div class="modal-card" onclick="event.stopPropagation()">
            <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
              <div id="leadModalTitle" style="font-size:22px; font-weight:900;">Add Lead</div>
              <button class="btn" type="button" onclick="closeLeadModal()">Close</button>
            </div>

            <input id="leadId" type="hidden" />

            <div class="panel">
              <h2 style="margin-top:0;">Quick Enrichment</h2>
              <div class="search-row">
                <input id="enrichUrl" placeholder="Paste website, Google Maps link, or Yelp link" />
                <button class="btn btn-primary" type="button" onclick="enrichLeadUrl()">Fetch Info</button>
                <button class="btn" type="button" onclick="clearLeadForm()">Clear</button>
              </div>
              <div id="enrichMessage" class="muted" style="margin-top:10px;"></div>
            </div>

            <div class="form-grid">
            <div class="form-field" style="grid-column:1 / -1;">
  <label>Smart Add</label>
  <textarea
    id="leadSmartPaste"
    placeholder="Paste anything: phone, company, city, website, Google Maps link, WhatsApp text, CNC/laser/capability notes..."
    oninput="smartParseLeadInput()"
    style="min-height:90px;"
  ></textarea>
  <div class="hint">
    Example: Sharma CNC Rajkot +919876543210 does CNC turning and laser cutting
  </div>
</div>

<div id="leadDuplicateMessage" style="grid-column:1 / -1; display:none;"></div>
              <div class="form-field">
                <label>Lead Type</label>
                <select id="leadCategory">
                  <option value="b2b">B2B</option>
                  <option value="b2c">B2C</option>
                </select>
              </div>

              <div class="form-field">
                <label>Status</label>
                <select id="leadStatus">
                  <option value="new">New</option>
                  <option value="in_progress">In Progress</option>
                  <option value="completed">Completed</option>
                </select>
              </div>

              <div class="form-field">
                <label>Phone</label>
                <input id="leadPhone" oninput="checkLeadPhoneDuplicate()" placeholder="+91..." />
              </div>

              <div class="form-field">
                <label>Lead Source</label>
                <select id="leadSource">
                  <option value="manual">Manual</option>
                  <option value="voice">Voice</option>
                  <option value="website">Website</option>
                  <option value="google_map">Google Map</option>
                  <option value="yelp">Yelp</option>
                </select>
              </div>
              
<div class="form-field" style="grid-column:1 / -1;">
  <div style="
    display:grid;
    grid-template-columns: 1fr;
    gap:12px;
    padding:12px;
    border:1px solid rgba(255,255,255,0.10);
    border-radius:12px;
    background:rgba(255,255,255,0.03);
  ">
    <label style="display:flex; align-items:center; gap:8px; cursor:pointer; font-weight:800;">
      <input id="leadL2Done" type="checkbox" style="margin:0; width:auto;" />
      <span>L2 Done</span>
    </label>
  </div>
</div>
              <div class="form-field">

<label>Lead Stage</label>
<select id="leadStage">
  <option value="">Select stage</option>
  <option value="new">New</option>
  <option value="prospect">Prospect</option>
  <option value="qualified">Qualified</option>
  <option value="not_fit">Not Fit</option>
  <option value="customer">Customer</option>

</select>
</div>

<div class="form-field">
  <label>Business / Organization Name</label>
  <input id="leadBusinessName" />
</div>

<div class="form-field">
  <label>Contact Name</label>
  <input id="leadContactName" />
</div>

<div class="form-field">
  <label>Website</label>
  <input id="leadWebsite" />
</div>

<div class="form-field">
  <label>City</label>
  <input id="leadCity" />
</div>

<div class="form-field">
  <label>State</label>
  <input id="leadState" />
</div>

<div class="form-field">
  <label>Industry</label>
  <input id="leadIndustry" />
</div>

<div class="form-field" style="grid-column:1 / -1;">
  <label>Notes</label>
  <textarea id="leadNotes"></textarea>
</div>

<div class="form-field" style="grid-column:1 / -1;">
  <button

    class="btn"
    type="button"
    onclick="toggleLeadAdvancedFields()"
    style="width:100%; justify-content:center;"
  >
    Show / Hide Advanced Fields
  </button>
</div>

<div id="leadAdvancedFields" style="grid-column:1 / -1; display:none;">
  <div class="form-grid">

<div class="form-field">
  <label>Pin Code</label>
  <input id="leadPinCode" />
</div>

<div class="form-field">
  <label>Location</label>
  <input id="leadLocation" />
</div>

<div class="form-field">
  <label>Country</label>
  <input id="leadCountry" />
</div>

<div class="form-field">
  <label>Year of Establishment</label>
  <input id="leadYearOfEstablishment" />
</div>

<div class="form-field">
  <label>Owner</label>
  <input id="leadOwnerName" />
</div>

<div class="form-field">
  <label>No. of Employees</label>
  <input id="leadNumberOfEmployees" />
</div>

<div class="form-field">
  <label>Company Size</label>
  <input id="leadCompanySize" />
</div>

<div class="form-field" style="grid-column:1 / -1;">
  <label>Enrichment Notes</label>
  <textarea id="leadEnrichmentNotes"></textarea>
</div>

              <div class="form-field">
                <label>Email</label>
                <input id="leadEmail" />
              </div>

              <div class="form-field">
                <label>Google Maps URL</label>
                <input id="leadGoogleMapsUrl" />
              </div>

              <div class="form-field">
                <label>Yelp URL</label>
                <input id="leadYelpUrl" />
              </div>

              <div class="form-field">
                <label>Address</label>
                <input id="leadAddress" />
              </div>

              <div class="form-field" style="grid-column:1 / -1;">
                <label>Latest Transcript / Summary</label>
                <textarea id="leadLatestTranscript"></textarea>
              </div>
            </div>
              </div>
</div>

            <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
              <button class="btn" type="button" onclick="closeLeadModal()">Cancel</button>
              <button class="btn btn-primary" type="button" onclick="saveBusinessLead()">Save Lead</button>
            </div>
          </div>
        </div>

        <script>

function toggleUploadBox(id) {
  const el = document.getElementById(id);
  if (!el) {
    console.error("Upload box not found:", id);
    return;
  }

  if (el.style.display === "none" || el.style.display === "") {
    el.style.display = "block";
  } else {
    el.style.display = "none";
  }
}

          const BUSINESS = ${JSON.stringify(business)};
let leadDuplicateFound = false;
let leadDuplicateTimer = null;

function normalizePhoneClient(value) {
  return String(value || "").replace(/\D/g, "");
}

function extractPhoneFromText(text) {
  const match = String(text || "").match(/(?:\\+?\\d[\\d\\s().-]{8,}\\d)/);
  return match ? match[0].trim() : "";
}

function extractUrlFromText(text) {
  const match = String(text || "").match(/https?:\\/\\/[^\\s]+/i);
  return match ? match[0].trim() : "";
}

function smartParseLeadInput() {
  const text = document.getElementById("leadSmartPaste")?.value || "";

  const phone = extractPhoneFromText(text);
  const phoneInput = document.getElementById("leadPhone");

  if (phone && phoneInput && !phoneInput.value.trim()) {
    phoneInput.value = phone;
    checkLeadPhoneDuplicate();
  }

  const url = extractUrlFromText(text);
  if (url) {
    const enrichInput = document.getElementById("enrichUrl");
    if (enrichInput) enrichInput.value = url;

    const mapsInput = document.getElementById("leadGoogleMapsUrl");
    const websiteInput = document.getElementById("leadWebsite");

    if ((url.includes("google.") || url.includes("maps")) && mapsInput) {
      mapsInput.value = url;
    } else if (websiteInput) {
      websiteInput.value = url;
    }
  }

  const lower = text.toLowerCase();
  const capabilities = [];

  if (lower.includes("cnc")) capabilities.push("CNC Machining");
  if (lower.includes("laser")) capabilities.push("Laser Cutting");
  if (lower.includes("injection")) capabilities.push("Injection Molding");
  if (lower.includes("fabrication")) capabilities.push("Fabrication");
  if (lower.includes("casting")) capabilities.push("Casting");
  if (lower.includes("mould") || lower.includes("mold")) capabilities.push("Tool & Die Making");

  const capBox = document.getElementById("leadManufacturingCapabilities");
  if (capabilities.length && capBox && !capBox.value.trim()) {
    capBox.value = capabilities.join(", ");
  }

  const notesBox = document.getElementById("leadNotes");
  if (notesBox && !notesBox.value.trim()) {
    notesBox.value = text;
  }
}

async function checkLeadPhoneDuplicate() {
  clearTimeout(leadDuplicateTimer);

  leadDuplicateTimer = setTimeout(async function () {
    const phoneInput = document.getElementById("leadPhone");
    const phone = phoneInput ? phoneInput.value.trim() : "";
    const box = document.getElementById("leadDuplicateMessage");

    leadDuplicateFound = false;

    if (!box) return;

    if (!phone || normalizePhoneClient(phone).length < 8) {
      box.style.display = "none";
      box.innerHTML = "";
      return;
    }

    box.style.display = "block";
    box.innerHTML =
      '<div style="padding:10px;border-radius:12px;background:rgba(255,255,255,0.06);">Checking duplicate...</div>';

    const res = await fetch(
      "/api/business-leads/" + BUSINESS + "/check-phone?phone=" + encodeURIComponent(phone)
    );

    const json = await res.json();

    if (!json.ok) {
      box.innerHTML =
        '<div style="padding:10px;border-radius:12px;background:rgba(239,107,115,0.14);">Could not check duplicate.</div>';
      return;
    }

    if (json.data && json.data.duplicate) {
      leadDuplicateFound = true;
      const lead = json.data.lead || {};

      box.innerHTML =
        '<div style="padding:12px;border-radius:12px;background:rgba(239,107,115,0.16);border:1px solid rgba(239,107,115,0.35);">' +
          '<strong>Duplicate found by phone number.</strong><br/>' +
          'Lead #' + escapeHtmlClient(lead.id) + ' — ' +
          escapeHtmlClient(lead.company || lead.business_name || lead.contact_name || "Existing lead") +
          (lead.city ? " · " + escapeHtmlClient(lead.city) : "") +
          (lead.status ? " · " + escapeHtmlClient(lead.status) : "") +
        '</div>';
    } else {
      box.innerHTML =
        '<div style="padding:10px;border-radius:12px;background:rgba(88,201,138,0.14);border:1px solid rgba(88,201,138,0.28);">' +
          'No duplicate found. Safe to add.' +
        '</div>';
    }
  }, 350);
}
          function escapeHtmlClient(value) {
            return String(value ?? "")
              .replace(/&/g, "&amp;")
              .replace(/</g, "&lt;")
              .replace(/>/g, "&gt;")
              .replace(/"/g, "&quot;");
          }

function toggleLeadAdvancedFields() {
  const box = document.getElementById("leadAdvancedFields");
  if (!box) return;

  box.style.display = box.style.display === "none" ? "block" : "none";
}

          function openLeadCreateModal() {
            clearLeadForm();
            document.getElementById("leadModalTitle").textContent = "Add Lead";
            document.getElementById("leadModal").classList.add("open");
          }

          async function openLeadEditModal(id) {
            clearLeadForm();
            document.getElementById("leadModalTitle").textContent = "Edit Lead #" + id;
            document.getElementById("leadModal").classList.add("open");

            const res = await fetch("/api/business-leads/" + BUSINESS + "/" + id);
            const json = await res.json();

            if (!json.ok) {
              alert(json.error || "Failed to load lead");
              return;
            }

            const lead = json.data || {};

            document.getElementById("leadId").value = lead.id || "";
            document.getElementById("leadCategory").value = lead.lead_category || "b2b";
            document.getElementById("leadStatus").value = lead.status || "new";
            document.getElementById("leadPhone").value = lead.phone || "";
            document.getElementById("leadSource").value = lead.lead_source || "manual";
            document.getElementById("leadBusinessName").value = lead.business_name || "";
            document.getElementById("leadContactName").value = lead.contact_name || "";
            document.getElementById("leadEmail").value = lead.email || "";
            document.getElementById("leadWebsite").value = lead.website || "";
            document.getElementById("leadGoogleMapsUrl").value = lead.google_maps_url || "";
            document.getElementById("leadYelpUrl").value = lead.yelp_url || "";
            document.getElementById("leadCity").value = lead.city || "";
            document.getElementById("leadState").value = lead.state || "";
            document.getElementById("leadIndustry").value = lead.industry || "";
            document.getElementById("leadAddress").value = lead.address || "";
            document.getElementById("leadNotes").value = lead.notes || "";
            document.getElementById("leadLatestTranscript").value = lead.latest_transcript || "";
const leadStageEl = document.getElementById("leadStage");
if (leadStageEl) {
  leadStageEl.value = lead.lead_stage || "";
}
document.getElementById("leadPinCode").value = lead.pin_code || "";
document.getElementById("leadLocation").value = lead.location || "";
document.getElementById("leadCountry").value = lead.country || "";
document.getElementById("leadYearOfEstablishment").value = lead.year_of_establishment || "";
document.getElementById("leadOwnerName").value = lead.owner_name || "";
document.getElementById("leadNumberOfEmployees").value = lead.number_of_employees || "";
document.getElementById("leadCompanySize").value = lead.company_size || "";
document.getElementById("leadEnrichmentNotes").value = lead.enrichment_notes || "";

document.getElementById("leadL2Done").checked = !!lead.l2_done;
}

          function closeLeadModal(event) {
            if (event && event.target && event.target.id !== "leadModal") return;
            document.getElementById("leadModal").classList.remove("open");
          }

 function clearLeadForm() {
  [
    "leadId",
    "leadPhone",
    "leadBusinessName",
    "leadContactName",
    "leadEmail",
    "leadWebsite",
    "leadGoogleMapsUrl",
    "leadYelpUrl",
    "leadCity",
    "leadState",
    "leadIndustry",
    "leadAddress",
    "leadNotes",
    "leadLatestTranscript",
    "enrichUrl",
    "enrichMessage",
    "leadSmartPaste",
    "leadStage",
    "leadPinCode",
    "leadLocation",
    "leadCountry",
    "leadYearOfEstablishment",
    "leadOwnerName",
    "leadNumberOfEmployees",
    "leadCompanySize",
    "leadEnrichmentNotes"
  ].forEach(function(id) {
    const el = document.getElementById(id);
    if (!el) return;

    if (id === "enrichMessage") {
      el.textContent = "";
    } else {
      el.value = "";
    }
  });

  leadDuplicateFound = false;

  const duplicateBox = document.getElementById("leadDuplicateMessage");
  if (duplicateBox) {
    duplicateBox.style.display = "none";
    duplicateBox.innerHTML = "";
  }

  const leadCategory = document.getElementById("leadCategory");
  if (leadCategory) leadCategory.value = BUSINESS === "rasset" ? "b2b" : "b2c";

  const leadStatus = document.getElementById("leadStatus");
  if (leadStatus) leadStatus.value = "new";

  const leadSource = document.getElementById("leadSource");
  if (leadSource) leadSource.value = "manual";

const leadStage = document.getElementById("leadStage");
if (leadStage) leadStage.value = "";

  const l2Done = document.getElementById("leadL2Done");
  if (l2Done) l2Done.checked = false;

}
          
          async function toggleLeadCheckbox(event, business, id, field, value) {
  event.stopPropagation();

  const res = await fetch("/api/business-leads/" + business + "/" + id + "/quick-toggle", {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      field,
      value
    })
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to update checkbox");
    event.target.checked = !value;
    return;
  }
}
          

async function deleteBusinessLead(business, id) {
  if (!confirm("Delete this lead and all related voice/call data? This cannot be undone.")) return;

  const res = await fetch("/api/business-leads/" + business + "/" + id, {
    method: "DELETE",
    headers: { "Content-Type": "application/json" }
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to delete lead");
    return;
  }

  window.location.reload();
}


async function uploadRassetExcel() {
  const input = document.getElementById("rassetExcelFile");

  if (!input || !input.files || !input.files[0]) {
    alert("Choose an Excel file first.");
    return;
  }

  const formData = new FormData();
  formData.append("file", input.files[0]);

  const res = await fetch("/api/rasset-leads/import-excel", {
    method: "POST",
    body: formData
  });

  const json = await res.json();

  if (!json.ok) {
    alert(["Excel import failed:", json.error || JSON.stringify(json)].join(String.fromCharCode(10)));
    console.error("Excel import failed:", json);
    return;
  }

  const d = json.data || {};

  alert([
    "Import complete",
    "Import ID: " + d.import_id,
    "Total: " + d.total,
    "Inserted: " + d.inserted,
    "Duplicates skipped: " + d.duplicates,
    "Skipped: " + d.skipped,
    "Errors: " + ((d.errors || []).length)
  ].join(String.fromCharCode(10)));

  window.location.href = "/leads/rasset/imports?import_id=" + d.import_id;
}


async function uploadJoolianB2BExcel() {
  const input = document.getElementById("joolianB2BExcelFile");

  if (!input || !input.files || !input.files[0]) {
    alert("Choose an Excel file first.");
    return;
  }

  const formData = new FormData();
  formData.append("file", input.files[0]);

  const res = await fetch("/api/joolian-leads/import-b2b-excel", {
    method: "POST",
    body: formData
  });

  const json = await res.json();

  if (!json.ok) {
    alert("Joolian B2B Excel import failed: " + (json.error || JSON.stringify(json)));
    console.error("Joolian import failed:", json);
    return;
  }

  const d = json.data || {};
  alert(
    "Joolian B2B import complete. Total: " + d.total +
    ", Inserted: " + d.inserted +
    ", Updated: " + d.updated +
    ", Skipped: " + d.skipped +
    ", Errors: " + (d.errors || []).length
  );

  window.location.reload();
}


          function getLeadPayloadFromForm() {
            return {
              phone: document.getElementById("leadPhone").value.trim(),
              lead_category: document.getElementById("leadCategory").value,
              status: document.getElementById("leadStatus").value,
              lead_source: document.getElementById("leadSource").value,
              business_name: document.getElementById("leadBusinessName").value.trim(),
              contact_name: document.getElementById("leadContactName").value.trim(),
              email: document.getElementById("leadEmail").value.trim(),
              website: document.getElementById("leadWebsite").value.trim(),
              google_maps_url: document.getElementById("leadGoogleMapsUrl").value.trim(),
              yelp_url: document.getElementById("leadYelpUrl").value.trim(),
              city: document.getElementById("leadCity").value.trim(),
              state: document.getElementById("leadState").value.trim(),
              industry: document.getElementById("leadIndustry").value.trim(),
              address: document.getElementById("leadAddress").value.trim(),
              notes: document.getElementById("leadNotes").value.trim(),
company: document.getElementById("leadBusinessName")?.value.trim() || "",
lead_stage: document.getElementById("leadStage")?.value || "",
pin_code: document.getElementById("leadPinCode")?.value.trim() || "",
location: document.getElementById("leadLocation")?.value.trim() || "",
country: document.getElementById("leadCountry")?.value.trim() || "",
year_of_establishment: document.getElementById("leadYearOfEstablishment")?.value.trim() || "",
owner_name: document.getElementById("leadOwnerName")?.value.trim() || "",
number_of_employees: document.getElementById("leadNumberOfEmployees")?.value.trim() || "",
company_size: document.getElementById("leadCompanySize")?.value.trim() || "",
enrichment_notes: document.getElementById("leadEnrichmentNotes")?.value.trim() || "",
qualification_done: document.getElementById("leadQualificationDone")?.checked || false,
worth_talking: document.getElementById("leadWorthTalking")?.checked || false,
l2_done: document.getElementById("leadL2Done")?.checked || false,
              latest_transcript: document.getElementById("leadLatestTranscript").value.trim()
            };
          }

async function saveBusinessLead() {
  const id = document.getElementById("leadId").value;
  const payload = getLeadPayloadFromForm();

  if (!id && leadDuplicateFound) {
    alert("Duplicate lead exists with this phone number. Please use the existing lead instead.");
    return;
  }

            const url = id
              ? "/api/business-leads/" + BUSINESS + "/" + id
              : "/api/business-leads/" + BUSINESS;

            const method = id ? "PUT" : "POST";

            const res = await fetch(url, {
              method,
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload)
            });

            const json = await res.json();

            if (!json.ok) {
              alert(json.error || "Failed to save lead");
              return;
            }

            window.location.reload();
          }


async function enrichLeadUrl() {
  const website = document.getElementById("leadWebsite").value.trim() || document.getElementById("enrichUrl").value.trim();
  const googleMapsUrl = document.getElementById("leadGoogleMapsUrl").value.trim();

  if (!website && !googleMapsUrl) {
    alert("Add website or Google Map link first.");
    return;
  }

  document.getElementById("enrichMessage").textContent = "Trying to fetch company info...";

  const res = await fetch("/api/rasset-leads/enrich", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      website,
      google_maps_url: googleMapsUrl
    })
  });

  const json = await res.json();

  if (!json.ok) {
    document.getElementById("enrichMessage").textContent = json.error || "Could not fetch.";
    return;
  }

  const result = json.data || {};
  const d = result.data || {};

  document.getElementById("enrichMessage").textContent = result.message || "Done.";

  if (d.company) document.getElementById("leadCompany").value = d.company;
  if (d.company) document.getElementById("leadBusinessName").value = d.company;
  if (d.website) document.getElementById("leadWebsite").value = d.website;
  if (d.google_maps_url) document.getElementById("leadGoogleMapsUrl").value = d.google_maps_url;
  if (d.lead_source) document.getElementById("leadSource").value = d.lead_source;
  if (d.email) document.getElementById("leadEmail").value = d.email;
  if (d.industry) document.getElementById("leadIndustry").value = d.industry;
  if (d.pin_code) document.getElementById("leadPinCode").value = d.pin_code;
  if (d.city) document.getElementById("leadCity").value = d.city;
  if (d.location) document.getElementById("leadLocation").value = d.location;
  if (d.phone) document.getElementById("leadPhone").value = d.phone;
  if (d.year_of_establishment) document.getElementById("leadYearOfEstablishment").value = d.year_of_establishment;
  if (d.owner_name) document.getElementById("leadOwnerName").value = d.owner_name;
  if (d.number_of_employees) document.getElementById("leadNumberOfEmployees").value = d.number_of_employees;
  if (d.company_size) document.getElementById("leadCompanySize").value = d.company_size;
  if (d.country) document.getElementById("leadCountry").value = d.country;
  if (d.notes) document.getElementById("leadNotes").value = d.notes;
  if (d.enrichment_notes) document.getElementById("leadEnrichmentNotes").value = d.enrichment_notes;
}

function formatHumanDateTime(value) {
  if (!value) return "-";

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";

  return date.toLocaleString("en-US", {
    timeZone: "America/Los_Angeles",
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  });
}

async function openCallSummaryModal(business, phone) {
  const modal = document.getElementById("callSummaryModal");
  const title = document.getElementById("callSummaryTitle");
  const body = document.getElementById("callSummaryBody");

  if (!modal || !title || !body) {
    alert("Call summary modal is missing");
    return;
  }

  title.textContent = "Call Summaries · " + phone;
  body.innerHTML = "Loading call summaries...";
  modal.classList.add("open");

  const res = await fetch(
    "/api/business-leads/" +
      encodeURIComponent(business) +
      "/call-summaries?phone=" +
      encodeURIComponent(phone)
  );

  const json = await res.json();

  if (!json.ok) {
    body.innerHTML = escapeHtmlClient(json.error || "Failed to load call summaries");
    return;
  }

  const rows = json.data || [];

  if (!rows.length) {
    body.innerHTML = "No call summaries found for this phone number yet.";
    return;
  }

  body.innerHTML = rows
    .map(function(item) {
      const summary =
        item.translated_text ||
        item.cleaned_transcript ||
        item.raw_transcript ||
        "No transcript available yet.";

      const conversationRows = Array.isArray(item.conversation_rows)
        ? item.conversation_rows
        : [];

      const conversationHtml = conversationRows.length
        ? (
            '<table style="width:100%; border-collapse:collapse; margin-top:12px;">' +
              '<thead>' +
                '<tr>' +
                  '<th style="text-align:left; padding:8px;">Speaker</th>' +
                  '<th style="text-align:left; padding:8px;">What was said</th>' +
                '</tr>' +
              '</thead>' +
              '<tbody>' +
                conversationRows.map(function(row) {
                  return (
                    '<tr>' +
                      '<td style="width:150px; font-weight:900; padding:8px; vertical-align:top;">' +
                        escapeHtmlClient(row.speaker || "Unknown") +
                      '</td>' +
                      '<td style="white-space:pre-wrap; padding:8px; vertical-align:top; line-height:1.55;">' +
                        escapeHtmlClient(row.text || "") +
                      '</td>' +
                    '</tr>'
                  );
                }).join("") +
              '</tbody>' +
            '</table>'
          )
        : (
            '<div style="white-space:pre-wrap; margin-top:12px; line-height:1.55;">' +
              escapeHtmlClient(summary) +
            '</div>'
          );

      return (
        '<div class="call-summary-card">' +
        
        (item.spoke_to_name
  ? '<div style="margin-bottom:8px;"><strong>Spoke to:</strong> ' +
      escapeHtmlClient(item.spoke_to_name) +
    '</div>'
  : ''
) +

          '<div style="display:flex; justify-content:space-between; gap:12px; align-items:flex-start;">' +

            '<div>' +
              '<div style="font-weight:900;">Call #' + escapeHtmlClient(item.id) + '</div>' +
              '<div class="muted" style="line-height:1.6; margin-top:4px;">' +
                'Created: ' + escapeHtmlClient(formatHumanDateTime(item.created_at)) + '<br>' +
                'Uploaded by: ' + escapeHtmlClient(item.sender_phone || "-") + '<br>' +
                'Verified by: ' + escapeHtmlClient(item.verified_by || "Not verified") + '<br>' +
                'Verified at: ' + escapeHtmlClient(formatHumanDateTime(item.verified_at)) + '<br>' +
                'Status: ' + escapeHtmlClient(item.status || "-") +
              '</div>' +
            '</div>' +

            '<div style="display:flex; gap:8px; flex-wrap:wrap;">' +

              '<audio controls preload="none" style="max-width:260px; height:36px;">' +
  '<source src="/api/lead-voice-uploads/' + Number(item.id) + '/audio">' +
  'Your browser does not support audio playback.' +
'</audio>' +

              '<button class="btn btn-danger" type="button" data-call-id="' +
                Number(item.id) +
                '" data-business="' +
                escapeHtmlClient(business) +
                '" data-phone="' +
                escapeHtmlClient(phone) +
                '" onclick="handleDeleteCallSummaryClick(this)">Delete</button>' +

            '</div>' +

          '</div>' +

          conversationHtml +

        '</div>'
      );
    })
    .join("");
}

function closeCallSummaryModal(event) {
  if (event && event.target && event.target.id !== "callSummaryModal") return;
  document.getElementById("callSummaryModal").classList.remove("open");
}

async function deleteCallSummary(id, business, phone) {
  if (!confirm("Delete this call summary?")) return;

  const res = await fetch("/api/lead-voice-uploads/" + id, {
    method: "DELETE"
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to delete call summary");
    return;
  }

  await openCallSummaryModal(business, phone);
}

async function handleDeleteCallSummaryClick(button) {
  const id = Number(button.getAttribute("data-call-id"));
  const business = button.getAttribute("data-business") || "";
  const phone = button.getAttribute("data-phone") || "";

  await deleteCallSummary(id, business, phone);
}

async function deleteVoiceTranscript(id) {
  if (!confirm("Delete this transcription only? The audio will remain and you can transcribe again.")) {
    return;
  }

  const res = await fetch("/api/lead-voice-uploads/" + id + "/transcription", {
    method: "DELETE"
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to delete transcription");
    return;
  }

  alert("Transcription deleted.");
  window.location.reload();
}

async function deleteSelectedVoiceUploads() {
  const ids = Array.from(document.querySelectorAll(".voice-delete-checkbox:checked"))
    .map(function(el) {
      return Number(el.value);
    })
    .filter(Boolean);

  if (!ids.length) {
    alert("Select at least one voice message.");
    return;
  }

  if (!confirm("Delete " + ids.length + " selected voice message(s)? This cannot be undone.")) {
    return;
  }

  const res = await fetch("/api/lead-voice-uploads/bulk-delete", {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ids: ids })
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to delete selected voice messages");
    return;
  }

  window.location.reload();
}

async function deleteVoiceUpload(id) {
  if (!confirm("Delete this voice lead completely? This removes audio link, transcript, notes, and review data.")) {
    return;
  }

  const res = await fetch("/api/lead-voice-uploads/" + id, {
    method: "DELETE"
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to delete voice lead");
    return;
  }

  alert("Voice lead deleted.");
  window.location.reload();
}

window.transcribeLead = async function transcribeLead(id) {
  const btn = document.querySelector('[data-transcribe-id="' + id + '"]');

  if (btn) {
    btn.disabled = true;
    btn.textContent = "Transcribing...";
  }

  try {
    const res = await fetch("/api/leads/" + id + "/transcribe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });

    const text = await res.text();
    let json;

    try {
      json = JSON.parse(text);
    } catch {
      alert("Server returned non-JSON response: " + text.slice(0, 800));
      return;
    }

    if (!json.ok) {
      alert("Transcription failed: " + (json.error || JSON.stringify(json)));
      return;
    }

    alert("Transcription completed.");
    window.location.reload();
  } catch (error) {
    alert("Transcription request failed: " + (error?.message || error));
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = "Transcribe";
    }
  }
};

async function saveTranscript(id) {
  const translated = document.getElementById("translated-" + id)?.value || "";
  const notes = document.getElementById("notes-" + id)?.value || "";

  if (!translated.trim()) {
    alert("English translation is required.");
    return;
  }

  const res = await fetch("/api/leads/" + id + "/transcript", {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      cleaned_transcript: translated,
      translated_text: translated,
      review_notes: notes
    })
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to save transcript");
    return;
  }

  alert("Transcript saved.");
  window.location.reload();
}

          async function approveLead(id) {
            if (!confirm("Approve this transcript and create/update business lead?")) return;

            const res = await fetch("/api/leads/" + id + "/approve", {
              method: "POST",
              headers: { "Content-Type": "application/json" }
            });

            const json = await res.json();

            if (!json.ok) {
              alert(json.error || "Failed to approve lead");
              return;
            }

            alert("Lead approved. It will now show under All Leads.");
            window.location.href = "/leads/" + BUSINESS + "?tab=all";
          }

          async function rejectLead(id) {
            const reason = prompt("Reason for rejection?", "");

            const res = await fetch("/api/leads/" + id + "/reject", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ reason: reason || "" })
            });

            const json = await res.json();

            if (!json.ok) {
              alert(json.error || "Failed to reject lead");
              return;
            }

            alert("Lead rejected.");
            window.location.reload();
          }

          async function updateBusinessLeadStatus(business, id, status) {
            const res = await fetch("/api/business-leads/" + business + "/" + id + "/status", {
              method: "PATCH",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ status })
            });

            const json = await res.json();

            if (!json.ok) {
              alert(json.error || "Failed to update status");
              return;
            }

            window.location.reload();
          }

          document.addEventListener("keydown", function(event) {
            if (event.key === "Escape") {
              closeLeadModal();
            }
          });
          
          
          function toggleVoiceActions(event, leadId) {
  event.preventDefault();
  event.stopPropagation();

  document.querySelectorAll(".lead-actions-menu.open").forEach(function(menu) {
    if (menu.id !== "voiceActions-" + leadId) {
      menu.classList.remove("open");
    }
  });

  const menu = document.getElementById("voiceActions-" + leadId);
  if (!menu) return;

  const rect = event.currentTarget.getBoundingClientRect();

  menu.style.top = rect.bottom + 6 + "px";
  menu.style.left = Math.max(12, rect.right - 180) + "px";
  menu.classList.toggle("open");
}

function toggleLeadActions(event, leadId) {
  event.preventDefault();
  event.stopPropagation();

  document.querySelectorAll(".lead-actions-menu.open").forEach(function(menu) {
    if (menu.id !== "leadActions-" + leadId) {
      menu.classList.remove("open");
    }
  });

  const menu = document.getElementById("leadActions-" + leadId);
  if (!menu) {
    console.error("Menu not found for lead:", leadId);
    return;
  }

  const rect = event.currentTarget.getBoundingClientRect();

  menu.style.top = rect.bottom + 6 + "px";
  menu.style.left = Math.max(12, rect.right - 180) + "px";
  menu.classList.toggle("open");
}

document.addEventListener("click", function () {
  document.querySelectorAll(".lead-actions-menu.open").forEach(function(menu) {
    menu.classList.remove("open");
  });
});

        </script>
      </body>
    </html>
  `;
}

function renderReportsSummaryHtml(compliance = {}, reportDate) {
  const safeCompliance = {
    full: compliance?.full || [],
    partial: compliance?.partial || [],
    missing: compliance?.missing || [],
    onLeave: compliance?.onLeave || [],
    off: compliance?.off || [],
  };

  return `
<div class="status-grid">
  <div class="status-chip-box">
    <div class="status-chip-title">Fully updated</div>
    <div class="status-chip-count">${escapeHtml(safeCompliance.full.length)}</div>
    <div class="status-chip-names">${escapeHtml(safeCompliance.full.join(", ") || "None")}</div>
  </div>
  <div class="status-chip-box">
    <div class="status-chip-title">Partially updated</div>
    <div class="status-chip-count">${escapeHtml(safeCompliance.partial.length)}</div>
    <div class="status-chip-names">${escapeHtml(safeCompliance.partial.join(", ") || "None")}</div>
  </div>
  <div class="status-chip-box">
    <div class="status-chip-title">Missing</div>
    <div class="status-chip-count">${escapeHtml(safeCompliance.missing.length)}</div>
    <div class="status-chip-names">${escapeHtml(safeCompliance.missing.join(", ") || "None")}</div>
  </div>
  <div class="status-chip-box">
    <div class="status-chip-title">On leave</div>
    <div class="status-chip-count">${escapeHtml(safeCompliance.onLeave.length)}</div>
    <div class="status-chip-names">${escapeHtml(safeCompliance.onLeave.join(", ") || "None")}</div>
  </div>
  <div class="status-chip-box">
    <div class="status-chip-title">${
      new Date(`${reportDate}T12:00:00`).getDay() === 0
        ? "Sunday Off"
        : "Off / not expected"
    }</div>
    <div class="status-chip-count">${escapeHtml(safeCompliance.off.length)}</div>
    <div class="status-chip-names">${escapeHtml(safeCompliance.off.join(", ") || "None")}</div>
  </div>
</div>
  `;
}

function renderReportCardsHtml(users = [], reportDate) {
  return users.length
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
<div class="report-card ${escapeHtml(user.reportCardClass || "")}" data-user-name="${escapeHtml(String(user.userName || "").toLowerCase())}">
  <div class="report-card-head">
    <div>
      <div class="report-name">
        <a href="/attendance/${escapeHtml(user.userId)}">${escapeHtml(user.userName)}</a>
      </div>
      <div class="report-date">
        ${escapeHtml(formatDateOnly(reportDate))}
        <a href="/reports?userId=${encodeURIComponent(user.userId)}&days=7" class="mini-report-link">
          Last 7 days
        </a>
      </div>
      <div class="micro-meta">${escapeHtml(user.compactMeta || "0 touched")}</div>
      <div class="report-reason">${escapeHtml(user.reportReason || "")}</div>
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
}

function renderReportsPage(data) {
  const reportDate = data?.reportDate || getReportDateString();
  const users = data?.users || [];
  const compliance = data?.compliance || {
    full: [],
    partial: [],
    missing: [],
    onLeave: [],
    off: [],
  };
  return `
    <html>
      <head>
        <title>Reports</title>
        <style>
  ${buildThemeCss()}
  ${buildBasePageCss()}
  ${buildTopNavCss()}

          .wrap { max-width: 1400px; margin: 0 auto; padding: 24px 18px 36px; }
          .topbar, .panel, .report-card, .status-chip-box, .modal-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
          }
          .mini-report-link {
  margin-left: 10px;
  font-size: 12px;
  font-weight: 600;
  color: #7c8cff; /* soft bluish purple */
  text-decoration: none;
  opacity: 0.85;
}

.mini-report-link:hover {
  opacity: 1;
  text-decoration: underline;
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
          
          .report-card-missing {
  border: 1px solid rgba(239, 107, 115, 0.58);
  background: linear-gradient(
    180deg,
    rgba(101, 33, 43, 0.94),
    rgba(69, 25, 34, 0.98)
  );
  box-shadow: 0 0 0 1px rgba(255,255,255,0.03), 0 10px 30px rgba(0,0,0,0.24);
}

.report-card-partial {
  border: 1px solid rgba(243, 181, 98, 0.38);
  background: linear-gradient(
    180deg,
    rgba(84, 64, 34, 0.90),
    rgba(58, 44, 24, 0.96)
  );
}

.report-card-off,
.report-card-leave {
  opacity: 0.88;
}

.report-reason {
  margin-top: 6px;
  font-size: 12px;
  color: #f3f6ff;
  opacity: 0.86;
  line-height: 1.45;
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
      ${renderTopNav("reports")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Daily Reporting</div>
              <h1>WeSolveHR // Reports</h1>
              <div class="subtitle">Attendance-day so far. Task narratives + extra work + open/blocked snapshot.</div>
            </div>
          </div>

          <div class="panel" style="padding:14px 16px; margin-bottom:16px;">
<strong>Date:</strong> ${escapeHtml(formatDateOnly(reportDate))} 
<span class="muted">(6:00 AM → next day 6:00 AM IST)</span>
</div>

<div id="reportsSummary">
  <div class="panel" style="padding:18px; margin-bottom:16px;">
    <div class="muted">Loading summary...</div>
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

          <div id="reportsGrid" class="reports-grid">
            <div class="panel" style="padding:18px;">
              <div class="muted">Loading reports...</div>
            </div>
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

async function loadReportSummary() {
  const mount = document.getElementById("reportsSummary");
  if (!mount) return;

  const params = new URLSearchParams(window.location.search);
  const date = params.get("date") || "";
  const userId = params.get("userId") || "";

  const qs = new URLSearchParams();
  if (date) qs.set("date", date);
  if (userId) qs.set("userId", userId);

  const url = "/api/reports/summary" + (qs.toString() ? "?" + qs.toString() : "");

  try {
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
    });

    const json = await res.json();

    if (!json.ok) {
      mount.innerHTML =
        '<div class="panel" style="padding:18px; margin-bottom:16px;">' +
          '<div class="muted">Failed to load summary.</div>' +
        '</div>';
      return;
    }

    mount.innerHTML =
      json.data.summaryHtml ||
      '<div class="panel" style="padding:18px; margin-bottom:16px;">' +
        '<div class="muted">No summary available.</div>' +
      '</div>';
  } catch (error) {
    mount.innerHTML =
      '<div class="panel" style="padding:18px; margin-bottom:16px;">' +
        '<div class="muted">Failed to load summary.</div>' +
      '</div>';
  }
}

async function loadReportCards() {
  const grid = document.getElementById("reportsGrid");
  if (!grid) return;

  const params = new URLSearchParams(window.location.search);
  const date = params.get("date") || "";
  const userId = params.get("userId") || "";

  const qs = new URLSearchParams();
  if (date) qs.set("date", date);
  if (userId) qs.set("userId", userId);

  const url = "/api/reports/cards" + (qs.toString() ? "?" + qs.toString() : "");

  try {
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
    });

    const json = await res.json();

    if (!json.ok) {
      grid.innerHTML =
        '<div class="panel" style="padding:18px;">' +
          '<div class="muted">Failed to load reports.</div>' +
        '</div>';
      return;
    }

    grid.innerHTML =
      json.data.cardsHtml ||
      '<div class="panel" style="padding:18px;">' +
        '<div class="muted">No users found.</div>' +
      '</div>';

    filterReports();
  } catch (error) {
    grid.innerHTML =
      '<div class="panel" style="padding:18px;">' +
        '<div class="muted">Failed to load reports.</div>' +
      '</div>';
  }
}

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
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};

  if (item.changeType === "task_created") {
    const owners = Array.isArray(newValue.owners) && newValue.owners.length
      ? newValue.owners.join(", ")
      : "-";

    return [
      "Created task",
      "Title: " + (newValue.title || "-"),
      "Owners: " + owners,
      "Priority: " + (newValue.priority || "-"),
      "Deadline: " + (newValue.deadline || "-"),
      "Business / Area: " + (newValue.business || "-") + " / " + (newValue.area || "-")
    ].join("\\n");
  }

if (item.changeType === "status_change") {
  const oldStatus = oldValue.status || "-";
  const newStatus = newValue.status || "-";
  const oldProgress = oldValue.progress ?? "-";
  const newProgress = newValue.progress ?? "-";
  const note = newValue.note ? "\\nNote: " + newValue.note : "";

return (
  "Status: " + oldStatus + " → " + newStatus +
  "\\nProgress: " + oldProgress + "% → " + newProgress + "%" +
  note
);
}

  if (item.changeType === "progress_change") {
    return [
      "Progress: " + (oldValue.progress ?? "-") + "% → " + (newValue.progress ?? "-") + "%",
      "Status: " + (oldValue.status || "-") + " → " + (newValue.status || "-"),
      newValue.note ? "Note: " + newValue.note : null
    ].filter(Boolean).join("\\n");
  }

  if (item.changeType === "owner_change") {
    const oldOwners = Array.isArray(oldValue.owners) ? oldValue.owners.join(", ") : "-";
    const newOwners = Array.isArray(newValue.owners) ? newValue.owners.join(", ") : "-";
    return "Owners: " + oldOwners + " → " + newOwners;
  }

  if (item.changeType === "deadline_change") {
    return "Deadline: " + (oldValue.deadline || "-") + " → " + (newValue.deadline || "-");
  }

  if (item.fieldName === "blocker_note") {
    return [
      "Blocker: " + (newValue.blocker_note || "-"),
      newValue.note ? "Note: " + newValue.note : null
    ].filter(Boolean).join("\\n");
  }

  if (item.fieldName === "title") {
    return "Title: " + (oldValue.title || "-") + " → " + (newValue.title || "-");
  }

  if (item.fieldName === "detail") {
    return "Detail updated";
  }

  if (item.fieldName === "priority") {
    return "Priority: " + (oldValue.priority || "-") + " → " + (newValue.priority || "-");
  }

  if (item.fieldName === "business") {
    return "Business: " + (oldValue.business || "-") + " → " + (newValue.business || "-");
  }

  if (item.fieldName === "area") {
    return "Area: " + (oldValue.area || "-") + " → " + (newValue.area || "-");
  }

  return "Updated";
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
      ? task.history.map(function(item) {
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
    body.innerHTML = '<div class="muted">Failed to load task detail</div>';
  }
}

document.addEventListener("DOMContentLoaded", function () {
  loadReportSummary();
  loadReportCards();
});

        </script>
      </body>
    </html>
  `;
}

function summarizeUserMultiDayReport(dailyReports) {
  let totalWorkingDays = 0;
  let fullDays = 0;
  let partialDays = 0;
  let missingDays = 0;
  let leaveDays = 0;
  let offDays = 0;

  for (const daily of dailyReports || []) {
    const user = (daily.users || [])[0];
    if (!user) continue;

    totalWorkingDays += Number(user.workDayWeight || 0);

    if (user.reportStatus === "full")
      fullDays += Number(user.workDayWeight || 0);
    else if (user.reportStatus === "partial")
      partialDays += Number(user.workDayWeight || 0);
    else if (user.reportStatus === "missing")
      missingDays += Number(user.workDayWeight || 0);
    else if (user.reportStatus === "leave") leaveDays += 1;
    else if (user.reportStatus === "off") offDays += 1;
  }

  return {
    totalWorkingDays,
    fullDays,
    partialDays,
    missingDays,
    leaveDays,
    offDays,
  };
}

function renderMultiDayUserReportsPage(data) {
  const days = data?.days || 7;
  const dailyReports = data?.dailyReports || [];
  const firstUser =
    dailyReports?.[0]?.users?.[0] ||
    dailyReports?.find((d) => (d.users || []).length)?.users?.[0] ||
    null;

  const pageTitle = firstUser
    ? days === 1
      ? `${firstUser.userName} — Today Report`
      : `${firstUser.userName} — Last ${days} Days`
    : days === 1
      ? `Today Report`
      : `Last ${days} Days Report`;

  const weeklySummary = summarizeUserMultiDayReport(dailyReports);

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
          <div class="report-card ${escapeHtml(user.reportCardClass || "")}">
          <div class="report-card-head">
            <div>
              <div class="report-name">${escapeHtml(formatDateOnly(reportDate))}</div>
              <div class="report-date">${escapeHtml(user.userName)}</div>
<div class="micro-meta">${escapeHtml(user.compactMeta || "0 touched")}</div>
<div class="report-reason">${escapeHtml(user.reportReason || "")}</div>
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
  ${buildTopNavCss()}

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
<div class="subtitle">
  ${
    days === 1
      ? "Today’s attendance-day report."
      : `Last ${escapeHtml(days)} attendance-days, one section per day.`
  }
</div>
</div>
          </div>
          
          <div class="panel" style="padding:14px 16px; margin-bottom:16px;">
  <strong>Total working days:</strong> ${escapeHtml(String(weeklySummary.totalWorkingDays))}
  <br />
  <strong>Fully updated:</strong> ${escapeHtml(String(weeklySummary.fullDays))}
  <br />
  <strong>Partially updated:</strong> ${escapeHtml(String(weeklySummary.partialDays))}
  <br />
  <strong>Missing:</strong> ${escapeHtml(String(weeklySummary.missingDays))}
  <br />
  <strong>Leave days:</strong> ${escapeHtml(String(weeklySummary.leaveDays))}
  <br />
  <strong>Off days:</strong> ${escapeHtml(String(weeklySummary.offDays))}
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
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};

  if (item.changeType === "task_created") {
    return "Task created";
  }

  if (item.changeType === "status_change") {
    const oldStatus = oldValue.status || "-";
    const newStatus = newValue.status || "-";
    const oldProgress = oldValue.progress ?? "-";
    const newProgress = newValue.progress ?? "-";
    const note = newValue.note ? "\\nNote: " + newValue.note : "";

    return (
      "Status: " + oldStatus + " → " + newStatus +
      "\\nProgress: " + oldProgress + "% → " + newProgress + "%" +
      note
    );
  }

  if (item.changeType === "progress_change") {
    const oldProgress = oldValue.progress ?? 0;
    const newProgress = newValue.progress ?? 0;
    const note = newValue.note ? "\\nNote: " + newValue.note : "";
    return "Progress: " + oldProgress + "% → " + newProgress + "%" + note;
  }

  if (item.changeType === "owner_change") {
    const oldOwners = Array.isArray(oldValue.owners) ? oldValue.owners.join(", ") : "-";
    const newOwners = Array.isArray(newValue.owners) ? newValue.owners.join(", ") : "-";
    return "Owners: " + oldOwners + " → " + newOwners;
  }

  if (item.changeType === "deadline_change") {
    return "Deadline: " + (oldValue.deadline || "-") + " → " + (newValue.deadline || "-");
  }

  if (item.fieldName === "blocker_note") {
    return [
      "Blocker: " + (newValue.blocker_note || "-"),
      newValue.note ? "Note: " + newValue.note : null
    ].filter(Boolean).join("\\n");
  }

  if (item.fieldName) {
    return (item.fieldName || "Field") + ": " +
      JSON.stringify(oldValue) + " → " + JSON.stringify(newValue);
  }

  return JSON.stringify(newValue || {});
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

function buildEmployeeMonthlyAttendanceSummaryFromData({
  events = [],
  leaveRows = [],
  lateRows = [],
  auditRows = [],
  startDate,
  endDateExclusive,
  redReportDates = [],
  shiftStartIso,
}) {
  const eventsByAttendanceDay = new Map();

  for (const ev of events || []) {
    const attendanceDate = parseIsoToAttendanceDateString(ev.created_at);
    if (!attendanceDate) continue;

    if (!eventsByAttendanceDay.has(attendanceDate)) {
      eventsByAttendanceDay.set(attendanceDate, []);
    }

    eventsByAttendanceDay.get(attendanceDate).push(ev);
  }

  for (const [, dayEvents] of eventsByAttendanceDay) {
    dayEvents.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
  }

  const nowDate = getAttendanceDayDateStringFromDate(new Date());

  let presentDays = 0;
  const leaveDays = leaveRows.length;

  let lateJoins = 0;
  let approvedLate = 0;
  let unapprovedLate = 0;
  let uninformedLate = 0;

  let totalLoginMinutes = 0;
  let loginDays = 0;

  let totalBreakMin = 0;
  let breakDays = 0;

  let longShiftCount = 0;
  let longBreakCount = 0;
  let possibleHalfDays = 0;

  for (
    let date = startDate;
    date < endDateExclusive;
    date = addDaysToDateString(date, 1)
  ) {
    const dayEvents = eventsByAttendanceDay.get(date) || [];
    if (!dayEvents.length) continue;

    const dayShiftStartIso =
      shiftStartIso || `${date}T10:30:00${APP_TIMEZONE_OFFSET}`;

    const summary = getAttendanceSummaryFromEvents(dayEvents, {
      shiftStartIso: dayShiftStartIso,
    });

    if (summary.firstLogin) {
      presentDays += 1;

      const firstLogin = new Date(summary.firstLogin.created_at);

      const loginTimeText = firstLogin.toLocaleTimeString("en-IN", {
        timeZone: APP_TIMEZONE,
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      });

      const [hh, mm] = loginTimeText.split(":").map(Number);

      if (!Number.isNaN(hh) && !Number.isNaN(mm)) {
        totalLoginMinutes += hh * 60 + mm;
        loginDays += 1;
      }
    }

    if (summary.breakMinutes > 0) {
      totalBreakMin += summary.breakMinutes;
      breakDays += 1;
    }

    if (summary.longShiftFlag) longShiftCount += 1;
    if (summary.longBreakFlag) longBreakCount += 1;
    if (summary.possibleHalfDay) possibleHalfDays += 1;
  }

  for (const row of lateRows || []) {
    const lateDate = row.late_date;
    const dayEvents = eventsByAttendanceDay.get(lateDate) || [];
    const firstLogin = getFirstLoginEvent(dayEvents);

    if (firstLogin) {
      const shiftStartForLate =
        row.shift_start_at || `${lateDate}T10:30:00${APP_TIMEZONE_OFFSET}`;

      const lateMinutes = Math.max(
        0,
        Math.round(
          (new Date(firstLogin.created_at) - new Date(shiftStartForLate)) /
            60000,
        ),
      );

      // If they actually logged in on time, ignore stale late_arrivals row.
      if (lateMinutes <= 10) {
        continue;
      }
    }

    lateJoins += 1;

    if (row.is_approved) {
      approvedLate += 1;
    } else {
      unapprovedLate += 1;
    }

    const isTimeUnsure =
      !row.expected_login_at || String(row.note || "").includes("TIME_UNSURE");

    if (isTimeUnsure) {
      uninformedLate += 1;
    }
  }

  const avgLoginTimeText =
    loginDays > 0
      ? (() => {
          const avgMinutes = Math.round(totalLoginMinutes / loginDays);
          const hh = Math.floor(avgMinutes / 60);
          const mm = avgMinutes % 60;

          const d = new Date();
          d.setHours(hh, mm, 0, 0);

          return d.toLocaleTimeString("en-IN", {
            timeZone: APP_TIMEZONE,
            hour: "numeric",
            minute: "2-digit",
            hour12: true,
          });
        })()
      : "-";

  const avgBreakMin = breakDays > 0 ? Math.round(totalBreakMin / breakDays) : 0;

  const pastLeaveDates = leaveRows
    .map((x) => x.off_date)
    .filter((d) => d < nowDate);

  const upcomingLeaveDates = leaveRows
    .map((x) => x.off_date)
    .filter((d) => d >= nowDate);

  const managerCorrectionCount = (auditRows || []).filter((row) => {
    const actionType = String(row.action_type || "");

    return (
      actionType.startsWith("mark_") ||
      actionType.startsWith("fix_") ||
      actionType.startsWith("force_") ||
      actionType.startsWith("remove_") ||
      actionType.startsWith("undo_") ||
      actionType.startsWith("reset_") ||
      actionType.startsWith("lock_") ||
      actionType.startsWith("unlock_")
    );
  }).length;

  let totalWorkingDays = 0;

  for (
    let date = startDate;
    date < endDateExclusive;
    date = addDaysToDateString(date, 1)
  ) {
    const weekday = getWeekdayNameFromDateString(date);

    if (weekday !== "sunday") {
      totalWorkingDays += 1;
    }
  }

  return {
    redReportDays: redReportDates.length,
    redReportDates,

    presentDays,
    leaveDays,
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

    managerCorrectionCount,
    totalWorkingDays,
  };
}

async function getEmployeeMonthlyAttendanceSummary(userId, orgId) {
  const { startDate, endDateExclusive } = getMonthDateRangeForTimeZone(
    new Date(),
    APP_TIMEZONE,
  );

  const attendanceStartUtc = new Date(
    `${startDate}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
  ).toISOString();

  const attendanceEndUtc = new Date(
    `${endDateExclusive}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
  ).toISOString();

  const [eventsResult, leaveResult, lateResult, auditResult, overrideResult] =
    await Promise.all([
      supabase
        .from("attendance_events")
        .select(
          "id, org_id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
        )
        .eq("user_id", userId)
        .eq("org_id", orgId)
        .gte("created_at", attendanceStartUtc)
        .lt("created_at", attendanceEndUtc)
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
        .select("id, action_type, created_at")
        .eq("target_user_id", userId)
        .eq("org_id", orgId)
        .gte("created_at", attendanceStartUtc)
        .lt("created_at", attendanceEndUtc)
        .order("created_at", { ascending: true }),

      supabase
        .from("work_day_expectation_overrides")
        .select("id, override_date, mode")
        .eq("user_id", userId)
        .eq("org_id", orgId)
        .gte("override_date", startDate)
        .lt("override_date", endDateExclusive)
        .order("override_date", { ascending: true }),
    ]);

  if (eventsResult.error) throw eventsResult.error;
  if (leaveResult.error) throw leaveResult.error;
  if (lateResult.error) throw lateResult.error;
  if (auditResult.error) throw auditResult.error;
  if (overrideResult.error) throw overrideResult.error;

  const events = eventsResult.data || [];
  const leaveRows = leaveResult.data || [];
  const lateRows = lateResult.data || [];
  const auditRows = auditResult.data || [];
  const overrideRows = overrideResult.data || [];

  const leaveDateSet = new Set((leaveRows || []).map((x) => x.off_date));
  const overrideByDate = new Map(
    (overrideRows || []).map((x) => [x.override_date, x.mode]),
  );

  const eventsByAttendanceDate = new Map();
  for (const ev of events) {
    const attendanceDate = getAttendanceDayDateStringFromDate(
      new Date(ev.created_at),
    );
    if (!eventsByAttendanceDate.has(attendanceDate)) {
      eventsByAttendanceDate.set(attendanceDate, []);
    }
    eventsByAttendanceDate.get(attendanceDate).push(ev);
  }

  const lateByDate = new Map();
  for (const row of lateRows) {
    lateByDate.set(row.late_date, row);
  }

  let presentDays = 0;
  let leaveDays = leaveRows.length;
  let lateJoins = 0;
  let approvedLate = 0;
  let unapprovedLate = 0;
  let uninformedLate = 0;
  let totalLoginMinutes = 0;
  let loginDaysCount = 0;
  let totalBreakMin = 0;
  let longShiftCount = 0;
  let longBreakCount = 0;
  let possibleHalfDays = 0;
  let totalWorkingDays = 0;

  let cursorDate = startDate;

  while (cursorDate < endDateExclusive) {
    const dayEvents = eventsByAttendanceDate.get(cursorDate) || [];
    const leaveRowExists = leaveDateSet.has(cursorDate);
    const overrideMode = overrideByDate.get(cursorDate) || null;

    const expectation = resolveWorkExpectation({
      reportDate: cursorDate,
      isOnLeave: leaveRowExists,
      overrideMode,
    });

    totalWorkingDays += Number(expectation.workDayWeight || 0);

    if (dayEvents.length > 0) {
      presentDays += 1;
    }

    const summary = getAttendanceSummaryFromEvents(dayEvents);

    if (summary.firstLogin) {
      const firstLoginDate = new Date(summary.firstLogin.created_at);
      const shiftStartIso = parseLocalDateTimeForToday(
        DEFAULT_SHIFT_START_TEXT,
      );

      if (shiftStartIso) {
        const shiftStartParts = getPartsInTimeZone(
          new Date(summary.firstLogin.created_at),
          APP_TIMEZONE,
        );

        const dayShiftStartIso = new Date(
          `${cursorDate}T10:30:00${APP_TIMEZONE_OFFSET}`,
        ).toISOString();

        const lateMinutes = Math.max(
          0,
          Math.round(
            (firstLoginDate.getTime() - new Date(dayShiftStartIso).getTime()) /
              60000,
          ),
        );

        if (lateMinutes > 10) {
          lateJoins += 1;

          const lateRow = lateByDate.get(cursorDate);
          if (lateRow) {
            if (lateRow.is_approved) approvedLate += 1;
            else unapprovedLate += 1;
          } else {
            uninformedLate += 1;
          }
        }
      }

      const parts = getPartsInTimeZone(firstLoginDate, APP_TIMEZONE);
      totalLoginMinutes += parts.hour * 60 + parts.minute;
      loginDaysCount += 1;
    }

    totalBreakMin += Number(summary.breakMinutes || 0);

    if (summary.longShiftFlag) {
      longShiftCount += 1;
    }

    if (summary.longBreakFlag) {
      longBreakCount += 1;
    }

    if (summary.possibleHalfDay) {
      possibleHalfDays += 1;
    }

    cursorDate = addDaysToDateString(cursorDate, 1);
  }

  const avgLoginMinutes = loginDaysCount
    ? Math.round(totalLoginMinutes / loginDaysCount)
    : null;

  const avgLoginTimeText =
    avgLoginMinutes == null
      ? "-"
      : (() => {
          const hours24 = Math.floor(avgLoginMinutes / 60);
          const mins = avgLoginMinutes % 60;
          const suffix = hours24 >= 12 ? "PM" : "AM";
          const hours12 = hours24 % 12 === 0 ? 12 : hours24 % 12;
          return `${hours12}:${String(mins).padStart(2, "0")} ${suffix}`;
        })();

  const avgBreakMin = presentDays ? Math.round(totalBreakMin / presentDays) : 0;

  const nowDate = getTodayDateStringInTimeZone(APP_TIMEZONE);

  const pastLeaveDates = leaveRows
    .map((x) => x.off_date)
    .filter((d) => d < nowDate);

  const upcomingLeaveDates = leaveRows
    .map((x) => x.off_date)
    .filter((d) => d >= nowDate);

  const managerCorrectionCount = (auditRows || []).filter(
    (row) =>
      String(row.action_type || "").startsWith("mark_") ||
      String(row.action_type || "").startsWith("fix_") ||
      String(row.action_type || "").startsWith("force_") ||
      String(row.action_type || "").startsWith("remove_") ||
      String(row.action_type || "").startsWith("undo_") ||
      String(row.action_type || "").startsWith("reset_") ||
      String(row.action_type || "").startsWith("lock_") ||
      String(row.action_type || "").startsWith("unlock_"),
  ).length;

  const redReportDates = await getMissingReportDatesForUserInRange({
    orgId,
    userId,
    startDate,
    endDateExclusive,
  });

  const redReportDays = redReportDates.length;

  return {
    redReportDays,
    redReportDates,
    presentDays,
    leaveDays,
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
    managerCorrectionCount,
    totalWorkingDays,
  };
}

async function getEmployeeAttendanceOverview(userId, orgId, options = {}) {
  const todayAttendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const { startUtc, endUtc } = getCurrentAttendanceDayRange();
  const monthNav =
    options.monthNav || getAttendanceMonthNavigation(options.month);

  const { startDate, endDateExclusive } = monthNav;

  const [
    userResult,
    todayEventsResult,
    monthlyEventsResult,
    leaveResult,
    lateResult,
    auditResult,
    overrideResult,
    redReportDates,
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

    supabase
      .from("work_day_expectation_overrides")
      .select("id, override_date, mode")
      .eq("user_id", userId)
      .eq("org_id", orgId)
      .gte("override_date", startDate)
      .lt("override_date", endDateExclusive)
      .order("override_date", { ascending: true }),

    Promise.resolve([]),
  ]);

  if (userResult.error) throw userResult.error;
  if (todayEventsResult.error) throw todayEventsResult.error;
  if (monthlyEventsResult.error) throw monthlyEventsResult.error;
  if (leaveResult.error) throw leaveResult.error;
  if (lateResult.error) throw lateResult.error;
  if (auditResult.error) throw auditResult.error;
  if (overrideResult.error) throw overrideResult.error;

  const user = userResult.data;
  if (!user) {
    throw new Error("Employee not found");
  }

  const todayEvents = todayEventsResult.data || [];
  const monthlyEvents = monthlyEventsResult.data || [];
  const leaveRows = leaveResult.data || [];
  const lateRows = lateResult.data || [];
  const auditRows = auditResult.data || [];
  const overrideRows = overrideResult.data || [];
  const lateByDate = new Map((lateRows || []).map((x) => [x.late_date, x]));
  const leaveByDate = new Map((leaveRows || []).map((x) => [x.off_date, x]));
  const auditCountByDate = new Map();

  for (const row of auditRows || []) {
    const auditDate = parseIsoToAttendanceDateString(row.created_at);
    if (!auditDate) continue;
    auditCountByDate.set(auditDate, (auditCountByDate.get(auditDate) || 0) + 1);
  }

  const overrideByDate = new Map(
    (overrideRows || []).map((x) => [x.override_date, x.mode]),
  );

  const shiftStartIso = await getShiftStartIsoForUserToday(user.id, orgId);
  const todaySummary = getAttendanceSummaryFromEvents(todayEvents, {
    shiftStartIso,
  });
  const leaveToday = leaveByDate.get(todayAttendanceDate) || null;
  const lateToday = lateByDate.get(todayAttendanceDate) || null;

  const todayOverrideMode = overrideByDate.get(todayAttendanceDate) || null;

  const todayExpectation = resolveWorkExpectation({
    reportDate: todayAttendanceDate,
    isOnLeave: !!leaveToday,
    overrideMode: todayOverrideMode,
  });

  const effectiveTodayStatus = !todayExpectation.expectedToWork
    ? leaveToday
      ? "leave"
      : "off"
    : todaySummary.currentStatus;

  const effectiveLeaveToday = !todayExpectation.expectedToWork && !!leaveToday;

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
    ...overrideRows.map((x) => x.override_date),
  ]);

  const sortedAttendanceDates = Array.from(allAttendanceDates).sort((a, b) =>
    a < b ? 1 : -1,
  );

  const monthlySummary = buildEmployeeMonthlyAttendanceSummaryFromData({
    events: monthlyEvents,
    leaveRows,
    lateRows,
    auditRows,
    startDate,
    endDateExclusive,
    redReportDates: redReportDates || [],
    shiftStartIso,
  });

  for (const attendanceDate of sortedAttendanceDates) {
    const dayEvents = eventsByAttendanceDay.get(attendanceDate) || [];
    const daySummary = getAttendanceSummaryFromEvents(dayEvents, {
      shiftStartIso,
    });
    const dayLate = lateByDate.get(attendanceDate) || null;
    const dayLeave = leaveByDate.get(attendanceDate) || null;

    const overrideMode = overrideByDate.get(attendanceDate) || null;

    const expectation = resolveWorkExpectation({
      reportDate: attendanceDate,
      isOnLeave: !!dayLeave,
      overrideMode,
    });

    const effectiveStatus = !expectation.expectedToWork
      ? dayLeave
        ? "leave"
        : "off"
      : daySummary.currentStatus;

    const effectiveLeaveText =
      !expectation.expectedToWork && dayLeave ? "Yes" : "No";

    const dayAuditCount = auditCountByDate.get(attendanceDate) || 0;

    history.push({
      attendance_date: attendanceDate,
      status: effectiveStatus,
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
      leave_text: effectiveLeaveText,
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
      current_status: effectiveTodayStatus,
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
      leave_today: effectiveLeaveToday,
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
  const monthNav = data?.monthNav || getAttendanceMonthNavigation();
  const selectedMonthLabel = monthNav.selectedMonth;
  const monthQuery = `month=${encodeURIComponent(monthNav.selectedMonth)}`;
  const history = data?.history || [];
  const recentAudit = data?.recent_audit || [];
  const selectedDays = Number(data?.selectedDays) === 7 ? 7 : 1;

  const todayFlags = [
    today.long_shift_flag ? "Long shift" : null,
    today.long_break_flag ? "Long break" : null,
    today.possible_half_day ? "Half day" : null,
  ].filter(Boolean);

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

  const leaveHistoryRows = history.length
    ? history
        .filter(
          (row) =>
            row.leave_text && row.leave_text !== "No" && row.leave_text !== "-",
        )
        .map(
          (row) => `
            <tr>
              <td>${escapeHtml(row.attendance_date)}</td>
              <td>${escapeHtml(row.leave_text)}</td>
              <td>${escapeHtml(row.status)}</td>
              <td>${escapeHtml(row.flags || "-")}</td>
            </tr>
          `,
        )
        .join("") ||
      `
          <tr>
            <td colspan="4" class="empty-cell">No leave entries found in this month view.</td>
          </tr>
        `
    : `
      <tr>
        <td colspan="4" class="empty-cell">No leave entries found in this month view.</td>
      </tr>
    `;

  const behaviorRows = history.length
    ? history
        .filter((row) => {
          const flags = String(row.flags || "").trim();
          return (
            (flags && flags !== "-") ||
            String(row.late_text || "") !== "No" ||
            Number(row.corrections || 0) > 0
          );
        })
        .map(
          (row) => `
            <tr>
              <td>${escapeHtml(row.attendance_date)}</td>
              <td>${escapeHtml(row.late_text || "No")}</td>
              <td>${escapeHtml(row.late_approved || "-")}</td>
              <td>${escapeHtml(row.flags || "-")}</td>
              <td>${escapeHtml(String(row.corrections || 0))}</td>
            </tr>
          `,
        )
        .join("") ||
      `
          <tr>
            <td colspan="5" class="empty-cell">No behavior flags found.</td>
          </tr>
        `
    : `
      <tr>
        <td colspan="5" class="empty-cell">No behavior flags found.</td>
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
  ${buildThemeCss()}
  ${buildBasePageCss()}
  ${buildTopNavCss()}

          .wrap {
            max-width: 1380px;
            margin: 0 auto;
            padding: 24px 18px 36px;
          }

          .topbar, .panel, .card, .tab-btn {
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

          .links a,
          .mini-report-link {
            color: var(--text);
            text-decoration: none;
            padding: 10px 14px;
            border-radius: 12px;
            border: 1px solid color-mix(in srgb, var(--secondary) 30%, transparent);
            background: var(--secondary-soft);
            font-weight: 600;
            display: inline-flex;
            align-items: center;
          }

          .report-date {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
          }

          .hero-grid {
            display: grid;
            grid-template-columns: 1.2fr 1fr;
            gap: 16px;
            margin-bottom: 18px;
          }

          .hero-card {
            padding: 18px;
          }

          .hero-status {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            border-radius: 999px;
            background: var(--primary-soft);
            border: 1px solid var(--line);
            font-weight: 700;
            margin-top: 10px;
          }

          .hero-meta {
            margin-top: 14px;
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 12px;
          }

          .hero-meta .meta-box {
            padding: 12px;
            border: 1px solid var(--line);
            border-radius: 14px;
            background: rgba(255,255,255,0.03);
          }

          .meta-label,
          .card-label {
            color: var(--muted);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 700;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }

          .meta-value {
            margin-top: 8px;
            font-size: 22px;
            font-weight: 700;
          }

          .cards {
            display: grid;
            grid-template-columns: repeat(6, minmax(0, 1fr));
            gap: 12px;
            margin-bottom: 18px;
          }

          .card {
            padding: 14px;
          }

          .card h2 {
            margin: 10px 0 0;
            font-size: 26px;
            line-height: 1.1;
          }

          .tabbar {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-bottom: 18px;
          }

          .tab-btn {
            cursor: pointer;
            padding: 12px 16px;
            color: var(--text);
            background: rgba(255,255,255,0.04);
            font-weight: 700;
          }

          .tab-btn.active {
            background: var(--primary-soft);
            border-color: color-mix(in srgb, var(--primary) 45%, transparent);
          }

          .tab-panel {
            display: none;
          }

          .tab-panel.active {
            display: block;
          }

          .grid-2 {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 16px;
          }

          .panel {
            padding: 18px;
            margin-bottom: 18px;
          }

          .panel h2 {
            margin: 0 0 14px;
            font-size: 20px;
            letter-spacing: -0.02em;
          }

          .panel h3 {
            margin: 0 0 12px;
            font-size: 16px;
          }

          .kv {
            display: grid;
            grid-template-columns: 180px 1fr;
            gap: 12px 16px;
          }

          .k {
            color: var(--muted);
            font-weight: 700;
          }

          .subcards {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 12px;
            margin-bottom: 14px;
          }

          .subcard {
            padding: 14px;
            border: 1px solid var(--line);
            border-radius: 14px;
            background: rgba(255,255,255,0.03);
          }

          .subcard .v {
            margin-top: 8px;
            font-size: 20px;
            font-weight: 700;
          }

          .flag-list {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
          }

          .flag-chip {
            padding: 7px 10px;
            border-radius: 999px;
            background: var(--accent-soft);
            border: 1px solid var(--line);
            font-size: 12px;
            font-weight: 700;
          }

          .table-wrap {
            overflow-x: auto;
          }

          table {
            width: 100%;
            border-collapse: collapse;
          }

          thead th {
            text-align: left;
            font-size: 12px;
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            border-bottom: 1px solid var(--line);
            padding: 12px 10px;
            white-space: nowrap;
          }

          tbody td {
            padding: 12px 10px;
            border-bottom: 1px solid rgba(255,255,255,0.06);
            vertical-align: top;
          }

          .empty-cell {
            text-align: center;
            color: var(--muted);
            padding: 18px;
          }

          .year-note {
            color: var(--muted);
            margin-top: 8px;
            font-size: 13px;
          }

          @media (max-width: 1100px) {
            .hero-grid,
            .grid-2,
            .cards,
            .subcards {
              grid-template-columns: 1fr;
            }

            .kv,
            .hero-meta {
              grid-template-columns: 1fr;
            }
          }
        </style>
      </head>
      <body>
      ${renderTopNav("reports")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Employee Attendance Detail</div>
              <h1>${escapeHtml(employee.name || "Employee")}</h1>
              <div class="subtitle">
                ${escapeHtml(employee.role || "member")} • ${escapeHtml(employee.phone_number || "-")}
              </div>
              <div class="report-date" style="margin-top: 10px;">
                <a href="/reports?userId=${encodeURIComponent(employee.id)}" class="mini-report-link">Today</a>
                <a href="/reports?userId=${encodeURIComponent(employee.id)}&days=7" class="mini-report-link">Last 7 days</a>
                <a class="btn" href="/attendance/${employee.id}?days=${selectedDays}&month=${monthNav.prevMonth}">← Previous Month</a>
<a class="btn" href="/attendance/${employee.id}?days=${selectedDays}&month=${monthNav.currentMonth}">Current Month</a>
<a class="btn" href="/attendance/${employee.id}?days=${selectedDays}&month=${monthNav.nextMonth}">Next Month →</a>
              </div>
            </div>
          </div>

          <div class="hero-grid">
            <div class="panel hero-card">
              <div class="meta-label">Current status</div>
              <div class="hero-status">${escapeHtml(today.current_status || "off")}</div>

              <div class="hero-meta">
                <div class="meta-box">
                  <div class="meta-label">Attendance date</div>
                  <div class="meta-value">${escapeHtml(today.attendance_date || "-")}</div>
                </div>
                <div class="meta-box">
                  <div class="meta-label">Late status</div>
                  <div class="meta-value">${escapeHtml(today.late_status || "-")}</div>
                </div>
                <div class="meta-box">
                  <div class="meta-label">First login</div>
                  <div class="meta-value">${escapeHtml(today.first_login_text || "-")}</div>
                </div>
                <div class="meta-box">
                  <div class="meta-label">Last logout</div>
                  <div class="meta-value">${escapeHtml(today.last_logout_text || "-")}</div>
                </div>
              </div>
            </div>

            <div class="panel hero-card">
              <h2 style="margin-top:0;">Today focus</h2>
              <div class="subcards">
                <div class="subcard">
                  <div class="meta-label">Worked today</div>
                  <div class="v">${escapeHtml(today.worked_text || "-")}</div>
                </div>
                <div class="subcard">
                  <div class="meta-label">Break today</div>
                  <div class="v">${escapeHtml(today.break_text || "-")}</div>
                </div>
                <div class="subcard">
                  <div class="meta-label">Break count</div>
                  <div class="v">${escapeHtml(String(today.break_count || 0))}</div>
                </div>
              </div>

              <div class="kv">
                <div class="k">Leave today</div><div>${today.leave_today ? "Yes" : "No"}</div>
                <div class="k">Flags</div>
                <div>
                  ${
                    todayFlags.length
                      ? `<div class="flag-list">${todayFlags
                          .map(
                            (flag) =>
                              `<span class="flag-chip">${escapeHtml(flag)}</span>`,
                          )
                          .join("")}</div>`
                      : "None"
                  }
                </div>
              </div>
            </div>
          </div>

          <div class="cards">
            <div class="card"><div class="card-label">Present days</div><h2>${escapeHtml(String(monthly.presentDays || 0))}</h2></div>
            <div class="card"><div class="card-label">Leave entries</div><h2>${escapeHtml(String(monthly.leaveDays || 0))}</h2></div>
            <div class="card"><div class="card-label">Late joins</div><h2>${escapeHtml(String(monthly.lateJoins || 0))}</h2></div>
            <div class="card"><div class="card-label">Avg login</div><h2>${escapeHtml(monthly.avgLoginTimeText || "-")}</h2></div>
            <div class="card"><div class="card-label">Avg break</div><h2>${escapeHtml(formatDurationMinutes(monthly.avgBreakMin || 0))}</h2></div>
            <div class="card"><div class="card-label">Corrections</div><h2>${escapeHtml(String(monthly.managerCorrectionCount || 0))}</h2></div>
          </div>

          <div class="tabbar">
            <button class="tab-btn active" data-tab="overview">Overview</button>
            <button class="tab-btn" data-tab="history">History</button>
            <button class="tab-btn" data-tab="leave">Leave & Vacations</button>
            <button class="tab-btn" data-tab="audit">Audit</button>
          </div>

          <div id="tab-overview" class="tab-panel active">
            <div class="grid-2">
              <div class="panel">
                <h2>Today summary</h2>
                <div class="kv">
                  <div class="k">Attendance date</div><div>${escapeHtml(today.attendance_date || "-")}</div>
                  <div class="k">First login</div><div>${escapeHtml(today.first_login_text || "-")}</div>
                  <div class="k">Last logout</div><div>${escapeHtml(today.last_logout_text || "-")}</div>
                  <div class="k">Worked</div><div>${escapeHtml(today.worked_text || "-")}</div>
                  <div class="k">Break</div><div>${escapeHtml(today.break_text || "-")}</div>
                  <div class="k">Break count</div><div>${escapeHtml(String(today.break_count || 0))}</div>
                  <div class="k">Late today</div><div>${escapeHtml(today.late_text || "No")}</div>
                  <div class="k">Late status</div><div>${escapeHtml(today.late_status || "-")}</div>
                  <div class="k">Leave today</div><div>${today.leave_today ? "Yes" : "No"}</div>
                </div>
              </div>

              <div class="panel">
                <h2>Month summary</h2>
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
                  <div class="k">Red report days</div>
                  <div id="redReportDaysValue"><span class="muted">Loading...</span></div>
                  <div class="k">Red report dates</div>
                  <div id="redReportDatesValue"><span class="muted">Loading...</span></div>
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
          </div>

          <div id="tab-history" class="tab-panel">
            <div class="panel">
              <h2>Attendance history · ${escapeHtml(selectedMonthLabel)}</h2>
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
              <h2>Behavior signals this month</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Late</th>
                      <th>Late status</th>
                      <th>Flags</th>
                      <th>Corrections</th>
                    </tr>
                  </thead>
                  <tbody>${behaviorRows}</tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-leave" class="tab-panel">
            <div class="grid-2">
              <div class="panel">
                <h2>Leave snapshot</h2>
                <div class="kv">
                  <div class="k">This month leave entries</div><div>${escapeHtml(String(monthly.leaveDays || 0))}</div>
                  <div class="k">Past leave dates</div><div>${escapeHtml(formatDateListForHumans(monthly.pastLeaveDates || []))}</div>
                  <div class="k">Upcoming leave dates</div><div>${escapeHtml(formatDateListForHumans(monthly.upcomingLeaveDates || []))}</div>
                </div>
                <div class="year-note">
                  This tab is ready for yearly vacation balance later. Right now it uses your existing monthly data.
                </div>
              </div>

              <div class="panel">
                <h2>Vacation / leave summary</h2>
                <div class="subcards">
                  <div class="subcard">
                    <div class="meta-label">This month leave</div>
                    <div class="v">${escapeHtml(String(monthly.leaveDays || 0))}</div>
                  </div>
                  <div class="subcard">
                    <div class="meta-label">Past leave dates</div>
                    <div class="v" style="font-size:15px; line-height:1.4;">${escapeHtml(formatDateListForHumans(monthly.pastLeaveDates || []))}</div>
                  </div>
                  <div class="subcard">
                    <div class="meta-label">Upcoming leave</div>
                    <div class="v" style="font-size:15px; line-height:1.4;">${escapeHtml(formatDateListForHumans(monthly.upcomingLeaveDates || []))}</div>
                  </div>
                </div>
              </div>
            </div>

            <div class="panel">
<h2>Leave rows · ${escapeHtml(selectedMonthLabel)}</h2>
<div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Leave</th>
                      <th>Status</th>
                      <th>Flags</th>
                    </tr>
                  </thead>
                  <tbody>${leaveHistoryRows}</tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-audit" class="tab-panel">
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
        </div>

<script>
  async function loadRedReports(userId) {
    const daysEl = document.getElementById("redReportDaysValue");
    const datesEl = document.getElementById("redReportDatesValue");

    if (!daysEl || !datesEl || !userId) return;

    try {
      const res = await fetch('/api/attendance/' + userId + '/red-reports?month=${monthNav.selectedMonth}', {
        headers: { Accept: "application/json" },
      });

      const json = await res.json();

      if (!json.ok) {
        daysEl.innerHTML = '<span class="muted">Failed to load</span>';
        datesEl.innerHTML = '<span class="muted">Failed to load</span>';
        return;
      }

      const payload = json.data || {};
      const redReportDays = Number(payload.redReportDays || 0);
      const redReportDates = Array.isArray(payload.redReportDates)
        ? payload.redReportDates
        : [];
      const redReportDatesText = payload.redReportDatesText || "None";

      daysEl.textContent = String(redReportDays);

      if (redReportDates.length) {
        datesEl.innerHTML =
          '<details>' +
            '<summary>' + redReportDays + ' date(s)</summary>' +
            '<div style="margin-top:8px;">' +
              escapeHtmlClient(redReportDatesText) +
            '</div>' +
          '</details>';
      } else {
        datesEl.textContent = "None";
      }
    } catch (error) {
      console.error("Red reports fetch failed:", error);
      daysEl.innerHTML = '<span class="muted">Failed to load</span>';
      datesEl.innerHTML = '<span class="muted">Failed to load</span>';
    }
  }

  function escapeHtmlClient(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function initAttendanceDetailTabs() {
    const buttons = Array.from(document.querySelectorAll(".tab-btn"));
    const panels = Array.from(document.querySelectorAll(".tab-panel"));

    buttons.forEach((btn) => {
      btn.addEventListener("click", function () {
        const tab = btn.getAttribute("data-tab");

        buttons.forEach((b) => b.classList.remove("active"));
        panels.forEach((p) => p.classList.remove("active"));

        btn.classList.add("active");
        const panel = document.getElementById("tab-" + tab);
        if (panel) panel.classList.add("active");
      });
    });
  }

  initAttendanceDetailTabs();
  loadRedReports(${JSON.stringify(employee.id || null)});
</script>
      </body>
    </html>
  `;
}

async function getDashboardData(orgId) {
  const today = getAttendanceDayDateStringFromDate(new Date());

  const [
    { data: users, error: usersError },
    { data: tasks, error: tasksError },
    { data: ownerRows, error: ownerError },
    attendanceRows,
    plannedOffRows,
    lateRows,
    reportPageData,
  ] = await Promise.all([
    supabase
      .from("users")
      .select("id, name, role, is_active")
      .eq("org_id", orgId)
      .eq("is_active", true)
      .order("name", { ascending: true }),

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
        progress,
        deadline,
        waiting_on_user_id,
        updated_at,
        business,
        area
      `,
      )
      .eq("org_id", orgId),

    supabase.from("task_owners").select("task_id, user_id").eq("org_id", orgId),

    getTodayAttendanceEventsForAllUsers(orgId),
    getPlannedOffRowsForDate(today, orgId),
    getLateArrivalRowsForDate(today, orgId),
    Promise.resolve(null),
  ]);

  if (usersError) throw usersError;
  if (tasksError) throw tasksError;
  if (ownerError) throw ownerError;

  const ownersByTaskId = {};
  for (const row of ownerRows || []) {
    if (!ownersByTaskId[row.task_id]) ownersByTaskId[row.task_id] = [];
    ownersByTaskId[row.task_id].push(row.user_id);
  }

  const usersById = {};
  for (const user of users || []) {
    usersById[user.id] = user;
  }

  const activeTasks = (tasks || []).filter(
    (t) => !["done", "cancelled", "archived"].includes(t.status || "open"),
  );

  const todayDate = new Date(`${today}T00:00:00Z`);

  const overdueTasks = activeTasks.filter((t) => {
    if (!t.deadline) return false;
    return new Date(`${t.deadline}T00:00:00Z`) < todayDate;
  });

  const blockedTasks = activeTasks.filter((t) => t.status === "blocked");

  const notStartedTasks = activeTasks.filter(
    (t) => !t.status || t.status === "open" || t.status === "pending",
  );

  const highPriorityTasks = activeTasks.filter((t) =>
    ["high", "urgent"].includes((t.priority || "").toLowerCase()),
  );

  const staleTasks = activeTasks.filter((t) => {
    if (!t.updated_at) return false;
    const updated = new Date(t.updated_at);
    const diffDays = (Date.now() - updated.getTime()) / (1000 * 60 * 60 * 24);
    return diffDays >= 5;
  });

  const plannedOff = plannedOffRows || [];
  const lateEntries = lateRows || [];
  const attendanceEvents = attendanceRows || [];

  const plannedOffUserIds = new Set(plannedOff.map((x) => x.user_id));

  const latestAttendanceByUser = new Map();
  for (const ev of attendanceEvents) {
    latestAttendanceByUser.set(ev.user_id, ev);
  }

  let employeesOnline = 0;
  let employeesOnBreak = 0;
  let employeesLoggedOut = 0;
  let employeesNoAttendance = 0;

  for (const user of users || []) {
    if (plannedOffUserIds.has(user.id)) continue;

    const latest = latestAttendanceByUser.get(user.id);

    if (!latest) {
      employeesNoAttendance += 1;
      continue;
    }

    if (latest.action === "break") employeesOnBreak += 1;
    else if (latest.action === "logout") employeesLoggedOut += 1;
    else if (latest.action === "login" || latest.action === "back")
      employeesOnline += 1;
  }

  const approvedLateCount = lateEntries.filter((x) => x.is_approved).length;
  const unapprovedLateCount = lateEntries.filter((x) => !x.is_approved).length;

  let missingReportsToday = 0;
  let redReportDays = 0;

  if (reportPageData?.rows?.length) {
    for (const row of reportPageData.rows) {
      if (row.report_status === "missing") missingReportsToday += 1;
      if (row.red_flag) redReportDays += 1;
    }
  }

  const user_task_stats = (users || []).map((user) => {
    const ownedTasks = activeTasks.filter((task) =>
      (ownersByTaskId[task.id] || []).includes(user.id),
    );

    const open_count = ownedTasks.length;

    const blocked_count = ownedTasks.filter(
      (t) => t.status === "blocked",
    ).length;

    const not_started_count = ownedTasks.filter(
      (t) => !t.status || t.status === "open" || t.status === "pending",
    ).length;

    const overdue_count = ownedTasks.filter((t) => {
      if (!t.deadline) return false;
      return new Date(`${t.deadline}T00:00:00Z`) < todayDate;
    }).length;

    const high_priority_count = ownedTasks.filter((t) =>
      ["high", "urgent"].includes((t.priority || "").toLowerCase()),
    ).length;

    const stale_count = ownedTasks.filter((t) => {
      if (!t.updated_at) return false;
      const updated = new Date(t.updated_at);
      const diffDays = (Date.now() - updated.getTime()) / (1000 * 60 * 60 * 24);
      return diffDays >= 5;
    }).length;

    const waiting_on_them_count = activeTasks.filter(
      (t) =>
        t.status === "blocked" &&
        Number(t.waiting_on_user_id) === Number(user.id),
    ).length;

    const load_score =
      open_count +
      overdue_count * 3 +
      blocked_count * 2 +
      high_priority_count * 2 +
      stale_count +
      waiting_on_them_count * 2;

    let health = "Healthy";
    if (load_score >= 35) health = "Critical";
    else if (load_score >= 22) health = "High Risk";
    else if (load_score >= 12) health = "Watch";
    else health = "Healthy";

    return {
      user_id: user.id,
      name: user.name,
      role: user.role,
      open_count,
      blocked_count,
      not_started_count,
      overdue_count,
      high_priority_count,
      stale_count,
      waiting_on_them_count,
      load_score,
      health,
    };
  });

  const summary = {
    open_tasks: activeTasks.length,
    overdue_tasks: overdueTasks.length,
    blocked_tasks: blockedTasks.length,
    not_started_tasks: notStartedTasks.length,
    high_priority_tasks: highPriorityTasks.length,
    stale_tasks: staleTasks.length,
    team_members: (users || []).length,
    employees_online: employeesOnline,
    employees_on_break: employeesOnBreak,
    employees_logged_out: employeesLoggedOut,
    employees_no_attendance: employeesNoAttendance,
    employees_on_leave: plannedOff.length,
    late_today_approved: approvedLateCount,
    late_today_unapproved: unapprovedLateCount,
    missing_reports_today: missingReportsToday,
    red_report_days: redReportDays,
  };

  return {
    summary,
    user_task_stats,
    task_groups: {
      overdue: overdueTasks
        .sort((a, b) => new Date(a.deadline) - new Date(b.deadline))
        .slice(0, 20),
      blocked: blockedTasks.slice(0, 20),
      stale: staleTasks.slice(0, 20),
      high_priority: highPriorityTasks.slice(0, 20),
    },
  };
}

app.use("/api", requireDashboardAuth);

function renderDashboardPage(data) {
  const summary = data?.summary || {};
  const userTaskStats = data?.user_task_stats || [];
  const taskGroups = data?.task_groups || {};

  const summaryCards = [
    {
      label: "Open Tasks",
      value: summary.open_tasks ?? 0,
      note: "All active tasks",
      cardClass: "info",
    },
    {
      label: "Overdue",
      value: summary.overdue_tasks ?? 0,
      note: "Past deadline and still active",
      cardClass: "danger",
    },
    {
      label: "Blocked",
      value: summary.blocked_tasks ?? 0,
      note: "Tasks currently blocked",
      cardClass: "warn",
    },
    {
      label: "High Priority",
      value: summary.high_priority_tasks ?? 0,
      note: "High + urgent active tasks",
      cardClass: "warn",
    },
    {
      label: "Stale Tasks",
      value: summary.stale_tasks ?? 0,
      note: "No updates in 5+ days",
      cardClass: "danger",
    },
    {
      label: "Team Members",
      value: summary.team_members ?? 0,
      note: "People in task dashboard",
      cardClass: "success",
    },
    {
      label: "Online Now",
      value: summary.employees_online ?? 0,
      note: "Logged in / back",
      cardClass: "success",
    },
    {
      label: "On Leave",
      value: summary.employees_on_leave ?? 0,
      note: "Planned leave today",
      cardClass: "info",
    },
    {
      label: "Missing Reports",
      value: summary.missing_reports_today ?? 0,
      note: "Missing report today",
      cardClass: "danger",
    },
    {
      label: "Late Today",
      value:
        (summary.late_today_approved ?? 0) +
        (summary.late_today_unapproved ?? 0),
      note: "Approved + not approved",
      cardClass: "warn",
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

  const sortedUsers = [...userTaskStats].sort(
    (a, b) => (b.load_score || 0) - (a.load_score || 0),
  );

  const userRows = sortedUsers.length
    ? sortedUsers
        .map(
          (row) => `
            <tr class="health-${normalizeText(row.health)}">
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'all', this)">
                  ${escapeHtml(row.name || "-")}
                </span>
              </td>
              <td>${escapeHtml(row.role || "-")}</td>
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'open', this)">
                  ${escapeHtml(row.open_count ?? 0)}
                </span>
              </td>
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'blocked', this)">
                  ${escapeHtml(row.blocked_count ?? 0)}
                </span>
              </td>
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'not_started', this)">
                  ${escapeHtml(row.not_started_count ?? 0)}
                </span>
              </td>
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'overdue', this)">
                  ${escapeHtml(row.overdue_count ?? 0)}
                </span>
              </td>
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'blocked_on_them', this)">
                  ${escapeHtml(row.waiting_on_them_count ?? 0)}
                </span>
              </td>
              <td>${escapeHtml(row.high_priority_count ?? 0)}</td>
              <td>${escapeHtml(row.stale_count ?? 0)}</td>
              <td>${escapeHtml(row.load_score ?? 0)}</td>
              <td><span class="mini-badge health-pill ${normalizeText(row.health).replace(/\s+/g, "-")}">${escapeHtml(row.health || "Healthy")}</span></td>
            </tr>
          `,
        )
        .join("")
    : `
      <tr>
        <td colspan="11" class="empty-cell">No task load data found.</td>
      </tr>
    `;

  function renderMiniTaskRows(rows, typeLabel) {
    if (!rows?.length) {
      return `<tr><td colspan="5" class="empty-cell">No ${escapeHtml(typeLabel)} tasks</td></tr>`;
    }

    return rows
      .map(
        (task) => `
        <tr>
          <td>#${escapeHtml(task.task_no || task.id)}</td>
          <td>${escapeHtml(task.title || "-")}</td>
          <td>${escapeHtml(task.priority || "-")}</td>
          <td>${escapeHtml(task.status || "-")}</td>
          <td>${escapeHtml(task.deadline || "-")}</td>
        </tr>
      `,
      )
      .join("");
  }

  return `
    <html>
      <head>
        <title>Dashboard</title>
        <style>
  ${buildThemeCss()}
  ${buildBasePageCss()}
  ${buildTopNavCss()}

          .wrap { max-width: 1700px; margin: 0 auto; padding: 24px 18px 36px; }
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

          h1 { margin: 0; font-size: 30px; letter-spacing: -0.04em; }
          .subtitle { color: var(--muted); margin-top: 8px; font-size: 14px; }

          .stats {
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 12px;
            margin-bottom: 20px;
          }

          .stat-card { padding: 14px; }
          .stat-label {
            color: var(--muted);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 700;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          }
          .stat-value { margin-top: 10px; font-size: 28px; font-weight: 700; }
          .stat-note { margin-top: 8px; color: var(--muted); font-size: 13px; }

          .panel {
            padding: 18px;
            margin-bottom: 18px;
          }

          .table-wrap {
            overflow-x: auto;
          }

          table {
            width: 100%;
            border-collapse: collapse;
          }

          th, td {
            text-align: left;
            padding: 12px;
            border-bottom: 1px solid rgba(255,255,255,0.08);
            vertical-align: middle;
          }

          th {
            color: var(--muted);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
          }

          tr:hover td {
            background: rgba(255,255,255,0.03);
          }

          .task-link {
            cursor: pointer;
            text-decoration: underline;
            text-underline-offset: 2px;
          }

          .task-link:hover {
            opacity: 0.85;
          }

          .tabbar {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-bottom: 18px;
          }

          .tab-btn {
            appearance: none;
            border: 1px solid var(--line);
            background: rgba(255,255,255,0.04);
            color: var(--text);
            padding: 10px 14px;
            border-radius: 12px;
            cursor: pointer;
            font-weight: 700;
          }

          .tab-btn.active {
            background: var(--primary-soft);
            border-color: rgba(139,124,246,0.45);
          }

          .tab-panel {
            display: none;
          }

          .tab-panel.active {
            display: block;
          }

          .grid-2 {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 18px;
          }

          .mini-badge {
            display: inline-flex;
            align-items: center;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 700;
            border: 1px solid rgba(255,255,255,0.12);
          }

          .health-pill.healthy { background: var(--success-soft); }
          .health-pill.watch { background: var(--accent-soft); }
          .health-pill.high-risk { background: rgba(255, 140, 0, 0.18); }
          .health-pill.critical { background: var(--danger-soft); }

          .health-critical td:first-child { border-left: 4px solid #ef6b73; }
          .health-high-risk td:first-child { border-left: 4px solid #f59e0b; }
          .health-watch td:first-child { border-left: 4px solid #f3b562; }
          .health-healthy td:first-child { border-left: 4px solid #58c98a; }

          .alert-list {
            display: grid;
            gap: 12px;
          }

          .alert-item {
            padding: 14px;
            border-radius: 14px;
            border: 1px solid var(--line);
            background: rgba(255,255,255,0.03);
          }

          .kpi-inline {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
          }

          .kpi-chip {
            padding: 8px 12px;
            border-radius: 999px;
            border: 1px solid var(--line);
            background: rgba(255,255,255,0.04);
            font-size: 13px;
          }

          .loading-overlay {
            position: fixed;
            inset: 0;
            background: rgba(8, 12, 22, 0.72);
            display: none;
            align-items: center;
            justify-content: center;
            z-index: 50;
          }

          .loading-overlay.show {
            display: flex;
          }

          .loading-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
            padding: 18px 22px;
            min-width: 260px;
            text-align: center;
          }

          .loading-spinner {
            width: 30px;
            height: 30px;
            border-radius: 50%;
            border: 3px solid rgba(255,255,255,0.16);
            border-top-color: var(--primary);
            margin: 0 auto 12px;
            animation: spin 0.8s linear infinite;
          }

          @keyframes spin {
            to { transform: rotate(360deg); }
          }

          @media (max-width: 1100px) {
            .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .grid-2 { grid-template-columns: 1fr; }
          }
        </style>
      </head>
      <body>
      ${renderTopNav("dashboard")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">WeSolveHR</div>
              <h1>Dashboard</h1>
              <div class="subtitle">Company-wide overview dashboard</div>
            </div>
          </div>

          <div class="stats">
            ${summaryCardsHtml}
          </div>

          <div class="tabbar">
            <button class="tab-btn active" data-tab="overview">Overview</button>
            <button class="tab-btn" data-tab="taskload">Task Load by User</button>
            <button class="tab-btn" data-tab="attention">Needs Attention</button>
            <button class="tab-btn" data-tab="taskviews">Task Views</button>
          </div>

          <div id="tab-overview" class="tab-panel active">
            <div class="panel">
              <h2 style="margin-top:0;">Leadership snapshot</h2>
              <div class="kpi-inline">
                <div class="kpi-chip">Online now: ${escapeHtml(summary.employees_online ?? 0)}</div>
                <div class="kpi-chip">On break: ${escapeHtml(summary.employees_on_break ?? 0)}</div>
                <div class="kpi-chip">No attendance yet: ${escapeHtml(summary.employees_no_attendance ?? 0)}</div>
                <div class="kpi-chip">Approved late: ${escapeHtml(summary.late_today_approved ?? 0)}</div>
                <div class="kpi-chip">Late not approved: ${escapeHtml(summary.late_today_unapproved ?? 0)}</div>
              </div>
            </div>

            <div class="panel">
              <h2 style="margin-top:0;">Task load by user</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Role</th>
                      <th>Open</th>
                      <th>Blocked</th>
                      <th>Not Started</th>
                      <th>Overdue</th>
                      <th>Blocked On Them</th>
                      <th>High Priority</th>
                      <th>Stale</th>
                      <th>Load Score</th>
                      <th>Health</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${userRows}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-taskload" class="tab-panel">
            <div class="panel">
              <h2 style="margin-top:0;">Full task load by user</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Role</th>
                      <th>Open</th>
                      <th>Blocked</th>
                      <th>Not Started</th>
                      <th>Overdue</th>
                      <th>Blocked On Them</th>
                      <th>High Priority</th>
                      <th>Stale</th>
                      <th>Load Score</th>
                      <th>Health</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${userRows}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-attention" class="tab-panel">
            <div class="grid-2">
              <div class="panel">
                <h2 style="margin-top:0;">Immediate attention</h2>
                <div class="alert-list">
                  <div class="alert-item">Overdue tasks: <strong>${escapeHtml(summary.overdue_tasks ?? 0)}</strong></div>
                  <div class="alert-item">Blocked tasks: <strong>${escapeHtml(summary.blocked_tasks ?? 0)}</strong></div>
                  <div class="alert-item">Missing reports today: <strong>${escapeHtml(summary.missing_reports_today ?? 0)}</strong></div>
                  <div class="alert-item">No attendance today: <strong>${escapeHtml(summary.employees_no_attendance ?? 0)}</strong></div>
                  <div class="alert-item">Stale tasks (5+ days no update): <strong>${escapeHtml(summary.stale_tasks ?? 0)}</strong></div>
                </div>
              </div>

              <div class="panel">
                <h2 style="margin-top:0;">Most overloaded people</h2>
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Name</th>
                        <th>Open</th>
                        <th>Overdue</th>
                        <th>Blocked</th>
                        <th>Score</th>
                        <th>Health</th>
                      </tr>
                    </thead>
                    <tbody>
                      ${sortedUsers
                        .slice(0, 8)
                        .map(
                          (row) => `
                          <tr>
                            <td>${escapeHtml(row.name)}</td>
                            <td>${escapeHtml(row.open_count ?? 0)}</td>
                            <td>${escapeHtml(row.overdue_count ?? 0)}</td>
                            <td>${escapeHtml(row.blocked_count ?? 0)}</td>
                            <td>${escapeHtml(row.load_score ?? 0)}</td>
                            <td><span class="mini-badge health-pill ${normalizeText(row.health).replace(/\s+/g, "-")}">${escapeHtml(row.health)}</span></td>
                          </tr>
                        `,
                        )
                        .join("")}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>

          <div id="tab-taskviews" class="tab-panel">
            <div class="grid-2">
              <div class="panel">
                <h2 style="margin-top:0;">Top overdue tasks</h2>
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Priority</th>
                        <th>Status</th>
                        <th>Deadline</th>
                      </tr>
                    </thead>
                    <tbody>${renderMiniTaskRows(taskGroups.overdue, "overdue")}</tbody>
                  </table>
                </div>
              </div>

              <div class="panel">
                <h2 style="margin-top:0;">Top blocked tasks</h2>
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Priority</th>
                        <th>Status</th>
                        <th>Deadline</th>
                      </tr>
                    </thead>
                    <tbody>${renderMiniTaskRows(taskGroups.blocked, "blocked")}</tbody>
                  </table>
                </div>
              </div>

              <div class="panel">
                <h2 style="margin-top:0;">High priority tasks</h2>
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Priority</th>
                        <th>Status</th>
                        <th>Deadline</th>
                      </tr>
                    </thead>
                    <tbody>${renderMiniTaskRows(taskGroups.high_priority, "high priority")}</tbody>
                  </table>
                </div>
              </div>

              <div class="panel">
                <h2 style="margin-top:0;">Stale tasks</h2>
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Priority</th>
                        <th>Status</th>
                        <th>Deadline</th>
                      </tr>
                    </thead>
                    <tbody>${renderMiniTaskRows(taskGroups.stale, "stale")}</tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div id="pageLoadingOverlay" class="loading-overlay">
          <div class="loading-card">
            <div class="loading-spinner"></div>
            <div id="pageLoadingTitle">Opening task list...</div>
          </div>
        </div>

        <script>
          const tabButtons = document.querySelectorAll('.tab-btn');
          const tabPanels = document.querySelectorAll('.tab-panel');

          tabButtons.forEach((btn) => {
            btn.addEventListener('click', () => {
              const tab = btn.dataset.tab;

              tabButtons.forEach((b) => b.classList.remove('active'));
              tabPanels.forEach((p) => p.classList.remove('active'));

              btn.classList.add('active');
              const panel = document.getElementById('tab-' + tab);
              if (panel) panel.classList.add('active');
            });
          });

          function goToTaskFilter(userId, type, clickedEl) {
            const params = new URLSearchParams();

            if (type !== 'blocked_on_them') {
              params.set('assignee', String(userId));
            }

            if (type === 'blocked') {
              params.set('blocked', 'true');
              params.append('progressBucket', 'not_begun');
              params.append('progressBucket', 'zero_to_fifty');
              params.append('progressBucket', 'fifty_to_hundred');
              params.append('progressBucket', 'complete');
              params.append('progressBucket', 'hide_cancelled');
            }

            if (type === 'overdue') {
              params.set('overdue', 'true');
            }

            if (type === 'not_started') {
              params.append('progressBucket', 'not_begun');
              params.append('progressBucket', 'hide_cancelled');
            }

            if (type === 'open' || type === 'all') {
              params.append('progressBucket', 'not_begun');
              params.append('progressBucket', 'zero_to_fifty');
              params.append('progressBucket', 'fifty_to_hundred');
              params.append('progressBucket', 'hide_cancelled');
            }

            if (type === 'blocked_on_them') {
              params.set('waitingOn', String(userId));
              params.set('blocked', 'true');
            }

            const overlay = document.getElementById('pageLoadingOverlay');
            const title = document.getElementById('pageLoadingTitle');

            if (title) {
              if (type === 'blocked_on_them') {
                title.textContent = 'Opening blocked tasks waiting on this person...';
              } else if (type === 'blocked') {
                title.textContent = 'Opening blocked tasks...';
              } else if (type === 'overdue') {
                title.textContent = 'Opening overdue tasks...';
              } else if (type === 'not_started') {
                title.textContent = 'Opening not started tasks...';
              } else {
                title.textContent = 'Opening task list...';
              }
            }

            if (overlay) overlay.classList.add('show');

            if (clickedEl) {
              clickedEl.style.opacity = '0.65';
              clickedEl.style.pointerEvents = 'none';
            }

            window.location.href = '/tasks?' + params.toString();
          }
        </script>
      </body>
    </html>
  `;
}

// =====================================================
// PHASE 6: CLIENT UPDATES - EDIT / ARCHIVE
// =====================================================

app.put(
  "/api/clients/:clientId/updates/:updateId",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const { clientId, updateId } = req.params;
      const { update_text, related_work_item_id } = req.body;

      if (!update_text) {
        return res.status(400).json({
          success: false,
          error: "Update text is required",
        });
      }

      const { data, error } = await supabase
        .from("client_updates")
        .update({
          update_text,
          related_work_item_id: related_work_item_id || null,
          updated_at: new Date().toISOString(),
        })
        .eq("id", updateId)
        .eq("client_id", clientId)
        .select()
        .single();

      if (error) throw error;

      res.json({ success: true, update: data });
    } catch (err) {
      console.error("Edit update failed:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  },
);

app.get(
  "/api/leads/voice/:id/debug",
  requireDashboardAuth,
  async (req, res) => {
    const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
    const id = Number(req.params.id);

    const { data, error } = await supabase
      .from("lead_voice_uploads")
      .select("*")
      .eq("org_id", orgId)
      .eq("id", id)
      .maybeSingle();

    if (error) return sendApiError(res, 500, error.message);
    if (!data) return sendApiError(res, 404, "Voice lead not found");

    return sendApiSuccess(res, data);
  },
);

app.post(
  "/api/clients/:clientId/milestones",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
      const actorUserId = req.loggedInUser?.id || null;
      const clientId = Number(req.params.clientId);

      const title = String(req.body.title || "").trim();

      if (!clientId || !title) {
        return sendApiError(res, 400, "Milestone title is required");
      }

      const row = {
        org_id: orgId,
        client_id: clientId,
        title,
        due_date: req.body.due_date || null,
        status: req.body.status || "planned",
        notes: req.body.notes || null,
        is_active: true,
        updated_at: new Date().toISOString(),
      };

      const { data, error } = await supabase
        .from("client_milestones")
        .insert([row])
        .select("*")
        .maybeSingle();

      if (error) throw error;

      await insertClientActivityLog({
        orgId,
        clientId,
        actorUserId,
        action: "milestone_created",
        entityType: "client_milestones",
        entityId: data.id,
        newValue: data,
      });

      await supabase
        .from("clients")
        .update({
          status: "active",
          updated_at: new Date().toISOString(),
          updated_by_user_id: actorUserId,
        })
        .eq("org_id", orgId)
        .eq("id", clientId);

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("Create milestone error:", error);
      return sendApiError(res, 500, "Failed to create milestone");
    }
  },
);

app.put(
  "/api/clients/:clientId/milestones/:milestoneId",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
      const actorUserId = req.loggedInUser?.id || null;
      const clientId = Number(req.params.clientId);
      const milestoneId = Number(req.params.milestoneId);

      const payload = {
        updated_at: new Date().toISOString(),
      };

      if (req.body.title !== undefined)
        payload.title = String(req.body.title || "").trim();
      if (req.body.due_date !== undefined)
        payload.due_date = req.body.due_date || null;
      if (req.body.status !== undefined)
        payload.status = req.body.status || "planned";
      if (req.body.notes !== undefined) payload.notes = req.body.notes || null;

      if (payload.title !== undefined && !payload.title) {
        return sendApiError(res, 400, "Milestone title is required");
      }

      const { data, error } = await supabase
        .from("client_milestones")
        .update(payload)
        .eq("id", milestoneId)
        .eq("client_id", clientId)
        .eq("org_id", orgId)
        .select("*")
        .maybeSingle();

      if (error) throw error;

      await insertClientActivityLog({
        orgId,
        clientId,
        actorUserId,
        action: "milestone_updated",
        entityType: "client_milestones",
        entityId: milestoneId,
        newValue: data,
      });

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("Update milestone error:", error);
      return sendApiError(res, 500, "Failed to update milestone");
    }
  },
);

app.post(
  "/api/clients/:clientId/milestones/:milestoneId/archive",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
      const actorUserId = req.loggedInUser?.id || null;
      const clientId = Number(req.params.clientId);
      const milestoneId = Number(req.params.milestoneId);

      const { data, error } = await supabase
        .from("client_milestones")
        .update({
          is_active: false,
          deleted_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        })
        .eq("id", milestoneId)
        .eq("client_id", clientId)
        .eq("org_id", orgId)
        .select("*")
        .maybeSingle();

      if (error) throw error;

      await supabase
        .from("client_work_items")
        .update({
          milestone_id: null,
          updated_at: new Date().toISOString(),
        })
        .eq("client_id", clientId)
        .eq("milestone_id", milestoneId);

      await insertClientActivityLog({
        orgId,
        clientId,
        actorUserId,
        action: "milestone_archived",
        entityType: "client_milestones",
        entityId: milestoneId,
        oldValue: data,
      });

      return sendApiSuccess(res, true);
    } catch (error) {
      console.error("Archive milestone error:", error);
      return sendApiError(res, 500, "Failed to archive milestone");
    }
  },
);
app.post(
  "/api/clients/:clientId/updates/:updateId/archive",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const { clientId, updateId } = req.params;

      const { error } = await supabase
        .from("client_updates")
        .update({
          is_active: false,
          archived_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        })
        .eq("id", updateId)
        .eq("client_id", clientId);

      if (error) throw error;

      res.json({ success: true });
    } catch (err) {
      console.error("Archive update failed:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  },
);

// =====================================================
// PHASE 7: ACTIONS NEEDED
// =====================================================

app.get(
  "/api/lead-voice-uploads/:id/audio",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const id = Number(req.params.id);

      if (!id) return res.status(400).send("Invalid audio ID");

      const { data: lead, error } = await supabase
        .from("lead_voice_uploads")
        .select("id, org_id, media_url, media_content_type")
        .eq("org_id", orgId)
        .eq("id", id)
        .maybeSingle();

      if (error) throw error;
      if (!lead || !lead.media_url)
        return res.status(404).send("Audio not found");

      const accountSid = process.env.TWILIO_ACCOUNT_SID;
      const authToken = process.env.TWILIO_AUTH_TOKEN;

      if (!accountSid || !authToken) {
        return res.status(500).send("Twilio credentials missing");
      }

      const auth = Buffer.from(`${accountSid}:${authToken}`).toString("base64");

      const response = await fetch(lead.media_url, {
        headers: {
          Authorization: `Basic ${auth}`,
        },
      });

      if (!response.ok) {
        const text = await response.text();
        console.error("Twilio audio fetch failed:", response.status, text);
        return res.status(response.status).send("Failed to load Twilio audio");
      }

      const arrayBuffer = await response.arrayBuffer();
      const buffer = Buffer.from(arrayBuffer);

      res.setHeader("Content-Type", lead.media_content_type || "audio/mpeg");
      res.setHeader("Content-Length", buffer.length);
      res.setHeader("Accept-Ranges", "bytes");

      return res.send(buffer);
    } catch (error) {
      console.error("Audio proxy error:", error);
      return res.status(500).send("Failed to load audio: " + error.message);
    }
  },
);

app.get(
  "/api/clients/:clientId/actions",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const { clientId } = req.params;

      const { data, error } = await supabase
        .from("client_actions")
        .select("*")
        .eq("client_id", clientId)
        .eq("archived", false)
        .order("due_date", { ascending: true, nullsFirst: false })
        .order("created_at", { ascending: false });

      if (error) throw error;

      res.json({ success: true, actions: data || [] });
    } catch (err) {
      console.error("Load actions failed:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  },
);

app.post(
  "/api/clients/:clientId/actions",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const { clientId } = req.params;

      const {
        title,
        owner_type,
        owner_name,
        due_date,
        status,
        priority,
        notes,
      } = req.body;

      if (!title || !owner_type) {
        return res.status(400).json({
          success: false,
          error: "Title and owner type are required",
        });
      }

      const { data, error } = await supabase
        .from("client_actions")
        .insert({
          client_id: clientId,
          title,
          owner_type,
          owner_name: owner_name || null,
          due_date: due_date || null,
          status: status || "Open",
          priority: priority || "Medium",
          notes: notes || null,
        })
        .select()
        .single();

      if (error) throw error;

      await supabase.from("client_updates").insert({
        client_id: clientId,
        update_text: `Action created: ${title}`,
        update_type: "action",
      });

      res.json({ success: true, action: data });
    } catch (err) {
      console.error("Create action failed:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  },
);

app.put(
  "/api/clients/:clientId/actions/:actionId",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const { clientId, actionId } = req.params;

      const payload = {
        title: req.body.title,
        owner_type: req.body.owner_type,
        owner_name: req.body.owner_name || null,
        due_date: req.body.due_date || null,
        status: req.body.status || "Open",
        priority: req.body.priority || "Medium",
        notes: req.body.notes || null,
        updated_at: new Date().toISOString(),
      };

      const { data, error } = await supabase
        .from("client_actions")
        .update(payload)
        .eq("id", actionId)
        .eq("client_id", clientId)
        .select()
        .single();

      if (error) throw error;

      await supabase.from("client_updates").insert({
        client_id: clientId,
        update_text: `Action updated: ${data.title}`,
        update_type: "action",
      });

      res.json({ success: true, action: data });
    } catch (err) {
      console.error("Update action failed:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  },
);

app.post(
  "/api/clients/:clientId/actions/:actionId/archive",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const { clientId, actionId } = req.params;

      const { data, error } = await supabase
        .from("client_actions")
        .update({
          archived: true,
          status: "Archived",
          updated_at: new Date().toISOString(),
        })
        .eq("id", actionId)
        .eq("client_id", clientId)
        .select()
        .single();

      if (error) throw error;

      await supabase.from("client_updates").insert({
        client_id: clientId,
        update_text: `Action archived: ${data.title}`,
        update_type: "action",
      });

      res.json({ success: true });
    } catch (err) {
      console.error("Archive action failed:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  },
);

// =====================================================
// PHASE 8: CONTRIBUTORS
// Internal / Contractor / Client Contact
// =====================================================

app.get(
  "/api/clients/:clientId/contributors",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const { clientId } = req.params;

      const { data, error } = await supabase
        .from("client_contributors")
        .select("*")
        .eq("client_id", clientId)
        .eq("archived", false)
        .order("person_type", { ascending: true })
        .order("created_at", { ascending: false });

      if (error) throw error;

      res.json({ success: true, contributors: data || [] });
    } catch (err) {
      console.error("Load contributors failed:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  },
);

app.post(
  "/api/clients/:clientId/contributors",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const { clientId } = req.params;

      const {
        person_type,
        user_id,
        name,
        email,
        phone,
        role,
        can_update_work,
        can_view_client_dashboard,
        status,
        notes,
      } = req.body;

      if (!person_type || !name || !role) {
        return res.status(400).json({
          success: false,
          error: "Person type, name, and role are required",
        });
      }

      const { data, error } = await supabase
        .from("client_contributors")
        .insert({
          client_id: clientId,
          person_type,
          user_id: user_id || null,
          name,
          email: email || null,
          phone: phone || null,
          role,
          can_update_work: !!can_update_work,
          can_view_client_dashboard: !!can_view_client_dashboard,
          status: status || "Active",
          notes: notes || null,
        })
        .select()
        .single();

      if (error) throw error;

      await supabase.from("client_updates").insert({
        client_id: clientId,
        update_text: `Contributor added: ${name} as ${role}`,
        update_type: "contributor",
      });

      res.json({ success: true, contributor: data });
    } catch (err) {
      console.error("Create contributor failed:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  },
);

app.put(
  "/api/clients/:clientId/contributors/:contributorId",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const { clientId, contributorId } = req.params;

      const payload = {
        person_type: req.body.person_type,
        user_id: req.body.user_id || null,
        name: req.body.name,
        email: req.body.email || null,
        phone: req.body.phone || null,
        role: req.body.role,
        can_update_work: !!req.body.can_update_work,
        can_view_client_dashboard: !!req.body.can_view_client_dashboard,
        status: req.body.status || "Active",
        notes: req.body.notes || null,
        updated_at: new Date().toISOString(),
      };

      const { data, error } = await supabase
        .from("client_contributors")
        .update(payload)
        .eq("id", contributorId)
        .eq("client_id", clientId)
        .select()
        .single();

      if (error) throw error;

      await supabase.from("client_updates").insert({
        client_id: clientId,
        update_text: `Contributor updated: ${data.name}`,
        update_type: "contributor",
      });

      res.json({ success: true, contributor: data });
    } catch (err) {
      console.error("Update contributor failed:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  },
);

app.post(
  "/api/clients/:clientId/contributors/:contributorId/archive",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const { clientId, contributorId } = req.params;

      const { data, error } = await supabase
        .from("client_contributors")
        .update({
          archived: true,
          status: "Inactive",
          updated_at: new Date().toISOString(),
        })
        .eq("id", contributorId)
        .eq("client_id", clientId)
        .select()
        .single();

      if (error) throw error;

      await supabase.from("client_updates").insert({
        client_id: clientId,
        update_text: `Contributor archived: ${data.name}`,
        update_type: "contributor",
      });

      res.json({ success: true });
    } catch (err) {
      console.error("Archive contributor failed:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  },
);

app.get("/leads", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
    const data = await getLeadsOverviewData(orgId);
    return res.send(renderLeadsOverviewPage(data));
  } catch (error) {
    console.error("GET /leads error:", error);
    return res.status(500).send("Failed to load leads page");
  }
});

app.get("/leads/:business", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
    const business = req.params.business;

    const allowedTabs = [
      "all",
      "b2b",
      "b2c",
      "in_progress",
      "completed",
      "voice_inbox",
    ];

    const selectedTab = allowedTabs.includes(req.query.tab)
      ? req.query.tab
      : "all";

    const search = String(req.query.search || "").trim();
    const page = Number(req.query.page || 1);

    const data = await getBusinessLeadsData(
      orgId,
      business,
      selectedTab,
      search,
      page,
      {
        industry: req.query.industry || "",
        capability: req.query.capability || "",
        entity_type: req.query.entity_type || "",
        status: req.query.status || "",
        city: req.query.city || "",
        state: req.query.state || "",
        assigned_to: req.query.assigned_to || "",
        qualified: req.query.qualified || "",
        worth_talking: req.query.worth_talking || "",
      },
    );

    return res.send(renderBusinessLeadsPage(data));
  } catch (error) {
    console.error("GET /leads/:business error:", error);
    return res
      .status(500)
      .send(
        "Failed to load business leads page: " +
          (error.message || String(error)),
      );
  }
});

app.post("/api/rasset-leads/enrich", requireDashboardAuth, async (req, res) => {
  try {
    const website = String(req.body.website || "").trim();
    const googleMapsUrl = String(req.body.google_maps_url || "").trim();

    const data = await enrichLeadFromUrl({
      url: website,
      googleMapsUrl,
    });

    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("POST /api/rasset-leads/enrich error:", error);
    return sendApiError(
      res,
      500,
      error.message || "Failed to enrich Rasset lead",
    );
  }
});

app.post(
  "/api/rasset-leads/import-excel",
  requireDashboardAuth,
  upload.single("file"),
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const user = req.session?.user || req.loggedInUser || {};

      if (!req.file?.buffer) {
        return sendApiError(res, 400, "Excel file is required");
      }

      const data = await importRassetLeadsFromExcel({
        orgId,
        buffer: req.file.buffer,
        fileName: req.file.originalname,
        uploadedByUserId: user.id || null,
        uploadedByName: user.name || user.phone_number || "Unknown",
      });

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("POST /api/rasset-leads/import-excel error:", error);
      return sendApiError(
        res,
        500,
        error.message || "Failed to import Rasset Excel",
      );
    }
  },
);

app.post(
  "/api/joolian-leads/import-b2b-excel",
  requireDashboardAuth,
  upload.single("file"),
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const user = req.session?.user || req.loggedInUser || {};

      if (!req.file?.buffer) {
        return sendApiError(res, 400, "Excel file is required");
      }

      const data = await importRassetLeadsFromExcel({
        orgId,
        buffer: req.file.buffer,
        fileName: req.file.originalname,
        uploadedByUserId: user.id || null,
        uploadedByName: user.name || user.phone_number || "Unknown",
      });

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("POST /api/joolian-leads/import-b2b-excel error:", error);
      return sendApiError(
        res,
        500,
        error.message || "Failed to import Joolian B2B Excel",
      );
    }
  },
);

app.delete(
  "/api/business-leads/:business/:id",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const business = req.params.business;
      const leadId = Number(req.params.id);

      if (!leadId) {
        return sendApiError(res, 400, "Invalid lead ID");
      }

      const data = await deleteBusinessLead({
        orgId,
        business,
        leadId,
      });

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("DELETE /api/business-leads/:business/:id error:", error);
      return sendApiError(res, 500, error.message || "Failed to delete lead");
    }
  },
);

app.get(
  "/api/business-leads/:business/check-phone",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const business = getBusinessCanonicalName(req.params.business);
      const tableName = getBusinessLeadTableName(business);
      const digits = normalizeLeadPhone(req.query.phone || "");

      if (!tableName) return sendApiError(res, 400, "Invalid business");
      if (!digits) return sendApiSuccess(res, { duplicate: false });

      const { data, error } = await supabase
        .from(tableName)
        .select(
          "id, phone, company, business_name, contact_name, city, state, status, lead_stage",
        )
        .eq("org_id", orgId);

      if (error) throw error;

      const duplicate = (data || []).find(function (row) {
        return normalizeLeadPhone(row.phone) === digits;
      });

      return sendApiSuccess(res, {
        duplicate: !!duplicate,
        lead: duplicate || null,
      });
    } catch (error) {
      console.error("check-phone error:", error);
      return sendApiError(res, 500, error.message || "Failed to check phone");
    }
  },
);

app.post(
  "/api/business-leads/:business",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const business = req.params.business;

      const data = await createBusinessLead({
        orgId,
        business,
        body: req.body,
      });

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("POST /api/business-leads/:business error:", error);
      return sendApiError(
        res,
        error.statusCode || 500,
        error.message || "Failed to create lead",
      );
    }
  },
);

app.get(
  "/api/business-leads/:business/call-summaries",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const business = getBusinessCanonicalName(req.params.business);
      const phone = (req.query.phone || "").trim();

      if (!business) {
        return sendApiError(res, 400, "Invalid business");
      }

      if (!phone) {
        return sendApiError(res, 400, "Phone is required");
      }

      const { data, error } = await supabase
        .from("lead_voice_uploads")
        .select(
          "id, business, lead_phone, sender_phone, status, raw_transcript, cleaned_transcript, translated_text, conversation_rows, transcription_model, transcription_confidence, important_points, pain_points, follow_up_questions, review_notes, media_url, created_at, updated_at, verified_by, verified_at",
        )
        .eq("org_id", orgId)
        .eq("business", business)
        .eq("lead_phone", phone)
        .order("created_at", { ascending: false });

      if (error) throw error;

      return sendApiSuccess(res, data || []);
    } catch (err) {
      console.error("call summaries error:", err);
      return sendApiError(
        res,
        500,
        err.message || "Failed to load call summaries",
      );
    }
  },
);

app.get(
  "/api/business-leads/:business/:id",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const business = req.params.business;
      const leadId = Number(req.params.id);

      if (!leadId) {
        return sendApiError(res, 400, "Invalid lead ID");
      }

      const data = await getBusinessLeadById({
        orgId,
        business,
        leadId,
      });

      if (!data) {
        return sendApiError(res, 404, "Lead not found");
      }

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("GET /api/business-leads/:business/:id error:", error);
      return sendApiError(res, 500, error.message || "Failed to fetch lead");
    }
  },
);

app.patch(
  "/api/business-leads/:business/:id/quick-toggle",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const business = getBusinessCanonicalName(req.params.business);
      const tableName = getBusinessLeadTableName(business);
      const leadId = Number(req.params.id);

      const field = String(req.body.field || "").trim();
      const value =
        field === "lead_stage"
          ? String(req.body.value || "").trim()
          : req.body.value === true;

      const allowedFields = ["l2_done", "qualified", "lead_stage"];
      if (!tableName || !leadId) {
        return sendApiError(res, 400, "Invalid lead");
      }

      if (!allowedFields.includes(field)) {
        return sendApiError(res, 400, "Invalid checkbox field");
      }

      const { data, error } = await supabase
        .from(tableName)
        .update({
          [field]: value,
          updated_at: new Date().toISOString(),
        })
        .eq("org_id", orgId)
        .eq("id", leadId)
        .select()
        .single();

      if (error) throw error;

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error(
        "PATCH /api/business-leads/:business/:id/quick-toggle error:",
        error,
      );
      return sendApiError(
        res,
        500,
        error.message || "Failed to update lead checkbox",
      );
    }
  },
);

app.put(
  "/api/business-leads/:business/:id",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const business = req.params.business;
      const leadId = Number(req.params.id);

      if (!leadId) {
        return sendApiError(res, 400, "Invalid lead ID");
      }

      const data = await updateBusinessLead({
        orgId,
        business,
        leadId,
        body: req.body,
      });

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("PUT /api/business-leads/:business/:id error:", error);
      return sendApiError(
        res,
        error.statusCode || 500,
        error.message || "Failed to update lead",
      );
    }
  },
);

app.post(
  "/api/business-leads/enrich-url",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const url = String(req.body.url || "").trim();

      const data = await enrichLeadFromUrl({
        url,
        googleMapsUrl: "",
      });

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("POST /api/business-leads/enrich-url error:", error);
      return sendApiError(res, 500, error.message || "Failed to enrich URL");
    }
  },
);

app.get("/health/live", (_req, res) => {
  return res.status(200).json({ ok: true, status: "live" });
});

app.get("/api/top-nav-summary", requireDashboardAuth, async (req, res) => {
  try {
    const actingUser = req.loggedInUser;

    if (!actingUser || !isManagerOrAdmin(actingUser)) {
      return sendApiSuccess(res, {
        offCount: 0,
        offNames: [],
        breakCount: 0,
        breakNames: [],
      });
    }

    const today = getAttendanceDayDateStringFromDate(new Date());

    const [plannedOffRows, usersResult, eventsResult] = await Promise.all([
      getPlannedOffRowsForDate(today, actingUser.org_id),
      supabase
        .from("users")
        .select("id, name")
        .eq("org_id", actingUser.org_id)
        .eq("is_active", true)
        .order("name", { ascending: true }),
      supabase
        .from("attendance_events")
        .select("user_id, action, created_at")
        .eq("org_id", actingUser.org_id)
        .order("created_at", { ascending: false }),
    ]);

    if (usersResult.error) {
      console.error("top nav users error:", usersResult.error);
      return sendApiError(res, 500, "Failed to fetch users");
    }

    if (eventsResult.error) {
      console.error("top nav events error:", eventsResult.error);
      return sendApiError(res, 500, "Failed to fetch events");
    }

    const offNames = (plannedOffRows || []).map(
      (x) => x.users?.name || "Unknown",
    );

    const latestByUser = new Map();
    for (const event of eventsResult.data || []) {
      if (!latestByUser.has(event.user_id)) {
        latestByUser.set(event.user_id, event);
      }
    }

    const breakNames = (usersResult.data || [])
      .filter((u) => latestByUser.get(u.id)?.action === "break")
      .map((u) => u.name);

    return sendApiSuccess(res, {
      offCount: offNames.length,
      offNames,
      breakCount: breakNames.length,
      breakNames,
    });
  } catch (error) {
    console.error("top nav summary error:", error);
    return sendApiError(res, 500, "Failed to load top nav summary");
  }
});

app.get("/account", requireUserLogin, async (req, res) => {
  const user = req.loggedInUser;
  const isAdminView = isManagerOrAdmin(user);

  const { data: appraisal } = await supabase
    .from("employee_feedback")
    .select("*")
    .eq("user_id", user.id)
    .eq("type", "appraisal")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  const { data: feedbackItems } = await supabase
    .from("employee_feedback")
    .select("*")
    .eq("user_id", user.id)
    .neq("type", "appraisal")
    .order("created_at", { ascending: false })
    .limit(20);

  const ptoRemaining = (user.pto_total || 12) - (user.pto_used || 0);
  const sickRemaining = (user.sick_total || 12) - (user.sick_used || 0);

  const feedbackHtml = feedbackItems?.length
    ? feedbackItems
        .map((item) => {
          const labelMap = {
            feedback: "Feedback",
            appreciation: "Appreciation",
            coaching: "Coaching",
            one_on_one: "1:1 Note",
          };

          return `
            <div class="timeline-item">
              <div class="timeline-badge">${escapeHtml(labelMap[item.type] || item.type)}</div>
              <div class="timeline-date">${formatDateTime(item.created_at)}</div>
              <div class="timeline-note">${escapeHtml(item.note || item.manager_comment || "")}</div>
            </div>
          `;
        })
        .join("")
    : `<div class="empty-state">No feedback yet</div>`;

  let futureLeaveRows = [];
  let teamFeedbackRows = [];
  let teamAppraisalRows = [];
  let leaveSummaryRows = [];

  if (isAdminView) {
    const today = getAttendanceDayDateStringFromDate(new Date());

    const [
      futureLeaveResult,
      teamFeedbackResult,
      teamAppraisalResult,
      usersResult,
      allPlannedLeaveResult,
    ] = await Promise.all([
      supabase
        .from("planned_time_off")
        .select(
          `
          id,
          off_date,
          note,
          created_at,
          users!planned_time_off_user_id_fkey(name),
          created_by:users!planned_time_off_created_by_user_id_fkey(name)
        `,
        )
        .eq("org_id", user.org_id)
        .gte("off_date", today)
        .order("off_date", { ascending: true })
        .limit(30),

      supabase
        .from("employee_feedback")
        .select(
          `
          id,
          type,
          note,
          manager_comment,
          created_at,
          users!employee_feedback_user_id_fkey(name),
          created_by:users!employee_feedback_created_by_user_id_fkey(name)
        `,
        )
        .eq("org_id", user.org_id)
        .neq("type", "appraisal")
        .order("created_at", { ascending: false })
        .limit(30),

      supabase
        .from("employee_feedback")
        .select(
          `
          id,
          rating,
          strengths,
          improvement_areas,
          manager_comment,
          created_at,
          users!employee_feedback_user_id_fkey(name),
          created_by:users!employee_feedback_created_by_user_id_fkey(name)
        `,
        )
        .eq("org_id", user.org_id)
        .eq("type", "appraisal")
        .order("created_at", { ascending: false })
        .limit(20),

      supabase
        .from("users")
        .select("id, name")
        .eq("org_id", user.org_id)
        .eq("is_active", true)
        .order("name", { ascending: true }),

      supabase
        .from("planned_time_off")
        .select(
          `
          id,
          user_id,
          off_date,
          users!planned_time_off_user_id_fkey(name)
        `,
        )
        .eq("org_id", user.org_id)
        .order("off_date", { ascending: false }),
    ]);

    futureLeaveRows = futureLeaveResult.data || [];
    teamFeedbackRows = teamFeedbackResult.data || [];
    teamAppraisalRows = teamAppraisalResult.data || [];

    const allUsers = usersResult.data || [];
    const allLeaves = allPlannedLeaveResult.data || [];

    leaveSummaryRows = allUsers.map((u) => {
      const userLeaves = allLeaves.filter((x) => x.user_id === u.id);
      const upcomingLeaves = userLeaves
        .filter((x) => x.off_date >= today)
        .sort((a, b) => String(a.off_date).localeCompare(String(b.off_date)));

      return {
        name: u.name,
        totalLeaveCount: userLeaves.length,
        upcomingLeaveCount: upcomingLeaves.length,
        nextLeaveDate: upcomingLeaves[0]?.off_date || null,
      };
    });
  }

  const futureLeaveHtml = futureLeaveRows.length
    ? futureLeaveRows
        .map(
          (row) => `
          <tr>
            <td>${escapeHtml(row.users?.name || "-")}</td>
            <td>${escapeHtml(formatDateOnly(row.off_date))}</td>
            <td>${escapeHtml(row.created_by?.name || "-")}</td>
            <td>${escapeHtml(row.note || "-")}</td>
          </tr>
        `,
        )
        .join("")
    : `<tr><td colspan="4" class="empty-cell">No upcoming leave found</td></tr>`;

  const teamFeedbackHtml = teamFeedbackRows.length
    ? teamFeedbackRows
        .map((row) => {
          const labelMap = {
            feedback: "Feedback",
            appreciation: "Appreciation",
            coaching: "Coaching",
            one_on_one: "1:1 Note",
          };

          return `
            <tr>
              <td>${escapeHtml(row.users?.name || "-")}</td>
              <td>${escapeHtml(labelMap[row.type] || row.type || "-")}</td>
              <td>${escapeHtml(row.note || row.manager_comment || "-")}</td>
              <td>${escapeHtml(row.created_by?.name || "-")}</td>
              <td>${escapeHtml(formatDateTime(row.created_at))}</td>
            </tr>
          `;
        })
        .join("")
    : `<tr><td colspan="5" class="empty-cell">No team feedback found</td></tr>`;

  const teamAppraisalHtml = teamAppraisalRows.length
    ? teamAppraisalRows
        .map(
          (row) => `
          <tr>
            <td>${escapeHtml(row.users?.name || "-")}</td>
            <td>${escapeHtml(row.rating || "-")}</td>
            <td>${escapeHtml(row.strengths || "-")}</td>
            <td>${escapeHtml(row.improvement_areas || "-")}</td>
            <td>${escapeHtml(row.manager_comment || "-")}</td>
            <td>${escapeHtml(formatDateTime(row.created_at))}</td>
          </tr>
        `,
        )
        .join("")
    : `<tr><td colspan="6" class="empty-cell">No team appraisals found</td></tr>`;

  const leaveSummaryHtml = leaveSummaryRows.length
    ? leaveSummaryRows
        .map(
          (row) => `
          <tr>
            <td>${escapeHtml(row.name || "-")}</td>
            <td>${escapeHtml(String(row.totalLeaveCount ?? 0))}</td>
            <td>${escapeHtml(String(row.upcomingLeaveCount ?? 0))}</td>
            <td>${escapeHtml(row.nextLeaveDate ? formatDateOnly(row.nextLeaveDate) : "-")}</td>
          </tr>
        `,
        )
        .join("")
    : `<tr><td colspan="4" class="empty-cell">No leave summary found</td></tr>`;

  res.send(`
    <html>
      <head>
        <title>My Account</title>
        <style>
          ${buildTopNavCss()}
          body {
            margin: 0;
            font-family: Inter, Arial, sans-serif;
            background:
              radial-gradient(circle at top left, rgba(139,124,246,0.14), transparent 30%),
              radial-gradient(circle at top right, rgba(86,199,217,0.14), transparent 22%),
              linear-gradient(180deg, #1b2238 0%, #151a2e 100%);
            color: #f3f6ff;
          }

          .wrap {
            max-width: 1200px;
            margin: 0 auto;
            padding: 28px 18px 40px;
          }

          .topbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            flex-wrap: wrap;
            margin-bottom: 22px;
          }

          .title-block h1 {
            margin: 0;
            font-size: 32px;
            letter-spacing: -0.03em;
          }

          .title-block p {
            margin: 8px 0 0;
            color: #c4cce0;
            font-size: 14px;
          }

          .grid {
            display: grid;
            grid-template-columns: 1.05fr 0.95fr;
            gap: 18px;
          }

          .card {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 18px;
            padding: 18px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.18);
          }

          .card h2 {
            margin: 0 0 14px;
            font-size: 20px;
          }

          .profile-meta,
          .stats-row {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 12px;
          }

          .meta-box,
          .stat-card {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 14px;
            padding: 14px;
          }

          .meta-label,
          .stat-label {
            font-size: 12px;
            color: #c4cce0;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 8px;
          }

          .meta-value,
          .appraisal-value,
          .timeline-note {
            font-size: 15px;
            line-height: 1.6;
          }

          .stat-value {
            font-size: 28px;
            font-weight: 700;
          }

          .appraisal-block {
            display: grid;
            gap: 12px;
          }

          .appraisal-row {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 14px;
            padding: 12px;
          }

          .appraisal-label {
            font-size: 12px;
            color: #c4cce0;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 6px;
          }

          .timeline {
            display: flex;
            flex-direction: column;
            gap: 12px;
          }

          .timeline-item {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 14px;
            padding: 14px;
          }

          .timeline-badge {
            display: inline-block;
            padding: 5px 10px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 700;
            background: rgba(139,124,246,0.16);
            border: 1px solid rgba(255,255,255,0.08);
            margin-bottom: 8px;
          }

          .timeline-date {
            font-size: 12px;
            color: #c4cce0;
            margin-bottom: 8px;
          }

          .empty-state {
            color: #c4cce0;
            padding: 16px;
            border: 1px dashed rgba(255,255,255,0.12);
            border-radius: 14px;
            text-align: center;
          }

          .admin-section {
            margin-top: 24px;
            display: grid;
            gap: 18px;
          }

          .section-eyebrow {
            font-size: 12px;
            color: #c4cce0;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            margin-bottom: 8px;
          }

          .table-wrap {
            overflow-x: auto;
          }

          table {
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
          }

          th,
          td {
            text-align: left;
            padding: 12px 10px;
            border-bottom: 1px solid rgba(255,255,255,0.08);
            vertical-align: top;
          }

          th {
            color: #c4cce0;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
          }

          .empty-cell {
            text-align: center;
            color: #c4cce0;
            padding: 18px;
          }

          @media (max-width: 860px) {
            .grid,
            .profile-meta,
            .stats-row {
              grid-template-columns: 1fr;
            }
          }
        </style>
      </head>
      <body>
        ${renderTopNav("account")}
        <div class="wrap">
          <div class="topbar">
            <div class="title-block">
              <h1>${escapeHtml(user.name || "My Account")}</h1>
              <p>${escapeHtml(user.role || "")}</p>
            </div>
          </div>

          <div class="grid">
            <div style="display:grid; gap:18px;">
              <div class="card">
                <h2>Profile</h2>
                <div class="profile-meta">
                  <div class="meta-box">
                    <div class="meta-label">Name</div>
                    <div class="meta-value">${escapeHtml(user.name || "-")}</div>
                  </div>
                  <div class="meta-box">
                    <div class="meta-label">Role</div>
                    <div class="meta-value">${escapeHtml(user.role || "-")}</div>
                  </div>
                </div>
              </div>

              <div class="card">
                <h2>Leave Balance</h2>
                <div class="stats-row">
                  <div class="stat-card">
                    <div class="stat-label">PTO Remaining</div>
                    <div class="stat-value">${ptoRemaining}</div>
                  </div>
                  <div class="stat-card">
                    <div class="stat-label">Sick Remaining</div>
                    <div class="stat-value">${sickRemaining}</div>
                  </div>
                </div>
              </div>

              <div class="card">
                <h2>Last Appraisal</h2>
                ${
                  appraisal
                    ? `
                      <div class="appraisal-block">
                        <div class="appraisal-row">
                          <div class="appraisal-label">Rating</div>
                          <div class="appraisal-value">${appraisal.rating || "-"}</div>
                        </div>
                        <div class="appraisal-row">
                          <div class="appraisal-label">Review Date</div>
                          <div class="appraisal-value">${formatDateTime(appraisal.created_at)}</div>
                        </div>
                        <div class="appraisal-row">
                          <div class="appraisal-label">Strengths</div>
                          <div class="appraisal-value">${escapeHtml(appraisal.strengths || "-")}</div>
                        </div>
                        <div class="appraisal-row">
                          <div class="appraisal-label">Improvement Areas</div>
                          <div class="appraisal-value">${escapeHtml(appraisal.improvement_areas || "-")}</div>
                        </div>
                        <div class="appraisal-row">
                          <div class="appraisal-label">Manager Comment</div>
                          <div class="appraisal-value">${escapeHtml(appraisal.manager_comment || "-")}</div>
                        </div>
                      </div>
                    `
                    : `<div class="empty-state">No appraisal yet</div>`
                }
              </div>
            </div>

            <div class="card">
              <h2>Feedback Timeline</h2>
              <div class="timeline">
                ${feedbackHtml}
              </div>
            </div>
          </div>

          ${
            isAdminView
              ? `
                <div class="admin-section">
                  <div class="card">
                    <div class="section-eyebrow">Admin only</div>
                    <h2>Future Leave</h2>
                    <div class="table-wrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Employee</th>
                            <th>Leave Date</th>
                            <th>Created By</th>
                            <th>Note</th>
                          </tr>
                        </thead>
                        <tbody>
                          ${futureLeaveHtml}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div class="card">
                    <div class="section-eyebrow">Admin only</div>
                    <h2>Team Feedback</h2>
                    <div class="table-wrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Employee</th>
                            <th>Type</th>
                            <th>Note</th>
                            <th>Created By</th>
                            <th>Created At</th>
                          </tr>
                        </thead>
                        <tbody>
                          ${teamFeedbackHtml}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div class="card">
                    <div class="section-eyebrow">Admin only</div>
                    <h2>Team Appraisals</h2>
                    <div class="table-wrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Employee</th>
                            <th>Rating</th>
                            <th>Strengths</th>
                            <th>Improvement Areas</th>
                            <th>Manager Comment</th>
                            <th>Created At</th>
                          </tr>
                        </thead>
                        <tbody>
                          ${teamAppraisalHtml}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div class="card">
                    <div class="section-eyebrow">Admin only</div>
                    <h2>Leave Summary</h2>
                    <div class="table-wrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Employee</th>
                            <th>Total Leave Entries</th>
                            <th>Upcoming Leaves</th>
                            <th>Next Leave</th>
                          </tr>
                        </thead>
                        <tbody>
                          ${leaveSummaryHtml}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              `
              : ""
          }
        </div>
      </body>
    </html>
  `);
});

app.get("/login", (req, res) => {
  res.send(renderLoginPage());
});

app.post("/login", async (req, res) => {
  try {
    const rawPhone = String(req.body.phone || "").trim();
    const password = String(req.body.password || "").trim();

    const normalizedPhone = normalizePhoneForLogin(rawPhone);
    const digitsOnly = normalizedPhone.replace(/\D/g, "");

    if (!normalizedPhone || !password) {
      return res
        .status(400)
        .send(renderLoginPage("Please enter phone number and password."));
    }

    const phoneCandidates = [
      normalizedPhone, // +12133081594
      digitsOnly, // 12133081594
      `whatsapp:${normalizedPhone}`, // whatsapp:+12133081594
      rawPhone, // whatever user typed
      rawPhone.replace(/^whatsapp:/i, "").trim(),
    ].filter(Boolean);

    const { data: users, error } = await supabase
      .from("users")
      .select("*")
      .in("phone_number", [...new Set(phoneCandidates)])
      .eq("is_active", true)
      .limit(1);

    if (error) {
      console.error("Login lookup error:", error);
      return res
        .status(500)
        .send(renderLoginPage("Unable to log in right now. Please try again."));
    }

    const user = users?.[0];

    if (!user || !user.password_hash) {
      return res
        .status(401)
        .send(renderLoginPage("Invalid phone number or password."));
    }

    const matches = await bcrypt.compare(password, user.password_hash);

    if (!matches) {
      return res
        .status(401)
        .send(renderLoginPage("Invalid phone number or password."));
    }

    req.session.userId = user.id;

    await supabase
      .from("users")
      .update({
        last_login_at: new Date().toISOString(),
      })
      .eq("id", user.id);

    return res.redirect(getPostLoginRedirectPath(user));
  } catch (err) {
    console.error("Login route error:", err);
    return res
      .status(500)
      .send(renderLoginPage("Something went wrong while logging in."));
  }
});

app.get("/my-dashboard", requireUserLogin, async (req, res) => {
  try {
    const user = req.loggedInUser;

    if (isManagerOrAdmin(user)) {
      return res.redirect("/dashboard");
    }

    const [taskData, attendanceData, reportData] = await Promise.all([
      getUserTaskWorkspaceData({
        userId: user.id,
        orgId: user.org_id,
        tab: "pending",
      }),
      getAttendancePageData(user.org_id),
      getDailyNarrativeReport({
        orgId: user.org_id,
        reportDate: getReportDateString(),
        userId: user.id,
      }),
    ]);

    const myAttendanceRows = Array.isArray(attendanceData?.rows)
      ? attendanceData.rows.filter(
          (row) => Number(row.user_id) === Number(user.id),
        )
      : [];

    const myAttendance = myAttendanceRows[0] || null;

    return res.status(200).type("html").send(
      renderMyDashboardPage({
        user,
        taskData,
        myAttendance,
        reportData,
      }),
    );
  } catch (error) {
    console.error("My dashboard error:", error);
    return res.status(500).type("html").send(`
      <html>
        <head><title>My Dashboard Error</title></head>
        <body>
          ${renderTopNav("dashboard")}
          <pre>${escapeHtml(error?.stack || error?.message || String(error))}</pre>
        </body>
      </html>
    `);
  }
});

app.get("/logout", (req, res) => {
  req.session.destroy(() => {
    res.redirect("/login");
  });
});

app.get("/reports", requireDashboardAuth, async (req, res) => {
  try {
    const userId = req.query.userId ? Number(req.query.userId) : null;
    const days = req.query.days ? Number(req.query.days) : 1;
    const reportDate =
      String(req.query.date || "").trim() || getReportDateString();

    if (userId) {
      const safeDays = Math.max(1, Number(days || 1));

      if (safeDays > 1) {
        const data = await getMultiDayNarrativeReport({
          orgId: DASHBOARD_ORG_ID,
          userId,
          days: safeDays,
          endDate: reportDate,
        });

        return res.status(200).send(renderMultiDayUserReportsPage(data));
      }

      const daily = await getDailyNarrativeReport({
        orgId: DASHBOARD_ORG_ID,
        reportDate,
        userId,
      });

      return res.status(200).send(
        renderMultiDayUserReportsPage({
          mode: "multi_day_user",
          userId,
          endDate: reportDate,
          days: 1,
          dailyReports: [daily],
        }),
      );
    }

    const data = await getDailyNarrativeReport({
      orgId: DASHBOARD_ORG_ID,
      reportDate,
      userId: null,
      includeUsers: false,
    });

    return res.status(200).send(renderReportsPage(data));
  } catch (error) {
    console.error("Reports page error:", error);
    return res.status(500).send("Failed to load reports page");
  }
});

app.get("/api/reports/summary", requireDashboardAuth, async (req, res) => {
  try {
    console.log("HIT /api/reports/summary");
    const reportDate = String(req.query.date || getReportDateString());
    const userId = req.query.userId ? Number(req.query.userId) : null;

    const data = await getDailyNarrativeReport({
      orgId: DASHBOARD_ORG_ID,
      reportDate,
      userId: userId || null,
      includeUsers: true,
    });

    console.log("summary data ready", {
      reportDate: data?.reportDate,
      usersCount: data?.users?.length,
      complianceKeys: Object.keys(data?.compliance || {}),
    });

    return sendApiSuccess(res, {
      summaryHtml: renderReportsSummaryHtml(
        data.compliance || {},
        data.reportDate,
      ),
    });
  } catch (error) {
    console.error("Reports summary API error:", error);
    return sendApiError(res, 500, "Failed to load reports summary");
  }
});

app.get("/api/reports/cards", requireDashboardAuth, async (req, res) => {
  try {
    console.log("HIT /api/reports/cards");
    const reportDate = String(req.query.date || getReportDateString());
    const userId = req.query.userId ? Number(req.query.userId) : null;

    const data = await getDailyNarrativeReport({
      orgId: DASHBOARD_ORG_ID,
      reportDate,
      userId: userId || null,
      includeUsers: true,
    });

    console.log("cards data ready", {
      reportDate: data?.reportDate,
      usersCount: data?.users?.length,
    });

    return sendApiSuccess(res, {
      cardsHtml: renderReportCardsHtml(data.users || [], data.reportDate),
    });
  } catch (error) {
    console.error("Reports cards API error:", error);
    return sendApiError(res, 500, "Failed to load reports");
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

    const days = Number(req.query.days) === 7 ? 7 : 1;
    const monthNav = getAttendanceMonthNavigation(req.query.month);

    const data = await getEmployeeAttendanceOverview(userId, DASHBOARD_ORG_ID, {
      days,
      monthNav,
    });

    return res.status(200).send(
      renderEmployeeAttendancePage({
        ...data,
        selectedDays: days,
        monthNav,
      }),
    );
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
        ${renderTopNav("attendance")}
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

app.get(
  "/api/attendance/:userId/red-reports",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const userId = Number(req.params.userId);
      if (!userId) {
        return sendApiError(res, 400, "Invalid user id");
      }

      const monthNav = getAttendanceMonthNavigation(req.query.month);
      const todayAttendanceDate = getAttendanceDayDateStringFromDate(
        new Date(),
      );

      const startDate = monthNav.startDate;

      // For current month, only check until today.
      // For previous months, check whole month.
      const endDateExclusive =
        monthNav.selectedMonth === monthNav.currentMonth
          ? addDaysToDateString(todayAttendanceDate, 1)
          : monthNav.endDateExclusive;

      const redReportDates = await getMissingReportDatesForUserInRange({
        orgId: DASHBOARD_ORG_ID,
        userId,
        startDate,
        endDateExclusive,
      });

      return sendApiSuccess(res, {
        redReportDays: redReportDates.length,
        redReportDates,
        redReportDatesText: formatDateListForHumans(redReportDates),
      });
    } catch (error) {
      console.error("Employee red reports API error:", error);
      return res.status(500).json({
        ok: false,
        error: error?.message || String(error),
        stack: error?.stack || null,
      });
    }
  },
);

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

function formatTopPeople(items, formatter) {
  return (items || []).slice(0, 3).map(formatter);
}

function rankRowsByNumber(rows, valueKey) {
  return [...(rows || [])].sort(
    (a, b) => Number(b[valueKey] || 0) - Number(a[valueKey] || 0),
  );
}

function getWeekDateRangeForAttendance(timeZone = APP_TIMEZONE) {
  const nowAttendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const current = new Date(
    `${nowAttendanceDate}T00:00:00${APP_TIMEZONE_OFFSET}`,
  );
  const day = current.getUTCDay(); // 0=Sun
  const diffToMonday = day === 0 ? 6 : day - 1;

  const start = new Date(current);
  start.setUTCDate(start.getUTCDate() - diffToMonday);

  const startDate = start.toISOString().slice(0, 10);
  const endDateExclusive = addDaysToDateString(nowAttendanceDate, 1);

  return { startDate, endDateExclusive };
}

async function getAttendanceInsightsForRange(
  orgId,
  startDate,
  endDateExclusive,
) {
  const { data: users, error: usersError } = await supabase
    .from("users")
    .select("id, name, role, attendance_start_date")
    .eq("org_id", orgId)
    .eq("is_active", true);

  if (usersError) throw usersError;

  const attendanceDates = [];
  let cursor = startDate;
  while (cursor < endDateExclusive) {
    attendanceDates.push(cursor);
    cursor = addDaysToDateString(cursor, 1);
  }

  const perUser = new Map();

  for (const user of users || []) {
    perUser.set(user.id, {
      user_id: user.id,
      name: user.name,
      role: user.role,
      late_count: 0,
      no_prior_info_count: 0,
      approved_late_count: 0,
      leave_count: 0,
      no_update_count: 0,
      total_break_min: 0,
      total_worked_min: 0,
      present_days: 0,
      on_time_days: 0,
      streak_on_time: 0,
      best_on_time_streak: 0,

      // new
      careless_login_days: 0,
      careless_login_min: 0,
      careless_login_examples: [],
    });
  }

  for (const date of attendanceDates) {
    const events = await getAttendanceEventsForAttendanceDay(date, orgId);
    const lateRows = await getLateArrivalRowsForDate(date, orgId);
    const plannedOffRows = await getPlannedOffRowsForDate(date, orgId);

    const plannedOffUserIds = new Set(
      (plannedOffRows || []).map((x) => x.user_id),
    );
    const lateByUser = new Map((lateRows || []).map((x) => [x.user_id, x]));
    const eventsByUser = new Map();

    for (const ev of events || []) {
      if (!eventsByUser.has(ev.user_id)) eventsByUser.set(ev.user_id, []);
      eventsByUser.get(ev.user_id).push(ev);
    }

    for (const user of users || []) {
      const agg = perUser.get(user.id);

      const attendanceStartDate = user.attendance_start_date || null;
      if (attendanceStartDate && date < attendanceStartDate) {
        continue;
      }

      const userEvents = eventsByUser.get(user.id) || [];
      const shiftStartIso = await getShiftStartIsoForUserToday(user.id, orgId);
      const daySummary = getAttendanceSummaryFromEvents(userEvents, {
        shiftStartIso,
      });
      const firstLogin = daySummary.firstLogin;
      const lateInfo = lateByUser.get(user.id) || null;
      const isLeave = plannedOffUserIds.has(user.id);

      const lateStatus = lateInfo
        ? lateInfo.is_approved
          ? "Approved"
          : "Not approved"
        : firstLogin
          ? daySummary.lateMinutes > 10
            ? "No prior info"
            : "No"
          : "-";

      if (isLeave) {
        agg.leave_count += 1;
        agg.streak_on_time = 0;
        continue;
      }

      if (!firstLogin && userEvents.length === 0) {
        agg.no_update_count += 1;
        agg.streak_on_time = 0;
        continue;
      }

      agg.present_days += 1;
      agg.total_break_min += daySummary.breakMinutes || 0;
      agg.total_worked_min += daySummary.workedMinutes || 0;

      const workedMinutes = daySummary.workedMinutes || 0;
      if (user.role !== "admin" && workedMinutes > LONG_SHIFT_THRESHOLD_MIN) {
        agg.careless_login_days += 1;
        agg.careless_login_min += workedMinutes;

        if (agg.careless_login_examples.length < 3) {
          agg.careless_login_examples.push(
            `${date} — ${formatDurationMinutes(workedMinutes)} worked`,
          );
        }
      }

      if (lateStatus === "Approved") agg.approved_late_count += 1;
      if (lateStatus === "Not approved") agg.late_count += 1;
      if (lateStatus === "No prior info") {
        agg.late_count += 1;
        agg.no_prior_info_count += 1;
      }

      if (lateStatus === "No") {
        agg.on_time_days += 1;
        agg.streak_on_time += 1;
        if (agg.streak_on_time > agg.best_on_time_streak) {
          agg.best_on_time_streak = agg.streak_on_time;
        }
      } else {
        agg.streak_on_time = 0;
      }
    }
  }

  return Array.from(perUser.values());
}

function buildWeeklyInsightsFromAgg(aggRows) {
  const mostLate = [...aggRows]
    .filter((x) => x.late_count > 0)
    .sort((a, b) => b.late_count - a.late_count);

  const bestStreak = [...aggRows]
    .filter((x) => x.best_on_time_streak > 0)
    .sort((a, b) => b.best_on_time_streak - a.best_on_time_streak);

  const mostBreak = [...aggRows]
    .filter((x) => x.total_break_min > 0)
    .sort((a, b) => b.total_break_min - a.total_break_min);

  const highestWork = [...aggRows]
    .filter((x) => x.total_worked_min > 0)
    .sort((a, b) => b.total_worked_min - a.total_worked_min);

  const carelessLogin = [...aggRows]
    .filter((x) => Number(x.careless_login_days || 0) > 0)
    .sort((a, b) => {
      if (
        Number(b.careless_login_days || 0) !==
        Number(a.careless_login_days || 0)
      ) {
        return (
          Number(b.careless_login_days || 0) -
          Number(a.careless_login_days || 0)
        );
      }
      return (
        Number(b.careless_login_min || 0) - Number(a.careless_login_min || 0)
      );
    });

  return {
    most_late_count_text: mostLate[0] ? String(mostLate[0].late_count) : "-",
    most_late_lines: formatTopPeople(
      mostLate,
      (x) => `${x.name} — ${x.late_count} late login(s)`,
    ),
    careless_login_text: carelessLogin[0]
      ? `${carelessLogin[0].careless_login_days} day(s)`
      : "-",

    careless_login_lines: formatTopPeople(
      carelessLogin,
      (x) =>
        `${x.name} — ${x.careless_login_days} day(s) above 10h${
          x.careless_login_examples?.length
            ? ` | ${x.careless_login_examples[0]}`
            : ""
        }`,
    ),

    best_streak_text: bestStreak[0]
      ? `${bestStreak[0].best_on_time_streak} days`
      : "-",
    best_streak_lines: formatTopPeople(
      bestStreak,
      (x) => `${x.name} — ${x.best_on_time_streak} on-time day streak`,
    ),

    most_break_time_text: mostBreak[0]
      ? formatDurationMinutes(mostBreak[0].total_break_min)
      : "-",
    most_break_time_lines: formatTopPeople(
      mostBreak,
      (x) => `${x.name} — ${formatDurationMinutes(x.total_break_min)} break`,
    ),

    highest_work_hours_text: highestWork[0]
      ? formatDurationMinutes(highestWork[0].total_worked_min)
      : "-",
    highest_work_hours_lines: formatTopPeople(
      highestWork,
      (x) => `${x.name} — ${formatDurationMinutes(x.total_worked_min)} worked`,
    ),
  };
}

function buildMonthlyInsightsFromAgg(aggRows) {
  const leaders = [...aggRows]
    .filter((x) => x.present_days > 0)
    .map((x) => {
      const score =
        x.on_time_days * 3 +
        x.present_days * 1 -
        x.late_count * 2 -
        x.no_update_count * 3;
      return { ...x, attendance_score: score };
    })
    .sort((a, b) => b.attendance_score - a.attendance_score);

  const needsAttention = [...aggRows]
    .map((x) => {
      const risk =
        x.late_count * 2 +
        x.no_prior_info_count * 3 +
        x.no_update_count * 3 +
        x.leave_count * 1;
      return { ...x, attendance_risk: risk };
    })
    .filter((x) => x.attendance_risk > 0)
    .sort((a, b) => b.attendance_risk - a.attendance_risk);

  const carelessLogin = [...aggRows]
    .filter((x) => Number(x.careless_login_days || 0) > 0)
    .sort((a, b) => {
      if (
        Number(b.careless_login_days || 0) !==
        Number(a.careless_login_days || 0)
      ) {
        return (
          Number(b.careless_login_days || 0) -
          Number(a.careless_login_days || 0)
        );
      }
      return (
        Number(b.careless_login_min || 0) - Number(a.careless_login_min || 0)
      );
    });

  const mostLate = [...aggRows]
    .filter((x) => x.late_count > 0)
    .sort((a, b) => b.late_count - a.late_count);

  const mostLeave = [...aggRows]
    .filter((x) => x.leave_count > 0)
    .sort((a, b) => b.leave_count - a.leave_count);

  return {
    attendance_leaders_text: leaders[0]
      ? String(leaders[0].attendance_score)
      : "-",
    attendance_leader_lines: formatTopPeople(
      leaders,
      (x) => `${x.name} — score ${x.attendance_score}`,
    ),

    needs_attention_text: needsAttention[0]
      ? String(needsAttention[0].attendance_risk)
      : "-",
    needs_attention_lines: formatTopPeople(
      needsAttention,
      (x) => `${x.name} — risk ${x.attendance_risk}`,
    ),

    most_late_text: mostLate[0] ? String(mostLate[0].late_count) : "-",
    most_late_lines: formatTopPeople(
      mostLate,
      (x) => `${x.name} — ${x.late_count} late login(s)`,
    ),
    careless_login_text: carelessLogin[0]
      ? `${carelessLogin[0].careless_login_days} day(s)`
      : "-",

    careless_login_lines: formatTopPeople(
      carelessLogin,
      (x) => `${x.name} — ${x.careless_login_days} day(s) above 10h`,
    ),

    most_leave_text: mostLeave[0] ? String(mostLeave[0].leave_count) : "-",
    most_leave_lines: formatTopPeople(
      mostLeave,
      (x) => `${x.name} — ${x.leave_count} leave day(s)`,
    ),
  };
}

async function getAttendancePageData(orgId) {
  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const { startUtc, endUtc } = getCurrentAttendanceDayRange();

  const [
    { data: users, error: usersError },
    { data: events, error: eventsError },
    plannedOffRows,
    lateRows,
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
      .order("created_at", { ascending: true }),

    getPlannedOffRowsForDate(attendanceDate, orgId),
    getLateArrivalRowsForDate(attendanceDate, orgId),
  ]);

  if (usersError) throw usersError;
  if (eventsError) throw eventsError;

  const plannedOff = plannedOffRows || [];
  const plannedOffUserIds = new Set(plannedOff.map((x) => x.user_id));
  const lateByUser = new Map((lateRows || []).map((x) => [x.user_id, x]));

  const eventsByUser = new Map();
  for (const ev of events || []) {
    if (!eventsByUser.has(ev.user_id)) {
      eventsByUser.set(ev.user_id, []);
    }
    eventsByUser.get(ev.user_id).push(ev);
  }

  const rows = await Promise.all(
    (users || []).map(async (user) => {
      const userEvents = eventsByUser.get(user.id) || [];
      const latest = userEvents[userEvents.length - 1] || null;
      const shiftStartIso = await getShiftStartIsoForUserToday(user.id, orgId);
      const summary = getAttendanceSummaryFromEvents(userEvents, {
        shiftStartIso,
      });
      const firstLogin = summary.firstLogin;
      const lateInfo = lateByUser.get(user.id) || null;

      let status = "no_update";
      if (plannedOffUserIds.has(user.id)) status = "leave";
      else if (latest?.action) status = latest.action;

      const flags = [];
      if (user.role !== "admin" && summary.longShiftFlag)
        flags.push("Long shift");
      if (user.role !== "admin" && summary.longBreakFlag)
        flags.push("Long break");
      if (lateInfo && !lateInfo.is_approved) flags.push("Late not approved");
      if (lateInfo && String(lateInfo.note || "").includes("TIME_UNSURE")) {
        flags.push("Time unsure");
      }

      return {
        user_id: user.id,
        name: user.name,
        role: user.role,
        status,
        since: latest?.created_at || null,
        since_text: latest?.created_at
          ? formatTimeOnly(latest.created_at)
          : plannedOffUserIds.has(user.id)
            ? "On leave today"
            : "-",
        worked_today_min: summary.workedMinutes || 0,
        worked_today_text: formatDurationMinutes(summary.workedMinutes || 0),
        break_today_min: summary.breakMinutes || 0,
        break_today_text: formatDurationMinutes(summary.breakMinutes || 0),
        first_login_at: firstLogin?.created_at || null,
        first_login_text: firstLogin?.created_at
          ? formatTimeOnly(firstLogin.created_at)
          : "-",
        late_status: lateInfo
          ? lateInfo.is_approved
            ? "Approved"
            : "Not approved"
          : firstLogin
            ? summary.lateMinutes > 10
              ? "No prior info"
              : "No"
            : "-",
        is_on_leave: plannedOffUserIds.has(user.id),
        flags,
        late_expected_login_text: lateInfo?.expected_login_at
          ? formatTimeOnly(lateInfo.expected_login_at)
          : String(lateInfo?.note || "").includes("TIME_UNSURE")
            ? "Time unsure"
            : "-",
        expected_shift_start_text: shiftStartIso
          ? formatTimeOnly(shiftStartIso)
          : "-",
      };
    }),
  );

  const summary = {
    logged_in_now: rows.filter(
      (x) => x.status === "login" || x.status === "back",
    ).length,
    on_break_now: rows.filter((x) => x.status === "break").length,
    not_logged_in_yet: rows.filter(
      (x) => x.role !== "admin" && x.status === "no_update",
    ).length,
    on_leave_today: rows.filter((x) => x.status === "leave").length,
    late_today: rows.filter(
      (x) =>
        x.late_status === "Approved" ||
        x.late_status === "Not approved" ||
        x.late_status === "No prior info",
    ).length,
    approved_late: rows.filter((x) => x.late_status === "Approved").length,
    unapproved_late: rows.filter((x) => x.late_status === "Not approved")
      .length,
    no_prior_info_late: rows.filter((x) => x.late_status === "No prior info")
      .length,
    long_break_flags: rows.filter((x) => x.flags.includes("Long break")).length,
    llong_shift_flags: rows.filter(
      (x) => x.role !== "admin" && x.flags.includes("Long shift"),
    ).length,
  };

  const groups = {
    on_break_now: rows.filter((x) => x.status === "break"),
    on_leave_today: rows.filter((x) => x.status === "leave"),
    expected_late: rows.filter(
      (x) => x.late_status === "Approved" || x.late_status === "Not approved",
    ),
    no_update_yet: rows.filter(
      (x) => x.role !== "admin" && x.status === "no_update",
    ),
    exceptions: rows.filter(
      (x) =>
        x.flags.length > 0 ||
        x.late_status === "Not approved" ||
        x.late_status === "No prior info",
    ),
  };
  return {
    attendance_date: attendanceDate,
    summary,
    rows,
    groups,
  };
}

async function getWorkProfilesByUser(orgId) {
  const { data, error } = await supabase
    .from("user_work_profiles")
    .select(
      "user_id, shift_start_time, employment_type, shift_end_time, working_hours",
    );

  if (error) throw error;

  const map = new Map();
  for (const row of data || []) {
    map.set(row.user_id, row);
  }
  return map;
}

async function getTasksPageData(filters = {}, orgId) {
  const search = String(filters.search || "").trim();
  const assignee = String(filters.assignee || "").trim();
  const waitingOn = String(filters.waitingOn || "").trim();
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
  const progressBuckets = Array.isArray(filters.progressBucket)
    ? filters.progressBucket
    : filters.progressBucket
      ? [filters.progressBucket]
      : ["not_begun", "zero_to_fifty", "fifty_to_hundred"];

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
  blocker_note,
  waiting_on_user_id
  `,
    )
    .eq("org_id", orgId)
    .order("deadline", { ascending: true, nullsFirst: false });

  if (priority) query = query.eq("priority", priority);
  if (business) query = query.eq("business", business);
  if (area) query = query.eq("area", area);
  if (waitingOn) {
    query = query.eq("waiting_on_user_id", Number(waitingOn));
  }

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
      owners,
      owner_names: owners.map((x) => x.name).filter(Boolean),
      assignee_name: owners
        .map((x) => x.name)
        .filter(Boolean)
        .join(", "),
    };
  });

  if (waitingOn) {
    rows = rows.filter(
      (task) => String(task.waiting_on_user_id || "") === String(waitingOn),
    );
  }

  if (assignee) {
    rows = rows.filter((task) =>
      (ownersByTaskId[task.id] || []).some(
        (owner) => String(owner.user_id) === assignee,
      ),
    );
  }

  if (progressBuckets.length) {
    const hideCancelled = progressBuckets.includes("hide_cancelled");
    const onlyCancelled = progressBuckets.includes("only_cancelled");

    rows = rows.filter((task) => {
      const progress = Number(task.progress ?? 0);
      const status = String(task.status || "").toLowerCase();

      if (onlyCancelled) {
        return status === "cancelled";
      }

      if (hideCancelled && status === "cancelled") {
        return false;
      }

      return progressBuckets.some((bucket) => {
        if (bucket === "not_begun")
          return progress === 0 && status !== "cancelled";
        if (bucket === "zero_to_fifty")
          return progress > 0 && progress < 50 && status !== "cancelled";
        if (bucket === "fifty_to_hundred")
          return progress >= 50 && progress < 100 && status !== "cancelled";
        if (bucket === "complete")
          return progress === 100 && status !== "cancelled";
        return false;
      });
    });
  }

  return rows;
}

async function getUserTaskWorkspaceData({ userId, orgId, tab = "pending" }) {
  const { data: user, error: userError } = await supabase
    .from("users")
    .select("id, name, role, is_active")
    .eq("org_id", orgId)
    .eq("id", userId)
    .maybeSingle();

  if (userError) {
    console.error("getUserTaskWorkspaceData user error:", userError);
    throw userError;
  }

  if (!user) {
    return null;
  }

  const { data: ownerRows, error: ownerError } = await supabase
    .from("task_owners")
    .select("task_id")
    .eq("org_id", orgId)
    .eq("user_id", userId);

  if (ownerError) {
    console.error("getUserTaskWorkspaceData owner rows error:", ownerError);
    throw ownerError;
  }

  const taskIds = (ownerRows || []).map((x) => x.task_id);

  let tasks = [];
  if (taskIds.length) {
    const { data: taskRows, error: taskError } = await supabase
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
        blocker_note,
        waiting_on_user_id,
        updated_at
      `,
      )
      .eq("org_id", orgId)
      .in("id", taskIds)
      .order("deadline", { ascending: true, nullsFirst: false });

    if (taskError) {
      console.error("getUserTaskWorkspaceData tasks error:", taskError);
      throw taskError;
    }

    tasks = taskRows || [];
  }

  const { data: allOwnerRows, error: allOwnerError } = taskIds.length
    ? await supabase
        .from("task_owners")
        .select(
          `
          task_id,
          user_id,
          users!task_owners_user_id_fkey(id, name)
        `,
        )
        .eq("org_id", orgId)
        .in("task_id", taskIds)
    : { data: [], error: null };

  if (allOwnerError) {
    console.error("getUserTaskWorkspaceData all owners error:", allOwnerError);
    throw allOwnerError;
  }

  const ownersByTaskId = {};
  for (const row of allOwnerRows || []) {
    if (!ownersByTaskId[row.task_id]) ownersByTaskId[row.task_id] = [];
    ownersByTaskId[row.task_id].push({
      user_id: row.user_id,
      name: row.users?.name || "",
    });
  }

  const { data: historyRows, error: historyError } = taskIds.length
    ? await supabase
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
          created_at,
          changer:users!task_history_changed_by_user_id_fkey(name)
        `,
        )
        .eq("org_id", orgId)
        .in("task_id", taskIds)
        .order("created_at", { ascending: false })
    : { data: [], error: null };

  if (historyError) {
    console.error("getUserTaskWorkspaceData history error:", historyError);
    throw historyError;
  }

  const historyByTaskId = {};
  for (const row of historyRows || []) {
    if (!historyByTaskId[row.task_id]) historyByTaskId[row.task_id] = [];
    historyByTaskId[row.task_id].push({
      id: row.id,
      task_id: row.task_id,
      change_type: row.change_type,
      field_name: row.field_name,
      old_value: row.old_value || {},
      new_value: row.new_value || {},
      created_at: row.created_at,
      changed_by_name: row.changer?.name || "",
    });
  }

  const enrichedTasks = tasks.map((task) => {
    const owners = ownersByTaskId[task.id] || [];
    const history = historyByTaskId[task.id] || [];
    const latestHistory = history[0] || null;

    return {
      ...task,
      owners,
      owner_names: owners.map((x) => x.name).filter(Boolean),
      assignee_name: owners
        .map((x) => x.name)
        .filter(Boolean)
        .join(", "),
      latest_update_text: latestHistory
        ? renderUserWorkspaceHistoryLine(latestHistory)
        : "No updates yet",
      latest_update_at: latestHistory?.created_at || null,
      latest_updated_by: latestHistory?.changed_by_name || "",
      mini_history: history.slice(0, 3),
    };
  });

  const { data: blockedOnMeRows, error: blockedOnMeError } = await supabase
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
      blocked_reason,
      business,
      area,
      assigned_to_user_id,
      waiting_on_user_id,
      waiting_since,
      created_by_user_id,
      last_updated_by_user_id
    `,
    )
    .eq("org_id", orgId)
    .eq("waiting_on_user_id", userId)
    .eq("status", "blocked")
    .order("updated_at", { ascending: false });

  if (blockedOnMeError) {
    console.error(
      "getUserTaskWorkspaceData blockedOnMe error:",
      blockedOnMeError,
    );
    throw blockedOnMeError;
  }

  const blockedOnMeTaskIds = [
    ...new Set((blockedOnMeRows || []).map((task) => task.id).filter(Boolean)),
  ];

  const { data: blockedOnMeOwnerRows, error: blockedOnMeOwnerError } =
    blockedOnMeTaskIds.length
      ? await supabase
          .from("task_owners")
          .select(
            `
          task_id,
          user_id,
          users!task_owners_user_id_fkey(id, name)
        `,
          )
          .eq("org_id", orgId)
          .in("task_id", blockedOnMeTaskIds)
      : { data: [], error: null };

  if (blockedOnMeOwnerError) {
    console.error(
      "getUserTaskWorkspaceData blockedOnMe owners error:",
      blockedOnMeOwnerError,
    );
    throw blockedOnMeOwnerError;
  }

  const blockedOnMeOwnersByTaskId = new Map();

  for (const row of blockedOnMeOwnerRows || []) {
    const taskId = row.task_id;
    if (!blockedOnMeOwnersByTaskId.has(taskId)) {
      blockedOnMeOwnersByTaskId.set(taskId, []);
    }
    blockedOnMeOwnersByTaskId.get(taskId).push({
      user_id: row.user_id,
      name: row.users?.name || "",
    });
  }

  const { data: blockedOnMeHistoryRows, error: blockedOnMeHistoryError } =
    blockedOnMeTaskIds.length
      ? await supabase
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
          created_at,
          changer:users!task_history_changed_by_user_id_fkey(name)
        `,
          )
          .eq("org_id", orgId)
          .in("task_id", blockedOnMeTaskIds)
          .order("created_at", { ascending: false })
      : { data: [], error: null };

  if (blockedOnMeHistoryError) {
    console.error(
      "getUserTaskWorkspaceData blockedOnMe history error:",
      blockedOnMeHistoryError,
    );
    throw blockedOnMeHistoryError;
  }

  const blockedOnMeHistoryByTaskId = new Map();

  for (const row of blockedOnMeHistoryRows || []) {
    if (!blockedOnMeHistoryByTaskId.has(row.task_id)) {
      blockedOnMeHistoryByTaskId.set(row.task_id, []);
    }
    blockedOnMeHistoryByTaskId.get(row.task_id).push({
      ...row,
      changed_by_name: row.changer?.name || "",
    });
  }

  const blockedOnMeTasks = (blockedOnMeRows || []).map((task) => {
    const owners = blockedOnMeOwnersByTaskId.get(task.id) || [];
    const taskHistory = blockedOnMeHistoryByTaskId.get(task.id) || [];
    const latestHistory = taskHistory[0] || null;

    return {
      ...task,
      owner_names: owners.map((owner) => owner.name).filter(Boolean),
      latest_update_text: latestHistory
        ? renderUserWorkspaceHistoryLine(latestHistory)
        : "No updates yet",
      latest_updated_by: latestHistory?.changed_by_name || "",
      latest_update_at: latestHistory?.created_at || null,
      mini_history: taskHistory.slice(0, 3),
    };
  });

  const blockedOnMeUniqueTasks = blockedOnMeTasks.filter(
    (task, index, arr) => arr.findIndex((x) => x.id === task.id) === index,
  );

  const todayStart = new Date();
  todayStart.setHours(0, 0, 0, 0);
  const todayIso = todayStart.toISOString();

  const tomorrowStart = new Date(todayStart);
  tomorrowStart.setDate(tomorrowStart.getDate() + 1);
  const tomorrowIso = tomorrowStart.toISOString();

  const pendingTasks = enrichedTasks.filter(
    (task) =>
      !["done", "cancelled", "archived", "blocked"].includes(
        String(task.status || "").toLowerCase(),
      ),
  );

  const blockedTasks = enrichedTasks.filter(
    (task) => String(task.status || "").toLowerCase() === "blocked",
  );

  const deletedTasks = enrichedTasks.filter(
    (task) => String(task.status || "").toLowerCase() === "cancelled",
  );

  const doneTodayTaskIds = new Set(
    (historyRows || [])
      .filter((row) => {
        const newStatus = row?.new_value?.status || row?.new_value?.["status"];
        return (
          row.change_type === "status_change" &&
          String(newStatus || "").toLowerCase() === "done" &&
          row.created_at >= todayIso &&
          row.created_at < tomorrowIso
        );
      })
      .map((row) => row.task_id),
  );

  const doneTodayTasks = enrichedTasks.filter((task) =>
    doneTodayTaskIds.has(task.id),
  );

  const taskMap = new Map(enrichedTasks.map((task) => [task.id, task]));

  const progressUpdates = (historyRows || [])
    .filter((row) =>
      ["progress_change", "status_change", "edit"].includes(row.change_type),
    )
    .filter((row) => {
      const task = taskMap.get(row.task_id);
      return !!task;
    })
    .map((row) => {
      const task = taskMap.get(row.task_id);

      return {
        id: row.id,
        task_id: row.task_id,
        task_no: task?.task_no || row.task_id,
        title: task?.title || "",
        change_type: row.change_type,
        field_name: row.field_name,
        old_value: row.old_value || {},
        new_value: row.new_value || {},
        created_at: row.created_at,
        changed_by_name: row.changer?.name || "",
      };
    });

  const tabs = {
    pending: pendingTasks,
    blocked: blockedTasks,
    blocked_on_me: blockedOnMeUniqueTasks,
    done_today: doneTodayTasks,
    deleted: deletedTasks,
    progress_updates: progressUpdates,
  };

  return {
    user,
    selectedTab: tab,
    counts: {
      pending: pendingTasks.length,
      blocked: blockedTasks.length,
      blocked_on_me: blockedOnMeUniqueTasks.length,
      done_today: doneTodayTasks.length,
      deleted: deletedTasks.length,
      progress_updates: progressUpdates.length,
    },
    tabs,
  };
}

function renderLeadImportLogsPage({
  business,
  logs = [],
  rows = [],
  selectedImportId = null,
  search = "",
  date = "",
  status = "",
  uploadedBy = "",
}) {
  const logRowsHtml = logs.length
    ? logs
        .map(
          (log) => `
          <tr>
            <td>
              <a href="/leads/${business}/imports?import_id=${log.id}&search=${encodeURIComponent(search)}&date=${encodeURIComponent(date)}&status=${encodeURIComponent(status)}&uploaded_by=${encodeURIComponent(uploadedBy)}">
                #${escapeHtml(log.id)}
              </a>
              <div class="muted">${escapeHtml(log.file_name || "-")}</div>
            </td>
            <td>${escapeHtml(log.created_at ? formatDateTime(log.created_at) : "-")}</td>
            <td>${escapeHtml(log.uploaded_by_name || "-")}</td>
            <td><span class="badge badge-info">${escapeHtml(log.status || "-")}</span></td>
            <td>${escapeHtml(log.total_rows || 0)}</td>
            <td>${escapeHtml(log.inserted_count || 0)}</td>
            <td>${escapeHtml(log.duplicate_count || 0)}</td>
            <td>${escapeHtml(log.skipped_count || 0)}</td>
            <td>${escapeHtml(log.error_count || 0)}</td>
          </tr>
        `,
        )
        .join("")
    : `<tr><td colspan="9" class="empty-cell">No import logs found.</td></tr>`;

  const detailRowsHtml = rows.length
    ? rows
        .map(
          (r) => `
          <tr>
            <td>${escapeHtml(r.row_number || "-")}</td>
            <td><span class="badge ${r.status === "success" ? "badge-ok" : r.status === "duplicate" ? "badge-warn" : r.status === "error" ? "badge-danger" : "badge-muted"}">${escapeHtml(r.status)}</span></td>
            <td>${escapeHtml(r.phone || "-")}</td>
            <td>${escapeHtml(r.company || "-")}</td>
            <td>${escapeHtml(r.website || "-")}</td>
            <td>${escapeHtml(r.message || "-")}</td>
            <td>${escapeHtml(r.existing_lead_id || "-")}</td>
          </tr>
        `,
        )
        .join("")
    : `<tr><td colspan="7" class="empty-cell">Select an import to view row details.</td></tr>`;

  return `
    <html>
      <head>
        <title>Lead Import Logs</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}
          ${buildTopNavCss()}

          .wrap { max-width: 1600px; margin: 0 auto; padding: 24px 18px 36px; }
          .topbar, .panel {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
          }
          .topbar {
            display:flex; justify-content:space-between; align-items:center;
            gap:16px; flex-wrap:wrap; margin-bottom:20px; padding:18px 20px;
          }
          h1 { margin:0; font-size:30px; letter-spacing:-0.04em; }
          .subtitle { color:var(--muted); margin-top:8px; font-size:14px; }
          .panel { padding:16px; margin-bottom:16px; overflow-x:auto; }
          .search-row { display:flex; gap:10px; flex-wrap:wrap; align-items:center; }
          input, select {
            padding:11px 12px; border-radius:12px; border:1px solid var(--line);
            background:rgba(255,255,255,0.04); color:var(--text); font:inherit;
          }
          .btn {
            display:inline-flex; align-items:center; text-decoration:none; color:var(--text);
            padding:10px 13px; border-radius:12px; background:rgba(255,255,255,0.05);
            border:1px solid rgba(255,255,255,0.12); font-weight:800; cursor:pointer;
          }
          .btn-primary {
            background:var(--primary-soft); color:var(--text-strong);
            border-color:color-mix(in srgb, var(--primary) 55%, transparent);
          }
          table { width:100%; border-collapse:collapse; }
          th, td {
            padding:12px; border-bottom:1px solid rgba(255,255,255,0.08);
            text-align:left; vertical-align:top; font-size:13px;
          }
          th {
            color:var(--muted); text-transform:uppercase; letter-spacing:0.08em;
            font-size:11px;
          }
          .badge {
            display:inline-flex; padding:6px 9px; border-radius:999px;
            font-size:12px; font-weight:800;
          }
          .badge-ok { background:var(--success-soft); }
          .badge-info { background:var(--info-soft); }
          .badge-warn { background:var(--accent-soft); }
          .badge-danger { background:var(--danger-soft); }
          .badge-muted { background:rgba(255,255,255,0.08); }
        </style>
      </head>
      <body>
        ${renderTopNav("leads")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <h1>${escapeHtml(business)} Import Logs</h1>
              <div class="subtitle">Excel upload history, duplicates skipped, errors, day-wise search, and uploader tracking.</div>
            </div>
            <a class="btn" href="/leads/${encodeURIComponent(business)}">← Back to Leads</a>
          </div>

          <div class="panel">
            <form class="search-row" method="GET" action="/leads/${encodeURIComponent(business)}/imports">
              <input name="search" value="${escapeHtml(search)}" placeholder="Search file, uploader, phone, company..." />
              <input type="date" name="date" value="${escapeHtml(date)}" />
              <input name="uploaded_by" value="${escapeHtml(uploadedBy)}" placeholder="Uploaded by..." />
              <select name="status">
                <option value="">All statuses</option>
                <option value="completed" ${status === "completed" ? "selected" : ""}>Completed</option>
                <option value="processing" ${status === "processing" ? "selected" : ""}>Processing</option>
                <option value="failed" ${status === "failed" ? "selected" : ""}>Failed</option>
              </select>
              <button class="btn btn-primary" type="submit">Search</button>
              <a class="btn" href="/leads/${encodeURIComponent(business)}/imports">Clear</a>
            </form>
          </div>

          <div class="panel">
            <h2>Uploads</h2>
            <table>
              <thead>
                <tr>
                  <th>Import</th>
                  <th>Uploaded At</th>
                  <th>Uploaded By</th>
                  <th>Status</th>
                  <th>Total</th>
                  <th>Inserted</th>
                  <th>Duplicates</th>
                  <th>Skipped</th>
                  <th>Errors</th>
                </tr>
              </thead>
              <tbody>${logRowsHtml}</tbody>
            </table>
          </div>

          <div class="panel">
            <h2>Selected Import Details ${selectedImportId ? `#${escapeHtml(selectedImportId)}` : ""}</h2>
            <table>
              <thead>
                <tr>
                  <th>Excel Row</th>
                  <th>Status</th>
                  <th>Phone</th>
                  <th>Company</th>
                  <th>Website</th>
                  <th>Message</th>
                  <th>Lead ID</th>
                </tr>
              </thead>
              <tbody>${detailRowsHtml}</tbody>
            </table>
          </div>
        </div>
      </body>
    </html>
  `;
}

function renderMyDashboardPage(data) {
  const user = data?.user || {};
  const taskData = data?.taskData || {};
  const myAttendance = data?.myAttendance || null;
  const reportData = data?.reportData || {};

  const counts = taskData?.counts || {};
  const pendingTasks = taskData?.tabs?.pending || [];
  const blockedTasks = taskData?.tabs?.blocked || [];
  const doneTodayTasks = taskData?.tabs?.done_today || [];

  const reportSummary =
    reportData?.summary_text ||
    reportData?.narrative ||
    "No report summary available for today.";

  return `
    <html>
      <head>
        <title>My Dashboard</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}
          ${buildTopNavCss()}

          .wrap {
            max-width: 1600px;
            margin: 0 auto;
            padding: 24px 18px 36px;
          }

          .topbar, .panel, .stat-card, .task-card {
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
          }

          .stat-value {
            margin-top: 10px;
            font-size: 28px;
            font-weight: 700;
          }

          .grid {
            display: grid;
            grid-template-columns: 1.2fr 1fr;
            gap: 16px;
          }

          .panel {
            padding: 16px;
          }

          .panel h2 {
            margin: 0 0 12px;
            font-size: 18px;
          }

          .task-list {
            display: grid;
            gap: 10px;
          }

          .task-card {
            padding: 12px;
          }

          .task-title {
            font-weight: 700;
            margin-bottom: 6px;
          }

          .task-meta {
            color: var(--muted);
            font-size: 13px;
            line-height: 1.6;
          }

          .attendance-line,
          .report-box {
            color: var(--text);
            line-height: 1.7;
            white-space: pre-wrap;
          }

          @media (max-width: 980px) {
            .stats {
              grid-template-columns: repeat(2, minmax(0, 1fr));
            }

            .grid {
              grid-template-columns: 1fr;
            }
          }
        </style>
      </head>
      <body>
        ${renderTopNav("dashboard")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Personal Workspace</div>
              <h1>Welcome, ${escapeHtml(user.name || "User")}</h1>
              <div class="subtitle">
                Your tasks, attendance, and report summary in one place
              </div>
            </div>
          </div>

          <div class="stats">
            <div class="stat-card">
              <div class="stat-label">Pending Tasks</div>
              <div class="stat-value">${escapeHtml(counts.pending || 0)}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Blocked Tasks</div>
              <div class="stat-value">${escapeHtml(counts.blocked || 0)}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Done Today</div>
              <div class="stat-value">${escapeHtml(counts.done_today || 0)}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Attendance Status</div>
              <div class="stat-value">${escapeHtml(myAttendance?.status || "-")}</div>
            </div>
          </div>

          <div class="grid">
            <div class="panel">
              <h2>My Tasks</h2>
              <div class="task-list">
                ${
                  pendingTasks.length
                    ? pendingTasks
                        .slice(0, 8)
                        .map(
                          (task) => `
                      <div class="task-card">
                        <div class="task-title">#${escapeHtml(task.task_no || task.id)} — ${escapeHtml(task.title || "")}</div>
                        <div class="task-meta">
                          Status: ${escapeHtml(task.status || "-")}<br />
                          Priority: ${escapeHtml(task.priority || "-")}<br />
                          Progress: ${escapeHtml(task.progress ?? 0)}%<br />
                          Deadline: ${escapeHtml(task.deadline || "-")}
                        </div>
                      </div>
                    `,
                        )
                        .join("")
                    : `<div class="muted">No pending tasks.</div>`
                }
              </div>
            </div>

            <div style="display:grid; gap:16px;">
              <div class="panel">
                <h2>My Attendance</h2>
                <div class="attendance-line">
Status: ${escapeHtml(myAttendance?.status || "-")}
Login: ${escapeHtml(myAttendance?.login_time || "-")}
Break: ${escapeHtml(myAttendance?.break_time || "-")}
Logout: ${escapeHtml(myAttendance?.logout_time || "-")}
Worked: ${escapeHtml(myAttendance?.worked_duration || "-")}
                </div>
              </div>

              <div class="panel">
                <h2>Today’s Report</h2>
                <div class="report-box">${escapeHtml(reportSummary)}</div>
              </div>

              <div class="panel">
                <h2>Quick Links</h2>
                <div class="attendance-line">
<a href="/tasks/user/${escapeHtml(user.id)}" style="color: var(--primary);">Open my full task workspace</a><br />
<a href="/attendance" style="color: var(--primary);">Open attendance page</a><br />
<a href="/reports?userId=${escapeHtml(user.id)}" style="color: var(--primary);">Open my reports</a>
                </div>
              </div>
            </div>
          </div>
        </div>
      </body>
    </html>
  `;
}

function renderUserWorkspaceHistoryLine(item) {
  const oldValue = item.old_value || {};
  const newValue = item.new_value || {};

  if (item.change_type === "progress_change") {
    const note = newValue.note || oldValue.note || "";
    return note
      ? `Progress: ${oldValue.progress ?? 0}% → ${newValue.progress ?? 0}% • ${note}`
      : `Progress: ${oldValue.progress ?? 0}% → ${newValue.progress ?? 0}%`;
  }

  if (item.change_type === "status_change") {
    const note = newValue.note || oldValue.note || "";
    return note
      ? `Status: ${oldValue.status || "-"} → ${newValue.status || "-"} • ${note}`
      : `Status: ${oldValue.status || "-"} → ${newValue.status || "-"}`;
  }

  if (item.change_type === "owner_change") {
    const oldOwners = Array.isArray(oldValue.owners)
      ? oldValue.owners.join(", ")
      : "-";
    const newOwners = Array.isArray(newValue.owners)
      ? newValue.owners.join(", ")
      : "-";
    return `Owners: ${oldOwners} → ${newOwners}`;
  }

  if (item.change_type === "deadline_change") {
    return `Deadline: ${oldValue.deadline || "-"} → ${newValue.deadline || "-"}`;
  }

  if (item.change_type === "edit") {
    if (item.field_name === "blocker_note") {
      return `Blocker updated: ${newValue.blocker_note || newValue.note || "-"}`;
    }

    if (item.field_name === "title") {
      return `Title: ${oldValue.title || "-"} → ${newValue.title || "-"}`;
    }

    if (item.field_name === "detail") {
      return `Detail updated`;
    }

    if (item.field_name === "priority") {
      return `Priority: ${oldValue.priority || "-"} → ${newValue.priority || "-"}`;
    }

    if (item.field_name === "business") {
      return `Business: ${oldValue.business || "-"} → ${newValue.business || "-"}`;
    }

    if (item.field_name === "area") {
      return `Area: ${oldValue.area || "-"} → ${newValue.area || "-"}`;
    }

    if (item.field_name === "deadline") {
      return `Deadline: ${oldValue.deadline || "-"} → ${newValue.deadline || "-"}`;
    }

    if (String(item.field_name || "").startsWith("clear_")) {
      return `${item.field_name.replace(/^clear_/, "").replace(/_/g, " ")} cleared`;
    }

    return `${item.field_name || "field"} updated`;
  }

  return item.change_type || "Updated";
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
        ${renderTopNav("dashboard")}
        <div class="box">
          <h1>WeSolveHR Server</h1>
          <p>Webhook + Dashboard is running.</p>
          <a href="/dashboard">Open Dashboard</a>
        </div>
      </body>
    </html>
  `);
});

app.get("/leads/:business/imports", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
    const business = getBusinessCanonicalName(req.params.business);

    const search = String(req.query.search || "").trim();
    const date = String(req.query.date || "").trim();
    const status = String(req.query.status || "").trim();
    const uploadedBy = String(req.query.uploaded_by || "").trim();
    const selectedImportId = req.query.import_id
      ? Number(req.query.import_id)
      : null;

    let query = supabase
      .from("lead_import_logs")
      .select("*")
      .eq("org_id", orgId)
      .eq("business", business)
      .order("created_at", { ascending: false })
      .limit(100);

    if (status) {
      query = query.eq("status", status);
    }

    if (uploadedBy) {
      query = query.ilike("uploaded_by_name", `%${uploadedBy}%`);
    }

    if (date) {
      const nextDate = new Date(`${date}T00:00:00+05:30`);
      nextDate.setDate(nextDate.getDate() + 1);

      query = query
        .gte("created_at", `${date}T00:00:00+05:30`)
        .lt("created_at", nextDate.toISOString());
    }

    if (search) {
      query = query.or(
        [
          `file_name.ilike.%${search}%`,
          `uploaded_by_name.ilike.%${search}%`,
        ].join(","),
      );
    }

    const { data: logs, error: logsError } = await query;
    if (logsError) throw logsError;

    let rows = [];

    if (selectedImportId) {
      let rowQuery = supabase
        .from("lead_import_log_rows")
        .select("*")
        .eq("org_id", orgId)
        .eq("business", business)
        .eq("import_id", selectedImportId)
        .order("row_number", { ascending: true })
        .limit(500);

      if (search) {
        rowQuery = rowQuery.or(
          [
            `phone.ilike.%${search}%`,
            `company.ilike.%${search}%`,
            `website.ilike.%${search}%`,
            `message.ilike.%${search}%`,
            `status.ilike.%${search}%`,
          ].join(","),
        );
      }

      const { data: rowData, error: rowError } = await rowQuery;
      if (rowError) throw rowError;
      rows = rowData || [];
    }

    return res.send(
      renderLeadImportLogsPage({
        business,
        logs: logs || [],
        rows,
        selectedImportId,
        search,
        date,
        status,
        uploadedBy,
      }),
    );
  } catch (error) {
    console.error("GET /leads/:business/imports error:", error);
    return res
      .status(500)
      .send("Failed to load import logs: " + (error.message || String(error)));
  }
});

app.get("/clients", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;

    const { data: clients, error } = await supabase
      .from("clients")
      .select(
        `
        id,
        name,
        company_name,
        slug,
        status,
        health_status,
        start_date,
        description,
        account_manager_user_id,
        project_manager_user_id,
        created_at,
        account_manager:users!clients_account_manager_user_id_fkey(name),
        project_manager:users!clients_project_manager_user_id_fkey(name)
      `,
      )
      .eq("org_id", orgId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("created_at", { ascending: false });

    if (error) {
      console.error("clients list error:", error);
      return res.status(500).send("Failed to load clients");
    }

    const clientIds = (clients || []).map((c) => c.id);

    let serviceRows = [];
    if (clientIds.length) {
      const { data: servicesData, error: servicesError } = await supabase
        .from("client_services")
        .select(
          `
          client_id,
          services(name)
        `,
        )
        .in("client_id", clientIds)
        .eq("is_active", true)
        .is("deleted_at", null);

      if (servicesError) {
        console.error("client services list error:", servicesError);
      } else {
        serviceRows = servicesData || [];
      }
    }

    const serviceMap = {};
    for (const row of serviceRows) {
      if (!serviceMap[row.client_id]) serviceMap[row.client_id] = [];
      if (row.services?.name) serviceMap[row.client_id].push(row.services.name);
    }

    const decoratedClients = (clients || []).map((client) => ({
      ...client,
      service_names: serviceMap[client.id] || [],
      account_manager_name: client.account_manager?.name || "",
      project_manager_name: client.project_manager?.name || "",
      open_work_count: 0,
      waiting_count: 0,
      last_update_text: "-",
    }));

    const summary = {
      total: decoratedClients.length,
      active: decoratedClients.filter((c) => c.status === "active").length,
      waiting: 0,
      atRisk: decoratedClients.filter((c) => c.health_status === "at_risk")
        .length,
    };

    res
      .type("html")
      .send(renderClientsListPage({ clients: decoratedClients, summary }));
  } catch (error) {
    console.error("GET /clients fatal error:", error);
    res.status(500).send("Failed to load clients");
  }
});

app.get(
  "/api/client-work-items/:id",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
      const id = Number(req.params.id);

      if (!id) {
        return sendApiError(res, 400, "Invalid work item id");
      }

      const { data, error } = await supabase
        .from("client_work_items")
        .select("*")
        .eq("org_id", orgId)
        .eq("id", id)
        .eq("is_active", true)
        .is("deleted_at", null)
        .maybeSingle();

      if (error) {
        console.error("get client work item error:", error);
        return sendApiError(res, 500, "Failed to load work item");
      }

      if (!data) {
        return sendApiError(res, 404, "Work item not found");
      }

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("GET /api/client-work-items/:id fatal error:", error);
      return sendApiError(res, 500, "Failed to load work item");
    }
  },
);

app.get("/clients/new", requireDashboardAuth, async (req, res) => {
  const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;

  const { data: users, error } = await supabase
    .from("users")
    .select("id, name")
    .eq("org_id", orgId)
    .eq("is_active", true)
    .order("name", { ascending: true });

  if (error) {
    console.error("clients new users error:", error);
  }

  res.type("html").send(renderNewClientPage({ users: users || [] }));
});

app.post("/api/client-work-items", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
    const actorUserId = req.loggedInUser?.id || null;
    const body = req.body || {};

    const clientId = Number(body.client_id);
    const title = String(body.title || "").trim();

    if (!clientId) {
      return sendApiError(res, 400, "Client is required");
    }

    if (!title) {
      return sendApiError(res, 400, "Title is required");
    }

    if (!["low", "medium", "high"].includes(body.priority || "medium")) {
      return sendApiError(res, 400, "Invalid priority");
    }

    const row = {
      org_id: orgId,
      client_id: clientId,
      title,
      description: body.description || null,
      owner_user_id: body.owner_user_id ? Number(body.owner_user_id) : null,
      dependency_work_item_id: body.dependency_work_item_id
        ? Number(body.dependency_work_item_id)
        : null,
      milestone_id: body.milestone_id ? Number(body.milestone_id) : null,
      priority: body.priority || "medium",
      status: "todo",
      due_date: body.due_date || null,
      is_active: true,
      created_by_user_id: actorUserId,
      last_updated_by_user_id: actorUserId,
      updated_at: new Date().toISOString(),
    };

    const { data, error } = await supabase
      .from("client_work_items")
      .insert([row])
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("create client work item error:", error);
      return sendApiError(res, 500, "Failed to create work item");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "work_item_created",
      entityType: "client_work_items",
      entityId: data.id,
      newValue: data,
    });

    await supabase
      .from("clients")
      .update({
        status: "active",
        updated_at: new Date().toISOString(),
        updated_by_user_id: actorUserId,
      })
      .eq("org_id", orgId)
      .eq("id", clientId);

    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("POST /api/client-work-items fatal error:", error);
    return sendApiError(res, 500, "Failed to create work item");
  }
});

app.post("/api/clients/:id/updates", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
    const actorUserId = req.loggedInUser?.id || null;
    const clientId = Number(req.params.id);
    const body = req.body || {};

    const updateText = String(body.update_text || "").trim();

    if (!clientId) {
      return sendApiError(res, 400, "Invalid client id");
    }

    if (!updateText) {
      return sendApiError(res, 400, "Update text is required");
    }

    const row = {
      org_id: orgId,
      client_id: clientId,
      title: body.title || null,
      update_text: updateText,
      update_type: body.update_type || "general",
      related_work_item_id: body.related_work_item_id
        ? Number(body.related_work_item_id)
        : null,
      is_client_visible: body.is_client_visible === true,
      is_active: true,
      created_by_user_id: actorUserId,
      updated_at: new Date().toISOString(),
    };

    const { data, error } = await supabase
      .from("client_updates")
      .insert([row])
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("add client update error:", error);
      return sendApiError(res, 500, "Failed to save update");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_update_created",
      entityType: "client_updates",
      entityId: data.id,
      newValue: data,
    });

    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("POST /api/clients/:id/updates fatal error:", error);
    return sendApiError(res, 500, "Failed to save update");
  }
});

app.post("/api/clients/:id/actions", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
    const actorUserId = req.loggedInUser?.id || null;
    const clientId = Number(req.params.id);
    const body = req.body || {};

    const title = String(body.title || "").trim();

    if (!clientId || !title) {
      return sendApiError(res, 400, "Action title is required");
    }

    const row = {
      org_id: orgId,
      client_id: clientId,
      title,
      owner_type: body.owner_type || "WeSolve",
      owner_name: body.owner_name || null,
      due_date: body.due_date || null,
      status: body.status || "Open",
      priority: body.priority || "Medium",
      notes: body.notes || null,
      archived: false,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };

    const { data, error } = await supabase
      .from("client_actions")
      .insert([row])
      .select("*")
      .maybeSingle();

    if (error) throw error;

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_action_created",
      entityType: "client_actions",
      entityId: data.id,
      newValue: data,
    });

    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("create action error:", error);
    return sendApiError(res, 500, "Failed to create action");
  }
});

app.post(
  "/api/clients/:id/documents",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
      const actorUserId = req.loggedInUser?.id || null;
      const clientId = Number(req.params.id);

      const title = String(req.body.title || "").trim();
      const url = String(req.body.url || "").trim();

      if (!clientId || !title || !url) {
        return res
          .status(400)
          .send("Document title and Google Drive link are required");
      }

      if (
        !url.startsWith("https://drive.google.com/") &&
        !url.startsWith("https://docs.google.com/")
      ) {
        return res
          .status(400)
          .send("Please enter a valid Google Drive or Google Docs link");
      }

      const { error } = await supabase.from("client_documents").insert([
        {
          org_id: orgId,
          client_id: clientId,
          title,
          url,
          notes: req.body.notes || null,
          is_client_visible: req.body.is_client_visible === "on",
          created_by_user_id: actorUserId,
        },
      ]);

      if (error) throw error;
      res.redirect(`/clients/${clientId}`);
    } catch (error) {
      console.error("add google drive document error:", error);
      res.status(500).send("Failed to add Google Drive link");
    }
  },
);

app.patch(
  "/api/client-work-items/:id",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
      const actorUserId = req.loggedInUser?.id || null;
      const id = Number(req.params.id);
      const body = req.body || {};

      if (!id) {
        return sendApiError(res, 400, "Invalid work item id");
      }

      const allowedStatuses = ["todo", "in_progress", "done"];
      const allowedPriorities = ["low", "medium", "high"];

      const { data: existing, error: existingError } = await supabase
        .from("client_work_items")
        .select("*")
        .eq("org_id", orgId)
        .eq("id", id)
        .eq("is_active", true)
        .is("deleted_at", null)
        .maybeSingle();

      if (existingError) {
        console.error("existing work item lookup error:", existingError);
        return sendApiError(res, 500, "Failed to load existing work item");
      }

      if (!existing) {
        return sendApiError(res, 404, "Work item not found");
      }

      if (body.archive === true) {
        const archivePatch = {
          is_active: false,
          deleted_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          last_updated_by_user_id: actorUserId,
        };

        const { data, error } = await supabase
          .from("client_work_items")
          .update(archivePatch)
          .eq("org_id", orgId)
          .eq("id", id)
          .select("*")
          .maybeSingle();

        if (error) {
          console.error("archive work item error:", error);
          return sendApiError(res, 500, "Failed to archive work item");
        }

        await insertClientActivityLog({
          orgId,
          clientId: existing.client_id,
          actorUserId,
          action: "work_item_archived",
          entityType: "client_work_items",
          entityId: id,
          oldValue: existing,
          newValue: data,
        });

        return sendApiSuccess(res, data);
      }

      if (body.status && !allowedStatuses.includes(body.status)) {
        return sendApiError(res, 400, "Invalid status");
      }

      if (body.priority && !allowedPriorities.includes(body.priority)) {
        return sendApiError(res, 400, "Invalid priority");
      }

      const dependencyId =
        body.dependency_work_item_id === "" ||
        body.dependency_work_item_id === undefined ||
        body.dependency_work_item_id === null
          ? null
          : Number(body.dependency_work_item_id);

      const milestoneId =
        body.milestone_id === "" ||
        body.milestone_id === undefined ||
        body.milestone_id === null
          ? null
          : Number(body.milestone_id);

      if (dependencyId && dependencyId === id) {
        return sendApiError(res, 400, "A work item cannot depend on itself");
      }

      let effectiveDependencyId = dependencyId;

      if (body.dependency_work_item_id === undefined) {
        effectiveDependencyId = existing.dependency_work_item_id || null;
      }

      if (effectiveDependencyId) {
        const { data: dependency, error: dependencyError } = await supabase
          .from("client_work_items")
          .select("id, client_id, title, status")
          .eq("org_id", orgId)
          .eq("id", effectiveDependencyId)
          .eq("client_id", existing.client_id)
          .eq("is_active", true)
          .is("deleted_at", null)
          .maybeSingle();

        if (dependencyError) {
          console.error("dependency lookup error:", dependencyError);
          return sendApiError(res, 500, "Failed to validate dependency");
        }

        if (!dependency) {
          return sendApiError(
            res,
            400,
            "Dependency work item not found for this client",
          );
        }

        if (body.status === "done" && dependency.status !== "done") {
          return sendApiError(
            res,
            400,
            `Cannot mark done yet. Dependency is still not done: ${dependency.title}`,
          );
        }
      }

      const patch = {
        updated_at: new Date().toISOString(),
        last_updated_by_user_id: actorUserId,
      };

      if (body.title !== undefined) {
        const title = String(body.title || "").trim();
        if (!title) return sendApiError(res, 400, "Title is required");
        patch.title = title;
      }

      if (body.description !== undefined) {
        patch.description = String(body.description || "").trim() || null;
      }

      if (body.owner_user_id !== undefined) {
        patch.owner_user_id = body.owner_user_id
          ? Number(body.owner_user_id)
          : null;
      }

      if (body.priority !== undefined) {
        patch.priority = body.priority || "medium";
      }

      if (body.due_date !== undefined) {
        patch.due_date = body.due_date || null;
      }

      if (body.dependency_work_item_id !== undefined) {
        patch.dependency_work_item_id = dependencyId;
      }

      if (body.milestone_id !== undefined) {
        patch.milestone_id = milestoneId;
      }

      if (body.status !== undefined) {
        patch.status = body.status;
        patch.completed_at =
          body.status === "done" ? new Date().toISOString() : null;
      }

      const { data, error } = await supabase
        .from("client_work_items")
        .update(patch)
        .eq("org_id", orgId)
        .eq("id", id)
        .select("*")
        .maybeSingle();

      if (error) {
        console.error("update work item error:", error);
        return sendApiError(res, 500, "Failed to update work item");
      }

      await insertClientActivityLog({
        orgId,
        clientId: existing.client_id,
        actorUserId,
        action: "work_item_updated",
        entityType: "client_work_items",
        entityId: id,
        oldValue: existing,
        newValue: data,
      });

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("PATCH /api/client-work-items/:id fatal error:", error);
      return sendApiError(res, 500, "Failed to update work item");
    }
  },
);

app.post("/api/clients", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
    const actorUserId = req.loggedInUser?.id || null;
    const body = req.body || {};

    const name = String(body.name || "").trim();
    const companyName = String(body.company_name || "").trim();
    const slug = normalizeSlug(body.slug || name);
    const googleDriveFolderUrl = String(
      body.google_drive_folder_url || "",
    ).trim();

    if (!googleDriveFolderUrl) {
      return res.status(400).send("Google Drive folder link is required");
    }

    if (!googleDriveFolderUrl.startsWith("https://drive.google.com/")) {
      return res
        .status(400)
        .send("Please enter a valid Google Drive folder link");
    }

    if (!name) {
      return res.status(400).send("Client name is required");
    }

    if (!slug) {
      return res.status(400).send("Slug is required");
    }

    const clientRow = {
      org_id: orgId,
      name,
      company_name: companyName || null,
      slug,
      google_drive_folder_url: googleDriveFolderUrl,
      status: body.status || "active",
      health_status: body.health_status || "healthy",
      start_date: body.start_date || null,
      description: body.description || null,
      account_manager_user_id: body.account_manager_user_id || null,
      project_manager_user_id: body.project_manager_user_id || null,
      created_by_user_id: actorUserId,
      updated_by_user_id: actorUserId,
      client_view_token: generateClientViewToken(),
      client_view_enabled: false,
    };

    const { data: client, error: clientError } = await supabase
      .from("clients")
      .insert([clientRow])
      .select("*")
      .maybeSingle();

    if (clientError) {
      console.error("client insert error:", clientError);

      if (clientError.code === "23505") {
        return res
          .status(400)
          .send(
            "A client with this slug already exists. Go back and choose a different slug.",
          );
      }

      return res.status(500).send("Failed to create client");
    }

    const selectedServices = ensureArray(body.services)
      .map((x) => String(x).trim())
      .filter(Boolean);

    if (selectedServices.length) {
      const { data: serviceRows, error: serviceLookupError } = await supabase
        .from("services")
        .select("id, name")
        .eq("org_id", orgId)
        .in("name", selectedServices);

      if (serviceLookupError) {
        console.error("service lookup error:", serviceLookupError);
      } else {
        const clientServiceRows = (serviceRows || []).map((service) => ({
          org_id: orgId,
          client_id: client.id,
          service_id: service.id,
        }));

        if (clientServiceRows.length) {
          const { error: clientServiceError } = await supabase
            .from("client_services")
            .insert(clientServiceRows);

          if (clientServiceError) {
            console.error("client services insert error:", clientServiceError);
          }
        }
      }
    }

    const contactRows = [];

    if (
      body.contact_name ||
      body.contact_email ||
      body.contact_phone ||
      body.contact_role
    ) {
      contactRows.push({
        org_id: orgId,
        client_id: client.id,
        name: body.contact_name || null,
        email: body.contact_email || null,
        phone: body.contact_phone || null,
        role: body.contact_role || null,
        is_primary: true,
      });
    }

    if (
      body.contact_2_name ||
      body.contact_2_email ||
      body.contact_2_phone ||
      body.contact_2_role
    ) {
      contactRows.push({
        org_id: orgId,
        client_id: client.id,
        name: body.contact_2_name || null,
        email: body.contact_2_email || null,
        phone: body.contact_2_phone || null,
        role: body.contact_2_role || null,
        is_primary: false,
      });
    }

    if (contactRows.length) {
      const { error: contactsError } = await supabase
        .from("client_contacts")
        .insert(contactRows);

      if (contactsError) {
        console.error("client contacts insert error:", contactsError);
      }
    }

    await insertClientActivityLog({
      orgId,
      clientId: client.id,
      actorUserId,
      action: "client_created",
      entityType: "clients",
      entityId: client.id,
      newValue: client,
    });

    return res.redirect(`/clients/${client.id}`);
  } catch (error) {
    console.error("POST /api/clients fatal error:", error);
    return res.status(500).send("Failed to create client");
  }
});

app.get("/clients/:id", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
    const clientId = Number(req.params.id);
    const selectedTab = String(req.query.tab || "overview");

    if (!clientId) {
      return res.status(400).send("Invalid client id");
    }

    // 1) Load main client
    const { data: client, error: clientError } = await supabase
      .from("clients")
      .select(
        `
        *,
        account_manager:users!clients_account_manager_user_id_fkey(name),
        project_manager:users!clients_project_manager_user_id_fkey(name)
      `,
      )
      .eq("org_id", orgId)
      .eq("id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    const contributorsResult = await supabase
      .from("client_contributors")
      .select("*")
      .eq("client_id", clientId)
      .eq("archived", false)
      .order("created_at", { ascending: false });

    if (clientError) {
      console.error("client workspace lookup error:", clientError);
      return res.status(500).send("Failed to load client");
    }

    if (!client) {
      return res.status(404).send("Client not found");
    }

    // 2) Load all client workspace related data
    const [
      contactsResult,
      servicesResult,
      workItemsResult,
      updatesResult,
      actionsResult,
      milestonesResult,
      documentsResult,
      usersResult,
      activityLogsResult,
    ] = await Promise.all([
      supabase
        .from("client_contacts")
        .select("*")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("is_primary", { ascending: false }),

      supabase
        .from("client_services")
        .select("services(name)")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null),

      supabase
        .from("client_work_items")
        .select("*")
        .eq("org_id", orgId)
        .eq("client_id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("status", { ascending: true })
        .order("priority", { ascending: false })
        .order("due_date", { ascending: true }),

      supabase
        .from("client_updates")
        .select("*")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .order("created_at", { ascending: false })
        .limit(50),

      supabase
        .from("client_actions")
        .select("*")
        .eq("client_id", clientId)
        .eq("archived", false)
        .order("created_at", { ascending: false }),

      supabase
        .from("client_milestones")
        .select("*")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("created_at", { ascending: false }),

      supabase
        .from("client_documents")
        .select("*")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("created_at", { ascending: false }),

      supabase
        .from("users")
        .select("id, name")
        .eq("org_id", orgId)
        .eq("is_active", true)
        .order("name", { ascending: true }),

      supabase
        .from("client_activity_logs")
        .select("*")
        .eq("org_id", orgId)
        .eq("client_id", clientId)
        .order("created_at", { ascending: false })
        .limit(50),
    ]);

    // 3) Optional error logging
    if (contactsResult.error)
      console.error("contactsResult error:", contactsResult.error);
    if (servicesResult.error)
      console.error("servicesResult error:", servicesResult.error);
    if (workItemsResult.error)
      console.error("workItemsResult error:", workItemsResult.error);
    if (updatesResult.error)
      console.error("updatesResult error:", updatesResult.error);
    if (actionsResult.error)
      console.error("actionsResult error:", actionsResult.error);
    if (milestonesResult.error)
      console.error("milestonesResult error:", milestonesResult.error);
    if (documentsResult.error)
      console.error("documentsResult error:", documentsResult.error);
    if (usersResult.error)
      console.error("usersResult error:", usersResult.error);
    if (activityLogsResult.error)
      console.error("activityLogsResult error:", activityLogsResult.error);

    // 4) Prepare clean client object for UI
    const decoratedClient = {
      ...client,
      account_manager_name: client.account_manager?.name || "",
      project_manager_name: client.project_manager?.name || "",
    };

    // 5) Clean services array
    const services = (servicesResult.data || [])
      .map((row) => row.services)
      .filter(Boolean);

    // 6) Render page
    return res.type("html").send(
      renderClientWorkspacePage({
        client: decoratedClient,
        contacts: contactsResult.data || [],
        services,
        workItems: workItemsResult.data || [],
        updates: updatesResult.data || [],
        actions: actionsResult.data || [],
        contributors: contributorsResult.data || [],
        milestones: milestonesResult.data || [],
        documents: documentsResult.data || [],
        users: usersResult.data || [],
        selectedTab,
        activityLogs: activityLogsResult.data || [],
      }),
    );
  } catch (error) {
    console.error("GET /clients/:id fatal error:", error);
    return res.status(500).send("Failed to load client workspace");
  }
});

app.post(
  "/api/clients/:id/client-view-link",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
      const actorUserId = req.loggedInUser?.id || null;
      const clientId = Number(req.params.id);

      if (!clientId) {
        return sendApiError(res, 400, "Invalid client id");
      }

      const { data: existingClient, error: loadError } = await supabase
        .from("clients")
        .select("id, org_id, client_view_token, client_view_enabled")
        .eq("org_id", orgId)
        .eq("id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null)
        .maybeSingle();

      if (loadError) throw loadError;

      if (!existingClient) {
        return sendApiError(res, 404, "Client not found");
      }

      const token =
        existingClient.client_view_token || generateClientViewToken();

      const { data, error } = await supabase
        .from("clients")
        .update({
          client_view_token: token,
          client_view_enabled: true,
          updated_by_user_id: actorUserId,
          updated_at: new Date().toISOString(),
        })
        .eq("org_id", orgId)
        .eq("id", clientId)
        .select("id, client_view_token, client_view_enabled")
        .maybeSingle();

      if (error) throw error;

      await insertClientActivityLog({
        orgId,
        clientId,
        actorUserId,
        action: "client_view_link_enabled",
        entityType: "clients",
        entityId: clientId,
        newValue: data,
      });

      const baseUrl = `${req.protocol}://${req.get("host")}`;
      return sendApiSuccess(res, {
        url: `${baseUrl}/client-view/${data.client_view_token}`,
        token: data.client_view_token,
      });
    } catch (error) {
      console.error("client view link error:", error);
      return sendApiError(res, 500, "Failed to create client view link");
    }
  },
);

app.get("/client-view/:token", async (req, res) => {
  try {
    const token = String(req.params.token || "").trim();

    if (!token) {
      return res.status(404).send("Client view not found");
    }

    const { data: client, error: clientError } = await supabase
      .from("clients")
      .select("*")
      .eq("client_view_token", token)
      .eq("client_view_enabled", true)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (clientError) {
      console.error("client view lookup error:", clientError);
      return res.status(500).send("Failed to load client view");
    }

    if (!client) {
      return res.status(404).send("Client view not found");
    }

    const clientId = client.id;
    const orgId = client.org_id;

    const [
      servicesResult,
      workItemsResult,
      updatesResult,
      actionsResult,
      documentsResult,
    ] = await Promise.all([
      supabase
        .from("client_services")
        .select("services(name)")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null),

      supabase
        .from("client_work_items")
        .select(
          "id, title, description, status, priority, due_date, updated_at",
        )
        .eq("org_id", orgId)
        .eq("client_id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("status", { ascending: true })
        .order("due_date", { ascending: true }),

      supabase
        .from("client_updates")
        .select("id, title, update_text, update_type, created_at")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .eq("is_client_visible", true)
        .order("created_at", { ascending: false })
        .limit(20),

      supabase
        .from("client_actions")
        .select(
          "id, title, owner_type, owner_name, due_date, status, priority, notes",
        )
        .eq("client_id", clientId)
        .eq("owner_type", "Client")
        .eq("archived", false)
        .order("due_date", { ascending: true, nullsFirst: false })
        .order("created_at", { ascending: false }),

      supabase
        .from("client_documents")
        .select("id, title, name, url")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("created_at", { ascending: false }),
    ]);

    const services = (servicesResult.data || [])
      .map((row) => row.services)
      .filter(Boolean);

    return res.type("html").send(
      renderClientViewOnlyPage({
        client,
        services,
        workItems: workItemsResult.data || [],
        updates: updatesResult.data || [],
        actions: actionsResult.data || [],
        documents: documentsResult.data || [],
      }),
    );
  } catch (error) {
    console.error("GET /client-view/:token fatal error:", error);
    return res.status(500).send("Failed to load client view");
  }
});

app.get("/clients/:id/edit", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
    const clientId = Number(req.params.id);

    const [clientResult, usersResult, contactsResult] = await Promise.all([
      supabase
        .from("clients")
        .select("*")
        .eq("org_id", orgId)
        .eq("id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null)
        .maybeSingle(),

      supabase
        .from("users")
        .select("id, name")
        .eq("org_id", orgId)
        .eq("is_active", true)
        .order("name", { ascending: true }),

      supabase
        .from("client_contacts")
        .select("*")
        .eq("client_id", clientId)
        .order("is_primary", { ascending: false })
        .order("created_at", { ascending: true }),
    ]);

    if (clientResult.error) throw clientResult.error;
    if (usersResult.error) throw usersResult.error;
    if (contactsResult.error) throw contactsResult.error;
    const client = clientResult.data;
    const users = usersResult.data || [];
    const contacts = contactsResult.data || [];
    const primaryContact = contacts[0] || {};
    const secondContact = contacts[1] || {};
    const thirdContact = contacts[2] || {};
    if (!client) {
      return res.status(404).send("Client not found");
    }

    res.type("html").send(`
      <html>
        <head>
          <title>Edit Client | WeSolveHR</title>
          <style>
            ${buildThemeCss()}
            ${buildBasePageCss()}
            ${buildTopNavCss()}

            .wrap {
              max-width: 1200px;
              margin: 0 auto;
              padding: 24px 18px 36px;
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

            .panel {
              padding: 20px;
              margin-bottom: 16px;
            }

            .eyebrow {
              font-size: 11px;
              letter-spacing: 0.16em;
              text-transform: uppercase;
              color: var(--primary);
              font-weight: 700;
              margin-bottom: 8px;
            }

            h1 {
              margin: 0;
              font-size: 30px;
              letter-spacing: -0.04em;
            }

            h2 {
              margin: 0 0 14px;
              font-size: 18px;
            }

            .subtitle {
              color: var(--muted);
              margin-top: 8px;
              font-size: 14px;
            }

            .grid {
              display: grid;
              grid-template-columns: repeat(2, minmax(0, 1fr));
              gap: 14px;
            }

            .field {
              display: flex;
              flex-direction: column;
              gap: 8px;
            }

            label {
              font-size: 13px;
              font-weight: 800;
            }

            input, select, textarea {
              width: 100%;
              padding: 12px 13px;
              border-radius: 12px;
              border: 1px solid var(--line);
              background: rgba(255,255,255,0.04);
              color: var(--text);
              font: inherit;
            }

            textarea {
              min-height: 100px;
              resize: vertical;
            }

            .actions {
              display: flex;
              gap: 10px;
              justify-content: flex-end;
              margin-top: 18px;
            }

            .btn {
              padding: 11px 14px;
              border-radius: 12px;
              border: 1px solid rgba(255,255,255,0.12);
              color: var(--text);
              background: rgba(255,255,255,0.05);
              font-weight: 800;
              text-decoration: none;
              cursor: pointer;
            }

            .btn-primary {
              background: var(--primary-soft);
              color: var(--text-strong);
              border-color: color-mix(in srgb, var(--primary) 55%, transparent);
            }

            @media (max-width: 800px) {
              .grid {
                grid-template-columns: 1fr;
              }
            }
          </style>
        </head>

        <body>
          ${renderTopNav("clients")}

          <div class="wrap">
            <div class="topbar">
              <div>
                <div class="eyebrow">Edit Client</div>
                <h1>${escapeHtml(client.name || "Client")}</h1>
                <div class="subtitle">Update client basics, status, health, ownership, and Drive folder.</div>
              </div>
              <a class="btn" href="/clients/${client.id}">← Back to Client</a>
            </div>

            <form method="POST" action="/clients/${client.id}/edit">
              <div class="panel">
                <h2>Basic Info</h2>

                <div class="grid">
                  <div class="field">
                    <label>Client Name</label>
                    <input name="name" value="${escapeHtml(client.name || "")}" required />
                  </div>

                  <div class="field">
                    <label>Company Name</label>
                    <input name="company_name" value="${escapeHtml(client.company_name || "")}" />
                  </div>

                  <div class="field">
                    <label>Slug</label>
                    <input name="slug" value="${escapeHtml(client.slug || "")}" />
                  </div>

                  <div class="field">
                    <label>Google Drive Folder Link</label>
                    <input name="google_drive_folder_url" value="${escapeHtml(client.google_drive_folder_url || "")}" required />
                  </div>

                  <div class="field">
                    <label>Status</label>
                    <select name="status">
                      <option value="active" ${client.status === "active" ? "selected" : ""}>Active</option>
                      <option value="paused" ${client.status === "paused" ? "selected" : ""}>Paused</option>
                      <option value="onboarding" ${client.status === "onboarding" ? "selected" : ""}>Onboarding</option>
                      <option value="completed" ${client.status === "completed" ? "selected" : ""}>Completed</option>
                      <option value="inactive" ${client.status === "inactive" ? "selected" : ""}>Inactive</option>
                    </select>
                  </div>

                  <div class="field">
                    <label>Health</label>
                    <select name="health_status">
                      <option value="healthy" ${client.health_status === "healthy" ? "selected" : ""}>Healthy</option>
                      <option value="watch" ${client.health_status === "watch" ? "selected" : ""}>Watch</option>
                      <option value="at_risk" ${client.health_status === "at_risk" ? "selected" : ""}>At Risk</option>
                    </select>
                  </div>

                  <div class="field">
                    <label>Start Date</label>
                    <input type="date" name="start_date" value="${escapeHtml(client.start_date || "")}" />
                  </div>

                  <div class="field">
                    <label>Account Manager</label>
                    <select name="account_manager_user_id">
                      <option value="">Select account manager</option>
                      ${users
                        .map(
                          (u) =>
                            `<option value="${u.id}" ${
                              String(client.account_manager_user_id || "") ===
                              String(u.id)
                                ? "selected"
                                : ""
                            }>${escapeHtml(u.name)}</option>`,
                        )
                        .join("")}
                    </select>
                  </div>

                  <div class="field">
                    <label>Project Manager</label>
                    <select name="project_manager_user_id">
                      <option value="">Select project manager</option>
                      ${users
                        .map(
                          (u) =>
                            `<option value="${u.id}" ${
                              String(client.project_manager_user_id || "") ===
                              String(u.id)
                                ? "selected"
                                : ""
                            }>${escapeHtml(u.name)}</option>`,
                        )
                        .join("")}
                    </select>
                  </div>
                </div>

                <div class="field" style="margin-top:14px;">
                  <label>Description</label>
                  <textarea name="description">${escapeHtml(client.description || "")}</textarea>
                </div>
              </div>
<div class="panel">
  <h2>Client Contacts</h2>

  <input type="hidden" name="contact_1_id" value="${escapeHtml(primaryContact.id || "")}" />
  <input type="hidden" name="contact_2_id" value="${escapeHtml(secondContact.id || "")}" />
  <input type="hidden" name="contact_3_id" value="${escapeHtml(thirdContact.id || "")}" />

  <div class="grid">
    <div class="field">
      <label>Primary Contact Name</label>
      <input name="contact_1_name" value="${escapeHtml(primaryContact.name || "")}" />
    </div>

    <div class="field">
      <label>Primary Contact Email</label>
      <input name="contact_1_email" value="${escapeHtml(primaryContact.email || "")}" />
    </div>

    <div class="field">
      <label>Primary Contact Phone</label>
      <input name="contact_1_phone" value="${escapeHtml(primaryContact.phone || "")}" />
    </div>

    <div class="field">
      <label>Primary Contact Role</label>
      <input name="contact_1_role" value="${escapeHtml(primaryContact.role || "")}" />
    </div>
  </div>

  <div class="grid" style="margin-top:18px;">
    <div class="field">
      <label>Contact 2 Name</label>
      <input name="contact_2_name" value="${escapeHtml(secondContact.name || "")}" />
    </div>

    <div class="field">
      <label>Contact 2 Email</label>
      <input name="contact_2_email" value="${escapeHtml(secondContact.email || "")}" />
    </div>

    <div class="field">
      <label>Contact 2 Phone</label>
      <input name="contact_2_phone" value="${escapeHtml(secondContact.phone || "")}" />
    </div>

    <div class="field">
      <label>Contact 2 Role</label>
      <input name="contact_2_role" value="${escapeHtml(secondContact.role || "")}" />
    </div>
  </div>

  <div class="grid" style="margin-top:18px;">
    <div class="field">
      <label>Contact 3 Name</label>
      <input name="contact_3_name" value="${escapeHtml(thirdContact.name || "")}" />
    </div>

    <div class="field">
      <label>Contact 3 Email</label>
      <input name="contact_3_email" value="${escapeHtml(thirdContact.email || "")}" />
    </div>

    <div class="field">
      <label>Contact 3 Phone</label>
      <input name="contact_3_phone" value="${escapeHtml(thirdContact.phone || "")}" />
    </div>

    <div class="field">
      <label>Contact 3 Role</label>
      <input name="contact_3_role" value="${escapeHtml(thirdContact.role || "")}" />
    </div>
  </div>
</div>
              <div class="panel">
                <div class="actions">
                  <a class="btn" href="/clients/${client.id}">Cancel</a>
                  <button class="btn btn-primary" type="submit">Save Client</button>
                </div>
              </div>
            </form>
          </div>
        </body>
      </html>
    `);
  } catch (error) {
    console.error("GET /clients/:id/edit error:", error);
    return res.status(500).send("Failed to load edit client page");
  }
});

app.post("/clients/:id/edit", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
    const actorUserId = req.loggedInUser?.id || null;
    const clientId = Number(req.params.id);
    const body = req.body || {};

    const name = String(body.name || "").trim();

    if (!clientId) {
      return res.status(400).send("Invalid client id");
    }

    if (!name) {
      return res.status(400).send("Client name is required");
    }

    const oldResult = await supabase
      .from("clients")
      .select("*")
      .eq("org_id", orgId)
      .eq("id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (oldResult.error) throw oldResult.error;

    if (!oldResult.data) {
      return res.status(404).send("Client not found");
    }

    const updateRow = {
      name,
      company_name: body.company_name || null,
      slug: normalizeSlug(body.slug || name),
      google_drive_folder_url: body.google_drive_folder_url || null,
      status: body.status || "active",
      health_status: body.health_status || "healthy",
      start_date: body.start_date || null,
      description: body.description || null,
      account_manager_user_id: body.account_manager_user_id
        ? Number(body.account_manager_user_id)
        : null,
      project_manager_user_id: body.project_manager_user_id
        ? Number(body.project_manager_user_id)
        : null,
      updated_by_user_id: actorUserId,
      updated_at: new Date().toISOString(),
    };

    const { data, error } = await supabase
      .from("clients")
      .update(updateRow)
      .eq("org_id", orgId)
      .eq("id", clientId)
      .select("*")
      .maybeSingle();

    if (error) throw error;

    async function upsertContactFromBody(index, isPrimary = false) {
      const contactId = body[`contact_${index}_id`]
        ? Number(body[`contact_${index}_id`])
        : null;

      const contactName = String(body[`contact_${index}_name`] || "").trim();
      const contactEmail = String(body[`contact_${index}_email`] || "").trim();
      const contactPhone = String(body[`contact_${index}_phone`] || "").trim();
      const contactRole = String(body[`contact_${index}_role`] || "").trim();

      const hasAnyContactData =
        contactName || contactEmail || contactPhone || contactRole;

      if (!hasAnyContactData && !contactId) {
        return null;
      }

      if (contactId && !hasAnyContactData) {
        const { error: archiveError } = await supabase
          .from("client_contacts")
          .update({
            is_active: false,
            deleted_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
          })
          .eq("id", contactId)
          .eq("client_id", clientId);

        if (archiveError) throw archiveError;
        return null;
      }

      const contactRow = {
        org_id: orgId,
        client_id: clientId,
        name: contactName || "Unnamed Contact",
        email: contactEmail || null,
        phone: contactPhone || null,
        role: contactRole || null,
        is_primary: isPrimary,
        is_active: true,
      };

      if (contactId) {
        const { data: updatedContact, error: contactUpdateError } =
          await supabase
            .from("client_contacts")
            .update(contactRow)
            .eq("id", contactId)
            .eq("client_id", clientId)
            .select("*")
            .maybeSingle();

        if (contactUpdateError) throw contactUpdateError;
        return updatedContact;
      }

      const { data: newContact, error: contactInsertError } = await supabase
        .from("client_contacts")
        .insert([contactRow])
        .select("*")
        .maybeSingle();

      if (contactInsertError) throw contactInsertError;
      return newContact;
    }

    await upsertContactFromBody(1, true);
    await upsertContactFromBody(2, false);
    await upsertContactFromBody(3, false);

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_updated",
      entityType: "clients",
      entityId: clientId,
      oldValue: oldResult.data,
      newValue: data,
    });

    return res.redirect(`/clients/${clientId}`);
  } catch (error) {
    console.error("POST /clients/:id/edit error:", error);
    return res
      .status(500)
      .send(
        `Failed to update client: ${escapeHtml(error?.message || String(error))}`,
      );
  }
});

app.get("/clients/:id/reset", requireDashboardAuth, async (req, res) => {
  const clientId = Number(req.params.id);

  res.type("html").send(`
    <html>
      <head>
        <title>Reset Client | WeSolveHR</title>
        <style>
          ${buildThemeCss()}
          ${buildBasePageCss()}
          ${buildTopNavCss()}

          .wrap {
            max-width: 900px;
            margin: 0 auto;
            padding: 24px 18px 36px;
          }

          .panel {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
            padding: 22px;
          }

          h1 {
            margin: 0 0 10px;
            font-size: 30px;
          }

          .muted {
            color: var(--muted);
            line-height: 1.6;
          }

          .danger-box {
            margin-top: 18px;
            padding: 16px;
            border-radius: 16px;
            border: 1px solid rgba(239,107,115,0.35);
            background: rgba(239,107,115,0.12);
          }

          .check-list {
            margin-top: 16px;
            display: grid;
            gap: 10px;
          }

          label {
            display: flex;
            gap: 10px;
            align-items: flex-start;
            color: var(--text);
            font-weight: 700;
          }

          input[type="checkbox"] {
            margin-top: 3px;
          }

          .actions {
            margin-top: 22px;
            display: flex;
            gap: 10px;
            justify-content: flex-end;
            flex-wrap: wrap;
          }

          .btn {
            display: inline-flex;
            align-items: center;
            text-decoration: none;
            color: var(--text);
            padding: 11px 14px;
            border-radius: 12px;
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.12);
            font-weight: 800;
            cursor: pointer;
            font: inherit;
          }

          .btn-danger {
            background: var(--danger-soft);
            color: var(--text-strong);
            border-color: rgba(239,107,115,0.45);
          }

          .confirm-input {
            width: 100%;
            margin-top: 14px;
            padding: 12px;
            border-radius: 12px;
            border: 1px solid var(--line);
            background: rgba(255,255,255,0.04);
            color: var(--text);
            font: inherit;
          }
        </style>
      </head>

      <body>
        ${renderTopNav("clients")}

        <div class="wrap">
          <div class="panel">
            <h1>Reset Client Workspace</h1>
            <div class="muted">
              This will archive workspace data for this client. It will not delete the client, Google Drive folder, contacts, services, or client view token.
            </div>

            <div class="danger-box">
              <strong>Important:</strong> This is meant for cleaning a test/demo client workspace.
              Existing work items, updates, actions, contributors, milestones, documents, and activity logs will be hidden/archived.
            </div>

            <form method="POST" action="/clients/${clientId}/reset">
              <div class="check-list">
                <label>
                  <input type="checkbox" name="reset_work_items" checked />
                  Archive work items
                </label>

                <label>
                  <input type="checkbox" name="reset_updates" checked />
                  Archive updates
                </label>

                <label>
                  <input type="checkbox" name="reset_actions" checked />
                  Archive actions
                </label>

                <label>
                  <input type="checkbox" name="reset_contributors" checked />
                  Archive contributors
                </label>

                <label>
                  <input type="checkbox" name="reset_milestones" checked />
                  Archive milestones
                </label>

                <label>
                  <input type="checkbox" name="reset_documents" checked />
                  Archive document records
                </label>

                <label>
                  <input type="checkbox" name="reset_activity_logs" checked />
                  Archive activity logs
                </label>
              </div>

              <input
                class="confirm-input"
                name="confirm_text"
                placeholder="Type RESET to confirm"
              />

              <div class="actions">
                <a class="btn" href="/clients/${clientId}">Cancel</a>
                <button class="btn btn-danger" type="submit">Reset Selected Data</button>
              </div>
            </form>
          </div>
        </div>
      </body>
    </html>
  `);
});

app.post("/clients/:id/reset", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
    const actorUserId = req.loggedInUser?.id || null;
    const clientId = Number(req.params.id);
    const body = req.body || {};
    const now = new Date().toISOString();

    if (!clientId) {
      return res.status(400).send("Invalid client id");
    }

    if (String(body.confirm_text || "").trim() !== "RESET") {
      return res.status(400).send("Please type RESET to confirm.");
    }

    const { data: client, error: clientError } = await supabase
      .from("clients")
      .select("id, org_id")
      .eq("org_id", orgId)
      .eq("id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (clientError) throw clientError;

    if (!client) {
      return res.status(404).send("Client not found");
    }

    const resetSummary = {};

    if (body.reset_work_items === "on") {
      const { data, error } = await supabase
        .from("client_work_items")
        .update({
          is_active: false,
          deleted_at: now,
          updated_at: now,
        })
        .eq("org_id", orgId)
        .eq("client_id", clientId)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.work_items = data?.length || 0;
    }

    if (body.reset_updates === "on") {
      const { data, error } = await supabase
        .from("client_updates")
        .update({
          is_active: false,
          archived_at: now,
          updated_at: now,
        })
        .eq("client_id", clientId)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.updates = data?.length || 0;
    }

    if (body.reset_actions === "on") {
      const { data, error } = await supabase
        .from("client_actions")
        .update({
          archived: true,
          status: "Archived",
          updated_at: now,
        })
        .eq("client_id", clientId)
        .eq("archived", false)
        .select("id");

      if (error) throw error;
      resetSummary.actions = data?.length || 0;
    }

    if (body.reset_contributors === "on") {
      const { data, error } = await supabase
        .from("client_contributors")
        .update({
          archived: true,
          status: "Inactive",
          updated_at: now,
        })
        .eq("client_id", clientId)
        .eq("archived", false)
        .select("id");

      if (error) throw error;
      resetSummary.contributors = data?.length || 0;
    }

    if (body.reset_milestones === "on") {
      const { data, error } = await supabase
        .from("client_milestones")
        .update({
          is_active: false,
          deleted_at: now,
          updated_at: now,
        })
        .eq("org_id", orgId)
        .eq("client_id", clientId)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.milestones = data?.length || 0;
    }

    if (body.reset_documents === "on") {
      const { data, error } = await supabase
        .from("client_documents")
        .update({
          is_active: false,
          deleted_at: now,
        })
        .eq("client_id", clientId)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.documents = data?.length || 0;
    }

    if (body.reset_activity_logs === "on") {
      const { data, error } = await supabase
        .from("client_activity_logs")
        .delete()
        .eq("org_id", orgId)
        .eq("client_id", clientId)
        .select("id");

      if (error) throw error;
      resetSummary.activity_logs = data?.length || 0;
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_workspace_reset",
      entityType: "clients",
      entityId: clientId,
      newValue: resetSummary,
    });

    return res.redirect("/clients");
  } catch (error) {
    console.error("POST /clients/:id/reset error:", error);
    return res
      .status(500)
      .send(
        `Failed to reset client: ${escapeHtml(error?.message || String(error))}`,
      );
  }
});

app.get("/dashboard", requireDashboardAuth, async (req, res) => {
  try {
    const user = req.loggedInUser;

    if (user && !isManagerOrAdmin(user)) {
      return res.redirect("/my-dashboard");
    }

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
        ${renderTopNav("dashboard")}
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

app.get("/api/attendance", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
    const data = await getAttendancePageData(orgId);
    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("attendance api error:", error);
    return sendApiError(res, 500, "Failed to load attendance");
  }
});

app.get("/api/attendance/insights", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
    const data = await getAttendanceInsightsData(orgId);
    return sendApiSuccess(res, data);
  } catch (error) {
    console.error(
      "attendance insights api error:",
      error?.message,
      error?.stack,
      error,
    );
    return sendApiError(res, 500, "Failed to load attendance insights");
  }
});

app.get("/api/tasks", requireDashboardAuth, async (req, res) => {
  try {
    const rows = await getTasksPageData(
      {
        search: req.query.search,
        assignee: req.query.assignee,
        waitingOn: req.query.waitingOn,
        business: req.query.business,
        area: req.query.area,
        status: req.query.status,
        priority: req.query.priority,
        blocked: req.query.blocked,
        overdue: req.query.overdue,
        progressBucket: req.query.progressBucket,
      },
      DASHBOARD_ORG_ID,
    );

    return sendApiSuccess(res, rows);
  } catch (error) {
    console.error("/api/tasks error:", error);
    return sendApiError(res, 500, "Failed to load tasks");
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
  ${buildTopNavCss()}

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

.owner-chip-link {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 6px 10px;
  border-radius: 999px;
  text-decoration: none;
  color: var(--text);
  background: var(--secondary-soft);
  border: 1px solid rgba(255,255,255,0.10);
  font-size: 12px;
  font-weight: 700;
  margin-right: 6px;
  margin-bottom: 4px;
}

.owner-chip-link:hover {
  border-color: color-mix(in srgb, var(--secondary) 55%, transparent);
  color: var(--text-strong);
}

tbody tr {
  cursor: pointer;
}

.modal {
  position: fixed;
  inset: 0;
  display: none;
  z-index: 9999;
}

.modal.open {
  display: block;
}

.modal-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(3, 8, 20, 0.72);
  backdrop-filter: blur(4px);
}

.modal-card {
  position: relative;
  width: min(920px, calc(100vw - 32px));
  max-height: calc(100vh - 40px);
  overflow: auto;
  margin: 20px auto;
  border-radius: 20px;
  border: 1px solid var(--line);
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  box-shadow: var(--shadow-soft);
}

.modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 18px 20px;
  border-bottom: 1px solid var(--line);
  position: sticky;
  top: 0;
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  z-index: 2;
}

.modal-title {
  font-size: 20px;
  font-weight: 800;
  letter-spacing: -0.02em;
}

.modal-close {
  border: 1px solid var(--line);
  background: rgba(255,255,255,0.04);
  color: var(--text);
  border-radius: 10px;
  padding: 8px 12px;
  cursor: pointer;
  font-size: 14px;
}

.modal-body {
  padding: 18px 20px 22px;
}

.modal-meta-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 18px;
}

.modal-meta-box {
  border: 1px solid var(--line);
  background: rgba(255,255,255,0.03);
  border-radius: 14px;
  padding: 12px 14px;
}

.modal-meta-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--muted);
  margin-bottom: 6px;
  font-weight: 700;
}

.modal-section {
  margin-top: 18px;
}

.modal-section h3 {
  margin: 0 0 10px 0;
  font-size: 15px;
}

.history-list {
  border: 1px solid var(--line);
  border-radius: 14px;
  overflow: hidden;
}

.history-item {
  padding: 12px 14px;
  border-bottom: 1px solid rgba(255,255,255,0.08);
  white-space: pre-wrap;
}

.history-item:last-child {
  border-bottom: none;
}

.history-top {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 6px;
  font-size: 13px;
}

.history-top strong {
  color: var(--text);
}

.history-top span {
  color: var(--muted);
}

.history-detail {
  color: var(--text);
  line-height: 1.55;
  white-space: pre-wrap;
}

@media (max-width: 760px) {
  .modal-meta-grid {
    grid-template-columns: 1fr;
  }
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
          
.controls select[multiple] {
  height: 44px;
  min-height: 44px;
  padding: 10px 12px;
  border-radius: 12px;
  border: 1px solid var(--line);
  background: rgba(255,255,255,0.04);
  color: var(--text);
  font: inherit;
  box-shadow: none;
}

.controls select[multiple]:focus {
  outline: none;
  border-color: var(--line-strong);
}

.controls select[multiple] option {
  color: var(--text);
  background: #1f2740;
}

.controls select[multiple] option:checked {
  background: linear-gradient(0deg, rgba(139,124,246,0.28), rgba(139,124,246,0.28));
  color: var(--text-strong);
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
      ${renderTopNav("tasks")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Task Operations</div>
              <h1>WeSolveHR // Tasks Console</h1>
              <div class="subtitle">Filter and inspect work across the team without changing backend behavior</div>
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

<select id="progressBucket" multiple size="1">
<option value="not_begun" selected>Not begun</option>
  <option value="zero_to_fifty" selected>0–50% complete</option>
  <option value="fifty_to_hundred" selected>50–100% complete</option>
  <option value="complete">100% complete</option>
    <option value="hide_cancelled" selected>Hide Cancelled</option>
<option value="only_cancelled">Cancelled only</option>
</select>

<label><input type="checkbox" id="blocked" /> Blocked only</label>
<label><input type="checkbox" id="overdue" /> Overdue only</label>
<button onclick="loadTasks()">Apply</button>
</div>
</div>
<div id="activeSpecialFilters" class="muted" style="margin: 8px 0 12px;"></div>
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

<div id="taskModal" class="modal">
  <div class="modal-backdrop" onclick="closeTaskModal()"></div>
  <div class="modal-card">
    <div class="modal-header">
      <div id="modalTitle" class="modal-title">Task Detail</div>
      <button class="modal-close" onclick="closeTaskModal()">✕</button>
    </div>
    <div id="modalBody" class="modal-body">
      <div class="muted">Loading task details...</div>
    </div>
  </div>
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
    try {
      return escapeHtml(JSON.stringify(value));
    } catch {
      return '-';
    }
  }
  return escapeHtml(String(value));
}

function closeTaskModal() {
  const modal = document.getElementById("taskModal");
  if (modal) {
    modal.classList.remove("open");
  }
}

function renderHistoryDetail(item) {
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};

  if (item.changeType === "task_created") {
    return "Task created";
  }

  if (item.changeType === "status_change") {
    const oldStatus = oldValue.status || "-";
    const newStatus = newValue.status || "-";
    const oldProgress = oldValue.progress ?? "-";
    const newProgress = newValue.progress ?? "-";
    const note = newValue.note ? "\\nNote: " + newValue.note : "";

    return "Status: " + oldStatus + " → " + newStatus +
      "\\nProgress: " + oldProgress + "% → " + newProgress + "%" +
      note;
  }

  if (item.changeType === "progress_change") {
    const oldProgress = oldValue.progress ?? 0;
    const newProgress = newValue.progress ?? 0;
    const note = newValue.note ? "\\nNote: " + newValue.note : "";

    return "Progress: " + oldProgress + "% → " + newProgress + "%" + note;
  }

  if (item.changeType === "owner_change") {
    const oldOwners = Array.isArray(oldValue.owners) ? oldValue.owners.join(", ") : "-";
    const newOwners = Array.isArray(newValue.owners) ? newValue.owners.join(", ") : "-";
    return "Owners: " + oldOwners + " → " + newOwners;
  }

  if (item.changeType === "deadline_change") {
    return "Deadline: " + (oldValue.deadline || "-") + " → " + (newValue.deadline || "-");
  }

  if (item.fieldName === "blocker_note") {
    return [
      "Blocker: " + (newValue.blocker_note || "-"),
      newValue.note ? "Note: " + newValue.note : null
    ].filter(Boolean).join("\\n");
  }

  if (item.fieldName === "title") {
    return "Title: " + (oldValue.title || "-") + " → " + (newValue.title || "-");
  }

  if (item.fieldName === "detail") {
    return "Detail updated";
  }

  if (item.fieldName === "priority") {
    return "Priority: " + (oldValue.priority || "-") + " → " + (newValue.priority || "-");
  }

  if (item.fieldName === "business") {
    return "Business: " + (oldValue.business || "-") + " → " + (newValue.business || "-");
  }

  if (item.fieldName === "area") {
    return "Area: " + (oldValue.area || "-") + " → " + (newValue.area || "-");
  }

  if (item.fieldName) {
    return (item.fieldName || "Field") + ": " +
      JSON.stringify(oldValue) + " → " + JSON.stringify(newValue);
  }

  return JSON.stringify(newValue || {});
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
      body.innerHTML =
        '<div class="muted">' + escapeHtml(json.error || "Failed to load task") + '</div>';
      return;
    }

    const task = json.data || {};
    title.textContent = "#" + (task.taskNo || task.id) + " — " + escapeHtml(task.title || "Untitled");

    const historyHtml = (task.history || []).length
      ? task.history.map(function(item) {
          return (
            '<div class="history-item">' +
              '<div class="history-top">' +
                '<strong>' + escapeHtml(item.changeType || "-") + '</strong>' +
                '<span>' + escapeHtml(item.at || "-") + ' • ' + escapeHtml(item.by || "-") + '</span>' +
              '</div>' +
              '<div class="history-detail">' + escapeHtml(renderHistoryDetail(item)) + '</div>' +
            '</div>'
          );
        }).join("")
      : '<div class="muted">No recent history</div>';

    body.innerHTML =
      '<div class="modal-meta-grid">' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Owners</div><div>' + escapeHtml(((task.owners || []).join(", ") || "-")) + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Status</div><div>' + escapeHtml(task.status || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Priority</div><div>' + escapeHtml(task.priority || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Progress</div><div>' + escapeHtml(String(task.progress ?? 0)) + '%</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Deadline</div><div>' + escapeHtml(task.deadline || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Business / Area</div><div>' + escapeHtml((task.business || "-") + " / " + (task.area || "-")) + '</div></div>' +
      '</div>' +

      ((task.detail || task.blockerNote) ? (
        '<div class="modal-section">' +
          '<h3>Details</h3>' +
          (task.detail
            ? '<div class="modal-meta-box" style="margin-bottom:10px;"><div class="modal-meta-label">Detail</div><div>' + escapeHtml(task.detail) + '</div></div>'
            : ''
          ) +
          (task.blockerNote
            ? '<div class="modal-meta-box"><div class="modal-meta-label">Blocker</div><div>' + escapeHtml(task.blockerNote) + '</div></div>'
            : ''
          ) +
        '</div>'
      ) : '') +

      '<div class="modal-section">' +
        '<h3>History</h3>' +
        '<div class="history-list">' + historyHtml + '</div>' +
      '</div>';
  } catch (error) {
    console.error("openTaskDetail error:", error);
    body.innerHTML = '<div class="muted">Could not load task detail</div>';
  }
}

document.addEventListener("keydown", function(event) {
  if (event.key === "Escape") {
    closeTaskModal();
  }
});

function renderTaskOwnerLinks(task) {
  const owners = Array.isArray(task.owners) ? task.owners : [];

  if (!owners.length) {
    return '<span class="muted">Unassigned</span>';
  }

  return owners
    .map(function (owner) {
      const id = owner.user_id || owner.id;
      const name = escapeHtml(owner.name || "Unknown");

      return (
        '<a class="owner-chip-link" ' +
        'href="/tasks/user/' + id + '" ' +
        'onclick="event.stopPropagation()">' +
        name +
        '</a>'
      );
    })
    .join(' ');
}

async function loadUsers() {
  try {
    console.log('loadUsers start');

    const select = document.getElementById('assignee');
    if (!select) {
      console.error('loadUsers: assignee select not found');
      return;
    }

    const res = await fetch('/api/users');
    console.log('loadUsers fetch status:', res.status, res.statusText);

    const json = await res.json();
    console.log('loadUsers response:', json);

    if (!json.ok) {
      console.error('loadUsers api error:', json);
      select.innerHTML = '<option value="">All assignee</option>';
      return;
    }

    select.innerHTML = '<option value="">All assignee</option>';

    for (const user of (json.data || [])) {
      const opt = document.createElement('option');
      opt.value = String(user.id);
      opt.textContent = user.name;
      select.appendChild(opt);
    }

    console.log('loadUsers done. option count:', select.options.length);
  } catch (error) {
    console.error('loadUsers fatal error:', error);
    const select = document.getElementById('assignee');
    if (select) {
      select.innerHTML = '<option value="">All assignee</option>';
    }
  }
}

function applyFiltersFromUrl() {
  const params = new URLSearchParams(window.location.search);

  const search = params.get('search') || '';
  const assignee = params.get('assignee') || '';
  const waitingOn = params.get('waitingOn') || '';
  const business = params.get('business') || '';
  const area = params.get('area') || '';
  const status = params.get('status') || '';
  const priority = params.get('priority') || '';
  const blocked = params.get('blocked') === 'true';
  const overdue = params.get('overdue') === 'true';
  const progressBuckets = params.getAll('progressBucket');

  const searchEl = document.getElementById('search');
  const assigneeEl = document.getElementById('assignee');
  const businessEl = document.getElementById('business');
  const areaEl = document.getElementById('area');
  const statusEl = document.getElementById('status');
  const priorityEl = document.getElementById('priority');
  const blockedEl = document.getElementById('blocked');
  const overdueEl = document.getElementById('overdue');
  const progressBucketEl = document.getElementById('progressBucket');

  if (searchEl) searchEl.value = search;
  if (assigneeEl) assigneeEl.value = assignee;
  if (businessEl) businessEl.value = business;
  if (areaEl) areaEl.value = area;
  if (statusEl) statusEl.value = status;
  if (priorityEl) priorityEl.value = priority;
  if (blockedEl) blockedEl.checked = blocked;
  if (overdueEl) overdueEl.checked = overdue;

  if (progressBucketEl) {
    if (progressBuckets.length) {
      for (const opt of progressBucketEl.options) {
        opt.selected = progressBuckets.includes(opt.value);
      }
    } else if (blocked) {
      const blockedDefaults = new Set([
        'not_begun',
        'zero_to_fifty',
        'fifty_to_hundred',
        'complete',
        'hide_cancelled',
      ]);

      for (const opt of progressBucketEl.options) {
        opt.selected = blockedDefaults.has(opt.value);
      }
    }
  }

  window.__waitingOn = waitingOn;
}

function renderSpecialFilterState() {
  const box = document.getElementById('activeSpecialFilters');
  if (!box) return;

  if (!window.__waitingOn) {
    box.innerHTML = '';
    return;
  }

  const assigneeSelect = document.getElementById('assignee');
  let waitingOnName = 'Selected user';

  if (assigneeSelect) {
    const opt = Array.from(assigneeSelect.options).find(
      o => String(o.value) === String(window.__waitingOn)
    );
    if (opt && opt.textContent) waitingOnName = opt.textContent;
  }

  box.innerHTML =
    'Filtered: waiting on <strong>' + waitingOnName + '</strong> ' +
    '<button type="button" onclick="clearWaitingOnFilter()" style="margin-left:8px;">Clear</button>';
}

function clearWaitingOnFilter() {
  window.__waitingOn = '';

  const url = new URL(window.location.href);
  url.searchParams.delete('waitingOn');
  window.history.replaceState({}, '', url.toString());

  loadTasks();
}

          async function loadTasks() {
            const params = new URLSearchParams();
            renderSpecialFilterState();
const search = document.getElementById('search').value.trim();
const assignee = document.getElementById('assignee').value;
const business = document.getElementById('business').value;
const area = document.getElementById('area').value;
const status = document.getElementById('status').value;

const priority = document.getElementById('priority').value;
const blocked = document.getElementById('blocked').checked;
const overdue = document.getElementById('overdue').checked;
const waitingOn = window.__waitingOn || '';
if (waitingOn) params.set('waitingOn', waitingOn);

const progressBucket = Array.from(
  document.getElementById('progressBucket').selectedOptions
).map(opt => opt.value);

if (search) params.set('search', search);
if (assignee) params.set('assignee', assignee);
if (business) params.set('business', business);
if (area) params.set('area', area);
if (status) params.set('status', status);
if (priority) params.set('priority', priority);
if (blocked) params.set('blocked', 'true');
if (overdue) params.set('overdue', 'true');

for (const bucket of progressBucket) {
  params.append('progressBucket', bucket);
}

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
document.getElementById('statusText').textContent =
  rows.length === 0
    ? 'No tasks found'
    : (rows.length + ' task' + (rows.length === 1 ? '' : 's') + ' shown');

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
    '<tr class="' + rowClasses + '" onclick="openTaskDetail(' + (task.task_no || task.id) + ')">' +
      '<td>' +
  '<span class="task-link" onclick="event.stopPropagation(); openTaskDetail(' + (task.task_no || task.id) + ')">' +
    '#' + (task.task_no || task.id) +
  '</span>' +
'</td>' +
      '<td>' + escapeHtml(task.title || '') + '</td>' +
      '<td>' + escapeHtml(task.business || '-') + '</td>' +
      '<td>' + escapeHtml(task.area || '-') + '</td>' +
'<td>' + renderTaskOwnerLinks(task) + '</td>' +
      '<td>' + escapeHtml(task.status || '') + '</td>' +
      '<td>' + (task.progress ?? 0) + '%</td>' +
      '<td>' + escapeHtml(task.priority || '') + '</td>' +
      '<td>' + escapeHtml(task.deadline || '-') + '</td>' +
      '<td>' + escapeHtml(task.blocker_note || '-') + '</td>' +
    '</tr>'
  );
}).join('');
          }
          
          function clearHiddenWaitingOn() {
  window.__waitingOn = '';
}

document.getElementById('assignee')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('business')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('area')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('status')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('priority')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('blocked')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('overdue')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('progressBucket')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('search')?.addEventListener('input', clearHiddenWaitingOn);


loadUsers()
  .then(() => {
    applyFiltersFromUrl();
    return loadTasks();
  })
  .catch((error) => {
    console.error('Tasks page init failed:', error);
    const status = document.getElementById('statusText');
    if (status) {
      status.textContent = 'Failed to initialize tasks page';
    }
  });

setInterval(() => {
  loadTasks().catch((error) => {
    console.error('Periodic loadTasks failed:', error);
  });
}, 60000);

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    loadTasks().catch((error) => {
      console.error('Visibility loadTasks failed:', error);
    });
  }
});
        </script>
      </body>
    </html>
  `);
});

app.get("/tasks/user/:userId", requireDashboardAuth, async (req, res) => {
  try {
    const userId = Number(req.params.userId);
    const tab = String(req.query.tab || "pending").trim();

    if (!Number.isFinite(userId)) {
      return res.status(400).type("html").send("Invalid user id");
    }

    const data = await getUserTaskWorkspaceData({
      userId,
      orgId: DASHBOARD_ORG_ID,
      tab,
    });

    if (!data) {
      return res.status(404).type("html").send("User not found");
    }

    res.status(200).type("html").send(renderUserTaskWorkspacePage(data));
  } catch (error) {
    console.error("User task workspace page error:", error);
    res.status(500).type("html").send(`
      <html>
        <head><title>User Task Workspace Error</title></head>
        <body>
          ${renderTopNav("tasks")}
          <pre>${escapeHtml(error?.stack || error?.message || String(error))}</pre>
        </body>
      </html>
    `);
  }
});

app.get("/attendance", requireDashboardAuth, async (_req, res) => {
  res.status(200).send(`
    <html>
      <head>
        <title>Attendance</title>
        <style>
  ${buildThemeCss()}
  ${buildBasePageCss()}
  ${buildTopNavCss()}

          .wrap {
            max-width: 1600px;
            margin: 0 auto;
            padding: 24px 18px 36px;
            position: relative;
            z-index: 1;
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

          .stats {
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 12px;
            margin-bottom: 20px;
          }

          .stat-card { padding: 14px; }
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

          .tabbar {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-bottom: 18px;
          }

          .tab-btn {
            appearance: none;
            border: 1px solid var(--line);
            background: rgba(255,255,255,0.04);
            color: var(--text);
            padding: 10px 14px;
            border-radius: 12px;
            cursor: pointer;
            font-weight: 700;
          }

          .tab-btn.active {
            background: var(--primary-soft);
            border-color: rgba(139,124,246,0.45);
          }

          .tab-panel { display: none; }
          .tab-panel.active { display: block; }

          .panel {
            padding: 18px;
            margin-bottom: 18px;
          }

          .grid-2 {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 18px;
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
            vertical-align: middle;
          }

          th {
            color: var(--muted);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
          }

          tr:hover td {
            background: rgba(255,255,255,0.03);
          }

          .person-link {
            cursor: pointer;
            text-decoration: underline;
            text-underline-offset: 2px;
          }

          .status-pill, .flag-pill {
            display: inline-flex;
            align-items: center;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 700;
            border: 1px solid rgba(255,255,255,0.12);
            margin-right: 6px;
            margin-bottom: 6px;
          }

          .status-login, .status-back { background: var(--success-soft); }
          .status-break { background: var(--accent-soft); }
          .status-logout { background: var(--neutral-soft); }
          .status-leave { background: var(--info-soft); }
          .status-no_update, .status-unknown { background: var(--dangerSoft); }

          .flag-danger { background: var(--danger-soft); }
          .flag-warn { background: var(--accent-soft); }
          .flag-info { background: var(--info-soft); }

          .alert-list {
            display: grid;
            gap: 12px;
          }

          .alert-item {
            padding: 14px;
            border-radius: 14px;
            border: 1px solid var(--line);
            background: rgba(255,255,255,0.03);
          }

          .loading-state, .error-state {
            color: var(--muted);
            padding: 20px 0;
          }

          .loading-overlay {
            position: fixed;
            inset: 0;
            background: rgba(8, 12, 22, 0.72);
            display: none;
            align-items: center;
            justify-content: center;
            z-index: 50;
          }

          .loading-overlay.show {
            display: flex;
          }

          .loading-card {
            background: linear-gradient(180deg, var(--panel), var(--panel-strong));
            border: 1px solid var(--line);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-soft);
            padding: 18px 22px;
            min-width: 260px;
            text-align: center;
          }
          
          .insight-section-title {
  font-size: 12px;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-weight: 800;
  margin-bottom: 10px;
}

.insight-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
}

.insight-card {
  background: rgba(255,255,255,0.03);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 16px;
  padding: 14px;
}

.insight-card-title {
  font-size: 13px;
  font-weight: 800;
  margin-bottom: 10px;
}

.insight-card-main {
  font-size: 24px;
  font-weight: 800;
  margin-bottom: 10px;
}

.insight-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.insight-line {
  font-size: 13px;
  color: var(--text);
  line-height: 1.45;
}

.insight-subtle {
  color: var(--muted);
  font-size: 12px;
}

@media (max-width: 1100px) {
  .insight-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 700px) {
  .insight-grid {
    grid-template-columns: 1fr;
  }
}

.grid-3 {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 16px;
}

@media (max-width: 1100px) {
  .grid-3 {
    grid-template-columns: 1fr;
  }
}

          .loading-spinner {
            width: 30px;
            height: 30px;
            border-radius: 50%;
            border: 3px solid rgba(255,255,255,0.16);
            border-top-color: var(--primary);
            margin: 0 auto 12px;
            animation: spin 0.8s linear infinite;
          }

          @keyframes spin {
            to { transform: rotate(360deg); }
          }

          @media (max-width: 1100px) {
            .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .grid-2 { grid-template-columns: 1fr; }
          }
        </style>
      </head>
      <body>
      ${renderTopNav("attendance")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">WeSolveHR</div>
              <h1>Attendance</h1>
              <div class="subtitle">Team attendance overview and exceptions</div>
            </div>
          </div>

          <div id="statsGrid" class="stats">
            <div class="stat-card"><div class="stat-label">Loading</div><div class="stat-value">...</div><div class="stat-note">Fetching attendance</div></div>
          </div>

          <div class="tabbar">
            <button class="tab-btn active" data-tab="overview">Live Overview</button>
            <button class="tab-btn" data-tab="exceptions">Late & Exceptions</button>
            <button class="tab-btn" data-tab="leave">Leave & No Update</button>
            <button class="tab-btn" data-tab="summary">Team Summary</button>
          </div>

<div class="grid-3">
  <div class="panel">
    <h2 style="margin-top:0;">Needs attention now</h2>
    <div id="attentionNow" class="alert-list">
      <div class="loading-state">Loading...</div>
    </div>
  </div>

  <div class="panel">
    <h2 style="margin-top:0;">Careless login</h2>
    <div id="carelessLoginList" class="alert-list">
      <div class="loading-state">Loading...</div>
    </div>
  </div>

  <div class="panel">
    <h2 style="margin-top:0;">Live grouped view</h2>
    <div id="liveGroups" class="alert-list">
      <div class="loading-state">Loading...</div>
    </div>
  </div>
</div>
            
            <div class="panel">
  <h2 style="margin-top:0;">Weekly & Monthly Insights</h2>

  <div class="insight-section">
    <div class="insight-section-title">This week</div>
    <div id="weeklyInsightsGrid" class="insight-grid">
      <div class="loading-state">Loading weekly insights...</div>
    </div>
  </div>

  <div class="insight-section" style="margin-top:18px;">
    <div class="insight-section-title">This month</div>
    <div id="monthlyInsightsGrid" class="insight-grid">
      <div class="loading-state">Loading monthly insights...</div>
    </div>
  </div>
</div>

            <div class="panel">
              <h2 style="margin-top:0;">Live employee table</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
<th>Name</th>
<th>Role</th>
<th>Expected Login</th>
<th>Current Status</th>
<th>Since</th>
<th>Worked Today</th>
<th>Break Today</th>
<th>First Login</th>
<th>Late</th>
<th>Leave</th>
<th>Flags</th>
                    </tr>
                  </thead>
                  <tbody id="attendanceTableBody">
<tr><td colspan="11" class="loading-state">Loading attendance...</td></tr>
</tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-exceptions" class="tab-panel">
            <div class="panel">
              <h2 style="margin-top:0;">Late & exception cases</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
<th>Name</th>
<th>Role</th>
<th>Expected Login</th>
<th>Status</th>
<th>Worked</th>
<th>Break</th>
<th>First Login</th>
<th>Late</th>
<th>Flags</th>
                    </tr>
                  </thead>
                  <tbody id="exceptionsTableBody">
                    <tr><td colspan="9" class="loading-state">Loading exceptions...</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-leave" class="tab-panel">
            <div class="grid-2">
              <div class="panel">
                <h2 style="margin-top:0;">On leave today</h2>
                <div id="leaveList" class="alert-list">
                  <div class="loading-state">Loading...</div>
                </div>
              </div>

              <div class="panel">
                <h2 style="margin-top:0;">No update yet</h2>
                <div id="noUpdateList" class="alert-list">
                  <div class="loading-state">Loading...</div>
                </div>
              </div>
            </div>
          </div>

          <div id="tab-summary" class="tab-panel">
            <div class="panel">
              <h2 style="margin-top:0;">Team summary</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Role</th>
                      <th>Status</th>
                      <th>Worked</th>
                      <th>Break</th>
                      <th>First Login</th>
                      <th>Late</th>
                      <th>Flags</th>
                    </tr>
                  </thead>
                  <tbody id="summaryTableBody">
                    <tr><td colspan="8" class="loading-state">Loading summary...</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>

        <div id="pageLoadingOverlay" class="loading-overlay">
          <div class="loading-card">
            <div class="loading-spinner"></div>
            <div style="font-weight:700;">Opening attendance details...</div>
          </div>
        </div>

        <script>
          function escapeHtmlClient(value) {
            return String(value ?? '')
              .replace(/&/g, '&amp;')
              .replace(/</g, '&lt;')
              .replace(/>/g, '&gt;')
              .replace(/"/g, '&quot;');
          }

          function statusPill(status) {
            const safe = String(status || 'unknown');
            const cls = 'status-pill status-' + safe;
            return '<span class="' + cls + '">' + escapeHtmlClient(safe) + '</span>';
          }

          function flagPills(flags) {
            if (!flags || !flags.length) return '-';

            return flags.map((flag) => {
              let cls = 'flag-pill flag-info';
              if (
                flag === 'Late not approved' ||
                flag === 'Long shift'
              ) cls = 'flag-pill flag-danger';
              else if (
                flag === 'Long break' ||
                flag === 'Time unsure'
              ) cls = 'flag-pill flag-warn';

              return '<span class="' + cls + '">' + escapeHtmlClient(flag) + '</span>';
            }).join(' ');
          }

          function employeeLink(userId, name) {
            return '<span class="person-link" onclick="openAttendanceDetail(' + Number(userId) + ')">' + escapeHtmlClient(name) + '</span>';
          }

          function openAttendanceDetail(userId) {
            const overlay = document.getElementById('pageLoadingOverlay');
            if (overlay) overlay.classList.add('show');
            setTimeout(() => {
              window.location.href = '/attendance/' + userId;
            }, 80);
          }
          
          function renderInsightLines(items, emptyText = '-') {
  if (!items || !items.length) {
    return '<div class="insight-subtle">' + escapeHtmlClient(emptyText) + '</div>';
  }

  return '<div class="insight-list">' + items.map((item) => {
    return '<div class="insight-line">' + escapeHtmlClient(item) + '</div>';
  }).join('') + '</div>';
}

function renderInsightsGrid(target, cards) {
  if (!target) return;

  target.innerHTML = cards.map((card) => {
    return '<div class="insight-card">' +
      '<div class="insight-card-title">' + escapeHtmlClient(card.title) + '</div>' +
      '<div class="insight-card-main">' + escapeHtmlClient(card.main ?? '-') + '</div>' +
      renderInsightLines(card.lines || [], 'No data yet') +
    '</div>';
  }).join('');
}

          async function loadAttendancePage() {
            const statsGrid = document.getElementById('statsGrid');
            const tableBody = document.getElementById('attendanceTableBody');
            const exceptionsBody = document.getElementById('exceptionsTableBody');
            const summaryBody = document.getElementById('summaryTableBody');
            const attentionNow = document.getElementById('attentionNow');
            const carelessLoginList = document.getElementById('carelessLoginList');
            const liveGroups = document.getElementById('liveGroups');
            const leaveList = document.getElementById('leaveList');
            const noUpdateList = document.getElementById('noUpdateList');
            const weeklyInsightsGrid = document.getElementById('weeklyInsightsGrid');
const monthlyInsightsGrid = document.getElementById('monthlyInsightsGrid');

            try {
const res = await fetch('/api/attendance');
const contentType = res.headers.get('content-type') || '';

if (!contentType.includes('application/json')) {
  const text = await res.text();
  throw new Error('Attendance API returned HTML instead of JSON');
}

const json = await res.json();

              if (!json.ok) {
                throw new Error(json.error || 'Failed to load attendance');
              }

              const data = json.data || {};
              const summary = data.summary || {};
              const groups = data.groups || {};
const rows = data.rows || [];
const carelessRows = rows.filter((row) =>
  row.role !== 'admin' &&
  Array.isArray(row.flags) &&
  row.flags.includes('Long shift')
);

              const cards = [
                ['Logged in now', summary.logged_in_now ?? 0, 'Working currently'],
                ['On break now', summary.on_break_now ?? 0, 'Currently on break'],
                ['Not logged in yet', summary.not_logged_in_yet ?? 0, 'No attendance update'],
                ['On leave today', summary.on_leave_today ?? 0, 'Planned leave'],
                ['Late today', summary.late_today ?? 0, 'All late categories'],
                ['Approved late', summary.approved_late ?? 0, 'Prior info approved'],
                ['Late not approved', summary.unapproved_late ?? 0, 'Needs attention'],
                ['No prior info', summary.no_prior_info_late ?? 0, 'Joined late directly'],
                ['Long breaks', summary.long_break_flags ?? 0, 'Break exception'],
['Careless login', summary.long_shift_flags ?? 0, 'Worked above 10h, likely wrong entry'],              ];

              statsGrid.innerHTML = cards.map((card) => {
                return '<div class="stat-card">' +
                  '<div class="stat-label">' + escapeHtmlClient(card[0]) + '</div>' +
                  '<div class="stat-value">' + escapeHtmlClient(card[1]) + '</div>' +
                  '<div class="stat-note">' + escapeHtmlClient(card[2]) + '</div>' +
                '</div>';
              }).join('');

              const sortedRows = [...rows].sort((a, b) => {
                const aRisk = (a.flags?.length || 0) + (a.late_status === 'Not approved' ? 2 : 0) + (a.late_status === 'No prior info' ? 2 : 0);
                const bRisk = (b.flags?.length || 0) + (b.late_status === 'Not approved' ? 2 : 0) + (b.late_status === 'No prior info' ? 2 : 0);
                return bRisk - aRisk;
              });

tableBody.innerHTML = sortedRows.map((row) => {
  return '<tr>' +
    '<td>' + employeeLink(row.user_id, row.name) + '</td>' +
    '<td>' + escapeHtmlClient(row.role || '-') + '</td>' +
    '<td>' + escapeHtmlClient(row.expected_shift_start_text || '-') + '</td>' +
    '<td>' + statusPill(row.status) + '</td>' +
    '<td>' + escapeHtmlClient(row.since_text || '-') + '</td>' +
    '<td>' + escapeHtmlClient(row.worked_today_text || '-') + '</td>' +
    '<td>' + escapeHtmlClient(row.break_today_text || '-') + '</td>' +
    '<td>' + escapeHtmlClient(row.first_login_text || '-') + '</td>' +
    '<td>' + escapeHtmlClient(row.late_status || '-') + '</td>' +
    '<td>' + (row.is_on_leave ? 'Yes' : 'No') + '</td>' +
    '<td>' + flagPills(row.flags || []) + '</td>' +
  '</tr>';
}).join('') || '<tr><td colspan="11" class="empty-cell">No attendance data found</td></tr>';
              const exceptionRows = rows.filter((row) =>
                (row.flags && row.flags.length) ||
                row.late_status === 'Not approved' ||
                row.late_status === 'No prior info'
              );

              exceptionsBody.innerHTML = exceptionRows.map((row) => {
                return '<tr>' +
                  '<td>' + employeeLink(row.user_id, row.name) + '</td>' +
                  '<td>' + statusPill(row.status) + '</td>' +
                  '<td>' + escapeHtmlClient(row.late_status || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.expected_shift_start_text || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.worked_today_text || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.break_today_text || '-') + '</td>' +
                  '<td>' + flagPills(row.flags || []) + '</td>' +
                '</tr>';
              }).join('') || '<tr><td colspan="7" class="empty-cell">No exceptions today</td></tr>';

              summaryBody.innerHTML = rows.map((row) => {
                return '<tr>' +
                  '<td>' + employeeLink(row.user_id, row.name) + '</td>' +
                  '<td>' + escapeHtmlClient(row.role || '-') + '</td>' +
                  '<td>' + statusPill(row.status) + '</td>' +
                  '<td>' + escapeHtmlClient(row.worked_today_text || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.break_today_text || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.first_login_text || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.late_status || '-') + '</td>' +
                  '<td>' + flagPills(row.flags || []) + '</td>' +
                '</tr>';
              }).join('') || '<tr><td colspan="8" class="empty-cell">No summary data</td></tr>';

const attentionItems = [];
if ((summary.unapproved_late ?? 0) > 0) {
  attentionItems.push('Late not approved: ' + summary.unapproved_late);
}
if ((summary.no_prior_info_late ?? 0) > 0) {
  attentionItems.push('Late without prior info: ' + summary.no_prior_info_late);
}
if ((summary.long_break_flags ?? 0) > 0) {
  attentionItems.push('Long break flags: ' + summary.long_break_flags);
}
if ((summary.long_shift_flags ?? 0) > 0) {
  attentionItems.push('Careless login: ' + summary.long_shift_flags);
}
if ((summary.not_logged_in_yet ?? 0) > 0) {
  attentionItems.push('No attendance update yet: ' + summary.not_logged_in_yet);
}

attentionNow.innerHTML = attentionItems.length
  ? attentionItems.map((item) => '<div class="alert-item">' + escapeHtmlClient(item) + '</div>').join('')
  : '<div class="alert-item">No immediate issues right now</div>';
          
if (carelessLoginList) {
  carelessLoginList.innerHTML = carelessRows.length
    ? carelessRows.map((row) => {
        return '<div class="alert-item">' +
          '<strong>' + employeeLink(row.user_id, row.name) + '</strong><br>' +
          'Worked: ' + escapeHtmlClient(row.worked_today_text || '-') +
          '<br><span class="muted">Likely incorrect attendance entry</span>' +
        '</div>';
      }).join('')
    : '<div class="alert-item">No careless login issues today</div>';
}

liveGroups.innerHTML = [
  '<div class="alert-item"><strong>On break now:</strong><br>' +
    ((groups.on_break_now || []).map((x) => employeeLink(x.user_id, x.name)).join('<br>') || 'None') +
  '</div>',

  '<div class="alert-item"><strong>Expected late:</strong><br>' +
    ((groups.expected_late || []).map((x) =>
      employeeLink(x.user_id, x.name) + ' (' + escapeHtmlClient(x.late_expected_login_text || '-') + ')'
    ).join('<br>') || 'None') +
  '</div>',

  '<div class="alert-item"><strong>No update yet:</strong><br>' +
    ((groups.no_update_yet || []).map((x) => employeeLink(x.user_id, x.name)).join('<br>') || 'None') +
  '</div>',

  '<div class="alert-item"><strong>On leave today:</strong><br>' +
    ((groups.on_leave_today || []).map((x) => employeeLink(x.user_id, x.name)).join('<br>') || 'None') +
  '</div>'
].join('');

              leaveList.innerHTML = (groups.on_leave_today || []).length
                ? (groups.on_leave_today || []).map((x) => '<div class="alert-item">' + employeeLink(x.user_id, x.name) + '</div>').join('')
                : '<div class="alert-item">Nobody is on leave today</div>';

noUpdateList.innerHTML = (groups.no_update_yet || []).length
  ? (groups.no_update_yet || []).map((x) => '<div class="alert-item">' + employeeLink(x.user_id, x.name) + '</div>').join('')
  : '<div class="alert-item">Everyone has updated attendance</div>';

            } catch (error) {
              console.error('Attendance page load failed:', error);
              statsGrid.innerHTML = '<div class="stat-card"><div class="stat-label">Error</div><div class="stat-value">!</div><div class="stat-note">' + escapeHtmlClient(error.message || 'Failed to load') + '</div></div>';
              tableBody.innerHTML = '<tr><td colspan="10" class="error-state">Failed to load attendance</td></tr>';
              exceptionsBody.innerHTML = '<tr><td colspan="7" class="error-state">Failed to load attendance</td></tr>';
              summaryBody.innerHTML = '<tr><td colspan="8" class="error-state">Failed to load attendance</td></tr>';
              attentionNow.innerHTML = '<div class="alert-item">Failed to load attendance</div>';
              liveGroups.innerHTML = '<div class="alert-item">Failed to load attendance</div>';
              leaveList.innerHTML = '<div class="alert-item">Failed to load attendance</div>';
              noUpdateList.innerHTML = '<div class="alert-item">Failed to load attendance</div>';
              if (carelessLoginList) {
  carelessLoginList.innerHTML = '<div class="alert-item">Failed to load attendance</div>';
}
            }
          }

async function loadAttendanceInsights() {
  const weeklyInsightsGrid = document.getElementById('weeklyInsightsGrid');
  const monthlyInsightsGrid = document.getElementById('monthlyInsightsGrid');

  if (!weeklyInsightsGrid || !monthlyInsightsGrid) return;

  try {
const res = await fetch('/api/attendance/insights');
const contentType = res.headers.get('content-type') || '';

if (!contentType.includes('application/json')) {
  const text = await res.text();
  throw new Error('Attendance insights API returned HTML instead of JSON');
}

const json = await res.json();

    if (!json.ok) {
      throw new Error(json.error || 'Failed to load attendance insights');
    }

    const data = json.data || {};
    const weekly = data.weekly || {};
    const monthly = data.monthly || {};

const weeklyCards = [
  {
    title: 'Most late this week',
    main: weekly.most_late_count_text ?? '-',
    lines: weekly.most_late_lines || [],
  },
  {
    title: 'Best attendance streak',
    main: weekly.best_streak_text ?? '-',
    lines: weekly.best_streak_lines || [],
  },
  {
    title: 'Most break time this week',
    main: weekly.most_break_time_text ?? '-',
    lines: weekly.most_break_time_lines || [],
  },
  {
    title: 'Careless login this week',
    main: weekly.careless_login_text ?? '-',
    lines: weekly.careless_login_lines || [],
  },
];

const monthlyCards = [
  {
    title: 'Attendance leaders',
    main: monthly.attendance_leaders_text ?? '-',
    lines: monthly.attendance_leader_lines || [],
  },
  {
    title: 'Needs attention',
    main: monthly.needs_attention_text ?? '-',
    lines: monthly.needs_attention_lines || [],
  },
  {
    title: 'Most late this month',
    main: monthly.most_late_text ?? '-',
    lines: monthly.most_late_lines || [],
  },
  {
    title: 'Most leave this month',
    main: monthly.most_leave_text ?? '-',
    lines: monthly.most_leave_lines || [],
  },
  {
    title: 'Careless login this month',
    main: monthly.careless_login_text ?? '-',
    lines: monthly.careless_login_lines || [],
  },
];

    renderInsightsGrid(weeklyInsightsGrid, weeklyCards);
    renderInsightsGrid(monthlyInsightsGrid, monthlyCards);
  } catch (error) {
    console.error('Attendance insights load failed:', error);

    weeklyInsightsGrid.innerHTML =
      '<div class="insight-card">' +
        '<div class="insight-card-title">This week</div>' +
        '<div class="insight-card-main">Failed</div>' +
        '<div class="insight-subtle">' + escapeHtmlClient(error.message || 'Failed to load') + '</div>' +
      '</div>';

    monthlyInsightsGrid.innerHTML =
      '<div class="insight-card">' +
        '<div class="insight-card-title">This month</div>' +
        '<div class="insight-card-main">Failed</div>' +
        '<div class="insight-subtle">' + escapeHtmlClient(error.message || 'Failed to load') + '</div>' +
      '</div>';
  }
}

          const tabButtons = document.querySelectorAll('.tab-btn');
          const tabPanels = document.querySelectorAll('.tab-panel');

          tabButtons.forEach((btn) => {
            btn.addEventListener('click', () => {
              const tab = btn.dataset.tab;
              tabButtons.forEach((b) => b.classList.remove('active'));
              tabPanels.forEach((p) => p.classList.remove('active'));
              btn.classList.add('active');
              const panel = document.getElementById('tab-' + tab);
              if (panel) panel.classList.add('active');
            });
          });

loadAttendancePage();
loadAttendanceInsights();

setInterval(() => {
  loadAttendancePage().catch((error) => {
    console.error('Periodic attendance load failed:', error);
  });
}, 60000);

setInterval(() => {
  loadAttendanceInsights().catch((error) => {
    console.error('Periodic attendance insights load failed:', error);
  });
}, 5 * 60000);

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    loadAttendancePage().catch((error) => {
      console.error('Visibility attendance load failed:', error);
    });

    loadAttendanceInsights().catch((error) => {
      console.error('Visibility attendance insights load failed:', error);
    });
  }
});
        </script>
      </body>
    </html>
  `);
});

app.get("/api/attendance/insights", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.loggedInUser?.org_id || DASHBOARD_ORG_ID;
    const data = await getAttendanceInsightsData(orgId);
    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("attendance insights api error:", error);
    return sendApiError(res, 500, "Failed to load attendance insights");
  }
});

app.get("/logs", requireDashboardAuth, async (_req, res) => {
  res.status(200).send(`
    <html>
      <head>
        <title>Logs</title>
        <style>
  ${buildThemeCss()}
  ${buildBasePageCss()}
  ${buildTopNavCss()}

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

.log-badge {
  display: inline-flex;
  align-items: center;
  padding: 6px 10px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 800;
  border: 1px solid rgba(255,255,255,0.10);
  white-space: nowrap;
}

.log-badge-success {
  background: rgba(88, 201, 138, 0.16);
  color: #c9ffe0;
  border-color: rgba(88, 201, 138, 0.28);
}

.log-badge-danger {
  background: rgba(239, 107, 115, 0.16);
  color: #ffd7da;
  border-color: rgba(239, 107, 115, 0.28);
}

.log-badge-warn {
  background: rgba(243, 181, 98, 0.16);
  color: #ffe4b8;
  border-color: rgba(243, 181, 98, 0.28);
}

.log-badge-muted {
  background: rgba(170, 182, 207, 0.14);
  color: #d9e1f2;
  border-color: rgba(170, 182, 207, 0.22);
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
          
          .stats-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 16px;
}

.log-stat-card {
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  border-radius: var(--radius-lg);
  padding: 14px;
  box-shadow: var(--shadow-soft);
}

.log-stat-label {
  color: var(--muted);
  font-size: 11px;
  font-weight: 900;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.log-stat-value {
  margin-top: 8px;
  font-size: 26px;
  font-weight: 900;
}

.filters-panel {
  margin-bottom: 16px;
}

.filters-grid {
  display: grid;
  grid-template-columns: 1.4fr 1.1fr 0.9fr 0.9fr 0.9fr auto auto;
  gap: 10px;
}

.filters-grid input,
.filters-grid select,
.filters-grid button {
  padding: 10px 11px;
  border-radius: 12px;
  border: 1px solid var(--line);
  background: rgba(255,255,255,0.04);
  color: var(--text);
  font: inherit;
}

.filters-grid button {
  cursor: pointer;
  font-weight: 900;
  background: var(--primary-soft);
}

.command-cell {
  max-width: 520px;
}

.command-preview {
  max-height: 44px;
  overflow: hidden;
  line-height: 1.45;
  white-space: pre-wrap;
}

.command-full {
  display: none;
  margin-top: 8px;
  padding: 10px;
  border-radius: 12px;
  background: rgba(255,255,255,0.05);
  white-space: pre-wrap;
  line-height: 1.5;
}

.command-full.open {
  display: block;
}

.mini-link {
  margin-top: 6px;
  border: 0;
  background: transparent;
  color: var(--primary);
  font-weight: 900;
  cursor: pointer;
  padding: 0;
}

.exception-pill {
  display: inline-flex;
  max-width: 240px;
  padding: 6px 9px;
  border-radius: 999px;
  background: var(--danger-soft);
  color: #ffd7da;
  font-size: 12px;
  font-weight: 900;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.exception-none {
  color: var(--muted);
}

.sid-small {
  font-size: 11px;
  color: var(--muted);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}

.person-stats {
  margin: 0 0 16px;
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}

.person-stat-box {
  background: rgba(255,255,255,0.035);
  border: 1px solid var(--line);
  border-radius: var(--radius-md);
  padding: 12px;
}

.person-stat-title {
  font-weight: 900;
  margin-bottom: 8px;
}

.person-chip {
  display: inline-flex;
  margin: 4px;
  padding: 7px 9px;
  border-radius: 999px;
  background: rgba(255,255,255,0.06);
  border: 1px solid rgba(255,255,255,0.10);
  font-size: 12px;
  font-weight: 800;
}

@media (max-width: 1100px) {
  .stats-grid,
  .person-stats {
    grid-template-columns: 1fr 1fr;
  }

  .filters-grid {
    grid-template-columns: 1fr 1fr;
  }
}

@media (max-width: 700px) {
  .stats-grid,
  .person-stats,
  .filters-grid {
    grid-template-columns: 1fr;
  }
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
      ${renderTopNav("logs")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Message Logging</div>
              <h1>WeSolveHR // Logs Console</h1>
              <div class="subtitle">Inbound command visibility for tracing, debugging, and audit review</div>
            </div>
          </div>

<div class="stats-grid" id="logsStats"></div>

<div class="panel filters-panel">
  <div class="filters-grid">
    <input id="filterSearch" placeholder="Search message or SID" />
    <input id="filterUser" placeholder="Filter by username / phone" />
    <select id="filterOutcome">
      <option value="">All outcomes</option>
      <option value="completed">Completed</option>
      <option value="failed">Failed</option>
      <option value="processing">Processing</option>
      <option value="unknown">Unknown</option>
    </select>
    <input id="filterDay" type="date" />
    <input id="filterMonth" type="month" />
    <button onclick="loadLogs()">Apply</button>
    <button onclick="clearLogFilters()">Clear</button>
  </div>
</div>

<div class="panel">
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Time</th>
          <th>Sender</th>
          <th>Command</th>
          <th>Outcome</th>
          <th>Type</th>
          <th>Exception</th>
          <th>SID</th>
        </tr>
      </thead>
      <tbody id="logRows"></tbody>
    </table>
  </div>
</div>
        </div>

        <script>


function escapeHtmlClient(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function getOutcomeBadgeClass(status) {
  const s = String(status || "").toLowerCase();

  if (s === "completed") return "log-badge log-badge-success";
  if (s === "failed") return "log-badge log-badge-danger";
  if (s === "processing") return "log-badge log-badge-warn";
  return "log-badge log-badge-muted";
}

function truncateText(text, limit = 120) {
  const value = String(text || "").trim();
  if (value.length <= limit) return value;
  return value.slice(0, limit) + "…";
}

function renderPersonStats(title, obj) {
  const entries = Object.entries(obj || {}).sort((a, b) => b[1] - a[1]);

  if (!entries.length) {
    return (
      '<div class="person-stat-box">' +
        '<div class="person-stat-title">' + escapeHtmlClient(title) + '</div>' +
        '<div class="muted">No commands</div>' +
      '</div>'
    );
  }

  return (
    '<div class="person-stat-box">' +
      '<div class="person-stat-title">' + escapeHtmlClient(title) + '</div>' +
      entries
        .map(function ([name, count]) {
          return '<span class="person-chip">' + escapeHtmlClient(name) + ': ' + count + '</span>';
        })
        .join("") +
    '</div>'
  );
}


function toggleCommand(id) {
  const el = document.getElementById("commandFull-" + id);
  if (el) el.classList.toggle("open");
}

function clearLogFilters() {
  document.getElementById("filterSearch").value = "";
  document.getElementById("filterUser").value = "";
  document.getElementById("filterOutcome").value = "";
  document.getElementById("filterDay").value = "";
  document.getElementById("filterMonth").value = "";
  loadLogs();
}

async function loadLogs() {
  try {
    const params = new URLSearchParams();

    const q = document.getElementById("filterSearch")?.value || "";
    const user = document.getElementById("filterUser")?.value || "";
    const outcome = document.getElementById("filterOutcome")?.value || "";
    const day = document.getElementById("filterDay")?.value || "";
    const month = document.getElementById("filterMonth")?.value || "";

    if (q.trim()) params.set("q", q.trim());
    if (user.trim()) params.set("user", user.trim());
    if (outcome) params.set("outcome", outcome);
    if (day) params.set("day", day);
    if (month) params.set("month", month);

    const res = await fetch("/api/logs?" + params.toString());
    const json = await res.json();

    if (!json.ok) return;

    const payload = json.data || {};
    const rows = payload.rows || [];
    const stats = payload.stats || {};

document.getElementById("logsStats").innerHTML =
  '<div class="log-stat-card">' +
    '<div class="log-stat-label">Loaded Logs</div>' +
    '<div class="log-stat-value">' + (stats.total || 0) + '</div>' +
  '</div>' +
  '<div class="log-stat-card">' +
    '<div class="log-stat-label">Completed</div>' +
    '<div class="log-stat-value">' + (stats.completed || 0) + '</div>' +
  '</div>' +
  '<div class="log-stat-card">' +
    '<div class="log-stat-label">Failed</div>' +
    '<div class="log-stat-value">' + (stats.failed || 0) + '</div>' +
  '</div>' +
  '<div class="log-stat-card">' +
    '<div class="log-stat-label">Unknown</div>' +
    '<div class="log-stat-value">' + (stats.unknown || 0) + '</div>' +
  '</div>' +
  '<div class="person-stats" style="grid-column: 1 / -1;">' +
    renderPersonStats("Commands Today by Person", stats.byPersonToday) +
    renderPersonStats("Commands This Month by Person", stats.byPersonMonth) +
  '</div>';

    document.getElementById("logRows").innerHTML = rows.length
      ? rows
          .map((row) => {
            const id = Number(row.id);
            const fullCommand = escapeHtmlClient(row.body || "-");
            const shortCommand = escapeHtmlClient(truncateText(row.body || "-", 130));
            const error = row.outcome_error || "";

return (
  '<tr>' +
    '<td>' + escapeHtmlClient(row.created_at_text || row.created_at || "") + '</td>' +
    '<td><strong>' + escapeHtmlClient(row.sender || "") + '</strong></td>' +
    '<td class="command-cell">' +
      '<div class="command-preview">' + shortCommand + '</div>' +
      (
        String(row.body || "").length > 130
          ? '<button class="mini-link" onclick="toggleCommand(' + id + ')">View full</button>' +
            '<div id="commandFull-' + id + '" class="command-full">' + fullCommand + '</div>'
          : ''
      ) +
    '</td>' +
    '<td>' +
      '<span class="' + getOutcomeBadgeClass(row.outcome_status) + '">' +
        escapeHtmlClient(row.outcome_status || "-") +
      '</span>' +
    '</td>' +
    '<td>' + escapeHtmlClient(row.outcome_result_type || "-") + '</td>' +
    '<td>' +
      (
        error
          ? '<span class="exception-pill" title="' + escapeHtmlClient(error) + '">' +
              escapeHtmlClient(error) +
            '</span>'
          : '<span class="exception-none">-</span>'
      ) +
    '</td>' +
    '<td class="sid-small" title="' + escapeHtmlClient(row.message_sid || "-") + '">' +
      escapeHtmlClient(truncateText(row.message_sid || "-", 14)) +
    '</td>' +
  '</tr>'
);
          })
          .join("")
      : '<tr><td colspan="7" class="empty-cell">No logs found.</td></tr>';
  } catch (err) {
    console.error("Failed to load logs:", err);
  }
}

["filterSearch", "filterUser"].forEach((id) => {
  document.addEventListener("input", function (event) {
    if (event.target && event.target.id === id) {
      clearTimeout(window.__logsFilterTimer);
      window.__logsFilterTimer = setTimeout(loadLogs, 450);
    }
  });
});

["filterOutcome", "filterDay", "filterMonth"].forEach((id) => {
  document.addEventListener("change", function (event) {
    if (event.target && event.target.id === id) {
      loadLogs();
    }
  });
});

loadLogs();

setInterval(loadLogs, 60000);

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    loadLogs();
  }
});

        </script>
      </body>
    </html>
  `);
});

app.get("/api/logs", requireDashboardAuth, async (req, res) => {
  try {
    const data = await getLogsPageData(null, {
      q: req.query.q,
      user: req.query.user,
      outcome: req.query.outcome,
      day: req.query.day,
      month: req.query.month,
    });

    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("API /api/logs error:", error);
    return sendApiError(res, 500, error?.message || "Failed to load logs");
  }
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

    const leadCommand = parseLeadUploadCommand(body);

    if (leadCommand?.error) {
      await logParse({
        intentDetected: "lead_upload_command",
        parserUsed: "parseLeadUploadCommand",
        parsedJson: leadCommand,
        validationPassed: false,
        validationError: "invalid_lead_upload_command",
        actionTaken: "reply_lead_command_error",
      });

      return sendTwiml(res, leadCommand.error);
    }

    if (leadCommand) {
      await logParse({
        intentDetected: "lead_upload_command",
        parserUsed: "parseLeadUploadCommand",
        parsedJson: leadCommand,
        validationPassed: true,
        validationError: null,
        actionTaken: "create_lead_upload_session",
      });

      return runInboundAction({
        successType: "lead_upload_session_created",
        failureType: "lead_upload_session_failed",
        action: async () => {
          await createLeadUploadSession({
            orgId: resolvedOrgId,
            senderPhone: from,
            business: leadCommand.business,
            leadPhone: leadCommand.lead_phone,
            userId: user?.id,
            spokeToName: leadCommand.spoke_to_name,
          });

          return sendTwiml(
            res,
            [
              "✅ Ready for lead voice upload.",
              `Business: ${leadCommand.business}`,
              `Lead phone: ${leadCommand.lead_phone}`,
              leadCommand.spoke_to_name
                ? `Spoke to: ${leadCommand.spoke_to_name}`
                : null,
              "",
              "Now send the voice note within 10 minutes.",
            ]
              .filter(Boolean)
              .join("\n"),
          );
        },
      });
    }

    const media = getTwilioMediaFromRequest(req);

    if (media) {
      const activeLeadSession = await getActiveLeadUploadSession({
        orgId: resolvedOrgId,
        senderPhone: from,
      });

      if (!activeLeadSession) {
        return sendTwiml(
          res,
          [
            "❌ I received media, but I do not know which lead it belongs to.",
            "",
            "First send:",
            "lead rasset upload +14085551234",
            "",
            "Then send the voice note.",
          ].join("\n"),
        );
      }

      if (!isAudioMedia(media.media_content_type)) {
        return sendTwiml(
          res,
          [
            "❌ I received media, but it does not look like a voice note.",
            `Type received: ${media.media_content_type || "unknown"}`,
            "",
            "Please send a WhatsApp voice note.",
          ].join("\n"),
        );
      }

      return runInboundAction({
        successType: "lead_voice_received",
        failureType: "lead_voice_save_failed",
        action: async () => {
          const savedLead = await saveLeadVoiceUpload({
            orgId: resolvedOrgId,
            business: activeLeadSession.business,
            leadPhone: activeLeadSession.lead_phone,
            senderPhone: from,
            uploadedByUserId: user?.id,
            twilioMessageSid: messageSid,
            mediaUrl: media.media_url,
            mediaContentType: media.media_content_type,
            spokeToName: activeLeadSession.spoke_to_name,
          });

          await markLeadUploadSessionCompleted(activeLeadSession.id);

          return sendTwiml(
            res,
            [
              "✅ Lead voice received.",
              `Business: ${activeLeadSession.business}`,
              `Lead phone: ${activeLeadSession.lead_phone}`,
              `Lead voice ID: ${savedLead.id}`,
              "Status: pending transcription",
            ].join("\n"),
          );
        },
      });
    }

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

    const passwordCommand = parseChangePasswordCommand(body);

    if (passwordCommand) {
      const passwordHash = await bcrypt.hash(passwordCommand.newPassword, 10);

      await supabase
        .from("users")
        .update({
          password_hash: passwordHash,
        })
        .eq("id", user.id);

      return sendTwiml(
        res,
        "✅ Password changed successfully. You can now use it for web login.",
      );
    }

    const employeeSummaryCommand = parseEmployeeSummaryCommand(body);
    if (employeeSummaryCommand) {
      return runInboundAction({
        successType: "attendance_query",
        failureType: "attendance_query_failed",
        action: () => handleEmployeeSummary(res, user, employeeSummaryCommand),
      });
    }

    const companyOffCommand = parseCompanyOffCommand(body);
    if (companyOffCommand) {
      return runInboundAction({
        successType: "attendance_updated",
        failureType: "attendance_update_failed",
        action: () => handleCompanyOffDay(res, user, companyOffCommand),
      });
    }

    const feedbackCommand = parseFeedbackCommand(body);
    if (feedbackCommand) {
      return await handleFeedbackCommand(res, user, feedbackCommand);
    }

    const appraisalCommand = parseAppraisalCommand(body);
    if (appraisalCommand) {
      return await handleAppraisalCommand(res, user, appraisalCommand);
    }

    const waitTaskCommand = parseWaitTaskCommand(body);
    const clearWaitTaskCommand = parseClearWaitTaskCommand(body);

    if (waitTaskCommand) {
      await logParse({
        intentDetected: "task_wait",
        parserUsed: "parseWaitTaskCommand",
        parsedJson: waitTaskCommand,
      });

      return runInboundAction({
        successType: "task_wait",
        successRefId: waitTaskCommand.taskId,
        action: () => handleWaitTask(res, user, waitTaskCommand),
      });
    }

    if (clearWaitTaskCommand) {
      await logParse({
        intentDetected: "task_clear_wait",
        parserUsed: "parseClearWaitTaskCommand",
        parsedJson: clearWaitTaskCommand,
      });

      return runInboundAction({
        successType: "task_clear_wait",
        successRefId: clearWaitTaskCommand.taskId,
        action: () =>
          handleUnblockTask(
            res,
            user,
            clearWaitTaskCommand.taskId,
            clearWaitTaskCommand.note,
          ),
      });
    }

    // if (clearWaitTaskCommand) {
    //   await logParse({
    //     intentDetected: "task_clear_wait",
    //     parserUsed: "parseClearWaitTaskCommand",
    //     parsedJson: clearWaitTaskCommand,
    //   });

    //   return runInboundAction({
    //     successType: "task_clear_wait",
    //     successRefId: clearWaitTaskCommand.taskId,
    //     action: () =>
    //       handleUnblockTask(
    //         res,
    //         user,
    //         clearWaitTaskCommand.taskId,
    //         clearWaitTaskCommand.note,
    //       ),
    //   });
    // }

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

    const workDayOverrideCommand = parseWorkDayOverrideCommand(body);
    if (workDayOverrideCommand) {
      return runInboundAction({
        successType: "attendance_update",
        failureType: "attendance_update_failed",
        action: () => handleWorkDayOverride(res, user, workDayOverrideCommand),
      });
    }

    const companyWorkDayOverrideCommand =
      parseCompanyWorkDayOverrideCommand(body);
    if (companyWorkDayOverrideCommand) {
      return runInboundAction({
        successType: "attendance_update",
        failureType: "attendance_update_failed",
        action: () =>
          handleCompanyWorkDayOverride(
            res,
            user,
            companyWorkDayOverrideCommand,
          ),
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
          const shiftStartIso = await getShiftStartIsoForUserToday(
            targetUser.id,
            user.org_id,
          );
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

app.post(
  "/api/leads/:id/transcribe",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const leadVoiceId = Number(req.params.id);

      if (!leadVoiceId) {
        return sendApiError(res, 400, "Invalid voice lead id");
      }

      const { data: voice, error: voiceError } = await supabase
        .from("lead_voice_uploads")
        .select("*")
        .eq("org_id", orgId)
        .eq("id", leadVoiceId)
        .maybeSingle();

      if (voiceError) throw voiceError;
      if (!voice) return sendApiError(res, 404, "Voice lead not found");
      if (!voice.media_url)
        return sendApiError(res, 400, "Voice lead has no media_url");

      await supabase
        .from("lead_voice_uploads")
        .update({
          status: "transcribing",
          updated_at: new Date().toISOString(),
        })
        .eq("org_id", orgId)
        .eq("id", leadVoiceId);

      const result = await transcribeLeadVoiceUploadById({
        leadVoiceId,
        orgId,
      });

      return sendApiSuccess(res, result || { message: "Transcribed" });
    } catch (error) {
      console.error("POST /api/leads/:id/transcribe error:", error);

      await supabase
        .from("lead_voice_uploads")
        .update({
          status: "pending_transcription",
          transcription_error: String(error.message || error),
          updated_at: new Date().toISOString(),
        })
        .eq("id", Number(req.params.id));

      return sendApiError(
        res,
        500,
        error.message || "Failed to transcribe voice lead",
      );
    }
  },
);

app.patch(
  "/api/leads/:id/transcript",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const leadVoiceId = Number(req.params.id);

      const cleanedTranscript = String(
        req.body.cleaned_transcript || "",
      ).trim();
      const translatedText = String(req.body.translated_text || "").trim();
      const reviewNotes = String(req.body.review_notes || "").trim();

      if (!leadVoiceId) {
        return sendApiError(res, 400, "Invalid lead voice ID");
      }

      if (!cleanedTranscript) {
        return sendApiError(res, 400, "Cleaned transcript is required");
      }

      const data = await updateLeadVoiceTranscript({
        leadVoiceId,
        orgId,
        cleanedTranscript,
        translatedText,
        reviewNotes,
      });

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("PATCH /api/leads/:id/transcript error:", error);
      return sendApiError(
        res,
        500,
        error.message || "Failed to update transcript",
      );
    }
  },
);

app.post("/api/leads/:id/approve", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
    const userId = req.session?.user?.id || null;
    const leadVoiceId = Number(req.params.id);

    if (!leadVoiceId) {
      return sendApiError(res, 400, "Invalid lead voice ID");
    }

    const data = await approveLeadVoiceUpload({
      leadVoiceId,
      orgId,
      userId,
      verifiedBy:
        req.session?.user?.phone || req.session?.user?.name || "admin",
      verifiedAt: new Date().toISOString(),
    });

    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("POST /api/leads/:id/approve error:", error);
    return sendApiError(res, 500, error.message || "Failed to approve lead");
  }
});

app.post("/api/leads/:id/reject", requireDashboardAuth, async (req, res) => {
  try {
    const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
    const userId = req.session?.user?.id || null;
    const leadVoiceId = Number(req.params.id);
    const reason = String(req.body.reason || "").trim();

    if (!leadVoiceId) {
      return sendApiError(res, 400, "Invalid lead voice ID");
    }

    const data = await rejectLeadVoiceUpload({
      leadVoiceId,
      orgId,
      userId,
      reason,
    });

    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("POST /api/leads/:id/reject error:", error);
    return sendApiError(res, 500, error.message || "Failed to reject lead");
  }
});

app.delete(
  "/api/lead-voice-uploads/bulk-delete",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const ids = Array.isArray(req.body.ids)
        ? req.body.ids.map(Number).filter(Boolean)
        : [];

      if (!ids.length) {
        return sendApiError(res, 400, "No voice message IDs provided");
      }

      const { error } = await supabase
        .from("lead_voice_uploads")
        .delete()
        .eq("org_id", orgId)
        .in("id", ids);

      if (error) throw error;

      return sendApiSuccess(res, { deleted: ids.length });
    } catch (error) {
      console.error("DELETE /api/lead-voice-uploads/bulk-delete error:", error);
      return sendApiError(
        res,
        500,
        error.message || "Failed to delete selected voice messages",
      );
    }
  },
);

app.delete(
  "/api/lead-voice-uploads/:id/transcription",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const id = Number(req.params.id);

      if (!id) {
        return sendApiError(res, 400, "Invalid voice lead ID");
      }

      const { data, error } = await supabase
        .from("lead_voice_uploads")
        .update({
          raw_transcript: null,
          cleaned_transcript: null,
          translated_text: null,
          detected_language: null,
          conversation_rows: [],
          important_points: [],
          pain_points: [],
          follow_up_questions: [],
          review_notes: null,
          transcription_confidence: null,
          transcription_model: null,
          transcription_chunked: false,
          transcription_chunk_seconds: null,
          status: "pending_transcription",
          updated_at: new Date().toISOString(),
        })
        .eq("org_id", orgId)
        .eq("id", id)
        .select()
        .single();

      if (error) throw error;

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error("DELETE transcription error:", error);
      return sendApiError(
        res,
        500,
        error.message || "Failed to delete transcription",
      );
    }
  },
);

app.delete(
  "/api/lead-voice-uploads/:id",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const id = Number(req.params.id);

      if (!id) {
        return sendApiError(res, 400, "Invalid call summary ID");
      }

      const { error } = await supabase
        .from("lead_voice_uploads")
        .delete()
        .eq("org_id", orgId)
        .eq("id", id);

      if (error) throw error;

      return sendApiSuccess(res, { deleted: true });
    } catch (error) {
      console.error("DELETE call summary error:", error);
      return sendApiError(
        res,
        500,
        error.message || "Failed to delete call summary",
      );
    }
  },
);

app.patch(
  "/api/business-leads/:business/:id/status",
  requireDashboardAuth,
  async (req, res) => {
    try {
      const orgId = req.session?.user?.org_id || DASHBOARD_ORG_ID;
      const business = req.params.business;
      const leadId = Number(req.params.id);
      const status = String(req.body.status || "").trim();

      if (!leadId) {
        return sendApiError(res, 400, "Invalid business lead ID");
      }

      const data = await updateBusinessLeadStatus({
        business,
        leadId,
        orgId,
        status,
      });

      return sendApiSuccess(res, data);
    } catch (error) {
      console.error(
        "PATCH /api/business-leads/:business/:id/status error:",
        error,
      );
      return sendApiError(
        res,
        500,
        error.message || "Failed to update business lead status",
      );
    }
  },
);

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
