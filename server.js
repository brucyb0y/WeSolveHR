import express from "express";
import dotenv from "dotenv";
import twilio from "twilio";
import { createClient } from "@supabase/supabase-js";
import OpenAI from "openai";

dotenv.config();

console.log("OPENAI KEY LOADED:", !!process.env.OPENAI_API_KEY);

const app = express();
const port = process.env.PORT || 3000;

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

function normalizeText(text) {
  return String(text || "")
    .trim()
    .toLowerCase();
}

app.use(express.json());

function sendTwiml(res, message) {
  const twiml = new twilio.twiml.MessagingResponse();
  if (message) {
    twiml.message(message);
  }
  res.writeHead(200, { "Content-Type": "text/xml" });
  return res.end(twiml.toString());
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
  if (["pending"].includes(v)) return "badge badge-warn";

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

function parseDeadline(deadlineText) {
  return parseFlexibleDateText(deadlineText);
}

function parseLocalDateTimeForToday(timeText) {
  const raw = String(timeText || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");

  const match = raw.match(/^(\d{1,2}):(\d{2})\s*(am|pm)$/i);
  if (!match) return null;

  let hour = Number(match[1]);
  const minute = Number(match[2]);
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
  const raw = String(text || "").trim();

  let match = raw.match(/^mark\s+(.+?)\s+(login|logout|back)$/i);
  if (match) {
    return {
      target_name: match[1].trim(),
      action: match[2].toLowerCase(),
      duration_min: null,
    };
  }

  match = raw.match(/^mark\s+(.+?)\s+break$/i);
  if (match) {
    return {
      target_name: match[1].trim(),
      action: "break",
      duration_min: null,
    };
  }

  match = raw.match(/^mark\s+(.+?)\s+break\s+(\d+)$/i);
  if (match) {
    return {
      target_name: match[1].trim(),
      action: "break",
      duration_min: Number(match[2]),
    };
  }

  return null;
}

function parseSimpleTaskCommand(text) {
  const raw = String(text || "").trim();

  let match = raw.match(
    /^task\s+(.+?)\s+(low|medium|high|urgent)\s+(.+?)\s+by\s+(.+)$/i,
  );

  if (match) {
    return {
      assignee_name: match[1].trim(),
      priority: match[2].toLowerCase(),
      title: match[3].trim(),
      deadline_text: match[4].trim(),
    };
  }

  match = raw.match(/^task\s+(.+?)\s+(.+?)\s+by\s+(.+)$/i);
  if (!match) return null;

  return {
    assignee_name: match[1].trim(),
    priority: null,
    title: match[2].trim(),
    deadline_text: match[3].trim(),
  };
}

function parseTaskIdCommand(text, commandWord) {
  const msg = normalizeText(text);
  const regex = new RegExp(`^${commandWord}\\s+(\\d+)$`);
  const match = msg.match(regex);

  if (!match) return null;
  return Number(match[1]);
}

function parseWhoIsOffTodayCommand(text) {
  const msg = normalizeText(text);
  return (
    msg === "who is off today" ||
    msg === "who all are on leave today" ||
    msg === "off today" ||
    msg === "leave today"
  );
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
  const msg = normalizeText(text);
  const match = msg.match(/^progress\s+(\d+)\s+(\d{1,3})$/);
  if (!match) return null;

  return {
    taskId: Number(match[1]),
    progress: Number(match[2]),
  };
}

function parseBlockCommand(text) {
  const raw = String(text || "").trim();
  const match = raw.match(/^block\s+(\d+)\s+(.+)$/i);
  if (!match) return null;

  return {
    taskId: Number(match[1]),
    reason: match[2].trim(),
  };
}

function parseUnblockCommand(text) {
  const raw = String(text || "").trim();
  const match = raw.match(/^unblock\s+(\d+)$/i);
  if (!match) return null;

  return { taskId: Number(match[1]) };
}

function parseTasksByNameCommand(text) {
  const raw = String(text || "").trim();
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
  const raw = String(text || "").trim();

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
  const raw = String(text || "").trim();

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
  const raw = String(text || "").trim();

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
  const raw = String(text || "").trim();

  const match = raw.match(/^late\s+(\d{1,2}:\d{2}\s*(?:am|pm))(?:\s+(.+))?$/i);
  if (!match) return null;

  return {
    time_text: match[1].trim().replace(/\s+/g, " "),
    note: match[2]?.trim() || null,
  };
}

function formatTaskLine(task) {
  return `#${task.id}${task.priority ? ` | ${task.priority}` : ""} | ${task.status} | ${task.title} | due ${task.deadline ?? "no deadline"} | ${task.progress}%`;
}

function validateAttendanceTransition(lastAction, nextAction, subjectName) {
  if (nextAction === "login" && lastAction === "login") {
    return `❌ ${subjectName} ${subjectName === "You" ? "are" : "is"} already logged in\nNo action was taken`;
  }

  if (
    nextAction === "break" &&
    lastAction !== "login" &&
    lastAction !== "back"
  ) {
    return `❌ Could not start break\nReason: ${subjectName === "You" ? "you must be logged in first" : `${subjectName} must be logged in first`}`;
  }

  if (nextAction === "back" && lastAction !== "break") {
    return `❌ Could not return from break\nReason: ${subjectName === "You" ? "you are not currently on break" : `${subjectName} is not currently on break`}`;
  }

  if (
    nextAction === "logout" &&
    lastAction !== "login" &&
    lastAction !== "back"
  ) {
    return `❌ Could not log out\nReason: ${subjectName === "You" ? "you are not currently logged in" : `${subjectName} is not currently logged in`}`;
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

function canReadTask(user, task) {
  if (!user || !task) return false;
  if (isManagerOrAdmin(user)) return true;
  return (
    task.assigned_to_user_id === user.id || task.created_by_user_id === user.id
  );
}

function canModifyTask(user, task) {
  if (!user || !task) return false;
  if (isManagerOrAdmin(user)) return true;
  return task.assigned_to_user_id === user.id;
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
    .select("id, name, phone_number, role, is_active")
    .eq("phone_number", phoneNumber)
    .eq("is_active", true)
    .maybeSingle();

  if (error) {
    console.error("User lookup error:", error);
    return { user: null, error };
  }

  return { user: data || null, error: null };
}

async function getLastAction(userId) {
  const { data, error } = await supabase
    .from("attendance_events")
    .select("action")
    .eq("user_id", userId)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("Error fetching last action:", error);
    return null;
  }

  return data?.action || null;
}

async function findUsersByName(name) {
  const trimmed = String(name || "").trim();

  const { data, error } = await supabase
    .from("users")
    .select("id, name, phone_number, role, is_active")
    .ilike("name", trimmed)
    .eq("is_active", true);

  if (error) {
    console.error("User name lookup error:", error);
    return [];
  }

  if (data?.length) return data;

  const { data: fuzzyData, error: fuzzyError } = await supabase
    .from("users")
    .select("id, name, phone_number, role, is_active")
    .ilike("name", `%${trimmed}%`)
    .eq("is_active", true);

  if (fuzzyError) {
    console.error("User fuzzy lookup error:", fuzzyError);
    return [];
  }

  return fuzzyData || [];
}

async function findUniqueUserByName(name) {
  const users = await findUsersByName(name);
  if (users.length !== 1) return null;
  return users[0];
}

async function getTaskAssignedCount(userId) {
  const { count, error } = await supabase
    .from("tasks")
    .select("*", { count: "exact", head: true })
    .eq("assigned_to_user_id", userId)
    .not("status", "in", '("done","archived")');

  if (error) {
    console.error("Assigned task count error:", error);
    return 0;
  }

  return count || 0;
}

async function getTaskById(taskId) {
  const { data, error } = await supabase
    .from("tasks")
    .select(
      `
      id,
      title,
      detail,
      priority,
      status,
      progress,
      deadline,
      blocker_note,
      assigned_to_user_id,
      created_by_user_id,
      last_updated_by_user_id,
      users!tasks_assigned_to_user_id_fkey(name)
    `,
    )
    .eq("id", taskId)
    .maybeSingle();

  if (error) {
    console.error("Get task by id error:", error);
    return { task: null, error };
  }

  return { task: data || null, error: null };
}

async function insertTaskHistory(
  taskId,
  changedByUserId,
  changeType,
  fieldName,
  oldValue,
  newValue,
) {
  const { error } = await supabase.from("task_history").insert([
    {
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

function formatDurationMinutes(totalMinutes) {
  const mins = Math.max(0, Number(totalMinutes || 0));
  const hours = Math.floor(mins / 60);
  const rem = mins % 60;
  if (hours === 0) return `${rem} min`;
  if (rem === 0) return `${hours}h`;
  return `${hours}h ${rem}m`;
}

async function getLatestAttendanceEvent(userId) {
  const { data, error } = await supabase
    .from("attendance_events")
    .select(
      "id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
    )
    .eq("user_id", userId)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("Latest attendance event error:", error);
    return null;
  }

  return data || null;
}

async function getLatestBreakEvent(userId) {
  const { data, error } = await supabase
    .from("attendance_events")
    .select(
      "id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
    )
    .eq("user_id", userId)
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

async function getTodayAttendanceEventsForAllUsers() {
  const { startUtc, endUtc } = getUtcRangeForTodayInTimeZone(APP_TIMEZONE);

  const { data, error } = await supabase
    .from("attendance_events")
    .select(
      "id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
    )
    .gte("created_at", startUtc)
    .lt("created_at", endUtc)
    .order("created_at", { ascending: true });

  if (error) {
    throw error;
  }

  return data || [];
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

async function logIncomingMessage(user, reqBody, body, from) {
  const incoming = {
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

async function handleMyTasks(res, user) {
  const { data, error } = await supabase
    .from("tasks")
    .select("id, title, priority, status, progress, deadline")
    .eq("assigned_to_user_id", user.id)
    .not("status", "in", '("done","archived")')
    .order("deadline", { ascending: true, nullsFirst: false })
    .order("created_at", { ascending: false });

  if (error) {
    console.error("My tasks query error:", error);
    return sendTwiml(res, "Failed to fetch your tasks.");
  }

  if (!data || data.length === 0) {
    return sendTwiml(res, "You have no open tasks.");
  }

  const lines = data.slice(0, 8).map(formatTaskLine);
  const suffix = data.length > 8 ? `\n...and ${data.length - 8} more.` : "";

  return sendTwiml(res, `Your open tasks:\n${lines.join("\n")}${suffix}`);
}

function canActOnTarget({ senderUser, targetUser }) {
  if (!senderUser || !targetUser) return false;
  if (senderUser.id === targetUser.id) return true;
  return isManagerOrAdmin(senderUser);
}

async function handleShowTask(res, user, taskId) {
  const { task, error } = await getTaskById(taskId);

  if (error) {
    return sendTwiml(res, "Failed to fetch that task.");
  }

  if (!task) {
    return sendTwiml(res, `Task #${taskId} not found.`);
  }

  if (!canReadTask(user, task)) {
    return sendTwiml(res, "You are not allowed to view that task.");
  }

  const assignedTo = task.users?.name || "Unknown";
  const detail = task.detail ? `\nDetail: ${task.detail}` : "";
  const blocker = task.blocker_note ? `\nBlocker: ${task.blocker_note}` : "";

  return sendTwiml(
    res,
    `Task #${task.id}\nAssigned to: ${assignedTo}\nPriority: ${task.priority}\nStatus: ${task.status}\nProgress: ${task.progress}%\nTitle: ${task.title}\nDeadline: ${task.deadline ?? "no deadline"}${detail}${blocker}`,
  );
}

async function handleDoneTask(res, user, taskId) {
  const { task, error } = await getTaskById(taskId);

  if (error) {
    return sendTwiml(res, "Failed to fetch that task.");
  }

  if (!task) {
    return sendTwiml(res, `Task #${taskId} not found.`);
  }

  if (!canModifyTask(user, task)) {
    return sendTwiml(res, "You are not allowed to modify that task.");
  }

  if (task.status === "done") {
    return sendTwiml(res, `Task #${taskId} is already marked done.`);
  }

  const { error: updateError } = await supabase
    .from("tasks")
    .update({
      status: "done",
      progress: 100,
      last_updated_by_user_id: user.id,
      updated_at: new Date().toISOString(),
    })
    .eq("id", taskId);

  if (updateError) {
    console.error("Done task update error:", updateError);
    return sendTwiml(res, "Failed to mark the task done.");
  }

  await insertTaskHistory(
    taskId,
    user.id,
    "status_change",
    "status",
    { status: task.status, progress: task.progress },
    { status: "done", progress: 100 },
  );

  return sendTwiml(res, `Task #${taskId} marked done: ${task.title}`);
}

async function handleProgressTask(res, user, taskId, progressValue) {
  if (progressValue < 0 || progressValue > 100) {
    return sendTwiml(res, "Progress must be between 0 and 100.");
  }

  const { task, error } = await getTaskById(taskId);

  if (error) {
    return sendTwiml(res, "Failed to fetch that task.");
  }

  if (!task) {
    return sendTwiml(res, `Task #${taskId} not found.`);
  }

  if (!canModifyTask(user, task)) {
    return sendTwiml(res, "You are not allowed to modify that task.");
  }

  const newStatus =
    progressValue === 100
      ? "done"
      : task.status === "pending"
        ? "in_progress"
        : task.status;

  const { error: updateError } = await supabase
    .from("tasks")
    .update({
      progress: progressValue,
      status: newStatus,
      last_updated_by_user_id: user.id,
      updated_at: new Date().toISOString(),
    })
    .eq("id", taskId);

  if (updateError) {
    console.error("Progress task update error:", updateError);
    return sendTwiml(res, "Failed to update task progress.");
  }

  await insertTaskHistory(
    taskId,
    user.id,
    "progress_change",
    "progress",
    { progress: task.progress, status: task.status },
    { progress: progressValue, status: newStatus },
  );

  return sendTwiml(
    res,
    `Task #${taskId} progress updated to ${progressValue}%: ${task.title}`,
  );
}

async function handleShowOverdue(res, user) {
  if (!isManagerOrAdmin(user)) {
    return sendTwiml(res, "You are not allowed to view overdue tasks.");
  }

  const { data, error } = await supabase
    .from("overdue_tasks_view")
    .select("*")
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
  const openTaskCount = await getTaskAssignedCount(user.id);

  return sendTwiml(
    res,
    `You are ${user.name} | role: ${user.role} | phone: ${user.phone_number} | open tasks: ${openTaskCount}`,
  );
}

async function handleStatus(res, user) {
  try {
    const today = getTodayDateStringInTimeZone(APP_TIMEZONE);
    const { startUtc, endUtc } = getUtcRangeForTodayInTimeZone(APP_TIMEZONE);

    const [latestEvent, eventsResult, lateRows] = await Promise.all([
      getLatestAttendanceEvent(user.id),
      supabase
        .from("attendance_events")
        .select(
          "id, user_id, action, created_at, expected_duration_min, reason, note",
        )
        .eq("user_id", user.id)
        .gte("created_at", startUtc)
        .lt("created_at", endUtc)
        .order("created_at", { ascending: true }),
      getLateArrivalRowsForDate(today),
    ]);

    if (eventsResult.error) {
      console.error("Status events query error:", eventsResult.error);
      return sendTwiml(res, "Failed to fetch your status.");
    }

    const userEvents = eventsResult.data || [];
    const workedMinutes = computeWorkedMinutesFromEvents(userEvents);

    let totalBreakMinutes = 0;
    for (let i = 0; i < userEvents.length; i += 1) {
      const ev = userEvents[i];
      if (ev.action !== "break") continue;

      const nextEnd = userEvents
        .slice(i + 1)
        .find((x) => x.action === "back" || x.action === "logout");

      if (nextEnd) {
        totalBreakMinutes += minutesBetween(ev.created_at, nextEnd.created_at);
      } else {
        totalBreakMinutes += minutesBetween(ev.created_at);
      }
    }

    const myLate = (lateRows || []).find((x) => x.user_id === user.id) || null;
    const firstLogin = getFirstLoginEvent(userEvents);

    const lines = [
      `👤 ${user.name}`,
      `Status: ${latestEvent?.action || "No update"}`,
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
      lines.push(`Expected login: ${formatTimeOnly(myLate.expected_login_at)}`);
      lines.push(
        `Late status: ${myLate.is_approved ? "Approved" : "Not approved"}`,
      );
    }

    lines.push("");
    lines.push("Today:");
    lines.push(`Worked: ${formatDurationMinutes(workedMinutes)}`);
    lines.push(`Break: ${formatDurationMinutes(totalBreakMinutes)}`);

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

  const { error, approved } = await upsertLateArrival(
    user.id,
    expectedLoginAtIso,
    lateCommand.note,
    user.id,
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

async function handleMarkedAttendance(res, actingUser, markCommand) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to mark attendance for others.");
  }

  const targetUser = await findUniqueUserByName(markCommand.target_name);

  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${markCommand.target_name}".`,
    );
  }

  const lastAction = await getLastAction(targetUser.id);
  const validationError = validateAttendanceTransition(
    lastAction,
    markCommand.action,
    targetUser.name,
  );

  if (validationError) {
    return sendTwiml(res, validationError);
  }

  let note = `Marked by ${actingUser.name}`;

  if (markCommand.action === "back") {
    const lastBreak = await getLatestBreakEvent(targetUser.id);
    if (lastBreak) {
      const actualMinutes = minutesBetween(lastBreak.created_at);
      note += ` | Actual break: ${actualMinutes} min`;
    }
  }

  const attendanceRow = {
    user_id: targetUser.id,
    target_phone: targetUser.phone_number,
    acted_by_phone: actingUser.phone_number,
    action: markCommand.action,
    duration_min: markCommand.duration_min ?? null,
    expected_duration_min:
      markCommand.expected_duration_min ?? markCommand.duration_min ?? null,
    reason: markCommand.reason ?? null,
    note,
  };

  const { error } = await supabase
    .from("attendance_events")
    .insert([attendanceRow]);

  if (error) {
    console.error("Marked attendance insert error:", error);
    return sendTwiml(res, "Failed to save marked attendance.");
  }

  if (markCommand.action === "break") {
    return sendTwiml(
      res,
      `${targetUser.name}: break started${markCommand.duration_min ? ` for ${markCommand.duration_min} minutes` : ""} by ${actingUser.name}.`,
    );
  }

  if (markCommand.action === "back") {
    const lastBreak = await getLatestBreakEvent(targetUser.id);
    const actualMinutes = lastBreak ? minutesBetween(lastBreak.created_at) : 0;
    return sendTwiml(
      res,
      `${targetUser.name}: back marked by ${actingUser.name}. Break duration was ${formatDurationMinutes(actualMinutes)}.`,
    );
  }

  return sendTwiml(
    res,
    `${targetUser.name}: ${markCommand.action} marked by ${actingUser.name}.`,
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

  const error = await createPlannedOffDay(user.id, offDate, user.id);
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
  const targetUser = await findUniqueUserByName(offCommand.target_name);

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

  const error = await createPlannedOffDay(
    targetUser.id,
    offDate,
    actingUser.id,
  );
  if (error) {
    console.error("Create off day for other error:", error);
    return sendTwiml(res, "Failed to save day off.");
  }

  return sendTwiml(
    res,
    `🌴 Leave saved for ${offDate}\nName: ${targetUser.name}\nMarked by: ${actingUser.name}`,
  );
}

async function handleSelfAttendance(res, user, attendanceCommand) {
  const lastAction = await getLastAction(user.id);
  const validationError = validateAttendanceTransition(
    lastAction,
    attendanceCommand.action,
    "You",
  );

  if (validationError) {
    return sendTwiml(res, validationError);
  }

  const attendanceRow = {
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
    const lastBreak = await getLatestBreakEvent(user.id);
    const actualMinutes = lastBreak ? minutesBetween(lastBreak.created_at) : 0;

    return sendTwiml(
      res,
      `✅ Back to work\nBreak duration: ${formatDurationMinutes(actualMinutes)}`,
    );
  }

  if (attendanceCommand.action === "login") {
    try {
      const today = getTodayDateStringInTimeZone(APP_TIMEZONE);
      const plannedOffRows = await getPlannedOffRowsForDate(today);
      const otherNames = (plannedOffRows || [])
        .filter((x) => x.user_id !== user.id)
        .map((x) => x.users?.name || "Unknown");

      const shiftStartIso = getShiftStartIsoForToday();
      const loginIso = new Date().toISOString();
      const delayMin = Math.max(
        0,
        Math.round((new Date(loginIso) - new Date(shiftStartIso)) / 60000),
      );

      const lateRows = await getLateArrivalRowsForDate(today);
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
  note = null,
) {
  const { error } = await supabase.from("planned_time_off").upsert(
    [
      {
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

async function getPlannedOffRowsForDate(dateString) {
  const { data, error } = await supabase
    .from("planned_time_off")
    .select(
      `
      id,
      user_id,
      off_date,
      note,
      users!planned_time_off_user_id_fkey(name)
    `,
    )
    .eq("off_date", dateString);

  if (error) {
    throw error;
  }

  return data || [];
}

async function getLateArrivalRowsForDate(dateString) {
  const { data, error } = await supabase
    .from("late_arrivals")
    .select(
      `
      id,
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
    .eq("late_date", dateString);

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
) {
  const todayDb = getTodayDateStringInTimeZone(APP_TIMEZONE);
  const shiftStartIso = getShiftStartIsoForToday();
  const informedAtIso = new Date().toISOString();
  const approved = isLateApproved(informedAtIso, shiftStartIso);

  const { error } = await supabase.from("late_arrivals").upsert(
    [
      {
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

async function handleCreateTask(res, user, taskCommand) {
  if (!taskCommand.assignee_name) {
    return sendTwiml(
      res,
      "I understood this as a task, but could not identify the assignee.",
    );
  }

  if (!taskCommand.title) {
    return sendTwiml(
      res,
      "I understood this as a task, but could not identify the title.",
    );
  }

  const assignee = await findUniqueUserByName(taskCommand.assignee_name);

  if (!assignee) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${taskCommand.assignee_name}".`,
    );
  }

  if (!isManagerOrAdmin(user) && assignee.id !== user.id) {
    return sendTwiml(
      res,
      "You are not allowed to assign tasks to other people.",
    );
  }

  const deadline = parseDeadline(taskCommand.deadline_text);

  if (!deadline) {
    return sendTwiml(
      res,
      `I could not understand the deadline "${taskCommand.deadline_text}". Use today, tomorrow, friday, 11 april, or april 11.`,
    );
  }

  const taskRow = {
    assigned_to_user_id: assignee.id,
    created_by_user_id: user.id,
    last_updated_by_user_id: user.id,
    title: taskCommand.title,
    detail: null,
    priority: taskCommand.priority || "medium",
    status: "pending",
    progress: 0,
    deadline,
    blocker_note: null,
    updated_at: new Date().toISOString(),
  };

  const { data: createdTask, error: taskError } = await supabase
    .from("tasks")
    .insert([taskRow])
    .select("id, title, priority, deadline")
    .single();

  if (taskError) {
    console.error("Task insert error:", taskError);
    return sendTwiml(
      res,
      "❌ Could not create task\nReason: system could not save it\nTry: please send the task again once",
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
      assigned_to_user_id: assignee.id,
    },
  );

  return sendTwiml(
    res,
    `✅ Task #${createdTask.id} created
Owner: ${assignee.name}
Priority: ${createdTask.priority || "none"}
Title: ${createdTask.title}
Due: ${createdTask.deadline || "no due date"}`,
  );
}

async function handleBlockTask(res, user, taskId, reason) {
  const { task, error } = await getTaskById(taskId);

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

  if (!isManagerOrAdmin(user)) {
    return sendTwiml(res, "You are not allowed to block tasks.");
  }

  if (task.status === "done" || task.status === "archived") {
    return sendTwiml(
      res,
      `Task #${taskId} cannot be blocked because it is ${task.status}.`,
    );
  }

  if (task.status === "blocked") {
    return sendTwiml(res, `Task #${taskId} is already blocked.`);
  }

  const { error: updateError } = await supabase
    .from("tasks")
    .update({
      status: "blocked",
      blocker_note: reason,
      last_updated_by_user_id: user.id,
      updated_at: new Date().toISOString(),
    })
    .eq("id", taskId);

  if (updateError) {
    console.error("Block task update error:", updateError);
    return sendTwiml(res, "Failed to block the task.");
  }

  await insertTaskHistory(
    taskId,
    user.id,
    "status_change",
    "status",
    { status: task.status, blocker_note: task.blocker_note },
    { status: "blocked", blocker_note: reason },
  );

  return sendTwiml(
    res,
    `⛔ Task #${taskId} blocked
Title: ${task.title}
Reason: ${reason}`,
  );
}

async function handleUnblockTask(res, user, taskId) {
  const { task, error } = await getTaskById(taskId);

  if (error) {
    return sendTwiml(res, "Failed to fetch that task.");
  }

  if (!task) {
    return sendTwiml(res, `Task #${taskId} not found.`);
  }

  if (!isManagerOrAdmin(user)) {
    return sendTwiml(res, "You are not allowed to unblock tasks.");
  }

  if (task.status !== "blocked") {
    return sendTwiml(res, `Task #${taskId} is not blocked.`);
  }

  const nextStatus = task.progress > 0 ? "in_progress" : "pending";

  const { error: updateError } = await supabase
    .from("tasks")
    .update({
      status: nextStatus,
      blocker_note: null,
      last_updated_by_user_id: user.id,
      updated_at: new Date().toISOString(),
    })
    .eq("id", taskId);

  if (updateError) {
    console.error("Unblock task update error:", updateError);
    return sendTwiml(res, "Failed to unblock the task.");
  }

  await insertTaskHistory(
    taskId,
    user.id,
    "status_change",
    "status",
    { status: task.status, blocker_note: task.blocker_note },
    { status: nextStatus, blocker_note: null },
  );

  return sendTwiml(res, `Task #${taskId} unblocked: ${task.title}`);
}

async function handleTasksByName(res, actingUser, assigneeName) {
  if (!isManagerOrAdmin(actingUser)) {
    return sendTwiml(res, "You are not allowed to view other people's tasks.");
  }

  const targetUser = await findUniqueUserByName(assigneeName);

  if (!targetUser) {
    return sendTwiml(
      res,
      `I could not uniquely find an active user named "${assigneeName}".`,
    );
  }

  const { data, error } = await supabase
    .from("tasks")
    .select("id, title, priority, status, progress, deadline")
    .eq("assigned_to_user_id", targetUser.id)
    .not("status", "in", '("done","archived")')
    .order("deadline", { ascending: true, nullsFirst: false })
    .order("created_at", { ascending: false });

  if (error) {
    console.error("Tasks by name query error:", error);
    return sendTwiml(res, "Failed to fetch tasks.");
  }

  if (!data || data.length === 0) {
    return sendTwiml(res, `${targetUser.name} has no open tasks.`);
  }

  const lines = data.slice(0, 8).map(formatTaskLine);
  const suffix = data.length > 8 ? `\n...and ${data.length - 8} more.` : "";

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
    .eq("is_active", true)
    .order("name", { ascending: true });

  if (usersError) {
    console.error("Who is on break users query error:", usersError);
    return sendTwiml(res, "Failed to fetch break status.");
  }

  const { data: events, error: eventsError } = await supabase
    .from("attendance_events")
    .select("user_id, action, created_at")
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
    const today = getTodayDateStringInTimeZone(APP_TIMEZONE);
    const plannedOffRows = await getPlannedOffRowsForDate(today);
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
    const today = getTodayDateStringInTimeZone(APP_TIMEZONE);

    const [usersResult, events, plannedOffRows, lateRows] = await Promise.all([
      supabase
        .from("users")
        .select("id, name")
        .eq("is_active", true)
        .order("name", { ascending: true }),
      getTodayAttendanceEventsForAllUsers(),
      getPlannedOffRowsForDate(today),
      getLateArrivalRowsForDate(today),
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
    const onLeaveToday = plannedOff.map((x) => x.users?.name || "Unknown");
    const loggedOutToday = [];
    const noUpdateToday = [];

    for (const user of users) {
      if (plannedOffUserIds.has(user.id)) continue;

      const userEvents = eventsByUser.get(user.id) || [];
      const latest = userEvents[userEvents.length - 1] || null;

      if (!latest) {
        const lateInfo = lateByUser.get(user.id);
        if (lateInfo) {
          noUpdateToday.push(
            `${user.name} (late till ${formatTimeOnly(lateInfo.expected_login_at)})`,
          );
        } else {
          noUpdateToday.push(user.name);
        }
        continue;
      }

      if (latest.action === "break") {
        onBreakNow.push(user.name);
        continue;
      }

      if (latest.action === "logout") {
        loggedOutToday.push(user.name);
        continue;
      }

      if (latest.action === "login" || latest.action === "back") {
        workingNow.push(user.name);
        continue;
      }

      noUpdateToday.push(user.name);
    }

    const lines = [
      "📋 Now summary",
      `Total team: ${users.length} | Working: ${workingNow.length} | Break: ${onBreakNow.length} | Leave: ${onLeaveToday.length} | Logged out: ${loggedOutToday.length} | No update: ${noUpdateToday.length}`,
      "",
      `✅ Working: ${workingNow.length ? workingNow.join(", ") : "None"}`,
      `☕ Break: ${onBreakNow.length ? onBreakNow.join(", ") : "None"}`,
      `🌴 Leave: ${onLeaveToday.length ? onLeaveToday.join(", ") : "None"}`,
      `🏁 Logged out: ${loggedOutToday.length ? loggedOutToday.join(", ") : "None"}`,
      `❓ No update: ${noUpdateToday.length ? noUpdateToday.join(", ") : "None"}`,
    ];

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
    const today = getTodayDateStringInTimeZone(APP_TIMEZONE);
    const shiftStartIso = getShiftStartIsoForToday();

    const [usersResult, events, plannedOffRows, lateRows] = await Promise.all([
      supabase
        .from("users")
        .select("id, name, role")
        .eq("is_active", true)
        .order("name", { ascending: true }),
      getTodayAttendanceEventsForAllUsers(),
      getPlannedOffRowsForDate(today),
      getLateArrivalRowsForDate(today),
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
          if (new Date() > new Date(lateInfo.expected_login_at)) {
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

      if (loginDelayMin > 0) {
        if (lateInfo && lateInfo.is_approved) {
          approvedLate.push(
            `${user.name} (${formatTimeOnly(firstLogin.created_at)}, ${loginDelayMin}m late)`,
          );
        } else if (lateInfo && !lateInfo.is_approved) {
          unapprovedLate.push(
            `${user.name} (${formatTimeOnly(firstLogin.created_at)}, ${loginDelayMin}m late)`,
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

async function handleHelp(res, user) {
  const lines = [
    "📘 WeSolveHR Commands",
    "",
    "Attendance",
    "login",
    "break",
    "break 15",
    "break personal issue",
    "break 15 personal issue",
    "back",
    "logout",
    "logout early due to family concern",
    "late 11:00 AM",
    "",
    "Tasks",
    "my tasks",
    "show task 8",
    "done 8",
    "progress 8 50",
    "block 8 waiting for Aj",
    "unblock 8",
    "",
    "Leave",
    "leave today",
    "leave tomorrow",
    "leave Aj tomorrow",
    "who is off today",
    "",
    "Team",
    "who am i",
    "status",
    "who is on break",
    "now",
    "summary today",
    "",
    "Create task example",
    "task Aj high test dashboard by tomorrow",
  ];

  if (isManagerOrAdmin(user)) {
    lines.splice(
      1,
      0,
      `Role: ${user.role}`,
      "Manager/Admin commands included below.",
      "",
    );
  }

  return sendTwiml(res, lines.join("\n"));
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

  const { task, error: taskError } = await getTaskById(history.task_id);
  if (taskError || !task) {
    return sendTwiml(res, "Failed to fetch the task for undo.");
  }

  if (!canModifyTask(user, task) && !isManagerOrAdmin(user)) {
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
  );

  return sendTwiml(
    res,
    `Reverted your last task change on task #${history.task_id}.`,
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

function buildDateForCurrentYear(month, day) {
  const year = getCurrentYearInTimeZone(APP_TIMEZONE);
  const d = new Date(Date.UTC(year, month - 1, day));
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

async function getLatestAttendanceByUser() {
  const today = getTodayDateStringInTimeZone(APP_TIMEZONE);
  const [usersResult, events, plannedOffRows] = await Promise.all([
    supabase
      .from("users")
      .select("id, name, role, phone_number")
      .eq("is_active", true)
      .order("name", { ascending: true }),
    getTodayAttendanceEventsForAllUsers(),
    getPlannedOffRowsForDate(today),
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

async function getDashboardData() {
  const [openTasksResult, overdueResult, blockedResult, attendanceRows] =
    await Promise.all([
      supabase.from("open_tasks_view").select("*").limit(100),
      supabase.from("overdue_tasks_view").select("*").limit(100),
      supabase.from("blocked_tasks_view").select("*").limit(100),
      getLatestAttendanceByUser(),
    ]);

  if (openTasksResult.error) throw openTasksResult.error;
  if (overdueResult.error) throw overdueResult.error;
  if (blockedResult.error) throw blockedResult.error;

  const openTasks = openTasksResult.data || [];
  const overdueTasks = overdueResult.data || [];
  const blockedTasks = blockedResult.data || [];
  const attendance = attendanceRows || [];

  const onBreak = attendance.filter((x) => x.status === "break");
  const loggedIn = attendance.filter(
    (x) => x.status === "login" || x.status === "back",
  );
  const plannedOff = attendance.filter((x) => x.status === "planned_off");
  const noLogin = attendance.filter((x) => x.status === "no_login");

  return {
    summary: {
      openTasks: openTasks.length,
      overdueTasks: overdueTasks.length,
      blockedTasks: blockedTasks.length,
      onBreakCount: onBreak.length,
      loggedInCount: loggedIn.length,
      plannedOffCount: plannedOff.length,
      noLoginCount: noLogin.length,
      teamCount: attendance.length,
    },
    openTasks,
    overdueTasks,
    blockedTasks,
    attendance,
  };
}

app.use("/api", requireDashboardAuth);

function renderDashboardPage(data) {
  const summaryCards = [
    { label: "Open Tasks", value: data.summary.openTasks },
    { label: "Overdue Tasks", value: data.summary.overdueTasks },
    { label: "Blocked Tasks", value: data.summary.blockedTasks },
    { label: "Logged In Now", value: data.summary.loggedInCount },
    { label: "On Break Now", value: data.summary.onBreakCount },
    { label: "Planned Off Today", value: data.summary.plannedOffCount },
    { label: "No Login Today", value: data.summary.noLoginCount },
    { label: "Active Team", value: data.summary.teamCount },
  ]
    .map(
      (card) => `
        <div class="card stat-card">
          <div class="stat-label">${escapeHtml(card.label)}</div>
          <div class="stat-value">${escapeHtml(card.value)}</div>
        </div>
      `,
    )
    .join("");

  const openTaskRows = data.openTasks.length
    ? data.openTasks
        .map(
          (task) => `
            <tr>
              <td>#${escapeHtml(task.id)}</td>
              <td>${escapeHtml(task.title)}</td>
              <td>${escapeHtml(task.assigned_to ?? task.name ?? "-")}</td>
              <td><span class="${badgeClass(task.priority)}">${escapeHtml(task.priority)}</span></td>
              <td><span class="${badgeClass(task.status)}">${escapeHtml(task.status)}</span></td>
              <td>${escapeHtml(task.progress ?? 0)}%</td>
              <td>${escapeHtml(formatDateOnly(task.deadline))}</td>
            </tr>
          `,
        )
        .join("")
    : `<tr><td colspan="7" class="empty">No open tasks</td></tr>`;

  const attendanceRows = data.attendance.length
    ? data.attendance
        .map(
          (row) => `
          <tr>
            <td>${escapeHtml(row.name)}</td>
            <td>${escapeHtml(row.role || "-")}</td>
            <td><span class="${badgeClass(row.status)}">${escapeHtml(row.status)}</span></td>
            <td>${escapeHtml(formatDateTime(row.last_action_at))}</td>
            <td>${row.duration_min != null ? escapeHtml(formatDurationMinutes(row.duration_min)) : "-"}</td>
            <td>${escapeHtml(formatDurationMinutes(row.worked_min_today || 0))}</td>
          </tr>
        `,
        )
        .join("")
    : `<tr><td colspan="6" class="empty">No attendance data</td></tr>`;

  const overdueRows = data.overdueTasks.length
    ? data.overdueTasks
        .slice(0, 12)
        .map(
          (task) => `
            <tr>
              <td>#${escapeHtml(task.id)}</td>
              <td>${escapeHtml(task.title)}</td>
              <td>${escapeHtml(task.assigned_to ?? "-")}</td>
              <td><span class="${badgeClass(task.priority)}">${escapeHtml(task.priority)}</span></td>
              <td>${escapeHtml(formatDateOnly(task.deadline))}</td>
              <td>${escapeHtml(task.days_overdue ?? 0)} day(s)</td>
            </tr>
          `,
        )
        .join("")
    : `<tr><td colspan="6" class="empty">No overdue tasks</td></tr>`;

  const blockedRows = data.blockedTasks.length
    ? data.blockedTasks
        .slice(0, 12)
        .map(
          (task) => `
            <tr>
              <td>#${escapeHtml(task.id)}</td>
              <td>${escapeHtml(task.title)}</td>
              <td>${escapeHtml(task.assigned_to ?? "-")}</td>
              <td>${escapeHtml(task.blocker_note ?? "-")}</td>
            </tr>
          `,
        )
        .join("")
    : `<tr><td colspan="4" class="empty">No blocked tasks</td></tr>`;

  return `
  <!DOCTYPE html>
  <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>WeSolveHR Dashboard</title>
      <style>
        :root {
          --bg: #0b1020;
          --panel: #121933;
          --panel-2: #182140;
          --text: #eef3ff;
          --muted: #9db0d5;
          --line: rgba(255, 255, 255, 0.08);
          --good: #1f9d55;
          --warn: #d69e2e;
          --bad: #e53e3e;
          --info: #3182ce;
          --shadow: 0 10px 30px rgba(0, 0, 0, 0.25);
        }

        * { box-sizing: border-box; }

        body {
          margin: 0;
          background:
            radial-gradient(circle at top left, rgba(49,130,206,0.18), transparent 25%),
            radial-gradient(circle at top right, rgba(229,62,62,0.12), transparent 22%),
            linear-gradient(180deg, #0a1020 0%, #10172d 100%);
          color: var(--text);
          font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }

        .wrap {
          max-width: 1400px;
          margin: 0 auto;
          padding: 28px 20px 40px;
        }

        .topbar {
          display: flex;
          justify-content: space-between;
          align-items: center;
          gap: 16px;
          margin-bottom: 24px;
          flex-wrap: wrap;
        }

        .title {
          font-size: 30px;
          font-weight: 800;
          letter-spacing: -0.03em;
          margin: 0;
        }

        .subtitle {
          color: var(--muted);
          margin-top: 6px;
          font-size: 14px;
        }

        .links {
          display: flex;
          gap: 10px;
          flex-wrap: wrap;
        }

        .link-btn {
          display: inline-block;
          padding: 10px 14px;
          border-radius: 12px;
          background: rgba(255,255,255,0.06);
          color: var(--text);
          text-decoration: none;
          border: 1px solid var(--line);
        }

        .stats-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 14px;
  margin-bottom: 22px;
}

        .card {
          background: linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02));
          border: 1px solid var(--line);
          border-radius: 18px;
          box-shadow: var(--shadow);
        }

        .stat-card {
          padding: 18px;
        }

        .stat-label {
          color: var(--muted);
          font-size: 13px;
          margin-bottom: 8px;
        }

        .stat-value {
          font-size: 30px;
          font-weight: 800;
          line-height: 1;
          letter-spacing: -0.04em;
        }

        .grid {
          display: grid;
          grid-template-columns: 1.5fr 1fr;
          gap: 18px;
        }

        .section {
          padding: 18px;
          overflow: hidden;
        }

        .section-title {
          margin: 0 0 14px;
          font-size: 18px;
          font-weight: 700;
        }

        .section-subtitle {
          color: var(--muted);
          font-size: 13px;
          margin-bottom: 14px;
        }

        table {
          width: 100%;
          border-collapse: collapse;
          font-size: 14px;
        }

        th, td {
          text-align: left;
          padding: 12px 10px;
          border-bottom: 1px solid var(--line);
          vertical-align: top;
        }

        th {
          color: var(--muted);
          font-size: 12px;
          font-weight: 700;
          text-transform: uppercase;
          letter-spacing: 0.04em;
        }

        .empty {
          text-align: center;
          color: var(--muted);
          padding: 18px;
        }

        .badge {
          display: inline-flex;
          align-items: center;
          padding: 5px 10px;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 700;
          border: 1px solid transparent;
          text-transform: capitalize;
        }

        .badge-danger {
          background: rgba(229, 62, 62, 0.14);
          color: #ffb3b3;
          border-color: rgba(229, 62, 62, 0.25);
        }

        .badge-warn {
          background: rgba(214, 158, 46, 0.14);
          color: #f8d68a;
          border-color: rgba(214, 158, 46, 0.25);
        }

        .badge-ok {
          background: rgba(31, 157, 85, 0.14);
          color: #9ae6b4;
          border-color: rgba(31, 157, 85, 0.25);
        }

        .badge-info {
          background: rgba(49, 130, 206, 0.14);
          color: #90cdf4;
          border-color: rgba(49, 130, 206, 0.25);
        }

        .badge-muted {
          background: rgba(255,255,255,0.08);
          color: #d8e3ff;
          border-color: rgba(255,255,255,0.12);
        }

        .stack {
          display: grid;
          gap: 18px;
        }

        .footer-note {
          margin-top: 20px;
          color: var(--muted);
          font-size: 12px;
        }

        @media (max-width: 1200px) {
          .stats-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        }

        @media (max-width: 700px) {
          .stats-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
          .wrap { padding: 18px 12px 30px; }
          .title { font-size: 24px; }
          th, td { padding: 10px 8px; font-size: 13px; }
        }
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="topbar">
          <div>
            <h1 class="title">WeSolveHR Dashboard</h1>
            <div class="subtitle">Live operations view for tasks, attendance, blockers, and overdue work</div>
          </div>
<div class="links">
  <a class="link-btn" href="/dashboard">Dashboard</a>
  <a class="link-btn" href="/tasks">Tasks</a>
  <a class="link-btn" href="/attendance">Attendance</a>
  <a class="link-btn" href="/logs">Logs</a>
</div>
        </div>

        <div class="stats-grid">
          ${summaryCards}
        </div>

        <div class="grid">
          <div class="card section">
            <h2 class="section-title">Open Tasks</h2>
            <div class="section-subtitle">Current active work across the team</div>
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Title</th>
                  <th>Assignee</th>
                  <th>Priority</th>
                  <th>Status</th>
                  <th>Progress</th>
                  <th>Deadline</th>
                </tr>
              </thead>
              <tbody>
                ${openTaskRows}
              </tbody>
            </table>
          </div>

          <div class="stack">
            <div class="card section">
              <h2 class="section-title">Latest Attendance</h2>
              <div class="section-subtitle">Most recent state for each active user</div>
              <table>
                <thead>
<tr>
  <th>Name</th>
  <th>Role</th>
  <th>Status</th>
  <th>Last Action</th>
  <th>Break Duration</th>
  <th>Worked Today</th>
</tr>
                </thead>
                <tbody>
                  ${attendanceRows}
                </tbody>
              </table>
            </div>

            <div class="card section">
              <h2 class="section-title">Blocked Tasks</h2>
              <div class="section-subtitle">Tasks that need intervention</div>
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Title</th>
                    <th>Assignee</th>
                    <th>Reason</th>
                  </tr>
                </thead>
                <tbody>
                  ${blockedRows}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <div style="height:18px"></div>

        <div class="card section">
          <h2 class="section-title">Overdue Tasks</h2>
          <div class="section-subtitle">Work past deadline, sorted by urgency in your view</div>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Title</th>
                <th>Assignee</th>
                <th>Priority</th>
                <th>Deadline</th>
                <th>Overdue</th>
              </tr>
            </thead>
            <tbody>
              ${overdueRows}
            </tbody>
          </table>
        </div>

        <div class="footer-note">
          Hardened build: task permissions, dashboard auth support, webhook signature validation, and rate limiting added.
        </div>
      </div>
    </body>
  </html>
  `;
}

async function getDashboardSummaryData() {
  const { startUtc, endUtc, todayDb } =
    getUtcRangeForTodayInTimeZone(APP_TIMEZONE);
  const today = todayDb;

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
      .not("status", "in", '("done","archived")'),

    supabase
      .from("tasks")
      .select("*", { count: "exact", head: true })
      .lt("deadline", today)
      .not("status", "in", '("done","archived")'),

    supabase
      .from("tasks")
      .select("*", { count: "exact", head: true })
      .eq("status", "blocked"),

    supabase
      .from("attendance_events")
      .select("user_id", { count: "exact" })
      .gte("created_at", startUtc)
      .lt("created_at", endUtc),

    supabase
      .from("users")
      .select("id, name, role")
      .eq("is_active", true)
      .order("name", { ascending: true }),

    supabase
      .from("attendance_events")
      .select("user_id, action, created_at")
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

async function getAttendancePageData() {
  const { startUtc, endUtc } = getUtcRangeForTodayInTimeZone(APP_TIMEZONE);

  const [
    { data: users, error: usersError },
    { data: events, error: eventsError },
  ] = await Promise.all([
    supabase
      .from("users")
      .select("id, name, role")
      .eq("is_active", true)
      .order("name", { ascending: true }),

    supabase
      .from("attendance_events")
      .select(
        "id, user_id, action, duration_min, expected_duration_min, reason, note, created_at",
      )
      .order("created_at", { ascending: false })
      .limit(500),
  ]);

  if (usersError) throw usersError;
  if (eventsError) throw eventsError;

  const latestByUser = new Map();
  for (const event of events || []) {
    if (!latestByUser.has(event.user_id)) {
      latestByUser.set(event.user_id, event);
    }
  }

  const currentStatus = (users || []).map((user) => {
    const latest = latestByUser.get(user.id);
    return {
      user_id: user.id,
      name: user.name,
      role: user.role,
      status: latest?.action || "unknown",
      last_event_at: latest?.created_at || null,
      last_event_at_text: latest?.created_at
        ? formatDateTime(latest.created_at)
        : "-",
    };
  });

  const activeTodayUserIds = new Set(
    (events || [])
      .filter((e) => e.created_at >= startUtc && e.created_at < endUtc)
      .map((e) => e.user_id),
  );

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

async function getTasksPageData(filters = {}) {
  const today = getTodayDateStringInTimeZone(APP_TIMEZONE);

  let query = supabase
    .from("tasks")
    .select(
      `
      id,
      title,
      priority,
      status,
      progress,
      deadline,
      blocker_note,
      assigned_to_user_id,
      created_by_user_id,
      updated_at,
      users!tasks_assigned_to_user_id_fkey(name)
    `,
    )
    .order("deadline", { ascending: true, nullsFirst: false })
    .order("updated_at", { ascending: false });

  if (filters.assignee) {
    query = query.eq("assigned_to_user_id", Number(filters.assignee));
  }

  if (filters.status) {
    query = query.eq("status", filters.status);
  }

  if (filters.priority) {
    query = query.eq("priority", filters.priority);
  }

  if (filters.blocked === "true") {
    query = query.eq("status", "blocked");
  }

  if (filters.overdue === "true") {
    query = query
      .lt("deadline", today)
      .not("status", "in", '("done","archived")');
  }

  const { data, error } = await query;
  if (error) throw error;

  let rows = (data || []).map((task) => ({
    id: task.id,
    title: task.title,
    assignee_name: task.users?.name || "Unknown",
    status: task.status,
    progress: task.progress,
    priority: task.priority,
    deadline: task.deadline,
    blocker_note: task.blocker_note,
    assigned_to_user_id: task.assigned_to_user_id,
    created_by_user_id: task.created_by_user_id,
  }));

  const search = String(filters.search || "")
    .trim()
    .toLowerCase();
  if (search) {
    rows = rows.filter((task) => {
      const matchesTitle = String(task.title || "")
        .toLowerCase()
        .includes(search);
      const matchesId = /^\d+$/.test(search)
        ? String(task.id) === search
        : false;
      return matchesTitle || matchesId;
    });
  }

  return rows;
}

async function getTaskDetailData(taskId) {
  const { data: task, error: taskError } = await supabase
    .from("tasks")
    .select(
      `
      id,
      title,
      detail,
      priority,
      status,
      progress,
      deadline,
      blocker_note,
      assigned_to_user_id,
      created_by_user_id,
      last_updated_by_user_id,
      created_at,
      updated_at,
      assignee:users!tasks_assigned_to_user_id_fkey(name)
    `,
    )
    .eq("id", taskId)
    .maybeSingle();

  if (taskError) throw taskError;
  if (!task) return null;

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
      changed_by_user_id
    `,
    )
    .eq("task_id", taskId)
    .order("created_at", { ascending: false });

  if (historyError) throw historyError;

  return {
    id: task.id,
    title: task.title,
    detail: task.detail,
    assignee_name: task.assignee?.name || "Unknown",
    priority: task.priority,
    status: task.status,
    progress: task.progress,
    deadline: task.deadline,
    blocker_note: task.blocker_note,
    assigned_to_user_id: task.assigned_to_user_id,
    created_by_user_id: task.created_by_user_id,
    created_at: task.created_at,
    updated_at: task.updated_at,
    task_history: history || [],
  };
}

async function getLogsPageData() {
  const { data, error } = await supabase
    .from("message_logs")
    .select(
      `
      id,
      user_id,
      phone_number,
      profile_name,
      message_text,
      twilio_message_sid,
      created_at,
      direction
    `,
    )
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
    const data = await getDashboardData();
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

app.get("/api/users", async (_req, res) => {
  try {
    const { data, error } = await supabase
      .from("users")
      .select("id, name, role, is_active")
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

app.get("/api/summary", async (_req, res) => {
  try {
    const data = await getDashboardSummaryData();
    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("API /api/summary error:", error);
    return sendApiError(res, 500, "Failed to load summary");
  }
});

app.get("/api/tasks", async (req, res) => {
  try {
    const data = await getTasksPageData(req.query);
    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("API /api/tasks error:", error);
    return sendApiError(res, 500, "Failed to load tasks");
  }
});

app.get("/api/tasks/:id", async (req, res) => {
  try {
    const taskId = Number(req.params.id);
    if (!taskId) {
      return sendApiError(res, 400, "Invalid task id");
    }

    const detail = await getTaskDetailData(taskId);
    if (!detail) {
      return sendApiError(res, 404, "Task not found");
    }

    const demoUser = { role: "admin", id: 0 };
    if (!canReadTask(demoUser, detail)) {
      return sendApiError(res, 403, "Not allowed to view this task");
    }

    return sendApiSuccess(res, detail);
  } catch (error) {
    console.error("API /api/tasks/:id error:", error);
    return sendApiError(res, 500, "Failed to load task");
  }
});

app.get("/api/attendance", async (_req, res) => {
  try {
    const data = await getAttendancePageData();
    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("API /api/attendance error:", error);
    return sendApiError(res, 500, "Failed to load attendance");
  }
});

app.get("/api/logs", async (_req, res) => {
  try {
    const data = await getLogsPageData();
    return sendApiSuccess(res, data);
  } catch (error) {
    console.error("API /api/logs error:", error);
    return sendApiError(res, 500, "Failed to load logs");
  }
});

app.get("/tasks", requireDashboardAuth, async (_req, res) => {
  res.status(200).send(`
    <html>
      <head>
        <title>Tasks</title>
        <style>
          body { font-family: Arial, sans-serif; background:#0b1020; color:#fff; margin:0; }
          .wrap { max-width: 1200px; margin: 0 auto; padding: 24px; }
          .topbar { display:flex; justify-content:space-between; align-items:center; margin-bottom:20px; }
          .controls { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:20px; }
          input, select { padding:10px; border-radius:8px; border:1px solid #334; background:#121933; color:#fff; }
          table { width:100%; border-collapse:collapse; background:#121933; border-radius:12px; overflow:hidden; }
          th, td { padding:12px; border-bottom:1px solid #24304f; text-align:left; }
          a { color:#9cc3ff; text-decoration:none; }
          .actions a { margin-right:12px; }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="topbar">
            <h1>Tasks</h1>
            <div class="actions">
              <a href="/dashboard">Dashboard</a>
              <a href="/attendance">Attendance</a>
              <a href="/logs">Logs</a>
            </div>
          </div>

          <div class="controls">
            <input id="search" placeholder="Search task title or ID" />
            <select id="assignee"><option value="">All assignees</option></select>
            <select id="status">
              <option value="">All status</option>
              <option value="pending">Pending</option>
              <option value="in_progress">In progress</option>
              <option value="blocked">Blocked</option>
              <option value="done">Done</option>
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

          <div id="statusText">Loading tasks...</div>

          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Title</th>
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

        <script>
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
            const status = document.getElementById('status').value;
            const priority = document.getElementById('priority').value;
            const blocked = document.getElementById('blocked').checked;
            const overdue = document.getElementById('overdue').checked;

            if (search) params.set('search', search);
            if (assignee) params.set('assignee', assignee);
            if (status) params.set('status', status);
            if (priority) params.set('priority', priority);
            if (blocked) params.set('blocked', 'true');
            if (overdue) params.set('overdue', 'true');

            document.getElementById('statusText').textContent = 'Loading tasks...';

            const res = await fetch('/api/tasks?' + params.toString());
            const json = await res.json();

            if (!json.ok) {
              document.getElementById('statusText').textContent = 'Could not load tasks';
              document.getElementById('taskRows').innerHTML = '';
              return;
            }

            const rows = json.data || [];
            document.getElementById('statusText').textContent = rows.length ? '' : 'No tasks found';

            document.getElementById('taskRows').innerHTML = rows.map(task => \`
              <tr onclick="window.open('/api/tasks/\${task.id}', '_blank')">
                <td>#\${task.id}</td>
                <td>\${task.title || ''}</td>
                <td>\${task.assignee_name || ''}</td>
                <td>\${task.status || ''}</td>
                <td>\${task.progress ?? 0}%</td>
                <td>\${task.priority || ''}</td>
                <td>\${task.deadline || '-'}</td>
                <td>\${task.blocker_note || '-'}</td>
              </tr>
            \`).join('');
          }

          loadUsers().then(loadTasks);
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
          body { font-family: Arial, sans-serif; background:#0b1020; color:#fff; margin:0; }
          .wrap { max-width: 1200px; margin: 0 auto; padding: 24px; }
          .cards { display:grid; grid-template-columns: repeat(3, 1fr); gap:16px; margin-bottom:24px; }
          .card { background:#121933; padding:16px; border-radius:12px; }
          table { width:100%; border-collapse:collapse; background:#121933; margin-top:16px; }
          th, td { padding:12px; border-bottom:1px solid #24304f; text-align:left; }
          a { color:#9cc3ff; text-decoration:none; margin-right:12px; }
        </style>
      </head>
      <body>
        <div class="wrap">
          <h1>Attendance</h1>
          <p><a href="/dashboard">Dashboard</a><a href="/tasks">Tasks</a><a href="/logs">Logs</a></p>

          <div class="cards">
            <div class="card"><div>Logged In</div><h2 id="loggedIn">-</h2></div>
            <div class="card"><div>On Break</div><h2 id="onBreak">-</h2></div>
            <div class="card"><div>Active Today</div><h2 id="activeToday">-</h2></div>
          </div>

          <h2>Current Status</h2>
          <table>
            <thead><tr><th>Name</th><th>Role</th><th>Status</th><th>Last Activity</th></tr></thead>
            <tbody id="currentStatusRows"></tbody>
          </table>

          <h2>Recent Events</h2>
          <table>
            <thead><tr><th>Time</th><th>User ID</th><th>Action</th><th>Duration</th><th>Note</th></tr></thead>
            <tbody id="recentEventRows"></tbody>
          </table>
        </div>

        <script>
          async function loadAttendance() {
            const res = await fetch('/api/attendance');
            const json = await res.json();
            if (!json.ok) return;

            const data = json.data;
            document.getElementById('loggedIn').textContent = data.summary.logged_in_count;
            document.getElementById('onBreak').textContent = data.summary.on_break_count;
            document.getElementById('activeToday').textContent = data.summary.active_today_count;

            document.getElementById('currentStatusRows').innerHTML = (data.current_status || []).map(row => \`
              <tr>
                <td>\${row.name || ''}</td>
                <td>\${row.role || ''}</td>
                <td>\${row.status || ''}</td>
                <td>\${row.last_event_at_text || '-'}</td>
              </tr>
            \`).join('');

            document.getElementById('recentEventRows').innerHTML = (data.recent_events || []).map(row => \`
              <tr>
                <td>\${row.created_at_text || ''}</td>
                <td>\${row.user_id || ''}</td>
                <td>\${row.action || ''}</td>
                <td>\${row.duration_min || '-'}</td>
                <td>\${row.note || '-'}</td>
              </tr>
            \`).join('');
          }

          loadAttendance();
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
          body { font-family: Arial, sans-serif; background:#0b1020; color:#fff; margin:0; }
          .wrap { max-width: 1200px; margin: 0 auto; padding: 24px; }
          table { width:100%; border-collapse:collapse; background:#121933; margin-top:16px; }
          th, td { padding:12px; border-bottom:1px solid #24304f; text-align:left; vertical-align:top; }
          a { color:#9cc3ff; text-decoration:none; margin-right:12px; }
          .msg { white-space:pre-wrap; }
        </style>
      </head>
      <body>
        <div class="wrap">
          <h1>Logs</h1>
          <p><a href="/dashboard">Dashboard</a><a href="/tasks">Tasks</a><a href="/attendance">Attendance</a></p>

          <table>
            <thead><tr><th>Time</th><th>Sender</th><th>Message</th><th>Message SID</th></tr></thead>
            <tbody id="logRows"></tbody>
          </table>
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
        </script>
      </body>
    </html>
  `);
});

app.post("/whatsapp", async (req, res) => {
  try {
    if (!validateTwilioRequest(req)) {
      console.warn("Rejected request due to invalid Twilio signature.");
      return res.status(403).send("Invalid Twilio signature");
    }

    console.log("Incoming message:", req.body);

    const from = req.body.From || null;
    const body = String(req.body.Body || "").trim();
    const normalizedBody = normalizeText(body);
    const rateLimitKey = from || req.ip || "unknown";

    if (!checkRateLimit(rateLimitKey)) {
      console.warn("Rate limit exceeded for:", rateLimitKey);
      return sendTwiml(
        res,
        "Too many requests. Please wait a minute and try again.",
      );
    }

    const { user, error: userError } = await getActiveUserByPhone(from);

    if (userError) {
      return sendTwiml(
        res,
        "❌ Could not verify your account right now\nReason: user lookup failed\nTry: please message again in a minute",
      );
    }

    const logResult = await logIncomingMessage(user, req.body, body, from);
    if (logResult.duplicate) {
      return sendEmptyTwiml(res);
    }

    if (!user) {
      console.log("Unknown sender:", from);
      return sendTwiml(
        res,
        "❌ Your number is not registered in this system\nPlease contact admin to get added",
      );
    }

    console.log(`Mapped sender to user: ${user.name} (${user.role})`);

    if (normalizedBody === "help" || normalizedBody === "commands") {
      return handleHelp(res, user);
    }

    if (normalizedBody === "my tasks") {
      return handleMyTasks(res, user);
    }

    if (normalizedBody === "show overdue") {
      return handleShowOverdue(res, user);
    }

    const showTaskId = parseShowTaskCommand(body);
    if (showTaskId) {
      return handleShowTask(res, user, showTaskId);
    }

    const doneTaskId = parseTaskIdCommand(body, "done");
    if (doneTaskId) {
      return handleDoneTask(res, user, doneTaskId);
    }

    const progressCommand = parseProgressCommand(body);
    if (progressCommand) {
      return handleProgressTask(
        res,
        user,
        progressCommand.taskId,
        progressCommand.progress,
      );
    }

    if (parseWhoAmICommand(body)) {
      return handleWhoAmI(res, user);
    }

    if (parseStatusCommand(body)) {
      return handleStatus(res, user);
    }

    const lateCommand = parseLateCommand(body);
    if (lateCommand) {
      return handleLateCommand(res, user, lateCommand);
    }

    const blockCommand = parseBlockCommand(body);

    if (blockCommand) {
      return handleBlockTask(
        res,
        user,
        blockCommand.taskId,
        blockCommand.reason,
      );
    }

    const unblockCommand = parseUnblockCommand(body);
    if (unblockCommand) {
      return handleUnblockTask(res, user, unblockCommand.taskId);
    }

    const tasksByNameCommand = parseTasksByNameCommand(body);
    if (tasksByNameCommand) {
      return handleTasksByName(res, user, tasksByNameCommand.assignee_name);
    }

    if (parseWhoIsOnBreakCommand(body)) {
      return handleWhoIsOnBreak(res, user);
    }

    if (parseNowCommand(body)) {
      return handleNowSummary(res, user);
    }

    if (parseSummaryTodayCommand(body)) {
      return handleSummaryToday(res, user);
    }

    if (parseUndoLastTaskChangeCommand(body)) {
      return handleUndoLastTaskChange(res, user);
    }

    const offDayCommand = parseOffDayCommand(body);
    if (offDayCommand) {
      const normalizedRaw = String(body || "").trim();

      // If it starts with "leave on " or "off on ", treat it as self leave
      if (/^(leave|off)\s+on\s+/i.test(normalizedRaw)) {
        return handleSelfOffDay(res, user, offDayCommand);
      }

      // If it is clearly just leave + date, also treat as self leave
      if (
        /^(leave|off)\s+(today|tomorrow|on\s+today|on\s+tomorrow|on\s+[a-z]+\s+\d{1,2}|on\s+\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+|[a-z]+\s+\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+)$/i.test(
          normalizedRaw,
        )
      ) {
        return handleSelfOffDay(res, user, offDayCommand);
      }
    }

    const offDayForOtherCommand = parseOffDayForOtherCommand(body);
    if (offDayForOtherCommand) {
      return handleOffDayForOther(res, user, offDayForOtherCommand);
    }

    if (offDayCommand) {
      return handleSelfOffDay(res, user, offDayCommand);
    }

    const markAttendanceCommand = parseMarkAttendanceCommand(body);
    if (markAttendanceCommand) {
      return handleMarkedAttendance(res, user, markAttendanceCommand);
    }

    const attendanceCommand = parseAttendanceCommand(body);
    if (attendanceCommand) {
      return handleSelfAttendance(res, user, attendanceCommand);
    }

    let taskCommand = parseSimpleTaskCommand(body);
    let aiParsingAttempted = false;

    if (!taskCommand && looksLikeTask(body)) {
      aiParsingAttempted = true;
      taskCommand = await parseTaskWithAI(body);
    }

    console.log("Body received for task parsing:", body);
    console.log("Final task command:", taskCommand);

    if (taskCommand) {
      return handleCreateTask(res, user, taskCommand);
    }

    if (aiParsingAttempted && !taskCommand) {
      return sendTwiml(
        res,
        "I could not parse that task automatically right now. Please use this format: task Ruhab high VPN testing by tomorrow",
      );
    }

    return sendTwiml(
      res,
      "❌ I did not understand that command\nTry: help\nExamples:\nlogin\nmy tasks\ntask Aj high test dashboard by tomorrow",
    );
  } catch (error) {
    console.error("Unhandled /whatsapp error:", error);
    return sendTwiml(res, "Something went wrong.");
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
