import dotenv from "dotenv";
import twilio from "twilio";
import { createClient } from "@supabase/supabase-js";
import OpenAI from "openai";
import { toFile } from "openai/uploads";
import bcrypt from "bcrypt";
import crypto from "crypto";
import XLSX from "xlsx";
import fs from "fs";
import os from "os";
import path from "path";
import ffmpeg from "fluent-ffmpeg";
import ffmpegStatic from "ffmpeg-static";

dotenv.config();

console.log("OPENAI KEY LOADED:", !!process.env.OPENAI_API_KEY);

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

// A client behaves as a "virtual lead business" identified by `client:<id>`,
// backed by the shared `client_leads` table (filtered by client_id) instead of
// a per-business table. This lets the rasset/joolian leads engine be reused
// per-client without touching the existing rasset_leads/joolian_leads tables.
const CLIENT_LEADS_TABLE = "client_leads";

// Static lead businesses whose client-workspace "Leads" tab renders the full
// inline client-leads UI (same as Navii) instead of embedding the standalone
// /leads/<business> page in an iframe. Their backing table must carry the
// client-lead columns (pipeline_stage, outreach_status, demo_status,
// reached_via_*, call_recording_url, is_starred, is_client_visible) — see
// sql/2026-06-25-rasset-leads-client-lead-columns.sql. Adding a business here
// switches both its rendering and its /api/clients/:id/leads write target to
// the static table (see resolveClientLeadBusiness).
const INLINE_CLIENT_LEADS_BUSINESSES = new Set(["rasset"]);

const CLIENT_LEAD_PIPELINE_STAGES = [
  { key: "prospect_identified", label: "Prospect Identified" },
  { key: "connection_sent", label: "Connection Sent" },
  { key: "outreach_initiated", label: "Outreach Initiated" },
  { key: "follow_up_required", label: "Follow-up Required" },
  { key: "follow_up_in_progress", label: "Follow-up in Progress" },
  { key: "engaged", label: "Engaged" },
  { key: "positive_response", label: "Positive Response" },
  { key: "meeting_scheduled", label: "Meeting Scheduled" },
  { key: "meeting_completed", label: "Meeting Completed" },
  { key: "qualified_opportunity", label: "Qualified Opportunity" },
  { key: "pilot_evaluation", label: "Pilot / Evaluation" },
  { key: "commercial_discussion", label: "Commercial Discussion" },
  { key: "converted", label: "Converted" },
  { key: "lost", label: "Lost" },
  { key: "not_interested", label: "Not Interested" },
  { key: "no_response", label: "No Response" },
  { key: "back_to_leads", label: "Back to leads" },
];

// Free-form "Category Type" dropdown on the Add/Edit Lead form (distinct from
// the b2b/b2c Lead Category above). Lives only on client_leads.
const CLIENT_LEAD_CATEGORY_TYPES = [
  { key: "agency_in", label: "Agency-IN" },
  { key: "agency_us", label: "Agency-US" },
  { key: "agency_europe", label: "Agency-Europe" },
  { key: "startups_in", label: "Startups-IN" },
  { key: "startups_us", label: "Startups-US" },
  { key: "scale_up_hiring_us", label: "Scale up hiring-US" },
  { key: "startup_canada", label: "Startup-Canada" },
  { key: "scale_up_hiring_canada", label: "Scale up hiring Canada" },
  { key: "scale_up_hiring_in", label: "Scale up hiring-IN" },
  { key: "scale_up_hiring_europe", label: "Scale up hiring-Europe" },
  { key: "micro", label: "Micro" },
  { key: "small", label: "Small" },
  { key: "medium", label: "Medium" },
  { key: "large", label: "Large" },
];
const CLIENT_LEAD_CATEGORY_TYPE_LABELS = Object.fromEntries(
  CLIENT_LEAD_CATEGORY_TYPES.map((c) => [c.key, c.label]),
);

// Per-client replacements for the Category Type list above.
//
// The default list is agency/startup sizing, which is meaningless for clients
// selling into a different market. A client id present here REPLACES the
// default outright — it is not merged — so the dropdowns, the filter popup and
// the bulk-set control offer only that client's own categories.
//
// Keys are stored in client_leads.category_type, so renaming a key orphans
// existing rows; add a new entry instead and migrate the data.
const CLIENT_LEAD_CATEGORY_TYPE_OVERRIDES = {
  // 22 = Revivflow (recovery/retention, so the categories are the verticals
  // it sells into rather than agency size).
  22: [
    { key: "ecommerce_stores", label: "E-commerce Stores" },
    { key: "subscription_businesses", label: "Subscription Businesses" },
    { key: "online_marketplaces", label: "Online Marketplaces" },
    { key: "travel_companies", label: "Travel Companies" },
    { key: "hospitality", label: "Hospitality" },
    { key: "healthcare_providers", label: "Healthcare Providers" },
    { key: "saas_companies", label: "SaaS Companies" },
    { key: "ticketing_platforms", label: "Ticketing Platforms" },
    { key: "food_delivery_businesses", label: "Food Delivery Businesses" },
    { key: "online_education", label: "Online Education" },
    { key: "digital_product_companies", label: "Digital Product Companies" },
  ],
};

// The Category Type list for one client: its override when it has one, the
// shared default otherwise. Every dropdown, filter and validator must resolve
// through this rather than reading CLIENT_LEAD_CATEGORY_TYPES directly, or a
// client is offered categories the next screen rejects.
function getClientLeadCategoryTypes(clientId) {
  return (
    CLIENT_LEAD_CATEGORY_TYPE_OVERRIDES[Number(clientId)] ||
    CLIENT_LEAD_CATEGORY_TYPES
  );
}

// Label lookup matching the list above. Falls back to the shared labels so a
// key stored before an override was added still renders its old name rather
// than a raw slug.
function getClientLeadCategoryTypeLabels(clientId) {
  const list = getClientLeadCategoryTypes(clientId);
  if (list === CLIENT_LEAD_CATEGORY_TYPES) {
    return CLIENT_LEAD_CATEGORY_TYPE_LABELS;
  }
  return {
    ...CLIENT_LEAD_CATEGORY_TYPE_LABELS,
    ...Object.fromEntries(list.map((c) => [c.key, c.label])),
  };
}
// First stage is the default any new/unstaged lead falls into.
const DEFAULT_CLIENT_LEAD_STAGE = CLIENT_LEAD_PIPELINE_STAGES[0].key;

// Outreach and demo status are tracked separately from the conversion pipeline
// stage so internal records stay complete (e.g. for Navii lead management):
// outreach (how far the contact attempt got) and demo (where the demo stands)
// are distinct dimensions from the overall conversion stage above.
// These columns exist only on client_leads, so the lead engine only writes them
// when the form/request explicitly provides them (see buildBusinessLeadPayloadFromBody).
const CLIENT_LEAD_OUTREACH_STATUSES = [
  { key: "not_started", label: "Not Started" },
  { key: "contacted", label: "Contacted" },
  { key: "follow_up", label: "Follow-up Sent" },
  { key: "responded", label: "Responded" },
  { key: "no_response", label: "No Response" },
];

const CLIENT_LEAD_DEMO_STATUSES = [
  { key: "not_scheduled", label: "Not Scheduled" },
  { key: "scheduled", label: "Scheduled" },
  { key: "completed", label: "Completed" },
  { key: "meeting_rescheduled", label: "Meeting Rescheduled" },
  { key: "brochure_sent", label: "Brochure Sent" },
  { key: "no_show", label: "No-show" },
  { key: "cancelled", label: "Cancelled" },
];

// Channels a lead can be "reached via". Each is a boolean column on client_leads
// (see sql/2026-06-23-client-leads-reach-channels.sql). This single list drives
// the per-lead checkboxes, the REACHED VIA filter, the display label, the bulk
// actions and the PATCH whitelist, so adding a channel is a one-line change here
// plus the column migration. `key` is the checkbox/filter value (column without
// the reached_via_ prefix).
const REACH_VIA_CHANNELS = [
  { key: "linkedin", label: "LinkedIn", column: "reached_via_linkedin" },
  { key: "email", label: "Email", column: "reached_via_email" },
  {
    key: "website_form",
    label: "Website Form",
    column: "reached_via_website_form",
  },
  { key: "whatsapp", label: "WhatsApp", column: "reached_via_whatsapp" },
  { key: "phone", label: "Phone", column: "reached_via_phone" },
  { key: "instagram", label: "Instagram", column: "reached_via_instagram" },
  { key: "facebook", label: "Facebook", column: "reached_via_facebook" },
];

function clientLeadStatusLabel(options, key, fallback) {
  const found = options.find((o) => o.key === key);
  return found ? found.label : fallback;
}

const RASSET_INDUSTRY_OPTIONS = [
  "Automotive",
  "Industrial Equipment",
  "Construction / Concrete",
  "Electronics / Electrical",
  "Textile / Garments",
  "Food & Beverage",
  "Pharma / Chemicals",
  "Packaging",
  "Furniture / Wood",
  "Rubber / Plastics",
  "Paper / Printing",
  "Consumer Goods",
  "Metal Products",
  "Energy / Infrastructure",
  "General Manufacturing",
  "Other",
];

const RASSET_CAPABILITY_OPTIONS = [
  "CNC Machining",
  "CNC Turning",
  "CNC Milling",
  "Laser Cutting",
  "Sheet Metal Fabrication",
  "Welding",
  "Casting",
  "Forging",
  "Injection Molding",
  "Plastic Processing",
  "Tool & Die Making",
  "Assembly",
  "Packaging",
  "Concrete Mixing",
  "Ready Mix Supply",
  "Textile Stitching",
  "Textile Printing",
  "Dyeing",
  "General Fabrication",
  "Other",
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

// Body parsing (urlencoded/json), raw body capture and sessions are handled by
// the Next.js adapter — see lib/server/adapter.js and lib/server/session.js.

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

function getLeadPhoneKey(input) {
  const digits = String(input || "")
    .replace(/^whatsapp:/i, "")
    .replace(/\D/g, "");

  if (!digits) return "";

  return digits.slice(-10);
}

// Normalizes a website to a comparable key (host only, no protocol/www/path) so
// imported client leads can be de-duplicated by site when they have no phone —
// the common case for sourced YC/Navii leads, where every row has a website but
// most have no contact number.
function getLeadWebsiteKey(input) {
  let s = String(input || "")
    .trim()
    .toLowerCase();
  if (!s) return "";
  s = s.replace(/^https?:\/\//, "").replace(/^www\./, "");
  s = s.split(/[\/?#]/)[0];
  return s;
}

// Normalizes an email to a comparable de-dupe key (trimmed, lowercased). Used to
// de-duplicate Navii imports by email, since their sourced rows always carry an
// email but frequently have no phone or website.
function getLeadEmailKey(input) {
  const s = String(input || "")
    .trim()
    .toLowerCase();
  return s.includes("@") ? s : "";
}

// Normalizes a person's LinkedIn profile URL to a comparable de-dupe key
// (/in/<handle>, protocol/host/query/trailing-slash stripped). This is the
// fallback identity for sourced rows whose email cell is not a real address —
// "n/a", "-", or a bare company domain. Those rows produce no email key, and
// Navii-style sheets deliberately skip website de-dupe (many contacts share one
// company website), so without this they re-insert on every single import.
// A LinkedIn profile identifies one person, which is exactly the granularity
// the email key provides.
function getLeadLinkedinKey(input) {
  let s = String(input || "")
    .trim()
    .toLowerCase();
  if (!s) return "";
  s = s.replace(/^https?:\/\//, "").replace(/^[a-z]{2,3}\./, "");
  s = s.split(/[?#]/)[0].replace(/\/+$/, "");
  // Only profile URLs are an identity; company pages are not.
  const m = /linkedin\.com\/in\/([^/]+)/.exec(s);
  return m ? `in/${m[1]}` : "";
}

// A `<select multiple>` field arrives from the urlencoded body parser as
// undefined (nothing selected), a string (one selected), or string[] (several)
// — see toObject() in lib/server/adapter.js. Coerce any of those into a clean,
// de-duplicated array of positive integer user ids.
function parseUserIdList(value) {
  const raw = Array.isArray(value) ? value : value == null ? [] : [value];
  const ids = [];
  for (const v of raw) {
    const n = Number(v);
    if (Number.isInteger(n) && n > 0 && !ids.includes(n)) ids.push(n);
  }
  return ids;
}

// Custom multi-select for "GTM Associate". A native <select multiple> renders as
// a tall list box, so to match the single-line look of the Account/Project
// Manager dropdowns (and sit inline beside them) we render our own control: a
// select-styled box that opens a checkbox panel. The checkboxes are named
// gtm_associate_user_ids and carry the form data directly (checked submit,
// hidden display:none ones still submit) — no hidden inputs needed. The helper
// embeds its own scoped <style>/<script> (guarded so it initializes once) so it
// is a drop-in field on any page without separate CSS/JS wiring.
const GTM_MULTISELECT_CSS = `
  .gtm-ms { position: relative; width: 100%; }
  .gtm-ms-control {
    width: 100%; padding: 12px 13px; border-radius: 12px;
    border: 1px solid var(--line); background: rgba(255,255,255,0.04);
    color: var(--text); font: inherit; cursor: pointer;
    display: flex; align-items: center; justify-content: space-between; gap: 8px;
  }
  .gtm-ms-text { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .gtm-ms-placeholder { color: var(--muted); }
  .gtm-ms-caret { flex: 0 0 auto; opacity: .7; font-size: 12px; transition: transform .15s ease; }
  .gtm-ms.open .gtm-ms-caret { transform: rotate(180deg); }
  .gtm-ms-panel {
    position: absolute; top: calc(100% + 6px); left: 0; right: 0; z-index: 60;
    max-height: 240px; overflow-y: auto; padding: 6px; border-radius: 12px;
    border: 1px solid var(--line); background: var(--panel-strong, #11162a);
    box-shadow: 0 12px 30px rgba(0,0,0,0.45); display: none;
  }
  .gtm-ms.open .gtm-ms-panel { display: block; }
  .gtm-ms-option {
    display: flex; align-items: center; gap: 10px; padding: 8px 9px;
    border-radius: 9px; cursor: pointer; font-weight: 600;
  }
  .gtm-ms-option:hover { background: rgba(255,255,255,0.06); }
  .gtm-ms-option input[type=checkbox] {
    width: auto; min-width: 0; margin: 0; padding: 0; flex: 0 0 auto;
    accent-color: var(--primary, #6d5efc);
  }
`;

const GTM_MULTISELECT_JS = `
  (function () {
    if (window.__gtmMsReady) return;
    window.__gtmMsReady = true;
    window.gtmToggle = function (el) {
      var ms = el.closest('.gtm-ms'); if (!ms) return;
      var willOpen = !ms.classList.contains('open');
      document.querySelectorAll('.gtm-ms.open').forEach(function (o) { if (o !== ms) o.classList.remove('open'); });
      ms.classList.toggle('open', willOpen);
    };
    function gtmUpdate(ms) {
      var names = [];
      ms.querySelectorAll('input[type=checkbox]').forEach(function (c) { if (c.checked) names.push(c.getAttribute('data-name') || ''); });
      var txt = ms.querySelector('.gtm-ms-text'); if (!txt) return;
      if (names.length === 0) { txt.textContent = 'Select GTM associates'; txt.classList.add('gtm-ms-placeholder'); }
      else { txt.textContent = names.join(', '); txt.classList.remove('gtm-ms-placeholder'); }
    }
    document.addEventListener('change', function (e) {
      if (e.target && e.target.matches && e.target.matches('.gtm-ms input[type=checkbox]')) {
        var ms = e.target.closest('.gtm-ms'); if (ms) gtmUpdate(ms);
      }
    });
    document.addEventListener('click', function (e) {
      if (!e.target.closest || !e.target.closest('.gtm-ms')) {
        document.querySelectorAll('.gtm-ms.open').forEach(function (o) { o.classList.remove('open'); });
      }
    });
    function gtmInitAll() { document.querySelectorAll('.gtm-ms').forEach(gtmUpdate); }
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', gtmInitAll);
    else gtmInitAll();
  })();
`;

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

async function uploadLeadCallAudio(buffer, filename) {
  const pathName = `calls/${Date.now()}-${filename}`;

  const { error } = await supabase.storage
    .from("lead-call-recordings")
    .upload(pathName, buffer, {
      contentType: "audio/mpeg",
      upsert: false,
    });

  if (error) {
    throw error;
  }

  const { data } = supabase.storage
    .from("lead-call-recordings")
    .getPublicUrl(pathName);

  return data.publicUrl;
}

const LEAD_AUDIO_BUCKET = "lead-call-recordings";

// Creates the lead audio bucket on first use if it doesn't exist yet, so a
// fresh Supabase project doesn't fail uploads with "Bucket not found". Public
// so the stored audio can be played back via a plain getPublicUrl().
async function ensureLeadAudioBucket() {
  const { error } = await supabase.storage.createBucket(LEAD_AUDIO_BUCKET, {
    public: true,
  });
  // Ignore "already exists" — createBucket is only meant to backfill a missing
  // bucket; any other error is surfaced to the caller.
  if (error && !/exist/i.test(error.message || "")) {
    throw error;
  }
}

// Stores a voice note attached to a lead note in the shared call-recordings
// bucket (under a notes/ prefix) and returns its public URL for playback.
async function uploadLeadNoteAudio(buffer, filename, contentType) {
  const safeName = String(filename || "voice-note").replace(/[^\w.\-]+/g, "_");
  const pathName = `notes/${Date.now()}-${safeName}`;

  await ensureLeadAudioBucket();

  const { error } = await supabase.storage
    .from(LEAD_AUDIO_BUCKET)
    .upload(pathName, buffer, {
      contentType: contentType || "audio/mpeg",
      upsert: false,
    });

  if (error) {
    throw error;
  }

  const { data } = supabase.storage
    .from(LEAD_AUDIO_BUCKET)
    .getPublicUrl(pathName);

  return data.publicUrl;
}

// express.json() and express-session are replaced by the Next.js adapter; the
// session cookie ("connect.sid", same secret/maxAge) is managed in
// lib/server/session.js.

// Build the TwiML reply. Returns the XML string; the route handler turns it
// into a Response. The first parameter is vestigial — it was Express's `res`
// and every one of the ~34 call sites reads `return sendTwiml(res, msg)`, so
// keeping the arity avoids touching all of them.
function sendTwiml(_res, message) {
  try {
    const twiml = new twilio.twiml.MessagingResponse();
    if (message && String(message).trim()) {
      twiml.message(String(message));
    }
    return twiml.toString();
  } catch (err) {
    console.error("sendTwiml failed:", err);
    return new twilio.twiml.MessagingResponse().toString();
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
  const { tableName, clientId } = resolveLeadSource(normalizedBusiness);

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
  // "Assign for Phone" / "Assign for Email" (client_leads only). Named after the
  // filter-popup controls that set them; both take the same values as the
  // assigned_to filter, including the "__unassigned__" sentinel.
  const phoneAssignedToFilter = String(filters.phone_assignee || "").trim();
  const emailAssignedToFilter = String(filters.email_assignee || "").trim();
  // "My leads only" — the logged-in user's name, matched across every assignee
  // role rather than a single column (see below).
  const mineNameFilter = String(filters.mine_name || "").trim();
  const qualifiedFilter = String(filters.qualified || "").trim();
  const worthTalkingFilter = String(filters.worth_talking || "").trim();
  const hasCallTranscriptionFilter = String(
    filters.has_call_transcription || "",
  ).trim();
  // Client-lead-only filters (Status / Demo / Reached-via columns + Notes).
  const pipelineStageFilter = String(filters.pipeline_stage || "").trim();
  const demoStatusFilter = String(filters.demo_status || "").trim();
  const categoryTypeFilter = String(filters.category_type || "").trim();
  const locationFilter = String(filters.location || "").trim();
  const callbackDateFromFilter = String(
    filters.callback_date_from || "",
  ).trim();
  const callbackDateToFilter = String(filters.callback_date_to || "").trim();
  const missedCallbackFilter = String(filters.missed_callback || "").trim();
  const reachedViaFilter = String(filters.reached_via || "").trim();
  const notesFilter = String(filters.notes || "").trim();
  const notesByFilter = String(filters.notes_by || "").trim();
  const noteAudioFilter = String(filters.has_note_audio || "").trim();
  const hasPhoneFilter = String(filters.has_phone || "").trim();
  // Updated-at date-range filter (IST calendar dates, YYYY-MM-DD) and column
  // sorting — applied in JS after the rows are fetched so they can reuse the
  // parsed notes history and the pipeline/demo ordering.
  const updatedFromFilter = String(filters.updated_from || "").trim();
  const updatedToFilter = String(filters.updated_to || "").trim();
  const sortField = String(filters.sort || "").trim();
  const sortDir =
    String(filters.sort_dir || "")
      .trim()
      .toLowerCase() === "asc"
      ? "asc"
      : "desc";
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
      .order("updated_at", { ascending: false });

    if (clientId) {
      query = query.eq("client_id", clientId);
    }

    if (tableName === "rasset_leads" || tableName === CLIENT_LEADS_TABLE) {
      query = query.or("is_deleted.is.null,is_deleted.eq.false");
    }

    // Bulk email search: pasting a list of emails (newline / comma / space /
    // semicolon separated) matches leads whose email equals ANY of them
    // (case-insensitive exact match, not substring). Only kicks in when every
    // token looks like an email, so normal free-text search is untouched.
    const bulkEmailTokens = q
      .split(/[\s,;]+/)
      .map((s) => s.trim())
      .filter(Boolean);
    // Strict charset (no parens/commas/wildcards) so the tokens are safe to
    // embed in the PostgREST or() expression below.
    const isBulkEmailSearch =
      bulkEmailTokens.length > 1 &&
      bulkEmailTokens.every((t) =>
        /^[A-Za-z0-9._%+'-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$/.test(t),
      );
    if (isBulkEmailSearch) {
      query = query.or(
        bulkEmailTokens.map((t) => `email.ilike.${t}`).join(","),
      );
    } else if (q) {
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

    // assigned_to / location exist on every lead table (rasset/joolian/client_leads),
    // so these apply regardless of which table backs the business — unlike the
    // rasset-only filters above.
    if (assignedToFilter === "__unassigned__") {
      query = query.or("assigned_to.is.null,assigned_to.eq.");
    } else if (assignedToFilter) {
      query = query.ilike("assigned_to", `%${assignedToFilter}%`);
    }
    // "My leads only": a lead is mine when I'm named in any assignee role —
    // "Assign for Phone", "Assign for Email", or the overall owner. Navii's
    // owners were moved into phone_assigned_to, so matching assigned_to alone
    // would show them nothing; other businesses still use assigned_to only.
    // Lead tables without the per-channel columns fall back to the owner.
    if (mineNameFilter) {
      const mineColumns = tableHasClientLeadColumns(tableName)
        ? ["phone_assigned_to", "email_assigned_to", "assigned_to"]
        : ["assigned_to"];
      query = query.or(
        mineColumns.map((c) => `${c}.ilike.%${mineNameFilter}%`).join(","),
      );
    }
    // "__none__" = the lead has no location data at all — the same columns the
    // text match below searches must all be null/empty.
    if (locationFilter === "__none__") {
      query = query
        .or("city.is.null,city.eq.")
        .or("state.is.null,state.eq.")
        .or("country.is.null,country.eq.");
    } else if (locationFilter) {
      query = query.or(
        `city.ilike.%${locationFilter}%,state.ilike.%${locationFilter}%,country.ilike.%${locationFilter}%`,
      );
    }
    // "Lead with number": whether the phone column has a value. Applies to every
    // lead table (rasset/joolian/client_leads all carry `phone`).
    if (hasPhoneFilter === "yes") {
      query = query.not("phone", "is", null).neq("phone", "");
    } else if (hasPhoneFilter === "no") {
      query = query.or("phone.is.null,phone.eq.");
    }

    if (tableHasClientLeadColumns(tableName)) {
      // "Assign for Phone" / "Assign for Email" — same matching rules as the
      // assigned_to filter above (exact-ish name match, or "__unassigned__" for
      // rows where nobody was picked), on the client-lead-only columns.
      const applyAssigneeFilter = (column, value) => {
        if (!value) return;
        query =
          value === "__unassigned__"
            ? query.or(`${column}.is.null,${column}.eq.`)
            : query.ilike(column, `%${value}%`);
      };
      applyAssigneeFilter("phone_assigned_to", phoneAssignedToFilter);
      applyAssigneeFilter("email_assigned_to", emailAssignedToFilter);
      // pipeline_stage / demo_status fall back to their first option when the
      // column is null (imported leads leave demo unset), so filtering on that
      // default value also matches null rows. "__none__" is stricter: only
      // rows where the column was never set at all.
      const applyStatusFilter = (column, value, defaultKey) => {
        if (!value) return;
        if (value === "__none__") {
          query = query.is(column, null);
          return;
        }
        query =
          value === defaultKey
            ? query.or(`${column}.eq.${value},${column}.is.null`)
            : query.eq(column, value);
      };
      applyStatusFilter(
        "pipeline_stage",
        pipelineStageFilter,
        DEFAULT_CLIENT_LEAD_STAGE,
      );
      applyStatusFilter(
        "demo_status",
        demoStatusFilter,
        CLIENT_LEAD_DEMO_STATUSES[0].key,
      );
      // Reached-via channels (boolean columns). Multi-select: the filter value
      // is a comma-separated key list and a lead matches when reached via ANY
      // selected channel. "both" (also the legacy single-select value) still
      // requires LinkedIn + Email together, nested as an and() inside the or().
      const reachedViaOrParts = reachedViaFilter
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
        .map((key) => {
          if (key === "both") {
            return "and(reached_via_linkedin.eq.true,reached_via_email.eq.true)";
          }
          // "__none__" = not reached via any channel (every column false/null).
          if (key === "__none__") {
            return `and(${REACH_VIA_CHANNELS.map((c) => `${c.column}.not.is.true`).join(",")})`;
          }
          const ch = REACH_VIA_CHANNELS.find((c) => c.key === key);
          return ch ? `${ch.column}.eq.true` : null;
        })
        .filter(Boolean);
      if (reachedViaOrParts.length) {
        query = query.or(reachedViaOrParts.join(","));
      }
      // Category type — multi-select, comma-separated keys; a lead matches ANY
      // selected type. A single key (e.g. from the pill row) is a 1-item list.
      // "__none__" matches leads with no category type set.
      const categoryTypeKeys = categoryTypeFilter
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      if (categoryTypeKeys.length) {
        const namedKeys = categoryTypeKeys.filter((k) => k !== "__none__");
        const categoryOrParts = [];
        if (categoryTypeKeys.includes("__none__")) {
          categoryOrParts.push("category_type.is.null", "category_type.eq.");
        }
        if (namedKeys.length) {
          categoryOrParts.push(`category_type.in.(${namedKeys.join(",")})`);
        }
        query = query.or(categoryOrParts.join(","));
      }
      // Callback date range (inclusive, plain YYYY-MM-DD comparison — the
      // column carries no time/timezone component).
      if (callbackDateFromFilter) {
        query = query.gte("callback_date", callbackDateFromFilter);
      }
      if (callbackDateToFilter) {
        query = query.lte("callback_date", callbackDateToFilter);
      }
      // Missed callback: mirrors the red/green callback badge. A lead only shows
      // the badge when it has a callback_date, so both directions require one.
      // "yes" = overdue (past, red: callback_date < today); "no" = upcoming
      // (today or future, green: callback_date >= today).
      if (missedCallbackFilter === "yes" || missedCallbackFilter === "no") {
        const todayStr = getTodayDateStringInTimeZone(APP_TIMEZONE);
        query = query.not("callback_date", "is", null);
        query =
          missedCallbackFilter === "yes"
            ? query.lt("callback_date", todayStr)
            : query.gte("callback_date", todayStr);
      } else if (missedCallbackFilter === "none") {
        // "None" = no callback date set at all (no badge either way).
        query = query.is("callback_date", null);
      }
    }

    // Supabase caps a single response at ~1000 rows, so a client with several
    // thousand leads would otherwise show only the first 1000 (and a wrong
    // total). Page through the filtered result set in 1000-row batches to load
    // every row; the exact count comes back with each batch. A batch ceiling
    // guards against a bad count spinning the loop forever.
    const FETCH_BATCH = 1000;
    const MAX_FETCH_BATCHES = 50; // up to 50k leads
    let exactCount = null;
    for (let batchIdx = 0; batchIdx < MAX_FETCH_BATCHES; batchIdx += 1) {
      const fetchOffset = batchIdx * FETCH_BATCH;
      const { data, error, count } = await query.range(
        fetchOffset,
        fetchOffset + FETCH_BATCH - 1,
      );
      if (error) throw error;
      if (exactCount === null) exactCount = count;
      const batch = data || [];
      businessRows = businessRows.concat(batch);
      if (batch.length < FETCH_BATCH) break;
      if (exactCount != null && fetchOffset + FETCH_BATCH >= exactCount) break;
    }
    totalBusinessCount = exactCount != null ? exactCount : businessRows.length;

    // Notes filters need the JSON notes history parsed, so they run in JS here
    // (not in the DB query) and the count is recomputed to match.
    if (
      tableHasClientLeadColumns(tableName) &&
      (notesFilter || notesByFilter || noteAudioFilter)
    ) {
      const byNeedle = notesByFilter.toLowerCase();
      businessRows = businessRows.filter((row) => {
        const history = parseLeadNotesHistory(row.notes);
        if (notesFilter === "added" && history.length < 1) return false;
        if (notesFilter === "multiple" && history.length < 2) return false;
        if (notesFilter === "none" && history.length > 0) return false;
        if (notesByFilter === "__none__") {
          if (history.length > 0) return false;
        } else if (
          notesByFilter &&
          !history.some(
            (n) =>
              String(n.by || "")
                .trim()
                .toLowerCase() === byNeedle,
          )
        ) {
          return false;
        }
        if (noteAudioFilter) {
          const hasAudio = history.some((n) => n && n.audio_url);
          if (noteAudioFilter === "yes" && !hasAudio) return false;
          if (noteAudioFilter === "no" && hasAudio) return false;
        }
        return true;
      });
      totalBusinessCount = businessRows.length;
    }

    // Updated-at date range: compare the row's IST calendar date against the
    // from/to bounds (inclusive). Rows with no updated_at are excluded once a
    // bound is set. Matches the client-view leads filter behaviour.
    if (updatedFromFilter || updatedToFilter) {
      businessRows = businessRows.filter((row) => {
        if (!row.updated_at) return false;
        const ud = getDateStringInTimeZone(
          new Date(row.updated_at),
          APP_TIMEZONE,
        );
        if (updatedFromFilter && ud < updatedFromFilter) return false;
        if (updatedToFilter && ud > updatedToFilter) return false;
        return true;
      });
      totalBusinessCount = businessRows.length;
    }
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
    // Pipeline-stage tabs (client_leads only).
    if (CLIENT_LEAD_PIPELINE_STAGES.some((s) => s.key === selectedTab)) {
      return (x.pipeline_stage || DEFAULT_CLIENT_LEAD_STAGE) === selectedTab;
    }
    return true;
  });

  // Column sort. The DB already returns updated_at desc, so we only re-sort when
  // an explicit sort field is requested. Stage/demo sort by their pipeline
  // order (not alphabetically); notes by history length; updated by timestamp.
  if (sortField) {
    const dirMul = sortDir === "asc" ? 1 : -1;
    const sortValue = (row) => {
      switch (sortField) {
        case "name":
          return String(row.company || row.business_name || "").toLowerCase();
        case "stage":
          return CLIENT_LEAD_PIPELINE_STAGES.findIndex(
            (s) => s.key === (row.pipeline_stage || DEFAULT_CLIENT_LEAD_STAGE),
          );
        case "demo":
          return CLIENT_LEAD_DEMO_STATUSES.findIndex(
            (s) =>
              s.key === (row.demo_status || CLIENT_LEAD_DEMO_STATUSES[0].key),
          );
        case "notes":
          return parseLeadNotesHistory(row.notes).length;
        case "updated":
          return row.updated_at ? new Date(row.updated_at).getTime() : 0;
        default:
          return 0;
      }
    };
    filteredBusinessRows.sort((a, b) => {
      const av = sortValue(a);
      const bv = sortValue(b);
      if (av < bv) return -1 * dirMul;
      if (av > bv) return 1 * dirMul;
      return 0;
    });
  }

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
      qualified: businessRows.filter((x) =>
        [
          "qualified_opportunity",
          "pilot_evaluation",
          "commercial_discussion",
          "converted",
        ].includes(x.pipeline_stage),
      ).length,
      meeting_completed: businessRows.filter(
        (x) => x.pipeline_stage === "meeting_completed",
      ).length,
      converted: businessRows.filter((x) => x.pipeline_stage === "converted")
        .length,
      voice_inbox: voiceInboxRows.length,
      total: totalBusinessCount,
      pending_review: voice.filter((x) => x.status === "pending_review").length,
    },
    // Ids of every row matching the current tab/search/filters across all
    // pages — powers the "Select all N leads" bulk option on the Leads tab.
    filteredIds: filteredBusinessRows.map((r) => Number(r.id)).filter(Boolean),
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
      pipeline_stage: pipelineStageFilter,
      demo_status: demoStatusFilter,
      category_type: categoryTypeFilter,
      location: locationFilter,
      reached_via: reachedViaFilter,
      notes: notesFilter,
      notes_by: notesByFilter,
      has_note_audio: noteAudioFilter,
      updated_from: updatedFromFilter,
      updated_to: updatedToFilter,
      callback_date_from: callbackDateFromFilter,
      callback_date_to: callbackDateToFilter,
      sort: sortField,
      sort_dir: sortDir,
    },
  };
}

// Distinct category_type values (+ counts) present across a lead table's rows
// for one business/client, ignoring every other active filter — powers the
// clickable Category Type pill row on the client workspace Leads tab so users
// can always see/switch categories regardless of what's currently filtered.
async function getClientLeadCategoryTypeCounts(orgId, tableName, clientId) {
  if (!tableName || !tableHasClientLeadColumns(tableName)) return [];

  let query = supabase
    .from(tableName)
    .select("category_type", { count: "exact" })
    .eq("org_id", orgId)
    .not("category_type", "is", null);
  if (clientId) query = query.eq("client_id", clientId);
  query = query.or("is_deleted.is.null,is_deleted.eq.false");

  const counts = {};
  const FETCH_BATCH = 1000;
  let exactCount = null;
  for (let batchIdx = 0; batchIdx < 50; batchIdx += 1) {
    const offset = batchIdx * FETCH_BATCH;
    const { data, error, count } = await query.range(
      offset,
      offset + FETCH_BATCH - 1,
    );
    if (error) throw error;
    if (exactCount === null) exactCount = count;
    (data || []).forEach((row) => {
      const key = String(row.category_type || "").trim();
      if (!key) return;
      counts[key] = (counts[key] || 0) + 1;
    });
    if (!data || data.length < FETCH_BATCH) break;
    if (exactCount != null && offset + FETCH_BATCH >= exactCount) break;
  }

  return Object.entries(counts)
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count);
}

// How many status transitions to keep per lead for the Leads-table history.
// Everything past this is dropped server side so a long-lived lead can't bloat
// the page payload; the table itself only previews the newest few.
const CLIENT_LEAD_STATUS_HISTORY_MAX = 30;

// How many of those transitions the Status cell shows inline before collapsing
// the rest behind the "earlier changes" link.
const LEAD_STATUS_HISTORY_PREVIEW = 2;

// Pipeline-stage change history for the leads currently on screen, keyed by
// lead id and newest first. Reads the `client_lead_status_changed` activity
// events every status write path emits (see the PATCH / note-audio routes), so
// the Status column can show who moved the lead and when.
async function getClientLeadStatusHistory(orgId, clientId, leadIds) {
  const ids = (leadIds || []).map((id) => Number(id)).filter(Boolean);
  if (!ids.length) return {};

  const { data, error } = await supabase
    .from("client_activity_logs")
    .select("entity_id, actor_user_id, new_value, created_at")
    .eq("org_id", orgId)
    .eq("client_id", clientId)
    .eq("action", "client_lead_status_changed")
    .in("entity_id", ids)
    .order("created_at", { ascending: false });
  if (error) throw error;

  const byLead = {};
  (data || []).forEach((row) => {
    const nv = row.new_value || {};
    // The same action also carries outreach/demo transitions (funnel reporting);
    // the Status column only tracks the pipeline stage.
    if (nv.field !== "pipeline_stage" || !nv.to) return;
    const key = String(row.entity_id);
    if (!byLead[key]) byLead[key] = [];
    if (byLead[key].length >= CLIENT_LEAD_STATUS_HISTORY_MAX) return;
    byLead[key].push({
      from: nv.from || null,
      to: nv.to,
      at: row.created_at,
      by_user_id: row.actor_user_id || null,
    });
  });
  return byLead;
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
// hello

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

// Themes SweetAlert2 popups to match the dashboard (dark panels, site fonts,
// brand primary/danger buttons). Include alongside buildThemeCss on any page
// that calls Swal.fire.
function buildSweetAlertCss() {
  return `
    .swal2-container { backdrop-filter: blur(2px); }
    .swal2-container .swal2-backdrop-show,
    .swal2-backdrop-show { background: rgba(5, 8, 16, 0.72) !important; }

    .swal2-popup {
      background: var(--panel) !important;
      color: var(--text) !important;
      border: 1px solid var(--line) !important;
      border-radius: var(--radius-lg) !important;
      box-shadow: var(--shadow-card) !important;
      font-family: inherit !important;
    }

    .swal2-title { color: var(--text-strong) !important; font-weight: 800 !important; }
    .swal2-html-container { color: var(--muted) !important; }

    .swal2-actions { gap: 10px !important; }

    .swal2-styled {
      border-radius: var(--radius-md) !important;
      font-weight: 700 !important;
      box-shadow: none !important;
      transition: filter .15s ease, transform .05s ease;
    }
    .swal2-styled:focus { box-shadow: none !important; }
    .swal2-styled:active { transform: translateY(1px); }

    .swal2-styled.swal2-confirm {
      background: var(--danger) !important;
      color: #fff !important;
    }
    .swal2-styled.swal2-confirm:hover { filter: brightness(1.08); }

    .swal2-styled.swal2-cancel {
      background: var(--panel-strong) !important;
      color: var(--text) !important;
      border: 1px solid var(--line-strong) !important;
    }
    .swal2-styled.swal2-cancel:hover { filter: brightness(1.12); }

    .swal2-icon.swal2-warning {
      border-color: var(--danger) !important;
      color: var(--danger) !important;
    }
    .swal2-icon.swal2-error { border-color: var(--danger) !important; }
    .swal2-icon.swal2-success { border-color: var(--success) !important; }
    .swal2-close { color: var(--muted) !important; }
    .swal2-close:hover { color: var(--text-strong) !important; }
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
      grid-template-columns: auto minmax(0, 1fr) auto;
      align-items: center;
      gap: 12px;
      min-height: 64px;
    }

    .brand {
      height: 42px;
      padding: 0 14px;
      border-radius: 14px;
      display: inline-flex;
      align-items: center;
      font-size: 18px;
      font-weight: 900;
      letter-spacing: -0.04em;
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
  min-width: 210px;
  padding: 8px;
  border-radius: 14px;
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  box-shadow: var(--shadow-soft);
  z-index: 9999;
}

.nav-dropdown-wrap:hover > .nav-dropdown-menu {
  display: block;
}

.nav-submenu-wrap {
  position: relative;
}

.nav-dropdown-menu a,
.nav-submenu-label {
  display: flex !important;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  margin: 0;
  padding: 10px 11px !important;
  border-radius: 10px !important;
  background: transparent !important;
  border: 0 !important;
  text-align: left;
  color: var(--text);
  text-decoration: none;
  font-weight: 800;
  white-space: nowrap;
}

.nav-dropdown-menu a:hover,
.nav-submenu-label:hover {
  background: rgba(255,255,255,0.07) !important;
  transform: none !important;
}

.nav-submenu-menu {
  display: none;
  position: absolute;
  top: 0;
  left: calc(100% + 8px);
  min-width: 180px;
  padding: 8px;
  border-radius: 14px;
  background: linear-gradient(180deg, var(--panel), var(--panel-strong));
  border: 1px solid var(--line);
  box-shadow: var(--shadow-soft);
}

/* Invisible hover bridge across the gap between the parent row and the submenu,
   so the cursor can travel into the submenu without it closing (the diagonal /
   dead-zone problem). It is part of the submenu (a child of the wrap), so
   hovering it keeps .nav-submenu-wrap:hover true. */
.nav-submenu-menu::before {
  content: "";
  position: absolute;
  top: 0;
  bottom: 0;
  left: -14px;
  width: 16px;
}

.nav-submenu-wrap:hover > .nav-submenu-menu {
  display: block;
}

    .top-nav-end {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      flex-wrap: nowrap;
      min-width: 0;
    }

    .top-nav-end a {
      color: var(--text);
      text-decoration: none;
      padding: 9px 13px;
      border-radius: 13px;
      border: 1px solid color-mix(in srgb, var(--secondary) 30%, transparent);
      background: var(--secondary-soft);
      font-weight: 750;
      transition: all 0.15s ease;
      white-space: nowrap;
      flex: 0 0 auto;
      min-height: 40px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }

    .top-nav-end a:hover {
      color: var(--text-strong);
    }

    .top-nav-end a.active {
      color: var(--text-strong);
      border-color: color-mix(in srgb, var(--primary) 55%, transparent);
      background: var(--primary-soft);
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
        grid-template-columns: auto minmax(0, 1fr);
      }

      .top-nav-end {
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

// ----------------------------------------------------------------------
// Auto-report chart kit — Tableau-style visuals built from pure inline
// SVG/CSS so the exact same markup renders on both the internal workspace
// and the public client-view page with zero client-side dependencies.
// Every primitive is data-correct: bar widths, stack flex ratios and donut
// arc lengths are derived straight from the passed counts.
// ----------------------------------------------------------------------

// Shared metric vocabulary (count metrics; incentive is money, kept apart) so
// every chart, legend and table colour-codes the same metric identically.
const CLIENT_REPORT_METRICS = [
  { key: "campaigns", label: "Campaigns", color: "#8b7cf6" },
  { key: "converted", label: "Converted", color: "#58c98a" },
  { key: "meetings", label: "Meetings", color: "#6ea8ff" },
  { key: "moms", label: "MOMs", color: "#f3b562" },
  { key: "blockers", label: "Blockers", color: "#ef6b73" },
];
// Pipeline stages that represent a dead end rather than forward progress —
// drawn in red and split below the funnel so the positive path stays clean.
const CLIENT_REPORT_NEGATIVE_STAGES = new Set([
  "lost",
  "not_interested",
  "no_response",
]);

const CLIENT_REPORT_ICONS = {
  megaphone:
    '<path d="M3 8.5 13 5v9L3 11.5z"/><path d="M13 6.5 16 6v6l-3-.5z"/><path d="M5.5 11.5V14a2 2 0 0 0 3.6 1.2"/>',
  check: '<circle cx="9" cy="9" r="6.5"/><path d="M6 9.2 8.2 11.4 12 7"/>',
  calendar:
    '<rect x="3" y="4" width="12" height="11" rx="2"/><path d="M3 7.5h12M6.5 2.5V5M11.5 2.5V5"/>',
  doc: '<path d="M5 2.5h5l4 4V15a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V3.5a1 1 0 0 1 1-1z"/><path d="M10 2.5V6h4"/>',
  alert: '<path d="M9 3 16 15H2z"/><path d="M9 7.5v3.5M9 13h.01"/>',
  rupee: '<path d="M6 4h6M6 7h6M6 4.4c4 0 4.4 5.6.4 5.6H7l4.6 5.6"/>',
  userplus:
    '<circle cx="7.5" cy="6.5" r="2.8"/><path d="M3 15.5a4.5 4.5 0 0 1 9 0"/><path d="M14 6v4M12 8h4"/>',
  moves:
    '<path d="M3 5.5h3.3L13 13h2.7"/><path d="M13.2 11.3 15.7 13l-2.5 1.7"/><path d="M3 14.5h3.3L9 11.2"/>',
  layers: '<path d="M9 2.5 16 6l-7 3.5L2 6z"/><path d="M2 10.5 9 14l7-3.5"/>',
  percent:
    '<path d="M5 14 14 5"/><circle cx="6.2" cy="6.2" r="1.8"/><circle cx="12.8" cy="12.8" r="1.8"/>',
  flag: '<path d="M5 16V3"/><path d="M5 3.5h8l-2 3 2 3H5"/>',
};
function arIcon(name) {
  return `<svg viewBox="0 0 18 18" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${CLIENT_REPORT_ICONS[name] || ""}</svg>`;
}

// A single energetic KPI tile: icon chip + big number, tinted by `color`.
function arKpiCard({ label, value, sub = "", color = "#8b7cf6", icon = "" }) {
  return `
    <div class="ar-kpi" style="--c:${color};">
      <div class="ar-kpi-top">
        <span class="ar-kpi-ico">${icon ? arIcon(icon) : ""}</span>
        <span class="ar-kpi-label">${escapeHtml(label)}</span>
      </div>
      <div class="ar-kpi-value">${escapeHtml(String(value))}</div>
      ${sub ? `<div class="ar-kpi-sub">${escapeHtml(sub)}</div>` : ""}
    </div>`;
}

// Colour key. Pass `value` on an item to show its count inline.

// Horizontal bar chart. rows: [{label, value, color?}]; widths scale to the
// largest value in the set so the leader fills the track.
function arBars(
  rows,
  { color = "#8b7cf6", emptyLabel = "No data yet.", hideZero = false } = {},
) {
  const visible = hideZero ? rows.filter((r) => Number(r.value) > 0) : rows;
  if (!visible.some((r) => Number(r.value) > 0)) {
    return `<div class="ar-empty">${escapeHtml(emptyLabel)}</div>`;
  }
  const max = Math.max(1, ...visible.map((r) => Number(r.value) || 0));
  return `<div class="ar-bars">${visible
    .map((r) => {
      const v = Number(r.value) || 0;
      const pct = v > 0 ? Math.max(Math.round((v / max) * 100), 3) : 0;
      return `
      <div class="ar-bar-row">
        <div class="ar-bar-label" title="${escapeHtml(r.label)}">${escapeHtml(r.label)}</div>
        <div class="ar-bar-track"><div class="ar-bar-fill" style="width:${pct}%; --c:${r.color || color};"></div></div>
        <div class="ar-bar-val">${escapeHtml(String(v))}</div>
      </div>`;
    })
    .join("")}</div>`;
}

// Per-row stacked bars — one row per member, segments sized by each metric's
// share (via flex-grow) and the whole bar scaled to the busiest member.

// SVG donut. Each visible segment is one full-circle stroke clipped to its
// arc via stroke-dasharray and pushed into place with a negative dashoffset.

// Centre-anchored funnel. Positive stages flow violet -> green by depth; the
// dead-end stages are grouped below in red. Width scales to the fullest stage.
function arFunnelChart(stages, total) {
  if (!total)
    return `<div class="ar-empty">No leads in the pipeline yet.</div>`;
  const max = Math.max(1, ...stages.map((s) => Number(s.value) || 0));
  const positives = stages.filter((s) => !s.negative);
  const negatives = stages.filter((s) => s.negative);
  const posSpan = Math.max(1, positives.length - 1);
  const row = (s, i, span, isNeg) => {
    const v = Number(s.value) || 0;
    const pct = v > 0 ? Math.max(Math.round((v / max) * 100), 3) : 2;
    const share = total ? Math.round((v / total) * 100) : 0;
    const hue = isNeg ? 353 : Math.round(262 - 112 * (span > 1 ? i / span : 0));
    const color = `hsl(${hue} ${isNeg ? 74 : 66}% 61%)`;
    return `
      <div class="ar-funnel-row">
        <div class="ar-funnel-label" title="${escapeHtml(s.label)}">${escapeHtml(s.label)}</div>
        <div class="ar-funnel-bar-wrap"><div class="ar-funnel-bar" style="width:${pct}%; background:${color};">${v > 0 ? `<span>${v}</span>` : ""}</div></div>
        <div class="ar-funnel-meta">${v}<small> · ${share}%</small></div>
      </div>`;
  };
  return `<div class="ar-funnel">
    ${positives.map((s, i) => row(s, i, posSpan, false)).join("")}
    ${negatives.length ? `<div class="ar-funnel-divider"><span>Closed · negative</span></div>${negatives.map((s) => row(s, 0, 1, true)).join("")}` : ""}
  </div>`;
}

// Tableau "highlight table": a crosstab where every numeric cell carries a
// data bar scaled to its column max. columns[0] is the label column.
function arHighlightTable({
  columns,
  rows,
  totals,
  emptyLabel = "No data yet.",
}) {
  const numCols = columns.filter((c) => !c.isLabel);
  const max = {};
  numCols.forEach((c) => {
    max[c.key] = Math.max(1, ...rows.map((r) => Number(r[c.key]) || 0));
  });
  const fmt = (c, v) => (c.fmt ? c.fmt(v) : String(v));
  const head = `<tr>${columns
    .map(
      (c) =>
        `<th class="${c.isLabel ? "ar-th-label" : "ar-th-num"}">${escapeHtml(c.label)}</th>`,
    )
    .join("")}</tr>`;
  const body = rows.length
    ? rows
        .map(
          (r) =>
            `<tr>${columns
              .map((c) => {
                if (c.isLabel)
                  return `<td class="ar-td-label">${escapeHtml(String(r[c.key] ?? ""))}</td>`;
                const v = Number(r[c.key]) || 0;
                const pct =
                  v > 0 ? Math.max(Math.round((v / max[c.key]) * 100), 5) : 0;
                return `<td class="ar-td-num"><span class="ar-cell-num">${escapeHtml(fmt(c, v))}</span><span class="ar-cell-track"><span class="ar-cell-bar" style="width:${pct}%; --c:${c.color};"></span></span></td>`;
              })
              .join("")}</tr>`,
        )
        .join("")
    : `<tr><td colspan="${columns.length}" class="ar-empty-cell">${escapeHtml(emptyLabel)}</td></tr>`;
  const foot =
    rows.length && totals
      ? `<tr class="ar-total-row">${columns
          .map((c, i) =>
            i === 0
              ? `<td class="ar-td-label">Total</td>`
              : `<td class="ar-td-num"><span class="ar-cell-num">${escapeHtml(fmt(c, Number(totals[c.key]) || 0))}</span></td>`,
          )
          .join("")}</tr>`
      : "";
  return `<div class="ar-table-wrap"><table class="ar-table"><thead>${head}</thead><tbody>${body}</tbody>${foot ? `<tfoot>${foot}</tfoot>` : ""}</table></div>`;
}

// Full activity report (daily or weekly): KPI tiles, a team-contribution
// stacked-bar chart, an activity-mix donut and a per-member highlight table.

// Scoped stylesheet for the report kit. Emitted once per page (prepended to
// the daily section). Uses the page theme vars with hard fallbacks so it is
// safe on both the internal workspace and the public client-view page.
const CLIENT_REPORT_STYLES = `<style id="ar-report-styles">
.ar-wrap{padding:20px;}
.ar-head{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:18px;}
.ar-head h2{margin:0;font-size:20px;font-weight:900;letter-spacing:-.01em;color:var(--text-strong,#fff);}
.ar-eyebrow{display:inline-flex;align-items:center;gap:7px;font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.09em;color:var(--primary,#8b7cf6);margin-bottom:7px;}
.ar-eyebrow svg{width:14px;height:14px;}
.ar-sub{color:var(--muted,#c4cce0);font-size:13px;margin-top:4px;}
.ar-chip{display:inline-flex;align-items:center;gap:7px;padding:7px 12px;border-radius:999px;font-size:12px;font-weight:800;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.11);color:var(--text,#e7ecf6);white-space:nowrap;}
.ar-live-dot{width:7px;height:7px;border-radius:50%;background:var(--success,#58c98a);animation:ar-pulse 2s infinite;}
@keyframes ar-pulse{0%{box-shadow:0 0 0 0 color-mix(in srgb,var(--success,#58c98a) 55%,transparent);}70%{box-shadow:0 0 0 7px transparent;}100%{box-shadow:0 0 0 0 transparent;}}

.ar-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(158px,1fr));gap:12px;margin-bottom:16px;}
.ar-kpi{position:relative;overflow:hidden;padding:15px 16px 16px;border-radius:16px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(150deg,color-mix(in srgb,var(--c) 15%,transparent),rgba(255,255,255,.02));transition:transform .18s ease,border-color .18s ease;}
.ar-kpi:hover{transform:translateY(-2px);border-color:color-mix(in srgb,var(--c) 45%,transparent);}
.ar-kpi::after{content:"";position:absolute;left:0;top:0;bottom:0;width:4px;background:var(--c);}
.ar-kpi-top{display:flex;align-items:center;gap:9px;}
.ar-kpi-ico{display:inline-grid;place-items:center;width:30px;height:30px;border-radius:10px;background:color-mix(in srgb,var(--c) 24%,transparent);color:var(--c);flex:none;}
.ar-kpi-ico svg{width:17px;height:17px;}
.ar-kpi-label{font-size:11.5px;font-weight:800;color:var(--muted,#c4cce0);text-transform:uppercase;letter-spacing:.04em;}
.ar-kpi-value{font-size:30px;font-weight:900;line-height:1.04;margin-top:12px;color:var(--text-strong,#fff);}
.ar-kpi-sub{margin-top:4px;font-size:11.5px;font-weight:700;color:var(--muted,#c4cce0);}

.ar-charts{display:grid;grid-template-columns:1.55fr 1fr;gap:14px;margin-bottom:14px;}
.ar-charts3{display:grid;grid-template-columns:repeat(auto-fit,minmax(228px,1fr));gap:14px;margin-bottom:14px;}
.ar-card{padding:16px 17px;border-radius:16px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.022);}
.ar-card-wide{margin-bottom:14px;}
.ar-card-center{display:flex;flex-direction:column;}
.ar-card-head{display:flex;align-items:baseline;justify-content:space-between;gap:10px;margin-bottom:16px;}
.ar-card-title{font-size:12.5px;font-weight:800;color:var(--text-strong,#fff);text-transform:uppercase;letter-spacing:.06em;}
.ar-card-sub{font-size:11.5px;color:var(--muted,#c4cce0);font-weight:600;}

.ar-bars{display:grid;gap:11px;}
.ar-bar-row{display:grid;grid-template-columns:122px 1fr 46px;align-items:center;gap:11px;}
.ar-bar-label{font-size:12px;font-weight:700;color:var(--text,#e7ecf6);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.ar-bar-track{position:relative;height:13px;border-radius:999px;background:rgba(255,255,255,.06);overflow:hidden;}
.ar-bar-fill{height:100%;border-radius:999px;min-width:3px;background:linear-gradient(90deg,color-mix(in srgb,var(--c) 55%,transparent),var(--c));transition:width .55s cubic-bezier(.22,.61,.36,1);}
.ar-stack{display:flex;height:13px;border-radius:999px;overflow:hidden;min-width:3px;transition:width .55s cubic-bezier(.22,.61,.36,1);}
.ar-seg{height:100%;background:var(--c);}
.ar-bar-val{font-size:12.5px;font-weight:800;color:var(--text-strong,#fff);text-align:right;}

.ar-legend{display:flex;flex-wrap:wrap;gap:8px 14px;margin-top:15px;}
.ar-legend-item{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;font-weight:700;color:var(--muted,#c4cce0);}
.ar-legend-item b{color:var(--text-strong,#fff);font-weight:800;}
.ar-dot{width:10px;height:10px;border-radius:3px;background:var(--c);flex:none;}

.ar-donut-wrap{display:flex;align-items:center;justify-content:center;flex:1;padding:8px 0 4px;}
.ar-donut{width:176px;height:176px;}
.ar-donut-num{font-size:30px;font-weight:900;}
.ar-donut-sub{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;}

.ar-funnel{display:grid;gap:7px;}
.ar-funnel-row{display:grid;grid-template-columns:150px 1fr 84px;align-items:center;gap:12px;}
.ar-funnel-label{font-size:12px;font-weight:700;color:var(--text,#e7ecf6);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.ar-funnel-bar-wrap{display:flex;justify-content:center;}
.ar-funnel-bar{height:25px;border-radius:7px;min-width:5px;display:flex;align-items:center;justify-content:center;box-shadow:inset 0 -7px 11px rgba(0,0,0,.16),inset 0 7px 9px rgba(255,255,255,.10);transition:width .55s cubic-bezier(.22,.61,.36,1);}
.ar-funnel-bar span{font-size:11px;font-weight:800;color:rgba(8,12,25,.8);white-space:nowrap;}
.ar-funnel-meta{font-size:13px;font-weight:800;color:var(--text-strong,#fff);text-align:right;}
.ar-funnel-meta small{color:var(--muted,#c4cce0);font-weight:700;}
.ar-funnel-divider{display:flex;align-items:center;gap:10px;margin:7px 0 1px;color:var(--muted,#c4cce0);font-size:10.5px;font-weight:800;text-transform:uppercase;letter-spacing:.09em;}
.ar-funnel-divider span{flex:none;}
.ar-funnel-divider::after{content:"";flex:1;height:1px;background:rgba(255,255,255,.12);}

.ar-table-wrap{overflow-x:auto;}
.ar-table{width:100%;border-collapse:collapse;}
.ar-table th{font-size:10.5px;font-weight:800;text-transform:uppercase;letter-spacing:.07em;color:var(--muted,#c4cce0);padding:0 12px 11px;border-bottom:1px solid rgba(255,255,255,.10);}
.ar-th-label{text-align:left;}
.ar-th-num{text-align:right;}
.ar-table td{padding:12px;border-bottom:1px solid rgba(255,255,255,.055);vertical-align:middle;}
.ar-td-label{font-weight:800;color:var(--text-strong,#fff);font-size:13px;white-space:nowrap;}
.ar-td-num{text-align:right;min-width:78px;}
.ar-cell-num{display:block;text-align:right;font-size:13px;font-weight:800;color:var(--text-strong,#fff);}
.ar-cell-track{display:block;height:4px;border-radius:3px;background:rgba(255,255,255,.06);margin-top:6px;overflow:hidden;}
.ar-cell-bar{display:block;height:100%;border-radius:3px;background:var(--c);transition:width .55s cubic-bezier(.22,.61,.36,1);}
.ar-total-row td{border-top:2px solid rgba(255,255,255,.18);border-bottom:none;padding-top:13px;}
.ar-total-row .ar-cell-num,.ar-total-row .ar-td-label{font-weight:900;}
.ar-empty-cell{text-align:center;color:var(--muted,#c4cce0);font-style:italic;font-size:12.5px;padding:22px 12px;}
.ar-empty{padding:20px;text-align:center;color:var(--muted,#c4cce0);font-size:12.5px;font-style:italic;border:1px dashed rgba(255,255,255,.13);border-radius:12px;}

@media (max-width:900px){.ar-charts{grid-template-columns:1fr;}}
@media (max-width:560px){
  .ar-wrap{padding:15px;}
  .ar-bar-row{grid-template-columns:90px 1fr 38px;gap:8px;}
  .ar-funnel-row{grid-template-columns:100px 1fr 64px;gap:8px;}
}
</style>`;

// The auto-report chart styles, as plain CSS for scripts/gen-css.mjs.
// CLIENT_REPORT_STYLES wraps the same rules in a <style> tag for the legacy
// server-rendered pages; this returns the body so app/globals.css can carry it
// for the React pages. Both read from the one constant — they cannot drift.
function buildAutoReportCss() {
  return CLIENT_REPORT_STYLES.replace(/^\s*<style[^>]*>/, "").replace(
    /<\/style>\s*$/,
    "",
  );
}

// Auto-computed daily / weekly / funnel report sections, shared by the internal
// client workspace (renderClientWorkspacePage) and the public client-view page
// (renderClientViewOnlyPage) so both render the identical report. Self-contained:
// computes its own per-user name lookup + MOM-filled check from the passed data.
//
// The report tab shows a "Daily" view plus one view per calendar week
// (Week 1 = the current Mon–Sat week, Week 2 = the previous Mon–Sat week, ...),
// back to the earliest tracked activity. Weeks run Monday 00:00 → Saturday end
// (Sunday is excluded), aligned to the calendar — NOT a rolling 7-day window off
// "now". CLIENT_REPORT_MAX_WEEKS caps how many week tabs are built (and how far
// back status-change logs are loaded by the callers) so old or stray timestamps
// can't explode the tab bar.
const CLIENT_REPORT_MAX_WEEKS = 104;

// Monday 00:00 (UTC) of the calendar week containing `ms`. Epoch is aligned to
// UTC midnight, so flooring to the day then backing up to Monday is exact. Shared
// by the Week N report windows and the weekly AI summary so both treat a week as
// Monday → Saturday (Sunday excluded), not a rolling 7-day span off "now".
function mondayStartOfUtcMs(ms) {
  const dayMs = 24 * 60 * 60 * 1000;
  const midnight = Math.floor(ms / dayMs) * dayMs;
  const daysSinceMonday = (new Date(midnight).getUTCDay() + 6) % 7;
  return midnight - daysSinceMonday * dayMs;
}

// Per-client override for the weekly-report numbering. Returns null for the
// default (Week 1 = current calendar week), or { anchorMs, minWeekNum } where the
// latest week's absolute number auto-advances from anchorMs (snapped to its
// Monday) and the dropdown stops at minWeekNum (the oldest week shown). Navii:
// anchor is the Monday whose calendar week is numbered Week 6 as of late Jun 2026
// (it climbs on its own each week), Week 4 pinned as the oldest.
const NAVII_WEEK_ANCHOR_MS = Date.parse("2026-05-25T00:00:00Z");
function clientWeeklyReportNumbering(client) {
  const id = String(client?.name || client?.slug || client?.company_name || "")
    .trim()
    .toLowerCase();
  if (id === "navii") {
    return { anchorMs: NAVII_WEEK_ANCHOR_MS, minWeekNum: 4 };
  }
  return null;
}

// The user-facing number of the CURRENT week for a client — i.e. the label the
// Report tab's flyout shows for "Week N Report".
//
// Weeks count DOWN into the past, so week k=1 (the current week) carries the
// LARGEST display number, derived from the client's anchor week. Extracted so
// the tab bar can label the flyout without building the whole report (which
// walks every lead stage event).
function clientLatestWeekDisplayNum(client) {
  const numbering = clientWeeklyReportNumbering(client);
  if (!numbering) return 1;
  const weekWindowMs = 7 * 24 * 60 * 60 * 1000;
  const currentWeekStartMs = mondayStartOfUtcMs(Date.now());
  return (
    Math.round(
      (currentWeekStartMs - mondayStartOfUtcMs(numbering.anchorMs)) /
        weekWindowMs,
    ) + 1
  );
}

function buildClientAutoReportSections({
  leadAllRows = [],
  campaigns = [],
  meetings = [],
  blockers = [],
  incentives = [],
  leadStageEvents = [],
  users = [],
  weekNumbering = null,
}) {
  const getUserName = (userId) =>
    users.find((u) => String(u.id) === String(userId))?.name || "-";
  const momFilled = (m) =>
    !!(
      m.summary ||
      m.discussion_points ||
      m.decisions ||
      m.deliverables ||
      m.action_items ||
      m.follow_ups ||
      m.next_steps
    );

  const dayMs = 24 * 60 * 60 * 1000;
  const weekWindowMs = 7 * dayMs;
  const weeklyNowMs = Date.now();
  const tsOf = (d) => (d ? new Date(d).getTime() : 0);

  const mondayStartOf = mondayStartOfUtcMs;
  const currentWeekStartMs = mondayStartOf(weeklyNowMs);

  // Map a user's display name -> id so free-text lead assignment can attribute.
  const userIdByName = {};
  users.forEach((u) => {
    if (u.name)
      userIdByName[String(u.name).trim().toLowerCase()] = String(u.id);
  });

  const transitionKey = (from, to) => `${from || "?"}->${to || "?"}`;
  const bump = (obj, key) => {
    obj[key] = (obj[key] || 0) + 1;
  };
  const sumValues = (obj) => Object.values(obj).reduce((a, b) => a + b, 0);
  const blankMember = () => ({
    leadsAdded: 0,
    stageMoves: 0,
    outreachMoves: 0,
    demoMoves: 0,
    converted: 0,
  });
  const buildMemberRows = (store) =>
    Object.keys(store)
      .map((key) => ({
        key,
        name: key === "unattributed" ? "Unattributed" : getUserName(key),
        ...store[key],
      }))
      .filter(
        (r) =>
          r.leadsAdded ||
          r.stageMoves ||
          r.outreachMoves ||
          r.demoMoves ||
          r.converted,
      )
      .sort((a, b) => {
        const score = (r) =>
          r.leadsAdded +
          r.stageMoves +
          r.outreachMoves +
          r.demoMoves +
          r.converted;
        return score(b) - score(a);
      });
  const sumMemberTotals = (rows) =>
    rows.reduce(
      (acc, r) => {
        acc.leadsAdded += r.leadsAdded;
        acc.stageMoves += r.stageMoves;
        acc.outreachMoves += r.outreachMoves;
        acc.demoMoves += r.demoMoves;
        acc.converted += r.converted;
        return acc;
      },
      {
        leadsAdded: 0,
        stageMoves: 0,
        outreachMoves: 0,
        demoMoves: 0,
        converted: 0,
      },
    );

  // Aggregate every report metric for a single time window, expressed as an
  // `inWindow(timestamp)` predicate. Reused for the daily view and for each
  // rolling weekly window so every report view shares identical attribution.
  const aggregateWindow = (inWindow) => {
    // Activity stats: campaigns / converted / meetings / MOMs / blockers / ₹.
    const stats = {};
    const ensure = (userId) => {
      const key = userId ? String(userId) : "unattributed";
      if (!stats[key]) {
        stats[key] = {
          campaigns: 0,
          converted: 0,
          meetings: 0,
          moms: 0,
          blockers: 0,
          incentive: 0,
        };
      }
      return stats[key];
    };
    campaigns.forEach((c) => {
      if (inWindow(c.created_at)) ensure(c.created_by_user_id).campaigns += 1;
    });
    leadAllRows.forEach((l) => {
      if (l.pipeline_stage === "converted" && inWindow(l.updated_at)) {
        const uid =
          userIdByName[
            String(l.assigned_to || "")
              .trim()
              .toLowerCase()
          ] || null;
        ensure(uid).converted += 1;
      }
    });
    meetings.forEach((m) => {
      const when = m.meeting_date || m.created_at;
      if (inWindow(when)) {
        const s = ensure(m.created_by_user_id);
        s.meetings += 1;
        if (momFilled(m)) s.moms += 1;
      }
    });
    blockers.forEach((b) => {
      if (inWindow(b.created_at)) ensure(b.owner_user_id).blockers += 1;
    });
    incentives.forEach((i) => {
      if (inWindow(i.created_at))
        ensure(i.gtm_user_id).incentive += Number(i.amount) || 0;
    });

    const totals = Object.values(stats).reduce(
      (acc, s) => {
        acc.campaigns += s.campaigns;
        acc.converted += s.converted;
        acc.meetings += s.meetings;
        acc.moms += s.moms;
        acc.blockers += s.blockers;
        acc.incentive += s.incentive;
        return acc;
      },
      {
        campaigns: 0,
        converted: 0,
        meetings: 0,
        moms: 0,
        blockers: 0,
        incentive: 0,
      },
    );
    const rows = Object.keys(stats)
      .map((key) => ({
        key,
        name: key === "unattributed" ? "Unattributed" : getUserName(key),
        ...stats[key],
      }))
      .filter(
        (r) =>
          r.campaigns ||
          r.converted ||
          r.meetings ||
          r.moms ||
          r.blockers ||
          r.incentive,
      )
      .sort((a, b) => {
        const score = (r) =>
          r.campaigns + r.converted + r.meetings + r.moms + r.blockers;
        return score(b) - score(a);
      });

    // Funnel movement: pipeline / outreach / demo transitions + per-member.
    const stageTrans = {};
    const outreachTo = {};
    const demoTo = {};
    const memberStore = {};
    const ensureMember = (key) => {
      if (!memberStore[key]) memberStore[key] = blankMember();
      return memberStore[key];
    };
    leadStageEvents.forEach((ev) => {
      const nv = ev.new_value || {};
      const field = nv.field;
      const to = nv.to;
      if (!field || !to) return;
      if (!inWindow(ev.created_at)) return;
      const actorKey = ev.actor_user_id
        ? String(ev.actor_user_id)
        : "unattributed";
      if (field === "pipeline_stage") {
        bump(stageTrans, transitionKey(nv.from, to));
        ensureMember(actorKey).stageMoves += 1;
      } else if (field === "outreach_status") {
        bump(outreachTo, to);
        ensureMember(actorKey).outreachMoves += 1;
      } else if (field === "demo_status") {
        bump(demoTo, to);
        ensureMember(actorKey).demoMoves += 1;
      }
    });
    leadAllRows.forEach((l) => {
      const uid =
        userIdByName[
          String(l.assigned_to || "")
            .trim()
            .toLowerCase()
        ] || "unattributed";
      if (inWindow(l.created_at)) ensureMember(uid).leadsAdded += 1;
      if (l.pipeline_stage === "converted" && inWindow(l.updated_at))
        ensureMember(uid).converted += 1;
    });

    const consecutiveTransitions = CLIENT_LEAD_PIPELINE_STAGES.slice(0, -1).map(
      (from, i) => {
        const to = CLIENT_LEAD_PIPELINE_STAGES[i + 1];
        return {
          label: `${from.label} → ${to.label}`,
          value: stageTrans[transitionKey(from.key, to.key)] || 0,
        };
      },
    );
    const outreachRows = CLIENT_LEAD_OUTREACH_STATUSES.map((s) => ({
      label: s.label,
      value: outreachTo[s.key] || 0,
    }));
    const demoRows = CLIENT_LEAD_DEMO_STATUSES.map((s) => ({
      label: s.label,
      value: demoTo[s.key] || 0,
    }));
    const leadsAdded = leadAllRows.filter((l) => inWindow(l.created_at)).length;
    const totalMoves =
      sumValues(stageTrans) + sumValues(outreachTo) + sumValues(demoTo);
    const memberRows = buildMemberRows(memberStore);
    const memberTotals = sumMemberTotals(memberRows);

    return {
      totals,
      rows,
      leadsAdded,
      totalMoves,
      memberRows,
      memberTotals,
      consecutiveTransitions,
      outreachRows,
      demoRows,
    };
  };

  // Live pipeline snapshot — identical for every window, so computed once and
  // reused by each funnel panel.
  const funnelSnapshot = {};
  CLIENT_LEAD_PIPELINE_STAGES.forEach((s) => {
    funnelSnapshot[s.key] = 0;
  });
  leadAllRows.forEach((l) => {
    const k = l.pipeline_stage || "prospect_identified";
    funnelSnapshot[k] = (funnelSnapshot[k] || 0) + 1;
  });
  const totalLeads = leadAllRows.length;
  const convertedNow = funnelSnapshot["converted"] || 0;
  const conversionRate = totalLeads
    ? Math.round((convertedNow / totalLeads) * 100)
    : 0;
  const funnelStages = CLIENT_LEAD_PIPELINE_STAGES.map((s) => ({
    label: s.label,
    value: funnelSnapshot[s.key] || 0,
    negative: CLIENT_REPORT_NEGATIVE_STAGES.has(s.key),
  }));
  const funnelChartHtml = arFunnelChart(funnelStages, totalLeads);

  // Builds the full lead-funnel panel for a single window from its aggregate:
  // KPI tiles, the live pipeline funnel, movement bar charts and a member table.
  const buildFunnelReport = ({
    colLabel,
    rangeLabel,
    agg,
    memberPeriodLabel,
  }) => {
    const pipelineMoveRows = [
      { label: "Leads added", value: agg.leadsAdded, color: "#8b7cf6" },
      ...agg.consecutiveTransitions.map((t) => ({
        label: t.label,
        value: t.value,
        color: "#6ea8ff",
      })),
    ];
    const outreachRows = agg.outreachRows.map((r) => ({
      label: r.label,
      value: r.value,
      color: "#f3b562",
    }));
    const demoRows = agg.demoRows.map((r) => ({
      label: r.label,
      value: r.value,
      color: "#2dd4bf",
    }));
    const memberCols = [
      { key: "name", label: "Team Member", isLabel: true },
      { key: "stageMoves", label: "Stage Moves", color: "#6ea8ff" },
      { key: "outreachMoves", label: "Outreach Moves", color: "#f3b562" },
      { key: "demoMoves", label: "Demo Moves", color: "#2dd4bf" },
      { key: "converted", label: "Converted", color: "#58c98a" },
    ];
    return `
      <div class="panel ar-wrap">
        <div class="ar-head">
          <div>
            <div class="ar-eyebrow">${arIcon("layers")} Lead funnel</div>
            <h2>Pipeline Funnel</h2>
            <div class="ar-sub">Movement for ${escapeHtml(colLabel.toLowerCase())} · ${escapeHtml(rangeLabel)}</div>
          </div>
          <span class="ar-chip"><span class="ar-live-dot"></span> ${totalLeads} leads live</span>
        </div>

        <div class="ar-kpis">
          ${arKpiCard({ label: "Leads added", value: agg.leadsAdded, color: "#8b7cf6", icon: "userplus", sub: colLabel })}
          ${arKpiCard({ label: "Status moves", value: agg.totalMoves, color: "#6ea8ff", icon: "moves", sub: colLabel })}
          ${arKpiCard({ label: "Total leads", value: totalLeads, color: "#f3b562", icon: "layers", sub: "in pipeline" })}
          ${arKpiCard({ label: "Converted", value: convertedNow, color: "#58c98a", icon: "check", sub: "all-time" })}
          ${arKpiCard({ label: "Conversion rate", value: conversionRate + "%", color: "#2dd4bf", icon: "percent", sub: "of all leads" })}
        </div>

        <div class="ar-card ar-card-wide">
          <div class="ar-card-head"><span class="ar-card-title">Where leads sit now</span><span class="ar-card-sub">current pipeline distribution</span></div>
          ${funnelChartHtml}
        </div>

        <div class="ar-charts3">
          <div class="ar-card">
            <div class="ar-card-head"><span class="ar-card-title">Pipeline movement</span><span class="ar-card-sub">${escapeHtml(colLabel)}</span></div>
            ${arBars(pipelineMoveRows, { emptyLabel: "No pipeline movement tracked yet.", hideZero: true })}
          </div>
          <div class="ar-card">
            <div class="ar-card-head"><span class="ar-card-title">Outreach movement</span><span class="ar-card-sub">${escapeHtml(colLabel)}</span></div>
            ${arBars(outreachRows, { emptyLabel: "No outreach movement tracked yet.", hideZero: true })}
          </div>
          <div class="ar-card">
            <div class="ar-card-head"><span class="ar-card-title">Demo movement</span><span class="ar-card-sub">${escapeHtml(colLabel)}</span></div>
            ${arBars(demoRows, { emptyLabel: "No demo movement tracked yet.", hideZero: true })}
          </div>
        </div>

        <div class="ar-card">
          <div class="ar-card-head"><span class="ar-card-title">By team member</span><span class="ar-card-sub">${escapeHtml(memberPeriodLabel)}</span></div>
          ${arHighlightTable({ columns: memberCols, rows: agg.memberRows, totals: agg.memberTotals, emptyLabel: `No per-member activity ${memberPeriodLabel} yet.` })}
        </div>
      </div>`;
  };

  // ----------------------------------------------------------------------
  // Daily view — activity since the start of today (UTC).
  // ----------------------------------------------------------------------
  const dailyDateStr = new Date(weeklyNowMs).toISOString().slice(0, 10);
  const dayStartMs = new Date(dailyDateStr + "T00:00:00Z").getTime();
  const inToday = (d) => {
    const t = tsOf(d);
    return t > 0 && t >= dayStartMs;
  };
  const dailyRangeLabel = formatDateOnly(dailyDateStr);
  const dailyAgg = aggregateWindow(inToday);

  // ----------------------------------------------------------------------
  // Weekly views — one calendar week (Mon–Sat) per tab. Week 1 = the current
  // week, Week 2 = the previous week, and so on, back to the earliest tracked
  // activity (capped at CLIENT_REPORT_MAX_WEEKS).
  // ----------------------------------------------------------------------
  let earliestMs = weeklyNowMs;
  const considerTs = (d) => {
    const t = tsOf(d);
    if (t > 0 && t < earliestMs) earliestMs = t;
  };
  leadAllRows.forEach((l) => considerTs(l.created_at));
  campaigns.forEach((c) => considerTs(c.created_at));
  meetings.forEach((m) => considerTs(m.meeting_date || m.created_at));
  blockers.forEach((b) => considerTs(b.created_at));
  incentives.forEach((i) => considerTs(i.created_at));
  leadStageEvents.forEach((ev) => considerTs(ev.created_at));

  // Number of calendar weeks from the earliest activity's week up to this week.
  const spanWeeks =
    Math.round(
      (currentWeekStartMs - mondayStartOf(earliestMs)) / weekWindowMs,
    ) + 1;
  const weekCount = Math.min(CLIENT_REPORT_MAX_WEEKS, Math.max(1, spanWeeks));

  // `num` (k) is the stable internal id: 1 = current calendar week, ascending
  // into the past. `displayNum` is the user-facing label. By default it matches
  // num (Week 1 = current week). When weekNumbering is set (e.g. Navii), the
  // latest week gets an absolute number that auto-advances from the anchor week,
  // labels count DOWN into the past, and any week older than minWeekNum is
  // dropped — so the dropdown shows the latest week on top and minWeekNum as the
  // oldest.
  const latestDisplayNum =
    weekNumbering != null
      ? Math.round(
          (currentWeekStartMs - mondayStartOf(weekNumbering.anchorMs)) /
            weekWindowMs,
        ) + 1
      : null;

  const weeklyReports = [];
  for (let k = 1; k <= weekCount; k++) {
    const displayNum =
      latestDisplayNum != null ? latestDisplayNum - (k - 1) : k;
    // Stop once we reach the pinned oldest week (weeks only get older as k grows).
    if (weekNumbering != null && displayNum < weekNumbering.minWeekNum) break;
    // Calendar week: Monday 00:00 → Saturday end (Sunday excluded). `startMs` is
    // this week's Monday for k=1, stepping back one week per k.
    const startMs = currentWeekStartMs - (k - 1) * weekWindowMs;
    const endMs = startMs + 6 * dayMs; // exclusive: Sunday 00:00, so Mon–Sat
    const inWeek = (d) => {
      const t = tsOf(d);
      return t > 0 && t >= startMs && t < endMs;
    };
    const agg = aggregateWindow(inWeek);
    // Label spans Monday → Saturday, but the current (in-progress) week is capped
    // at today so it reads "just the days elapsed so far".
    const labelEndMs = Math.min(startMs + 5 * dayMs, weeklyNowMs);
    const rangeLabel = `${formatDateOnly(new Date(startMs).toISOString().slice(0, 10))} – ${formatDateOnly(new Date(labelEndMs).toISOString().slice(0, 10))}`;
    weeklyReports.push({
      num: k,
      displayNum,
      rangeLabel,
      // Monday (UTC) date string — the storage key for this week's AI summary.
      weekStart: new Date(startMs).toISOString().slice(0, 10),
      // Raw aggregate, so the React kit can render this week without the
      // HTML strings below. Additive — the *Html fields still work.
      agg,
    });
  }

  return {
    weeklyReports,
    // ---- data for the React chart kit -------------------------------------
    // The *Html fields above are the legacy server-rendered path and are left
    // untouched. These expose the same aggregates so components/charts can
    // render them directly — one aggregation, two renderers, no drift.
    reportData: {
      dailyRangeLabel,
      dailyAgg,
      funnelStages,
      totalLeads,
      convertedNow,
      conversionRate,
    },
  };
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

// Same as formatDateTime but without the trailing " IST" — e.g. "18 Jun 2026, 11:18 pm".
function formatDateTimeNoTz(isoString) {
  if (!isoString) return "-";
  const d = new Date(isoString);
  if (Number.isNaN(d.getTime())) return String(isoString);

  return d.toLocaleString("en-IN", {
    timeZone: APP_TIMEZONE,
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  });
}

// Client-lead notes are stored in the `notes` text column as a JSON array of
// { text, at, by } entries (newest last). Legacy rows hold a plain string; we
// surface those as a single entry with no author/date so nothing is lost.
function parseLeadNotesHistory(raw) {
  if (Array.isArray(raw)) {
    return raw.filter((n) => n && typeof n === "object" && n.text != null);
  }
  if (typeof raw !== "string") return [];
  const trimmed = raw.trim();
  if (!trimmed) return [];
  if (trimmed[0] === "[") {
    try {
      const arr = JSON.parse(trimmed);
      if (Array.isArray(arr)) {
        return arr.filter((n) => n && typeof n === "object" && n.text != null);
      }
    } catch (e) {
      /* fall through to legacy single-note handling */
    }
  }
  return [{ text: trimmed, at: null, by: null }];
}

// Append a new note entry to an existing notes value and return the JSON string
// to persist. `by` is the display name of the user who added it.
function appendLeadNote(raw, text, by, extra) {
  const history = parseLeadNotesHistory(raw);
  const clean = String(text || "").trim();
  if (!clean) return raw == null ? null : raw;
  const note = { text: clean, at: new Date().toISOString(), by: by || null };
  if (extra && typeof extra === "object") {
    for (const [key, value] of Object.entries(extra)) {
      if (value !== undefined && value !== null && value !== "") {
        note[key] = value;
      }
    }
  }
  history.push(note);
  return JSON.stringify(history);
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

// Verifies Twilio's request signature. Takes plain values rather than an
// Express request so it works from a native route handler.
//
// FAIL-OPEN WHEN UNCONFIGURED, matching the original: with no TWILIO_AUTH_TOKEN
// the check is skipped and a warning logged, so a misconfigured environment
// still receives messages rather than silently dropping them.
function validateTwilioRequest({ signature, url, body }) {
  const authToken = process.env.TWILIO_AUTH_TOKEN;
  if (!authToken) {
    console.warn(
      "TWILIO_AUTH_TOKEN missing; skipping Twilio signature validation.",
    );
    return true;
  }
  if (!signature) return false;
  return twilio.validateRequest(authToken, signature, url, body);
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
    rebus: "rebus",
    rebusai: "rebus",
    "rebus ai": "rebus",
    "rebus.ai": "rebus",
    revivflow: "revivflow",
    "reviv flow": "revivflow",
    revivflowai: "revivflow",
    "revivflow.ai": "revivflow",
    "revivflow.com": "revivflow",
  };

  return aliases[key] || key;
}

function getBusinessLeadTableName(business) {
  const normalized = getBusinessCanonicalName(business);
  return getBusinessConfig(normalized)?.table || null;
}

// Returns the client id when `business` is a virtual client lead-business
// ("client:<id>"), otherwise null.
function parseClientLeadBusiness(business) {
  const m = String(business || "").match(/^client:(\d+)$/);
  return m ? Number(m[1]) : null;
}

// Resolves any business key (static like "rasset"/"joolian" or virtual like
// "client:12") to the underlying table name plus an optional client_id filter.
function resolveLeadSource(business) {
  const clientId = parseClientLeadBusiness(business);
  if (clientId) return { tableName: CLIENT_LEADS_TABLE, clientId };
  return { tableName: getBusinessLeadTableName(business), clientId: null };
}

// True when a lead table carries the client-lead columns (pipeline_stage,
// demo_status, reached_via_*, notes history, ...). That's client_leads plus any
// inline static business's table (e.g. rasset_leads once migrated), so the
// shared leads engine can apply the same filters to all of them.
function tableHasClientLeadColumns(tableName) {
  if (tableName === CLIENT_LEADS_TABLE) return true;
  for (const business of INLINE_CLIENT_LEADS_BUSINESSES) {
    if (getBusinessLeadTableName(business) === tableName) return true;
  }
  return false;
}

// Resolves the lead business backing a client's workspace "Leads" tab. A client
// whose name/company maps to an inline static lead business (e.g. "Rasset" ->
// rasset_leads) reads & writes that business's own table so the client page
// mirrors the standalone leads engine; every other client uses its per-client
// virtual business ("client:<id>") backed by client_leads. The static mapping
// is gated on INLINE_CLIENT_LEADS_BUSINESSES so businesses still embedded via
// iframe (e.g. Joolian) keep their existing client:<id> write target.
async function resolveClientLeadBusiness(orgId, clientId) {
  const fallback = `client:${clientId}`;
  const { data: client, error } = await supabase
    .from("clients")
    .select("name, company_name")
    .eq("org_id", orgId)
    .eq("id", clientId)
    .maybeSingle();
  if (error || !client) return fallback;
  const matched = [client.name, client.company_name]
    .map((s) => getBusinessCanonicalName(s))
    .find(
      (key) =>
        getBusinessConfig(key) && INLINE_CLIENT_LEADS_BUSINESSES.has(key),
    );
  return matched || fallback;
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

async function transcribeAudioBuffer({
  buffer,
  contentType,
  fileName,
  prompt,
}) {
  if (!openai) {
    throw new Error("OpenAI client is not configured");
  }

  const safeFileName = fileName || guessFilenameFromContentType(contentType);

  const transcription = await openai.audio.transcriptions.create({
    model: "gpt-4o-transcribe",
    file: await toFile(buffer, safeFileName, {
      type: contentType || "audio/ogg",
    }),
    // Optional hint to bias the transcription (e.g. preserve the spoken
    // language / script verbatim). Omitted unless a caller passes one so the
    // default behaviour of other callers is unchanged.
    ...(prompt ? { prompt } : {}),
  });

  return transcription.text || "";
}

// Hint that forces the transcription out as Hinglish: the words stay exactly as
// spoken (never translated), but Hindi is always written in Roman/Latin script
// rather than Devanagari, and English stays English. The worked example matters
// — it steers the model far more reliably than the instruction alone.
const HINGLISH_TRANSCRIPTION_PROMPT =
  'Transcribe the audio in Hinglish. Write Hindi words phonetically in Roman (Latin) script — never Devanagari — and keep English words in English. Do not translate: keep the speaker\'s own words, only transliterated. For example, write: "maine unko kal call kiya tha, but woh available nahi the, so main aaj follow up karunga."';

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
        prompt: HINGLISH_TRANSCRIPTION_PROMPT,
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
        prompt: HINGLISH_TRANSCRIPTION_PROMPT,
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

  const leadPhoneKey = getLeadPhoneKey(lead.lead_phone);

  const { data: possibleExistingLeads, error: existingError } = await supabase
    .from(tableName)
    .select("*")
    .eq("org_id", orgId);

  if (existingError) throw existingError;

  const existingLead =
    (possibleExistingLeads || []).find((row) => {
      return getLeadPhoneKey(row.phone) === leadPhoneKey;
    }) || null;

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

    // Only written when explicitly provided. Client-lead notes are an append-only
    // history (see appendLeadNote); the client-lead edit form omits `notes` so a
    // full edit never overwrites that history. rasset/joolian forms always send
    // it, so their payloads stay byte-identical.
    ...(body.notes !== undefined
      ? { notes: String(body.notes || "").trim() || null }
      : {}),
    status: normalizeText(body.status || "new"),
    enrichment_status: normalizeText(body.enrichment_status || "not_enriched"),
    enrichment_notes: String(body.enrichment_notes || "").trim() || null,
    qualification_done:
      body.qualification_done === true || body.qualification_done === "true",
    worth_talking: body.worth_talking === true || body.worth_talking === "true",
    l2_done: body.l2_done === true || body.l2_done === "true",
    qualified: body.qualified === true || body.qualified === "true",

    // pipeline_stage exists only on client_leads. Send it only when provided
    // (the client lead form always does); rasset/joolian forms never send it,
    // so their payloads stay byte-identical and never reference a missing column.
    ...(body.pipeline_stage !== undefined
      ? {
          pipeline_stage: normalizeText(
            body.pipeline_stage || "prospect_identified",
          ),
        }
      : {}),

    // outreach_status / demo_status exist only on client_leads; written only
    // when provided so rasset/joolian payloads stay byte-identical.
    ...(body.outreach_status !== undefined
      ? {
          outreach_status: normalizeText(body.outreach_status || "not_started"),
        }
      : {}),
    ...(body.demo_status !== undefined
      ? { demo_status: normalizeText(body.demo_status || "not_scheduled") }
      : {}),

    // assigned_to is only written when explicitly provided, so existing
    // rasset/joolian create/update payloads stay byte-identical to before.
    ...(body.assigned_to !== undefined
      ? { assigned_to: String(body.assigned_to || "").trim() || null }
      : {}),

    // verified_by mirrors assigned_to: a team-member name stored on the lead,
    // written only when provided so rasset/joolian payloads stay byte-identical.
    ...(body.verified_by !== undefined
      ? { verified_by: String(body.verified_by || "").trim() || null }
      : {}),

    // phone_assigned_to / email_assigned_to ("Assign for Phone" / "Assign for
    // Email") split a lead's outreach between two team members while
    // assigned_to stays the overall owner. client_leads-only, so each is
    // written only when provided — rasset/joolian payloads never mention them.
    ...(body.phone_assigned_to !== undefined
      ? {
          phone_assigned_to:
            String(body.phone_assigned_to || "").trim() || null,
        }
      : {}),
    ...(body.email_assigned_to !== undefined
      ? {
          email_assigned_to:
            String(body.email_assigned_to || "").trim() || null,
        }
      : {}),

    // category_type exists only on client_leads; sent only when provided.
    ...(body.category_type !== undefined
      ? { category_type: String(body.category_type || "").trim() || null }
      : {}),

    // is_client_visible exists only on client_leads; sent only when provided.
    ...(body.is_client_visible !== undefined
      ? {
          is_client_visible:
            body.is_client_visible === true ||
            body.is_client_visible === "true",
        }
      : {}),

    // The sourced-lead columns surfaced on the client lead form (personal /
    // company LinkedIn and the funding trio). These exist only on client_leads,
    // so each is written only when the caller provides it — rasset/joolian
    // payloads never mention them and stay byte-identical.
    ...(body.person_linkedin_url !== undefined
      ? {
          person_linkedin_url:
            String(body.person_linkedin_url || "").trim() || null,
        }
      : {}),
    ...(body.company_linkedin_url !== undefined
      ? {
          company_linkedin_url:
            String(body.company_linkedin_url || "").trim() || null,
        }
      : {}),
    ...(body.company_last_round_amount !== undefined
      ? {
          company_last_round_amount:
            String(body.company_last_round_amount || "").trim() || null,
        }
      : {}),
    ...(body.company_last_funding_date !== undefined
      ? {
          company_last_funding_date:
            String(body.company_last_funding_date || "").trim() || null,
        }
      : {}),
    ...(body.company_funding_round !== undefined
      ? {
          company_funding_round:
            String(body.company_funding_round || "").trim() || null,
        }
      : {}),

    // The Revivflow sheet's own columns (persona, LinkedIn activity, payment
    // profile, ICP segment). Free text, and — like the block above — written
    // only when the caller sends them, so forms that never render these fields
    // keep producing byte-identical payloads.
    ...(body.persona !== undefined
      ? { persona: String(body.persona || "").trim() || null }
      : {}),
    ...(body.last_linkedin_activity !== undefined
      ? {
          last_linkedin_activity:
            String(body.last_linkedin_activity || "").trim() || null,
        }
      : {}),
    ...(body.monthly_chargebacks !== undefined
      ? {
          monthly_chargebacks:
            String(body.monthly_chargebacks || "").trim() || null,
        }
      : {}),
    ...(body.mode_of_payment !== undefined
      ? { mode_of_payment: String(body.mode_of_payment || "").trim() || null }
      : {}),
    ...(body.icp_category !== undefined
      ? { icp_category: String(body.icp_category || "").trim() || null }
      : {}),
    ...(body.company_email !== undefined
      ? { company_email: String(body.company_email || "").trim() || null }
      : {}),
    ...(body.last_instagram_activity !== undefined
      ? {
          last_instagram_activity:
            String(body.last_instagram_activity || "").trim() || null,
        }
      : {}),
    ...(body.company_hq_phone !== undefined
      ? { company_hq_phone: String(body.company_hq_phone || "").trim() || null }
      : {}),
    ...(body.company_instagram_url !== undefined
      ? {
          company_instagram_url:
            String(body.company_instagram_url || "").trim() || null,
        }
      : {}),
    ...(body.company_subtype !== undefined
      ? { company_subtype: String(body.company_subtype || "").trim() || null }
      : {}),
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
  const { tableName, clientId } = resolveLeadSource(business);

  if (!tableName) {
    throw new Error(`No lead table configured for ${business}`);
  }
  body.phone = normalizeLeadPhone(body.phone);

  const payload = buildBusinessLeadPayloadFromBody(body);
  const validationError = validateBusinessLeadPayload(payload);

  if (validationError) {
    const err = new Error(validationError);
    err.statusCode = 400;
    throw err;
  }

  const phoneKey = getLeadPhoneKey(body.phone);

  if (phoneKey) {
    let dupQuery = supabase
      .from(tableName)
      .select("id, phone, company, business_name")
      .eq("org_id", orgId)
      .or("is_deleted.is.null,is_deleted.eq.false");

    if (clientId) {
      dupQuery = dupQuery.eq("client_id", clientId);
    }

    const { data: existingRows, error: dupError } = await dupQuery;

    if (dupError) throw dupError;

    const duplicate = (existingRows || []).find((row) => {
      return getLeadPhoneKey(row.phone) === phoneKey;
    });

    if (duplicate) {
      const err = new Error(
        `Duplicate lead exists with same phone last 10 digits: Lead #${duplicate.id}`,
      );
      err.statusCode = 409;
      throw err;
    }
  }

  const insertPayload = {
    org_id: orgId,
    ...(clientId ? { client_id: clientId } : {}),
    // Client leads default to visible on the client's external dashboard
    // (matches the CSV/Excel import, see mapExcelRowToClientLead); the per-lead
    // "Client visible" toggle can hide them later. The column exists only on
    // client_leads, so it's added only for those, and any explicit value from
    // the form (spread via `payload` below) still wins.
    ...(clientId ? { is_client_visible: true } : {}),
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
  const { tableName, clientId } = resolveLeadSource(business);

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

  let updateQuery = supabase
    .from(tableName)
    .update(updatePayload)
    .eq("org_id", orgId)
    .eq("id", leadId);

  if (clientId) {
    updateQuery = updateQuery.eq("client_id", clientId);
  }

  const { data, error } = await updateQuery.select().single();

  if (error) throw error;
  return data;
}

async function getBusinessLeadById({ orgId, business, leadId }) {
  const { tableName, clientId } = resolveLeadSource(business);

  if (!tableName) {
    throw new Error(`No lead table configured for ${business}`);
  }

  let query = supabase
    .from(tableName)
    .select("*")
    .eq("org_id", orgId)
    .eq("id", leadId);

  if (clientId) {
    query = query.eq("client_id", clientId);
  }

  const { data, error } = await query.maybeSingle();

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

  // The RasssetAI_Leads sheet (Company Name / Gmap / Company Type / Owner Name /
  // Company website / No. of Employee / Turn over / Phone / Company city /
  // Company Address / Calling Status / Demo / Notes) carries a few columns with
  // no dedicated rasset_leads column. Preserve them in notes so nothing on the
  // sheet is lost on import.
  const noteLines = [];
  const baseNotes = get("notes", "Notes");
  if (baseNotes) noteLines.push(baseNotes);
  const pushNote = (label, ...keys) => {
    const v = get(...keys);
    if (v) noteLines.push(`${label}: ${v}`);
  };
  pushNote("Turn over", "Turn over", "turnover", "turn_over");
  pushNote("Calling Status", "Calling Status", "calling_status");
  pushNote("Demo", "Demo", "demo_status");

  return {
    company: get("Company Name", "Company", "company", "business_name"),
    business_name: get("Company Name", "Company", "company", "business_name"),
    website: get("Company website", "website", "Website"),
    // "Work Email" is the RasssetAI sheet header; other sheets use "Email".
    email: get("Work Email", "Email", "email"),
    industry: get("Company Type", "Industry", "industry", "company_type"),
    pin_code: get("Pin code", "pincode", "pin_code", "zip"),
    city: get("Company city", "city", "City"),
    location: get("Company Address", "Location", "address", "Address"),
    phone: normalizePhoneForLogin(get("Phone", "phone", "mobile")),
    year_of_establishment: get(
      "year of estb.",
      "year_of_establishment",
      "established",
    ),
    owner_name: get("Owner Name", "ownwer", "owner", "owner_name"),
    number_of_employees: get(
      "No. of Employee",
      "No of Employe",
      "No of Employee",
      "employees",
      "number_of_employees",
    ),
    company_size: get("Company Size", "company_size"),
    google_maps_url: get("Gmap", "Google Map", "google_map", "google_maps_url"),
    country: get("country", "Country"),

    lead_category: normalizeText(
      get("lead_category", "type of lead", "lead type") || "b2b",
    ),
    lead_stage: normalizeText(get("lead_stage", "stage") || "prospect"),
    lead_source: normalizeText(get("lead_source", "source") || "excel"),
    status: normalizeText(get("status") || "new"),
    notes: noteLines.join("\n"),
    enrichment_status: "imported",
  };
}

// Maps a client lead spreadsheet row (Navii's YC-style sheet, Rebus AI's contact
// sheet, Revivflow's R_Leads sheet) to a client_leads payload — this is the
// mapper for every client's import.
// Differs from the rasset mapper in the ways these sheets need:
//   - phone comes from a "Contact No" column,
//   - "Team size" is the headcount,
//   - a "Location" like "San Francisco, California" is split into city/state so
//     the Leads table (which renders city + state) actually shows it, and
//   - role / LinkedIn / URL / status columns that have no dedicated client_leads
//     column are preserved in `notes` so nothing on the sheet is lost on import.
function mapExcelRowToClientLead(row) {
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

  // Some cells hold several values on separate lines (e.g. two founder LinkedIn
  // URLs, or more than one email). Keep the first for single-value DB columns;
  // the full text is preserved in notes below.
  const firstLine = (text) =>
    String(text || "")
      .split(/[\n\r]+/)
      .map((s) => s.trim())
      .filter(Boolean)[0] || "";

  const company = get("Company name", "Company", "company", "business_name");
  const website = get("Company website", "Website", "website");
  // "Persona Email" is the Revivflow "Final Format" sheet's contact email;
  // "Work Email" is the Rebus AI header; other sheets use "Email". The persona's
  // own email is primary; the sheet's "Company Email" is only a fallback (and is
  // kept in notes when it differs — see below).
  const emailRaw = get("Persona Email", "Work Email", "Email", "email");
  const companyEmail = get("Company Email", "company_email");
  const email = firstLine(emailRaw) || firstLine(companyEmail);
  const teamSize = get(
    "Company employee range",
    "Team size",
    // Revivflow's header for the same thing ("51–200 employees").
    "Size of the company",
    "team_size",
    "company_size",
    "employees",
  );

  // Person-level location (Apollo / Navii sheet) is the most precise; fall back
  // to a "San Francisco, California" style Location split, then company location.
  //   "San Francisco, California" -> city "San Francisco" / state "California".
  //   Single-token locations (e.g. "New York") stay entirely in city.
  const location = get(
    "Company raw address",
    "Location",
    "location",
    "address",
    "Address",
  );
  const commaAt = location.indexOf(",");
  const splitCity =
    commaAt === -1 ? location : location.slice(0, commaAt).trim();
  const splitState = commaAt === -1 ? "" : location.slice(commaAt + 1).trim();
  // The Navii sheet has plain "City" / "State" / "Country" columns, so those are
  // checked alongside the Apollo "Person city" style headers.
  const city = get("Person city", "person_city", "City", "city") || splitCity;
  const state = get("Person state", "person_state", "State") || splitState;

  // Full contact name: explicit "Full name" column, else "First name Last name".
  const fullName =
    get("Full name", "full_name") ||
    [get("First name", "first_name"), get("Last name", "last_name")]
      .filter(Boolean)
      .join(" ")
      .trim();

  // The Navii sheet's column is "Phone numbers" (plural) and can carry more than
  // one number in a cell. Keep the first for the phone column — normalizeLeadPhone
  // strips non-digits, so two numbers left in one string would fuse into garbage —
  // and preserve the full cell in notes below.
  const phoneRaw = get(
    // Revivflow "Final Format": the persona's own number is the primary phone;
    // "Company Number" is the fallback (and also lands in company_hq_phone).
    "Persona Number",
    "persona_number",
    "Phone Number",
    "Phone numbers",
    "Phone No",
    "Contact No",
    "Contact Number",
    "Contact",
    "Company HQ Phone",
    "Phone",
    "phone",
    "Mobile Number",
    "mobile",
    "Company Number",
    "company_number",
    // Revivflow's older sheet used the bare "Number". Kept last so a sheet that
    // has both a real phone column and a "Number" column still prefers the former.
    "Number",
  );
  const phoneFirst =
    String(phoneRaw)
      .split(/[\n\r,;/]+/)
      .map((s) => s.trim())
      .filter(Boolean)[0] || "";
  // A sheet with no number for a contact writes a placeholder ("n/a", "-"), and
  // the split above would leave "n" in the phone column. Anything without a
  // digit is not a phone number.
  const phone = /\d/.test(phoneFirst) ? phoneFirst : "";

  const openRoles = String(get("Posts", "posts") || "")
    .replace(/^\{\{|\}\}$/g, "")
    .split(/[|\n\r]+/)
    .map((s) => s.trim())
    .filter(Boolean);

  // Preserve the YC-style columns that have no dedicated client_leads column.
  const noteLines = [];
  // The sheet's own free-text "Notes" column (call outcome / context) is the
  // lead's actual note, so it leads; the labelled lines below are overflow
  // fields that have no dedicated column.
  const importedNote = get("Notes", "notes", "Note", "Remarks");
  if (importedNote) noteLines.push(importedNote);
  const pushNote = (label, ...keys) => {
    const v = get(...keys);
    if (v) noteLines.push(`${label}: ${v}`);
  };
  pushNote("Job Role", "Job Role", "job_role");
  pushNote("Open Positions", "Openings", "Positions open", "open_positions");
  const roles = get("Roles", "roles");
  if (roles) noteLines.push(`Roles:\n${roles}`);
  // The Navii sheet's "Posts" column lists the open roles as
  // "{{Senior Backend Engineer|Senior Product Engineer}}" — unwrap the braces and
  // split on "|" so the titles are readable in notes and in active_job_titles.
  if (openRoles.length) noteLines.push(`Open Roles:\n${openRoles.join("\n")}`);
  // "Hiring date & Notes" holds free text like "2 weeks ago" or "1 July".
  pushNote("Hiring Date", "Hiring date & Notes", "hiring_date_notes");
  pushNote("Company LinkedIn", "LinkedIn", "linkedin");
  pushNote("Founder LinkedIn", "Founder LinkedIn", "founder_linkedin");
  pushNote("URL", "URLs", "url", "urls");
  pushNote("L1 Status", "L1 status", "l1_status");
  pushNote("Hiring Location", "Hiring location", "hiring_location");
  // Rebus AI sheet columns without a dedicated client_leads column.
  pushNote(
    "Recent Activity (LinkedIn)",
    "Recent activity linkedIn",
    "recent_activity_linkedin",
  );
  pushNote(
    "Agency / Audience Size",
    "Agency Size Audience size",
    "agency_size_audience_size",
    "Audience size",
    "Agency size",
  );
  if (emailRaw && emailRaw !== email) noteLines.push(`Emails:\n${emailRaw}`);
  // Only worth keeping when the cell held more than the number we took from it —
  // a bare "n/a" placeholder is not a note.
  if (phoneRaw && phoneRaw !== phone && /\d/.test(phoneRaw)) {
    noteLines.push(`Phone numbers:\n${phoneRaw}`);
  }

  return {
    company,
    business_name: company,
    website,
    email,
    phone,
    contact_name: fullName || null,
    owner_name: fullName || null,
    location,
    address: location || null,
    city,
    state,
    country: get("Person country", "Company country", "country") || null,
    industry:
      get("Company industry", "Company Category", "Industry", "industry") ||
      null,
    year_of_establishment:
      get("Company founded", "year of estb.", "year_of_establishment") || null,
    number_of_employees: teamSize,
    company_size: teamSize || null,

    // Dedicated columns for the full Navii / Apollo lead sheet so no field on
    // the imported CSV is lost. All stored as text (raw values like "Unknown",
    // "3.70E+09", "-" are preserved verbatim). See the matching migration:
    // sql/2026-06-20-client-leads-navii-import-columns.sql
    first_name: get("First name", "first_name") || null,
    last_name: get("Last name", "last_name") || null,
    full_name: fullName || null,
    person_linkedin_url:
      get("Person LinkedIn URL", "person_linkedin_url", "Linked In") || null,
    // Revivflow's "Persona" is the contact's role ("FOUNDER/CHAIRMAN"), which is
    // what job_title holds — it fills both that and the dedicated persona column.
    job_title: get("Job title", "job_title", "Persona", "persona") || null,
    job_department: get("Job department", "job_department") || null,
    job_seniority: get("Job seniority", "job_seniority") || null,
    // The Navii sheet's Country / State / City are the contact's location, so
    // they back-fill the person_* columns as well as the top-level ones.
    person_country:
      get("Person country", "person_country", "Country", "country") || null,
    person_state: get("Person state", "person_state", "State") || null,
    person_city: get("Person city", "person_city", "City", "city") || null,
    person_headline: get("Person headline", "person_headline") || null,
    company_industry: get("Company industry", "company_industry") || null,
    company_website: get("Company website", "company_website") || null,
    company_employee_range:
      get("Company employee range", "company_employee_range") || null,
    company_domain: get("Company domain", "company_domain") || null,
    company_linkedin_url:
      get("Company LinkedIn URL", "company_linkedin_url", "Company LinkedIn") ||
      null,
    company_facebook_url:
      get("Company Facebook URL", "company_facebook_url", "Facebook link") ||
      null,
    company_twitter_url:
      get("Company Twitter URL", "company_twitter_url") || null,
    company_instagram_url:
      get(
        "Company Instagram URL",
        "company_instagram_url",
        "Instagram URL",
        "instagram_url",
        "Instagram Link",
      ) || null,
    company_youtube_url:
      get("Company YouTube URL", "company_youtube_url") || null,
    company_crunchbase_url:
      get("Company Crunchbase URL", "company_crunchbase_url") || null,
    company_type: get("Company type", "company_type") || null,
    company_hq_phone:
      get(
        "Company HQ Phone",
        "company_hq_phone",
        "Company Number",
        "company_number",
      ) || null,
    company_country: get("Company country", "company_country") || null,
    company_state: get("Company state", "company_state") || null,
    company_city: get("Company city", "company_city") || null,
    company_raw_address:
      get("Company raw address", "company_raw_address") || null,
    company_funding_total_amount:
      get("Company funding total amount", "company_funding_total_amount") ||
      null,
    company_funding_total_rounds:
      get(
        "Company funding total number of rounds",
        "company_funding_total_number_of_rounds",
        "company_funding_total_rounds",
      ) || null,
    // The Navii sheet's funding trio: "Funding Amount" ($500 million),
    // "Funding Date / Month" (October 2024 | February 3, 2025) and
    // "Funding Round (Seed, Series A, B, etc.)" (Series B). All text — the
    // amounts and dates come through in whatever format the sheet used.
    company_last_round_amount:
      get(
        "Company last round amount",
        "company_last_round_amount",
        "Funding Amount",
        "funding_amount",
      ) || null,
    company_last_funding_date:
      get(
        "Company last funding date",
        "Last funding date",
        "company_last_funding_date",
        "last_funding_date",
        "Funding Date / Month",
        "Funding Date",
        "funding_date",
      ) || null,
    company_funding_round:
      get(
        "Company funding round",
        "Funding round",
        "company_funding_round",
        "funding_round",
        "Last funding round",
        "Funding Round (Seed, Series A, B, etc.)",
      ) || null,
    company_revenue_range:
      get("Company revenue range", "company_revenue_range") || null,
    company_founded: get("Company founded", "company_founded") || null,
    company_email_domain:
      get("Company email domain", "company_email_domain") || null,
    active_job_count:
      get("Active job count", "active_job_count", "Openings", "openings") ||
      null,
    active_job_titles:
      get("Active job titles", "active_job_titles") ||
      (openRoles.length ? openRoles.join(", ") : null),
    company_subtype:
      get(
        "Company Subtype",
        "company_subtype",
        "Sub Category",
        "sub_category",
      ) || null,
    business_model: get("Business Model", "business_model") || null,

    // The Revivflow sheet's own columns (R_Leads.csv). All free text — the
    // sheet's values are ranges ("High"), prose ("Posted 2 days ago") and comma
    // lists of payment methods. See the migration:
    // sql/2026-08-04-client-leads-revivflow-sheet-columns.sql
    persona: get("Persona", "persona") || null,
    last_linkedin_activity:
      get(
        "Last Linkedin Activity",
        "last_linkedin_activity",
        "Recent activity linkedIn",
      ) || null,
    // Revivflow "Final Format" additions. company_email is the sheet's
    // company-level address (the persona's own email is the primary `email`
    // above); last_instagram_activity pairs with last_linkedin_activity. See
    // sql/2026-08-20-client-leads-company-email-instagram-activity.sql
    company_email: companyEmail || null,
    last_instagram_activity:
      get("Last Instagram Activity", "last_instagram_activity") || null,
    monthly_chargebacks:
      get("Monthly Chargebacks", "monthly_chargebacks", "Chargebacks") || null,
    mode_of_payment:
      get("Mode of Payment", "mode_of_payment", "Payment Mode") || null,
    // The client's own segment naming ("Travel Companies"), free text and
    // deliberately separate from category_type, which the import dialog stamps
    // on every row from a fixed dropdown.
    icp_category: get("ICP Category", "icp_category", "ICP") || null,
    // The Navii sheet's "assign to" column carries a team member's name, which
    // is exactly what assigned_to stores (the Leads tab assigns by name, and
    // reporting maps that name back to a user id).
    assigned_to:
      get("assign to", "assigned to", "assigned_to", "Assignee", "Owner") ||
      null,

    // Navii splits ownership across two people: "Assigned for Phone" (who calls
    // the lead) and "Assigned for email" (who emails it). Both cells hold a team
    // member's name and land in the columns behind the lead form's "Assign for
    // Phone" / "Assign for Email" dropdowns. See the migration:
    // sql/2026-07-30-client-leads-phone-email-assignees.sql
    phone_assigned_to:
      get(
        "Assigned for Phone",
        "Assign for Phone",
        "assigned_for_phone",
        "phone_assigned_to",
      ) || null,
    email_assigned_to:
      get(
        "Assigned for email",
        "Assign for Email",
        "assigned_for_email",
        "email_assigned_to",
      ) || null,

    // The Navii sheet's "Verified By" column mirrors "assign to": a team
    // member's name, stored verbatim in verified_by (same as the Leads tab's
    // Verified By dropdown).
    verified_by: get("verified by", "verified_by", "Verified") || null,

    lead_category: "b2b",
    lead_stage: "prospect",
    lead_source: normalizeText(
      get("lead_source", "source", "Sources") || "excel",
    ),
    status: "new",
    notes: noteLines.join("\n"),
    enrichment_status: "imported",
    // Imported leads are shown on the client's external dashboard by default.
    // The per-lead "Client visible" toggle in the workspace can hide them later.
    is_client_visible: true,
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

  // Pre-load ALL existing phone + email keys for this org once so re-imports are
  // idempotent and de-dupe on phone-or-email. A plain .select() is capped at
  // 1000 rows by PostgREST, but rasset_leads holds far more (thousands), so page
  // through with .range(); otherwise existing leads past row 1000 are missed and
  // their re-import hits the rasset_leads_org_id_phone_active_key unique
  // constraint as a hard error instead of being counted as a duplicate.
  const seenPhoneKeys = new Map();
  const seenEmailKeys = new Map();
  const EXISTING_PAGE_SIZE = 1000;
  for (let from = 0; ; from += EXISTING_PAGE_SIZE) {
    const { data: page, error: pageError } = await supabase
      .from("rasset_leads")
      .select("id, phone, email")
      .eq("org_id", orgId)
      .or("is_deleted.is.null,is_deleted.eq.false")
      .range(from, from + EXISTING_PAGE_SIZE - 1);
    if (pageError) throw pageError;
    for (const r of page || []) {
      const pk = getLeadPhoneKey(r.phone);
      if (pk && !seenPhoneKeys.has(pk)) seenPhoneKeys.set(pk, r.id);
      const ek = getLeadEmailKey(r.email);
      if (ek && !seenEmailKeys.has(ek)) seenEmailKeys.set(ek, r.id);
    }
    if (!page || page.length < EXISTING_PAGE_SIZE) break;
  }

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

        const phoneKey = getLeadPhoneKey(payload.phone);
        const emailKey = getLeadEmailKey(payload.email);

        // De-dupe on phone number OR work email against the pre-loaded keys
        // (which also accumulate leads inserted earlier in this same file, so
        // in-file duplicates are caught too).
        let duplicateOfId = null;
        let duplicateMessage = "";
        if (phoneKey && seenPhoneKeys.has(phoneKey)) {
          duplicateOfId = seenPhoneKeys.get(phoneKey);
          duplicateMessage = "Duplicate phone number skipped";
        } else if (emailKey && seenEmailKeys.has(emailKey)) {
          duplicateOfId = seenEmailKeys.get(emailKey);
          duplicateMessage = "Duplicate work email skipped";
        }

        if (duplicateOfId) {
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
            message: duplicateMessage,
            existingLeadId: duplicateOfId,
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

        if (insertError) {
          // Safety net: if a lead still collides with the DB unique constraint
          // (rasset_leads_org_id_phone_active_key), record it as a duplicate
          // rather than a hard error so re-imports stay clean.
          if (insertError.code === "23505") {
            results.duplicates += 1;
            if (phoneKey)
              seenPhoneKeys.set(phoneKey, seenPhoneKeys.get(phoneKey) || null);

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
            });

            continue;
          }
          throw insertError;
        }

        results.inserted += 1;
        // Track the just-inserted keys so later rows in this same file that
        // repeat the phone/email are caught as duplicates.
        if (phoneKey) seenPhoneKeys.set(phoneKey, insertedLead?.id || null);
        if (emailKey) seenEmailKeys.set(emailKey, insertedLead?.id || null);

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
  const { startUtc, endUtc } = getCurrentAttendanceDayRange();

  const { data, error } = await supabase
    .from("attendance_events")
    .select("action")
    .eq("user_id", userId)
    .eq("org_id", orgId)
    .gte("created_at", startUtc)
    .lt("created_at", endUtc)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
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
  return getDateStringInTimeZone(new Date(), timeZone);
}

// The YYYY-MM-DD calendar date of `date` as seen in `timeZone`.
function getDateStringInTimeZone(date, timeZone = APP_TIMEZONE) {
  const parts = getPartsInTimeZone(date, timeZone);
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

function getLeadDisplayName(lead) {
  return (
    lead.company ||
    lead.business_name ||
    lead.company_name ||
    lead.contact_name ||
    lead.owner_name ||
    `Lead #${lead.id}`
  );
}

function getLeadIndustry(lead) {
  return (
    lead.industry_primary ||
    lead.industry ||
    lead.raw_industry ||
    lead.activity_category ||
    lead.type_of_business ||
    "Unknown"
  );
}

function hasLeadTranscript(lead) {
  return !!String(lead.latest_transcript || "").trim();
}

function isCompletedLead(lead) {
  return String(lead.status || "").toLowerCase() === "completed";
}

function leadNeedsReview(lead) {
  const transcript = String(lead.latest_transcript || "").toLowerCase();

  const keywords = [
    "send details",
    "call later",
    "pricing",
    "price",
    "requirement",
    "owner",
    "decision maker",
    "interested",
    "whatsapp",
    "follow up",
    "demo",
  ];

  const hasKeyword = keywords.some((k) => transcript.includes(k));

  return (
    !!lead.qualified &&
    !!lead.worth_talking &&
    hasLeadTranscript(lead) &&
    !isCompletedLead(lead) &&
    hasKeyword
  );
}

function groupCount(rows, keyFn) {
  const map = new Map();

  for (const row of rows || []) {
    const key = keyFn(row) || "Unknown";

    if (!map.has(key)) {
      map.set(key, {
        key,
        total: 0,
        qualified: 0,
        worthTalking: 0,
        completed: 0,
        withTranscript: 0,
      });
    }

    const item = map.get(key);
    item.total += 1;
    if (row.qualified) item.qualified += 1;
    if (row.worth_talking) item.worthTalking += 1;
    if (isCompletedLead(row)) item.completed += 1;
    if (hasLeadTranscript(row)) item.withTranscript += 1;
  }

  return Array.from(map.values()).sort((a, b) => b.total - a.total);
}

function getLeadTimeframeRange(timeframe) {
  const now = new Date();
  const start = new Date(now);
  const end = new Date(now);

  if (timeframe === "yesterday") {
    start.setDate(start.getDate() - 1);
    start.setHours(0, 0, 0, 0);
    end.setDate(end.getDate() - 1);
    end.setHours(23, 59, 59, 999);
  } else if (timeframe === "this_week") {
    const day = start.getDay();
    const diff = start.getDate() - day + (day === 0 ? -6 : 1);
    start.setDate(diff);
    start.setHours(0, 0, 0, 0);
    end.setHours(23, 59, 59, 999);
  } else if (timeframe === "this_month") {
    start.setDate(1);
    start.setHours(0, 0, 0, 0);
    end.setHours(23, 59, 59, 999);
  } else {
    start.setHours(0, 0, 0, 0);
    end.setHours(23, 59, 59, 999);
  }

  return {
    startIso: start.toISOString(),
    endIso: end.toISOString(),
  };
}

function buildLeadIntelligenceMetrics(
  rows = [],
  voiceRows = [],
  timeframe = "today",
) {
  const { startIso, endIso } = getLeadTimeframeRange(timeframe);

  const inRange = (dateValue) => {
    if (!dateValue) return false;
    const d = new Date(dateValue);
    return d >= new Date(startIso) && d <= new Date(endIso);
  };

  const filteredLeads = rows.filter(
    (x) => inRange(x.created_at) || inRange(x.updated_at),
  );
  const filteredVoices = voiceRows.filter((x) => inRange(x.created_at));

  const callsWithTranscript = filteredLeads.filter((x) =>
    String(x.latest_transcript || "").trim(),
  );

  const industryMap = new Map();
  const employeeMap = new Map();

  for (const lead of filteredLeads) {
    const industry =
      lead.industry_primary || lead.industry || lead.raw_industry || "Unknown";

    if (!industryMap.has(industry)) {
      industryMap.set(industry, {
        industry,
        leads: 0,
        transcripts: 0,
        qualified: 0,
        worth_talking: 0,
        in_progress: 0,
        completed: 0,
      });
    }

    const item = industryMap.get(industry);
    item.leads += 1;
    if (lead.latest_transcript) item.transcripts += 1;
    if (lead.qualified) item.qualified += 1;
    if (lead.worth_talking) item.worth_talking += 1;
    if (lead.status === "in_progress") item.in_progress += 1;
    if (lead.status === "completed") item.completed += 1;

    const employee =
      lead.assigned_to_employee ||
      lead.assigned_employee ||
      lead.assigned_user_name ||
      lead.owner_employee ||
      lead.employee_name ||
      lead.uploaded_by_employee ||
      "Unknown";

    if (!employeeMap.has(employee)) {
      employeeMap.set(employee, {
        employee,
        leads: 0,
        transcripts: 0,
        qualified: 0,
        worth_talking: 0,
        completed: 0,
      });
    }

    const emp = employeeMap.get(employee);
    emp.leads += 1;
    if (lead.latest_transcript) emp.transcripts += 1;
    if (lead.qualified) emp.qualified += 1;
    if (lead.worth_talking) emp.worth_talking += 1;
    if (lead.status === "completed") emp.completed += 1;
  }

  return {
    timeframe,
    total_leads: filteredLeads.length,
    calls_uploaded: filteredVoices.length,
    calls_with_transcript: callsWithTranscript.length,
    qualified: filteredLeads.filter((x) => x.qualified).length,
    worth_talking: filteredLeads.filter((x) => x.worth_talking).length,
    in_progress: filteredLeads.filter((x) => x.status === "in_progress").length,
    completed: filteredLeads.filter((x) => x.status === "completed").length,
    industryRows: Array.from(industryMap.values()).sort(
      (a, b) => b.leads - a.leads,
    ),
    employeeRows: Array.from(employeeMap.values()).sort(
      (a, b) => b.leads - a.leads,
    ),
    recentTranscriptRows: callsWithTranscript.slice(0, 10),
  };
}

function getLeadDisplayNameForAI(lead) {
  return (
    lead.company ||
    lead.business_name ||
    lead.contact_name ||
    lead.owner_name ||
    lead.phone ||
    "Unknown Lead"
  );
}

function getLeadIndustryForAI(lead) {
  return (
    lead.industry_primary || lead.industry || lead.raw_industry || "Unknown"
  );
}

function getLeadOwnerForAI(lead) {
  return (
    lead.assigned_to ||
    lead.last_spoke_to_name ||
    lead.last_call_uploaded_by_phone ||
    "Unknown"
  );
}

function getLeadAIRowsForTimeframe(rows = [], timeframe = "today") {
  const transcriptRows = rows.filter((lead) =>
    String(lead.latest_transcript || "").trim(),
  );

  if (timeframe === "all_history") {
    return transcriptRows;
  }

  const { startIso, endIso } = getLeadTimeframeRange(timeframe);
  const start = new Date(startIso);
  const end = new Date(endIso);

  return transcriptRows.filter((lead) => {
    const dateValue = lead.updated_at || lead.created_at;
    if (!dateValue) return false;

    const d = new Date(dateValue);
    return d >= start && d <= end;
  });
}

function getRecentLeadTranscriptsForAI(rows = [], timeframe = "today") {
  const maxItems = timeframe === "all_history" ? 100 : 30;
  const maxChars = timeframe === "all_history" ? 1800 : 2500;

  return getLeadAIRowsForTimeframe(rows, timeframe)
    .slice(0, maxItems)
    .map((lead) => ({
      lead_id: lead.id,
      lead_name: getLeadDisplayNameForAI(lead),
      industry: getLeadIndustryForAI(lead),
      owner: getLeadOwnerForAI(lead),
      status: lead.status || "",
      qualified: !!lead.qualified,
      worth_talking: !!lead.worth_talking,
      created_at: lead.created_at || null,
      updated_at: lead.updated_at || null,
      transcript: String(lead.latest_transcript || "").slice(0, maxChars),
    }));
}

async function generateLeadAIIntelligence({ business, timeframe, rows }) {
  if (!openai) {
    throw new Error(
      "OPENAI_API_KEY is missing. Add it in Railway variables first.",
    );
  }

  const transcripts = getRecentLeadTranscriptsForAI(rows, timeframe);

  if (!transcripts.length) {
    throw new Error("No call transcripts found for AI analysis.");
  }

  const timeframeLabel =
    timeframe === "all_history" ? "all available past transcripts" : timeframe;

  const prompt = `
You are analyzing sales / discovery calls for a factory lead CRM business called ${business}.

Timeframe being analyzed: ${timeframeLabel}

Your job:
1. Summarize what we are learning from calls.
2. Find industry-wise patterns.
3. Extract objections, pain points, buying signals, urgency, and referrals.
4. Give recommendations.
5. Every important insight must include supporting lead IDs.

Return ONLY valid JSON.

JSON shape:
{
  "overall_summary": "",
  "top_learnings": [
    {
      "insight": "",
      "why_it_matters": "",
      "supporting_lead_ids": []
    }
  ],
  "industry_intelligence": [
    {
      "industry": "",
      "industry_thesis": "",
      "common_pain_points": [],
      "common_objections": [],
      "successful_pitch_patterns": [],
      "recommendations": [],
      "supporting_lead_ids": []
    }
  ],
  "employee_intelligence": [
    {
      "employee": "",
      "strengths_seen": [],
      "improvement_opportunities": [],
      "good_examples": [],
      "supporting_lead_ids": []
    }
  ],
  "leads_to_review": [
    {
      "lead_id": "",
      "lead_name": "",
      "industry": "",
      "reason": "",
      "recommended_next_step": ""
    }
  ],
  "recommended_actions": []
}

Important:
- Do not invent facts.
- Use only transcripts provided.
- Keep it practical for a small business owner.
- Avoid generic advice.
- Do not give employee scores.
- Mention supporting lead IDs.
`;

  const completion = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    temperature: 0.2,
    messages: [
      {
        role: "system",
        content:
          "You are a practical business intelligence analyst. Return only valid JSON.",
      },
      {
        role: "user",
        content:
          prompt + "\n\nTRANSCRIPTS:\n" + JSON.stringify(transcripts, null, 2),
      },
    ],
  });

  const raw = completion.choices?.[0]?.message?.content || "";
  const parsed = safeParseJson(raw);

  if (!parsed) {
    throw new Error("AI returned invalid JSON.");
  }

  parsed._meta = {
    timeframe,
    transcript_count: transcripts.length,
    generated_from: timeframeLabel,
  };

  return parsed;
}

async function getLeadAIIntelligenceHistory({ orgId, business, limit = 20 }) {
  const { data, error } = await supabase
    .from("lead_ai_intelligence_runs")
    .select("*")
    .eq("org_id", orgId)
    .eq("business", business)
    .neq("timeframe", "cumulative")
    .order("created_at", { ascending: false })
    .limit(limit);

  if (error) throw error;
  return data || [];
}

async function generateCumulativeLeadAIIntelligence({ business, runs }) {
  if (!openai) {
    throw new Error(
      "OPENAI_API_KEY is missing. Add it in Railway variables first.",
    );
  }

  const usableRuns = (runs || []).slice(0, 12).map((run) => ({
    id: run.id,
    timeframe: run.timeframe,
    generated_at: run.created_at,
    transcript_count: run.transcript_count || 0,
    summary: run.summary || {},
  }));

  if (!usableRuns.length) {
    throw new Error("No prior AI intelligence runs found.");
  }

  const prompt = `
You are creating cumulative business intelligence for ${business}.

You are NOT reading raw transcripts now.
You are reading prior saved AI intelligence snapshots.

Your job:
1. Combine prior intelligence without losing old learning.
2. Identify repeated patterns.
3. Identify changes over time.
4. Identify strongest industries.
5. Identify objections that keep repeating.
6. Recommend what the team should do next.

Return ONLY valid JSON.

JSON shape:
{
  "cumulative_summary": "",
  "repeated_patterns": [],
  "industry_trends": [
    {
      "industry": "",
      "trend": "",
      "why_it_matters": "",
      "recommended_action": ""
    }
  ],
  "recurring_objections": [],
  "improving_signals": [],
  "warning_signals": [],
  "best_next_actions": [],
  "what_to_watch_next": []
}

Important:
- Do not invent facts.
- Use only the saved snapshots.
- Be practical and specific.
`;

  const completion = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    temperature: 0.2,
    messages: [
      {
        role: "system",
        content:
          "You are a cumulative business intelligence analyst. Return only valid JSON.",
      },
      {
        role: "user",
        content:
          prompt +
          "\n\nPRIOR_SAVED_INTELLIGENCE:\n" +
          JSON.stringify(usableRuns, null, 2),
      },
    ],
  });

  const raw = completion.choices?.[0]?.message?.content || "";
  const parsed = safeParseJson(raw);

  if (!parsed) {
    throw new Error("AI returned invalid JSON.");
  }

  parsed._meta = {
    source_runs: usableRuns.map((x) => x.id),
    source_run_count: usableRuns.length,
  };

  return parsed;
}

async function getLatestLeadAIIntelligenceRun({ orgId, business, timeframe }) {
  const { data, error } = await supabase
    .from("lead_ai_intelligence_runs")
    .select("*")
    .eq("org_id", orgId)
    .eq("business", business)
    .eq("timeframe", timeframe)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  return data || null;
}

// ===========================================================================
// Client report AI summary
//
// A short natural-language recap of a client's pipeline activity, shown in the
// Report tab on both the internal workspace and the public client link. Two
// periods: "daily" (last 24h) and "weekly" (the current calendar week, Mon–Sat,
// matching the Week N report windows). Generated nightly at 9 PM PST (see the
// scheduler + /api/cron endpoint) and on demand via the internal "Regenerate"
// button. Stored in client_report_ai_summaries.
// ===========================================================================
const CLIENT_REPORT_SUMMARY_PERIODS = ["daily", "weekly"];
const CLIENT_REPORT_SUMMARY_TZ = "America/Los_Angeles";

// "leads Found" is derived from the enriched count: every enriched lead implies
// additional raw leads researched before enrichment, so we surface enriched × 2.5,
// trimmed to a whole number (e.g. 10 enriched → 25 leads Found).
const LEADS_FOUND_ENRICHED_MULTIPLIER = 2.5;
// Fallback used for client-facing meetings that have no logged duration_min:
// estimate a flat hour each. Meetings with a real duration use that instead.
const COLLAB_HOURS_PER_MEETING = 1;

// The Monday-00:00 (UTC) date string (YYYY-MM-DD) of the calendar week containing
// `ms`. This is the storage key for a weekly summary so each week keeps its own.
function weekStartDateString(ms) {
  return new Date(mondayStartOfUtcMs(ms)).toISOString().slice(0, 10);
}

function clientReportSummaryWindowMs(period) {
  const day = 24 * 60 * 60 * 1000;
  return period === "weekly" ? 7 * day : day;
}

// Compact, model-friendly stats for one window. Standalone (does not depend on
// the HTML report builder) so the nightly job and the manual endpoint can call it
// directly. Daily = last 24h (rolling). Weekly = the current calendar week from
// Monday 00:00 through Saturday end (Sunday excluded), matching the Week N tabs.
function buildClientReportStatsForAI(
  {
    leadAllRows = [],
    campaigns = [],
    meetings = [],
    blockers = [],
    incentives = [],
    leadStageEvents = [],
    users = [],
    workItems = [],
    linkedTasks = [],
  },
  period,
  refMs = Date.now(),
) {
  const now = Date.now();
  const dayMs = 24 * 60 * 60 * 1000;
  let startMs;
  let endMs;
  if (period === "weekly") {
    // The calendar week containing refMs (defaults to this week). refMs lets the
    // per-week Regenerate button and any historical week compute their own stats.
    startMs = mondayStartOfUtcMs(refMs); // that week's Monday 00:00
    endMs = startMs + 6 * dayMs; // exclusive: Sunday 00:00 → covers Mon–Sat
  } else {
    startMs = now - clientReportSummaryWindowMs(period);
    endMs = Infinity;
  }
  const tsOf = (d) => (d ? new Date(d).getTime() : 0);
  const inWindow = (d) => {
    const t = tsOf(d);
    return t > 0 && t >= startMs && t < endMs;
  };
  const nameById = {};
  users.forEach((u) => {
    nameById[String(u.id)] = u.name || "";
  });

  const stageLabel = {};
  CLIENT_LEAD_PIPELINE_STAGES.forEach((s) => {
    stageLabel[s.key] = s.label;
  });

  // ---- Leads uploaded in the window ----
  // Client wants only the headline count; region / type / company breakdowns
  // are deliberately not collected (see leads_uploaded in the return below).
  const newLeads = leadAllRows.filter((l) => inWindow(l.created_at));
  // "Enriched" = explicitly marked enriched, or carrying enrichment-tool data
  // (industry / location) beyond the bare company name.
  const enrichedLeads = newLeads.filter(
    (l) =>
      l.enrichment_status === "enriched" ||
      l.industry ||
      l.country ||
      l.city ||
      l.state,
  ).length;
  // "leads Found" is enriched inflated by the raw-research multiplier, trimmed to a
  // whole number (client wants no decimals in the headline figure).
  const leadsFound = Math.trunc(
    enrichedLeads * LEADS_FOUND_ENRICHED_MULTIPLIER,
  );
  // Sourcing channels come from the Reached Via tab: how many distinct outreach
  // channels this window's leads were reached through (max = REACH_VIA_CHANNELS).
  const sourcingChannels = REACH_VIA_CHANNELS.filter((ch) =>
    newLeads.some((l) => l[ch.column]),
  ).length;
  // Touchpoints come from the Reached Via tab: for each lead touched (updated) in
  // the window, every channel it was reached through counts once — so a lead
  // reached via LinkedIn + email = 2 touchpoints. leadsUpdated is kept as the
  // count of distinct leads touched (used for the has_activity signal).
  const touchedLeads = leadAllRows.filter((l) => inWindow(l.updated_at));
  const leadsUpdated = touchedLeads.length;
  const touchpoints = touchedLeads.reduce(
    (sum, l) => sum + REACH_VIA_CHANNELS.filter((ch) => l[ch.column]).length,
    0,
  );
  // Per-channel Reached-Via breakdown: for each channel, how many touched leads
  // were reached through it, split by their current pipeline status at that point
  // (e.g. LinkedIn → 10 reached: 4 Connection Sent, 4 Engaged). Channels with no
  // leads in the window are omitted; statuses are ordered most-common first.
  const reachBreakdown = REACH_VIA_CHANNELS.map((ch) => {
    const chLeads = touchedLeads.filter((l) => l[ch.column]);
    if (!chLeads.length) return null;
    const byStatus = {};
    chLeads.forEach((l) => {
      const k = l.pipeline_stage || DEFAULT_CLIENT_LEAD_STAGE;
      byStatus[k] = (byStatus[k] || 0) + 1;
    });
    const statuses = Object.entries(byStatus)
      .sort((a, b) => b[1] - a[1])
      .map(([k, count]) => ({ status: stageLabel[k] || k, count }));
    return { channel: ch.label, count: chLeads.length, statuses };
  }).filter(Boolean);

  // ---- Movement / qualification (from logged status transitions) ----
  const QUALIFIED_KEYS = new Set([
    "qualified_opportunity",
    "pilot_evaluation",
    "commercial_discussion",
    "converted",
  ]);
  const moversByUser = {};
  const transitionCounts = {};
  let stageMoves = 0;
  let outreachMoves = 0;
  let demoMoves = 0;
  let qualifiedMoves = 0;
  // Positive replies = leads moved into the "Positive Response" pipeline stage.
  let positiveReplyMoves = 0;
  leadStageEvents.forEach((ev) => {
    const nv = ev.new_value || {};
    if (!nv.field || !nv.to || !inWindow(ev.created_at)) return;
    if (nv.field === "pipeline_stage") {
      stageMoves += 1;
      if (QUALIFIED_KEYS.has(nv.to)) qualifiedMoves += 1;
      if (nv.to === "positive_response") positiveReplyMoves += 1;
      const label = `${stageLabel[nv.from] || nv.from || "?"} → ${stageLabel[nv.to] || nv.to}`;
      transitionCounts[label] = (transitionCounts[label] || 0) + 1;
    } else if (nv.field === "outreach_status") outreachMoves += 1;
    else if (nv.field === "demo_status") demoMoves += 1;
    const who = ev.actor_user_id
      ? nameById[String(ev.actor_user_id)] || "Unattributed"
      : "Unattributed";
    moversByUser[who] = (moversByUser[who] || 0) + 1;
  });
  const topTransitions = Object.entries(transitionCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)
    .map(([label, count]) => ({ label, count }));
  const topMovers = Object.entries(moversByUser)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([name, moves]) => ({ name, moves }));
  const converted = leadAllRows.filter(
    (l) => l.pipeline_stage === "converted" && inWindow(l.updated_at),
  ).length;

  // ---- Tasks / work items touched in the window ----
  // Strategic Execution is task-driven, so alongside the counts we surface the
  // actual task titles + details for the period, including each work item's
  // linked (dependency) task title, so the AI summary can describe the real
  // work. We fold in both sources shown on the client workspace: the client's
  // own work items AND the "Linked Tasks" (general tasks whose `business` names
  // this client).
  const workItemById = {};
  workItems.forEach((w) => {
    workItemById[String(w.id)] = w;
  });
  const tasksCompleted = [];
  const tasksWorkedOn = [];
  const taskDetails = [];
  workItems.forEach((w) => {
    if (!inWindow(w.updated_at)) return;
    const title = String(w.title || "").trim();
    if (!title) return;
    if (w.status === "done") {
      if (tasksCompleted.length < 15) tasksCompleted.push(title);
    } else if (tasksWorkedOn.length < 15) {
      tasksWorkedOn.push(title);
    }
    if (taskDetails.length < 15) {
      const linked = w.dependency_work_item_id
        ? workItemById[String(w.dependency_work_item_id)]
        : null;
      taskDetails.push({
        title,
        description:
          String(w.description || "")
            .trim()
            .slice(0, 300) || null,
        status: w.status || null,
        linked_title: linked ? String(linked.title || "").trim() || null : null,
      });
    }
  });
  // Linked tasks (tasks table, matched to this client by `business`). "done"
  // tasks count as finalized action items; anything else is in progress. Their
  // `detail` text and `area` give the summary the specifics for this client.
  linkedTasks.forEach((t) => {
    if (!inWindow(t.updated_at)) return;
    const title = String(t.title || "").trim();
    if (!title) return;
    if (t.status === "done") {
      if (tasksCompleted.length < 15) tasksCompleted.push(title);
    } else if (tasksWorkedOn.length < 15) {
      tasksWorkedOn.push(title);
    }
    if (taskDetails.length < 15) {
      taskDetails.push({
        title,
        description:
          String(t.detail || "")
            .trim()
            .slice(0, 300) || null,
        status: t.status || null,
        area: String(t.area || "").trim() || null,
        linked_title: null,
        source: "linked_task",
      });
    }
  });

  // ---- Outreach (campaign counters) ----
  // Campaign counters are cumulative, so attribute a campaign's totals to the
  // window when the campaign was created or last updated inside it. Touchpoints
  // and positive replies now come from lead activity (updates / status moves);
  // campaign response_count is kept as a secondary "responses" signal.
  const activeCampaigns = campaigns.filter(
    (c) => inWindow(c.created_at) || inWindow(c.updated_at),
  );
  const outreachResponses = activeCampaigns.reduce(
    (n, c) => n + (Number(c.response_count) || 0),
    0,
  );

  // ---- Other activity ----
  // Meetings split by type: internal/review sessions count as strategic work,
  // while sync calls / ad-hoc calls are client-facing collaboration.
  const meetingsInWindow = meetings.filter((m) =>
    inWindow(m.meeting_date || m.created_at),
  );
  const isStrategyMeeting = (m) =>
    m.meeting_type === "internal" || m.meeting_type === "review";
  const strategySessionsCount =
    meetingsInWindow.filter(isStrategyMeeting).length;
  // Client-facing meetings (sync + ad-hoc) feed Client Collaboration; every
  // meeting logged in the window is a "meeting booked" for Outreach Execution.
  const clientFacingMeetings = meetingsInWindow.filter(
    (m) => !isStrategyMeeting(m),
  );
  const meetingsCount = clientFacingMeetings.length;
  const meetingsBookedCount = meetingsInWindow.length;
  // Collaboration hours = the actual logged duration of client-facing meetings.
  // Meetings without a logged duration fall back to the flat per-meeting estimate,
  // so pre-duration rows read the same as before.
  const collaborationMinutes = clientFacingMeetings.reduce((sum, m) => {
    const d = Number(m.duration_min);
    return (
      sum + (Number.isFinite(d) && d > 0 ? d : COLLAB_HOURS_PER_MEETING * 60)
    );
  }, 0);
  const collaborationHours = Math.round((collaborationMinutes / 60) * 10) / 10;
  const campaignsCount = campaigns.filter((c) => inWindow(c.created_at)).length;
  const blockersCount = blockers.filter((b) => inWindow(b.created_at)).length;
  const incentivesPaid = incentives
    .filter((i) => inWindow(i.created_at))
    .reduce((s, i) => s + (Number(i.amount) || 0), 0);

  // ---- Live snapshot (where leads sit now) ----
  const snap = {};
  leadAllRows.forEach((l) => {
    const k = l.pipeline_stage || "prospect_identified";
    snap[k] = (snap[k] || 0) + 1;
  });
  const pipelineSnapshotTop = Object.entries(snap)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([k, count]) => ({ stage: stageLabel[k] || k, count }));

  const totalMoves = stageMoves + outreachMoves + demoMoves;
  const prospectiveNow = snap["prospect_identified"] || 0;
  return {
    period,
    // Client wants leads kept to a single headline number — the per-region,
    // B2B/B2C and sample-company breakdowns are intentionally NOT surfaced.
    leads_uploaded: {
      total: newLeads.length,
      enriched: enrichedLeads,
      leads_found: leadsFound,
      sourcing_channels: sourcingChannels,
    },
    outreach: {
      touchpoints,
      reach_breakdown: reachBreakdown,
      responses: outreachResponses,
      positive_replies: positiveReplyMoves,
      meetings_booked: meetingsBookedCount,
    },
    lead_qualification: {
      qualified_moves: qualifiedMoves,
      prospective_now: prospectiveNow,
      converted,
      pipeline_moves: stageMoves,
      outreach_moves: outreachMoves,
      demo_moves: demoMoves,
      total_moves: totalMoves,
      top_transitions: topTransitions,
    },
    tasks: {
      completed_count: tasksCompleted.length,
      completed: tasksCompleted,
      worked_on: tasksWorkedOn,
      details: taskDetails,
    },
    other_activity: {
      meetings: meetingsCount,
      collaboration_hours: collaborationHours,
      strategy_review_sessions: strategySessionsCount,
      campaigns: campaignsCount,
      blockers_raised: blockersCount,
      incentives_paid: incentivesPaid,
    },
    top_movers: topMovers,
    total_leads: leadAllRows.length,
    pipeline_snapshot_top: pipelineSnapshotTop,
    has_activity: !!(
      newLeads.length ||
      leadsUpdated ||
      converted ||
      totalMoves ||
      tasksCompleted.length ||
      tasksWorkedOn.length ||
      meetingsInWindow.length ||
      strategySessionsCount ||
      campaignsCount ||
      blockersCount
    ),
  };
}

// Returns a structured, client-facing summary: { headline, sections: [{ title,
// items: [ "text" | { label, items: ["text"] } ] }] }. Categorised so a client
// can glance and understand what was done (leads uploaded incl. region/type,
// qualification, tasks completed, other activity).
async function generateClientReportSummaryStructured({
  clientName,
  period,
  stats,
}) {
  if (!openai) {
    throw new Error(
      "OPENAI_API_KEY is missing. Add it in Railway variables first.",
    );
  }
  const periodLabel =
    period === "weekly"
      ? "this week so far (since Monday)"
      : "the last 24 hours";
  const prompt = `You are writing a short, structured progress update for the client "${clientName}", covering ${periodLabel}. Imagine someone asked "tell me what ${clientName}'s report is" and you reply with a clean, skimmable message.

Return ONLY valid JSON in this exact shape:
{
  "headline": "Team Effort: Activities executed by **GTM Specialists and Growth/Data Associate**, covering ...",
  "sections": [
    {
      "title": "Lead Generation",
      "description": "one short sentence describing what was done in this area",
      "stats": [
        { "value": "380", "label": "leads Found" },
        { "value": "365", "label": "enriched" },
        { "value": "12", "label": "sourcing channels" }
      ]
    }
  ]
}

BACKGROUND (use this to frame the work — it is effort context, not numbers to quote):
- This is not a simple upload. The leads team extracts leads from many different lead sources, then enriches each lead using lead tools — and often more than one tool when a specific data column is needed (for example, fund-raising signals require a separate enrichment tool). A lot of effort goes into every lead before it is uploaded.

Guidance:
- "headline": MUST follow this template — "Team Effort: Activities executed by **GTM Specialists and Growth/Data Associate**, covering <the areas worked over ${periodLabel}>." Keep the roles exactly as written including the ** markers (the UI renders **...** in bold), then finish the sentence naturally with the areas actually worked (e.g. lead research, enrichment, outreach, pipeline progression, strategic planning, and client collaboration).
- Group the work into sections with EXACTLY these titles, in this order, skipping a section entirely only if it has no data at all: "Lead Generation", "Outreach Execution", "Strategic Execution", "Client Collaboration".
- Each section has: a "title", a one-sentence "description" of what was done, and a "stats" array. Each stat is { "value": "<number>", "label": "<2-4 word label>" }. The UI renders the value in bold followed by the label, with stats separated by " | ".
- "Lead Generation" — description like "Identified, researched, validated, and enriched target accounts to build a high-quality prospect database." Stats from leads_uploaded, in this order: leads_found → "leads Found", enriched → "enriched", sourcing_channels → "sourcing channels". Do NOT break leads down by region/country, do NOT mention B2B/B2C, and do NOT list example company names.
- "Outreach Execution" — description like "Executed personalized multi-channel outreach, follow-ups, and prospect engagement across priority accounts." Stats from outreach, in this order: touchpoints → "touchpoints", positive_replies → "positive replies", meetings_booked → "meetings booked". You may add pipeline progress from lead_qualification (e.g. qualified_moves → "leads qualified") when notable. Do NOT turn outreach.reach_breakdown into stats or list individual channels/statuses — that per-channel breakdown is rendered separately, so keep this section's stats to the summary totals above.
- "Strategic Execution" — this section is task-driven (reviewing GTM performance, finalizing goals, executing planned work). tasks.details already covers BOTH the client's work items AND the linked tasks (the general tasks assigned to this client, some carrying source: "linked_task" with an "area" and "description"/detail), each scoped to the relevant reporting period. Write the "description" to reflect the ACTUAL tasks worked this period, drawn from tasks.details: mention the key task titles naturally in one or two sentences, and where a task carries a "description" or "area", weave that specific detail in (fall back to the generic "Reviewed GTM performance, refined messaging, prioritized accounts, and planned upcoming campaigns." only if tasks.details is empty). If the client is Navii, be more specific — incorporate the task descriptions, areas, and any linked task titles (tasks.details[].linked_title) so the client sees exactly what was worked on. Stats from tasks and other_activity, in this order: other_activity.strategy_review_sessions → "strategy & review sessions", tasks.completed_count → "action items finalized", tasks.worked_on length → "tasks in progress", other_activity.campaigns → "campaigns launched". Never invent task titles — use only what is in tasks.details.
- "Client Collaboration" — description like "Aligned with the ${clientName} team through execution reviews, planning discussions, and weekly progress tracking." Stats from other_activity, in this order: meetings → "meetings", collaboration_hours → "collaboration hours".
- Use ONLY real numbers from the DATA below for stat values. Do NOT invent any metric or number that is not in the data — if a metric is missing or zero, omit that stat entirely (and skip the whole section if all its stats are zero). Keep every value a real figure and every label short.
- If there was essentially no activity, return a headline saying so and an empty "sections" array.

DATA (JSON):
${JSON.stringify(stats, null, 2)}`;

  const completion = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    temperature: 0.3,
    response_format: { type: "json_object" },
    messages: [
      {
        role: "system",
        content:
          "You are an account-management assistant who writes concise, factual, well-structured client activity summaries. Return only valid JSON.",
      },
      { role: "user", content: prompt },
    ],
  });

  const parsed = safeParseJson(completion.choices?.[0]?.message?.content || "");
  if (!parsed || !Array.isArray(parsed.sections)) {
    throw new Error("AI returned an invalid summary.");
  }
  return parsed;
}

// Flatten the structured summary to plain text (fallback stored in summary_text
// and used by any non-HTML consumer).
function flattenReportSummary(data) {
  if (!data || !Array.isArray(data.sections)) return "";
  const lines = [];
  if (data.headline) lines.push(String(data.headline).replace(/\*\*/g, ""));
  data.sections.forEach((sec) => {
    lines.push("");
    if (sec.title) lines.push(String(sec.title));
    if (sec.description != null && String(sec.description).trim()) {
      lines.push(String(sec.description));
    }
    if (Array.isArray(sec.stats) && sec.stats.length) {
      const statLine = sec.stats
        .filter((s) => s && (s.value != null || s.label))
        .map((s) =>
          `${s.value != null ? String(s.value) : ""}${s.label ? " " + String(s.label) : ""}`.trim(),
        )
        .join(" | ");
      if (statLine) lines.push(statLine);
    }
    // Legacy bullet items (older rows).
    (sec.items || []).forEach((item) => {
      if (item && typeof item === "object" && Array.isArray(item.items)) {
        if (item.label) lines.push("• " + String(item.label));
        item.items.forEach((s) => lines.push("   - " + String(s)));
      } else {
        lines.push("• " + String(item));
      }
    });
  });
  return lines.join("\n").trim();
}

// Fetch just the inputs the summary needs, directly from client_leads + the
// activity tables (a lean version of the report route's big Promise.all).
async function fetchClientReportInputsForAI(
  orgId,
  clientId,
  sinceMs = null,
  client = null,
) {
  // 8 days covers both the daily (24h) and weekly (7d) windows for events. A
  // caller regenerating a specific historical week passes sinceMs so the status
  // logs for that older week are loaded too.
  const since = new Date(
    sinceMs != null ? sinceMs : Date.now() - 8 * 24 * 60 * 60 * 1000,
  ).toISOString();
  const [
    leadsR,
    campaignsR,
    meetingsR,
    blockersR,
    incentivesR,
    eventsR,
    usersR,
    workItemsR,
    linkedTasksR,
  ] = await Promise.all([
    supabase
      .from(CLIENT_LEADS_TABLE)
      .select(
        "id, company, business_name, pipeline_stage, outreach_status, demo_status, assigned_to, country, city, state, industry, lead_category, lead_source, enrichment_status, created_at, updated_at, reached_via_linkedin, reached_via_email, reached_via_website_form, reached_via_whatsapp, reached_via_phone, reached_via_instagram, reached_via_facebook",
      )
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .or("is_deleted.is.null,is_deleted.eq.false"),
    supabase
      .from("client_campaigns")
      .select(
        "id, sent_count, response_count, positive_replies, created_at, updated_at, created_by_user_id",
      )
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null),
    supabase
      .from("client_meetings")
      .select(
        "id, meeting_date, meeting_type, duration_min, created_at, created_by_user_id",
      )
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null),
    supabase
      .from("client_blockers")
      .select("id, created_at, owner_user_id")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null),
    supabase
      .from("client_incentives")
      .select("id, amount, created_at, gtm_user_id")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null),
    supabase
      .from("client_activity_logs")
      .select("actor_user_id, new_value, created_at")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("action", "client_lead_status_changed")
      .gte("created_at", since)
      .order("created_at", { ascending: false }),
    supabase
      .from("users")
      .select("id, name")
      .eq("org_id", orgId)
      .eq("is_active", true),
    supabase
      .from("client_work_items")
      .select(
        "id, title, description, status, dependency_work_item_id, created_at, updated_at",
      )
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null),
    // General tasks that name a business — the "Linked Tasks" surfaced on the
    // client workspace. Filtered to this client in JS by exact
    // (case-insensitive) match against the client name / company name, so the
    // Strategic Execution section can describe this work too.
    supabase
      .from("tasks")
      .select(
        "id, task_no, title, detail, business, area, status, priority, deadline, updated_at",
      )
      .eq("org_id", orgId)
      .not("business", "is", null),
  ]);

  // Match the client workspace's "Linked Tasks" rule: keep only tasks whose
  // free-text `business` equals this client's name or company name.
  const clientNameKeys = new Set(
    [client?.name, client?.company_name]
      .filter(Boolean)
      .map((s) => String(s).trim().toLowerCase()),
  );
  const linkedTasks = clientNameKeys.size
    ? (linkedTasksR.data || []).filter((t) =>
        clientNameKeys.has(
          String(t.business || "")
            .trim()
            .toLowerCase(),
        ),
      )
    : [];

  return {
    leadAllRows: leadsR.data || [],
    campaigns: campaignsR.data || [],
    meetings: meetingsR.data || [],
    blockers: blockersR.data || [],
    incentives: incentivesR.data || [],
    leadStageEvents: eventsR.data || [],
    users: usersR.data || [],
    workItems: workItemsR.data || [],
    linkedTasks,
  };
}

// Generate (or templated-skip) one period's summary for one client and upsert it
// onto today's PST row. `inputs` may be passed to avoid re-fetching per period.
async function runClientReportSummary({
  orgId,
  client,
  period,
  userId = null,
  inputs = null,
  weekStartMs = null,
}) {
  // Which week a weekly summary covers (defaults to the current calendar week).
  // Daily summaries are always the rolling last-24h window.
  const refMs = period === "weekly" && weekStartMs ? weekStartMs : Date.now();
  const data =
    inputs ||
    (await fetchClientReportInputsForAI(
      orgId,
      client.id,
      // For an older week, load status logs back to that week's Monday.
      period === "weekly" && weekStartMs
        ? mondayStartOfUtcMs(weekStartMs)
        : null,
      client,
    ));
  const stats = buildClientReportStatsForAI(data, period, refMs);
  const clientName = client.name || client.company_name || "this client";

  let summaryJson;
  if (!stats.has_activity) {
    const noActivity =
      period === "weekly"
        ? "No tracked activity this week so far."
        : "No tracked activity in the last 24 hours.";
    summaryJson = { headline: noActivity, sections: [] };
  } else {
    summaryJson = await generateClientReportSummaryStructured({
      clientName,
      period,
      stats,
    });
  }
  const summaryText =
    flattenReportSummary(summaryJson) || summaryJson.headline || "";
  if (!summaryText) throw new Error("AI returned an empty summary.");

  // Weekly summaries are keyed by the Monday of the week they cover, so each week
  // keeps its own row (and the current week refreshes in place as it progresses).
  // Daily summaries are keyed by today's PST date. created_at is stamped now on
  // every (re)generation so the "Generated …" badge reflects the latest refresh.
  const summaryDate =
    period === "weekly"
      ? weekStartDateString(refMs)
      : getTodayDateStringInTimeZone(CLIENT_REPORT_SUMMARY_TZ);
  const baseRow = {
    org_id: orgId,
    client_id: client.id,
    period,
    summary_date: summaryDate,
    summary_text: summaryText,
    summary_json: summaryJson,
    model: stats.has_activity ? "gpt-4o-mini" : "template",
    stats,
    created_by_user_id: userId,
    created_at: new Date().toISOString(),
  };
  const upsert = (row) =>
    supabase
      .from("client_report_ai_summaries")
      .upsert([row], { onConflict: "org_id,client_id,period,summary_date" })
      .select()
      .single();

  let { data: saved, error } = await upsert(baseRow);
  // Graceful fallback if the summary_json column isn't migrated yet: store the
  // flattened text only (still readable; bullets survive as plain text). Run the
  // ALTER in sql/2026-06-23-client-report-ai-summaries.sql for rich rendering.
  if (error && /summary_json/.test(error.message || "")) {
    const { summary_json, ...rowNoJson } = baseRow;
    ({ data: saved, error } = await upsert(rowNoJson));
  }
  if (error) throw error;
  return saved;
}

// Latest stored summary per period for a client. Degrades gracefully (returns
// nulls) if the table is missing, so the Report tab still renders its placeholder.
async function getLatestClientReportSummaries(orgId, clientId) {
  // `daily`/`weekly` = the most recent of each (used by the Overview + Daily
  // tab). `weeklyByDate` maps each week's Monday date → that week's stored weekly
  // summary, so every Week N tab can show its own. Enough rows to cover ~2 years
  // of weekly history plus recent dailies.
  const out = { daily: null, weekly: null, weeklyByDate: {} };
  try {
    const { data, error } = await supabase
      .from("client_report_ai_summaries")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .order("created_at", { ascending: false })
      .limit(160);
    if (error) {
      console.error("getLatestClientReportSummaries error:", error.message);
      return out;
    }
    for (const row of data || []) {
      if (!out[row.period]) out[row.period] = row;
      if (row.period === "weekly" && row.summary_date) {
        const key = String(row.summary_date).slice(0, 10);
        // Rows are newest-first, so keep the first (latest) per week date.
        if (!out.weeklyByDate[key]) out.weeklyByDate[key] = row;
      }
    }
  } catch (e) {
    console.error("getLatestClientReportSummaries threw:", e.message);
  }
  return out;
}

// The manually-curated Goals block for a client (one row per client). Returns
// null when unset or if the table isn't migrated yet, so the panel renders its
// empty-goals placeholder instead of failing.
async function getClientGoals(orgId, clientId) {
  try {
    const { data, error } = await supabase
      .from("client_goals")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .maybeSingle();
    if (error) {
      console.error("getClientGoals error:", error.message);
      return null;
    }
    return data || null;
  } catch (e) {
    console.error("getClientGoals threw:", e.message);
    return null;
  }
}

// Nightly batch: generate today's daily + weekly summary for every active
// client. Idempotent — skips (client, period) already done for today's PST date.
async function runDailyClientReportSummaries() {
  if (!openai) {
    console.warn(
      "[report-summary] OPENAI_API_KEY missing; skipping nightly run.",
    );
    return { ok: false, generated: 0, reason: "no_openai_key" };
  }
  const { data: clients, error } = await supabase
    .from("clients")
    .select("id, org_id, name, company_name")
    .eq("is_active", true)
    .is("deleted_at", null);
  if (error) {
    console.error("[report-summary] clients fetch error:", error.message);
    return { ok: false, generated: 0, reason: "clients_fetch_failed" };
  }

  // Daily is keyed by today; the weekly summary is keyed by (and refreshed for)
  // the current calendar week's Monday, so it updates each night as the week runs.
  const todayStr = getTodayDateStringInTimeZone(CLIENT_REPORT_SUMMARY_TZ);
  const weekStr = weekStartDateString(Date.now());
  const summaryDate = todayStr;
  const periodDate = { daily: todayStr, weekly: weekStr };
  let generated = 0;
  let skipped = 0;
  let failed = 0;
  for (const client of clients || []) {
    const orgId = client.org_id || DASHBOARD_ORG_ID;
    try {
      const { data: existing } = await supabase
        .from("client_report_ai_summaries")
        .select("period, summary_date, created_at")
        .eq("org_id", orgId)
        .eq("client_id", client.id)
        .in("summary_date", [todayStr, weekStr]);
      // A period counts as done only if its row for the right date was already
      // (re)generated today — so the weekly summary is refreshed once per day.
      const generatedToday = (period) =>
        (existing || []).some(
          (r) =>
            r.period === period &&
            r.summary_date === periodDate[period] &&
            getDateStringInTimeZone(
              new Date(r.created_at),
              CLIENT_REPORT_SUMMARY_TZ,
            ) === todayStr,
        );
      if (generatedToday("daily") && generatedToday("weekly")) {
        skipped += 1;
        continue;
      }
      const inputs = await fetchClientReportInputsForAI(
        orgId,
        client.id,
        null,
        client,
      );
      for (const period of CLIENT_REPORT_SUMMARY_PERIODS) {
        if (generatedToday(period)) continue;
        await runClientReportSummary({ orgId, client, period, inputs });
        generated += 1;
      }
    } catch (e) {
      failed += 1;
      console.error(`[report-summary] client ${client.id} failed:`, e.message);
    }
  }
  console.log(
    `[report-summary] done date=${summaryDate} generated=${generated} skipped=${skipped} failed=${failed}`,
  );
  return { ok: true, generated, skipped, failed, date: summaryDate };
}

// Best-effort in-process nightly trigger (~9 PM PST). For guaranteed runs also
// configure an external scheduler to POST /api/cron/generate-report-summaries.
// Idempotent via the DB unique index, so a duplicate trigger is harmless.
let __lastReportSummaryRunDate = null;
function startClientReportSummaryScheduler() {
  if (!openai) return;
  const tick = async () => {
    try {
      const { hour } = getPartsInTimeZone(new Date(), CLIENT_REPORT_SUMMARY_TZ);
      const todayPst = getTodayDateStringInTimeZone(CLIENT_REPORT_SUMMARY_TZ);
      if (hour >= 21 && __lastReportSummaryRunDate !== todayPst) {
        __lastReportSummaryRunDate = todayPst;
        console.log("[report-summary] nightly trigger firing for", todayPst);
        await runDailyClientReportSummaries();
      }
    } catch (e) {
      console.error("[report-summary] scheduler tick error:", e.message);
    }
  };
  // Interval only (no immediate run) so importing this module — e.g. during a
  // Next.js build — never triggers generation. At runtime the first tick lands
  // within 15 minutes of startup. unref() so the timer never keeps a process
  // alive on its own (e.g. scripts/gen-routes.mjs imports this module).
  const timer = setInterval(tick, 15 * 60 * 1000);
  if (timer && typeof timer.unref === "function") timer.unref();
}
startClientReportSummaryScheduler();

// Renders the AI-summary panel for one period. `row` is the stored summary (or
// null → placeholder). `editable` adds the internal "Regenerate" button.
// Renders the structured summary body: a "Team Effort:" headline (bold label)
// followed by sections, each with a bold title, a one-line description, and a
// stats line of bold numbers separated by "|". Falls back to the legacy bullet
// shape for older rows, then plain text; "" if there's nothing.
function renderReportSummaryBody(row) {
  const json = row && row.summary_json;
  // The Reached-Via channel/status breakdown is rendered deterministically from
  // the stored stats (not the AI text) so the per-channel numbers are exact.
  const reachBreakdown =
    (row &&
      row.stats &&
      row.stats.outreach &&
      row.stats.outreach.reach_breakdown) ||
    [];
  if (
    json &&
    Array.isArray(json.sections) &&
    (json.sections.length || json.headline)
  ) {
    // Headline: bold the label up to the first colon (e.g. "Team Effort:") and
    // any **...** segments (e.g. the team roles) — matching the client format.
    const renderHeadline = (text) => {
      const str = String(text);
      const idx = str.indexOf(":");
      const hasLabel = idx > 0 && idx < 40;
      const label = hasLabel
        ? `<span style="font-weight:700;">${escapeHtml(str.slice(0, idx + 1))}</span>`
        : "";
      const rest = escapeHtml(hasLabel ? str.slice(idx + 1) : str).replace(
        /\*\*([^*]+)\*\*/g,
        '<span style="font-weight:700;">$1</span>',
      );
      return label + rest;
    };
    // Legacy bullet items (older rows without description/stats).
    const renderItem = (item) => {
      if (item && typeof item === "object" && Array.isArray(item.items)) {
        return `<li style="margin:4px 0;">${item.label ? `<span style="font-weight:700;">${escapeHtml(String(item.label))}</span>` : ""}<ul style="margin:4px 0 0; padding-left:20px; list-style:circle;">${item.items.map((s) => `<li style="margin:2px 0;">${escapeHtml(String(s))}</li>`).join("")}</ul></li>`;
      }
      return `<li style="margin:3px 0;">${escapeHtml(String(item))}</li>`;
    };
    // Stats line: "<b>380</b> leads added | <b>365</b> enriched".
    const renderStats = (stats) =>
      (stats || [])
        .filter((s) => s && (s.value != null || s.label))
        .map(
          (s) =>
            `<span style="font-weight:700;">${escapeHtml(String(s.value != null ? s.value : ""))}</span>${s.label ? ` ${escapeHtml(String(s.label))}` : ""}`,
        )
        .join(' <span style="opacity:.4;">|</span> ');
    // Reached-Via breakdown sub-list: "LinkedIn — 10 reached: 4 Connection Sent · 4 Engaged".
    const renderReachBreakdown = (breakdown) =>
      `<ul style="margin:6px 0 0; padding-left:20px; list-style:disc; font-size:13px; line-height:1.5;">${breakdown
        .filter((b) => b && b.channel)
        .map((b) => {
          const statuses = (b.statuses || [])
            .filter((s) => s && (s.count != null || s.status))
            .map(
              (s) =>
                `<span style="font-weight:700;">${escapeHtml(String(s.count != null ? s.count : ""))}</span> ${escapeHtml(String(s.status || ""))}`,
            )
            .join(" · ");
          return `<li style="margin:2px 0;"><span style="font-weight:700;">${escapeHtml(String(b.channel))}</span> — <span style="font-weight:700;">${escapeHtml(String(b.count != null ? b.count : ""))}</span> reached${statuses ? `: ${statuses}` : ""}</li>`;
        })
        .join("")}</ul>`;
    const head = json.headline
      ? `<div style="font-size:14px; line-height:1.6; margin-bottom:4px;">${renderHeadline(json.headline)}</div>`
      : "";
    const sections = (json.sections || [])
      .map((sec) => {
        const title = `<div style="font-weight:700; font-size:15px; margin-bottom:2px;">${escapeHtml(String(sec.title || ""))}</div>`;
        const hasStats = Array.isArray(sec.stats) && sec.stats.length;
        const hasDesc =
          sec.description != null && String(sec.description).trim();
        if (hasStats || hasDesc) {
          const desc = hasDesc
            ? `<div style="font-size:14px; line-height:1.5; margin-top:2px;">${escapeHtml(String(sec.description))}</div>`
            : "";
          const stats = hasStats
            ? `<div style="font-size:14px; line-height:1.5; margin-top:4px;">${renderStats(sec.stats)}</div>`
            : "";
          // Attach the per-channel Reached-Via / status breakdown under Outreach Execution.
          const breakdown =
            String(sec.title || "").trim() === "Outreach Execution" &&
            reachBreakdown.length
              ? renderReachBreakdown(reachBreakdown)
              : "";
          return `<div style="margin-top:16px;">${title}${desc}${stats}${breakdown}</div>`;
        }
        // Legacy shape: bullet list of items.
        return `
      <div style="margin-top:16px;">
        ${title}
        <ul style="margin:6px 0 0; padding-left:20px; list-style:disc; font-size:14px; line-height:1.5;">${(sec.items || []).map(renderItem).join("")}</ul>
      </div>`;
      })
      .join("");
    return head + sections;
  }
  if (row && row.summary_text) {
    return `<div style="font-size:14px; line-height:1.65; white-space:pre-wrap;">${escapeHtml(row.summary_text)}</div>`;
  }
  return "";
}

function renderReportSummaryPanel({
  period,
  row,
  editable,
  clientId,
  weekStart = null,
  weekLabel = "",
  rangeLabel = "",
}) {
  const isWeekly = period === "weekly";
  const title = isWeekly
    ? `${weekLabel ? escapeHtml(weekLabel) + " Summary" : "Weekly Summary"}`
    : "Daily Summary";
  const sub = isWeekly
    ? `${rangeLabel ? escapeHtml(rangeLabel) : "this week (since Monday)"} · auto-generated daily at 9 PM PST`
    : "last 24 hours · auto-generated daily at 9 PM PST";
  const contentHtml = renderReportSummaryBody(row);
  const hasContent = !!contentHtml;
  const when = row && row.created_at ? formatDateTime(row.created_at) : "";
  const body = hasContent
    ? contentHtml
    : `<div class="meta" style="font-size:13px; line-height:1.65;">🕘 Your ${isWeekly ? "weekly" : "daily"} AI summary is generated automatically every day at 9&nbsp;PM&nbsp;PST. Check back then${editable ? ", or generate it now." : "."}</div>`;
  const btn = editable
    ? `<button class="btn" type="button" style="padding:5px 12px; font-size:12px; white-space:nowrap; background:#16a34a; border:1px solid #16a34a; color:#fff;" onclick="regenReportSummary('${period}', this, ${Number(clientId)}, ${weekStart ? `'${escapeHtml(String(weekStart))}'` : "null"})">${hasContent ? "Regenerate" : "Generate now"}</button>`
    : "";
  const whenBadge = when
    ? `<span class="meta" style="font-size:12px; font-weight:400; white-space:nowrap;">Generated ${escapeHtml(when)}</span>`
    : "";
  return `
    <div class="panel" data-ai-sum="${period}" style="margin-bottom:16px;">
      <div class="panel-head" style="display:flex; align-items:flex-start; gap:12px; margin-bottom:12px;">
        <div>
          <h2 style="margin:0; display:flex; align-items:center; flex-wrap:wrap; gap:10px;">✨ ${title}${btn}${whenBadge}</h2>
          <div class="meta" style="font-size:12px;">${sub}</div>
        </div>
      </div>
      ${body}
    </div>`;
}

// Normalizes a stored client_goals row into the structured shape the UI uses:
// a list of { title, value } goal items plus a free-text `notes` block. Rows
// created before the structured migration only have `goals_text`, so we surface
// that legacy free text as the notes so nothing is lost.
function normalizeClientGoalsData(row) {
  let items = [];
  if (row && Array.isArray(row.goals_json)) {
    items = row.goals_json
      .map((g) => ({
        title: String((g && g.title) || "").trim(),
        value: String(g && g.value != null ? g.value : "").trim(),
      }))
      .filter((g) => g.title || g.value);
  }
  let notes = row && row.notes != null ? String(row.notes) : "";
  if (!items.length && !notes.trim() && row && row.goals_text) {
    notes = String(row.goals_text);
  }
  return { items, notes };
}

// Markup for one editable goal row in the modal. Shared shape with the
// client-side goalRowMarkup() so server-rendered and JS-added rows match.

// Inner form body for the Edit Goals modal: pre-filled title/number rows, an
// "add goal" button, and a Notes textarea. Rendered server-side from the stored
// row; JS then adds/removes rows client-side.

// Manually-curated Goals panel shown beside the AI summary. `row` is the stored
// client_goals row (or null → placeholder). `editable` adds the "Edit goals"
// button (staff only). `users` resolves the "last updated by" name.
function renderClientGoalsPanel({ row, editable, clientId, users = [] }) {
  const { items, notes } = normalizeClientGoalsData(row);
  const notesText = notes.trim();
  const hasText = items.length > 0 || !!notesText;
  const when = row && row.updated_at ? formatDateTime(row.updated_at) : "";
  const byName =
    row && row.updated_by_user_id
      ? users.find((u) => String(u.id) === String(row.updated_by_user_id))?.name
      : "";
  const editBtn = editable
    ? `<button class="btn" type="button" style="padding:5px 12px; font-size:12px; white-space:nowrap;" onclick="openGoalsModal(${Number(clientId)})">${hasText ? "Edit goals" : "Add goals"}</button>`
    : "";
  const metaLine =
    hasText && when
      ? `<div class="meta" style="font-size:12px; margin-top:10px;">Last updated ${escapeHtml(when)}${byName ? ` by ${escapeHtml(byName)}` : ""}</div>`
      : "";
  const goalsListHtml = items.length
    ? `<div style="display:flex; flex-direction:column; gap:8px;">${items
        .map(
          (
            g,
          ) => `<div style="display:flex; align-items:baseline; justify-content:space-between; gap:12px;">
        <span style="font-weight:700; font-size:14px;">${escapeHtml(g.title)}</span>
        <span style="font-weight:700; font-size:14px; white-space:nowrap;">${escapeHtml(g.value)}</span>
      </div>`,
        )
        .join("")}</div>`
    : "";
  const notesHtml = notesText
    ? `<div style="margin-top:${items.length ? 14 : 0}px; font-size:14px; line-height:1.6; white-space:pre-wrap;">${escapeHtml(notes)}</div>`
    : "";
  const body = hasText
    ? `${goalsListHtml}${notesHtml}`
    : `<div class="meta" style="font-size:13px; line-height:1.65;">🎯 No goals set yet.${editable ? " Use “Add goals” to capture this client’s goals." : ""}</div>`;
  return `
    <div class="panel" data-client-goals="${Number(clientId)}" style="margin-bottom:16px; height:97%;">
      <div class="panel-head" style="display:flex; align-items:flex-start; gap:12px; margin-bottom:12px;">
        <div>
          <h2 style="margin:0; display:flex; align-items:center; flex-wrap:wrap; gap:10px;">🎯 Weekly Goals${editBtn}</h2>
          <div class="meta" style="font-size:12px;">Manually curated · visible to the client</div>
        </div>
      </div>
      ${body}
      ${metaLine}
    </div>`;
}

// Two-column layout: AI summary on the left, manually-curated Goals on the
// right. Stacks vertically on narrow screens (flex-wrap).
function renderSummaryWithGoals({
  period,
  summaryRow,
  goalsRow,
  editable,
  clientId,
  users = [],
  weekStart = null,
  weekLabel = "",
  rangeLabel = "",
}) {
  // Goals show alongside both the daily and weekly summaries, so the client
  // sees the curated targets next to either view.
  const goalsPanel = `<div style="flex:1 1 320px; min-width:280px;">${renderClientGoalsPanel({ row: goalsRow, editable, clientId, users })}</div>`;
  return `
    <div style="display:flex; gap:16px; align-items:stretch; flex-wrap:wrap;">
      <div style="flex:1 1 380px; min-width:300px;">${renderReportSummaryPanel({ period, row: summaryRow, editable, clientId, weekStart, weekLabel, rangeLabel })}</div>
      ${goalsPanel}
    </div>`;
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

// =====================================================
// PHASE 6: CLIENT UPDATES - EDIT / ARCHIVE
// =====================================================

// =====================================================
// PHASE 7: ACTIONS NEEDED
// =====================================================

// =====================================================
// PHASE 8: CONTRIBUTORS
// Internal / Contractor / Client Contact
// =====================================================

// Nightly cron entrypoint for client report AI summaries. Point an external
// scheduler at this (POST, 9 PM PST) with the CRON_SECRET. Auth is the secret
// only, so it is unusable until CRON_SECRET is set (safe by default).

// Manual single-client regenerate (internal "Regenerate" button).

// Upsert the manually-curated Goals block for a client (staff only; the client
// sees it read-only on their external dashboard). Records who last changed it.

// Editable single-select profile fields on the /account page. Each maps to a
// column on the users row; values are validated against these lists on save.
const ACCOUNT_FIELD_OPTIONS = {
  department: ["GTM", "Leads", "Others"],
  designation: [
    "CEO",
    "Program Head",
    "Project Manager",
    "Sr. Manager",
    "Associate",
  ],
};

// Render a profile-field dropdown. When the stored value isn't one of the known
// options (or is unset) a selected "-" option is appended so nothing is lost
// visually until the user picks a real value.

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

// Lightweight client list for the top-nav "Clients" dropdown (id + name only).

// ---------------------------------------------------------------------------
// Bulk import client leads from Excel (reuses the rasset column mapper +
// existing lead_import_logs/lead_import_log_rows tables).
// ---------------------------------------------------------------------------
// Fields an Excel re-import must NOT touch when it matches an existing lead by
// email. Everything else on the mapped row is sourced data and gets refreshed.
//   - pipeline_stage / lead_stage / status / outreach-side state: the team owns
//     where the lead sits; the mapper always emits the "new lead" defaults, so
//     writing them would drag a qualified lead back to the start.
//   - notes: this is the lead's note history (call notes are appended via
//     add_note), not a sheet column — overwriting it destroys the team's work.
//   - lead_source / enrichment_status: record how the lead first arrived.
//   - is_client_visible: the per-lead visibility toggle is a manual decision.
const IMPORT_PRESERVED_ON_UPDATE = new Set([
  "pipeline_stage",
  "lead_stage",
  "lead_category",
  "status",
  "notes",
  "lead_source",
  "enrichment_status",
  "is_client_visible",
]);

// Compares an "Assigned to" / "Verified by" name the way people type it: case-
// and spacing-insensitive, so "  marish  bhat " matches the user "Marish Bhat".
function getUserNameKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

// The sheet columns that hold a team member's name and must therefore resolve to
// a real user. All are stored as plain text on the lead and are rendered /
// filtered / attributed by name elsewhere in the app.
const EXCEL_USER_NAME_COLUMNS = [
  { field: "assigned_to", label: "Assigned to" },
  { field: "phone_assigned_to", label: "Assigned for Phone" },
  { field: "email_assigned_to", label: "Assigned for email" },
  { field: "verified_by", label: "Verified by" },
];

// Rejects the whole sheet when any row's assignee / "Verified by" name (see
// EXCEL_USER_NAME_COLUMNS) is not an active user of this org. Throws a 400 error listing the exact sheet
// rows so the uploader can fix the names (or add the user) and re-upload. A
// blank cell is allowed — that is an unassigned / unverified lead, the same as
// the lead form's "Unassigned" and "Not verified" options.
async function assertExcelUserNamesExist({ orgId, payloads }) {
  const cited = [];
  for (const column of EXCEL_USER_NAME_COLUMNS) {
    payloads.forEach((p, i) => {
      const name = String(p?.[column.field] || "").trim();
      if (name) cited.push({ row: i + 2, name, label: column.label });
    });
  }
  if (!cited.length) return;

  const { data: activeUsers, error } = await supabase
    .from("users")
    .select("name")
    .eq("org_id", orgId)
    .eq("is_active", true);
  if (error) throw error;

  const knownNames = new Set(
    (activeUsers || []).map((u) => getUserNameKey(u.name)).filter(Boolean),
  );
  const unknown = cited.filter((r) => !knownNames.has(getUserNameKey(r.name)));
  if (!unknown.length) return;

  // Report in sheet order (both columns of row 3 together, then row 4) rather
  // than column by column, so the uploader can walk the sheet top to bottom.
  unknown.sort((a, b) => a.row - b.row);

  // Only the first rows are listed so one bad column doesn't produce a
  // thousand-line alert; the count still reports the true total.
  const MAX_LISTED = 15;
  const listed = unknown
    .slice(0, MAX_LISTED)
    .map((r) => `Row ${r.row} — ${r.label}: "${r.name}"`);
  if (unknown.length > MAX_LISTED) {
    listed.push(`...and ${unknown.length - MAX_LISTED} more`);
  }

  const err = new Error(
    [
      `Import cancelled — nothing was imported. ${unknown.length} name(s) in the sheet are not an active user:`,
      ...listed,
      `Fix these names in the sheet (they must match a user exactly) or add the user, then upload again.`,
    ].join("\n"),
  );
  err.statusCode = 400;
  throw err;
}

async function importClientLeadsFromExcel({
  orgId,
  clientId,
  buffer,
  fileName,
  uploadedByUserId,
  uploadedByName,
  categoryType,
}) {
  // Every client_leads import column is text, so read cells as their displayed
  // text (raw: false on sheet_to_json) rather than as raw numbers. Without this,
  // a real date cell in an .xlsx arrives as the Excel serial 45566.0001 instead
  // of a date, and numeric-looking phone columns lose their formatting. dateNF
  // normalizes any cell the sheet typed as a date to yyyy-mm-dd.
  //
  // The read-time options below only affect text formats (CSV), which is how the
  // Navii sheet is exported:
  //   - codepage 65001 decodes the file as UTF-8; the default (cp1252) turns
  //     "₹2.75 crore" into "â¹2.75 crore".
  //   - raw: true stops the CSV parser from guessing types per cell. Its guesses
  //     corrupt this sheet — an "Openings" range of "5-8" becomes the date
  //     2001-05-08 — and since every target column is text, the sheet's own text
  //     ("July 02, 2024", "2+", "$4.2M") is exactly what we want to store.
  // .xlsx workbooks are unaffected: their date cells are still typed dates and
  // are formatted by dateNF as before.
  const workbook = XLSX.read(buffer, {
    type: "buffer",
    cellDates: true,
    dateNF: "yyyy-mm-dd",
    codepage: 65001,
    raw: true,
  });
  const sheetName = workbook.SheetNames[0];
  if (!sheetName) throw new Error("Excel file has no sheets");

  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: "", raw: false });

  // Map every row up front so the sheet can be validated as a whole before a
  // single lead is written. The loop below reuses these payloads.
  const payloads = rows.map((row) => mapExcelRowToClientLead(row));

  // The assignee columns ("Assigned to" / "Assigned for Phone" / "Assigned for
  // email") and "Verified by" are stored as plain names, and everything
  // downstream (the Leads tab's dropdowns and filters, the reporting attribution
  // that maps a lead's assignee back to a user id) only works when the name is
  // an active user. A typo or an ex-employee would silently import leads nobody
  // owns, so the whole sheet is rejected before the import log is created and the
  // offending sheet rows are named back to the uploader.
  await assertExcelUserNamesExist({ orgId, payloads });

  const business = `client:${clientId}`;
  const importLog = await createLeadImportLog({
    orgId,
    business,
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

  // Navii's sourced sheets (Apollo/YC exports) routinely have no company,
  // website, or phone but always carry an email + contact name. For Navii we
  // therefore (a) never treat an email/name-bearing row as "empty" and (b)
  // de-dupe on phone-or-email instead of phone-or-website. Other clients keep
  // the original phone-or-website behavior.
  const { data: clientRow } = await supabase
    .from("clients")
    .select("name, company_name")
    .eq("org_id", orgId)
    .eq("id", clientId)
    .maybeSingle();
  const isNavii = [clientRow?.name, clientRow?.company_name].some(
    (s) => getBusinessCanonicalName(s) === "navii",
  );
  // Rebus AI imports a contact sheet (Work Email / Phone Number, see the
  // RebusAI_Leads CSV) that also has no company/website on many rows and must be
  // de-duped on phone-or-email. It therefore follows the same keep-any-row and
  // email-dedupe rules as Navii.
  const isRebus = [clientRow?.name, clientRow?.company_name].some(
    (s) => getBusinessCanonicalName(s) === "rebus",
  );
  // Revivflow's sheet (R_Leads.csv) is one row per contact, so several rows
  // share a company website and many carry "n/a" in the Number column — website
  // de-dupe would collapse a company's contacts into a single lead. Email (or
  // the person's LinkedIn) is the identity there, same as Navii/Rebus.
  const isRevivflow = [clientRow?.name, clientRow?.company_name].some(
    (s) => getBusinessCanonicalName(s) === "revivflow",
  );
  // Clients whose sheets carry a contact email but often no phone/website:
  // keep every non-blank row and de-dupe on phone-or-email instead of website.
  const dedupeOnEmail = isNavii || isRebus || isRevivflow;

  // Pre-load existing phones + websites + emails for this client once (avoids
  // per-row queries). Sourced client leads often have no phone, so we also
  // de-dupe by website (or, for Navii, email) to keep re-imports idempotent.
  //
  // A plain .select() is capped at 1000 rows by PostgREST and Navii-style
  // clients hold far more leads than that, so page through with .range().
  // Without paging, every existing lead past row 1000 is invisible to the
  // de-dupe and its row re-imports as a brand new duplicate instead of
  // updating the lead that is already there.
  const seenPhoneKeys = new Map();
  const seenWebsiteKeys = new Map();
  const seenEmailKeys = new Map();
  const seenLinkedinKeys = new Map();
  const EXISTING_PAGE_SIZE = 1000;
  for (let from = 0; ; from += EXISTING_PAGE_SIZE) {
    const { data: page, error: pageError } = await supabase
      .from(CLIENT_LEADS_TABLE)
      .select("id, phone, website, email, person_linkedin_url")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .or("is_deleted.is.null,is_deleted.eq.false")
      // Without an explicit order, Postgres makes no promise that successive
      // LIMIT/OFFSET pages are disjoint — rows could be skipped between pages
      // and their emails would then re-import as duplicates.
      .order("id", { ascending: true })
      .range(from, from + EXISTING_PAGE_SIZE - 1);
    if (pageError) throw pageError;
    for (const r of page || []) {
      const pk = getLeadPhoneKey(r.phone);
      if (pk && !seenPhoneKeys.has(pk)) seenPhoneKeys.set(pk, r.id);
      const wk = getLeadWebsiteKey(r.website);
      if (wk && !seenWebsiteKeys.has(wk)) seenWebsiteKeys.set(wk, r.id);
      const ek = getLeadEmailKey(r.email);
      if (ek && !seenEmailKeys.has(ek)) seenEmailKeys.set(ek, r.id);
      const lk = getLeadLinkedinKey(r.person_linkedin_url);
      if (lk && !seenLinkedinKeys.has(lk)) seenLinkedinKeys.set(lk, r.id);
    }
    if (!page || page.length < EXISTING_PAGE_SIZE) break;
  }

  try {
    for (let i = 0; i < rows.length; i += 1) {
      const rowNumber = i + 2;
      try {
        const payload = payloads[i];
        payload.phone = normalizeLeadPhone(payload.phone);
        // Imported client leads land in the first pipeline stage.
        payload.pipeline_stage = DEFAULT_CLIENT_LEAD_STAGE;
        // The category type picked in the import dialog is stamped on every row
        // of the sheet (and on re-imports, refreshes the existing lead's type).
        if (categoryType) payload.category_type = categoryType;

        // Navii keeps every row that has any data at all. We test the RAW sheet
        // row (not just the mapped fields) so a row is only dropped when every
        // cell is blank — that way leads are never lost just because a column
        // header isn't one the mapper recognizes. Other clients keep the
        // original "no company/website/phone/maps -> empty" rule.
        const rawRowHasData = Object.values(rows[i] || {}).some(
          (v) => String(v == null ? "" : v).trim() !== "",
        );
        const isEmptyRow = dedupeOnEmail
          ? !rawRowHasData
          : !payload.company &&
            !payload.website &&
            !payload.phone &&
            !payload.google_maps_url;
        if (isEmptyRow) {
          results.skipped += 1;
          await addLeadImportRowLog({
            importId: importLog.id,
            orgId,
            business,
            rowNumber,
            phone: payload.phone,
            company: payload.company,
            website: payload.website,
            status: "skipped",
            message: "Empty row skipped",
          });
          continue;
        }

        const phoneKey = getLeadPhoneKey(payload.phone);
        const websiteKey = getLeadWebsiteKey(payload.website);
        const emailKey = getLeadEmailKey(payload.email);

        // Import is an upsert on email: a row whose email already exists on this
        // client updates that lead with the sheet's values instead of being
        // inserted again. Only non-empty sheet values are written, so a column
        // the sheet leaves blank never wipes existing data, and the fields the
        // team owns (see IMPORT_PRESERVED_ON_UPDATE) are left alone — a
        // re-import refreshes the sourced data without resetting the lead's
        // place in the pipeline or overwriting its note history.
        //
        // When the email cell is not a real address ("n/a", "-", a bare company
        // domain) there is no email key, so the person's LinkedIn profile is the
        // fallback identity. Without it those rows match nothing and re-insert
        // on every import.
        const linkedinKey = getLeadLinkedinKey(payload.person_linkedin_url);
        const matchedLeadId =
          (emailKey && seenEmailKeys.get(emailKey)) ||
          (!emailKey && linkedinKey && seenLinkedinKeys.get(linkedinKey)) ||
          null;
        if (matchedLeadId) {
          const existingLeadId = matchedLeadId;
          const updatePayload = {};
          for (const [key, value] of Object.entries(payload)) {
            if (IMPORT_PRESERVED_ON_UPDATE.has(key)) continue;
            if (value === null || value === undefined) continue;
            if (typeof value === "string" && value.trim() === "") continue;
            updatePayload[key] = value;
          }
          updatePayload.updated_at = new Date().toISOString();
          const { error: updateError } = await supabase
            .from(CLIENT_LEADS_TABLE)
            .update(updatePayload)
            .eq("org_id", orgId)
            .eq("client_id", clientId)
            .eq("id", existingLeadId);
          if (updateError) throw updateError;

          if (phoneKey) seenPhoneKeys.set(phoneKey, existingLeadId);
          if (websiteKey) seenWebsiteKeys.set(websiteKey, existingLeadId);
          if (linkedinKey && !seenLinkedinKeys.has(linkedinKey)) {
            seenLinkedinKeys.set(linkedinKey, existingLeadId);
          }
          results.updated += 1;
          await addLeadImportRowLog({
            importId: importLog.id,
            orgId,
            business,
            rowNumber,
            phone: payload.phone,
            company: payload.company,
            website: payload.website,
            status: "success",
            message: emailKey
              ? "Existing lead updated (matched on email)"
              : "Existing lead updated (matched on LinkedIn profile — no valid email on the row)",
            existingLeadId,
          });
          continue;
        }

        // Email / LinkedIn matches are handled by the upsert above; remaining
        // rows de-dupe on phone (everyone) or website (non-Navii/Rebus).
        let duplicateOfId = null;
        let duplicateMessage = "";
        if (phoneKey && seenPhoneKeys.has(phoneKey)) {
          duplicateOfId = seenPhoneKeys.get(phoneKey);
          duplicateMessage = "Duplicate phone number skipped";
        } else if (
          !dedupeOnEmail &&
          websiteKey &&
          seenWebsiteKeys.has(websiteKey)
        ) {
          duplicateOfId = seenWebsiteKeys.get(websiteKey);
          duplicateMessage = "Duplicate website skipped";
        }
        if (duplicateOfId) {
          results.duplicates += 1;
          await addLeadImportRowLog({
            importId: importLog.id,
            orgId,
            business,
            rowNumber,
            phone: payload.phone,
            company: payload.company,
            website: payload.website,
            status: "duplicate",
            message: duplicateMessage,
            existingLeadId: duplicateOfId,
          });
          continue;
        }

        const { data: insertedLead, error: insertError } = await supabase
          .from(CLIENT_LEADS_TABLE)
          .insert([{ org_id: orgId, client_id: clientId, ...payload }])
          .select("id")
          .single();
        if (insertError) throw insertError;

        if (phoneKey) seenPhoneKeys.set(phoneKey, insertedLead?.id);
        if (websiteKey) seenWebsiteKeys.set(websiteKey, insertedLead?.id);
        if (emailKey) seenEmailKeys.set(emailKey, insertedLead?.id);
        if (linkedinKey && !seenLinkedinKeys.has(linkedinKey)) {
          seenLinkedinKeys.set(linkedinKey, insertedLead?.id);
        }
        results.inserted += 1;
        await addLeadImportRowLog({
          importId: importLog.id,
          orgId,
          business,
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
          business,
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

// Upload a call recording for a single client lead. Stores the audio in the
// shared "lead-call-recordings" bucket and saves the public URL on the lead.

// ---------------------------------------------------------------------------
// Per-client Leads (reuses the rasset/joolian leads engine via "client:<id>")
// ---------------------------------------------------------------------------

// Attach a voice note to a lead: stores the audio for playback, transcribes it
// (best effort) and appends the typed note + transcription to the notes history.

// One-click "call made" log from the Leads table. Records that a call was
// placed (is_call_made), when (call_time) and by whom (call_made_by), server
// side, so the call icon can lock red + disabled. Idempotent — re-logging a
// lead that's already marked is a no-op that returns the existing values.

// Delete a single client lead.

// Make the client workspace "Linked Tasks" actionable. These are org-wide tasks
// whose free-text `business` names this client; we allow quick inline edits
// (status / priority / progress) and mirror each change into task_history so the
// edit is consistent with the rest of the command-driven task system.

// ---------------------------------------------------------------------------
// Per-client Blockers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Per-client Meetings & MOMs (Minutes of Meeting)
// ---------------------------------------------------------------------------
function buildClientMeetingPayloadFromBody(body) {
  const meetingType = ["sync_call", "internal", "review", "adhoc"].includes(
    body.meeting_type,
  )
    ? body.meeting_type
    : "sync_call";
  const durationRaw = Number(body.duration_min);
  const durationMin =
    Number.isFinite(durationRaw) && durationRaw > 0
      ? Math.round(durationRaw)
      : null;
  return {
    title: String(body.title || "").trim() || null,
    meeting_date: body.meeting_date || null,
    meeting_type: meetingType,
    duration_min: durationMin,
    participants: String(body.participants || "").trim() || null,
    summary: String(body.summary || "").trim() || null,
    action_items: String(body.action_items || "").trim() || null,
    next_steps: String(body.next_steps || "").trim() || null,
    discussion_points: String(body.discussion_points || "").trim() || null,
    decisions: String(body.decisions || "").trim() || null,
    deliverables: String(body.deliverables || "").trim() || null,
    follow_ups: String(body.follow_ups || "").trim() || null,
  };
}

// Parse raw meeting notes into the structured MOM fields the meeting form
// uses. Extracted from the route handler because the prompt is ~40 lines and
// belongs beside the other AI helpers — the prompt text below is verbatim.
//
// Throws a 500-tagged error when OPENAI_API_KEY is missing so the caller can
// surface that specific message. Every field defaults to "" so a partially
// understood note still fills what it can.
export async function parseMeetingNotesWithAI(notes) {
  if (!openai) {
    const err = new Error(
      "OPENAI_API_KEY is missing. Add it in Railway variables first.",
    );
    err.statusCode = 500;
    throw err;
  }
  const today = new Date().toISOString().slice(0, 10);

  const prompt = `
You are extracting structured meeting log fields from raw meeting notes written by an account/project manager.

Today's date is ${today}. Use it to resolve relative dates like "today" or "yesterday".

Return JSON only with EXACT structure:
{
  "title": "Short meeting title, e.g. 'Weekly sync call with Acme'",
  "meeting_date": "YYYY-MM-DD or empty string if no date is mentioned",
  "meeting_type": "sync_call|review|internal|adhoc",
  "participants": "Names, comma-separated, empty string if none mentioned",
  "summary": "Brief readable summary of the meeting (2-5 sentences)",
  "discussion_points": "Key points discussed, one per line, empty string if none",
  "decisions": "Decisions taken, one per line, empty string if none",
  "deliverables": "Agreed deliverables, one per line, empty string if none",
  "action_items": "Action items as 'Who does what', one per line, empty string if none",
  "follow_ups": "Follow-up items, one per line, empty string if none",
  "next_steps": "Next steps, one per line, empty string if none"
}

RULES:
- Return valid JSON only. No markdown. No explanation.
- Do NOT invent names, dates, decisions, or commitments that are not in the notes.
- Leave a field as an empty string when the notes do not cover it.
- meeting_type: use "review" for review/QBR/performance meetings, "internal" for internal-only team meetings, "adhoc" for one-off/unplanned calls, otherwise "sync_call".
- Keep the original language of the notes for the content fields, but field structure must match exactly.
`;

  const completion = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      { role: "system", content: prompt },
      { role: "user", content: notes },
    ],
    response_format: { type: "json_object" },
  });

  const parsed =
    safeParseJson(completion.choices?.[0]?.message?.content || "{}") || {};

  const asText = (v) =>
    Array.isArray(v)
      ? v
          .map((x) => String(x).trim())
          .filter(Boolean)
          .join("\n")
      : String(v || "").trim();

  const meetingDate = asText(parsed.meeting_date);

  return {
    title: asText(parsed.title),
    meeting_date: /^\d{4}-\d{2}-\d{2}$/.test(meetingDate) ? meetingDate : "",
    meeting_type: ["sync_call", "internal", "review", "adhoc"].includes(
      parsed.meeting_type,
    )
      ? parsed.meeting_type
      : "sync_call",
    participants: asText(parsed.participants),
    summary: asText(parsed.summary),
    discussion_points: asText(parsed.discussion_points),
    decisions: asText(parsed.decisions),
    deliverables: asText(parsed.deliverables),
    action_items: asText(parsed.action_items),
    follow_ups: asText(parsed.follow_ups),
    next_steps: asText(parsed.next_steps),
  };
}

// ---------------------------------------------------------------------------
// Generic helper: soft-archive + light update for simple per-client entities.
// ---------------------------------------------------------------------------
async function loadActiveClientChild(table, orgId, clientId, id) {
  const { data, error } = await supabase
    .from(table)
    .select("*")
    .eq("org_id", orgId)
    .eq("client_id", clientId)
    .eq("id", id)
    .eq("is_active", true)
    .is("deleted_at", null)
    .maybeSingle();
  if (error) throw error;
  return data;
}

// ---------------------------------------------------------------------------
// Per-client Campaigns (Email / Calling / LinkedIn / WhatsApp)
// ---------------------------------------------------------------------------
const CAMPAIGN_TYPES = [
  "email",
  "calling",
  "linkedin",
  "whatsapp",
  "sms",
  "events",
  "ads",
  "content",
  "referral",
  "other",
];
const CAMPAIGN_STATUSES = ["planned", "active", "paused", "completed"];

function buildCampaignPayloadFromBody(body) {
  return {
    name: String(body.name || "").trim() || null,
    campaign_type: CAMPAIGN_TYPES.includes(body.campaign_type)
      ? body.campaign_type
      : "email",
    channel: String(body.channel || "").trim() || null,
    status: CAMPAIGN_STATUSES.includes(body.status) ? body.status : "planned",
    sent_count: Number.isFinite(Number(body.sent_count))
      ? Number(body.sent_count)
      : 0,
    response_count: Number.isFinite(Number(body.response_count))
      ? Number(body.response_count)
      : 0,
    positive_replies: Number.isFinite(Number(body.positive_replies))
      ? Number(body.positive_replies)
      : 0,
    notes: String(body.notes || "").trim() || null,
  };
}

// ---------------------------------------------------------------------------
// Per-client Incentives (attribution / commission / credit log)
// ---------------------------------------------------------------------------
const INCENTIVE_STATUSES = ["pending", "approved", "paid"];

function buildIncentivePayloadFromBody(body) {
  return {
    title: String(body.title || "").trim() || null,
    gtm_user_id: body.gtm_user_id ? Number(body.gtm_user_id) : null,
    related_lead_id: body.related_lead_id ? Number(body.related_lead_id) : null,
    amount: Number.isFinite(Number(body.amount)) ? Number(body.amount) : 0,
    status: INCENTIVE_STATUSES.includes(body.status) ? body.status : "pending",
    notes: String(body.notes || "").trim() || null,
  };
}

// ---------------------------------------------------------------------------
// Per-client Weekly Reports (PM publishes; client-visible)
// ---------------------------------------------------------------------------
function buildWeeklyReportPayloadFromBody(body) {
  return {
    period_label: String(body.period_label || "").trim() || null,
    week_start: body.week_start || null,
    summary: String(body.summary || "").trim() || null,
    highlights: String(body.highlights || "").trim() || null,
    lowlights: String(body.lowlights || "").trim() || null,
    next_week_plan: String(body.next_week_plan || "").trim() || null,
  };
}

// ---------------------------------------------------------------------------
// Team Work (calendar icon in the top nav)
//
// A manually-maintained, date-scoped grid of how many hours each team member
// spent on each project/client for a given day — mirroring the Google Sheet the
// team kept by hand. Backed by sql/2026-06-30-team-work.sql. Every cell edit is
// recorded in team_work_logs so the page can show an activity feed.
// ---------------------------------------------------------------------------

const TEAM_WORK_TEAMS = ["LEADS", "GTM"];

function normalizeTeamWorkTeam(value) {
  const t = String(value || "")
    .trim()
    .toUpperCase();
  return TEAM_WORK_TEAMS.includes(t) ? t : "LEADS";
}

function isMissingTableError(error) {
  if (!error) return false;
  const msg = String(error.message || error.code || "").toLowerCase();
  return (
    error.code === "42P01" ||
    msg.includes("does not exist") ||
    msg.includes("could not find the table") ||
    msg.includes("schema cache")
  );
}

// Loads everything the /team-work page needs for one date. Degrades gracefully
// (tablesMissing=true) when the migration has not been run yet.
async function loadTeamWorkData(orgId, workDate) {
  const result = {
    date: workDate,
    tablesMissing: false,
    columns: [],
    members: [],
    hours: {},
  };

  const { data: columns, error: colErr } = await supabase
    .from("team_work_columns")
    .select("id, label, sort_order")
    .eq("org_id", orgId)
    .eq("is_active", true)
    .order("sort_order", { ascending: true })
    .order("id", { ascending: true });
  if (colErr) {
    if (isMissingTableError(colErr)) {
      result.tablesMissing = true;
      return result;
    }
    throw colErr;
  }
  result.columns = (columns || []).map((c) => ({
    id: c.id,
    label: c.label || "",
  }));

  const { data: members, error: memErr } = await supabase
    .from("team_work_members")
    .select("id, name, team, responsibility, sort_order")
    .eq("org_id", orgId)
    .eq("is_active", true)
    .order("team", { ascending: true })
    .order("sort_order", { ascending: true })
    .order("id", { ascending: true });
  if (memErr) {
    if (isMissingTableError(memErr)) {
      result.tablesMissing = true;
      return result;
    }
    throw memErr;
  }
  result.members = (members || []).map((m) => ({
    id: m.id,
    name: m.name || "",
    team: normalizeTeamWorkTeam(m.team),
    responsibility: m.responsibility || "",
  }));

  const { data: hours, error: hoursErr } = await supabase
    .from("team_work_hours")
    .select("member_id, column_id, hours")
    .eq("org_id", orgId)
    .eq("work_date", workDate);
  if (hoursErr) {
    if (isMissingTableError(hoursErr)) {
      result.tablesMissing = true;
      return result;
    }
    throw hoursErr;
  }
  for (const h of hours || []) {
    result.hours[`${h.member_id}:${h.column_id}`] = Number(h.hours) || 0;
  }

  return result;
}

async function getRecentTeamWorkLogs(orgId, limit = 40) {
  const { data, error } = await supabase
    .from("team_work_logs")
    .select("*")
    .eq("org_id", orgId)
    .order("created_at", { ascending: false })
    .limit(limit);
  if (error) {
    if (isMissingTableError(error)) return [];
    console.error("getRecentTeamWorkLogs error:", error.message);
    return [];
  }
  return data || [];
}

async function insertTeamWorkLog(entry) {
  try {
    const { error } = await supabase.from("team_work_logs").insert([entry]);
    if (error && !isMissingTableError(error)) {
      console.error("insertTeamWorkLog error:", error.message);
    }
  } catch (e) {
    console.error("insertTeamWorkLog threw:", e.message);
  }
}

// Update a member's editable fields (currently the free-text notes shown in the
// name hover-card, plus optional name/team). Used by the name hover-card.

// The WhatsApp bot webhook — ~1200 lines of command handling below.
//
// Takes a REQUEST-SHAPED OBJECT ({ body, ip, signature, url }) rather than an
// Express request. Every internal reference is `req.body.*` or `req.ip`, so
// keeping that shape means the command logic is untouched by the move off
// Express.
//
// Returns a TwiML XML string (from sendTwiml) or { status, text } for the
// signature rejection; the route handler maps both to a Response.
export async function handleWhatsAppWebhook(req) {
  // Vestigial: sendTwiml ignores its first argument now, but the ~34 call
  // sites below still pass it.
  const res = null;
  let messageSid = null;

  try {
    if (
      !validateTwilioRequest({
        signature: req.signature,
        url: req.url,
        body: req.body,
      })
    ) {
      console.warn("Rejected request due to invalid Twilio signature.");
      return { status: 403, text: "Invalid Twilio signature" };
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
}

// Served entirely by Next.js. This module is now a LIBRARY of shared helpers
// imported by app/api/**/route.js and the RSC pages — it no longer defines a
// server, and the Express adapter it used to depend on has been deleted.
// (was: served by Next.js instead of app.listen(); the route registry
// built above is consumed by the generated App Router handlers via dispatch().

// ---------------------------------------------------------------------------
// Exports consumed by the React/App Router tree.
//
// Pages under app/ are React Server Components: they call these loaders
// directly rather than fetching their own HTTP endpoints, so a page render is
// one process-local call instead of a round trip that would have to re-present
// the session cookie to itself.
//
// Everything here already existed and is unchanged — this block only widens
// visibility. As each render*Page function is replaced by a page.jsx it gets
// deleted from this file, but the loaders below stay: they are the data layer.
// ---------------------------------------------------------------------------
export {
  // Clients + process config
  supabase,
  openai,
  DASHBOARD_ORG_ID,
  APP_TIMEZONE,

  // Theme + CSS builders (used once by scripts/gen-css.mjs to emit globals.css)
  UI_THEME,
  buildThemeCss,
  buildBasePageCss,
  buildTopNavCss,
  buildSweetAlertCss,
  buildAutoReportCss,
  CLIENT_REPORT_METRICS,

  // Domain constants that drive dropdowns, filters and badges
  LEAD_BUSINESSES,
  CLIENT_LEADS_TABLE,
  INLINE_CLIENT_LEADS_BUSINESSES,
  CLIENT_LEAD_PIPELINE_STAGES,
  CLIENT_LEAD_CATEGORY_TYPES,
  CLIENT_LEAD_CATEGORY_TYPE_LABELS,
  getClientLeadCategoryTypes,
  getClientLeadCategoryTypeLabels,
  DEFAULT_CLIENT_LEAD_STAGE,
  CLIENT_LEAD_OUTREACH_STATUSES,
  CLIENT_LEAD_DEMO_STATUSES,
  REACH_VIA_CHANNELS,
  CLIENT_REPORT_MAX_WEEKS,
  RASSET_INDUSTRY_OPTIONS,
  RASSET_CAPABILITY_OPTIONS,
  STAGE0_BUG_COLUMNS,
  STAGE0_BUG_SEVERITIES,
  STAGE0_BUG_STATUSES,
  ACCOUNT_FIELD_OPTIONS,

  // Page data loaders
  getDashboardData,
  getDashboardSummaryData,
  getTasksPageData,
  getUserTaskWorkspaceData,
  getTaskDetailData,
  renderUserWorkspaceHistoryLine,
  getAttendancePageData,
  getAttendanceInsightsData,
  getEmployeeAttendanceOverview,
  getLeadsOverviewData,
  getBusinessLeadsData,
  getStage0BugBoardData,
  getLogsPageData,
  loadTeamWorkData,
  getRecentTeamWorkLogs,
  getDailyNarrativeReport,
  getMultiDayNarrativeReport,
  getClientGoals,
  getLatestClientReportSummaries,
  getWorkProfilesByUser,
  getUserWorkProfile,
  getLeadAIIntelligenceHistory,
  getLatestLeadAIIntelligenceRun,
  buildLeadIntelligenceMetrics,
  getAllActiveUsersInOrg,
  getPlannedOffRowsForDate,
  getAttendanceDayUtcRange,
  insertClientActivityLog,
  loadActiveClientChild,
  buildCampaignPayloadFromBody,
  buildClientMeetingPayloadFromBody,
  buildIncentivePayloadFromBody,
  buildWeeklyReportPayloadFromBody,
  generateClientViewToken,
  normalizeText,
  insertTaskHistory,
  resolveClientLeadBusiness,
  createBusinessLead,
  CLIENT_REPORT_SUMMARY_PERIODS,
  mondayStartOfUtcMs,
  runClientReportSummary,
  appendLeadNote,
  updateBusinessLead,
  getBusinessLeadById,
  tableHasClientLeadColumns,
  getTaskById,
  getTaskOwnerNames,
  insertTeamWorkLog,
  normalizeTeamWorkTeam,
  addDaysToDateString,
  getMissingReportDatesForUserInRange,
  isValidStage0BugColumn,
  getBusinessLeadTableName,
  normalizeLeadPhone,
  getLeadPhoneKey,
  deleteBusinessLead,
  updateBusinessLeadStatus,
  approveLeadVoiceUpload,
  ensureArray,
  uploadLeadNoteAudio,
  uploadLeadCallAudio,
  importRassetLeadsFromExcel,
  importClientLeadsFromExcel,
  enrichLeadFromUrl,
  generateLeadAIIntelligence,
  generateCumulativeLeadAIIntelligence,
  transcribeLeadVoiceUploadById,
  updateLeadVoiceTranscript,
  transcribeAudioBuffer,
  HINGLISH_TRANSCRIPTION_PROMPT,
  rejectLeadVoiceUpload,
  runDailyClientReportSummaries,
  isValidStage0BugSeverity,
  isValidStage0BugStatus,

  // Formatting + auth helpers shared with the React tree
  isManagerOrAdmin,
  getPostLoginRedirectPath,
  normalizePhoneForLogin,
  formatDateTime,
  formatDateTimeNoTz,
  formatDateOnly,
  formatTimeOnly,
  formatDurationMinutes,
  formatDateListForHumans,
  formatShortDate,
  badgeClass,
  bugSeverityBadgeClass,
  bugStatusBadgeClass,
  bugSeveritySortWeight,
  getActiveLeadBusinesses,
  getBusinessConfig,
  getBusinessCanonicalName,
  getAttendanceMonthNavigation,
  getTodayDateStringInTimeZone,
  getAttendanceDayDateStringFromDate,
  getReportDateString,
  normalizeClientGoalsData,
  parseUserIdList,
  INCENTIVE_STATUSES,
  CAMPAIGN_STATUSES,
  CAMPAIGN_TYPES,
  getClientLeadStatusHistory,
  getClientLeadCategoryTypeCounts,
  resolveLeadSource,
  clientWeeklyReportNumbering,
  clientLatestWeekDisplayNum,
  renderSummaryWithGoals,
  buildClientAutoReportSections,
  clientLeadStatusLabel,
  getDateStringInTimeZone,
  parseLeadNotesHistory,
  normalizeSlug,
};
