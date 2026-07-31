// Domain constants and pure lookup helpers, extracted verbatim from the
// original Express monolith (lib/server/app.js lines 27-198).

import { escapeHtml } from "../ui/html.js";

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
  { key: "website_form", label: "Website Form", column: "reached_via_website_form" },
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

function renderMultiSelectOptions(options) {
  return options
    .map((x) => `<option value="${escapeHtml(x)}">${escapeHtml(x)}</option>`)
    .join("");
}

function getActiveLeadBusinesses() {
  return LEAD_BUSINESSES.filter((x) => x.active);
}

function getBusinessConfig(business) {
  const key = String(business || "")
    .trim()
    .toLowerCase();
  return getActiveLeadBusinesses().find((x) => x.business === key) || null;
}


export {
  DASHBOARD_ORG_ID,
  LEAD_BUSINESSES,
  CLIENT_LEADS_TABLE,
  INLINE_CLIENT_LEADS_BUSINESSES,
  CLIENT_LEAD_PIPELINE_STAGES,
  CLIENT_LEAD_CATEGORY_TYPES,
  CLIENT_LEAD_CATEGORY_TYPE_LABELS,
  DEFAULT_CLIENT_LEAD_STAGE,
  CLIENT_LEAD_OUTREACH_STATUSES,
  CLIENT_LEAD_DEMO_STATUSES,
  REACH_VIA_CHANNELS,
  clientLeadStatusLabel,
  RASSET_INDUSTRY_OPTIONS,
  RASSET_CAPABILITY_OPTIONS,
  renderMultiSelectOptions,
  getActiveLeadBusinesses,
  getBusinessConfig,
};
