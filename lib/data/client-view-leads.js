// Decorates client-view lead rows with the values the filter/sort UI needs.
//
// The original carried these on the <tr> as data-* attributes (data-stage,
// data-reach, data-notescount, data-search, ...) and the filter engine read
// them back off the DOM. They are computed once here instead, so the client
// component filters plain objects.

import {
  CLIENT_LEAD_PIPELINE_STAGES,
  CLIENT_LEAD_DEMO_STATUSES,
  CLIENT_LEAD_CATEGORY_TYPES,
  REACH_VIA_CHANNELS,
  DEFAULT_CLIENT_LEAD_STAGE,
  clientLeadStatusLabel,
  parseLeadNotesHistory,
  getDateStringInTimeZone,
  APP_TIMEZONE,
} from "@/lib/server/app.js";

const STAGE_INDEX = Object.fromEntries(
  CLIENT_LEAD_PIPELINE_STAGES.map((s, i) => [s.key, i]),
);
const DEMO_INDEX = Object.fromEntries(
  CLIENT_LEAD_DEMO_STATUSES.map((s, i) => [s.key, i]),
);

const lower = (v) => String(v || "").trim().toLowerCase();

// IST calendar date, matching the data-updateddate the original compared on.
const dateOnly = (iso) => {
  if (!iso) return "";
  try {
    return getDateStringInTimeZone(new Date(iso), APP_TIMEZONE);
  } catch {
    return "";
  }
};

export function decorateLeads(leads) {
  return leads.map((l) => {
    const notes = parseLeadNotesHistory(l.notes);
    const noteBy = Array.from(
      new Set(notes.map((n) => lower(n.by)).filter(Boolean)),
    );

    const reach = REACH_VIA_CHANNELS.filter((c) => l[c.column]).map((c) => c.key);

    const name = l.company || l.business_name || l.contact_name || "";
    const location = lower(
      [l.city, l.state, l.country].filter(Boolean).join(" "),
    );

    return {
      id: l.id,
      name,
      contact_name: l.contact_name || "",
      phone: l.phone || "",
      email: l.email || "",
      assigned_to: l.assigned_to || "",
      notesRaw: l.notes || "",
      notesCount: notes.length,
      noteAudio: notes.some((n) => n.audio_url || n.audioUrl),
      noteBy,

      stage: l.pipeline_stage || "",
      stageLabel: clientLeadStatusLabel(
        CLIENT_LEAD_PIPELINE_STAGES,
        l.pipeline_stage || DEFAULT_CLIENT_LEAD_STAGE,
        "Prospect Identified",
      ),
      stageIdx: STAGE_INDEX[l.pipeline_stage] ?? -1,

      demo: l.demo_status || "",
      demoLabel: clientLeadStatusLabel(
        CLIENT_LEAD_DEMO_STATUSES,
        l.demo_status || CLIENT_LEAD_DEMO_STATUSES[0].key,
        "Not Scheduled",
      ),
      demoIdx: DEMO_INDEX[l.demo_status] ?? -1,

      category: l.category_type || "",
      assignee: lower(l.assigned_to),
      location,
      locationLabel:
        [l.city, l.state, l.country].filter(Boolean).join(", ") || "",
      reach,
      hasPhone: !!String(l.phone || "").trim(),
      callback: l.callback_date || "",
      updatedDate: dateOnly(l.updated_at),
      updatedAt: l.updated_at ? new Date(l.updated_at).getTime() : 0,

      search: [
        name,
        l.contact_name,
        l.phone,
        l.email,
        l.city,
        l.state,
        l.country,
        l.assigned_to,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase(),
    };
  });
}

// Option lists for the filter popup, mirroring the internal Leads tab —
// including its "none / never set" entries.
export function buildLeadFilterOptions(decorated) {
  const uniqueSorted = (values) =>
    Array.from(new Set(values.filter(Boolean))).sort((a, b) =>
      a.localeCompare(b),
    );

  const assignees = uniqueSorted(decorated.map((l) => l.assigned_to));
  const noteAuthors = uniqueSorted(
    decorated.flatMap((l) => l.noteBy).map((n) => n),
  );

  return {
    stage: [
      { key: "__none__", label: "None (never set)" },
      ...CLIENT_LEAD_PIPELINE_STAGES,
    ],
    demo: [
      { key: "__none__", label: "None (never set)" },
      ...CLIENT_LEAD_DEMO_STATUSES,
    ],
    category: [
      { key: "__none__", label: "None (no category)" },
      ...CLIENT_LEAD_CATEGORY_TYPES,
    ],
    reach: [
      { key: "__none__", label: "None (not reached)" },
      ...REACH_VIA_CHANNELS.map((c) => ({ key: c.key, label: c.label })),
      { key: "both", label: "LinkedIn + Email" },
    ],
    notes: [
      { key: "none", label: "No notes" },
      { key: "added", label: "Has notes" },
      { key: "multiple", label: "Multiple notes" },
    ],
    audio: [
      { key: "yes", label: "Has audio" },
      { key: "no", label: "No audio" },
    ],
    noteBy: [
      { key: "__none__", label: "No notes" },
      ...noteAuthors.map((n) => ({ key: n, label: n })),
    ],
    assignee: [
      { key: "__unassigned__", label: "Unassigned" },
      ...assignees.map((n) => ({ key: lower(n), label: n })),
    ],
    hasAssignees: assignees.length > 0,
  };
}

// Category pill counts shown above the table.
export function buildCategoryCounts(decorated) {
  const counts = {};
  decorated.forEach((l) => {
    const key = l.category;
    if (key) counts[key] = (counts[key] || 0) + 1;
  });

  return CLIENT_LEAD_CATEGORY_TYPES.filter((c) => counts[c.key]).map((c) => ({
    key: c.key,
    label: c.label,
    count: counts[c.key],
  }));
}

export const DEFAULTS = {
  stage: DEFAULT_CLIENT_LEAD_STAGE,
  demo: CLIENT_LEAD_DEMO_STATUSES[0].key,
};
