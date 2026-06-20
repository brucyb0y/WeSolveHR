"use client";

// Business leads console. Ported from renderBusinessLeadsPage() in
// lib/server/app.js. The lead table, voice inbox, filters, tabs, pagination, and
// Excel imports render from the server-provided `data`; the three modals (lead
// form, call summaries, L2/calls) are child islands. Every mutation reloads the
// page exactly as the original did, so the server-rendered table stays the source
// of truth (no risky optimistic updates).
//
// NOTE: the lead row has 8 cells under a 7-column header — a pre-existing
// mismatch in the original markup, preserved here verbatim rather than "fixed".

import { useEffect, useRef, useState } from "react";
import { formatDateTime } from "@/lib/utils/datetime.js";
import {
  RASSET_INDUSTRY_OPTIONS,
  RASSET_CAPABILITY_OPTIONS,
  ENTITY_TYPES,
  RASSET_FILTER_STATUSES,
} from "./leadOptions.js";
import LeadFormModal from "./LeadFormModal.jsx";
import CallSummaryModal from "./CallSummaryModal.jsx";
import LeadCallsModal from "./LeadCallsModal.jsx";

// Ported from badgeClass() in lib/server/app.js. These class names are not styled
// on this page (the original defined no .badge CSS), matching the original output.
function badgeClass(value) {
  const v = String(value || "").trim().toLowerCase();
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

const TABS = [
  { key: "all", label: "All Leads" },
  { key: "b2b", label: "B2B" },
  { key: "b2c", label: "B2C" },
  { key: "in_progress", label: "In Progress" },
  { key: "completed", label: "Completed" },
  { key: "voice_inbox", label: "Voice Inbox" },
];

export default function BusinessLeadsConsole({ data }) {
  const business = data.business;
  const rows = data.rows || [];
  const selectedTab = data.selectedTab || "all";
  const counts = data.counts || {};
  const search = data.search || "";
  const pagination = data.pagination || {};
  const filters = data.filters || {};
  const embed = !!data.embed;

  const [leadModal, setLeadModal] = useState({ open: false, editId: null });
  const [callSummary, setCallSummary] = useState({ open: false, phone: "" });
  const [leadCalls, setLeadCalls] = useState({ open: false, leadId: null });
  const [menu, setMenu] = useState(null); // { kind, row, top, left }
  const [rassetUploadOpen, setRassetUploadOpen] = useState(false);
  const [joolianUploadOpen, setJoolianUploadOpen] = useState(false);
  const [selectedVoice, setSelectedVoice] = useState(() => new Set());

  const rassetFileRef = useRef(null);
  const joolianFileRef = useRef(null);
  // Per-voice transcript textareas (id -> text), seeded from the rows.
  const transcriptsRef = useRef({});

  useEffect(() => {
    const close = () => setMenu(null);
    const onKey = (e) => {
      if (e.key === "Escape") {
        setMenu(null);
        setLeadModal({ open: false, editId: null });
      }
    };
    document.addEventListener("click", close);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("click", close);
      document.removeEventListener("keydown", onKey);
    };
  }, []);

  function openMenu(event, kind, row) {
    event.preventDefault();
    event.stopPropagation();
    const rect = event.currentTarget.getBoundingClientRect();
    setMenu((prev) =>
      prev && prev.kind === kind && prev.row.id === row.id
        ? null
        : { kind, row, top: rect.bottom + 6, left: Math.max(12, rect.right - 180) },
    );
  }

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
    has_call_transcription: filters.has_call_transcription || "",
  }).toString();

  async function toggleLeadCheckbox(event, id, field, value) {
    event.stopPropagation();
    const checkbox = event.target;
    try {
      const res = await fetch(`/api/business-leads/${business}/${id}/quick-toggle`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ field, value }),
      });
      const json = await res.json();
      if (!json.ok) {
        alert(json.error || "Failed to update checkbox");
        checkbox.checked = !value;
      }
    } catch {
      alert("Failed to update checkbox");
      checkbox.checked = !value;
    }
  }

  async function deleteBusinessLead(id) {
    if (!confirm("Delete this lead and all related voice/call data? This cannot be undone.")) return;
    const res = await fetch(`/api/business-leads/${business}/${id}`, {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
    });
    const json = await res.json();
    if (!json.ok) {
      alert(json.error || "Failed to delete lead");
      return;
    }
    window.location.reload();
  }

  async function updateBusinessLeadStatus(id, status) {
    const res = await fetch(`/api/business-leads/${business}/${id}/status`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status }),
    });
    const json = await res.json();
    if (!json.ok) {
      alert(json.error || "Failed to update status");
      return;
    }
    window.location.reload();
  }

  async function uploadExcel(ref, url, onDone) {
    const input = ref.current;
    if (!input || !input.files || !input.files[0]) {
      alert("Choose an Excel file first.");
      return;
    }
    const formData = new FormData();
    formData.append("file", input.files[0]);
    const res = await fetch(url, { method: "POST", body: formData });
    const json = await res.json();
    if (!json.ok) {
      alert("Excel import failed: " + (json.error || JSON.stringify(json)));
      console.error("Excel import failed:", json);
      return;
    }
    onDone(json.data || {});
  }

  function uploadRassetExcel() {
    uploadExcel(rassetFileRef, "/api/rasset-leads/import-excel", (d) => {
      alert(
        [
          "Import complete",
          "Import ID: " + d.import_id,
          "Total: " + d.total,
          "Inserted: " + d.inserted,
          "Duplicates skipped: " + d.duplicates,
          "Skipped: " + d.skipped,
          "Errors: " + (d.errors || []).length,
        ].join("\n"),
      );
      window.location.href = "/leads/rasset/imports?import_id=" + d.import_id;
    });
  }

  function uploadJoolianB2BExcel() {
    uploadExcel(joolianFileRef, "/api/joolian-leads/import-b2b-excel", (d) => {
      alert(
        "Joolian B2B import complete. Total: " +
          d.total +
          ", Inserted: " +
          d.inserted +
          ", Updated: " +
          d.updated +
          ", Skipped: " +
          d.skipped +
          ", Errors: " +
          (d.errors || []).length,
      );
      window.location.reload();
    });
  }

  // ---- Voice inbox actions ----
  async function saveTranscript(id) {
    const translated = transcriptsRef.current[id] ?? "";
    if (!translated.trim()) {
      alert("English translation is required.");
      return;
    }
    const res = await fetch(`/api/leads/${id}/transcript`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        cleaned_transcript: translated,
        translated_text: translated,
        review_notes: "",
      }),
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
    const res = await fetch(`/api/leads/${id}/approve`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });
    const json = await res.json();
    if (!json.ok) {
      alert(json.error || "Failed to approve lead");
      return;
    }
    alert("Lead approved. It will now show under All Leads.");
    window.location.href = "/leads/" + business + "?tab=all";
  }

  async function rejectLead(id) {
    const reason = prompt("Reason for rejection?", "");
    const res = await fetch(`/api/leads/${id}/reject`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason: reason || "" }),
    });
    const json = await res.json();
    if (!json.ok) {
      alert(json.error || "Failed to reject lead");
      return;
    }
    alert("Lead rejected.");
    window.location.reload();
  }

  async function deleteVoiceTranscript(id) {
    if (!confirm("Delete this transcription only? The audio will remain and you can transcribe again.")) return;
    const res = await fetch(`/api/lead-voice-uploads/${id}/transcription`, { method: "DELETE" });
    const json = await res.json();
    if (!json.ok) {
      alert(json.error || "Failed to delete transcription");
      return;
    }
    alert("Transcription deleted.");
    window.location.reload();
  }

  async function deleteVoiceUpload(id) {
    if (!confirm("Delete this voice lead completely? This removes audio link, transcript, notes, and review data.")) return;
    const res = await fetch(`/api/lead-voice-uploads/${id}`, { method: "DELETE" });
    const json = await res.json();
    if (!json.ok) {
      alert(json.error || "Failed to delete voice lead");
      return;
    }
    alert("Voice lead deleted.");
    window.location.reload();
  }

  async function deleteSelectedVoiceUploads() {
    const ids = Array.from(selectedVoice);
    if (!ids.length) {
      alert("Select at least one voice message.");
      return;
    }
    if (!confirm("Delete " + ids.length + " selected voice message(s)? This cannot be undone.")) return;
    const res = await fetch("/api/lead-voice-uploads/bulk-delete", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ids }),
    });
    const json = await res.json();
    if (!json.ok) {
      alert(json.error || "Failed to delete selected voice messages");
      return;
    }
    window.location.reload();
  }

  function toggleVoiceSelected(id, checked) {
    setSelectedVoice((prev) => {
      const next = new Set(prev);
      if (checked) next.add(id);
      else next.delete(id);
      return next;
    });
  }

  // Action handlers fired from the floating menu.
  function runMenuAction(fn) {
    setMenu(null);
    fn();
  }

  const isVoiceTab = selectedTab === "voice_inbox";

  return (
    <>
      <div className="wrap">
        {embed ? null : (
          <div className="topbar">
            <div>
              <div className="eyebrow">Business Lead CRM</div>
              <h1>{business} Leads</h1>
              <div className="subtitle">
                All leads, B2B/B2C split, manual onboarding, search, and voice
                inbox.
              </div>
            </div>
            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
              <a className="btn" href="/leads">
                ← Leads Overview
              </a>
              <a className="btn" href={`/leads/${encodeURIComponent(business)}/intelligence`}>
                Intelligence
              </a>
              <button
                className="btn btn-primary"
                type="button"
                onClick={() => setLeadModal({ open: true, editId: null })}
              >
                + Add Lead
              </button>
            </div>
          </div>
        )}

        {embed ? null : (
          <div className="stats">
            <div className="stat-card">
              <div className="stat-label">All Leads</div>
              <div className="stat-value">{counts.all || 0}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">B2B</div>
              <div className="stat-value">{counts.b2b || 0}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">B2C</div>
              <div className="stat-value">{counts.b2c || 0}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Voice Inbox</div>
              <div className="stat-value">{counts.voice_inbox || 0}</div>
            </div>
          </div>
        )}

        <div className="tabs">
          {TABS.map((t) => (
            <a
              key={t.key}
              className={`tab ${selectedTab === t.key ? "active" : ""}`}
              href={`/leads/${encodeURIComponent(business)}?tab=${t.key}&search=${encodeURIComponent(search)}`}
            >
              {t.label} ({counts[t.key] || 0})
            </a>
          ))}
        </div>

        {!isVoiceTab ? (
          <>
            <div className="panel">
              <form method="GET" action={`/leads/${encodeURIComponent(business)}`}>
                <input type="hidden" name="tab" value={selectedTab} />
                {business === "rasset" ? (
                  <div className="advanced-filter-grid">
                    <input
                      name="search"
                      defaultValue={search}
                      placeholder="Search company, phone, city, CNC, laser, owner, notes..."
                    />
                    <select name="industry" defaultValue={filters.industry || ""}>
                      <option value="">All Industries</option>
                      {RASSET_INDUSTRY_OPTIONS.map((x) => (
                        <option value={x} key={x}>
                          {x}
                        </option>
                      ))}
                    </select>
                    <select name="capability" defaultValue={filters.capability || ""}>
                      <option value="">All Capabilities</option>
                      {RASSET_CAPABILITY_OPTIONS.map((x) => (
                        <option value={x} key={x}>
                          {x}
                        </option>
                      ))}
                    </select>
                    <select name="entity_type" defaultValue={filters.entity_type || ""}>
                      <option value="">All Entity Types</option>
                      {ENTITY_TYPES.map((x) => (
                        <option value={x} key={x}>
                          {x}
                        </option>
                      ))}
                    </select>
                    <select name="status" defaultValue={filters.status || ""}>
                      <option value="">All Status</option>
                      {RASSET_FILTER_STATUSES.map((x) => (
                        <option value={x} key={x}>
                          {x}
                        </option>
                      ))}
                    </select>
                    <input name="city" defaultValue={filters.city || ""} placeholder="City" />
                    <input name="state" defaultValue={filters.state || ""} placeholder="State" />
                    <input
                      name="assigned_to"
                      defaultValue={filters.assigned_to || ""}
                      placeholder="Assigned to"
                    />
                    <select name="qualified" defaultValue={filters.qualified || ""}>
                      <option value="">Qualified?</option>
                      <option value="yes">Qualified</option>
                      <option value="no">Not Qualified</option>
                    </select>
                    <select name="worth_talking" defaultValue={filters.worth_talking || ""}>
                      <option value="">Worth Talking?</option>
                      <option value="yes">Worth Talking</option>
                      <option value="no">Not Worth Talking</option>
                    </select>
                    <select
                      name="has_call_transcription"
                      defaultValue={filters.has_call_transcription || ""}
                    >
                      <option value="">Call Transcription?</option>
                      <option value="yes">Has transcription</option>
                      <option value="no">No transcription</option>
                    </select>
                  </div>
                ) : (
                  <div className="search-row">
                    <input
                      name="search"
                      defaultValue={search}
                      placeholder="Search phone, business, contact, city, notes..."
                    />
                    <button className="btn btn-primary" type="submit">
                      Search
                    </button>
                    <a
                      className="btn"
                      href={`/leads/${encodeURIComponent(business)}?tab=${selectedTab}`}
                    >
                      Clear
                    </a>
                  </div>
                )}

                {business === "rasset" ? (
                  <div style={{ display: "flex", gap: 10, marginTop: 12, flexWrap: "wrap" }}>
                    <button className="btn btn-primary" type="submit">
                      Search / Filter
                    </button>
                    <a
                      className="btn"
                      href={`/leads/${encodeURIComponent(business)}?tab=${selectedTab}`}
                    >
                      Clear
                    </a>
                  </div>
                ) : null}
              </form>
            </div>

            {business === "rasset" ? (
              <div style={{ marginBottom: 12 }}>
                <button
                  className="btn btn-primary"
                  type="button"
                  onClick={() => setRassetUploadOpen((o) => !o)}
                  title="Upload Excel with Company, Email, Phone, Industry, Location, etc."
                >
                  ＋ Import Rasset Excel
                </button>
                <a className="btn" href="/leads/rasset/imports">
                  Import Logs
                </a>
                {rassetUploadOpen ? (
                  <div style={{ marginTop: 10 }}>
                    <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                      <input ref={rassetFileRef} type="file" accept=".xlsx,.xls,.csv" />
                      <button className="btn btn-primary" type="button" onClick={uploadRassetExcel}>
                        Upload
                      </button>
                    </div>
                    <div className="muted" style={{ marginTop: 6, fontSize: 12 }}>
                      Supports: Company, Website, Email, Industry, City, Phone, Owner,
                      Employees, Size, Country
                    </div>
                  </div>
                ) : null}
              </div>
            ) : null}

            {business === "joolian" ? (
              <div style={{ marginBottom: 12 }}>
                <button
                  className="btn btn-primary"
                  type="button"
                  onClick={() => setJoolianUploadOpen((o) => !o)}
                  title="Upload Excel with AP details, category, pricing, etc."
                >
                  ＋ Import Joolian Excel
                </button>
                {joolianUploadOpen ? (
                  <div style={{ marginTop: 10 }}>
                    <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                      <input ref={joolianFileRef} type="file" accept=".xlsx,.xls,.csv" />
                      <button className="btn btn-primary" type="button" onClick={uploadJoolianB2BExcel}>
                        Upload
                      </button>
                    </div>
                    <div className="muted" style={{ marginTop: 6, fontSize: 12 }}>
                      Supports: AP Name, Phone, Email, City, Category, Pricing, Owner,
                      etc.
                    </div>
                  </div>
                ) : null}
              </div>
            ) : null}

            <div className="panel">
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
                <tbody>
                  {rows.length ? (
                    rows.map((lead) => {
                      const caps = lead.manufacturing_capabilities
                        ? String(lead.manufacturing_capabilities)
                            .split(",")
                            .filter(Boolean)
                            .slice(0, 4)
                        : [];
                      return (
                        <tr key={lead.id}>
                          <td className="lead-name-cell">
                            <div className="lead-company-name">
                              {lead.company ||
                                lead.business_name ||
                                lead.company_name ||
                                "Lead #" + lead.id}
                              {lead.factory_setup === "multiple_sites" ? (
                                <span className="mini-chip">Multi-site</span>
                              ) : null}
                            </div>
                            <div className="muted lead-contact-line">
                              {[lead.contact_name || lead.owner_name, lead.phone]
                                .filter(Boolean)
                                .join(" · ") || "-"}
                            </div>
                            {lead.last_spoke_to_name ? (
                              <div style={{ fontSize: 12, marginTop: 4 }}>
                                <strong>Spoke to:</strong> {lead.last_spoke_to_name}
                              </div>
                            ) : null}
                          </td>

                          <td>
                            <div className="lead-chip-row">
                              {caps.length ? (
                                caps.map((x, i) => (
                                  <span className="lead-chip" key={i}>
                                    {x.trim()}
                                  </span>
                                ))
                              ) : (
                                <span className="muted">No capabilities</span>
                              )}
                            </div>
                          </td>

                          <td>
                            <div>
                              <strong>
                                {lead.industry_primary || lead.industry || "-"}
                              </strong>
                            </div>
                            <div className="muted">{lead.raw_industry || ""}</div>
                            <div className="lead-chip-row">
                              {lead.entity_type ? (
                                <span className="lead-chip">{lead.entity_type}</span>
                              ) : null}
                              {lead.company_size ? (
                                <span className="lead-chip">{lead.company_size}</span>
                              ) : null}
                              {lead.assigned_to ? (
                                <span className="lead-chip">{lead.assigned_to}</span>
                              ) : null}
                            </div>
                          </td>

                          <td>
                            <div>
                              {[lead.city, lead.state, lead.country]
                                .filter(Boolean)
                                .join(", ") || "-"}
                            </div>
                            <div className="muted">{lead.pin_code || lead.location || ""}</div>
                          </td>

                          <td style={{ textAlign: "left", padding: "10px 12px", minWidth: 135 }}>
                            <div style={{ display: "flex", flexDirection: "column", gap: 7, alignItems: "flex-start" }}>
                              <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer", whiteSpace: "nowrap" }}>
                                <input
                                  type="checkbox"
                                  style={{ margin: 0, width: "auto" }}
                                  defaultChecked={!!lead.l2_done}
                                  onChange={(e) =>
                                    toggleLeadCheckbox(e, lead.id, "l2_done", e.target.checked)
                                  }
                                />
                                <span>L2 Done</span>
                              </label>
                            </div>
                          </td>

                          <td>
                            {business === "joolian" ? (
                              <>
                                <div>{lead.activity_category || lead.industry || "-"}</div>
                                <div className="muted">{lead.sub_activity_category || ""}</div>
                                <div className="muted">Ages: {lead.age_group || "-"}</div>
                                <div className="muted">
                                  Type: {lead.type_of_business || lead.company_size || "-"}
                                </div>
                                <div className="muted">Price: {lead.pricing_approx || "-"}</div>
                              </>
                            ) : (
                              <>
                                <div>{lead.industry || "-"}</div>
                                <div className="muted">Emp: {lead.number_of_employees || "-"}</div>
                                <div className="muted">Machines: {lead.machine_count || "-"}</div>
                              </>
                            )}
                          </td>

                          <td>
                            <button
                              className="btn"
                              type="button"
                              onClick={() =>
                                setCallSummary({ open: true, phone: lead.phone || "" })
                              }
                            >
                              Calls
                            </button>
                          </td>

                          <td className="actions-cell">
                            <button
                              className="kebab-btn"
                              type="button"
                              onClick={(e) => openMenu(e, "lead", lead)}
                            >
                              ...
                            </button>
                          </td>
                        </tr>
                      );
                    })
                  ) : (
                    <tr>
                      <td colSpan={7} className="empty-cell">
                        No leads found.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>

              <div className="pagination">
                {pagination.hasPrev ? (
                  <a
                    className="btn"
                    href={`/leads/${encodeURIComponent(business)}?${filterQuery}&page=${Number(pagination.page) - 1}`}
                  >
                    ← Previous
                  </a>
                ) : null}
                <span className="btn">Page {pagination.page || 1}</span>
                {pagination.hasNext ? (
                  <a
                    className="btn"
                    href={`/leads/${encodeURIComponent(business)}?${filterQuery}&page=${Number(pagination.page) + 1}`}
                  >
                    Next →
                  </a>
                ) : null}
              </div>
            </div>
          </>
        ) : (
          <div className="lead-list">
            {rows.length ? (
              <>
                <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 12 }}>
                  <button className="btn btn-danger" type="button" onClick={deleteSelectedVoiceUploads}>
                    Delete Selected Voice Messages
                  </button>
                </div>
                {rows.map((lead) => (
                  <div className="lead-card compact-voice-card" key={lead.id}>
                    <div className="voice-header">
                      <div className="voice-main">
                        <div className="voice-title-row">
                          <label className="voice-check" title="Select for bulk delete">
                            <input
                              type="checkbox"
                              className="voice-delete-checkbox"
                              checked={selectedVoice.has(Number(lead.id))}
                              onChange={(e) =>
                                toggleVoiceSelected(Number(lead.id), e.target.checked)
                              }
                            />
                          </label>
                          <div>
                            <div className="voice-title">Voice Lead #{lead.id}</div>
                            <div className="voice-meta">
                              {formatDateTime(lead.created_at)} · {lead.lead_phone} ·{" "}
                              {lead.sender_phone}
                            </div>
                          </div>
                        </div>
                      </div>
                      <div className="voice-side">
                        <span className={badgeClass(lead.status)}>{lead.status}</span>
                        <button
                          className="kebab-btn"
                          type="button"
                          onClick={(e) => openMenu(e, "voice", lead)}
                        >
                          ⋯
                        </button>
                      </div>
                    </div>

                    <div className="voice-audio">
                      <audio controls preload="none">
                        <source
                          src={`/api/lead-voice-uploads/${Number(lead.id)}/audio`}
                          type={lead.media_content_type || "audio/mpeg"}
                        />
                      </audio>
                    </div>

                    <details className="voice-transcript">
                      <summary>
                        <span>🗣 Transcript</span>
                        <span className="transcript-preview">
                          {(lead.translated_text || "").slice(0, 160)}
                          {(lead.translated_text || "").length > 160 ? "..." : ""}
                        </span>
                      </summary>
                      <textarea
                        className="transcript-textarea"
                        defaultValue={lead.translated_text || ""}
                        onChange={(e) => {
                          transcriptsRef.current[lead.id] = e.target.value;
                        }}
                      />
                    </details>
                  </div>
                ))}
              </>
            ) : (
              <div className="panel">No voice leads need review.</div>
            )}
          </div>
        )}
      </div>

      {/* Floating action menu (one open at a time, fixed-positioned). */}
      {menu ? (
        <div
          className="lead-actions-menu open"
          style={{ top: menu.top, left: menu.left }}
          onClick={(e) => e.stopPropagation()}
        >
          {menu.kind === "lead" ? (
            <>
              <button
                type="button"
                onClick={() =>
                  runMenuAction(() => setLeadModal({ open: true, editId: menu.row.id }))
                }
              >
                Edit
              </button>
              <button
                type="button"
                onClick={() =>
                  runMenuAction(() => setLeadCalls({ open: true, leadId: menu.row.id }))
                }
              >
                Save L2 Data / Calls
              </button>
              <button
                type="button"
                onClick={() => runMenuAction(() => updateBusinessLeadStatus(menu.row.id, "new"))}
              >
                Mark New
              </button>
              <button
                type="button"
                onClick={() =>
                  runMenuAction(() => updateBusinessLeadStatus(menu.row.id, "in_progress"))
                }
              >
                Mark In Progress
              </button>
              <button
                type="button"
                onClick={() =>
                  runMenuAction(() => updateBusinessLeadStatus(menu.row.id, "completed"))
                }
              >
                Mark Completed
              </button>
              <button
                type="button"
                className="danger-menu-item"
                onClick={() => runMenuAction(() => deleteBusinessLead(menu.row.id))}
              >
                Delete
              </button>
            </>
          ) : (
            <>
              <button
                type="button"
                onClick={() => runMenuAction(() => deleteVoiceUpload(menu.row.id))}
              >
                Delete Voice
              </button>
              {menu.row.status === "pending_review" ? (
                <>
                  <button type="button" onClick={() => runMenuAction(() => saveTranscript(menu.row.id))}>
                    Save
                  </button>
                  <button type="button" onClick={() => runMenuAction(() => approveLead(menu.row.id))}>
                    Approve
                  </button>
                  <button type="button" onClick={() => runMenuAction(() => rejectLead(menu.row.id))}>
                    Reject
                  </button>
                  <button
                    type="button"
                    onClick={() => runMenuAction(() => deleteVoiceTranscript(menu.row.id))}
                  >
                    Delete Transcript
                  </button>
                </>
              ) : null}
              {menu.row.status === "rejected" ? (
                <button type="button" onClick={() => runMenuAction(() => saveTranscript(menu.row.id))}>
                  Edit &amp; Reopen
                </button>
              ) : null}
            </>
          )}
        </div>
      ) : null}

      <LeadFormModal
        business={business}
        open={leadModal.open}
        editId={leadModal.editId}
        onClose={() => setLeadModal({ open: false, editId: null })}
      />
      <CallSummaryModal
        open={callSummary.open}
        business={business}
        phone={callSummary.phone}
        onClose={() => setCallSummary({ open: false, phone: "" })}
      />
      <LeadCallsModal
        open={leadCalls.open}
        business={business}
        leadId={leadCalls.leadId}
        onClose={() => setLeadCalls({ open: false, leadId: null })}
      />
    </>
  );
}
