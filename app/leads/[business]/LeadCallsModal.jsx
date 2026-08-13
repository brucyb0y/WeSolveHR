"use client";

// "Save L2 Data / Calls" modal: a quick L2 update form plus the list of
// uploaded call audio and transcripts for one lead.
//
// Two API quirks are preserved rather than tidied, because the endpoints
// themselves return these shapes:
//   - GET .../l2 lead details and the calls list answer with `success`, not the
//     `ok` envelope the rest of the app uses;
//   - the industry/capability multiselects round-trip as a comma-joined string
//     (setMultiSelectValues split on [,;\n]; saveLeadL2Data re-joined with ", ").
//
// The original also read the lead through the page-level BUSINESS constant
// rather than the business argument it was handed. They are always equal here,
// so this component just takes the one `business` prop.

import { useCallback, useEffect, useState } from "react";
import styles from "./leads.module.css";

const BEHAVIOURS = [
  ["", "Behavior"],
  ["helpful", "Helpful"],
  ["busy", "Busy"],
  ["not_helpful", "Not helpful"],
  ["rude", "Rude"],
  ["interested", "Interested"],
  ["not_interested", "Not interested"],
];

const OUTCOMES = [
  ["", "Call Outcome"],
  ["connected", "Connected"],
  ["busy", "Busy"],
  ["wrong_number", "Wrong number"],
  ["owner_not_available", "Owner not available"],
  ["callback_requested", "Callback requested"],
  ["not_relevant", "Not relevant"],
];

const EMPTY_FORM = {
  spoke_to_name: "",
  designation: "",
  industry: [],
  capability: [],
  behavior: "",
  call_outcome: "",
  notes: "",
};

// Mirrors setMultiSelectValues(): a stored comma/semicolon/newline separated
// string becomes the set of selected options.
const toSelection = (value) =>
  String(value || "")
    .split(/[,;\n]/)
    .map((x) => x.trim())
    .filter(Boolean);

export default function LeadCallsModal({
  business,
  leadId,
  industryOptions,
  capabilityOptions,
  onClose,
}) {
  const [form, setForm] = useState(EMPTY_FORM);
  const [calls, setCalls] = useState({ status: "loading", items: [], error: "" });
  const [saving, setSaving] = useState(false);

  const set = (key) => (e) => setForm((f) => ({ ...f, [key]: e.target.value }));

  const setMulti = (key) => (e) =>
    setForm((f) => ({
      ...f,
      [key]: Array.from(e.target.selectedOptions).map((o) => o.value),
    }));

  useEffect(() => {
    let cancelled = false;

    (async () => {
      try {
        const res = await fetch(`/api/business-leads/${business}/${leadId}`);
        const json = await res.json();
        if (cancelled || !json.ok || !json.data) return;

        const lead = json.data;
        setForm({
          spoke_to_name: lead.contact_name || lead.spoke_to_name || "",
          designation: lead.contact_designation || lead.designation || "",
          industry: toSelection(lead.industry || lead.industry_primary || ""),
          capability: toSelection(
            lead.manufacturing_capabilities || lead.capability || "",
          ),
          behavior: lead.behavior || "",
          call_outcome: lead.last_call_outcome || lead.call_outcome || "",
          notes: lead.notes || "",
        });
      } catch {
        /* leave the form empty; the calls list reports its own failure */
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [business, leadId]);

  const loadCalls = useCallback(async () => {
    setCalls({ status: "loading", items: [], error: "" });
    try {
      const res = await fetch(
        `/api/leads/${encodeURIComponent(business)}/${encodeURIComponent(leadId)}/calls`,
      );
      const json = await res.json();

      if (!json.success) {
        setCalls({
          status: "error",
          items: [],
          error: json.error || "Failed to load calls",
        });
        return;
      }
      setCalls({ status: "ready", items: json.calls || [], error: "" });
    } catch {
      setCalls({ status: "error", items: [], error: "Failed to load calls" });
    }
  }, [business, leadId]);

  useEffect(() => {
    loadCalls();
  }, [loadCalls]);

  async function saveL2() {
    setSaving(true);
    try {
      const res = await fetch(
        `/api/leads/${encodeURIComponent(business)}/${encodeURIComponent(leadId)}/l2`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            spoke_to_name: form.spoke_to_name.trim(),
            designation: form.designation.trim(),
            industry: form.industry.join(", "),
            capability: form.capability.join(", "),
            behavior: form.behavior,
            call_outcome: form.call_outcome,
            notes: form.notes.trim(),
          }),
        },
      );

      const json = await res.json();

      if (!json.success) {
        alert(json.error || "Failed to save L2 data");
        return;
      }

      alert("L2 data saved");
      window.location.reload();
    } catch {
      alert("Failed to save L2 data");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div
      className={`${styles.modal} ${styles.open}`}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className={styles.modalCard}>
        <div className={styles.modalHead}>
          <div className={styles.modalTitle}>Save L2 Data</div>
          <button className={styles.btn} type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className={styles.panel}>
          <h2 className={styles.panelHeadFlush}>Quick L2 Update</h2>

          <div className={styles.l2Grid}>
            <input
              placeholder="Person spoken to"
              value={form.spoke_to_name}
              onChange={set("spoke_to_name")}
            />
            <input
              placeholder="Designation"
              value={form.designation}
              onChange={set("designation")}
            />

            <select
              multiple
              size={6}
              value={form.industry}
              onChange={setMulti("industry")}
            >
              {industryOptions.map((x) => (
                <option value={x} key={x}>
                  {x}
                </option>
              ))}
            </select>

            <select
              multiple
              size={6}
              value={form.capability}
              onChange={setMulti("capability")}
            >
              {capabilityOptions.map((x) => (
                <option value={x} key={x}>
                  {x}
                </option>
              ))}
            </select>

            <select value={form.behavior} onChange={set("behavior")}>
              {BEHAVIOURS.map(([v, label]) => (
                <option value={v} key={v}>
                  {label}
                </option>
              ))}
            </select>

            <select value={form.call_outcome} onChange={set("call_outcome")}>
              {OUTCOMES.map(([v, label]) => (
                <option value={v} key={v}>
                  {label}
                </option>
              ))}
            </select>
          </div>

          <textarea
            placeholder="Short notes"
            className={styles.l2Notes}
            value={form.notes}
            onChange={set("notes")}
          />

          <button
            className={`${styles.btn} ${styles.btnPrimary} ${styles.l2Save}`}
            type="button"
            onClick={saveL2}
            disabled={saving}
          >
            {saving ? "Saving..." : "Save L2 Data"}
          </button>
        </div>

        <div className={styles.panel}>
          <h2 className={styles.panelHeadFlush}>
            Call Audio / Transcript / Translation
          </h2>

          {calls.status === "loading" ? (
            <div className="muted">Loading calls...</div>
          ) : calls.status === "error" ? (
            <div className="muted">{calls.error}</div>
          ) : !calls.items.length ? (
            <div className="muted">No calls uploaded yet.</div>
          ) : (
            calls.items.map((call, i) => (
              <div className={styles.callCard} key={call.id ?? i}>
                <div className={styles.callCardDate}>{call.created_at || ""}</div>

                {call.audio_url ? (
                  <audio
                    controls
                    src={call.audio_url}
                    className={styles.callAudio}
                  />
                ) : (
                  <div className="muted">No audio</div>
                )}

                <div className={styles.callTranscriptWrap}>
                  <strong>Transcript</strong>
                  <div className={`muted ${styles.callTranscript}`}>
                    {call.transcript || "No transcript yet"}
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
