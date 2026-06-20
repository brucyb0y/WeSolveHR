"use client";

// "Save L2 Data" + calls modal. Ported from openLeadCallsModal()/saveLeadL2Data()/
// loadLeadCalls() in renderBusinessLeadsPage(). On open it loads the lead to
// prefill the L2 form and the lead's calls. Note: the /l2 and /calls endpoints
// return { success, ... } (not the { ok, data } shape) — preserved exactly.

import { useCallback, useEffect, useState } from "react";
import {
  RASSET_INDUSTRY_OPTIONS,
  RASSET_CAPABILITY_OPTIONS,
  L2_BEHAVIOR_OPTIONS,
  L2_CALL_OUTCOME_OPTIONS,
  parseMultiValue,
} from "./leadOptions.js";

const EMPTY_L2 = {
  spokeToName: "",
  designation: "",
  industry: [],
  capability: [],
  behavior: "",
  callOutcome: "",
  notes: "",
};

export default function LeadCallsModal({ open, business, leadId, onClose }) {
  const [l2, setL2] = useState(EMPTY_L2);
  const [callsStatus, setCallsStatus] = useState("loading"); // loading | ready | error | empty
  const [calls, setCalls] = useState([]);
  const [callsError, setCallsError] = useState("");

  const setField = (name) => (e) => setL2((s) => ({ ...s, [name]: e.target.value }));
  const onMulti = (name) => (e) =>
    setL2((s) => ({
      ...s,
      [name]: Array.from(e.target.selectedOptions).map((o) => o.value),
    }));

  const loadCalls = useCallback(async () => {
    setCallsStatus("loading");
    try {
      const res = await fetch(
        `/api/leads/${encodeURIComponent(business)}/${encodeURIComponent(leadId)}/calls`,
      );
      const json = await res.json();
      if (!json.success) {
        setCallsError(json.error || "Failed to load calls");
        setCallsStatus("error");
        return;
      }
      if (!json.calls || !json.calls.length) {
        setCallsStatus("empty");
        return;
      }
      setCalls(json.calls);
      setCallsStatus("ready");
    } catch {
      setCallsError("Failed to load calls");
      setCallsStatus("error");
    }
  }, [business, leadId]);

  useEffect(() => {
    if (!open || !leadId) return;
    setL2(EMPTY_L2);
    let alive = true;
    fetch(`/api/business-leads/${business}/${leadId}`)
      .then((r) => r.json())
      .then((json) => {
        if (!alive) return;
        if (json.ok && json.data) {
          const lead = json.data;
          setL2({
            spokeToName: lead.contact_name || lead.spoke_to_name || "",
            designation: lead.contact_designation || lead.designation || "",
            industry: parseMultiValue(lead.industry || lead.industry_primary || ""),
            capability: parseMultiValue(
              lead.manufacturing_capabilities || lead.capability || "",
            ),
            behavior: lead.behavior || "",
            callOutcome: lead.last_call_outcome || lead.call_outcome || "",
            notes: lead.notes || "",
          });
        }
      })
      .catch(() => {});
    loadCalls();
    return () => {
      alive = false;
    };
  }, [open, business, leadId, loadCalls]);

  async function save() {
    const payload = {
      spoke_to_name: l2.spokeToName.trim(),
      designation: l2.designation.trim(),
      industry: l2.industry.join(", "),
      capability: l2.capability.join(", "),
      behavior: l2.behavior,
      call_outcome: l2.callOutcome,
      notes: l2.notes.trim(),
    };
    try {
      const res = await fetch(
        `/api/leads/${encodeURIComponent(business)}/${encodeURIComponent(leadId)}/l2`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
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
    }
  }

  return (
    <div
      className={`modal ${open ? "open" : ""}`}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="modal-card">
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: 12,
            alignItems: "center",
            marginBottom: 14,
          }}
        >
          <div style={{ fontSize: 22, fontWeight: 900 }}>Save L2 Data</div>
          <button className="btn" type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="panel">
          <h2 style={{ marginTop: 0 }}>Quick L2 Update</h2>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
            <input
              placeholder="Person spoken to"
              value={l2.spokeToName}
              onChange={setField("spokeToName")}
            />
            <input
              placeholder="Designation"
              value={l2.designation}
              onChange={setField("designation")}
            />
            <select multiple size={6} value={l2.industry} onChange={onMulti("industry")}>
              {RASSET_INDUSTRY_OPTIONS.map((x) => (
                <option value={x} key={x}>
                  {x}
                </option>
              ))}
            </select>
            <select multiple size={6} value={l2.capability} onChange={onMulti("capability")}>
              {RASSET_CAPABILITY_OPTIONS.map((x) => (
                <option value={x} key={x}>
                  {x}
                </option>
              ))}
            </select>
            <select value={l2.behavior} onChange={setField("behavior")}>
              {L2_BEHAVIOR_OPTIONS.map((o) => (
                <option value={o.value} key={o.label}>
                  {o.label}
                </option>
              ))}
            </select>
            <select value={l2.callOutcome} onChange={setField("callOutcome")}>
              {L2_CALL_OUTCOME_OPTIONS.map((o) => (
                <option value={o.value} key={o.label}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>

          <textarea
            placeholder="Short notes"
            value={l2.notes}
            onChange={setField("notes")}
            style={{ marginTop: 10, width: "100%", minHeight: 80 }}
          />

          <button
            className="btn btn-primary"
            type="button"
            onClick={save}
            style={{ marginTop: 12 }}
          >
            Save L2 Data
          </button>
        </div>

        <div className="panel">
          <h2 style={{ marginTop: 0 }}>Call Audio / Transcript / Translation</h2>
          <div className="muted">
            {callsStatus === "loading" ? (
              "Loading calls..."
            ) : callsStatus === "error" ? (
              callsError
            ) : callsStatus === "empty" ? (
              "No calls uploaded yet."
            ) : (
              calls.map((call, i) => (
                <div
                  key={i}
                  style={{
                    border: "1px solid rgba(255,255,255,0.10)",
                    borderRadius: 12,
                    padding: 12,
                    marginBottom: 12,
                  }}
                >
                  <div style={{ fontWeight: 800, marginBottom: 8 }}>
                    {call.created_at || ""}
                  </div>
                  {call.audio_url ? (
                    <audio
                      controls
                      src={call.audio_url}
                      style={{ width: "100%", marginBottom: 10 }}
                    />
                  ) : (
                    <div className="muted">No audio</div>
                  )}
                  <div style={{ marginTop: 10 }}>
                    <strong>Transcript</strong>
                    <div className="muted" style={{ whiteSpace: "pre-wrap", marginTop: 6 }}>
                      {call.transcript || "No transcript yet"}
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
