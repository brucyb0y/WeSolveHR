"use client";

// Call summaries modal. Ported from openCallSummaryModal()/deleteCallSummary() in
// renderBusinessLeadsPage(). Fetches /api/business-leads/:business/call-summaries
// (dispatch shim) for a phone number and lists audio + transcript/conversation,
// with per-call delete. formatHumanDateTime is ported verbatim (LA timezone).

import { useCallback, useEffect, useState } from "react";

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

export default function CallSummaryModal({ open, business, phone, onClose }) {
  const [status, setStatus] = useState("loading"); // loading | ready | error
  const [rows, setRows] = useState([]);
  const [errorMsg, setErrorMsg] = useState("");

  const load = useCallback(async () => {
    if (!open || !phone) return;
    setStatus("loading");
    try {
      const res = await fetch(
        `/api/business-leads/${encodeURIComponent(business)}/call-summaries?phone=${encodeURIComponent(phone)}`,
      );
      const json = await res.json();
      if (!json.ok) {
        setErrorMsg(json.error || "Failed to load call summaries");
        setStatus("error");
        return;
      }
      setRows(json.data || []);
      setStatus("ready");
    } catch {
      setErrorMsg("Failed to load call summaries");
      setStatus("error");
    }
  }, [open, business, phone]);

  useEffect(() => {
    load();
  }, [load]);

  async function deleteCall(id) {
    if (!confirm("Delete this call summary?")) return;
    try {
      const res = await fetch(`/api/lead-voice-uploads/${id}`, { method: "DELETE" });
      const json = await res.json();
      if (!json.ok) {
        alert(json.error || "Failed to delete call summary");
        return;
      }
      load();
    } catch {
      alert("Failed to delete call summary");
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
          <div style={{ fontSize: 22, fontWeight: 900 }}>
            Call Summaries{phone ? ` · ${phone}` : ""}
          </div>
          <button className="btn" type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="muted">
          {status === "loading" ? (
            "Loading call summaries..."
          ) : status === "error" ? (
            errorMsg
          ) : rows.length === 0 ? (
            "No call summaries found for this phone number yet."
          ) : (
            rows.map((item) => {
              const summary =
                item.translated_text ||
                item.cleaned_transcript ||
                item.raw_transcript ||
                "No transcript available yet.";
              const conversationRows = Array.isArray(item.conversation_rows)
                ? item.conversation_rows
                : [];
              return (
                <div className="call-summary-card" key={item.id}>
                  {item.spoke_to_name ? (
                    <div style={{ marginBottom: 8 }}>
                      <strong>Spoke to:</strong> {item.spoke_to_name}
                    </div>
                  ) : null}

                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      gap: 12,
                      alignItems: "flex-start",
                    }}
                  >
                    <div>
                      <div style={{ fontWeight: 900 }}>Call #{item.id}</div>
                      <div className="muted" style={{ lineHeight: 1.6, marginTop: 4 }}>
                        Created: {formatHumanDateTime(item.created_at)}
                        <br />
                        Uploaded by: {item.sender_phone || "-"}
                        <br />
                        Verified by: {item.verified_by || "Not verified"}
                        <br />
                        Verified at: {formatHumanDateTime(item.verified_at)}
                        <br />
                        Status: {item.status || "-"}
                      </div>
                    </div>

                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                      <audio
                        controls
                        preload="none"
                        style={{ maxWidth: 260, height: 36 }}
                      >
                        <source src={`/api/lead-voice-uploads/${Number(item.id)}/audio`} />
                        Your browser does not support audio playback.
                      </audio>
                      <button
                        className="btn btn-danger"
                        type="button"
                        onClick={() => deleteCall(Number(item.id))}
                      >
                        Delete
                      </button>
                    </div>
                  </div>

                  {conversationRows.length ? (
                    <table style={{ width: "100%", borderCollapse: "collapse", marginTop: 12 }}>
                      <thead>
                        <tr>
                          <th style={{ textAlign: "left", padding: 8 }}>Speaker</th>
                          <th style={{ textAlign: "left", padding: 8 }}>What was said</th>
                        </tr>
                      </thead>
                      <tbody>
                        {conversationRows.map((row, i) => (
                          <tr key={i}>
                            <td style={{ width: 150, fontWeight: 900, padding: 8, verticalAlign: "top" }}>
                              {row.speaker || "Unknown"}
                            </td>
                            <td style={{ whiteSpace: "pre-wrap", padding: 8, verticalAlign: "top", lineHeight: 1.55 }}>
                              {row.text || ""}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : (
                    <div style={{ whiteSpace: "pre-wrap", marginTop: 12, lineHeight: 1.55 }}>
                      {summary}
                    </div>
                  )}
                </div>
              );
            })
          )}
        </div>
      </div>
    </div>
  );
}
