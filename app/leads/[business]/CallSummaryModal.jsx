"use client";

// Call summaries for one phone number.
//
// Ported from openCallSummaryModal(). Two structural improvements fall out of
// using React rather than innerHTML:
//   - the delete button no longer round-trips its arguments through
//     data-call-id / data-business / data-phone attributes and a
//     handleDeleteCallSummaryClick() dispatcher; it closes over the values;
//   - the conversation table and the plain-transcript fallback are the same
//     branch as before, but the transcript is escaped by React rather than by a
//     hand-rolled escapeHtmlClient().
//
// Deleting refetches the list rather than reloading the page, matching the
// original (it re-invoked openCallSummaryModal on success).

import { useCallback, useEffect, useState } from "react";
import styles from "./leads.module.css";

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

export default function CallSummaryModal({ business, phone, onClose }) {
  const [state, setState] = useState({ status: "loading", rows: [], error: "" });

  const load = useCallback(async () => {
    setState({ status: "loading", rows: [], error: "" });
    try {
      const res = await fetch(
        `/api/business-leads/${encodeURIComponent(business)}/call-summaries?phone=${encodeURIComponent(phone)}`,
      );
      const json = await res.json();

      if (!json.ok) {
        setState({
          status: "error",
          rows: [],
          error: json.error || "Failed to load call summaries",
        });
        return;
      }
      setState({ status: "ready", rows: json.data || [], error: "" });
    } catch {
      setState({
        status: "error",
        rows: [],
        error: "Failed to load call summaries",
      });
    }
  }, [business, phone]);

  useEffect(() => {
    load();
  }, [load]);

  async function deleteCallSummary(id) {
    if (!confirm("Delete this call summary?")) return;

    try {
      const res = await fetch(`/api/lead-voice-uploads/${id}`, {
        method: "DELETE",
      });
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
      className={`${styles.modal} ${styles.open}`}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className={styles.modalCard}>
        <div className={styles.modalHead}>
          <div className={styles.modalTitle}>Call Summaries · {phone}</div>
          <button className={styles.btn} type="button" onClick={onClose}>
            Close
          </button>
        </div>

        {state.status === "loading" ? (
          <div className="muted">Loading call summaries...</div>
        ) : state.status === "error" ? (
          <div className="muted">{state.error}</div>
        ) : !state.rows.length ? (
          <div className="muted">
            No call summaries found for this phone number yet.
          </div>
        ) : (
          state.rows.map((item) => {
            const summary =
              item.translated_text ||
              item.cleaned_transcript ||
              item.raw_transcript ||
              "No transcript available yet.";

            const conversationRows = Array.isArray(item.conversation_rows)
              ? item.conversation_rows
              : [];

            return (
              <div className={styles.callSummaryCard} key={item.id}>
                {item.spoke_to_name ? (
                  <div className={styles.spokeToLine}>
                    <strong>Spoke to:</strong> {item.spoke_to_name}
                  </div>
                ) : null}

                <div className={styles.callSummaryHead}>
                  <div>
                    <div className={styles.callSummaryId}>Call #{item.id}</div>
                    <div className={`muted ${styles.callSummaryMeta}`}>
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

                  <div className={styles.callSummaryActions}>
                    <audio
                      controls
                      preload="none"
                      className={styles.callSummaryAudio}
                    >
                      <source src={`/api/lead-voice-uploads/${item.id}/audio`} />
                      Your browser does not support audio playback.
                    </audio>

                    <button
                      className={`${styles.btn} ${styles.btnDanger}`}
                      type="button"
                      onClick={() => deleteCallSummary(item.id)}
                    >
                      Delete
                    </button>
                  </div>
                </div>

                {conversationRows.length ? (
                  <table className={styles.conversationTable}>
                    <thead>
                      <tr>
                        <th>Speaker</th>
                        <th>What was said</th>
                      </tr>
                    </thead>
                    <tbody>
                      {conversationRows.map((row, i) => (
                        <tr key={i}>
                          <td className={styles.conversationSpeaker}>
                            {row.speaker || "Unknown"}
                          </td>
                          <td className={styles.conversationBody}>
                            {row.text || ""}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <div className={styles.transcriptBlock}>{summary}</div>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
