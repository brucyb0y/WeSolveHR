"use client";

// Voice Inbox tab: one card per voice upload, with bulk delete, a per-card
// kebab menu, and an expandable transcript.
//
// The status-dependent menu items are preserved exactly: Save / Approve /
// Reject / Delete Transcript only for pending_review, and "Edit & Reopen" only
// for rejected.
//
// The original's renderConversationRows() is NOT ported — it was defined but
// never called (dead code), which is why .conversationRow / .speakerPill sit
// unused in the stylesheet.

import { useEffect, useState } from "react";
import styles from "./leads.module.css";

const MENU_WIDTH = 180;

export default function VoiceInbox({ rows }) {
  const [openMenuId, setOpenMenuId] = useState(null);
  const [menuPos, setMenuPos] = useState({ top: 0, left: 0 });
  const [selected, setSelected] = useState([]);
  const [transcripts, setTranscripts] = useState(() =>
    Object.fromEntries(rows.map((r) => [r.id, r.translated_text || ""])),
  );

  useEffect(() => {
    if (openMenuId === null) return undefined;
    const close = () => setOpenMenuId(null);
    document.addEventListener("click", close);
    return () => document.removeEventListener("click", close);
  }, [openMenuId]);

  function toggleMenu(event, leadId) {
    event.preventDefault();
    event.stopPropagation();
    const rect = event.currentTarget.getBoundingClientRect();
    setMenuPos({
      top: rect.bottom + 6,
      left: Math.max(12, rect.right - MENU_WIDTH),
    });
    setOpenMenuId((current) => (current === leadId ? null : leadId));
  }

  function toggleSelected(id, checked) {
    setSelected((s) => (checked ? [...s, id] : s.filter((x) => x !== id)));
  }

  async function call(url, options, failMessage, confirmMessage) {
    if (confirmMessage && !confirm(confirmMessage)) return;
    try {
      const res = await fetch(url, options);
      const json = await res.json();
      if (!json.ok) {
        alert(json.error || failMessage);
        return;
      }
      window.location.reload();
    } catch {
      alert(failMessage);
    }
  }

  const jsonPost = (body) => ({
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const saveTranscript = (id) =>
    call(
      `/api/lead-voice-uploads/${id}/transcription`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ translated_text: transcripts[id] ?? "" }),
      },
      "Failed to save transcript",
    );

  const approveLead = (id) =>
    call(`/api/leads/${id}/approve`, jsonPost({}), "Failed to approve lead");

  const rejectLead = (id) =>
    call(`/api/leads/${id}/reject`, jsonPost({}), "Failed to reject lead");

  const deleteTranscript = (id) =>
    call(
      `/api/lead-voice-uploads/${id}/transcription`,
      { method: "DELETE" },
      "Failed to delete transcript",
      "Delete this transcript?",
    );

  const deleteVoice = (id) =>
    call(
      `/api/lead-voice-uploads/${id}`,
      { method: "DELETE" },
      "Failed to delete voice upload",
      "Delete this voice message? This cannot be undone.",
    );

  const deleteSelected = () => {
    if (!selected.length) {
      alert("Select at least one voice message first.");
      return;
    }
    call(
      "/api/lead-voice-uploads/bulk-delete",
      jsonPost({ ids: selected }),
      "Failed to delete selected voice messages",
      `Delete ${selected.length} selected voice message(s)? This cannot be undone.`,
    );
  };

  if (!rows.length) {
    return <div className={styles.panel}>No voice leads need review.</div>;
  }

  return (
    <>
      <div className={styles.bulkBar}>
        <button
          className={`${styles.btn} ${styles.btnDanger}`}
          type="button"
          onClick={deleteSelected}
        >
          Delete Selected Voice Messages
        </button>
      </div>

      {rows.map((lead) => {
        const preview = (lead.translated_text || "").slice(0, 160);
        const isPending = lead.status === "pending_review";
        const isRejected = lead.status === "rejected";

        return (
          <div
            className={`${styles.leadCard} ${styles.compactVoiceCard}`}
            key={lead.id}
          >
            <div className={styles.voiceHeader}>
              <div className={styles.voiceMain}>
                <div className={styles.voiceTitleRow}>
                  <label
                    className={styles.voiceCheck}
                    title="Select for bulk delete"
                  >
                    <input
                      type="checkbox"
                      checked={selected.includes(lead.id)}
                      onChange={(e) => toggleSelected(lead.id, e.target.checked)}
                    />
                  </label>

                  <div>
                    <div className={styles.voiceTitle}>
                      Voice Lead #{lead.id}
                    </div>
                    <div className={styles.voiceMeta}>
                      {lead.created_at_text} · {lead.lead_phone} ·{" "}
                      {lead.sender_phone}
                    </div>
                  </div>
                </div>
              </div>

              <div className={styles.voiceSide}>
                <span className={lead.statusBadgeClass}>{lead.status}</span>

                <button
                  className={styles.kebabBtn}
                  type="button"
                  onClick={(e) => toggleMenu(e, lead.id)}
                >
                  ⋯
                </button>

                <div
                  className={`${styles.leadActionsMenu} ${
                    openMenuId === lead.id ? styles.open : ""
                  }`}
                  style={
                    openMenuId === lead.id
                      ? { top: `${menuPos.top}px`, left: `${menuPos.left}px` }
                      : undefined
                  }
                >
                  <button type="button" onClick={() => deleteVoice(lead.id)}>
                    Delete Voice
                  </button>

                  {isPending ? (
                    <>
                      <button
                        type="button"
                        onClick={() => saveTranscript(lead.id)}
                      >
                        Save
                      </button>
                      <button
                        type="button"
                        onClick={() => approveLead(lead.id)}
                      >
                        Approve
                      </button>
                      <button type="button" onClick={() => rejectLead(lead.id)}>
                        Reject
                      </button>
                      <button
                        type="button"
                        onClick={() => deleteTranscript(lead.id)}
                      >
                        Delete Transcript
                      </button>
                    </>
                  ) : null}

                  {isRejected ? (
                    <button
                      type="button"
                      onClick={() => saveTranscript(lead.id)}
                    >
                      Edit &amp; Reopen
                    </button>
                  ) : null}
                </div>
              </div>
            </div>

            <div className={styles.voiceAudio}>
              <audio controls preload="none">
                <source
                  src={`/api/lead-voice-uploads/${lead.id}/audio`}
                  type={lead.media_content_type || "audio/mpeg"}
                />
              </audio>
            </div>

            <details className={styles.voiceTranscript}>
              <summary>
                <span>🗣 Transcript</span>
                <span className={styles.transcriptPreview}>
                  {preview}
                  {(lead.translated_text || "").length > 160 ? "..." : ""}
                </span>
              </summary>

              <textarea
                className={styles.transcriptTextarea}
                value={transcripts[lead.id] ?? ""}
                onChange={(e) =>
                  setTranscripts((s) => ({ ...s, [lead.id]: e.target.value }))
                }
              />
            </details>
          </div>
        );
      })}
    </>
  );
}
