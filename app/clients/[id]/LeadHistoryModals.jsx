"use client";

// Read-only history dialogs for a lead: status changes and notes.
//
// The two get their data differently, and that is deliberate:
//   * Status history is already on the page. page.jsx ships a per-lead trail
//     with labels, actor names and timestamps resolved server-side, so opening
//     this costs no request — the row already shows a preview of the same rows.
//   * Notes history is FETCHED on open. Notes can be long and carry audio URLs;
//     embedding every note of every lead in the initial payload would bloat the
//     page for a panel most rows never open.

import { useEffect, useState } from "react";
import { WorkModal } from "./WorkModal";
import styles from "./workspace.module.css";

function Entry({ children }) {
  return (
    <div
      style={{
        padding: "8px 10px",
        border: "1px solid var(--line)",
        borderRadius: 8,
      }}
    >
      {children}
    </div>
  );
}

export function LeadStatusHistoryModal({ rows, onClose }) {
  return (
    <WorkModal title="Status History" onClose={onClose} readOnly>
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: 8,
          gridColumn: "1 / -1",
        }}
      >
        {rows.length ? (
          rows.map((h, i) => (
            <Entry key={i}>
              <div style={{ fontWeight: 700, color: "var(--text, inherit)" }}>
                {/* The first recorded change has no "from" — showing "→ X"
                    with an empty left side would read as a lost value. */}
                {h.from ? `${h.from} → ${h.to}` : h.to}
              </div>
              <div
                className={styles.meta}
                style={{ fontSize: 11, marginTop: 4 }}
              >
                {h.by}
                {h.at ? ` · ${h.at}` : ""}
              </div>
            </Entry>
          ))
        ) : (
          <div className={styles.meta}>No status changes recorded yet.</div>
        )}
      </div>
    </WorkModal>
  );
}

export function LeadNotesHistoryModal({ clientId, leadId, onClose }) {
  const [notes, setNotes] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;

    (async () => {
      try {
        const res = await fetch(`/api/clients/${clientId}/leads/${leadId}`);
        const json = await res.json();
        if (cancelled) return;

        if (!json.ok) {
          setError(json.error || "Failed to load notes");
          return;
        }
        setNotes(parseLeadNotes((json.data || {}).notes));
      } catch {
        if (!cancelled) setError("Failed to load notes");
      }
    })();

    // The dialog can be closed while the request is still in flight; without
    // this the response would set state on an unmounted component.
    return () => {
      cancelled = true;
    };
  }, [clientId, leadId]);

  return (
    <WorkModal title="Notes History" onClose={onClose} readOnly>
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: 8,
          gridColumn: "1 / -1",
        }}
      >
        {error ? (
          <div className={styles.meta}>{error}</div>
        ) : notes === null ? (
          <div className={styles.meta}>Loading notes…</div>
        ) : notes.length ? (
          // Newest first for readability — the stored order is oldest first.
          notes
            .slice()
            .reverse()
            .map((n, i) => {
              const byline = [n.by || "", n.atText || ""]
                .filter(Boolean)
                .join(" · ");
              return (
                <Entry key={i}>
                  <div
                    style={{
                      whiteSpace: "pre-wrap",
                      color: "var(--text, inherit)",
                    }}
                  >
                    {n.text}
                  </div>
                  {n.audio_url ? (
                    <audio
                      controls
                      preload="none"
                      style={{
                        marginTop: 6,
                        width: "100%",
                        maxWidth: 340,
                        height: 34,
                      }}
                      src={n.audio_url}
                    />
                  ) : null}
                  {byline ? (
                    <div
                      className={styles.meta}
                      style={{ fontSize: 11, marginTop: 4 }}
                    >
                      {byline}
                    </div>
                  ) : null}
                </Entry>
              );
            })
        ) : (
          <div className={styles.meta}>No notes yet.</div>
        )}
      </div>
    </WorkModal>
  );
}

// Notes are stored as a JSON array, but older rows are plain text. Both shapes
// have to keep working.
//
// JSON.parse is only attempted when the trimmed string STARTS WITH "[" — that
// guard is load-bearing, not a shortcut. A plain-text note like "123" or
// "{done}" parses as valid JSON but is not an array, so parsing everything
// would discard those notes entirely instead of showing them as text.
//
// Entries without a `text` are dropped, matching the original's filter.
function parseLeadNotes(raw) {
  const normalize = (arr) =>
    arr
      .filter((n) => n && typeof n === "object" && n.text != null)
      .map((n) => ({
        text: n.text || "",
        by: n.by || "",
        audio_url: n.audio_url || "",
        atText: n.at ? formatNoteTime(n.at) : "",
      }));

  if (Array.isArray(raw)) return normalize(raw);
  if (typeof raw !== "string") return [];

  const t = raw.trim();
  if (!t) return [];

  if (t.charAt(0) === "[") {
    try {
      const arr = JSON.parse(t);
      if (Array.isArray(arr)) return normalize(arr);
    } catch {
      // Falls through to the plain-text reading below.
    }
  }

  return [{ text: t, by: "", audio_url: "", atText: "" }];
}

// Matches the original's en-IN / Asia/Kolkata formatting — the app's timezone,
// not the viewer's.
function formatNoteTime(at) {
  try {
    return new Date(at).toLocaleString("en-IN", {
      timeZone: "Asia/Kolkata",
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    });
  } catch {
    return "";
  }
}
