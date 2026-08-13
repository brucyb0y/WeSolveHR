"use client";

// Meetings & MOMs tab — replaces meetingsTabHtml and archiveMeeting.
//
// "MOM" (minutes of meeting) counts as filled when ANY of the six minute fields
// has content — summary, discussion points, decisions, deliverables, action
// items, follow-ups or next steps. That drives both the per-row Done/Pending
// badge and the "MOM pending" KPI, so it is computed once here.
//
// PRESERVED DEFECT: the fourth column header reads "Status" but its cells show
// the meeting TYPE (Sync Call / Review / Internal / Ad-hoc); the fifth reads
// "MOM" and holds the Done/Pending badge. The header labels are off by one
// relative to what the columns contain. Kept as-is — renaming them is a visible
// content change, not a conversion.

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";

const TYPE_LABELS = {
  sync_call: "Sync Call",
  internal: "Internal",
  review: "Review",
  adhoc: "Ad-hoc",
};

const typeLabel = (t) => TYPE_LABELS[t] || "Sync Call";

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

const clip = (text, limit) => {
  const value = String(text || "").trim();
  return value.length <= limit ? value : value.slice(0, limit) + "…";
};

export default function MeetingsTab({
  clientId,
  meetings,
  meetingsThisWeek,
  syncCompliant,
  nextMeetingDate,
  onAdd,
  onEdit,
}) {
  const router = useRouter();
  const [busyId, setBusyId] = useState(null);

  const momPendingCount = meetings.filter((m) => !momFilled(m)).length;

  async function archive(id) {
    if (!confirm("Archive this meeting?")) return;

    setBusyId(id);
    try {
      const res = await fetch(`/api/clients/${clientId}/meetings/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ archive: true }),
      });
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to archive meeting");
        return;
      }

      router.refresh();
    } catch {
      alert("Failed to archive meeting");
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div className={styles.panel}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          gap: 12,
          alignItems: "center",
          marginBottom: 14,
        }}
      >
        <div>
          <h2 style={{ margin: 0 }}>Meetings &amp; MOMs</h2>
          <div className={styles.sectionSubtitle}>
            Call log, minutes of meeting, and sync-call compliance
          </div>
        </div>
        <button
          className={`${styles.btn} ${styles.btnPrimary}`}
          type="button"
          onClick={onAdd}
        >
          + Log Meeting
        </button>
      </div>

      <div className={styles.kpiGrid}>
        <div className={styles.kpiCard}>
          <div className={styles.kpiLabel}>Total meetings</div>
          <div className={styles.kpiValue}>{meetings.length}</div>
        </div>
        <div className={styles.kpiCard}>
          <div className={styles.kpiLabel}>This week</div>
          <div className={styles.kpiValue}>{meetingsThisWeek}</div>
        </div>
        <div className={styles.kpiCard}>
          <div className={styles.kpiLabel}>Sync compliance</div>
          <div className={styles.kpiValue} style={{ marginTop: 6 }}>
            <span
              className={`${styles.badge} ${
                syncCompliant ? styles.badgeOk : styles.badgeWarn
              }`}
            >
              {syncCompliant ? "On track" : "Overdue"}
            </span>
          </div>
        </div>
        <div className={styles.kpiCard}>
          <div className={styles.kpiLabel}>MOM pending</div>
          <div className={styles.kpiValue}>{momPendingCount}</div>
        </div>
        <div className={styles.kpiCard}>
          <div className={styles.kpiLabel}>Next meeting</div>
          <div className={styles.kpiValue}>{nextMeetingDate || "—"}</div>
        </div>
      </div>

      <div style={{ overflowX: "auto" }}>
        <table
          className={styles.workTable}
          style={{ width: "100%", borderCollapse: "collapse" }}
        >
          <thead>
            <tr>
              <th style={{ textAlign: "left" }}>Date</th>
              <th style={{ textAlign: "left" }}>Participants</th>
              <th style={{ textAlign: "left" }}>Summary</th>
              <th style={{ textAlign: "left" }}>Status</th>
              <th style={{ textAlign: "left" }}>MOM</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {meetings.length ? (
              meetings.map((m) => {
                const filled = momFilled(m);
                return (
                  <tr
                    key={m.id}
                    style={{ cursor: "pointer" }}
                    onClick={() => onEdit(m.id)}
                  >
                    <td>
                      <div style={{ fontWeight: 800 }}>
                        {m.meeting_date || "No date"}
                      </div>
                      <div className={styles.meta}>{m.title || "Meeting"}</div>
                    </td>
                    <td>
                      {m.participants ? (
                        clip(m.participants, 60)
                      ) : (
                        <span className={styles.meta}>—</span>
                      )}
                    </td>
                    <td>
                      {m.summary ? (
                        clip(m.summary, 90)
                      ) : (
                        <span className={styles.meta}>—</span>
                      )}
                    </td>
                    <td>
                      <span className={`${styles.badge} ${styles.badgeInfo}`}>
                        {typeLabel(m.meeting_type)}
                      </span>
                    </td>
                    <td>
                      <span
                        className={`${styles.badge} ${
                          filled ? styles.badgeOk : styles.badgeWarn
                        }`}
                      >
                        {filled ? "Done" : "Pending"}
                      </span>
                    </td>
                    {/* Row click opens the editor; the buttons must not also
                        trigger it. */}
                    <td onClick={(e) => e.stopPropagation()}>
                      <button
                        className={styles.btn}
                        type="button"
                        onClick={() => onEdit(m.id)}
                      >
                        Edit
                      </button>
                      <button
                        className={styles.btn}
                        type="button"
                        disabled={busyId === m.id}
                        onClick={() => archive(m.id)}
                      >
                        Archive
                      </button>
                    </td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td colSpan={6} className={styles.meta}>
                  No meetings logged yet. Record the first client meeting or
                  sync call.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
