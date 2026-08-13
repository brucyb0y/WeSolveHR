"use client";

// Updates / Progress Timeline tab.
//
// The timeline merges two sources into one chronological list: manual client
// updates typed by staff, and automatic activity logged when work items change.
// That merge (and its sort) happens in the data loader, so this component takes
// `timelineEvents` already ordered and pre-formatted — the chips still report
// the two source counts separately so it stays clear where entries came from.
//
// Client component only because of the + Add Update button; everything below it
// is static.

import styles from "./workspace.module.css";

export default function UpdatesTab({
  updates,
  activityLogs,
  timelineEvents,
  onAdd,
}) {
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
          <h2 style={{ margin: 0 }}>Updates / Progress Timeline</h2>
          <div className={styles.meta}>
            Manual client updates + automatic work-item activity.
          </div>
        </div>
        <button
          className={`${styles.btn} ${styles.btnPrimary}`}
          type="button"
          onClick={onAdd}
        >
          + Add Update
        </button>
      </div>

      <div className={styles.workSummaryChips}>
        <span className={styles.summaryChip}>
          Manual Updates {updates.length}
        </span>
        <span className={styles.summaryChip}>
          Activity Logs {activityLogs.length}
        </span>
        <span className={styles.summaryChip}>
          Timeline {timelineEvents.length}
        </span>
      </div>

      <div style={{ marginTop: 16 }}>
        {timelineEvents.length ? (
          timelineEvents.map((event, i) => (
            <div className={styles.item} key={`${event.type}-${event.id ?? i}`}>
              <div className={styles.itemTitle}>
                {event.title}
                {event.relatedWorkItemTitle
                  ? ` · ${event.relatedWorkItemTitle}`
                  : ""}
              </div>
              <div className={styles.meta}>{event.text}</div>
              <div className={styles.meta}>
                {event.atText}
                {" · by "}
                {event.by || "-"}
                {" · "}
                {event.type === "manual_update"
                  ? "Manual update"
                  : "System activity"}
              </div>
            </div>
          ))
        ) : (
          <div className={styles.meta}>No updates or activity yet.</div>
        )}
      </div>
    </div>
  );
}
