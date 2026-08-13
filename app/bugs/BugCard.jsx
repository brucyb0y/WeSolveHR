"use client";

// One bug card. The four selects PATCH /api/bugs/:id and then refresh, which is
// what the old inline updateBug() did with location.reload(). router.refresh()
// re-renders the server component in place instead of reloading the document,
// so scroll position and the rest of the board survive the update.
//
// Badge class names arrive precomputed from the server (bugSeverityBadgeClass /
// bugStatusBadgeClass) so this component does not need to import anything from
// lib/server/app.js, which is server-only.

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import styles from "./bugs.module.css";

export default function BugCard({ bug, users, columns, severities, statuses }) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [saving, setSaving] = useState(false);

  async function updateBug(patch) {
    setSaving(true);
    try {
      const res = await fetch(`/api/bugs/${bug.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(patch),
      });

      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to update bug");
        return;
      }

      startTransition(() => router.refresh());
    } catch {
      alert("Failed to update bug");
    } finally {
      setSaving(false);
    }
  }

  const busy = saving || isPending;

  const hasSource =
    bug.source_message_sid || bug.source_phone_number || bug.source_message_text;

  return (
    <div className={styles.bugCard} data-id={bug.id}>
      <div className={styles.bugTop}>
        <div className={styles.bugId}>#{bug.id}</div>
        <div className={styles.bugBadges}>
          <span className={bug.severityBadgeClass}>{bug.severity}</span>
          <span className={bug.statusBadgeClass}>{bug.status}</span>
        </div>
      </div>

      <div className={styles.bugTitle}>{bug.title}</div>

      {bug.description ? (
        <div className={styles.bugDesc}>{bug.description}</div>
      ) : null}

      <div className={styles.bugMeta}>
        <div>
          <strong>Assignee:</strong> {bug.assigned_to_name || "-"}
        </div>
        <div>
          <strong>Created by:</strong> {bug.created_by_name || "-"}
        </div>
        <div>
          <strong>Created:</strong> {bug.created_at_text || "-"}
        </div>
      </div>

      {hasSource ? (
        <div className={styles.bugSource}>
          {bug.source_message_sid ? (
            <div>
              <strong>SID:</strong> {bug.source_message_sid}
            </div>
          ) : null}
          {bug.source_phone_number ? (
            <div>
              <strong>Phone:</strong> {bug.source_phone_number}
            </div>
          ) : null}
          {bug.source_message_text ? (
            <div>
              <strong>Message:</strong> {bug.source_message_text}
            </div>
          ) : null}
        </div>
      ) : null}

      <div className={styles.bugActions}>
        <select
          className={styles.cardSelect}
          value={bug.board_column ?? ""}
          disabled={busy}
          onChange={(e) => updateBug({ board_column: e.target.value })}
        >
          {columns.map((col) => (
            <option value={col} key={col}>
              {col}
            </option>
          ))}
        </select>

        <select
          className={styles.cardSelect}
          value={bug.severity ?? ""}
          disabled={busy}
          onChange={(e) => updateBug({ severity: e.target.value })}
        >
          {severities.map((sev) => (
            <option value={sev} key={sev}>
              {sev}
            </option>
          ))}
        </select>

        <select
          className={styles.cardSelect}
          value={bug.status ?? ""}
          disabled={busy}
          onChange={(e) => updateBug({ status: e.target.value })}
        >
          {statuses.map((st) => (
            <option value={st} key={st}>
              {st}
            </option>
          ))}
        </select>

        <select
          className={styles.cardSelect}
          value={bug.assigned_to_user_id ?? ""}
          disabled={busy}
          onChange={(e) =>
            updateBug({ assigned_to_user_id: e.target.value || null })
          }
        >
          <option value="">Unassigned</option>
          {users.map((u) => (
            <option value={u.id} key={u.id}>
              {u.name}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}
