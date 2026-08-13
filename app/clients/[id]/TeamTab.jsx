"use client";

// Team tab — assigned employees, their task load, and external contributors.
// Internal only; none of this reaches the client dashboard.
//
// Employees and contributors are two different populations, which is why the
// tab has two lists:
//   * teamMembers — WeSolve users associated with the client (account manager,
//     project manager, or owner of at least one work item). Each gets a card
//     with their assigned tasks.
//   * contributors — rows in client_contributors: contractors and client-side
//     people who are not WeSolve users at all.
//
// Employee cards arrive pre-computed from page.jsx (task list, counts, work
// state, next deadline). Doing that server-side keeps the sorting rules — open
// tasks before done, then earliest deadline — in one place next to the loader
// rather than recomputed on every client render.

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";
import { taskStatusBadgeClass, taskStatusLabel, VIS_CHIP } from "./badges";

function ProgressBar({ percent }) {
  return (
    <div className={styles.empProg}>
      <span className={styles.empProgFill} style={{ width: `${percent}%` }} />
    </div>
  );
}

function EmployeeCard({ member }) {
  const {
    name,
    roleLabel,
    workState,
    total,
    inProgressCount,
    doneCount,
    overdueCount,
    nextDeadlineText,
    avgProgress,
    tasks,
  } = member;

  return (
    <div className={`${styles.workCard} ${styles.empCard}`}>
      <div className={styles.workCardTop}>
        <div>
          <div className={styles.workCardTitle}>
            {name || "-"} <span className={VIS_CHIP.internal}>INTERNAL</span>
          </div>
          <div className={styles.meta}>{roleLabel}</div>
        </div>
        <span className={workState.cls}>{workState.label}</span>
      </div>

      <div className={styles.empStatRow}>
        <div className={styles.empStat}>
          <div className={styles.empStatVal}>{total}</div>
          <div className={styles.empStatLabel}>Assigned</div>
        </div>
        <div className={styles.empStat}>
          <div className={styles.empStatVal}>{inProgressCount}</div>
          <div className={styles.empStatLabel}>In Progress</div>
        </div>
        <div className={styles.empStat}>
          <div className={styles.empStatVal}>{doneCount}</div>
          <div className={styles.empStatLabel}>Completed</div>
        </div>
        <div className={styles.empStat}>
          {/* Overdue count turns pink only when there is something overdue. */}
          <div
            className={styles.empStatVal}
            style={overdueCount ? { color: "#ffd7da" } : undefined}
          >
            {overdueCount}
          </div>
          <div className={styles.empStatLabel}>Overdue</div>
        </div>
        <div className={styles.empStat}>
          <div className={styles.empStatVal}>{nextDeadlineText || "—"}</div>
          <div className={styles.empStatLabel}>Next Deadline</div>
        </div>
      </div>

      <div className={styles.empOverall}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            fontSize: 12,
            marginBottom: 6,
          }}
        >
          <span className={styles.meta}>Overall progress</span>
          <strong>{avgProgress}%</strong>
        </div>
        <ProgressBar percent={avgProgress} />
      </div>

      <div style={{ overflowX: "auto", marginTop: 14 }}>
        <table
          className={styles.workTable}
          style={{ width: "100%", borderCollapse: "collapse" }}
        >
          <thead>
            <tr>
              <th style={{ textAlign: "left" }}>Assigned task</th>
              <th style={{ textAlign: "left" }}>Status</th>
              <th style={{ textAlign: "left" }}>Progress</th>
              <th style={{ textAlign: "left" }}>Deadline</th>
            </tr>
          </thead>
          <tbody>
            {tasks.length ? (
              tasks.map((t) => (
                <tr key={t.id}>
                  <td>
                    <div style={{ fontWeight: 700 }}>
                      {t.title || "Untitled"}
                    </div>
                    <div className={styles.meta}>
                      {(t.priority || "medium") + " priority"}
                    </div>
                  </td>
                  <td>
                    <span className={taskStatusBadgeClass(t)}>
                      {taskStatusLabel(t)}
                    </span>
                  </td>
                  <td style={{ minWidth: 150 }}>
                    <ProgressBar percent={t.progressPercent} />
                    <div className={styles.meta} style={{ marginTop: 4 }}>
                      {t.progressPercent}%
                    </div>
                  </td>
                  <td>
                    {t.due_date ? (
                      <span
                        className={
                          t.isOverdue ? styles.overduePill : styles.meta
                        }
                      >
                        {t.dueText}
                        {t.isOverdue ? " · overdue" : ""}
                      </span>
                    ) : (
                      <span className={styles.meta}>No deadline</span>
                    )}
                  </td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={4} className={styles.meta}>
                  No tasks assigned to this employee yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function TeamTab({
  clientId,
  clientName,
  teamMembers,
  contributors,
  onAdd,
  onEdit,
}) {
  const router = useRouter();
  const [busyId, setBusyId] = useState(null);

  async function archiveContributor(id) {
    if (!confirm("Archive this contributor?")) return;

    setBusyId(id);
    try {
      const res = await fetch(
        `/api/clients/${clientId}/contributors/${id}/archive`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to archive contributor");
        return;
      }

      router.refresh();
    } catch {
      alert("Failed to archive contributor");
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
          <h2 style={{ margin: 0 }}>
            Team <span className={VIS_CHIP.internal}>INTERNAL</span>
          </h2>
          <div className={styles.meta}>
            Assigned employees, roles, and open task counts. Internal only.
          </div>
        </div>
        <button
          className={`${styles.btn} ${styles.btnPrimary}`}
          type="button"
          onClick={onAdd}
        >
          + Add Contributor
        </button>
      </div>

      <div className={styles.empSectionHead}>
        <h3 style={{ margin: 0 }}>
          Employees on {clientName}{" "}
          <span className={VIS_CHIP.internal}>INTERNAL</span>
        </h3>
        <div className={styles.meta}>
          Every WeSolve employee assigned to this project — assigned tasks,
          progress, deadlines, and current work status. Visible to the WeSolve
          team only.
        </div>
      </div>

      <div className={styles.empCardList}>
        {teamMembers.map((m) => (
          <EmployeeCard member={m} key={m.id} />
        ))}
      </div>

      <h3 style={{ margin: "24px 0 10px" }}>Open task load</h3>
      <div style={{ overflowX: "auto", marginBottom: 18 }}>
        <table
          className={styles.workTable}
          style={{ width: "100%", borderCollapse: "collapse" }}
        >
          <thead>
            <tr>
              <th style={{ textAlign: "left" }}>Team member</th>
              <th style={{ textAlign: "left" }}>Role</th>
              <th style={{ textAlign: "left" }}>Open tasks</th>
            </tr>
          </thead>
          <tbody>
            {teamMembers.length ? (
              teamMembers.map((m) => (
                <tr key={m.id}>
                  <td>{m.name}</td>
                  <td>{m.roleLabel}</td>
                  <td>{m.openTaskCount}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={3} className={styles.meta}>
                  No team members assigned yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <h3 style={{ margin: "6px 0 10px" }}>Contributors</h3>
      {contributors.length ? (
        contributors.map((p) => (
          <div className={styles.workCard} key={p.id}>
            <div className={styles.workCardTop}>
              <div>
                <div className={styles.workCardTitle}>{p.name}</div>
                <div className={styles.meta}>{p.role || "-"}</div>
              </div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                <span className={`${styles.badge} ${styles.badgeInfo}`}>
                  {p.person_type || "-"}
                </span>
                <span className={`${styles.badge} ${styles.badgeMuted}`}>
                  {p.status || "Active"}
                </span>
              </div>
            </div>

            <div className={styles.workCardMeta}>
              <div>
                <strong>Email:</strong> {p.email || "-"}
              </div>
              <div>
                <strong>Phone:</strong> {p.phone || "-"}
              </div>
              <div>
                <strong>Can update work:</strong>{" "}
                {p.can_update_work ? "Yes" : "No"}
              </div>
              <div>
                <strong>Can view client dashboard:</strong>{" "}
                {p.can_view_client_dashboard ? "Yes" : "No"}
              </div>
            </div>

            {p.notes ? (
              <div className={styles.meta} style={{ marginTop: 10 }}>
                {p.notes}
              </div>
            ) : null}

            <div className={styles.workCardActions}>
              <button
                className={styles.btn}
                type="button"
                onClick={() => onEdit(p.id)}
              >
                Edit
              </button>
              <button
                className={styles.btn}
                type="button"
                disabled={busyId === p.id}
                onClick={() => archiveContributor(p.id)}
              >
                Archive
              </button>
            </div>
          </div>
        ))
      ) : (
        <div className={styles.meta}>No contributors yet.</div>
      )}
    </div>
  );
}
