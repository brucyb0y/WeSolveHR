// Tasks / Work Progress, Blockers, and the published Weekly Progress Reports
// list. All server-rendered.
//
// The Report tab's daily/weekly subviews are NOT here — those are built by
// renderSummaryWithGoals() plus the auto-report/funnel HTML and are handled
// separately.

import styles from "./client-view.module.css";

function Table({ columns, rows, emptyText, renderRow }) {
  return (
    <div className={styles.tableWrap}>
      <table>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length ? (
            rows.map(renderRow)
          ) : (
            <tr>
              <td colSpan={columns.length} className={styles.meta}>
                {emptyText}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

export function BlockersTab({ blockers }) {
  return (
    <div className={styles.panel}>
      <div className={styles.panelHead}>
        <h2>Pending From Your Side</h2>
      </div>
      <div className={`${styles.meta} ${styles.panelNote}`}>
        Items awaiting approvals, responses, or actions from your team.
      </div>
      <Table
        columns={["Item", "Priority", "Status", "Logged"]}
        rows={blockers}
        emptyText="Nothing pending from your side. 🎉"
        renderRow={(b) => (
          <tr key={b.id}>
            <td>
              <strong>{b.title || "Pending item"}</strong>
              {b.description ? (
                <div className={styles.meta}>{b.description}</div>
              ) : null}
            </td>
            <td>{b.priority || "medium"}</td>
            <td>
              <span className={styles.badge}>
                {String(b.resolution_status || "open").replaceAll("_", " ")}
              </span>
            </td>
            <td>{b.createdAtText}</td>
          </tr>
        )}
      />
    </div>
  );
}

export function WorkTab({ linkedTasks, workOwnerGroups }) {
  return (
    <>
      <div className={styles.panel}>
        <div className={styles.panelHead}>
          <h2>Tasks</h2>
        </div>
        <div className={`${styles.meta} ${styles.panelNoteWide}`}>
          Tasks the team is actively driving for you.
        </div>
        <Table
          columns={["Task", "Status", "Priority", "Progress", "Due"]}
          rows={linkedTasks}
          emptyText="No tasks shared yet."
          renderRow={(t) => (
            <tr key={t.id}>
              <td>
                <strong>
                  #{t.task_no || t.id} · {t.title || "Untitled"}
                </strong>
                {t.area ? <div className={styles.meta}>{t.area}</div> : null}
              </td>
              <td>
                <span className={styles.badge}>
                  {String(t.status || "open").replace("_", " ")}
                </span>
              </td>
              <td>{t.priority || "-"}</td>
              <td>{Number(t.progress) || 0}%</td>
              <td>{t.deadline || "-"}</td>
            </tr>
          )}
        />
      </div>

      <div className={styles.panel}>
        <div className={styles.panelHead}>
          <h2>Work Progress</h2>
        </div>
        <div className={`${styles.meta} ${styles.panelNoteWide}`}>
          Ongoing activities and completion tracking by team member.
        </div>

        {workOwnerGroups.length ? (
          workOwnerGroups.map((g) => (
            <div className={styles.workOwnerBlock} key={g.name}>
              <div className={styles.workOwnerHead}>
                <strong>{g.name}</strong>
                <span className={styles.meta}>
                  {g.done}/{g.total} complete · {g.pct}%
                </span>
              </div>
              <div className={styles.progressTrack}>
                <div
                  className={styles.progressFill}
                  style={{ width: `${g.pct}%` }}
                />
              </div>
              <div className={`${styles.tableWrap} ${styles.workOwnerTable}`}>
                <table>
                  <thead>
                    <tr>
                      <th>Work Item</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {g.items.map((w) => (
                      <tr key={w.id}>
                        <td>
                          <strong>{w.title || "Untitled"}</strong>
                          {w.description ? (
                            <div className={styles.meta}>{w.description}</div>
                          ) : null}
                        </td>
                        <td>
                          <span className={styles.badge}>
                            {String(w.status || "open").replace("_", " ")}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))
        ) : (
          <div className={styles.meta}>No work items shared yet.</div>
        )}
      </div>
    </>
  );
}

export function WeeklyReportsList({ reports }) {
  const Section = ({ label, value }) =>
    value ? (
      <div className={styles.reportSection}>
        <strong>{label}:</strong>
        <div className={`${styles.meta} ${styles.reportSectionText}`}>
          {value}
        </div>
      </div>
    ) : null;

  return (
    <div className={styles.panel}>
      <div className={styles.panelHead}>
        <h2>Weekly Progress Reports</h2>
      </div>
      {reports.length ? (
        reports.map((r) => (
          <div className={styles.workOwnerBlock} key={r.id}>
            <div className={styles.workOwnerHead}>
              <strong>
                {r.period_label ||
                  (r.week_start ? `Week of ${r.week_start}` : "Report")}
              </strong>
              <span className={styles.meta}>{r.createdAtText}</span>
            </div>
            <Section label="Summary" value={r.summary} />
            <Section label="Highlights" value={r.highlights} />
            <Section label="Lowlights / Risks" value={r.lowlights} />
            <Section label="Next Week Plan" value={r.next_week_plan} />
          </div>
        ))
      ) : (
        <div className={styles.meta}>No weekly reports published yet.</div>
      )}
    </div>
  );
}
