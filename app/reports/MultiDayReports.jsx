// Per-user report view (single day or multi-day). Ported from
// renderMultiDayUserReportsPage() in lib/server/app.js. Server-rendered from the
// report data; task sentences keep their linkified #task buttons (injected HTML)
// which call window.openTaskDetail registered by the shared TaskModal client
// island. Styling comes from the global reports.css (scoped under .reports-page).

import { formatDateOnly } from "@/lib/utils/datetime.js";
import TaskModal from "./TaskModal.jsx";

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// Ported from linkifyTaskSentence(): turns a leading "Task #N" into a clickable
// button wired to the global openTaskDetail.
function linkifyTaskSentence(sentence, taskNo) {
  const safe = escapeHtml(sentence || "");
  const clickable = `<button type="button" class="task-inline-link" onclick="openTaskDetail(${Number(taskNo)})">#${escapeHtml(taskNo)}</button>`;
  return safe.replace(/^Task #\d+/, `Task ${clickable}`);
}

// Ported from summarizeUserMultiDayReport().
function summarize(dailyReports) {
  let totalWorkingDays = 0;
  let fullDays = 0;
  let partialDays = 0;
  let missingDays = 0;
  let leaveDays = 0;
  let offDays = 0;

  for (const daily of dailyReports || []) {
    const user = (daily.users || [])[0];
    if (!user) continue;
    totalWorkingDays += Number(user.workDayWeight || 0);
    if (user.reportStatus === "full") fullDays += Number(user.workDayWeight || 0);
    else if (user.reportStatus === "partial")
      partialDays += Number(user.workDayWeight || 0);
    else if (user.reportStatus === "missing")
      missingDays += Number(user.workDayWeight || 0);
    else if (user.reportStatus === "leave") leaveDays += 1;
    else if (user.reportStatus === "off") offDays += 1;
  }

  return { totalWorkingDays, fullDays, partialDays, missingDays, leaveDays, offDays };
}

function DayCard({ daily }) {
  const reportDate = daily.reportDate;
  const user = (daily.users || [])[0];

  if (!user) {
    return (
      <div className="report-card">
        <div className="report-card-head">
          <div>
            <div className="report-name">{formatDateOnly(reportDate)}</div>
            <div className="report-date muted">No report data</div>
          </div>
        </div>
        <div className="report-section">
          <div className="muted">No updates found for this day.</div>
        </div>
      </div>
    );
  }

  const taskNarratives = user.taskNarratives || [];
  const extraWork = user.extraWork || [];

  return (
    <div className={`report-card ${user.reportCardClass || ""}`}>
      <div className="report-card-head">
        <div>
          <div className="report-name">{formatDateOnly(reportDate)}</div>
          <div className="report-date">{user.userName}</div>
          <div className="micro-meta">{user.compactMeta || "0 touched"}</div>
          <div className="report-reason">{user.reportReason || ""}</div>
        </div>
        <div className="summary-pill">
          Open: {user.summary?.open ?? 0} | Blocked: {user.summary?.blocked ?? 0}
        </div>
      </div>

      <div className="report-section">
        <div className="section-title">Task updates</div>
        <ul className="report-list">
          {taskNarratives.length ? (
            taskNarratives.map((item, i) => (
              <li className="report-task-item" key={i}>
                <div
                  className="task-line"
                  dangerouslySetInnerHTML={{
                    __html: linkifyTaskSentence(item.sentence, item.taskNo),
                  }}
                />
                {(item.compactChanges || []).length ? (
                  <div className="change-chips">
                    {item.compactChanges.map((chip, j) => (
                      <span
                        className="change-chip"
                        title={chip.detail || chip.label}
                        key={j}
                      >
                        {chip.label}
                      </span>
                    ))}
                  </div>
                ) : null}
              </li>
            ))
          ) : (
            <li className="muted">No task updates</li>
          )}
        </ul>
      </div>

      <div className="report-section">
        <div className="section-title">Extra work</div>
        <ul className="report-list">
          {extraWork.length ? (
            extraWork.map((note, i) => <li key={i}>{note}</li>)
          ) : (
            <li className="muted">No extra work notes</li>
          )}
        </ul>
      </div>
    </div>
  );
}

export default function MultiDayReports({ data }) {
  const days = data?.days || 7;
  const dailyReports = data?.dailyReports || [];
  const firstUser =
    dailyReports?.[0]?.users?.[0] ||
    dailyReports?.find((d) => (d.users || []).length)?.users?.[0] ||
    null;

  const pageTitle = firstUser
    ? days === 1
      ? `${firstUser.userName} — Today Report`
      : `${firstUser.userName} — Last ${days} Days`
    : days === 1
      ? "Today Report"
      : `Last ${days} Days Report`;

  const summary = summarize(dailyReports);

  return (
    <>
      <div className="wrap">
        <div className="topbar">
          <div>
            <div className="eyebrow">Multi-Day Reporting</div>
            <h1>{pageTitle}</h1>
            <div className="subtitle">
              {days === 1
                ? "Today’s attendance-day report."
                : `Last ${days} attendance-days, one section per day.`}
            </div>
          </div>
        </div>

        <div className="panel" style={{ padding: "14px 16px", marginBottom: 16 }}>
          <strong>Total working days:</strong> {String(summary.totalWorkingDays)}
          <br />
          <strong>Fully updated:</strong> {String(summary.fullDays)}
          <br />
          <strong>Partially updated:</strong> {String(summary.partialDays)}
          <br />
          <strong>Missing:</strong> {String(summary.missingDays)}
          <br />
          <strong>Leave days:</strong> {String(summary.leaveDays)}
          <br />
          <strong>Off days:</strong> {String(summary.offDays)}
        </div>

        <div className="reports-stack">
          {dailyReports.map((daily, i) => (
            <DayCard daily={daily} key={i} />
          ))}
        </div>
      </div>

      <TaskModal />
    </>
  );
}
