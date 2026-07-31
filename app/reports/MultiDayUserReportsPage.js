// Markup for GET /reports?userId= (per-user view).
//
// Body markup extracted verbatim from renderMultiDayUserReportsPage() (lib/server/app.js
// lines 30638-31120). The document shell now comes from
// app/layout.jsx, the <style> block from ./reports-user.css, and the inline
// <script> from public/js/.

import { escapeHtml, formatDateOnly } from "@/lib/ui/html.js";

function escapeHtmlAttr(value) {
  return escapeHtml(value).replace(/'/g, "&#39;");
}

function linkifyTaskSentence(sentence, taskNo, taskId) {
  const safeSentence = escapeHtml(sentence || "");
  const clickable = `<button type="button" class="task-inline-link" onclick="openTaskDetail(${Number(taskNo)})">#${escapeHtml(taskNo)}</button>`;
  return safeSentence.replace(/^Task #\d+/, `Task ${clickable}`);
}

function summarizeUserMultiDayReport(dailyReports) {
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

    if (user.reportStatus === "full")
      fullDays += Number(user.workDayWeight || 0);
    else if (user.reportStatus === "partial")
      partialDays += Number(user.workDayWeight || 0);
    else if (user.reportStatus === "missing")
      missingDays += Number(user.workDayWeight || 0);
    else if (user.reportStatus === "leave") leaveDays += 1;
    else if (user.reportStatus === "off") offDays += 1;
  }

  return {
    totalWorkingDays,
    fullDays,
    partialDays,
    missingDays,
    leaveDays,
    offDays,
  };
}

// The document <title> is derived from the same data as the <h1>, so it lives
// here and is reused by the page's generateMetadata().
function multiDayUserReportsTitle(data) {
  const days = data?.days || 7;
  const dailyReports = data?.dailyReports || [];
  const firstUser =
    dailyReports?.[0]?.users?.[0] ||
    dailyReports?.find((d) => (d.users || []).length)?.users?.[0] ||
    null;

  return firstUser
    ? days === 1
      ? `${firstUser.userName} — Today Report`
      : `${firstUser.userName} — Last ${days} Days`
    : days === 1
      ? `Today Report`
      : `Last ${days} Days Report`;
}

function renderMultiDayUserReportsPage(data) {
  const days = data?.days || 7;
  const dailyReports = data?.dailyReports || [];
  const firstUser =
    dailyReports?.[0]?.users?.[0] ||
    dailyReports?.find((d) => (d.users || []).length)?.users?.[0] ||
    null;

  const pageTitle = multiDayUserReportsTitle(data);

  const weeklySummary = summarizeUserMultiDayReport(dailyReports);

  const dayCardsHtml = dailyReports
    .map((daily) => {
      const reportDate = daily.reportDate;
      const user = (daily.users || [])[0];

      if (!user) {
        return `
<div class="report-card">
<div class="report-card-head">
              <div>
                <div class="report-name">${escapeHtml(formatDateOnly(reportDate))}</div>
                <div class="report-date muted">No report data</div>
              </div>
            </div>
            <div class="report-section">
              <div class="muted">No updates found for this day.</div>
            </div>
          </div>
        `;
      }

      const taskHtml = (user.taskNarratives || []).length
        ? user.taskNarratives
            .map((item) => {
              const chipsHtml = (item.compactChanges || []).length
                ? `
                  <div class="change-chips">
                    ${item.compactChanges
                      .map(
                        (chip) => `
                          <span
                            class="change-chip"
                            title="${escapeHtmlAttr(chip.detail || chip.label)}"
                          >
                            ${escapeHtml(chip.label)}
                          </span>
                        `,
                      )
                      .join("")}
                  </div>
                `
                : "";

              return `
                <li class="report-task-item">
                  <div class="task-line">
                    ${linkifyTaskSentence(item.sentence, item.taskNo, item.taskId)}
                  </div>
                  ${chipsHtml}
                </li>
              `;
            })
            .join("")
        : `<li class="muted">No task updates</li>`;

      const extraHtml = (user.extraWork || []).length
        ? user.extraWork.map((note) => `<li>${escapeHtml(note)}</li>`).join("")
        : `<li class="muted">No extra work notes</li>`;

      return `
          <div class="report-card ${escapeHtml(user.reportCardClass || "")}">
          <div class="report-card-head">
            <div>
              <div class="report-name">${escapeHtml(formatDateOnly(reportDate))}</div>
              <div class="report-date">${escapeHtml(user.userName)}</div>
<div class="micro-meta">${escapeHtml(user.compactMeta || "0 touched")}</div>
<div class="report-reason">${escapeHtml(user.reportReason || "")}</div>
</div>
            <div class="summary-pill">
              Open: ${escapeHtml(user.summary?.open ?? 0)} | Blocked: ${escapeHtml(user.summary?.blocked ?? 0)}
            </div>
          </div>

          <div class="report-section">
            <div class="section-title">Task updates</div>
            <ul class="report-list">${taskHtml}</ul>
          </div>

          <div class="report-section">
            <div class="section-title">Extra work</div>
            <ul class="report-list">${extraHtml}</ul>
          </div>
        </div>
      `;
    })
    .join("");

  return `
            <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Multi-Day Reporting</div>
              <h1>${escapeHtml(pageTitle)}</h1>
<div class="subtitle">
  ${
    days === 1
      ? "Today’s attendance-day report."
      : `Last ${escapeHtml(days)} attendance-days, one section per day.`
  }
</div>
</div>
          </div>
          
          <div class="panel" style="padding:14px 16px; margin-bottom:16px;">
  <strong>Total working days:</strong> ${escapeHtml(String(weeklySummary.totalWorkingDays))}
  <br />
  <strong>Fully updated:</strong> ${escapeHtml(String(weeklySummary.fullDays))}
  <br />
  <strong>Partially updated:</strong> ${escapeHtml(String(weeklySummary.partialDays))}
  <br />
  <strong>Missing:</strong> ${escapeHtml(String(weeklySummary.missingDays))}
  <br />
  <strong>Leave days:</strong> ${escapeHtml(String(weeklySummary.leaveDays))}
  <br />
  <strong>Off days:</strong> ${escapeHtml(String(weeklySummary.offDays))}
</div>

          <div class="reports-stack">
            ${dayCardsHtml}
          </div>
        </div>

        <div id="taskModal" class="modal-backdrop" onclick="closeTaskModal(event)">
          <div class="modal-card" onclick="event.stopPropagation()">
            <div class="modal-head">
              <div>
                <div class="eyebrow">Task detail</div>
                <h2 id="modalTitle" class="modal-title">Loading...</h2>
              </div>
              <button class="modal-close" onclick="closeTaskModal()">Close</button>
            </div>

            <div id="modalBody">
              <div class="muted">Loading task details...</div>
            </div>
          </div>
        </div>

        <script src="/js/reports-user.js"></script>
      
  `;
}

export {
  multiDayUserReportsTitle,
  renderMultiDayUserReportsPage,
};
