// Markup for GET /attendance/:userId.
//
// Body markup extracted verbatim from renderEmployeeAttendancePage() (lib/server/app.js
// lines 31992-32787). The document shell now comes from
// app/layout.jsx, the <style> block from ./employee-attendance.css, and the inline
// <script> from public/js/.

import { formatDurationMinutes, getAttendanceMonthNavigation } from "@/lib/data/attendance-core.js";
import { escapeHtml, formatDateListForHumans } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderEmployeeAttendancePage(data) {
  const employee = data?.employee || {};
  const today = data?.today || {};
  const monthly = data?.monthly || {};
  const monthNav = data?.monthNav || getAttendanceMonthNavigation();
  const selectedMonthLabel = monthNav.selectedMonth;
  const monthQuery = `month=${encodeURIComponent(monthNav.selectedMonth)}`;
  const history = data?.history || [];
  const recentAudit = data?.recent_audit || [];
  const selectedDays = Number(data?.selectedDays) === 7 ? 7 : 1;

  const todayFlags = [
    today.long_shift_flag ? "Long shift" : null,
    today.long_break_flag ? "Long break" : null,
    today.possible_half_day ? "Half day" : null,
  ].filter(Boolean);

  const todayTimelineRows = (today.events || []).length
    ? today.events
        .map(
          (ev) => `
            <tr>
              <td>${escapeHtml(ev.time_text || "-")}</td>
              <td>${escapeHtml(ev.action || "-")}</td>
              <td>${escapeHtml(
                ev.expected_duration_min
                  ? `${ev.expected_duration_min} min`
                  : "-",
              )}</td>
              <td>${escapeHtml(ev.reason || "-")}</td>
              <td>${escapeHtml(ev.note || "-")}</td>
            </tr>
          `,
        )
        .join("")
    : `
      <tr>
        <td colspan="5" class="empty-cell">No attendance events today.</td>
      </tr>
    `;

  const historyRows = history.length
    ? history
        .map(
          (row) => `
            <tr>
              <td>${escapeHtml(row.attendance_date)}</td>
              <td>${escapeHtml(row.status)}</td>
              <td>${escapeHtml(row.first_login_text)}</td>
              <td>${escapeHtml(row.last_logout_text)}</td>
              <td>${escapeHtml(row.worked_text)}</td>
              <td>${escapeHtml(row.break_text)}</td>
              <td>${escapeHtml(row.late_text)}</td>
              <td>${escapeHtml(row.late_approved)}</td>
              <td>${escapeHtml(row.leave_text)}</td>
              <td>${escapeHtml(row.flags)}</td>
              <td>${escapeHtml(String(row.corrections || 0))}</td>
            </tr>
          `,
        )
        .join("")
    : `
      <tr>
        <td colspan="11" class="empty-cell">No history found for this month.</td>
      </tr>
    `;

  const leaveHistoryRows = history.length
    ? history
        .filter(
          (row) =>
            row.leave_text && row.leave_text !== "No" && row.leave_text !== "-",
        )
        .map(
          (row) => `
            <tr>
              <td>${escapeHtml(row.attendance_date)}</td>
              <td>${escapeHtml(row.leave_text)}</td>
              <td>${escapeHtml(row.status)}</td>
              <td>${escapeHtml(row.flags || "-")}</td>
            </tr>
          `,
        )
        .join("") ||
      `
          <tr>
            <td colspan="4" class="empty-cell">No leave entries found in this month view.</td>
          </tr>
        `
    : `
      <tr>
        <td colspan="4" class="empty-cell">No leave entries found in this month view.</td>
      </tr>
    `;

  const behaviorRows = history.length
    ? history
        .filter((row) => {
          const flags = String(row.flags || "").trim();
          return (
            (flags && flags !== "-") ||
            String(row.late_text || "") !== "No" ||
            Number(row.corrections || 0) > 0
          );
        })
        .map(
          (row) => `
            <tr>
              <td>${escapeHtml(row.attendance_date)}</td>
              <td>${escapeHtml(row.late_text || "No")}</td>
              <td>${escapeHtml(row.late_approved || "-")}</td>
              <td>${escapeHtml(row.flags || "-")}</td>
              <td>${escapeHtml(String(row.corrections || 0))}</td>
            </tr>
          `,
        )
        .join("") ||
      `
          <tr>
            <td colspan="5" class="empty-cell">No behavior flags found.</td>
          </tr>
        `
    : `
      <tr>
        <td colspan="5" class="empty-cell">No behavior flags found.</td>
      </tr>
    `;

  const auditRows = recentAudit.length
    ? recentAudit
        .map(
          (row) => `
            <tr>
              <td>${escapeHtml(row.created_at_text)}</td>
              <td>${escapeHtml(row.action_type)}</td>
              <td>${escapeHtml(row.note)}</td>
            </tr>
          `,
        )
        .join("")
    : `
      <tr>
        <td colspan="3" class="empty-cell">No recent audit entries.</td>
      </tr>
    `;

  return `
          ${renderTopNav("reports")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Employee Attendance Detail</div>
              <h1>${escapeHtml(employee.name || "Employee")}</h1>
              <div class="subtitle">
                ${escapeHtml(employee.role || "member")} • ${escapeHtml(employee.phone_number || "-")}
              </div>
              <div class="report-date" style="margin-top: 10px;">
                <a href="/reports?userId=${encodeURIComponent(employee.id)}" class="mini-report-link">Today</a>
                <a href="/reports?userId=${encodeURIComponent(employee.id)}&days=7" class="mini-report-link">Last 7 days</a>
                <a class="btn" href="/attendance/${employee.id}?days=${selectedDays}&month=${monthNav.prevMonth}">← Previous Month</a>
<a class="btn" href="/attendance/${employee.id}?days=${selectedDays}&month=${monthNav.currentMonth}">Current Month</a>
<a class="btn" href="/attendance/${employee.id}?days=${selectedDays}&month=${monthNav.nextMonth}">Next Month →</a>
              </div>
            </div>
          </div>

          <div class="hero-grid">
            <div class="panel hero-card">
              <div class="meta-label">Current status</div>
              <div class="hero-status">${escapeHtml(today.current_status || "off")}</div>

              <div class="hero-meta">
                <div class="meta-box">
                  <div class="meta-label">Attendance date</div>
                  <div class="meta-value">${escapeHtml(today.attendance_date || "-")}</div>
                </div>
                <div class="meta-box">
                  <div class="meta-label">Late status</div>
                  <div class="meta-value">${escapeHtml(today.late_status || "-")}</div>
                </div>
                <div class="meta-box">
                  <div class="meta-label">First login</div>
                  <div class="meta-value">${escapeHtml(today.first_login_text || "-")}</div>
                </div>
                <div class="meta-box">
                  <div class="meta-label">Last logout</div>
                  <div class="meta-value">${escapeHtml(today.last_logout_text || "-")}</div>
                </div>
              </div>
            </div>

            <div class="panel hero-card">
              <h2 style="margin-top:0;">Today focus</h2>
              <div class="subcards">
                <div class="subcard">
                  <div class="meta-label">Worked today</div>
                  <div class="v">${escapeHtml(today.worked_text || "-")}</div>
                </div>
                <div class="subcard">
                  <div class="meta-label">Break today</div>
                  <div class="v">${escapeHtml(today.break_text || "-")}</div>
                </div>
                <div class="subcard">
                  <div class="meta-label">Break count</div>
                  <div class="v">${escapeHtml(String(today.break_count || 0))}</div>
                </div>
              </div>

              <div class="kv">
                <div class="k">Leave today</div><div>${today.leave_today ? "Yes" : "No"}</div>
                <div class="k">Flags</div>
                <div>
                  ${
                    todayFlags.length
                      ? `<div class="flag-list">${todayFlags
                          .map(
                            (flag) =>
                              `<span class="flag-chip">${escapeHtml(flag)}</span>`,
                          )
                          .join("")}</div>`
                      : "None"
                  }
                </div>
              </div>
            </div>
          </div>

          <div class="cards">
            <div class="card"><div class="card-label">Present days</div><h2>${escapeHtml(String(monthly.presentDays || 0))}</h2></div>
            <div class="card"><div class="card-label">Leave entries</div><h2>${escapeHtml(String(monthly.leaveDays || 0))}</h2></div>
            <div class="card"><div class="card-label">Late joins</div><h2>${escapeHtml(String(monthly.lateJoins || 0))}</h2></div>
            <div class="card"><div class="card-label">Avg login</div><h2>${escapeHtml(monthly.avgLoginTimeText || "-")}</h2></div>
            <div class="card"><div class="card-label">Avg break</div><h2>${escapeHtml(formatDurationMinutes(monthly.avgBreakMin || 0))}</h2></div>
            <div class="card"><div class="card-label">Corrections</div><h2>${escapeHtml(String(monthly.managerCorrectionCount || 0))}</h2></div>
          </div>

          <div class="tabbar">
            <button class="tab-btn active" data-tab="overview">Overview</button>
            <button class="tab-btn" data-tab="history">History</button>
            <button class="tab-btn" data-tab="leave">Leave & Vacations</button>
            <button class="tab-btn" data-tab="audit">Audit</button>
          </div>

          <div id="tab-overview" class="tab-panel active">
            <div class="grid-2">
              <div class="panel">
                <h2>Today summary</h2>
                <div class="kv">
                  <div class="k">Attendance date</div><div>${escapeHtml(today.attendance_date || "-")}</div>
                  <div class="k">First login</div><div>${escapeHtml(today.first_login_text || "-")}</div>
                  <div class="k">Last logout</div><div>${escapeHtml(today.last_logout_text || "-")}</div>
                  <div class="k">Worked</div><div>${escapeHtml(today.worked_text || "-")}</div>
                  <div class="k">Break</div><div>${escapeHtml(today.break_text || "-")}</div>
                  <div class="k">Break count</div><div>${escapeHtml(String(today.break_count || 0))}</div>
                  <div class="k">Late today</div><div>${escapeHtml(today.late_text || "No")}</div>
                  <div class="k">Late status</div><div>${escapeHtml(today.late_status || "-")}</div>
                  <div class="k">Leave today</div><div>${today.leave_today ? "Yes" : "No"}</div>
                </div>
              </div>

              <div class="panel">
                <h2>Month summary</h2>
                <div class="kv">
                  <div class="k">Present days</div><div>${escapeHtml(String(monthly.presentDays || 0))}</div>
                  <div class="k">Total leave entries</div><div>${escapeHtml(String(monthly.leaveDays || 0))}</div>
                  <div class="k">Past leave dates</div><div>${escapeHtml(formatDateListForHumans(monthly.pastLeaveDates || []))}</div>
                  <div class="k">Upcoming leave dates</div><div>${escapeHtml(formatDateListForHumans(monthly.upcomingLeaveDates || []))}</div>
                  <div class="k">Late joins</div><div>${escapeHtml(String(monthly.lateJoins || 0))}</div>
                  <div class="k">Approved late</div><div>${escapeHtml(String(monthly.approvedLate || 0))}</div>
                  <div class="k">Late not approved</div><div>${escapeHtml(String(monthly.unapprovedLate || 0))}</div>
                  <div class="k">Late without prior info</div><div>${escapeHtml(String(monthly.uninformedLate || 0))}</div>
                  <div class="k">Average login time</div><div>${escapeHtml(monthly.avgLoginTimeText || "-")}</div>
                  <div class="k">Average break time</div><div>${escapeHtml(formatDurationMinutes(monthly.avgBreakMin || 0))}</div>
                  <div class="k">Long shift flags</div><div>${escapeHtml(String(monthly.longShiftCount || 0))}</div>
                  <div class="k">Long break flags</div><div>${escapeHtml(String(monthly.longBreakCount || 0))}</div>
                  <div class="k">Possible half days</div><div>${escapeHtml(String(monthly.possibleHalfDays || 0))}</div>
                  <div class="k">Manager corrections</div><div>${escapeHtml(String(monthly.managerCorrectionCount || 0))}</div>
                  <div class="k">Red report days</div>
                  <div id="redReportDaysValue"><span class="muted">Loading...</span></div>
                  <div class="k">Red report dates</div>
                  <div id="redReportDatesValue"><span class="muted">Loading...</span></div>
                </div>
              </div>
            </div>

            <div class="panel">
              <h2>Today timeline</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Action</th>
                      <th>Expected Duration</th>
                      <th>Reason</th>
                      <th>Note</th>
                    </tr>
                  </thead>
                  <tbody>${todayTimelineRows}</tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-history" class="tab-panel">
            <div class="panel">
              <h2>Attendance history · ${escapeHtml(selectedMonthLabel)}</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Status</th>
                      <th>Login</th>
                      <th>Logout</th>
                      <th>Worked</th>
                      <th>Break</th>
                      <th>Late</th>
                      <th>Late status</th>
                      <th>Leave</th>
                      <th>Flags</th>
                      <th>Corrections</th>
                    </tr>
                  </thead>
                  <tbody>${historyRows}</tbody>
                </table>
              </div>
            </div>

            <div class="panel">
              <h2>Behavior signals this month</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Late</th>
                      <th>Late status</th>
                      <th>Flags</th>
                      <th>Corrections</th>
                    </tr>
                  </thead>
                  <tbody>${behaviorRows}</tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-leave" class="tab-panel">
            <div class="grid-2">
              <div class="panel">
                <h2>Leave snapshot</h2>
                <div class="kv">
                  <div class="k">This month leave entries</div><div>${escapeHtml(String(monthly.leaveDays || 0))}</div>
                  <div class="k">Past leave dates</div><div>${escapeHtml(formatDateListForHumans(monthly.pastLeaveDates || []))}</div>
                  <div class="k">Upcoming leave dates</div><div>${escapeHtml(formatDateListForHumans(monthly.upcomingLeaveDates || []))}</div>
                </div>
                <div class="year-note">
                  This tab is ready for yearly vacation balance later. Right now it uses your existing monthly data.
                </div>
              </div>

              <div class="panel">
                <h2>Vacation / leave summary</h2>
                <div class="subcards">
                  <div class="subcard">
                    <div class="meta-label">This month leave</div>
                    <div class="v">${escapeHtml(String(monthly.leaveDays || 0))}</div>
                  </div>
                  <div class="subcard">
                    <div class="meta-label">Past leave dates</div>
                    <div class="v" style="font-size:15px; line-height:1.4;">${escapeHtml(formatDateListForHumans(monthly.pastLeaveDates || []))}</div>
                  </div>
                  <div class="subcard">
                    <div class="meta-label">Upcoming leave</div>
                    <div class="v" style="font-size:15px; line-height:1.4;">${escapeHtml(formatDateListForHumans(monthly.upcomingLeaveDates || []))}</div>
                  </div>
                </div>
              </div>
            </div>

            <div class="panel">
<h2>Leave rows · ${escapeHtml(selectedMonthLabel)}</h2>
<div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Leave</th>
                      <th>Status</th>
                      <th>Flags</th>
                    </tr>
                  </thead>
                  <tbody>${leaveHistoryRows}</tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-audit" class="tab-panel">
            <div class="panel">
              <h2>Recent attendance audit</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Action Type</th>
                      <th>Note</th>
                    </tr>
                  </thead>
                  <tbody>${auditRows}</tbody>
                </table>
              </div>
            </div>
          </div>
        </div>

<script>
  async function loadRedReports(userId) {
    const daysEl = document.getElementById("redReportDaysValue");
    const datesEl = document.getElementById("redReportDatesValue");

    if (!daysEl || !datesEl || !userId) return;

    try {
      const res = await fetch('/api/attendance/' + userId + '/red-reports?month=${monthNav.selectedMonth}', {
        headers: { Accept: "application/json" },
      });

      const json = await res.json();

      if (!json.ok) {
        daysEl.innerHTML = '<span class="muted">Failed to load</span>';
        datesEl.innerHTML = '<span class="muted">Failed to load</span>';
        return;
      }

      const payload = json.data || {};
      const redReportDays = Number(payload.redReportDays || 0);
      const redReportDates = Array.isArray(payload.redReportDates)
        ? payload.redReportDates
        : [];
      const redReportDatesText = payload.redReportDatesText || "None";

      daysEl.textContent = String(redReportDays);

      if (redReportDates.length) {
        datesEl.innerHTML =
          '<details>' +
            '<summary>' + redReportDays + ' date(s)</summary>' +
            '<div style="margin-top:8px;">' +
              escapeHtmlClient(redReportDatesText) +
            '</div>' +
          '</details>';
      } else {
        datesEl.textContent = "None";
      }
    } catch (error) {
      console.error("Red reports fetch failed:", error);
      daysEl.innerHTML = '<span class="muted">Failed to load</span>';
      datesEl.innerHTML = '<span class="muted">Failed to load</span>';
    }
  }

  function escapeHtmlClient(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function initAttendanceDetailTabs() {
    const buttons = Array.from(document.querySelectorAll(".tab-btn"));
    const panels = Array.from(document.querySelectorAll(".tab-panel"));

    buttons.forEach((btn) => {
      btn.addEventListener("click", function () {
        const tab = btn.getAttribute("data-tab");

        buttons.forEach((b) => b.classList.remove("active"));
        panels.forEach((p) => p.classList.remove("active"));

        btn.classList.add("active");
        const panel = document.getElementById("tab-" + tab);
        if (panel) panel.classList.add("active");
      });
    });
  }

  initAttendanceDetailTabs();
  loadRedReports(${JSON.stringify(employee.id || null)});
</script>
      
  `;
}

export {
  renderEmployeeAttendancePage,
};
