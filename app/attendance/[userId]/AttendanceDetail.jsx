"use client";

// Employee attendance detail body. Ported from renderEmployeeAttendancePage() in
// lib/server/app.js. All the data is fetched on the server and passed in as
// props; the only client behavior is the tab switching and the "red reports"
// lookup (fetched from /api/attendance/:id/red-reports, served by the dispatch
// shim) — both now React state instead of the old inline scripts.

import { useEffect, useState } from "react";
import {
  formatDateListForHumans,
  formatDurationMinutes,
} from "@/lib/utils/datetime.js";
import styles from "./attendance-detail.module.css";

const TABS = [
  { key: "overview", label: "Overview" },
  { key: "history", label: "History" },
  { key: "leave", label: "Leave & Vacations" },
  { key: "audit", label: "Audit" },
];

export default function AttendanceDetail({
  employee = {},
  today = {},
  monthly = {},
  history = [],
  recentAudit = [],
  selectedDays = 1,
  monthNav = {},
}) {
  const [activeTab, setActiveTab] = useState("overview");
  const [redReports, setRedReports] = useState(null); // null=loading | {failed} | {days,dates,datesText}

  useEffect(() => {
    const userId = employee?.id;
    if (!userId) return;
    let alive = true;
    fetch(
      `/api/attendance/${userId}/red-reports?month=${encodeURIComponent(monthNav.selectedMonth || "")}`,
      { headers: { Accept: "application/json" } },
    )
      .then((r) => r.json())
      .then((json) => {
        if (!alive) return;
        if (!json.ok) {
          setRedReports({ failed: true });
          return;
        }
        const p = json.data || {};
        setRedReports({
          days: Number(p.redReportDays || 0),
          dates: Array.isArray(p.redReportDates) ? p.redReportDates : [],
          datesText: p.redReportDatesText || "None",
        });
      })
      .catch((error) => {
        console.error("Red reports fetch failed:", error);
        if (alive) setRedReports({ failed: true });
      });
    return () => {
      alive = false;
    };
  }, [employee?.id, monthNav.selectedMonth]);

  const todayFlags = [
    today.long_shift_flag ? "Long shift" : null,
    today.long_break_flag ? "Long break" : null,
    today.possible_half_day ? "Half day" : null,
  ].filter(Boolean);

  const leaveHistory = history.filter(
    (row) =>
      row.leave_text && row.leave_text !== "No" && row.leave_text !== "-",
  );
  const behavior = history.filter((row) => {
    const flags = String(row.flags || "").trim();
    return (
      (flags && flags !== "-") ||
      String(row.late_text || "") !== "No" ||
      Number(row.corrections || 0) > 0
    );
  });

  const redDaysValue =
    redReports === null ? (
      <span className="muted">Loading...</span>
    ) : redReports.failed ? (
      <span className="muted">Failed to load</span>
    ) : (
      String(redReports.days)
    );

  const redDatesValue =
    redReports === null ? (
      <span className="muted">Loading...</span>
    ) : redReports.failed ? (
      <span className="muted">Failed to load</span>
    ) : redReports.dates.length ? (
      <details>
        <summary>{redReports.days} date(s)</summary>
        <div style={{ marginTop: 8 }}>{redReports.datesText}</div>
      </details>
    ) : (
      "None"
    );

  const tabClass = (key) =>
    `${styles.tabPanel} ${activeTab === key ? styles.active : ""}`;

  return (
    <div className={styles.wrap}>
      <div className={styles.topbar}>
        <div>
          <div className={styles.eyebrow}>Employee Attendance Detail</div>
          <h1>{employee.name || "Employee"}</h1>
          <div className={styles.subtitle}>
            {employee.role || "member"} • {employee.phone_number || "-"}
          </div>
          <div className={styles.reportDate} style={{ marginTop: 10 }}>
            <a
              href={`/reports?userId=${encodeURIComponent(employee.id)}`}
              className={styles.miniReportLink}
            >
              Today
            </a>
            <a
              href={`/reports?userId=${encodeURIComponent(employee.id)}&days=7`}
              className={styles.miniReportLink}
            >
              Last 7 days
            </a>
            <a
              className={styles.btn}
              href={`/attendance/${employee.id}?days=${selectedDays}&month=${monthNav.prevMonth}`}
            >
              ← Previous Month
            </a>
            <a
              className={styles.btn}
              href={`/attendance/${employee.id}?days=${selectedDays}&month=${monthNav.currentMonth}`}
            >
              Current Month
            </a>
            <a
              className={styles.btn}
              href={`/attendance/${employee.id}?days=${selectedDays}&month=${monthNav.nextMonth}`}
            >
              Next Month →
            </a>
          </div>
        </div>
      </div>

      <div className={styles.heroGrid}>
        <div className={`${styles.panel} ${styles.heroCard}`}>
          <div className={styles.metaLabel}>Current status</div>
          <div className={styles.heroStatus}>{today.current_status || "off"}</div>

          <div className={styles.heroMeta}>
            <div className={styles.metaBox}>
              <div className={styles.metaLabel}>Attendance date</div>
              <div className={styles.metaValue}>{today.attendance_date || "-"}</div>
            </div>
            <div className={styles.metaBox}>
              <div className={styles.metaLabel}>Late status</div>
              <div className={styles.metaValue}>{today.late_status || "-"}</div>
            </div>
            <div className={styles.metaBox}>
              <div className={styles.metaLabel}>First login</div>
              <div className={styles.metaValue}>{today.first_login_text || "-"}</div>
            </div>
            <div className={styles.metaBox}>
              <div className={styles.metaLabel}>Last logout</div>
              <div className={styles.metaValue}>{today.last_logout_text || "-"}</div>
            </div>
          </div>
        </div>

        <div className={`${styles.panel} ${styles.heroCard}`}>
          <h2 style={{ marginTop: 0 }}>Today focus</h2>
          <div className={styles.subcards}>
            <div className={styles.subcard}>
              <div className={styles.metaLabel}>Worked today</div>
              <div className="v">{today.worked_text || "-"}</div>
            </div>
            <div className={styles.subcard}>
              <div className={styles.metaLabel}>Break today</div>
              <div className="v">{today.break_text || "-"}</div>
            </div>
            <div className={styles.subcard}>
              <div className={styles.metaLabel}>Break count</div>
              <div className="v">{String(today.break_count || 0)}</div>
            </div>
          </div>

          <div className={styles.kv}>
            <div className={styles.k}>Leave today</div>
            <div>{today.leave_today ? "Yes" : "No"}</div>
            <div className={styles.k}>Flags</div>
            <div>
              {todayFlags.length ? (
                <div className={styles.flagList}>
                  {todayFlags.map((flag) => (
                    <span className={styles.flagChip} key={flag}>
                      {flag}
                    </span>
                  ))}
                </div>
              ) : (
                "None"
              )}
            </div>
          </div>
        </div>
      </div>

      <div className={styles.cards}>
        <div className={styles.card}>
          <div className={styles.cardLabel}>Present days</div>
          <h2>{String(monthly.presentDays || 0)}</h2>
        </div>
        <div className={styles.card}>
          <div className={styles.cardLabel}>Leave entries</div>
          <h2>{String(monthly.leaveDays || 0)}</h2>
        </div>
        <div className={styles.card}>
          <div className={styles.cardLabel}>Late joins</div>
          <h2>{String(monthly.lateJoins || 0)}</h2>
        </div>
        <div className={styles.card}>
          <div className={styles.cardLabel}>Avg login</div>
          <h2>{monthly.avgLoginTimeText || "-"}</h2>
        </div>
        <div className={styles.card}>
          <div className={styles.cardLabel}>Avg break</div>
          <h2>{formatDurationMinutes(monthly.avgBreakMin || 0)}</h2>
        </div>
        <div className={styles.card}>
          <div className={styles.cardLabel}>Corrections</div>
          <h2>{String(monthly.managerCorrectionCount || 0)}</h2>
        </div>
      </div>

      <div className={styles.tabbar}>
        {TABS.map((tab) => (
          <button
            key={tab.key}
            className={`${styles.tabBtn} ${activeTab === tab.key ? styles.active : ""}`}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className={tabClass("overview")}>
        <div className={styles.grid2}>
          <div className={styles.panel}>
            <h2>Today summary</h2>
            <div className={styles.kv}>
              <div className={styles.k}>Attendance date</div>
              <div>{today.attendance_date || "-"}</div>
              <div className={styles.k}>First login</div>
              <div>{today.first_login_text || "-"}</div>
              <div className={styles.k}>Last logout</div>
              <div>{today.last_logout_text || "-"}</div>
              <div className={styles.k}>Worked</div>
              <div>{today.worked_text || "-"}</div>
              <div className={styles.k}>Break</div>
              <div>{today.break_text || "-"}</div>
              <div className={styles.k}>Break count</div>
              <div>{String(today.break_count || 0)}</div>
              <div className={styles.k}>Late today</div>
              <div>{today.late_text || "No"}</div>
              <div className={styles.k}>Late status</div>
              <div>{today.late_status || "-"}</div>
              <div className={styles.k}>Leave today</div>
              <div>{today.leave_today ? "Yes" : "No"}</div>
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Month summary</h2>
            <div className={styles.kv}>
              <div className={styles.k}>Present days</div>
              <div>{String(monthly.presentDays || 0)}</div>
              <div className={styles.k}>Total leave entries</div>
              <div>{String(monthly.leaveDays || 0)}</div>
              <div className={styles.k}>Past leave dates</div>
              <div>{formatDateListForHumans(monthly.pastLeaveDates || [])}</div>
              <div className={styles.k}>Upcoming leave dates</div>
              <div>{formatDateListForHumans(monthly.upcomingLeaveDates || [])}</div>
              <div className={styles.k}>Late joins</div>
              <div>{String(monthly.lateJoins || 0)}</div>
              <div className={styles.k}>Approved late</div>
              <div>{String(monthly.approvedLate || 0)}</div>
              <div className={styles.k}>Late not approved</div>
              <div>{String(monthly.unapprovedLate || 0)}</div>
              <div className={styles.k}>Late without prior info</div>
              <div>{String(monthly.uninformedLate || 0)}</div>
              <div className={styles.k}>Average login time</div>
              <div>{monthly.avgLoginTimeText || "-"}</div>
              <div className={styles.k}>Average break time</div>
              <div>{formatDurationMinutes(monthly.avgBreakMin || 0)}</div>
              <div className={styles.k}>Long shift flags</div>
              <div>{String(monthly.longShiftCount || 0)}</div>
              <div className={styles.k}>Long break flags</div>
              <div>{String(monthly.longBreakCount || 0)}</div>
              <div className={styles.k}>Possible half days</div>
              <div>{String(monthly.possibleHalfDays || 0)}</div>
              <div className={styles.k}>Manager corrections</div>
              <div>{String(monthly.managerCorrectionCount || 0)}</div>
              <div className={styles.k}>Red report days</div>
              <div>{redDaysValue}</div>
              <div className={styles.k}>Red report dates</div>
              <div>{redDatesValue}</div>
            </div>
          </div>
        </div>

        <div className={styles.panel}>
          <h2>Today timeline</h2>
          <div className={styles.tableWrap}>
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
              <tbody>
                {(today.events || []).length ? (
                  today.events.map((ev, i) => (
                    <tr key={i}>
                      <td>{ev.time_text || "-"}</td>
                      <td>{ev.action || "-"}</td>
                      <td>
                        {ev.expected_duration_min
                          ? `${ev.expected_duration_min} min`
                          : "-"}
                      </td>
                      <td>{ev.reason || "-"}</td>
                      <td>{ev.note || "-"}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5} className="empty-cell">
                      No attendance events today.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className={tabClass("history")}>
        <div className={styles.panel}>
          <h2>Attendance history · {monthNav.selectedMonth}</h2>
          <div className={styles.tableWrap}>
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
              <tbody>
                {history.length ? (
                  history.map((row, i) => (
                    <tr key={i}>
                      <td>{row.attendance_date}</td>
                      <td>{row.status}</td>
                      <td>{row.first_login_text}</td>
                      <td>{row.last_logout_text}</td>
                      <td>{row.worked_text}</td>
                      <td>{row.break_text}</td>
                      <td>{row.late_text}</td>
                      <td>{row.late_approved}</td>
                      <td>{row.leave_text}</td>
                      <td>{row.flags}</td>
                      <td>{String(row.corrections || 0)}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={11} className="empty-cell">
                      No history found for this month.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className={styles.panel}>
          <h2>Behavior signals this month</h2>
          <div className={styles.tableWrap}>
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
              <tbody>
                {behavior.length ? (
                  behavior.map((row, i) => (
                    <tr key={i}>
                      <td>{row.attendance_date}</td>
                      <td>{row.late_text || "No"}</td>
                      <td>{row.late_approved || "-"}</td>
                      <td>{row.flags || "-"}</td>
                      <td>{String(row.corrections || 0)}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5} className="empty-cell">
                      No behavior flags found.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className={tabClass("leave")}>
        <div className={styles.grid2}>
          <div className={styles.panel}>
            <h2>Leave snapshot</h2>
            <div className={styles.kv}>
              <div className={styles.k}>This month leave entries</div>
              <div>{String(monthly.leaveDays || 0)}</div>
              <div className={styles.k}>Past leave dates</div>
              <div>{formatDateListForHumans(monthly.pastLeaveDates || [])}</div>
              <div className={styles.k}>Upcoming leave dates</div>
              <div>{formatDateListForHumans(monthly.upcomingLeaveDates || [])}</div>
            </div>
            <div className={styles.yearNote}>
              This tab is ready for yearly vacation balance later. Right now it
              uses your existing monthly data.
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Vacation / leave summary</h2>
            <div className={styles.subcards}>
              <div className={styles.subcard}>
                <div className={styles.metaLabel}>This month leave</div>
                <div className="v">{String(monthly.leaveDays || 0)}</div>
              </div>
              <div className={styles.subcard}>
                <div className={styles.metaLabel}>Past leave dates</div>
                <div className="v" style={{ fontSize: 15, lineHeight: 1.4 }}>
                  {formatDateListForHumans(monthly.pastLeaveDates || [])}
                </div>
              </div>
              <div className={styles.subcard}>
                <div className={styles.metaLabel}>Upcoming leave</div>
                <div className="v" style={{ fontSize: 15, lineHeight: 1.4 }}>
                  {formatDateListForHumans(monthly.upcomingLeaveDates || [])}
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className={styles.panel}>
          <h2>Leave rows · {monthNav.selectedMonth}</h2>
          <div className={styles.tableWrap}>
            <table>
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Leave</th>
                  <th>Status</th>
                  <th>Flags</th>
                </tr>
              </thead>
              <tbody>
                {leaveHistory.length ? (
                  leaveHistory.map((row, i) => (
                    <tr key={i}>
                      <td>{row.attendance_date}</td>
                      <td>{row.leave_text}</td>
                      <td>{row.status}</td>
                      <td>{row.flags || "-"}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={4} className="empty-cell">
                      No leave entries found in this month view.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className={tabClass("audit")}>
        <div className={styles.panel}>
          <h2>Recent attendance audit</h2>
          <div className={styles.tableWrap}>
            <table>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Action Type</th>
                  <th>Note</th>
                </tr>
              </thead>
              <tbody>
                {recentAudit.length ? (
                  recentAudit.map((row, i) => (
                    <tr key={i}>
                      <td>{row.created_at_text}</td>
                      <td>{row.action_type}</td>
                      <td>{row.note}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={3} className="empty-cell">
                      No recent audit entries.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
