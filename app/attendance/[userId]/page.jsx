// /attendance/:userId — replaces renderEmployeeAttendancePage() +
// app.get("/attendance/:userId").
//
// Everything except the tab toggle and the red-report cells is server-rendered;
// the panel bodies are passed into AttendanceTabs as elements.
//
// The top nav is highlighted as "reports" here, not "attendance" — carried over
// from the original renderTopNav("reports") call.

import { notFound } from "next/navigation";
import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import {
  DASHBOARD_ORG_ID,
  getEmployeeAttendanceOverview,
  getAttendanceMonthNavigation,
  formatDurationMinutes,
  formatDateListForHumans,
} from "@/lib/server/app.js";
import AttendanceTabs from "./AttendanceTabs";
import RedReports from "./RedReports";
import styles from "./employee-attendance.module.css";

export const metadata = { title: "Employee Attendance" };
export const dynamic = "force-dynamic";

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
              <td colSpan={columns.length} className={styles.emptyCell}>
                {emptyText}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

const KV = ({ label, children }) => (
  <>
    <div className={styles.k}>{label}</div>
    <div>{children}</div>
  </>
);

export default async function EmployeeAttendancePage({ params, searchParams }) {
  const user = await requireDashboardUser();
  const { userId: rawUserId } = await params;
  const sp = await searchParams;

  const userId = Number(rawUserId);
  if (!userId) notFound();

  const selectedDays = Number(sp?.days) === 7 ? 7 : 1;
  const monthNav = getAttendanceMonthNavigation(sp?.month);

  const data = await getEmployeeAttendanceOverview(userId, DASHBOARD_ORG_ID, {
    days: selectedDays,
    monthNav,
  });

  const employee = data?.employee || {};
  const today = data?.today || {};
  const monthly = data?.monthly || {};
  const history = data?.history || [];
  const recentAudit = data?.recent_audit || [];
  const selectedMonthLabel = monthNav.selectedMonth;

  const todayFlags = [
    today.long_shift_flag ? "Long shift" : null,
    today.long_break_flag ? "Long break" : null,
    today.possible_half_day ? "Half day" : null,
  ].filter(Boolean);

  const leaveRows = history.filter(
    (row) =>
      row.leave_text && row.leave_text !== "No" && row.leave_text !== "-",
  );

  const behaviorRows = history.filter((row) => {
    const flags = String(row.flags || "").trim();
    return (
      (flags && flags !== "-") ||
      String(row.late_text || "") !== "No" ||
      Number(row.corrections || 0) > 0
    );
  });

  const monthHref = (month) =>
    `/attendance/${employee.id}?days=${selectedDays}&month=${month}`;

  const overviewPanel = (
    <>
      <div className={styles.grid2}>
        <div className={styles.panel}>
          <h2>Today summary</h2>
          <div className={styles.kv}>
            <KV label="Attendance date">{today.attendance_date || "-"}</KV>
            <KV label="First login">{today.first_login_text || "-"}</KV>
            <KV label="Last logout">{today.last_logout_text || "-"}</KV>
            <KV label="Worked">{today.worked_text || "-"}</KV>
            <KV label="Break">{today.break_text || "-"}</KV>
            <KV label="Break count">{String(today.break_count || 0)}</KV>
            <KV label="Late today">{today.late_text || "No"}</KV>
            <KV label="Late status">{today.late_status || "-"}</KV>
            <KV label="Leave today">{today.leave_today ? "Yes" : "No"}</KV>
          </div>
        </div>

        <div className={styles.panel}>
          <h2>Month summary</h2>
          <div className={styles.kv}>
            <KV label="Present days">{String(monthly.presentDays || 0)}</KV>
            <KV label="Total leave entries">
              {String(monthly.leaveDays || 0)}
            </KV>
            <KV label="Past leave dates">
              {formatDateListForHumans(monthly.pastLeaveDates || [])}
            </KV>
            <KV label="Upcoming leave dates">
              {formatDateListForHumans(monthly.upcomingLeaveDates || [])}
            </KV>
            <KV label="Late joins">{String(monthly.lateJoins || 0)}</KV>
            <KV label="Approved late">{String(monthly.approvedLate || 0)}</KV>
            <KV label="Late not approved">
              {String(monthly.unapprovedLate || 0)}
            </KV>
            <KV label="Late without prior info">
              {String(monthly.uninformedLate || 0)}
            </KV>
            <KV label="Average login time">
              {monthly.avgLoginTimeText || "-"}
            </KV>
            <KV label="Average break time">
              {formatDurationMinutes(monthly.avgBreakMin || 0)}
            </KV>
            <KV label="Long shift flags">
              {String(monthly.longShiftCount || 0)}
            </KV>
            <KV label="Long break flags">
              {String(monthly.longBreakCount || 0)}
            </KV>
            <KV label="Possible half days">
              {String(monthly.possibleHalfDays || 0)}
            </KV>
            <KV label="Manager corrections">
              {String(monthly.managerCorrectionCount || 0)}
            </KV>
            <RedReports
              userId={employee.id}
              month={monthNav.selectedMonth}
              labelClassName={styles.k}
            />
          </div>
        </div>
      </div>

      <div className={styles.panel}>
        <h2>Today timeline</h2>
        <Table
          columns={["Time", "Action", "Expected Duration", "Reason", "Note"]}
          rows={today.events || []}
          emptyText="No attendance events today."
          renderRow={(ev, i) => (
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
          )}
        />
      </div>
    </>
  );

  const historyPanel = (
    <>
      <div className={styles.panel}>
        <h2>Attendance history · {selectedMonthLabel}</h2>
        <Table
          columns={[
            "Date",
            "Status",
            "Login",
            "Logout",
            "Worked",
            "Break",
            "Late",
            "Late status",
            "Leave",
            "Flags",
            "Corrections",
          ]}
          rows={history}
          emptyText="No history found for this month."
          renderRow={(row) => (
            <tr key={row.attendance_date}>
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
          )}
        />
      </div>

      <div className={styles.panel}>
        <h2>Behavior signals this month</h2>
        <Table
          columns={["Date", "Late", "Late status", "Flags", "Corrections"]}
          rows={behaviorRows}
          emptyText="No behavior flags found."
          renderRow={(row) => (
            <tr key={row.attendance_date}>
              <td>{row.attendance_date}</td>
              <td>{row.late_text || "No"}</td>
              <td>{row.late_approved || "-"}</td>
              <td>{row.flags || "-"}</td>
              <td>{String(row.corrections || 0)}</td>
            </tr>
          )}
        />
      </div>
    </>
  );

  const leavePanel = (
    <>
      <div className={styles.grid2}>
        <div className={styles.panel}>
          <h2>Leave snapshot</h2>
          <div className={styles.kv}>
            <KV label="This month leave entries">
              {String(monthly.leaveDays || 0)}
            </KV>
            <KV label="Past leave dates">
              {formatDateListForHumans(monthly.pastLeaveDates || [])}
            </KV>
            <KV label="Upcoming leave dates">
              {formatDateListForHumans(monthly.upcomingLeaveDates || [])}
            </KV>
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
              <div className={styles.v}>{String(monthly.leaveDays || 0)}</div>
            </div>
            <div className={styles.subcard}>
              <div className={styles.metaLabel}>Past leave dates</div>
              <div className={`${styles.v} ${styles.subcardDates}`}>
                {formatDateListForHumans(monthly.pastLeaveDates || [])}
              </div>
            </div>
            <div className={styles.subcard}>
              <div className={styles.metaLabel}>Upcoming leave</div>
              <div className={`${styles.v} ${styles.subcardDates}`}>
                {formatDateListForHumans(monthly.upcomingLeaveDates || [])}
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className={styles.panel}>
        <h2>Leave rows · {selectedMonthLabel}</h2>
        <Table
          columns={["Date", "Leave", "Status", "Flags"]}
          rows={leaveRows}
          emptyText="No leave entries found in this month view."
          renderRow={(row) => (
            <tr key={row.attendance_date}>
              <td>{row.attendance_date}</td>
              <td>{row.leave_text}</td>
              <td>{row.status}</td>
              <td>{row.flags || "-"}</td>
            </tr>
          )}
        />
      </div>
    </>
  );

  const auditPanel = (
    <div className={styles.panel}>
      <h2>Recent attendance audit</h2>
      <Table
        columns={["Time", "Action Type", "Note"]}
        rows={recentAudit}
        emptyText="No recent audit entries."
        renderRow={(row, i) => (
          <tr key={i}>
            <td>{row.created_at_text}</td>
            <td>{row.action_type}</td>
            <td>{row.note}</td>
          </tr>
        )}
      />
    </div>
  );

  return (
    <>
      <TopNav active="reports" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Employee Attendance Detail</div>
            <h1>{employee.name || "Employee"}</h1>
            <div className={styles.subtitle}>
              {employee.role || "member"} • {employee.phone_number || "-"}
            </div>
            <div className={styles.reportDate}>
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
              {/* class="btn" is intentionally unstyled — see the CSS module. */}
              <a className="btn" href={monthHref(monthNav.prevMonth)}>
                ← Previous Month
              </a>
              <a className="btn" href={monthHref(monthNav.currentMonth)}>
                Current Month
              </a>
              <a className="btn" href={monthHref(monthNav.nextMonth)}>
                Next Month →
              </a>
            </div>
          </div>
        </div>

        <div className={styles.heroGrid}>
          <div className={`${styles.panel} ${styles.heroCard}`}>
            <div className={styles.metaLabel}>Current status</div>
            <div className={styles.heroStatus}>
              {today.current_status || "off"}
            </div>

            <div className={styles.heroMeta}>
              <div className={styles.metaBox}>
                <div className={styles.metaLabel}>Attendance date</div>
                <div className={styles.metaValue}>
                  {today.attendance_date || "-"}
                </div>
              </div>
              <div className={styles.metaBox}>
                <div className={styles.metaLabel}>Late status</div>
                <div className={styles.metaValue}>
                  {today.late_status || "-"}
                </div>
              </div>
              <div className={styles.metaBox}>
                <div className={styles.metaLabel}>First login</div>
                <div className={styles.metaValue}>
                  {today.first_login_text || "-"}
                </div>
              </div>
              <div className={styles.metaBox}>
                <div className={styles.metaLabel}>Last logout</div>
                <div className={styles.metaValue}>
                  {today.last_logout_text || "-"}
                </div>
              </div>
            </div>
          </div>

          <div
            className={`${styles.panel} ${styles.heroCard} ${styles.panelHeadFlush}`}
          >
            <h2>Today focus</h2>
            <div className={styles.subcards}>
              <div className={styles.subcard}>
                <div className={styles.metaLabel}>Worked today</div>
                <div className={styles.v}>{today.worked_text || "-"}</div>
              </div>
              <div className={styles.subcard}>
                <div className={styles.metaLabel}>Break today</div>
                <div className={styles.v}>{today.break_text || "-"}</div>
              </div>
              <div className={styles.subcard}>
                <div className={styles.metaLabel}>Break count</div>
                <div className={styles.v}>{String(today.break_count || 0)}</div>
              </div>
            </div>

            <div className={styles.kv}>
              <KV label="Leave today">{today.leave_today ? "Yes" : "No"}</KV>
              <KV label="Flags">
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
              </KV>
            </div>
          </div>
        </div>

        <div className={styles.cards}>
          {[
            ["Present days", String(monthly.presentDays || 0)],
            ["Leave entries", String(monthly.leaveDays || 0)],
            ["Late joins", String(monthly.lateJoins || 0)],
            ["Avg login", monthly.avgLoginTimeText || "-"],
            ["Avg break", formatDurationMinutes(monthly.avgBreakMin || 0)],
            ["Corrections", String(monthly.managerCorrectionCount || 0)],
          ].map(([label, value]) => (
            <div className={styles.card} key={label}>
              <div className={styles.cardLabel}>{label}</div>
              <h2>{value}</h2>
            </div>
          ))}
        </div>

        <AttendanceTabs
          tabs={[
            { key: "overview", label: "Overview", content: overviewPanel },
            { key: "history", label: "History", content: historyPanel },
            { key: "leave", label: "Leave & Vacations", content: leavePanel },
            { key: "audit", label: "Audit", content: auditPanel },
          ]}
        />
      </div>
    </>
  );
}
