"use client";

// Live attendance board: stat cards, four tabs, and the weekly/monthly insight
// grids. Both data sources stay client-side because the page polls —
// /api/attendance every 60s and /api/attendance/insights every 5 minutes, both
// also on tab focus, exactly as before.
//
// FIXED HERE (see page.jsx note): the tab bar now actually switches tabs. The
// original markup had no #tab-overview element and closed .wrap early, so the
// overview content was never inside a tab panel — it stayed visible under every
// other tab, and the exceptions/leave/summary panels rendered outside the
// max-width container. All four panels are proper siblings now.
//
// PRESERVED: the "Late & exceptions" table still emits 7 cells beneath 9
// headers, in the original order, so its columns land under the same headings
// they do today. See EXCEPTION_COLUMNS below.

import { useCallback, useEffect, useState } from "react";
import styles from "./attendance.module.css";

const ATTENDANCE_REFRESH_MS = 60000;
const INSIGHTS_REFRESH_MS = 5 * 60000;

const TABS = [
  { key: "overview", label: "Live Overview" },
  { key: "exceptions", label: "Late & Exceptions" },
  { key: "leave", label: "Leave & No Update" },
  { key: "summary", label: "Team Summary" },
];

const LIVE_COLUMNS = [
  "Name",
  "Role",
  "Expected Login",
  "Current Status",
  "Since",
  "Worked Today",
  "Break Today",
  "First Login",
  "Late",
  "Leave",
  "Flags",
];

// Nine headers, seven cells — carried over verbatim.
const EXCEPTION_COLUMNS = [
  "Name",
  "Role",
  "Expected Login",
  "Status",
  "Worked",
  "Break",
  "First Login",
  "Late",
  "Flags",
];

const SUMMARY_COLUMNS = [
  "Name",
  "Role",
  "Status",
  "Worked",
  "Break",
  "First Login",
  "Late",
  "Flags",
];

const STAT_CARDS = [
  ["Logged in now", "logged_in_now", "Working currently"],
  ["On break now", "on_break_now", "Currently on break"],
  ["Not logged in yet", "not_logged_in_yet", "No attendance update"],
  ["On leave today", "on_leave_today", "Planned leave"],
  ["Late today", "late_today", "All late categories"],
  ["Approved late", "approved_late", "Prior info approved"],
  ["Late not approved", "unapproved_late", "Needs attention"],
  ["No prior info", "no_prior_info_late", "Joined late directly"],
  ["Long breaks", "long_break_flags", "Break exception"],
  [
    "Careless login",
    "long_shift_flags",
    "Worked above 10h, likely wrong entry",
  ],
];

const STATUS_CLASSES = {
  login: styles.statusLogin,
  back: styles.statusBack,
  break: styles.statusBreak,
  logout: styles.statusLogout,
  leave: styles.statusLeave,
  no_update: styles.statusNoUpdate,
  unknown: styles.statusUnknown,
};

function StatusPill({ status }) {
  const safe = String(status || "unknown");
  const tone = STATUS_CLASSES[safe] || "";
  return <span className={`${styles.statusPill} ${tone}`}>{safe}</span>;
}

function FlagPills({ flags }) {
  if (!flags || !flags.length) return "-";

  return flags.map((flag) => {
    let tone = styles.flagInfo;
    if (flag === "Late not approved" || flag === "Long shift") {
      tone = styles.flagDanger;
    } else if (flag === "Long break" || flag === "Time unsure") {
      tone = styles.flagWarn;
    }
    return (
      <span className={`${styles.flagPill} ${tone}`} key={flag}>
        {flag}
      </span>
    );
  });
}

function InsightCards({ cards }) {
  return cards.map((card) => (
    <div className={styles.insightCard} key={card.title}>
      <div className={styles.insightCardTitle}>{card.title}</div>
      <div className={styles.insightCardMain}>{card.main ?? "-"}</div>
      {card.lines && card.lines.length ? (
        <div className={styles.insightList}>
          {card.lines.map((line, i) => (
            <div className={styles.insightLine} key={i}>
              {line}
            </div>
          ))}
        </div>
      ) : (
        <div className={styles.insightSubtle}>No data yet</div>
      )}
    </div>
  ));
}

export default function AttendanceBoard() {
  const [tab, setTab] = useState("overview");
  const [data, setData] = useState(null);
  const [dataError, setDataError] = useState("");
  const [insights, setInsights] = useState(null);
  const [insightsError, setInsightsError] = useState("");
  const [navigating, setNavigating] = useState(false);

  const loadAttendance = useCallback(async () => {
    try {
      const res = await fetch("/api/attendance");
      const contentType = res.headers.get("content-type") || "";
      if (!contentType.includes("application/json")) {
        throw new Error("Attendance API returned HTML instead of JSON");
      }
      const json = await res.json();
      if (!json.ok) throw new Error(json.error || "Failed to load attendance");

      setData(json.data || {});
      setDataError("");
    } catch (error) {
      console.error("Attendance page load failed:", error);
      setDataError(error.message || "Failed to load");
    }
  }, []);

  const loadInsights = useCallback(async () => {
    try {
      const res = await fetch("/api/attendance/insights");
      const contentType = res.headers.get("content-type") || "";
      if (!contentType.includes("application/json")) {
        throw new Error("Attendance insights API returned HTML instead of JSON");
      }
      const json = await res.json();
      if (!json.ok) {
        throw new Error(json.error || "Failed to load attendance insights");
      }

      setInsights(json.data || {});
      setInsightsError("");
    } catch (error) {
      console.error("Attendance insights load failed:", error);
      setInsightsError(error.message || "Failed to load");
    }
  }, []);

  useEffect(() => {
    loadAttendance();
    loadInsights();

    const attendanceTimer = setInterval(loadAttendance, ATTENDANCE_REFRESH_MS);
    const insightsTimer = setInterval(loadInsights, INSIGHTS_REFRESH_MS);
    const onVisible = () => {
      if (document.visibilityState !== "visible") return;
      loadAttendance();
      loadInsights();
    };

    document.addEventListener("visibilitychange", onVisible);

    return () => {
      clearInterval(attendanceTimer);
      clearInterval(insightsTimer);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [loadAttendance, loadInsights]);

  function openAttendanceDetail(userId) {
    setNavigating(true);
    setTimeout(() => {
      window.location.href = `/attendance/${userId}`;
    }, 80);
  }

  const EmployeeLink = ({ userId, name }) => (
    <span
      className={styles.personLink}
      onClick={() => openAttendanceDetail(userId)}
    >
      {name}
    </span>
  );

  const summary = data?.summary || {};
  const groups = data?.groups || {};
  const rows = data?.rows || [];

  const carelessRows = rows.filter(
    (row) =>
      row.role !== "admin" &&
      Array.isArray(row.flags) &&
      row.flags.includes("Long shift"),
  );

  const riskOf = (r) =>
    (r.flags?.length || 0) +
    (r.late_status === "Not approved" ? 2 : 0) +
    (r.late_status === "No prior info" ? 2 : 0);

  const sortedRows = [...rows].sort((a, b) => riskOf(b) - riskOf(a));

  const exceptionRows = rows.filter(
    (row) =>
      (row.flags && row.flags.length) ||
      row.late_status === "Not approved" ||
      row.late_status === "No prior info",
  );

  const attentionItems = [];
  if ((summary.unapproved_late ?? 0) > 0) {
    attentionItems.push(`Late not approved: ${summary.unapproved_late}`);
  }
  if ((summary.no_prior_info_late ?? 0) > 0) {
    attentionItems.push(`Late without prior info: ${summary.no_prior_info_late}`);
  }
  if ((summary.long_break_flags ?? 0) > 0) {
    attentionItems.push(`Long break flags: ${summary.long_break_flags}`);
  }
  if ((summary.long_shift_flags ?? 0) > 0) {
    attentionItems.push(`Careless login: ${summary.long_shift_flags}`);
  }
  if ((summary.not_logged_in_yet ?? 0) > 0) {
    attentionItems.push(
      `No attendance update yet: ${summary.not_logged_in_yet}`,
    );
  }

  const groupBlock = (label, items, render) => (
    <div className={styles.alertItem}>
      <strong>{label}</strong>
      <br />
      {items.length
        ? items.map((x, i) => (
            <span key={x.user_id ?? i}>
              {render(x)}
              {i < items.length - 1 ? <br /> : null}
            </span>
          ))
        : "None"}
    </div>
  );

  const weeklyCards = insights
    ? [
        {
          title: "Most late this week",
          main: insights.weekly?.most_late_count_text ?? "-",
          lines: insights.weekly?.most_late_lines || [],
        },
        {
          title: "Best attendance streak",
          main: insights.weekly?.best_streak_text ?? "-",
          lines: insights.weekly?.best_streak_lines || [],
        },
        {
          title: "Most break time this week",
          main: insights.weekly?.most_break_time_text ?? "-",
          lines: insights.weekly?.most_break_time_lines || [],
        },
        {
          title: "Careless login this week",
          main: insights.weekly?.careless_login_text ?? "-",
          lines: insights.weekly?.careless_login_lines || [],
        },
      ]
    : [];

  const monthlyCards = insights
    ? [
        {
          title: "Attendance leaders",
          main: insights.monthly?.attendance_leaders_text ?? "-",
          lines: insights.monthly?.attendance_leader_lines || [],
        },
        {
          title: "Needs attention",
          main: insights.monthly?.needs_attention_text ?? "-",
          lines: insights.monthly?.needs_attention_lines || [],
        },
        {
          title: "Most late this month",
          main: insights.monthly?.most_late_text ?? "-",
          lines: insights.monthly?.most_late_lines || [],
        },
        {
          title: "Most leave this month",
          main: insights.monthly?.most_leave_text ?? "-",
          lines: insights.monthly?.most_leave_lines || [],
        },
        {
          title: "Careless login this month",
          main: insights.monthly?.careless_login_text ?? "-",
          lines: insights.monthly?.careless_login_lines || [],
        },
      ]
    : [];

  const panelClass = (key) =>
    `${styles.tabPanel} ${tab === key ? styles.active : ""}`;

  return (
    <>
      <div className={styles.stats}>
        {dataError ? (
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Error</div>
            <div className={styles.statValue}>!</div>
            <div className={styles.statNote}>{dataError}</div>
          </div>
        ) : !data ? (
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Loading</div>
            <div className={styles.statValue}>...</div>
            <div className={styles.statNote}>Fetching attendance</div>
          </div>
        ) : (
          STAT_CARDS.map(([label, key, note]) => (
            <div className={styles.statCard} key={key}>
              <div className={styles.statLabel}>{label}</div>
              <div className={styles.statValue}>{summary[key] ?? 0}</div>
              <div className={styles.statNote}>{note}</div>
            </div>
          ))
        )}
      </div>

      <div className={styles.tabbar}>
        {TABS.map((t) => (
          <button
            className={`${styles.tabBtn} ${tab === t.key ? styles.active : ""}`}
            key={t.key}
            onClick={() => setTab(t.key)}
          >
            {t.label}
          </button>
        ))}
      </div>

      <div className={panelClass("overview")}>
        <div className={styles.grid3}>
          <div className={styles.panel}>
            <h2>Needs attention now</h2>
            <div className={styles.alertList}>
              {dataError ? (
                <div className={styles.alertItem}>Failed to load attendance</div>
              ) : !data ? (
                <div className={styles.loadingState}>Loading...</div>
              ) : attentionItems.length ? (
                attentionItems.map((item) => (
                  <div className={styles.alertItem} key={item}>
                    {item}
                  </div>
                ))
              ) : (
                <div className={styles.alertItem}>
                  No immediate issues right now
                </div>
              )}
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Careless login</h2>
            <div className={styles.alertList}>
              {dataError ? (
                <div className={styles.alertItem}>Failed to load attendance</div>
              ) : !data ? (
                <div className={styles.loadingState}>Loading...</div>
              ) : carelessRows.length ? (
                carelessRows.map((row) => (
                  <div className={styles.alertItem} key={row.user_id}>
                    <strong>
                      <EmployeeLink userId={row.user_id} name={row.name} />
                    </strong>
                    <br />
                    Worked: {row.worked_today_text || "-"}
                    <br />
                    <span className="muted">
                      Likely incorrect attendance entry
                    </span>
                  </div>
                ))
              ) : (
                <div className={styles.alertItem}>
                  No careless login issues today
                </div>
              )}
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Live grouped view</h2>
            <div className={styles.alertList}>
              {dataError ? (
                <div className={styles.alertItem}>Failed to load attendance</div>
              ) : !data ? (
                <div className={styles.loadingState}>Loading...</div>
              ) : (
                <>
                  {groupBlock(
                    "On break now:",
                    groups.on_break_now || [],
                    (x) => <EmployeeLink userId={x.user_id} name={x.name} />,
                  )}
                  {groupBlock("Expected late:", groups.expected_late || [], (x) => (
                    <>
                      <EmployeeLink userId={x.user_id} name={x.name} /> (
                      {x.late_expected_login_text || "-"})
                    </>
                  ))}
                  {groupBlock(
                    "No update yet:",
                    groups.no_update_yet || [],
                    (x) => <EmployeeLink userId={x.user_id} name={x.name} />,
                  )}
                  {groupBlock(
                    "On leave today:",
                    groups.on_leave_today || [],
                    (x) => <EmployeeLink userId={x.user_id} name={x.name} />,
                  )}
                </>
              )}
            </div>
          </div>
        </div>

        <div className={styles.panel}>
          <h2>Weekly &amp; Monthly Insights</h2>

          <div className={styles.insightSection}>
            <div className={styles.insightSectionTitle}>This week</div>
            <div className={styles.insightGrid}>
              {insightsError ? (
                <div className={styles.insightCard}>
                  <div className={styles.insightCardTitle}>This week</div>
                  <div className={styles.insightCardMain}>Failed</div>
                  <div className={styles.insightSubtle}>{insightsError}</div>
                </div>
              ) : !insights ? (
                <div className={styles.loadingState}>
                  Loading weekly insights...
                </div>
              ) : (
                <InsightCards cards={weeklyCards} />
              )}
            </div>
          </div>

          <div
            className={`${styles.insightSection} ${styles.insightSectionSpaced}`}
          >
            <div className={styles.insightSectionTitle}>This month</div>
            <div className={styles.insightGrid}>
              {insightsError ? (
                <div className={styles.insightCard}>
                  <div className={styles.insightCardTitle}>This month</div>
                  <div className={styles.insightCardMain}>Failed</div>
                  <div className={styles.insightSubtle}>{insightsError}</div>
                </div>
              ) : !insights ? (
                <div className={styles.loadingState}>
                  Loading monthly insights...
                </div>
              ) : (
                <InsightCards cards={monthlyCards} />
              )}
            </div>
          </div>
        </div>

        <div className={styles.panel}>
          <h2>Live employee table</h2>
          <div className={styles.tableWrap}>
            <table>
              <thead>
                <tr>
                  {LIVE_COLUMNS.map((c) => (
                    <th key={c}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dataError ? (
                  <tr>
                    <td colSpan={10} className={styles.errorState}>
                      Failed to load attendance
                    </td>
                  </tr>
                ) : !data ? (
                  <tr>
                    <td colSpan={11} className={styles.loadingState}>
                      Loading attendance...
                    </td>
                  </tr>
                ) : sortedRows.length ? (
                  sortedRows.map((row) => (
                    <tr key={row.user_id}>
                      <td>
                        <EmployeeLink userId={row.user_id} name={row.name} />
                      </td>
                      <td>{row.role || "-"}</td>
                      <td>{row.expected_shift_start_text || "-"}</td>
                      <td>
                        <StatusPill status={row.status} />
                      </td>
                      <td>{row.since_text || "-"}</td>
                      <td>{row.worked_today_text || "-"}</td>
                      <td>{row.break_today_text || "-"}</td>
                      <td>{row.first_login_text || "-"}</td>
                      <td>{row.late_status || "-"}</td>
                      <td>{row.is_on_leave ? "Yes" : "No"}</td>
                      <td>
                        <FlagPills flags={row.flags || []} />
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={11} className="empty-cell">
                      No attendance data found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className={panelClass("exceptions")}>
        <div className={styles.panel}>
          <h2>Late &amp; exception cases</h2>
          <div className={styles.tableWrap}>
            <table>
              <thead>
                <tr>
                  {EXCEPTION_COLUMNS.map((c) => (
                    <th key={c}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dataError ? (
                  <tr>
                    <td colSpan={7} className={styles.errorState}>
                      Failed to load attendance
                    </td>
                  </tr>
                ) : !data ? (
                  <tr>
                    <td colSpan={9} className={styles.loadingState}>
                      Loading exceptions...
                    </td>
                  </tr>
                ) : exceptionRows.length ? (
                  exceptionRows.map((row) => (
                    <tr key={row.user_id}>
                      <td>
                        <EmployeeLink userId={row.user_id} name={row.name} />
                      </td>
                      <td>
                        <StatusPill status={row.status} />
                      </td>
                      <td>{row.late_status || "-"}</td>
                      <td>{row.expected_shift_start_text || "-"}</td>
                      <td>{row.worked_today_text || "-"}</td>
                      <td>{row.break_today_text || "-"}</td>
                      <td>
                        <FlagPills flags={row.flags || []} />
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={7} className="empty-cell">
                      No exceptions today
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className={panelClass("leave")}>
        <div className={styles.grid2}>
          <div className={styles.panel}>
            <h2>On leave today</h2>
            <div className={styles.alertList}>
              {dataError ? (
                <div className={styles.alertItem}>Failed to load attendance</div>
              ) : !data ? (
                <div className={styles.loadingState}>Loading...</div>
              ) : (groups.on_leave_today || []).length ? (
                groups.on_leave_today.map((x) => (
                  <div className={styles.alertItem} key={x.user_id}>
                    <EmployeeLink userId={x.user_id} name={x.name} />
                  </div>
                ))
              ) : (
                <div className={styles.alertItem}>Nobody is on leave today</div>
              )}
            </div>
          </div>

          <div className={styles.panel}>
            <h2>No update yet</h2>
            <div className={styles.alertList}>
              {dataError ? (
                <div className={styles.alertItem}>Failed to load attendance</div>
              ) : !data ? (
                <div className={styles.loadingState}>Loading...</div>
              ) : (groups.no_update_yet || []).length ? (
                groups.no_update_yet.map((x) => (
                  <div className={styles.alertItem} key={x.user_id}>
                    <EmployeeLink userId={x.user_id} name={x.name} />
                  </div>
                ))
              ) : (
                <div className={styles.alertItem}>
                  Everyone has updated attendance
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      <div className={panelClass("summary")}>
        <div className={styles.panel}>
          <h2>Team summary</h2>
          <div className={styles.tableWrap}>
            <table>
              <thead>
                <tr>
                  {SUMMARY_COLUMNS.map((c) => (
                    <th key={c}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dataError ? (
                  <tr>
                    <td colSpan={8} className={styles.errorState}>
                      Failed to load attendance
                    </td>
                  </tr>
                ) : !data ? (
                  <tr>
                    <td colSpan={8} className={styles.loadingState}>
                      Loading summary...
                    </td>
                  </tr>
                ) : rows.length ? (
                  rows.map((row) => (
                    <tr key={row.user_id}>
                      <td>
                        <EmployeeLink userId={row.user_id} name={row.name} />
                      </td>
                      <td>{row.role || "-"}</td>
                      <td>
                        <StatusPill status={row.status} />
                      </td>
                      <td>{row.worked_today_text || "-"}</td>
                      <td>{row.break_today_text || "-"}</td>
                      <td>{row.first_login_text || "-"}</td>
                      <td>{row.late_status || "-"}</td>
                      <td>
                        <FlagPills flags={row.flags || []} />
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={8} className="empty-cell">
                      No summary data
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div
        className={`${styles.loadingOverlay} ${navigating ? styles.show : ""}`}
      >
        <div className={styles.loadingCard}>
          <div className={styles.loadingSpinner} />
          <div className={styles.loadingLabel}>
            Opening attendance details...
          </div>
        </div>
      </div>
    </>
  );
}
