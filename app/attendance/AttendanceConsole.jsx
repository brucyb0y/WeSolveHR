"use client";

// Attendance overview body. Ported from the inline scripts of the GET
// /attendance handler in lib/server/app.js: fetches /api/attendance and
// /api/attendance/insights (still served by the dispatch shim), renders the
// stats, tables, grouped lists and weekly/monthly insight cards, refreshes on a
// timer + on tab focus, and opens a per-employee detail with a loading overlay.
// The "overview" content is always visible; exceptions/leave/summary are toggle
// panels — matching the original (which had no tab-overview wrapper).

import { useCallback, useEffect, useState } from "react";
import styles from "./attendance.module.css";

const STATUS_CLASS = {
  login: styles.statusLogin,
  back: styles.statusBack,
  break: styles.statusBreak,
  logout: styles.statusLogout,
  leave: styles.statusLeave,
  no_update: styles.statusNoUpdate,
  unknown: styles.statusUnknown,
};

function flagClass(flag) {
  if (flag === "Late not approved" || flag === "Long shift")
    return styles.flagDanger;
  if (flag === "Long break" || flag === "Time unsure") return styles.flagWarn;
  return styles.flagInfo;
}

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
  ["Careless login", "long_shift_flags", "Worked above 10h, likely wrong entry"],
];

const TABS = [
  { key: "overview", label: "Live Overview" },
  { key: "exceptions", label: "Late & Exceptions" },
  { key: "leave", label: "Leave & No Update" },
  { key: "summary", label: "Team Summary" },
];

function rowRisk(row) {
  return (
    (row.flags?.length || 0) +
    (row.late_status === "Not approved" ? 2 : 0) +
    (row.late_status === "No prior info" ? 2 : 0)
  );
}

export default function AttendanceConsole() {
  const [data, setData] = useState(null);
  const [dataError, setDataError] = useState("");
  const [insights, setInsights] = useState(null);
  const [insightsError, setInsightsError] = useState("");
  const [activeTab, setActiveTab] = useState("overview");
  const [overlay, setOverlay] = useState(false);

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
      if (!json.ok)
        throw new Error(json.error || "Failed to load attendance insights");
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

    const attTimer = setInterval(loadAttendance, 60000);
    const insTimer = setInterval(loadInsights, 5 * 60000);
    const onVisible = () => {
      if (document.visibilityState === "visible") {
        loadAttendance();
        loadInsights();
      }
    };
    document.addEventListener("visibilitychange", onVisible);

    return () => {
      clearInterval(attTimer);
      clearInterval(insTimer);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [loadAttendance, loadInsights]);

  function openDetail(userId) {
    setOverlay(true);
    setTimeout(() => {
      window.location.href = "/attendance/" + userId;
    }, 80);
  }

  const person = (userId, name) => (
    <span className={styles.personLink} onClick={() => openDetail(userId)}>
      {name}
    </span>
  );

  const statusPill = (status) => {
    const safe = String(status || "unknown");
    return (
      <span className={`${styles.statusPill} ${STATUS_CLASS[safe] || styles.statusUnknown}`}>
        {safe}
      </span>
    );
  };

  const flagPills = (flags) => {
    if (!flags || !flags.length) return "-";
    return flags.map((flag, i) => (
      <span className={`${styles.flagPill} ${flagClass(flag)}`} key={i}>
        {flag}
      </span>
    ));
  };

  const summary = data?.summary || {};
  const groups = data?.groups || {};
  const rows = data?.rows || [];

  const sortedRows = [...rows].sort((a, b) => rowRisk(b) - rowRisk(a));
  const exceptionRows = rows.filter(
    (r) =>
      (r.flags && r.flags.length) ||
      r.late_status === "Not approved" ||
      r.late_status === "No prior info",
  );
  const carelessRows = rows.filter(
    (r) =>
      r.role !== "admin" &&
      Array.isArray(r.flags) &&
      r.flags.includes("Long shift"),
  );

  const attentionItems = [];
  if ((summary.unapproved_late ?? 0) > 0)
    attentionItems.push("Late not approved: " + summary.unapproved_late);
  if ((summary.no_prior_info_late ?? 0) > 0)
    attentionItems.push("Late without prior info: " + summary.no_prior_info_late);
  if ((summary.long_break_flags ?? 0) > 0)
    attentionItems.push("Long break flags: " + summary.long_break_flags);
  if ((summary.long_shift_flags ?? 0) > 0)
    attentionItems.push("Careless login: " + summary.long_shift_flags);
  if ((summary.not_logged_in_yet ?? 0) > 0)
    attentionItems.push("No attendance update yet: " + summary.not_logged_in_yet);

  const weekly = insights?.weekly || {};
  const monthly = insights?.monthly || {};
  const weeklyCards = [
    { title: "Most late this week", main: weekly.most_late_count_text, lines: weekly.most_late_lines },
    { title: "Best attendance streak", main: weekly.best_streak_text, lines: weekly.best_streak_lines },
    { title: "Most break time this week", main: weekly.most_break_time_text, lines: weekly.most_break_time_lines },
    { title: "Careless login this week", main: weekly.careless_login_text, lines: weekly.careless_login_lines },
  ];
  const monthlyCards = [
    { title: "Attendance leaders", main: monthly.attendance_leaders_text, lines: monthly.attendance_leader_lines },
    { title: "Needs attention", main: monthly.needs_attention_text, lines: monthly.needs_attention_lines },
    { title: "Most late this month", main: monthly.most_late_text, lines: monthly.most_late_lines },
    { title: "Most leave this month", main: monthly.most_leave_text, lines: monthly.most_leave_lines },
    { title: "Careless login this month", main: monthly.careless_login_text, lines: monthly.careless_login_lines },
  ];

  const renderInsightCards = (cards, error) => {
    if (error) {
      return (
        <div className={styles.insightCard}>
          <div className={styles.insightCardMain}>Failed</div>
          <div className={styles.insightSubtle}>{error}</div>
        </div>
      );
    }
    return cards.map((card, i) => (
      <div className={styles.insightCard} key={i}>
        <div className={styles.insightCardTitle}>{card.title}</div>
        <div className={styles.insightCardMain}>{card.main ?? "-"}</div>
        {(card.lines || []).length ? (
          <div className={styles.insightList}>
            {card.lines.map((line, j) => (
              <div className={styles.insightLine} key={j}>
                {line}
              </div>
            ))}
          </div>
        ) : (
          <div className={styles.insightSubtle}>No data yet</div>
        )}
      </div>
    ));
  };

  return (
    <>
      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>WeSolveHR</div>
            <h1>Attendance</h1>
            <div className={styles.subtitle}>
              Team attendance overview and exceptions
            </div>
          </div>
        </div>

        <div className={styles.stats}>
          {dataError ? (
            <div className={styles.statCard}>
              <div className={styles.statLabel}>Error</div>
              <div className={styles.statValue}>!</div>
              <div className={styles.statNote}>{dataError}</div>
            </div>
          ) : (
            STAT_CARDS.map(([label, field, note]) => (
              <div className={styles.statCard} key={field}>
                <div className={styles.statLabel}>{label}</div>
                <div className={styles.statValue}>{summary[field] ?? 0}</div>
                <div className={styles.statNote}>{note}</div>
              </div>
            ))
          )}
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

        {/* Overview content is always visible (matches the original markup). */}
        <div className={styles.grid3}>
          <div className={styles.panel}>
            <h2>Needs attention now</h2>
            <div className={styles.alertList}>
              {dataError ? (
                <div className={styles.alertItem}>Failed to load attendance</div>
              ) : attentionItems.length ? (
                attentionItems.map((item, i) => (
                  <div className={styles.alertItem} key={i}>
                    {item}
                  </div>
                ))
              ) : (
                <div className={styles.alertItem}>No immediate issues right now</div>
              )}
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Careless login</h2>
            <div className={styles.alertList}>
              {dataError ? (
                <div className={styles.alertItem}>Failed to load attendance</div>
              ) : carelessRows.length ? (
                carelessRows.map((row) => (
                  <div className={styles.alertItem} key={row.user_id}>
                    <strong>{person(row.user_id, row.name)}</strong>
                    <br />
                    Worked: {row.worked_today_text || "-"}
                    <br />
                    <span className="muted">Likely incorrect attendance entry</span>
                  </div>
                ))
              ) : (
                <div className={styles.alertItem}>No careless login issues today</div>
              )}
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Live grouped view</h2>
            <div className={styles.alertList}>
              {dataError ? (
                <div className={styles.alertItem}>Failed to load attendance</div>
              ) : (
                <>
                  <div className={styles.alertItem}>
                    <strong>On break now:</strong>
                    <br />
                    {(groups.on_break_now || []).length
                      ? (groups.on_break_now || []).map((x, i) => (
                          <span key={x.user_id}>
                            {i > 0 ? <br /> : null}
                            {person(x.user_id, x.name)}
                          </span>
                        ))
                      : "None"}
                  </div>
                  <div className={styles.alertItem}>
                    <strong>Expected late:</strong>
                    <br />
                    {(groups.expected_late || []).length
                      ? (groups.expected_late || []).map((x, i) => (
                          <span key={x.user_id}>
                            {i > 0 ? <br /> : null}
                            {person(x.user_id, x.name)} (
                            {x.late_expected_login_text || "-"})
                          </span>
                        ))
                      : "None"}
                  </div>
                  <div className={styles.alertItem}>
                    <strong>No update yet:</strong>
                    <br />
                    {(groups.no_update_yet || []).length
                      ? (groups.no_update_yet || []).map((x, i) => (
                          <span key={x.user_id}>
                            {i > 0 ? <br /> : null}
                            {person(x.user_id, x.name)}
                          </span>
                        ))
                      : "None"}
                  </div>
                  <div className={styles.alertItem}>
                    <strong>On leave today:</strong>
                    <br />
                    {(groups.on_leave_today || []).length
                      ? (groups.on_leave_today || []).map((x, i) => (
                          <span key={x.user_id}>
                            {i > 0 ? <br /> : null}
                            {person(x.user_id, x.name)}
                          </span>
                        ))
                      : "None"}
                  </div>
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
              {renderInsightCards(weeklyCards, insightsError)}
            </div>
          </div>
          <div className={styles.insightSection} style={{ marginTop: 18 }}>
            <div className={styles.insightSectionTitle}>This month</div>
            <div className={styles.insightGrid}>
              {renderInsightCards(monthlyCards, insightsError)}
            </div>
          </div>
        </div>

        <div className={styles.panel}>
          <h2>Live employee table</h2>
          <div className={styles.tableWrap}>
            <table>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Role</th>
                  <th>Expected Login</th>
                  <th>Current Status</th>
                  <th>Since</th>
                  <th>Worked Today</th>
                  <th>Break Today</th>
                  <th>First Login</th>
                  <th>Late</th>
                  <th>Leave</th>
                  <th>Flags</th>
                </tr>
              </thead>
              <tbody>
                {dataError ? (
                  <tr>
                    <td colSpan={11} className={styles.errorState}>
                      Failed to load attendance
                    </td>
                  </tr>
                ) : sortedRows.length ? (
                  sortedRows.map((row) => (
                    <tr key={row.user_id}>
                      <td>{person(row.user_id, row.name)}</td>
                      <td>{row.role || "-"}</td>
                      <td>{row.expected_shift_start_text || "-"}</td>
                      <td>{statusPill(row.status)}</td>
                      <td>{row.since_text || "-"}</td>
                      <td>{row.worked_today_text || "-"}</td>
                      <td>{row.break_today_text || "-"}</td>
                      <td>{row.first_login_text || "-"}</td>
                      <td>{row.late_status || "-"}</td>
                      <td>{row.is_on_leave ? "Yes" : "No"}</td>
                      <td>{flagPills(row.flags || [])}</td>
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

        <div className={`${styles.tabPanel} ${activeTab === "exceptions" ? styles.active : ""}`}>
          <div className={styles.panel}>
            <h2>Late &amp; exception cases</h2>
            <div className={styles.tableWrap}>
              <table>
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Status</th>
                    <th>Late</th>
                    <th>Expected Login</th>
                    <th>Worked</th>
                    <th>Break</th>
                    <th>Flags</th>
                  </tr>
                </thead>
                <tbody>
                  {dataError ? (
                    <tr>
                      <td colSpan={7} className={styles.errorState}>
                        Failed to load attendance
                      </td>
                    </tr>
                  ) : exceptionRows.length ? (
                    exceptionRows.map((row) => (
                      <tr key={row.user_id}>
                        <td>{person(row.user_id, row.name)}</td>
                        <td>{statusPill(row.status)}</td>
                        <td>{row.late_status || "-"}</td>
                        <td>{row.expected_shift_start_text || "-"}</td>
                        <td>{row.worked_today_text || "-"}</td>
                        <td>{row.break_today_text || "-"}</td>
                        <td>{flagPills(row.flags || [])}</td>
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

        <div className={`${styles.tabPanel} ${activeTab === "leave" ? styles.active : ""}`}>
          <div className={styles.grid2}>
            <div className={styles.panel}>
              <h2>On leave today</h2>
              <div className={styles.alertList}>
                {dataError ? (
                  <div className={styles.alertItem}>Failed to load attendance</div>
                ) : (groups.on_leave_today || []).length ? (
                  (groups.on_leave_today || []).map((x) => (
                    <div className={styles.alertItem} key={x.user_id}>
                      {person(x.user_id, x.name)}
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
                ) : (groups.no_update_yet || []).length ? (
                  (groups.no_update_yet || []).map((x) => (
                    <div className={styles.alertItem} key={x.user_id}>
                      {person(x.user_id, x.name)}
                    </div>
                  ))
                ) : (
                  <div className={styles.alertItem}>Everyone has updated attendance</div>
                )}
              </div>
            </div>
          </div>
        </div>

        <div className={`${styles.tabPanel} ${activeTab === "summary" ? styles.active : ""}`}>
          <div className={styles.panel}>
            <h2>Team summary</h2>
            <div className={styles.tableWrap}>
              <table>
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Role</th>
                    <th>Status</th>
                    <th>Worked</th>
                    <th>Break</th>
                    <th>First Login</th>
                    <th>Late</th>
                    <th>Flags</th>
                  </tr>
                </thead>
                <tbody>
                  {dataError ? (
                    <tr>
                      <td colSpan={8} className={styles.errorState}>
                        Failed to load attendance
                      </td>
                    </tr>
                  ) : rows.length ? (
                    rows.map((row) => (
                      <tr key={row.user_id}>
                        <td>{person(row.user_id, row.name)}</td>
                        <td>{row.role || "-"}</td>
                        <td>{statusPill(row.status)}</td>
                        <td>{row.worked_today_text || "-"}</td>
                        <td>{row.break_today_text || "-"}</td>
                        <td>{row.first_login_text || "-"}</td>
                        <td>{row.late_status || "-"}</td>
                        <td>{flagPills(row.flags || [])}</td>
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
      </div>

      <div className={`${styles.loadingOverlay} ${overlay ? styles.show : ""}`}>
        <div className={styles.loadingCard}>
          <div className={styles.loadingSpinner} />
          <div style={{ fontWeight: 700 }}>Opening attendance details...</div>
        </div>
      </div>
    </>
  );
}
