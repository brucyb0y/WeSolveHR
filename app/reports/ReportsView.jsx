"use client";

// /reports overview (no userId): compliance chips, per-user report cards, and
// the name search.
//
// The compliance summary and the cards arrive as props instead of being fetched
// after paint. The old page called getDailyNarrativeReport once on the server
// and then the browser hit /api/reports/summary and /api/reports/cards, each of
// which re-ran that same query and returned an HTML string that was assigned
// straight to innerHTML — three identical queries per view. It is one now.

import { useMemo, useState } from "react";
import TaskDetailModal from "./TaskDetailModal";
import { TaskUpdates, ExtraWork } from "./ReportCards";
import styles from "./reports.module.css";

const CARD_TONE = {
  missing: styles.reportCardMissing,
  partial: styles.reportCardPartial,
  off: styles.reportCardOff,
  leave: styles.reportCardLeave,
};

export default function ReportsView({ users, compliance, reportDateText }) {
  const [search, setSearch] = useState("");
  const [openTaskNo, setOpenTaskNo] = useState(null);

  const term = search.trim().toLowerCase();

  const visible = useMemo(
    () =>
      users.filter(
        (u) => !term || String(u.userName || "").toLowerCase().includes(term),
      ),
    [users, term],
  );

  const chips = [
    { title: "Fully updated", names: compliance.full },
    { title: "Partially updated", names: compliance.partial },
    { title: "Missing", names: compliance.missing },
    { title: "On leave", names: compliance.onLeave },
    { title: compliance.offTitle, names: compliance.off },
  ];

  return (
    <>
      <div className={`${styles.panel} ${styles.datePanel}`}>
        <strong>Date:</strong> {reportDateText}{" "}
        <span className="muted">(6:00 AM → next day 6:00 AM IST)</span>
      </div>

      <div className={styles.statusGrid}>
        {chips.map((chip) => (
          <div className={styles.statusChipBox} key={chip.title}>
            <div className={styles.statusChipTitle}>{chip.title}</div>
            <div className={styles.statusChipCount}>{chip.names.length}</div>
            <div className={styles.statusChipNames}>
              {chip.names.join(", ") || "None"}
            </div>
          </div>
        ))}
      </div>

      <div className={`${styles.panel} ${styles.searchPanel}`}>
        <input
          className={styles.searchInput}
          type="text"
          placeholder="Search user name"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>

      {visible.length ? (
        <div className={styles.reportsGrid}>
          {visible.map((user) => (
            <div
              className={`${styles.reportCard} ${CARD_TONE[user.reportStatus] || ""}`}
              key={user.userId}
            >
              <div className={styles.reportCardHead}>
                <div>
                  <div className={styles.reportName}>
                    <a href={`/attendance/${user.userId}`}>{user.userName}</a>
                  </div>
                  <div className={styles.reportDate}>
                    {reportDateText}
                    <a
                      href={`/reports?userId=${encodeURIComponent(user.userId)}&days=7`}
                      className={styles.miniReportLink}
                    >
                      Last 7 days
                    </a>
                  </div>
                  <div className={styles.microMeta}>
                    {user.compactMeta || "0 touched"}
                  </div>
                  <div className={styles.reportReason}>
                    {user.reportReason || ""}
                  </div>
                </div>
                <div className={styles.summaryPill}>
                  Open: {user.summary?.open ?? 0} | Blocked:{" "}
                  {user.summary?.blocked ?? 0}
                </div>
              </div>

              <div className={styles.reportSection}>
                <div className={styles.sectionTitle}>Task updates</div>
                <ul className={styles.reportList}>
                  <TaskUpdates
                    styles={styles}
                    narratives={user.taskNarratives}
                    onOpen={setOpenTaskNo}
                    emptyText="No task updates today"
                  />
                </ul>
              </div>

              <div className={styles.reportSection}>
                <div className={styles.sectionTitle}>Extra work</div>
                <ul className={styles.reportList}>
                  <ExtraWork notes={user.extraWork} />
                </ul>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className={`${styles.panel} ${styles.emptyPanel}`}>
          <div className="muted">No users found.</div>
        </div>
      )}

      {openTaskNo !== null ? (
        <TaskDetailModal
          styles={styles}
          taskNo={openTaskNo}
          onClose={() => setOpenTaskNo(null)}
        />
      ) : null}
    </>
  );
}
