"use client";

// /reports?userId=... — one card per attendance-day, plus the roll-up panel.
//
// This view renders no top navigation, matching the original page.

import { useState } from "react";
import TaskDetailModal from "./TaskDetailModal";
import { TaskUpdates, ExtraWork } from "./ReportCards";
import styles from "./multiday.module.css";

export default function MultiDayView({ pageTitle, days, summary, dayCards }) {
  const [openTaskNo, setOpenTaskNo] = useState(null);

  return (
    <>
      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Multi-Day Reporting</div>
            <h1>{pageTitle}</h1>
            <div className={styles.subtitle}>
              {days === 1
                ? "Today’s attendance-day report."
                : `Last ${days} attendance-days, one section per day.`}
            </div>
          </div>
        </div>

        <div className={`${styles.panel} ${styles.summaryPanel}`}>
          <strong>Total working days:</strong> {summary.totalWorkingDays}
          <br />
          <strong>Fully updated:</strong> {summary.fullDays}
          <br />
          <strong>Partially updated:</strong> {summary.partialDays}
          <br />
          <strong>Missing:</strong> {summary.missingDays}
          <br />
          <strong>Leave days:</strong> {summary.leaveDays}
          <br />
          <strong>Off days:</strong> {summary.offDays}
        </div>

        <div className={styles.reportsStack}>
          {dayCards.map((card, i) => {
            if (!card.user) {
              return (
                <div className={styles.reportCard} key={card.reportDate ?? i}>
                  <div className={styles.reportCardHead}>
                    <div>
                      <div className={styles.reportName}>
                        {card.reportDateText}
                      </div>
                      <div className={`${styles.reportDate} muted`}>
                        No report data
                      </div>
                    </div>
                  </div>
                  <div className={styles.reportSection}>
                    <div className="muted">No updates found for this day.</div>
                  </div>
                </div>
              );
            }

            const user = card.user;

            return (
              <div className={styles.reportCard} key={card.reportDate ?? i}>
                <div className={styles.reportCardHead}>
                  <div>
                    <div className={styles.reportName}>
                      {card.reportDateText}
                    </div>
                    <div className={styles.reportDate}>{user.userName}</div>
                    <div className={styles.microMeta}>
                      {user.compactMeta || "0 touched"}
                    </div>
                    {/* .reportReason is intentionally unstyled on this view —
                        the original page never defined the rule. */}
                    <div>{user.reportReason || ""}</div>
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
                      emptyText="No task updates"
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
            );
          })}
        </div>
      </div>

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
