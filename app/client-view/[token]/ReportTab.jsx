"use client";

// Report tab: Daily / Week-N subviews behind a subtab bar with a week picker.
//
// The charts are real components now. Each subview renders through the shared
// kit in components/charts — the SAME components /clients/[id]'s Report tab
// uses, fed by the same aggregates from buildClientAutoReportSections(). One
// renderer for both audiences is what keeps the customer-facing report honest
// against the internal one.
//
// No HTML strings remain: the AI summary and goals panels are components too,
// rendered read-only (editable={false}) so the customer never sees the
// regenerate or edit-goals controls.

import { useEffect, useRef, useState } from "react";
import styles from "./client-view.module.css";

export default function ReportTab({ daily, weeks }) {
  const [view, setView] = useState("daily");
  const [weekMenuOpen, setWeekMenuOpen] = useState(false);
  const ddRef = useRef(null);

  useEffect(() => {
    if (!weekMenuOpen) return undefined;
    const onDocClick = (e) => {
      if (ddRef.current && !ddRef.current.contains(e.target)) {
        setWeekMenuOpen(false);
      }
    };
    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, [weekMenuOpen]);

  const activeWeek = weeks.find((w) => `week${w.num}` === view);
  const weekLabel = activeWeek ? `Week ${activeWeek.displayNum}` : "Week";

  return (
    <>
      <div className={styles.reportSubtabs} role="tablist">
        <button
          type="button"
          role="tab"
          className={`${styles.reportSubtab} ${view === "daily" ? styles.active : ""}`}
          onClick={() => setView("daily")}
        >
          Daily Report
        </button>

        {weeks.length ? (
          <div className={styles.reportWeekDd} ref={ddRef}>
            <button
              type="button"
              aria-haspopup="true"
              aria-expanded={weekMenuOpen}
              className={`${styles.reportSubtab} ${styles.reportWeekBtn} ${
                view !== "daily" ? styles.active : ""
              }`}
              onClick={() => setWeekMenuOpen((v) => !v)}
            >
              <span>{weekLabel}</span>
              <span className={styles.reportWeekCaret}>▾</span>
            </button>

            {weekMenuOpen ? (
              <div className={styles.reportWeekMenu} role="menu">
                {weeks.map((w) => (
                  <button
                    type="button"
                    role="menuitem"
                    key={w.num}
                    className={styles.reportWeekItem}
                    onClick={() => {
                      setView(`week${w.num}`);
                      setWeekMenuOpen(false);
                    }}
                  >
                    Week {w.displayNum} Report
                  </button>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}
      </div>

      {view === "daily" ? (
        <div className={styles.reportSubview}>
          {daily.summary}
          {daily.activity}
          {daily.funnel}
        </div>
      ) : null}

      {weeks.map((w) =>
        view === `week${w.num}` ? (
          <div key={w.num} className={styles.reportSubview}>
            {w.summary}
            {w.activity}
            {w.funnel}
          </div>
        ) : null,
      )}
    </>
  );
}
