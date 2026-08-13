"use client";

// Report tab: Daily / Week-N subviews behind a subtab bar with a week picker.
//
// BRIDGE — read this before changing it.
//
// Each subview's body is still produced as an HTML string by the server:
// renderSummaryWithGoals() plus the auto-report and lead-funnel sections from
// buildClientAutoReportSections(), which render through the arKpiCard /
// arBars / arStackedBars / arDonut / arFunnelChart SVG kit (~350 lines in
// lib/server/app.js). That kit is shared with /clients/[id]'s Report tab, so it
// is worth converting ONCE as a shared component rather than twice — and that
// conversion belongs with /clients/[id], not here.
//
// Until then the server-rendered markup is injected with
// dangerouslySetInnerHTML. It is our own server output, built from escaped
// values, and no user input reaches it unescaped — but it is the one place in
// this migration that is not real JSX, and it should not be copied as a
// pattern. The subtab switching and week picker below ARE real React.

import { useEffect, useRef, useState } from "react";
import styles from "./client-view.module.css";

export default function ReportTab({ dailyHtml, weeks }) {
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
        <div
          className={styles.reportSubview}
          dangerouslySetInnerHTML={{ __html: dailyHtml }}
        />
      ) : null}

      {weeks.map((w) =>
        view === `week${w.num}` ? (
          <div
            key={w.num}
            className={styles.reportSubview}
            dangerouslySetInnerHTML={{ __html: w.html }}
          />
        ) : null,
      )}
    </>
  );
}
