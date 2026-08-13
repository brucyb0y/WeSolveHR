"use client";

// Daily / Week-N subtab bar for the Report tab.
//
// Replaces setReportView / toggleWeekMenu / initReportView and the
// display:none subview swapping. The weeks live behind a dropdown rather than
// as inline tabs because a long-running client accumulates dozens of them and
// they would wrap the bar into several lines.
//
// Two classes from the original are dropped, both of which had no CSS rule:
// `report-subview` (switching was inline display:none, now conditional
// rendering) and `report-week-label` (its text was set by JS, now state).
//
// The original supported deep links — /clients/:id?tab=report#week3 opened that
// week directly — via an initReportView IIFE reading location.hash on load.
// That is preserved here by seeding state from the hash, and kept in sync when
// the user picks a week so the URL stays shareable.

import { useEffect, useRef, useState } from "react";
import styles from "./workspace.module.css";

export default function ReportSubviews({ daily, weeks }) {
  const [view, setView] = useState("daily");
  const [menuOpen, setMenuOpen] = useState(false);
  const ddRef = useRef(null);

  // Seed from the hash on mount. Done in an effect rather than in useState so
  // the server and first client render agree — reading location during render
  // would be a hydration mismatch.
  useEffect(() => {
    const h = (window.location.hash || "").replace(/^#/, "");
    if (h === "daily" || /^week[0-9]+$/.test(h)) setView(h);
  }, []);

  useEffect(() => {
    if (!menuOpen) return undefined;
    const onDocClick = (e) => {
      if (ddRef.current && !ddRef.current.contains(e.target)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, [menuOpen]);

  const activeWeek = weeks.find((w) => `week${w.num}` === view);

  const pick = (next) => {
    setView(next);
    setMenuOpen(false);
    // Keeps the deep link honest without a navigation.
    if (typeof window !== "undefined") {
      window.history.replaceState(null, "", `#${next}`);
    }
  };

  return (
    <>
      <div className={styles.reportSubtabs} role="tablist">
        <button
          className={`${styles.reportSubtab} ${view === "daily" ? styles.active : ""}`}
          type="button"
          role="tab"
          aria-selected={view === "daily"}
          onClick={() => pick("daily")}
        >
          Daily Report
        </button>

        {weeks.length ? (
          <div className={styles.reportWeekDd} ref={ddRef}>
            <button
              type="button"
              className={`${styles.reportSubtab} ${styles.reportWeekBtn} ${
                activeWeek ? styles.active : ""
              }`}
              aria-haspopup="true"
              aria-expanded={menuOpen}
              onClick={() => setMenuOpen((v) => !v)}
            >
              <span>
                {activeWeek ? `Week ${activeWeek.displayNum}` : "Week"}
              </span>
              <span className={styles.reportWeekCaret}>▾</span>
            </button>

            {menuOpen ? (
              <div className={styles.reportWeekMenu} role="menu">
                {weeks.map((w) => (
                  <button
                    key={w.num}
                    type="button"
                    className={styles.reportWeekItem}
                    role="menuitem"
                    onClick={() => pick(`week${w.num}`)}
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
        <div>
          {daily.summary}
          {daily.activity}
          {daily.funnel}
        </div>
      ) : null}

      {weeks.map((w) =>
        view === `week${w.num}` ? (
          <div key={w.num}>
            {w.summary}
            {w.activity}
            {w.funnel}
          </div>
        ) : null,
      )}
    </>
  );
}
