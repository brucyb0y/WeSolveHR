"use client";

// Tab switcher for the employee attendance detail page.
//
// The panel bodies are server-rendered and passed in as React elements, so only
// the visibility toggle lives on the client — the tables and summaries never
// travel to the browser as data.

import { useState } from "react";
import styles from "./employee-attendance.module.css";

export default function AttendanceTabs({ tabs }) {
  const [active, setActive] = useState(tabs[0]?.key);

  return (
    <>
      <div className={styles.tabbar}>
        {tabs.map((tab) => (
          <button
            key={tab.key}
            className={`${styles.tabBtn} ${active === tab.key ? styles.active : ""}`}
            onClick={() => setActive(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {tabs.map((tab) => (
        <div
          key={tab.key}
          className={`${styles.tabPanel} ${active === tab.key ? styles.active : ""}`}
        >
          {tab.content}
        </div>
      ))}
    </>
  );
}
