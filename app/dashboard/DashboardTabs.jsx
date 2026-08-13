"use client";

// Dashboard tab switcher. Panel bodies are server-rendered and passed in as
// elements, so only the active-tab state lives here.

import { useState } from "react";
import styles from "./dashboard.module.css";

export default function DashboardTabs({ tabs }) {
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
