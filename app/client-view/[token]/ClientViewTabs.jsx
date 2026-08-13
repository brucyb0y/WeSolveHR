"use client";

// The nine-tab switcher. Panel bodies are server-rendered and passed in as
// elements, so only the active-tab state is client-side — the same shape used
// on /attendance/[userId] and /dashboard.
//
// All panels stay mounted and are shown/hidden by class, matching the original
// showClientViewTab(), which toggled `.active` on pre-rendered panels.

import { useState } from "react";
import styles from "./client-view.module.css";

export default function ClientViewTabs({ tabs }) {
  const [active, setActive] = useState(tabs[0]?.key);

  return (
    <>
      <div className={styles.clientViewTabs}>
        {tabs.map((tab) => (
          <button
            key={tab.key}
            className={`${styles.clientViewTab} ${active === tab.key ? styles.active : ""}`}
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
