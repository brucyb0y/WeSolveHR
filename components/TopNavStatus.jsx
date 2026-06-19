"use client";

// Live "Off / Break" status pills in the top nav. Replaces the inline IIFE in
// renderTopNav() that fetched /api/top-nav-summary and wrote the pills by hand.
// The endpoint is still served by the dispatch shim and shares the session
// cookie, so the same fetch works unchanged.

import { useEffect, useState } from "react";
import styles from "./TopNav.module.css";

function summaryLabel(prefix, count, names) {
  if ((count || 0) === 0) return `${prefix}: 0`;
  if ((count || 0) <= 2) return `${prefix}: ${names.join(", ")}`;
  return `${prefix}: ${count}`;
}

export default function TopNavStatus() {
  const [data, setData] = useState(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let alive = true;
    fetch("/api/top-nav-summary")
      .then((r) => r.json())
      .then((json) => {
        if (!alive) return;
        if (json.ok && json.data) setData(json.data);
        else setFailed(true);
      })
      .catch(() => {
        if (alive) setFailed(true);
      });
    return () => {
      alive = false;
    };
  }, []);

  if (!data && !failed) {
    return (
      <>
        <span className={`${styles.topNavPill} ${styles.loading}`}>Off: ...</span>
        <span className={`${styles.topNavPill} ${styles.loading}`}>Break: ...</span>
      </>
    );
  }

  if (failed || !data) {
    return (
      <>
        <span className={`${styles.topNavPill} ${styles.muted}`}>Off: -</span>
        <span className={`${styles.topNavPill} ${styles.muted}`}>Break: -</span>
      </>
    );
  }

  const offNames = data.offNames || [];
  const breakNames = data.breakNames || [];

  const offTitle = offNames.length ? offNames.join(", ") : "Nobody off today";
  const breakTitle = breakNames.length ? breakNames.join(", ") : "Nobody on break";

  return (
    <>
      <span className={styles.topNavPill} title={offTitle}>
        {summaryLabel("Off", data.offCount, offNames)}
      </span>
      <span className={styles.topNavPill} title={breakTitle}>
        {summaryLabel("Break", data.breakCount, breakNames)}
      </span>
    </>
  );
}
