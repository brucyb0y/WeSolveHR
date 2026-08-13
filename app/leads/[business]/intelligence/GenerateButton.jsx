"use client";

// "Generate / Refresh" button plus the status line beside it.
//
// Same two endpoints as before: the cumulative timeframe posts to
// .../intelligence/generate-cumulative with no body, everything else posts to
// .../intelligence/generate with { timeframe }. On success the page reloads.
//
// window.location.reload() is kept rather than router.refresh(): generation
// rewrites the stored AI run and the original reloaded outright, so a full
// re-read is the honest behaviour here.

import { useState } from "react";
import styles from "./intelligence.module.css";

export default function GenerateButton({ business, timeframe, lastGenerated }) {
  const isCumulative = timeframe === "cumulative";
  const [status, setStatus] = useState(
    lastGenerated ? `Last generated: ${lastGenerated}` : "Not generated yet.",
  );
  const [busy, setBusy] = useState(false);

  async function generate() {
    setBusy(true);
    setStatus(
      isCumulative
        ? "Generating cumulative intelligence from prior saved runs..."
        : "Generating AI intelligence...",
    );

    const failMessage = isCumulative
      ? "Failed to generate cumulative intelligence."
      : "Failed to generate AI intelligence.";

    try {
      const url = isCumulative
        ? `/api/leads/${encodeURIComponent(business)}/intelligence/generate-cumulative`
        : `/api/leads/${encodeURIComponent(business)}/intelligence/generate`;

      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        ...(isCumulative ? {} : { body: JSON.stringify({ timeframe }) }),
      });

      const json = await res.json();

      if (!json.ok) {
        setStatus(json.error || failMessage);
        alert(json.error || failMessage);
        return;
      }

      setStatus(
        isCumulative
          ? "Cumulative intelligence generated. Refreshing..."
          : "AI intelligence generated. Refreshing...",
      );
      window.location.reload();
    } catch {
      setStatus(failMessage);
      alert(failMessage);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className={styles.aiActions}>
      <button
        className={styles.btn}
        type="button"
        onClick={generate}
        disabled={busy}
      >
        {isCumulative
          ? "Generate / Refresh Cumulative Intelligence"
          : "Generate / Refresh AI Intelligence"}
      </button>
      <div className={styles.aiStatus}>{status}</div>
    </div>
  );
}
