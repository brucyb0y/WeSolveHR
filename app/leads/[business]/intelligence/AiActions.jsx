"use client";

// AI generate/refresh actions for the lead intelligence page. Ported from the
// generateLeadAIIntelligenceNow()/generateCumulativeLeadAIIntelligenceNow()
// scripts: posts to /api/leads/:business/intelligence/generate(-cumulative)
// (dispatch shim), then router.refresh() in place of the old location.reload().

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./intelligence.module.css";

export default function AiActions({ business, timeframe, lastGeneratedText }) {
  const router = useRouter();
  const [status, setStatus] = useState(lastGeneratedText);
  const cumulative = timeframe === "cumulative";

  async function generate() {
    setStatus(
      cumulative
        ? "Generating cumulative intelligence from prior saved runs..."
        : "Generating AI intelligence...",
    );

    const url = cumulative
      ? `/api/leads/${encodeURIComponent(business)}/intelligence/generate-cumulative`
      : `/api/leads/${encodeURIComponent(business)}/intelligence/generate`;

    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: cumulative ? undefined : JSON.stringify({ timeframe }),
      });
      const json = await res.json();
      if (!json.ok) {
        const msg =
          json.error ||
          (cumulative
            ? "Failed to generate cumulative intelligence."
            : "Failed to generate AI intelligence.");
        setStatus(msg);
        alert(msg);
        return;
      }
      setStatus(
        cumulative
          ? "Cumulative intelligence generated. Refreshing..."
          : "AI intelligence generated. Refreshing...",
      );
      router.refresh();
    } catch (error) {
      const msg = cumulative
        ? "Failed to generate cumulative intelligence."
        : "Failed to generate AI intelligence.";
      setStatus(msg);
      alert(msg);
    }
  }

  return (
    <div className={styles.aiActions}>
      <button className={styles.btn} type="button" onClick={generate}>
        {cumulative
          ? "Generate / Refresh Cumulative Intelligence"
          : "Generate / Refresh AI Intelligence"}
      </button>
      <div className={styles.aiStatus}>{status}</div>
    </div>
  );
}
