"use client";

// "Generate now" / "Regenerate" for an AI report summary — replaces
// regenReportSummary(). Staff-only; /client-view never renders it.
//
// `weekStart` is the Monday date string identifying WHICH week to generate, and
// is null for the daily summary. It is the storage key for that week's row, so
// omitting it on a weekly regenerate would overwrite the daily one instead.
//
// Generation is slow (it calls out to the model), so the button disables and
// reports progress rather than looking inert.

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";

export default function RegenSummaryButton({
  clientId,
  period,
  weekStart = null,
  hasContent,
}) {
  const router = useRouter();
  const [busy, setBusy] = useState(false);

  async function generate() {
    setBusy(true);
    try {
      const res = await fetch(
        `/api/clients/${clientId}/report-summary/generate`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ period, week_start: weekStart || null }),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to generate summary");
        return;
      }

      router.refresh();
    } catch {
      alert("Failed to generate summary");
    } finally {
      setBusy(false);
    }
  }

  return (
    <button
      className={styles.btn}
      type="button"
      disabled={busy}
      onClick={generate}
      style={{
        padding: "5px 12px",
        fontSize: 12,
        whiteSpace: "nowrap",
        background: "#16a34a",
        border: "1px solid #16a34a",
        color: "#fff",
      }}
    >
      {busy ? "Generating…" : hasContent ? "Regenerate" : "Generate now"}
    </button>
  );
}
