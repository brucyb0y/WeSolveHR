"use client";

// "External Link" — mints a share token and opens the customer-facing
// /client-view/<token> page. Replaces generateClientViewLink().
//
// THE TAB IS OPENED SYNCHRONOUSLY, before the fetch. Popup blockers only allow
// window.open inside the click gesture; opening after the await resolves gets
// rejected. So an empty tab is claimed first and pointed at the URL once it
// arrives — and closed again if the request fails, rather than leaving the user
// with a stray blank tab.
//
// `noopener` is set by clearing .opener on the handle rather than by passing
// the window feature, because passing "noopener" makes window.open return null
// and there would be no handle left to navigate.

import { useState } from "react";
import styles from "./workspace.module.css";

export default function ClientViewLinkButton({ clientId }) {
  const [busy, setBusy] = useState(false);

  async function generate() {
    const newTab = window.open("", "_blank");
    if (newTab) newTab.opener = null;

    setBusy(true);
    try {
      const res = await fetch(`/api/clients/${clientId}/client-view-link`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      const json = await res.json();

      if (!json.ok) {
        if (newTab) newTab.close();
        alert(json.error || "Failed to create client link");
        return;
      }

      const url = json.data.url;
      if (newTab) {
        newTab.location.href = url;
      } else {
        // The blocker won: try once more, which at least surfaces the
        // browser's "popup blocked" affordance.
        window.open(url, "_blank", "noopener,noreferrer");
      }
    } catch {
      if (newTab) newTab.close();
      alert("Failed to create client link");
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
    >
      External Link
    </button>
  );
}
