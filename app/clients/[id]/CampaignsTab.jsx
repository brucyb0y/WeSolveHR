"use client";

// Campaigns tab — replaces campaignsTabHtml and archiveCampaign.
//
// The response-rate percentage is only appended when something was actually
// sent, matching the original: with sent = 0 the rate would read "0 (0%)",
// which reads as a measured zero rather than "nothing sent yet".

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";

const TYPE_LABELS = {
  email: "Email",
  calling: "Calling",
  linkedin: "LinkedIn",
  whatsapp: "WhatsApp",
  sms: "SMS",
  events: "Events / Webinar",
  ads: "Paid Ads",
  content: "Content / SEO",
  referral: "Referral",
  reddit: "Reddit",
  other: "Other",
};

const typeLabel = (t) => TYPE_LABELS[t] || "Email";

export default function CampaignsTab({ clientId, campaigns, onAdd, onEdit }) {
  const router = useRouter();
  const [busyId, setBusyId] = useState(null);

  const sum = (key) => campaigns.reduce((n, c) => n + (Number(c[key]) || 0), 0);

  const statusClass = (s) =>
    s === "completed"
      ? `${styles.badge} ${styles.badgeOk}`
      : s === "active"
        ? `${styles.badge} ${styles.badgeInfo}`
        : s === "paused"
          ? `${styles.badge} ${styles.badgeWarn}`
          : `${styles.badge} ${styles.badgeMuted}`;

  async function archive(id) {
    if (!confirm("Archive this campaign?")) return;

    setBusyId(id);
    try {
      const res = await fetch(`/api/clients/${clientId}/campaigns/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ archive: true }),
      });
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to archive campaign");
        return;
      }

      router.refresh();
    } catch {
      alert("Failed to archive campaign");
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div className={styles.panel}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          gap: 12,
          alignItems: "center",
          marginBottom: 14,
        }}
      >
        <div>
          <h2 style={{ margin: 0 }}>Campaigns</h2>
          <div className={styles.sectionSubtitle}>
            Email · Calling · LinkedIn · WhatsApp outreach
          </div>
        </div>
        <button
          className={`${styles.btn} ${styles.btnPrimary}`}
          type="button"
          onClick={onAdd}
        >
          + Add Campaign
        </button>
      </div>

      <div className={styles.workSummaryChips} style={{ marginBottom: 12 }}>
        <span className={styles.summaryChip}>Campaigns {campaigns.length}</span>
        <span className={styles.summaryChip}>
          Active {campaigns.filter((c) => c.status === "active").length}
        </span>
        <span className={styles.summaryChip}>
          Total sent {sum("sent_count")}
        </span>
        <span className={styles.summaryChip}>
          Total responses {sum("response_count")}
        </span>
        <span className={styles.summaryChip}>
          Positive replies {sum("positive_replies")}
        </span>
      </div>

      <div style={{ overflowX: "auto" }}>
        <table
          className={styles.workTable}
          style={{ width: "100%", borderCollapse: "collapse" }}
        >
          <thead>
            <tr>
              <th style={{ textAlign: "left" }}>Campaign</th>
              <th style={{ textAlign: "left" }}>Type</th>
              <th style={{ textAlign: "left" }}>Channel</th>
              <th style={{ textAlign: "left" }}>Status</th>
              <th style={{ textAlign: "left" }}>Sent</th>
              <th style={{ textAlign: "left" }}>Responses</th>
              <th style={{ textAlign: "left" }}>Positive</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {campaigns.length ? (
              campaigns.map((c) => {
                const sent = Number(c.sent_count) || 0;
                const responses = Number(c.response_count) || 0;
                const positive = Number(c.positive_replies) || 0;
                const rate = sent ? Math.round((responses / sent) * 100) : 0;
                return (
                  <tr key={c.id}>
                    <td>
                      <div style={{ fontWeight: 800 }}>
                        {c.name || "Untitled"}
                      </div>
                    </td>
                    <td>
                      <span className={`${styles.badge} ${styles.badgeMuted}`}>
                        {typeLabel(c.campaign_type)}
                      </span>
                    </td>
                    <td>{c.channel || "-"}</td>
                    <td>
                      <span className={statusClass(c.status)}>
                        {c.status || "planned"}
                      </span>
                    </td>
                    <td>{sent}</td>
                    <td>
                      {responses}
                      {sent ? ` (${rate}%)` : ""}
                    </td>
                    <td>{positive}</td>
                    <td>
                      <button
                        className={styles.btn}
                        type="button"
                        onClick={() => onEdit(c.id)}
                      >
                        Edit
                      </button>
                      <button
                        className={styles.btn}
                        type="button"
                        disabled={busyId === c.id}
                        onClick={() => archive(c.id)}
                      >
                        Archive
                      </button>
                    </td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td colSpan={8} className={styles.meta}>
                  No campaigns yet. Add an email, calling, LinkedIn, or WhatsApp
                  campaign.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
