// Server-rendered pieces of the client view: the header, the four stat cards,
// the Overview tab (engagement blurb / start date / services / team) and the
// Lead Funnel tab's KPI row and pipeline.
//
// No "use client" — none of this is interactive.

import { CLIENT_LEAD_PIPELINE_STAGES } from "@/lib/server/app.js";
import { statusTone, initialOf } from "@/lib/data/client-view-metrics";
import styles from "./client-view.module.css";

const TONE_CLASS = {
  ok: styles.statusOk,
  warn: styles.statusWarn,
  danger: styles.statusDanger,
  info: styles.statusInfo,
};

const NOTEBOOK_URL =
  "https://notebooklm.google.com/notebook/76c66777-16e6-447f-b6a7-d40befa08590";

export function ClientViewHeader({ client }) {
  const tone = statusTone(client.status);

  return (
    <div className={styles.topbar}>
      <div className={styles.topbarMain}>
        <div className={styles.topbarAvatar} aria-hidden="true">
          {initialOf(client.name)}
        </div>
        <div>
          <div className={styles.eyebrow}>
            <span>Client Project View</span>
            <span className={`${styles.statusBadge} ${TONE_CLASS[tone]}`}>
              <span className={styles.statusDot} />
              {client.status || "-"}
            </span>
          </div>
          <h1>{client.name || "-"}</h1>
          <div className={styles.subtitle}>{client.company_name || ""}</div>
        </div>
      </div>

      <div className={styles.topbarActions}>
        {client.google_drive_folder_url ? (
          <a
            className={styles.topbarCta}
            href={client.google_drive_folder_url}
            target="_blank"
            rel="noopener noreferrer"
          >
            <span aria-hidden="true">📁</span>Google Drive
          </a>
        ) : null}
        <a
          className={styles.topbarCta}
          href={NOTEBOOK_URL}
          target="_blank"
          rel="noopener noreferrer"
        >
          <span aria-hidden="true">📓</span>Notebook
        </a>
      </div>
    </div>
  );
}

export function ClientViewStats({
  totalLeads,
  qualifiedLeads,
  openWorkCount,
  actionCount,
}) {
  const cards = [
    ["Leads", totalLeads, styles.statInfo],
    ["Qualified+", qualifiedLeads, styles.statSuccess],
    ["Open Work", openWorkCount, styles.statWarn],
    ["Action Needed", actionCount, styles.statDanger],
  ];

  return (
    <div className={styles.stats}>
      {cards.map(([label, value, tone]) => (
        <div className={`${styles.statCard} ${tone}`} key={label}>
          <div className={styles.statLabel}>{label}</div>
          <div className={styles.statValue}>{value}</div>
        </div>
      ))}
    </div>
  );
}

export function OverviewTab({ client, services, teamMembers }) {
  const serviceNames = services.map((s) => s.name).filter(Boolean);

  return (
    <>
      <div className={styles.panel}>
        <div className={styles.panelHead}>
          <h2>Overview</h2>
        </div>
        <div className={styles.overviewGrid}>
          <div className={`${styles.overviewCard} ${styles.overviewCardWide}`}>
            <div className={styles.overviewCardBody}>
              <div className={styles.overviewCardLabel}>
                About this engagement
              </div>
              <p className={styles.overviewCardText}>
                {client.description || "Project progress and updates."}
              </p>
            </div>
          </div>

          <div className={styles.overviewCard}>
            <div className={styles.overviewCardBody}>
              <div className={styles.overviewCardLabel}>Engagement Start</div>
              <div className={styles.overviewCardValue}>
                {client.start_date || "-"}
              </div>
            </div>
          </div>

          <div className={styles.overviewCard}>
            <div className={styles.overviewCardBody}>
              <div className={styles.overviewCardLabel}>Services Engaged</div>
              <div className={styles.chipRow}>
                {serviceNames.length ? (
                  serviceNames.map((n) => (
                    <span className={styles.chip} key={n}>
                      {n}
                    </span>
                  ))
                ) : (
                  <span className={`${styles.chip} ${styles.chipMuted}`}>
                    No services listed
                  </span>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className={styles.panel}>
        <div className={styles.panelHead}>
          <h2>Engagement Team</h2>
        </div>
        {teamMembers.length ? (
          <div className={styles.teamGrid}>
            {teamMembers.map((m, i) => (
              <div className={styles.teamCard} key={`${m.name}-${i}`}>
                <div className={styles.teamAvatar} aria-hidden="true">
                  {initialOf(m.name)}
                </div>
                <div>
                  <div className={styles.teamName}>{m.name}</div>
                  <div className={styles.teamRole}>{m.role}</div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className={styles.meta}>Team details will appear here.</div>
        )}
      </div>
    </>
  );
}

export function LeadFunnel({ metrics }) {
  const {
    stageCount,
    totalLeads,
    qualifiedLeads,
    meetingLeads,
    convertedLeads,
  } = metrics;

  const kpis = [
    ["Total Leads", totalLeads],
    ["Qualified+", qualifiedLeads],
    ["Meetings", meetingLeads],
    ["Converted", convertedLeads],
  ];

  const n = CLIENT_LEAD_PIPELINE_STAGES.length;

  return (
    <div className={styles.panel}>
      <div className={styles.panelHead}>
        <h2>Lead Funnel</h2>
      </div>

      <div className={styles.kpiGrid}>
        {kpis.map(([label, value]) => (
          <div className={styles.kpiCard} key={label}>
            <div className={styles.kpiLabel}>{label}</div>
            <div className={styles.kpiValue}>{value}</div>
          </div>
        ))}
      </div>

      <div className={styles.pipeline}>
        {CLIENT_LEAD_PIPELINE_STAGES.map((s, i) => {
          // Hue progresses violet (L1) -> teal (Converted).
          const hue = Math.round(255 - (i / (n - 1)) * 105);
          return (
            <div key={s.key} style={{ display: "contents" }}>
              <div
                className={styles.pipelineStage}
                style={{
                  "--stage": `hsl(${hue} 70% 58%)`,
                  animationDelay: `${i * 55}ms`,
                }}
              >
                <div className={styles.pipelineCount}>
                  {stageCount[s.key] || 0}
                </div>
                <div className={styles.pipelineName}>{s.label}</div>
              </div>
              {i < n - 1 ? (
                <div className={styles.pipelineArrow} aria-hidden="true">
                  →
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
}
