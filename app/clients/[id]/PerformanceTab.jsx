// Performance tab — GTM velocity and inactivity alerts. Read-only, so this
// stays a server component.
//
// Every figure is derived from lead and meeting timestamps rather than stored,
// and the derivation runs in page.jsx so the "now" it measures against is the
// server's request time. Computing it here instead would make the numbers
// depend on how long a client tab had been left open.
//
// null vs 0 matters in two of these cards: `daysSinceLastLead` is null when
// there has never been a lead at all, which renders "—". Showing 0 there would
// claim a lead arrived today.

import styles from "./workspace.module.css";
import { VIS_CHIP } from "./badges";

function Kpi({ label, value }) {
  return (
    <div className={styles.kpiCard}>
      <div className={styles.kpiLabel}>{label}</div>
      <div className={styles.kpiValue}>{value}</div>
    </div>
  );
}

export default function PerformanceTab({ performance }) {
  const {
    alerts,
    leadsLast3,
    leadsLast7,
    convertedCount,
    daysSinceLastLead,
    daysSinceLastMeeting,
    totalLeads,
  } = performance;

  return (
    <div className={styles.panel}>
      <div style={{ marginBottom: 14 }}>
        <h2 style={{ margin: 0 }}>
          Performance <span className={VIS_CHIP.internal}>INTERNAL</span>
        </h2>
        <div className={styles.sectionSubtitle}>
          GTM velocity &amp; inactivity alerts (internal only)
        </div>
      </div>

      {alerts.length ? (
        <div className={styles.alertStrip}>
          {alerts.map((a) => (
            <span key={a}>⚠️ {a}</span>
          ))}
        </div>
      ) : null}

      <div className={styles.kpiGrid}>
        <Kpi label="Leads · last 3 days" value={leadsLast3} />
        <Kpi label="Leads · last 7 days" value={leadsLast7} />
        <Kpi label="Converted (total)" value={convertedCount} />
        <Kpi
          label="Days since last lead"
          value={daysSinceLastLead === null ? "—" : daysSinceLastLead}
        />
        <Kpi
          label="Days since last demo"
          value={daysSinceLastMeeting === null ? "—" : daysSinceLastMeeting}
        />
        <Kpi label="Total leads" value={totalLeads} />
      </div>
    </div>
  );
}
