// Table-based tabs of the client view: Highlighted Calls, Campaigns, Demos &
// Meetings. All server-rendered — no interactivity.
//
// Stage / demo / campaign / meeting labels are resolved on the server via
// clientLeadStatusLabel() and passed down already-formatted.

import styles from "./client-view.module.css";

function Table({ columns, rows, emptyText, renderRow }) {
  return (
    <div className={styles.tableWrap}>
      <table>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length ? (
            rows.map(renderRow)
          ) : (
            <tr>
              <td colSpan={columns.length} className={styles.meta}>
                {emptyText}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

const NameCell = ({ lead }) => (
  <td>
    <strong>{lead.company || lead.business_name || "(no name)"}</strong>
    {lead.contact_name ? (
      <div className={styles.meta}>{lead.contact_name}</div>
    ) : null}
  </td>
);

export function HighlightedCalls({ rows }) {
  return (
    <div className={styles.panel}>
      <div className={styles.panelHead}>
        <h2>Highlighted Calls</h2>
      </div>
      <div className={`${styles.meta} ${styles.panelNote}`}>
        High-quality conversations flagged by the team for your review.
      </div>
      <Table
        columns={["Company / Contact", "Stage", "Recording"]}
        rows={rows}
        emptyText="No highlighted calls shared yet."
        renderRow={(l) => (
          <tr key={l.id}>
            <NameCell lead={l} />
            <td>
              <span className={styles.badge}>{l.stageLabel}</span>
            </td>
            <td>
              <audio controls preload="none" className={styles.callRecording}>
                <source src={l.call_recording_url} />
              </audio>{" "}
              <a
                href={l.call_recording_url}
                target="_blank"
                rel="noopener noreferrer"
              >
                Open
              </a>
            </td>
          </tr>
        )}
      />
    </div>
  );
}

export function CampaignsTab({ campaigns, totals }) {
  const kpis = [
    ["Campaigns", campaigns.length],
    ["Active", campaigns.filter((c) => c.status === "active").length],
    ["Total Outreach", totals.sent],
    ["Responses", totals.responses],
    ["Positive Replies", totals.positiveReplies],
  ];

  return (
    <div className={styles.panel}>
      <div className={styles.panelHead}>
        <h2>Campaign Tracking</h2>
      </div>

      <div className={styles.kpiGrid}>
        {kpis.map(([label, value]) => (
          <div className={styles.kpiCard} key={label}>
            <div className={styles.kpiLabel}>{label}</div>
            <div className={styles.kpiValue}>{value}</div>
          </div>
        ))}
      </div>

      <Table
        columns={[
          "Campaign",
          "Type",
          "Channel",
          "Status",
          "Sent",
          "Responses",
        ]}
        rows={campaigns}
        emptyText="No campaigns shared yet."
        renderRow={(c) => {
          const sent = Number(c.sent_count) || 0;
          const responses = Number(c.response_count) || 0;
          const rate = sent ? Math.round((responses / sent) * 100) : 0;

          return (
            <tr key={c.id}>
              <td>
                <strong>{c.name || "Untitled"}</strong>
              </td>
              <td>
                <span className={styles.badge}>{c.typeLabel}</span>
              </td>
              <td>{c.channel || "-"}</td>
              <td>
                <span className={styles.badge}>{c.status || "planned"}</span>
              </td>
              <td>{sent}</td>
              <td>
                {responses}
                {sent ? ` (${rate}%)` : ""}
              </td>
            </tr>
          );
        }}
      />
    </div>
  );
}

export function MeetingsTab({ demoLeads, meetings }) {
  return (
    <>
      <div className={styles.panel}>
        <div className={styles.panelHead}>
          <h2>Demos Scheduled</h2>
        </div>
        <Table
          columns={["Company / Contact", "Demo Status", "Stage", "Owner"]}
          rows={demoLeads}
          emptyText="No demos scheduled yet."
          renderRow={(l) => (
            <tr key={l.id}>
              <NameCell lead={l} />
              <td>
                <span className={styles.badge}>{l.demoLabel}</span>
              </td>
              <td>{l.stageLabel}</td>
              <td>{l.assigned_to || "-"}</td>
            </tr>
          )}
        />
      </div>

      <div className={styles.panel}>
        <div className={styles.panelHead}>
          <h2>Sessions &amp; Strategy Calls (MOMs)</h2>
        </div>
        <Table
          columns={["Date", "Meeting", "Participants", "Summary & Next Steps"]}
          rows={meetings}
          emptyText="No sessions logged yet."
          renderRow={(m) => (
            <tr key={m.id}>
              <td>{m.meeting_date || "-"}</td>
              <td>
                <strong>{m.title || "Meeting"}</strong>
                <div className={styles.meta}>{m.typeLabel}</div>
              </td>
              <td>{m.participants || "-"}</td>
              <td>
                {m.summary ? m.summary : <span className={styles.meta}>—</span>}
                {m.action_items ? (
                  <div className={`${styles.meta} ${styles.metaSpaced}`}>
                    <strong>Action items:</strong> {m.action_items}
                  </div>
                ) : null}
                {m.next_steps ? (
                  <div className={`${styles.meta} ${styles.metaSpacedTight}`}>
                    <strong>Next:</strong> {m.next_steps}
                  </div>
                ) : null}
              </td>
            </tr>
          )}
        />
      </div>
    </>
  );
}
