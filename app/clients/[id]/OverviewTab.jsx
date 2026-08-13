// Overview tab: client facts, services, contacts, recent updates, and the
// editable weekly AI summary + goals block.
//
// Server-rendered. The summary/goals block is still produced as HTML by
// renderSummaryWithGoals() — the same bridge documented in
// app/client-view/[token]/ReportTab.jsx. Note editable: true here (the internal
// workspace can edit goals), unlike the client-facing view.

import styles from "./workspace.module.css";

const Meta = ({ label, children }) => (
  <div className={styles.meta}>
    <strong>{label}:</strong> {children}
  </div>
);

export default function OverviewTab({
  client,
  services,
  contacts,
  updates,
  gtmAssociateNames,
  lastActivity,
  summaryAndGoals,
}) {
  return (
    <>
      <div className={styles.grid2}>
        <div className={styles.panel}>
          <h2>Overview</h2>
          <Meta label="Description">{client.description || "-"}</Meta>
          <Meta label="Start Date">{client.start_date || "-"}</Meta>
          <Meta label="Slug">{client.slug || "-"}</Meta>
          <Meta label="Account Manager">
            {client.account_manager_name || "-"}
          </Meta>
          <Meta label="Project Manager">
            {client.project_manager_name || "-"}
          </Meta>
          <Meta label="GTM Associates">{gtmAssociateNames || "-"}</Meta>
          <Meta label="Last Activity">{lastActivity || "-"}</Meta>
          <div className={styles.meta}>
            <strong>Google Drive Folder:</strong>{" "}
            {client.google_drive_folder_url ? (
              <a
                href={client.google_drive_folder_url}
                target="_blank"
                rel="noopener noreferrer"
              >
                📁 Open Client Folder
              </a>
            ) : (
              <span className={styles.notSet}>Not set</span>
            )}
          </div>
        </div>

        <div className={styles.panel}>
          <h2>Services</h2>
          {services.length ? (
            services.map((s) => (
              <div className={styles.item} key={s.name}>
                <div className={styles.itemTitle}>{s.name}</div>
              </div>
            ))
          ) : (
            <div className={styles.meta}>No services selected.</div>
          )}
        </div>
      </div>

      <div className={styles.grid2}>
        <div className={styles.panel}>
          <h2>Client Contacts</h2>
          {contacts.length ? (
            contacts.map((c) => (
              <div className={styles.item} key={c.id}>
                <div className={styles.itemTitle}>
                  {c.name || "-"} {c.is_primary ? "· Primary" : ""}
                </div>
                <div className={styles.meta}>{c.role || "-"}</div>
                <div className={styles.meta}>
                  {c.email || "-"} · {c.phone || "-"}
                </div>
              </div>
            ))
          ) : (
            <div className={styles.meta}>No contacts added.</div>
          )}
        </div>

        <div className={styles.panel}>
          <h2>Recent Updates</h2>
          {updates.length ? (
            updates.map((u) => (
              <div className={styles.item} key={u.id}>
                <div className={styles.itemTitle}>{u.title || "Update"}</div>
                <div className={styles.meta}>{u.update_text || ""}</div>
              </div>
            ))
          ) : (
            <div className={styles.meta}>No updates yet.</div>
          )}
        </div>
      </div>

      {/* Rendered element, not an HTML string — the summary and goals panels
          are components in components/charts/SummaryPanel. */}
      <div className={styles.summaryBlock}>{summaryAndGoals}</div>
    </>
  );
}
