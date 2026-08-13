// Documents tab — read-only, so this stays a server component.
//
// Google Drive is the real document system; this tab is a pointer to the
// client's Drive folder plus whatever rows happen to exist in client_documents.
// When no folder URL is set the page says so in the danger colour rather than
// rendering a dead link, which is the original's behaviour and the more useful
// one — a missing folder is a setup gap somebody needs to fix.

import styles from "./workspace.module.css";

export default function DocumentsTab({ client, documents }) {
  return (
    <div className={styles.panel}>
      <h2>Documents</h2>
      <div className={styles.meta} style={{ marginBottom: 12 }}>
        Main document system is Google Drive.{" "}
        {client.google_drive_folder_url ? (
          <a
            href={client.google_drive_folder_url}
            target="_blank"
            rel="noopener noreferrer"
          >
            Open Client Folder
          </a>
        ) : (
          <span style={{ color: "var(--danger)" }}>
            Google Drive folder not set
          </span>
        )}
      </div>

      {documents.length ? (
        documents.map((d) => (
          <div className={styles.item} key={d.id}>
            <div className={styles.itemTitle}>
              {d.title || d.name || "Document"}
            </div>
            <div className={styles.meta}>{d.url || "-"}</div>
          </div>
        ))
      ) : (
        <div className={styles.meta}>
          No separate documents tracked. Use the Google Drive folder.
        </div>
      )}
    </div>
  );
}
