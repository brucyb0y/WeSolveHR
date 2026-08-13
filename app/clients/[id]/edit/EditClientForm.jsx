"use client";

import { useActionState } from "react";
import { updateClientAction } from "./actions";
import GtmMultiselect from "./GtmMultiselect";
import styles from "./edit-client.module.css";

const STATUS_OPTIONS = [
  ["active", "Active"],
  ["paused", "Paused"],
  ["onboarding", "Onboarding"],
  ["completed", "Completed"],
  ["inactive", "Inactive"],
];

const HEALTH_OPTIONS = [
  ["healthy", "Healthy"],
  ["watch", "Watch"],
  ["at_risk", "At Risk"],
];

const CONTACT_FIELDS = [
  ["name", "Name"],
  ["email", "Email"],
  ["phone", "Phone"],
  ["role", "Role"],
];

function ContactGroup({ index, label, contact, spaced }) {
  return (
    <div className={`${styles.grid} ${spaced ? styles.gridSpaced : ""}`}>
      {CONTACT_FIELDS.map(([key, fieldLabel]) => (
        <div className={styles.field} key={key}>
          <label>
            {label} {fieldLabel}
          </label>
          <input
            name={`contact_${index}_${key}`}
            defaultValue={contact[key] || ""}
          />
        </div>
      ))}
    </div>
  );
}

export default function EditClientForm({ client, users, contacts }) {
  const [state, formAction, isPending] = useActionState(
    updateClientAction.bind(null, client.id),
    { error: "" },
  );

  const [primary = {}, second = {}, third = {}] = contacts;

  return (
    <form action={formAction}>
      {state?.error ? (
        <div className={styles.errorBox}>{state.error}</div>
      ) : null}

      <div className={styles.panel}>
        <h2>Basic Info</h2>

        <div className={styles.grid}>
          <div className={styles.field}>
            <label>Client Name</label>
            <input name="name" defaultValue={client.name || ""} required />
          </div>

          <div className={styles.field}>
            <label>Company Name</label>
            <input
              name="company_name"
              defaultValue={client.company_name || ""}
            />
          </div>

          <div className={styles.field}>
            <label>Slug</label>
            <input name="slug" defaultValue={client.slug || ""} />
          </div>

          <div className={styles.field}>
            <label>Google Drive Folder Link</label>
            <input
              name="google_drive_folder_url"
              defaultValue={client.google_drive_folder_url || ""}
              required
            />
          </div>

          <div className={styles.field}>
            <label>Status</label>
            <select name="status" defaultValue={client.status || "active"}>
              {STATUS_OPTIONS.map(([value, text]) => (
                <option value={value} key={value}>
                  {text}
                </option>
              ))}
            </select>
          </div>

          <div className={styles.field}>
            <label>Health</label>
            <select
              name="health_status"
              defaultValue={client.health_status || "healthy"}
            >
              {HEALTH_OPTIONS.map(([value, text]) => (
                <option value={value} key={value}>
                  {text}
                </option>
              ))}
            </select>
          </div>

          <div className={styles.field}>
            <label>Start Date</label>
            <input
              type="date"
              name="start_date"
              defaultValue={client.start_date || ""}
            />
          </div>

          <div className={styles.field}>
            <label>Account Manager</label>
            <select
              name="account_manager_user_id"
              defaultValue={String(client.account_manager_user_id || "")}
            >
              <option value="">Select account manager</option>
              {users.map((u) => (
                <option value={u.id} key={u.id}>
                  {u.name}
                </option>
              ))}
            </select>
          </div>

          <div className={styles.field}>
            <label>Project Manager</label>
            <select
              name="project_manager_user_id"
              defaultValue={String(client.project_manager_user_id || "")}
            >
              <option value="">Select project manager</option>
              {users.map((u) => (
                <option value={u.id} key={u.id}>
                  {u.name}
                </option>
              ))}
            </select>
          </div>

          <GtmMultiselect
            users={users}
            selectedIds={client.gtm_associate_user_ids}
          />
        </div>

        <div className={`${styles.field} ${styles.fieldSpaced}`}>
          <label>Description</label>
          <textarea name="description" defaultValue={client.description || ""} />
        </div>
      </div>

      <div className={styles.panel}>
        <h2>Client Contacts</h2>

        <input type="hidden" name="contact_1_id" value={primary.id || ""} />
        <input type="hidden" name="contact_2_id" value={second.id || ""} />
        <input type="hidden" name="contact_3_id" value={third.id || ""} />

        <ContactGroup index={1} label="Primary Contact" contact={primary} />
        <ContactGroup index={2} label="Contact 2" contact={second} spaced />
        <ContactGroup index={3} label="Contact 3" contact={third} spaced />
      </div>

      <div className={styles.panel}>
        <div className={styles.actions}>
          <a className={styles.btn} href={`/clients/${client.id}`}>
            Cancel
          </a>
          <button
            className={`${styles.btn} ${styles.btnPrimary}`}
            type="submit"
            disabled={isPending}
          >
            {isPending ? "Saving..." : "Save Client"}
          </button>
        </div>
      </div>
    </form>
  );
}
