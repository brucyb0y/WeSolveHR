"use client";

import { useActionState } from "react";
import { resetClientWorkspaceAction } from "./actions";
import styles from "./reset.module.css";

const RESET_OPTIONS = [
  { name: "reset_work_items", label: "Archive work items" },
  { name: "reset_updates", label: "Archive updates" },
  { name: "reset_actions", label: "Archive actions" },
  { name: "reset_contributors", label: "Archive contributors" },
  { name: "reset_milestones", label: "Archive milestones" },
  { name: "reset_documents", label: "Archive document records" },
  { name: "reset_activity_logs", label: "Archive activity logs" },
];

export default function ResetForm({ clientId }) {
  const [state, formAction, isPending] = useActionState(
    resetClientWorkspaceAction.bind(null, clientId),
    { error: "" },
  );

  return (
    <form action={formAction}>
      {state?.error ? (
        <div className={styles.errorBox}>{state.error}</div>
      ) : null}

      <div className={styles.checkList}>
        {RESET_OPTIONS.map((option) => (
          <label key={option.name}>
            <input type="checkbox" name={option.name} defaultChecked />
            {option.label}
          </label>
        ))}
      </div>

      <input
        className={styles.confirmInput}
        name="confirm_text"
        placeholder="Type RESET to confirm"
      />

      <div className={styles.actions}>
        <a className={styles.btn} href={`/clients/${clientId}`}>
          Cancel
        </a>
        <button
          className={`${styles.btn} ${styles.btnDanger}`}
          type="submit"
          disabled={isPending}
        >
          {isPending ? "Resetting..." : "Reset Selected Data"}
        </button>
      </div>
    </form>
  );
}
