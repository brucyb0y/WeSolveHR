"use client";

// "GTM Associate" multi-select. Replaces renderGtmMultiselectField() +
// GTM_MULTISELECT_JS in lib/server/app.js. The checkboxes are real form inputs
// named gtm_associate_user_ids, so when this sits inside a <form> their checked
// values submit directly (read server-side with formData.getAll(...)) — no
// hidden inputs needed. Open/close, the summary label, and outside-click close
// are React state instead of the old global document listeners.

import { useEffect, useRef, useState } from "react";
import styles from "./GtmMultiselect.module.css";

export default function GtmMultiselect({ users = [], selectedIds = [] }) {
  const initial = new Set((selectedIds || []).map(String));
  const [selected, setSelected] = useState(initial);
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);

  useEffect(() => {
    if (!open) return;
    const onDocClick = (e) => {
      if (rootRef.current && !rootRef.current.contains(e.target)) {
        setOpen(false);
      }
    };
    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, [open]);

  function toggleOption(id) {
    setSelected((prev) => {
      const next = new Set(prev);
      const key = String(id);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  const selectedNames = users
    .filter((u) => selected.has(String(u.id)))
    .map((u) => u.name || "");
  const hasSelection = selectedNames.length > 0;

  return (
    <div className={styles.field}>
      <label className={styles.fieldLabel}>GTM Associate</label>
      <div
        ref={rootRef}
        className={`${styles.gtmMs} ${open ? styles.open : ""}`}
      >
        <div
          className={styles.gtmMsControl}
          onClick={() => setOpen((o) => !o)}
          role="button"
          tabIndex={0}
        >
          <span
            className={`${styles.gtmMsText} ${
              hasSelection ? "" : styles.gtmMsPlaceholder
            }`}
          >
            {hasSelection ? selectedNames.join(", ") : "Select GTM associates"}
          </span>
          <span className={styles.gtmMsCaret}>▾</span>
        </div>
        <div className={styles.gtmMsPanel}>
          {users.map((u) => (
            <label className={styles.gtmMsOption} key={u.id}>
              <input
                type="checkbox"
                name="gtm_associate_user_ids"
                value={u.id}
                checked={selected.has(String(u.id))}
                onChange={() => toggleOption(u.id)}
              />
              <span>{u.name || ""}</span>
            </label>
          ))}
        </div>
      </div>
    </div>
  );
}
