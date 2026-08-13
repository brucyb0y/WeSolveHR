"use client";

// GTM Associate multiselect — the React form of renderGtmMultiselectField()
// plus its inlined GTM_MULTISELECT_JS.
//
// Same behaviour: the control shows the selected names (or a placeholder),
// clicking toggles the panel, clicking anywhere outside closes it. The
// window.__gtmMsReady guard and the global gtmToggle/document listeners are
// gone — each instance owns its own state now.
//
// The checkboxes keep name="gtm_associate_user_ids" so the Server Action reads
// them with formData.getAll() exactly as the Express handler read the repeated
// urlencoded field.

import { useEffect, useRef, useState } from "react";
import styles from "./edit-client.module.css";

export default function GtmMultiselect({ users, selectedIds }) {
  const initial = (Array.isArray(selectedIds) ? selectedIds : []).map(String);
  const [selected, setSelected] = useState(initial);
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);

  useEffect(() => {
    if (!open) return undefined;

    const onDocClick = (e) => {
      if (rootRef.current && !rootRef.current.contains(e.target)) {
        setOpen(false);
      }
    };

    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, [open]);

  function toggle(id) {
    const key = String(id);
    setSelected((current) =>
      current.includes(key)
        ? current.filter((x) => x !== key)
        : [...current, key],
    );
  }

  const names = users
    .filter((u) => selected.includes(String(u.id)))
    .map((u) => u.name || "");

  return (
    <div className={styles.field}>
      <label>GTM Associate</label>
      <div
        className={`${styles.gtmMs} ${open ? styles.open : ""}`}
        ref={rootRef}
      >
        <div
          className={styles.gtmMsControl}
          onClick={() => setOpen((v) => !v)}
        >
          <span
            className={`${styles.gtmMsText} ${names.length ? "" : styles.gtmMsPlaceholder}`}
          >
            {names.length ? names.join(", ") : "Select GTM associates"}
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
                checked={selected.includes(String(u.id))}
                onChange={() => toggle(u.id)}
              />
              <span>{u.name || ""}</span>
            </label>
          ))}
        </div>
      </div>
    </div>
  );
}
