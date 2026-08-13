"use client";

// Department / Designation dropdown with inline save status.
//
// Same contract as before: change fires POST /api/account/profile-field with
// { field, value }, the select is disabled while in flight, and the status line
// shows "Saving…" then "Saved" or the error message.
//
// When the stored value is not one of the known options (or is unset) a
// selected "-" option is appended so nothing is lost visually until the user
// picks a real value — carried over from renderAccountFieldSelect().

import { useState } from "react";
import styles from "./account.module.css";

export default function ProfileFieldSelect({ field, options, currentValue }) {
  const known = options.includes(currentValue);
  const [value, setValue] = useState(known ? currentValue : "");
  const [status, setStatus] = useState({ text: "", tone: "" });
  const [saving, setSaving] = useState(false);

  async function save(next) {
    setValue(next);
    setStatus({ text: "Saving…", tone: "" });
    setSaving(true);

    try {
      const res = await fetch("/api/account/profile-field", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ field, value: next }),
      });
      const json = await res.json();

      if (!res.ok || !json.ok) throw new Error(json.error || "Failed to save");

      setStatus({ text: "Saved", tone: "ok" });
    } catch (err) {
      setStatus({ text: err.message || "Failed to save", tone: "err" });
    } finally {
      setSaving(false);
    }
  }

  const statusClass = [
    styles.saveStatus,
    status.tone === "ok" ? styles.saveStatusOk : "",
    status.tone === "err" ? styles.saveStatusErr : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <>
      <select
        className={styles.metaSelect}
        value={value}
        disabled={saving}
        onChange={(e) => save(e.target.value)}
      >
        {options.map((opt) => (
          <option value={opt} key={opt}>
            {opt}
          </option>
        ))}
        {known ? null : <option value="">-</option>}
      </select>
      <span className={statusClass}>{status.text}</span>
    </>
  );
}
