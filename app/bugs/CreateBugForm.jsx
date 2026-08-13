"use client";

// "Create bug" panel. Same POST /api/bugs payload as the old createBug(); the
// seven document.getElementById reads become controlled state.

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import styles from "./bugs.module.css";

const EMPTY = {
  title: "",
  description: "",
  board_column: "",
  severity: "P0",
  source_message_sid: "",
  source_phone_number: "",
  source_message_text: "",
};

export default function CreateBugForm({ columns }) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [saving, setSaving] = useState(false);
  const [form, setForm] = useState({
    ...EMPTY,
    board_column: columns[0] || "",
  });

  const set = (key) => (e) => setForm((f) => ({ ...f, [key]: e.target.value }));

  async function createBug() {
    const title = form.title.trim();

    if (!title) {
      alert("Title is required");
      return;
    }

    setSaving(true);
    try {
      const res = await fetch("/api/bugs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title,
          description: form.description.trim(),
          board_column: form.board_column,
          severity: form.severity,
          source_message_sid: form.source_message_sid.trim(),
          source_phone_number: form.source_phone_number.trim(),
          source_message_text: form.source_message_text.trim(),
        }),
      });

      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to create bug");
        return;
      }

      setForm({ ...EMPTY, board_column: columns[0] || "" });
      startTransition(() => router.refresh());
    } catch {
      alert("Failed to create bug");
    } finally {
      setSaving(false);
    }
  }

  const busy = saving || isPending;

  return (
    <div className={`${styles.panel} ${styles.createPanel}`}>
      <h2>Create bug</h2>

      <div className={styles.createRow}>
        <input
          className={styles.field}
          placeholder="Bug title"
          value={form.title}
          onChange={set("title")}
        />
        <select
          className={styles.field}
          value={form.board_column}
          onChange={set("board_column")}
        >
          {columns.map((x) => (
            <option value={x} key={x}>
              {x}
            </option>
          ))}
        </select>
        <select
          className={styles.field}
          value={form.severity}
          onChange={set("severity")}
        >
          <option value="P0">P0</option>
          <option value="P1">P1</option>
          <option value="P2">P2</option>
        </select>
        <button className={styles.createBtn} onClick={createBug} disabled={busy}>
          {busy ? "Creating..." : "Create"}
        </button>
      </div>

      <textarea
        className={styles.textarea}
        placeholder="Description"
        value={form.description}
        onChange={set("description")}
      />

      <div className={styles.createSourceRow}>
        <input
          className={styles.field}
          placeholder="Source Message SID (optional)"
          value={form.source_message_sid}
          onChange={set("source_message_sid")}
        />
        <input
          className={styles.field}
          placeholder="Source Phone (optional)"
          value={form.source_phone_number}
          onChange={set("source_phone_number")}
        />
      </div>

      <textarea
        className={styles.textareaShort}
        placeholder="Source message text (optional)"
        value={form.source_message_text}
        onChange={set("source_message_text")}
      />
    </div>
  );
}
