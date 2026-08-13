"use client";

// Edit Goals modal — replaces openGoalsModal / closeGoalsModal / addGoalRow /
// removeGoalRow / goalRowMarkup / saveGoals.
//
// The goal rows were built with insertAdjacentHTML and read back out of the DOM
// at save time; here they are just state. Two behaviours carried over:
//   * Removing the last row re-adds an empty one, so there is always something
//     to type into.
//   * Rows where BOTH title and value are blank are dropped on save, so the
//     trailing empty row never becomes a stored goal.
//
// Rows carry a stable key rather than using the array index — with index keys,
// deleting a middle row would make React reuse the wrong input and shift the
// text the user typed up a row.

import { useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { WorkModal, Field } from "./WorkModal";
import styles from "./workspace.module.css";

export default function GoalsModal({ clientId, goals, onClose }) {
  const router = useRouter();
  const nextKey = useRef(0);
  const makeRow = (title = "", value = "") => ({
    key: nextKey.current++,
    title,
    value,
  });

  const [rows, setRows] = useState(() => {
    const items = goals?.items || [];
    return items.length
      ? items.map((g) => makeRow(g.title, g.value))
      : [makeRow()];
  });
  const [notes, setNotes] = useState(goals?.notes || "");
  const [saving, setSaving] = useState(false);

  const setRow = (key, field) => (e) =>
    setRows((rs) =>
      rs.map((r) => (r.key === key ? { ...r, [field]: e.target.value } : r)),
    );

  const addRow = () => setRows((rs) => [...rs, makeRow()]);

  const removeRow = (key) =>
    setRows((rs) => {
      const next = rs.filter((r) => r.key !== key);
      // Always keep at least one empty row so there's something to fill in.
      return next.length ? next : [makeRow()];
    });

  async function save() {
    const items = rows
      .map((r) => ({ title: r.title.trim(), value: r.value.trim() }))
      .filter((g) => g.title || g.value);

    setSaving(true);
    try {
      const res = await fetch(`/api/clients/${clientId}/goals`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ goals_json: items, notes }),
      });
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to save goals");
        return;
      }

      onClose();
      router.refresh();
    } catch {
      alert("Failed to save goals");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title="🎯 Edit Goals"
      saveLabel="Save Goals"
      saving={saving}
      onSave={save}
      onClose={onClose}
    >
      <Field label="Goals (visible to the client)" wide>
        <div>
          {rows.map((row, i) => (
            <div
              key={row.key}
              style={{
                display: "flex",
                gap: 8,
                alignItems: "center",
                marginBottom: 8,
              }}
            >
              <input
                type="text"
                placeholder="Title"
                style={{ flex: "1 1 auto" }}
                autoFocus={i === 0}
                value={row.title}
                onChange={setRow(row.key, "title")}
              />
              <input
                type="number"
                placeholder="Number"
                style={{ width: 120 }}
                value={row.value}
                onChange={setRow(row.key, "value")}
              />
              <button
                className={styles.btn}
                type="button"
                style={{ padding: "6px 10px", whiteSpace: "nowrap" }}
                onClick={() => removeRow(row.key)}
              >
                ✕
              </button>
            </div>
          ))}
        </div>
        <button
          className={styles.btn}
          type="button"
          style={{ marginTop: 4 }}
          onClick={addRow}
        >
          + Add goal
        </button>
      </Field>

      <Field label="Notes" wide>
        <textarea
          rows={6}
          placeholder="Additional notes…"
          value={notes}
          onChange={(e) => setNotes(e.target.value)}
        />
      </Field>
    </WorkModal>
  );
}
