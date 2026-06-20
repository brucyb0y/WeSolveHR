"use client";

// Stage 0 Bug Board body. Replaces the inline createBug()/updateBug() scripts in
// renderStage0BugBoardPage(). The create form is React state; creating/updating
// posts to /api/bugs (served by the dispatch shim) then router.refresh() re-runs
// the server component to show the new board — the idiomatic stand-in for the
// original location.reload().

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./bugs.module.css";

const STAGE0_BUG_COLUMNS = [
  "Parsing",
  "Duplicate / idempotency",
  "Webhook / Twilio",
  "DB / save failure",
  "Dashboard / logs",
  "Infra / regional access",
  "Unknown",
];

const SEVERITIES = ["P0", "P1", "P2"];
const STATUSES = ["open", "in_progress", "blocked", "done"];

function severityBadge(severity) {
  if (severity === "P0") return styles.badgeDanger;
  if (severity === "P1") return styles.badgeWarn;
  return styles.badgeInfo;
}

function statusBadge(status) {
  if (status === "done") return styles.badgeOk;
  if (status === "blocked") return styles.badgeDanger;
  if (status === "in_progress") return styles.badgeInfo;
  return styles.badgeWarn;
}

const EMPTY_FORM = {
  title: "",
  description: "",
  board_column: STAGE0_BUG_COLUMNS[0],
  severity: "P0",
  source_message_sid: "",
  source_phone_number: "",
  source_message_text: "",
};

export default function BugBoard({ columns = [], users = [] }) {
  const router = useRouter();
  const [form, setForm] = useState(EMPTY_FORM);
  const [busy, setBusy] = useState(false);

  const setField = (name) => (e) =>
    setForm((f) => ({ ...f, [name]: e.target.value }));

  async function createBug() {
    if (!form.title.trim()) {
      alert("Title is required");
      return;
    }
    setBusy(true);
    try {
      const res = await fetch("/api/bugs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: form.title.trim(),
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
      setForm(EMPTY_FORM);
      router.refresh();
    } finally {
      setBusy(false);
    }
  }

  async function updateBug(id, patch) {
    const res = await fetch("/api/bugs/" + id, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    });
    const json = await res.json();
    if (!json.ok) {
      alert(json.error || "Failed to update bug");
      return;
    }
    router.refresh();
  }

  return (
    <>
      <div className={styles.panel}>
        <h2>Create bug</h2>
        <div className={styles.createGrid}>
          <input
            className={styles.control}
            placeholder="Bug title"
            value={form.title}
            onChange={setField("title")}
          />
          <select
            className={styles.control}
            value={form.board_column}
            onChange={setField("board_column")}
          >
            {STAGE0_BUG_COLUMNS.map((x) => (
              <option value={x} key={x}>
                {x}
              </option>
            ))}
          </select>
          <select
            className={styles.control}
            value={form.severity}
            onChange={setField("severity")}
          >
            {SEVERITIES.map((s) => (
              <option value={s} key={s}>
                {s}
              </option>
            ))}
          </select>
          <button
            className={styles.createBtn}
            onClick={createBug}
            disabled={busy}
          >
            Create
          </button>
        </div>

        <textarea
          className={styles.controlTextarea}
          placeholder="Description"
          value={form.description}
          onChange={setField("description")}
        />

        <div className={styles.createSourceGrid}>
          <input
            className={styles.control}
            placeholder="Source Message SID (optional)"
            value={form.source_message_sid}
            onChange={setField("source_message_sid")}
          />
          <input
            className={styles.control}
            placeholder="Source Phone (optional)"
            value={form.source_phone_number}
            onChange={setField("source_phone_number")}
          />
        </div>
        <textarea
          className={styles.controlTextarea}
          style={{ minHeight: 70, marginTop: 10 }}
          placeholder="Source message text (optional)"
          value={form.source_message_text}
          onChange={setField("source_message_text")}
        />
      </div>

      <div className={styles.board}>
        {columns.map((column) => (
          <div className={styles.boardCol} key={column.name}>
            <div className={styles.boardColHead}>
              <div className={styles.boardColTitle}>{column.name}</div>
              <div className={styles.boardColCount}>{column.count}</div>
            </div>
            <div className={styles.boardColBody}>
              {(column.items || []).length ? (
                column.items.map((bug) => (
                  <div className={styles.bugCard} key={bug.id}>
                    <div className={styles.bugTop}>
                      <div className={styles.bugId}>#{bug.id}</div>
                      <div className={styles.bugBadges}>
                        <span className={`${styles.badge} ${severityBadge(bug.severity)}`}>
                          {bug.severity}
                        </span>
                        <span className={`${styles.badge} ${statusBadge(bug.status)}`}>
                          {bug.status}
                        </span>
                      </div>
                    </div>

                    <div className={styles.bugTitle}>{bug.title}</div>

                    {bug.description ? (
                      <div className={styles.bugDesc}>{bug.description}</div>
                    ) : null}

                    <div className={styles.bugMeta}>
                      <div>
                        <strong>Assignee:</strong> {bug.assigned_to_name || "-"}
                      </div>
                      <div>
                        <strong>Created by:</strong> {bug.created_by_name || "-"}
                      </div>
                      <div>
                        <strong>Created:</strong> {bug.created_at_text || "-"}
                      </div>
                    </div>

                    {bug.source_message_sid ||
                    bug.source_phone_number ||
                    bug.source_message_text ? (
                      <div className={styles.bugSource}>
                        {bug.source_message_sid ? (
                          <div>
                            <strong>SID:</strong> {bug.source_message_sid}
                          </div>
                        ) : null}
                        {bug.source_phone_number ? (
                          <div>
                            <strong>Phone:</strong> {bug.source_phone_number}
                          </div>
                        ) : null}
                        {bug.source_message_text ? (
                          <div>
                            <strong>Message:</strong> {bug.source_message_text}
                          </div>
                        ) : null}
                      </div>
                    ) : null}

                    <div className={styles.bugActions}>
                      <select
                        className={styles.cardSelect}
                        key={`col-${bug.id}-${bug.board_column}`}
                        defaultValue={bug.board_column}
                        onChange={(e) =>
                          updateBug(bug.id, { board_column: e.target.value })
                        }
                      >
                        {STAGE0_BUG_COLUMNS.map((col) => (
                          <option value={col} key={col}>
                            {col}
                          </option>
                        ))}
                      </select>

                      <select
                        className={styles.cardSelect}
                        key={`sev-${bug.id}-${bug.severity}`}
                        defaultValue={bug.severity}
                        onChange={(e) =>
                          updateBug(bug.id, { severity: e.target.value })
                        }
                      >
                        {SEVERITIES.map((sev) => (
                          <option value={sev} key={sev}>
                            {sev}
                          </option>
                        ))}
                      </select>

                      <select
                        className={styles.cardSelect}
                        key={`st-${bug.id}-${bug.status}`}
                        defaultValue={bug.status}
                        onChange={(e) =>
                          updateBug(bug.id, { status: e.target.value })
                        }
                      >
                        {STATUSES.map((st) => (
                          <option value={st} key={st}>
                            {st}
                          </option>
                        ))}
                      </select>

                      <select
                        className={styles.cardSelect}
                        key={`as-${bug.id}-${bug.assigned_to_user_id || ""}`}
                        defaultValue={String(bug.assigned_to_user_id || "")}
                        onChange={(e) =>
                          updateBug(bug.id, {
                            assigned_to_user_id: e.target.value || null,
                          })
                        }
                      >
                        <option value="">Unassigned</option>
                        {users.map((u) => (
                          <option value={String(u.id)} key={u.id}>
                            {u.name}
                          </option>
                        ))}
                      </select>
                    </div>
                  </div>
                ))
              ) : (
                <div className={styles.emptyCol}>No bugs here</div>
              )}
            </div>
          </div>
        ))}
      </div>
    </>
  );
}
