"use client";

// The Team Work grid.
//
// Ported from the page's inline STATE-plus-innerHTML script. Behaviour is the
// same — optimistic cell writes with rollback + alert on failure, a "Saved"
// flash, confirm() before deleting a person or column, date navigation that
// refetches and rewrites the URL — with one thing deleted rather than ported:
//
//   syncHourInputs() existed because the same hour lived in two DOM inputs (the
//   grid cell and the hover-card row) that had to be written in lockstep. Both
//   now read the same `hours` state, so they cannot drift and the helper is gone.

import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import styles from "./team-work.module.css";

const TEAMS = ["LEADS", "GTM"];
const POP_HIDE_MS = 180;
const FLASH_MS = 900;

const keyOf = (memberId, colId) => `${memberId}:${colId}`;

// Matches the original fmtHours: numbers render as-is, no forced decimals.
const fmtHours = (n) => String(Number(n) || 0);

function relTime(iso) {
  const d = new Date(iso);
  const diff = (Date.now() - d.getTime()) / 1000;
  if (diff < 60) return "just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return d.toLocaleDateString();
}

// Free-typing input that only writes back on blur or Enter, and re-syncs
// whenever the stored value changes underneath it (hover-card edit, rollback,
// or a new date being loaded).
function HourInput({ value, className, onCommit }) {
  const [text, setText] = useState(value > 0 ? fmtHours(value) : "");

  useEffect(() => {
    setText(value > 0 ? fmtHours(value) : "");
  }, [value]);

  function commit() {
    const raw = String(text).trim().replace(/,/g, ".") || "0";
    const next = Number(raw);

    if (Number.isNaN(next) || next < 0) {
      setText(value > 0 ? fmtHours(value) : "");
      return;
    }

    if (next === value) {
      setText(next > 0 ? fmtHours(next) : "");
      return;
    }

    onCommit(next);
  }

  return (
    <input
      className={className}
      type="text"
      inputMode="decimal"
      placeholder="0"
      value={text}
      onChange={(e) => setText(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === "Enter") e.currentTarget.blur();
      }}
    />
  );
}

function LogItem({ log }) {
  const who = log.actor_name || "Someone";
  let main;

  if (log.action === "member_added") {
    main = (
      <>
        {who} added <strong>{log.member_name}</strong> to{" "}
        {log.detail || "the team"}
      </>
    );
  } else if (log.action === "member_removed") {
    main = (
      <>
        {who} removed <strong>{log.member_name}</strong>
      </>
    );
  } else if (log.action === "column_added") {
    main = (
      <>
        {who} added column <strong>{log.column_label}</strong>
      </>
    );
  } else if (log.action === "column_removed") {
    main = (
      <>
        {who} removed column <strong>{log.column_label}</strong>
      </>
    );
  } else {
    const oldH = Number(log.old_hours) || 0;
    const newH = Number(log.new_hours) || 0;
    const up = newH >= oldH;
    main = (
      <>
        <strong>{log.member_name}</strong>{" "}
        <span className={up ? styles.up : styles.down}>
          {up ? "increased" : "decreased"}
        </span>{" "}
        {log.column_label} hours {fmtHours(oldH)} → <strong>{fmtHours(newH)}</strong>
      </>
    );
  }

  return (
    <div className={styles.logItem}>
      <div className={styles.lMain}>{main}</div>
      <div className={styles.lMeta}>
        {relTime(log.created_at)}
        {log.work_date ? ` · ${log.work_date}` : ""}
      </div>
    </div>
  );
}

export default function TeamWorkBoard({ initialState, today }) {
  const [state, setState] = useState(initialState);
  const [flash, setFlash] = useState(false);
  const [memberModal, setMemberModal] = useState(false);
  const [colModal, setColModal] = useState(false);
  const [memberForm, setMemberForm] = useState({
    name: "",
    team: "LEADS",
    responsibility: "",
  });
  const [colLabel, setColLabel] = useState("");

  const { date, tablesMissing, columns, members, hours, logs } = state;

  const getHours = useCallback(
    (memberId, colId) => Number(hours[keyOf(memberId, colId)] ?? 0),
    [hours],
  );

  const rowTotal = (memberId) =>
    columns.reduce((sum, c) => sum + getHours(memberId, c.id), 0);
  const colTotal = (colId) =>
    members.reduce((sum, m) => sum + getHours(m.id, colId), 0);
  const grandTotal = members.reduce((sum, m) => sum + rowTotal(m.id), 0);

  const flashTimer = useRef(null);
  function flashSaved() {
    setFlash(true);
    clearTimeout(flashTimer.current);
    flashTimer.current = setTimeout(() => setFlash(false), FLASH_MS);
  }
  useEffect(() => () => clearTimeout(flashTimer.current), []);

  const setHoursValue = (memberId, colId, value) =>
    setState((s) => ({
      ...s,
      hours: { ...s.hours, [keyOf(memberId, colId)]: value },
    }));

  async function refreshLogs() {
    try {
      const res = await fetch("/api/team-work/logs");
      const json = await res.json();
      if (json.ok) setState((s) => ({ ...s, logs: json.data }));
    } catch {
      /* logs are non-critical; leave the current list in place */
    }
  }

  async function commitCell(memberId, colId, value) {
    const previous = getHours(memberId, colId);
    setHoursValue(memberId, colId, value);

    try {
      const res = await fetch("/api/team-work/hours", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          date,
          member_id: memberId,
          column_id: colId,
          hours: value,
        }),
      });
      const json = await res.json();
      if (!json.ok) throw new Error(json.error || "save failed");
      flashSaved();
      refreshLogs();
    } catch {
      setHoursValue(memberId, colId, previous);
      alert("Could not save that change.");
    }
  }

  async function commitNote(memberId, rawValue) {
    const value = String(rawValue).trim();
    const member = members.find((m) => m.id === memberId);
    if (!member || value === (member.responsibility || "")) return;

    const previous = member.responsibility || "";
    setState((s) => ({
      ...s,
      members: s.members.map((m) =>
        m.id === memberId ? { ...m, responsibility: value } : m,
      ),
    }));

    try {
      const res = await fetch(`/api/team-work/members/${memberId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ responsibility: value }),
      });
      const json = await res.json();
      if (!json.ok) throw new Error(json.error || "save failed");
      flashSaved();
    } catch {
      setState((s) => ({
        ...s,
        members: s.members.map((m) =>
          m.id === memberId ? { ...m, responsibility: previous } : m,
        ),
      }));
      alert("Could not save notes.");
    }
  }

  async function addMember() {
    const name = memberForm.name.trim();
    if (!name) {
      alert("Name is required");
      return;
    }

    try {
      const res = await fetch("/api/team-work/members", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          team: memberForm.team,
          responsibility: memberForm.responsibility.trim(),
        }),
      });
      const json = await res.json();
      if (!json.ok) throw new Error(json.error);
      setState((s) => ({ ...s, members: [...s.members, json.data] }));
      setMemberModal(false);
      refreshLogs();
    } catch {
      alert("Could not add person.");
    }
  }

  async function addColumn() {
    const label = colLabel.trim();
    if (!label) {
      alert("Label is required");
      return;
    }

    try {
      const res = await fetch("/api/team-work/columns", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ label }),
      });
      const json = await res.json();
      if (!json.ok) throw new Error(json.error);
      setState((s) => ({ ...s, columns: [...s.columns, json.data] }));
      setColModal(false);
      refreshLogs();
    } catch {
      alert("Could not add column.");
    }
  }

  async function deleteMember(id) {
    const member = members.find((m) => String(m.id) === String(id));
    if (
      !member ||
      !confirm(`Remove ${member.name}? Their hours for all dates are deleted.`)
    ) {
      return;
    }

    try {
      const res = await fetch(`/api/team-work/members/${id}`, {
        method: "DELETE",
      });
      const json = await res.json();
      if (!json.ok) throw new Error(json.error);
      setState((s) => ({
        ...s,
        members: s.members.filter((m) => String(m.id) !== String(id)),
      }));
      refreshLogs();
    } catch {
      alert("Could not remove person.");
    }
  }

  async function deleteColumn(id) {
    const column = columns.find((c) => String(c.id) === String(id));
    if (
      !column ||
      !confirm(
        `Remove column "${column.label}"? Its hours for all dates are deleted.`,
      )
    ) {
      return;
    }

    try {
      const res = await fetch(`/api/team-work/columns/${id}`, {
        method: "DELETE",
      });
      const json = await res.json();
      if (!json.ok) throw new Error(json.error);
      setState((s) => ({
        ...s,
        columns: s.columns.filter((c) => String(c.id) !== String(id)),
      }));
      refreshLogs();
    } catch {
      alert("Could not remove column.");
    }
  }

  async function loadDate(nextDate) {
    try {
      const res = await fetch(
        `/api/team-work?date=${encodeURIComponent(nextDate)}`,
      );
      const json = await res.json();
      if (!json.ok) throw new Error(json.error || "load failed");

      setState((s) => ({ ...json.data, logs: s.logs }));

      const url = new URL(window.location.href);
      url.searchParams.set("date", nextDate);
      window.history.replaceState({}, "", url.toString());
    } catch {
      alert("Could not load that date.");
    }
  }

  function shiftDate(days) {
    const d = new Date(`${date}T00:00:00Z`);
    d.setUTCDate(d.getUTCDate() + days);
    loadDate(d.toISOString().slice(0, 10));
  }

  // ---- hover card -------------------------------------------------------
  const [pop, setPop] = useState(null); // { memberId, rect }
  const [popPos, setPopPos] = useState({ left: -9999, top: -9999 });
  const popRef = useRef(null);
  const hideTimer = useRef(null);

  const cancelHide = () => clearTimeout(hideTimer.current);
  const scheduleHide = () => {
    cancelHide();
    hideTimer.current = setTimeout(() => setPop(null), POP_HIDE_MS);
  };
  useEffect(() => () => clearTimeout(hideTimer.current), []);

  // Position beside the name, flipping left / clamping up when it would
  // overflow — measured after render, as the original did.
  useLayoutEffect(() => {
    if (!pop || !popRef.current) return;

    const { rect } = pop;
    const pw = popRef.current.offsetWidth;
    const ph = popRef.current.offsetHeight;

    let left = rect.right + 10;
    if (left + pw > window.innerWidth - 8) left = rect.left - pw - 10;
    if (left < 8) left = 8;

    let top = rect.top;
    if (top + ph > window.innerHeight - 8) top = window.innerHeight - ph - 8;
    if (top < 8) top = 8;

    setPopPos({ left, top });
  }, [pop]);

  const popMember = pop ? members.find((m) => m.id === pop.memberId) : null;

  return (
    <>
      <div className={styles.topbar}>
        <div>
          <div className={styles.eyebrow}>Team Division</div>
          <h1>
            Team Work{" "}
            <span
              className={`${styles.saveFlash} ${flash ? styles.show : ""}`}
            >
              Saved
            </span>
          </h1>
          <div className={styles.subtitle}>
            Hours each person spent per project — updated manually, per day.
          </div>
        </div>
        <div className={styles.dateControls}>
          <button
            className={styles.btn}
            title="Previous day"
            onClick={() => shiftDate(-1)}
          >
            ‹
          </button>
          <input
            type="date"
            value={date}
            onChange={(e) => e.target.value && loadDate(e.target.value)}
          />
          <button
            className={styles.btn}
            title="Next day"
            onClick={() => shiftDate(1)}
          >
            ›
          </button>
          <button className={styles.btn} onClick={() => loadDate(today)}>
            Today
          </button>
        </div>
      </div>

      {tablesMissing ? (
        <div className={styles.banner}>
          The Team Work tables haven&apos;t been created yet. Run{" "}
          <strong>sql/2026-06-30-team-work.sql</strong> against this database to
          enable the page.
        </div>
      ) : null}

      <div className={styles.summary}>
        <div className={`${styles.summaryCard} ${styles.panel}`}>
          <div className={styles.sLabel}>Total hours</div>
          <div className={styles.sValue}>{fmtHours(grandTotal)}</div>
          <div className={styles.sNote}>
            {members.length} people · {columns.length} projects
          </div>
        </div>
        {columns.map((c) => {
          const total = colTotal(c.id);
          const people = members.filter((m) => getHours(m.id, c.id) > 0).length;
          return (
            <div className={`${styles.summaryCard} ${styles.panel}`} key={c.id}>
              <div className={styles.sLabel}>{c.label}</div>
              <div className={styles.sValue}>{fmtHours(total)}</div>
              <div className={styles.sNote}>
                {people} {people === 1 ? "person" : "people"}
              </div>
            </div>
          );
        })}
      </div>

      <div className={styles.layout}>
        <div>
          <div className={styles.panel}>
            <div className={styles.panelHead}>
              <h2>Daily breakdown</h2>
              <div className={styles.headActions}>
                <button
                  className={styles.btn}
                  onClick={() => {
                    setColLabel("");
                    setColModal(true);
                  }}
                >
                  + Column
                </button>
                <button
                  className={`${styles.btn} ${styles.btnPrimary}`}
                  onClick={() => {
                    setMemberForm({
                      name: "",
                      team: "LEADS",
                      responsibility: "",
                    });
                    setMemberModal(true);
                  }}
                >
                  + Person
                </button>
              </div>
            </div>

            <div className={styles.tableWrap}>
              <table className={styles.grid}>
                <thead>
                  <tr>
                    <th className={styles.memberHead}>Member</th>
                    {columns.map((c) => (
                      <th key={c.id}>
                        {c.label}{" "}
                        <button
                          className={`${styles.rowDel} ${styles.colDel}`}
                          title="Remove column"
                          onClick={() => deleteColumn(c.id)}
                        >
                          ×
                        </button>
                      </th>
                    ))}
                    <th>Total</th>
                  </tr>
                </thead>

                <tbody>
                  {TEAMS.map((team) => {
                    const rows = members.filter((m) => m.team === team);
                    return (
                      <TeamSection
                        key={team}
                        team={team}
                        rows={rows}
                        columns={columns}
                        colSpan={columns.length + 2}
                        getHours={getHours}
                        rowTotal={rowTotal}
                        onCommitCell={commitCell}
                        onDeleteMember={deleteMember}
                        onHoverName={(memberId, rect) => {
                          cancelHide();
                          setPop({ memberId, rect });
                        }}
                        onLeaveName={scheduleHide}
                      />
                    );
                  })}
                </tbody>

                <tfoot>
                  <tr>
                    <td className={styles.footLabel}>Total</td>
                    {columns.map((c) => (
                      <td key={c.id}>{fmtHours(colTotal(c.id))}</td>
                    ))}
                    <td className={styles.grand}>{fmtHours(grandTotal)}</td>
                  </tr>
                </tfoot>
              </table>
            </div>
          </div>
        </div>

        <div className={styles.panel}>
          <h2>Logs</h2>
          <div className={styles.logsList}>
            {logs && logs.length ? (
              logs.map((log, i) => <LogItem log={log} key={log.id ?? i} />)
            ) : (
              <div className={styles.empty}>No changes logged yet.</div>
            )}
          </div>
        </div>
      </div>

      {/* Hover card: editable hours + notes for one person */}
      <div
        ref={popRef}
        className={`${styles.namepop} ${pop ? styles.open : ""}`}
        style={pop ? { left: popPos.left, top: popPos.top } : undefined}
        onMouseEnter={cancelHide}
        onMouseLeave={scheduleHide}
      >
        {popMember ? (
          <>
            <div className={styles.npHead}>
              <span className={styles.npName}>{popMember.name}</span>
              <span className={styles.npTeam}>{popMember.team}</span>
            </div>
            <div className={styles.npTotal}>
              Working hours <b>{fmtHours(rowTotal(popMember.id))}</b>
            </div>
            <div className={styles.npHours}>
              {columns.map((c) => (
                <div className={styles.npHrow} key={c.id}>
                  <span>{c.label}</span>
                  <HourInput
                    value={getHours(popMember.id, c.id)}
                    onCommit={(v) => commitCell(popMember.id, c.id, v)}
                  />
                </div>
              ))}
            </div>
            <div className={styles.npNotesLabel}>Notes</div>
            <textarea
              className={styles.npNotes}
              placeholder="e.g. split across Navii & Rasset"
              defaultValue={popMember.responsibility || ""}
              key={`note-${popMember.id}`}
              onBlur={(e) => commitNote(popMember.id, e.target.value)}
            />
            <div className={styles.npHint}>Edits save automatically.</div>
          </>
        ) : null}
      </div>

      {/* Add person */}
      <Modal
        open={memberModal}
        title="Add person"
        onClose={() => setMemberModal(false)}
        onSave={addMember}
        saveLabel="Add person"
      >
        <div className={styles.field}>
          <label>Name</label>
          <input
            type="text"
            placeholder="e.g. Mehnoor"
            autoFocus
            value={memberForm.name}
            onChange={(e) =>
              setMemberForm((f) => ({ ...f, name: e.target.value }))
            }
          />
        </div>
        <div className={styles.field}>
          <label>Team</label>
          <select
            value={memberForm.team}
            onChange={(e) =>
              setMemberForm((f) => ({ ...f, team: e.target.value }))
            }
          >
            {TEAMS.map((t) => (
              <option value={t} key={t}>
                {t}
              </option>
            ))}
          </select>
        </div>
        <div className={styles.field}>
          <label>Responsibility (optional)</label>
          <input
            type="text"
            placeholder="e.g. split across Navii & Rasset"
            value={memberForm.responsibility}
            onChange={(e) =>
              setMemberForm((f) => ({ ...f, responsibility: e.target.value }))
            }
          />
        </div>
      </Modal>

      {/* Add column */}
      <Modal
        open={colModal}
        title="Add column"
        onClose={() => setColModal(false)}
        onSave={addColumn}
        saveLabel="Add column"
      >
        <div className={styles.field}>
          <label>Column label</label>
          <input
            type="text"
            placeholder="e.g. WS04"
            autoFocus
            value={colLabel}
            onChange={(e) => setColLabel(e.target.value)}
          />
        </div>
      </Modal>
    </>
  );
}

function TeamSection({
  team,
  rows,
  columns,
  colSpan,
  getHours,
  rowTotal,
  onCommitCell,
  onDeleteMember,
  onHoverName,
  onLeaveName,
}) {
  return (
    <>
      <tr className={styles.teamRow}>
        <td colSpan={colSpan}>{team}</td>
      </tr>

      {rows.length === 0 ? (
        <tr>
          <td className={styles.empty} colSpan={colSpan}>
            No one in {team} yet
          </td>
        </tr>
      ) : (
        rows.map((m) => (
          <tr key={m.id}>
            <td className={styles.nameCell}>
              <div
                className={styles.nm}
                onMouseEnter={(e) =>
                  onHoverName(m.id, e.currentTarget.getBoundingClientRect())
                }
                onMouseLeave={onLeaveName}
              >
                <button
                  className={styles.rowDel}
                  title="Remove person"
                  onClick={() => onDeleteMember(m.id)}
                >
                  ×
                </button>
                <span className={styles.nmText}>{m.name}</span>
              </div>
              {m.responsibility ? (
                <div className={styles.resp}>{m.responsibility}</div>
              ) : null}
            </td>

            {columns.map((c) => {
              const h = getHours(m.id, c.id);
              return (
                <td className={h > 0 ? styles.hasHours : ""} key={c.id}>
                  <HourInput
                    className={styles.hr}
                    value={h}
                    onCommit={(v) => onCommitCell(m.id, c.id, v)}
                  />
                </td>
              );
            })}

            <td className={styles.totalCell}>{fmtHours(rowTotal(m.id))}</td>
          </tr>
        ))
      )}
    </>
  );
}

function Modal({ open, title, children, onClose, onSave, saveLabel }) {
  return (
    <div
      className={`${styles.modalOverlay} ${open ? styles.open : ""}`}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className={styles.modal}>
        <h3>{title}</h3>
        {children}
        <div className={styles.modalActions}>
          <button className={styles.btn} onClick={onClose}>
            Cancel
          </button>
          <button
            className={`${styles.btn} ${styles.btnPrimary}`}
            onClick={onSave}
          >
            {saveLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
