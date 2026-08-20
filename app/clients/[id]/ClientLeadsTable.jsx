"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Swal from "sweetalert2";
import styles from "./workspace.module.css";
const BULK_BATCH = 15;

function PhoneIcon() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z" />
    </svg>
  );
}

function SortTh({ href, label, arrow, style }) {
  return (
    <th style={{ textAlign: "left", ...style }}>
      <a
        href={href}
        style={{
          color: "inherit",
          textDecoration: "none",
          whiteSpace: "nowrap",
          cursor: "pointer",
        }}
        title={`Sort by ${label}`}
      >
        {label}
        <SortArrow arrow={arrow} />
      </a>
    </th>
  );
}

function SortArrow({ arrow }) {
  if (arrow === "asc") return <> ↑</>;
  if (arrow === "desc") return <> ↓</>;
  return (
    <>
      {" "}
      <span style={{ opacity: 0.4 }}>↕</span>
    </>
  );
}

function ReachDropdown({ lead, channels, onToggleChannel }) {
  const [open, setOpen] = useState(false);
  // column -> boolean, only for values still in flight.
  const [pending, setPending] = useState({});
  const wrapRef = useRef(null);

  useEffect(() => {
    setPending((prev) => {
      const next = {};
      for (const [col, want] of Object.entries(prev)) {
        if (!!lead[col] !== want) next[col] = want;
      }
      return Object.keys(next).length === Object.keys(prev).length
        ? prev
        : next;
    });
  }, [lead]);

  useEffect(() => {
    if (!open) return undefined;
    const onDocClick = (e) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target)) {
        setOpen(false);
      }
    };
    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, [open]);

  const valueOf = (column) =>
    column in pending ? pending[column] : !!lead[column];

  const activeLabels = channels
    .filter((c) => valueOf(c.column))
    .map((c) => c.label);
  const label = activeLabels.length ? activeLabels.join(", ") : "Not reached";

  async function toggle(channel, checked) {
    setPending((p) => ({ ...p, [channel.column]: checked }));
    const ok = await onToggleChannel(lead.id, channel, checked);
    if (!ok) {
      setPending((p) => {
        const next = { ...p };
        delete next[channel.column];
        return next;
      });
    }
  }

  return (
    <div
      style={{ position: "relative", display: "inline-block" }}
      ref={wrapRef}
    >
      <button
        type="button"
        className={styles.btn}
        onClick={() => setOpen((v) => !v)}
        style={{
          padding: "4px 10px",
          fontSize: 12,
          minWidth: 120,
          textAlign: "left",
          display: "flex",
          justifyContent: "space-between",
          gap: 6,
          alignItems: "center",
        }}
      >
        <span>{label}</span>
        <span style={{ opacity: 0.6 }}>▾</span>
      </button>
      {open ? (
        <div
          style={{
            position: "absolute",
            zIndex: 50,
            top: "calc(100% + 4px)",
            left: 0,
            background: "var(--card, #1e1e2e)",
            border: "1px solid var(--line)",
            borderRadius: 8,
            padding: 6,
            minWidth: 140,
            boxShadow: "0 8px 24px rgba(0,0,0,0.25)",
          }}
        >
          {channels.map((c) => (
            <label
              key={c.key}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 8,
                fontSize: 12,
                whiteSpace: "nowrap",
                padding: "3px 4px",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={valueOf(c.column)}
                onChange={(e) => toggle(c, e.target.checked)}
              />
              {c.label}
            </label>
          ))}
        </div>
      ) : null}
    </div>
  );
}

export default function ClientLeadsTable({
  clientId,
  leads,
  filteredIds,
  users,
  stages,
  demoStatuses,
  categoryTypes,
  reachChannels,
  sort,
  todayStr,
  onEditLead,
  onStageChange,
  onDemoChange,
  onQuickUpdate,
  onLogMeeting,
  onAddNote,
  onNotesHistory,
  onStatusHistory,
}) {
  const router = useRouter();
  const [selected, setSelected] = useState(() => new Set());
  // True once "Select all N leads" is used — the selection then covers rows
  // that are not on screen, so the count must come from filteredIds.
  const [allMatching, setAllMatching] = useState(false);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    setVisibleOverride((prev) => {
      const next = {};
      for (const [id, want] of Object.entries(prev)) {
        const row = leads.find((l) => String(l.id) === String(id));
        if (row && !!row.is_client_visible !== want) next[id] = want;
      }
      return Object.keys(next).length === Object.keys(prev).length
        ? prev
        : next;
    });
  }, [leads]);

  const pageIds = leads.map((l) => l.id);
  const selectedCount = allMatching ? filteredIds.length : selected.size;
  const selectedIds = allMatching ? filteredIds : Array.from(selected);

  function toggleOne(id, checked) {
    setAllMatching(false);
    setSelected((prev) => {
      const next = new Set(prev);
      if (checked) next.add(id);
      else next.delete(id);
      return next;
    });
  }

  function toggleAllOnPage(checked) {
    setAllMatching(false);
    setSelected(checked ? new Set(pageIds) : new Set());
  }

  function clearSelection() {
    setAllMatching(false);
    setSelected(new Set());
  }

  async function send(url, options, failMessage) {
    setBusy(true);
    try {
      const res = await fetch(url, {
        headers: { "Content-Type": "application/json" },
        ...options,
      });
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || failMessage);
        return false;
      }

      router.refresh();
      return true;
    } catch {
      alert(failMessage);
      return false;
    } finally {
      setBusy(false);
    }
  }

  const patchLead = (leadId, body, failMessage) =>
    send(
      `/api/clients/${clientId}/leads/${leadId}`,
      { method: "PATCH", body: JSON.stringify(body) },
      failMessage,
    );

  function requestStageChange(e, leadId) {
    const select = e.target;
    const value = select.value;
    const previous = select.dataset.prev ?? value;
    onStageChange(leadId, value, () => {
      select.value = previous;
    });
  }

  function requestDemoChange(e, leadId) {
    const select = e.target;
    const value = select.value;
    const previous = select.dataset.prev ?? value;
    onDemoChange(leadId, value, () => {
      select.value = previous;
    });
  }

  const [visibleOverride, setVisibleOverride] = useState({});

  const isVisible = (l) =>
    l.id in visibleOverride ? visibleOverride[l.id] : !!l.is_client_visible;

  async function toggleVisible(leadId, checked) {
    setVisibleOverride((p) => ({ ...p, [leadId]: checked }));
    const ok = await patchLead(
      leadId,
      { is_client_visible: checked },
      "Failed to update visibility",
    );
    if (!ok) {
      setVisibleOverride((p) => {
        const next = { ...p };
        delete next[leadId];
        return next;
      });
    }
  }

  const toggleChannel = (leadId, channel, checked) =>
    patchLead(
      leadId,
      { [channel.column]: checked },
      `Failed to update ${channel.label}`,
    );

  function deleteLead(leadId, company) {
    if (!confirm(`Delete lead “${company}”? This cannot be undone.`)) return;
    send(
      `/api/clients/${clientId}/leads/${leadId}`,
      { method: "DELETE" },
      "Failed to delete lead",
    );
  }

  async function runBulkLeadAction(ids, makeBody, loadingMessage) {
    let failed = 0;

    setBusy(true);
    Swal.fire({
      title: loadingMessage,
      allowOutsideClick: false,
      didOpen: () => Swal.showLoading(),
    });

    try {
      for (let i = 0; i < ids.length; i += BULK_BATCH) {
        const chunk = ids.slice(i, i + BULK_BATCH);
        const results = await Promise.all(
          chunk.map((id) =>
            fetch(`/api/clients/${clientId}/leads/${id}`, {
              method: "PATCH",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(makeBody(id)),
            })
              .then((r) => r.json())
              .then((j) => !!(j && j.ok))
              .catch(() => false),
          ),
        );
        failed += results.filter((ok) => !ok).length;

        if (ids.length > BULK_BATCH) {
          Swal.update({
            title: `${loadingMessage} (${Math.min(i + BULK_BATCH, ids.length)}/${ids.length} done)`,
          });
        }
      }
    } finally {
      Swal.close();
      setBusy(false);
    }

    if (failed) {
      await Swal.fire(
        "Partial failure",
        `${failed} of ${ids.length} lead(s) could not be updated.`,
        "warning",
      );
    }

    clearSelection();
    router.refresh();
  }

  async function promptBulkNote(count, label) {
    const res = await Swal.fire({
      title: `Update ${label} for ${count} lead(s)`,
      input: "textarea",
      inputLabel: "Note (optional) — added to every selected lead",
      inputPlaceholder: "Why is this changing?",
      showCancelButton: true,
      confirmButtonText: `Apply to ${count} lead(s)`,
      cancelButtonText: "Cancel",
      reverseButtons: true,
    });
    if (!res.isConfirmed) return null;
    return String(res.value || "").trim();
  }

  async function bulkWithNote(value, field, label) {
    const ids = selectedIds;
    if (!ids.length) return;

    const note = await promptBulkNote(ids.length, label);
    if (note === null) return;

    const body = { [field]: value };
    if (note) body.add_note = note;

    await runBulkLeadAction(
      ids,
      () => body,
      `Updating ${label} for ${ids.length} lead(s)...`,
    );
  }

  // Reached-via is stored as one boolean column per channel, so the bulk action
  // writes those columns directly. Sending a virtual `reached_via` key instead
  // (as this once did) is not on the API's light-update whitelist, so the PATCH
  // falls through to the full update, which rejects the row for missing
  // identity fields — the "Enter at least phone, company, …" error. "none"
  // clears every channel; a channel key marks just that one.
  async function bulkReachedVia(value) {
    const ids = selectedIds;
    if (!ids.length) return;
    let body;
    if (value === "none") {
      body = Object.fromEntries(reachChannels.map((c) => [c.column, false]));
    } else {
      const channel = reachChannels.find((c) => c.key === value);
      if (!channel) return;
      body = { [channel.column]: true };
    }
    await runBulkLeadAction(
      ids,
      () => body,
      `Updating reached via for ${ids.length} lead(s)...`,
    );
  }

  // "Unassigned" clears the field, so the sentinel is translated to null here —
  // writing the literal "__unassigned__" would store it as the assignee's name
  // and it would not even match the "Unassigned" filter afterwards.
  async function bulkAssign(value, field, loadingLabel) {
    const ids = selectedIds;
    if (!ids.length) return;
    const assignee = value === "__unassigned__" ? null : value;
    await runBulkLeadAction(
      ids,
      () => ({ [field]: assignee }),
      `${loadingLabel} for ${ids.length} lead(s)...`,
    );
  }

  // "__clear__" removes the category; anything else sets it. The raw sentinel
  // would be rejected by the API as an invalid category key.
  async function bulkCategory(value) {
    const ids = selectedIds;
    if (!ids.length) return;
    const category = value === "__clear__" ? null : value;
    await runBulkLeadAction(
      ids,
      () => ({ category_type: category }),
      `Setting category type for ${ids.length} lead(s)...`,
    );
  }

  async function bulkDelete() {
    const ids = selectedIds;
    if (!ids.length) return;
    if (
      !confirm(
        `Delete ${ids.length} lead${ids.length === 1 ? "" : "s"}? This cannot be undone.`,
      )
    )
      return;

    let failed = 0;
    setBusy(true);
    Swal.fire({
      title: `Deleting ${ids.length} lead(s)...`,
      allowOutsideClick: false,
      didOpen: () => Swal.showLoading(),
    });

    try {
      for (let i = 0; i < ids.length; i += BULK_BATCH) {
        const chunk = ids.slice(i, i + BULK_BATCH);
        const results = await Promise.all(
          chunk.map((id) =>
            fetch(`/api/clients/${clientId}/leads/${id}`, { method: "DELETE" })
              .then((r) => r.json())
              .then((j) => !!(j && j.ok))
              .catch(() => false),
          ),
        );
        failed += results.filter((ok) => !ok).length;
      }
    } finally {
      Swal.close();
      setBusy(false);
    }

    if (failed) {
      await Swal.fire(
        "Partial failure",
        `${failed} of ${ids.length} lead(s) could not be deleted.`,
        "warning",
      );
    }

    clearSelection();
    router.refresh();
  }

  // Bulk <select>s are commands, not state: each returns to its placeholder so
  // the same action can be repeated on a new selection.
  const runFromSelect = (e, fn) => {
    const value = e.target.value;
    e.target.value = "";
    if (!value) return;
    fn(value);
  };

  const userNames = Array.from(
    new Set((users || []).map((u) => u && u.name).filter(Boolean)),
  );

  return (
    <>
      {selectedCount ? (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            flexWrap: "wrap",
            margin: "0 0 12px",
            padding: "10px 14px",
            background: "var(--panel-strong, #11162a)",
            border: "1px solid var(--line)",
            borderRadius: 12,
          }}
        >
          <strong style={{ fontSize: 13 }}>{selectedCount} selected</strong>

          {/* Only offered when the page selection is a strict subset of the
              filtered set — otherwise it would be a no-op. */}
          {!allMatching && filteredIds.length > selected.size ? (
            <button
              className={styles.btn}
              type="button"
              onClick={() => setAllMatching(true)}
              style={{
                padding: "4px 10px",
                fontSize: 12,
                color: "#8b7cf6",
                borderColor: "rgba(139,124,246,0.5)",
              }}
            >
              Select all {filteredIds.length} leads
            </button>
          ) : null}

          <select
            className={styles.stageSelect}
            disabled={busy}
            defaultValue=""
            onChange={(e) =>
              runFromSelect(e, (v) =>
                bulkWithNote(v, "pipeline_stage", "status"),
              )
            }
          >
            <option value="">Set status…</option>
            {stages.map((s) => (
              <option value={s.key} key={s.key}>
                {s.label}
              </option>
            ))}
          </select>

          <select
            className={styles.stageSelect}
            disabled={busy}
            defaultValue=""
            onChange={(e) =>
              runFromSelect(e, (v) => bulkWithNote(v, "demo_status", "demo"))
            }
          >
            <option value="">Set demo…</option>
            {demoStatuses.map((s) => (
              <option value={s.key} key={s.key}>
                {s.label}
              </option>
            ))}
          </select>

          <select
            className={styles.stageSelect}
            disabled={busy}
            defaultValue=""
            onChange={(e) => runFromSelect(e, bulkReachedVia)}
          >
            <option value="">Set reached via…</option>
            {reachChannels.map((c) => (
              <option value={c.key} key={c.key}>
                Mark {c.label}
              </option>
            ))}
            <option value="none">Clear reached via</option>
          </select>

          <select
            className={styles.stageSelect}
            disabled={busy}
            defaultValue=""
            onChange={(e) =>
              runFromSelect(e, (v) =>
                bulkAssign(v, "phone_assigned_to", "Assigning for phone"),
              )
            }
          >
            <option value="">Assign for phone…</option>
            <option value="__unassigned__">Unassigned</option>
            {userNames.map((n) => (
              <option value={n} key={n}>
                {n}
              </option>
            ))}
          </select>

          <select
            className={styles.stageSelect}
            disabled={busy}
            defaultValue=""
            onChange={(e) =>
              runFromSelect(e, (v) =>
                bulkAssign(v, "email_assigned_to", "Assigning for email"),
              )
            }
          >
            <option value="">Assign for email…</option>
            <option value="__unassigned__">Unassigned</option>
            {userNames.map((n) => (
              <option value={n} key={n}>
                {n}
              </option>
            ))}
          </select>

          <select
            className={styles.stageSelect}
            disabled={busy}
            defaultValue=""
            onChange={(e) => runFromSelect(e, bulkCategory)}
          >
            <option value="">Set category type…</option>
            <option value="__clear__">Clear category type</option>
            {categoryTypes.map((c) => (
              <option value={c.key} key={c.key}>
                {c.label}
              </option>
            ))}
          </select>

          <button
            className={styles.btn}
            type="button"
            disabled={busy}
            onClick={() => bulkDelete()}
            style={{ color: "#ef4444", borderColor: "rgba(239,68,68,0.4)" }}
          >
            Delete selected
          </button>

          <button className={styles.btn} type="button" onClick={clearSelection}>
            Clear
          </button>
        </div>
      ) : null}

      <div style={{ overflowX: "auto" }}>
        <table
          className={styles.workTable}
          style={{ width: "100%", borderCollapse: "collapse" }}
        >
          <thead>
            <tr>
              <th style={{ width: 34, textAlign: "center" }}>
                <input
                  type="checkbox"
                  title="Select all on this page"
                  checked={
                    pageIds.length > 0 &&
                    pageIds.every((id) => selected.has(id))
                  }
                  onChange={(e) => toggleAllOnPage(e.target.checked)}
                />
              </th>
              <SortTh
                href={sort.hrefs.name}
                arrow={sort.arrows.name}
                label="Company"
                style={{ width: 200 }}
              />
              <th style={{ textAlign: "left", width: 200 }}>
                Phone / Email / Source
              </th>
              <SortTh
                href={sort.hrefs.stage}
                arrow={sort.arrows.stage}
                label="Status"
              />
              <SortTh
                href={sort.hrefs.demo}
                arrow={sort.arrows.demo}
                label="Demo"
              />
              <SortTh
                href={sort.hrefs.notes}
                arrow={sort.arrows.notes}
                label="Notes"
                style={{ width: 360 }}
              />
              <th style={{ textAlign: "left" }}>Reached Via</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {leads.length ? (
              leads.map((l) => (
                <tr key={l.id}>
                  <td
                    style={{
                      textAlign: "center",
                      verticalAlign: "top",
                      paddingTop: 12,
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={allMatching || selected.has(l.id)}
                      onChange={(e) => toggleOne(l.id, e.target.checked)}
                      aria-label={`Select ${l.company}`}
                    />
                  </td>

                  <td>
                    <div
                      style={{ display: "flex", alignItems: "center", gap: 6 }}
                    >
                      <input
                        type="checkbox"
                        title="Client visible"
                        checked={isVisible(l)}
                        onChange={(e) => toggleVisible(l.id, e.target.checked)}
                      />
                      <span
                        style={{
                          fontWeight: 800,
                          cursor: "pointer",
                          textDecoration: "underline",
                        }}
                        title="Open / Edit"
                        onClick={() => onEditLead(l.id)}
                      >
                        {l.company}
                      </span>
                      {/* A call already logged is a record, not a button — it
                          renders disabled rather than re-opening the form. */}
                      {l.is_call_made ? (
                        <span
                          aria-disabled="true"
                          title={l.callMadeTitle}
                          aria-label="Call already made"
                          style={{
                            display: "inline-flex",
                            alignItems: "center",
                            color: "#ef4444",
                            flexShrink: 0,
                            cursor: "not-allowed",
                          }}
                        >
                          <PhoneIcon />
                        </span>
                      ) : (
                        <span
                          role="button"
                          tabIndex={0}
                          title={`Log a call — status, demo, reached via & note for ${l.company}`}
                          aria-label={`Log call to ${l.company}`}
                          onClick={() => onQuickUpdate(l)}
                          style={{
                            display: "inline-flex",
                            alignItems: "center",
                            color: "var(--muted)",
                            flexShrink: 0,
                            cursor: "pointer",
                          }}
                        >
                          <PhoneIcon />
                        </span>
                      )}
                    </div>

                    {l.locationText ? (
                      <div className={styles.meta}>{l.locationText}</div>
                    ) : null}
                    <div className={styles.meta}>{l.contact_name || ""}</div>
                    <div className={styles.meta} style={{ fontSize: 11 }}>
                      <a
                        href={sort.hrefs.updated}
                        style={{
                          color: "inherit",
                          textDecoration: "none",
                          cursor: "pointer",
                        }}
                        title="Sort by Updated"
                      >
                        Updated {l.updatedText}
                        <SortArrow arrow={sort.arrows.updated} />
                      </a>
                    </div>
                    {l.callback_date ? (
                      <div
                        style={{
                          fontSize: 11,
                          marginTop: 2,
                          fontWeight: 700,
                          color:
                            l.callback_date < todayStr ? "#ef4444" : "#22c55e",
                        }}
                      >
                        Callback: {l.callbackText}
                      </div>
                    ) : null}
                  </td>

                  <td
                    style={{
                      width: 130,
                      fontSize: 12,
                      wordBreak: "break-word",
                    }}
                  >
                    <div>{l.phone || "-"}</div>
                    {l.phone_assigned_to ? (
                      <div
                        className={styles.meta}
                        style={{ fontSize: 11 }}
                        title="Assigned for phone"
                      >
                        ☎ {l.phone_assigned_to}
                      </div>
                    ) : null}
                    <div className={styles.meta}>{l.email || "-"}</div>
                    {l.email_assigned_to ? (
                      <div
                        className={styles.meta}
                        style={{ fontSize: 11 }}
                        title="Assigned for email"
                      >
                        ✉ {l.email_assigned_to}
                      </div>
                    ) : null}
                    <div className={styles.meta}>{l.lead_source || "-"}</div>
                    {l.verified_by ? (
                      <div
                        className={styles.meta}
                        style={{
                          fontSize: 11,
                          marginTop: 2,
                          color: "#22c55e",
                        }}
                        title="Verified by"
                      >
                        ✓ {l.verified_by}
                      </div>
                    ) : null}
                  </td>

                  <td>
                    {/* Uncontrolled so an inline pick sticks while its note
                        dialog is open, but keyed on the SAVED stage so it
                        remounts — and shows the new value — when the row's
                        stage changes elsewhere (e.g. the quick-update popup). */}
                    <select
                      key={l.stage}
                      className={styles.stageSelect}
                      defaultValue={l.stage}
                      onFocus={(e) => {
                        e.target.dataset.prev = e.target.value;
                      }}
                      onChange={(e) => requestStageChange(e, l.id)}
                    >
                      {stages.map((s) => (
                        <option value={s.key} key={s.key}>
                          {s.label}
                        </option>
                      ))}
                    </select>

                    {l.stageHistoryPreview.length ? (
                      <div
                        style={{
                          marginTop: 6,
                          display: "flex",
                          flexDirection: "column",
                          gap: 4,
                        }}
                      >
                        {l.stageHistoryPreview.map((h, i) => (
                          <div
                            key={i}
                            style={{
                              fontSize: 10,
                              lineHeight: 1.4,
                              color: "var(--muted, #9aa3c0)",
                            }}
                          >
                            <span
                              style={{
                                fontWeight: 700,
                                color: "var(--text)",
                              }}
                            >
                              {h.to}
                            </span>
                            <div>
                              {h.by} · {h.at}
                            </div>
                          </div>
                        ))}
                        {l.stageHistoryMore > 0 ? (
                          <div
                            style={{
                              fontSize: 10,
                              cursor: "pointer",
                              textDecoration: "underline",
                              color: "var(--muted, #9aa3c0)",
                            }}
                            onClick={() => onStatusHistory(l.id)}
                          >
                            +{l.stageHistoryMore} earlier change
                            {l.stageHistoryMore === 1 ? "" : "s"}
                          </div>
                        ) : null}
                      </div>
                    ) : null}
                  </td>

                  <td>
                    <div
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 6,
                      }}
                    >
                      {/* Keyed on the saved demo so it remounts to the new
                          value when the row changes elsewhere (quick-update
                          popup); uncontrolled otherwise, like the stage cell. */}
                      <select
                        key={l.demo}
                        className={styles.stageSelect}
                        defaultValue={l.demo}
                        onFocus={(e) => {
                          e.target.dataset.prev = e.target.value;
                        }}
                        onChange={(e) => requestDemoChange(e, l.id)}
                      >
                        {demoStatuses.map((s) => (
                          <option value={s.key} key={s.key}>
                            {s.label}
                          </option>
                        ))}
                      </select>
                      {/* A completed demo can be logged as a meeting: the +
                          opens Log Meeting, and saving it advances this lead to
                          Meeting Completed. Keyed off the SAVED demo value, so
                          it appears only after the change is persisted. */}
                      {l.demo === "completed" ? (
                        <button
                          className={styles.btn}
                          type="button"
                          title={`Log meeting for ${l.company} — moves it to Meeting Completed`}
                          aria-label={`Log meeting for ${l.company}`}
                          onClick={() => onLogMeeting(l)}
                          style={{
                            display: "inline-flex",
                            alignItems: "center",
                            justifyContent: "center",
                            padding: 0,
                            width: 22,
                            height: 22,
                            minWidth: 22,
                            borderRadius: "50%",
                            fontSize: 14,
                            lineHeight: 1,
                            flexShrink: 0,
                          }}
                        >
                          +
                        </button>
                      ) : null}
                    </div>
                  </td>

                  <td style={{ width: 360 }}>
                    <div
                      style={{
                        display: "flex",
                        alignItems: "flex-start",
                        gap: 6,
                      }}
                    >
                      <div style={{ flex: 1, minWidth: 0 }}>
                        {l.latestNote ? (
                          <>
                            <div
                              style={{
                                fontSize: 12,
                                whiteSpace: "pre-wrap",
                                wordBreak: "break-word",
                              }}
                            >
                              {l.latestNote.text}
                            </div>
                            {l.latestNote.audio_url ? (
                              <audio
                                controls
                                preload="none"
                                style={{
                                  marginTop: 4,
                                  width: "100%",
                                  maxWidth: 240,
                                  height: 30,
                                }}
                                src={l.latestNote.audio_url}
                              />
                            ) : null}
                            {l.latestNoteByline ? (
                              <div
                                className={styles.meta}
                                style={{ fontSize: 11 }}
                              >
                                {l.latestNoteByline}
                              </div>
                            ) : null}
                            {l.noteHistoryMore > 0 ? (
                              <div
                                className={styles.meta}
                                style={{
                                  fontSize: 11,
                                  cursor: "pointer",
                                  textDecoration: "underline",
                                }}
                                onClick={() => onNotesHistory(l.id)}
                              >
                                +{l.noteHistoryMore} earlier note
                                {l.noteHistoryMore === 1 ? "" : "s"}
                              </div>
                            ) : null}
                          </>
                        ) : (
                          <div className={styles.meta} style={{ fontSize: 12 }}>
                            No notes yet
                          </div>
                        )}
                      </div>
                      <button
                        className={styles.btn}
                        type="button"
                        title="Add note"
                        aria-label="Add note"
                        onClick={() => onAddNote(l.id)}
                        style={{
                          padding: 0,
                          width: 20,
                          height: 20,
                          minWidth: 20,
                          fontSize: 14,
                          lineHeight: 1,
                          flexShrink: 0,
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "center",
                        }}
                      >
                        +
                      </button>
                    </div>
                  </td>

                  <td>
                    <ReachDropdown
                      lead={l}
                      channels={reachChannels}
                      onToggleChannel={toggleChannel}
                    />
                  </td>

                  <td style={{ textAlign: "center", width: 40 }}>
                    <button
                      type="button"
                      title="Delete lead"
                      aria-label="Delete lead"
                      onClick={() => deleteLead(l.id, l.company)}
                      style={{
                        background: "transparent",
                        border: "none",
                        color: "#ef4444",
                        cursor: "pointer",
                        fontSize: 15,
                        lineHeight: 1,
                        padding: "4px 6px",
                        borderRadius: 6,
                      }}
                    >
                      ✕
                    </button>
                  </td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={8} className={styles.meta}>
                  No leads yet for this client. Add the first lead.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}
