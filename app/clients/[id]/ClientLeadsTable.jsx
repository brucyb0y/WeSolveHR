"use client";

// The client leads table plus its bulk-action bar.
//
// These are one component because they share selection state: the bar only
// appears once something is ticked, and every bulk action operates on exactly
// that set. Splitting them would mean lifting selection into a parent that has
// no other use for it.
//
// Two selection scopes, deliberately kept distinct — the original had both and
// conflating them would silently widen destructive actions:
//   * the header checkbox selects the rows ON THIS PAGE;
//   * "Select all N leads" selects every lead MATCHING THE CURRENT FILTERS,
//     including rows on other pages, and only appears once the page-level box
//     is used. `filteredIds` carries those ids from the server.
//
// PRESERVED DEFECT: the original tagged both call icons class="lead-call-icon",
// but no rule ever defined it and no script queries it — all the styling is
// inline. The class is dropped here rather than adding a rule the original
// never had; appearance is unchanged either way.
//
// THE STAGE AND DEMO DROPDOWNS DO NOT SAVE ON CHANGE. Each one opens a note
// dialog and the change is written only once a reason is supplied; cancelling
// puts the dropdown back where it was. Patching straight from onChange would be
// the obvious reading of this UI and would silently drop the audit trail the
// original goes out of its way to collect. The revert closure is passed up with
// the request so the parent can restore the element it came from.

import { useState } from "react";
import { useRouter } from "next/navigation";
import Swal from "sweetalert2";
import styles from "./workspace.module.css";

// Bulk requests go out in batches rather than all at once: an all-pages
// selection can be thousands of leads, and firing that many concurrent PATCHes
// would flood the server. 15 matches the original.
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

// Sort headers are built here from plain href/arrow DATA rather than arriving
// as rendered elements, because the row body also needs a sort link ("Updated
// …" under each company) — and a function that builds links cannot be passed
// from a server component to a client one.
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

// An inactive column shows a dimmed double-arrow; the active one shows its
// direction at full opacity.
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

// Reached-via is a small checkbox dropdown per row: a lead can be reached
// through several channels at once, so this is not a single-select.
function ReachDropdown({ lead, channels, onToggleChannel }) {
  const [open, setOpen] = useState(false);

  const activeLabels = channels
    .filter((c) => lead[c.column])
    .map((c) => c.label);
  const label = activeLabels.length ? activeLabels.join(", ") : "Not reached";

  return (
    <div style={{ position: "relative", display: "inline-block" }}>
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
                display: "block",
                fontSize: 12,
                whiteSpace: "nowrap",
                padding: "3px 4px",
              }}
            >
              <input
                type="checkbox"
                checked={!!lead[c.column]}
                onChange={(e) => onToggleChannel(lead.id, c, e.target.checked)}
              />{" "}
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

  // ---- inline row edits -------------------------------------------------

  // Hands the new value up along with a revert for the exact <select> that
  // produced it, so Cancel restores that row and no other.
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

  const toggleVisible = (leadId, checked) =>
    patchLead(
      leadId,
      { is_client_visible: checked },
      "Failed to update visibility",
    );

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

  // ---- bulk actions -----------------------------------------------------

  // There is no bulk endpoint — the original fans the same PATCH out across
  // every selected id and reloads once at the end. Preserved, including the
  // partial-failure report: with thousands of leads some requests can fail
  // while others succeed, and silently reloading would hide that.
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

  // Status and demo changes offer an optional note applied to every selected
  // lead, so a sweeping change carries its reason. Returning null means the
  // user cancelled — the action is abandoned, not applied without a note.
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
    if (note === null) return; // cancelled

    const body = { [field]: value };
    if (note) body.add_note = note;

    await runBulkLeadAction(
      ids,
      () => body,
      `Updating ${label} for ${ids.length} lead(s)...`,
    );
  }

  async function bulkSet(value, field, loadingLabel) {
    const ids = selectedIds;
    if (!ids.length) return;
    await runBulkLeadAction(
      ids,
      () => ({ [field]: value }),
      `${loadingLabel} for ${ids.length} lead(s)...`,
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
            onChange={(e) =>
              runFromSelect(e, (v) =>
                bulkSet(v, "reached_via", "Updating reached via"),
              )
            }
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
                bulkSet(v, "phone_assigned_to", "Assigning for phone"),
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
                bulkSet(v, "email_assigned_to", "Assigning for email"),
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
            onChange={(e) =>
              runFromSelect(e, (v) =>
                bulkSet(v, "category_type", "Setting category type"),
              )
            }
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
                        checked={!!l.is_client_visible}
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
                    <select
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
                    <select
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
