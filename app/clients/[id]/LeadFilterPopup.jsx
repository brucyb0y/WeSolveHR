"use client";

// The Leads "Filter" popup — a plain GET form in a dropdown.
//
// Filtering stays a navigation, not client-side state: submitting reloads
// /clients/:id?tab=leads&... so the filtered view is a real URL that can be
// bookmarked, shared and reloaded. That is why every control is a named form
// input rather than React state, and why search/sort/mine values ride along as
// hidden inputs — omitting them would make applying a filter silently drop the
// user's search term.
//
// Only the open/closed state is client-side, which is the sole reason this is
// not a server component.

import { useEffect, useRef, useState } from "react";
import styles from "./workspace.module.css";

// Query keys the controls in this popup own — one entry per named input below.
//
// The parent must NOT also pass these as hidden inputs. This form submits every
// active filter twice otherwise: once from the stale hidden input the parent
// carries to preserve state, and once from the control here. Because the hidden
// inputs render before the controls, the stale value comes first in the query
// string and the server's firstOf() keeps it — so the SECOND time you change a
// filter it silently keeps showing the first value. Only pass-through state the
// popup has no control for (sort, sort_dir, mine) belongs in hiddenInputs.
export const LEAD_FILTER_FIELD_NAMES = [
  "pipeline_stage",
  "demo_status",
  "category_type",
  "location",
  "phone_assignee",
  "email_assignee",
  "has_phone",
  "reached_via",
  "notes",
  "has_note_audio",
  "notes_by",
  "updated_from",
  "updated_to",
  "callback_date_from",
  "callback_date_to",
  "missed_callback",
];

const LABEL_STYLE = {
  display: "flex",
  flexDirection: "column",
  gap: 6,
  fontSize: 11,
  fontWeight: 800,
  letterSpacing: "0.04em",
  color: "var(--muted, #9aa3c0)",
};

const CONTROL_STYLE = {
  width: "100%",
  padding: 8,
  borderRadius: 8,
  border: "1px solid var(--line)",
  background: "rgba(255,255,255,0.04)",
  color: "var(--text)",
};

const DATE_STYLE = { ...CONTROL_STYLE, colorScheme: "dark" };

function Select({ name, options, value }) {
  return (
    <select name={name} defaultValue={value} style={CONTROL_STYLE}>
      <option value="">Any</option>
      {options.map((o) => (
        <option value={o.key} key={o.key}>
          {o.label}
        </option>
      ))}
    </select>
  );
}

// Multi-select filters submit one checkbox per key; the parser joins them with
// commas, so several boxes under the same name is the intended shape.
function MultiSelect({ name, options, selected }) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 4,
        maxHeight: 140,
        overflow: "auto",
        padding: "4px 2px",
      }}
    >
      {options.map((o) => (
        <label
          key={o.key}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            fontSize: 12,
            fontWeight: 400,
            letterSpacing: 0,
            color: "var(--text)",
            cursor: "pointer",
          }}
        >
          <input
            type="checkbox"
            name={name}
            value={o.key}
            defaultChecked={selected.includes(o.key)}
          />
          {o.label}
        </label>
      ))}
    </div>
  );
}

export default function LeadFilterPopup({
  clientId,
  activeCount,
  search,
  hiddenInputs,
  filters,
  options,
  clearHref,
}) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef(null);

  // Click-outside closes the popup, matching the original's document-level
  // handler. Bound only while open so it is not a permanent listener.
  useEffect(() => {
    if (!open) return;
    const onDocClick = (e) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target)) {
        setOpen(false);
      }
    };
    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, [open]);

  const {
    pipelineStages,
    demoStatuses,
    categoryTypes,
    assigneeOptions,
    hasPhoneOptions,
    reachedViaOptions,
    notesOptions,
    noteAudioOptions,
    missedCallbackOptions,
    noteAuthorOptions,
  } = options;

  const withNone = (label, list) => [{ key: "__none__", label }, ...list];

  return (
    <div style={{ position: "relative" }} ref={wrapRef}>
      <button
        className={`${styles.btn} ${activeCount ? styles.btnPrimary : ""}`}
        type="button"
        aria-haspopup="true"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
      >
        Filter{activeCount ? ` (${activeCount})` : ""} ▾
      </button>

      <form
        method="GET"
        action={`/clients/${clientId}`}
        style={{
          display: open ? "flex" : "none",
          position: "absolute",
          right: 0,
          top: "calc(100% + 6px)",
          zIndex: 60,
          width: 240,
          maxHeight: "72vh",
          overflow: "auto",
          flexDirection: "column",
          gap: 10,
          padding: 14,
          background: "var(--panel-strong, #11162a)",
          border: "1px solid var(--line)",
          borderRadius: 12,
          boxShadow: "0 12px 32px rgba(0,0,0,0.45)",
        }}
      >
        <input type="hidden" name="tab" value="leads" />
        <input type="hidden" name="search" value={search} />
        {/* Sort and "my leads only" must survive an Apply, or filtering would
            quietly reset them. */}
        {hiddenInputs.map((h) => (
          <input
            type="hidden"
            name={h.name}
            value={h.value}
            key={`${h.name}:${h.value}`}
          />
        ))}

        <label style={LABEL_STYLE}>
          STATUS
          <Select
            name="pipeline_stage"
            options={withNone("None (never set)", pipelineStages)}
            value={filters.pipeline_stage}
          />
        </label>

        <label style={LABEL_STYLE}>
          DEMO
          <Select
            name="demo_status"
            options={withNone("None (never set)", demoStatuses)}
            value={filters.demo_status}
          />
        </label>

        <div style={LABEL_STYLE}>
          CATEGORY TYPE
          <MultiSelect
            name="category_type"
            options={withNone("None (no category)", categoryTypes)}
            selected={filters.category_type_list}
          />
        </div>

        <div style={LABEL_STYLE}>
          LOCATION OF LEAD
          <input
            type="text"
            name="location"
            defaultValue={
              filters.location === "__none__" ? "" : filters.location
            }
            placeholder="City, state, or country"
            style={CONTROL_STYLE}
          />
          <label
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              fontSize: 12,
              fontWeight: 400,
              letterSpacing: 0,
              color: "var(--text)",
              cursor: "pointer",
            }}
          >
            <input
              type="checkbox"
              name="location"
              value="__none__"
              defaultChecked={filters.location === "__none__"}
            />{" "}
            None (no location data)
          </label>
        </div>

        <label style={LABEL_STYLE}>
          ASSIGNED FOR PHONE
          <Select
            name="phone_assignee"
            options={assigneeOptions}
            value={filters.phone_assignee}
          />
        </label>

        <label style={LABEL_STYLE}>
          ASSIGNED FOR EMAIL
          <Select
            name="email_assignee"
            options={assigneeOptions}
            value={filters.email_assignee}
          />
        </label>

        <label style={LABEL_STYLE}>
          LEAD WITH NUMBER
          <Select
            name="has_phone"
            options={hasPhoneOptions}
            value={filters.has_phone}
          />
        </label>

        <div style={LABEL_STYLE}>
          REACHED VIA
          <MultiSelect
            name="reached_via"
            options={reachedViaOptions}
            selected={filters.reached_via_list}
          />
        </div>

        <label style={LABEL_STYLE}>
          NOTES
          <Select name="notes" options={notesOptions} value={filters.notes} />
        </label>

        <label style={LABEL_STYLE}>
          NOTES AUDIO
          <Select
            name="has_note_audio"
            options={noteAudioOptions}
            value={filters.has_note_audio}
          />
        </label>

        {/* Only offered when somebody has actually written a note — otherwise
            the dropdown would hold nothing but "No notes". */}
        {noteAuthorOptions.length ? (
          <label style={LABEL_STYLE}>
            NOTES BY
            <Select
              name="notes_by"
              options={withNone("No notes", noteAuthorOptions)}
              value={filters.notes_by}
            />
          </label>
        ) : null}

        <label style={LABEL_STYLE}>
          UPDATED FROM
          <input
            type="date"
            name="updated_from"
            defaultValue={filters.updated_from}
            style={DATE_STYLE}
          />
        </label>

        <label style={LABEL_STYLE}>
          UPDATED TO
          <input
            type="date"
            name="updated_to"
            defaultValue={filters.updated_to}
            style={DATE_STYLE}
          />
        </label>

        <label style={LABEL_STYLE}>
          CALLBACK DATE FROM
          <input
            type="date"
            name="callback_date_from"
            defaultValue={filters.callback_date_from}
            style={DATE_STYLE}
          />
        </label>

        <label style={LABEL_STYLE}>
          CALLBACK DATE TO
          <input
            type="date"
            name="callback_date_to"
            defaultValue={filters.callback_date_to}
            style={DATE_STYLE}
          />
        </label>

        <label style={LABEL_STYLE}>
          MISSED CALLBACK
          <Select
            name="missed_callback"
            options={missedCallbackOptions}
            value={filters.missed_callback}
          />
        </label>

        <div style={{ display: "flex", gap: 6, marginTop: 4 }}>
          <button
            className={`${styles.btn} ${styles.btnPrimary}`}
            type="submit"
            style={{ flex: 1 }}
          >
            Apply
          </button>
          {activeCount ? (
            <a className={styles.btn} href={clearHref}>
              Clear
            </a>
          ) : null}
        </div>
      </form>
    </div>
  );
}
