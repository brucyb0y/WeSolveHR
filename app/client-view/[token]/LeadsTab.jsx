"use client";

// Leads table with search, the 13-filter popup, category pills, sortable
// headers, pagination and the notes-history modal.
//
// The original was ~580 lines of DOM work: it stashed every filterable value on
// the <tr> as data-* attributes, read them back with getAttribute, hid rows with
// style.display, and re-appended nodes to force the sorted order. All of that
// collapses into deriving `visible` from state — the rows are already objects
// (see lib/data/client-view-leads.js).
//
// Filter semantics are preserved exactly, including the subtle ones:
//   * selecting the DEFAULT stage/demo also matches rows that never had one set
//     (a null column falls back to the default in the UI);
//   * "__none__" matches only never-set rows;
//   * REACHED VIA matches ANY selected channel; "both" needs LinkedIn AND Email;
//   * a date-range bound excludes rows with no date at all;
//   * MISSED CALLBACK "yes"/"no" only match rows that HAVE a callback date.

import { useMemo, useState } from "react";
import styles from "./client-view.module.css";

const PAGE_SIZE = 25;

const EMPTY = {
  stage: "",
  demo: "",
  category: [],
  location: "",
  locationNone: false,
  assignee: "",
  hasPhone: "",
  reach: [],
  notes: "",
  audio: "",
  noteBy: "",
  updatedFrom: "",
  updatedTo: "",
  callbackFrom: "",
  callbackTo: "",
  missedCallback: "",
};

const SORT_COLUMNS = [
  { key: "name", label: "Company / Contact" },
  { key: "stage", label: "Stage" },
  { key: "demo", label: "Demo" },
  { key: "notes", label: "Notes" },
  { key: "updated", label: "Updated" },
];

function sortValue(row, key) {
  if (key === "name") return row.name.toLowerCase();
  if (key === "stage") return row.stageIdx;
  if (key === "demo") return row.demoIdx;
  if (key === "notes") return row.notesCount;
  if (key === "updated") return row.updatedAt;
  return "";
}

function MultiSelect({ label, options, value, onChange }) {
  const [open, setOpen] = useState(false);
  const summary =
    value.length === 0
      ? "All"
      : value.length === 1
        ? options.find((o) => o.key === value[0])?.label || "1 selected"
        : `${value.length} selected`;

  return (
    <div className={styles.filterLabel}>
      {label}
      <div className={styles.extMs}>
        <button
          type="button"
          className={styles.extMsBtn}
          onClick={() => setOpen((v) => !v)}
        >
          <span>{summary}</span>
          <span className={styles.extMsCaret}>▾</span>
        </button>
        {open ? (
          <div className={styles.extMsPanel}>
            {options.map((o) => (
              <label className={styles.extMsOption} key={o.key}>
                <input
                  type="checkbox"
                  checked={value.includes(o.key)}
                  onChange={(e) =>
                    onChange(
                      e.target.checked
                        ? [...value, o.key]
                        : value.filter((k) => k !== o.key),
                    )
                  }
                />
                {o.label}
              </label>
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}

const Select = ({ label, options, value, onChange }) => (
  <label className={styles.filterLabel}>
    {label}
    <select
      className={styles.filterControl}
      value={value}
      onChange={(e) => onChange(e.target.value)}
    >
      <option value="">All</option>
      {options.map((o) => (
        <option value={o.key} key={o.key}>
          {o.label}
        </option>
      ))}
    </select>
  </label>
);

export default function LeadsTab({
  leads,
  options,
  categoryCounts,
  defaults,
  todayStr,
}) {
  const [search, setSearch] = useState("");
  const [filters, setFilters] = useState(EMPTY);
  const [filterOpen, setFilterOpen] = useState(false);
  const [sort, setSort] = useState({ key: "", dir: 1 });
  const [page, setPage] = useState(1);
  const [notesLead, setNotesLead] = useState(null);

  const set = (key) => (value) => {
    setFilters((f) => ({ ...f, [key]: value }));
    setPage(1);
  };

  const activeCount = useMemo(
    () =>
      Object.entries(filters).filter(([, v]) =>
        Array.isArray(v) ? v.length > 0 : v !== "" && v !== false,
      ).length,
    [filters],
  );

  const visible = useMemo(() => {
    const q = search.trim().toLowerCase();
    const f = filters;

    const rows = leads.filter((r) => {
      if (f.stage) {
        if (f.stage === "__none__") {
          if (r.stage !== "") return false;
        } else if (f.stage === defaults.stage) {
          if (r.stage !== defaults.stage && r.stage !== "") return false;
        } else if (r.stage !== f.stage) return false;
      }

      if (f.demo) {
        if (f.demo === "__none__") {
          if (r.demo !== "") return false;
        } else if (f.demo === defaults.demo) {
          if (r.demo !== defaults.demo && r.demo !== "") return false;
        } else if (r.demo !== f.demo) return false;
      }

      if (f.category.length) {
        const ok = f.category.some((k) =>
          k === "__none__" ? r.category === "" : r.category === k,
        );
        if (!ok) return false;
      }

      if (f.assignee) {
        if (f.assignee === "__unassigned__") {
          if (r.assignee !== "") return false;
        } else if (r.assignee !== f.assignee) return false;
      }

      if (f.locationNone && r.location !== "") return false;
      if (f.location && !r.location.includes(f.location.trim().toLowerCase()))
        return false;

      if (f.reach.length) {
        const ok = f.reach.some((k) => {
          if (k === "both")
            return r.reach.includes("linkedin") && r.reach.includes("email");
          if (k === "__none__") return r.reach.length === 0;
          return r.reach.includes(k);
        });
        if (!ok) return false;
      }

      if (f.notes) {
        if (f.notes === "none" && r.notesCount > 0) return false;
        if (f.notes === "added" && r.notesCount < 1) return false;
        if (f.notes === "multiple" && r.notesCount < 2) return false;
      }

      if (f.audio) {
        if (f.audio === "yes" && !r.noteAudio) return false;
        if (f.audio === "no" && r.noteAudio) return false;
      }

      if (f.noteBy) {
        if (f.noteBy === "__none__") {
          if (r.noteBy.length > 0) return false;
        } else if (!r.noteBy.includes(f.noteBy)) return false;
      }

      // A bound excludes rows with no date at all.
      if (f.updatedFrom || f.updatedTo) {
        if (!r.updatedDate) return false;
        if (f.updatedFrom && r.updatedDate < f.updatedFrom) return false;
        if (f.updatedTo && r.updatedDate > f.updatedTo) return false;
      }

      if (f.callbackFrom || f.callbackTo) {
        if (!r.callback) return false;
        if (f.callbackFrom && r.callback < f.callbackFrom) return false;
        if (f.callbackTo && r.callback > f.callbackTo) return false;
      }

      if (f.hasPhone) {
        if (f.hasPhone === "yes" && !r.hasPhone) return false;
        if (f.hasPhone === "no" && r.hasPhone) return false;
      }

      if (f.missedCallback) {
        if (f.missedCallback === "none") {
          if (r.callback) return false;
        } else {
          if (!r.callback) return false;
          const overdue = r.callback < todayStr;
          if (f.missedCallback === "yes" && !overdue) return false;
          if (f.missedCallback === "no" && overdue) return false;
        }
      }

      if (q && !r.search.includes(q)) return false;
      return true;
    });

    if (sort.key) {
      rows.sort((a, b) => {
        const av = sortValue(a, sort.key);
        const bv = sortValue(b, sort.key);
        if (av < bv) return -1 * sort.dir;
        if (av > bv) return 1 * sort.dir;
        return 0;
      });
    }

    return rows;
  }, [leads, search, filters, sort, defaults, todayStr]);

  const total = visible.length;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const current = Math.min(page, totalPages);
  const start = (current - 1) * PAGE_SIZE;
  const slice = visible.slice(start, start + PAGE_SIZE);

  function toggleSort(key) {
    setSort((s) => (s.key === key ? { key, dir: -s.dir } : { key, dir: 1 }));
    setPage(1);
  }

  const arrow = (key) =>
    sort.key === key ? (sort.dir === 1 ? "↑" : "↓") : "↕";

  return (
    <div className={styles.panel}>
      <div className={`${styles.panelHead} ${styles.leadsHead}`}>
        <h2 className={styles.leadsTitle}>Leads</h2>
        <div className={styles.leadsControls}>
          <input
            type="search"
            className={styles.leadsSearch}
            placeholder="Search company, phone, or emails…"
            aria-label="Search leads by company, phone, or a pasted list of emails"
            value={search}
            onChange={(e) => {
              setSearch(e.target.value);
              setPage(1);
            }}
          />

          <div className={styles.filterWrap}>
            <button
              type="button"
              className={`${styles.badge} ${styles.filterBtn}`}
              aria-haspopup="true"
              aria-expanded={filterOpen}
              onClick={() => setFilterOpen((v) => !v)}
            >
              Filter {activeCount ? `(${activeCount})` : ""} ▾
            </button>

            {filterOpen ? (
              <div className={styles.filterPopup}>
                <Select
                  label="STATUS"
                  options={options.stage}
                  value={filters.stage}
                  onChange={set("stage")}
                />
                <Select
                  label="DEMO"
                  options={options.demo}
                  value={filters.demo}
                  onChange={set("demo")}
                />
                <MultiSelect
                  label="CATEGORY TYPE"
                  options={options.category}
                  value={filters.category}
                  onChange={set("category")}
                />

                <div className={styles.filterLabel}>
                  LOCATION OF LEAD
                  <input
                    type="text"
                    className={styles.filterControl}
                    placeholder="City, state, or country"
                    value={filters.location}
                    onChange={(e) => set("location")(e.target.value)}
                  />
                  <label className={styles.extMsOption}>
                    <input
                      type="checkbox"
                      checked={filters.locationNone}
                      onChange={(e) => set("locationNone")(e.target.checked)}
                    />{" "}
                    None (no location data)
                  </label>
                </div>

                {options.hasAssignees ? (
                  <Select
                    label="ASSIGNED TO"
                    options={options.assignee}
                    value={filters.assignee}
                    onChange={set("assignee")}
                  />
                ) : null}

                <Select
                  label="LEAD WITH NUMBER"
                  options={[
                    { key: "yes", label: "Yes" },
                    { key: "no", label: "No" },
                  ]}
                  value={filters.hasPhone}
                  onChange={set("hasPhone")}
                />
                <MultiSelect
                  label="REACHED VIA"
                  options={options.reach}
                  value={filters.reach}
                  onChange={set("reach")}
                />
                <Select
                  label="NOTES"
                  options={options.notes}
                  value={filters.notes}
                  onChange={set("notes")}
                />
                <Select
                  label="NOTES AUDIO"
                  options={options.audio}
                  value={filters.audio}
                  onChange={set("audio")}
                />
                <Select
                  label="NOTES BY"
                  options={options.noteBy}
                  value={filters.noteBy}
                  onChange={set("noteBy")}
                />

                <div className={styles.filterLabel}>
                  UPDATED AT
                  <input
                    type="date"
                    className={styles.filterControl}
                    aria-label="Updated at from"
                    value={filters.updatedFrom}
                    onChange={(e) => set("updatedFrom")(e.target.value)}
                  />
                  <input
                    type="date"
                    className={styles.filterControl}
                    aria-label="Updated at to"
                    value={filters.updatedTo}
                    onChange={(e) => set("updatedTo")(e.target.value)}
                  />
                </div>

                <div className={styles.filterLabel}>
                  CALLBACK DATE
                  <input
                    type="date"
                    className={styles.filterControl}
                    aria-label="Callback date from"
                    value={filters.callbackFrom}
                    onChange={(e) => set("callbackFrom")(e.target.value)}
                  />
                  <input
                    type="date"
                    className={styles.filterControl}
                    aria-label="Callback date to"
                    value={filters.callbackTo}
                    onChange={(e) => set("callbackTo")(e.target.value)}
                  />
                </div>

                <Select
                  label="MISSED CALLBACK"
                  options={[
                    { key: "yes", label: "Yes — overdue (past)" },
                    { key: "no", label: "No — upcoming (future)" },
                    { key: "none", label: "No callback date set" },
                  ]}
                  value={filters.missedCallback}
                  onChange={set("missedCallback")}
                />

                <button
                  type="button"
                  className={`${styles.badge} ${styles.filterClear}`}
                  onClick={() => {
                    setFilters(EMPTY);
                    setPage(1);
                  }}
                >
                  Clear filters
                </button>
              </div>
            ) : null}
          </div>
        </div>
      </div>

      {categoryCounts.length ? (
        <div className={styles.categoryPills}>
          {categoryCounts.map((c) => (
            <button
              type="button"
              key={c.key}
              className={`${styles.badge} ${styles.categoryPill} ${
                filters.category.includes(c.key) ? styles.active : ""
              }`}
              onClick={() =>
                set("category")(
                  filters.category.includes(c.key)
                    ? filters.category.filter((k) => k !== c.key)
                    : [...filters.category, c.key],
                )
              }
            >
              {c.label} ({c.count})
            </button>
          ))}
        </div>
      ) : null}

      <div className={styles.tableWrap}>
        <table>
          <thead>
            <tr>
              {SORT_COLUMNS.map((c) => (
                <th
                  key={c.key}
                  className={styles.sortHeader}
                  onClick={() => toggleSort(c.key)}
                >
                  {c.label}{" "}
                  <span className={styles.sortArrow}>{arrow(c.key)}</span>
                </th>
              ))}
              <th>Reached Via</th>
              <th>Location</th>
            </tr>
          </thead>
          <tbody>
            {slice.length ? (
              slice.map((l) => (
                <tr key={l.id}>
                  <td>
                    <strong>{l.name || "(no name)"}</strong>
                    {l.contact_name ? (
                      <div className={styles.meta}>{l.contact_name}</div>
                    ) : null}
                  </td>
                  <td>
                    <span className={styles.badge}>{l.stageLabel}</span>
                  </td>
                  <td>
                    <span className={styles.badge}>{l.demoLabel}</span>
                  </td>
                  <td>
                    {l.notesCount ? (
                      <button
                        type="button"
                        className={styles.notesLink}
                        onClick={() => setNotesLead(l)}
                      >
                        {l.notesCount} note{l.notesCount === 1 ? "" : "s"}
                      </button>
                    ) : (
                      <span className={styles.meta}>—</span>
                    )}
                  </td>
                  <td>{l.updatedDate || "-"}</td>
                  <td>
                    {l.reach.length ? (
                      <div className={styles.chipRow}>
                        {l.reach.map((k) => (
                          <span className={styles.chip} key={k}>
                            {k}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <span className={styles.meta}>—</span>
                    )}
                  </td>
                  <td>{l.locationLabel || "-"}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={7} className={styles.meta}>
                  No leads match these filters.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <div className={styles.leadsPager}>
        <span className={styles.meta}>
          {total
            ? `Showing ${start + 1}–${Math.min(start + PAGE_SIZE, total)} of ${total}`
            : "0 results"}
        </span>
        {total > PAGE_SIZE ? (
          <span className={styles.leadsPagerControls}>
            <button
              type="button"
              className={styles.badge}
              disabled={current <= 1}
              onClick={() => setPage(current - 1)}
            >
              ← Prev
            </button>
            <span className={styles.meta}>
              Page {current} of {totalPages}
            </span>
            <button
              type="button"
              className={styles.badge}
              disabled={current >= totalPages}
              onClick={() => setPage(current + 1)}
            >
              Next →
            </button>
          </span>
        ) : null}
      </div>

      {notesLead ? (
        <div
          className={`${styles.extModal} ${styles.open}`}
          onClick={(e) => {
            if (e.target === e.currentTarget) setNotesLead(null);
          }}
        >
          <div className={styles.extModalCard}>
            <div className={styles.extModalHead}>
              <div className={styles.extModalTitle}>
                Notes · {notesLead.name || "(no name)"}
              </div>
              <button
                type="button"
                className={styles.extModalClose}
                onClick={() => setNotesLead(null)}
              >
                Close
              </button>
            </div>
            <div className={styles.notesBody}>{notesLead.notesRaw || "—"}</div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
