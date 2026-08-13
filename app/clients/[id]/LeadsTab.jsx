"use client";

// Leads tab shell — header, category pills, pagination and the table.
//
// TWO MODES, both from the original:
//   1. When the client maps to a static lead business (e.g. Joolian ->
//      joolian_leads), the tab is just a same-origin iframe of
//      /leads/<business>?embed=1 — the full leads page, chrome stripped. That
//      page is already converted, so this branch is a handful of lines.
//   2. Otherwise the client owns its own leads and gets the native table below.
//
// Client component because it holds the lead modal state (edit / quick-update /
// note / notes-history / status-history) that the table's row controls open.
//
// Every href arrives precomputed as a string — mineOnHref/mineOffHref, each
// category pill's own href, the pagination hrefs. None of them can be a
// href-BUILDING function: page.jsx is a server component, and functions do not
// cross that boundary. The pills therefore carry their href on the row data.

import { useState } from "react";
import styles from "./workspace.module.css";
import ClientLeadsTable from "./ClientLeadsTable";
import LeadFilterPopup from "./LeadFilterPopup";
import LeadsPagination from "./LeadsPagination";
import LeadNoteModal from "./LeadNoteModal";
import LeadQuickUpdateModal from "./LeadQuickUpdateModal";
import LeadImportModal from "./LeadImportModal";
import ClientLeadModal from "./ClientLeadModal";
import {
  LeadStatusHistoryModal,
  LeadNotesHistoryModal,
} from "./LeadHistoryModals";

export default function LeadsTab({
  clientId,
  staticLeadBusiness,
  leads,
  filteredIds,
  totalCount,
  users,
  stages,
  demoStatuses,
  categoryTypes,
  categoryTypeLabels,
  categoryCounts,
  reachChannels,
  sort,
  pagination,
  paginationHrefs,
  search,
  hasActiveQuery,
  clearSearchHref,
  mineOnly,
  mineOnHref,
  mineOffHref,
  filters,
  filterOptions,
  activeFilterCount,
  clearFiltersHref,
  filterHiddenInputs,
  allCategoryPillHref,
  todayStr,
  statusHistory,
  importCategoryTypeRequired,
  showOptionalSheetFields,
}) {
  const [leadModal, setLeadModal] = useState(null);

  // Mode 1: the client's leads live in a static lead business — embed that
  // page rather than duplicating its table.
  if (staticLeadBusiness) {
    return (
      <iframe
        src={`/leads/${encodeURIComponent(staticLeadBusiness)}?embed=1`}
        title={`${staticLeadBusiness} leads`}
        style={{
          width: "100%",
          height: "82vh",
          border: "1px solid var(--line)",
          borderRadius: "var(--radius-lg)",
          background: "transparent",
        }}
      />
    );
  }

  const resultLabel = hasActiveQuery
    ? ` result${totalCount === 1 ? "" : "s"}${search ? ` for “${search}”` : ""}`
    : " total";

  return (
    <div className={styles.panel}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          gap: 12,
          alignItems: "center",
          marginBottom: 14,
          flexWrap: "wrap",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <h2 style={{ margin: 0 }}>Leads</h2>
          <div>
            <div className={styles.meta}>
              {totalCount}
              {resultLabel}
            </div>
          </div>

          {/* "My leads only" is a link dressed as a switch: toggling it is a
              navigation, so the filtered view stays a shareable URL. */}
          <label
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 8,
              fontSize: 12,
              fontWeight: 700,
              letterSpacing: "0.02em",
              color: "var(--muted, #9aa3c0)",
              cursor: "pointer",
              userSelect: "none",
            }}
            title="Show only leads where you are assigned for phone, assigned for email, or the overall owner"
          >
            <span
              style={{
                position: "relative",
                display: "inline-block",
                width: 34,
                height: 19,
                flexShrink: 0,
              }}
            >
              <input
                type="checkbox"
                checked={mineOnly}
                onChange={(e) => {
                  window.location.href = e.target.checked
                    ? mineOnHref
                    : mineOffHref;
                }}
                style={{
                  opacity: 0,
                  width: "100%",
                  height: "100%",
                  margin: 0,
                  position: "absolute",
                  cursor: "pointer",
                }}
              />
              <span
                style={{
                  position: "absolute",
                  inset: 0,
                  background: mineOnly ? "#8b7cf6" : "rgba(255,255,255,0.15)",
                  borderRadius: 999,
                  transition: "background .15s",
                  pointerEvents: "none",
                }}
              />
              <span
                style={{
                  position: "absolute",
                  top: 2,
                  left: mineOnly ? 17 : 2,
                  width: 15,
                  height: 15,
                  background: "#fff",
                  borderRadius: "50%",
                  transition: "left .15s",
                  pointerEvents: "none",
                }}
              />
            </span>
            My leads only
          </label>
        </div>

        <div
          style={{
            display: "flex",
            gap: 8,
            alignItems: "center",
            flexWrap: "wrap",
          }}
        >
          {/* Search is a GET form for the same reason the filters are: the
              result must be a URL. Filter/sort/mine ride along as hidden
              inputs so searching does not reset them. */}
          <form
            method="GET"
            action={`/clients/${clientId}`}
            style={{
              display: "flex",
              gap: 6,
              alignItems: "center",
              margin: 0,
            }}
          >
            <input type="hidden" name="tab" value="leads" />
            {filterHiddenInputs.map((h) => (
              <input
                type="hidden"
                name={h.name}
                value={h.value}
                key={`${h.name}:${h.value}`}
              />
            ))}
            <input
              type="search"
              name="search"
              defaultValue={search}
              placeholder="Search company, phone, or emails…"
              aria-label="Search leads by company, phone, or a pasted list of emails"
              style={{
                padding: "8px 10px",
                borderRadius: 8,
                border: "1px solid var(--line)",
                background: "rgba(255,255,255,0.04)",
                color: "var(--text)",
                minWidth: 220,
              }}
            />
            <button className={styles.btn} type="submit">
              Search
            </button>
            {search ? (
              <a className={styles.btn} href={clearSearchHref}>
                Clear
              </a>
            ) : null}
          </form>

          <LeadFilterPopup
            clientId={clientId}
            activeCount={activeFilterCount}
            search={search}
            hiddenInputs={filterHiddenInputs.filter(
              (h) => h.name !== "search" && h.name !== "tab",
            )}
            filters={filters}
            options={filterOptions}
            clearHref={clearFiltersHref}
          />

          <button
            className={styles.btn}
            type="button"
            onClick={() => setLeadModal({ kind: "import" })}
          >
            ⬆ Import from Excel
          </button>
          <button
            className={`${styles.btn} ${styles.btnPrimary}`}
            type="button"
            onClick={() => setLeadModal({ kind: "lead" })}
          >
            + Add Lead
          </button>
        </div>
      </div>

      {/* Category pills are a shortcut into the same category_type filter the
          popup sets — clicking one navigates, it does not toggle local state. */}
      {categoryCounts.length ? (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            flexWrap: "wrap",
            margin: "0 0 14px",
          }}
        >
          <CategoryPill
            href={allCategoryPillHref}
            active={!filters.category_type}
            label="All"
          />
          {categoryCounts.map((c) => (
            <CategoryPill
              key={c.key}
              href={c.href}
              active={filters.category_type_list.includes(c.key)}
              label={categoryTypeLabels[c.key] || c.key}
              count={c.count}
            />
          ))}
        </div>
      ) : null}

      <LeadsPagination
        pagination={pagination}
        prevHref={paginationHrefs.prev}
        nextHref={paginationHrefs.next}
        compact
      />

      <ClientLeadsTable
        clientId={clientId}
        leads={leads}
        filteredIds={filteredIds}
        users={users}
        stages={stages}
        demoStatuses={demoStatuses}
        categoryTypes={categoryTypes}
        reachChannels={reachChannels}
        sort={sort}
        todayStr={todayStr}
        onEditLead={(id) => setLeadModal({ kind: "lead", id })}
        onStageChange={(id, stage, revert) =>
          setLeadModal({ kind: "note", mode: "stage", id, stage, revert })
        }
        onDemoChange={(id, demo, revert) =>
          setLeadModal({ kind: "note", mode: "demo", id, demo, revert })
        }
        onQuickUpdate={(lead) => setLeadModal({ kind: "quickUpdate", lead })}
        onAddNote={(id) => setLeadModal({ kind: "note", mode: "note", id })}
        onNotesHistory={(id) => setLeadModal({ kind: "notesHistory", id })}
        onStatusHistory={(id) => setLeadModal({ kind: "statusHistory", id })}
      />

      <LeadsPagination
        pagination={pagination}
        prevHref={paginationHrefs.prev}
        nextHref={paginationHrefs.next}
      />

      {leadModal?.kind === "lead" ? (
        <ClientLeadModal
          clientId={clientId}
          leadId={leadModal.id}
          stages={stages}
          demoStatuses={demoStatuses}
          categoryTypes={categoryTypes}
          showOptionalSheetFields={showOptionalSheetFields}
          onClose={() => setLeadModal(null)}
        />
      ) : null}

      {leadModal?.kind === "import" ? (
        <LeadImportModal
          clientId={clientId}
          categoryTypes={categoryTypes}
          categoryTypeRequired={importCategoryTypeRequired}
          onClose={() => setLeadModal(null)}
        />
      ) : null}

      {leadModal?.kind === "note" ? (
        <LeadNoteModal
          clientId={clientId}
          leadId={leadModal.id}
          mode={leadModal.mode}
          stage={leadModal.stage}
          demo={leadModal.demo}
          onCancel={leadModal.revert}
          onClose={() => setLeadModal(null)}
        />
      ) : null}

      {leadModal?.kind === "quickUpdate" ? (
        <LeadQuickUpdateModal
          clientId={clientId}
          lead={leadModal.lead}
          stages={stages}
          demoStatuses={demoStatuses}
          reachChannels={reachChannels}
          onClose={() => setLeadModal(null)}
        />
      ) : null}

      {leadModal?.kind === "statusHistory" ? (
        <LeadStatusHistoryModal
          rows={statusHistory[String(leadModal.id)] || []}
          onClose={() => setLeadModal(null)}
        />
      ) : null}

      {leadModal?.kind === "notesHistory" ? (
        <LeadNotesHistoryModal
          clientId={clientId}
          leadId={leadModal.id}
          onClose={() => setLeadModal(null)}
        />
      ) : null}
    </div>
  );
}

function CategoryPill({ href, active, label, count }) {
  return (
    <a
      href={href}
      title={`Filter leads by ${label}`}
      style={{
        padding: "4px 12px",
        borderRadius: 999,
        fontSize: 12,
        fontWeight: 700,
        textDecoration: "none",
        border: "1px solid var(--line)",
        ...(active
          ? { background: "#8b7cf6", color: "#fff", borderColor: "#8b7cf6" }
          : { background: "rgba(255,255,255,0.04)", color: "var(--text)" }),
      }}
    >
      {label}
      {count === undefined ? null : (
        <>
          {" "}
          <span style={{ opacity: 0.7, fontWeight: 600 }}>{count}</span>
        </>
      )}
    </a>
  );
}
