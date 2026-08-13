"use client";

// Owns the modal state shared between the "+ Add Lead" button in the header and
// the row actions in the table, and renders the three modals.
//
// Everything static — header text, stats, tabs, the filter form, pagination —
// is server-rendered and passed in as elements, so only the interactive parts
// cross into the client bundle.

import { useState } from "react";
import LeadsTable from "./LeadsTable";
import LeadFormModal from "./LeadFormModal";
import CallSummaryModal from "./CallSummaryModal";
import LeadCallsModal from "./LeadCallsModal";
import styles from "./leads.module.css";

export default function LeadsBoard({
  business,
  rows,
  industryOptions,
  capabilityOptions,
  embed,
  header,
  headerLinks,
  stats,
  tabs,
  filters,
  uploads,
  table,
  pagination,
}) {
  // { kind: "form" | "callSummary" | "calls", leadId?, phone? } | null
  const [modal, setModal] = useState(null);
  const close = () => setModal(null);

  return (
    <>
      <div className={embed ? styles.wrapEmbed : styles.wrap}>
        {embed ? null : (
          <div className={styles.topbar}>
            {header}
            <div className={styles.topbarActions}>
              {headerLinks}
              <button
                className={`${styles.btn} ${styles.btnPrimary}`}
                type="button"
                onClick={() => setModal({ kind: "form", leadId: null })}
              >
                + Add Lead
              </button>
            </div>
          </div>
        )}

        {embed ? null : stats}
        {tabs}
        {filters}
        {uploads}

        {table ?? (
          <div className={styles.panel}>
            <LeadsTable
              business={business}
              rows={rows}
              onOpenCallSummary={(phone) =>
                setModal({ kind: "callSummary", phone })
              }
              onOpenEdit={(leadId) => setModal({ kind: "form", leadId })}
              onOpenCalls={(leadId) => setModal({ kind: "calls", leadId })}
            />
            {pagination}
          </div>
        )}
      </div>

      {modal?.kind === "form" ? (
        <LeadFormModal
          business={business}
          leadId={modal.leadId}
          industryOptions={industryOptions}
          capabilityOptions={capabilityOptions}
          onClose={close}
        />
      ) : null}

      {modal?.kind === "callSummary" ? (
        <CallSummaryModal
          business={business}
          phone={modal.phone}
          onClose={close}
        />
      ) : null}

      {modal?.kind === "calls" ? (
        <LeadCallsModal
          business={business}
          leadId={modal.leadId}
          industryOptions={industryOptions}
          capabilityOptions={capabilityOptions}
          onClose={close}
        />
      ) : null}
    </>
  );
}
