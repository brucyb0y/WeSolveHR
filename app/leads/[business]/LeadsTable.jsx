"use client";

// Leads table for /leads/:business.
//
// Rows are plain data; the interaction lives here: the L2-done checkbox
// (optimistic, reverted on failure like the original), the per-row kebab menu,
// status changes, delete, and the three modal triggers.
//
// The kebab menu keeps the original's fixed positioning (top = button bottom
// + 6, left = right edge - 180, floored at 12) but "only one open at a time"
// now falls out of a single `openMenuId` instead of a querySelectorAll sweep.

import { useEffect, useState } from "react";
import styles from "./leads.module.css";

const MENU_WIDTH = 180;

const COLUMNS = [
  "Company / Contact",
  "Category / Capability",
  "Industry / Entity",
  "Location",
  "Lead Quality",
  "Call Summary",
  "Actions",
];

const STATUS_ACTIONS = [
  ["new", "Mark New"],
  ["in_progress", "Mark In Progress"],
  ["completed", "Mark Completed"],
];

export default function LeadsTable({
  business,
  rows,
  onOpenCallSummary,
  onOpenEdit,
  onOpenCalls,
}) {
  const [openMenuId, setOpenMenuId] = useState(null);
  const [menuPos, setMenuPos] = useState({ top: 0, left: 0 });
  const [l2, setL2] = useState(() =>
    Object.fromEntries(rows.map((r) => [r.id, !!r.l2_done])),
  );

  useEffect(() => {
    if (openMenuId === null) return undefined;
    const close = () => setOpenMenuId(null);
    document.addEventListener("click", close);
    return () => document.removeEventListener("click", close);
  }, [openMenuId]);

  function toggleMenu(event, leadId) {
    event.preventDefault();
    event.stopPropagation();
    const rect = event.currentTarget.getBoundingClientRect();
    setMenuPos({
      top: rect.bottom + 6,
      left: Math.max(12, rect.right - MENU_WIDTH),
    });
    setOpenMenuId((current) => (current === leadId ? null : leadId));
  }

  async function toggleL2(event, leadId, value) {
    event.stopPropagation();
    setL2((s) => ({ ...s, [leadId]: value }));

    try {
      const res = await fetch(
        `/api/business-leads/${business}/${leadId}/quick-toggle`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ field: "l2_done", value }),
        },
      );
      const json = await res.json();
      if (!json.ok) throw new Error(json.error);
    } catch (err) {
      alert(err?.message || "Failed to update checkbox");
      setL2((s) => ({ ...s, [leadId]: !value }));
    }
  }

  async function updateStatus(leadId, status) {
    try {
      const res = await fetch(
        `/api/business-leads/${business}/${leadId}/status`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ status }),
        },
      );
      const json = await res.json();
      if (!json.ok) {
        alert(json.error || "Failed to update status");
        return;
      }
      window.location.reload();
    } catch {
      alert("Failed to update status");
    }
  }

  async function deleteLead(leadId) {
    if (
      !confirm(
        "Delete this lead and all related voice/call data? This cannot be undone.",
      )
    ) {
      return;
    }

    try {
      const res = await fetch(`/api/business-leads/${business}/${leadId}`, {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
      });
      const json = await res.json();
      if (!json.ok) {
        alert(json.error || "Failed to delete lead");
        return;
      }
      window.location.reload();
    } catch {
      alert("Failed to delete lead");
    }
  }

  return (
    <table>
      <thead>
        <tr>
          {COLUMNS.map((c) => (
            <th key={c}>{c}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.length ? (
          rows.map((lead) => {
            const capabilities = lead.manufacturing_capabilities
              ? String(lead.manufacturing_capabilities)
                  .split(",")
                  .filter(Boolean)
                  .slice(0, 4)
              : [];

            const contactLine =
              [lead.contact_name || lead.owner_name, lead.phone]
                .filter(Boolean)
                .join(" · ") || "-";

            const location =
              [lead.city, lead.state, lead.country].filter(Boolean).join(", ") ||
              "-";

            return (
              <tr key={lead.id}>
                <td className={styles.leadNameCell}>
                  <div className={styles.leadCompanyName}>
                    {lead.company ||
                      lead.business_name ||
                      lead.company_name ||
                      `Lead #${lead.id}`}
                    {lead.factory_setup === "multiple_sites" ? (
                      <span className={styles.miniChip}>Multi-site</span>
                    ) : null}
                  </div>

                  <div className={`muted ${styles.leadContactLine}`}>
                    {contactLine}
                  </div>

                  {lead.last_spoke_to_name ? (
                    <div style={{ fontSize: "12px", marginTop: "4px" }}>
                      <strong>Spoke to:</strong> {lead.last_spoke_to_name}
                    </div>
                  ) : null}
                </td>

                <td>
                  <div className={styles.leadChipRow}>
                    {capabilities.length ? (
                      capabilities.map((x, i) => (
                        <span className={styles.leadChip} key={i}>
                          {x.trim()}
                        </span>
                      ))
                    ) : (
                      <span className="muted">No capabilities</span>
                    )}
                  </div>
                </td>

                <td>
                  <div>
                    <strong>
                      {lead.industry_primary || lead.industry || "-"}
                    </strong>
                  </div>
                  <div className="muted">{lead.raw_industry || ""}</div>

                  <div className={styles.leadChipRow}>
                    {[lead.entity_type, lead.company_size, lead.assigned_to]
                      .filter(Boolean)
                      .map((x, i) => (
                        <span className={styles.leadChip} key={i}>
                          {x}
                        </span>
                      ))}
                  </div>
                </td>

                <td>
                  <div>{location}</div>
                  <div className="muted">
                    {lead.pin_code || lead.location || ""}
                  </div>
                </td>

                <td className={styles.qualityCell}>
                  <div className={styles.qualityStack}>
                    <label className={styles.qualityCheck}>
                      <input
                        type="checkbox"
                        checked={!!l2[lead.id]}
                        onChange={(e) =>
                          toggleL2(e, lead.id, e.target.checked)
                        }
                      />
                      <span>L2 Done</span>
                    </label>
                  </div>
                </td>

                <td>
                  {business === "joolian" ? (
                    <>
                      <div>{lead.activity_category || lead.industry || "-"}</div>
                      <div className="muted">
                        {lead.sub_activity_category || ""}
                      </div>
                      <div className="muted">Ages: {lead.age_group || "-"}</div>
                      <div className="muted">
                        Type:{" "}
                        {lead.type_of_business || lead.company_size || "-"}
                      </div>
                      <div className="muted">
                        Price: {lead.pricing_approx || "-"}
                      </div>
                    </>
                  ) : (
                    <>
                      <div>{lead.industry || "-"}</div>
                      <div className="muted">
                        Emp: {lead.number_of_employees || "-"}
                      </div>
                      <div className="muted">
                        Machines: {lead.machine_count || "-"}
                      </div>
                    </>
                  )}
                </td>

                <td>
                  <button
                    className={styles.btn}
                    type="button"
                    onClick={() => onOpenCallSummary(lead.phone || "")}
                  >
                    Calls
                  </button>
                </td>

                <td className={styles.actionsCell}>
                  <button
                    className={styles.kebabBtn}
                    type="button"
                    onClick={(e) => toggleMenu(e, lead.id)}
                  >
                    ...
                  </button>

                  <div
                    className={`${styles.leadActionsMenu} ${
                      openMenuId === lead.id ? styles.open : ""
                    }`}
                    style={
                      openMenuId === lead.id
                        ? { top: `${menuPos.top}px`, left: `${menuPos.left}px` }
                        : undefined
                    }
                  >
                    <button type="button" onClick={() => onOpenEdit(lead.id)}>
                      Edit
                    </button>
                    <button type="button" onClick={() => onOpenCalls(lead.id)}>
                      Save L2 Data / Calls
                    </button>
                    {STATUS_ACTIONS.map(([status, label]) => (
                      <button
                        type="button"
                        key={status}
                        onClick={() => updateStatus(lead.id, status)}
                      >
                        {label}
                      </button>
                    ))}
                    <button
                      type="button"
                      className={styles.dangerMenuItem}
                      onClick={() => deleteLead(lead.id)}
                    >
                      Delete
                    </button>
                  </div>
                </td>
              </tr>
            );
          })
        ) : (
          <tr>
            <td colSpan={7} className="empty-cell">
              No leads found.
            </td>
          </tr>
        )}
      </tbody>
    </table>
  );
}
