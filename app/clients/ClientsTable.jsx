"use client";

// Client rows plus the per-row "⋯" actions menu.
//
// The old page kept the menu in the DOM for every row and toggled an .open
// class, closing the others by hand. Here a single `openId` holds whichever row
// is expanded, so "only one open at a time" falls out of the state rather than
// a querySelectorAll sweep. Position is still computed from the button's
// bounding rect because the menu is position: fixed.

import { useEffect, useState } from "react";
import styles from "./clients.module.css";

const MENU_WIDTH = 180;

function healthBadgeClass(health) {
  if (health === "at_risk") return `${styles.badge} ${styles.badgeDanger}`;
  if (health === "watch") return `${styles.badge} ${styles.badgeWarn}`;
  return `${styles.badge} ${styles.badgeOk}`;
}

export default function ClientsTable({ clients }) {
  const [openId, setOpenId] = useState(null);
  const [position, setPosition] = useState({ top: 0, left: 0 });

  useEffect(() => {
    if (openId === null) return undefined;

    const closeAll = () => setOpenId(null);
    const onKeyDown = (event) => {
      if (event.key === "Escape") setOpenId(null);
    };

    document.addEventListener("click", closeAll);
    document.addEventListener("keydown", onKeyDown);

    return () => {
      document.removeEventListener("click", closeAll);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [openId]);

  function toggleMenu(event, clientId) {
    // Keeps the document listener above from closing the menu we are opening.
    event.stopPropagation();

    const rect = event.currentTarget.getBoundingClientRect();
    setPosition({
      top: rect.bottom + 6,
      left: Math.max(12, rect.right - MENU_WIDTH),
    });
    setOpenId((current) => (current === clientId ? null : clientId));
  }

  if (!clients.length) {
    return (
      <tbody>
        <tr>
          <td colSpan={9} className={styles.empty}>
            No clients yet. Click “New Client” to start.
          </td>
        </tr>
      </tbody>
    );
  }

  return (
    <tbody>
      {clients.map((client) => {
        const serviceNames = (client.service_names || []).join(", ") || "-";
        const isOpen = openId === client.id;

        return (
          <tr key={client.id}>
            <td>
              <div className={styles.clientName}>
                <a
                  href={`/clients/${client.id}`}
                  className={styles.clientNameLink}
                >
                  {client.name}
                </a>
              </div>
              <div className="muted">{client.company_name || "-"}</div>
            </td>
            <td>{serviceNames}</td>
            <td>{client.project_manager_name || "-"}</td>
            <td>
              <span className={`${styles.badge} ${styles.badgeInfo}`}>
                {client.status || "-"}
              </span>
            </td>
            <td>
              <span className={healthBadgeClass(client.health_status)}>
                {client.health_status || "-"}
              </span>
            </td>
            <td>{client.open_work_count || 0}</td>
            <td>{client.waiting_count || 0}</td>
            <td>{client.last_update_text || "-"}</td>
            <td>
              <button
                className={styles.actionKebab}
                type="button"
                onClick={(event) => toggleMenu(event, client.id)}
              >
                ⋯
              </button>

              <div
                className={`${styles.floatingActionsMenu} ${isOpen ? styles.open : ""}`}
                style={
                  isOpen
                    ? { top: `${position.top}px`, left: `${position.left}px` }
                    : undefined
                }
              >
                <a href={`/clients/${client.id}/edit`}>Edit Client</a>
                <a href={`/clients/${client.id}/reset`}>Reset Workspace</a>
                <a href={`/clients/${client.id}`}>Open Workspace</a>
              </div>
            </td>
          </tr>
        );
      })}
    </tbody>
  );
}
