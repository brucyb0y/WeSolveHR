"use client";

// Clients table with the per-row "⋯" actions menu. Replaces the inline
// toggleClientActionsMenu() script in renderClientsListPage(): a single
// open-menu-at-a-time model (React state) drives a fixed-position floating menu,
// which closes on outside click or Escape — same behavior as the original.

import { useEffect, useState } from "react";
import styles from "./clients.module.css";

function healthBadgeClass(health) {
  if (health === "at_risk") return styles.badgeDanger;
  if (health === "watch") return styles.badgeWarn;
  return styles.badgeOk;
}

export default function ClientsTable({ clients }) {
  // menu = null | { id, top, left } — only one row's menu is open at a time.
  const [menu, setMenu] = useState(null);

  useEffect(() => {
    if (!menu) return;
    const close = () => setMenu(null);
    const onKey = (e) => {
      if (e.key === "Escape") setMenu(null);
    };
    document.addEventListener("click", close);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("click", close);
      document.removeEventListener("keydown", onKey);
    };
  }, [menu]);

  function toggleMenu(event, clientId) {
    event.stopPropagation();
    const rect = event.currentTarget.getBoundingClientRect();
    setMenu((prev) =>
      prev?.id === clientId
        ? null
        : {
            id: clientId,
            top: rect.bottom + 6,
            left: Math.max(12, rect.right - 180),
          },
    );
  }

  return (
    <>
      <table>
        <thead>
          <tr>
            <th>Client</th>
            <th>Services</th>
            <th>Project Manager</th>
            <th>Status</th>
            <th>Health</th>
            <th>Open Work</th>
            <th>Waiting</th>
            <th>Last Update</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {clients.length ? (
            clients.map((client) => {
              const serviceNames =
                (client.service_names || []).join(", ") || "-";
              return (
                <tr key={client.id}>
                  <td>
                    <div style={{ fontWeight: 800 }}>
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
                    <span
                      className={`${styles.badge} ${healthBadgeClass(
                        client.health_status,
                      )}`}
                    >
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
                      onClick={(e) => toggleMenu(e, client.id)}
                    >
                      ⋯
                    </button>
                  </td>
                </tr>
              );
            })
          ) : (
            <tr>
              <td colSpan={9} className={styles.empty}>
                No clients yet. Click “New Client” to start.
              </td>
            </tr>
          )}
        </tbody>
      </table>

      {menu ? (
        <div
          className={`${styles.floatingActionsMenu} ${styles.open}`}
          style={{ top: menu.top, left: menu.left }}
          onClick={(e) => e.stopPropagation()}
        >
          <a href={`/clients/${menu.id}/edit`}>Edit Client</a>
          <a href={`/clients/${menu.id}/reset`}>Reset Workspace</a>
          <a href={`/clients/${menu.id}`}>Open Workspace</a>
        </div>
      ) : null}
    </>
  );
}
