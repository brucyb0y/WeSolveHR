"use client";

// Clients dropdown in the top nav. Replaces the inline IIFE in renderTopNav()
// that fetched /api/clients/nav-list and built the submenu by hand. Each client
// expands to the same quick actions (Open Workspace, Add Lead, Leads, Report).

import { useEffect, useState } from "react";
import styles from "./TopNav.module.css";

export default function ClientsNavMenu({ item, active }) {
  const [clients, setClients] = useState(null); // null = loading
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let alive = true;
    fetch("/api/clients/nav-list")
      .then((r) => r.json())
      .then((json) => {
        if (!alive) return;
        if (json.ok && Array.isArray(json.data)) setClients(json.data);
        else setClients([]);
      })
      .catch(() => {
        if (alive) setFailed(true);
      });
    return () => {
      alive = false;
    };
  }, []);

  const linkClass = active === item.key ? styles.active : undefined;

  return (
    <div className={styles.navDropdownWrap}>
      <a
        href={item.href}
        className={linkClass}
        title={item.label}
        aria-label={item.label}
      >
        {item.short || item.label}
      </a>
      <div className={styles.navDropdownMenu}>
        <a href="/clients">All clients</a>
        {clients === null && !failed ? (
          <div className={styles.dropdownNote}>Loading…</div>
        ) : null}
        {failed ? <div className={styles.dropdownNote}>Failed to load</div> : null}
        {clients !== null && clients.length === 0 && !failed ? (
          <div className={styles.dropdownNote}>No clients yet</div>
        ) : null}
        {(clients || []).map((c) => {
          const base = `/clients/${encodeURIComponent(c.id)}`;
          return (
            <div className={styles.navSubmenuWrap} key={c.id}>
              <a className={styles.navSubmenuLabel} href={base}>
                {c.name} <span>›</span>
              </a>
              <div className={styles.navSubmenuMenu}>
                <a href={base}>Open Workspace</a>
                <a href={`${base}?tab=leads&addLead=1`}>Add Lead</a>
                <a href={`${base}?tab=leads`}>Leads</a>
                <a href={`${base}?tab=report`}>Report</a>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
