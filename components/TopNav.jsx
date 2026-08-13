// Shared top navigation — the React replacement for renderTopNav().
//
// Two differences from the string version, both deliberate:
//
//   1. The status pills and the Clients dropdown were populated by two
//      client-side fetches to /api/top-nav-summary and /api/clients/nav-list.
//      This is a server component, so both are awaited during render and the
//      "Off: ..." / "Loading…" placeholders no longer flash.
//   2. The inline escapeHtmlClient() helper is gone; React escapes text.
//
// Plain <a> rather than next/link is intentional while the migration is in
// flight: several of these destinations are still Express-backed route handlers
// returning whole HTML documents, which the client-side router cannot treat as
// RSC payloads. These become <Link> once every target is a page.

import { getTopNavSummary, getClientsNavList } from "@/lib/data/nav";
import { orgIdFor } from "@/lib/auth";

const NAV_ITEMS = [
  { href: "/dashboard", label: "Dashboard", short: "Home", key: "dashboard" },
  { href: "/tasks", label: "Tasks", short: "Tasks", key: "tasks" },
  { href: "/attendance", label: "Attendance", short: "Attend", key: "attendance" },
  { href: "/reports", label: "Reports", short: "Reports", key: "reports" },
  {
    href: "/clients",
    label: "Clients",
    short: "Clients",
    key: "clients",
    clientsDropdown: true,
  },
  { href: "/account", label: "My Account", short: "Me", key: "account", optional: true },
  { href: "/logout", label: "Logout", short: "⏻", key: "logout", icon: true },
];

function itemClassName(item, active) {
  return [
    active === item.key || (item.key === "leads" && active === "lead-detail")
      ? "active"
      : "",
    item.key === "logout" ? "logout-link" : "",
    item.icon ? "nav-icon-link" : "",
    item.optional ? "nav-text-optional" : "",
  ]
    .filter(Boolean)
    .join(" ");
}

// "Off: 0" when nobody, the names themselves when there are one or two, and a
// bare count beyond that — same thresholds the inline script used.
function statusPill(prefix, count, names, emptyTitle) {
  return {
    title: names.length ? names.join(", ") : emptyTitle,
    label:
      count === 0
        ? `${prefix}: 0`
        : count <= 2
          ? `${prefix}: ${names.join(", ")}`
          : `${prefix}: ${count}`,
  };
}

// `authenticated` must be false on pages that render this nav WITHOUT an auth
// gate in front of them — currently just "/". The old nav populated its Clients
// dropdown from a client-side fetch to /api/clients/nav-list, which redirects to
// /login for an anonymous visitor, so the list stayed empty there. Rendering it
// server-side would otherwise hand the client roster to anyone hitting "/".
//
// It is a separate prop rather than a `user != null` check because
// requireDashboardUser() legitimately returns null on an authorised basic-auth
// request when no admin/manager row exists to attach.
export default async function TopNav({
  active = "",
  user = null,
  authenticated = true,
}) {
  const [summary, clients] = await Promise.all([
    getTopNavSummary(user),
    authenticated ? getClientsNavList(orgIdFor(user)) : Promise.resolve([]),
  ]);

  const off = statusPill(
    "Off",
    summary.offCount,
    summary.offNames,
    "Nobody off today",
  );
  const onBreak = statusPill(
    "Break",
    summary.breakCount,
    summary.breakNames,
    "Nobody on break",
  );

  return (
    <div className="top-nav">
      <div className="top-nav-inner">
        <a className="brand" href="/dashboard" title="WeSolveHR">
          WeSolve
        </a>

        <div className="nav-links">
          {NAV_ITEMS.map((item) => {
            const className = itemClassName(item, active);

            if (item.clientsDropdown) {
              return (
                <div className="nav-dropdown-wrap" key={item.key}>
                  <a
                    href={item.href}
                    className={className}
                    title={item.label}
                    aria-label={item.label}
                  >
                    {item.short || item.label}
                  </a>
                  <div className="nav-dropdown-menu" id="clientsNavMenu">
                    <a href="/clients">All clients</a>
                    {clients.length === 0 ? (
                      <div className="meta" style={{ padding: "8px 11px" }}>
                        No clients yet
                      </div>
                    ) : (
                      clients.map((client) => {
                        const base = `/clients/${encodeURIComponent(client.id)}`;
                        return (
                          <div className="nav-submenu-wrap" key={client.id}>
                            <a className="nav-submenu-label" href={base}>
                              {client.name} <span>›</span>
                            </a>
                            <div className="nav-submenu-menu">
                              <a href={base}>Open Workspace</a>
                              <a href={`${base}?tab=leads&addLead=1`}>Add Lead</a>
                              <a href={`${base}?tab=leads`}>Leads</a>
                              <a href={`${base}?tab=report`}>Report</a>
                            </div>
                          </div>
                        );
                      })
                    )}
                  </div>
                </div>
              );
            }

            return (
              <a
                key={item.key}
                href={item.href}
                className={className}
                title={item.label}
                aria-label={item.label}
              >
                {item.short || item.label}
              </a>
            );
          })}
        </div>

        <div className="top-nav-end">
          <div className="top-nav-status" id="topNavStatus">
            <span className="top-nav-pill" title={off.title}>
              {off.label}
            </span>
            <span className="top-nav-pill" title={onBreak.title}>
              {onBreak.label}
            </span>
          </div>
          <a
            className={`nav-icon-link${active === "team-work" ? " active" : ""}`}
            href="/team-work"
            title="Team Work"
            aria-label="Team Work"
          >
            📅
          </a>
          <a
            className={active === "help" ? "active" : ""}
            href="/help"
            title="Help"
            aria-label="Help"
          >
            Help
          </a>
          <a
            className={`nav-icon-link${active === "logs" ? " active" : ""}`}
            href="/logs"
            title="Logs"
            aria-label="Logs"
          >
            ?
          </a>
        </div>
      </div>
    </div>
  );
}
