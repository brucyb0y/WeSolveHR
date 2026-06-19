// Top navigation bar (Server Component). Ported from renderTopNav() in
// lib/server/app.js. The static markup renders on the server; the two live
// pieces — the off/break status pills and the Clients dropdown — are small
// client islands (TopNavStatus, ClientsNavMenu) that fetch their data the same
// way the original inline scripts did.

import styles from "./TopNav.module.css";
import TopNavStatus from "./TopNavStatus.jsx";
import ClientsNavMenu from "./ClientsNavMenu.jsx";

const NAV_ITEMS = [
  { href: "/dashboard", label: "Dashboard", short: "Home", key: "dashboard" },
  { href: "/tasks", label: "Tasks", short: "Tasks", key: "tasks" },
  { href: "/attendance", label: "Attendance", short: "Attend", key: "attendance" },
  { href: "/reports", label: "Reports", short: "Reports", key: "reports" },
  { href: "/clients", label: "Clients", short: "Clients", key: "clients", clientsDropdown: true },
  { href: "/account", label: "My Account", short: "Me", key: "account", optional: true },
  { href: "/logout", label: "Logout", short: "⏻", key: "logout", icon: true },
];

function navLinkClass(item, active) {
  return [
    active === item.key || (item.key === "leads" && active === "lead-detail")
      ? styles.active
      : "",
    item.key === "logout" ? styles.logoutLink : "",
    item.icon ? styles.navIconLink : "",
    item.optional ? styles.navTextOptional : "",
  ]
    .filter(Boolean)
    .join(" ");
}

export default function TopNav({ active = "" }) {
  return (
    <div className={styles.topNav}>
      <div className={styles.topNavInner}>
        <a className={styles.brand} href="/dashboard" title="WeSolveHR">
          WeSolve
        </a>

        <div className={styles.navLinks}>
          {NAV_ITEMS.map((item) => {
            if (item.clientsDropdown) {
              return <ClientsNavMenu key={item.key} item={item} active={active} />;
            }

            const classes = navLinkClass(item, active);
            return (
              <a
                key={item.key}
                href={item.href}
                className={classes || undefined}
                title={item.label}
                aria-label={item.label}
              >
                {item.short || item.label}
              </a>
            );
          })}
        </div>

        <div className={styles.topNavEnd}>
          <div className={styles.topNavStatus}>
            <TopNavStatus />
          </div>
          <a
            className={active === "help" ? styles.active : undefined}
            href="/help"
            title="Help"
            aria-label="Help"
          >
            Help
          </a>
          <a
            className={`${styles.navIconLink}${active === "logs" ? ` ${styles.active}` : ""}`}
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
