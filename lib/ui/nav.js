// Shared chrome for every dashboard page: the top navigation bar. Markup
// extracted verbatim from renderTopNav (monolith lines 14076-14353); its
// inline <script> body now lives in public/js/top-nav.js and is loaded by
// src so the markup stays declarative.

import { escapeHtml } from "./html.js";

function renderTopNav(active = "") {
  const items = [
    { href: "/dashboard", label: "Dashboard", short: "Home", key: "dashboard" },
    { href: "/tasks", label: "Tasks", short: "Tasks", key: "tasks" },
    {
      href: "/attendance",
      label: "Attendance",
      short: "Attend",
      key: "attendance",
    },
    { href: "/reports", label: "Reports", short: "Reports", key: "reports" },
    // {
    //   href: "/leads",
    //   label: "Leads",
    //   short: "Leads",
    //   key: "leads",
    //   dropdown: getActiveLeadBusinesses().map((b) => ({
    //     href: `/leads/${b.business}`,
    //     label: b.label,
    //     children: [
    //       {
    //         href: `/leads/${b.business}?openAddLead=1`,
    //         label: "Add Lead",
    //       },
    //       {
    //         href: `/leads/${b.business}/intelligence`,
    //         label: "Intelligence",
    //       },
    //     ],
    //   })),
    // },
    {
      href: "/clients",
      label: "Clients",
      short: "Clients",
      key: "clients",
      clientsDropdown: true,
    },
    {
      href: "/account",
      label: "My Account",
      short: "Me",
      key: "account",
      optional: true,
    },
    { href: "/logout", label: "Logout", short: "⏻", key: "logout", icon: true },
  ];

  return `
    <div class="top-nav">
      <div class="top-nav-inner">
        <a class="brand" href="/dashboard" title="WeSolveHR">WeSolve</a>

        <div class="nav-links">
          ${items
            .map((item) => {
              const classes = [
                active === item.key ||
                (item.key === "leads" && active === "lead-detail")
                  ? "active"
                  : "",
                item.key === "logout" ? "logout-link" : "",
                item.icon ? "nav-icon-link" : "",
                item.optional ? "nav-text-optional" : "",
              ]
                .filter(Boolean)
                .join(" ");

              // Clients item: dropdown of client names is loaded client-side
              // (see the populate script below) into #clientsNavMenu.
              if (item.clientsDropdown) {
                return `
      <div class="nav-dropdown-wrap">
        <a
          href="${item.href}"
          class="${classes}"
          title="${escapeHtml(item.label)}"
          aria-label="${escapeHtml(item.label)}"
        >
          ${escapeHtml(item.short || item.label)}
        </a>
        <div class="nav-dropdown-menu" id="clientsNavMenu">
          <a href="/clients">All clients</a>
          <div class="meta" style="padding:8px 11px;">Loading…</div>
        </div>
      </div>
    `;
              }

              const dropdownHtml =
                item.dropdown && item.dropdown.length
                  ? `
      <div class="nav-dropdown-wrap">
        <a
          href="${item.href}"
          class="${classes}"
          title="${escapeHtml(item.label)}"
          aria-label="${escapeHtml(item.label)}"
        >
          ${escapeHtml(item.short || item.label)}
        </a>
        <div class="nav-dropdown-menu">
          ${item.dropdown
            .map((child) => {
              if (child.children && child.children.length) {
                return `
        <div class="nav-submenu-wrap">
          <a class="nav-submenu-label" href="${child.href}">
            ${escapeHtml(child.label)} <span>›</span>
          </a>

          <div class="nav-submenu-menu">
            ${child.children
              .map(
                (sub) => `
                  <a href="${sub.href}">
                    ${escapeHtml(sub.label)}
                  </a>
                `,
              )
              .join("")}
          </div>
        </div>
      `;
              }

              return `
      <a href="${child.href}">
        ${escapeHtml(child.label)}
      </a>
    `;
            })
            .join("")}
        </div>
      </div>
    `
                  : `
      <a
        href="${item.href}"
        class="${classes}"
        title="${escapeHtml(item.label)}"
        aria-label="${escapeHtml(item.label)}"
      >
        ${escapeHtml(item.short || item.label)}
      </a>
    `;

              return dropdownHtml;
            })
            .join("")}
        </div>

        <div class="top-nav-end">
          <div class="top-nav-status" id="topNavStatus">
            <span class="top-nav-pill loading">Off: ...</span>
            <span class="top-nav-pill loading">Break: ...</span>
          </div>
          <a
            class="nav-icon-link${active === "team-work" ? " active" : ""}"
            href="/team-work"
            title="Team Work"
            aria-label="Team Work"
          >📅</a>
          <a
            class="${active === "help" ? "active" : ""}"
            href="/help"
            title="Help"
            aria-label="Help"
          >Help</a>
          <a
            class="nav-icon-link${active === "logs" ? " active" : ""}"
            href="/logs"
            title="Logs"
            aria-label="Logs"
          >?</a>
        </div>
      </div>
    </div>

    <script src="/js/top-nav.js"></script>
  `;
}


export { renderTopNav };
