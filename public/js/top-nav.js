// Extracted verbatim from renderTopNav() in the original monolith
// (lib/server/app.js lines 14256-14350).
(function () {
  const mount = document.getElementById("topNavStatus");
  if (!mount) return;

  fetch("/api/top-nav-summary")
    .then((r) => r.json())
    .then((json) => {
      if (!json.ok || !json.data) {
        mount.innerHTML =
          '<span class="top-nav-pill muted">Off: -</span>' +
          '<span class="top-nav-pill muted">Break: -</span>';
        return;
      }

      const data = json.data;

      const offTitle = data.offNames && data.offNames.length
        ? data.offNames.join(", ")
        : "Nobody off today";

      const breakTitle = data.breakNames && data.breakNames.length
        ? data.breakNames.join(", ")
        : "Nobody on break";

      const offLabel =
        (data.offCount || 0) === 0
          ? "Off: 0"
          : (data.offCount || 0) <= 2
            ? "Off: " + data.offNames.join(", ")
            : "Off: " + data.offCount;

      const breakLabel =
        (data.breakCount || 0) === 0
          ? "Break: 0"
          : (data.breakCount || 0) <= 2
            ? "Break: " + data.breakNames.join(", ")
            : "Break: " + data.breakCount;

      mount.innerHTML =
        '<span class="top-nav-pill" title="' + escapeHtmlClient(offTitle) + '">' + escapeHtmlClient(offLabel) + '</span>' +
        '<span class="top-nav-pill" title="' + escapeHtmlClient(breakTitle) + '">' + escapeHtmlClient(breakLabel) + '</span>';
    })
    .catch(() => {
      mount.innerHTML =
        '<span class="top-nav-pill muted">Off: -</span>' +
        '<span class="top-nav-pill muted">Break: -</span>';
    });
})();

// Populate the Clients dropdown with client names. Each client expands to
// a submenu of quick actions (Open Workspace, Add Lead, Leads, Report).
(function () {
  var menu = document.getElementById("clientsNavMenu");
  if (!menu) return;
  fetch("/api/clients/nav-list")
    .then(function (r) { return r.json(); })
    .then(function (json) {
      if (!json.ok || !json.data || !json.data.length) {
        menu.innerHTML =
          '<a href="/clients">All clients</a>' +
          '<div class="meta" style="padding:8px 11px;">No clients yet</div>';
        return;
      }
      var html = '<a href="/clients">All clients</a>';
      json.data.forEach(function (c) {
        var base = "/clients/" + encodeURIComponent(c.id);
        html +=
          '<div class="nav-submenu-wrap">' +
            '<a class="nav-submenu-label" href="' + base + '">' +
              escapeHtmlClient(c.name) + ' <span>›</span>' +
            '</a>' +
            '<div class="nav-submenu-menu">' +
              '<a href="' + base + '">Open Workspace</a>' +
              '<a href="' + base + '?tab=leads&addLead=1">Add Lead</a>' +
              '<a href="' + base + '?tab=leads">Leads</a>' +
              '<a href="' + base + '?tab=report">Report</a>' +
            '</div>' +
          '</div>';
      });
      menu.innerHTML = html;
    })
    .catch(function () {
      menu.innerHTML =
        '<a href="/clients">All clients</a>' +
        '<div class="meta" style="padding:8px 11px;">Failed to load</div>';
    });
})();

function escapeHtmlClient(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
