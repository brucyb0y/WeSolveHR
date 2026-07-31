// Client script for GET /logs, extracted verbatim from the inline
// <script> of its Express handler (lib/server/app.js lines 47405-47957).
function escapeHtmlClient(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function getOutcomeBadgeClass(status) {
  const s = String(status || "").toLowerCase();

  if (s === "completed") return "log-badge log-badge-success";
  if (s === "failed") return "log-badge log-badge-danger";
  if (s === "processing") return "log-badge log-badge-warn";
  return "log-badge log-badge-muted";
}

function truncateText(text, limit = 120) {
  const value = String(text || "").trim();
  if (value.length <= limit) return value;
  return value.slice(0, limit) + "…";
}

function renderPersonStats(title, obj) {
  const entries = Object.entries(obj || {}).sort((a, b) => b[1] - a[1]);

  if (!entries.length) {
    return (
      '<div class="person-stat-box">' +
        '<div class="person-stat-title">' + escapeHtmlClient(title) + '</div>' +
        '<div class="muted">No commands</div>' +
      '</div>'
    );
  }

  return (
    '<div class="person-stat-box">' +
      '<div class="person-stat-title">' + escapeHtmlClient(title) + '</div>' +
      entries
        .map(function ([name, count]) {
          return '<span class="person-chip">' + escapeHtmlClient(name) + ': ' + count + '</span>';
        })
        .join("") +
    '</div>'
  );
}


function toggleCommand(id) {
  const el = document.getElementById("commandFull-" + id);
  if (el) el.classList.toggle("open");
}

function clearLogFilters() {
  document.getElementById("filterSearch").value = "";
  document.getElementById("filterUser").value = "";
  document.getElementById("filterOutcome").value = "";
  document.getElementById("filterDay").value = "";
  document.getElementById("filterMonth").value = "";
  loadLogs();
}

async function loadLogs() {
  try {
    const params = new URLSearchParams();

    const q = document.getElementById("filterSearch")?.value || "";
    const user = document.getElementById("filterUser")?.value || "";
    const outcome = document.getElementById("filterOutcome")?.value || "";
    const day = document.getElementById("filterDay")?.value || "";
    const month = document.getElementById("filterMonth")?.value || "";

    if (q.trim()) params.set("q", q.trim());
    if (user.trim()) params.set("user", user.trim());
    if (outcome) params.set("outcome", outcome);
    if (day) params.set("day", day);
    if (month) params.set("month", month);

    const res = await fetch("/api/logs?" + params.toString());
    const json = await res.json();

    if (!json.ok) return;

    const payload = json.data || {};
    const rows = payload.rows || [];
    const stats = payload.stats || {};

document.getElementById("logsStats").innerHTML =
  '<div class="log-stat-card">' +
    '<div class="log-stat-label">Loaded Logs</div>' +
    '<div class="log-stat-value">' + (stats.total || 0) + '</div>' +
  '</div>' +
  '<div class="log-stat-card">' +
    '<div class="log-stat-label">Completed</div>' +
    '<div class="log-stat-value">' + (stats.completed || 0) + '</div>' +
  '</div>' +
  '<div class="log-stat-card">' +
    '<div class="log-stat-label">Failed</div>' +
    '<div class="log-stat-value">' + (stats.failed || 0) + '</div>' +
  '</div>' +
  '<div class="log-stat-card">' +
    '<div class="log-stat-label">Unknown</div>' +
    '<div class="log-stat-value">' + (stats.unknown || 0) + '</div>' +
  '</div>' +
  '<div class="person-stats" style="grid-column: 1 / -1;">' +
    renderPersonStats("Commands Today by Person", stats.byPersonToday) +
    renderPersonStats("Commands This Month by Person", stats.byPersonMonth) +
  '</div>';

    document.getElementById("logRows").innerHTML = rows.length
      ? rows
          .map((row) => {
            const id = Number(row.id);
            const fullCommand = escapeHtmlClient(row.body || "-");
            const shortCommand = escapeHtmlClient(truncateText(row.body || "-", 130));
            const error = row.outcome_error || "";

return (
  '<tr>' +
    '<td>' + escapeHtmlClient(row.created_at_text || row.created_at || "") + '</td>' +
    '<td><strong>' + escapeHtmlClient(row.sender || "") + '</strong></td>' +
    '<td class="command-cell">' +
      '<div class="command-preview">' + shortCommand + '</div>' +
      (
        String(row.body || "").length > 130
          ? '<button class="mini-link" onclick="toggleCommand(' + id + ')">View full</button>' +
            '<div id="commandFull-' + id + '" class="command-full">' + fullCommand + '</div>'
          : ''
      ) +
    '</td>' +
    '<td>' +
      '<span class="' + getOutcomeBadgeClass(row.outcome_status) + '">' +
        escapeHtmlClient(row.outcome_status || "-") +
      '</span>' +
    '</td>' +
    '<td>' + escapeHtmlClient(row.outcome_result_type || "-") + '</td>' +
    '<td>' +
      (
        error
          ? '<span class="exception-pill" title="' + escapeHtmlClient(error) + '">' +
              escapeHtmlClient(error) +
            '</span>'
          : '<span class="exception-none">-</span>'
      ) +
    '</td>' +
    '<td class="sid-small" title="' + escapeHtmlClient(row.message_sid || "-") + '">' +
      escapeHtmlClient(truncateText(row.message_sid || "-", 14)) +
    '</td>' +
  '</tr>'
);
          })
          .join("")
      : '<tr><td colspan="7" class="empty-cell">No logs found.</td></tr>';
  } catch (err) {
    console.error("Failed to load logs:", err);
  }
}

["filterSearch", "filterUser"].forEach((id) => {
  document.addEventListener("input", function (event) {
    if (event.target && event.target.id === id) {
      clearTimeout(window.__logsFilterTimer);
      window.__logsFilterTimer = setTimeout(loadLogs, 450);
    }
  });
});

["filterOutcome", "filterDay", "filterMonth"].forEach((id) => {
  document.addEventListener("change", function (event) {
    if (event.target && event.target.id === id) {
      loadLogs();
    }
  });
});

loadLogs();

setInterval(loadLogs, 60000);

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    loadLogs();
  }
});
