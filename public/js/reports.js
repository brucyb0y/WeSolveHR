// Client script for GET /reports (org view), extracted verbatim from the
// inline <script> of renderReportsPage() (lib/server/app.js lines 29958-30602).
async function loadReportSummary() {
  const mount = document.getElementById("reportsSummary");
  if (!mount) return;

  const params = new URLSearchParams(window.location.search);
  const date = params.get("date") || "";
  const userId = params.get("userId") || "";

  const qs = new URLSearchParams();
  if (date) qs.set("date", date);
  if (userId) qs.set("userId", userId);

  const url = "/api/reports/summary" + (qs.toString() ? "?" + qs.toString() : "");

  try {
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
    });

    const json = await res.json();

    if (!json.ok) {
      mount.innerHTML =
        '<div class="panel" style="padding:18px; margin-bottom:16px;">' +
          '<div class="muted">Failed to load summary.</div>' +
        '</div>';
      return;
    }

    mount.innerHTML =
      json.data.summaryHtml ||
      '<div class="panel" style="padding:18px; margin-bottom:16px;">' +
        '<div class="muted">No summary available.</div>' +
      '</div>';
  } catch (error) {
    mount.innerHTML =
      '<div class="panel" style="padding:18px; margin-bottom:16px;">' +
        '<div class="muted">Failed to load summary.</div>' +
      '</div>';
  }
}

async function loadReportCards() {
  const grid = document.getElementById("reportsGrid");
  if (!grid) return;

  const params = new URLSearchParams(window.location.search);
  const date = params.get("date") || "";
  const userId = params.get("userId") || "";

  const qs = new URLSearchParams();
  if (date) qs.set("date", date);
  if (userId) qs.set("userId", userId);

  const url = "/api/reports/cards" + (qs.toString() ? "?" + qs.toString() : "");

  try {
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
    });

    const json = await res.json();

    if (!json.ok) {
      grid.innerHTML =
        '<div class="panel" style="padding:18px;">' +
          '<div class="muted">Failed to load reports.</div>' +
        '</div>';
      return;
    }

    grid.innerHTML =
      json.data.cardsHtml ||
      '<div class="panel" style="padding:18px;">' +
        '<div class="muted">No users found.</div>' +
      '</div>';

    filterReports();
  } catch (error) {
    grid.innerHTML =
      '<div class="panel" style="padding:18px;">' +
        '<div class="muted">Failed to load reports.</div>' +
      '</div>';
  }
}

          function filterReports() {
            const input = document.getElementById("reportSearch");
            const query = String(input?.value || "").trim().toLowerCase();
            const cards = document.querySelectorAll(".report-card");

            for (const card of cards) {
              const userName = String(card.getAttribute("data-user-name") || "");
              card.style.display = !query || userName.includes(query) ? "" : "none";
            }
          }

function closeTaskModal(event) {
  if (event && event.target && event.target.id !== "taskModal") return;
  document.getElementById("taskModal").classList.remove("open");
}

function renderHistoryDetail(item) {
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};

  if (item.changeType === "task_created") {
    const owners = Array.isArray(newValue.owners) && newValue.owners.length
      ? newValue.owners.join(", ")
      : "-";

    return [
      "Created task",
      "Title: " + (newValue.title || "-"),
      "Owners: " + owners,
      "Priority: " + (newValue.priority || "-"),
      "Deadline: " + (newValue.deadline || "-"),
      "Business / Area: " + (newValue.business || "-") + " / " + (newValue.area || "-")
    ].join("\n");
  }

if (item.changeType === "status_change") {
  const oldStatus = oldValue.status || "-";
  const newStatus = newValue.status || "-";
  const oldProgress = oldValue.progress ?? "-";
  const newProgress = newValue.progress ?? "-";
  const note = newValue.note ? "\nNote: " + newValue.note : "";

return (
  "Status: " + oldStatus + " → " + newStatus +
  "\nProgress: " + oldProgress + "% → " + newProgress + "%" +
  note
);
}

  if (item.changeType === "progress_change") {
    return [
      "Progress: " + (oldValue.progress ?? "-") + "% → " + (newValue.progress ?? "-") + "%",
      "Status: " + (oldValue.status || "-") + " → " + (newValue.status || "-"),
      newValue.note ? "Note: " + newValue.note : null
    ].filter(Boolean).join("\n");
  }

  if (item.changeType === "owner_change") {
    const oldOwners = Array.isArray(oldValue.owners) ? oldValue.owners.join(", ") : "-";
    const newOwners = Array.isArray(newValue.owners) ? newValue.owners.join(", ") : "-";
    return "Owners: " + oldOwners + " → " + newOwners;
  }

  if (item.changeType === "deadline_change") {
    return "Deadline: " + (oldValue.deadline || "-") + " → " + (newValue.deadline || "-");
  }

  if (item.fieldName === "blocker_note") {
    return [
      "Blocker: " + (newValue.blocker_note || "-"),
      newValue.note ? "Note: " + newValue.note : null
    ].filter(Boolean).join("\n");
  }

  if (item.fieldName === "title") {
    return "Title: " + (oldValue.title || "-") + " → " + (newValue.title || "-");
  }

  if (item.fieldName === "detail") {
    return "Detail updated";
  }

  if (item.fieldName === "priority") {
    return "Priority: " + (oldValue.priority || "-") + " → " + (newValue.priority || "-");
  }

  if (item.fieldName === "business") {
    return "Business: " + (oldValue.business || "-") + " → " + (newValue.business || "-");
  }

  if (item.fieldName === "area") {
    return "Area: " + (oldValue.area || "-") + " → " + (newValue.area || "-");
  }

  return "Updated";
}

async function openTaskDetail(taskNo) {
  const modal = document.getElementById("taskModal");
  const title = document.getElementById("modalTitle");
  const body = document.getElementById("modalBody");

  title.textContent = "Task #" + taskNo;
  body.innerHTML = '<div class="muted">Loading task details...</div>';
  modal.classList.add("open");

  try {
    const res = await fetch("/api/reports/task/" + taskNo);
    const json = await res.json();

    if (!json.ok) {
      body.innerHTML = '<div class="muted">' + (json.error || "Failed to load task") + '</div>';
      return;
    }

    const task = json.data || {};
    title.textContent = "#" + (task.taskNo || task.id) + " — " + (task.title || "Untitled");

    const historyHtml = (task.history || []).length
      ? task.history.map(function(item) {
          return (
            '<div class="history-item">' +
              '<div class="history-top">' +
                '<strong>' + (item.changeType || "-") + '</strong>' +
                '<span>' + (item.at || "-") + ' • ' + (item.by || "-") + '</span>' +
              '</div>' +
              '<div class="history-detail">' + renderHistoryDetail(item) + '</div>' +
            '</div>'
          );
        }).join("")
      : '<div class="muted">No recent history</div>';

    body.innerHTML =
      '<div class="modal-meta-grid">' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Owners</div><div>' + ((task.owners || []).join(", ") || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Status</div><div>' + (task.status || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Priority</div><div>' + (task.priority || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Progress</div><div>' + (task.progress ?? "-") + '%</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Deadline</div><div>' + (task.deadline || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Business / Area</div><div>' + ((task.business || "-") + ' / ' + (task.area || "-")) + '</div></div>' +
      '</div>' +

      '<div class="report-section">' +
        '<div class="section-title">Detail</div>' +
        '<div>' + (task.detail || '<span class="muted">No detail</span>') + '</div>' +
      '</div>' +

      '<div class="report-section">' +
        '<div class="section-title">Blocker</div>' +
        '<div>' + (task.blockerNote || '<span class="muted">No blocker</span>') + '</div>' +
      '</div>' +

      '<div class="report-section">' +
        '<div class="section-title">Recent history</div>' +
        '<div class="history-list">' + historyHtml + '</div>' +
      '</div>';
  } catch (error) {
    body.innerHTML = '<div class="muted">Failed to load task detail</div>';
  }
}

document.addEventListener("DOMContentLoaded", function () {
  loadReportSummary();
  loadReportCards();
});
