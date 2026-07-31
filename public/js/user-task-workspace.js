// Client script for GET /tasks/user/:userId, extracted verbatim from the
// inline <script> of renderUserTaskWorkspacePage() (lib/server/app.js lines 1843-2509).

        function escapeHtmlClient(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}


function renderUserWorkspaceTaskHistoryDetail(item) {
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};

  if (item.changeType === "progress_change") {
    return "Progress: " + (oldValue.progress ?? 0) + "% → " + (newValue.progress ?? 0) + "%" +
      (newValue.note ? "\nNote: " + newValue.note : "");
  }

  if (item.changeType === "status_change") {
    return "Status: " + (oldValue.status || "-") + " → " + (newValue.status || "-") +
      (newValue.note ? "\nNote: " + newValue.note : "");
  }

  if (item.changeType === "owner_change") {
    const oldOwners = Array.isArray(oldValue.owners) ? oldValue.owners.join(", ") : "-";
    const newOwners = Array.isArray(newValue.owners) ? newValue.owners.join(", ") : "-";
    return "Owners: " + oldOwners + " → " + newOwners;
  }

  if (item.changeType === "deadline_change") {
    return "Deadline: " + (oldValue.deadline || "-") + " → " + (newValue.deadline || "-");
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

  if (item.fieldName === "blocker_note") {
    return [
      "Blocker: " + (newValue.blocker_note || "-"),
      newValue.note ? "Note: " + newValue.note : null
    ].filter(Boolean).join("\n");
  }

  if (item.fieldName) {
    return (item.fieldName || "Field") + ": " +
      JSON.stringify(oldValue) + " → " + JSON.stringify(newValue);
  }

  return JSON.stringify(newValue || {});
}

async function openUserWorkspaceTaskDetail(taskNo) {
  const modal = document.getElementById("taskModal");
  const title = document.getElementById("taskModalTitle");
  const body = document.getElementById("taskModalBody");

  if (!modal || !title || !body) return;

  title.textContent = "Task #" + taskNo;
  body.innerHTML = '<div class="muted">Loading task details...</div>';
  modal.classList.add("open");

  try {
    const res = await fetch("/api/reports/task/" + taskNo);
    const json = await res.json();

    if (!json.ok) {
      body.innerHTML =
        '<div class="muted">' + escapeHtmlClient(json.error || "Failed to load task") + '</div>';
      return;
    }

    const task = json.data || {};
    title.textContent = "#" + (task.taskNo || task.id) + " — " + escapeHtmlClient(task.title || "Untitled");

    const historyHtml = (task.history || []).length
      ? task.history.map(function(item) {
          return (
            '<div class="history-item">' +
              '<div class="history-top">' +
                '<strong>' + escapeHtmlClient(item.changeType || "-") + '</strong>' +
                '<span>' + escapeHtmlClient(item.at || "-") + ' • ' + escapeHtmlClient(item.by || "-") + '</span>' +
              '</div>' +
              '<div class="history-detail">' + escapeHtmlClient(renderUserWorkspaceTaskHistoryDetail(item)) + '</div>' +
            '</div>'
          );
        }).join("")
      : '<div class="muted">No recent history</div>';

    body.innerHTML =
      '<div class="modal-meta-grid">' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Owners</div><div>' + escapeHtmlClient(((task.owners || []).join(", ") || "-")) + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Status</div><div>' + escapeHtmlClient(task.status || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Priority</div><div>' + escapeHtmlClient(task.priority || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Progress</div><div>' + escapeHtmlClient(String(task.progress ?? 0)) + '%</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Deadline</div><div>' + escapeHtmlClient(task.deadline || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Business / Area</div><div>' + escapeHtmlClient((task.business || "-") + " / " + (task.area || "-")) + '</div></div>' +
      '</div>' +

      ((task.detail || task.blockerNote) ? (
        '<div class="modal-section">' +
          '<h3>Details</h3>' +
          (task.detail
            ? '<div class="modal-meta-box" style="margin-bottom:10px;"><div class="modal-meta-label">Detail</div><div>' + escapeHtmlClient(task.detail) + '</div></div>'
            : ''
          ) +
          (task.blockerNote
            ? '<div class="modal-meta-box"><div class="modal-meta-label">Blocker</div><div>' + escapeHtmlClient(task.blockerNote) + '</div></div>'
            : ''
          ) +
        '</div>'
      ) : '') +

      '<div class="modal-section">' +
        '<h3>History</h3>' +
        historyHtml +
      '</div>';
  } catch (error) {
    body.innerHTML =
      '<div class="muted">' + escapeHtmlClient(error?.message || "Failed to load task") + '</div>';
  }
}

function closeUserWorkspaceTaskDetail(event) {
  if (event && event.target && event.target.id !== "taskModal") return;
  const modal = document.getElementById("taskModal");
  if (modal) modal.classList.remove("open");
}

setInterval(() => {
  window.location.reload();
}, 60000);

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    window.location.reload();
  }
});

document.addEventListener("keydown", function (event) {
  if (event.key === "Escape") {
    closeUserWorkspaceTaskDetail();
  }
});
