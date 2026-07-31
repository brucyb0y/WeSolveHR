// Client script for GET /reports?userId= (per-user view), extracted verbatim from the
// inline <script> of renderMultiDayUserReportsPage() (lib/server/app.js lines 30638-31120).
          function closeTaskModal(event) {
            if (event && event.target && event.target.id !== "taskModal") return;
            document.getElementById("taskModal").classList.remove("open");
          }

function renderHistoryDetail(item) {
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};

  if (item.changeType === "task_created") {
    return "Task created";
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
    const oldProgress = oldValue.progress ?? 0;
    const newProgress = newValue.progress ?? 0;
    const note = newValue.note ? "\nNote: " + newValue.note : "";
    return "Progress: " + oldProgress + "% → " + newProgress + "%" + note;
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

  if (item.fieldName) {
    return (item.fieldName || "Field") + ": " +
      JSON.stringify(oldValue) + " → " + JSON.stringify(newValue);
  }

  return JSON.stringify(newValue || {});
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
                ? task.history.map((item) => {
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
              body.innerHTML = '<div class="muted">Failed to load task details</div>';
            }
          }
