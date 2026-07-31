// Client script for GET /tasks, extracted verbatim from the inline
// <script> of its Express handler (lib/server/app.js lines 44191-45188).
        
        function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function formatTime(ts) {
  try {
    const d = new Date(ts);
    return d.toLocaleString('en-IN', {
      timeZone: 'Asia/Kolkata',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    }) + ' IST';
  } catch {
    return ts;
  }
}

function formatJsonValue(value) {
  if (value == null) return '-';
  if (typeof value === 'object') {
    try {
      return escapeHtml(JSON.stringify(value));
    } catch {
      return '-';
    }
  }
  return escapeHtml(String(value));
}

function closeTaskModal() {
  const modal = document.getElementById("taskModal");
  if (modal) {
    modal.classList.remove("open");
  }
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

    return "Status: " + oldStatus + " → " + newStatus +
      "\nProgress: " + oldProgress + "% → " + newProgress + "%" +
      note;
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
      body.innerHTML =
        '<div class="muted">' + escapeHtml(json.error || "Failed to load task") + '</div>';
      return;
    }

    const task = json.data || {};
    title.textContent = "#" + (task.taskNo || task.id) + " — " + escapeHtml(task.title || "Untitled");

    const historyHtml = (task.history || []).length
      ? task.history.map(function(item) {
          return (
            '<div class="history-item">' +
              '<div class="history-top">' +
                '<strong>' + escapeHtml(item.changeType || "-") + '</strong>' +
                '<span>' + escapeHtml(item.at || "-") + ' • ' + escapeHtml(item.by || "-") + '</span>' +
              '</div>' +
              '<div class="history-detail">' + escapeHtml(renderHistoryDetail(item)) + '</div>' +
            '</div>'
          );
        }).join("")
      : '<div class="muted">No recent history</div>';

    body.innerHTML =
      '<div class="modal-meta-grid">' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Owners</div><div>' + escapeHtml(((task.owners || []).join(", ") || "-")) + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Status</div><div>' + escapeHtml(task.status || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Priority</div><div>' + escapeHtml(task.priority || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Progress</div><div>' + escapeHtml(String(task.progress ?? 0)) + '%</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Deadline</div><div>' + escapeHtml(task.deadline || "-") + '</div></div>' +
        '<div class="modal-meta-box"><div class="modal-meta-label">Business / Area</div><div>' + escapeHtml((task.business || "-") + " / " + (task.area || "-")) + '</div></div>' +
      '</div>' +

      ((task.detail || task.blockerNote) ? (
        '<div class="modal-section">' +
          '<h3>Details</h3>' +
          (task.detail
            ? '<div class="modal-meta-box" style="margin-bottom:10px;"><div class="modal-meta-label">Detail</div><div>' + escapeHtml(task.detail) + '</div></div>'
            : ''
          ) +
          (task.blockerNote
            ? '<div class="modal-meta-box"><div class="modal-meta-label">Blocker</div><div>' + escapeHtml(task.blockerNote) + '</div></div>'
            : ''
          ) +
        '</div>'
      ) : '') +

      '<div class="modal-section">' +
        '<h3>History</h3>' +
        '<div class="history-list">' + historyHtml + '</div>' +
      '</div>';
  } catch (error) {
    console.error("openTaskDetail error:", error);
    body.innerHTML = '<div class="muted">Could not load task detail</div>';
  }
}

document.addEventListener("keydown", function(event) {
  if (event.key === "Escape") {
    closeTaskModal();
  }
});

function renderTaskOwnerLinks(task) {
  const owners = Array.isArray(task.owners) ? task.owners : [];

  if (!owners.length) {
    return '<span class="muted">Unassigned</span>';
  }

  return owners
    .map(function (owner) {
      const id = owner.user_id || owner.id;
      const name = escapeHtml(owner.name || "Unknown");

      return (
        '<a class="owner-chip-link" ' +
        'href="/tasks/user/' + id + '" ' +
        'onclick="event.stopPropagation()">' +
        name +
        '</a>'
      );
    })
    .join(' ');
}

async function loadUsers() {
  try {
    console.log('loadUsers start');

    const select = document.getElementById('assignee');
    if (!select) {
      console.error('loadUsers: assignee select not found');
      return;
    }

    const res = await fetch('/api/users');
    console.log('loadUsers fetch status:', res.status, res.statusText);

    const json = await res.json();
    console.log('loadUsers response:', json);

    if (!json.ok) {
      console.error('loadUsers api error:', json);
      select.innerHTML = '<option value="">All assignee</option>';
      return;
    }

    select.innerHTML = '<option value="">All assignee</option>';

    for (const user of (json.data || [])) {
      const opt = document.createElement('option');
      opt.value = String(user.id);
      opt.textContent = user.name;
      select.appendChild(opt);
    }

    console.log('loadUsers done. option count:', select.options.length);
  } catch (error) {
    console.error('loadUsers fatal error:', error);
    const select = document.getElementById('assignee');
    if (select) {
      select.innerHTML = '<option value="">All assignee</option>';
    }
  }
}

function applyFiltersFromUrl() {
  const params = new URLSearchParams(window.location.search);

  const search = params.get('search') || '';
  const assignee = params.get('assignee') || '';
  const waitingOn = params.get('waitingOn') || '';
  const business = params.get('business') || '';
  const area = params.get('area') || '';
  const status = params.get('status') || '';
  const priority = params.get('priority') || '';
  const blocked = params.get('blocked') === 'true';
  const overdue = params.get('overdue') === 'true';
  const progressBuckets = params.getAll('progressBucket');

  const searchEl = document.getElementById('search');
  const assigneeEl = document.getElementById('assignee');
  const businessEl = document.getElementById('business');
  const areaEl = document.getElementById('area');
  const statusEl = document.getElementById('status');
  const priorityEl = document.getElementById('priority');
  const blockedEl = document.getElementById('blocked');
  const overdueEl = document.getElementById('overdue');
  const progressBucketEl = document.getElementById('progressBucket');

  if (searchEl) searchEl.value = search;
  if (assigneeEl) assigneeEl.value = assignee;
  if (businessEl) businessEl.value = business;
  if (areaEl) areaEl.value = area;
  if (statusEl) statusEl.value = status;
  if (priorityEl) priorityEl.value = priority;
  if (blockedEl) blockedEl.checked = blocked;
  if (overdueEl) overdueEl.checked = overdue;

  if (progressBucketEl) {
    if (progressBuckets.length) {
      for (const opt of progressBucketEl.options) {
        opt.selected = progressBuckets.includes(opt.value);
      }
    } else if (blocked) {
      const blockedDefaults = new Set([
        'not_begun',
        'zero_to_fifty',
        'fifty_to_hundred',
        'complete',
        'hide_cancelled',
      ]);

      for (const opt of progressBucketEl.options) {
        opt.selected = blockedDefaults.has(opt.value);
      }
    }
  }

  window.__waitingOn = waitingOn;
}

function renderSpecialFilterState() {
  const box = document.getElementById('activeSpecialFilters');
  if (!box) return;

  if (!window.__waitingOn) {
    box.innerHTML = '';
    return;
  }

  const assigneeSelect = document.getElementById('assignee');
  let waitingOnName = 'Selected user';

  if (assigneeSelect) {
    const opt = Array.from(assigneeSelect.options).find(
      o => String(o.value) === String(window.__waitingOn)
    );
    if (opt && opt.textContent) waitingOnName = opt.textContent;
  }

  box.innerHTML =
    'Filtered: waiting on <strong>' + waitingOnName + '</strong> ' +
    '<button type="button" onclick="clearWaitingOnFilter()" style="margin-left:8px;">Clear</button>';
}

function clearWaitingOnFilter() {
  window.__waitingOn = '';

  const url = new URL(window.location.href);
  url.searchParams.delete('waitingOn');
  window.history.replaceState({}, '', url.toString());

  loadTasks();
}

          async function loadTasks() {
            const params = new URLSearchParams();
            renderSpecialFilterState();
const search = document.getElementById('search').value.trim();
const assignee = document.getElementById('assignee').value;
const business = document.getElementById('business').value;
const area = document.getElementById('area').value;
const status = document.getElementById('status').value;

const priority = document.getElementById('priority').value;
const blocked = document.getElementById('blocked').checked;
const overdue = document.getElementById('overdue').checked;
const waitingOn = window.__waitingOn || '';
if (waitingOn) params.set('waitingOn', waitingOn);

const progressBucket = Array.from(
  document.getElementById('progressBucket').selectedOptions
).map(opt => opt.value);

if (search) params.set('search', search);
if (assignee) params.set('assignee', assignee);
if (business) params.set('business', business);
if (area) params.set('area', area);
if (status) params.set('status', status);
if (priority) params.set('priority', priority);
if (blocked) params.set('blocked', 'true');
if (overdue) params.set('overdue', 'true');

for (const bucket of progressBucket) {
  params.append('progressBucket', bucket);
}

            document.getElementById('statusText').textContent = 'Loading tasks...';

            const res = await fetch('/api/tasks?' + params.toString());
            const json = await res.json();

if (!json.ok) {
  document.getElementById('statusText').textContent =
    'Could not load tasks: ' + (json.error || 'unknown error');
  document.getElementById('taskRows').innerHTML = '';
  console.error('loadTasks api error:', json);
  return;
}


            const rows = json.data || [];
console.log('tasks rows:', rows);
document.getElementById('statusText').textContent =
  rows.length === 0
    ? 'No tasks found'
    : (rows.length + ' task' + (rows.length === 1 ? '' : 's') + ' shown');

document.getElementById('taskRows').innerHTML = rows.map(function(task) {
  const status = String(task.status || '').toLowerCase();

  const isBlocked = status === 'blocked';

  const isOverdue =
    !!task.deadline &&
    status !== 'done' &&
    status !== 'cancelled' &&
    new Date(task.deadline + 'T23:59:59') < new Date();

  const rowClasses = [
    isBlocked ? 'task-row-blocked' : '',
    isOverdue ? 'task-row-overdue' : ''
  ].filter(Boolean).join(' ');

  return (
    '<tr class="' + rowClasses + '" onclick="openTaskDetail(' + (task.task_no || task.id) + ')">' +
      '<td>' +
  '<span class="task-link" onclick="event.stopPropagation(); openTaskDetail(' + (task.task_no || task.id) + ')">' +
    '#' + (task.task_no || task.id) +
  '</span>' +
'</td>' +
      '<td>' + escapeHtml(task.title || '') + '</td>' +
      '<td>' + escapeHtml(task.business || '-') + '</td>' +
'<td>' + renderTaskOwnerLinks(task) + '</td>' +
      '<td>' + escapeHtml(task.status || '') + '</td>' +
      '<td>' + escapeHtml(task.priority || '') + '</td>' +
      '<td>' + escapeHtml(task.deadline || '-') + '</td>' +
      '<td>' + escapeHtml(task.blocker_note || '-') + '</td>' +
    '</tr>'
  );
}).join('');
          }
          
          function clearHiddenWaitingOn() {
  window.__waitingOn = '';
}

document.getElementById('assignee')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('business')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('area')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('status')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('priority')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('blocked')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('overdue')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('progressBucket')?.addEventListener('change', clearHiddenWaitingOn);
document.getElementById('search')?.addEventListener('input', clearHiddenWaitingOn);


loadUsers()
  .then(() => {
    applyFiltersFromUrl();
    return loadTasks();
  })
  .catch((error) => {
    console.error('Tasks page init failed:', error);
    const status = document.getElementById('statusText');
    if (status) {
      status.textContent = 'Failed to initialize tasks page';
    }
  });

setInterval(() => {
  loadTasks().catch((error) => {
    console.error('Periodic loadTasks failed:', error);
  });
}, 60000);

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    loadTasks().catch((error) => {
      console.error('Visibility loadTasks failed:', error);
    });
  }
});
