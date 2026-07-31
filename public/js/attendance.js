// Client script for GET /attendance, extracted verbatim from the inline
// <script> of its Express handler (lib/server/app.js lines 45224-46101).
          function escapeHtmlClient(value) {
            return String(value ?? '')
              .replace(/&/g, '&amp;')
              .replace(/</g, '&lt;')
              .replace(/>/g, '&gt;')
              .replace(/"/g, '&quot;');
          }

          function statusPill(status) {
            const safe = String(status || 'unknown');
            const cls = 'status-pill status-' + safe;
            return '<span class="' + cls + '">' + escapeHtmlClient(safe) + '</span>';
          }

          function flagPills(flags) {
            if (!flags || !flags.length) return '-';

            return flags.map((flag) => {
              let cls = 'flag-pill flag-info';
              if (
                flag === 'Late not approved' ||
                flag === 'Long shift'
              ) cls = 'flag-pill flag-danger';
              else if (
                flag === 'Long break' ||
                flag === 'Time unsure'
              ) cls = 'flag-pill flag-warn';

              return '<span class="' + cls + '">' + escapeHtmlClient(flag) + '</span>';
            }).join(' ');
          }

          function employeeLink(userId, name) {
            return '<span class="person-link" onclick="openAttendanceDetail(' + Number(userId) + ')">' + escapeHtmlClient(name) + '</span>';
          }

          function openAttendanceDetail(userId) {
            const overlay = document.getElementById('pageLoadingOverlay');
            if (overlay) overlay.classList.add('show');
            setTimeout(() => {
              window.location.href = '/attendance/' + userId;
            }, 80);
          }
          
          function renderInsightLines(items, emptyText = '-') {
  if (!items || !items.length) {
    return '<div class="insight-subtle">' + escapeHtmlClient(emptyText) + '</div>';
  }

  return '<div class="insight-list">' + items.map((item) => {
    return '<div class="insight-line">' + escapeHtmlClient(item) + '</div>';
  }).join('') + '</div>';
}

function renderInsightsGrid(target, cards) {
  if (!target) return;

  target.innerHTML = cards.map((card) => {
    return '<div class="insight-card">' +
      '<div class="insight-card-title">' + escapeHtmlClient(card.title) + '</div>' +
      '<div class="insight-card-main">' + escapeHtmlClient(card.main ?? '-') + '</div>' +
      renderInsightLines(card.lines || [], 'No data yet') +
    '</div>';
  }).join('');
}

          async function loadAttendancePage() {
            const statsGrid = document.getElementById('statsGrid');
            const tableBody = document.getElementById('attendanceTableBody');
            const exceptionsBody = document.getElementById('exceptionsTableBody');
            const summaryBody = document.getElementById('summaryTableBody');
            const attentionNow = document.getElementById('attentionNow');
            const carelessLoginList = document.getElementById('carelessLoginList');
            const liveGroups = document.getElementById('liveGroups');
            const leaveList = document.getElementById('leaveList');
            const noUpdateList = document.getElementById('noUpdateList');
            const weeklyInsightsGrid = document.getElementById('weeklyInsightsGrid');
const monthlyInsightsGrid = document.getElementById('monthlyInsightsGrid');

            try {
const res = await fetch('/api/attendance');
const contentType = res.headers.get('content-type') || '';

if (!contentType.includes('application/json')) {
  const text = await res.text();
  throw new Error('Attendance API returned HTML instead of JSON');
}

const json = await res.json();

              if (!json.ok) {
                throw new Error(json.error || 'Failed to load attendance');
              }

              const data = json.data || {};
              const summary = data.summary || {};
              const groups = data.groups || {};
const rows = data.rows || [];
const carelessRows = rows.filter((row) =>
  row.role !== 'admin' &&
  Array.isArray(row.flags) &&
  row.flags.includes('Long shift')
);

              const cards = [
                ['Logged in now', summary.logged_in_now ?? 0, 'Working currently'],
                ['On break now', summary.on_break_now ?? 0, 'Currently on break'],
                ['Not logged in yet', summary.not_logged_in_yet ?? 0, 'No attendance update'],
                ['On leave today', summary.on_leave_today ?? 0, 'Planned leave'],
                ['Late today', summary.late_today ?? 0, 'All late categories'],
                ['Approved late', summary.approved_late ?? 0, 'Prior info approved'],
                ['Late not approved', summary.unapproved_late ?? 0, 'Needs attention'],
                ['No prior info', summary.no_prior_info_late ?? 0, 'Joined late directly'],
                ['Long breaks', summary.long_break_flags ?? 0, 'Break exception'],
['Careless login', summary.long_shift_flags ?? 0, 'Worked above 10h, likely wrong entry'],              ];

              statsGrid.innerHTML = cards.map((card) => {
                return '<div class="stat-card">' +
                  '<div class="stat-label">' + escapeHtmlClient(card[0]) + '</div>' +
                  '<div class="stat-value">' + escapeHtmlClient(card[1]) + '</div>' +
                  '<div class="stat-note">' + escapeHtmlClient(card[2]) + '</div>' +
                '</div>';
              }).join('');

              const sortedRows = [...rows].sort((a, b) => {
                const aRisk = (a.flags?.length || 0) + (a.late_status === 'Not approved' ? 2 : 0) + (a.late_status === 'No prior info' ? 2 : 0);
                const bRisk = (b.flags?.length || 0) + (b.late_status === 'Not approved' ? 2 : 0) + (b.late_status === 'No prior info' ? 2 : 0);
                return bRisk - aRisk;
              });

tableBody.innerHTML = sortedRows.map((row) => {
  return '<tr>' +
    '<td>' + employeeLink(row.user_id, row.name) + '</td>' +
    '<td>' + escapeHtmlClient(row.role || '-') + '</td>' +
    '<td>' + escapeHtmlClient(row.expected_shift_start_text || '-') + '</td>' +
    '<td>' + statusPill(row.status) + '</td>' +
    '<td>' + escapeHtmlClient(row.since_text || '-') + '</td>' +
    '<td>' + escapeHtmlClient(row.worked_today_text || '-') + '</td>' +
    '<td>' + escapeHtmlClient(row.break_today_text || '-') + '</td>' +
    '<td>' + escapeHtmlClient(row.first_login_text || '-') + '</td>' +
    '<td>' + escapeHtmlClient(row.late_status || '-') + '</td>' +
    '<td>' + (row.is_on_leave ? 'Yes' : 'No') + '</td>' +
    '<td>' + flagPills(row.flags || []) + '</td>' +
  '</tr>';
}).join('') || '<tr><td colspan="11" class="empty-cell">No attendance data found</td></tr>';
              const exceptionRows = rows.filter((row) =>
                (row.flags && row.flags.length) ||
                row.late_status === 'Not approved' ||
                row.late_status === 'No prior info'
              );

              exceptionsBody.innerHTML = exceptionRows.map((row) => {
                return '<tr>' +
                  '<td>' + employeeLink(row.user_id, row.name) + '</td>' +
                  '<td>' + statusPill(row.status) + '</td>' +
                  '<td>' + escapeHtmlClient(row.late_status || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.expected_shift_start_text || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.worked_today_text || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.break_today_text || '-') + '</td>' +
                  '<td>' + flagPills(row.flags || []) + '</td>' +
                '</tr>';
              }).join('') || '<tr><td colspan="7" class="empty-cell">No exceptions today</td></tr>';

              summaryBody.innerHTML = rows.map((row) => {
                return '<tr>' +
                  '<td>' + employeeLink(row.user_id, row.name) + '</td>' +
                  '<td>' + escapeHtmlClient(row.role || '-') + '</td>' +
                  '<td>' + statusPill(row.status) + '</td>' +
                  '<td>' + escapeHtmlClient(row.worked_today_text || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.break_today_text || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.first_login_text || '-') + '</td>' +
                  '<td>' + escapeHtmlClient(row.late_status || '-') + '</td>' +
                  '<td>' + flagPills(row.flags || []) + '</td>' +
                '</tr>';
              }).join('') || '<tr><td colspan="8" class="empty-cell">No summary data</td></tr>';

const attentionItems = [];
if ((summary.unapproved_late ?? 0) > 0) {
  attentionItems.push('Late not approved: ' + summary.unapproved_late);
}
if ((summary.no_prior_info_late ?? 0) > 0) {
  attentionItems.push('Late without prior info: ' + summary.no_prior_info_late);
}
if ((summary.long_break_flags ?? 0) > 0) {
  attentionItems.push('Long break flags: ' + summary.long_break_flags);
}
if ((summary.long_shift_flags ?? 0) > 0) {
  attentionItems.push('Careless login: ' + summary.long_shift_flags);
}
if ((summary.not_logged_in_yet ?? 0) > 0) {
  attentionItems.push('No attendance update yet: ' + summary.not_logged_in_yet);
}

attentionNow.innerHTML = attentionItems.length
  ? attentionItems.map((item) => '<div class="alert-item">' + escapeHtmlClient(item) + '</div>').join('')
  : '<div class="alert-item">No immediate issues right now</div>';
          
if (carelessLoginList) {
  carelessLoginList.innerHTML = carelessRows.length
    ? carelessRows.map((row) => {
        return '<div class="alert-item">' +
          '<strong>' + employeeLink(row.user_id, row.name) + '</strong><br>' +
          'Worked: ' + escapeHtmlClient(row.worked_today_text || '-') +
          '<br><span class="muted">Likely incorrect attendance entry</span>' +
        '</div>';
      }).join('')
    : '<div class="alert-item">No careless login issues today</div>';
}

liveGroups.innerHTML = [
  '<div class="alert-item"><strong>On break now:</strong><br>' +
    ((groups.on_break_now || []).map((x) => employeeLink(x.user_id, x.name)).join('<br>') || 'None') +
  '</div>',

  '<div class="alert-item"><strong>Expected late:</strong><br>' +
    ((groups.expected_late || []).map((x) =>
      employeeLink(x.user_id, x.name) + ' (' + escapeHtmlClient(x.late_expected_login_text || '-') + ')'
    ).join('<br>') || 'None') +
  '</div>',

  '<div class="alert-item"><strong>No update yet:</strong><br>' +
    ((groups.no_update_yet || []).map((x) => employeeLink(x.user_id, x.name)).join('<br>') || 'None') +
  '</div>',

  '<div class="alert-item"><strong>On leave today:</strong><br>' +
    ((groups.on_leave_today || []).map((x) => employeeLink(x.user_id, x.name)).join('<br>') || 'None') +
  '</div>'
].join('');

              leaveList.innerHTML = (groups.on_leave_today || []).length
                ? (groups.on_leave_today || []).map((x) => '<div class="alert-item">' + employeeLink(x.user_id, x.name) + '</div>').join('')
                : '<div class="alert-item">Nobody is on leave today</div>';

noUpdateList.innerHTML = (groups.no_update_yet || []).length
  ? (groups.no_update_yet || []).map((x) => '<div class="alert-item">' + employeeLink(x.user_id, x.name) + '</div>').join('')
  : '<div class="alert-item">Everyone has updated attendance</div>';

            } catch (error) {
              console.error('Attendance page load failed:', error);
              statsGrid.innerHTML = '<div class="stat-card"><div class="stat-label">Error</div><div class="stat-value">!</div><div class="stat-note">' + escapeHtmlClient(error.message || 'Failed to load') + '</div></div>';
              tableBody.innerHTML = '<tr><td colspan="10" class="error-state">Failed to load attendance</td></tr>';
              exceptionsBody.innerHTML = '<tr><td colspan="7" class="error-state">Failed to load attendance</td></tr>';
              summaryBody.innerHTML = '<tr><td colspan="8" class="error-state">Failed to load attendance</td></tr>';
              attentionNow.innerHTML = '<div class="alert-item">Failed to load attendance</div>';
              liveGroups.innerHTML = '<div class="alert-item">Failed to load attendance</div>';
              leaveList.innerHTML = '<div class="alert-item">Failed to load attendance</div>';
              noUpdateList.innerHTML = '<div class="alert-item">Failed to load attendance</div>';
              if (carelessLoginList) {
  carelessLoginList.innerHTML = '<div class="alert-item">Failed to load attendance</div>';
}
            }
          }

async function loadAttendanceInsights() {
  const weeklyInsightsGrid = document.getElementById('weeklyInsightsGrid');
  const monthlyInsightsGrid = document.getElementById('monthlyInsightsGrid');

  if (!weeklyInsightsGrid || !monthlyInsightsGrid) return;

  try {
const res = await fetch('/api/attendance/insights');
const contentType = res.headers.get('content-type') || '';

if (!contentType.includes('application/json')) {
  const text = await res.text();
  throw new Error('Attendance insights API returned HTML instead of JSON');
}

const json = await res.json();

    if (!json.ok) {
      throw new Error(json.error || 'Failed to load attendance insights');
    }

    const data = json.data || {};
    const weekly = data.weekly || {};
    const monthly = data.monthly || {};

const weeklyCards = [
  {
    title: 'Most late this week',
    main: weekly.most_late_count_text ?? '-',
    lines: weekly.most_late_lines || [],
  },
  {
    title: 'Best attendance streak',
    main: weekly.best_streak_text ?? '-',
    lines: weekly.best_streak_lines || [],
  },
  {
    title: 'Most break time this week',
    main: weekly.most_break_time_text ?? '-',
    lines: weekly.most_break_time_lines || [],
  },
  {
    title: 'Careless login this week',
    main: weekly.careless_login_text ?? '-',
    lines: weekly.careless_login_lines || [],
  },
];

const monthlyCards = [
  {
    title: 'Attendance leaders',
    main: monthly.attendance_leaders_text ?? '-',
    lines: monthly.attendance_leader_lines || [],
  },
  {
    title: 'Needs attention',
    main: monthly.needs_attention_text ?? '-',
    lines: monthly.needs_attention_lines || [],
  },
  {
    title: 'Most late this month',
    main: monthly.most_late_text ?? '-',
    lines: monthly.most_late_lines || [],
  },
  {
    title: 'Most leave this month',
    main: monthly.most_leave_text ?? '-',
    lines: monthly.most_leave_lines || [],
  },
  {
    title: 'Careless login this month',
    main: monthly.careless_login_text ?? '-',
    lines: monthly.careless_login_lines || [],
  },
];

    renderInsightsGrid(weeklyInsightsGrid, weeklyCards);
    renderInsightsGrid(monthlyInsightsGrid, monthlyCards);
  } catch (error) {
    console.error('Attendance insights load failed:', error);

    weeklyInsightsGrid.innerHTML =
      '<div class="insight-card">' +
        '<div class="insight-card-title">This week</div>' +
        '<div class="insight-card-main">Failed</div>' +
        '<div class="insight-subtle">' + escapeHtmlClient(error.message || 'Failed to load') + '</div>' +
      '</div>';

    monthlyInsightsGrid.innerHTML =
      '<div class="insight-card">' +
        '<div class="insight-card-title">This month</div>' +
        '<div class="insight-card-main">Failed</div>' +
        '<div class="insight-subtle">' + escapeHtmlClient(error.message || 'Failed to load') + '</div>' +
      '</div>';
  }
}

          const tabButtons = document.querySelectorAll('.tab-btn');
          const tabPanels = document.querySelectorAll('.tab-panel');

          tabButtons.forEach((btn) => {
            btn.addEventListener('click', () => {
              const tab = btn.dataset.tab;
              tabButtons.forEach((b) => b.classList.remove('active'));
              tabPanels.forEach((p) => p.classList.remove('active'));
              btn.classList.add('active');
              const panel = document.getElementById('tab-' + tab);
              if (panel) panel.classList.add('active');
            });
          });

loadAttendancePage();
loadAttendanceInsights();

setInterval(() => {
  loadAttendancePage().catch((error) => {
    console.error('Periodic attendance load failed:', error);
  });
}, 60000);

setInterval(() => {
  loadAttendanceInsights().catch((error) => {
    console.error('Periodic attendance insights load failed:', error);
  });
}, 5 * 60000);

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    loadAttendancePage().catch((error) => {
      console.error('Visibility attendance load failed:', error);
    });

    loadAttendanceInsights().catch((error) => {
      console.error('Visibility attendance insights load failed:', error);
    });
  }
});
