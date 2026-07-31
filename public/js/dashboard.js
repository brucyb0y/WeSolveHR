// Client script for GET /dashboard, extracted verbatim from the
// inline <script> of renderDashboardPage() (lib/server/app.js lines 33026-33717).
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

function goToTaskFilter(userId, type, clickedEl) {
  const params = new URLSearchParams();

  if (type !== 'blocked_on_them') {
    params.set('assignee', String(userId));
  }

  if (type === 'blocked') {
    params.set('blocked', 'true');
    params.append('progressBucket', 'not_begun');
    params.append('progressBucket', 'zero_to_fifty');
    params.append('progressBucket', 'fifty_to_hundred');
    params.append('progressBucket', 'complete');
    params.append('progressBucket', 'hide_cancelled');
  }

  if (type === 'overdue') {
    params.set('overdue', 'true');
  }

  if (type === 'not_started') {
    params.append('progressBucket', 'not_begun');
    params.append('progressBucket', 'hide_cancelled');
  }

  if (type === 'open' || type === 'all') {
    params.append('progressBucket', 'not_begun');
    params.append('progressBucket', 'zero_to_fifty');
    params.append('progressBucket', 'fifty_to_hundred');
    params.append('progressBucket', 'hide_cancelled');
  }

  if (type === 'blocked_on_them') {
    params.set('waitingOn', String(userId));
    params.set('blocked', 'true');
  }

  const overlay = document.getElementById('pageLoadingOverlay');
  const title = document.getElementById('pageLoadingTitle');

  if (title) {
    if (type === 'blocked_on_them') {
      title.textContent = 'Opening blocked tasks waiting on this person...';
    } else if (type === 'blocked') {
      title.textContent = 'Opening blocked tasks...';
    } else if (type === 'overdue') {
      title.textContent = 'Opening overdue tasks...';
    } else if (type === 'not_started') {
      title.textContent = 'Opening not started tasks...';
    } else {
      title.textContent = 'Opening task list...';
    }
  }

  if (overlay) overlay.classList.add('show');

  if (clickedEl) {
    clickedEl.style.opacity = '0.65';
    clickedEl.style.pointerEvents = 'none';
  }

  window.location.href = '/tasks?' + params.toString();
}
