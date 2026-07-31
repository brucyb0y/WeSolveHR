// Client script for GET /client-view/:token, extracted verbatim from the
// inline <script> of renderClientViewOnlyPage() (lib/server/app.js lines 11435-14074).
function openExtLeadNotes(el) {
  var data = el.parentNode.querySelector(".ext-lead-notes-data");
  var body = document.getElementById("extLeadNotesBody");
  var subtitle = document.getElementById("extLeadNotesSubtitle");
  if (!data || !body) return;
  body.innerHTML = data.innerHTML;
  subtitle.textContent = el.getAttribute("data-company") || "";
  document.getElementById("extLeadNotesModal").classList.add("open");
}

function closeExtLeadNotes(event) {
  if (event && event.target && event.target.id !== "extLeadNotesModal") return;
  document.getElementById("extLeadNotesModal").classList.remove("open");
}

document.addEventListener("keydown", function(e) {
  if (e.key === "Escape") closeExtLeadNotes();
});

function showClientViewTab(key, btn) {
  document.querySelectorAll(".tab-panel").forEach(function(panel) {
    panel.classList.remove("active");
  });

  document.querySelectorAll(".client-view-tab").forEach(function(tab) {
    tab.classList.remove("active");
  });

  const panel = document.getElementById("clientViewTab-" + key);
  if (panel) panel.classList.add("active");
  if (btn) btn.classList.add("active");
}

// Report sub-view toggle (Daily / Week 1 / Week 2 / ...), matching the
// internal report. The view arg is "daily" or "week" plus a number;
// the weekly views live behind a "Week" dropdown.
function setReportView(view) {
  var views = document.querySelectorAll(".report-subview");
  if (!views.length) return;
  var targetId = "reportView-" + view;
  if (!document.getElementById(targetId)) return;
  for (var i = 0; i < views.length; i++) {
    views[i].style.display = views[i].id === targetId ? "" : "none";
  }
  var isWeek = /^week[0-9]+$/.test(view);
  var dailyBtn = document.querySelector('.report-subtab[data-view="daily"]');
  if (dailyBtn) dailyBtn.classList.toggle("active", view === "daily");
  var weekBtn = document.querySelector(".report-week-btn");
  if (weekBtn) weekBtn.classList.toggle("active", isWeek);
  var label = document.querySelector(".report-week-label");
  var items = document.querySelectorAll(".report-week-item");
  for (var j = 0; j < items.length; j++) {
    var match = items[j].getAttribute("data-view") === view;
    items[j].classList.toggle("active", match);
    if (match && label)
      label.textContent = items[j].getAttribute("data-label");
  }
  if (!isWeek && label) label.textContent = "Week";
  var menu = document.querySelector(".report-week-menu");
  if (menu) menu.classList.remove("open");
}

function toggleWeekMenu(e) {
  if (e) e.stopPropagation();
  var menu = document.querySelector(".report-week-menu");
  if (menu) menu.classList.toggle("open");
}

document.addEventListener("click", function () {
  var menu = document.querySelector(".report-week-menu");
  if (menu) menu.classList.remove("open");
});
