// Client script for GET /clients, extracted verbatim from the
// inline <script> of renderClientsListPage() (lib/server/app.js lines 3397-3734).
function toggleClientActionsMenu(event, clientId) {
  event.stopPropagation();

  document.querySelectorAll(".floating-actions-menu.open").forEach(function(menu) {
    if (menu.id !== "clientActionsMenu-" + clientId) {
      menu.classList.remove("open");
    }
  });

  const menu = document.getElementById("clientActionsMenu-" + clientId);
  if (!menu) return;

  const rect = event.currentTarget.getBoundingClientRect();

  menu.style.top = rect.bottom + 6 + "px";
  menu.style.left = Math.max(12, rect.right - 180) + "px";
  menu.classList.toggle("open");
}

document.addEventListener("click", function() {
  document.querySelectorAll(".floating-actions-menu.open").forEach(function(menu) {
    menu.classList.remove("open");
  });
});

document.addEventListener("keydown", function(event) {
  if (event.key === "Escape") {
    document.querySelectorAll(".floating-actions-menu.open").forEach(function(menu) {
      menu.classList.remove("open");
    });
  }
});
