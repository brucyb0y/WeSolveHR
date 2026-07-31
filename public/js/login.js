// Client script for GET /login, extracted verbatim from the
// inline <script> of renderLoginPage() (lib/server/app.js lines 1230-1533).
(function () {
  const form = document.getElementById("loginForm");
  const overlay = document.getElementById("loginLoadingOverlay");
  const btn = document.getElementById("loginSubmitBtn");

  if (!form) return;

  form.addEventListener("submit", function () {
    if (btn) {
      btn.disabled = true;
      btn.textContent = "Logging in...";
    }
    if (overlay) {
      overlay.classList.add("show");
    }
  });
})();
