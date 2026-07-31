// Client script for GET /account, extracted verbatim from the inline
// <script> of its Express handler (lib/server/app.js lines 35384-36100).
(function () {
  var selects = document.querySelectorAll("select[data-field]");
  selects.forEach(function (select) {
    var field = select.getAttribute("data-field");
    var status = document.getElementById(field + "-status");

    select.addEventListener("change", async function () {
      var value = select.value;
      if (status) {
        status.className = "save-status";
        status.textContent = "Saving…";
      }
      select.disabled = true;

      try {
        var res = await fetch("/api/account/profile-field", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ field: field, value: value }),
        });
        var json = await res.json();
        if (!res.ok || !json.ok) {
          throw new Error(json.error || "Failed to save");
        }
        if (status) {
          status.className = "save-status ok";
          status.textContent = "Saved";
        }
      } catch (err) {
        if (status) {
          status.className = "save-status err";
          status.textContent = err.message || "Failed to save";
        }
      } finally {
        select.disabled = false;
      }
    });
  });
})();
