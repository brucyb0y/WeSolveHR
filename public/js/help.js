// Client script for GET /help, extracted verbatim from the
// inline <script> of renderHelpPage() (lib/server/app.js lines 36134-36779).
(function () {
  // Click-to-copy
  document.querySelectorAll(".help-cmd").forEach(function (btn) {
    btn.addEventListener("click", function () {
      var text = btn.getAttribute("data-cmd") || btn.textContent;
      var done = function () {
        var original = btn.textContent;
        btn.classList.add("copied");
        btn.textContent = "Copied!";
        setTimeout(function () {
          btn.textContent = original;
          btn.classList.remove("copied");
        }, 900);
      };
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(done, done);
      } else {
        done();
      }
    });
  });

  // Live search filter
  var search = document.getElementById("helpSearch");
  var empty = document.getElementById("helpEmpty");
  search.addEventListener("input", function () {
    var term = search.value.trim().toLowerCase();
    var anySection = false;
    document.querySelectorAll(".help-section").forEach(function (sec) {
      var titleMatch = (sec.getAttribute("data-title") || "").toLowerCase().indexOf(term) !== -1;
      var anyRow = false;
      sec.querySelectorAll(".help-cmd-row").forEach(function (row) {
        var match = !term || titleMatch || row.textContent.toLowerCase().indexOf(term) !== -1;
        row.style.display = match ? "" : "none";
        if (match) anyRow = true;
      });
      // Hide sub-labels / notes whose group has no visible rows is overkill;
      // just show/hide the whole section.
      var show = !term || titleMatch || anyRow;
      sec.style.display = show ? "" : "none";
      if (show) anySection = true;
    });
    empty.style.display = anySection ? "none" : "block";
  });
})();
