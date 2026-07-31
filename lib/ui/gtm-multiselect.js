// The GTM associates multi-select field. It carries its own scoped <style>
// and <script> (guarded so it initialises once) because it is embedded in
// forms rather than owning a page. Extracted verbatim from the monolith.

import { escapeHtml } from "./html.js";

const GTM_MULTISELECT_CSS = `
  .gtm-ms { position: relative; width: 100%; }
  .gtm-ms-control {
    width: 100%; padding: 12px 13px; border-radius: 12px;
    border: 1px solid var(--line); background: rgba(255,255,255,0.04);
    color: var(--text); font: inherit; cursor: pointer;
    display: flex; align-items: center; justify-content: space-between; gap: 8px;
  }
  .gtm-ms-text { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .gtm-ms-placeholder { color: var(--muted); }
  .gtm-ms-caret { flex: 0 0 auto; opacity: .7; font-size: 12px; transition: transform .15s ease; }
  .gtm-ms.open .gtm-ms-caret { transform: rotate(180deg); }
  .gtm-ms-panel {
    position: absolute; top: calc(100% + 6px); left: 0; right: 0; z-index: 60;
    max-height: 240px; overflow-y: auto; padding: 6px; border-radius: 12px;
    border: 1px solid var(--line); background: var(--panel-strong, #11162a);
    box-shadow: 0 12px 30px rgba(0,0,0,0.45); display: none;
  }
  .gtm-ms.open .gtm-ms-panel { display: block; }
  .gtm-ms-option {
    display: flex; align-items: center; gap: 10px; padding: 8px 9px;
    border-radius: 9px; cursor: pointer; font-weight: 600;
  }
  .gtm-ms-option:hover { background: rgba(255,255,255,0.06); }
  .gtm-ms-option input[type=checkbox] {
    width: auto; min-width: 0; margin: 0; padding: 0; flex: 0 0 auto;
    accent-color: var(--primary, #6d5efc);
  }
`;

const GTM_MULTISELECT_JS = `
  (function () {
    if (window.__gtmMsReady) return;
    window.__gtmMsReady = true;
    window.gtmToggle = function (el) {
      var ms = el.closest('.gtm-ms'); if (!ms) return;
      var willOpen = !ms.classList.contains('open');
      document.querySelectorAll('.gtm-ms.open').forEach(function (o) { if (o !== ms) o.classList.remove('open'); });
      ms.classList.toggle('open', willOpen);
    };
    function gtmUpdate(ms) {
      var names = [];
      ms.querySelectorAll('input[type=checkbox]').forEach(function (c) { if (c.checked) names.push(c.getAttribute('data-name') || ''); });
      var txt = ms.querySelector('.gtm-ms-text'); if (!txt) return;
      if (names.length === 0) { txt.textContent = 'Select GTM associates'; txt.classList.add('gtm-ms-placeholder'); }
      else { txt.textContent = names.join(', '); txt.classList.remove('gtm-ms-placeholder'); }
    }
    document.addEventListener('change', function (e) {
      if (e.target && e.target.matches && e.target.matches('.gtm-ms input[type=checkbox]')) {
        var ms = e.target.closest('.gtm-ms'); if (ms) gtmUpdate(ms);
      }
    });
    document.addEventListener('click', function (e) {
      if (!e.target.closest || !e.target.closest('.gtm-ms')) {
        document.querySelectorAll('.gtm-ms.open').forEach(function (o) { o.classList.remove('open'); });
      }
    });
    function gtmInitAll() { document.querySelectorAll('.gtm-ms').forEach(gtmUpdate); }
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', gtmInitAll);
    else gtmInitAll();
  })();
`;

function renderGtmMultiselectField(users, selectedIds) {
  const selected = (Array.isArray(selectedIds) ? selectedIds : []).map(String);
  const options = (users || [])
    .map((u) => {
      const name = escapeHtml(u.name || "");
      const isSel = selected.includes(String(u.id));
      return `<label class="gtm-ms-option"><input type="checkbox" name="gtm_associate_user_ids" value="${u.id}" data-name="${name}" ${isSel ? "checked" : ""} /><span>${name}</span></label>`;
    })
    .join("");
  return `
    <div class="field">
      <label>GTM Associate</label>
      <div class="gtm-ms">
        <div class="gtm-ms-control" onclick="gtmToggle(this)">
          <span class="gtm-ms-text gtm-ms-placeholder">Select GTM associates</span>
          <span class="gtm-ms-caret">▾</span>
        </div>
        <div class="gtm-ms-panel">${options}</div>
      </div>
    </div>
    <style>${GTM_MULTISELECT_CSS}</style>
    <script>${GTM_MULTISELECT_JS}</script>`;
}

export {
  renderGtmMultiselectField,
};
