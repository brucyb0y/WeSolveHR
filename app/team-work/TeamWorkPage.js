// Markup for GET /team-work.
//
// Body markup extracted verbatim from the Express handler (lib/server/app.js
// lines 46238-47058), which built the whole document inline.
// The document shell now comes from app/layout.jsx, the <style> block from
// ./team-work.css, and any static <script> from public/js/.

import { escapeHtml } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderTeamWorkPage({ initialDate, today, bootstrap }) {
  return `        ${renderTopNav("team-work")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Team Division</div>
              <h1>Team Work <span class="save-flash" id="saveFlash">Saved</span></h1>
              <div class="subtitle">Hours each person spent per project — updated manually, per day.</div>
            </div>
            <div class="date-controls">
              <button class="btn" id="prevDay" title="Previous day">‹</button>
              <input type="date" id="datePicker" value="${escapeHtml(initialDate)}" />
              <button class="btn" id="nextDay" title="Next day">›</button>
              <button class="btn" id="todayBtn">Today</button>
            </div>
          </div>

          <div id="missingBanner" class="banner" style="display:none;">
            The Team Work tables haven't been created yet. Run
            <strong>sql/2026-06-30-team-work.sql</strong> against this database to enable the page.
          </div>

          <div class="summary" id="summary"></div>

          <div class="layout">
            <div>
              <div class="panel">
                <div class="panel-head">
                  <h2>Daily breakdown</h2>
                  <div style="display:flex; gap:8px;">
                    <button class="btn" id="addColBtn">+ Column</button>
                    <button class="btn primary" id="addMemberBtn">+ Person</button>
                  </div>
                </div>
                <div class="table-wrap">
                  <table class="grid" id="grid"></table>
                </div>
              </div>
            </div>

            <div class="panel">
              <h2>Logs</h2>
              <div class="logs-list" id="logs"></div>
            </div>
          </div>
        </div>

        <!-- Hover-card shown when hovering an employee name -->
        <div class="namepop" id="namePop"></div>

        <!-- Add person modal -->
        <div class="modal-overlay" id="memberModal">
          <div class="modal">
            <h3>Add person</h3>
            <div class="field">
              <label>Name</label>
              <input type="text" id="memberName" placeholder="e.g. Mehnoor" />
            </div>
            <div class="field">
              <label>Team</label>
              <select id="memberTeam">
                <option value="LEADS">LEADS</option>
                <option value="GTM">GTM</option>
              </select>
            </div>
            <div class="field">
              <label>Responsibility (optional)</label>
              <input type="text" id="memberResp" placeholder="e.g. split across Navii &amp; Rasset" />
            </div>
            <div class="modal-actions">
              <button class="btn" data-close-modal>Cancel</button>
              <button class="btn primary" id="memberSave">Add person</button>
            </div>
          </div>
        </div>

        <!-- Add column modal -->
        <div class="modal-overlay" id="colModal">
          <div class="modal">
            <h3>Add column</h3>
            <div class="field">
              <label>Column label</label>
              <input type="text" id="colLabel" placeholder="e.g. WS04" />
            </div>
            <div class="modal-actions">
              <button class="btn" data-close-modal>Cancel</button>
              <button class="btn primary" id="colSave">Add column</button>
            </div>
          </div>
        </div>

        <script>
          var STATE = ${bootstrap};

          function esc(v) {
            return String(v == null ? "" : v)
              .replace(/&/g, "&amp;").replace(/</g, "&lt;")
              .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
          }
          function fmtHours(n) {
            n = Number(n) || 0;
            return n % 1 === 0 ? String(n) : String(n);
          }
          function keyOf(memberId, colId) { return memberId + ":" + colId; }
          function getHours(memberId, colId) {
            var v = STATE.hours[keyOf(memberId, colId)];
            return v == null ? 0 : Number(v);
          }

          function flashSaved() {
            var f = document.getElementById("saveFlash");
            f.classList.add("show");
            setTimeout(function () { f.classList.remove("show"); }, 900);
          }

          function rowTotal(memberId) {
            var t = 0;
            STATE.columns.forEach(function (c) { t += getHours(memberId, c.id); });
            return t;
          }
          function colTotal(colId) {
            var t = 0;
            STATE.members.forEach(function (m) { t += getHours(m.id, colId); });
            return t;
          }

          function renderSummary() {
            var wrap = document.getElementById("summary");
            var grand = 0;
            STATE.members.forEach(function (m) { grand += rowTotal(m.id); });
            var html =
              '<div class="summary-card panel">' +
                '<div class="s-label">Total hours</div>' +
                '<div class="s-value">' + fmtHours(grand) + '</div>' +
                '<div class="s-note">' + STATE.members.length + ' people · ' + STATE.columns.length + ' projects</div>' +
              '</div>';
            STATE.columns.forEach(function (c) {
              var total = colTotal(c.id);
              var people = STATE.members.filter(function (m) { return getHours(m.id, c.id) > 0; }).length;
              html +=
                '<div class="summary-card panel">' +
                  '<div class="s-label">' + esc(c.label) + '</div>' +
                  '<div class="s-value">' + fmtHours(total) + '</div>' +
                  '<div class="s-note">' + people + (people === 1 ? ' person' : ' people') + '</div>' +
                '</div>';
            });
            wrap.innerHTML = html;
          }

          function memberRow(m) {
            var cells = '';
            STATE.columns.forEach(function (c) {
              var h = getHours(m.id, c.id);
              cells +=
                '<td class="' + (h > 0 ? 'has-hours' : '') + '">' +
                  '<input class="hr" type="text" inputmode="decimal" ' +
                    'value="' + (h > 0 ? fmtHours(h) : '') + '" ' +
                    'data-member="' + m.id + '" data-col="' + c.id + '" ' +
                    'placeholder="0" />' +
                '</td>';
            });
            var resp = m.responsibility
              ? '<div class="resp">' + esc(m.responsibility) + '</div>' : '';
            return (
              '<tr data-member-row="' + m.id + '">' +
                '<td class="name-cell">' +
                  '<div class="nm" data-pop-name="' + m.id + '">' +
                    '<button class="row-del" title="Remove person" data-del-member="' + m.id + '">×</button>' +
                    '<span class="nm-text">' + esc(m.name) + '</span>' +
                  '</div>' + resp +
                '</td>' +
                cells +
                '<td class="total-cell" data-row-total="' + m.id + '">' + fmtHours(rowTotal(m.id)) + '</td>' +
              '</tr>'
            );
          }

          function renderGrid() {
            var table = document.getElementById("grid");
            var head = '<thead><tr><th style="text-align:left; padding-left:12px;">Member</th>';
            STATE.columns.forEach(function (c) {
              head +=
                '<th>' + esc(c.label) +
                  ' <button class="row-del" style="opacity:0.4;" title="Remove column" data-del-col="' + c.id + '">×</button>' +
                '</th>';
            });
            head += '<th>Total</th></tr></thead>';

            var body = '<tbody>';
            ["LEADS", "GTM"].forEach(function (team) {
              var rows = STATE.members.filter(function (m) { return m.team === team; });
              body += '<tr class="team-row"><td colspan="' + (STATE.columns.length + 2) + '">' + team + '</td></tr>';
              if (!rows.length) {
                body += '<tr><td class="empty" colspan="' + (STATE.columns.length + 2) + '">No one in ' + team + ' yet</td></tr>';
              }
              rows.forEach(function (m) { body += memberRow(m); });
            });
            body += '</tbody>';

            var foot = '<tfoot><tr><td style="text-align:left; padding-left:12px;">Total</td>';
            var grand = 0;
            STATE.columns.forEach(function (c) {
              var t = colTotal(c.id);
              grand += t;
              foot += '<td data-col-total="' + c.id + '">' + fmtHours(t) + '</td>';
            });
            foot += '<td class="grand" data-grand-total>' + fmtHours(grand) + '</td></tr></tfoot>';

            table.innerHTML = head + body + foot;
            wireGridInputs();
          }

          function recomputeTotals() {
            STATE.members.forEach(function (m) {
              var el = document.querySelector('[data-row-total="' + m.id + '"]');
              if (el) el.textContent = fmtHours(rowTotal(m.id));
            });
            var grand = 0;
            STATE.columns.forEach(function (c) {
              var t = colTotal(c.id);
              grand += t;
              var el = document.querySelector('[data-col-total="' + c.id + '"]');
              if (el) el.textContent = fmtHours(t);
            });
            var gEl = document.querySelector('[data-grand-total]');
            if (gEl) gEl.textContent = fmtHours(grand);
            var popTotal = document.querySelector('[data-pop-total]');
            if (popTotal) {
              popTotal.textContent = fmtHours(rowTotal(Number(popTotal.getAttribute('data-pop-total'))));
            }
            renderSummary();
          }

          // Mirror a saved hours value across BOTH the grid cell and the hover-card
          // input for the same person/project, so they never drift apart.
          function syncHourInputs(memberId, colId, val) {
            STATE.hours[keyOf(memberId, colId)] = val;
            document
              .querySelectorAll('input.hr[data-member="' + memberId + '"][data-col="' + colId + '"]')
              .forEach(function (i) {
                i.value = val > 0 ? fmtHours(val) : '';
                var td = i.closest('td');
                if (td) td.classList.toggle('has-hours', val > 0);
              });
            recomputeTotals();
          }

          function wireGridInputs() {
            document.querySelectorAll('input.hr').forEach(function (inp) {
              inp.addEventListener('focus', function () { inp.dataset.orig = inp.value; });
              inp.addEventListener('keydown', function (e) {
                if (e.key === 'Enter') { inp.blur(); }
              });
              inp.addEventListener('blur', function () { commitCell(inp); });
            });
            document.querySelectorAll('[data-del-member]').forEach(function (b) {
              b.addEventListener('click', function () { deleteMember(b.getAttribute('data-del-member')); });
            });
            document.querySelectorAll('[data-del-col]').forEach(function (b) {
              b.addEventListener('click', function () { deleteColumn(b.getAttribute('data-del-col')); });
            });
            document.querySelectorAll('[data-pop-name]').forEach(function (nm) {
              nm.addEventListener('mouseenter', function () {
                showNamePop(Number(nm.getAttribute('data-pop-name')), nm);
              });
              nm.addEventListener('mouseleave', scheduleHideNamePop);
            });
          }

          // ---- Employee name hover-card: working hours (editable) + notes ----
          var popHideTimer = null;
          var popMemberId = null;

          function cancelHideNamePop() { if (popHideTimer) { clearTimeout(popHideTimer); popHideTimer = null; } }
          function scheduleHideNamePop() {
            cancelHideNamePop();
            popHideTimer = setTimeout(hideNamePop, 180);
          }
          function hideNamePop() {
            var pop = document.getElementById('namePop');
            pop.classList.remove('open');
            popMemberId = null;
          }

          function buildNamePop(m) {
            var rows = '';
            STATE.columns.forEach(function (c) {
              var h = getHours(m.id, c.id);
              rows +=
                '<div class="np-hrow">' +
                  '<span>' + esc(c.label) + '</span>' +
                  '<input class="hr" type="text" inputmode="decimal" ' +
                    'data-member="' + m.id + '" data-col="' + c.id + '" ' +
                    'value="' + (h > 0 ? fmtHours(h) : '') + '" placeholder="0" />' +
                '</div>';
            });
            return (
              '<div class="np-head">' +
                '<span class="np-name">' + esc(m.name) + '</span>' +
                '<span class="np-team">' + esc(m.team) + '</span>' +
              '</div>' +
              '<div class="np-total">Working hours <b data-pop-total="' + m.id + '">' + fmtHours(rowTotal(m.id)) + '</b></div>' +
              '<div class="np-hours">' + rows + '</div>' +
              '<div class="np-notes-label">Notes</div>' +
              '<textarea class="np-notes" data-note-member="' + m.id + '" placeholder="e.g. split across Navii &amp; Rasset">' + esc(m.responsibility) + '</textarea>' +
              '<div class="np-hint">Edits save automatically.</div>'
            );
          }

          function showNamePop(memberId, anchor) {
            cancelHideNamePop();
            var m = STATE.members.find(function (x) { return x.id === memberId; });
            if (!m) return;
            var pop = document.getElementById('namePop');
            popMemberId = memberId;
            pop.innerHTML = buildNamePop(m);

            // Position next to the name, flipping left/up if it would overflow.
            pop.classList.add('open');
            var r = anchor.getBoundingClientRect();
            var pw = pop.offsetWidth, ph = pop.offsetHeight;
            var left = r.right + 10;
            if (left + pw > window.innerWidth - 8) left = r.left - pw - 10;
            if (left < 8) left = 8;
            var top = r.top;
            if (top + ph > window.innerHeight - 8) top = window.innerHeight - ph - 8;
            if (top < 8) top = 8;
            pop.style.left = left + 'px';
            pop.style.top = top + 'px';

            wireNamePop(pop);
          }

          function wireNamePop(pop) {
            pop.onmouseenter = cancelHideNamePop;
            pop.onmouseleave = scheduleHideNamePop;
            pop.querySelectorAll('input.hr').forEach(function (inp) {
              inp.addEventListener('focus', function () { inp.dataset.orig = inp.value; });
              inp.addEventListener('keydown', function (e) { if (e.key === 'Enter') inp.blur(); });
              inp.addEventListener('blur', function () { commitCell(inp); });
            });
            var ta = pop.querySelector('textarea.np-notes');
            if (ta) {
              ta.addEventListener('focus', function () { ta.dataset.orig = ta.value; });
              ta.addEventListener('blur', function () { commitNote(ta); });
            }
          }

          function commitNote(ta) {
            var memberId = Number(ta.getAttribute('data-note-member'));
            var val = String(ta.value).trim();
            var m = STATE.members.find(function (x) { return x.id === memberId; });
            if (!m || val === (m.responsibility || '')) return;
            var prev = m.responsibility || '';
            m.responsibility = val;

            fetch('/api/team-work/members/' + memberId, {
              method: 'PATCH',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ responsibility: val })
            })
              .then(function (r) { return r.json(); })
              .then(function (json) {
                if (!json.ok) throw new Error(json.error || 'save failed');
                flashSaved();
                // Reflect the note under the name without closing the card.
                var row = document.querySelector('[data-member-row="' + memberId + '"] .name-cell');
                if (row) {
                  var resp = row.querySelector('.resp');
                  if (val) {
                    if (!resp) {
                      resp = document.createElement('div');
                      resp.className = 'resp';
                      row.appendChild(resp);
                    }
                    resp.textContent = val;
                  } else if (resp) {
                    resp.remove();
                  }
                }
              })
              .catch(function () {
                m.responsibility = prev;
                ta.value = prev;
                alert('Could not save notes.');
              });
          }

          function commitCell(inp) {
            var raw = String(inp.value).trim().replace(/,/g, '.');
            if (raw === '' ) raw = '0';
            var val = Number(raw);
            if (isNaN(val) || val < 0) { inp.value = inp.dataset.orig || ''; return; }
            var memberId = Number(inp.dataset.member);
            var colId = Number(inp.dataset.col);
            var prev = getHours(memberId, colId);
            if (val === prev) { inp.value = val > 0 ? fmtHours(val) : ''; return; }

            syncHourInputs(memberId, colId, val);

            fetch('/api/team-work/hours', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ date: STATE.date, member_id: memberId, column_id: colId, hours: val })
            })
              .then(function (r) { return r.json(); })
              .then(function (json) {
                if (!json.ok) throw new Error(json.error || 'save failed');
                flashSaved();
                refreshLogs();
              })
              .catch(function () {
                syncHourInputs(memberId, colId, prev);
                alert('Could not save that change.');
              });
          }

          function relTime(iso) {
            var d = new Date(iso);
            var diff = (Date.now() - d.getTime()) / 1000;
            if (diff < 60) return 'just now';
            if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
            if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
            return d.toLocaleDateString();
          }

          function renderLogs() {
            var wrap = document.getElementById("logs");
            if (!STATE.logs || !STATE.logs.length) {
              wrap.innerHTML = '<div class="empty">No changes logged yet.</div>';
              return;
            }
            wrap.innerHTML = STATE.logs.map(function (l) {
              var who = esc(l.actor_name || 'Someone');
              var main;
              if (l.action === 'member_added') main = who + ' added <strong>' + esc(l.member_name) + '</strong> to ' + esc(l.detail || 'the team');
              else if (l.action === 'member_removed') main = who + ' removed <strong>' + esc(l.member_name) + '</strong>';
              else if (l.action === 'column_added') main = who + ' added column <strong>' + esc(l.column_label) + '</strong>';
              else if (l.action === 'column_removed') main = who + ' removed column <strong>' + esc(l.column_label) + '</strong>';
              else {
                var oldH = Number(l.old_hours) || 0, newH = Number(l.new_hours) || 0;
                var dir = newH >= oldH ? 'up' : 'down';
                var verb = newH >= oldH ? 'increased' : 'decreased';
                main = '<strong>' + esc(l.member_name) + '</strong> <span class="' + dir + '">' + verb + '</span> ' +
                  esc(l.column_label) + ' hours ' + fmtHours(oldH) + ' → <strong>' + fmtHours(newH) + '</strong>';
              }
              var dateLabel = l.work_date ? (' · ' + l.work_date) : '';
              return '<div class="log-item"><div class="l-main">' + main + '</div>' +
                '<div class="l-meta">' + relTime(l.created_at) + dateLabel + '</div></div>';
            }).join('');
          }

          function refreshLogs() {
            fetch('/api/team-work/logs')
              .then(function (r) { return r.json(); })
              .then(function (json) { if (json.ok) { STATE.logs = json.data; renderLogs(); } });
          }

          function loadDate(date) {
            fetch('/api/team-work?date=' + encodeURIComponent(date))
              .then(function (r) { return r.json(); })
              .then(function (json) {
                if (!json.ok) throw new Error(json.error || 'load failed');
                STATE = json.data;
                renderAll();
                var url = new URL(window.location.href);
                url.searchParams.set('date', date);
                window.history.replaceState({}, '', url.toString());
              })
              .catch(function () { alert('Could not load that date.'); });
          }

          function renderAll() {
            document.getElementById("missingBanner").style.display = STATE.tablesMissing ? 'block' : 'none';
            document.getElementById("datePicker").value = STATE.date;
            renderSummary();
            renderGrid();
            renderLogs();
          }

          // --- modals ---
          function openModal(id) { document.getElementById(id).classList.add('open'); }
          function closeModals() {
            document.querySelectorAll('.modal-overlay').forEach(function (m) { m.classList.remove('open'); });
          }
          document.querySelectorAll('[data-close-modal]').forEach(function (b) {
            b.addEventListener('click', closeModals);
          });
          document.querySelectorAll('.modal-overlay').forEach(function (o) {
            o.addEventListener('click', function (e) { if (e.target === o) closeModals(); });
          });

          document.getElementById("addMemberBtn").addEventListener('click', function () {
            document.getElementById("memberName").value = '';
            document.getElementById("memberResp").value = '';
            openModal('memberModal');
            setTimeout(function () { document.getElementById("memberName").focus(); }, 30);
          });
          document.getElementById("memberSave").addEventListener('click', function () {
            var name = document.getElementById("memberName").value.trim();
            var team = document.getElementById("memberTeam").value;
            var resp = document.getElementById("memberResp").value.trim();
            if (!name) { alert('Name is required'); return; }
            fetch('/api/team-work/members', {
              method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ name: name, team: team, responsibility: resp })
            })
              .then(function (r) { return r.json(); })
              .then(function (json) {
                if (!json.ok) throw new Error(json.error);
                STATE.members.push(json.data);
                closeModals();
                renderGrid(); renderSummary(); refreshLogs();
              })
              .catch(function () { alert('Could not add person.'); });
          });

          document.getElementById("addColBtn").addEventListener('click', function () {
            document.getElementById("colLabel").value = '';
            openModal('colModal');
            setTimeout(function () { document.getElementById("colLabel").focus(); }, 30);
          });
          document.getElementById("colSave").addEventListener('click', function () {
            var label = document.getElementById("colLabel").value.trim();
            if (!label) { alert('Label is required'); return; }
            fetch('/api/team-work/columns', {
              method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ label: label })
            })
              .then(function (r) { return r.json(); })
              .then(function (json) {
                if (!json.ok) throw new Error(json.error);
                STATE.columns.push(json.data);
                closeModals();
                renderGrid(); renderSummary(); refreshLogs();
              })
              .catch(function () { alert('Could not add column.'); });
          });

          function deleteMember(id) {
            var m = STATE.members.find(function (x) { return String(x.id) === String(id); });
            if (!m || !confirm('Remove ' + m.name + '? Their hours for all dates are deleted.')) return;
            fetch('/api/team-work/members/' + id, { method: 'DELETE' })
              .then(function (r) { return r.json(); })
              .then(function (json) {
                if (!json.ok) throw new Error(json.error);
                STATE.members = STATE.members.filter(function (x) { return String(x.id) !== String(id); });
                renderGrid(); renderSummary(); refreshLogs();
              })
              .catch(function () { alert('Could not remove person.'); });
          }

          function deleteColumn(id) {
            var c = STATE.columns.find(function (x) { return String(x.id) === String(id); });
            if (!c || !confirm('Remove column "' + c.label + '"? Its hours for all dates are deleted.')) return;
            fetch('/api/team-work/columns/' + id, { method: 'DELETE' })
              .then(function (r) { return r.json(); })
              .then(function (json) {
                if (!json.ok) throw new Error(json.error);
                STATE.columns = STATE.columns.filter(function (x) { return String(x.id) !== String(id); });
                renderGrid(); renderSummary(); refreshLogs();
              })
              .catch(function () { alert('Could not remove column.'); });
          }

          // --- date controls ---
          function shiftDate(days) {
            var d = new Date(STATE.date + 'T00:00:00Z');
            d.setUTCDate(d.getUTCDate() + days);
            loadDate(d.toISOString().slice(0, 10));
          }
          document.getElementById("prevDay").addEventListener('click', function () { shiftDate(-1); });
          document.getElementById("nextDay").addEventListener('click', function () { shiftDate(1); });
          document.getElementById("todayBtn").addEventListener('click', function () {
            loadDate("${today}");
          });
          document.getElementById("datePicker").addEventListener('change', function (e) {
            if (e.target.value) loadDate(e.target.value);
          });

          renderAll();
        </script>
      `;
}

export { renderTeamWorkPage };
