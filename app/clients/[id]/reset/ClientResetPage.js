// Markup for GET /clients/:id/reset.
//
// Body markup extracted verbatim from the Express handler (lib/server/app.js
// lines 43565-43737), which built the whole document inline.
// The document shell now comes from app/layout.jsx, the <style> block from
// ./client-reset.css, and any static <script> from public/js/.

import { renderTopNav } from "@/lib/ui/nav.js";

function renderClientResetPage({ clientId }) {
  return `        ${renderTopNav("clients")}

        <div class="wrap">
          <div class="panel">
            <h1>Reset Client Workspace</h1>
            <div class="muted">
              This will archive workspace data for this client. It will not delete the client, Google Drive folder, contacts, services, or client view token.
            </div>

            <div class="danger-box">
              <strong>Important:</strong> This is meant for cleaning a test/demo client workspace.
              Existing work items, updates, actions, contributors, milestones, documents, and activity logs will be hidden/archived.
            </div>

            <form method="POST" action="/clients/${clientId}/reset">
              <div class="check-list">
                <label>
                  <input type="checkbox" name="reset_work_items" checked />
                  Archive work items
                </label>

                <label>
                  <input type="checkbox" name="reset_updates" checked />
                  Archive updates
                </label>

                <label>
                  <input type="checkbox" name="reset_actions" checked />
                  Archive actions
                </label>

                <label>
                  <input type="checkbox" name="reset_contributors" checked />
                  Archive contributors
                </label>

                <label>
                  <input type="checkbox" name="reset_milestones" checked />
                  Archive milestones
                </label>

                <label>
                  <input type="checkbox" name="reset_documents" checked />
                  Archive document records
                </label>

                <label>
                  <input type="checkbox" name="reset_activity_logs" checked />
                  Archive activity logs
                </label>
              </div>

              <input
                class="confirm-input"
                name="confirm_text"
                placeholder="Type RESET to confirm"
              />

              <div class="actions">
                <a class="btn" href="/clients/${clientId}">Cancel</a>
                <button class="btn btn-danger" type="submit">Reset Selected Data</button>
              </div>
            </form>
          </div>
        </div>
      `;
}

export { renderClientResetPage };
