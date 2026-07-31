// Markup for GET /clients/:id/edit.
//
// Body markup extracted verbatim from the Express handler (lib/server/app.js
// lines 43043-43408), which built the whole document inline.
// The document shell now comes from app/layout.jsx, the <style> block from
// ./client-edit.css, and any static <script> from public/js/.

import { renderGtmMultiselectField } from "@/lib/ui/gtm-multiselect.js";
import { escapeHtml } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderClientEditPage({ client, users, primaryContact, secondContact, thirdContact }) {
  return `          ${renderTopNav("clients")}

          <div class="wrap">
            <div class="topbar">
              <div>
                <div class="eyebrow">Edit Client</div>
                <h1>${escapeHtml(client.name || "Client")}</h1>
                <div class="subtitle">Update client basics, status, health, ownership, and Drive folder.</div>
              </div>
              <a class="btn" href="/clients/${client.id}">← Back to Client</a>
            </div>

            <form method="POST" action="/clients/${client.id}/edit">
              <div class="panel">
                <h2>Basic Info</h2>

                <div class="grid">
                  <div class="field">
                    <label>Client Name</label>
                    <input name="name" value="${escapeHtml(client.name || "")}" required />
                  </div>

                  <div class="field">
                    <label>Company Name</label>
                    <input name="company_name" value="${escapeHtml(client.company_name || "")}" />
                  </div>

                  <div class="field">
                    <label>Slug</label>
                    <input name="slug" value="${escapeHtml(client.slug || "")}" />
                  </div>

                  <div class="field">
                    <label>Google Drive Folder Link</label>
                    <input name="google_drive_folder_url" value="${escapeHtml(client.google_drive_folder_url || "")}" required />
                  </div>

                  <div class="field">
                    <label>Status</label>
                    <select name="status">
                      <option value="active" ${client.status === "active" ? "selected" : ""}>Active</option>
                      <option value="paused" ${client.status === "paused" ? "selected" : ""}>Paused</option>
                      <option value="onboarding" ${client.status === "onboarding" ? "selected" : ""}>Onboarding</option>
                      <option value="completed" ${client.status === "completed" ? "selected" : ""}>Completed</option>
                      <option value="inactive" ${client.status === "inactive" ? "selected" : ""}>Inactive</option>
                    </select>
                  </div>

                  <div class="field">
                    <label>Health</label>
                    <select name="health_status">
                      <option value="healthy" ${client.health_status === "healthy" ? "selected" : ""}>Healthy</option>
                      <option value="watch" ${client.health_status === "watch" ? "selected" : ""}>Watch</option>
                      <option value="at_risk" ${client.health_status === "at_risk" ? "selected" : ""}>At Risk</option>
                    </select>
                  </div>

                  <div class="field">
                    <label>Start Date</label>
                    <input type="date" name="start_date" value="${escapeHtml(client.start_date || "")}" />
                  </div>

                  <div class="field">
                    <label>Account Manager</label>
                    <select name="account_manager_user_id">
                      <option value="">Select account manager</option>
                      ${users
                        .map(
                          (u) =>
                            `<option value="${u.id}" ${
                              String(client.account_manager_user_id || "") ===
                              String(u.id)
                                ? "selected"
                                : ""
                            }>${escapeHtml(u.name)}</option>`,
                        )
                        .join("")}
                    </select>
                  </div>

                  <div class="field">
                    <label>Project Manager</label>
                    <select name="project_manager_user_id">
                      <option value="">Select project manager</option>
                      ${users
                        .map(
                          (u) =>
                            `<option value="${u.id}" ${
                              String(client.project_manager_user_id || "") ===
                              String(u.id)
                                ? "selected"
                                : ""
                            }>${escapeHtml(u.name)}</option>`,
                        )
                        .join("")}
                    </select>
                  </div>

                  ${renderGtmMultiselectField(users, client.gtm_associate_user_ids)}
                </div>

                <div class="field" style="margin-top:14px;">
                  <label>Description</label>
                  <textarea name="description">${escapeHtml(client.description || "")}</textarea>
                </div>
              </div>
<div class="panel">
  <h2>Client Contacts</h2>

  <input type="hidden" name="contact_1_id" value="${escapeHtml(primaryContact.id || "")}" />
  <input type="hidden" name="contact_2_id" value="${escapeHtml(secondContact.id || "")}" />
  <input type="hidden" name="contact_3_id" value="${escapeHtml(thirdContact.id || "")}" />

  <div class="grid">
    <div class="field">
      <label>Primary Contact Name</label>
      <input name="contact_1_name" value="${escapeHtml(primaryContact.name || "")}" />
    </div>

    <div class="field">
      <label>Primary Contact Email</label>
      <input name="contact_1_email" value="${escapeHtml(primaryContact.email || "")}" />
    </div>

    <div class="field">
      <label>Primary Contact Phone</label>
      <input name="contact_1_phone" value="${escapeHtml(primaryContact.phone || "")}" />
    </div>

    <div class="field">
      <label>Primary Contact Role</label>
      <input name="contact_1_role" value="${escapeHtml(primaryContact.role || "")}" />
    </div>
  </div>

  <div class="grid" style="margin-top:18px;">
    <div class="field">
      <label>Contact 2 Name</label>
      <input name="contact_2_name" value="${escapeHtml(secondContact.name || "")}" />
    </div>

    <div class="field">
      <label>Contact 2 Email</label>
      <input name="contact_2_email" value="${escapeHtml(secondContact.email || "")}" />
    </div>

    <div class="field">
      <label>Contact 2 Phone</label>
      <input name="contact_2_phone" value="${escapeHtml(secondContact.phone || "")}" />
    </div>

    <div class="field">
      <label>Contact 2 Role</label>
      <input name="contact_2_role" value="${escapeHtml(secondContact.role || "")}" />
    </div>
  </div>

  <div class="grid" style="margin-top:18px;">
    <div class="field">
      <label>Contact 3 Name</label>
      <input name="contact_3_name" value="${escapeHtml(thirdContact.name || "")}" />
    </div>

    <div class="field">
      <label>Contact 3 Email</label>
      <input name="contact_3_email" value="${escapeHtml(thirdContact.email || "")}" />
    </div>

    <div class="field">
      <label>Contact 3 Phone</label>
      <input name="contact_3_phone" value="${escapeHtml(thirdContact.phone || "")}" />
    </div>

    <div class="field">
      <label>Contact 3 Role</label>
      <input name="contact_3_role" value="${escapeHtml(thirdContact.role || "")}" />
    </div>
  </div>
</div>
              <div class="panel">
                <div class="actions">
                  <a class="btn" href="/clients/${client.id}">Cancel</a>
                  <button class="btn btn-primary" type="submit">Save Client</button>
                </div>
              </div>
            </form>
          </div>
        `;
}

export { renderClientEditPage };
