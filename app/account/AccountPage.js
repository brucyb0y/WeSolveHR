// Markup for GET /account.
//
// Body markup extracted verbatim from the Express handler (lib/server/app.js
// lines 35384-36100), which built the whole document inline.
// The document shell now comes from app/layout.jsx, the <style> block from
// ./account.css, and any static <script> from public/js/.

import { renderAccountFieldSelect } from "@/lib/ui/account-fields.js";
import { escapeHtml, formatDateTime } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderAccountPage({
  user,
  isAdminView,
  appraisal,
  timeText,
  notesText,
  ptoRemaining,
  sickRemaining,
  feedbackHtml,
  futureLeaveHtml,
  leaveSummaryHtml,
  teamAppraisalHtml,
  teamFeedbackHtml,
}) {
  return `        ${renderTopNav("account")}
        <div class="wrap">
          <div class="topbar">
            <div class="title-block">
              <h1>${escapeHtml(user.name || "My Account")}</h1>
              <p>${escapeHtml(user.role || "")}</p>
            </div>
          </div>

          <div class="grid">
            <div style="display:grid; gap:18px;">
              <div class="card">
                <h2>Profile</h2>
                <div class="profile-meta">
                  <div class="meta-box">
                    <div class="meta-label">Name</div>
                    <div class="meta-value">${escapeHtml(user.name || "-")}</div>
                  </div>
                  <div class="meta-box">
                    <div class="meta-label">Role</div>
                    <div class="meta-value">${escapeHtml(user.role || "-")}</div>
                  </div>
                  <div class="meta-box">
                    <div class="meta-label">Department</div>
                    <div class="meta-value">${renderAccountFieldSelect("department", user.department)}</div>
                  </div>
                  <div class="meta-box">
                    <div class="meta-label">Designation</div>
                    <div class="meta-value">${renderAccountFieldSelect("designation", user.designation)}</div>
                  </div>
                  <div class="meta-box">
                    <div class="meta-label">Time</div>
                    <div class="meta-value">${escapeHtml(timeText)}</div>
                  </div>
                  <div class="meta-box">
                    <div class="meta-label">Notes</div>
                    <div class="meta-value">${escapeHtml(notesText)}</div>
                  </div>
                </div>
              </div>

              <div class="card">
                <h2>Leave Balance</h2>
                <div class="stats-row">
                  <div class="stat-card">
                    <div class="stat-label">PTO Remaining</div>
                    <div class="stat-value">${ptoRemaining}</div>
                  </div>
                  <div class="stat-card">
                    <div class="stat-label">Sick Remaining</div>
                    <div class="stat-value">${sickRemaining}</div>
                  </div>
                </div>
              </div>

              <div class="card">
                <h2>Last Appraisal</h2>
                ${
                  appraisal
                    ? `
                      <div class="appraisal-block">
                        <div class="appraisal-row">
                          <div class="appraisal-label">Rating</div>
                          <div class="appraisal-value">${appraisal.rating || "-"}</div>
                        </div>
                        <div class="appraisal-row">
                          <div class="appraisal-label">Review Date</div>
                          <div class="appraisal-value">${formatDateTime(appraisal.created_at)}</div>
                        </div>
                        <div class="appraisal-row">
                          <div class="appraisal-label">Strengths</div>
                          <div class="appraisal-value">${escapeHtml(appraisal.strengths || "-")}</div>
                        </div>
                        <div class="appraisal-row">
                          <div class="appraisal-label">Improvement Areas</div>
                          <div class="appraisal-value">${escapeHtml(appraisal.improvement_areas || "-")}</div>
                        </div>
                        <div class="appraisal-row">
                          <div class="appraisal-label">Manager Comment</div>
                          <div class="appraisal-value">${escapeHtml(appraisal.manager_comment || "-")}</div>
                        </div>
                      </div>
                    `
                    : `<div class="empty-state">No appraisal yet</div>`
                }
              </div>
            </div>

            <div class="card">
              <h2>Feedback Timeline</h2>
              <div class="timeline">
                ${feedbackHtml}
              </div>
            </div>
          </div>

          ${
            isAdminView
              ? `
                <div class="admin-section">
                  <div class="card">
                    <div class="section-eyebrow">Admin only</div>
                    <h2>Future Leave</h2>
                    <div class="table-wrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Employee</th>
                            <th>Leave Date</th>
                            <th>Created By</th>
                            <th>Note</th>
                          </tr>
                        </thead>
                        <tbody>
                          ${futureLeaveHtml}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div class="card">
                    <div class="section-eyebrow">Admin only</div>
                    <h2>Team Feedback</h2>
                    <div class="table-wrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Employee</th>
                            <th>Type</th>
                            <th>Note</th>
                            <th>Created By</th>
                            <th>Created At</th>
                          </tr>
                        </thead>
                        <tbody>
                          ${teamFeedbackHtml}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div class="card">
                    <div class="section-eyebrow">Admin only</div>
                    <h2>Team Appraisals</h2>
                    <div class="table-wrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Employee</th>
                            <th>Rating</th>
                            <th>Strengths</th>
                            <th>Improvement Areas</th>
                            <th>Manager Comment</th>
                            <th>Created At</th>
                          </tr>
                        </thead>
                        <tbody>
                          ${teamAppraisalHtml}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div class="card">
                    <div class="section-eyebrow">Admin only</div>
                    <h2>Leave Summary</h2>
                    <div class="table-wrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Employee</th>
                            <th>Total Leave Entries</th>
                            <th>Upcoming Leaves</th>
                            <th>Next Leave</th>
                          </tr>
                        </thead>
                        <tbody>
                          ${leaveSummaryHtml}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              `
              : ""
          }
        </div>
        <script src="/js/account.js"></script>
      `;
}

export { renderAccountPage };
