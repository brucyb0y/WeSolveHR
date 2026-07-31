// Markup for GET /leads/:business/intelligence.
//
// Body markup extracted verbatim from renderBusinessLeadIntelligencePage() (lib/server/app.js
// lines 29213-29827). The document shell now comes from
// app/layout.jsx, the <style> block from ./lead-intelligence.css, and the inline
// <script> from public/js/.

import { escapeHtml, formatDateTime } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function getLeadTimeframeRange(timeframe) {
  const now = new Date();
  const start = new Date(now);
  const end = new Date(now);

  if (timeframe === "yesterday") {
    start.setDate(start.getDate() - 1);
    start.setHours(0, 0, 0, 0);
    end.setDate(end.getDate() - 1);
    end.setHours(23, 59, 59, 999);
  } else if (timeframe === "this_week") {
    const day = start.getDay();
    const diff = start.getDate() - day + (day === 0 ? -6 : 1);
    start.setDate(diff);
    start.setHours(0, 0, 0, 0);
    end.setHours(23, 59, 59, 999);
  } else if (timeframe === "this_month") {
    start.setDate(1);
    start.setHours(0, 0, 0, 0);
    end.setHours(23, 59, 59, 999);
  } else {
    start.setHours(0, 0, 0, 0);
    end.setHours(23, 59, 59, 999);
  }

  return {
    startIso: start.toISOString(),
    endIso: end.toISOString(),
  };
}

function buildLeadIntelligenceMetrics(
  rows = [],
  voiceRows = [],
  timeframe = "today",
) {
  const { startIso, endIso } = getLeadTimeframeRange(timeframe);

  const inRange = (dateValue) => {
    if (!dateValue) return false;
    const d = new Date(dateValue);
    return d >= new Date(startIso) && d <= new Date(endIso);
  };

  const filteredLeads = rows.filter(
    (x) => inRange(x.created_at) || inRange(x.updated_at),
  );
  const filteredVoices = voiceRows.filter((x) => inRange(x.created_at));

  const callsWithTranscript = filteredLeads.filter((x) =>
    String(x.latest_transcript || "").trim(),
  );

  const industryMap = new Map();
  const employeeMap = new Map();

  for (const lead of filteredLeads) {
    const industry =
      lead.industry_primary || lead.industry || lead.raw_industry || "Unknown";

    if (!industryMap.has(industry)) {
      industryMap.set(industry, {
        industry,
        leads: 0,
        transcripts: 0,
        qualified: 0,
        worth_talking: 0,
        in_progress: 0,
        completed: 0,
      });
    }

    const item = industryMap.get(industry);
    item.leads += 1;
    if (lead.latest_transcript) item.transcripts += 1;
    if (lead.qualified) item.qualified += 1;
    if (lead.worth_talking) item.worth_talking += 1;
    if (lead.status === "in_progress") item.in_progress += 1;
    if (lead.status === "completed") item.completed += 1;

    const employee =
      lead.assigned_to_employee ||
      lead.assigned_employee ||
      lead.assigned_user_name ||
      lead.owner_employee ||
      lead.employee_name ||
      lead.uploaded_by_employee ||
      "Unknown";

    if (!employeeMap.has(employee)) {
      employeeMap.set(employee, {
        employee,
        leads: 0,
        transcripts: 0,
        qualified: 0,
        worth_talking: 0,
        completed: 0,
      });
    }

    const emp = employeeMap.get(employee);
    emp.leads += 1;
    if (lead.latest_transcript) emp.transcripts += 1;
    if (lead.qualified) emp.qualified += 1;
    if (lead.worth_talking) emp.worth_talking += 1;
    if (lead.status === "completed") emp.completed += 1;
  }

  return {
    timeframe,
    total_leads: filteredLeads.length,
    calls_uploaded: filteredVoices.length,
    calls_with_transcript: callsWithTranscript.length,
    qualified: filteredLeads.filter((x) => x.qualified).length,
    worth_talking: filteredLeads.filter((x) => x.worth_talking).length,
    in_progress: filteredLeads.filter((x) => x.status === "in_progress").length,
    completed: filteredLeads.filter((x) => x.status === "completed").length,
    industryRows: Array.from(industryMap.values()).sort(
      (a, b) => b.leads - a.leads,
    ),
    employeeRows: Array.from(employeeMap.values()).sort(
      (a, b) => b.leads - a.leads,
    ),
    recentTranscriptRows: callsWithTranscript.slice(0, 10),
  };
}

function renderBusinessLeadIntelligencePage(data) {
  const business = data.business;
  const timeframe = data.timeframe || "today";
  const metrics = buildLeadIntelligenceMetrics(
    data.businessRows || [],
    data.voiceRows || [],
    timeframe,
  );
  const aiRun = data.aiRun || null;
  const aiSummary = aiRun?.summary || null;

  const aiHistoryRuns = data.aiHistoryRuns || [];

  const aiHistoryHtml = aiHistoryRuns.length
    ? aiHistoryRuns
        .map((run) => {
          const s = run.summary || {};
          return `
          <div class="ai-card">
            <div class="ai-card-title">
              ${escapeHtml(run.source_label || run.timeframe || "AI Run")}
            </div>
            <div class="muted">
              Generated: ${escapeHtml(formatDateTime(run.created_at))}
              · Transcripts: ${escapeHtml(run.transcript_count || 0)}
              · Run ID: ${escapeHtml(run.id)}
            </div>
            <div style="margin-top:10px;">
              ${escapeHtml(s.overall_summary || s.cumulative_summary || "No summary saved.")}
            </div>
            <div class="ai-ref">
              Timeframe: ${escapeHtml(run.timeframe || "-")}
            </div>
          </div>
        `;
        })
        .join("")
    : `<div class="empty-cell">No previous AI intelligence runs yet.</div>`;

  const cumulativeSummary = aiSummary?.cumulative_summary || "";

  const cumulativeHtml =
    timeframe === "cumulative"
      ? `
        <div class="ai-card">
          <div class="ai-card-title">Cumulative Summary</div>
          <div class="muted">${escapeHtml(cumulativeSummary || "Generate cumulative intelligence to see this.")}</div>
        </div>

        <h3>Repeated Patterns</h3>
        ${renderList(aiSummary?.repeated_patterns)}

        <h3>Recurring Objections</h3>
        ${renderList(aiSummary?.recurring_objections)}

        <h3>Improving Signals</h3>
        ${renderList(aiSummary?.improving_signals)}

        <h3>Warning Signals</h3>
        ${renderList(aiSummary?.warning_signals)}

        <h3>Best Next Actions</h3>
        ${renderList(aiSummary?.best_next_actions)}

        <h3>What To Watch Next</h3>
        ${renderList(aiSummary?.what_to_watch_next)}
      `
      : "";

  const renderList = (items) =>
    Array.isArray(items) && items.length
      ? `<ul>${items.map((x) => `<li>${escapeHtml(x)}</li>`).join("")}</ul>`
      : `<div class="muted">None found yet.</div>`;

  const aiTopLearningsHtml = aiSummary?.top_learnings?.length
    ? aiSummary.top_learnings
        .map(
          (x) => `
        <div class="ai-card">
          <div class="ai-card-title">${escapeHtml(x.insight || "")}</div>
          <div class="muted">${escapeHtml(x.why_it_matters || "")}</div>
          <div class="ai-ref">Supported by leads: ${escapeHtml((x.supporting_lead_ids || []).join(", ") || "-")}</div>
        </div>
      `,
        )
        .join("")
    : `<div class="empty-cell">No AI learning generated yet.</div>`;

  const aiIndustryHtml = aiSummary?.industry_intelligence?.length
    ? aiSummary.industry_intelligence
        .map(
          (x) => `
        <div class="ai-card">
          <div class="ai-card-title">${escapeHtml(x.industry || "Unknown Industry")}</div>
          <div class="muted"><strong>Thesis:</strong> ${escapeHtml(x.industry_thesis || "")}</div>
          <div><strong>Pain Points</strong>${renderList(x.common_pain_points)}</div>
          <div><strong>Objections</strong>${renderList(x.common_objections)}</div>
          <div><strong>Recommendations</strong>${renderList(x.recommendations)}</div>
          <div class="ai-ref">Supported by leads: ${escapeHtml((x.supporting_lead_ids || []).join(", ") || "-")}</div>
        </div>
      `,
        )
        .join("")
    : `<div class="empty-cell">No industry intelligence generated yet.</div>`;

  const aiReviewHtml = aiSummary?.leads_to_review?.length
    ? aiSummary.leads_to_review
        .map(
          (x) => `
        <tr>
          <td>${escapeHtml(x.lead_id || "-")}</td>
          <td>${escapeHtml(x.lead_name || "-")}</td>
          <td>${escapeHtml(x.industry || "-")}</td>
          <td>${escapeHtml(x.reason || "-")}</td>
          <td>${escapeHtml(x.recommended_next_step || "-")}</td>
        </tr>
      `,
        )
        .join("")
    : `<tr><td colspan="5" class="empty-cell">No AI review leads yet.</td></tr>`;
  const timeframeLink = (key, label) => `
    <a class="filter-chip ${timeframe === key ? "active" : ""}" href="/leads/${business}/intelligence?timeframe=${key}">
      ${label}
    </a>
  `;

  const industryRowsHtml = metrics.industryRows.length
    ? metrics.industryRows
        .map(
          (x) => `
      <tr>
        <td><strong>${escapeHtml(x.industry)}</strong></td>
        <td>${x.leads}</td>
        <td>${x.transcripts}</td>
        <td>${x.qualified}</td>
        <td>${x.worth_talking}</td>
        <td>${x.in_progress}</td>
        <td>${x.completed}</td>
      </tr>
    `,
        )
        .join("")
    : `<tr><td colspan="7" class="empty-cell">No industry data found for this timeframe.</td></tr>`;

  const employeeRowsHtml = metrics.employeeRows.length
    ? metrics.employeeRows
        .map(
          (x) => `
      <tr>
        <td><strong>${escapeHtml(x.employee)}</strong></td>
        <td>${x.leads}</td>
        <td>${x.transcripts}</td>
        <td>${x.qualified}</td>
        <td>${x.worth_talking}</td>
        <td>${x.completed}</td>
      </tr>
    `,
        )
        .join("")
    : `<tr><td colspan="6" class="empty-cell">No employee data found for this timeframe.</td></tr>`;

  const transcriptRowsHtml = metrics.recentTranscriptRows.length
    ? metrics.recentTranscriptRows
        .map(
          (x) => `
      <div class="transcript-card">
        <div class="transcript-title">
          ${escapeHtml(x.company || x.business_name || x.contact_name || x.phone || "Unknown lead")}
        </div>
        <div class="muted">
          ${escapeHtml(x.industry_primary || x.industry || "Unknown industry")}
          · ${escapeHtml(x.assigned_to || x.last_spoke_to_name || "Unknown owner")}
          · ${escapeHtml(x.status || "-")}
        </div>
        <div class="transcript-text">${escapeHtml(x.latest_transcript || "")}</div>
      </div>
    `,
        )
        .join("")
    : `<div class="empty-cell">No transcripts found for this timeframe.</div>`;

  return `
            ${renderTopNav("leads")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Lead Intelligence</div>
              <h1>${escapeHtml(business)} Intelligence</h1>
              <div class="subtitle">
                Phase 1 operational intelligence: calls, transcripts, industries, employees, qualified leads.
              </div>
            </div>

            <a class="btn" href="/leads/${business}">← Back to Leads</a>
          </div>

          <div class="filters">
            ${timeframeLink("today", "Today")}
            ${timeframeLink("yesterday", "Yesterday")}
            ${timeframeLink("this_week", "This Week")}
            ${timeframeLink("this_month", "This Month")}
            ${timeframeLink("all_history", "All Past Transcripts")}
${timeframeLink("cumulative", "Cumulative")}
          </div>

          <div class="stats">
            <div class="stat-card">
              <div class="stat-label">Leads</div>
              <div class="stat-value">${metrics.total_leads}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Calls Uploaded</div>
              <div class="stat-value">${metrics.calls_uploaded}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Transcripts</div>
              <div class="stat-value">${metrics.calls_with_transcript}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Qualified</div>
              <div class="stat-value">${metrics.qualified}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Worth Talking</div>
              <div class="stat-value">${metrics.worth_talking}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">In Progress</div>
              <div class="stat-value">${metrics.in_progress}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Completed</div>
              <div class="stat-value">${metrics.completed}</div>
            </div>
          </div>

          <div class="grid-2">
            <div class="panel">
              <h2>Industry Report</h2>
              <table>
                <thead>
                  <tr>
                    <th>Industry</th>
                    <th>Leads</th>
                    <th>Transcripts</th>
                    <th>Qualified</th>
                    <th>Worth Talking</th>
                    <th>In Progress</th>
                    <th>Completed</th>
                  </tr>
                </thead>
                <tbody>
                  ${industryRowsHtml}
                </tbody>
              </table>
            </div>

            <div class="panel">
              <h2>Person-wise Report</h2>
              <table>
                <thead>
                  <tr>
                    <th>Person</th>
                    <th>Leads</th>
                    <th>Transcripts</th>
                    <th>Qualified</th>
                    <th>Worth Talking</th>
                    <th>Completed</th>
                  </tr>
                </thead>
                <tbody>
                  ${employeeRowsHtml}
                </tbody>
              </table>
            </div>
          </div>

          <div class="panel">
            <h2>Recent Call Transcripts</h2>
            ${transcriptRowsHtml}
          </div>
                              <div class="panel">
            <h2>AI Intelligence Layer</h2>

            <div class="ai-actions">
              ${
                timeframe === "cumulative"
                  ? `
                    <button class="btn" type="button" onclick="generateCumulativeLeadAIIntelligenceNow()">
                      Generate / Refresh Cumulative Intelligence
                    </button>
                  `
                  : `
                    <button class="btn" type="button" onclick="generateLeadAIIntelligenceNow()">
                      Generate / Refresh AI Intelligence
                    </button>
                  `
              }

              <div class="ai-status" id="aiStatus">
                ${
                  aiRun
                    ? `Last generated: ${escapeHtml(formatDateTime(aiRun.created_at))}`
                    : "Not generated yet."
                }
              </div>
            </div>

            ${
              timeframe === "all_history"
                ? `
                  <div class="ai-card">
                    <div class="ai-card-title">All Past Transcripts Snapshot</div>
                    <div class="muted">
                      This analyzes older saved transcripts as a one-time historical snapshot.
                      It gets saved in history and can later feed cumulative intelligence.
                    </div>
                  </div>
                `
                : ""
            }

            ${
              timeframe === "cumulative"
                ? cumulativeHtml
                : `
                  <div class="ai-card">
                    <div class="ai-card-title">Overall Summary</div>
                    <div class="muted">
                      ${escapeHtml(aiSummary?.overall_summary || "Generate AI intelligence to see summary.")}
                    </div>
                  </div>

                  <h3>Top Learnings</h3>
                  ${aiTopLearningsHtml}

                  <h3>Industry Intelligence</h3>
                  ${aiIndustryHtml}

                  <h3>Leads To Review</h3>
                  <table>
                    <thead>
                      <tr>
                        <th>Lead ID</th>
                        <th>Lead</th>
                        <th>Industry</th>
                        <th>Reason</th>
                        <th>Next Step</th>
                      </tr>
                    </thead>
                    <tbody>
                      ${aiReviewHtml}
                    </tbody>
                  </table>
                `
            }
          </div>

          <div class="panel">
            <h2>AI Intelligence History</h2>
            <div class="muted" style="margin-bottom:12px;">
              Every AI generation is saved here. This prevents losing prior daily, weekly, monthly, and all-history intelligence.
            </div>
            ${aiHistoryHtml}
          </div>
                <script>
                    async function generateLeadAIIntelligenceNow() {
            const status = document.getElementById("aiStatus");
            if (status) status.textContent = "Generating AI intelligence...";

            const res = await fetch("/api/leads/${encodeURIComponent(business)}/intelligence/generate", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ timeframe: "${escapeHtml(timeframe)}" })
            });

            const json = await res.json();

            if (!json.ok) {
              if (status) status.textContent = json.error || "Failed to generate AI intelligence.";
              alert(json.error || "Failed to generate AI intelligence.");
              return;
            }

            if (status) status.textContent = "AI intelligence generated. Refreshing...";
            window.location.reload();
          }

          async function generateCumulativeLeadAIIntelligenceNow() {
            const status = document.getElementById("aiStatus");
            if (status) status.textContent = "Generating cumulative intelligence from prior saved runs...";

            const res = await fetch("/api/leads/${encodeURIComponent(business)}/intelligence/generate-cumulative", {
              method: "POST",
              headers: { "Content-Type": "application/json" }
            });

            const json = await res.json();

            if (!json.ok) {
              if (status) status.textContent = json.error || "Failed to generate cumulative intelligence.";
              alert(json.error || "Failed to generate cumulative intelligence.");
              return;
            }

            if (status) status.textContent = "Cumulative intelligence generated. Refreshing...";
            window.location.reload();
          }
          
        </script>
      
  `;
}

export {
  renderBusinessLeadIntelligencePage,
};
