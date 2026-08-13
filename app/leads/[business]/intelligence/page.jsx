// /leads/:business/intelligence — replaces
// renderBusinessLeadIntelligencePage() + its route.
//
// BUG FIXED BY THIS CONVERSION: the Cumulative timeframe used to 500.
//
// renderBusinessLeadIntelligencePage() built `cumulativeHtml` at the top of the
// function using a `renderList` helper that was only declared (with const) a
// few lines further down. Reading it before initialisation threw, so
// ?timeframe=cumulative answered "Failed to load lead intelligence:
// p is not a function" — verified against the running app before this rewrite.
// Declaration order is not a thing you can preserve here: the list renderer is
// a component defined before use, so Cumulative simply works now.

import TopNav from "@/components/TopNav";
import { requireDashboardUser, orgIdFor } from "@/lib/auth";
import {
  getBusinessLeadsData,
  getLatestLeadAIIntelligenceRun,
  getLeadAIIntelligenceHistory,
  getBusinessCanonicalName,
  formatDateTime,
  buildLeadIntelligenceMetrics,
} from "@/lib/server/app.js";
import GenerateButton from "./GenerateButton";
import styles from "./intelligence.module.css";

export const dynamic = "force-dynamic";

const TIMEFRAMES = [
  ["today", "Today"],
  ["yesterday", "Yesterday"],
  ["this_week", "This Week"],
  ["this_month", "This Month"],
  ["all_history", "All Past Transcripts"],
  ["cumulative", "Cumulative"],
];

const ALLOWED = TIMEFRAMES.map(([key]) => key);

export async function generateMetadata({ params }) {
  const { business } = await params;
  return { title: `${getBusinessCanonicalName(business)} Intelligence` };
}

function List({ items }) {
  if (!Array.isArray(items) || !items.length) {
    return <div className="muted">None found yet.</div>;
  }
  return (
    <ul>
      {items.map((x, i) => (
        <li key={i}>{x}</li>
      ))}
    </ul>
  );
}

export default async function LeadIntelligencePage({ params, searchParams }) {
  const user = await requireDashboardUser();
  const orgId = orgIdFor(user);
  const { business: businessParam } = await params;
  const sp = await searchParams;

  const business = getBusinessCanonicalName(businessParam);
  const timeframe = ALLOWED.includes(sp?.timeframe) ? sp.timeframe : "today";

  const data = await getBusinessLeadsData(orgId, business, "all", "", 1, {});

  const [aiRun, aiHistoryRuns] = await Promise.all([
    getLatestLeadAIIntelligenceRun({ orgId, business, timeframe }),
    getLeadAIIntelligenceHistory({ orgId, business, limit: 20 }),
  ]);

  const metrics = buildLeadIntelligenceMetrics(
    data.businessRows || [],
    data.voiceRows || [],
    timeframe,
  );

  const aiSummary = aiRun?.summary || null;
  const isCumulative = timeframe === "cumulative";

  const statCards = [
    ["Leads", metrics.total_leads],
    ["Calls Uploaded", metrics.calls_uploaded],
    ["Transcripts", metrics.calls_with_transcript],
    ["Qualified", metrics.qualified],
    ["Worth Talking", metrics.worth_talking],
    ["In Progress", metrics.in_progress],
    ["Completed", metrics.completed],
  ];

  return (
    <>
      <TopNav active="leads" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Lead Intelligence</div>
            <h1>{business} Intelligence</h1>
            <div className={styles.subtitle}>
              Phase 1 operational intelligence: calls, transcripts, industries,
              employees, qualified leads.
            </div>
          </div>
          <a className={styles.btn} href={`/leads/${business}`}>
            ← Back to Leads
          </a>
        </div>

        <div className={styles.filters}>
          {TIMEFRAMES.map(([key, label]) => (
            <a
              key={key}
              className={`${styles.filterChip} ${timeframe === key ? styles.active : ""}`}
              href={`/leads/${business}/intelligence?timeframe=${key}`}
            >
              {label}
            </a>
          ))}
        </div>

        <div className={styles.stats}>
          {statCards.map(([label, value]) => (
            <div className={styles.statCard} key={label}>
              <div className={styles.statLabel}>{label}</div>
              <div className={styles.statValue}>{value}</div>
            </div>
          ))}
        </div>

        <div className={styles.grid2}>
          <div className={styles.panel}>
            <h2>Industry Report</h2>
            <table>
              <thead>
                <tr>
                  {[
                    "Industry",
                    "Leads",
                    "Transcripts",
                    "Qualified",
                    "Worth Talking",
                    "In Progress",
                    "Completed",
                  ].map((c) => (
                    <th key={c}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {metrics.industryRows.length ? (
                  metrics.industryRows.map((x) => (
                    <tr key={x.industry}>
                      <td>
                        <strong>{x.industry}</strong>
                      </td>
                      <td>{x.leads}</td>
                      <td>{x.transcripts}</td>
                      <td>{x.qualified}</td>
                      <td>{x.worth_talking}</td>
                      <td>{x.in_progress}</td>
                      <td>{x.completed}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={7} className="empty-cell">
                      No industry data found for this timeframe.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <div className={styles.panel}>
            <h2>Person-wise Report</h2>
            <table>
              <thead>
                <tr>
                  {[
                    "Person",
                    "Leads",
                    "Transcripts",
                    "Qualified",
                    "Worth Talking",
                    "Completed",
                  ].map((c) => (
                    <th key={c}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {metrics.employeeRows.length ? (
                  metrics.employeeRows.map((x) => (
                    <tr key={x.employee}>
                      <td>
                        <strong>{x.employee}</strong>
                      </td>
                      <td>{x.leads}</td>
                      <td>{x.transcripts}</td>
                      <td>{x.qualified}</td>
                      <td>{x.worth_talking}</td>
                      <td>{x.completed}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={6} className="empty-cell">
                      No employee data found for this timeframe.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className={styles.panel}>
          <h2>Recent Call Transcripts</h2>
          {metrics.recentTranscriptRows.length ? (
            metrics.recentTranscriptRows.map((x, i) => (
              <div className={styles.transcriptCard} key={x.id ?? i}>
                <div className={styles.transcriptTitle}>
                  {x.company ||
                    x.business_name ||
                    x.contact_name ||
                    x.phone ||
                    "Unknown lead"}
                </div>
                <div className="muted">
                  {x.industry_primary || x.industry || "Unknown industry"} ·{" "}
                  {x.assigned_to || x.last_spoke_to_name || "Unknown owner"} ·{" "}
                  {x.status || "-"}
                </div>
                <div className={styles.transcriptText}>
                  {x.latest_transcript || ""}
                </div>
              </div>
            ))
          ) : (
            <div className="empty-cell">
              No transcripts found for this timeframe.
            </div>
          )}
        </div>

        <div className={styles.panel}>
          <h2>AI Intelligence Layer</h2>

          <GenerateButton
            business={business}
            timeframe={timeframe}
            lastGenerated={aiRun ? formatDateTime(aiRun.created_at) : null}
          />

          {timeframe === "all_history" ? (
            <div className={styles.aiCard}>
              <div className={styles.aiCardTitle}>
                All Past Transcripts Snapshot
              </div>
              <div className="muted">
                This analyzes older saved transcripts as a one-time historical
                snapshot. It gets saved in history and can later feed cumulative
                intelligence.
              </div>
            </div>
          ) : null}

          {isCumulative ? (
            <>
              <div className={styles.aiCard}>
                <div className={styles.aiCardTitle}>Cumulative Summary</div>
                <div className="muted">
                  {aiSummary?.cumulative_summary ||
                    "Generate cumulative intelligence to see this."}
                </div>
              </div>

              <h3>Repeated Patterns</h3>
              <List items={aiSummary?.repeated_patterns} />

              <h3>Recurring Objections</h3>
              <List items={aiSummary?.recurring_objections} />

              <h3>Improving Signals</h3>
              <List items={aiSummary?.improving_signals} />

              <h3>Warning Signals</h3>
              <List items={aiSummary?.warning_signals} />

              <h3>Best Next Actions</h3>
              <List items={aiSummary?.best_next_actions} />

              <h3>What To Watch Next</h3>
              <List items={aiSummary?.what_to_watch_next} />
            </>
          ) : (
            <>
              <div className={styles.aiCard}>
                <div className={styles.aiCardTitle}>Overall Summary</div>
                <div className="muted">
                  {aiSummary?.overall_summary ||
                    "Generate AI intelligence to see summary."}
                </div>
              </div>

              <h3>Top Learnings</h3>
              {aiSummary?.top_learnings?.length ? (
                aiSummary.top_learnings.map((x, i) => (
                  <div className={styles.aiCard} key={i}>
                    <div className={styles.aiCardTitle}>{x.insight || ""}</div>
                    <div className="muted">{x.why_it_matters || ""}</div>
                    <div className={styles.aiRef}>
                      Supported by leads:{" "}
                      {(x.supporting_lead_ids || []).join(", ") || "-"}
                    </div>
                  </div>
                ))
              ) : (
                <div className="empty-cell">No AI learning generated yet.</div>
              )}

              <h3>Industry Intelligence</h3>
              {aiSummary?.industry_intelligence?.length ? (
                aiSummary.industry_intelligence.map((x, i) => (
                  <div className={styles.aiCard} key={i}>
                    <div className={styles.aiCardTitle}>
                      {x.industry || "Unknown Industry"}
                    </div>
                    <div className="muted">
                      <strong>Thesis:</strong> {x.industry_thesis || ""}
                    </div>
                    <div>
                      <strong>Pain Points</strong>
                      <List items={x.common_pain_points} />
                    </div>
                    <div>
                      <strong>Objections</strong>
                      <List items={x.common_objections} />
                    </div>
                    <div>
                      <strong>Recommendations</strong>
                      <List items={x.recommendations} />
                    </div>
                    <div className={styles.aiRef}>
                      Supported by leads:{" "}
                      {(x.supporting_lead_ids || []).join(", ") || "-"}
                    </div>
                  </div>
                ))
              ) : (
                <div className="empty-cell">
                  No industry intelligence generated yet.
                </div>
              )}

              <h3>Leads To Review</h3>
              <table>
                <thead>
                  <tr>
                    {["Lead ID", "Lead", "Industry", "Reason", "Next Step"].map(
                      (c) => (
                        <th key={c}>{c}</th>
                      ),
                    )}
                  </tr>
                </thead>
                <tbody>
                  {aiSummary?.leads_to_review?.length ? (
                    aiSummary.leads_to_review.map((x, i) => (
                      <tr key={i}>
                        <td>{x.lead_id || "-"}</td>
                        <td>{x.lead_name || "-"}</td>
                        <td>{x.industry || "-"}</td>
                        <td>{x.reason || "-"}</td>
                        <td>{x.recommended_next_step || "-"}</td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan={5} className="empty-cell">
                        No AI review leads yet.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </>
          )}
        </div>

        <div className={styles.panel}>
          <h2>AI Intelligence History</h2>
          <div className={`muted ${styles.historyNote}`}>
            Every AI generation is saved here. This prevents losing prior daily,
            weekly, monthly, and all-history intelligence.
          </div>
          {aiHistoryRuns.length ? (
            aiHistoryRuns.map((run) => {
              const s = run.summary || {};
              return (
                <div className={styles.aiCard} key={run.id}>
                  <div className={styles.aiCardTitle}>
                    {run.source_label || run.timeframe || "AI Run"}
                  </div>
                  <div className="muted">
                    Generated: {formatDateTime(run.created_at)} · Transcripts:{" "}
                    {run.transcript_count || 0} · Run ID: {run.id}
                  </div>
                  <div style={{ marginTop: "10px" }}>
                    {s.overall_summary ||
                      s.cumulative_summary ||
                      "No summary saved."}
                  </div>
                  <div className={styles.aiRef}>
                    Timeframe: {run.timeframe || "-"}
                  </div>
                </div>
              );
            })
          ) : (
            <div className="empty-cell">
              No previous AI intelligence runs yet.
            </div>
          )}
        </div>
      </div>
    </>
  );
}
