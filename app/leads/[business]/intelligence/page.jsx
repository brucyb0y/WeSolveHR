// Lead Intelligence (Server Component). Replaces GET /leads/:business/intelligence
// + renderBusinessLeadIntelligencePage(). Stats, industry/employee tables,
// transcripts and the saved AI-run summaries render on the server; the
// generate/refresh buttons are a small client island (AiActions). The original's
// `renderList` TDZ bug for the cumulative view is naturally avoided here.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import {
  getBusinessCanonicalName,
  getBusinessLeadIntelligenceData,
} from "@/lib/services/leads.js";
import { formatDateTime } from "@/lib/utils/datetime.js";
import AiActions from "./AiActions.jsx";
import styles from "./intelligence.module.css";

export const metadata = { title: "Lead Intelligence | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const TIMEFRAMES = [
  { key: "today", label: "Today" },
  { key: "yesterday", label: "Yesterday" },
  { key: "this_week", label: "This Week" },
  { key: "this_month", label: "This Month" },
  { key: "all_history", label: "All Past Transcripts" },
  { key: "cumulative", label: "Cumulative" },
];
const ALLOWED_TIMEFRAMES = TIMEFRAMES.map((t) => t.key);

function List({ items }) {
  return Array.isArray(items) && items.length ? (
    <ul>
      {items.map((x, i) => (
        <li key={i}>{x}</li>
      ))}
    </ul>
  ) : (
    <div className="muted">None found yet.</div>
  );
}

export default async function LeadIntelligencePage({ params, searchParams }) {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const { business: rawBusiness } = await params;
  const sp = await searchParams;
  const business = getBusinessCanonicalName(rawBusiness);
  const timeframe = ALLOWED_TIMEFRAMES.includes(sp.timeframe)
    ? sp.timeframe
    : "today";

  const { metrics, aiRun, aiHistoryRuns } = await getBusinessLeadIntelligenceData(
    {
      orgId: user.org_id || DASHBOARD_ORG_ID,
      business,
      timeframe,
    },
  );

  const aiSummary = aiRun?.summary || null;
  const lastGeneratedText = aiRun
    ? `Last generated: ${formatDateTime(aiRun.created_at)}`
    : "Not generated yet.";

  return (
    <>
      <TopNav active="leads" />
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
          {TIMEFRAMES.map((t) => (
            <a
              key={t.key}
              className={`${styles.filterChip} ${timeframe === t.key ? styles.active : ""}`}
              href={`/leads/${business}/intelligence?timeframe=${t.key}`}
            >
              {t.label}
            </a>
          ))}
        </div>

        <div className={styles.stats}>
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Leads</div>
            <div className={styles.statValue}>{metrics.total_leads}</div>
          </div>
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Calls Uploaded</div>
            <div className={styles.statValue}>{metrics.calls_uploaded}</div>
          </div>
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Transcripts</div>
            <div className={styles.statValue}>{metrics.calls_with_transcript}</div>
          </div>
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Qualified</div>
            <div className={styles.statValue}>{metrics.qualified}</div>
          </div>
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Worth Talking</div>
            <div className={styles.statValue}>{metrics.worth_talking}</div>
          </div>
          <div className={styles.statCard}>
            <div className={styles.statLabel}>In Progress</div>
            <div className={styles.statValue}>{metrics.in_progress}</div>
          </div>
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Completed</div>
            <div className={styles.statValue}>{metrics.completed}</div>
          </div>
        </div>

        <div className={styles.grid2}>
          <div className={styles.panel}>
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
                {metrics.industryRows.length ? (
                  metrics.industryRows.map((x, i) => (
                    <tr key={i}>
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
                  <th>Person</th>
                  <th>Leads</th>
                  <th>Transcripts</th>
                  <th>Qualified</th>
                  <th>Worth Talking</th>
                  <th>Completed</th>
                </tr>
              </thead>
              <tbody>
                {metrics.employeeRows.length ? (
                  metrics.employeeRows.map((x, i) => (
                    <tr key={i}>
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
              <div className={styles.transcriptCard} key={i}>
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
            <div className="empty-cell">No transcripts found for this timeframe.</div>
          )}
        </div>

        <div className={styles.panel}>
          <h2>AI Intelligence Layer</h2>

          <AiActions
            business={business}
            timeframe={timeframe}
            lastGeneratedText={lastGeneratedText}
          />

          {timeframe === "all_history" ? (
            <div className={styles.aiCard}>
              <div className={styles.aiCardTitle}>All Past Transcripts Snapshot</div>
              <div className="muted">
                This analyzes older saved transcripts as a one-time historical
                snapshot. It gets saved in history and can later feed cumulative
                intelligence.
              </div>
            </div>
          ) : null}

          {timeframe === "cumulative" ? (
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
                    <th>Lead ID</th>
                    <th>Lead</th>
                    <th>Industry</th>
                    <th>Reason</th>
                    <th>Next Step</th>
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
          <div className="muted" style={{ marginBottom: 12 }}>
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
                  <div style={{ marginTop: 10 }}>
                    {s.overall_summary || s.cumulative_summary || "No summary saved."}
                  </div>
                  <div className={styles.aiRef}>Timeframe: {run.timeframe || "-"}</div>
                </div>
              );
            })
          ) : (
            <div className="empty-cell">No previous AI intelligence runs yet.</div>
          )}
        </div>
      </div>
    </>
  );
}
