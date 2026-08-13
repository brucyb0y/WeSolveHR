// /client-view/:token — replaces renderClientViewOnlyPage() + its route.
//
// This is the customer-facing view. It renders NO navigation (the original
// never called renderTopNav here) and no auth beyond the token itself: the
// lookup in lib/data/client-view.js requires client_view_enabled, is_active and
// a null deleted_at, so a revoked link stops resolving and 404s.
//
// The client-visible filters in that loader are the security boundary — see the
// note there before touching them.

import { notFound } from "next/navigation";
import {
  formatDateTime,
  getTodayDateStringInTimeZone,
  APP_TIMEZONE,
  clientLeadStatusLabel,
  CLIENT_LEAD_PIPELINE_STAGES,
  CLIENT_LEAD_DEMO_STATUSES,
  buildClientAutoReportSections,
  CLIENT_REPORT_METRICS,
  normalizeClientGoalsData,
  clientWeeklyReportNumbering,
  renderSummaryWithGoals,
} from "@/lib/server/app.js";
import { getClientViewData } from "@/lib/data/client-view";
import {
  buildLeadMetrics,
  buildTeamMembers,
} from "@/lib/data/client-view-metrics";
import {
  decorateLeads,
  buildLeadFilterOptions,
  buildCategoryCounts,
  DEFAULTS,
} from "@/lib/data/client-view-leads";
import ClientViewTabs from "./ClientViewTabs";
import LeadsTab from "./LeadsTab";
import ReportTab from "./ReportTab";
import ActivityReport from "@/components/charts/ActivityReport";
import FunnelReport from "@/components/charts/FunnelReport";
import {
  SummaryWithGoals,
  ReportSummaryPanel,
  ClientGoalsPanel,
} from "@/components/charts/SummaryPanel";
import {
  ClientViewHeader,
  ClientViewStats,
  OverviewTab,
  LeadFunnel,
} from "./OverviewPanels";
import { HighlightedCalls, CampaignsTab, MeetingsTab } from "./TablePanels";
import { BlockersTab, WorkTab, WeeklyReportsList } from "./WorkPanels";
import styles from "./client-view.module.css";

export const dynamic = "force-dynamic";

const MEETING_LABELS = {
  demo: "Demo",
  strategy: "Strategy Call",
  review: "Review",
  onboarding: "Onboarding",
};

const CAMPAIGN_LABELS = {
  email: "Email",
  linkedin: "LinkedIn",
  cold_call: "Cold Call",
  whatsapp: "WhatsApp",
  ads: "Ads",
};

const label = (map, key) =>
  map[String(key || "").toLowerCase()] ||
  String(key || "-").replaceAll("_", " ");

export async function generateMetadata({ params }) {
  const { token } = await params;
  const data = await getClientViewData(token);
  return {
    title: data
      ? `${data.client.name || "Client"} — Project View`
      : "Client view",
  };
}

export default async function ClientViewPage({ params }) {
  const { token } = await params;
  const data = await getClientViewData(token);

  if (!data) notFound();

  const {
    client,
    services,
    workItems,
    actions,
    documents,
    leads,
    campaigns,
    meetings,
    blockers,
    reports,
    contributors,
    users,
    linkedTasks,
    leadAllRows,
    incentives,
    leadStageEvents,
    reportSummaries,
    clientGoals,
  } = data;

  const metrics = buildLeadMetrics(leads);
  const teamMembers = buildTeamMembers({ client, users, contributors });

  const openWorkItems = workItems.filter((w) => w.status !== "done");
  const clientActions = actions.filter((a) => a.owner_type === "Client");

  // ---- leads ------------------------------------------------------------
  const decoratedLeads = decorateLeads(leads);
  const leadOptions = buildLeadFilterOptions(decoratedLeads);
  const categoryCounts = buildCategoryCounts(decoratedLeads);
  const todayStr = getTodayDateStringInTimeZone(APP_TIMEZONE);

  const stageLabelOf = (l) =>
    clientLeadStatusLabel(
      CLIENT_LEAD_PIPELINE_STAGES,
      l.pipeline_stage || "prospect_identified",
      "Prospect Identified",
    );

  const highlightedCalls = leads
    .filter((l) => l.is_starred && l.call_recording_url)
    .map((l) => ({ ...l, stageLabel: stageLabelOf(l) }));

  const demoLeads = leads
    .filter((l) => l.demo_status && l.demo_status !== "not_scheduled")
    .map((l) => ({
      ...l,
      stageLabel: stageLabelOf(l),
      demoLabel: clientLeadStatusLabel(
        CLIENT_LEAD_DEMO_STATUSES,
        l.demo_status,
        "Not Scheduled",
      ),
    }));

  // ---- work grouped by owner -------------------------------------------
  const userNameById = Object.fromEntries(
    users.map((u) => [String(u.id), u.name || ""]),
  );
  const workByOwner = {};
  workItems.forEach((w) => {
    const key = w.owner_user_id ? String(w.owner_user_id) : "unassigned";
    (workByOwner[key] ||= []).push(w);
  });
  const workOwnerGroups = Object.entries(workByOwner).map(([key, items]) => {
    const done = items.filter((w) => w.status === "done").length;
    return {
      name: userNameById[key] || "Unassigned",
      items,
      done,
      total: items.length,
      pct: items.length ? Math.round((done / items.length) * 100) : 0,
    };
  });

  // ---- report sections (server-built HTML — see ReportTab.jsx) -----------
  const sections = buildClientAutoReportSections({
    leadAllRows,
    campaigns,
    meetings,
    blockers,
    incentives,
    leadStageEvents,
    users,
    weekNumbering: clientWeeklyReportNumbering(client),
  });

  // Summary + goals, read-only: editable={false} means no regenerate button
  // and no edit-goals control ever reaches the customer.
  const goals = normalizeClientGoalsData(clientGoals);
  const summaryPanel = (period, summaryRow, extra = {}) => (
    <SummaryWithGoals
      summary={
        <ReportSummaryPanel
          period={period}
          row={summaryRow}
          editable={false}
          weekLabel={extra.weekLabel || ""}
          rangeLabel={extra.rangeLabel || ""}
          generatedText={
            summaryRow?.created_at ? formatDateTime(summaryRow.created_at) : ""
          }
        />
      }
      goals={
        <ClientGoalsPanel
          clientId={client.id}
          goals={goals}
          editable={false}
          updatedText={
            clientGoals?.updated_at
              ? formatDateTime(clientGoals.updated_at)
              : ""
          }
        />
      }
    />
  );

  // Charts render through the shared kit; only the AI summary + goals panel is
  // still a server-built HTML string.
  const rd = sections.reportData;

  const funnelSnapshot = {
    funnelStages: rd.funnelStages,
    totalLeads: rd.totalLeads,
    convertedNow: rd.convertedNow,
    conversionRate: rd.conversionRate,
  };

  const daily = {
    summary: summaryPanel("daily", reportSummaries.daily),
    activity: (
      <ActivityReport
        title="Today's Report"
        eyebrow="Daily snapshot"
        rangeLabel={rd.dailyRangeLabel}
        totals={rd.dailyAgg.totals}
        rows={rd.dailyAgg.rows}
        periodWord="today"
        live
        metrics={CLIENT_REPORT_METRICS}
      />
    ),
    funnel: (
      <FunnelReport
        colLabel="Today"
        rangeLabel={rd.dailyRangeLabel}
        memberPeriodLabel="today"
        agg={rd.dailyAgg}
        {...funnelSnapshot}
      />
    ),
  };

  const weeks = (sections.weeklyReports || []).map((w) => ({
    num: w.num,
    displayNum: w.displayNum,
    summary: summaryPanel(
      "weekly",
      (reportSummaries.weeklyByDate || {})[w.weekStart],
      {
        weekLabel: `Week ${w.displayNum}`,
        rangeLabel: w.rangeLabel,
      },
    ),
    activity: (
      <ActivityReport
        title={`Week ${w.displayNum} Report`}
        eyebrow="Mon–Sat snapshot"
        rangeLabel={w.rangeLabel}
        totals={w.agg.totals}
        rows={w.agg.rows}
        periodWord={`in week ${w.displayNum}`}
        live={false}
        metrics={CLIENT_REPORT_METRICS}
      />
    ),
    funnel: (
      <FunnelReport
        colLabel={`Week ${w.displayNum}`}
        rangeLabel={w.rangeLabel}
        memberPeriodLabel={`in week ${w.displayNum}`}
        agg={w.agg}
        {...funnelSnapshot}
      />
    ),
  }));

  const totals = {
    sent: campaigns.reduce((n, c) => n + (Number(c.sent_count) || 0), 0),
    responses: campaigns.reduce(
      (n, c) => n + (Number(c.response_count) || 0),
      0,
    ),
    positiveReplies: campaigns.reduce(
      (n, c) => n + (Number(c.positive_replies) || 0),
      0,
    ),
  };

  const tabs = [
    {
      key: "overview",
      label: "Overview",
      content: (
        <OverviewTab
          client={client}
          services={services}
          teamMembers={teamMembers}
        />
      ),
    },
    {
      key: "leads",
      label: "Leads",
      content: (
        <>
          <LeadFunnel metrics={metrics} />
          <LeadsTab
            leads={decoratedLeads}
            options={leadOptions}
            categoryCounts={categoryCounts}
            defaults={DEFAULTS}
            todayStr={todayStr}
          />
          <HighlightedCalls rows={highlightedCalls} />
        </>
      ),
    },
    {
      key: "work",
      label: "Tasks",
      content: (
        <WorkTab linkedTasks={linkedTasks} workOwnerGroups={workOwnerGroups} />
      ),
    },
    {
      key: "campaigns",
      label: "Campaigns",
      content: (
        <CampaignsTab
          campaigns={campaigns.map((c) => ({
            ...c,
            typeLabel: label(CAMPAIGN_LABELS, c.campaign_type),
          }))}
          totals={totals}
        />
      ),
    },
    {
      key: "meetings",
      label: "Demos & Meetings",
      content: (
        <MeetingsTab
          demoLeads={demoLeads}
          meetings={meetings.map((m) => ({
            ...m,
            typeLabel: label(MEETING_LABELS, m.meeting_type),
          }))}
        />
      ),
    },
    {
      key: "blockers",
      label: "Blockers",
      content: (
        <BlockersTab
          blockers={blockers.map((b) => ({
            ...b,
            createdAtText: b.created_at ? formatDateTime(b.created_at) : "-",
          }))}
        />
      ),
    },
    {
      key: "reports",
      label: "Report",
      content: (
        <>
          <ReportTab daily={daily} weeks={weeks} />
          <WeeklyReportsList
            reports={reports.map((r) => ({
              ...r,
              createdAtText: r.created_at ? formatDateTime(r.created_at) : "",
            }))}
          />
        </>
      ),
    },
    {
      key: "actions",
      label: "Actions Needed",
      content: (
        <div className={styles.panel}>
          <div className={styles.panelHead}>
            <h2>Actions Needed From You</h2>
          </div>
          <div className={styles.tableWrap}>
            <table>
              <thead>
                <tr>
                  <th>Action</th>
                  <th>Priority</th>
                  <th>Status</th>
                  <th>Due</th>
                </tr>
              </thead>
              <tbody>
                {clientActions.length ? (
                  clientActions.map((a) => (
                    <tr key={a.id}>
                      <td>
                        <strong>{a.title || "Action"}</strong>
                        {a.notes ? (
                          <div className={styles.meta}>{a.notes}</div>
                        ) : null}
                      </td>
                      <td>{a.priority || "medium"}</td>
                      <td>
                        <span className={styles.badge}>
                          {String(a.status || "open").replaceAll("_", " ")}
                        </span>
                      </td>
                      <td>{a.due_date || "-"}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={4} className={styles.meta}>
                      Nothing needed from you right now. 🎉
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      ),
    },
    {
      key: "documents",
      label: "Documents",
      content: (
        <div className={styles.panel}>
          <div className={styles.panelHead}>
            <h2>Documents</h2>
          </div>
          <div className={styles.tableWrap}>
            <table>
              <thead>
                <tr>
                  <th>Document</th>
                  <th>Link</th>
                </tr>
              </thead>
              <tbody>
                {documents.length ? (
                  documents.map((d) => (
                    <tr key={d.id}>
                      <td>
                        <strong>{d.title || d.name || "Document"}</strong>
                      </td>
                      <td>
                        {d.url ? (
                          <a
                            href={d.url}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            Open
                          </a>
                        ) : (
                          <span className={styles.meta}>—</span>
                        )}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={2} className={styles.meta}>
                      No documents shared yet.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      ),
    },
  ];

  return (
    <div className={styles.page}>
      <div className={styles.wrap}>
        <ClientViewHeader client={client} />
        <ClientViewStats
          totalLeads={metrics.totalLeads}
          qualifiedLeads={metrics.qualifiedLeads}
          openWorkCount={openWorkItems.length}
          actionCount={clientActions.length}
        />
        <ClientViewTabs tabs={tabs} />
      </div>
    </div>
  );
}
