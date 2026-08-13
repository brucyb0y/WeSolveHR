// /clients/:id — the client workspace.
//
// This is the assembly point: it authenticates, loads everything in one pass
// through getClientWorkspaceData, decorates rows for display, and renders the
// tab bar plus the active panel.
//
// WHY ROWS ARE DECORATED HERE rather than in the components:
//   * Formatting (dates, owner names, related-item titles) needs the users and
//     work-item lists that only this scope holds. Passing those lists into every
//     tab so each could re-resolve them would mean the same lookup running
//     dozens of times per render.
//   * Anything time-dependent — "overdue", "days since last lead" — has to be
//     measured against ONE instant. Computed here, every card on the page
//     agrees; computed in client components, the numbers would drift with how
//     long a tab had been open.
//
// WHY EVERY HREF IS A STRING: the interactive tabs are client components, and
// functions cannot cross the server/client boundary. So sort links, pagination
// links, category pills and the "my leads only" toggle are all built here and
// handed down already-resolved.

import { notFound, redirect } from "next/navigation";
import { requireDashboardUser, orgIdFor } from "@/lib/auth";
import {
  getClientWorkspaceData,
  buildLeadFilters,
  buildClientPerformanceMetrics,
  HAS_PHONE_FILTER_OPTIONS,
  NOTES_FILTER_OPTIONS,
  NOTE_AUDIO_FILTER_OPTIONS,
  MISSED_CALLBACK_FILTER_OPTIONS,
  buildReachedViaFilterOptions,
} from "@/lib/data/client-workspace";
import {
  CLIENT_LEAD_PIPELINE_STAGES,
  CLIENT_LEAD_DEMO_STATUSES,
  CLIENT_LEAD_CATEGORY_TYPES,
  CLIENT_LEAD_CATEGORY_TYPE_LABELS,
  REACH_VIA_CHANNELS,
  CAMPAIGN_TYPES,
  CAMPAIGN_STATUSES,
  INCENTIVE_STATUSES,
  clientLeadStatusLabel,
  formatDateTime,
  formatDateTimeNoTz,
  formatDateOnly,
  getDateStringInTimeZone,
  normalizeClientGoalsData,
  buildClientAutoReportSections,
  clientWeeklyReportNumbering,
  CLIENT_REPORT_METRICS,
  APP_TIMEZONE,
} from "@/lib/server/app";

import TopNav from "@/components/TopNav";
import styles from "./workspace.module.css";
import ActivityReport from "@/components/charts/ActivityReport";
import FunnelReport from "@/components/charts/FunnelReport";
import ReportSubviews from "./ReportSubviews";
import ClientViewLinkButton from "./ClientViewLinkButton";
import RegenSummaryButton from "./RegenSummaryButton";
import EditGoalsButton from "./EditGoalsButton";
import {
  SummaryWithGoals,
  ReportSummaryPanel,
  ClientGoalsPanel,
  hasSummaryContent,
} from "@/components/charts/SummaryPanel";

import WorkspaceTabs from "./WorkspaceTabs";
import WorkspaceShell from "./WorkspaceShell";
import OverviewTab from "./OverviewTab";
import DocumentsTab from "./DocumentsTab";
import PerformanceTab from "./PerformanceTab";
import LeadsTab from "./LeadsTab";

export const dynamic = "force-dynamic";

const LEAD_STATUS_HISTORY_PREVIEW = 2;
const FOLLOW_UP_STAGE = "follow_up_required";

// Numeric-ish columns open descending on first click — for counts and dates
// "most" is the useful first answer, not "least".
const NUMERIC_LEAD_SORTS = { notes: true, updated: true };

const first = (v) => (Array.isArray(v) ? v[0] : v);

export default async function ClientWorkspacePage({ params, searchParams }) {
  const { id } = await params;
  const sp = (await searchParams) || {};

  const user = await requireDashboardUser();
  if (!user) redirect("/login");

  const clientId = Number(id);
  if (!clientId) notFound();

  const activeTab = String(first(sp.tab) || "overview");
  const leadFilters = buildLeadFilters(sp, {
    selectedTab: activeTab,
    actingUser: user,
  });

  const data = await getClientWorkspaceData({
    orgId: orgIdFor(user),
    clientId,
    selectedTab: activeTab,
    searchParams: sp,
    actingUser: user,
  });

  if (!data?.client) notFound();

  const {
    client,
    contacts,
    services,
    workItems,
    updates,
    actions,
    contributors,
    milestones,
    documents,
    users,
    activityLogs,
    blockers,
    meetings,
    campaigns,
    incentives,
    reports,
    leads,
    leadAllRows,
    leadFilteredIds,
    leadStatusHistory,
    leadPagination,
    leadSearch,
    leadMineOnly,
    leadCategoryTypeCounts,
    staticLeadBusiness,
    clientGoals,
  } = data;

  // ---- one clock for the whole render -----------------------------------
  const nowMs = Date.now();
  const todayStr = getDateStringInTimeZone(new Date(nowMs), APP_TIMEZONE);

  const userName = (userId) =>
    users.find((u) => String(u.id) === String(userId))?.name || "-";

  const workItemTitle = (workItemId) =>
    workItems.find((w) => String(w.id) === String(workItemId))?.title || "";

  const isOverdue = (w) =>
    w.due_date &&
    String(w.due_date).slice(0, 10) < todayStr &&
    w.status !== "done";

  // ---- lead query string helpers ----------------------------------------
  // Every lead link has to carry the whole current query or it silently drops
  // the user's filters. baseParams is that query minus the one key each link
  // owns.
  const baseParams = (omit = []) => {
    const p = new URLSearchParams();
    p.set("tab", "leads");
    const put = (k, v) => {
      if (v && !omit.includes(k)) p.set(k, String(v));
    };
    put("search", leadSearch);
    for (const [k, v] of Object.entries(leadFilters.filters || {})) {
      if (k === "mine_name") continue;
      put(k, v);
    }
    if (leadMineOnly && !omit.includes("mine")) p.set("mine", "1");
    return p;
  };

  const hrefWith = (extra = {}, omit = []) => {
    const p = baseParams(omit);
    for (const [k, v] of Object.entries(extra)) {
      if (v === null || v === undefined || v === "") p.delete(k);
      else p.set(k, String(v));
    }
    return `/clients/${clientId}?${p.toString()}`;
  };

  const currentSort = leadFilters.filters.sort || "";
  const currentSortDir = leadFilters.filters.sort_dir || "";

  const sortHref = (field) => {
    const dir =
      currentSort === field
        ? currentSortDir === "asc"
          ? "desc"
          : "asc"
        : NUMERIC_LEAD_SORTS[field]
          ? "desc"
          : "asc";
    return hrefWith({ sort: field, sort_dir: dir });
  };

  const sortArrow = (field) =>
    currentSort === field ? (currentSortDir === "asc" ? "asc" : "desc") : "";

  const SORT_FIELDS = ["name", "stage", "demo", "notes", "updated"];
  const sort = {
    hrefs: Object.fromEntries(SORT_FIELDS.map((f) => [f, sortHref(f)])),
    arrows: Object.fromEntries(SORT_FIELDS.map((f) => [f, sortArrow(f)])),
  };

  // Hidden inputs so a search or filter submit preserves everything else.
  const filterHiddenInputs = [];
  for (const [k, v] of Object.entries(leadFilters.filters || {})) {
    if (k === "mine_name" || !v) continue;
    filterHiddenInputs.push({ name: k, value: String(v) });
  }
  if (leadMineOnly) filterHiddenInputs.push({ name: "mine", value: "1" });

  const activeFilterCount = Object.entries(leadFilters.filters || {}).filter(
    ([k, v]) => v && k !== "sort" && k !== "sort_dir" && k !== "mine_name",
  ).length;

  const hasActiveLeadQuery = !!leadSearch || activeFilterCount > 0;

  // A category pill toggles its own key out of the multi-select.
  const categoryPillHref = (key) => {
    const list = (leadFilters.filters.category_type || "")
      .split(",")
      .filter(Boolean);
    const next = key
      ? list.includes(key)
        ? list.filter((k) => k !== key)
        : [...list, key]
      : [];
    return hrefWith({ category_type: next.join(",") || null });
  };

  // ---- decorated rows ----------------------------------------------------
  const decoratedBlockers = blockers.map((b) => ({
    ...b,
    ownerName: userName(b.owner_user_id),
    relatedTitle: b.related_work_item_id
      ? workItemTitle(b.related_work_item_id) || `#${b.related_work_item_id}`
      : "-",
    createdText: b.created_at ? formatDateTime(b.created_at) : "-",
  }));

  const leadLabel = (leadId) => {
    const l = leadAllRows.find((r) => String(r.id) === String(leadId));
    if (!l) return leadId ? `Lead #${leadId}` : "-";
    return l.company || l.business_name || l.contact_name || `Lead #${l.id}`;
  };

  const decoratedIncentives = incentives.map((i) => ({
    ...i,
    gtmName: userName(i.gtm_user_id),
    leadLabel: i.related_lead_id ? leadLabel(i.related_lead_id) : "-",
  }));

  const decoratedMilestones = milestones.map((m) => ({
    ...m,
    updatedText: m.updated_at ? formatDateTime(m.updated_at) : "-",
  }));

  const decoratedReports = reports.map((r) => ({
    ...r,
    createdText: r.created_at ? formatDateTime(r.created_at) : "",
  }));

  // Manual updates and system activity merged into one chronological trail.
  const timelineEvents = [
    ...updates.map((u) => ({
      id: u.id,
      type: "manual_update",
      title: u.title || "Update",
      text: u.update_text || "",
      at: u.created_at,
      by: userName(u.created_by_user_id),
      relatedWorkItemTitle: u.related_work_item_id
        ? workItemTitle(u.related_work_item_id)
        : "",
    })),
    ...activityLogs.map((a) => ({
      id: a.id,
      type: "activity",
      title: a.action || "Activity",
      text: a.details || "",
      at: a.created_at,
      by: userName(a.actor_user_id),
      relatedWorkItemTitle: a.work_item_id ? workItemTitle(a.work_item_id) : "",
    })),
  ]
    .sort((a, b) => String(b.at || "").localeCompare(String(a.at || "")))
    .map((e) => ({ ...e, atText: e.at ? formatDateTime(e.at) : "-" }));

  // ---- team ---------------------------------------------------------------
  const associatedUserIds = new Set(
    [
      client.account_manager_user_id,
      client.project_manager_user_id,
      ...workItems.map((w) => w.owner_user_id),
    ]
      .filter(Boolean)
      .map(String),
  );

  const teamRoleLabel = (u) => {
    const roles = [];
    if (String(u.id) === String(client.account_manager_user_id))
      roles.push("Account Manager");
    if (String(u.id) === String(client.project_manager_user_id))
      roles.push("Project Manager");
    if (!roles.length) roles.push("Contributor");
    return roles.join(" · ");
  };

  const teamMembers = users
    .filter((u) => associatedUserIds.has(String(u.id)))
    .map((u) => {
      const tasks = workItems
        .filter((w) => String(w.owner_user_id) === String(u.id))
        .sort((a, b) => {
          // Open tasks first, then earliest deadline first.
          const ad = a.status === "done" ? 1 : 0;
          const bd = b.status === "done" ? 1 : 0;
          if (ad !== bd) return ad - bd;
          return String(a.due_date || "9999-12-31").localeCompare(
            String(b.due_date || "9999-12-31"),
          );
        });

      const total = tasks.length;
      const doneCount = tasks.filter((t) => t.status === "done").length;
      const inProgressCount = tasks.filter(
        (t) => t.status === "in_progress",
      ).length;
      const overdueCount = tasks.filter(isOverdue).length;
      const avgProgress = total
        ? Math.round(
            tasks.reduce((s, t) => s + (Number(t.progress) || 0), 0) / total,
          )
        : 0;
      const nextDeadline = tasks
        .filter((t) => t.status !== "done" && t.due_date)
        .sort((a, b) =>
          String(a.due_date).localeCompare(String(b.due_date)),
        )[0]?.due_date;

      // Overall state, in priority order: nothing assigned, anything overdue,
      // anything moving, everything finished, otherwise untouched.
      let workState;
      if (total === 0)
        workState = { label: "No tasks", cls: badgeCls("muted") };
      else if (overdueCount)
        workState = { label: "Behind schedule", cls: badgeCls("danger") };
      else if (inProgressCount)
        workState = { label: "Working", cls: badgeCls("info") };
      else if (doneCount === total)
        workState = { label: "All clear", cls: badgeCls("ok") };
      else workState = { label: "Not started", cls: badgeCls("warn") };

      return {
        id: u.id,
        name: u.name || "-",
        roleLabel: teamRoleLabel(u),
        workState,
        total,
        doneCount,
        inProgressCount,
        overdueCount,
        avgProgress,
        nextDeadlineText: nextDeadline ? formatDateOnly(nextDeadline) : "",
        openTaskCount: tasks.filter((t) => t.status !== "done").length,
        tasks: tasks.map((t) => ({
          ...t,
          progressPercent: Math.max(0, Math.min(100, Number(t.progress) || 0)),
          isOverdue: isOverdue(t),
          dueText: t.due_date ? formatDateOnly(t.due_date) : "",
        })),
      };
    });

  // ---- leads --------------------------------------------------------------
  const stageActorName = (userId) => {
    if (!userId) return "Unknown user";
    const name = userName(userId);
    // `users` only holds active members, so a change made by someone since
    // deactivated falls back to a neutral label rather than "-".
    return name && name !== "-" ? name : "Unknown user";
  };

  const statusHistory = Object.fromEntries(
    Object.entries(leadStatusHistory || {}).map(([leadId, rows]) => [
      leadId,
      rows.map((h) => ({
        from: h.from
          ? clientLeadStatusLabel(CLIENT_LEAD_PIPELINE_STAGES, h.from, h.from)
          : "",
        to: clientLeadStatusLabel(CLIENT_LEAD_PIPELINE_STAGES, h.to, h.to),
        by: stageActorName(h.by_user_id),
        at: h.at ? formatDateTimeNoTz(h.at) : "",
      })),
    ]),
  );

  const decoratedLeads = leads.map((l) => {
    const trail = statusHistory[String(l.id)] || [];
    const noteHistory = parseNotes(l.notes);
    const latestNote = noteHistory.length
      ? noteHistory[noteHistory.length - 1]
      : null;

    return {
      ...l,
      company:
        l.company || l.business_name || l.contact_name || `Lead #${l.id}`,
      stage: l.pipeline_stage || "prospect_identified",
      demo: l.demo_status || "not_scheduled",
      locationText: [l.city, l.state, l.country].filter(Boolean).join(", "),
      updatedText: l.updated_at
        ? getDateStringInTimeZone(new Date(l.updated_at), APP_TIMEZONE)
        : "-",
      callbackText: l.callback_date ? formatDateOnly(l.callback_date) : "",
      callMadeTitle: `Call made${l.call_made_by ? ` by ${l.call_made_by}` : ""}${
        l.call_time ? ` · ${formatDateTimeNoTz(l.call_time)}` : ""
      }`,
      stageHistoryPreview: trail.slice(0, LEAD_STATUS_HISTORY_PREVIEW),
      stageHistoryMore: Math.max(0, trail.length - LEAD_STATUS_HISTORY_PREVIEW),
      latestNote,
      latestNoteByline: latestNote
        ? [latestNote.by, latestNote.atText].filter(Boolean).join(" · ")
        : "",
      noteHistoryMore: Math.max(0, noteHistory.length - 1),
    };
  });

  const assigneeOptions = [
    { key: "__unassigned__", label: "Unassigned" },
    ...Array.from(new Set(users.map((u) => u && u.name).filter(Boolean))).map(
      (n) => ({ key: n, label: n }),
    ),
  ];

  const noteAuthorOptions = Array.from(
    new Set(users.map((u) => u && u.name).filter(Boolean)),
  ).map((n) => ({ key: n, label: n }));

  // Revivflow's sheet brings its own columns; other clients do not render them.
  const showOptionalSheetFields = !!client.show_revivflow_lead_fields;

  const performance = buildClientPerformanceMetrics({
    leadAllRows,
    meetings,
    nowMs,
  });

  const counts = {
    openWork: workItems.filter((w) => w.status !== "done").length,
    overdue: workItems.filter(isOverdue).length,
    leads: leadAllRows.length,
    campaigns: campaigns.length,
    openBlockers: blockers.filter((b) => b.resolution_status !== "resolved")
      .length,
    actions: actions.filter((a) => a.status !== "done").length,
  };

  const goals = normalizeClientGoalsData(clientGoals);

  // Data bundle for every modal the shell can open.
  const modalData = {
    users,
    workItems,
    milestones: decoratedMilestones,
    actions,
    contributors,
    blockers: decoratedBlockers,
    campaigns,
    meetings,
    incentives: decoratedIncentives,
    reports: decoratedReports,
    leads: leadAllRows,
    goals,
    campaignTypes: CAMPAIGN_TYPES,
    campaignStatuses: CAMPAIGN_STATUSES,
    incentiveStatuses: INCENTIVE_STATUSES,
  };

  // Summary + goals block. Overview shows the daily one; the Report tab shows
  // a copy per subview (built below, where each week's row is to hand).
  const goalsUpdatedByName = clientGoals?.updated_by_user_id
    ? userName(clientGoals.updated_by_user_id)
    : "";

  const summaryWithGoals = (period, row, extra = {}) => (
    <SummaryWithGoals
      summary={
        <ReportSummaryPanel
          period={period}
          row={row}
          editable
          weekLabel={extra.weekLabel || ""}
          rangeLabel={extra.rangeLabel || ""}
          generatedText={row?.created_at ? formatDateTime(row.created_at) : ""}
          regenButton={
            <RegenSummaryButton
              clientId={clientId}
              period={period}
              weekStart={extra.weekStart || null}
              hasContent={hasSummaryContent(row)}
            />
          }
        />
      }
      goals={
        <ClientGoalsPanel
          clientId={clientId}
          goals={goals}
          editable
          updatedText={
            clientGoals?.updated_at
              ? formatDateTime(clientGoals.updated_at)
              : ""
          }
          updatedByName={goalsUpdatedByName}
          editButton={
            <EditGoalsButton hasGoals={!!(goals.items.length || goals.notes)} />
          }
        />
      }
    />
  );

  // Only the tabs with no callbacks render here; everything interactive is
  // constructed inside WorkspaceShell, which owns the modal state.
  const panel =
    activeTab === "documents" ? (
      <DocumentsTab client={client} documents={documents} />
    ) : activeTab === "performance" ? (
      <PerformanceTab performance={performance} />
    ) : (
      <OverviewTab
        client={client}
        services={services}
        contacts={contacts}
        updates={updates}
        gtmAssociateNames={teamMembers.map((m) => m.name).join(", ")}
        lastActivity={timelineEvents[0]?.atText || "-"}
        summaryAndGoals={summaryWithGoals("daily", data.reportSummaries?.daily)}
      />
    );

  // Task tab: summary chips and the overdue / high-priority alert strip.
  // Both are derived counts, so they are computed here against the one clock
  // rather than recounted inside the client component.
  const highPriorityCount = workItems.filter(
    (w) => w.priority === "high" && w.status !== "done",
  ).length;
  const overdueCount = workItems.filter(isOverdue).length;
  const countStatus = (status) =>
    workItems.filter((w) => w.status === status).length;

  const taskChips = [
    { label: "All", value: workItems.length },
    { label: "Todo", value: countStatus("todo") },
    { label: "In Progress", value: countStatus("in_progress") },
    { label: "Done", value: countStatus("done") },
    { label: "Overdue", value: overdueCount },
    { label: "High priority", value: highPriorityCount },
  ];

  // The strip is absent entirely when nothing is wrong — an empty banner reads
  // as a rendering fault rather than "all clear".
  const taskAlertStrip =
    overdueCount || highPriorityCount ? (
      <div className={styles.alertStrip}>
        {overdueCount ? (
          <span>
            ⚠️ {overdueCount} overdue task{overdueCount === 1 ? "" : "s"}
          </span>
        ) : null}
        {highPriorityCount ? (
          <span>
            🔴 {highPriorityCount} open high-priority task
            {highPriorityCount === 1 ? "" : "s"}
          </span>
        ) : null}
      </div>
    ) : null;

  // Auto-report subviews (daily + one per week). Built only for the Report tab
  // — the aggregation walks every lead stage event, which is wasted work on the
  // other thirteen.
  let reportSubviews = null;
  if (activeTab === "report") {
    const sections = buildClientAutoReportSections({
      leadAllRows,
      campaigns,
      meetings,
      blockers,
      incentives,
      leadStageEvents: data.leadStageEvents || [],
      users,
      weekNumbering: clientWeeklyReportNumbering(client),
    });

    const rd = sections.reportData;

    // Snapshot figures are all-time and identical on every subview, so they are
    // built once and spread into each FunnelReport.
    const snapshot = {
      funnelStages: rd.funnelStages,
      totalLeads: rd.totalLeads,
      convertedNow: rd.convertedNow,
      conversionRate: rd.conversionRate,
    };

    reportSubviews = (
      <ReportSubviews
        daily={{
          summary: summaryWithGoals("daily", data.reportSummaries?.daily),
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
              {...snapshot}
            />
          ),
        }}
        weeks={(sections.weeklyReports || []).map((w) => ({
          num: w.num,
          displayNum: w.displayNum,
          summary: summaryWithGoals(
            "weekly",
            (data.reportSummaries?.weeklyByDate || {})[w.weekStart],
            {
              weekStart: w.weekStart,
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
              {...snapshot}
            />
          ),
        }))}
      />
    );
  }

  // Meeting stats measured against the single render clock.
  const weekAgoMs = nowMs - 7 * 24 * 60 * 60 * 1000;
  const meetingMs = (m) =>
    m.meeting_date
      ? new Date(m.meeting_date).getTime()
      : m.created_at
        ? new Date(m.created_at).getTime()
        : 0;

  const meetingStats = {
    thisWeek: meetings.filter((m) => meetingMs(m) >= weekAgoMs).length,
    // Compliant when at least one SYNC CALL (not any meeting) was logged in
    // the last seven days.
    syncCompliant:
      meetings.filter(
        (m) => m.meeting_type === "sync_call" && meetingMs(m) >= weekAgoMs,
      ).length >= 1,
    nextMeetingDate:
      meetings
        .filter(
          (m) => m.meeting_date && new Date(m.meeting_date).getTime() > nowMs,
        )
        .sort(
          (a, b) =>
            new Date(a.meeting_date).getTime() -
            new Date(b.meeting_date).getTime(),
        )[0]?.meeting_date || null,
  };

  return (
    <>
      <TopNav user={user} active="clients" />
      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Client Workspace internal</div>
            <h1>{client.name}</h1>
            <div className={styles.subtitle}>
              {client.company_name || "-"} · {client.status || "-"} ·{" "}
              {client.health_status || "-"}
            </div>
          </div>

          <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
            <a className={styles.btn} href="/clients">
              ← Clients
            </a>
            {/* Only shown when the client actually has a Drive folder — a dead
                link is worse than an absent one. */}
            {client.google_drive_folder_url ? (
              <a
                className={styles.btn}
                href={client.google_drive_folder_url}
                target="_blank"
                rel="noopener noreferrer"
              >
                Drive
              </a>
            ) : null}
            <a
              className={styles.btn}
              href="https://notebooklm.google.com/notebook/76c66777-16e6-447f-b6a7-d40befa08590"
              target="_blank"
              rel="noopener noreferrer"
            >
              Notebook
            </a>
            <ClientViewLinkButton clientId={clientId} />
            <a
              className={`${styles.btn} ${styles.btnPrimary}`}
              href={`/clients/${clientId}/edit`}
            >
              Edit Client
            </a>
            <a className={styles.btn} href={`/clients/${clientId}/reset`}>
              Reset
            </a>
          </div>
        </div>

        <WorkspaceTabs
          clientId={clientId}
          activeTab={activeTab}
          counts={counts}
        />

        {activeTab === "leads" ? (
          <LeadsTab
            clientId={clientId}
            staticLeadBusiness={staticLeadBusiness}
            leads={decoratedLeads}
            filteredIds={leadFilteredIds}
            totalCount={leadAllRows.length}
            users={users}
            stages={CLIENT_LEAD_PIPELINE_STAGES}
            demoStatuses={CLIENT_LEAD_DEMO_STATUSES}
            categoryTypes={CLIENT_LEAD_CATEGORY_TYPES}
            categoryTypeLabels={CLIENT_LEAD_CATEGORY_TYPE_LABELS}
            categoryCounts={leadCategoryTypeCounts.map((c) => ({
              ...c,
              href: categoryPillHref(c.key),
            }))}
            reachChannels={REACH_VIA_CHANNELS}
            sort={sort}
            pagination={leadPagination}
            paginationHrefs={{
              prev: hrefWith({ page: (leadPagination?.page || 1) - 1 }),
              next: hrefWith({ page: (leadPagination?.page || 1) + 1 }),
            }}
            search={leadSearch}
            hasActiveQuery={hasActiveLeadQuery}
            clearSearchHref={hrefWith({ search: null })}
            mineOnly={leadMineOnly}
            mineOnHref={hrefWith({ mine: 1 })}
            mineOffHref={hrefWith({}, ["mine"])}
            filters={{
              ...leadFilters.filters,
              category_type_list: (leadFilters.filters.category_type || "")
                .split(",")
                .filter(Boolean),
              reached_via_list: (leadFilters.filters.reached_via || "")
                .split(",")
                .filter(Boolean),
            }}
            filterOptions={{
              pipelineStages: CLIENT_LEAD_PIPELINE_STAGES,
              demoStatuses: CLIENT_LEAD_DEMO_STATUSES,
              categoryTypes: CLIENT_LEAD_CATEGORY_TYPES,
              assigneeOptions,
              hasPhoneOptions: HAS_PHONE_FILTER_OPTIONS,
              reachedViaOptions:
                buildReachedViaFilterOptions(REACH_VIA_CHANNELS),
              notesOptions: NOTES_FILTER_OPTIONS,
              noteAudioOptions: NOTE_AUDIO_FILTER_OPTIONS,
              missedCallbackOptions: MISSED_CALLBACK_FILTER_OPTIONS,
              noteAuthorOptions,
            }}
            activeFilterCount={activeFilterCount}
            clearFiltersHref={`/clients/${clientId}?tab=leads`}
            filterHiddenInputs={filterHiddenInputs}
            allCategoryPillHref={categoryPillHref("")}
            todayStr={todayStr}
            statusHistory={statusHistory}
            importCategoryTypeRequired={!showOptionalSheetFields}
            showOptionalSheetFields={showOptionalSheetFields}
          />
        ) : (
          <WorkspaceShell
            clientId={clientId}
            activeTab={activeTab}
            panel={panel}
            data={modalData}
            taskChips={taskChips}
            taskAlertStrip={taskAlertStrip}
            clientName={client.name}
            teamMembers={teamMembers}
            timelineEvents={timelineEvents}
            activityLogs={activityLogs}
            updates={updates}
            meetingStats={meetingStats}
            reportSubviews={reportSubviews}
          />
        )}
      </div>
    </>
  );
}

// Tiny helper so the team work-state badges use the same classes as everything
// else without importing the whole badge module into this scope twice.
function badgeCls(tone) {
  return `${styles.badge} ${
    {
      muted: styles.badgeMuted,
      danger: styles.badgeDanger,
      info: styles.badgeInfo,
      ok: styles.badgeOk,
      warn: styles.badgeWarn,
    }[tone]
  }`;
}

// Notes are append-only JSON, with older rows stored as plain text. Mirrors the
// client-side reader in LeadHistoryModals — the "[" guard matters, see there.
function parseNotes(raw) {
  const normalize = (arr) =>
    arr
      .filter((n) => n && typeof n === "object" && n.text != null)
      .map((n) => ({
        text: n.text || "",
        by: n.by || "",
        audio_url: n.audio_url || "",
        atText: n.at ? formatDateTimeNoTz(n.at) : "",
      }));

  if (Array.isArray(raw)) return normalize(raw);
  if (typeof raw !== "string") return [];

  const t = raw.trim();
  if (!t) return [];

  if (t.charAt(0) === "[") {
    try {
      const arr = JSON.parse(t);
      if (Array.isArray(arr)) return normalize(arr);
    } catch {
      /* falls through to plain text */
    }
  }

  return [{ text: t, by: "", audio_url: "", atText: "" }];
}
