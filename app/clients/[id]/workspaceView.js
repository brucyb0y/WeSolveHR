// Client-workspace view model. Ported verbatim (logic-for-logic) from the
// computation section of renderClientWorkspacePage() in lib/server/app.js — every
// derived metric, rollup, funnel tally, and per-tab stat. Pure & serializable so
// it runs in the Server Component and the client island stays a pure renderer
// (no Date.now() in the client → no hydration drift). Raw HTML generation is NOT
// done here; the React component renders the rows from these structures.

import {
  CLIENT_LEAD_PIPELINE_STAGES,
  CLIENT_LEAD_OUTREACH_STATUSES,
  CLIENT_LEAD_DEMO_STATUSES,
} from "@/lib/server/app.js";
import { formatDateOnly } from "@/lib/utils/datetime.js";

const ACTIVE_TABS = [
  "overview",
  "task",
  "leads",
  "campaigns",
  "meetings",
  "blockers",
  "team",
  "performance",
  "incentives",
  "report",
  "updates",
  "actions",
  "milestones",
  "documents",
];

export function buildClientWorkspaceView(data) {
  const {
    client,
    workItems = [],
    updates = [],
    actions = [],
    contributors = [],
    milestones = [],
    documents = [],
    users = [],
    selectedTab = "overview",
    activityLogs = [],
    blockers = [],
    meetings = [],
    campaigns = [],
    incentives = [],
    reports = [],
    leadAllRows = [],
    leadStageEvents = [],
    leadCounts = {},
  } = data;

  const activeTab = ACTIVE_TABS.includes(selectedTab) ? selectedTab : "overview";

  const getUserName = (userId) =>
    users.find((u) => String(u.id) === String(userId))?.name || "-";
  const getWorkItemTitle = (workItemId) =>
    workItems.find((w) => String(w.id) === String(workItemId))?.title || "";
  const getMilestoneTitle = (milestoneId) =>
    milestones.find((m) => String(m.id) === String(milestoneId))?.title || "";

  // --- Timeline (manual updates + activity log) -----------------------------
  const manualUpdateEvents = updates.map((u) => ({
    type: "manual_update",
    at: u.created_at,
    title: u.title || "Client update",
    text: u.update_text || "",
    by: getUserName(u.created_by_user_id),
    relatedWorkItemTitle: u.related_work_item_id
      ? getWorkItemTitle(u.related_work_item_id)
      : "",
  }));

  const activityEvents = activityLogs.map((log) => {
    const actionLabel = String(log.action || "").replaceAll("_", " ");
    const newValue = log.new_value || {};
    const oldValue = log.old_value || {};
    let text = actionLabel;
    if (log.action === "work_item_created") {
      text = `Created work item: ${newValue.title || "-"}`;
    }
    if (log.action === "work_item_updated") {
      if (oldValue.status !== newValue.status) {
        text = `Status changed: ${oldValue.status || "-"} → ${newValue.status || "-"}`;
      } else {
        text = `Updated work item: ${newValue.title || "-"}`;
      }
    }
    if (log.action === "work_item_archived") {
      text = `Archived work item: ${oldValue.title || newValue.title || "-"}`;
    }
    return {
      type: "activity",
      at: log.created_at,
      title: actionLabel,
      text,
      by: getUserName(log.actor_user_id),
      relatedWorkItemTitle:
        log.entity_type === "client_work_items"
          ? getWorkItemTitle(log.entity_id)
          : "",
    };
  });

  const timelineEvents = [...manualUpdateEvents, ...activityEvents]
    .filter((x) => x.at)
    .sort((a, b) => new Date(b.at).getTime() - new Date(a.at).getTime());

  // --- Work items / tasks ---------------------------------------------------
  const todayStr = new Date().toISOString().slice(0, 10);
  const isOverdue = (w) =>
    w.due_date && String(w.due_date).slice(0, 10) < todayStr && w.status !== "done";
  const overdueCount = workItems.filter(isOverdue).length;
  const highPriorityCount = workItems.filter(
    (w) => w.priority === "high" && w.status !== "done",
  ).length;
  const openBlockerCount = blockers.filter(
    (b) => b.resolution_status !== "resolved",
  ).length;

  // --- Meetings -------------------------------------------------------------
  const nowMs = Date.now();
  const weekAgoMs = nowMs - 7 * 24 * 60 * 60 * 1000;
  const meetingTime = (m) =>
    m.meeting_date
      ? new Date(m.meeting_date).getTime()
      : m.created_at
        ? new Date(m.created_at).getTime()
        : 0;
  const meetingsThisWeek = meetings.filter(
    (m) => meetingTime(m) >= weekAgoMs,
  ).length;
  const syncCallsThisWeek = meetings.filter(
    (m) => m.meeting_type === "sync_call" && meetingTime(m) >= weekAgoMs,
  ).length;
  const momFilled = (m) =>
    !!(
      m.summary ||
      m.discussion_points ||
      m.decisions ||
      m.deliverables ||
      m.action_items ||
      m.follow_ups ||
      m.next_steps
    );
  const momPendingCount = meetings.filter((m) => !momFilled(m)).length;
  const syncCompliant = syncCallsThisWeek >= 1;
  const nextMeeting = meetings
    .filter((m) => m.meeting_date && new Date(m.meeting_date).getTime() > nowMs)
    .sort(
      (a, b) =>
        new Date(a.meeting_date).getTime() - new Date(b.meeting_date).getTime(),
    )[0];
  const nextMeetingDate = nextMeeting ? nextMeeting.meeting_date : null;

  // --- Campaigns ------------------------------------------------------------
  const totalSent = campaigns.reduce((n, c) => n + (Number(c.sent_count) || 0), 0);
  const totalResponses = campaigns.reduce(
    (n, c) => n + (Number(c.response_count) || 0),
    0,
  );
  const totalPositiveReplies = campaigns.reduce(
    (n, c) => n + (Number(c.positive_replies) || 0),
    0,
  );

  // --- Team & employee workload ---------------------------------------------
  const openTaskCountByUser = {};
  workItems.forEach((w) => {
    if (w.status !== "done" && w.owner_user_id) {
      const k = String(w.owner_user_id);
      openTaskCountByUser[k] = (openTaskCountByUser[k] || 0) + 1;
    }
  });
  const associatedUserIds = new Set();
  if (client.account_manager_user_id)
    associatedUserIds.add(String(client.account_manager_user_id));
  if (client.project_manager_user_id)
    associatedUserIds.add(String(client.project_manager_user_id));
  workItems.forEach((w) => {
    if (w.owner_user_id) associatedUserIds.add(String(w.owner_user_id));
  });
  blockers.forEach((b) => {
    if (b.owner_user_id) associatedUserIds.add(String(b.owner_user_id));
  });
  const teamRoleLabel = (u) => {
    const roles = [];
    if (String(u.id) === String(client.account_manager_user_id))
      roles.push("Account Manager");
    if (String(u.id) === String(client.project_manager_user_id))
      roles.push("Project Manager");
    if (!roles.length) roles.push("Contributor");
    return roles.join(" · ");
  };
  const teamMembersRaw = users.filter((u) => associatedUserIds.has(String(u.id)));
  const teamTaskRows = teamMembersRaw.map((u) => ({
    name: u.name || "-",
    role: teamRoleLabel(u),
    count: openTaskCountByUser[String(u.id)] || 0,
  }));

  const dateOnly = (d) => (d ? String(d).slice(0, 10) : "");
  const employeeCards = teamMembersRaw.map((u) => {
    const tasks = workItems
      .filter((w) => String(w.owner_user_id) === String(u.id))
      .sort((a, b) => {
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
    const overdueTasks = tasks.filter(isOverdue);
    const avgProgress = total
      ? Math.round(
          tasks.reduce((s, t) => s + (Number(t.progress) || 0), 0) / total,
        )
      : 0;
    const nextDeadline = tasks
      .filter((t) => t.status !== "done" && t.due_date)
      .sort((a, b) => String(a.due_date).localeCompare(String(b.due_date)))[0]
      ?.due_date;
    let workState;
    if (total === 0) workState = { label: "No tasks", cls: "badge badge-muted" };
    else if (overdueTasks.length)
      workState = { label: "Behind schedule", cls: "badge badge-danger" };
    else if (inProgressCount)
      workState = { label: "Working", cls: "badge badge-info" };
    else if (doneCount === total)
      workState = { label: "All clear", cls: "badge badge-ok" };
    else workState = { label: "Not started", cls: "badge badge-warn" };

    const taskRows = tasks.map((t) => {
      const prog = Math.max(0, Math.min(100, Number(t.progress) || 0));
      const over = isOverdue(t);
      const statusLabel =
        t.status === "done"
          ? "Done"
          : t.status === "in_progress"
            ? "In Progress"
            : "To Do";
      const statusBadgeClass =
        t.status === "done"
          ? "badge badge-ok"
          : t.status === "in_progress"
            ? "badge badge-info"
            : "badge badge-muted";
      return {
        title: t.title || "Untitled",
        priorityLabel: (t.priority || "medium") + " priority",
        statusLabel,
        statusBadgeClass,
        prog,
        over,
        dueDate: t.due_date ? dateOnly(t.due_date) : null,
      };
    });

    return {
      name: u.name || "-",
      role: teamRoleLabel(u),
      total,
      doneCount,
      inProgressCount,
      overdueCount: overdueTasks.length,
      avgProgress,
      nextDeadline: nextDeadline ? dateOnly(nextDeadline) : null,
      workState,
      taskRows,
    };
  });

  // --- Performance ----------------------------------------------------------
  const perfNowMs = Date.now();
  const dayMs = 24 * 60 * 60 * 1000;
  const leadCreatedMs = (l) => (l.created_at ? new Date(l.created_at).getTime() : 0);
  const leadsLast3 = leadAllRows.filter(
    (l) => leadCreatedMs(l) >= perfNowMs - 3 * dayMs,
  ).length;
  const leadsLast7 = leadAllRows.filter(
    (l) => leadCreatedMs(l) >= perfNowMs - 7 * dayMs,
  ).length;
  const convertedCount = leadAllRows.filter(
    (l) => l.pipeline_stage === "converted",
  ).length;
  const lastLeadMs = leadAllRows.reduce(
    (max, l) => Math.max(max, leadCreatedMs(l)),
    0,
  );
  const daysSinceLastLead = lastLeadMs
    ? Math.floor((perfNowMs - lastLeadMs) / dayMs)
    : null;
  const lastMeetingMs = meetings.reduce((max, m) => {
    const t = m.meeting_date
      ? new Date(m.meeting_date).getTime()
      : m.created_at
        ? new Date(m.created_at).getTime()
        : 0;
    return Math.max(max, t);
  }, 0);
  const daysSinceLastMeeting = lastMeetingMs
    ? Math.floor((perfNowMs - lastMeetingMs) / dayMs)
    : null;
  const perfAlerts = [];
  if (daysSinceLastLead === null || daysSinceLastLead >= 3) {
    perfAlerts.push(
      `No new leads in ${daysSinceLastLead === null ? "the recorded period" : daysSinceLastLead + " day" + (daysSinceLastLead === 1 ? "" : "s")} — GTM inactivity`,
    );
  }
  if (leadsLast7 === 0) {
    perfAlerts.push("No weekly progress: 0 leads added in the last 7 days");
  }

  // --- Incentives -----------------------------------------------------------
  const leadLabelById = {};
  leadAllRows.forEach((l) => {
    leadLabelById[String(l.id)] =
      l.company || l.business_name || l.contact_name || `Lead #${l.id}`;
  });
  const totalIncentive = incentives.reduce(
    (n, i) => n + (Number(i.amount) || 0),
    0,
  );
  const incentiveLeadOptions = leadAllRows.map((l) => ({
    id: Number(l.id),
    label: l.company || l.business_name || l.contact_name || `Lead #${l.id}`,
  }));

  // --- Weekly / daily auto report -------------------------------------------
  const weekWindowMs = 7 * 24 * 60 * 60 * 1000;
  const weeklyNowMs = Date.now();
  const weeklyStartMs = weeklyNowMs - weekWindowMs;
  const tsOf = (d) => (d ? new Date(d).getTime() : 0);
  const inThisWeek = (d) => {
    const t = tsOf(d);
    return t > 0 && t >= weeklyStartMs;
  };
  const userIdByName = {};
  users.forEach((u) => {
    if (u.name) userIdByName[String(u.name).trim().toLowerCase()] = String(u.id);
  });

  const buildStatsBucket = (predicate) => {
    const stats = {};
    const ensure = (userId) => {
      const key = userId ? String(userId) : "unattributed";
      if (!stats[key]) {
        stats[key] = {
          campaigns: 0,
          converted: 0,
          meetings: 0,
          moms: 0,
          blockers: 0,
          incentive: 0,
        };
      }
      return stats[key];
    };
    campaigns.forEach((c) => {
      if (predicate(c.created_at)) ensure(c.created_by_user_id).campaigns += 1;
    });
    leadAllRows.forEach((l) => {
      if (l.pipeline_stage === "converted" && predicate(l.updated_at)) {
        const uid =
          userIdByName[String(l.assigned_to || "").trim().toLowerCase()] || null;
        ensure(uid).converted += 1;
      }
    });
    meetings.forEach((m) => {
      const when = m.meeting_date || m.created_at;
      if (predicate(when)) {
        const s = ensure(m.created_by_user_id);
        s.meetings += 1;
        if (momFilled(m)) s.moms += 1;
      }
    });
    blockers.forEach((b) => {
      if (predicate(b.created_at)) ensure(b.owner_user_id).blockers += 1;
    });
    incentives.forEach((i) => {
      if (predicate(i.created_at))
        ensure(i.gtm_user_id).incentive += Number(i.amount) || 0;
    });
    return stats;
  };

  const rollup = (stats) => {
    const totals = Object.values(stats).reduce(
      (acc, s) => {
        acc.campaigns += s.campaigns;
        acc.converted += s.converted;
        acc.meetings += s.meetings;
        acc.moms += s.moms;
        acc.blockers += s.blockers;
        acc.incentive += s.incentive;
        return acc;
      },
      {
        campaigns: 0,
        converted: 0,
        meetings: 0,
        moms: 0,
        blockers: 0,
        incentive: 0,
      },
    );
    const rows = Object.keys(stats)
      .map((key) => ({
        key,
        name: key === "unattributed" ? "Unattributed" : getUserName(key),
        ...stats[key],
      }))
      .filter(
        (r) =>
          r.campaigns ||
          r.converted ||
          r.meetings ||
          r.moms ||
          r.blockers ||
          r.incentive,
      )
      .sort((a, b) => {
        const score = (r) =>
          r.campaigns + r.converted + r.meetings + r.moms + r.blockers;
        return score(b) - score(a);
      });
    return { rows, totals };
  };

  const weekly = rollup(buildStatsBucket(inThisWeek));
  const weeklyRows = weekly.rows;
  const weeklyTotals = weekly.totals;
  const weeklyRangeLabel = `${formatDateOnly(new Date(weeklyStartMs).toISOString().slice(0, 10))} – ${formatDateOnly(new Date(weeklyNowMs).toISOString().slice(0, 10))}`;

  const dailyDateStr = new Date(weeklyNowMs).toISOString().slice(0, 10);
  const dayStartMs = new Date(dailyDateStr + "T00:00:00Z").getTime();
  const inToday = (d) => {
    const t = tsOf(d);
    return t > 0 && t >= dayStartMs;
  };
  const daily = rollup(buildStatsBucket(inToday));
  const dailyRows = daily.rows;
  const dailyTotals = daily.totals;
  const dailyRangeLabel = formatDateOnly(dailyDateStr);

  // --- Lead funnel ----------------------------------------------------------
  const leadsAddedToday = leadAllRows.filter((l) => inToday(l.created_at)).length;
  const leadsAddedWeek = leadAllRows.filter((l) =>
    inThisWeek(l.created_at),
  ).length;

  const funnelSnapshot = {};
  CLIENT_LEAD_PIPELINE_STAGES.forEach((s) => {
    funnelSnapshot[s.key] = 0;
  });
  leadAllRows.forEach((l) => {
    const k = l.pipeline_stage || "prospect_identified";
    funnelSnapshot[k] = (funnelSnapshot[k] || 0) + 1;
  });

  const stageTransToday = {};
  const stageTransWeek = {};
  const outreachToToday = {};
  const outreachToWeek = {};
  const demoToToday = {};
  const demoToWeek = {};
  const transitionKey = (from, to) => `${from || "?"}->${to || "?"}`;
  const bump = (obj, key) => {
    obj[key] = (obj[key] || 0) + 1;
  };
  const memberFunnel = {};
  const ensureMemberFunnel = (key) => {
    if (!memberFunnel[key]) {
      memberFunnel[key] = {
        leadsAdded: 0,
        stageMoves: 0,
        outreachMoves: 0,
        demoMoves: 0,
        converted: 0,
      };
    }
    return memberFunnel[key];
  };

  leadStageEvents.forEach((ev) => {
    const nv = ev.new_value || {};
    const field = nv.field;
    const to = nv.to;
    if (!field || !to) return;
    const today = inToday(ev.created_at);
    const week = inThisWeek(ev.created_at);
    if (!week) return;
    const actorKey = ev.actor_user_id ? String(ev.actor_user_id) : "unattributed";
    if (field === "pipeline_stage") {
      const k = transitionKey(nv.from, to);
      bump(stageTransWeek, k);
      if (today) bump(stageTransToday, k);
      ensureMemberFunnel(actorKey).stageMoves += 1;
    } else if (field === "outreach_status") {
      bump(outreachToWeek, to);
      if (today) bump(outreachToToday, to);
      ensureMemberFunnel(actorKey).outreachMoves += 1;
    } else if (field === "demo_status") {
      bump(demoToWeek, to);
      if (today) bump(demoToToday, to);
      ensureMemberFunnel(actorKey).demoMoves += 1;
    }
  });

  leadAllRows.forEach((l) => {
    const uid =
      userIdByName[String(l.assigned_to || "").trim().toLowerCase()] ||
      "unattributed";
    if (inThisWeek(l.created_at)) ensureMemberFunnel(uid).leadsAdded += 1;
    if (l.pipeline_stage === "converted" && inThisWeek(l.updated_at))
      ensureMemberFunnel(uid).converted += 1;
  });

  const consecutiveTransitions = CLIENT_LEAD_PIPELINE_STAGES.slice(0, -1).map(
    (from, i) => {
      const to = CLIENT_LEAD_PIPELINE_STAGES[i + 1];
      const k = transitionKey(from.key, to.key);
      return {
        label: `${from.label} → ${to.label}`,
        today: stageTransToday[k] || 0,
        week: stageTransWeek[k] || 0,
      };
    },
  );
  const outreachMovementRows = CLIENT_LEAD_OUTREACH_STATUSES.map((s) => ({
    label: `→ ${s.label}`,
    today: outreachToToday[s.key] || 0,
    week: outreachToWeek[s.key] || 0,
  }));
  const demoMovementRows = CLIENT_LEAD_DEMO_STATUSES.map((s) => ({
    label: `→ ${s.label}`,
    today: demoToToday[s.key] || 0,
    week: demoToWeek[s.key] || 0,
  }));

  const sumValues = (obj) => Object.values(obj).reduce((a, b) => a + b, 0);
  const totalMovesToday =
    sumValues(stageTransToday) + sumValues(outreachToToday) + sumValues(demoToToday);
  const totalMovesWeek =
    sumValues(stageTransWeek) + sumValues(outreachToWeek) + sumValues(demoToWeek);

  const memberFunnelRows = Object.keys(memberFunnel)
    .map((key) => ({
      key,
      name: key === "unattributed" ? "Unattributed" : getUserName(key),
      ...memberFunnel[key],
    }))
    .filter(
      (r) =>
        r.leadsAdded ||
        r.stageMoves ||
        r.outreachMoves ||
        r.demoMoves ||
        r.converted,
    )
    .sort((a, b) => {
      const score = (r) =>
        r.leadsAdded + r.stageMoves + r.outreachMoves + r.demoMoves + r.converted;
      return score(b) - score(a);
    });
  const memberFunnelTotals = memberFunnelRows.reduce(
    (acc, r) => {
      acc.leadsAdded += r.leadsAdded;
      acc.stageMoves += r.stageMoves;
      acc.outreachMoves += r.outreachMoves;
      acc.demoMoves += r.demoMoves;
      acc.converted += r.converted;
      return acc;
    },
    { leadsAdded: 0, stageMoves: 0, outreachMoves: 0, demoMoves: 0, converted: 0 },
  );

  // --- Contextual stats row + tab counts ------------------------------------
  const num = (v) => (typeof v === "number" && !Number.isNaN(v) ? v : 0);
  const openWork = workItems.filter((w) => w.status !== "done").length;
  const doneWork = workItems.filter((w) => w.status === "done").length;
  const openActions = actions.filter(
    (a) =>
      !["done", "completed", "resolved"].includes(
        String(a.status || "").toLowerCase(),
      ),
  ).length;
  const doneActions = actions.length - openActions;
  const resolvedBlockers = blockers.filter(
    (b) => b.resolution_status === "resolved",
  ).length;
  const upcomingMeetings = meetings.filter(
    (m) => m.meeting_date && new Date(m.meeting_date).getTime() > nowMs,
  ).length;
  const activeCampaigns = campaigns.filter((c) => c.status === "active").length;
  const completedCampaigns = campaigns.filter(
    (c) => c.status === "completed",
  ).length;
  const doneMilestones = milestones.filter((m) =>
    ["done", "completed"].includes(String(m.status || "").toLowerCase()),
  ).length;

  const statsByTab = {
    task: [
      ["Total Tasks", workItems.length],
      ["Open", openWork],
      ["Done", doneWork],
      ["Overdue", overdueCount],
    ],
    leads: [
      ["Total Leads", num(leadCounts.all)],
      ["Qualified", num(leadCounts.qualified)],
      ["Meetings Completed", num(leadCounts.meeting_completed)],
      ["Converted", num(leadCounts.converted)],
    ],
    campaigns: [
      ["Campaigns", campaigns.length],
      ["Active", activeCampaigns],
      ["Completed", completedCampaigns],
    ],
    meetings: [
      ["Meetings", meetings.length],
      ["Upcoming", upcomingMeetings],
    ],
    blockers: [
      ["Blockers", blockers.length],
      ["Open", openBlockerCount],
      ["Resolved", resolvedBlockers],
    ],
    team: [["Team Members", contributors.length]],
    performance: [
      ["Tasks Done", doneWork],
      ["Open Work", openWork],
      ["Overdue", overdueCount],
    ],
    incentives: [
      ["Records", incentives.length],
      ["Total ₹", totalIncentive],
    ],
    report: [["Weekly Reports", reports.length]],
    actions: [
      ["Total", actions.length],
      ["Open", openActions],
      ["Done", doneActions],
    ],
    milestones: [
      ["Milestones", milestones.length],
      ["Completed", doneMilestones],
    ],
    updates: [["Updates", updates.length]],
    documents: [["Documents", documents.length]],
  };
  const statsCards = statsByTab[activeTab] || null;

  const leadsBadge = typeof leadCounts?.all === "number" ? leadCounts.all : null;

  return {
    activeTab,
    todayStr,
    timelineEvents,
    overdueCount,
    highPriorityCount,
    openBlockerCount,
    openWorkCount: openWork,
    openActionsCount: openActions,
    leadsBadge,
    // meetings
    meetingsThisWeek,
    momPendingCount,
    syncCompliant,
    nextMeetingDate,
    // campaigns
    totalSent,
    totalResponses,
    totalPositiveReplies,
    // team
    teamTaskRows,
    employeeCards,
    // performance
    leadsLast3,
    leadsLast7,
    convertedCount,
    daysSinceLastLead,
    daysSinceLastMeeting,
    perfAlerts,
    // incentives
    leadLabelById,
    totalIncentive,
    incentiveLeadOptions,
    // reports auto
    weeklyRows,
    weeklyTotals,
    weeklyRangeLabel,
    dailyRows,
    dailyTotals,
    dailyRangeLabel,
    // funnel
    leadsAddedToday,
    leadsAddedWeek,
    funnelSnapshot,
    consecutiveTransitions,
    outreachMovementRows,
    demoMovementRows,
    totalMovesToday,
    totalMovesWeek,
    memberFunnelRows,
    memberFunnelTotals,
    // stats + counts
    statsCards,
    // option lists for the lead modal / selects
    stages: {
      pipeline: CLIENT_LEAD_PIPELINE_STAGES,
      outreach: CLIENT_LEAD_OUTREACH_STATUSES,
      demo: CLIENT_LEAD_DEMO_STATUSES,
    },
  };
}
