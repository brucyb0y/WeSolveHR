// Data for /clients/:id — the query body of app.get("/clients/:id").
//
// Returns null when the client does not resolve, which the page turns into a
// 404. Several loads are deliberately conditional on the selected tab; see the
// notes inline — they exist to keep the common page load cheap.

import {
  supabase,
  CLIENT_REPORT_MAX_WEEKS,
  INLINE_CLIENT_LEADS_BUSINESSES,
  getBusinessLeadsData,
  getBusinessCanonicalName,
  getBusinessConfig,
  resolveLeadSource,
  getClientLeadCategoryTypeCounts,
  getClientLeadStatusHistory,
  getLatestClientReportSummaries,
  getClientGoals,
} from "@/lib/server/app.js";

const LEAD_TABS = ["leads", "performance", "incentives", "report"];

const firstOf = (v) => (Array.isArray(v) ? v[0] : v);
const listOf = (v) => (v === undefined ? [] : Array.isArray(v) ? v : [v]);

// The location text input and its "None (no location data)" checkbox share a
// name, so the value can arrive as an array — the checkbox wins.
function locationFilter(raw) {
  const vals = listOf(raw)
    .map((s) => String(s).trim())
    .filter(Boolean);
  return vals.includes("__none__") ? "__none__" : vals[0] || "";
}

export function buildLeadFilters(sp, { selectedTab, actingUser }) {
  const phoneAssignee = String(firstOf(sp?.phone_assignee) || "").trim();
  const emailAssignee = String(firstOf(sp?.email_assignee) || "").trim();

  // "My leads only" is scoped to the Leads tab so it never narrows the
  // Performance / Incentives / Report view of the full lead set. An explicit
  // assignee filter outranks it — picking a person is a stronger signal.
  const mineOnly =
    selectedTab === "leads" &&
    !phoneAssignee &&
    !emailAssignee &&
    String(firstOf(sp?.mine) || "") === "1";

  const mineName = mineOnly
    ? actingUser?.name || actingUser?.email || ""
    : "";

  return {
    mineOnly,
    filters: {
      pipeline_stage: String(firstOf(sp?.pipeline_stage) || ""),
      demo_status: String(firstOf(sp?.demo_status) || ""),
      // Multi-select checkboxes submit repeated keys; normalise to the
      // comma-separated form used everywhere else.
      category_type: listOf(sp?.category_type).join(","),
      location: locationFilter(sp?.location),
      phone_assignee: phoneAssignee,
      email_assignee: emailAssignee,
      reached_via: listOf(sp?.reached_via).join(","),
      notes: String(firstOf(sp?.notes) || ""),
      notes_by: String(firstOf(sp?.notes_by) || ""),
      has_note_audio: String(firstOf(sp?.has_note_audio) || ""),
      has_phone: String(firstOf(sp?.has_phone) || ""),
      updated_from: String(firstOf(sp?.updated_from) || ""),
      updated_to: String(firstOf(sp?.updated_to) || ""),
      callback_date_from: String(firstOf(sp?.callback_date_from) || ""),
      callback_date_to: String(firstOf(sp?.callback_date_to) || ""),
      missed_callback: String(firstOf(sp?.missed_callback) || ""),
      sort: String(firstOf(sp?.sort) || ""),
      sort_dir: String(firstOf(sp?.sort_dir) || ""),
      // A lead counts as "mine" when my name is on it in ANY assignee role —
      // see mine_name in getBusinessLeadsData.
      ...(mineName ? { mine_name: mineName } : {}),
    },
  };
}

export async function getClientWorkspaceData({
  orgId,
  clientId,
  selectedTab,
  searchParams,
  actingUser,
}) {
  const sp = searchParams || {};

  const { data: client, error: clientError } = await supabase
    .from("clients")
    .select(
      `
      *,
      account_manager:users!clients_account_manager_user_id_fkey(name),
      project_manager:users!clients_project_manager_user_id_fkey(name)
    `,
    )
    .eq("org_id", orgId)
    .eq("id", clientId)
    .eq("is_active", true)
    .is("deleted_at", null)
    .maybeSingle();

  if (clientError) {
    console.error("client workspace lookup error:", clientError);
    throw new Error("Failed to load client");
  }
  if (!client) return null;

  const byClient = (table, extra = (q) => q) =>
    extra(supabase.from(table).select("*").eq("client_id", clientId));

  const [
    contributorsResult,
    contactsResult,
    servicesResult,
    workItemsResult,
    updatesResult,
    actionsResult,
    milestonesResult,
    documentsResult,
    usersResult,
    activityLogsResult,
    blockersResult,
    meetingsResult,
    campaignsResult,
    incentivesResult,
    reportsResult,
    linkedTasksResult,
    leadStageEventsResult,
  ] = await Promise.all([
    byClient("client_contributors", (q) =>
      q.eq("archived", false).order("created_at", { ascending: false }),
    ),

    byClient("client_contacts", (q) =>
      q
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("is_primary", { ascending: false }),
    ),

    supabase
      .from("client_services")
      .select("services(name)")
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null),

    supabase
      .from("client_work_items")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("status", { ascending: true })
      .order("priority", { ascending: false })
      .order("due_date", { ascending: true }),

    byClient("client_updates", (q) =>
      q
        .eq("is_active", true)
        .order("created_at", { ascending: false })
        .limit(50),
    ),

    byClient("client_actions", (q) =>
      q.eq("archived", false).order("created_at", { ascending: false }),
    ),

    byClient("client_milestones", (q) =>
      q
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("created_at", { ascending: false }),
    ),

    byClient("client_documents", (q) =>
      q
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("created_at", { ascending: false }),
    ),

    supabase
      .from("users")
      .select("id, name")
      .eq("org_id", orgId)
      .eq("is_active", true)
      .order("name", { ascending: true }),

    supabase
      .from("client_activity_logs")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .order("created_at", { ascending: false })
      .limit(50),

    supabase
      .from("client_blockers")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("resolution_status", { ascending: true })
      .order("priority", { ascending: false })
      .order("created_at", { ascending: false }),

    supabase
      .from("client_meetings")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("meeting_date", { ascending: false })
      .order("created_at", { ascending: false }),

    supabase
      .from("client_campaigns")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }),

    supabase
      .from("client_incentives")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }),

    supabase
      .from("client_weekly_reports")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }),

    // Tasks naming a business — narrowed to this client in JS below.
    supabase
      .from("tasks")
      .select(
        "id, task_no, title, business, area, status, priority, progress, deadline, assigned_to_user_id, updated_at, is_client_visible",
      )
      .eq("org_id", orgId)
      .not("business", "is", null)
      .order("deadline", { ascending: true, nullsFirst: false }),

    // Lead status transitions — powers the report tab's funnel-movement metrics.
    supabase
      .from("client_activity_logs")
      .select("entity_id, actor_user_id, new_value, created_at")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("action", "client_lead_status_changed")
      .gte(
        "created_at",
        new Date(
          Date.now() - CLIENT_REPORT_MAX_WEEKS * 7 * 24 * 60 * 60 * 1000,
        ).toISOString(),
      )
      .order("created_at", { ascending: false }),
  ]);

  // Graceful fallback: if is_client_visible has not been migrated yet the select
  // above errors (42703) and would blank the whole list, so re-fetch without it.
  // Tasks still show; the client-visibility checkbox just defaults off.
  let linkedTaskRows = linkedTasksResult.data;
  if (linkedTasksResult.error) {
    const retry = await supabase
      .from("tasks")
      .select(
        "id, task_no, title, business, area, status, priority, progress, deadline, assigned_to_user_id, updated_at",
      )
      .eq("org_id", orgId)
      .not("business", "is", null)
      .order("deadline", { ascending: true, nullsFirst: false });
    if (retry.error) console.error("linkedTasks fallback error:", retry.error);
    linkedTaskRows = retry.data;
  }

  const clientNameKeys = new Set(
    [client.name, client.company_name]
      .filter(Boolean)
      .map((s) => String(s).trim().toLowerCase()),
  );
  const linkedTasks = (linkedTaskRows || []).filter((t) =>
    clientNameKeys.has(String(t.business || "").trim().toLowerCase()),
  );

  // ---- leads ------------------------------------------------------------
  // If this client's name maps to a static lead business (e.g. "Joolian" ->
  // joolian_leads) read that; otherwise use the client's own virtual business
  // backed by client_leads. Inline businesses render the full client-leads UI
  // here, so only non-inline matches drive the iframe.
  const matchedLeadBusiness = [client.name, client.company_name]
    .map((s) => getBusinessCanonicalName(s))
    .find((key) => getBusinessConfig(key));

  const clientLeadBusiness = matchedLeadBusiness || `client:${clientId}`;
  const embeddedLeadBusiness =
    matchedLeadBusiness &&
    !INLINE_CLIENT_LEADS_BUSINESSES.has(matchedLeadBusiness)
      ? matchedLeadBusiness
      : null;

  const selectedLeadTab = String(firstOf(sp.leadTab) || "all");
  const leadSearch = String(firstOf(sp.search) || "");
  const { mineOnly: leadMineOnly, filters: leadFilters } = buildLeadFilters(sp, {
    selectedTab,
    actingUser,
  });

  let leads = [];
  let leadAllRows = [];
  let leadFilteredIds = [];
  let leadCounts = {};
  let leadPagination = null;
  let leadCategoryTypeCounts = [];
  let leadStatusHistory = {};

  // Only the tabs that need leads pay for the query.
  if (LEAD_TABS.includes(selectedTab)) {
    try {
      const leadsData = await getBusinessLeadsData(
        orgId,
        clientLeadBusiness,
        selectedLeadTab,
        leadSearch,
        Number(firstOf(sp.page)) || 1,
        leadFilters,
      );
      leads = leadsData.rows || [];
      leadAllRows = leadsData.businessRows || [];
      leadFilteredIds = leadsData.filteredIds || [];
      leadCounts = leadsData.counts || {};
      leadPagination = leadsData.pagination || null;
    } catch (leadErr) {
      console.error("client leads load error:", leadErr);
    }
  }

  if (selectedTab === "leads") {
    try {
      const { tableName, clientId: categoryClientId } = resolveLeadSource(
        getBusinessCanonicalName(clientLeadBusiness),
      );
      leadCategoryTypeCounts = await getClientLeadCategoryTypeCounts(
        orgId,
        tableName,
        categoryClientId,
      );
    } catch (categoryErr) {
      console.error("client lead category type counts error:", categoryErr);
    }

    // Scoped to the leads on this page so the query stays small.
    if (leads.length) {
      try {
        leadStatusHistory = await getClientLeadStatusHistory(
          orgId,
          clientId,
          leads.map((l) => l.id),
        );
      } catch (historyErr) {
        console.error("client lead status history load error:", historyErr);
      }
    }
  }

  const needsSummaryAndGoals =
    selectedTab === "report" || selectedTab === "overview";

  const [reportSummaries, clientGoals] = await Promise.all([
    needsSummaryAndGoals
      ? getLatestClientReportSummaries(orgId, clientId)
      : Promise.resolve({ daily: null, weekly: null, weeklyByDate: {} }),
    needsSummaryAndGoals ? getClientGoals(orgId, clientId) : Promise.resolve(null),
  ]);

  return {
    client: {
      ...client,
      account_manager_name: client.account_manager?.name || "",
      project_manager_name: client.project_manager?.name || "",
    },
    contacts: contactsResult.data || [],
    services: (servicesResult.data || []).map((r) => r.services).filter(Boolean),
    workItems: workItemsResult.data || [],
    updates: updatesResult.data || [],
    actions: actionsResult.data || [],
    contributors: contributorsResult.data || [],
    milestones: milestonesResult.data || [],
    documents: documentsResult.data || [],
    users: usersResult.data || [],
    linkedTasks,
    activityLogs: activityLogsResult.data || [],
    blockers: blockersResult.data || [],
    meetings: meetingsResult.data || [],
    campaigns: campaignsResult.data || [],
    incentives: incentivesResult.data || [],
    reports: reportsResult.data || [],
    leads,
    leadAllRows,
    leadFilteredIds,
    leadStageEvents: leadStageEventsResult.data || [],
    leadStatusHistory,
    leadCounts,
    leadPagination,
    selectedLeadTab,
    leadSearch,
    leadFilters,
    leadMineOnly,
    leadCategoryTypeCounts,
    staticLeadBusiness: embeddedLeadBusiness,
    reportSummaries,
    clientGoals,
  };
}
