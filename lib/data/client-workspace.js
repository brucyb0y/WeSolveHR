// Data behind GET /clients/:id. The loading sequence is copied verbatim from
// the original handler (lib/server/app.js lines 42221-42678); only the
// Express req/res surface became arguments and a return value.

import { supabase } from "../server/supabase.js";
import { DASHBOARD_ORG_ID, INLINE_CLIENT_LEADS_BUSINESSES, getBusinessConfig } from "../server/constants.js";
import { getBusinessCanonicalName, getBusinessLeadsData, resolveLeadSource } from "./leads.js";
import { getClientLeadCategoryTypeCounts } from "./client-leads.js";
import { CLIENT_REPORT_MAX_WEEKS, getClientGoals, getLatestClientReportSummaries } from "./client-reports.js";

async function getClientWorkspaceData({ user, params, query }) {

    const orgId = user?.org_id || DASHBOARD_ORG_ID;
    const clientId = Number(params.id);
    const selectedTab = String(query.tab || "overview");

    if (!clientId) {
      return { __halt: { status: 400, body: "Invalid client id" } };
    }

    // 1) Load main client
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

    const contributorsResult = await supabase
      .from("client_contributors")
      .select("*")
      .eq("client_id", clientId)
      .eq("archived", false)
      .order("created_at", { ascending: false });

    if (clientError) {
      console.error("client workspace lookup error:", clientError);
      return { __halt: { status: 500, body: "Failed to load client" } };
    }

    if (!client) {
      return { __halt: { status: 404, body: "Client not found" } };
    }

    // 2) Load all client workspace related data
    const [
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
      supabase
        .from("client_contacts")
        .select("*")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("is_primary", { ascending: false }),

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

      supabase
        .from("client_updates")
        .select("*")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .order("created_at", { ascending: false })
        .limit(50),

      supabase
        .from("client_actions")
        .select("*")
        .eq("client_id", clientId)
        .eq("archived", false)
        .order("created_at", { ascending: false }),

      supabase
        .from("client_milestones")
        .select("*")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("created_at", { ascending: false }),

      supabase
        .from("client_documents")
        .select("*")
        .eq("client_id", clientId)
        .eq("is_active", true)
        .is("deleted_at", null)
        .order("created_at", { ascending: false }),

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

      // General tasks that name a business — filtered to this client in JS by
      // exact (case-insensitive) match against the client name / company name.
      supabase
        .from("tasks")
        .select(
          "id, task_no, title, business, area, status, priority, progress, deadline, assigned_to_user_id, updated_at, is_client_visible",
        )
        .eq("org_id", orgId)
        .not("business", "is", null)
        .order("deadline", { ascending: true, nullsFirst: false }),

      // Lead status transitions (pipeline stage / outreach / demo) — powers the
      // funnel movement metrics and per-member breakdown in the report tab. Pulled
      // back CLIENT_REPORT_MAX_WEEKS weeks so every weekly view has its movement data.
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

    // 3) Optional error logging
    if (contactsResult.error)
      console.error("contactsResult error:", contactsResult.error);
    if (servicesResult.error)
      console.error("servicesResult error:", servicesResult.error);
    if (workItemsResult.error)
      console.error("workItemsResult error:", workItemsResult.error);
    if (updatesResult.error)
      console.error("updatesResult error:", updatesResult.error);
    if (actionsResult.error)
      console.error("actionsResult error:", actionsResult.error);
    if (milestonesResult.error)
      console.error("milestonesResult error:", milestonesResult.error);
    if (documentsResult.error)
      console.error("documentsResult error:", documentsResult.error);
    if (usersResult.error)
      console.error("usersResult error:", usersResult.error);
    if (activityLogsResult.error)
      console.error("activityLogsResult error:", activityLogsResult.error);
    if (blockersResult.error)
      console.error("blockersResult error:", blockersResult.error);
    if (meetingsResult.error)
      console.error("meetingsResult error:", meetingsResult.error);
    if (campaignsResult.error)
      console.error("campaignsResult error:", campaignsResult.error);
    if (incentivesResult.error)
      console.error("incentivesResult error:", incentivesResult.error);
    if (reportsResult.error)
      console.error("reportsResult error:", reportsResult.error);
    if (linkedTasksResult.error)
      console.error("linkedTasksResult error:", linkedTasksResult.error);
    if (leadStageEventsResult.error)
      console.error("leadStageEventsResult error:", leadStageEventsResult.error);

    // 4) Prepare clean client object for UI
    const decoratedClient = {
      ...client,
      account_manager_name: client.account_manager?.name || "",
      project_manager_name: client.project_manager?.name || "",
    };

    // Tasks whose free-text `business` exactly matches this client's name or
    // company name (case-insensitive) — surfaced read-only in Work Progress.
    const clientNameKeys = new Set(
      [client.name, client.company_name]
        .filter(Boolean)
        .map((s) => String(s).trim().toLowerCase()),
    );
    // Graceful fallback: if the is_client_visible column isn't migrated yet the
    // select above errors (42703) and would blank the whole list — so re-fetch
    // without it. Tasks still show; the client-visibility checkbox just defaults
    // off until the migration runs.
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
      if (retry.error)
        console.error("linkedTasks fallback error:", retry.error);
      linkedTaskRows = retry.data;
    }
    const linkedTasks = (linkedTaskRows || []).filter((t) =>
      clientNameKeys.has(String(t.business || "").trim().toLowerCase()),
    );

    // 5) Clean services array
    const services = (servicesResult.data || [])
      .map((row) => row.services)
      .filter(Boolean);

    // 5b) Leads (and the rich engine) are fetched only for the tabs that need
    // them — Leads, Performance (velocity/inactivity), Incentives (attribution
    // dropdown) — to avoid an extra query on every workspace page load.
    const selectedLeadTab = String(query.leadTab || "all");
    let leads = [];
    let leadAllRows = [];
    let leadFilteredIds = [];
    let leadCounts = {};
    let leadPagination = null;
    const leadSearch = String(query.search || "");
    // Explicit "Assigned To" filter picked from the Leads filter popup — takes
    // precedence over the "My leads only" toggle when set (picking a specific
    // assignee is a stronger signal than the "just show mine" toggle).
    const leadAssigneeFilter = String(query.assignee || "").trim();
    // Same deal for the two per-channel assignee filters: an explicit pick beats
    // the toggle.
    const leadPhoneAssigneeFilter = String(query.phone_assignee || "").trim();
    const leadEmailAssigneeFilter = String(query.email_assignee || "").trim();
    // "My leads only" toggle on the Leads tab — off by default, scoped to the
    // Leads tab so it never narrows the Performance/Incentives/Report tabs'
    // view of the client's full lead set.
    const leadMineOnly =
      selectedTab === "leads" &&
      !leadAssigneeFilter &&
      !leadPhoneAssigneeFilter &&
      !leadEmailAssigneeFilter &&
      String(query.mine || "") === "1";
    const leadMineOnlyName = leadMineOnly
      ? user?.name || user?.email || ""
      : "";
    const leadFilters = {
      pipeline_stage: String(query.pipeline_stage || ""),
      demo_status: String(query.demo_status || ""),
      // Multi-select checkboxes submit repeated category_type keys (an array
      // here); normalize to the comma-separated form used everywhere else.
      category_type: [].concat(query.category_type || []).join(","),
      // The location text input and its "None (no location data)" checkbox
      // share the name, so this can arrive as an array; the checkbox wins.
      location: (() => {
        const vals = []
          .concat(query.location || [])
          .map((s) => String(s).trim())
          .filter(Boolean);
        return vals.includes("__none__") ? "__none__" : vals[0] || "";
      })(),
      assignee: leadAssigneeFilter,
      // "Assign for Phone" / "Assign for Email" — each matched against its own
      // column, independent of the ASSIGNED TO filter.
      phone_assignee: leadPhoneAssigneeFilter,
      email_assignee: leadEmailAssigneeFilter,
      // Multi-select checkboxes submit repeated reached_via keys (an array
      // here); normalize to the comma-separated form used everywhere else.
      reached_via: [].concat(query.reached_via || []).join(","),
      notes: String(query.notes || ""),
      notes_by: String(query.notes_by || ""),
      has_note_audio: String(query.has_note_audio || ""),
      has_phone: String(query.has_phone || ""),
      updated_from: String(query.updated_from || ""),
      updated_to: String(query.updated_to || ""),
      callback_date_from: String(query.callback_date_from || ""),
      callback_date_to: String(query.callback_date_to || ""),
      missed_callback: String(query.missed_callback || ""),
      sort: String(query.sort || ""),
      sort_dir: String(query.sort_dir || ""),
      ...(leadAssigneeFilter ? { assigned_to: leadAssigneeFilter } : {}),
      // "My leads only" is no longer an assigned_to lookup: a lead counts as
      // mine when my name is on it in ANY assignee role (see mine_name in
      // getBusinessLeadsData).
      ...(leadMineOnlyName ? { mine_name: leadMineOnlyName } : {}),
    };
    // If this client's name/company maps to a static lead business (e.g.
    // "Joolian" -> joolian_leads), read that business's leads here so they show
    // up on the client page. Otherwise fall back to the client's own virtual
    // lead business backed by client_leads.
    const matchedLeadBusiness = [client.name, client.company_name]
      .map((s) => getBusinessCanonicalName(s))
      .find((key) => getBusinessConfig(key));
    const clientLeadBusiness = matchedLeadBusiness || `client:${clientId}`;
    // Inline businesses (e.g. Rasset) render the full client-leads UI on the
    // client page (same as Navii) instead of embedding /leads/<business> in an
    // iframe — so only non-inline matches drive the iframe via staticLeadBusiness.
    const embeddedLeadBusiness =
      matchedLeadBusiness &&
      !INLINE_CLIENT_LEADS_BUSINESSES.has(matchedLeadBusiness)
        ? matchedLeadBusiness
        : null;
    let leadCategoryTypeCounts = [];
    if (["leads", "performance", "incentives", "report"].includes(selectedTab)) {
      try {
        const leadsData = await getBusinessLeadsData(
          orgId,
          clientLeadBusiness,
          selectedLeadTab,
          leadSearch,
          Number(query.page) || 1,
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
        const { tableName: categoryTableName, clientId: categoryClientId } =
          resolveLeadSource(getBusinessCanonicalName(clientLeadBusiness));
        leadCategoryTypeCounts = await getClientLeadCategoryTypeCounts(
          orgId,
          categoryTableName,
          categoryClientId,
        );
      } catch (categoryErr) {
        console.error("client lead category type counts error:", categoryErr);
      }
    }

    // Latest AI report summaries + manual Goals (shown on both the Report tab
    // and the Overview tab).
    const needsSummaryAndGoals =
      selectedTab === "report" || selectedTab === "overview";
    const reportSummaries = needsSummaryAndGoals
      ? await getLatestClientReportSummaries(orgId, clientId)
      : { daily: null, weekly: null, weeklyByDate: {} };
    const clientGoals = needsSummaryAndGoals
      ? await getClientGoals(orgId, clientId)
      : null;

    // 6) Render page

  return {
        client: decoratedClient,
        contacts: contactsResult.data || [],
        services,
        workItems: workItemsResult.data || [],
        updates: updatesResult.data || [],
        actions: actionsResult.data || [],
        contributors: contributorsResult.data || [],
        milestones: milestonesResult.data || [],
        documents: documentsResult.data || [],
        users: usersResult.data || [],
        linkedTasks,
        selectedTab,
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

export { getClientWorkspaceData };
