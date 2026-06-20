// Client workspace data. Ported from the GET /clients/:id handler's data
// assembly in lib/server/app.js (the 16-query Promise.all + linked-task filter +
// optional leads-engine load). Queries are kept identical so the workspace shows
// the same data once its UI is migrated. Returns null when the client is missing.

import { supabase } from "@/lib/db/supabase.js";
import {
  getBusinessLeadsData,
  getBusinessCanonicalName,
  getBusinessConfig,
} from "@/lib/services/leads.js";

export async function getClientWorkspaceData({
  orgId,
  clientId,
  selectedTab = "overview",
  selectedLeadTab = "all",
  leadSearch = "",
  leadPage = 1,
}) {
  // 1) Main client
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
    throw clientError;
  }
  if (!client) return null;

  const contributorsResult = await supabase
    .from("client_contributors")
    .select("*")
    .eq("client_id", clientId)
    .eq("archived", false)
    .order("created_at", { ascending: false });

  // 2) Workspace-related datasets
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

    supabase
      .from("tasks")
      .select(
        "id, task_no, title, business, area, status, priority, progress, deadline, assigned_to_user_id, updated_at, is_client_visible",
      )
      .eq("org_id", orgId)
      .not("business", "is", null)
      .order("deadline", { ascending: true, nullsFirst: false }),

    supabase
      .from("client_activity_logs")
      .select("entity_id, actor_user_id, new_value, created_at")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("action", "client_lead_status_changed")
      .gte("created_at", new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString())
      .order("created_at", { ascending: false }),
  ]);

  const decoratedClient = {
    ...client,
    account_manager_name: client.account_manager?.name || "",
    project_manager_name: client.project_manager?.name || "",
  };

  // Linked tasks: general tasks whose free-text `business` matches this client's
  // name/company (case-insensitive), with the is_client_visible fallback.
  const clientNameKeys = new Set(
    [client.name, client.company_name]
      .filter(Boolean)
      .map((s) => String(s).trim().toLowerCase()),
  );
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
  const linkedTasks = (linkedTaskRows || []).filter((t) =>
    clientNameKeys.has(String(t.business || "").trim().toLowerCase()),
  );

  const services = (servicesResult.data || [])
    .map((row) => row.services)
    .filter(Boolean);

  // Leads engine — only loaded for the tabs that use it.
  let leads = [];
  let leadAllRows = [];
  let leadCounts = {};
  let leadPagination = null;
  const matchedLeadBusiness = [client.name, client.company_name]
    .map((s) => getBusinessCanonicalName(s))
    .find((key) => getBusinessConfig(key));
  const clientLeadBusiness = matchedLeadBusiness || `client:${clientId}`;
  if (["leads", "performance", "incentives", "report"].includes(selectedTab)) {
    try {
      const leadsData = await getBusinessLeadsData(
        orgId,
        clientLeadBusiness,
        selectedLeadTab,
        leadSearch,
        Number(leadPage) || 1,
        {},
      );
      leads = leadsData.rows || [];
      leadAllRows = leadsData.businessRows || [];
      leadCounts = leadsData.counts || {};
      leadPagination = leadsData.pagination || null;
    } catch (leadErr) {
      console.error("client leads load error:", leadErr);
    }
  }

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
    leadStageEvents: leadStageEventsResult.data || [],
    leadCounts,
    leadPagination,
    selectedLeadTab,
    leadSearch,
    staticLeadBusiness: matchedLeadBusiness || null,
  };
}
