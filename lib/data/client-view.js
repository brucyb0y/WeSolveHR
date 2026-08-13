// Data for /client-view/:token — the query body of app.get("/client-view/:token").
//
// The token itself is the credential: the lookup requires client_view_enabled,
// is_active and a null deleted_at, so a revoked or disabled link stops
// resolving. Returns null when nothing matches, which the page turns into a 404.
//
// Everything here is scoped to what the CLIENT is allowed to see, and those
// filters are load-bearing — they are the difference between a customer-facing
// page and an internal one:
//   * client_updates      -> is_client_visible = true
//   * client_actions      -> owner_type = "Client" (their to-dos, not ours)
//   * client_blockers     -> blocker_side = "client_side" (internal blockers stay hidden)
//   * client_weekly_reports -> is_published = true AND is_client_visible != false
//   * tasks               -> is_client_visible = true, then narrowed by business name

import {
  supabase,
  CLIENT_LEADS_TABLE,
  REACH_VIA_CHANNELS,
  CLIENT_REPORT_MAX_WEEKS,
  getLatestClientReportSummaries,
  getClientGoals,
} from "@/lib/server/app.js";

const LEAD_PAGE = 1000;

// PostgREST caps a plain .select() at 1000 rows while clients hold thousands of
// leads. Without paging, the funnel numbers and the table silently stopped at
// 1000 and drifted away from the internal Leads tab.
async function fetchAllClientLeadRows(columns, orgId, clientId) {
  const rows = [];

  for (let from = 0; ; from += LEAD_PAGE) {
    const { data: page, error } = await supabase
      .from(CLIENT_LEADS_TABLE)
      .select(columns)
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .or("is_deleted.is.null,is_deleted.eq.false")
      .order("updated_at", { ascending: false })
      .order("id", { ascending: false })
      .range(from, from + LEAD_PAGE - 1);

    if (error) throw error;
    rows.push(...(page || []));
    if (!page || page.length < LEAD_PAGE) break;
  }

  return rows;
}

export async function getClientViewData(token) {
  const safeToken = String(token || "").trim();
  if (!safeToken) return null;

  const { data: clientRaw, error: clientError } = await supabase
    .from("clients")
    .select(
      `
      *,
      account_manager:users!clients_account_manager_user_id_fkey(name),
      project_manager:users!clients_project_manager_user_id_fkey(name)
    `,
    )
    .eq("client_view_token", safeToken)
    .eq("client_view_enabled", true)
    .eq("is_active", true)
    .is("deleted_at", null)
    .maybeSingle();

  if (clientError) {
    console.error("client view lookup error:", clientError);
    throw new Error("Failed to load client view");
  }

  if (!clientRaw) return null;

  const client = {
    ...clientRaw,
    account_manager_name: clientRaw.account_manager?.name || "",
    project_manager_name: clientRaw.project_manager?.name || "",
  };

  const clientId = client.id;
  const orgId = client.org_id;

  const leadColumns = `id, company, business_name, contact_name, phone, email, pipeline_stage, outreach_status, demo_status, assigned_to, lead_source, notes, created_at, updated_at, is_starred, call_recording_url, city, state, country, category_type, callback_date, ${REACH_VIA_CHANNELS.map((c) => c.column).join(", ")}`;

  const [
    servicesResult,
    workItemsResult,
    updatesResult,
    actionsResult,
    documentsResult,
    leads,
    campaignsResult,
    meetingsResult,
    blockersResult,
    reportsResult,
    contributorsResult,
    usersResult,
    linkedTasksResult,
    leadAllRows,
    incentivesResult,
    leadStageEventsResult,
  ] = await Promise.all([
    supabase
      .from("client_services")
      .select("services(name)")
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null),

    supabase
      .from("client_work_items")
      .select(
        "id, title, description, status, priority, due_date, updated_at, owner_user_id",
      )
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("status", { ascending: true })
      .order("due_date", { ascending: true }),

    supabase
      .from("client_updates")
      .select("id, title, update_text, update_type, created_at")
      .eq("client_id", clientId)
      .eq("is_active", true)
      .eq("is_client_visible", true)
      .order("created_at", { ascending: false })
      .limit(20),

    supabase
      .from("client_actions")
      .select(
        "id, title, owner_type, owner_name, due_date, status, priority, notes",
      )
      .eq("client_id", clientId)
      .eq("owner_type", "Client")
      .eq("archived", false)
      .order("due_date", { ascending: true, nullsFirst: false })
      .order("created_at", { ascending: false }),

    supabase
      .from("client_documents")
      .select("id, title, name, url")
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }),

    // Every lead reaches the external dashboard (view-only), mirroring the
    // internal Leads tab exactly — same rows, same funnel numbers.
    fetchAllClientLeadRows(leadColumns, orgId, clientId),

    supabase
      .from("client_campaigns")
      .select(
        "id, name, campaign_type, channel, status, sent_count, response_count, positive_replies, notes, created_at",
      )
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }),

    supabase
      .from("client_meetings")
      .select(
        "id, title, meeting_date, meeting_type, participants, summary, action_items, next_steps, decisions",
      )
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("meeting_date", { ascending: false })
      .order("created_at", { ascending: false }),

    // Client-side blockers only: dependencies awaited from the client's side
    // (approvals, demo actions, awaited info). Internal blockers stay hidden.
    supabase
      .from("client_blockers")
      .select(
        "id, title, description, blocker_side, priority, resolution_status, created_at",
      )
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("blocker_side", "client_side")
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("resolution_status", { ascending: true })
      .order("created_at", { ascending: false }),

    // Published, client-visible weekly reports only.
    supabase
      .from("client_weekly_reports")
      .select(
        "id, period_label, week_start, summary, highlights, lowlights, next_week_plan, created_at",
      )
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .eq("is_published", true)
      .neq("is_client_visible", false)
      .order("created_at", { ascending: false }),

    supabase
      .from("client_contributors")
      .select("name, role, person_type, status")
      .eq("client_id", clientId)
      .eq("archived", false)
      .order("created_at", { ascending: false }),

    supabase.from("users").select("id, name").eq("org_id", orgId),

    // Client-visible linked tasks: org-wide tasks flagged to show on the client
    // dashboard, narrowed to this client by free-text business below.
    supabase
      .from("tasks")
      .select(
        "id, task_no, title, business, area, status, priority, progress, deadline, updated_at",
      )
      .eq("org_id", orgId)
      .eq("is_client_visible", true)
      .not("business", "is", null)
      .order("updated_at", { ascending: false }),

    // All client leads (lightweight columns) for the auto-report numbers, so the
    // client-view report mirrors the internal workspace report exactly.
    fetchAllClientLeadRows(
      "id, pipeline_stage, outreach_status, demo_status, assigned_to, created_at, updated_at",
      orgId,
      clientId,
    ),

    supabase
      .from("client_incentives")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }),

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

  const services = (servicesResult.data || [])
    .map((row) => row.services)
    .filter(Boolean);

  // Narrow client-visible tasks to this client (business == name/company).
  const taskNameKeys = new Set(
    [client.name, client.company_name]
      .filter(Boolean)
      .map((s) => String(s).trim().toLowerCase()),
  );
  const linkedTasks = (linkedTasksResult.data || []).filter((t) =>
    taskNameKeys.has(String(t.business || "").trim().toLowerCase()),
  );

  const [reportSummaries, clientGoals] = await Promise.all([
    getLatestClientReportSummaries(orgId, clientId),
    getClientGoals(orgId, clientId),
  ]);

  return {
    client,
    services,
    workItems: workItemsResult.data || [],
    updates: updatesResult.data || [],
    actions: actionsResult.data || [],
    documents: documentsResult.data || [],
    leads,
    campaigns: campaignsResult.data || [],
    meetings: meetingsResult.data || [],
    blockers: blockersResult.data || [],
    reports: reportsResult.data || [],
    contributors: contributorsResult.data || [],
    users: usersResult.data || [],
    linkedTasks,
    leadAllRows,
    incentives: incentivesResult.data || [],
    leadStageEvents: leadStageEventsResult.data || [],
    reportSummaries,
    clientGoals,
  };
}
