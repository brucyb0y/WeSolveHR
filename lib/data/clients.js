// Data for the /clients list page — the query body of app.get("/clients"),
// lifted out so the page can await it directly.
//
// open_work_count / waiting_count / last_update_text are placeholders in the
// original too: the handler decorated every row with 0 / 0 / "-" and the
// summary's `waiting` is hard-coded to 0. Preserved as-is; wiring them up is a
// separate change from this migration.

import { supabase } from "@/lib/server/app.js";

const CLIENT_COLUMNS = `
  id,
  name,
  company_name,
  slug,
  status,
  health_status,
  start_date,
  description,
  account_manager_user_id,
  project_manager_user_id,
  created_at,
  account_manager:users!clients_account_manager_user_id_fkey(name),
  project_manager:users!clients_project_manager_user_id_fkey(name)
`;

export async function getClientsListData(orgId) {
  const { data: clients, error } = await supabase
    .from("clients")
    .select(CLIENT_COLUMNS)
    .eq("org_id", orgId)
    .eq("is_active", true)
    .is("deleted_at", null)
    .order("created_at", { ascending: false });

  if (error) {
    console.error("clients list error:", error);
    throw new Error("Failed to load clients");
  }

  const clientIds = (clients || []).map((c) => c.id);

  let serviceRows = [];
  if (clientIds.length) {
    const { data: servicesData, error: servicesError } = await supabase
      .from("client_services")
      .select(`client_id, services(name)`)
      .in("client_id", clientIds)
      .eq("is_active", true)
      .is("deleted_at", null);

    if (servicesError) {
      console.error("client services list error:", servicesError);
    } else {
      serviceRows = servicesData || [];
    }
  }

  const serviceMap = {};
  for (const row of serviceRows) {
    if (!serviceMap[row.client_id]) serviceMap[row.client_id] = [];
    if (row.services?.name) serviceMap[row.client_id].push(row.services.name);
  }

  const decoratedClients = (clients || []).map((client) => ({
    ...client,
    service_names: serviceMap[client.id] || [],
    account_manager_name: client.account_manager?.name || "",
    project_manager_name: client.project_manager?.name || "",
    open_work_count: 0,
    waiting_count: 0,
    last_update_text: "-",
  }));

  return {
    clients: decoratedClients,
    summary: {
      total: decoratedClients.length,
      active: decoratedClients.filter((c) => c.status === "active").length,
      waiting: 0,
      atRisk: decoratedClients.filter((c) => c.health_status === "at_risk")
        .length,
    },
  };
}
