// Client-lead aggregates shared by the client workspace and the lead APIs.
// Extracted verbatim from the original monolith.

import { CLIENT_LEADS_TABLE, INLINE_CLIENT_LEADS_BUSINESSES } from "../server/constants.js";
import { supabase } from "../server/supabase.js";
import { getBusinessLeadTableName } from "./leads.js";

async function getClientLeadCategoryTypeCounts(orgId, tableName, clientId) {
  if (!tableName || !tableHasClientLeadColumns(tableName)) return [];

  let query = supabase
    .from(tableName)
    .select("category_type", { count: "exact" })
    .eq("org_id", orgId)
    .not("category_type", "is", null);
  if (clientId) query = query.eq("client_id", clientId);
  query = query.or("is_deleted.is.null,is_deleted.eq.false");

  const counts = {};
  const FETCH_BATCH = 1000;
  let exactCount = null;
  for (let batchIdx = 0; batchIdx < 50; batchIdx += 1) {
    const offset = batchIdx * FETCH_BATCH;
    const { data, error, count } = await query.range(
      offset,
      offset + FETCH_BATCH - 1,
    );
    if (error) throw error;
    if (exactCount === null) exactCount = count;
    (data || []).forEach((row) => {
      const key = String(row.category_type || "").trim();
      if (!key) return;
      counts[key] = (counts[key] || 0) + 1;
    });
    if (!data || data.length < FETCH_BATCH) break;
    if (exactCount != null && offset + FETCH_BATCH >= exactCount) break;
  }

  return Object.entries(counts)
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count);
}

function tableHasClientLeadColumns(tableName) {
  if (tableName === CLIENT_LEADS_TABLE) return true;
  for (const business of INLINE_CLIENT_LEADS_BUSINESSES) {
    if (getBusinessLeadTableName(business) === tableName) return true;
  }
  return false;
}

export {
  getClientLeadCategoryTypeCounts,
};
