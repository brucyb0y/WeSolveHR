// Client activity-log writer, shared by every client mutation. Extracted
// verbatim from the original monolith.

import { supabase } from "../server/supabase.js";

async function insertClientActivityLog({
  orgId,
  clientId,
  actorUserId,
  action,
  entityType = null,
  entityId = null,
  oldValue = null,
  newValue = null,
}) {
  const { error } = await supabase.from("client_activity_logs").insert([
    {
      org_id: orgId,
      client_id: clientId,
      actor_user_id: actorUserId || null,
      action,
      entity_type: entityType,
      entity_id: entityId,
      old_value: oldValue,
      new_value: newValue,
    },
  ]);

  if (error) {
    console.error("insertClientActivityLog error:", error);
  }
}

export {
  insertClientActivityLog,
};
