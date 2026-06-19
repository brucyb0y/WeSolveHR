"use server";

// Edit-client Server Action — replaces the POST /clients/:id/edit Express
// handler. clientId is bound on the server when the form is rendered; formData
// carries the field values (gtm_associate_user_ids is multi-valued).

import { redirect } from "next/navigation";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import { updateClient } from "@/lib/services/clients.js";

export async function updateClientAction(clientId, formData) {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const body = Object.fromEntries(formData);
  // Repeated checkboxes need getAll; Object.fromEntries keeps only the last.
  body.gtm_associate_user_ids = formData.getAll("gtm_associate_user_ids");

  await updateClient({
    orgId: user.org_id || DASHBOARD_ORG_ID,
    actorUserId: user.id || null,
    clientId: Number(clientId),
    body,
  });

  // Outside any try/catch so the redirect control-flow signal isn't swallowed.
  redirect(`/clients/${clientId}`);
}
