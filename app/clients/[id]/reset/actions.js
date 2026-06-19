"use server";

// Reset-workspace Server Action — replaces the POST /clients/:id/reset Express
// handler. clientId is bound when the form is rendered; the checkboxes submit
// "on" when checked, which resetClientWorkspace() interprets exactly as before.

import { redirect } from "next/navigation";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import { resetClientWorkspace } from "@/lib/services/clients.js";

export async function resetClientAction(clientId, formData) {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  await resetClientWorkspace({
    orgId: user.org_id || DASHBOARD_ORG_ID,
    actorUserId: user.id || null,
    clientId: Number(clientId),
    body: Object.fromEntries(formData),
  });

  redirect("/clients");
}
