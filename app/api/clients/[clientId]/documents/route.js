// POST /api/clients/:clientId/documents — attach a Google Drive link.
//
// THIS IS A FORM POST, NOT A JSON API. It is the one endpoint in this family
// that does not speak the {ok, data} envelope, and that is preserved:
//   * the body is form-encoded, so `is_client_visible` arrives as the string
//     "on" from an HTML checkbox (=== "on", not a boolean);
//   * errors are plain text with a status, because a browser renders them
//     directly rather than a fetch() reading them;
//   * success REDIRECTS back to the client workspace.
// Wrapping it in JSON would break whatever form posts here. The React
// DocumentsTab is read-only and does not call this.
//
// The URL allow-list is a real guard, not cosmetic: only drive.google.com and
// docs.google.com links are accepted, so this cannot become an open redirect
// surface for arbitrary URLs shown to clients.

import { supabase } from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import { routeParams } from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const ALLOWED_PREFIXES = [
  "https://drive.google.com/",
  "https://docs.google.com/",
];

const text = (status, body) =>
  new Response(body, { status, headers: { "Content-Type": "text/plain" } });

export async function POST(request, ctx) {
  const { user, response } = await requireApiUser(request);
  if (response) return response;

  try {
    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);

    const form = await request.formData();
    const title = String(form.get("title") || "").trim();
    const url = String(form.get("url") || "").trim();

    if (!clientId || !title || !url) {
      return text(400, "Document title and Google Drive link are required");
    }
    if (!ALLOWED_PREFIXES.some((p) => url.startsWith(p))) {
      return text(400, "Please enter a valid Google Drive or Google Docs link");
    }

    const { error } = await supabase.from("client_documents").insert([
      {
        org_id: orgIdForApi(user),
        client_id: clientId,
        title,
        url,
        notes: form.get("notes") || null,
        is_client_visible: form.get("is_client_visible") === "on",
        created_by_user_id: user?.id || null,
      },
    ]);

    if (error) {
      console.error("add google drive document error:", error);
      return text(500, "Failed to add Google Drive link");
    }

    return Response.redirect(new URL(`/clients/${clientId}`, request.url), 303);
  } catch (error) {
    console.error("add google drive document error:", error);
    return text(500, "Failed to add Google Drive link");
  }
}
