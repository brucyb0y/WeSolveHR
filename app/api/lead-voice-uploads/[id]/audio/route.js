// GET /api/lead-voice-uploads/:id/audio — audio proxy for WhatsApp voice notes.
//
// NOT a JSON endpoint: it streams bytes, so errors are plain text and the
// success response is the audio itself.
//
// It exists to keep Twilio credentials server-side. The stored media_url is a
// Twilio URL requiring Basic auth; the browser cannot fetch it directly without
// being handed those credentials, so this route authenticates and relays the
// bytes.
//
// Upstream status is forwarded rather than flattened to 500 — a 404 from Twilio
// means the recording expired, which is different from our own failure.

import { supabase, DASHBOARD_ORG_ID } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import { routeParams } from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const text = (status, body) =>
  new Response(body, { status, headers: { "Content-Type": "text/plain" } });

export async function GET(request, ctx) {
  const { response } = await requireApiUser(request);
  if (response) return response;

  try {
    const { id: raw } = await routeParams(ctx);
    const id = Number(raw);
    if (!id) return text(400, "Invalid audio ID");

    const { data: lead, error } = await supabase
      .from("lead_voice_uploads")
      .select("id, org_id, media_url, media_content_type")
      .eq("org_id", DASHBOARD_ORG_ID)
      .eq("id", id)
      .maybeSingle();

    if (error) {
      console.error("audio proxy lookup error:", error);
      return text(500, "Failed to load audio");
    }
    if (!lead?.media_url) return text(404, "Audio not found");

    const accountSid = process.env.TWILIO_ACCOUNT_SID;
    const authToken = process.env.TWILIO_AUTH_TOKEN;
    if (!accountSid || !authToken) return text(500, "Twilio credentials missing");

    const auth = Buffer.from(`${accountSid}:${authToken}`).toString("base64");
    const upstream = await fetch(lead.media_url, {
      headers: { Authorization: `Basic ${auth}` },
    });

    if (!upstream.ok) {
      console.error(
        "Twilio audio fetch failed:",
        upstream.status,
        await upstream.text(),
      );
      return text(upstream.status, "Failed to load Twilio audio");
    }

    const buffer = Buffer.from(await upstream.arrayBuffer());

    return new Response(buffer, {
      headers: {
        "Content-Type": lead.media_content_type || "audio/mpeg",
        "Content-Length": String(buffer.length),
        // Players need this to seek within the clip.
        "Accept-Ranges": "bytes",
      },
    });
  } catch (error) {
    console.error("audio proxy error:", error);
    return text(500, `Failed to load audio: ${error.message}`);
  }
}
