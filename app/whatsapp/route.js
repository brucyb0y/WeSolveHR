// POST /whatsapp — Twilio inbound webhook for the WhatsApp bot.
//
// Twilio posts form-encoded data and signs it over the EXACT absolute URL it
// called. That URL must be reconstructed from the forwarded headers: behind a
// proxy `request.url` is the internal origin, and signature validation would
// fail for every message.
//
// The reply is TwiML (text/xml). The command handling itself lives in
// handleWhatsAppWebhook in lib/server/app.js — ~1200 lines that would swamp
// this file.

import { handleWhatsAppWebhook } from "@/lib/server/app";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function absoluteUrl(request) {
  const h = request.headers;
  const url = new URL(request.url);
  const proto = h.get("x-forwarded-proto") || url.protocol.replace(":", "");
  const host = h.get("x-forwarded-host") || h.get("host") || url.host;
  return `${proto}://${host}${url.pathname}${url.search}`;
}

export async function POST(request) {
  try {
    const form = await request.formData();
    const body = {};
    for (const [k, v] of form.entries()) body[k] = v;

    const result = await handleWhatsAppWebhook({
      body,
      signature: request.headers.get("x-twilio-signature"),
      url: absoluteUrl(request),
      ip:
        request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ||
        "unknown",
    });

    // Signature rejection comes back as { status, text }.
    if (result && typeof result === "object" && result.status) {
      return new Response(result.text, {
        status: result.status,
        headers: { "Content-Type": "text/plain" },
      });
    }

    return new Response(String(result || ""), {
      status: 200,
      headers: { "Content-Type": "text/xml" },
    });
  } catch (error) {
    console.error("/whatsapp route error:", error);
    // Twilio retries on 5xx; an empty 200 TwiML ends the exchange quietly
    // rather than looping a message that already failed.
    return new Response(
      '<?xml version="1.0" encoding="UTF-8"?><Response></Response>',
      { status: 200, headers: { "Content-Type": "text/xml" } },
    );
  }
}
