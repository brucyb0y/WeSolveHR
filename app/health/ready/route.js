// GET /health/ready — readiness probe.
//
// Unlike /health/live this DOES touch the database: readiness means "can serve
// traffic", so a failing query must report not-ready (500) and take the
// instance out of rotation.
//
// The openai/twilioAuth flags report whether those integrations are configured;
// they are informational and do not affect readiness.

import { supabase } from "@/lib/server/app";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const { error } = await supabase.from("users").select("id").limit(1);
    if (error) {
      return Response.json(
        { ok: false, status: "db_error", error: error.message },
        { status: 500 },
      );
    }
    return Response.json({
      ok: true,
      status: "ready",
      openai: !!process.env.OPENAI_API_KEY,
      twilioAuth: !!process.env.TWILIO_AUTH_TOKEN,
    });
  } catch (error) {
    return Response.json(
      { ok: false, status: "error", error: error?.message || String(error) },
      { status: 500 },
    );
  }
}
