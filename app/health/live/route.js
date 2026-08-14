// GET /health/live — liveness probe. Answers as long as the process is up;
// deliberately touches nothing external so a database blip cannot restart it.

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export function GET() {
  return Response.json({ ok: true, status: "live" });
}
