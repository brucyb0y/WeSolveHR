// GET /api/top-nav-summary — the "Off: N / Break: N" counters in the top nav.
//
// NON-MANAGERS GET ZEROES, NOT A 403. Who is off or on break is management
// information, so the original returns an empty-but-valid payload for everyone
// else — the nav renders the same shape for all users and simply shows nothing.
// Returning an error here would make the nav render a failure state for most of
// the company.
//
// The events query is scoped to the current attendance day rather than reading
// the whole table, then reduced to each user's latest event: ordering
// descending and keeping the first row per user is what makes "currently on
// break" correct without a per-user query.

import {
  supabase,
  isManagerOrAdmin,
  getAttendanceDayDateStringFromDate,
  getAttendanceDayUtcRange,
  getPlannedOffRowsForDate,
} from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import { apiSuccess, apiError, withApiErrors } from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const EMPTY = { offCount: 0, offNames: [], breakCount: 0, breakNames: [] };

export const GET = withApiErrors(
  "GET /api/top-nav-summary",
  async (request) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    if (!user || !isManagerOrAdmin(user)) return apiSuccess(EMPTY);

    const today = getAttendanceDayDateStringFromDate(new Date());
    const { startUtc, endUtc } = getAttendanceDayUtcRange(today);

    const [plannedOffRows, usersResult, eventsResult] = await Promise.all([
      getPlannedOffRowsForDate(today, user.org_id),
      supabase
        .from("users")
        .select("id, name")
        .eq("org_id", user.org_id)
        .eq("is_active", true)
        .order("name", { ascending: true }),
      supabase
        .from("attendance_events")
        .select("user_id, action, created_at")
        .eq("org_id", user.org_id)
        .gte("created_at", startUtc)
        .lt("created_at", endUtc)
        .order("created_at", { ascending: false }),
    ]);

    if (usersResult.error) {
      console.error("top nav users error:", usersResult.error);
      return apiError(500, "Failed to fetch users");
    }
    if (eventsResult.error) {
      console.error("top nav events error:", eventsResult.error);
      return apiError(500, "Failed to fetch events");
    }

    const offNames = (plannedOffRows || []).map(
      (x) => x.users?.name || "Unknown",
    );

    // Rows arrive newest-first, so the first one seen per user is their latest.
    const latestByUser = new Map();
    for (const event of eventsResult.data || []) {
      if (!latestByUser.has(event.user_id)) {
        latestByUser.set(event.user_id, event);
      }
    }

    const breakNames = (usersResult.data || [])
      .filter((u) => latestByUser.get(u.id)?.action === "break")
      .map((u) => u.name);

    return apiSuccess({
      offCount: offNames.length,
      offNames,
      breakCount: breakNames.length,
      breakNames,
    });
  },
);
