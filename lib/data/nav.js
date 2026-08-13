// Data behind the top navigation.
//
// These are the query bodies of app.get("/api/top-nav-summary") and
// app.get("/api/clients/nav-list"), lifted out so the TopNav server component
// can await them directly. The HTTP endpoints still exist and still work —
// they are what the not-yet-converted HTML-string pages fetch — but a React
// page renders the nav with its data already in hand, so the "Off: ..." /
// "Loading…" placeholders never flash.

import {
  supabase,
  isManagerOrAdmin,
  getAttendanceDayDateStringFromDate,
  getAttendanceDayUtcRange,
  getPlannedOffRowsForDate,
} from "@/lib/server/app.js";

const EMPTY_SUMMARY = {
  offCount: 0,
  offNames: [],
  breakCount: 0,
  breakNames: [],
};

// Who is off today and who is on break. Non-managers see zeroes, matching the
// endpoint's early return.
export async function getTopNavSummary(actingUser) {
  if (!actingUser || !isManagerOrAdmin(actingUser)) return EMPTY_SUMMARY;

  try {
    const today = getAttendanceDayDateStringFromDate(new Date());
    const { startUtc, endUtc } = getAttendanceDayUtcRange(today);

    const [plannedOffRows, usersResult, eventsResult] = await Promise.all([
      getPlannedOffRowsForDate(today, actingUser.org_id),
      supabase
        .from("users")
        .select("id, name")
        .eq("org_id", actingUser.org_id)
        .eq("is_active", true)
        .order("name", { ascending: true }),
      // Only each user's latest event for *today* is needed to know who is on
      // break; scoping to the attendance day avoids scanning the whole table.
      supabase
        .from("attendance_events")
        .select("user_id, action, created_at")
        .eq("org_id", actingUser.org_id)
        .gte("created_at", startUtc)
        .lt("created_at", endUtc)
        .order("created_at", { ascending: false }),
    ]);

    if (usersResult.error) {
      console.error("top nav users error:", usersResult.error);
      return EMPTY_SUMMARY;
    }

    if (eventsResult.error) {
      console.error("top nav events error:", eventsResult.error);
      return EMPTY_SUMMARY;
    }

    const offNames = (plannedOffRows || []).map(
      (x) => x.users?.name || "Unknown",
    );

    const latestByUser = new Map();
    for (const event of eventsResult.data || []) {
      if (!latestByUser.has(event.user_id)) {
        latestByUser.set(event.user_id, event);
      }
    }

    const breakNames = (usersResult.data || [])
      .filter((u) => latestByUser.get(u.id)?.action === "break")
      .map((u) => u.name);

    return {
      offCount: offNames.length,
      offNames,
      breakCount: breakNames.length,
      breakNames,
    };
  } catch (error) {
    console.error("top nav summary error:", error);
    return EMPTY_SUMMARY;
  }
}

// Active clients for the Clients dropdown.
export async function getClientsNavList(orgId) {
  try {
    const { data, error } = await supabase
      .from("clients")
      .select("id, name, company_name")
      .eq("org_id", orgId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("name", { ascending: true });

    if (error) throw error;

    return (data || []).map((c) => ({
      id: c.id,
      name: c.name || c.company_name || `Client #${c.id}`,
    }));
  } catch (error) {
    console.error("clients nav list error:", error);
    return [];
  }
}
