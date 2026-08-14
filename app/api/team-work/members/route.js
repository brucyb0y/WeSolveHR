// POST /api/team-work/members — add a person to the board.
//
// New members are appended to the END of their team: the highest existing
// sort_order in that team plus 10. The gap of 10 (not 1) leaves room to
// reorder by rewriting a single row's sort_order rather than renumbering the
// whole team.
//
// `team` goes through normalizeTeamWorkTeam so an unknown value lands in the
// canonical default rather than creating a phantom team column on the board.

import {
  supabase,
  insertTeamWorkLog,
  normalizeTeamWorkTeam,
  getTodayDateStringInTimeZone,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readJsonBody,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const SORT_GAP = 10;

export const POST = withApiErrors(
  "POST /api/team-work/members",
  async (request) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const actorName = user?.name || "";

    const body = await readJsonBody(request);
    const name = String(body.name || "").trim();
    const team = normalizeTeamWorkTeam(body.team);
    const responsibility = String(body.responsibility || "").trim();

    if (!name) return apiError(400, "Name is required");

    const { data: last } = await supabase
      .from("team_work_members")
      .select("sort_order")
      .eq("org_id", orgId)
      .eq("team", team)
      .order("sort_order", { ascending: false })
      .limit(1)
      .maybeSingle();

    const { data, error } = await supabase
      .from("team_work_members")
      .insert([
        {
          org_id: orgId,
          name,
          team,
          responsibility,
          sort_order: (last?.sort_order || 0) + SORT_GAP,
          is_active: true,
        },
      ])
      .select("id, name, team, responsibility")
      .maybeSingle();

    if (error) {
      console.error("add team work member error:", error);
      return apiError(500, "Failed to add person");
    }

    await insertTeamWorkLog({
      org_id: orgId,
      work_date: getTodayDateStringInTimeZone(),
      member_id: data.id,
      member_name: name,
      action: "member_added",
      detail: team,
      actor_user_id: actorUserId,
      actor_name: actorName,
    });

    return apiSuccess({
      id: data.id,
      name: data.name,
      team: normalizeTeamWorkTeam(data.team),
      responsibility: data.responsibility || "",
    });
  },
);
