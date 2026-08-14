// PATCH / DELETE /api/team-work/members/:id
//
// PATCH builds its patch key-by-key and REJECTS an empty one ("Nothing to
// update") rather than issuing a no-op write — the hover-card saves on blur, so
// an untouched field must not round-trip.
//
// Note the differing guards, preserved from the original: `responsibility`
// accepts an empty string (clearing the note is a real edit), while `name`
// requires a non-empty value (a member with no name would be unidentifiable on
// the board).
//
// DELETE is a HARD delete — team_work_members has no soft-delete column. The
// member's name is read first so the activity log can record who was removed
// after the row is gone.

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
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const PATCH = withApiErrors(
  "PATCH /api/team-work/members/[id]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { id } = await routeParams(ctx);
    const memberId = Number(id);
    if (!memberId) return apiError(400, "Invalid id");

    const body = await readJsonBody(request);
    const patch = {};

    if (typeof body.responsibility === "string") {
      patch.responsibility = body.responsibility.trim();
    }
    if (typeof body.name === "string" && body.name.trim()) {
      patch.name = body.name.trim();
    }
    if (body.team != null) {
      patch.team = normalizeTeamWorkTeam(body.team);
    }

    if (!Object.keys(patch).length) return apiError(400, "Nothing to update");

    const { data, error } = await supabase
      .from("team_work_members")
      .update(patch)
      .eq("org_id", orgIdForApi(user))
      .eq("id", memberId)
      .select("id, name, team, responsibility")
      .maybeSingle();

    if (error) {
      console.error("update team work member error:", error);
      return apiError(500, "Failed to update person");
    }
    if (!data) return apiError(404, "Person not found");

    return apiSuccess({
      id: data.id,
      name: data.name,
      team: normalizeTeamWorkTeam(data.team),
      responsibility: data.responsibility || "",
    });
  },
);

export const DELETE = withApiErrors(
  "DELETE /api/team-work/members/[id]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { id } = await routeParams(ctx);
    const memberId = Number(id);
    if (!memberId) return apiError(400, "Invalid id");

    const orgId = orgIdForApi(user);

    // Read the name BEFORE deleting — it is gone afterwards.
    const { data: member } = await supabase
      .from("team_work_members")
      .select("name")
      .eq("id", memberId)
      .maybeSingle();

    const { error } = await supabase
      .from("team_work_members")
      .delete()
      .eq("org_id", orgId)
      .eq("id", memberId);

    if (error) {
      console.error("delete team work member error:", error);
      return apiError(500, "Failed to remove person");
    }

    await insertTeamWorkLog({
      org_id: orgId,
      work_date: getTodayDateStringInTimeZone(),
      member_id: memberId,
      member_name: member?.name || "",
      action: "member_removed",
      actor_user_id: user?.id || null,
      actor_name: user?.name || "",
    });

    return apiSuccess({ id: memberId });
  },
);
