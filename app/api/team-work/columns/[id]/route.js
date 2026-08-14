// DELETE /api/team-work/columns/:id — remove a work column.
//
// Hard delete, like members. The label is read BEFORE the delete so the
// activity log can name the removed column afterwards.
//
// Note this does not touch team_work_hours rows referencing the column — the
// original left them, and the board simply stops rendering that column. Adding
// a cascade here would destroy historical hour data the log still refers to.

import {
  supabase,
  insertTeamWorkLog,
  getTodayDateStringInTimeZone,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const DELETE = withApiErrors(
  "DELETE /api/team-work/columns/[id]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { id } = await routeParams(ctx);
    const columnId = Number(id);
    if (!columnId) return apiError(400, "Invalid id");

    const orgId = orgIdForApi(user);

    const { data: column } = await supabase
      .from("team_work_columns")
      .select("label")
      .eq("id", columnId)
      .maybeSingle();

    const { error } = await supabase
      .from("team_work_columns")
      .delete()
      .eq("org_id", orgId)
      .eq("id", columnId);

    if (error) {
      console.error("delete team work column error:", error);
      return apiError(500, "Failed to remove column");
    }

    await insertTeamWorkLog({
      org_id: orgId,
      work_date: getTodayDateStringInTimeZone(),
      column_id: columnId,
      column_label: column?.label || "",
      action: "column_removed",
      actor_user_id: user?.id || null,
      actor_name: user?.name || "",
    });

    return apiSuccess({ id: columnId });
  },
);
