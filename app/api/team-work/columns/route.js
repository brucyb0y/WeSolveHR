// POST /api/team-work/columns — add a work column to the board.
//
// Same append-with-a-gap scheme as members: highest existing sort_order + 10,
// so a column can be reordered by rewriting one row.
//
// Columns are org-wide (no team scoping), unlike members.

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
  readJsonBody,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const SORT_GAP = 10;

export const POST = withApiErrors(
  "POST /api/team-work/columns",
  async (request) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const orgId = orgIdForApi(user);
    const body = await readJsonBody(request);
    const label = String(body.label || "").trim();

    if (!label) return apiError(400, "Label is required");

    const { data: last } = await supabase
      .from("team_work_columns")
      .select("sort_order")
      .eq("org_id", orgId)
      .order("sort_order", { ascending: false })
      .limit(1)
      .maybeSingle();

    const { data, error } = await supabase
      .from("team_work_columns")
      .insert([
        {
          org_id: orgId,
          label,
          sort_order: (last?.sort_order || 0) + SORT_GAP,
          is_active: true,
        },
      ])
      .select("id, label")
      .maybeSingle();

    if (error) {
      console.error("add team work column error:", error);
      return apiError(500, "Failed to add column");
    }

    await insertTeamWorkLog({
      org_id: orgId,
      work_date: getTodayDateStringInTimeZone(),
      column_id: data.id,
      column_label: label,
      action: "column_added",
      actor_user_id: user?.id || null,
      actor_name: user?.name || "",
    });

    return apiSuccess({ id: data.id, label: data.label });
  },
);
