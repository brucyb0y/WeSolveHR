// POST /api/team-work/hours — set one member's hours in one column for a day.
//
// Upserts on (org_id, work_date, member_id, column_id) — one cell per
// member/column/day, so re-saving a cell updates it rather than accumulating
// duplicate rows.
//
// Hours are CLAMPED, not rejected: anything non-numeric or negative becomes 0
// and anything above 24 becomes 24. The board's inputs are free-text, and the
// original chose to accept and correct rather than error mid-edit.
//
// The activity log is written ONLY when the value actually changed. Re-saving
// the same number (a blur with no edit) must not fill the log with no-ops —
// which is why the previous value is read before the upsert.

import { supabase, insertTeamWorkLog } from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readJsonBody,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/team-work/hours",
  async (request) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const actorName = user?.name || "";

    const body = await readJsonBody(request);
    const date = String(body.date || "").trim();
    const memberId = Number(body.member_id);
    const columnId = Number(body.column_id);

    if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !memberId || !columnId) {
      return apiError(400, "Invalid date, member or column");
    }

    let hours = Number(body.hours);
    if (Number.isNaN(hours) || hours < 0) hours = 0;
    if (hours > 24) hours = 24;

    // Labels for the log entry, plus the previous value to decide whether to
    // log at all.
    const [{ data: member }, { data: column }, { data: existing }] =
      await Promise.all([
        supabase
          .from("team_work_members")
          .select("name")
          .eq("id", memberId)
          .maybeSingle(),
        supabase
          .from("team_work_columns")
          .select("label")
          .eq("id", columnId)
          .maybeSingle(),
        supabase
          .from("team_work_hours")
          .select("hours")
          .eq("org_id", orgId)
          .eq("work_date", date)
          .eq("member_id", memberId)
          .eq("column_id", columnId)
          .maybeSingle(),
      ]);

    const oldHours = existing ? Number(existing.hours) || 0 : 0;

    const { data, error } = await supabase
      .from("team_work_hours")
      .upsert(
        [
          {
            org_id: orgId,
            work_date: date,
            member_id: memberId,
            column_id: columnId,
            hours,
            updated_at: new Date().toISOString(),
          },
        ],
        { onConflict: "org_id,work_date,member_id,column_id" },
      )
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("save team work hours error:", error);
      return apiError(500, "Failed to save hours");
    }

    if (oldHours !== hours) {
      await insertTeamWorkLog({
        org_id: orgId,
        work_date: date,
        member_id: memberId,
        column_id: columnId,
        member_name: member?.name || "",
        column_label: column?.label || "",
        action: "hours_changed",
        old_hours: oldHours,
        new_hours: hours,
        actor_user_id: actorUserId,
        actor_name: actorName,
      });
    }

    return apiSuccess(data);
  },
);
