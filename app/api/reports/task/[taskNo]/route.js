// GET /api/reports/task/:taskNo — task detail for the report / tasks drill-in.
//
// Called from three places (tasks console, per-user workspace, reports page),
// all of which render the same modal, so the response shape is a contract.
//
// Scoped to DASHBOARD_ORG_ID rather than the caller's org, matching the
// original — these are cross-org operations views.
//
// History is capped at the 15 most recent entries and resolved to names in ONE
// extra query: the changed-by ids are de-duplicated first, so a task with 15
// edits by the same person costs one lookup rather than fifteen.

import {
  supabase,
  DASHBOARD_ORG_ID,
  getTaskById,
  getTaskOwnerNames,
  formatDateTime,
} from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const GET = withApiErrors(
  "GET /api/reports/task/[taskNo]",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { taskNo: raw } = await routeParams(ctx);
    const taskNo = Number(raw);
    if (!taskNo) return apiError(400, "Invalid task number");

    const { task, error } = await getTaskById(taskNo, DASHBOARD_ORG_ID);
    if (error) {
      console.error("report task detail fetch error:", error);
      return apiError(500, "Failed to fetch task");
    }
    if (!task) return apiError(404, "Task not found");

    const ownerNames = await getTaskOwnerNames(task.id, DASHBOARD_ORG_ID);

    const { data: historyRows, error: historyError } = await supabase
      .from("task_history")
      .select(
        "id, task_id, changed_by_user_id, change_type, field_name, old_value, new_value, created_at",
      )
      .eq("org_id", DASHBOARD_ORG_ID)
      .eq("task_id", task.id)
      .order("created_at", { ascending: false })
      .limit(15);

    if (historyError) {
      console.error("report task history fetch error:", historyError);
      return apiError(500, "Failed to fetch task history");
    }

    const changedByIds = [
      ...new Set(
        (historyRows || []).map((x) => x.changed_by_user_id).filter(Boolean),
      ),
    ];

    let userMap = new Map();
    if (changedByIds.length) {
      const { data: userRows } = await supabase
        .from("users")
        .select("id, name")
        .eq("org_id", DASHBOARD_ORG_ID)
        .in("id", changedByIds);
      userMap = new Map((userRows || []).map((u) => [u.id, u.name]));
    }

    const history = (historyRows || []).map((row) => ({
      id: row.id,
      at: formatDateTime(row.created_at),
      // Falls back to "User <id>" so a deleted account still shows who acted.
      by:
        userMap.get(row.changed_by_user_id) ||
        `User ${row.changed_by_user_id || "-"}`,
      changeType: row.change_type,
      fieldName: row.field_name,
      oldValue: row.old_value || {},
      newValue: row.new_value || {},
    }));

    return apiSuccess({
      id: task.id,
      taskNo: task.task_no || task.id,
      title: task.title,
      detail: task.detail,
      status: task.status,
      priority: task.priority,
      progress: task.progress,
      deadline: task.deadline,
      blockerNote: task.blocker_note,
      business: task.business,
      area: task.area,
      owners: ownerNames,
      history,
    });
  },
);
