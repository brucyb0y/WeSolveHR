// PATCH /api/clients/:clientId/linked-tasks/:taskId
//
// Linked tasks live in the ORG-WIDE tasks table, not under the client. There is
// no foreign key — a task is "linked" when its free-text `business` matches the
// client's name or company_name. So this route AUTHORIZES on that match before
// writing: without the check, any client's URL could edit any task by id.
// That guard is the whole reason this endpoint exists rather than reusing the
// generic task API.
//
// Status changes mirror the command system's side effects, which is why they
// are not a plain field write:
//   done            -> progress forced to 100
//   open (from 100) -> progress reset to 0
//   leaving blocked -> blocker_note cleared
//
// Visibility is deliberately NOT written to task history — it controls whether
// the task appears on the client's external dashboard, which is not a change to
// the task's content.

import {
  supabase,
  insertTaskHistory,
  normalizeText,
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

const STATUSES = ["open", "in_progress", "blocked", "done"];
const PRIORITIES = ["low", "medium", "high", "urgent"];

export const PATCH = withApiErrors(
  "PATCH /api/clients/[clientId]/linked-tasks/[taskId]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const params = await routeParams(ctx);
    const clientId = Number(params.clientId);
    const taskId = Number(params.taskId);
    if (!clientId || !taskId) {
      return apiError(400, "Invalid client or task id");
    }

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const body = await readJsonBody(request);

    const { data: client, error: clientError } = await supabase
      .from("clients")
      .select("name, company_name")
      .eq("org_id", orgId)
      .eq("id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (clientError) {
      console.error("linked task client lookup error:", clientError);
      return apiError(500, "Failed to update task");
    }
    if (!client) return apiError(404, "Client not found");

    const { data: task, error: taskError } = await supabase
      .from("tasks")
      .select("id, business, status, priority, progress, blocker_note")
      .eq("org_id", orgId)
      .eq("id", taskId)
      .maybeSingle();

    if (taskError) {
      console.error("linked task lookup error:", taskError);
      return apiError(500, "Failed to update task");
    }
    if (!task) return apiError(404, "Task not found");

    const clientNameKeys = new Set(
      [client.name, client.company_name]
        .filter(Boolean)
        .map((s) => String(s).trim().toLowerCase()),
    );
    if (!clientNameKeys.has(String(task.business || "").trim().toLowerCase())) {
      return apiError(403, "Task is not linked to this client");
    }

    const patch = {
      last_updated_by_user_id: actorUserId,
      updated_at: new Date().toISOString(),
    };
    const historyCalls = [];

    if (body.status !== undefined) {
      const status = normalizeText(body.status || "");
      if (!STATUSES.includes(status)) return apiError(400, "Invalid status");
      patch.status = status;
      if (status === "done") patch.progress = 100;
      if (status === "open" && Number(task.progress) === 100) patch.progress = 0;
      if (task.blocker_note && status !== "blocked") patch.blocker_note = null;
      historyCalls.push([
        "status_change",
        "status",
        { status: task.status, progress: task.progress },
        { status, progress: patch.progress ?? task.progress },
      ]);
    }

    if (body.priority !== undefined) {
      const priority = normalizeText(body.priority || "");
      if (!PRIORITIES.includes(priority)) {
        return apiError(400, "Invalid priority");
      }
      patch.priority = priority;
      historyCalls.push(["edit", "priority", task.priority, priority]);
    }

    if (body.progress !== undefined) {
      let progress = Math.round(Number(body.progress));
      if (!Number.isFinite(progress)) return apiError(400, "Invalid progress");
      progress = Math.max(0, Math.min(100, progress));
      patch.progress = progress;
      historyCalls.push([
        "progress_change",
        "progress",
        { progress: task.progress, status: task.status },
        { progress, status: patch.status ?? task.status },
      ]);
    }

    if (body.is_client_visible !== undefined) {
      patch.is_client_visible =
        body.is_client_visible === true || body.is_client_visible === "true";
    }

    // Reject a no-op rather than bumping updated_at for nothing.
    if (historyCalls.length === 0 && body.is_client_visible === undefined) {
      return apiError(400, "No editable fields provided");
    }

    const { data: updated, error: updateError } = await supabase
      .from("tasks")
      .update(patch)
      .eq("org_id", orgId)
      .eq("id", taskId)
      .select("id, status, priority, progress, is_client_visible")
      .maybeSingle();

    if (updateError) {
      console.error("linked task update error:", updateError);
      return apiError(500, "Failed to update task");
    }
    if (!updated) return apiError(404, "Task not found");

    for (const [changeType, field, oldValue, newValue] of historyCalls) {
      await insertTaskHistory(
        taskId,
        actorUserId,
        changeType,
        field,
        oldValue,
        newValue,
      );
    }

    return apiSuccess(updated);
  },
);
