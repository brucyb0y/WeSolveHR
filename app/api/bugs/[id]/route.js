// PATCH /api/bugs/:id — update a bug on the Stage-0 board.
//
// Partial update built key-by-key on `!== undefined`, so omitting a field
// leaves it alone while sending null clears it. There is no payload builder
// here — do NOT merge with the existing row.
//
// The assignee is VALIDATED against the users table before being written:
// it must exist, be active, and belong to this org. A bare foreign key would
// accept an id from another org, silently assigning a bug to someone the board
// can't display.
//
// Scoped to DASHBOARD_ORG_ID rather than the caller's org, matching the
// original — the bug board is a single cross-org surface.

import {
  supabase,
  DASHBOARD_ORG_ID,
  isValidStage0BugColumn,
  isValidStage0BugSeverity,
  isValidStage0BugStatus,
} from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
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
  "PATCH /api/bugs/[id]",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { id } = await routeParams(ctx);
    const bugId = Number(id);
    if (!bugId) return apiError(400, "Invalid bug id");

    const body = await readJsonBody(request);

    const { data: existingBug, error: existingBugError } = await supabase
      .from("stage0_bug_board")
      .select("id, org_id")
      .eq("id", bugId)
      .eq("org_id", DASHBOARD_ORG_ID)
      .maybeSingle();

    if (existingBugError) {
      console.error("bug lookup before patch error:", existingBugError);
      return apiError(500, "Failed to fetch bug");
    }
    if (!existingBug) return apiError(404, "Bug not found");

    const patch = { updated_at: new Date().toISOString() };

    if (body.title !== undefined) {
      const cleanTitle = String(body.title).trim();
      if (!cleanTitle) return apiError(400, "Title cannot be empty");
      patch.title = cleanTitle;
    }
    if (body.description !== undefined) {
      patch.description =
        body.description == null ? null : String(body.description).trim();
    }
    if (body.board_column !== undefined) {
      if (!isValidStage0BugColumn(body.board_column)) {
        return apiError(400, "Invalid board_column");
      }
      patch.board_column = String(body.board_column).trim();
    }
    if (body.severity !== undefined) {
      if (!isValidStage0BugSeverity(body.severity)) {
        return apiError(400, "Invalid severity");
      }
      patch.severity = String(body.severity).trim();
    }
    if (body.status !== undefined) {
      if (!isValidStage0BugStatus(body.status)) {
        return apiError(400, "Invalid status");
      }
      patch.status = String(body.status).trim();
    }

    for (const key of [
      "source_message_sid",
      "source_phone_number",
      "source_message_text",
    ]) {
      if (body[key] !== undefined) {
        patch[key] = body[key] ? String(body[key]).trim() : null;
      }
    }

    if (body.assigned_to_user_id !== undefined) {
      if (!body.assigned_to_user_id) {
        patch.assigned_to_user_id = null;
      } else {
        const numericUserId = Number(body.assigned_to_user_id);
        if (!numericUserId) return apiError(400, "Invalid assigned_to_user_id");

        const { data: assigneeUser, error: assigneeError } = await supabase
          .from("users")
          .select("id, org_id, is_active")
          .eq("id", numericUserId)
          .eq("org_id", DASHBOARD_ORG_ID)
          .eq("is_active", true)
          .maybeSingle();

        if (assigneeError) {
          console.error("bug assignee lookup error:", assigneeError);
          return apiError(500, "Failed to validate assignee");
        }
        if (!assigneeUser) {
          return apiError(
            400,
            "Assigned user not found, inactive, or belongs to another org",
          );
        }
        patch.assigned_to_user_id = numericUserId;
      }
    }

    const { data, error } = await supabase
      .from("stage0_bug_board")
      .update(patch)
      .eq("id", bugId)
      .eq("org_id", DASHBOARD_ORG_ID)
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("update bug error:", error);
      return apiError(500, "Failed to update bug");
    }

    return apiSuccess(data);
  },
);
