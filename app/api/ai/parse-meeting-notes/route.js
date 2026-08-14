// POST /api/ai/parse-meeting-notes — AI quick-fill for the meeting form.
//
// The prompt and response shaping live in parseMeetingNotesWithAI
// (lib/server/app.js) beside the other AI helpers; this handler only validates
// input and maps errors. A ~40-line prompt inside a route file would be
// unreadable and would drift from the original.
//
// A missing OPENAI_API_KEY is tagged statusCode 500 by the helper and its
// message is passed through, because it names the exact fix.

import { parseMeetingNotesWithAI } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readJsonBody,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/ai/parse-meeting-notes",
  async (request) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const body = await readJsonBody(request);
    const notes = String(body?.notes || "").trim();
    if (!notes) return apiError(400, "Meeting details are required");

    try {
      return apiSuccess(await parseMeetingNotesWithAI(notes));
    } catch (error) {
      console.error("parse meeting notes error:", error);
      return apiError(
        error.statusCode || 500,
        error.message || "Failed to parse meeting notes",
      );
    }
  },
);
