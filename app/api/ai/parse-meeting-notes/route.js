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
