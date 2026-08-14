// POST /api/rasset-leads/import-excel — bulk-import leads from a spreadsheet.

import { DASHBOARD_ORG_ID, importRassetLeadsFromExcel } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readUploadedFile,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/rasset-leads/import-excel",
  async (request) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const form = await request.formData();
    const file = await readUploadedFile(form, "file");
    if (!file) return apiError(400, "Excel file is required");

    try {
      return apiSuccess(
        await importRassetLeadsFromExcel({
          orgId: DASHBOARD_ORG_ID,
          buffer: file.buffer,
          fileName: file.originalname,
          uploadedByUserId: user?.id || null,
          // Recorded on the import log so a bad sheet can be traced back.
          uploadedByName: user?.name || user?.phone_number || "Unknown",
        }),
      );
    } catch (error) {
      console.error("rasset excel import error:", error);
      return apiError(500, error.message || "Failed to import leads");
    }
  },
);
