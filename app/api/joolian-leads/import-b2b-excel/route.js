// POST /api/joolian-leads/import-b2b-excel — B2B sheet import.
//
// Uses the SAME importer as the Rasset route: the two businesses share a sheet
// format, so the mapper is common. Only the endpoint name differs, which is why
// this file is a near-duplicate rather than a distinct implementation.

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
  "POST /api/joolian-leads/import-b2b-excel",
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
          uploadedByName: user?.name || user?.phone_number || "Unknown",
        }),
      );
    } catch (error) {
      console.error("joolian b2b excel import error:", error);
      return apiError(500, error.message || "Failed to import leads");
    }
  },
);
