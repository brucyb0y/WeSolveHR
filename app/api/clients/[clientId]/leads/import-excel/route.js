// POST /api/clients/:clientId/leads/import-excel — bulk-import a client's leads.
//
// TWO IMPORTERS, chosen by how the client stores leads:
//   * per-client `client_leads` rows -> importClientLeadsFromExcel, which also
//     stamps the dialog's category_type on every row;
//   * a client mapped to an inline static business (e.g. Rasset) -> that
//     business's own importer, mirroring the standalone /leads/<business>
//     import. `category_type` does not apply there, which is why it is only
//     passed to the first.
//
// The category is validated against the client's own category list before
// the Leads tab's chips are built from it, so an unrecognised value would
// produce leads no chip can reach.

import {
  insertClientActivityLog,
  resolveClientLeadBusiness,
  resolveLeadSource,
  importClientLeadsFromExcel,
  importRassetLeadsFromExcel,
  getClientLeadCategoryTypes,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
  readUploadedFile,
  formToBody,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/leads/import-excel",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: raw } = await routeParams(ctx);
    const clientId = Number(raw);
    if (!clientId) return apiError(400, "Invalid client id");

    const form = await request.formData();
    const file = await readUploadedFile(form, "file");
    if (!file) return apiError(400, "Excel file is required");

    const body = formToBody(form);
    const categoryType = String(body.category_type || "").trim();
    if (
      categoryType &&
      !getClientLeadCategoryTypes(clientId).some((c) => c.key === categoryType)
    ) {
      return apiError(400, "Invalid category type");
    }

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const uploadedByName = user?.name || user?.phone_number || "Unknown";

    const business = await resolveClientLeadBusiness(orgId, clientId);
    const { tableName: leadTable, clientId: leadClientId } =
      resolveLeadSource(business);

    try {
      const data = leadClientId
        ? await importClientLeadsFromExcel({
            orgId,
            clientId,
            buffer: file.buffer,
            fileName: file.originalname,
            uploadedByUserId: actorUserId,
            uploadedByName,
            categoryType,
          })
        : await importRassetLeadsFromExcel({
            orgId,
            buffer: file.buffer,
            fileName: file.originalname,
            uploadedByUserId: actorUserId,
            uploadedByName,
          });

      await insertClientActivityLog({
        orgId,
        clientId,
        actorUserId,
        action: "client_leads_imported",
        entityType: leadTable,
        entityId: null,
        newValue: {
          inserted: data.inserted,
          updated: data.updated || 0,
          duplicates: data.duplicates,
          total: data.total,
          category_type: categoryType || null,
        },
      });

      return apiSuccess(data);
    } catch (error) {
      console.error("client leads excel import error:", error);
      return apiError(500, error.message || "Failed to import leads");
    }
  },
);
