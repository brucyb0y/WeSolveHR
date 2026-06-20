// Tasks data service. The per-user workspace data builder lives in
// lib/server/app.js and is re-exported here until the task engine migrates; the
// page imports from this service so its data entry point stays stable.

export { getUserTaskWorkspaceData } from "@/lib/server/app.js";
