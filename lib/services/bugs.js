// Stage 0 bug board data. The query lives in lib/server/app.js and is
// re-exported here until the bug subsystem is fully migrated; the page imports
// from this service so the data-layer entry point stays stable.

export { getStage0BugBoardData } from "@/lib/server/app.js";
