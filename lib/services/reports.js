// Reports data service. The narrative-report builders live in lib/server/app.js
// and are re-exported here until the reporting engine migrates; the page imports
// from this service so its data entry points stay stable.

export {
  getDailyNarrativeReport,
  getMultiDayNarrativeReport,
  getReportDateString,
} from "@/lib/server/app.js";
