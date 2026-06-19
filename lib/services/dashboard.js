// Dashboard data service.
//
// The dashboard's summary/task-load computation is tightly coupled to the
// attendance engine (getTodayAttendanceEventsForAllUsers, planned-off and
// late-arrival lookups), which still lives in lib/server/app.js. Rather than
// fork that logic and risk drift before the attendance slice is migrated, we
// re-export the canonical implementation. When attendance is converted, the
// body of getDashboardData moves here and this re-export becomes a real
// implementation — the page.jsx import path stays the same.

export { getDashboardData, DASHBOARD_ORG_ID } from "@/lib/server/app.js";
