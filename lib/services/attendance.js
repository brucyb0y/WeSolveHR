// Attendance data service. The per-employee overview + month-navigation helpers
// still live in lib/server/app.js (tightly coupled to the attendance engine) and
// are re-exported here until that engine is fully migrated; the detail page
// imports from this service so its data entry point stays stable.

export {
  getEmployeeAttendanceOverview,
  getAttendanceMonthNavigation,
} from "@/lib/server/app.js";
