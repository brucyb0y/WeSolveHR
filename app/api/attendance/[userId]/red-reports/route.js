// GET /api/attendance/:userId/red-reports — days in a month with no report filed.
//
// The scanned range depends on WHICH month is asked for: the current month is
// only checked up to today (you cannot be late for a day that has not happened),
// while a past month is checked in full. Without that distinction every future
// day of the current month would count as a missing report.

import {
  DASHBOARD_ORG_ID,
  getAttendanceMonthNavigation,
  getAttendanceDayDateStringFromDate,
  addDaysToDateString,
  getMissingReportDatesForUserInRange,
  formatDateListForHumans,
} from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
  searchParamsToQuery,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const GET = withApiErrors(
  "GET /api/attendance/[userId]/red-reports",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { userId: raw } = await routeParams(ctx);
    const userId = Number(raw);
    if (!userId) return apiError(400, "Invalid user id");

    const monthNav = getAttendanceMonthNavigation(
      searchParamsToQuery(request).month,
    );
    const todayAttendanceDate = getAttendanceDayDateStringFromDate(new Date());

    const endDateExclusive =
      monthNav.selectedMonth === monthNav.currentMonth
        ? addDaysToDateString(todayAttendanceDate, 1)
        : monthNav.endDateExclusive;

    const redReportDates = await getMissingReportDatesForUserInRange({
      orgId: DASHBOARD_ORG_ID,
      userId,
      startDate: monthNav.startDate,
      endDateExclusive,
    });

    return apiSuccess({
      redReportDays: redReportDates.length,
      redReportDates,
      redReportDatesText: formatDateListForHumans(redReportDates),
    });
  },
);
