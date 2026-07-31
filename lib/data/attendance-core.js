// Attendance computation shared by the attendance screens, the dashboard
// and the daily reports: shift resolution, per-day summaries and the month
// navigator. Extracted verbatim from the original monolith.

import { APP_TIMEZONE, APP_TIMEZONE_OFFSET, ATTENDANCE_DAY_START_HOUR, DEFAULT_SHIFT_START_TEXT, HALF_DAY_THRESHOLD_MIN, LONG_BREAK_THRESHOLD_MIN, LONG_SHIFT_THRESHOLD_MIN } from "../server/runtime.js";
import { supabase } from "../server/supabase.js";
import { addDaysToDateString, formatDateForDbFromParts, getAttendanceDayDateStringFromDate, getAttendanceDayUtcRange, getTodayDateStringInTimeZone } from "../server/time.js";
import { formatDateTime, formatTimeOnly } from "../ui/html.js";
import { getLateArrivalRowsForDate, getPlannedOffRowsForDate } from "./attendance.js";

function parseLocalDateTimeForToday(timeText) {
  const raw = String(timeText || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");

  const match = raw.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)$/i);
  if (!match) return null;

  let hour = Number(match[1]);
  const minute = match[2] == null ? 0 : Number(match[2]);
  const ampm = match[3].toLowerCase();

  if (hour < 1 || hour > 12 || minute < 0 || minute > 59) return null;

  if (ampm === "pm" && hour !== 12) hour += 12;
  if (ampm === "am" && hour === 12) hour = 0;

  const todayDb = getTodayDateStringInTimeZone(APP_TIMEZONE);
  const iso = `${todayDb}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00${APP_TIMEZONE_OFFSET}`;
  const d = new Date(iso);

  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

async function getUserWorkProfile(userId, orgId) {
  const { data, error } = await supabase
    .from("user_work_profiles")
    .select(
      "user_id, employment_type, shift_start_time, shift_end_time, working_hours",
    )
    .eq("user_id", userId)
    .maybeSingle();

  if (error) {
    console.error("getUserWorkProfile error:", error);
    return null;
  }

  return data || null;
}

function parseTimeValueToTodayIso(timeValue) {
  if (!timeValue) return null;

  const raw = String(timeValue).trim();

  // supports "21:30:00" or "21:30"
  const m24 = raw.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (m24) {
    const hour = Number(m24[1]);
    const minute = Number(m24[2]);
    const second = Number(m24[3] || 0);

    if (
      Number.isNaN(hour) ||
      Number.isNaN(minute) ||
      Number.isNaN(second) ||
      hour < 0 ||
      hour > 23 ||
      minute < 0 ||
      minute > 59 ||
      second < 0 ||
      second > 59
    ) {
      return null;
    }

    const todayDb = getTodayDateStringInTimeZone(APP_TIMEZONE);
    const iso = `${todayDb}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:${String(second).padStart(2, "0")}${APP_TIMEZONE_OFFSET}`;
    const d = new Date(iso);

    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString();
  }

  // fallback for old style like "10:30 AM"
  return parseLocalDateTimeForToday(raw);
}

async function getShiftStartIsoForUserToday(userId, orgId) {
  const workProfile = await getUserWorkProfile(userId, orgId);

  if (workProfile?.shift_start_time) {
    const customIso = parseTimeValueToTodayIso(workProfile.shift_start_time);
    if (customIso) return customIso;
  }

  return parseLocalDateTimeForToday(DEFAULT_SHIFT_START_TEXT);
}

function getShiftStartIsoForToday() {
  return parseLocalDateTimeForToday(DEFAULT_SHIFT_START_TEXT);
}

function getFirstLoginEvent(userEvents) {
  return userEvents.find((e) => e.action === "login") || null;
}

function minutesBetween(earlierIso, laterIso = new Date().toISOString()) {
  if (!earlierIso) return 0;
  const start = new Date(earlierIso);
  const end = new Date(laterIso);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return 0;
  return Math.max(0, Math.round((end.getTime() - start.getTime()) / 60000));
}

function formatDurationMinutes(totalMinutes) {
  const mins = Math.max(0, Number(totalMinutes || 0));
  const hours = Math.floor(mins / 60);
  const rem = mins % 60;
  if (hours === 0) return `${rem} min`;
  if (rem === 0) return `${hours}h`;
  return `${hours}h ${rem}m`;
}

function getLastLogoutEvent(events) {
  let lastLogout = null;
  for (const ev of events || []) {
    if (ev.action === "logout") {
      lastLogout = ev;
    }
  }
  return lastLogout;
}

function getAttendanceSummaryFromEvents(events, options = {}) {
  const shiftStartIso = options.shiftStartIso || getShiftStartIsoForToday();

  let workedMinutes = 0;
  let breakMinutes = 0;
  let workStart = null;
  let openBreak = null;
  let longestBreakMin = 0;
  let breakCount = 0;

  for (const ev of events || []) {
    if (ev.action === "login" || ev.action === "back") {
      if (!workStart) {
        workStart = ev.created_at;
      }
      if (ev.action === "back" && openBreak) {
        const oneBreak = minutesBetween(openBreak.created_at, ev.created_at);
        breakMinutes += oneBreak;
        if (oneBreak > longestBreakMin) longestBreakMin = oneBreak;
        openBreak = null;
      }
      continue;
    }

    if (ev.action === "break") {
      if (workStart) {
        workedMinutes += minutesBetween(workStart, ev.created_at);
        workStart = null;
      }
      openBreak = ev;
      breakCount += 1;
      continue;
    }

    if (ev.action === "logout") {
      if (workStart) {
        workedMinutes += minutesBetween(workStart, ev.created_at);
        workStart = null;
      }
      if (openBreak) {
        const oneBreak = minutesBetween(openBreak.created_at, ev.created_at);
        breakMinutes += oneBreak;
        if (oneBreak > longestBreakMin) longestBreakMin = oneBreak;
        openBreak = null;
      }
    }
  }

  if (workStart) {
    workedMinutes += minutesBetween(workStart);
  }

  if (openBreak) {
    const oneBreak = minutesBetween(openBreak.created_at);
    breakMinutes += oneBreak;
    if (oneBreak > longestBreakMin) longestBreakMin = oneBreak;
  }

  const firstLogin = getFirstLoginEvent(events);
  const lastLogout = getLastLogoutEvent(events);
  const latest = events?.length ? events[events.length - 1] : null;

  const lateMinutes = firstLogin
    ? Math.max(
        0,
        Math.round(
          (new Date(firstLogin.created_at) - new Date(shiftStartIso)) / 60000,
        ),
      )
    : 0;

  return {
    firstLogin,
    lastLogout,
    latest,
    workedMinutes,
    breakMinutes,
    breakCount,
    longestBreakMin,
    currentStatus: latest?.action || "no_update",
    lateMinutes,
    longShiftFlag: workedMinutes > LONG_SHIFT_THRESHOLD_MIN,
    longBreakFlag: longestBreakMin >= LONG_BREAK_THRESHOLD_MIN,
    possibleHalfDay:
      workedMinutes > 0 && workedMinutes < HALF_DAY_THRESHOLD_MIN,
  };
}

function getCurrentAttendanceDayRange() {
  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
  return getAttendanceDayUtcRange(attendanceDate);
}

function parseIsoToAttendanceDateString(isoString) {
  if (!isoString) return null;
  const d = new Date(isoString);
  if (Number.isNaN(d.getTime())) return null;
  return getAttendanceDayDateStringFromDate(d);
}

function getAttendanceMonthNavigation(monthQuery) {
  const todayAttendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const currentMonth = todayAttendanceDate.slice(0, 7);

  const selectedMonth = /^\d{4}-\d{2}$/.test(String(monthQuery || ""))
    ? String(monthQuery)
    : currentMonth;

  const [year, month] = selectedMonth.split("-").map(Number);

  const startDate = formatDateForDbFromParts(year, month, 1);

  const nextMonthYear = month === 12 ? year + 1 : year;
  const nextMonthNumber = month === 12 ? 1 : month + 1;
  const endDateExclusive = formatDateForDbFromParts(
    nextMonthYear,
    nextMonthNumber,
    1,
  );

  const prevMonthYear = month === 1 ? year - 1 : year;
  const prevMonthNumber = month === 1 ? 12 : month - 1;
  const prevMonth = `${prevMonthYear}-${String(prevMonthNumber).padStart(2, "0")}`;

  const nextMonth = `${nextMonthYear}-${String(nextMonthNumber).padStart(2, "0")}`;

  return {
    selectedMonth,
    currentMonth,
    prevMonth,
    nextMonth,
    startDate,
    endDateExclusive,
  };
}

function getWeekdayNameFromDateString(dateString) {
  const d = new Date(`${dateString}T00:00:00${APP_TIMEZONE_OFFSET}`);
  return d
    .toLocaleDateString("en-US", {
      timeZone: APP_TIMEZONE,
      weekday: "long",
    })
    .toLowerCase();
}

function getDefaultWorkExpectationForDate(reportDate) {
  const weekday = getWeekdayNameFromDateString(reportDate);

  if (weekday === "sunday") {
    return {
      expectedToWork: false,
      workDayWeight: 0,
      workMode: "off",
      source: "default",
      label: "Sunday off",
    };
  }

  if (weekday === "saturday") {
    return {
      expectedToWork: true,
      workDayWeight: 0.5,
      workMode: "half_day",
      source: "default",
      label: "Saturday half day",
    };
  }

  return {
    expectedToWork: true,
    workDayWeight: 1,
    workMode: "full_day",
    source: "default",
    label: "Working day",
  };
}

function resolveWorkExpectation({ reportDate, isOnLeave, overrideMode }) {
  if (overrideMode === "half_day") {
    return {
      expectedToWork: true,
      workDayWeight: 0.5,
      workMode: "half_day",
      source: "override",
      label: "Override: half day",
    };
  }

  if (overrideMode === "full_day") {
    return {
      expectedToWork: true,
      workDayWeight: 1,
      workMode: "full_day",
      source: "override",
      label: "Override: full day",
    };
  }

  if (isOnLeave) {
    return {
      expectedToWork: false,
      workDayWeight: 0,
      workMode: "off",
      source: "leave",
      label: "On leave",
    };
  }

  return getDefaultWorkExpectationForDate(reportDate);
}

function buildEmployeeMonthlyAttendanceSummaryFromData({
  events = [],
  leaveRows = [],
  lateRows = [],
  auditRows = [],
  startDate,
  endDateExclusive,
  redReportDates = [],
  shiftStartIso,
}) {
  const eventsByAttendanceDay = new Map();

  for (const ev of events || []) {
    const attendanceDate = parseIsoToAttendanceDateString(ev.created_at);
    if (!attendanceDate) continue;

    if (!eventsByAttendanceDay.has(attendanceDate)) {
      eventsByAttendanceDay.set(attendanceDate, []);
    }

    eventsByAttendanceDay.get(attendanceDate).push(ev);
  }

  for (const [, dayEvents] of eventsByAttendanceDay) {
    dayEvents.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
  }

  const nowDate = getAttendanceDayDateStringFromDate(new Date());

  let presentDays = 0;
  const leaveDays = leaveRows.length;

  let lateJoins = 0;
  let approvedLate = 0;
  let unapprovedLate = 0;
  let uninformedLate = 0;

  let totalLoginMinutes = 0;
  let loginDays = 0;

  let totalBreakMin = 0;
  let breakDays = 0;

  let longShiftCount = 0;
  let longBreakCount = 0;
  let possibleHalfDays = 0;

  for (
    let date = startDate;
    date < endDateExclusive;
    date = addDaysToDateString(date, 1)
  ) {
    const dayEvents = eventsByAttendanceDay.get(date) || [];
    if (!dayEvents.length) continue;

    const dayShiftStartIso =
      shiftStartIso || `${date}T10:30:00${APP_TIMEZONE_OFFSET}`;

    const summary = getAttendanceSummaryFromEvents(dayEvents, {
      shiftStartIso: dayShiftStartIso,
    });

    if (summary.firstLogin) {
      presentDays += 1;

      const firstLogin = new Date(summary.firstLogin.created_at);

      const loginTimeText = firstLogin.toLocaleTimeString("en-IN", {
        timeZone: APP_TIMEZONE,
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      });

      const [hh, mm] = loginTimeText.split(":").map(Number);

      if (!Number.isNaN(hh) && !Number.isNaN(mm)) {
        totalLoginMinutes += hh * 60 + mm;
        loginDays += 1;
      }
    }

    if (summary.breakMinutes > 0) {
      totalBreakMin += summary.breakMinutes;
      breakDays += 1;
    }

    if (summary.longShiftFlag) longShiftCount += 1;
    if (summary.longBreakFlag) longBreakCount += 1;
    if (summary.possibleHalfDay) possibleHalfDays += 1;
  }

  for (const row of lateRows || []) {
    const lateDate = row.late_date;
    const dayEvents = eventsByAttendanceDay.get(lateDate) || [];
    const firstLogin = getFirstLoginEvent(dayEvents);

    if (firstLogin) {
      const shiftStartForLate =
        row.shift_start_at || `${lateDate}T10:30:00${APP_TIMEZONE_OFFSET}`;

      const lateMinutes = Math.max(
        0,
        Math.round(
          (new Date(firstLogin.created_at) - new Date(shiftStartForLate)) /
            60000,
        ),
      );

      // If they actually logged in on time, ignore stale late_arrivals row.
      if (lateMinutes <= 10) {
        continue;
      }
    }

    lateJoins += 1;

    if (row.is_approved) {
      approvedLate += 1;
    } else {
      unapprovedLate += 1;
    }

    const isTimeUnsure =
      !row.expected_login_at || String(row.note || "").includes("TIME_UNSURE");

    if (isTimeUnsure) {
      uninformedLate += 1;
    }
  }

  const avgLoginTimeText =
    loginDays > 0
      ? (() => {
          const avgMinutes = Math.round(totalLoginMinutes / loginDays);
          const hh = Math.floor(avgMinutes / 60);
          const mm = avgMinutes % 60;

          const d = new Date();
          d.setHours(hh, mm, 0, 0);

          return d.toLocaleTimeString("en-IN", {
            timeZone: APP_TIMEZONE,
            hour: "numeric",
            minute: "2-digit",
            hour12: true,
          });
        })()
      : "-";

  const avgBreakMin = breakDays > 0 ? Math.round(totalBreakMin / breakDays) : 0;

  const pastLeaveDates = leaveRows
    .map((x) => x.off_date)
    .filter((d) => d < nowDate);

  const upcomingLeaveDates = leaveRows
    .map((x) => x.off_date)
    .filter((d) => d >= nowDate);

  const managerCorrectionCount = (auditRows || []).filter((row) => {
    const actionType = String(row.action_type || "");

    return (
      actionType.startsWith("mark_") ||
      actionType.startsWith("fix_") ||
      actionType.startsWith("force_") ||
      actionType.startsWith("remove_") ||
      actionType.startsWith("undo_") ||
      actionType.startsWith("reset_") ||
      actionType.startsWith("lock_") ||
      actionType.startsWith("unlock_")
    );
  }).length;

  let totalWorkingDays = 0;

  for (
    let date = startDate;
    date < endDateExclusive;
    date = addDaysToDateString(date, 1)
  ) {
    const weekday = getWeekdayNameFromDateString(date);

    if (weekday !== "sunday") {
      totalWorkingDays += 1;
    }
  }

  return {
    redReportDays: redReportDates.length,
    redReportDates,

    presentDays,
    leaveDays,
    pastLeaveDates,
    upcomingLeaveDates,

    lateJoins,
    approvedLate,
    unapprovedLate,
    uninformedLate,

    avgLoginTimeText,
    avgBreakMin,

    longShiftCount,
    longBreakCount,
    possibleHalfDays,

    managerCorrectionCount,
    totalWorkingDays,
  };
}

async function getEmployeeAttendanceOverview(userId, orgId, options = {}) {
  const todayAttendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const { startUtc, endUtc } = getCurrentAttendanceDayRange();
  const monthNav =
    options.monthNav || getAttendanceMonthNavigation(options.month);

  const { startDate, endDateExclusive } = monthNav;

  const [
    userResult,
    todayEventsResult,
    monthlyEventsResult,
    leaveResult,
    lateResult,
    auditResult,
    overrideResult,
    redReportDates,
  ] = await Promise.all([
    supabase
      .from("users")
      .select("id, name, role, phone_number")
      .eq("id", userId)
      .eq("org_id", orgId)
      .eq("is_active", true)
      .maybeSingle(),

    supabase
      .from("attendance_events")
      .select(
        "id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
      )
      .eq("user_id", userId)
      .eq("org_id", orgId)
      .gte("created_at", startUtc)
      .lt("created_at", endUtc)
      .order("created_at", { ascending: true }),

    supabase
      .from("attendance_events")
      .select(
        "id, user_id, action, created_at, duration_min, expected_duration_min, reason, note",
      )
      .eq("user_id", userId)
      .eq("org_id", orgId)
      .gte(
        "created_at",
        new Date(
          `${startDate}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
        ).toISOString(),
      )
      .lt(
        "created_at",
        new Date(
          `${endDateExclusive}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
        ).toISOString(),
      )
      .order("created_at", { ascending: true }),

    supabase
      .from("planned_time_off")
      .select("id, off_date, note")
      .eq("user_id", userId)
      .eq("org_id", orgId)
      .gte("off_date", startDate)
      .lt("off_date", endDateExclusive)
      .order("off_date", { ascending: true }),

    supabase
      .from("late_arrivals")
      .select(
        "id, late_date, expected_login_at, informed_at, shift_start_at, is_approved, note",
      )
      .eq("user_id", userId)
      .eq("org_id", orgId)
      .gte("late_date", startDate)
      .lt("late_date", endDateExclusive)
      .order("late_date", { ascending: true }),

    supabase
      .from("attendance_audit")
      .select(
        "id, action_type, old_value, new_value, note, created_at, acted_by_user_id",
      )
      .eq("target_user_id", userId)
      .eq("org_id", orgId)
      .gte(
        "created_at",
        new Date(
          `${startDate}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
        ).toISOString(),
      )
      .lt(
        "created_at",
        new Date(
          `${endDateExclusive}T${String(ATTENDANCE_DAY_START_HOUR).padStart(2, "0")}:00:00${APP_TIMEZONE_OFFSET}`,
        ).toISOString(),
      )
      .order("created_at", { ascending: false }),

    supabase
      .from("work_day_expectation_overrides")
      .select("id, override_date, mode")
      .eq("user_id", userId)
      .eq("org_id", orgId)
      .gte("override_date", startDate)
      .lt("override_date", endDateExclusive)
      .order("override_date", { ascending: true }),

    Promise.resolve([]),
  ]);

  if (userResult.error) throw userResult.error;
  if (todayEventsResult.error) throw todayEventsResult.error;
  if (monthlyEventsResult.error) throw monthlyEventsResult.error;
  if (leaveResult.error) throw leaveResult.error;
  if (lateResult.error) throw lateResult.error;
  if (auditResult.error) throw auditResult.error;
  if (overrideResult.error) throw overrideResult.error;

  const user = userResult.data;
  if (!user) {
    throw new Error("Employee not found");
  }

  const todayEvents = todayEventsResult.data || [];
  const monthlyEvents = monthlyEventsResult.data || [];
  const leaveRows = leaveResult.data || [];
  const lateRows = lateResult.data || [];
  const auditRows = auditResult.data || [];
  const overrideRows = overrideResult.data || [];
  const lateByDate = new Map((lateRows || []).map((x) => [x.late_date, x]));
  const leaveByDate = new Map((leaveRows || []).map((x) => [x.off_date, x]));
  const auditCountByDate = new Map();

  for (const row of auditRows || []) {
    const auditDate = parseIsoToAttendanceDateString(row.created_at);
    if (!auditDate) continue;
    auditCountByDate.set(auditDate, (auditCountByDate.get(auditDate) || 0) + 1);
  }

  const overrideByDate = new Map(
    (overrideRows || []).map((x) => [x.override_date, x.mode]),
  );

  const shiftStartIso = await getShiftStartIsoForUserToday(user.id, orgId);
  const todaySummary = getAttendanceSummaryFromEvents(todayEvents, {
    shiftStartIso,
  });
  const leaveToday = leaveByDate.get(todayAttendanceDate) || null;
  const lateToday = lateByDate.get(todayAttendanceDate) || null;

  const todayOverrideMode = overrideByDate.get(todayAttendanceDate) || null;

  const todayExpectation = resolveWorkExpectation({
    reportDate: todayAttendanceDate,
    isOnLeave: !!leaveToday,
    overrideMode: todayOverrideMode,
  });

  const effectiveTodayStatus = !todayExpectation.expectedToWork
    ? leaveToday
      ? "leave"
      : "off"
    : todaySummary.currentStatus;

  const effectiveLeaveToday = !todayExpectation.expectedToWork && !!leaveToday;

  const eventsByAttendanceDay = new Map();

  for (const ev of monthlyEvents) {
    const attendanceDate = parseIsoToAttendanceDateString(ev.created_at);
    if (!attendanceDate) continue;

    if (!eventsByAttendanceDay.has(attendanceDate)) {
      eventsByAttendanceDay.set(attendanceDate, []);
    }
    eventsByAttendanceDay.get(attendanceDate).push(ev);
  }

  const history = [];

  const allAttendanceDates = new Set([
    ...Array.from(eventsByAttendanceDay.keys()),
    ...leaveRows.map((x) => x.off_date),
    ...overrideRows.map((x) => x.override_date),
  ]);

  const sortedAttendanceDates = Array.from(allAttendanceDates).sort((a, b) =>
    a < b ? 1 : -1,
  );

  const monthlySummary = buildEmployeeMonthlyAttendanceSummaryFromData({
    events: monthlyEvents,
    leaveRows,
    lateRows,
    auditRows,
    startDate,
    endDateExclusive,
    redReportDates: redReportDates || [],
    shiftStartIso,
  });

  for (const attendanceDate of sortedAttendanceDates) {
    const dayEvents = eventsByAttendanceDay.get(attendanceDate) || [];
    const daySummary = getAttendanceSummaryFromEvents(dayEvents, {
      shiftStartIso,
    });
    const dayLate = lateByDate.get(attendanceDate) || null;
    const dayLeave = leaveByDate.get(attendanceDate) || null;

    const overrideMode = overrideByDate.get(attendanceDate) || null;

    const expectation = resolveWorkExpectation({
      reportDate: attendanceDate,
      isOnLeave: !!dayLeave,
      overrideMode,
    });

    const effectiveStatus = !expectation.expectedToWork
      ? dayLeave
        ? "leave"
        : "off"
      : daySummary.currentStatus;

    const effectiveLeaveText =
      !expectation.expectedToWork && dayLeave ? "Yes" : "No";

    const dayAuditCount = auditCountByDate.get(attendanceDate) || 0;

    history.push({
      attendance_date: attendanceDate,
      status: effectiveStatus,
      first_login_text: daySummary.firstLogin
        ? formatTimeOnly(daySummary.firstLogin.created_at)
        : "-",
      last_logout_text: daySummary.lastLogout
        ? formatTimeOnly(daySummary.lastLogout.created_at)
        : "-",
      worked_text: formatDurationMinutes(daySummary.workedMinutes),
      break_text: formatDurationMinutes(daySummary.breakMinutes),
      late_text:
        daySummary.lateMinutes > 10 ? `${daySummary.lateMinutes} min` : "No",
      late_approved:
        daySummary.lateMinutes > 10
          ? dayLate
            ? dayLate.is_approved
              ? "approved"
              : "not approved"
            : "no prior info"
          : "-",
      leave_text: effectiveLeaveText,
      flags:
        [
          daySummary.longShiftFlag ? "Long shift" : null,
          daySummary.longBreakFlag ? "Long break" : null,
          daySummary.possibleHalfDay ? "Half day" : null,
        ]
          .filter(Boolean)
          .join(", ") || "-",
      corrections: dayAuditCount,
      timeline: dayEvents.map((ev) => ({
        id: ev.id,
        action: ev.action,
        created_at: ev.created_at,
        time_text: formatTimeOnly(ev.created_at),
        reason: ev.reason || null,
        note: ev.note || null,
        expected_duration_min: ev.expected_duration_min || null,
      })),
    });
  }

  return {
    employee: user,
    today: {
      attendance_date: todayAttendanceDate,
      current_status: effectiveTodayStatus,
      first_login_text: todaySummary.firstLogin
        ? formatTimeOnly(todaySummary.firstLogin.created_at)
        : "-",
      last_logout_text: todaySummary.lastLogout
        ? formatTimeOnly(todaySummary.lastLogout.created_at)
        : "-",
      worked_text: formatDurationMinutes(todaySummary.workedMinutes),
      break_text: formatDurationMinutes(todaySummary.breakMinutes),
      break_count: todaySummary.breakCount,
      late_text:
        todaySummary.lateMinutes > 10
          ? `${todaySummary.lateMinutes} min`
          : "No",
      late_status:
        todaySummary.lateMinutes > 10
          ? lateToday
            ? lateToday.is_approved
              ? "approved"
              : "not approved"
            : "no prior info"
          : "-",
      leave_today: effectiveLeaveToday,
      long_shift_flag: todaySummary.longShiftFlag,
      long_break_flag: todaySummary.longBreakFlag,
      possible_half_day: todaySummary.possibleHalfDay,
      events: todayEvents.map((ev) => ({
        id: ev.id,
        action: ev.action,
        time_text: formatTimeOnly(ev.created_at),
        reason: ev.reason || null,
        note: ev.note || null,
        expected_duration_min: ev.expected_duration_min || null,
      })),
    },
    monthly: monthlySummary,
    recent_audit: auditRows.slice(0, 20).map((row) => ({
      id: row.id,
      action_type: row.action_type,
      note: row.note || "-",
      created_at_text: row.created_at ? formatDateTime(row.created_at) : "-",
    })),
    history,
  };
}

async function getAttendancePageData(orgId) {
  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
  const { startUtc, endUtc } = getCurrentAttendanceDayRange();

  const [
    { data: users, error: usersError },
    { data: events, error: eventsError },
    plannedOffRows,
    lateRows,
  ] = await Promise.all([
    supabase
      .from("users")
      .select("id, name, role")
      .eq("org_id", orgId)
      .eq("is_active", true)
      .order("name", { ascending: true }),

    supabase
      .from("attendance_events")
      .select(
        "id, user_id, action, duration_min, expected_duration_min, reason, note, created_at",
      )
      .eq("org_id", orgId)
      .gte("created_at", startUtc)
      .lt("created_at", endUtc)
      .order("created_at", { ascending: true }),

    getPlannedOffRowsForDate(attendanceDate, orgId),
    getLateArrivalRowsForDate(attendanceDate, orgId),
  ]);

  if (usersError) throw usersError;
  if (eventsError) throw eventsError;

  const plannedOff = plannedOffRows || [];
  const plannedOffUserIds = new Set(plannedOff.map((x) => x.user_id));
  const lateByUser = new Map((lateRows || []).map((x) => [x.user_id, x]));

  const eventsByUser = new Map();
  for (const ev of events || []) {
    if (!eventsByUser.has(ev.user_id)) {
      eventsByUser.set(ev.user_id, []);
    }
    eventsByUser.get(ev.user_id).push(ev);
  }

  const rows = await Promise.all(
    (users || []).map(async (user) => {
      const userEvents = eventsByUser.get(user.id) || [];
      const latest = userEvents[userEvents.length - 1] || null;
      const shiftStartIso = await getShiftStartIsoForUserToday(user.id, orgId);
      const summary = getAttendanceSummaryFromEvents(userEvents, {
        shiftStartIso,
      });
      const firstLogin = summary.firstLogin;
      const lateInfo = lateByUser.get(user.id) || null;

      let status = "no_update";
      if (plannedOffUserIds.has(user.id)) status = "leave";
      else if (latest?.action) status = latest.action;

      const flags = [];
      if (user.role !== "admin" && summary.longShiftFlag)
        flags.push("Long shift");
      if (user.role !== "admin" && summary.longBreakFlag)
        flags.push("Long break");
      if (lateInfo && !lateInfo.is_approved) flags.push("Late not approved");
      if (lateInfo && String(lateInfo.note || "").includes("TIME_UNSURE")) {
        flags.push("Time unsure");
      }

      return {
        user_id: user.id,
        name: user.name,
        role: user.role,
        status,
        since: latest?.created_at || null,
        since_text: latest?.created_at
          ? formatTimeOnly(latest.created_at)
          : plannedOffUserIds.has(user.id)
            ? "On leave today"
            : "-",
        worked_today_min: summary.workedMinutes || 0,
        worked_today_text: formatDurationMinutes(summary.workedMinutes || 0),
        break_today_min: summary.breakMinutes || 0,
        break_today_text: formatDurationMinutes(summary.breakMinutes || 0),
        first_login_at: firstLogin?.created_at || null,
        first_login_text: firstLogin?.created_at
          ? formatTimeOnly(firstLogin.created_at)
          : "-",
        late_status: lateInfo
          ? lateInfo.is_approved
            ? "Approved"
            : "Not approved"
          : firstLogin
            ? summary.lateMinutes > 10
              ? "No prior info"
              : "No"
            : "-",
        is_on_leave: plannedOffUserIds.has(user.id),
        flags,
        late_expected_login_text: lateInfo?.expected_login_at
          ? formatTimeOnly(lateInfo.expected_login_at)
          : String(lateInfo?.note || "").includes("TIME_UNSURE")
            ? "Time unsure"
            : "-",
        expected_shift_start_text: shiftStartIso
          ? formatTimeOnly(shiftStartIso)
          : "-",
      };
    }),
  );

  const summary = {
    logged_in_now: rows.filter(
      (x) => x.status === "login" || x.status === "back",
    ).length,
    on_break_now: rows.filter((x) => x.status === "break").length,
    not_logged_in_yet: rows.filter(
      (x) => x.role !== "admin" && x.status === "no_update",
    ).length,
    on_leave_today: rows.filter((x) => x.status === "leave").length,
    late_today: rows.filter(
      (x) =>
        x.late_status === "Approved" ||
        x.late_status === "Not approved" ||
        x.late_status === "No prior info",
    ).length,
    approved_late: rows.filter((x) => x.late_status === "Approved").length,
    unapproved_late: rows.filter((x) => x.late_status === "Not approved")
      .length,
    no_prior_info_late: rows.filter((x) => x.late_status === "No prior info")
      .length,
    long_break_flags: rows.filter((x) => x.flags.includes("Long break")).length,
    llong_shift_flags: rows.filter(
      (x) => x.role !== "admin" && x.flags.includes("Long shift"),
    ).length,
  };

  const groups = {
    on_break_now: rows.filter((x) => x.status === "break"),
    on_leave_today: rows.filter((x) => x.status === "leave"),
    expected_late: rows.filter(
      (x) => x.late_status === "Approved" || x.late_status === "Not approved",
    ),
    no_update_yet: rows.filter(
      (x) => x.role !== "admin" && x.status === "no_update",
    ),
    exceptions: rows.filter(
      (x) =>
        x.flags.length > 0 ||
        x.late_status === "Not approved" ||
        x.late_status === "No prior info",
    ),
  };
  return {
    attendance_date: attendanceDate,
    summary,
    rows,
    groups,
  };
}

export {
  getUserWorkProfile,
  formatDurationMinutes,
  getAttendanceMonthNavigation,
  getAttendancePageData,
  getDefaultWorkExpectationForDate,
  getEmployeeAttendanceOverview,
  getWeekdayNameFromDateString,
  resolveWorkExpectation,
};
