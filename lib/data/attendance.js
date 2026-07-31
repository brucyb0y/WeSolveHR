// Attendance reads shared by the dashboard, the attendance screens and the
// WhatsApp command handlers. Extracted verbatim from the original monolith.

import { supabase } from "../server/supabase.js";
import { getAttendanceDayDateStringFromDate, getAttendanceDayUtcRange } from "../server/time.js";

async function getAttendanceEventsForAttendanceDay(
  attendanceDateString,
  orgId,
) {
  const { startUtc, endUtc } = getAttendanceDayUtcRange(attendanceDateString);

  const { data, error } = await supabase
    .from("attendance_events")
    .select(
      "id, org_id, user_id, action, created_at, duration_min, expected_duration_min, reason, note, acted_by_phone, target_phone",
    )
    .eq("org_id", orgId)
    .gte("created_at", startUtc)
    .lt("created_at", endUtc)
    .order("created_at", { ascending: true });

  if (error) {
    throw error;
  }

  return data || [];
}

async function getTodayAttendanceEventsForAllUsers(orgId) {
  const attendanceDate = getAttendanceDayDateStringFromDate(new Date());
  return getAttendanceEventsForAttendanceDay(attendanceDate, orgId);
}

async function getPlannedOffRowsForDate(dateString, orgId) {
  const { data, error } = await supabase
    .from("planned_time_off")
    .select(
      `
      id,
      org_id,
      user_id,
      off_date,
      note,
      users!planned_time_off_user_id_fkey(name)
    `,
    )
    .eq("off_date", dateString)
    .eq("org_id", orgId);

  if (error) {
    throw error;
  }

  return data || [];
}

async function getLateArrivalRowsForDate(dateString, orgId) {
  const { data, error } = await supabase
    .from("late_arrivals")
    .select(
      `
      id,
      org_id,
      user_id,
      late_date,
      expected_login_at,
      informed_at,
      shift_start_at,
      is_approved,
      note,
      users!late_arrivals_user_id_fkey(name)
    `,
    )
    .eq("late_date", dateString)
    .eq("org_id", orgId);

  if (error) {
    throw error;
  }

  return data || [];
}

export {
  getAttendanceEventsForAttendanceDay,
  getLateArrivalRowsForDate,
  getPlannedOffRowsForDate,
  getTodayAttendanceEventsForAllUsers,
};
