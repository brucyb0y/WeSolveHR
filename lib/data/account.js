// Data for /account — the query bodies of app.get("/account").
//
// Everything is unchanged: the latest appraisal, the 20 most recent non-
// appraisal feedback items, leave balances derived from the user row, the
// shift window / working hours off the work profile, and — for managers and
// admins only — the four org-wide tables.

import {
  supabase,
  getUserWorkProfile,
  getAttendanceDayDateStringFromDate,
} from "@/lib/server/app.js";

const shortTime = (t) => (t ? String(t).slice(0, 5) : "");

export async function getAccountData(user, isAdminView) {
  const [{ data: appraisal }, { data: feedbackItems }, workProfile] =
    await Promise.all([
      supabase
        .from("employee_feedback")
        .select("*")
        .eq("user_id", user.id)
        .eq("type", "appraisal")
        .order("created_at", { ascending: false })
        .limit(1)
        .maybeSingle(),

      supabase
        .from("employee_feedback")
        .select("*")
        .eq("user_id", user.id)
        .neq("type", "appraisal")
        .order("created_at", { ascending: false })
        .limit(20),

      getUserWorkProfile(user.id, user.org_id),
    ]);

  const timeParts = [];
  if (workProfile?.shift_start_time || workProfile?.shift_end_time) {
    const s = shortTime(workProfile.shift_start_time);
    const e = shortTime(workProfile.shift_end_time);
    timeParts.push(s && e ? `${s}–${e}` : s || e);
  }
  if (workProfile?.working_hours) {
    timeParts.push(`${workProfile.working_hours} hrs`);
  }

  const base = {
    appraisal: appraisal || null,
    feedbackItems: feedbackItems || [],
    ptoRemaining: (user.pto_total || 12) - (user.pto_used || 0),
    sickRemaining: (user.sick_total || 12) - (user.sick_used || 0),
    timeText: timeParts.join(" · ") || "-",
    notesText: user.notes || "-",
    futureLeaveRows: [],
    teamFeedbackRows: [],
    teamAppraisalRows: [],
    leaveSummaryRows: [],
  };

  if (!isAdminView) return base;

  const today = getAttendanceDayDateStringFromDate(new Date());

  const [
    futureLeaveResult,
    teamFeedbackResult,
    teamAppraisalResult,
    usersResult,
    allPlannedLeaveResult,
  ] = await Promise.all([
    supabase
      .from("planned_time_off")
      .select(
        `
        id,
        off_date,
        note,
        created_at,
        users!planned_time_off_user_id_fkey(name),
        created_by:users!planned_time_off_created_by_user_id_fkey(name)
      `,
      )
      .eq("org_id", user.org_id)
      .gte("off_date", today)
      .order("off_date", { ascending: true })
      .limit(30),

    supabase
      .from("employee_feedback")
      .select(
        `
        id,
        type,
        note,
        manager_comment,
        created_at,
        users!employee_feedback_user_id_fkey(name),
        created_by:users!employee_feedback_created_by_user_id_fkey(name)
      `,
      )
      .eq("org_id", user.org_id)
      .neq("type", "appraisal")
      .order("created_at", { ascending: false })
      .limit(30),

    supabase
      .from("employee_feedback")
      .select(
        `
        id,
        rating,
        strengths,
        improvement_areas,
        manager_comment,
        created_at,
        users!employee_feedback_user_id_fkey(name),
        created_by:users!employee_feedback_created_by_user_id_fkey(name)
      `,
      )
      .eq("org_id", user.org_id)
      .eq("type", "appraisal")
      .order("created_at", { ascending: false })
      .limit(20),

    supabase
      .from("users")
      .select("id, name")
      .eq("org_id", user.org_id)
      .eq("is_active", true)
      .order("name", { ascending: true }),

    supabase
      .from("planned_time_off")
      .select(
        `
        id,
        user_id,
        off_date,
        users!planned_time_off_user_id_fkey(name)
      `,
      )
      .eq("org_id", user.org_id)
      .order("off_date", { ascending: false }),
  ]);

  const allUsers = usersResult.data || [];
  const allLeaves = allPlannedLeaveResult.data || [];

  const leaveSummaryRows = allUsers.map((u) => {
    const userLeaves = allLeaves.filter((x) => x.user_id === u.id);
    const upcomingLeaves = userLeaves
      .filter((x) => x.off_date >= today)
      .sort((a, b) => String(a.off_date).localeCompare(String(b.off_date)));

    return {
      name: u.name,
      totalLeaveCount: userLeaves.length,
      upcomingLeaveCount: upcomingLeaves.length,
      nextLeaveDate: upcomingLeaves[0]?.off_date || null,
    };
  });

  return {
    ...base,
    futureLeaveRows: futureLeaveResult.data || [],
    teamFeedbackRows: teamFeedbackResult.data || [],
    teamAppraisalRows: teamAppraisalResult.data || [],
    leaveSummaryRows,
  };
}
