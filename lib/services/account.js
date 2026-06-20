// Account page data. Ported from the GET /account handler in lib/server/app.js:
// the user's own appraisal + feedback timeline + leave balances, plus the
// admin-only team tables when the viewer is a manager/admin. Queries are kept
// identical to the original.

import { supabase } from "@/lib/db/supabase.js";
import { isManagerOrAdmin } from "@/lib/services/auth.js";
import { getAttendanceDayDateStringFromDate } from "@/lib/server/app.js";

export async function getAccountData(user) {
  const isAdminView = isManagerOrAdmin(user);

  const { data: appraisal } = await supabase
    .from("employee_feedback")
    .select("*")
    .eq("user_id", user.id)
    .eq("type", "appraisal")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  const { data: feedbackItems } = await supabase
    .from("employee_feedback")
    .select("*")
    .eq("user_id", user.id)
    .neq("type", "appraisal")
    .order("created_at", { ascending: false })
    .limit(20);

  const ptoRemaining = (user.pto_total || 12) - (user.pto_used || 0);
  const sickRemaining = (user.sick_total || 12) - (user.sick_used || 0);

  let futureLeaveRows = [];
  let teamFeedbackRows = [];
  let teamAppraisalRows = [];
  let leaveSummaryRows = [];

  if (isAdminView) {
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

    futureLeaveRows = futureLeaveResult.data || [];
    teamFeedbackRows = teamFeedbackResult.data || [];
    teamAppraisalRows = teamAppraisalResult.data || [];

    const allUsers = usersResult.data || [];
    const allLeaves = allPlannedLeaveResult.data || [];

    leaveSummaryRows = allUsers.map((u) => {
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
  }

  return {
    isAdminView,
    appraisal,
    feedbackItems: feedbackItems || [],
    ptoRemaining,
    sickRemaining,
    futureLeaveRows,
    teamFeedbackRows,
    teamAppraisalRows,
    leaveSummaryRows,
  };
}
