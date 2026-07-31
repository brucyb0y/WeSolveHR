// Data behind GET /account, copied verbatim from the original handler
// (lib/server/app.js lines 35384-36100). The handler also pre-rendered a
// few HTML fragments inline; those come back as strings so the view stays
// byte-for-byte what it was.

import { supabase } from "../server/supabase.js";
import { getAttendanceDayDateStringFromDate } from "../server/time.js";
import { isManagerOrAdmin } from "../server/users.js";
import { escapeHtml, formatDateOnly, formatDateTime } from "../ui/html.js";
import { getUserWorkProfile } from "./attendance-core.js";

async function getAccountPageData({ user }) {

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

  // Extra profile fields: Department / Designation / Notes come off the user
  // record (fall back to "-" when unset), while Time is the person's shift
  // window + working hours from their work profile.
  const workProfile = await getUserWorkProfile(user.id, user.org_id);
  const shortTime = (t) => (t ? String(t).slice(0, 5) : "");
  const timeParts = [];
  if (workProfile?.shift_start_time || workProfile?.shift_end_time) {
    const s = shortTime(workProfile.shift_start_time);
    const e = shortTime(workProfile.shift_end_time);
    timeParts.push(s && e ? `${s}–${e}` : s || e);
  }
  if (workProfile?.working_hours) timeParts.push(`${workProfile.working_hours} hrs`);
  const timeText = timeParts.join(" · ") || "-";
  const notesText = user.notes || "-";

  const feedbackHtml = feedbackItems?.length
    ? feedbackItems
        .map((item) => {
          const labelMap = {
            feedback: "Feedback",
            appreciation: "Appreciation",
            coaching: "Coaching",
            one_on_one: "1:1 Note",
          };

          return `
            <div class="timeline-item">
              <div class="timeline-badge">${escapeHtml(labelMap[item.type] || item.type)}</div>
              <div class="timeline-date">${formatDateTime(item.created_at)}</div>
              <div class="timeline-note">${escapeHtml(item.note || item.manager_comment || "")}</div>
            </div>
          `;
        })
        .join("")
    : `<div class="empty-state">No feedback yet</div>`;

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

  const futureLeaveHtml = futureLeaveRows.length
    ? futureLeaveRows
        .map(
          (row) => `
          <tr>
            <td>${escapeHtml(row.users?.name || "-")}</td>
            <td>${escapeHtml(formatDateOnly(row.off_date))}</td>
            <td>${escapeHtml(row.created_by?.name || "-")}</td>
            <td>${escapeHtml(row.note || "-")}</td>
          </tr>
        `,
        )
        .join("")
    : `<tr><td colspan="4" class="empty-cell">No upcoming leave found</td></tr>`;

  const teamFeedbackHtml = teamFeedbackRows.length
    ? teamFeedbackRows
        .map((row) => {
          const labelMap = {
            feedback: "Feedback",
            appreciation: "Appreciation",
            coaching: "Coaching",
            one_on_one: "1:1 Note",
          };

          return `
            <tr>
              <td>${escapeHtml(row.users?.name || "-")}</td>
              <td>${escapeHtml(labelMap[row.type] || row.type || "-")}</td>
              <td>${escapeHtml(row.note || row.manager_comment || "-")}</td>
              <td>${escapeHtml(row.created_by?.name || "-")}</td>
              <td>${escapeHtml(formatDateTime(row.created_at))}</td>
            </tr>
          `;
        })
        .join("")
    : `<tr><td colspan="5" class="empty-cell">No team feedback found</td></tr>`;

  const teamAppraisalHtml = teamAppraisalRows.length
    ? teamAppraisalRows
        .map(
          (row) => `
          <tr>
            <td>${escapeHtml(row.users?.name || "-")}</td>
            <td>${escapeHtml(row.rating || "-")}</td>
            <td>${escapeHtml(row.strengths || "-")}</td>
            <td>${escapeHtml(row.improvement_areas || "-")}</td>
            <td>${escapeHtml(row.manager_comment || "-")}</td>
            <td>${escapeHtml(formatDateTime(row.created_at))}</td>
          </tr>
        `,
        )
        .join("")
    : `<tr><td colspan="6" class="empty-cell">No team appraisals found</td></tr>`;

  const leaveSummaryHtml = leaveSummaryRows.length
    ? leaveSummaryRows
        .map(
          (row) => `
          <tr>
            <td>${escapeHtml(row.name || "-")}</td>
            <td>${escapeHtml(String(row.totalLeaveCount ?? 0))}</td>
            <td>${escapeHtml(String(row.upcomingLeaveCount ?? 0))}</td>
            <td>${escapeHtml(row.nextLeaveDate ? formatDateOnly(row.nextLeaveDate) : "-")}</td>
          </tr>
        `,
        )
        .join("")
    : `<tr><td colspan="4" class="empty-cell">No leave summary found</td></tr>`;

  return {
    user,
    isAdminView,
    appraisal,
    timeText,
    notesText,
    ptoRemaining,
    sickRemaining,
    feedbackHtml,
    futureLeaveHtml,
    leaveSummaryHtml,
    teamAppraisalHtml,
    teamFeedbackHtml,
  };
}

export { getAccountPageData };
