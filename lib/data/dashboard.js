// Data behind GET /dashboard, extracted verbatim from the original monolith
// (getDashboardData, lines 32789-33022).

import { supabase } from "../server/supabase.js";
import { getAttendanceDayDateStringFromDate } from "../server/time.js";
import { getLateArrivalRowsForDate, getPlannedOffRowsForDate, getTodayAttendanceEventsForAllUsers } from "./attendance.js";

async function getDashboardData(orgId) {
  const today = getAttendanceDayDateStringFromDate(new Date());

  const [
    { data: users, error: usersError },
    { data: tasks, error: tasksError },
    { data: ownerRows, error: ownerError },
    attendanceRows,
    plannedOffRows,
    lateRows,
    reportPageData,
  ] = await Promise.all([
    supabase
      .from("users")
      .select("id, name, role, is_active")
      .eq("org_id", orgId)
      .eq("is_active", true)
      .order("name", { ascending: true }),

    supabase
      .from("tasks")
      .select(
        `
        id,
        org_id,
        task_no,
        title,
        priority,
        status,
        progress,
        deadline,
        waiting_on_user_id,
        updated_at,
        business,
        area
      `,
      )
      .eq("org_id", orgId),

    supabase.from("task_owners").select("task_id, user_id").eq("org_id", orgId),

    getTodayAttendanceEventsForAllUsers(orgId),
    getPlannedOffRowsForDate(today, orgId),
    getLateArrivalRowsForDate(today, orgId),
    Promise.resolve(null),
  ]);

  if (usersError) throw usersError;
  if (tasksError) throw tasksError;
  if (ownerError) throw ownerError;

  const ownersByTaskId = {};
  for (const row of ownerRows || []) {
    if (!ownersByTaskId[row.task_id]) ownersByTaskId[row.task_id] = [];
    ownersByTaskId[row.task_id].push(row.user_id);
  }

  const usersById = {};
  for (const user of users || []) {
    usersById[user.id] = user;
  }

  const activeTasks = (tasks || []).filter(
    (t) => !["done", "cancelled", "archived"].includes(t.status || "open"),
  );

  const todayDate = new Date(`${today}T00:00:00Z`);

  const overdueTasks = activeTasks.filter((t) => {
    if (!t.deadline) return false;
    return new Date(`${t.deadline}T00:00:00Z`) < todayDate;
  });

  const blockedTasks = activeTasks.filter((t) => t.status === "blocked");

  const notStartedTasks = activeTasks.filter(
    (t) => !t.status || t.status === "open" || t.status === "pending",
  );

  const highPriorityTasks = activeTasks.filter((t) =>
    ["high", "urgent"].includes((t.priority || "").toLowerCase()),
  );

  const staleTasks = activeTasks.filter((t) => {
    if (!t.updated_at) return false;
    const updated = new Date(t.updated_at);
    const diffDays = (Date.now() - updated.getTime()) / (1000 * 60 * 60 * 24);
    return diffDays >= 5;
  });

  const plannedOff = plannedOffRows || [];
  const lateEntries = lateRows || [];
  const attendanceEvents = attendanceRows || [];

  const plannedOffUserIds = new Set(plannedOff.map((x) => x.user_id));

  const latestAttendanceByUser = new Map();
  for (const ev of attendanceEvents) {
    latestAttendanceByUser.set(ev.user_id, ev);
  }

  let employeesOnline = 0;
  let employeesOnBreak = 0;
  let employeesLoggedOut = 0;
  let employeesNoAttendance = 0;

  for (const user of users || []) {
    if (plannedOffUserIds.has(user.id)) continue;

    const latest = latestAttendanceByUser.get(user.id);

    if (!latest) {
      employeesNoAttendance += 1;
      continue;
    }

    if (latest.action === "break") employeesOnBreak += 1;
    else if (latest.action === "logout") employeesLoggedOut += 1;
    else if (latest.action === "login" || latest.action === "back")
      employeesOnline += 1;
  }

  const approvedLateCount = lateEntries.filter((x) => x.is_approved).length;
  const unapprovedLateCount = lateEntries.filter((x) => !x.is_approved).length;

  let missingReportsToday = 0;
  let redReportDays = 0;

  if (reportPageData?.rows?.length) {
    for (const row of reportPageData.rows) {
      if (row.report_status === "missing") missingReportsToday += 1;
      if (row.red_flag) redReportDays += 1;
    }
  }

  const user_task_stats = (users || []).map((user) => {
    const ownedTasks = activeTasks.filter((task) =>
      (ownersByTaskId[task.id] || []).includes(user.id),
    );

    const open_count = ownedTasks.length;

    const blocked_count = ownedTasks.filter(
      (t) => t.status === "blocked",
    ).length;

    const not_started_count = ownedTasks.filter(
      (t) => !t.status || t.status === "open" || t.status === "pending",
    ).length;

    const overdue_count = ownedTasks.filter((t) => {
      if (!t.deadline) return false;
      return new Date(`${t.deadline}T00:00:00Z`) < todayDate;
    }).length;

    const high_priority_count = ownedTasks.filter((t) =>
      ["high", "urgent"].includes((t.priority || "").toLowerCase()),
    ).length;

    const stale_count = ownedTasks.filter((t) => {
      if (!t.updated_at) return false;
      const updated = new Date(t.updated_at);
      const diffDays = (Date.now() - updated.getTime()) / (1000 * 60 * 60 * 24);
      return diffDays >= 5;
    }).length;

    const waiting_on_them_count = activeTasks.filter(
      (t) =>
        t.status === "blocked" &&
        Number(t.waiting_on_user_id) === Number(user.id),
    ).length;

    const load_score =
      open_count +
      overdue_count * 3 +
      blocked_count * 2 +
      high_priority_count * 2 +
      stale_count +
      waiting_on_them_count * 2;

    let health = "Healthy";
    if (load_score >= 35) health = "Critical";
    else if (load_score >= 22) health = "High Risk";
    else if (load_score >= 12) health = "Watch";
    else health = "Healthy";

    return {
      user_id: user.id,
      name: user.name,
      role: user.role,
      open_count,
      blocked_count,
      not_started_count,
      overdue_count,
      high_priority_count,
      stale_count,
      waiting_on_them_count,
      load_score,
      health,
    };
  });

  const summary = {
    open_tasks: activeTasks.length,
    overdue_tasks: overdueTasks.length,
    blocked_tasks: blockedTasks.length,
    not_started_tasks: notStartedTasks.length,
    high_priority_tasks: highPriorityTasks.length,
    stale_tasks: staleTasks.length,
    team_members: (users || []).length,
    employees_online: employeesOnline,
    employees_on_break: employeesOnBreak,
    employees_logged_out: employeesLoggedOut,
    employees_no_attendance: employeesNoAttendance,
    employees_on_leave: plannedOff.length,
    late_today_approved: approvedLateCount,
    late_today_unapproved: unapprovedLateCount,
    missing_reports_today: missingReportsToday,
    red_report_days: redReportDays,
  };

  return {
    summary,
    user_task_stats,
    task_groups: {
      overdue: overdueTasks
        .sort((a, b) => new Date(a.deadline) - new Date(b.deadline))
        .slice(0, 20),
      blocked: blockedTasks.slice(0, 20),
      stale: staleTasks.slice(0, 20),
      high_priority: highPriorityTasks.slice(0, 20),
    },
  };
}

export {
  getDashboardData,
};
