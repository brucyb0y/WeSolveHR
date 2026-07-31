// Narrative report generation (daily and multi-day), extracted verbatim
// from the original monolith.

import { APP_TIMEZONE } from "../server/runtime.js";
import { supabase } from "../server/supabase.js";
import { addDaysToDateString, getAttendanceDayDateStringFromDate, getAttendanceDayUtcRange } from "../server/time.js";
import { resolveWorkExpectation } from "./attendance-core.js";
import { getPlannedOffRowsForDate } from "./attendance.js";

function getReportCardStatus({
  reportDate,
  isOnLeave,
  expectedToWork,
  workMode,
  hasTaskUpdates,
  hasExtraWork,
}) {
  if (!expectedToWork) {
    return {
      status: isOnLeave ? "leave" : "off",
      cardClass: isOnLeave ? "report-card-leave" : "report-card-off",
      reason: isOnLeave ? "On leave" : "Not expected to work",
    };
  }

  if (!hasTaskUpdates && !hasExtraWork) {
    return {
      status: "missing",
      cardClass: "report-card-missing",
      reason:
        workMode === "half_day"
          ? "Expected half day, but no task or extra work update"
          : "Expected full day, but no task or extra work update",
    };
  }

  if (!hasTaskUpdates || !hasExtraWork) {
    return {
      status: "partial",
      cardClass: "report-card-partial",
      reason:
        workMode === "half_day"
          ? "Half-day update is partial"
          : "Day update is partial",
    };
  }

  return {
    status: "full",
    cardClass: "report-card-full",
    reason: "Updated",
  };
}

function formatWorkDayWeight(weight) {
  if (Number(weight) === 1) return "1";
  if (Number(weight) === 0.5) return "0.5";
  return "0";
}

function getReportDateString(date = new Date()) {
  return getAttendanceDayDateStringFromDate(date);
}

function getReportDayUtcRange(reportDate) {
  return getAttendanceDayUtcRange(reportDate);
}

async function getDailyReportNotes({ orgId, reportDate, userId = null }) {
  let query = supabase
    .from("daily_report_notes")
    .select("id, org_id, user_id, report_date, note, created_at")
    .eq("org_id", orgId)
    .eq("report_date", reportDate)
    .order("created_at", { ascending: true });

  if (userId) {
    query = query.eq("user_id", userId);
  }

  const { data, error } = await query;

  if (error) {
    console.error("getDailyReportNotes error:", error);
    return [];
  }

  return data || [];
}

async function getWorkDayOverrideRowsForDate({
  orgId,
  reportDate,
  userId = null,
}) {
  let query = supabase
    .from("work_day_expectation_overrides")
    .select(
      "id, org_id, user_id, override_date, mode, note, created_by_user_id",
    )
    .eq("org_id", orgId)
    .eq("override_date", reportDate);

  if (userId) {
    query = query.eq("user_id", userId);
  }

  const { data, error } = await query;

  if (error) {
    console.error("getWorkDayOverrideRowsForDate error:", error);
    return [];
  }

  return data || [];
}

async function getOpenBlockedCountsForUsers(orgId, userIds = []) {
  const safeUserIds = Array.from(
    new Set((userIds || []).map((x) => Number(x)).filter(Boolean)),
  );

  if (!safeUserIds.length) {
    return new Map();
  }

  const { data, error } = await supabase
    .from("task_owners")
    .select(
      `
      user_id,
      task_id,
      tasks!inner(id, org_id, status)
    `,
    )
    .eq("org_id", orgId)
    .in("user_id", safeUserIds);

  if (error) {
    console.error("getOpenBlockedCountsForUsers error:", error);
    return new Map();
  }

  const counts = new Map();

  for (const userId of safeUserIds) {
    counts.set(userId, { open: 0, blocked: 0 });
  }

  for (const row of data || []) {
    const task = row.tasks;
    if (!task || task.org_id !== orgId) continue;
    if (["done", "archived", "cancelled"].includes(task.status)) continue;

    const current = counts.get(row.user_id) || { open: 0, blocked: 0 };
    current.open += 1;
    if (task.status === "blocked") current.blocked += 1;
    counts.set(row.user_id, current);
  }

  return counts;
}

function formatShortDate(dateString) {
  if (!dateString) return "-";

  const d = new Date(`${dateString}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return String(dateString);

  return d.toLocaleDateString("en-IN", {
    timeZone: APP_TIMEZONE,
    day: "numeric",
    month: "short",
  });
}

function summarizeProgressDelta(taskNarratives) {
  let totalDelta = 0;

  for (const item of taskNarratives || []) {
    const from = Number(item.fromProgress);
    const to = Number(item.toProgress);

    if (!Number.isNaN(from) && !Number.isNaN(to) && to > from) {
      totalDelta += to - from;
    }
  }

  return totalDelta;
}

function buildCompactUserMeta(userReport) {
  const touched = (userReport.taskNarratives || []).length;
  const delta = summarizeProgressDelta(userReport.taskNarratives || []);
  const blocked = Number(userReport.summary?.blocked || 0);
  const hasExtra = (userReport.extraWork || []).length > 0;

  const parts = [];
  parts.push(`${touched} touched`);
  if (delta > 0) parts.push(`+${delta}%`);
  if (blocked > 0) parts.push(`${blocked} blocked`);
  if (hasExtra) parts.push("extra");

  return parts.join(" · ");
}

function extractCompactChangeChips(entries) {
  const chipMap = new Map();

  for (const entry of entries || []) {
    const fieldName = String(entry.field_name || "").toLowerCase();
    const changeType = String(entry.change_type || "").toLowerCase();
    const oldValue = entry.old_value || {};
    const newValue = entry.new_value || {};

    if (fieldName === "deadline" || changeType === "deadline_change") {
      chipMap.set("deadline", {
        key: "deadline",
        label: "deadline",
        detail: `${formatShortDate(oldValue.deadline)} → ${formatShortDate(newValue.deadline)}`,
      });
    }

    if (fieldName === "owner" || changeType === "owner_change") {
      const oldOwners = Array.isArray(oldValue.owners)
        ? oldValue.owners.join(", ")
        : "-";
      const newOwners = Array.isArray(newValue.owners)
        ? newValue.owners.join(", ")
        : "-";

      chipMap.set("owner", {
        key: "owner",
        label: "owner",
        detail: `${oldOwners} → ${newOwners}`,
      });
    }

    if (fieldName === "status" || changeType === "status_change") {
      chipMap.set("status", {
        key: "status",
        label: "status",
        detail: `${oldValue.status || "-"} → ${newValue.status || "-"}`,
      });
    }

    if (fieldName === "priority") {
      chipMap.set("priority", {
        key: "priority",
        label: "priority",
        detail: `${oldValue.priority || "-"} → ${newValue.priority || "-"}`,
      });
    }
  }

  return Array.from(chipMap.values());
}

function classifyReportUsers(users) {
  const full = [];
  const partial = [];
  const missing = [];
  const onLeave = [];
  const off = [];

  for (const user of users || []) {
    if (user.reportStatus === "leave") {
      onLeave.push(user.userName);
      continue;
    }

    if (user.reportStatus === "off") {
      off.push(user.userName);
      continue;
    }

    if (user.reportStatus === "full") {
      full.push(user.userName);
      continue;
    }

    if (user.reportStatus === "partial") {
      partial.push(user.userName);
      continue;
    }

    missing.push(user.userName);
  }

  return { full, partial, missing, onLeave, off };
}

function buildTaskNarrativeFromHistoryEntries(entries, taskTitle, taskNoOrId) {
  if (!entries || !entries.length) return null;

  let firstProgress = null;
  let lastProgress = null;
  let finalStatus = null;
  let blockerAdded = null;
  let blockerCleared = false;
  const notes = [];

  for (const entry of entries) {
    const oldValue = entry.old_value || {};
    const newValue = entry.new_value || {};
    const changeType = String(entry.change_type || "");
    const fieldName = String(entry.field_name || "");

    if (oldValue.progress != null && firstProgress == null) {
      firstProgress = oldValue.progress;
    }

    if (newValue.progress != null) {
      lastProgress = newValue.progress;
    }

    if (newValue.status) {
      finalStatus = String(newValue.status).toLowerCase();
    }

    if (
      (fieldName === "status" || fieldName === "blocker_note") &&
      newValue.blocker_note
    ) {
      blockerAdded = newValue.blocker_note;
    }

    if (
      oldValue.blocker_note &&
      (newValue.blocker_note == null || newValue.blocker_note === "")
    ) {
      blockerCleared = true;
    }

    const possibleNote = newValue.note || oldValue.note || null;

    if (possibleNote && !notes.includes(possibleNote)) {
      notes.push(possibleNote);
    }

    if (
      changeType === "edit" &&
      fieldName === "blocker_note" &&
      newValue.blocker_note
    ) {
      if (!notes.includes(newValue.blocker_note)) {
        notes.push(newValue.blocker_note);
      }
    }
  }

  let sentence = `Task #${taskNoOrId} — ${taskTitle}: `;

  if (
    firstProgress != null &&
    lastProgress != null &&
    firstProgress !== lastProgress
  ) {
    sentence += `Worked on this from ${firstProgress}% to ${lastProgress}%`;
  } else if (finalStatus === "done") {
    sentence += "Completed this task";
  } else if (blockerAdded) {
    sentence += "Worked on this and got blocked";
  } else if (blockerCleared) {
    sentence += "Cleared blocker and resumed progress";
  } else {
    sentence += "Updated this task";
  }

  if (finalStatus === "done" && notes.length) {
    sentence += ` by ${notes[0]}`;
  } else if (blockerAdded) {
    sentence += ` waiting on ${blockerAdded}`;
  } else if (notes.length) {
    sentence += ` and ${notes[0]}`;
  }

  sentence += ".";

  return {
    sentence,
    fromProgress: firstProgress,
    toProgress: lastProgress,
    finalStatus,
    blockerAdded,
    blockerCleared,
    notePreview: notes[0] || null,
  };
}

async function getDailyTaskNarratives({ orgId, reportDate, userId = null }) {
  const { startUtc, endUtc } = getReportDayUtcRange(reportDate);

  let query = supabase
    .from("task_history")
    .select(
      `
      id,
      org_id,
      task_id,
      changed_by_user_id,
      change_type,
      field_name,
      old_value,
      new_value,
      created_at
    `,
    )
    .eq("org_id", orgId)
    .gte("created_at", startUtc)
    .lt("created_at", endUtc)
    .order("created_at", { ascending: true });

  if (userId) {
    query = query.eq("changed_by_user_id", userId);
  }

  const { data: historyRows, error: historyError } = await query;

  if (historyError) {
    console.error("getDailyTaskNarratives history error:", historyError);
    return [];
  }

  const history = (historyRows || []).filter((row) => {
    const changeType = String(row.change_type || "");
    return [
      "task_created",
      "progress_change",
      "status_change",
      "edit",
      "owner_change",
      "deadline_change",
    ].includes(changeType);
  });

  if (!history.length) return [];

  const taskIds = [...new Set(history.map((x) => x.task_id).filter(Boolean))];
  if (!taskIds.length) return [];

  const { data: taskRows, error: taskError } = await supabase
    .from("tasks")
    .select("id, task_no, title")
    .eq("org_id", orgId)
    .in("id", taskIds);

  if (taskError) {
    console.error("getDailyTaskNarratives task fetch error:", taskError);
    return [];
  }

  const taskMap = new Map((taskRows || []).map((task) => [task.id, task]));
  const grouped = new Map();

  for (const row of history) {
    const task = taskMap.get(row.task_id);
    if (!task) continue;

    const key = `${row.changed_by_user_id}::${row.task_id}`;

    if (!grouped.has(key)) {
      grouped.set(key, {
        userId: row.changed_by_user_id,
        taskId: row.task_id,
        taskNo: task.task_no || task.id,
        title: task.title,
        entries: [],
      });
    }

    grouped.get(key).entries.push(row);
  }

  const out = [];

  for (const group of grouped.values()) {
    const narrative = buildTaskNarrativeFromHistoryEntries(
      group.entries,
      group.title,
      group.taskNo,
    );

    if (!narrative) continue;

    out.push({
      userId: group.userId,
      taskId: group.taskId,
      taskNo: group.taskNo,
      title: group.title,
      sentence: narrative.sentence,
      fromProgress: narrative.fromProgress,
      toProgress: narrative.toProgress,
      finalStatus: narrative.finalStatus,
      notePreview: narrative.notePreview,
      compactChanges: extractCompactChangeChips(group.entries),
    });
  }

  out.sort((a, b) => {
    if (a.userId !== b.userId) return a.userId - b.userId;
    return a.taskNo - b.taskNo;
  });

  return out;
}

function emptyUserDailyReport(user) {
  return {
    userId: user.id,
    userName: user.name,
    taskNarratives: [],
    extraWork: [],
    summary: {
      open: 0,
      blocked: 0,
    },
    isOnLeave: false,
    expectedToWork: false,
    workDayWeight: 0,
    workMode: "off",
    workRuleSource: "default",
    reportStatus: "off",
    reportCardClass: "report-card-off",
    reportReason: "Not expected to work",
  };
}

async function getDailyNarrativeReport({
  orgId,
  reportDate,
  userId = null,
  includeUsers = true,
}) {
  let users = [];

  if (includeUsers) {
    let usersQuery = supabase
      .from("users")
      .select("id, name, role")
      .eq("org_id", orgId)
      .eq("is_active", true)
      .order("name", { ascending: true });

    if (userId) {
      usersQuery = usersQuery.eq("id", userId);
    }

    const { data: userRows, error: usersError } = await usersQuery;
    if (usersError) {
      throw usersError;
    }

    users = userRows || [];
  }

  const [taskNarratives, extraNotes, plannedOffRows, overrideRows] =
    await Promise.all([
      getDailyTaskNarratives({ orgId, reportDate, userId }),
      getDailyReportNotes({ orgId, reportDate, userId }),
      getPlannedOffRowsForDate(reportDate, orgId),
      getWorkDayOverrideRowsForDate({ orgId, reportDate, userId }),
    ]);

  if (!includeUsers) {
    const emptyUsers = [];
    return {
      reportDate,
      users: emptyUsers,
      compliance: classifyReportUsers(emptyUsers),
    };
  }

  const leaveSet = new Set((plannedOffRows || []).map((x) => x.user_id));

  const narrativesByUser = new Map();
  for (const item of taskNarratives) {
    if (!narrativesByUser.has(item.userId)) {
      narrativesByUser.set(item.userId, []);
    }
    narrativesByUser.get(item.userId).push(item);
  }

  const notesByUser = new Map();
  for (const note of extraNotes) {
    if (!notesByUser.has(note.user_id)) {
      notesByUser.set(note.user_id, []);
    }
    notesByUser.get(note.user_id).push(note.note);
  }

  const overridesByUser = new Map();
  for (const row of overrideRows || []) {
    overridesByUser.set(row.user_id, row);
  }

  const countsByUser = await getOpenBlockedCountsForUsers(
    orgId,
    (users || []).map((u) => u.id),
  );

  const resultUsers = [];

  for (const user of users || []) {
    const row = emptyUserDailyReport(user);

    row.taskNarratives = narrativesByUser.get(user.id) || [];
    row.extraWork = notesByUser.get(user.id) || [];
    row.summary = countsByUser.get(user.id) || { open: 0, blocked: 0 };
    row.isOnLeave = leaveSet.has(user.id);

    const overrideMode = overridesByUser.get(user.id)?.mode || null;

    const expectation = resolveWorkExpectation({
      reportDate,
      isOnLeave: row.isOnLeave,
      overrideMode,
    });

    row.expectedToWork = expectation.expectedToWork;
    row.workDayWeight = expectation.workDayWeight;
    row.workMode = expectation.workMode;
    row.workRuleSource = expectation.source;

    const hasTaskUpdates = row.taskNarratives.length > 0;
    const hasExtraWork = row.extraWork.length > 0;

    const cardStatus = getReportCardStatus({
      reportDate,
      isOnLeave: row.isOnLeave,
      expectedToWork: row.expectedToWork,
      workMode: row.workMode,
      hasTaskUpdates,
      hasExtraWork,
    });

    row.reportStatus = cardStatus.status;
    row.reportCardClass = cardStatus.cardClass;
    row.reportReason = cardStatus.reason;

    row.compactMeta = `${buildCompactUserMeta(row)} · day ${formatWorkDayWeight(row.workDayWeight)}`;

    resultUsers.push(row);
  }

  return {
    reportDate,
    users: resultUsers,
    compliance: classifyReportUsers(resultUsers),
  };
}

async function getMultiDayNarrativeReport({
  orgId,
  userId,
  days = 7,
  endDate = null,
}) {
  const safeDays = Math.max(1, Math.min(31, Number(days || 7)));
  const finalDate = endDate || getReportDateString();

  const dailyReports = [];

  for (let i = 0; i < safeDays; i += 1) {
    const reportDate = addDaysToDateString(finalDate, -i);

    const daily = await getDailyNarrativeReport({
      orgId,
      reportDate,
      userId,
    });

    dailyReports.push(daily);
  }

  return {
    mode: "multi_day_user",
    userId,
    endDate: finalDate,
    days: safeDays,
    dailyReports,
  };
}

export {
  getDailyNarrativeReport,
  getMultiDayNarrativeReport,
  getReportDateString,
};
