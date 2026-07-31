// Task workspace data, extracted verbatim from the original monolith.

import { supabase } from "../server/supabase.js";

async function getUserTaskWorkspaceData({ userId, orgId, tab = "pending" }) {
  const { data: user, error: userError } = await supabase
    .from("users")
    .select("id, name, role, is_active")
    .eq("org_id", orgId)
    .eq("id", userId)
    .maybeSingle();

  if (userError) {
    console.error("getUserTaskWorkspaceData user error:", userError);
    throw userError;
  }

  if (!user) {
    return null;
  }

  const { data: ownerRows, error: ownerError } = await supabase
    .from("task_owners")
    .select("task_id")
    .eq("org_id", orgId)
    .eq("user_id", userId);

  if (ownerError) {
    console.error("getUserTaskWorkspaceData owner rows error:", ownerError);
    throw ownerError;
  }

  const taskIds = (ownerRows || []).map((x) => x.task_id);

  let tasks = [];
  if (taskIds.length) {
    const { data: taskRows, error: taskError } = await supabase
      .from("tasks")
      .select(
        `
        id,
        org_id,
        task_no,
        title,
        business,
        area,
        status,
        progress,
        priority,
        deadline,
        blocker_note,
        waiting_on_user_id,
        updated_at
      `,
      )
      .eq("org_id", orgId)
      .in("id", taskIds)
      .order("deadline", { ascending: true, nullsFirst: false });

    if (taskError) {
      console.error("getUserTaskWorkspaceData tasks error:", taskError);
      throw taskError;
    }

    tasks = taskRows || [];
  }

  const { data: allOwnerRows, error: allOwnerError } = taskIds.length
    ? await supabase
        .from("task_owners")
        .select(
          `
          task_id,
          user_id,
          users!task_owners_user_id_fkey(id, name)
        `,
        )
        .eq("org_id", orgId)
        .in("task_id", taskIds)
    : { data: [], error: null };

  if (allOwnerError) {
    console.error("getUserTaskWorkspaceData all owners error:", allOwnerError);
    throw allOwnerError;
  }

  const ownersByTaskId = {};
  for (const row of allOwnerRows || []) {
    if (!ownersByTaskId[row.task_id]) ownersByTaskId[row.task_id] = [];
    ownersByTaskId[row.task_id].push({
      user_id: row.user_id,
      name: row.users?.name || "",
    });
  }

  const { data: historyRows, error: historyError } = taskIds.length
    ? await supabase
        .from("task_history")
        .select(
          `
          id,
          task_id,
          changed_by_user_id,
          change_type,
          field_name,
          old_value,
          new_value,
          created_at,
          changer:users!task_history_changed_by_user_id_fkey(name)
        `,
        )
        .eq("org_id", orgId)
        .in("task_id", taskIds)
        .order("created_at", { ascending: false })
    : { data: [], error: null };

  if (historyError) {
    console.error("getUserTaskWorkspaceData history error:", historyError);
    throw historyError;
  }

  const historyByTaskId = {};
  for (const row of historyRows || []) {
    if (!historyByTaskId[row.task_id]) historyByTaskId[row.task_id] = [];
    historyByTaskId[row.task_id].push({
      id: row.id,
      task_id: row.task_id,
      change_type: row.change_type,
      field_name: row.field_name,
      old_value: row.old_value || {},
      new_value: row.new_value || {},
      created_at: row.created_at,
      changed_by_name: row.changer?.name || "",
    });
  }

  const enrichedTasks = tasks.map((task) => {
    const owners = ownersByTaskId[task.id] || [];
    const history = historyByTaskId[task.id] || [];
    const latestHistory = history[0] || null;

    return {
      ...task,
      owners,
      owner_names: owners.map((x) => x.name).filter(Boolean),
      assignee_name: owners
        .map((x) => x.name)
        .filter(Boolean)
        .join(", "),
      latest_update_text: latestHistory
        ? renderUserWorkspaceHistoryLine(latestHistory)
        : "No updates yet",
      latest_update_at: latestHistory?.created_at || null,
      latest_updated_by: latestHistory?.changed_by_name || "",
      mini_history: history.slice(0, 3),
    };
  });

  const { data: blockedOnMeRows, error: blockedOnMeError } = await supabase
    .from("tasks")
    .select(
      `
      id,
      org_id,
      task_no,
      title,
      detail,
      priority,
      status,
      progress,
      deadline,
      blocker_note,
      blocked_reason,
      business,
      area,
      assigned_to_user_id,
      waiting_on_user_id,
      waiting_since,
      created_by_user_id,
      last_updated_by_user_id
    `,
    )
    .eq("org_id", orgId)
    .eq("waiting_on_user_id", userId)
    .eq("status", "blocked")
    .order("updated_at", { ascending: false });

  if (blockedOnMeError) {
    console.error(
      "getUserTaskWorkspaceData blockedOnMe error:",
      blockedOnMeError,
    );
    throw blockedOnMeError;
  }

  const blockedOnMeTaskIds = [
    ...new Set((blockedOnMeRows || []).map((task) => task.id).filter(Boolean)),
  ];

  const { data: blockedOnMeOwnerRows, error: blockedOnMeOwnerError } =
    blockedOnMeTaskIds.length
      ? await supabase
          .from("task_owners")
          .select(
            `
          task_id,
          user_id,
          users!task_owners_user_id_fkey(id, name)
        `,
          )
          .eq("org_id", orgId)
          .in("task_id", blockedOnMeTaskIds)
      : { data: [], error: null };

  if (blockedOnMeOwnerError) {
    console.error(
      "getUserTaskWorkspaceData blockedOnMe owners error:",
      blockedOnMeOwnerError,
    );
    throw blockedOnMeOwnerError;
  }

  const blockedOnMeOwnersByTaskId = new Map();

  for (const row of blockedOnMeOwnerRows || []) {
    const taskId = row.task_id;
    if (!blockedOnMeOwnersByTaskId.has(taskId)) {
      blockedOnMeOwnersByTaskId.set(taskId, []);
    }
    blockedOnMeOwnersByTaskId.get(taskId).push({
      user_id: row.user_id,
      name: row.users?.name || "",
    });
  }

  const { data: blockedOnMeHistoryRows, error: blockedOnMeHistoryError } =
    blockedOnMeTaskIds.length
      ? await supabase
          .from("task_history")
          .select(
            `
          id,
          task_id,
          changed_by_user_id,
          change_type,
          field_name,
          old_value,
          new_value,
          created_at,
          changer:users!task_history_changed_by_user_id_fkey(name)
        `,
          )
          .eq("org_id", orgId)
          .in("task_id", blockedOnMeTaskIds)
          .order("created_at", { ascending: false })
      : { data: [], error: null };

  if (blockedOnMeHistoryError) {
    console.error(
      "getUserTaskWorkspaceData blockedOnMe history error:",
      blockedOnMeHistoryError,
    );
    throw blockedOnMeHistoryError;
  }

  const blockedOnMeHistoryByTaskId = new Map();

  for (const row of blockedOnMeHistoryRows || []) {
    if (!blockedOnMeHistoryByTaskId.has(row.task_id)) {
      blockedOnMeHistoryByTaskId.set(row.task_id, []);
    }
    blockedOnMeHistoryByTaskId.get(row.task_id).push({
      ...row,
      changed_by_name: row.changer?.name || "",
    });
  }

  const blockedOnMeTasks = (blockedOnMeRows || []).map((task) => {
    const owners = blockedOnMeOwnersByTaskId.get(task.id) || [];
    const taskHistory = blockedOnMeHistoryByTaskId.get(task.id) || [];
    const latestHistory = taskHistory[0] || null;

    return {
      ...task,
      owner_names: owners.map((owner) => owner.name).filter(Boolean),
      latest_update_text: latestHistory
        ? renderUserWorkspaceHistoryLine(latestHistory)
        : "No updates yet",
      latest_updated_by: latestHistory?.changed_by_name || "",
      latest_update_at: latestHistory?.created_at || null,
      mini_history: taskHistory.slice(0, 3),
    };
  });

  const blockedOnMeUniqueTasks = blockedOnMeTasks.filter(
    (task, index, arr) => arr.findIndex((x) => x.id === task.id) === index,
  );

  const todayStart = new Date();
  todayStart.setHours(0, 0, 0, 0);
  const todayIso = todayStart.toISOString();

  const tomorrowStart = new Date(todayStart);
  tomorrowStart.setDate(tomorrowStart.getDate() + 1);
  const tomorrowIso = tomorrowStart.toISOString();

  const pendingTasks = enrichedTasks.filter(
    (task) =>
      !["done", "cancelled", "archived", "blocked"].includes(
        String(task.status || "").toLowerCase(),
      ),
  );

  const blockedTasks = enrichedTasks.filter(
    (task) => String(task.status || "").toLowerCase() === "blocked",
  );

  const deletedTasks = enrichedTasks.filter(
    (task) => String(task.status || "").toLowerCase() === "cancelled",
  );

  const doneTodayTaskIds = new Set(
    (historyRows || [])
      .filter((row) => {
        const newStatus = row?.new_value?.status || row?.new_value?.["status"];
        return (
          row.change_type === "status_change" &&
          String(newStatus || "").toLowerCase() === "done" &&
          row.created_at >= todayIso &&
          row.created_at < tomorrowIso
        );
      })
      .map((row) => row.task_id),
  );

  const doneTodayTasks = enrichedTasks.filter((task) =>
    doneTodayTaskIds.has(task.id),
  );

  const taskMap = new Map(enrichedTasks.map((task) => [task.id, task]));

  const progressUpdates = (historyRows || [])
    .filter((row) =>
      ["progress_change", "status_change", "edit"].includes(row.change_type),
    )
    .filter((row) => {
      const task = taskMap.get(row.task_id);
      return !!task;
    })
    .map((row) => {
      const task = taskMap.get(row.task_id);

      return {
        id: row.id,
        task_id: row.task_id,
        task_no: task?.task_no || row.task_id,
        title: task?.title || "",
        change_type: row.change_type,
        field_name: row.field_name,
        old_value: row.old_value || {},
        new_value: row.new_value || {},
        created_at: row.created_at,
        changed_by_name: row.changer?.name || "",
      };
    });

  const tabs = {
    pending: pendingTasks,
    blocked: blockedTasks,
    blocked_on_me: blockedOnMeUniqueTasks,
    done_today: doneTodayTasks,
    deleted: deletedTasks,
    progress_updates: progressUpdates,
  };

  return {
    user,
    selectedTab: tab,
    counts: {
      pending: pendingTasks.length,
      blocked: blockedTasks.length,
      blocked_on_me: blockedOnMeUniqueTasks.length,
      done_today: doneTodayTasks.length,
      deleted: deletedTasks.length,
      progress_updates: progressUpdates.length,
    },
    tabs,
  };
}

function renderUserWorkspaceHistoryLine(item) {
  const oldValue = item.old_value || {};
  const newValue = item.new_value || {};

  if (item.change_type === "progress_change") {
    const note = newValue.note || oldValue.note || "";
    return note
      ? `Progress: ${oldValue.progress ?? 0}% → ${newValue.progress ?? 0}% • ${note}`
      : `Progress: ${oldValue.progress ?? 0}% → ${newValue.progress ?? 0}%`;
  }

  if (item.change_type === "status_change") {
    const note = newValue.note || oldValue.note || "";
    return note
      ? `Status: ${oldValue.status || "-"} → ${newValue.status || "-"} • ${note}`
      : `Status: ${oldValue.status || "-"} → ${newValue.status || "-"}`;
  }

  if (item.change_type === "owner_change") {
    const oldOwners = Array.isArray(oldValue.owners)
      ? oldValue.owners.join(", ")
      : "-";
    const newOwners = Array.isArray(newValue.owners)
      ? newValue.owners.join(", ")
      : "-";
    return `Owners: ${oldOwners} → ${newOwners}`;
  }

  if (item.change_type === "deadline_change") {
    return `Deadline: ${oldValue.deadline || "-"} → ${newValue.deadline || "-"}`;
  }

  if (item.change_type === "edit") {
    if (item.field_name === "blocker_note") {
      return `Blocker updated: ${newValue.blocker_note || newValue.note || "-"}`;
    }

    if (item.field_name === "title") {
      return `Title: ${oldValue.title || "-"} → ${newValue.title || "-"}`;
    }

    if (item.field_name === "detail") {
      return `Detail updated`;
    }

    if (item.field_name === "priority") {
      return `Priority: ${oldValue.priority || "-"} → ${newValue.priority || "-"}`;
    }

    if (item.field_name === "business") {
      return `Business: ${oldValue.business || "-"} → ${newValue.business || "-"}`;
    }

    if (item.field_name === "area") {
      return `Area: ${oldValue.area || "-"} → ${newValue.area || "-"}`;
    }

    if (item.field_name === "deadline") {
      return `Deadline: ${oldValue.deadline || "-"} → ${newValue.deadline || "-"}`;
    }

    if (String(item.field_name || "").startsWith("clear_")) {
      return `${item.field_name.replace(/^clear_/, "").replace(/_/g, " ")} cleared`;
    }

    return `${item.field_name || "field"} updated`;
  }

  return item.change_type || "Updated";
}

export {
  getUserTaskWorkspaceData,
  renderUserWorkspaceHistoryLine,
};
