// Task-history line/detail formatters, ported from lib/server/app.js. Pure and
// client-safe.
//
// renderUserWorkspaceHistoryLine works on the snake_case history rows that
// getUserTaskWorkspaceData returns (change_type/field_name/old_value/new_value).
// renderTaskHistoryDetail works on the camelCase shape the /api/reports/task
// endpoint returns (changeType/fieldName/oldValue/newValue).

export function renderUserWorkspaceHistoryLine(item) {
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
    if (String(item.field_name || "").startsWith("clear_")) {
      return `${item.field_name.replace(/^clear_/, "").replace(/_/g, " ")} cleared`;
    }
    return `${item.field_name || "field"} updated`;
  }

  return item.change_type || "Updated";
}

// Modal history line, ported from renderUserWorkspaceTaskHistoryDetail() (the
// camelCase /api/reports/task shape). Returns text that may contain newlines.
export function renderTaskHistoryDetail(item) {
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};

  if (item.changeType === "progress_change") {
    return (
      "Progress: " +
      (oldValue.progress ?? 0) +
      "% → " +
      (newValue.progress ?? 0) +
      "%" +
      (newValue.note ? "\nNote: " + newValue.note : "")
    );
  }

  if (item.changeType === "status_change") {
    return (
      "Status: " +
      (oldValue.status || "-") +
      " → " +
      (newValue.status || "-") +
      (newValue.note ? "\nNote: " + newValue.note : "")
    );
  }

  if (item.changeType === "owner_change") {
    const oldOwners = Array.isArray(oldValue.owners)
      ? oldValue.owners.join(", ")
      : "-";
    const newOwners = Array.isArray(newValue.owners)
      ? newValue.owners.join(", ")
      : "-";
    return "Owners: " + oldOwners + " → " + newOwners;
  }

  if (item.changeType === "deadline_change") {
    return "Deadline: " + (oldValue.deadline || "-") + " → " + (newValue.deadline || "-");
  }

  if (item.fieldName === "title")
    return "Title: " + (oldValue.title || "-") + " → " + (newValue.title || "-");
  if (item.fieldName === "detail") return "Detail updated";
  if (item.fieldName === "priority")
    return "Priority: " + (oldValue.priority || "-") + " → " + (newValue.priority || "-");
  if (item.fieldName === "business")
    return "Business: " + (oldValue.business || "-") + " → " + (newValue.business || "-");
  if (item.fieldName === "area")
    return "Area: " + (oldValue.area || "-") + " → " + (newValue.area || "-");
  if (item.fieldName === "blocker_note") {
    return [
      "Blocker: " + (newValue.blocker_note || "-"),
      newValue.note ? "Note: " + newValue.note : null,
    ]
      .filter(Boolean)
      .join("\n");
  }
  if (item.fieldName) {
    return (
      (item.fieldName || "Field") +
      ": " +
      JSON.stringify(oldValue) +
      " → " +
      JSON.stringify(newValue)
    );
  }

  return JSON.stringify(newValue || {});
}
