"use client";

// Add / Edit Work Item modal — replaces openWorkItemModal /
// openWorkItemDetail / closeWorkItemModal / createWorkItem /
// saveWorkItemChanges / closeWorkItemDetail.
//
// The original split this into two dialogs: a create modal and a separate
// "detail" view that also allowed edits. They took the same fields against the
// same endpoints, so this is one component with an isEdit branch — POST to
// /api/client-work-items, PATCH to /api/client-work-items/:id.
//
// "Depends On" excludes the item being edited, so a work item cannot be made to
// depend on itself (the original built the list from all work items and relied
// on nobody trying).

import { useState } from "react";
import { useRouter } from "next/navigation";
import { WorkModal, TextField, SelectField, TextAreaField } from "./WorkModal";

const PRIORITIES = [
  { value: "medium", label: "Medium" },
  { value: "high", label: "High" },
  { value: "low", label: "Low" },
];

const STATUSES = [
  { value: "todo", label: "Todo" },
  { value: "in_progress", label: "In Progress" },
  { value: "done", label: "Done" },
];

const EMPTY = {
  title: "",
  owner_user_id: "",
  priority: "medium",
  due_date: "",
  dependency_work_item_id: "",
  milestone_id: "",
  status: "todo",
  description: "",
};

export default function WorkItemModal({
  clientId,
  workItem,
  users,
  workItems,
  milestones,
  onClose,
}) {
  const router = useRouter();
  const isEdit = !!workItem;

  const [form, setForm] = useState(() =>
    workItem
      ? {
          title: workItem.title || "",
          owner_user_id: workItem.owner_user_id
            ? String(workItem.owner_user_id)
            : "",
          priority: workItem.priority || "medium",
          due_date: workItem.due_date || "",
          dependency_work_item_id: workItem.dependency_work_item_id
            ? String(workItem.dependency_work_item_id)
            : "",
          milestone_id: workItem.milestone_id
            ? String(workItem.milestone_id)
            : "",
          status: workItem.status || "todo",
          description: workItem.description || "",
        }
      : EMPTY,
  );
  const [saving, setSaving] = useState(false);

  const set = (key) => (value) => setForm((f) => ({ ...f, [key]: value }));

  const ownerOptions = [
    { value: "", label: "Select owner" },
    ...users.map((u) => ({ value: String(u.id), label: u.name })),
  ];

  const dependencyOptions = [
    { value: "", label: "No dependency" },
    ...workItems
      .filter((w) => !isEdit || w.id !== workItem.id)
      .map((w) => ({ value: String(w.id), label: `#${w.id} · ${w.title}` })),
  ];

  const milestoneOptions = [
    { value: "", label: "No milestone" },
    ...milestones.map((m) => ({ value: String(m.id), label: m.title })),
  ];

  async function save() {
    if (!form.title.trim()) {
      alert("Title is required");
      return;
    }

    const payload = {
      // client_id is create-only: the PATCH handler builds its patch from an
      // explicit whitelist and never reads it, and the row's client cannot move.
      ...(isEdit ? {} : { client_id: clientId }),
      title: form.title.trim(),
      owner_user_id: form.owner_user_id ? Number(form.owner_user_id) : null,
      priority: form.priority,
      due_date: form.due_date || null,
      dependency_work_item_id: form.dependency_work_item_id
        ? Number(form.dependency_work_item_id)
        : null,
      milestone_id: form.milestone_id ? Number(form.milestone_id) : null,
      description: form.description.trim(),
      ...(isEdit ? { status: form.status } : {}),
    };

    setSaving(true);
    try {
      const res = await fetch(
        isEdit
          ? `/api/client-work-items/${workItem.id}`
          : "/api/client-work-items",
        {
          method: isEdit ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to save work item");
        return;
      }

      onClose();
      router.refresh();
    } catch {
      alert("Failed to save work item");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title={isEdit ? "Edit Work Item" : "Add Work Item"}
      saveLabel="Save Work Item"
      saving={saving}
      onSave={save}
      onClose={onClose}
    >
      <TextField
        label="Title"
        placeholder="Example: Build landing page"
        value={form.title}
        onChange={set("title")}
      />
      <SelectField
        label="Owner"
        options={ownerOptions}
        value={form.owner_user_id}
        onChange={set("owner_user_id")}
      />
      <SelectField
        label="Priority"
        options={PRIORITIES}
        value={form.priority}
        onChange={set("priority")}
      />
      <TextField
        label="Due Date"
        type="date"
        value={form.due_date}
        onChange={set("due_date")}
      />
      <SelectField
        label="Depends On"
        options={dependencyOptions}
        value={form.dependency_work_item_id}
        onChange={set("dependency_work_item_id")}
      />
      <SelectField
        label="Milestone"
        options={milestoneOptions}
        value={form.milestone_id}
        onChange={set("milestone_id")}
      />
      {isEdit ? (
        <SelectField
          label="Status"
          options={STATUSES}
          value={form.status}
          onChange={set("status")}
        />
      ) : null}
      <TextAreaField
        label="Description"
        value={form.description}
        onChange={set("description")}
      />
    </WorkModal>
  );
}
