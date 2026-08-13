"use client";

// Add / Edit Blocker modal — replaces openBlockerModal / openBlockerDetail /
// closeBlockerModal / saveBlocker / updateBlocker.
//
// Two details carried over:
//   * Resolution Status is edit-only. The original rendered the field with
//     style="display:none" and revealed it when opening an existing blocker, so
//     a new blocker always starts at the default rather than letting you create
//     one already "resolved".
//   * Side matters beyond this page: `client_side` blockers are the ones the
//     customer dashboard shows ("Pending From Your Side"); `internal` stays
//     hidden from them. See the client_blockers filter in lib/data/client-view.js.

import { useState } from "react";
import { useRouter } from "next/navigation";
import { WorkModal, TextField, SelectField, TextAreaField } from "./WorkModal";

const SIDES = [
  { value: "internal", label: "Internal" },
  { value: "client_side", label: "Client-side" },
];

const PRIORITIES = [
  { value: "medium", label: "Medium" },
  { value: "high", label: "High" },
  { value: "low", label: "Low" },
];

const STATUSES = [
  { value: "open", label: "Open" },
  { value: "in_progress", label: "In Progress" },
  { value: "resolved", label: "Resolved" },
];

const EMPTY = {
  title: "",
  blocker_side: "internal",
  priority: "medium",
  owner_user_id: "",
  related_work_item_id: "",
  resolution_status: "open",
  description: "",
};

export default function BlockerModal({
  clientId,
  blocker,
  users,
  workItems,
  onClose,
}) {
  const router = useRouter();
  const isEdit = !!blocker;

  const [form, setForm] = useState(() =>
    blocker
      ? {
          title: blocker.title || "",
          blocker_side: blocker.blocker_side || "internal",
          priority: blocker.priority || "medium",
          owner_user_id: blocker.owner_user_id
            ? String(blocker.owner_user_id)
            : "",
          related_work_item_id: blocker.related_work_item_id
            ? String(blocker.related_work_item_id)
            : "",
          resolution_status: blocker.resolution_status || "open",
          description: blocker.description || "",
        }
      : EMPTY,
  );
  const [saving, setSaving] = useState(false);

  const set = (key) => (value) => setForm((f) => ({ ...f, [key]: value }));

  const ownerOptions = [
    { value: "", label: "Select owner" },
    ...users.map((u) => ({ value: String(u.id), label: u.name })),
  ];

  const workItemOptions = [
    { value: "", label: "No related work item" },
    ...workItems.map((w) => ({
      value: String(w.id),
      label: `#${w.id} · ${w.title}`,
    })),
  ];

  async function save() {
    if (!form.title.trim()) {
      alert("Title is required");
      return;
    }

    const payload = {
      title: form.title.trim(),
      blocker_side: form.blocker_side,
      priority: form.priority,
      owner_user_id: form.owner_user_id || null,
      related_work_item_id: form.related_work_item_id || null,
      description: form.description.trim(),
      // Only sent when editing — a new blocker takes the server default.
      ...(isEdit ? { resolution_status: form.resolution_status } : {}),
    };

    setSaving(true);
    try {
      const res = await fetch(
        isEdit
          ? `/api/clients/${clientId}/blockers/${blocker.id}`
          : `/api/clients/${clientId}/blockers`,
        {
          method: isEdit ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to save blocker");
        return;
      }

      onClose();
      router.refresh();
    } catch {
      alert("Failed to save blocker");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title={isEdit ? "Edit Blocker" : "Add Blocker"}
      saveLabel="Save Blocker"
      saving={saving}
      onSave={save}
      onClose={onClose}
    >
      <TextField
        label="Title"
        placeholder="Example: Waiting on client API credentials"
        value={form.title}
        onChange={set("title")}
      />
      <SelectField
        label="Side"
        options={SIDES}
        value={form.blocker_side}
        onChange={set("blocker_side")}
      />
      <SelectField
        label="Priority"
        options={PRIORITIES}
        value={form.priority}
        onChange={set("priority")}
      />
      <SelectField
        label="Owner"
        options={ownerOptions}
        value={form.owner_user_id}
        onChange={set("owner_user_id")}
      />
      <SelectField
        label="Related Work Item"
        options={workItemOptions}
        value={form.related_work_item_id}
        onChange={set("related_work_item_id")}
      />
      {isEdit ? (
        <SelectField
          label="Resolution Status"
          options={STATUSES}
          value={form.resolution_status}
          onChange={set("resolution_status")}
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
