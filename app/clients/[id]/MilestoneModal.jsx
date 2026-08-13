"use client";

// Add / Edit Milestone modal — replaces openMilestoneModal /
// openMilestoneEditModal / closeMilestoneModal / saveMilestone.

import { useState } from "react";
import { useRouter } from "next/navigation";
import { WorkModal, TextField, SelectField, TextAreaField } from "./WorkModal";

const STATUSES = [
  { value: "planned", label: "Planned" },
  { value: "in_progress", label: "In Progress" },
  { value: "done", label: "Done" },
  { value: "closed", label: "Closed" },
];

const EMPTY = { title: "", due_date: "", status: "planned", notes: "" };

export default function MilestoneModal({ clientId, milestone, onClose }) {
  const router = useRouter();
  const isEdit = !!milestone;

  const [form, setForm] = useState(() =>
    milestone
      ? {
          title: milestone.title || "",
          due_date: milestone.due_date || "",
          status: milestone.status || "planned",
          notes: milestone.notes || "",
        }
      : EMPTY,
  );
  const [saving, setSaving] = useState(false);

  const set = (key) => (value) => setForm((f) => ({ ...f, [key]: value }));

  async function save() {
    if (!form.title.trim()) {
      alert("Title is required");
      return;
    }

    setSaving(true);
    try {
      const res = await fetch(
        isEdit
          ? `/api/clients/${clientId}/milestones/${milestone.id}`
          : `/api/clients/${clientId}/milestones`,
        {
          method: isEdit ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ...form, title: form.title.trim() }),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to save milestone");
        return;
      }

      onClose();
      router.refresh();
    } catch {
      alert("Failed to save milestone");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title={isEdit ? "Edit Milestone" : "Add Milestone"}
      saveLabel="Save Milestone"
      saving={saving}
      onSave={save}
      onClose={onClose}
    >
      <TextField
        label="Title"
        placeholder="Example: MVP Launch"
        value={form.title}
        onChange={set("title")}
      />
      <TextField
        label="Due Date"
        type="date"
        value={form.due_date}
        onChange={set("due_date")}
      />
      <SelectField
        label="Status"
        options={STATUSES}
        value={form.status}
        onChange={set("status")}
      />
      <TextAreaField label="Notes" value={form.notes} onChange={set("notes")} />
    </WorkModal>
  );
}
