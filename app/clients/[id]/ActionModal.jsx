"use client";

// Add / Edit Action modal — replaces openActionModal / openActionEditModal /
// closeActionModal / saveAction.
//
// POST creates, PATCH updates, matching the original saveAction() which
// branched on the hidden #actionId field.

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import {
  WorkModal,
  TextField,
  SelectField,
  TextAreaField,
} from "./WorkModal";

const OWNER_TYPES = ["WeSolve", "Client"];
const STATUSES = ["Open", "In Progress", "Waiting", "Done"];
const PRIORITIES = ["Low", "Medium", "High", "Urgent"];

const EMPTY = {
  title: "",
  owner_type: "WeSolve",
  owner_name: "",
  due_date: "",
  status: "Open",
  priority: "Medium",
  notes: "",
};

export default function ActionModal({ clientId, action, onClose }) {
  const router = useRouter();
  const isEdit = !!action;
  const [form, setForm] = useState(() =>
    action
      ? {
          title: action.title || "",
          owner_type: action.owner_type || "WeSolve",
          owner_name: action.owner_name || "",
          due_date: action.due_date || "",
          status: action.status || "Open",
          priority: action.priority || "Medium",
          notes: action.notes || "",
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
          ? `/api/clients/${clientId}/actions/${action.id}`
          : `/api/clients/${clientId}/actions`,
        {
          method: isEdit ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ...form, title: form.title.trim() }),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to save action");
        return;
      }

      onClose();
      router.refresh();
    } catch {
      alert("Failed to save action");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title={isEdit ? "Edit Action" : "Add Action"}
      saveLabel="Save Action"
      saving={saving}
      onSave={save}
      onClose={onClose}
    >
      <TextField
        label="Title"
        placeholder="Need logo from client"
        value={form.title}
        onChange={set("title")}
      />
      <SelectField
        label="Owner Type"
        options={OWNER_TYPES}
        value={form.owner_type}
        onChange={set("owner_type")}
      />
      <TextField
        label="Owner Name"
        placeholder="Aj / Malikah / Client"
        value={form.owner_name}
        onChange={set("owner_name")}
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
      <SelectField
        label="Priority"
        options={PRIORITIES}
        value={form.priority}
        onChange={set("priority")}
      />
      <TextAreaField
        label="Notes"
        value={form.notes}
        onChange={set("notes")}
      />
    </WorkModal>
  );
}
