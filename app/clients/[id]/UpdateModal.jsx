"use client";

// Add Update modal — replaces openClientUpdateModal / closeClientUpdateModal /
// createClientUpdate.
//
// Create-only: the original had no edit path for updates, so there is no
// `update` prop here.
//
// The Visibility control maps to is_client_visible. "Client visible later"
// stores true but the client view additionally filters on it, so an update only
// reaches the customer dashboard once that flag is set — see the
// client_updates filter in lib/data/client-view.js.

import { useState } from "react";
import { useRouter } from "next/navigation";
import { WorkModal, TextField, SelectField, TextAreaField } from "./WorkModal";

const UPDATE_TYPES = [
  { value: "general", label: "General" },
  { value: "progress", label: "Progress" },
  { value: "blocker", label: "Blocker" },
  { value: "client_call", label: "Client Call" },
  { value: "delivery", label: "Delivery" },
];

const VISIBILITY = [
  { value: "internal", label: "Internal only" },
  { value: "client", label: "Client visible later" },
];

const EMPTY = {
  title: "",
  related_work_item_id: "",
  update_type: "general",
  visibility: "internal",
  update_text: "",
};

export default function UpdateModal({ clientId, workItems, onClose }) {
  const router = useRouter();
  const [form, setForm] = useState(EMPTY);
  const [saving, setSaving] = useState(false);

  const set = (key) => (value) => setForm((f) => ({ ...f, [key]: value }));

  const workItemOptions = [
    { value: "", label: "No related work item" },
    ...workItems.map((w) => ({
      value: String(w.id),
      label: `#${w.id} · ${w.title}`,
    })),
  ];

  async function save() {
    if (!form.update_text.trim()) {
      alert("Update text is required");
      return;
    }

    setSaving(true);
    try {
      const res = await fetch(`/api/clients/${clientId}/updates`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: form.title.trim(),
          related_work_item_id: form.related_work_item_id || null,
          update_type: form.update_type,
          is_client_visible: form.visibility === "client",
          update_text: form.update_text.trim(),
        }),
      });
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to save update");
        return;
      }

      onClose();
      router.refresh();
    } catch {
      alert("Failed to save update");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title="Add Update"
      saveLabel="Save Update"
      saving={saving}
      onSave={save}
      onClose={onClose}
    >
      <TextField
        label="Title"
        placeholder="Example: Weekly progress update"
        value={form.title}
        onChange={set("title")}
      />
      <SelectField
        label="Related Work Item"
        options={workItemOptions}
        value={form.related_work_item_id}
        onChange={set("related_work_item_id")}
      />
      <SelectField
        label="Update Type"
        options={UPDATE_TYPES}
        value={form.update_type}
        onChange={set("update_type")}
      />
      <SelectField
        label="Visibility"
        options={VISIBILITY}
        value={form.visibility}
        onChange={set("visibility")}
      />
      <TextAreaField
        label="Update"
        value={form.update_text}
        onChange={set("update_text")}
      />
    </WorkModal>
  );
}
