"use client";

// Add / Edit Contributor modal — replaces openContributorModal /
// openContributorEditModal / closeContributorModal / saveContributor.
//
// Defaults match the original's `selected` attributes: Person Type defaults to
// Contractor (not Internal, which is merely first in the list), Status to Active.

import { useState } from "react";
import { useRouter } from "next/navigation";
import {
  WorkModal,
  TextField,
  SelectField,
  TextAreaField,
  CheckboxField,
} from "./WorkModal";

const PERSON_TYPES = ["Internal", "Contractor", "Client"];
const STATUSES = ["Active", "Inactive"];

const EMPTY = {
  person_type: "Contractor",
  name: "",
  email: "",
  phone: "",
  role: "",
  status: "Active",
  can_update_work: false,
  can_view_client_dashboard: false,
  notes: "",
};

export default function ContributorModal({ clientId, contributor, onClose }) {
  const router = useRouter();
  const isEdit = !!contributor;

  const [form, setForm] = useState(() =>
    contributor
      ? {
          person_type: contributor.person_type || "Contractor",
          name: contributor.name || "",
          email: contributor.email || "",
          phone: contributor.phone || "",
          role: contributor.role || "",
          status: contributor.status || "Active",
          can_update_work: !!contributor.can_update_work,
          can_view_client_dashboard: !!contributor.can_view_client_dashboard,
          notes: contributor.notes || "",
        }
      : EMPTY,
  );
  const [saving, setSaving] = useState(false);

  const set = (key) => (value) => setForm((f) => ({ ...f, [key]: value }));

  async function save() {
    if (!form.name.trim()) {
      alert("Name is required");
      return;
    }

    setSaving(true);
    try {
      const res = await fetch(
        isEdit
          ? `/api/clients/${clientId}/contributors/${contributor.id}`
          : `/api/clients/${clientId}/contributors`,
        {
          method: isEdit ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ...form, name: form.name.trim() }),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to save contributor");
        return;
      }

      onClose();
      router.refresh();
    } catch {
      alert("Failed to save contributor");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title={isEdit ? "Edit Contributor" : "Add Contributor"}
      saveLabel="Save Contributor"
      saving={saving}
      onSave={save}
      onClose={onClose}
    >
      <SelectField
        label="Person Type"
        options={PERSON_TYPES}
        value={form.person_type}
        onChange={set("person_type")}
      />
      <TextField
        label="Name"
        placeholder="Name"
        value={form.name}
        onChange={set("name")}
      />
      <TextField
        label="Email"
        placeholder="email@example.com"
        value={form.email}
        onChange={set("email")}
      />
      <TextField
        label="Phone"
        placeholder="+91..."
        value={form.phone}
        onChange={set("phone")}
      />
      <TextField
        label="Role"
        placeholder="Developer / Designer / Client Contact"
        value={form.role}
        onChange={set("role")}
      />
      <SelectField
        label="Status"
        options={STATUSES}
        value={form.status}
        onChange={set("status")}
      />
      <CheckboxField
        label="Can update work"
        checked={form.can_update_work}
        onChange={set("can_update_work")}
      />
      <CheckboxField
        label="Can view client dashboard"
        checked={form.can_view_client_dashboard}
        onChange={set("can_view_client_dashboard")}
      />
      <TextAreaField label="Notes" value={form.notes} onChange={set("notes")} />
    </WorkModal>
  );
}
