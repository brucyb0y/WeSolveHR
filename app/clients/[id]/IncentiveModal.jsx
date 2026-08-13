"use client";

// Add / Edit Incentive modal — replaces openIncentiveModal / openIncentiveDetail
// / closeIncentiveModal / saveIncentive.
//
// The payload shape mirrors buildIncentivePayloadFromBody on the server: empty
// title/notes become null rather than "", gtm_user_id and related_lead_id are
// numbers or null, and status falls back to "pending" if it is not one of
// INCENTIVE_STATUSES (that list is passed in rather than hardcoded, so the
// dropdown cannot drift from what the handler accepts).

import { useState } from "react";
import { useRouter } from "next/navigation";
import { WorkModal, TextField, SelectField, TextAreaField } from "./WorkModal";

const EMPTY = {
  title: "",
  gtm_user_id: "",
  related_lead_id: "",
  amount: "0",
  status: "pending",
  notes: "",
};

const titleCase = (s) =>
  String(s || "")
    .charAt(0)
    .toUpperCase() + String(s || "").slice(1);

export default function IncentiveModal({
  clientId,
  incentive,
  users,
  leads,
  statuses,
  onClose,
}) {
  const router = useRouter();
  const isEdit = !!incentive;

  const [form, setForm] = useState(() =>
    incentive
      ? {
          title: incentive.title || "",
          gtm_user_id: incentive.gtm_user_id
            ? String(incentive.gtm_user_id)
            : "",
          related_lead_id: incentive.related_lead_id
            ? String(incentive.related_lead_id)
            : "",
          amount: String(incentive.amount ?? 0),
          status: incentive.status || "pending",
          notes: incentive.notes || "",
        }
      : EMPTY,
  );
  const [saving, setSaving] = useState(false);

  const set = (key) => (value) => setForm((f) => ({ ...f, [key]: value }));

  const gtmOptions = [
    { value: "", label: "Select team member" },
    ...users.map((u) => ({ value: String(u.id), label: u.name })),
  ];

  const leadOptions = [
    { value: "", label: "No lead" },
    ...leads.map((l) => ({
      value: String(l.id),
      label: l.company || l.business_name || l.contact_name || `Lead #${l.id}`,
    })),
  ];

  async function save() {
    setSaving(true);
    try {
      const res = await fetch(
        isEdit
          ? `/api/clients/${clientId}/incentives/${incentive.id}`
          : `/api/clients/${clientId}/incentives`,
        {
          method: isEdit ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            title: form.title.trim() || null,
            gtm_user_id: form.gtm_user_id ? Number(form.gtm_user_id) : null,
            related_lead_id: form.related_lead_id
              ? Number(form.related_lead_id)
              : null,
            amount: Number.isFinite(Number(form.amount))
              ? Number(form.amount)
              : 0,
            status: form.status,
            notes: form.notes.trim() || null,
          }),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to save incentive");
        return;
      }

      onClose();
      router.refresh();
    } catch {
      alert("Failed to save incentive");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title={isEdit ? "Edit Incentive" : "Add Incentive"}
      saveLabel="Save Incentive"
      saving={saving}
      onSave={save}
      onClose={onClose}
    >
      <TextField
        label="Title"
        placeholder="Example: Converted Acme deal commission"
        value={form.title}
        onChange={set("title")}
      />
      <SelectField
        label="GTM (attribution)"
        options={gtmOptions}
        value={form.gtm_user_id}
        onChange={set("gtm_user_id")}
      />
      <SelectField
        label="Related Lead"
        options={leadOptions}
        value={form.related_lead_id}
        onChange={set("related_lead_id")}
      />
      <TextField
        label="Amount"
        type="number"
        value={form.amount}
        onChange={set("amount")}
      />
      <SelectField
        label="Status"
        options={statuses.map((s) => ({ value: s, label: titleCase(s) }))}
        value={form.status}
        onChange={set("status")}
      />
      <TextAreaField label="Notes" value={form.notes} onChange={set("notes")} />
    </WorkModal>
  );
}
