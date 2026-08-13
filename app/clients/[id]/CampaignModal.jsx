"use client";

// Add / Edit Campaign modal — replaces openCampaignModal / openCampaignDetail /
// closeCampaignModal / saveCampaign.
//
// Type and status option lists come from CAMPAIGN_TYPES / CAMPAIGN_STATUSES in
// lib/server/app.js, passed down by page.jsx — the same lists the PATCH/POST
// handlers validate against, so the dropdowns cannot drift from the API.

import { useState } from "react";
import { useRouter } from "next/navigation";
import { WorkModal, TextField, SelectField, TextAreaField } from "./WorkModal";

const EMPTY = {
  name: "",
  campaign_type: "email",
  channel: "",
  status: "planned",
  sent_count: "0",
  response_count: "0",
  positive_replies: "0",
  notes: "",
};

const titleCase = (s) =>
  String(s || "")
    .charAt(0)
    .toUpperCase() + String(s || "").slice(1);

export default function CampaignModal({
  clientId,
  campaign,
  types,
  statuses,
  onClose,
}) {
  const router = useRouter();
  const isEdit = !!campaign;

  const [form, setForm] = useState(() =>
    campaign
      ? {
          name: campaign.name || "",
          campaign_type: campaign.campaign_type || "email",
          channel: campaign.channel || "",
          status: campaign.status || "planned",
          sent_count: String(campaign.sent_count ?? 0),
          response_count: String(campaign.response_count ?? 0),
          positive_replies: String(campaign.positive_replies ?? 0),
          notes: campaign.notes || "",
        }
      : EMPTY,
  );
  const [saving, setSaving] = useState(false);

  const set = (key) => (value) => setForm((f) => ({ ...f, [key]: value }));

  const asOptions = (list) =>
    list.map((v) => ({ value: v, label: titleCase(v) }));

  async function save() {
    if (!form.name.trim()) {
      alert("Name is required");
      return;
    }

    setSaving(true);
    try {
      const res = await fetch(
        isEdit
          ? `/api/clients/${clientId}/campaigns/${campaign.id}`
          : `/api/clients/${clientId}/campaigns`,
        {
          method: isEdit ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            name: form.name.trim(),
            campaign_type: form.campaign_type,
            channel: form.channel.trim(),
            status: form.status,
            sent_count: Number(form.sent_count) || 0,
            response_count: Number(form.response_count) || 0,
            positive_replies: Number(form.positive_replies) || 0,
            notes: form.notes.trim(),
          }),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to save campaign");
        return;
      }

      onClose();
      router.refresh();
    } catch {
      alert("Failed to save campaign");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title={isEdit ? "Edit Campaign" : "Add Campaign"}
      saveLabel="Save Campaign"
      saving={saving}
      onSave={save}
      onClose={onClose}
    >
      <TextField
        label="Name"
        placeholder="Example: Q3 cold email blast"
        value={form.name}
        onChange={set("name")}
      />
      <SelectField
        label="Type"
        options={asOptions(types)}
        value={form.campaign_type}
        onChange={set("campaign_type")}
      />
      <TextField
        label="Channel / Tool"
        placeholder="e.g. Apollo, Instantly"
        value={form.channel}
        onChange={set("channel")}
      />
      <SelectField
        label="Status"
        options={asOptions(statuses)}
        value={form.status}
        onChange={set("status")}
      />
      <TextField
        label="Sent"
        type="number"
        value={form.sent_count}
        onChange={set("sent_count")}
      />
      <TextField
        label="Responses"
        type="number"
        value={form.response_count}
        onChange={set("response_count")}
      />
      <TextField
        label="Positive replies"
        type="number"
        value={form.positive_replies}
        onChange={set("positive_replies")}
      />
      <TextAreaField label="Notes" value={form.notes} onChange={set("notes")} />
    </WorkModal>
  );
}
