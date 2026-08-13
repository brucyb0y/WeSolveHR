"use client";

// New / Edit Weekly Report modal — replaces openReportModal / openReportDetail /
// closeReportModal / saveReport.
//
// Validation matches the original: at least ONE of period label, week start, or
// summary must be filled. It is deliberately not "all three required" — a report
// logged mid-week often has only a summary.

import { useState } from "react";
import { useRouter } from "next/navigation";
import {
  WorkModal,
  TextField,
  TextAreaField,
  CheckboxField,
} from "./WorkModal";

const EMPTY = {
  period_label: "",
  week_start: "",
  summary: "",
  highlights: "",
  lowlights: "",
  next_week_plan: "",
  is_client_visible: true,
};

export default function ReportModal({ clientId, report, onClose }) {
  const router = useRouter();
  const isEdit = !!report;

  const [form, setForm] = useState(() =>
    report
      ? {
          period_label: report.period_label || "",
          week_start: report.week_start || "",
          summary: report.summary || "",
          highlights: report.highlights || "",
          lowlights: report.lowlights || "",
          next_week_plan: report.next_week_plan || "",
          is_client_visible: report.is_client_visible !== false,
        }
      : EMPTY,
  );
  const [saving, setSaving] = useState(false);

  const set = (key) => (value) => setForm((f) => ({ ...f, [key]: value }));

  async function save() {
    const payload = {
      period_label: form.period_label.trim(),
      week_start: form.week_start || null,
      summary: form.summary.trim(),
      highlights: form.highlights.trim(),
      lowlights: form.lowlights.trim(),
      next_week_plan: form.next_week_plan.trim(),
      is_client_visible: form.is_client_visible,
    };

    if (!payload.period_label && !payload.week_start && !payload.summary) {
      alert("Add a period label, week start, or summary");
      return;
    }

    setSaving(true);
    try {
      const res = await fetch(
        isEdit
          ? `/api/clients/${clientId}/reports/${report.id}`
          : `/api/clients/${clientId}/reports`,
        {
          method: isEdit ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || `Failed to ${isEdit ? "update" : "save"} report`);
        return;
      }

      onClose();
      router.refresh();
    } catch {
      alert(`Failed to ${isEdit ? "update" : "save"} report`);
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title={isEdit ? "Edit Weekly Report" : "New Weekly Report"}
      saveLabel="Save Report"
      saving={saving}
      onSave={save}
      onClose={onClose}
    >
      <TextField
        label="Period Label"
        placeholder="e.g. Week 23 · Jun 3–9"
        value={form.period_label}
        onChange={set("period_label")}
      />
      <TextField
        label="Week Start"
        type="date"
        value={form.week_start}
        onChange={set("week_start")}
      />
      <TextAreaField
        label="Summary"
        placeholder="Overall progress this week"
        value={form.summary}
        onChange={set("summary")}
      />
      <TextAreaField
        label="Highlights"
        placeholder="Wins, milestones hit"
        value={form.highlights}
        onChange={set("highlights")}
      />
      <TextAreaField
        label="Lowlights / Risks"
        placeholder="Risks, blockers, misses"
        value={form.lowlights}
        onChange={set("lowlights")}
      />
      <TextAreaField
        label="Next Week Plan"
        placeholder="Plan for next week"
        value={form.next_week_plan}
        onChange={set("next_week_plan")}
      />
      <CheckboxField
        label="Visible to client (when published)"
        checked={form.is_client_visible}
        onChange={set("is_client_visible")}
      />
    </WorkModal>
  );
}
