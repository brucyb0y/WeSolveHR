"use client";

// Log / Edit Meeting modal — replaces openMeetingModal / openMeetingDetail /
// closeMeetingModal / saveMeeting / aiFillMeetingFromNotes.
//
// The AI quick-fill posts the pasted notes to /api/ai/parse-meeting-notes and
// merges the parsed fields into the form. It only OVERWRITES fields the model
// returned a value for, so a partially filled form is not blanked by a fill —
// same as the original, which assigned each field individually behind a check.

import { useState } from "react";
import { useRouter } from "next/navigation";
import {
  WorkModal,
  Field,
  TextField,
  SelectField,
  TextAreaField,
} from "./WorkModal";
import styles from "./workspace.module.css";

const TYPES = [
  { value: "sync_call", label: "Sync Call" },
  { value: "review", label: "Review" },
  { value: "internal", label: "Internal" },
  { value: "adhoc", label: "Ad-hoc" },
];

const MEETING_COMPLETED_STAGE = "meeting_completed";

const LONG_FIELDS = [
  ["summary", "Summary", "Brief call summary"],
  ["discussion_points", "Discussion Points", "Key points discussed"],
  ["decisions", "Decisions Taken", "Decisions made"],
  ["deliverables", "Deliverables", "Agreed deliverables"],
  ["action_items", "Action Items", "Who does what"],
  ["follow_ups", "Follow-ups", "Follow-up items"],
  ["next_steps", "Next Steps", "Next steps"],
];

const EMPTY = {
  title: "",
  meeting_date: "",
  meeting_type: "sync_call",
  duration_minutes: "",
  participants: "",
  summary: "",
  discussion_points: "",
  decisions: "",
  deliverables: "",
  action_items: "",
  follow_ups: "",
  next_steps: "",
};

// advanceLead (optional): when this modal is opened from a lead row whose demo
// is Completed, saving the meeting also moves that lead to the Meeting Completed
// pipeline stage. Passed as the decorated lead ({ id, stage, company }).
export default function MeetingModal({
  clientId,
  meeting,
  advanceLead,
  onClose,
}) {
  const router = useRouter();
  const isEdit = !!meeting;

  const [form, setForm] = useState(() =>
    meeting ? { ...EMPTY, ...pick(meeting) } : EMPTY,
  );
  const [aiNotes, setAiNotes] = useState("");
  const [filling, setFilling] = useState(false);
  const [saving, setSaving] = useState(false);

  const set = (key) => (value) => setForm((f) => ({ ...f, [key]: value }));

  async function aiFill() {
    const notes = aiNotes.trim();
    if (!notes) {
      alert("Paste the meeting details first.");
      return;
    }

    setFilling(true);
    try {
      const res = await fetch("/api/ai/parse-meeting-notes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ notes }),
      });
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "AI auto-fill failed");
        return;
      }

      const m = json.data || {};
      // Only overwrite what the model actually returned.
      setForm((f) => {
        const next = { ...f };
        for (const key of Object.keys(EMPTY)) {
          if (m[key]) next[key] = m[key];
        }
        return next;
      });
    } catch {
      alert("AI auto-fill failed");
    } finally {
      setFilling(false);
    }
  }

  async function save() {
    if (!form.title.trim()) {
      alert("Title is required");
      return;
    }

    setSaving(true);
    try {
      const res = await fetch(
        isEdit
          ? `/api/clients/${clientId}/meetings/${meeting.id}`
          : `/api/clients/${clientId}/meetings`,
        {
          method: isEdit ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ...form, title: form.title.trim() }),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to save meeting");
        return;
      }

      // Opened from a "demo completed → +" lead action: advance that lead to
      // Meeting Completed. The meeting is already saved, so a failure here is
      // surfaced but does not undo it. Skip when the lead is already there, to
      // avoid a redundant status-history entry. The server logs the stage
      // transition automatically; the note records why it moved.
      if (advanceLead && advanceLead.stage !== MEETING_COMPLETED_STAGE) {
        try {
          const title = form.title.trim();
          const res2 = await fetch(
            `/api/clients/${clientId}/leads/${advanceLead.id}`,
            {
              method: "PATCH",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                pipeline_stage: MEETING_COMPLETED_STAGE,
                add_note: `Moved to Meeting Completed after logging meeting${
                  title ? `: "${title}"` : ""
                }.`,
              }),
            },
          );
          const json2 = await res2.json();
          if (!json2.ok) {
            alert(
              `Meeting saved, but the lead status could not be updated: ${
                json2.error || "unknown error"
              }`,
            );
          }
        } catch {
          alert("Meeting saved, but the lead status could not be updated.");
        }
      }

      onClose();
      router.refresh();
    } catch {
      alert("Failed to save meeting");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title={isEdit ? "Edit Meeting" : "Log Meeting"}
      saveLabel="Save Meeting"
      saving={saving}
      onSave={save}
      onClose={onClose}
    >
      <Field label="Meeting Details (AI Quick Fill)" wide>
        <textarea
          rows={6}
          className={styles.aiNotes}
          placeholder="Write or paste the full meeting details here — what was discussed, who joined, decisions, action items — then click Auto-fill."
          value={aiNotes}
          onChange={(e) => setAiNotes(e.target.value)}
        />
        <button
          className={`${styles.btn} ${styles.btnPrimary}`}
          type="button"
          onClick={aiFill}
          disabled={filling}
        >
          {filling ? "Filling..." : "✨ Auto-fill with AI"}
        </button>
      </Field>

      <TextField
        label="Title"
        placeholder="Example: Weekly sync call"
        value={form.title}
        onChange={set("title")}
      />
      <TextField
        label="Date"
        type="date"
        value={form.meeting_date}
        onChange={set("meeting_date")}
      />
      <SelectField
        label="Type"
        options={TYPES}
        value={form.meeting_type}
        onChange={set("meeting_type")}
      />
      <TextField
        label="Duration (min)"
        type="number"
        placeholder="e.g. 30"
        value={form.duration_minutes}
        onChange={set("duration_minutes")}
      />
      <TextField
        label="Participants"
        placeholder="Names, comma-separated"
        value={form.participants}
        onChange={set("participants")}
      />

      {LONG_FIELDS.map(([key, label]) => (
        <TextAreaField
          key={key}
          label={label}
          value={form[key]}
          onChange={set(key)}
        />
      ))}
    </WorkModal>
  );
}

function pick(meeting) {
  const out = {};
  for (const key of Object.keys(EMPTY)) {
    if (meeting[key] != null) out[key] = meeting[key];
  }
  return out;
}
