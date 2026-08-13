"use client";

// The three "add a note" dialogs, which are one component because they differ
// only in what they save alongside the note:
//
//   mode="note"   — a note on its own (the + button in the Notes column).
//   mode="stage"  — a pipeline-stage change. The dropdown does NOT save on
//                   change; it opens this, and the stage is written only once a
//                   note is supplied. Cancelling reverts the dropdown.
//   mode="demo"   — the same contract for the demo-status dropdown.
//
// A NOTE IS MANDATORY FOR STAGE AND DEMO CHANGES. That is the whole point of
// routing those dropdowns through a dialog: every pipeline movement carries a
// written or spoken reason. Patching the dropdown directly would be simpler and
// would quietly destroy that audit trail.
//
// The MIN_TYPED_NOTE_CHARS floor applies ONLY to mode="note", and only when no
// audio is attached. Two separate exemptions, both from the original:
//   * stage/demo notes are checked for non-empty but have NO length floor —
//     "client asked to wait" is a legitimate reason for a status change;
//   * a voice note supplies its detail through transcription, so length is not
//     measured against the typed box at all.
//
// Audio takes the /note-audio multipart endpoint; text alone takes a JSON
// PATCH. Both carry the same stage/demo/callback fields.

import { useState } from "react";
import { useRouter } from "next/navigation";
import Swal from "sweetalert2";
import { WorkModal, Field, TextAreaField } from "./WorkModal";

const FOLLOW_UP_STAGE = "follow_up_required";

// Raised from 75 to 25 in b3f9d92 — kept as a named constant so the message and
// the check cannot drift apart.
const MIN_TYPED_NOTE_CHARS = 25;

const TITLES = {
  note: "Add Note",
  stage: "Status Change — Add Note",
  demo: "Demo Change — Add Note",
};

const FAILURES = {
  note: "Failed to save note",
  stage: "Failed to update stage",
  demo: "Failed to update demo status",
};

const LOADING = {
  note: "Saving note...",
  stage: "Updating pipeline stage...",
  demo: "Updating demo status...",
};

export default function LeadNoteModal({
  clientId,
  leadId,
  mode = "note",
  stage,
  demo,
  onClose,
  onCancel,
}) {
  const router = useRouter();
  const [text, setText] = useState("");
  const [audioFile, setAudioFile] = useState(null);
  const [callbackDate, setCallbackDate] = useState("");
  const [saving, setSaving] = useState(false);

  const needsCallback = mode === "stage" && stage === FOLLOW_UP_STAGE;

  function cancel() {
    // Lets the caller put a stage/demo dropdown back to its previous value —
    // nothing was saved.
    onCancel?.();
    onClose();
  }

  async function save() {
    const note = text.trim();

    if (!note && !audioFile) {
      alert(
        mode === "note"
          ? "Write a note or attach a voice note first."
          : "Add a note or attach a voice note before saving the change.",
      );
      return;
    }

    // Length floor: standalone notes only, and only when nothing was recorded.
    if (mode === "note" && !audioFile && note.length < MIN_TYPED_NOTE_CHARS) {
      alert(
        `Please write a more detailed note — at least ${MIN_TYPED_NOTE_CHARS} characters (currently ${note.length}).`,
      );
      return;
    }

    if (needsCallback && !callbackDate) {
      alert("Select a callback date for this follow-up.");
      return;
    }

    setSaving(true);
    Swal.fire({
      title: audioFile
        ? "Uploading & transcribing voice note..."
        : LOADING[mode],
      allowOutsideClick: false,
      didOpen: () => Swal.showLoading(),
    });

    try {
      let json;

      if (audioFile) {
        const fd = new FormData();
        fd.append("audio", audioFile);
        if (note) fd.append("text", note);
        if (mode === "stage") fd.append("pipeline_stage", stage);
        if (mode === "demo") fd.append("demo_status", demo);
        if (needsCallback) fd.append("callback_date", callbackDate);

        const res = await fetch(
          `/api/clients/${clientId}/leads/${leadId}/note-audio`,
          { method: "POST", body: fd },
        );
        json = await res.json();
      } else {
        const body = { add_note: note };
        if (mode === "stage") body.pipeline_stage = stage;
        if (mode === "demo") body.demo_status = demo;
        if (needsCallback) body.callback_date = callbackDate;

        const res = await fetch(`/api/clients/${clientId}/leads/${leadId}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        json = await res.json();
      }

      Swal.close();

      if (!json.ok) {
        alert(json.error || FAILURES[mode]);
        return;
      }

      onClose();
      router.refresh();
    } catch {
      Swal.close();
      alert(FAILURES[mode]);
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title={TITLES[mode]}
      saveLabel="Save"
      saving={saving}
      onSave={save}
      onClose={cancel}
    >
      <TextAreaField
        label="Note"
        placeholder={
          mode === "note"
            ? "What happened with this lead?"
            : "Why is this changing?"
        }
        value={text}
        onChange={setText}
      />

      {needsCallback ? (
        <Field label="Callback Date">
          <input
            type="date"
            value={callbackDate}
            onChange={(e) => setCallbackDate(e.target.value)}
          />
        </Field>
      ) : null}

      <Field label="Voice Note (optional)" wide>
        <input
          type="file"
          accept="audio/*"
          onChange={(e) => setAudioFile(e.target.files?.[0] || null)}
        />
      </Field>
    </WorkModal>
  );
}
