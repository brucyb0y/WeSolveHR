"use client";

// Quick "log a call" dialog, opened from the phone icon on a lead row.
// Replaces openLeadQuickUpdate / closeLeadQuickUpdate / confirmLeadQuickUpdate
// and the recording helpers.
//
// THE CALL FLAG IS A LOCK, NOT A RECORD OF THE OUTCOME.
// Opening this dialog immediately POSTs .../log-call, which sets is_call_made
// and paints the row's icon red for everyone. BOTH Save and Cancel then write
// is_call_made:false. So the flag is held only while the dialog is open — it
// stops two people dialling the same lead at once, and is released either way.
// (The original's comment at the open site claims the flag "sticks even if the
// user Cancels"; the code immediately below it does the opposite. The code is
// what shipped, so the code is what is preserved here.)
//
// ORDERING IS LOAD-BEARING: the release must await the open-time log-call
// request before writing false. Without that wait a slow log-call can land
// after the release and leave the lead locked with nobody in the dialog.
//
// A live recording takes precedence over an attached file when both exist.

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Swal from "sweetalert2";
import { WorkModal, Field, SelectField, TextAreaField } from "./WorkModal";
import styles from "./workspace.module.css";

// Stages that commit to contacting the lead again on a specific day, so a
// callback date is shown and required. Keep in sync with the same set in
// LeadNoteModal.jsx.
const CALLBACK_STAGES = new Set([
  "follow_up_required",
  "follow_up_in_progress",
  "meeting_scheduled",
]);

export default function LeadQuickUpdateModal({
  clientId,
  lead,
  stages,
  demoStatuses,
  reachChannels,
  onClose,
}) {
  const router = useRouter();

  const [stage, setStage] = useState(lead.stage || "");
  const [demo, setDemo] = useState(lead.demo || "");
  const [callbackDate, setCallbackDate] = useState(lead.callback_date || "");
  const needsCallback = CALLBACK_STAGES.has(stage);
  const [note, setNote] = useState("");
  const [reached, setReached] = useState(() =>
    Object.fromEntries(reachChannels.map((c) => [c.key, !!lead[c.column]])),
  );
  const [audioFile, setAudioFile] = useState(null);
  const [recordedBlob, setRecordedBlob] = useState(null);
  const [recording, setRecording] = useState(false);
  const [seconds, setSeconds] = useState(0);
  const [saving, setSaving] = useState(false);

  const recorderRef = useRef(null);
  const timerRef = useRef(null);
  // Held so both Save and Cancel can await the open-time write before
  // releasing the lock.
  const logCallRef = useRef(null);
  const releasedRef = useRef(false);

  // Take the lock as the dialog opens.
  useEffect(() => {
    logCallRef.current = fetch(
      `/api/clients/${clientId}/leads/${lead.id}/log-call`,
      { method: "POST" },
    ).catch(() => {});
    // Deliberately not cleaning up here: releasing the lock is done explicitly
    // by save()/cancel() so the request is awaited in order, not fired from a
    // teardown that cannot wait.
  }, [clientId, lead.id]);

  // Stop the microphone if the dialog goes away mid-recording — otherwise the
  // browser keeps showing the "recording" indicator with nothing listening.
  useEffect(
    () => () => {
      if (timerRef.current) clearInterval(timerRef.current);
      stopTracks(recorderRef.current);
    },
    [],
  );

  function stopTracks(recorder) {
    try {
      recorder?.stream?.getTracks?.().forEach((t) => t.stop());
    } catch {
      /* stream already gone */
    }
  }

  async function releaseLock() {
    if (releasedRef.current) return;
    releasedRef.current = true;
    try {
      if (logCallRef.current) await logCallRef.current;
      await fetch(`/api/clients/${clientId}/leads/${lead.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ is_call_made: false }),
      });
    } catch {
      /* the lock frees on the next page load regardless */
    }
  }

  function cancel() {
    if (timerRef.current) clearInterval(timerRef.current);
    stopTracks(recorderRef.current);
    recorderRef.current = null;
    onClose();
    // Not awaited: the dialog should close instantly, and the release is
    // ordered internally against the open-time write.
    releaseLock().then(() => router.refresh());
  }

  async function startRecording() {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const recorder = new MediaRecorder(stream);
      const chunks = [];

      recorder.ondataavailable = (e) => {
        if (e.data && e.data.size) chunks.push(e.data);
      };
      recorder.onstop = () => {
        setRecordedBlob(
          new Blob(chunks, { type: recorder.mimeType || "audio/webm" }),
        );
        stopTracks(recorder);
      };

      recorder.start();
      recorderRef.current = recorder;
      setRecording(true);
      setSeconds(0);
      timerRef.current = setInterval(() => setSeconds((s) => s + 1), 1000);
    } catch {
      alert("Could not access the microphone.");
    }
  }

  function stopRecording() {
    if (timerRef.current) clearInterval(timerRef.current);
    timerRef.current = null;
    try {
      recorderRef.current?.stop();
    } catch {
      /* already stopped */
    }
    setRecording(false);
  }

  function clearRecording() {
    setRecordedBlob(null);
    setSeconds(0);
  }

  async function save() {
    const text = note.trim();
    const blob = recordedBlob;
    const file = audioFile;
    const hasAudio = !!(blob || file);

    if (!text && !hasAudio) {
      alert("Add a note, attach a voice note, or record one before saving.");
      return;
    }
    if (needsCallback && !callbackDate) {
      alert("Select a callback date for this stage.");
      return;
    }

    if (timerRef.current) clearInterval(timerRef.current);
    stopTracks(recorderRef.current);
    recorderRef.current = null;

    setSaving(true);
    Swal.fire({
      title: hasAudio
        ? "Uploading & transcribing voice note..."
        : "Updating lead...",
      allowOutsideClick: false,
      didOpen: () => Swal.showLoading(),
    });

    try {
      // The lock release must be the LAST write, so the open-time log-call is
      // awaited first.
      if (logCallRef.current) {
        try {
          await logCallRef.current;
        } catch {
          /* the update below still stands */
        }
      }
      releasedRef.current = true;

      let json;
      if (hasAudio) {
        const fd = new FormData();
        // A live recording wins over an attached file.
        if (blob) fd.append("audio", blob, recordingFileName());
        else fd.append("audio", file);
        fd.append("pipeline_stage", stage);
        fd.append("demo_status", demo);
        if (needsCallback) fd.append("callback_date", callbackDate);
        fd.append("is_call_made", "false");
        for (const c of reachChannels) {
          fd.append(`reached_via_${c.key}`, reached[c.key] ? "true" : "false");
        }
        if (text) fd.append("text", text);

        const res = await fetch(
          `/api/clients/${clientId}/leads/${lead.id}/note-audio`,
          { method: "POST", body: fd },
        );
        json = await res.json();
      } else {
        const body = {
          pipeline_stage: stage,
          demo_status: demo,
          add_note: text,
          is_call_made: false,
        };
        if (needsCallback) body.callback_date = callbackDate;
        for (const c of reachChannels) {
          body[`reached_via_${c.key}`] = reached[c.key];
        }

        const res = await fetch(`/api/clients/${clientId}/leads/${lead.id}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        json = await res.json();
      }

      Swal.close();

      if (!json.ok) {
        alert(json.error || "Failed to update lead");
        return;
      }

      onClose();
      router.refresh();
    } catch {
      Swal.close();
      alert("Failed to update lead");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title={lead.company ? `Update — ${lead.company}` : "Update Lead"}
      saveLabel="Save"
      saving={saving}
      onSave={save}
      onClose={cancel}
    >
      <SelectField
        label="Status"
        options={stages.map((s) => ({ value: s.key, label: s.label }))}
        value={stage}
        onChange={setStage}
      />

      {/* Only stages that schedule a next contact need a date, so the field
          appears with those stages rather than sitting empty the rest of the
          time. */}
      {needsCallback ? (
        <Field label="Callback Date">
          <input
            type="date"
            value={callbackDate}
            onChange={(e) => setCallbackDate(e.target.value)}
          />
        </Field>
      ) : null}

      <SelectField
        label="Demo"
        options={demoStatuses.map((s) => ({ value: s.key, label: s.label }))}
        value={demo}
        onChange={setDemo}
      />

      <Field label="Reached Via" wide>
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
          {reachChannels.map((c) => (
            <label
              key={c.key}
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 6,
                fontSize: 13,
              }}
            >
              <input
                type="checkbox"
                checked={!!reached[c.key]}
                onChange={(e) =>
                  setReached((r) => ({ ...r, [c.key]: e.target.checked }))
                }
              />
              {c.label}
            </label>
          ))}
        </div>
      </Field>

      <TextAreaField
        label="Note"
        placeholder="What was discussed?"
        value={note}
        onChange={setNote}
      />

      <Field label="Voice Note" wide>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            flexWrap: "wrap",
          }}
        >
          {recording ? (
            <button
              className={styles.btn}
              type="button"
              onClick={stopRecording}
            >
              ⏹ Stop ({formatSeconds(seconds)})
            </button>
          ) : (
            <button
              className={styles.btn}
              type="button"
              onClick={startRecording}
            >
              🎤 Record
            </button>
          )}

          {recordedBlob ? (
            <>
              <span className={styles.meta}>
                Recorded {formatSeconds(seconds)}
              </span>
              <button
                className={styles.btn}
                type="button"
                onClick={clearRecording}
              >
                Clear
              </button>
            </>
          ) : (
            <input
              type="file"
              accept="audio/*"
              onChange={(e) => setAudioFile(e.target.files?.[0] || null)}
            />
          )}
        </div>
      </Field>
    </WorkModal>
  );
}

const pad = (n) => String(n).padStart(2, "0");

const formatSeconds = (total) =>
  `${pad(Math.floor(total / 60))}:${pad(total % 60)}`;

// Timestamped so uploads from one lead don't collide in storage.
function recordingFileName() {
  const d = new Date();
  return `voice-note-${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(
    d.getDate(),
  )}-${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}.webm`;
}
