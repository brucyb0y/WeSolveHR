"use client";

// Shared shell for the workspace's CRUD modals.
//
// Every modal in renderClientWorkspacePage followed the same shape: a
// .work-modal backdrop that closes on outside click, a .work-modal-card that
// stops propagation, a title row with a Close button, a .form-grid body, and a
// Cancel / Save footer. That repeated markup — and its matching pair of
// open*/close* functions per modal — lives here once.
//
// Escape closes, which the originals did not implement.

import { useEffect } from "react";
import styles from "./workspace.module.css";

// `readOnly` drops the Cancel/Save footer entirely — used by the history
// dialogs, which only display. Without it they would render a Save button with
// no label and no handler.
export function WorkModal({
  title,
  onClose,
  onSave,
  saveLabel,
  saving,
  readOnly,
  children,
}) {
  useEffect(() => {
    const onKey = (e) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div
      className={`${styles.workModal} ${styles.open}`}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className={styles.workModalCard}>
        <div className={styles.modalHead}>
          <div className={styles.modalTitle}>{title}</div>
          <button className={styles.btn} type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className={styles.formGrid}>{children}</div>

        {readOnly ? null : (
          <div className={styles.modalActions}>
            <button className={styles.btn} type="button" onClick={onClose}>
              Cancel
            </button>
            <button
              className={`${styles.btn} ${styles.btnPrimary}`}
              type="button"
              onClick={onSave}
              disabled={saving}
            >
              {saving ? "Saving..." : saveLabel}
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

export function Field({ label, wide, children }) {
  return (
    <div className={`${styles.formField} ${wide ? styles.fieldWide : ""}`}>
      <label>{label}</label>
      {children}
    </div>
  );
}

// Text/date input and select helpers, so each modal is just its field list.
export function TextField({ label, value, onChange, placeholder, type, wide }) {
  return (
    <Field label={label} wide={wide}>
      <input
        type={type || "text"}
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
    </Field>
  );
}

export function SelectField({ label, value, onChange, options, wide }) {
  return (
    <Field label={label} wide={wide}>
      <select value={value} onChange={(e) => onChange(e.target.value)}>
        {options.map((o) =>
          typeof o === "string" ? (
            <option value={o} key={o}>
              {o}
            </option>
          ) : (
            <option value={o.value} key={o.value}>
              {o.label}
            </option>
          ),
        )}
      </select>
    </Field>
  );
}

// Checkbox sits inside the label, matching the original markup. The explicit
// width:auto is needed because the form's `input` rule sets width:100%.
export function CheckboxField({ label, checked, onChange }) {
  return (
    <div className={styles.formField}>
      <label>
        <input
          type="checkbox"
          className={styles.inlineCheckbox}
          checked={checked}
          onChange={(e) => onChange(e.target.checked)}
        />{" "}
        {label}
      </label>
    </div>
  );
}

export function TextAreaField({ label, value, onChange, wide = true }) {
  return (
    <Field label={label} wide={wide}>
      <textarea value={value} onChange={(e) => onChange(e.target.value)} />
    </Field>
  );
}
