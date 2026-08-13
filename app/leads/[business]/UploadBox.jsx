"use client";

// Excel import box (rasset and joolian each get one, hitting different
// endpoints). Ported from toggleUploadBox() + uploadRassetExcel() /
// uploadJoolianB2BExcel(), which were the same flow twice over.

import { useRef, useState } from "react";
import styles from "./leads.module.css";

export default function UploadBox({ label, title, endpoint, note, extraLink }) {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const fileRef = useRef(null);

  async function upload() {
    const file = fileRef.current?.files?.[0];
    if (!file) {
      alert("Choose an Excel file first.");
      return;
    }

    const body = new FormData();
    body.append("file", file);

    setBusy(true);
    try {
      const res = await fetch(endpoint, { method: "POST", body });
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Upload failed");
        return;
      }

      window.location.reload();
    } catch {
      alert("Upload failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className={styles.uploadWrapper}>
      <button
        className={`${styles.btn} ${styles.btnPrimary}`}
        type="button"
        title={title}
        onClick={() => setOpen((v) => !v)}
      >
        ＋ {label}
      </button>

      {extraLink ? (
        <a className={styles.btn} href={extraLink.href}>
          {extraLink.label}
        </a>
      ) : null}

      {open ? (
        <div className={styles.uploadBox}>
          <div className={styles.uploadRow}>
            <input type="file" accept=".xlsx,.xls,.csv" ref={fileRef} />
            <button
              className={`${styles.btn} ${styles.btnPrimary}`}
              type="button"
              onClick={upload}
              disabled={busy}
            >
              {busy ? "Uploading..." : "Upload"}
            </button>
          </div>
          <div className={`muted ${styles.uploadNote}`}>{note}</div>
        </div>
      ) : null}
    </div>
  );
}
