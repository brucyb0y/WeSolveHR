// Badge class helpers for the client workspace.
//
// The original built these as template strings ("badge badge-danger") inside the
// page renderer. Under CSS modules the names are hashed, so the mapping has to
// go through `styles` — and it lives here rather than in each tab so that
// priority/status colours cannot drift between the Task, Blockers and other
// tabs that all render the same badges.
//
// Plain .js, not .jsx: imported by both page.jsx (server, to pre-decorate rows)
// and the client tab components.

import styles from "./workspace.module.css";

export const priorityBadgeClass = (p) =>
  p === "high"
    ? `${styles.badge} ${styles.badgeDanger}`
    : p === "medium"
      ? `${styles.badge} ${styles.badgeWarn}`
      : `${styles.badge} ${styles.badgeMuted}`;

export const blockerStatusClass = (s) =>
  s === "resolved"
    ? `${styles.badge} ${styles.badgeOk}`
    : s === "in_progress"
      ? `${styles.badge} ${styles.badgeInfo}`
      : `${styles.badge} ${styles.badgeWarn}`;

export const mutedBadgeClass = () => `${styles.badge} ${styles.badgeMuted}`;

export const blockerSideLabel = (s) =>
  s === "client_side" ? "Client-side" : "Internal";

// "in_progress" -> "in progress"; used for badge text throughout.
export const humanizeStatus = (s) => String(s || "").replaceAll("_", " ");

// Visibility chips (VIS_CHIP in the original). Marks whether a section is
// internal-only or mirrored to the client dashboard — an editorial signal for
// staff, so it must not silently vanish under the class rename.
export const VIS_CHIP = {
  internal: `${styles.visChip} ${styles.visInternal}`,
  client: `${styles.visChip} ${styles.visClient}`,
};

export const incentiveStatusClass = (s) =>
  s === "paid"
    ? `${styles.badge} ${styles.badgeOk}`
    : s === "approved"
      ? `${styles.badge} ${styles.badgeInfo}`
      : `${styles.badge} ${styles.badgeWarn}`;

export const taskStatusLabel = (w) =>
  w.status === "done"
    ? "Done"
    : w.status === "in_progress"
      ? "In Progress"
      : "To Do";

export const taskStatusBadgeClass = (w) =>
  w.status === "done"
    ? `${styles.badge} ${styles.badgeOk}`
    : w.status === "in_progress"
      ? `${styles.badge} ${styles.badgeInfo}`
      : `${styles.badge} ${styles.badgeMuted}`;
