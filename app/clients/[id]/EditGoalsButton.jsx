"use client";

// "Edit goals" / "Add goals" — opens the Goals modal from inside the Report
// tab's goals panel.
//
// The modal itself is mounted by WorkspaceShell, which owns modal state for the
// whole workspace. This button cannot reach that state directly (the panel is
// rendered server-side and passed down as an element), so it dispatches an
// event the shell listens for. That keeps a single Goals modal instance rather
// than a second copy living here with its own state — two editors over one row
// is how a save silently loses the other's edits.

export const OPEN_GOALS_EVENT = "wsr:open-goals";

export default function EditGoalsButton({ hasGoals }) {
  return (
    <button
      className="btn"
      type="button"
      style={{ padding: "5px 12px", fontSize: 12, whiteSpace: "nowrap" }}
      onClick={() => window.dispatchEvent(new CustomEvent(OPEN_GOALS_EVENT))}
    >
      {hasGoals ? "Edit goals" : "Add goals"}
    </button>
  );
}
