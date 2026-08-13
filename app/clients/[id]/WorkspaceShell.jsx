"use client";

// Owns modal state for the whole workspace and renders the active tab.
//
// This exists because page.jsx is a server component and cannot pass functions
// to client components — the tabs need onAdd/onEdit callbacks, so something on
// the client has to supply them. Static panels (Overview and the other
// read-only tabs) are still server-rendered and arrive as elements through
// `panel`; only the tabs that open modals are constructed here.
//
// modal shape: { kind, id? } — `kind` picks the component, `id` selects the row
// being edited (absent for "add").

import { useState } from "react";
import ActionsTab from "./ActionsTab";
import TaskTab from "./TaskTab";
import ActionModal from "./ActionModal";

export default function WorkspaceShell({
  clientId,
  activeTab,
  panel,
  actions,
  workItems,
  taskChips,
  taskAlertStrip,
}) {
  const [modal, setModal] = useState(null);

  const open = (kind) => (id) =>
    setModal({ kind, id: typeof id === "number" ? id : undefined });
  const close = () => setModal(null);

  const rowById = (rows, id) => rows.find((r) => r.id === id) || null;

  return (
    <>
      {activeTab === "actions" ? (
        <ActionsTab
          clientId={clientId}
          actions={actions}
          onAdd={open("action")}
          onEdit={open("action")}
        />
      ) : activeTab === "task" ? (
        <TaskTab
          clientId={clientId}
          workItems={workItems}
          chips={taskChips}
          alertStrip={taskAlertStrip}
          onAdd={open("workItem")}
        />
      ) : (
        panel
      )}

      {modal?.kind === "action" ? (
        <ActionModal
          clientId={clientId}
          action={modal.id ? rowById(actions, modal.id) : null}
          onClose={close}
        />
      ) : null}
    </>
  );
}
