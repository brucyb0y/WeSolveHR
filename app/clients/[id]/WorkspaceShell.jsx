"use client";

// Owns modal state for the whole workspace and renders the active tab.
//
// This exists because page.jsx is a server component and cannot pass functions
// to client components — the tabs need onAdd/onEdit callbacks, so something on
// the client has to supply them. Static panels (read-only tabs) are still
// server-rendered and arrive as elements through `panel`; only the tabs that
// open modals are constructed here.
//
// Every modal in the workspace is mounted from this one place rather than from
// the tab that opens it. In the original each modal was a permanently-present
// hidden <div> that any tab's onclick could reveal, so several are opened from
// outside their own tab (the Overview header opens Goals; the alert strip opens
// a work item). Keeping them here preserves that, and means a modal survives
// nothing more than `modal` being non-null.
//
// modal shape: { kind, id? } — `kind` picks the component, `id` selects the row
// being edited (absent for "add").

import { useEffect, useState } from "react";
import ActionsTab from "./ActionsTab";
import TaskTab from "./TaskTab";
import BlockersTab from "./BlockersTab";
import CampaignsTab from "./CampaignsTab";
import MeetingsTab from "./MeetingsTab";
import IncentivesTab from "./IncentivesTab";
import MilestonesTab from "./MilestonesTab";
import UpdatesTab from "./UpdatesTab";
import TeamTab from "./TeamTab";
import ReportsPanel from "./ReportsPanel";
import { OPEN_GOALS_EVENT } from "./EditGoalsButton";

import ActionModal from "./ActionModal";
import BlockerModal from "./BlockerModal";
import CampaignModal from "./CampaignModal";
import ContributorModal from "./ContributorModal";
import GoalsModal from "./GoalsModal";
import IncentiveModal from "./IncentiveModal";
import MeetingModal from "./MeetingModal";
import MilestoneModal from "./MilestoneModal";
import ReportModal from "./ReportModal";
import UpdateModal from "./UpdateModal";
import WorkItemModal from "./WorkItemModal";

export default function WorkspaceShell({
  clientId,
  activeTab,
  panel,
  data,
  taskChips,
  taskAlertStrip,
  clientName,
  teamMembers = [],
  timelineEvents = [],
  activityLogs = [],
  updates = [],
  meetingStats = {},
  reportSubviews = null,
}) {
  const [modal, setModal] = useState(null);

  // The Report tab's goals panel is server-rendered and handed down as an
  // element, so its "Edit goals" button cannot call open() directly. It
  // dispatches an event instead; this is the single Goals modal for the page.
  useEffect(() => {
    const onOpenGoals = () => setModal({ kind: "goals" });
    window.addEventListener(OPEN_GOALS_EVENT, onOpenGoals);
    return () => window.removeEventListener(OPEN_GOALS_EVENT, onOpenGoals);
  }, []);

  // Tabs call onAdd() with no argument and onEdit(id) with a row id; both route
  // to the same modal, which switches on whether a row was found.
  const open = (kind) => (id) =>
    setModal({ kind, id: typeof id === "number" ? id : undefined });
  const close = () => setModal(null);

  const {
    users = [],
    workItems = [],
    milestones = [],
    actions = [],
    contributors = [],
    blockers = [],
    campaigns = [],
    meetings = [],
    incentives = [],
    reports = [],
    leads = [],
    goals = null,
    campaignTypes = [],
    campaignStatuses = [],
    incentiveStatuses = [],
  } = data || {};

  // `id` is undefined when adding, so this returns null and the modal opens
  // blank — that is the add/edit switch every modal reads.
  const row = (rows) =>
    modal?.id ? rows.find((r) => r.id === modal.id) || null : null;

  const common = { clientId, onClose: close };

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
      ) : activeTab === "blockers" ? (
        <BlockersTab
          clientId={clientId}
          blockers={blockers}
          onAdd={open("blocker")}
          onEdit={open("blocker")}
        />
      ) : activeTab === "campaigns" ? (
        <CampaignsTab
          clientId={clientId}
          campaigns={campaigns}
          onAdd={open("campaign")}
          onEdit={open("campaign")}
        />
      ) : activeTab === "meetings" ? (
        <MeetingsTab
          clientId={clientId}
          meetings={meetings}
          meetingsThisWeek={meetingStats.thisWeek}
          syncCompliant={meetingStats.syncCompliant}
          nextMeetingDate={meetingStats.nextMeetingDate}
          onAdd={open("meeting")}
          onEdit={open("meeting")}
        />
      ) : activeTab === "incentives" ? (
        <IncentivesTab
          clientId={clientId}
          incentives={incentives}
          onAdd={open("incentive")}
          onEdit={open("incentive")}
        />
      ) : activeTab === "milestones" ? (
        <MilestonesTab
          clientId={clientId}
          milestones={milestones}
          workItems={workItems}
          onAdd={open("milestone")}
          onEdit={open("milestone")}
        />
      ) : activeTab === "updates" ? (
        <UpdatesTab
          updates={updates}
          activityLogs={activityLogs}
          timelineEvents={timelineEvents}
          onAdd={open("update")}
        />
      ) : activeTab === "team" ? (
        <TeamTab
          clientId={clientId}
          clientName={clientName}
          teamMembers={teamMembers}
          contributors={contributors}
          onAdd={open("contributor")}
          onEdit={open("contributor")}
        />
      ) : activeTab === "report" ? (
        <>
          {reportSubviews}
          <ReportsPanel
            clientId={clientId}
            reports={reports}
            onAdd={open("report")}
            onEdit={open("report")}
          />
        </>
      ) : (
        panel
      )}

      {modal?.kind === "action" ? (
        <ActionModal {...common} action={row(actions)} />
      ) : null}

      {modal?.kind === "workItem" ? (
        <WorkItemModal
          {...common}
          workItem={row(workItems)}
          users={users}
          workItems={workItems}
          milestones={milestones}
        />
      ) : null}

      {modal?.kind === "milestone" ? (
        <MilestoneModal {...common} milestone={row(milestones)} />
      ) : null}

      {modal?.kind === "contributor" ? (
        <ContributorModal {...common} contributor={row(contributors)} />
      ) : null}

      {modal?.kind === "blocker" ? (
        <BlockerModal
          {...common}
          blocker={row(blockers)}
          users={users}
          workItems={workItems}
        />
      ) : null}

      {modal?.kind === "campaign" ? (
        <CampaignModal
          {...common}
          campaign={row(campaigns)}
          types={campaignTypes}
          statuses={campaignStatuses}
        />
      ) : null}

      {modal?.kind === "meeting" ? (
        <MeetingModal {...common} meeting={row(meetings)} />
      ) : null}

      {modal?.kind === "incentive" ? (
        <IncentiveModal
          {...common}
          incentive={row(incentives)}
          users={users}
          leads={leads}
          statuses={incentiveStatuses}
        />
      ) : null}

      {modal?.kind === "update" ? (
        <UpdateModal {...common} workItems={workItems} />
      ) : null}

      {modal?.kind === "report" ? (
        <ReportModal {...common} report={row(reports)} />
      ) : null}

      {modal?.kind === "goals" ? (
        <GoalsModal {...common} goals={goals} />
      ) : null}
    </>
  );
}
