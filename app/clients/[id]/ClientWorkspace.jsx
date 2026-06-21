"use client";

// Client workspace console. Ported from renderClientWorkspacePage() in
// lib/server/app.js (the ~5.5k-line giant). The server (page.jsx) authenticates,
// loads the data, and computes the derived view model (workspaceView.js); this
// island renders the topbar, contextual stats, the 14 tabs, and the 17 modals,
// and owns every mutation. All /api/clients/:id/* endpoints stay on the dispatch
// shim — exactly like the original, every successful mutation reloads the page so
// the server-rendered tabs remain the source of truth (no optimistic updates).

import { useEffect, useRef, useState } from "react";
import {
  formatDateTime,
  formatDateTimeNoTz,
} from "@/lib/utils/datetime.js";

// ---------------------------------------------------------------------------
// Pure presentational helpers (ported verbatim from the render function)
// ---------------------------------------------------------------------------
const priorityBadgeClass = (p) =>
  p === "high"
    ? "badge badge-danger"
    : p === "medium"
      ? "badge badge-warn"
      : "badge badge-muted";

const blockerSideLabel = (s) => (s === "client_side" ? "Client-side" : "Internal");
const blockerStatusClass = (s) =>
  s === "resolved"
    ? "badge badge-ok"
    : s === "in_progress"
      ? "badge badge-info"
      : "badge badge-warn";

const meetingTypeLabel = (t) =>
  ({
    sync_call: "Sync Call",
    internal: "Internal",
    review: "Review",
    adhoc: "Ad-hoc",
  })[t] || "Sync Call";
const meetingClip = (text, limit) => {
  const value = String(text || "").trim();
  return value.length <= limit ? value : value.slice(0, limit) + "…";
};
const momFilled = (m) =>
  !!(
    m.summary ||
    m.discussion_points ||
    m.decisions ||
    m.deliverables ||
    m.action_items ||
    m.follow_ups ||
    m.next_steps
  );

const campaignTypeLabel = (t) =>
  ({
    email: "Email",
    calling: "Calling",
    linkedin: "LinkedIn",
    whatsapp: "WhatsApp",
    sms: "SMS",
    events: "Events / Webinar",
    ads: "Paid Ads",
    content: "Content / SEO",
    referral: "Referral",
    reddit: "Reddit",
    other: "Other",
  })[t] || "Email";
const campaignStatusClass = (s) =>
  s === "completed"
    ? "badge badge-ok"
    : s === "active"
      ? "badge badge-info"
      : s === "paused"
        ? "badge badge-warn"
        : "badge badge-muted";

const incentiveStatusClass = (s) =>
  s === "paid"
    ? "badge badge-ok"
    : s === "approved"
      ? "badge badge-info"
      : "badge badge-warn";

const taskStatusClass = (s) =>
  s === "done"
    ? "badge badge-ok"
    : s === "in_progress"
      ? "badge badge-info"
      : s === "blocked"
        ? "badge badge-warn"
        : "badge badge-muted";

// Client-lead notes are an append-only history stored as a JSON array of
// { text, at, by }. Legacy rows hold a plain string -> single entry.
function parseLeadNotes(raw) {
  if (Array.isArray(raw))
    return raw.filter((n) => n && typeof n === "object" && n.text != null);
  if (typeof raw !== "string") return [];
  const t = raw.trim();
  if (!t) return [];
  if (t.charAt(0) === "[") {
    try {
      const arr = JSON.parse(t);
      if (Array.isArray(arr))
        return arr.filter((n) => n && typeof n === "object" && n.text != null);
    } catch (e) {
      /* fall through */
    }
  }
  return [{ text: t, at: null, by: null }];
}

// Visibility chips (internal-only vs client-visible markers).
const VisInternal = () => (
  <span className="vis-chip vis-internal">INTERNAL</span>
);
const VisClient = () => <span className="vis-chip vis-client">CLIENT</span>;

const ICON = (n) => (n ? <span className="tab-ico" aria-hidden="true">{n}</span> : null);

export default function ClientWorkspace({ data, view }) {
  const {
    client,
    contacts = [],
    services = [],
    workItems = [],
    updates = [],
    actions = [],
    contributors = [],
    milestones = [],
    documents = [],
    users = [],
    linkedTasks = [],
    activityLogs = [],
    blockers = [],
    meetings = [],
    campaigns = [],
    incentives = [],
    reports = [],
    leads = [],
    leadAllRows = [],
    leadCounts = {},
    staticLeadBusiness = null,
  } = data;

  const {
    activeTab,
    todayStr,
    timelineEvents,
    overdueCount,
    highPriorityCount,
    openBlockerCount,
    openWorkCount,
    openActionsCount,
    leadsBadge,
    meetingsThisWeek,
    momPendingCount,
    syncCompliant,
    nextMeetingDate,
    totalSent,
    totalResponses,
    totalPositiveReplies,
    teamTaskRows,
    employeeCards,
    leadsLast3,
    leadsLast7,
    convertedCount,
    daysSinceLastLead,
    daysSinceLastMeeting,
    perfAlerts,
    leadLabelById,
    totalIncentive,
    incentiveLeadOptions,
    weeklyRows,
    weeklyTotals,
    weeklyRangeLabel,
    dailyRows,
    dailyTotals,
    dailyRangeLabel,
    leadsAddedToday,
    leadsAddedWeek,
    funnelSnapshot,
    consecutiveTransitions,
    outreachMovementRows,
    demoMovementRows,
    totalMovesToday,
    totalMovesWeek,
    memberFunnelRows,
    memberFunnelTotals,
    statsCards,
    stages,
  } = view;

  const clientId = Number(client.id);
  const PIPELINE = stages.pipeline;
  const OUTREACH = stages.outreach;
  const DEMO = stages.demo;

  const isOverdue = (w) =>
    w.due_date && String(w.due_date).slice(0, 10) < todayStr && w.status !== "done";
  const getUserName = (id) =>
    users.find((u) => String(u.id) === String(id))?.name || "-";
  const getWorkItemTitle = (id) =>
    workItems.find((w) => String(w.id) === String(id))?.title || "";
  const getMilestoneTitle = (id) =>
    milestones.find((m) => String(m.id) === String(id))?.title || "";

  // -------------------------------------------------------------------------
  // Shared mutation helpers — fetch + reload, mirroring the original handlers.
  // -------------------------------------------------------------------------
  const [loading, setLoading] = useState(null);
  const showLoading = (msg) => setLoading(msg || "Loading...");

  const send = async (url, method, body) =>
    (
      await fetch(url, {
        method,
        headers: { "Content-Type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
      })
    ).json();

  // -------------------------------------------------------------------------
  // Modal state. Each modal stores its form fields (null = closed).
  // -------------------------------------------------------------------------
  const [milestone, setMilestone] = useState(null);
  const [workItem, setWorkItem] = useState(null);
  const [workItemDetail, setWorkItemDetail] = useState(null); // {loading, form, meta}
  const [lead, setLead] = useState(null); // add/edit lead form
  const [blocker, setBlocker] = useState(null);
  const [meeting, setMeeting] = useState(null); // {isEdit, aiNotes, aiBusy, fields...}
  const [campaign, setCampaign] = useState(null);
  const [incentive, setIncentive] = useState(null);
  const [report, setReport] = useState(null);
  const [clientUpdate, setClientUpdate] = useState(null);
  const [actionModal, setActionModal] = useState(null);
  const [contributor, setContributor] = useState(null);
  const [leadNotesHistory, setLeadNotesHistory] = useState(null); // {notes}
  const [leadNote, setLeadNote] = useState(null); // {leadId, text}
  const [leadDemoNote, setLeadDemoNote] = useState(null); // {leadId, demoStatus, prev, selectEl, text}
  const [leadStageNote, setLeadStageNote] = useState(null); // {leadId, stage, prev, selectEl, text}
  const [openReach, setOpenReach] = useState(null); // leadId with open reach dropdown
  const [reportView, setReportView] = useState("daily");

  const closeAllModals = () => {
    setMilestone(null);
    setWorkItem(null);
    setWorkItemDetail(null);
    setLead(null);
    setBlocker(null);
    setMeeting(null);
    setCampaign(null);
    setIncentive(null);
    setReport(null);
    setClientUpdate(null);
    setActionModal(null);
    setContributor(null);
    setLeadNotesHistory(null);
    setLeadNote(null);
    setLeadDemoNote(null);
    setLeadStageNote(null);
  };

  // Escape closes any open modal.
  useEffect(() => {
    const onKey = (e) => {
      if (e.key === "Escape") closeAllModals();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  // Close reach dropdowns when clicking outside one.
  useEffect(() => {
    const onClick = (e) => {
      if (!e.target.closest || !e.target.closest(".reach-ms")) setOpenReach(null);
    };
    document.addEventListener("click", onClick);
    return () => document.removeEventListener("click", onClick);
  }, []);

  // Auto-open the Add Lead modal when navigated here with ?addLead=1, and
  // initialise the report sub-view from the URL hash (Daily / Week 1).
  useEffect(() => {
    try {
      const params = new URLSearchParams(window.location.search);
      if (params.get("addLead") === "1") openClientLeadModal();
    } catch (e) {
      /* ignore */
    }
    if (/week1/i.test(window.location.hash || "")) setReportView("week");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ===========================================================================
  // Handlers
  // ===========================================================================
  async function generateClientViewLink() {
    const newTab = window.open("", "_blank");
    if (newTab) newTab.opener = null;
    const json = await send(
      "/api/clients/" + clientId + "/client-view-link",
      "POST",
    );
    if (!json.ok) {
      if (newTab) newTab.close();
      alert(json.error || "Failed to create client link");
      return;
    }
    const url = json.data.url;
    if (newTab) newTab.location.href = url;
    else window.open(url, "_blank", "noopener,noreferrer");
  }

  // --- Milestones ---
  const openMilestoneModal = () =>
    setMilestone({ id: "", title: "", due_date: "", status: "planned", notes: "" });
  const openMilestoneEdit = (m) =>
    setMilestone({
      id: m.id,
      title: m.title || "",
      due_date: m.due_date || "",
      status: m.status || "planned",
      notes: m.notes || "",
    });
  async function saveMilestone() {
    const payload = {
      title: (milestone.title || "").trim(),
      due_date: milestone.due_date || null,
      status: milestone.status,
      notes: (milestone.notes || "").trim(),
    };
    if (!payload.title) {
      alert("Milestone title is required");
      return;
    }
    const url = milestone.id
      ? "/api/clients/" + clientId + "/milestones/" + milestone.id
      : "/api/clients/" + clientId + "/milestones";
    const json = await send(url, milestone.id ? "PUT" : "POST", payload);
    if (!json.ok) {
      alert(json.error || "Failed to save milestone");
      return;
    }
    window.location.reload();
  }
  async function closeMilestone(id) {
    if (!confirm("Close this milestone?")) return;
    const json = await send(
      "/api/clients/" + clientId + "/milestones/" + id,
      "PUT",
      { status: "closed" },
    );
    if (!json.ok) {
      alert(json.error || "Failed to close milestone");
      return;
    }
    window.location.reload();
  }
  async function archiveMilestone(id) {
    if (
      !confirm(
        "Archive this milestone? Work items will remain, but the milestone will be hidden.",
      )
    )
      return;
    const json = await send(
      "/api/clients/" + clientId + "/milestones/" + id + "/archive",
      "POST",
    );
    if (!json.ok) {
      alert(json.error || "Failed to archive milestone");
      return;
    }
    window.location.reload();
  }

  // --- Actions ---
  const openActionModal = () =>
    setActionModal({
      id: "",
      title: "",
      owner_type: "WeSolve",
      owner_name: "",
      due_date: "",
      status: "Open",
      priority: "Medium",
      notes: "",
    });
  const openActionEdit = (a) =>
    setActionModal({
      id: a.id,
      title: a.title || "",
      owner_type: a.owner_type || "WeSolve",
      owner_name: a.owner_name || "",
      due_date: a.due_date || "",
      status: a.status || "Open",
      priority: a.priority || "Medium",
      notes: a.notes || "",
    });
  async function saveAction() {
    const payload = {
      title: (actionModal.title || "").trim(),
      owner_type: actionModal.owner_type,
      owner_name: (actionModal.owner_name || "").trim(),
      due_date: actionModal.due_date || null,
      status: actionModal.status,
      priority: actionModal.priority,
      notes: (actionModal.notes || "").trim(),
    };
    if (!payload.title) {
      alert("Action title is required");
      return;
    }
    const url = actionModal.id
      ? "/api/clients/" + clientId + "/actions/" + actionModal.id
      : "/api/clients/" + clientId + "/actions";
    const json = await send(url, actionModal.id ? "PUT" : "POST", payload);
    if (!json.success && !json.ok) {
      alert(json.error || "Failed to save action");
      return;
    }
    window.location.reload();
  }
  async function archiveAction(id) {
    if (!confirm("Archive this action?")) return;
    const json = await send(
      "/api/clients/" + clientId + "/actions/" + id + "/archive",
      "POST",
    );
    if (!json.success && !json.ok) {
      alert(json.error || "Failed to archive action");
      return;
    }
    window.location.reload();
  }

  // --- Contributors ---
  const openContributorModal = () =>
    setContributor({
      id: "",
      person_type: "Contractor",
      name: "",
      email: "",
      phone: "",
      role: "",
      status: "Active",
      can_update_work: true,
      can_view_client_dashboard: false,
      notes: "",
    });
  const openContributorEdit = (p) =>
    setContributor({
      id: p.id,
      person_type: p.person_type || "Contractor",
      name: p.name || "",
      email: p.email || "",
      phone: p.phone || "",
      role: p.role || "",
      status: p.status || "Active",
      can_update_work: !!p.can_update_work,
      can_view_client_dashboard: !!p.can_view_client_dashboard,
      notes: p.notes || "",
    });
  async function saveContributor() {
    const payload = {
      person_type: contributor.person_type,
      name: (contributor.name || "").trim(),
      email: (contributor.email || "").trim(),
      phone: (contributor.phone || "").trim(),
      role: (contributor.role || "").trim(),
      status: contributor.status,
      can_update_work: contributor.can_update_work,
      can_view_client_dashboard: contributor.can_view_client_dashboard,
      notes: (contributor.notes || "").trim(),
    };
    if (!payload.name || !payload.role) {
      alert("Name and role are required");
      return;
    }
    const url = contributor.id
      ? "/api/clients/" + clientId + "/contributors/" + contributor.id
      : "/api/clients/" + clientId + "/contributors";
    const json = await send(url, contributor.id ? "PUT" : "POST", payload);
    if (!json.success && !json.ok) {
      alert(json.error || "Failed to save contributor");
      return;
    }
    window.location.reload();
  }
  async function archiveContributor(id) {
    if (!confirm("Archive this contributor?")) return;
    const json = await send(
      "/api/clients/" + clientId + "/contributors/" + id + "/archive",
      "POST",
    );
    if (!json.success && !json.ok) {
      alert(json.error || "Failed to archive contributor");
      return;
    }
    window.location.reload();
  }

  // --- Client updates ---
  const openClientUpdateModal = () =>
    setClientUpdate({
      title: "",
      related_work_item_id: "",
      update_type: "general",
      visibility: "internal",
      update_text: "",
    });
  async function createClientUpdate() {
    const updateText = (clientUpdate.update_text || "").trim();
    if (!updateText) {
      alert("Update text is required");
      return;
    }
    showLoading("Saving client update...");
    const json = await send("/api/clients/" + clientId + "/updates", "POST", {
      title: (clientUpdate.title || "").trim(),
      update_text: updateText,
      update_type: clientUpdate.update_type,
      related_work_item_id: clientUpdate.related_work_item_id || null,
      is_client_visible: clientUpdate.visibility === "client",
    });
    if (!json.ok) {
      alert(json.error || "Failed to save update");
      setLoading(null);
      setClientUpdate(null);
      return;
    }
    window.location.reload();
  }

  // --- Work items ---
  const openWorkItemModal = () =>
    setWorkItem({
      title: "",
      owner_user_id: "",
      priority: "medium",
      due_date: "",
      dependency_work_item_id: "",
      milestone_id: "",
      description: "",
    });
  async function createWorkItem() {
    const title = (workItem.title || "").trim();
    if (!title) {
      alert("Title is required");
      return;
    }
    showLoading("Creating work item...");
    const json = await send("/api/client-work-items", "POST", {
      client_id: clientId,
      title,
      description: (workItem.description || "").trim(),
      owner_user_id: workItem.owner_user_id || null,
      priority: workItem.priority,
      due_date: workItem.due_date || null,
      dependency_work_item_id: workItem.dependency_work_item_id || null,
      milestone_id: workItem.milestone_id || null,
    });
    if (!json.ok) {
      alert("Create failed: " + (json.error || "Unknown error"));
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function quickUpdateWorkItem(id, status) {
    showLoading("Updating work item status...");
    const json = await send("/api/client-work-items/" + id, "PATCH", { status });
    if (!json.ok) {
      alert(json.error || "Failed to update work item");
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function archiveWorkItem(id) {
    if (
      !confirm(
        "Archive this work item? It will be hidden but not permanently deleted.",
      )
    )
      return;
    showLoading("Archiving work item...");
    const json = await send("/api/client-work-items/" + id, "PATCH", {
      archive: true,
    });
    if (!json.ok) {
      alert(json.error || "Failed to archive work item");
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function openWorkItemDetail(id) {
    setWorkItemDetail({ loading: true, form: null, meta: null });
    const json = await send("/api/client-work-items/" + id, "GET");
    if (!json.ok) {
      setWorkItemDetail({ loading: false, error: json.error || "Failed to load work item" });
      return;
    }
    const w = json.data;
    setWorkItemDetail({
      loading: false,
      form: {
        id: w.id,
        title: w.title || "",
        status: w.status || "todo",
        priority: w.priority || "medium",
        due_date: w.due_date || "",
        owner_user_id: w.owner_user_id || "",
        dependency_work_item_id: w.dependency_work_item_id || "",
        milestone_id: w.milestone_id || "",
        description: w.description || "",
      },
      meta: { created_at: w.created_at || "-", updated_at: w.updated_at || "-" },
    });
  }
  async function saveWorkItemChanges() {
    const f = workItemDetail.form;
    const title = (f.title || "").trim();
    if (!title) {
      alert("Title is required");
      return;
    }
    showLoading("Saving work item changes...");
    const json = await send("/api/client-work-items/" + f.id, "PATCH", {
      title,
      status: f.status,
      priority: f.priority,
      owner_user_id: f.owner_user_id || null,
      due_date: f.due_date || null,
      dependency_work_item_id: f.dependency_work_item_id || null,
      milestone_id: f.milestone_id || null,
      description: (f.description || "").trim(),
    });
    if (!json.ok) {
      alert(json.error || "Failed to update work item");
      setLoading(null);
      setWorkItemDetail(null);
      return;
    }
    window.location.reload();
  }

  // --- Leads ---
  function openClientLeadModal() {
    setLead({
      id: "",
      company: "",
      contact_name: "",
      phone: "",
      email: "",
      city: "",
      state: "",
      website: "",
      lead_category: "b2b",
      lead_source: "manual",
      pipeline_stage: "prospect_identified",
      outreach_status: "not_started",
      demo_status: "not_scheduled",
      status: "new",
      assigned_to: "",
      newNote: "",
      notesHistory: [],
    });
  }
  async function openClientLeadDetail(leadId) {
    showLoading("Loading lead...");
    try {
      const json = await send("/api/clients/" + clientId + "/leads/" + leadId, "GET");
      if (!json.ok) {
        alert(json.error || "Failed to load lead");
        setLoading(null);
        return;
      }
      const l = json.data || {};
      setLoading(null);
      setLead({
        id: l.id,
        company: l.company || l.business_name || "",
        contact_name: l.contact_name || "",
        phone: l.phone || "",
        email: l.email || "",
        city: l.city || "",
        state: l.state || "",
        website: l.website || "",
        lead_category: l.lead_category || "b2b",
        lead_source: l.lead_source || "manual",
        pipeline_stage: l.pipeline_stage || "prospect_identified",
        outreach_status: l.outreach_status || "not_started",
        demo_status: l.demo_status || "not_scheduled",
        status: l.status || "new",
        assigned_to: l.assigned_to || "",
        newNote: "",
        notesHistory: parseLeadNotes(l.notes),
      });
    } catch (e) {
      setLoading(null);
      alert("Failed to load lead");
    }
  }
  async function saveClientLead() {
    const company = (lead.company || "").trim();
    const phone = (lead.phone || "").trim();
    if (!company && !phone) {
      alert("Enter at least a company name or phone number.");
      return;
    }
    const leadId = lead.id;
    const newNote = (lead.newNote || "").trim();
    const payload = {
      business_name: company,
      company,
      contact_name: (lead.contact_name || "").trim(),
      phone,
      email: (lead.email || "").trim(),
      city: (lead.city || "").trim(),
      state: (lead.state || "").trim(),
      website: (lead.website || "").trim(),
      lead_category: lead.lead_category,
      lead_source: lead.lead_source,
      pipeline_stage: lead.pipeline_stage,
      outreach_status: lead.outreach_status,
      demo_status: lead.demo_status,
      status: lead.status,
      assigned_to: (lead.assigned_to || "").trim(),
    };
    showLoading(leadId ? "Updating lead..." : "Creating lead...");
    const url = leadId
      ? "/api/clients/" + clientId + "/leads/" + leadId
      : "/api/clients/" + clientId + "/leads";
    const json = await send(url, leadId ? "PATCH" : "POST", payload);
    if (!json.ok) {
      alert((leadId ? "Update" : "Create") + " failed: " + (json.error || "Unknown error"));
      setLoading(null);
      return;
    }
    const savedLeadId = leadId || (json.data && json.data.id);
    if (savedLeadId && newNote) {
      const noteJson = await send(
        "/api/clients/" + clientId + "/leads/" + savedLeadId,
        "PATCH",
        { add_note: newNote },
      );
      if (!noteJson.ok) {
        alert("Lead saved, but note failed: " + (noteJson.error || "Unknown error"));
        setLoading(null);
        return;
      }
    }
    window.location.reload();
  }
  async function deleteClientLead(leadId) {
    const hasSwal = typeof window !== "undefined" && typeof window.Swal !== "undefined";
    if (hasSwal) {
      const result = await window.Swal.fire({
        title: "Delete this lead?",
        text: "Are you sure you want to delete that lead?",
        icon: "warning",
        showCancelButton: true,
        confirmButtonText: "Yes, delete it",
        cancelButtonText: "No",
        reverseButtons: true,
      });
      if (!result.isConfirmed) return;
    } else if (!confirm("Are you sure you want to delete that lead?")) {
      return;
    }
    showLoading("Deleting lead...");
    const json = await send(
      "/api/clients/" + clientId + "/leads/" + leadId,
      "DELETE",
    );
    if (!json.ok) {
      if (hasSwal) window.Swal.fire("Error", json.error || "Failed to delete lead", "error");
      else alert(json.error || "Failed to delete lead");
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  // Pipeline-stage change requires a note (themed modal) before saving.
  function updateLeadStage(leadId, stage, selectEl) {
    setLeadStageNote({
      leadId,
      stage,
      prev: selectEl ? selectEl.dataset.prev : undefined,
      selectEl: selectEl || null,
      text: "",
    });
  }
  function cancelLeadStageNote() {
    if (
      leadStageNote &&
      leadStageNote.selectEl &&
      typeof leadStageNote.prev !== "undefined"
    ) {
      leadStageNote.selectEl.value = leadStageNote.prev;
    }
    setLeadStageNote(null);
  }
  async function confirmLeadStageNote() {
    const notes = leadStageNote.text;
    if (!notes || !notes.trim()) {
      alert("Add a note before saving the status change.");
      return;
    }
    const { leadId, stage } = leadStageNote;
    setLeadStageNote(null);
    showLoading("Updating pipeline stage...");
    const json = await send("/api/clients/" + clientId + "/leads/" + leadId, "PATCH", {
      pipeline_stage: stage,
      add_note: notes.trim(),
    });
    if (!json.ok) {
      alert(json.error || "Failed to update stage");
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  function updateLeadDemo(leadId, demoStatus, selectEl) {
    setLeadDemoNote({
      leadId,
      demoStatus,
      prev: selectEl ? selectEl.dataset.prev : undefined,
      selectEl: selectEl || null,
      text: "",
    });
  }
  function cancelLeadDemoNote() {
    if (
      leadDemoNote &&
      leadDemoNote.selectEl &&
      typeof leadDemoNote.prev !== "undefined"
    ) {
      leadDemoNote.selectEl.value = leadDemoNote.prev;
    }
    setLeadDemoNote(null);
  }
  async function confirmLeadDemoNote() {
    const notes = leadDemoNote.text;
    if (!notes || !notes.trim()) {
      alert("Add a note before saving the demo status change.");
      return;
    }
    const { leadId, demoStatus } = leadDemoNote;
    setLeadDemoNote(null);
    showLoading("Updating demo status...");
    const json = await send("/api/clients/" + clientId + "/leads/" + leadId, "PATCH", {
      demo_status: demoStatus,
      add_note: notes.trim(),
    });
    if (!json.ok) {
      alert(json.error || "Failed to update demo status");
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  function openLeadNoteModal(leadId) {
    setLeadNote({ leadId, text: "" });
  }
  async function confirmLeadNote() {
    const text = (leadNote.text || "").trim();
    if (!text) {
      alert("Write a note first.");
      return;
    }
    const { leadId } = leadNote;
    setLeadNote(null);
    showLoading("Saving note...");
    const json = await send("/api/clients/" + clientId + "/leads/" + leadId, "PATCH", {
      add_note: text,
    });
    if (!json.ok) {
      alert(json.error || "Failed to save note");
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function openLeadNotesHistory(leadId) {
    showLoading("Loading notes...");
    try {
      const json = await send("/api/clients/" + clientId + "/leads/" + leadId, "GET");
      setLoading(null);
      if (!json.ok) {
        alert(json.error || "Failed to load notes");
        return;
      }
      setLeadNotesHistory({ notes: parseLeadNotes((json.data || {}).notes) });
    } catch (e) {
      setLoading(null);
      alert("Failed to load notes");
    }
  }
  async function updateLinkedTask(taskId, field, value) {
    showLoading("Updating task...");
    const json = await send(
      "/api/clients/" + clientId + "/linked-tasks/" + taskId,
      "PATCH",
      { [field]: value },
    );
    if (!json.ok) {
      alert(json.error || "Failed to update task");
      window.location.reload();
      return;
    }
    window.location.reload();
  }
  async function toggleLeadVisible(leadId, isVisible) {
    showLoading("Updating visibility...");
    const json = await send("/api/clients/" + clientId + "/leads/" + leadId, "PATCH", {
      is_client_visible: isVisible,
    });
    if (!json.ok) {
      alert(json.error || "Failed to update visibility");
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function updateLeadReached(l, channel, checked) {
    const body = {
      reached_via_linkedin:
        channel === "linkedin" ? checked : !!l.reached_via_linkedin,
      reached_via_email: channel === "email" ? checked : !!l.reached_via_email,
    };
    showLoading("Updating reach channels...");
    const json = await send("/api/clients/" + clientId + "/leads/" + l.id, "PATCH", body);
    if (!json.ok) {
      alert(json.error || "Failed to update reach channels");
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function uploadClientLeadsExcel(file) {
    if (!file) {
      alert("Choose an Excel file first.");
      return;
    }
    const formData = new FormData();
    formData.append("file", file);
    showLoading("Importing leads from Excel...");
    const res = await fetch("/api/clients/" + clientId + "/leads/import-excel", {
      method: "POST",
      body: formData,
    });
    const json = await res.json();
    if (!json.ok) {
      alert("Import failed: " + (json.error || "Unknown error"));
      setLoading(null);
      return;
    }
    const d = json.data || {};
    alert(
      [
        "Import complete",
        "Total rows: " + d.total,
        "Inserted: " + d.inserted,
        "Duplicates skipped: " + d.duplicates,
        "Empty skipped: " + d.skipped,
        "Errors: " + ((d.errors || []).length),
      ].join(String.fromCharCode(10)),
    );
    window.location.reload();
  }

  // --- Blockers ---
  const openBlockerModal = () =>
    setBlocker({
      id: "",
      title: "",
      description: "",
      blocker_side: "internal",
      priority: "medium",
      owner_user_id: "",
      related_work_item_id: "",
      resolution_status: "open",
      isEdit: false,
    });
  const openBlockerDetail = (id) => {
    const b = blockers.find((x) => String(x.id) === String(id));
    if (!b) {
      alert("Blocker not found");
      return;
    }
    setBlocker({
      id: b.id,
      title: b.title || "",
      description: b.description || "",
      blocker_side: b.blocker_side || "internal",
      priority: b.priority || "medium",
      owner_user_id: b.owner_user_id || "",
      related_work_item_id: b.related_work_item_id || "",
      resolution_status: b.resolution_status || "open",
      isEdit: true,
    });
  };
  async function saveBlocker() {
    const title = (blocker.title || "").trim();
    if (!title) {
      alert("Title is required");
      return;
    }
    const payload = {
      title,
      description: (blocker.description || "").trim(),
      blocker_side: blocker.blocker_side,
      priority: blocker.priority,
      owner_user_id: blocker.owner_user_id || null,
      related_work_item_id: blocker.related_work_item_id || null,
    };
    if (blocker.id) payload.resolution_status = blocker.resolution_status;
    showLoading(blocker.id ? "Updating blocker..." : "Creating blocker...");
    const url = blocker.id
      ? "/api/clients/" + clientId + "/blockers/" + blocker.id
      : "/api/clients/" + clientId + "/blockers";
    const json = await send(url, blocker.id ? "PATCH" : "POST", payload);
    if (!json.ok) {
      alert((blocker.id ? "Update" : "Create") + " failed: " + (json.error || "Unknown error"));
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function updateBlocker(id, patch) {
    showLoading("Updating blocker...");
    const json = await send("/api/clients/" + clientId + "/blockers/" + id, "PATCH", patch);
    if (!json.ok) {
      alert(json.error || "Failed to update blocker");
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function archiveBlocker(id) {
    if (!confirm("Archive this blocker? It will be hidden but not permanently deleted."))
      return;
    showLoading("Archiving blocker...");
    const json = await send("/api/clients/" + clientId + "/blockers/" + id, "PATCH", {
      archive: true,
    });
    if (!json.ok) {
      alert(json.error || "Failed to archive blocker");
      setLoading(null);
      return;
    }
    window.location.reload();
  }

  // --- Meetings ---
  const emptyMeeting = (isEdit) => ({
    id: "",
    title: "",
    meeting_date: "",
    meeting_type: "sync_call",
    participants: "",
    summary: "",
    discussion_points: "",
    decisions: "",
    deliverables: "",
    action_items: "",
    follow_ups: "",
    next_steps: "",
    aiNotes: "",
    aiBusy: false,
    isEdit,
  });
  const openMeetingModal = () => setMeeting(emptyMeeting(false));
  const openMeetingDetail = (id) => {
    const m = meetings.find((x) => String(x.id) === String(id));
    if (!m) {
      alert("Meeting not found");
      return;
    }
    setMeeting({
      id: m.id,
      title: m.title || "",
      meeting_date: m.meeting_date || "",
      meeting_type: m.meeting_type || "sync_call",
      participants: m.participants || "",
      summary: m.summary || "",
      discussion_points: m.discussion_points || "",
      decisions: m.decisions || "",
      deliverables: m.deliverables || "",
      action_items: m.action_items || "",
      follow_ups: m.follow_ups || "",
      next_steps: m.next_steps || "",
      aiNotes: "",
      aiBusy: false,
      isEdit: true,
    });
  };
  async function aiFillMeetingFromNotes() {
    const notes = (meeting.aiNotes || "").trim();
    if (!notes) {
      alert("Write or paste the meeting details first.");
      return;
    }
    setMeeting((p) => ({ ...p, aiBusy: true }));
    try {
      const json = await send("/api/ai/parse-meeting-notes", "POST", { notes });
      if (!json.ok) {
        alert(json.error || "AI auto-fill failed");
        return;
      }
      const m = json.data || {};
      setMeeting((p) => {
        const next = { ...p };
        [
          "title",
          "meeting_date",
          "meeting_type",
          "participants",
          "summary",
          "discussion_points",
          "decisions",
          "deliverables",
          "action_items",
          "follow_ups",
          "next_steps",
        ].forEach((f) => {
          if (m[f]) next[f] = m[f];
        });
        if (!next.meeting_date)
          next.meeting_date = new Date().toISOString().slice(0, 10);
        return next;
      });
    } catch (e) {
      alert("AI auto-fill failed: " + (e && e.message ? e.message : "network error"));
    } finally {
      setMeeting((p) => (p ? { ...p, aiBusy: false } : p));
    }
  }
  async function saveMeeting() {
    const fields = [
      "title",
      "meeting_date",
      "meeting_type",
      "participants",
      "summary",
      "discussion_points",
      "decisions",
      "deliverables",
      "action_items",
      "follow_ups",
      "next_steps",
    ];
    const payload = {};
    fields.forEach((f) => {
      payload[f] = (meeting[f] || "").trim();
    });
    if (!payload.title && !payload.meeting_date) {
      alert("Meeting title or date is required");
      return;
    }
    showLoading(meeting.id ? "Updating meeting..." : "Saving meeting...");
    const url = meeting.id
      ? "/api/clients/" + clientId + "/meetings/" + meeting.id
      : "/api/clients/" + clientId + "/meetings";
    const json = await send(url, meeting.id ? "PATCH" : "POST", payload);
    if (!json.ok) {
      alert((meeting.id ? "Update" : "Save") + " failed: " + (json.error || "Unknown error"));
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function archiveMeeting(id) {
    if (!confirm("Archive this meeting? It will be hidden but not permanently deleted."))
      return;
    showLoading("Archiving meeting...");
    const json = await send("/api/clients/" + clientId + "/meetings/" + id, "PATCH", {
      archive: true,
    });
    if (!json.ok) {
      alert(json.error || "Failed to archive meeting");
      setLoading(null);
      return;
    }
    window.location.reload();
  }

  // --- Campaigns ---
  const openCampaignModal = () =>
    setCampaign({
      id: "",
      name: "",
      campaign_type: "email",
      channel: "",
      status: "planned",
      sent_count: "0",
      response_count: "0",
      positive_replies: "0",
      notes: "",
    });
  const openCampaignDetail = (id) => {
    const c = campaigns.find((x) => String(x.id) === String(id));
    if (!c) {
      alert("Campaign not found");
      return;
    }
    setCampaign({
      id: c.id,
      name: c.name || "",
      campaign_type: c.campaign_type || "email",
      channel: c.channel || "",
      status: c.status || "planned",
      sent_count: c.sent_count || 0,
      response_count: c.response_count || 0,
      positive_replies: c.positive_replies || 0,
      notes: c.notes || "",
    });
  };
  async function saveCampaign() {
    const name = (campaign.name || "").trim();
    if (!name) {
      alert("Campaign name is required");
      return;
    }
    const payload = {
      name,
      campaign_type: campaign.campaign_type,
      channel: (campaign.channel || "").trim(),
      status: campaign.status,
      sent_count: Number(campaign.sent_count) || 0,
      response_count: Number(campaign.response_count) || 0,
      positive_replies: Number(campaign.positive_replies) || 0,
      notes: (campaign.notes || "").trim(),
    };
    showLoading(campaign.id ? "Updating campaign..." : "Saving campaign...");
    const url = campaign.id
      ? "/api/clients/" + clientId + "/campaigns/" + campaign.id
      : "/api/clients/" + clientId + "/campaigns";
    const json = await send(url, campaign.id ? "PATCH" : "POST", payload);
    if (!json.ok) {
      alert((campaign.id ? "Update" : "Save") + " failed: " + (json.error || "Unknown error"));
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function archiveCampaign(id) {
    if (!confirm("Archive this campaign?")) return;
    showLoading("Archiving campaign...");
    const json = await send("/api/clients/" + clientId + "/campaigns/" + id, "PATCH", {
      archive: true,
    });
    if (!json.ok) {
      alert(json.error || "Failed to archive campaign");
      setLoading(null);
      return;
    }
    window.location.reload();
  }

  // --- Incentives ---
  const openIncentiveModal = () =>
    setIncentive({
      id: "",
      title: "",
      gtm_user_id: "",
      related_lead_id: "",
      amount: "0",
      status: "pending",
      notes: "",
    });
  const openIncentiveDetail = (id) => {
    const i = incentives.find((x) => String(x.id) === String(id));
    if (!i) {
      alert("Incentive not found");
      return;
    }
    setIncentive({
      id: i.id,
      title: i.title || "",
      gtm_user_id: i.gtm_user_id || "",
      related_lead_id: i.related_lead_id || "",
      amount: i.amount || 0,
      status: i.status || "pending",
      notes: i.notes || "",
    });
  };
  async function saveIncentive() {
    const title = (incentive.title || "").trim();
    if (!title) {
      alert("Incentive title is required");
      return;
    }
    const payload = {
      title,
      gtm_user_id: incentive.gtm_user_id || null,
      related_lead_id: incentive.related_lead_id || null,
      amount: Number(incentive.amount) || 0,
      status: incentive.status,
      notes: (incentive.notes || "").trim(),
    };
    showLoading(incentive.id ? "Updating incentive..." : "Saving incentive...");
    const url = incentive.id
      ? "/api/clients/" + clientId + "/incentives/" + incentive.id
      : "/api/clients/" + clientId + "/incentives";
    const json = await send(url, incentive.id ? "PATCH" : "POST", payload);
    if (!json.ok) {
      alert((incentive.id ? "Update" : "Save") + " failed: " + (json.error || "Unknown error"));
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function archiveIncentive(id) {
    if (!confirm("Archive this incentive?")) return;
    showLoading("Archiving incentive...");
    const json = await send("/api/clients/" + clientId + "/incentives/" + id, "PATCH", {
      archive: true,
    });
    if (!json.ok) {
      alert(json.error || "Failed to archive incentive");
      setLoading(null);
      return;
    }
    window.location.reload();
  }

  // --- Weekly reports ---
  const openReportModal = () =>
    setReport({
      id: "",
      period_label: "",
      week_start: "",
      summary: "",
      highlights: "",
      lowlights: "",
      next_week_plan: "",
      is_client_visible: true,
    });
  const openReportDetail = (id) => {
    const r = reports.find((x) => String(x.id) === String(id));
    if (!r) {
      alert("Report not found");
      return;
    }
    setReport({
      id: r.id,
      period_label: r.period_label || "",
      week_start: r.week_start || "",
      summary: r.summary || "",
      highlights: r.highlights || "",
      lowlights: r.lowlights || "",
      next_week_plan: r.next_week_plan || "",
      is_client_visible: r.is_client_visible !== false,
    });
  };
  async function saveReport() {
    const payload = {
      period_label: (report.period_label || "").trim(),
      week_start: report.week_start || null,
      summary: (report.summary || "").trim(),
      highlights: (report.highlights || "").trim(),
      lowlights: (report.lowlights || "").trim(),
      next_week_plan: (report.next_week_plan || "").trim(),
      is_client_visible: report.is_client_visible,
    };
    if (!payload.period_label && !payload.week_start && !payload.summary) {
      alert("Add a period label, week start, or summary");
      return;
    }
    showLoading(report.id ? "Updating report..." : "Saving report...");
    const url = report.id
      ? "/api/clients/" + clientId + "/reports/" + report.id
      : "/api/clients/" + clientId + "/reports";
    const json = await send(url, report.id ? "PATCH" : "POST", payload);
    if (!json.ok) {
      alert((report.id ? "Update" : "Save") + " failed: " + (json.error || "Unknown error"));
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function updateReport(id, patch) {
    showLoading("Updating report...");
    const json = await send("/api/clients/" + clientId + "/reports/" + id, "PATCH", patch);
    if (!json.ok) {
      alert(json.error || "Failed to update report");
      setLoading(null);
      return;
    }
    window.location.reload();
  }
  async function archiveReport(id) {
    if (!confirm("Archive this report?")) return;
    showLoading("Archiving report...");
    const json = await send("/api/clients/" + clientId + "/reports/" + id, "PATCH", {
      archive: true,
    });
    if (!json.ok) {
      alert(json.error || "Failed to archive report");
      setLoading(null);
      return;
    }
    window.location.reload();
  }

  // ===========================================================================
  // Render helpers
  // ===========================================================================
  const tabHref = (key) => "/clients/" + clientId + "?tab=" + key;
  const tabLink = (key, label, icon = "", count = null, tone = "") => {
    const showCount = typeof count === "number" && count > 0;
    const countCls = "tab-count" + (tone === "attention" ? " tab-count-attention" : "");
    return (
      <a
        className={"tab " + (activeTab === key ? "active" : "")}
        href={tabHref(key)}
        aria-current={activeTab === key ? "page" : "false"}
      >
        {ICON(icon)}
        <span className="tab-label">{label}</span>
        {showCount ? (
          <span className={countCls}>{count > 99 ? "99+" : count}</span>
        ) : null}
      </a>
    );
  };

  const movementTable = (title, rows, emptyLabel) => (
    <div style={{ marginTop: "14px" }}>
      <div className="section-subtitle" style={{ marginBottom: "8px" }}>{title}</div>
      <div style={{ overflowX: "auto" }}>
        <table className="work-table" style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <th style={{ textAlign: "left" }}>Movement</th>
              <th style={{ textAlign: "left" }}>Today</th>
              <th style={{ textAlign: "left" }}>Last 7 days</th>
            </tr>
          </thead>
          <tbody>
            {rows.some((r) => r.today || r.week) ? (
              rows.map((r, i) => (
                <tr key={i}>
                  <td>{r.label}</td>
                  <td>{r.today}</td>
                  <td>{r.week}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={3} className="meta">{emptyLabel}</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );

  const autoReportPanel = (heading, rangeLabel, totals, rows, emptyLabel) => (
    <div className="panel">
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          gap: "12px",
          alignItems: "center",
          marginBottom: "14px",
          flexWrap: "wrap",
        }}
      >
        <div>
          <h2 style={{ margin: 0 }}>{heading}</h2>
          <div className="section-subtitle">{rangeLabel}</div>
        </div>
      </div>
      <div className="kpi-grid">
        <div className="kpi-card"><div className="kpi-label">{heading === "Weekly Report " ? "Campaigns this week" : "Campaigns today"}</div><div className="kpi-value">{totals.campaigns}</div></div>
        <div className="kpi-card"><div className="kpi-label">Leads converted</div><div className="kpi-value">{totals.converted}</div></div>
        <div className="kpi-card"><div className="kpi-label">Meetings</div><div className="kpi-value">{totals.meetings}</div></div>
        <div className="kpi-card"><div className="kpi-label">MOMs recorded</div><div className="kpi-value">{totals.moms}</div></div>
        <div className="kpi-card"><div className="kpi-label">Blockers raised</div><div className="kpi-value">{totals.blockers}</div></div>
        <div className="kpi-card"><div className="kpi-label">Incentives ₹</div><div className="kpi-value">{totals.incentive}</div></div>
      </div>
      <div style={{ overflowX: "auto" }}>
        <table className="work-table" style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <th style={{ textAlign: "left" }}>Team Member</th>
              <th style={{ textAlign: "left" }}>Campaigns</th>
              <th style={{ textAlign: "left" }}>Leads Converted</th>
              <th style={{ textAlign: "left" }}>Meetings</th>
              <th style={{ textAlign: "left" }}>MOMs</th>
              <th style={{ textAlign: "left" }}>Blockers</th>
              <th style={{ textAlign: "left" }}>Incentives ₹</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((r) => (
                <tr key={r.key}>
                  <td style={{ fontWeight: 800 }}>{r.name}</td>
                  <td>{r.campaigns}</td>
                  <td>{r.converted}</td>
                  <td>{r.meetings}</td>
                  <td>{r.moms}</td>
                  <td>{r.blockers}</td>
                  <td>{r.incentive}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={7} className="meta">{emptyLabel}</td>
              </tr>
            )}
          </tbody>
          {rows.length ? (
            <tfoot>
              <tr style={{ borderTop: "2px solid var(--line-strong)" }}>
                <td style={{ fontWeight: 900 }}>Total</td>
                <td style={{ fontWeight: 900 }}>{totals.campaigns}</td>
                <td style={{ fontWeight: 900 }}>{totals.converted}</td>
                <td style={{ fontWeight: 900 }}>{totals.meetings}</td>
                <td style={{ fontWeight: 900 }}>{totals.moms}</td>
                <td style={{ fontWeight: 900 }}>{totals.blockers}</td>
                <td style={{ fontWeight: 900 }}>{totals.incentive}</td>
              </tr>
            </tfoot>
          ) : null}
        </table>
      </div>
    </div>
  );

  const fileInputRef = useRef(null);

  // GTM associate names for the overview tab.
  const gtmAssociateNames =
    (Array.isArray(client.gtm_associate_user_ids)
      ? client.gtm_associate_user_ids
      : []
    )
      .map((id) => (users.find((u) => String(u.id) === String(id)) || {}).name)
      .filter(Boolean)
      .join(", ") || "-";

  return (
    <div className="wrap">
      {/* Topbar */}
      <div className="topbar">
        <div>
          <div className="eyebrow">Client Workspace internal</div>
          <h1>{client.name}</h1>
          <div className="subtitle">
            {(client.company_name || "-") + " · " + (client.status || "-") + " · " + (client.health_status || "-")}
          </div>
        </div>
        <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
          <a className="btn" href="/clients">← Clients</a>
          {client.google_drive_folder_url ? (
            <a className="btn" href={client.google_drive_folder_url} target="_blank" rel="noopener noreferrer">Drive</a>
          ) : null}
          <a className="btn" href="https://notebooklm.google.com/notebook/76c66777-16e6-447f-b6a7-d40befa08590" target="_blank" rel="noopener noreferrer">Notebook</a>
          <button className="btn" type="button" onClick={generateClientViewLink}>External Link</button>
          <a className="btn btn-primary" href={"/clients/" + client.id + "/edit"}>Edit Client</a>
          <a className="btn" href={"/clients/" + client.id + "/reset"}>Reset</a>
        </div>
      </div>

      {/* Contextual stats row */}
      {statsCards && statsCards.length ? (
        <div className="stats">
          {statsCards.map(([label, value], i) => (
            <div className="stat-card" key={i}>
              <div className="stat-label">{label}</div>
              <div className="stat-value">{String(value)}</div>
            </div>
          ))}
        </div>
      ) : null}

      {/* Tabs */}
      <div className="tabs" role="tablist">
        {tabLink("overview", "Overview")}
        {tabLink("task", "Task", openWorkCount, overdueCount > 0 ? "attention" : "")}
        {tabLink("leads", "Leads", leadsBadge)}
        {tabLink("campaigns", "Campaigns", campaigns.length)}
        {tabLink("meetings", "Meetings & MOMs")}
        {tabLink("blockers", "Blockers", openBlockerCount, openBlockerCount > 0 ? "attention" : "")}
        {tabLink("team", "Team")}
        {tabLink("performance", "Performance")}
        {tabLink("incentives", "Incentives")}
        <div className="tab-flyout-wrap">
          <a
            className={"tab " + (activeTab === "report" ? "active" : "")}
            href={tabHref("report")}
            aria-current={activeTab === "report" ? "page" : "false"}
          >
            <span className="tab-label">Report</span>
          </a>
          <div className="tab-flyout" role="menu">
            <a
              className="tab-flyout-item"
              role="menuitem"
              href={tabHref("report") + "#daily"}
              onClick={(e) => {
                if (changeReportView("daily")) return;
                e.preventDefault();
              }}
            >
              Daily Report
            </a>
            <a
              className="tab-flyout-item"
              role="menuitem"
              href={tabHref("report") + "#week1"}
              onClick={(e) => {
                if (changeReportView("week")) return;
                e.preventDefault();
              }}
            >
              Week 1 Report
            </a>
          </div>
        </div>
        {tabLink("actions", "Actions Needed", openActionsCount, openActionsCount > 0 ? "attention" : "")}
      </div>

      <div className="tab-content-wrap">
        {/* Overview */}
        {activeTab === "overview" ? (
          <>
            <div className="grid-2">
              <div className="panel">
                <h2>Overview</h2>
                <div className="meta"><strong>Description:</strong> {client.description || "-"}</div>
                <div className="meta"><strong>Start Date:</strong> {client.start_date || "-"}</div>
                <div className="meta"><strong>Slug:</strong> {client.slug || "-"}</div>
                <div className="meta"><strong>Account Manager:</strong> {client.account_manager_name || "-"}</div>
                <div className="meta"><strong>Project Manager:</strong> {client.project_manager_name || "-"}</div>
                <div className="meta"><strong>GTM Associates:</strong> {gtmAssociateNames}</div>
                <div className="meta">
                  <strong>Last Activity:</strong>{" "}
                  {timelineEvents.length
                    ? formatDateTime(timelineEvents[0].at) + " · " + timelineEvents[0].text
                    : "-"}
                </div>
                <div className="meta">
                  <strong>Google Drive Folder:</strong>{" "}
                  {client.google_drive_folder_url ? (
                    <a href={client.google_drive_folder_url} target="_blank" rel="noopener noreferrer">📁 Open Client Folder</a>
                  ) : (
                    <span style={{ color: "var(--danger)" }}>Not set</span>
                  )}
                </div>
              </div>
              <div className="panel">
                <h2>Services</h2>
                {services.length ? (
                  services.map((s, i) => (
                    <div className="item" key={i}>
                      <div className="item-title">{s.name}</div>
                    </div>
                  ))
                ) : (
                  <div className="meta">No services selected.</div>
                )}
              </div>
            </div>

            <div className="grid-2">
              <div className="panel">
                <h2>Client Contacts</h2>
                {contacts.length ? (
                  contacts.map((c) => (
                    <div className="item" key={c.id}>
                      <div className="item-title">{(c.name || "-") + (c.is_primary ? " · Primary" : "")}</div>
                      <div className="meta">{c.role || "-"}</div>
                      <div className="meta">{(c.email || "-") + " · " + (c.phone || "-")}</div>
                    </div>
                  ))
                ) : (
                  <div className="meta">No contacts added.</div>
                )}
              </div>
              <div className="panel">
                <h2>Recent Updates</h2>
                {updates.length ? (
                  updates.map((u) => (
                    <div className="item" key={u.id}>
                      <div className="item-title">{u.title || "Update"}</div>
                      <div className="meta">{u.update_text || ""}</div>
                    </div>
                  ))
                ) : (
                  <div className="meta">No updates yet.</div>
                )}
              </div>
            </div>
          </>
        ) : null}

        {/* Leads */}
        {activeTab === "leads" ? (
          staticLeadBusiness ? (
            <iframe
              id="clientLeadsFrame"
              src={"/leads/" + encodeURIComponent(staticLeadBusiness) + "?embed=1"}
              title={staticLeadBusiness + " leads"}
              style={{
                width: "100%",
                height: "82vh",
                border: "1px solid var(--line)",
                borderRadius: "var(--radius-lg)",
                background: "transparent",
              }}
            />
          ) : (
            <div className="panel">
              <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
                <div>
                  <h2 style={{ margin: 0 }}>Leads</h2>
                  <div className="meta">{leadAllRows.length} total</div>
                </div>
                <div style={{ display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
                  <input
                    type="file"
                    ref={fileInputRef}
                    accept=".xlsx,.xls,.csv"
                    style={{ display: "none" }}
                    onChange={(e) => uploadClientLeadsExcel(e.target.files && e.target.files[0])}
                  />
                  <button className="btn" type="button" onClick={() => fileInputRef.current && fileInputRef.current.click()}>⬆ Import from Excel</button>
                  <button className="btn btn-primary" type="button" onClick={openClientLeadModal}>+ Add Lead</button>
                </div>
              </div>
              <div style={{ overflowX: "auto" }}>
                <table className="work-table" style={{ width: "100%", borderCollapse: "collapse" }}>
                  <thead>
                    <tr>
                      <th style={{ textAlign: "left", width: "200px" }}>Company</th>
                      <th style={{ textAlign: "left", width: "200px" }}>Phone / Email / Source</th>
                      <th style={{ textAlign: "left" }}>Status</th>
                      <th style={{ textAlign: "left" }}>Demo</th>
                      <th style={{ textAlign: "left", width: "360px" }}>Notes</th>
                      <th style={{ textAlign: "left" }}>Reached Via</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {leads.length ? (
                      leads.map((l) => {
                        const stage = l.pipeline_stage || "prospect_identified";
                        const outreach = l.outreach_status || "not_started";
                        const demo = l.demo_status || "not_scheduled";
                        const company = l.company || l.business_name || "(no name)";
                        const loc = [l.city, l.state].filter(Boolean).join(", ");
                        const noteHistory = parseLeadNotes(l.notes);
                        const latestNote = noteHistory.length ? noteHistory[noteHistory.length - 1] : null;
                        const reachChannels = [
                          l.reached_via_linkedin ? "LinkedIn" : null,
                          l.reached_via_email ? "Email" : null,
                        ].filter(Boolean);
                        const reachLabel = reachChannels.length ? reachChannels.join(", ") : "Select";
                        const latestNoteByline = latestNote
                          ? [latestNote.by || "", latestNote.at ? formatDateTimeNoTz(latestNote.at) : ""].filter(Boolean).join(" · ")
                          : "";
                        return (
                          <tr className="client-lead-row" data-stage={stage} key={l.id}>
                            <td>
                              <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                                <input
                                  type="checkbox"
                                  title="Client visible"
                                  defaultChecked={!!l.is_client_visible}
                                  onChange={(e) => toggleLeadVisible(Number(l.id), e.target.checked)}
                                />
                                <span
                                  style={{ fontWeight: 800, cursor: "pointer", textDecoration: "underline" }}
                                  title="Open / Edit"
                                  onClick={() => openClientLeadDetail(Number(l.id))}
                                >
                                  {company}
                                </span>
                              </div>
                              {loc ? <div className="meta">{loc}</div> : null}
                              <div className="meta">{l.contact_name || ""}</div>
                            </td>
                            <td style={{ width: "130px", fontSize: "12px", wordBreak: "break-word" }}>
                              <div>{l.phone || "-"}</div>
                              <div className="meta">{l.email || "-"}</div>
                              <div className="meta">{l.lead_source || "-"}</div>
                            </td>
                            <td>
                              <select
                                className="stage-select"
                                defaultValue={stage}
                                onFocus={(e) => (e.target.dataset.prev = e.target.value)}
                                onChange={(e) => updateLeadStage(Number(l.id), e.target.value, e.target)}
                              >
                                {PIPELINE.map((s) => (
                                  <option value={s.key} key={s.key}>{s.label}</option>
                                ))}
                              </select>
                            </td>
                            <td>
                              <select
                                className="stage-select"
                                defaultValue={demo}
                                onFocus={(e) => (e.target.dataset.prev = e.target.value)}
                                onChange={(e) => updateLeadDemo(Number(l.id), e.target.value, e.target)}
                              >
                                {DEMO.map((s) => (
                                  <option value={s.key} key={s.key}>{s.label}</option>
                                ))}
                              </select>
                            </td>
                            <td style={{ width: "360px" }}>
                              {latestNote ? (
                                <>
                                  <div
                                    style={{
                                      fontSize: "12px",
                                      whiteSpace: "pre-wrap",
                                      display: "-webkit-box",
                                      WebkitLineClamp: 2,
                                      WebkitBoxOrient: "vertical",
                                      overflow: "hidden",
                                      textOverflow: "ellipsis",
                                    }}
                                    title={latestNote.text}
                                  >
                                    {latestNote.text}
                                  </div>
                                  {latestNoteByline ? (
                                    <div className="meta" style={{ fontSize: "11px" }}>{latestNoteByline}</div>
                                  ) : null}
                                  {noteHistory.length > 1 ? (
                                    <div
                                      className="meta"
                                      style={{ fontSize: "11px", cursor: "pointer", textDecoration: "underline" }}
                                      onClick={() => openLeadNotesHistory(Number(l.id))}
                                    >
                                      {"+" + (noteHistory.length - 1) + " earlier note" + (noteHistory.length - 1 === 1 ? "" : "s")}
                                    </div>
                                  ) : null}
                                </>
                              ) : (
                                <div className="meta" style={{ fontSize: "12px" }}>No notes yet</div>
                              )}
                              <button
                                className="btn"
                                type="button"
                                style={{ padding: "2px 8px", marginTop: "4px", fontSize: "11px" }}
                                onClick={() => openLeadNoteModal(Number(l.id))}
                              >
                                + Add note
                              </button>
                            </td>
                            <td>
                              <div className="reach-ms" style={{ position: "relative", display: "inline-block" }}>
                                <button
                                  type="button"
                                  className="btn"
                                  style={{ padding: "4px 10px", fontSize: "12px", minWidth: "120px", textAlign: "left", display: "flex", justifyContent: "space-between", gap: "6px", alignItems: "center" }}
                                  onClick={() => setOpenReach(openReach === l.id ? null : l.id)}
                                >
                                  <span>{reachLabel}</span>
                                  <span style={{ opacity: 0.6 }}>▾</span>
                                </button>
                                <div
                                  className="reach-ms-panel"
                                  style={{
                                    display: openReach === l.id ? "block" : "none",
                                    position: "absolute",
                                    zIndex: 50,
                                    top: "calc(100% + 4px)",
                                    left: 0,
                                    background: "var(--card, #1e1e2e)",
                                    border: "1px solid var(--line)",
                                    borderRadius: "8px",
                                    padding: "6px",
                                    minWidth: "130px",
                                    boxShadow: "0 8px 24px rgba(0,0,0,0.25)",
                                  }}
                                >
                                  <label style={{ display: "block", fontSize: "12px", whiteSpace: "nowrap", padding: "3px 4px" }}>
                                    <input type="checkbox" value="linkedin" defaultChecked={!!l.reached_via_linkedin} onChange={(e) => updateLeadReached(l, "linkedin", e.target.checked)} /> LinkedIn
                                  </label>
                                  <label style={{ display: "block", fontSize: "12px", whiteSpace: "nowrap", padding: "3px 4px" }}>
                                    <input type="checkbox" value="email" defaultChecked={!!l.reached_via_email} onChange={(e) => updateLeadReached(l, "email", e.target.checked)} /> Email
                                  </label>
                                </div>
                              </div>
                            </td>
                            <td style={{ textAlign: "center", width: "40px" }}>
                              <button
                                type="button"
                                title="Delete lead"
                                aria-label="Delete lead"
                                onClick={() => deleteClientLead(Number(l.id))}
                                style={{ background: "transparent", border: "none", color: "#ef4444", cursor: "pointer", fontSize: "15px", lineHeight: 1, padding: "4px 6px", borderRadius: "6px" }}
                              >
                                ✕
                              </button>
                            </td>
                          </tr>
                        );
                      })
                    ) : (
                      <tr>
                        <td colSpan={7} className="meta">No leads yet for this client. Add the first lead.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )
        ) : null}

        {/* Campaigns */}
        {activeTab === "campaigns" ? (
          <div className="panel">
            <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
              <div>
                <h2 style={{ margin: 0 }}>Campaigns</h2>
                <div className="section-subtitle">Email · Calling · LinkedIn · WhatsApp outreach</div>
              </div>
              <button className="btn btn-primary" type="button" onClick={openCampaignModal}>+ Add Campaign</button>
            </div>
            <div className="work-summary-chips" style={{ marginBottom: "12px" }}>
              <span className="summary-chip">Campaigns {campaigns.length}</span>
              <span className="summary-chip">Active {campaigns.filter((c) => c.status === "active").length}</span>
              <span className="summary-chip">Total sent {totalSent}</span>
              <span className="summary-chip">Total responses {totalResponses}</span>
              <span className="summary-chip">Positive replies {totalPositiveReplies}</span>
            </div>
            <div style={{ overflowX: "auto" }}>
              <table className="work-table" style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left" }}>Campaign</th>
                    <th style={{ textAlign: "left" }}>Type</th>
                    <th style={{ textAlign: "left" }}>Channel</th>
                    <th style={{ textAlign: "left" }}>Status</th>
                    <th style={{ textAlign: "left" }}>Sent</th>
                    <th style={{ textAlign: "left" }}>Responses</th>
                    <th style={{ textAlign: "left" }}>Positive</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {campaigns.length ? (
                    campaigns.map((c) => {
                      const sent = Number(c.sent_count) || 0;
                      const responses = Number(c.response_count) || 0;
                      const positive = Number(c.positive_replies) || 0;
                      const rate = sent ? Math.round((responses / sent) * 100) : 0;
                      return (
                        <tr key={c.id}>
                          <td><div style={{ fontWeight: 800 }}>{c.name || "Untitled"}</div></td>
                          <td><span className="badge badge-muted">{campaignTypeLabel(c.campaign_type)}</span></td>
                          <td>{c.channel || "-"}</td>
                          <td><span className={campaignStatusClass(c.status)}>{c.status || "planned"}</span></td>
                          <td>{sent}</td>
                          <td>{responses}{sent ? " (" + rate + "%)" : ""}</td>
                          <td>{positive}</td>
                          <td>
                            <button className="btn" type="button" onClick={() => openCampaignDetail(Number(c.id))}>Edit</button>{" "}
                            <button className="btn" type="button" onClick={() => archiveCampaign(Number(c.id))}>Archive</button>
                          </td>
                        </tr>
                      );
                    })
                  ) : (
                    <tr>
                      <td colSpan={8} className="meta">No campaigns yet. Add an email, calling, LinkedIn, or WhatsApp campaign.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        ) : null}

        {/* Meetings */}
        {activeTab === "meetings" ? (
          <div className="panel">
            <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
              <div>
                <h2 style={{ margin: 0 }}>Meetings &amp; MOMs</h2>
                <div className="section-subtitle">Call log, minutes of meeting, and sync-call compliance</div>
              </div>
              <button className="btn btn-primary" type="button" onClick={openMeetingModal}>+ Log Meeting</button>
            </div>
            <div className="kpi-grid">
              <div className="kpi-card"><div className="kpi-label">Total meetings</div><div className="kpi-value">{meetings.length}</div></div>
              <div className="kpi-card"><div className="kpi-label">This week</div><div className="kpi-value">{meetingsThisWeek}</div></div>
              <div className="kpi-card"><div className="kpi-label">Sync compliance</div><div className="kpi-value" style={{ marginTop: "6px" }}><span className={"badge " + (syncCompliant ? "badge-ok" : "badge-warn")}>{syncCompliant ? "On track" : "Overdue"}</span></div></div>
              <div className="kpi-card"><div className="kpi-label">MOM pending</div><div className="kpi-value">{momPendingCount}</div></div>
              <div className="kpi-card"><div className="kpi-label">Next meeting</div><div className="kpi-value">{nextMeetingDate || "—"}</div></div>
            </div>
            <div style={{ overflowX: "auto" }}>
              <table className="work-table" style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left" }}>Date</th>
                    <th style={{ textAlign: "left" }}>Participants</th>
                    <th style={{ textAlign: "left" }}>Summary</th>
                    <th style={{ textAlign: "left" }}>Status</th>
                    <th style={{ textAlign: "left" }}>MOM</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {meetings.length ? (
                    meetings.map((m) => {
                      const filled = momFilled(m);
                      return (
                        <tr style={{ cursor: "pointer" }} onClick={() => openMeetingDetail(Number(m.id))} key={m.id}>
                          <td>
                            <div style={{ fontWeight: 800 }}>{m.meeting_date || "No date"}</div>
                            <div className="meta">{m.title || "Meeting"}</div>
                          </td>
                          <td>{m.participants ? meetingClip(m.participants, 60) : <span className="meta">—</span>}</td>
                          <td>{m.summary ? meetingClip(m.summary, 90) : <span className="meta">—</span>}</td>
                          <td><span className="badge badge-info">{meetingTypeLabel(m.meeting_type)}</span></td>
                          <td><span className={"badge " + (filled ? "badge-ok" : "badge-warn")}>{filled ? "Done" : "Pending"}</span></td>
                          <td onClick={(e) => e.stopPropagation()}>
                            <button className="btn" type="button" onClick={() => openMeetingDetail(Number(m.id))}>Edit</button>{" "}
                            <button className="btn" type="button" onClick={() => archiveMeeting(Number(m.id))}>Archive</button>
                          </td>
                        </tr>
                      );
                    })
                  ) : (
                    <tr>
                      <td colSpan={6} className="meta">No meetings logged yet. Record the first client meeting or sync call.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        ) : null}

        {/* Blockers */}
        {activeTab === "blockers" ? (
          <div className="panel">
            <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
              <div>
                <h2 style={{ margin: 0 }}>Blockers</h2>
                <div className="section-subtitle">Internal &amp; client-side blockers · ownership · resolution status</div>
              </div>
              <button className="btn btn-primary" type="button" onClick={openBlockerModal}>+ Add Blocker</button>
            </div>
            {openBlockerCount ? (
              <div className="alert-strip"><span>⛔ {openBlockerCount} open blocker{openBlockerCount === 1 ? "" : "s"}</span></div>
            ) : null}
            <div className="work-summary-chips" style={{ marginBottom: "12px" }}>
              <span className="summary-chip">Total {blockers.length}</span>
              <span className="summary-chip">Open {blockers.filter((b) => b.resolution_status === "open").length}</span>
              <span className="summary-chip">In progress {blockers.filter((b) => b.resolution_status === "in_progress").length}</span>
              <span className="summary-chip">Resolved {blockers.filter((b) => b.resolution_status === "resolved").length}</span>
            </div>
            <div className="standard-list">
              {blockers.length ? (
                blockers.map((b) => {
                  const relatedTitle = b.related_work_item_id
                    ? getWorkItemTitle(b.related_work_item_id) || "#" + b.related_work_item_id
                    : "-";
                  return (
                    <div className="standard-card" key={b.id}>
                      <div className="standard-card-top">
                        <div>
                          <div className="standard-card-title">{b.title || "Untitled blocker"}</div>
                          <div className="meta">{b.description || "No description"}</div>
                        </div>
                        <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", justifyContent: "flex-end" }}>
                          <span className="badge badge-muted">{blockerSideLabel(b.blocker_side)}</span>
                          <span className={priorityBadgeClass(b.priority)}>{b.priority || "medium"}</span>
                          <span className={blockerStatusClass(b.resolution_status)}>{String(b.resolution_status || "open").replaceAll("_", " ")}</span>
                        </div>
                      </div>
                      <div className="work-card-meta">
                        <div><strong>Owner:</strong> {getUserName(b.owner_user_id)}</div>
                        <div><strong>Related work item:</strong> {relatedTitle}</div>
                        <div><strong>Created:</strong> {b.created_at ? formatDateTime(b.created_at) : "-"}</div>
                      </div>
                      <div className="work-card-actions">
                        {b.resolution_status === "open" ? (
                          <button className="btn" type="button" onClick={() => updateBlocker(Number(b.id), { resolution_status: "in_progress" })}>Start</button>
                        ) : null}
                        {b.resolution_status !== "resolved" ? (
                          <button className="btn" type="button" onClick={() => updateBlocker(Number(b.id), { resolution_status: "resolved" })}>Resolve</button>
                        ) : null}
                        <button className="btn" type="button" onClick={() => openBlockerDetail(Number(b.id))}>Edit</button>
                        <button className="btn" type="button" onClick={() => archiveBlocker(Number(b.id))}>Archive</button>
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="meta">No blockers logged. Add one when something is blocking progress.</div>
              )}
            </div>
          </div>
        ) : null}

        {/* Performance */}
        {activeTab === "performance" ? (
          <div className="panel">
            <div style={{ marginBottom: "14px" }}>
              <h2 style={{ margin: 0 }}>Performance <VisInternal /></h2>
              <div className="section-subtitle">GTM velocity &amp; inactivity alerts (internal only)</div>
            </div>
            {perfAlerts.length ? (
              <div className="alert-strip">
                {perfAlerts.map((a, i) => (
                  <span key={i}>⚠️ {a}</span>
                ))}
              </div>
            ) : null}
            <div className="kpi-grid">
              <div className="kpi-card"><div className="kpi-label">Leads · last 3 days</div><div className="kpi-value">{leadsLast3}</div></div>
              <div className="kpi-card"><div className="kpi-label">Leads · last 7 days</div><div className="kpi-value">{leadsLast7}</div></div>
              <div className="kpi-card"><div className="kpi-label">Converted (total)</div><div className="kpi-value">{convertedCount}</div></div>
              <div className="kpi-card"><div className="kpi-label">Days since last lead</div><div className="kpi-value">{daysSinceLastLead === null ? "—" : daysSinceLastLead}</div></div>
              <div className="kpi-card"><div className="kpi-label">Days since last demo</div><div className="kpi-value">{daysSinceLastMeeting === null ? "—" : daysSinceLastMeeting}</div></div>
              <div className="kpi-card"><div className="kpi-label">Total leads</div><div className="kpi-value">{leadAllRows.length}</div></div>
            </div>
          </div>
        ) : null}

        {/* Incentives */}
        {activeTab === "incentives" ? (
          <div className="panel">
            <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
              <div>
                <h2 style={{ margin: 0 }}>Incentives <VisInternal /></h2>
                <div className="section-subtitle">Attribution · commission · credit log (internal only)</div>
              </div>
              <button className="btn btn-primary" type="button" onClick={openIncentiveModal}>+ Add Incentive</button>
            </div>
            <div className="work-summary-chips" style={{ marginBottom: "12px" }}>
              <span className="summary-chip">Entries {incentives.length}</span>
              <span className="summary-chip">Paid {incentives.filter((i) => i.status === "paid").length}</span>
              <span className="summary-chip">Total amount {totalIncentive}</span>
            </div>
            <div style={{ overflowX: "auto" }}>
              <table className="work-table" style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left" }}>Title</th>
                    <th style={{ textAlign: "left" }}>GTM (attribution)</th>
                    <th style={{ textAlign: "left" }}>Lead</th>
                    <th style={{ textAlign: "left" }}>Amount</th>
                    <th style={{ textAlign: "left" }}>Status</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {incentives.length ? (
                    incentives.map((i) => (
                      <tr key={i.id}>
                        <td style={{ fontWeight: 700 }}>{i.title || "-"}</td>
                        <td>{getUserName(i.gtm_user_id)}</td>
                        <td>{i.related_lead_id ? leadLabelById[String(i.related_lead_id)] || "Lead #" + i.related_lead_id : "-"}</td>
                        <td>{Number(i.amount) || 0}</td>
                        <td><span className={incentiveStatusClass(i.status)}>{i.status || "pending"}</span></td>
                        <td>
                          <button className="btn" type="button" onClick={() => openIncentiveDetail(Number(i.id))}>Edit</button>{" "}
                          <button className="btn" type="button" onClick={() => archiveIncentive(Number(i.id))}>Archive</button>
                        </td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan={6} className="meta">No incentives logged yet.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        ) : null}

        {/* Report */}
        {activeTab === "report" ? (
          <>
            <div className="report-subtabs" role="tablist">
              <button className={"report-subtab " + (reportView === "daily" ? "active" : "")} type="button" data-view="daily" role="tab" onClick={() => changeReportView("daily")}>Daily Report</button>
              <button className={"report-subtab " + (reportView === "week" ? "active" : "")} type="button" data-view="week" role="tab" onClick={() => changeReportView("week")}>Week 1 Report</button>
            </div>
            <div id="reportView-daily" className="report-subview" style={{ display: reportView === "week" ? "none" : "" }}>
              {autoReportPanel("Today's Report", dailyRangeLabel, dailyTotals, dailyRows, "No tracked activity today yet.")}
            </div>
            <div id="reportView-week" className="report-subview" style={{ display: reportView === "week" ? "" : "none" }}>
              {autoReportPanel("Weekly Report ", weeklyRangeLabel, weeklyTotals, weeklyRows, "No tracked activity in the last 7 days yet.")}
            </div>

            {/* Lead funnel */}
            <div className="panel">
              <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px", flexWrap: "wrap" }}>
                <div>
                  <h2 style={{ margin: 0 }}>Lead Funnel</h2>
                  <div className="section-subtitle">Today ({dailyRangeLabel}) vs last 7 days ({weeklyRangeLabel})</div>
                </div>
              </div>
              <div className="kpi-grid">
                <div className="kpi-card"><div className="kpi-label">Leads added today</div><div className="kpi-value">{leadsAddedToday}</div></div>
                <div className="kpi-card"><div className="kpi-label">Leads added (7 days)</div><div className="kpi-value">{leadsAddedWeek}</div></div>
                <div className="kpi-card"><div className="kpi-label">Status moves today</div><div className="kpi-value">{totalMovesToday}</div></div>
                <div className="kpi-card"><div className="kpi-label">Status moves (7 days)</div><div className="kpi-value">{totalMovesWeek}</div></div>
                <div className="kpi-card"><div className="kpi-label">Total leads</div><div className="kpi-value">{leadAllRows.length}</div></div>
              </div>
              {movementTable("Pipeline movement", [{ label: "Leads added", today: leadsAddedToday, week: leadsAddedWeek }, ...consecutiveTransitions], "No pipeline movement tracked yet.")}
              {movementTable("Outreach movement", outreachMovementRows, "No outreach movement tracked yet.")}
              {movementTable("Demo movement", demoMovementRows, "No demo movement tracked yet.")}
              <div style={{ marginTop: "14px" }}>
                <div className="section-subtitle" style={{ marginBottom: "8px" }}>By team member (last 7 days)</div>
                <div style={{ overflowX: "auto" }}>
                  <table className="work-table" style={{ width: "100%", borderCollapse: "collapse" }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left" }}>Team Member</th>
                        <th style={{ textAlign: "left" }}>Leads Added</th>
                        <th style={{ textAlign: "left" }}>Stage Moves</th>
                        <th style={{ textAlign: "left" }}>Outreach Moves</th>
                        <th style={{ textAlign: "left" }}>Demo Moves</th>
                        <th style={{ textAlign: "left" }}>Converted</th>
                      </tr>
                    </thead>
                    <tbody>
                      {memberFunnelRows.length ? (
                        memberFunnelRows.map((r) => (
                          <tr key={r.key}>
                            <td style={{ fontWeight: 800 }}>{r.name}</td>
                            <td>{r.leadsAdded}</td>
                            <td>{r.stageMoves}</td>
                            <td>{r.outreachMoves}</td>
                            <td>{r.demoMoves}</td>
                            <td>{r.converted}</td>
                          </tr>
                        ))
                      ) : (
                        <tr>
                          <td colSpan={6} className="meta">No per-member activity in the last 7 days yet.</td>
                        </tr>
                      )}
                    </tbody>
                    {memberFunnelRows.length ? (
                      <tfoot>
                        <tr style={{ borderTop: "2px solid var(--line-strong)" }}>
                          <td style={{ fontWeight: 900 }}>Total</td>
                          <td style={{ fontWeight: 900 }}>{memberFunnelTotals.leadsAdded}</td>
                          <td style={{ fontWeight: 900 }}>{memberFunnelTotals.stageMoves}</td>
                          <td style={{ fontWeight: 900 }}>{memberFunnelTotals.outreachMoves}</td>
                          <td style={{ fontWeight: 900 }}>{memberFunnelTotals.demoMoves}</td>
                          <td style={{ fontWeight: 900 }}>{memberFunnelTotals.converted}</td>
                        </tr>
                      </tfoot>
                    ) : null}
                  </table>
                </div>
              </div>
              <div style={{ marginTop: "14px" }}>
                <div className="section-subtitle" style={{ marginBottom: "8px" }}>Current funnel</div>
                <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                  {PIPELINE.map((s) => (
                    <span className="summary-chip" key={s.key}>{s.label}: {funnelSnapshot[s.key] || 0}</span>
                  ))}
                </div>
              </div>
            </div>

            {/* Published weekly reports */}
            <div className="panel">
              <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
                <div>
                  <h2 style={{ margin: 0 }}>Weekly Report <VisClient /></h2>
                  <div className="section-subtitle">PM publishes · visible to client when published</div>
                </div>
                <button className="btn btn-primary" type="button" onClick={openReportModal}>+ New Report</button>
              </div>
              <div className="standard-list">
                {reports.length ? (
                  reports.map((r) => {
                    const period = r.period_label || (r.week_start ? "Week of " + r.week_start : "Report");
                    return (
                      <div className="standard-card" key={r.id}>
                        <div className="standard-card-top">
                          <div>
                            <div className="standard-card-title">{period}</div>
                            <div className="meta">{r.created_at ? formatDateTime(r.created_at) : ""}</div>
                          </div>
                          <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", justifyContent: "flex-end" }}>
                            <span className={r.is_published ? "badge badge-ok" : "badge badge-muted"}>{r.is_published ? "Published" : "Draft"}</span>
                            {r.is_client_visible ? <VisClient /> : <VisInternal />}
                          </div>
                        </div>
                        {r.summary ? <div style={{ marginTop: "8px" }}><strong>Summary:</strong><div className="meta" style={{ whiteSpace: "pre-wrap" }}>{r.summary}</div></div> : null}
                        {r.highlights ? <div style={{ marginTop: "8px" }}><strong>Highlights:</strong><div className="meta" style={{ whiteSpace: "pre-wrap" }}>{r.highlights}</div></div> : null}
                        {r.lowlights ? <div style={{ marginTop: "8px" }}><strong>Lowlights / Risks:</strong><div className="meta" style={{ whiteSpace: "pre-wrap" }}>{r.lowlights}</div></div> : null}
                        {r.next_week_plan ? <div style={{ marginTop: "8px" }}><strong>Next Week Plan:</strong><div className="meta" style={{ whiteSpace: "pre-wrap" }}>{r.next_week_plan}</div></div> : null}
                        <div className="work-card-actions" style={{ marginTop: "12px" }}>
                          {r.is_published ? (
                            <button className="btn" type="button" onClick={() => updateReport(Number(r.id), { unpublish: true })}>Unpublish</button>
                          ) : (
                            <button className="btn" type="button" onClick={() => updateReport(Number(r.id), { publish: true })}>Publish</button>
                          )}
                          <button className="btn" type="button" onClick={() => openReportDetail(Number(r.id))}>Edit</button>
                          <button className="btn" type="button" onClick={() => archiveReport(Number(r.id))}>Archive</button>
                        </div>
                      </div>
                    );
                  })
                ) : (
                  <div className="meta">No weekly reports yet. Publish the first weekly update for this client.</div>
                )}
              </div>
            </div>
          </>
        ) : null}

        {/* Task */}
        {activeTab === "task" ? (
          <div className="panel">
            <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
              <div>
                <h2 style={{ margin: 0 }}>Task</h2>
                <div className="work-summary-chips">
                  <span className="summary-chip">All {workItems.length}</span>
                  <span className="summary-chip">Todo {workItems.filter((w) => w.status === "todo").length}</span>
                  <span className="summary-chip">In Progress {workItems.filter((w) => w.status === "in_progress").length}</span>
                  <span className="summary-chip">Done {workItems.filter((w) => w.status === "done").length}</span>
                  <span className="summary-chip">Overdue {overdueCount}</span>
                  <span className="summary-chip">High priority {highPriorityCount}</span>
                </div>
              </div>
              <button className="btn btn-primary" type="button" onClick={openWorkItemModal}>+ Add Work Item</button>
            </div>
            {overdueCount || highPriorityCount ? (
              <div className="alert-strip">
                {overdueCount ? <span>⚠️ {overdueCount} overdue task{overdueCount === 1 ? "" : "s"}</span> : null}
                {highPriorityCount ? <span>🔴 {highPriorityCount} open high-priority task{highPriorityCount === 1 ? "" : "s"}</span> : null}
              </div>
            ) : null}
            <div className="work-card-list">
              {workItems.length ? (
                workItems.map((w) => {
                  const ownerName = getUserName(w.owner_user_id);
                  const dep = w.dependency_work_item_id
                    ? workItems.find((x) => String(x.id) === String(w.dependency_work_item_id))
                    : null;
                  const isBlockedByDependency = dep && dep.status !== "done";
                  const statusClass =
                    w.status === "done"
                      ? "badge badge-ok"
                      : w.status === "in_progress"
                        ? "badge badge-info"
                        : isBlockedByDependency
                          ? "badge badge-warn"
                          : "badge badge-muted";
                  const dependencyText = dep
                    ? isBlockedByDependency
                      ? "Blocked by #" + dep.id + " · " + dep.title
                      : "Dependency complete: #" + dep.id + " · " + dep.title
                    : "No dependency";
                  return (
                    <div className="work-card" key={w.id}>
                      <div className="work-card-top">
                        <div>
                          <div className="work-card-title">{w.title || "Untitled"}</div>
                          <div className="meta">{w.description || "No description"}</div>
                        </div>
                        <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", justifyContent: "flex-end" }}>
                          {isOverdue(w) ? <span className="overdue-pill">Overdue</span> : null}
                          <span className={statusClass}>{isBlockedByDependency && w.status !== "done" ? "blocked" : w.status || "todo"}</span>
                          <span className={priorityBadgeClass(w.priority)}>{w.priority || "medium"}</span>
                        </div>
                      </div>
                      <div className="work-card-meta">
                        <div><strong>Owner:</strong> {ownerName}</div>
                        <div><strong>Due:</strong> {w.due_date || "-"}</div>
                        <div><strong>Depends:</strong> {dependencyText}</div>
                        <div><strong>Milestone:</strong> {w.milestone_id ? getMilestoneTitle(w.milestone_id) : "-"}</div>
                        <div><strong>Last updated:</strong> {w.updated_at ? formatDateTime(w.updated_at) : "-"}</div>
                      </div>
                      <div className="work-card-actions">
                        <button className="btn" type="button" onClick={() => openWorkItemDetail(Number(w.id))}>Open / Edit</button>
                        <button className="btn" type="button" onClick={() => quickUpdateWorkItem(Number(w.id), "in_progress")}>Start</button>
                        <button className="btn" type="button" onClick={() => quickUpdateWorkItem(Number(w.id), "done")}>Done</button>
                        <button className="btn" type="button" onClick={() => archiveWorkItem(Number(w.id))}>Archive</button>
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="meta">No work items yet. Add the first work item for this client.</div>
              )}
            </div>
          </div>
        ) : null}

        {/* Linked tasks (Task tab) */}
        {activeTab === "task" && linkedTasks.length ? (
          <div className="panel">
            <div style={{ marginBottom: "14px" }}>
              <h2 style={{ margin: 0 }}>Linked Tasks</h2>
              <div className="section-subtitle">Tasks from the task system where this client is set as the business · update status, priority &amp; progress inline</div>
            </div>
            <div className="work-card-list">
              {linkedTasks.map((t) => {
                const ownerName = getUserName(t.assigned_to_user_id);
                const taskRefNo = t.task_no || t.id;
                const openHref = t.assigned_to_user_id ? "/tasks/user/" + Number(t.assigned_to_user_id) : "";
                return (
                  <div className="work-card" key={t.id}>
                    <div className="work-card-top">
                      <div>
                        {openHref ? (
                          <a className="work-card-title" href={openHref} style={{ textDecoration: "none" }}>#{taskRefNo} · {t.title || "Untitled"}</a>
                        ) : (
                          <div className="work-card-title">#{taskRefNo} · {t.title || "Untitled"}</div>
                        )}
                      </div>
                      <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", justifyContent: "flex-end", alignItems: "center" }}>
                        <select className="stage-select" title="Status" defaultValue={t.status || "open"} onChange={(e) => updateLinkedTask(Number(t.id), "status", e.target.value)}>
                          {["open", "in_progress", "blocked", "done"].map((s) => (
                            <option value={s} key={s}>{s.replace("_", " ")}</option>
                          ))}
                        </select>
                        <select className="stage-select" title="Priority" defaultValue={t.priority || "medium"} onChange={(e) => updateLinkedTask(Number(t.id), "priority", e.target.value)}>
                          {["low", "medium", "high", "urgent"].map((p) => (
                            <option value={p} key={p}>{p}</option>
                          ))}
                        </select>
                        <label style={{ fontSize: "12px", whiteSpace: "nowrap", display: "inline-flex", alignItems: "center", gap: "4px" }} title="Show this task on the client's external dashboard">
                          <input type="checkbox" defaultChecked={!!t.is_client_visible} onChange={(e) => updateLinkedTask(Number(t.id), "is_client_visible", e.target.checked)} /> Client
                        </label>
                      </div>
                    </div>
                    <div className="work-card-meta">
                      <div><strong>Owner:</strong> {ownerName}</div>
                      <div><strong>Area:</strong> {t.area || "-"}</div>
                      <div><strong>Progress:</strong> <input type="number" min="0" max="100" step="5" defaultValue={Number(t.progress) || 0} style={{ width: "62px", padding: "4px 6px", borderRadius: "8px", border: "1px solid var(--line)", background: "rgba(255,255,255,0.04)", color: "var(--text)" }} onChange={(e) => updateLinkedTask(Number(t.id), "progress", e.target.value)} />%</div>
                      <div><strong>Due:</strong> {t.deadline || "-"}</div>
                      <div><strong>Last updated:</strong> {t.updated_at ? formatDateTime(t.updated_at) : "-"}</div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ) : null}

        {/* Updates */}
        {activeTab === "updates" ? (
          <div className="panel">
            <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
              <div>
                <h2 style={{ margin: 0 }}>Updates / Progress Timeline</h2>
                <div className="meta">Manual client updates + automatic work-item activity.</div>
              </div>
              <button className="btn btn-primary" type="button" onClick={openClientUpdateModal}>+ Add Update</button>
            </div>
            <div className="work-summary-chips">
              <span className="summary-chip">Manual Updates {updates.length}</span>
              <span className="summary-chip">Activity Logs {activityLogs.length}</span>
              <span className="summary-chip">Timeline {timelineEvents.length}</span>
            </div>
            <div style={{ marginTop: "16px" }}>
              {timelineEvents.length ? (
                timelineEvents.map((event, i) => (
                  <div className="item" key={i}>
                    <div className="item-title">
                      {event.title}
                      {event.relatedWorkItemTitle ? " · " + event.relatedWorkItemTitle : ""}
                    </div>
                    <div className="meta">{event.text}</div>
                    <div className="meta">
                      {(event.at ? formatDateTime(event.at) : "-") + " · by " + (event.by || "-") + " · " + (event.type === "manual_update" ? "Manual update" : "System activity")}
                    </div>
                  </div>
                ))
              ) : (
                <div className="meta">No updates or activity yet.</div>
              )}
            </div>
          </div>
        ) : null}

        {/* Actions */}
        {activeTab === "actions" ? (
          <div className="panel">
            <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
              <div>
                <h2 style={{ margin: 0 }}>Actions Needed</h2>
                <div className="meta">Track simple client or WeSolve follow-ups.</div>
              </div>
              <button className="btn btn-primary" type="button" onClick={openActionModal}>+ Add Action</button>
            </div>
            {actions.length ? (
              actions.map((a) => (
                <div className="work-card" key={a.id}>
                  <div className="work-card-top">
                    <div>
                      <div className="work-card-title">{a.title}</div>
                      <div className="meta">{a.notes || ""}</div>
                    </div>
                    <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                      <span className="badge badge-info">{a.status || "Open"}</span>
                      <span className="badge badge-muted">{a.priority || "Medium"}</span>
                    </div>
                  </div>
                  <div className="work-card-meta">
                    <div><strong>Owner:</strong> {(a.owner_type || "-") + " "}{a.owner_name ? "· " + a.owner_name : ""}</div>
                    <div><strong>Due:</strong> {a.due_date || "-"}</div>
                    <div><strong>Updated:</strong> {a.updated_at ? formatDateTime(a.updated_at) : "-"}</div>
                  </div>
                  <div className="work-card-actions">
                    <button className="btn" type="button" onClick={() => openActionEdit(a)}>Edit</button>
                    <button className="btn" type="button" onClick={() => archiveAction(Number(a.id))}>Archive</button>
                  </div>
                </div>
              ))
            ) : (
              <div className="meta">No actions yet.</div>
            )}
          </div>
        ) : null}

        {/* Team */}
        {activeTab === "team" ? (
          <div className="panel">
            <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
              <div>
                <h2 style={{ margin: 0 }}>Team <VisInternal /></h2>
                <div className="meta">Assigned employees, roles, and open task counts. Internal only.</div>
              </div>
              <button className="btn btn-primary" type="button" onClick={openContributorModal}>+ Add Contributor</button>
            </div>
            <div className="emp-section-head">
              <h3 style={{ margin: 0 }}>Employees on {client.name} <VisInternal /></h3>
              <div className="meta">Every WeSolve employee assigned to this project — assigned tasks, progress, deadlines, and current work status. Visible to the WeSolve team only.</div>
            </div>
            <div className="emp-card-list">
              {employeeCards.length ? (
                employeeCards.map((u, idx) => (
                  <div className="work-card emp-card" key={idx}>
                    <div className="work-card-top">
                      <div>
                        <div className="work-card-title">{u.name} <VisInternal /></div>
                        <div className="meta">{u.role}</div>
                      </div>
                      <span className={u.workState.cls}>{u.workState.label}</span>
                    </div>
                    <div className="emp-stat-row">
                      <div className="emp-stat"><div className="emp-stat-val">{u.total}</div><div className="emp-stat-label">Assigned</div></div>
                      <div className="emp-stat"><div className="emp-stat-val">{u.inProgressCount}</div><div className="emp-stat-label">In Progress</div></div>
                      <div className="emp-stat"><div className="emp-stat-val">{u.doneCount}</div><div className="emp-stat-label">Completed</div></div>
                      <div className="emp-stat"><div className="emp-stat-val" style={u.overdueCount ? { color: "#ffd7da" } : undefined}>{u.overdueCount}</div><div className="emp-stat-label">Overdue</div></div>
                      <div className="emp-stat"><div className="emp-stat-val">{u.nextDeadline || "—"}</div><div className="emp-stat-label">Next Deadline</div></div>
                    </div>
                    <div className="emp-overall">
                      <div style={{ display: "flex", justifyContent: "space-between", fontSize: "12px", marginBottom: "6px" }}>
                        <span className="meta">Overall progress</span>
                        <strong>{u.avgProgress}%</strong>
                      </div>
                      <div className="emp-prog"><span className="emp-prog-fill" style={{ width: u.avgProgress + "%" }}></span></div>
                    </div>
                    <div style={{ overflowX: "auto", marginTop: "14px" }}>
                      <table className="work-table" style={{ width: "100%", borderCollapse: "collapse" }}>
                        <thead>
                          <tr>
                            <th style={{ textAlign: "left" }}>Assigned task</th>
                            <th style={{ textAlign: "left" }}>Status</th>
                            <th style={{ textAlign: "left" }}>Progress</th>
                            <th style={{ textAlign: "left" }}>Deadline</th>
                          </tr>
                        </thead>
                        <tbody>
                          {u.taskRows.length ? (
                            u.taskRows.map((t, ti) => (
                              <tr key={ti}>
                                <td>
                                  <div style={{ fontWeight: 700 }}>{t.title}</div>
                                  <div className="meta">{t.priorityLabel}</div>
                                </td>
                                <td><span className={t.statusBadgeClass}>{t.statusLabel}</span></td>
                                <td style={{ minWidth: "150px" }}>
                                  <div className="emp-prog"><span className="emp-prog-fill" style={{ width: t.prog + "%" }}></span></div>
                                  <div className="meta" style={{ marginTop: "4px" }}>{t.prog}%</div>
                                </td>
                                <td>
                                  {t.dueDate ? (
                                    <span className={t.over ? "overdue-pill" : "meta"}>{t.dueDate}{t.over ? " · overdue" : ""}</span>
                                  ) : (
                                    <span className="meta">No deadline</span>
                                  )}
                                </td>
                              </tr>
                            ))
                          ) : (
                            <tr>
                              <td colSpan={4} className="meta">No tasks assigned to this employee yet.</td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ))
              ) : (
                <div className="meta">No WeSolve employees assigned to this project yet. Set an account/project manager or assign work items to populate this section.</div>
              )}
            </div>

            <h3 style={{ margin: "24px 0 10px" }}>Open task load</h3>
            <div style={{ overflowX: "auto", marginBottom: "18px" }}>
              <table className="work-table" style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left" }}>Team member</th>
                    <th style={{ textAlign: "left" }}>Role</th>
                    <th style={{ textAlign: "left" }}>Open tasks</th>
                  </tr>
                </thead>
                <tbody>
                  {teamTaskRows.length ? (
                    teamTaskRows.map((u, i) => (
                      <tr key={i}>
                        <td style={{ fontWeight: 700 }}>{u.name}</td>
                        <td>{u.role}</td>
                        <td>{u.count}</td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan={3} className="meta">No team members assigned yet. Set an account/project manager or assign work items.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            <h3 style={{ margin: "6px 0 10px" }}>Contributors</h3>
            {contributors.length ? (
              contributors.map((p) => (
                <div className="work-card" key={p.id}>
                  <div className="work-card-top">
                    <div>
                      <div className="work-card-title">{p.name}</div>
                      <div className="meta">{p.role || "-"}</div>
                    </div>
                    <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                      <span className="badge badge-info">{p.person_type || "-"}</span>
                      <span className="badge badge-muted">{p.status || "Active"}</span>
                    </div>
                  </div>
                  <div className="work-card-meta">
                    <div><strong>Email:</strong> {p.email || "-"}</div>
                    <div><strong>Phone:</strong> {p.phone || "-"}</div>
                    <div><strong>Can update work:</strong> {p.can_update_work ? "Yes" : "No"}</div>
                    <div><strong>Can view client dashboard:</strong> {p.can_view_client_dashboard ? "Yes" : "No"}</div>
                  </div>
                  {p.notes ? <div className="meta" style={{ marginTop: "10px" }}>{p.notes}</div> : null}
                  <div className="work-card-actions">
                    <button className="btn" type="button" onClick={() => openContributorEdit(p)}>Edit</button>
                    <button className="btn" type="button" onClick={() => archiveContributor(Number(p.id))}>Archive</button>
                  </div>
                </div>
              ))
            ) : (
              <div className="meta">No contributors yet.</div>
            )}
          </div>
        ) : null}

        {/* Milestones */}
        {activeTab === "milestones" ? (
          <div className="panel">
            <div className="section-head">
              <div>
                <h2 className="section-title">Milestones</h2>
                <div className="section-subtitle">Project checkpoints connected to work items.</div>
              </div>
              <button className="btn btn-primary" type="button" onClick={openMilestoneModal}>+ Add Milestone</button>
            </div>
            <div className="standard-list">
              {milestones.length ? (
                milestones.map((m) => {
                  const linkedCount = workItems.filter((w) => String(w.milestone_id || "") === String(m.id)).length;
                  return (
                    <div className="standard-card" key={m.id}>
                      <div className="standard-card-top">
                        <div>
                          <div className="standard-card-title">{m.title || "Milestone"}</div>
                          <div className="meta">{m.notes || ""}</div>
                        </div>
                        <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                          <span className="badge badge-info">{m.status || "planned"}</span>
                          <span className="badge badge-muted">{linkedCount} work items</span>
                        </div>
                      </div>
                      <div className="standard-card-meta">
                        <div><strong>Due:</strong> {m.due_date || "-"}</div>
                        <div><strong>Updated:</strong> {m.updated_at ? formatDateTime(m.updated_at) : "-"}</div>
                      </div>
                      <div className="standard-card-actions">
                        <button className="btn" type="button" onClick={() => openMilestoneEdit(m)}>Edit</button>
                        <button className="btn" type="button" onClick={() => closeMilestone(Number(m.id))}>Close</button>
                        <button className="btn" type="button" onClick={() => archiveMilestone(Number(m.id))}>Archive</button>
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="meta">No milestones yet.</div>
              )}
            </div>
          </div>
        ) : null}

        {/* Documents */}
        {activeTab === "documents" ? (
          <div className="panel">
            <h2>Documents</h2>
            <div className="meta" style={{ marginBottom: "12px" }}>
              Main document system is Google Drive.{" "}
              {client.google_drive_folder_url ? (
                <a href={client.google_drive_folder_url} target="_blank" rel="noopener noreferrer">Open Client Folder</a>
              ) : (
                <span style={{ color: "var(--danger)" }}>Google Drive folder not set</span>
              )}
            </div>
            {documents.length ? (
              documents.map((d) => (
                <div className="item" key={d.id}>
                  <div className="item-title">{d.title || d.name || "Document"}</div>
                  <div className="meta">{d.url || "-"}</div>
                </div>
              ))
            ) : (
              <div className="meta">No separate documents tracked. Use the Google Drive folder.</div>
            )}
          </div>
        ) : null}
      </div>

      {/* ===================== Modals ===================== */}
      {milestone ? (
        <Modal id="milestoneModal" onClose={() => setMilestone(null)}>
          <ModalHead title={milestone.id ? "Edit Milestone" : "Add Milestone"} onClose={() => setMilestone(null)} />
          <div className="form-grid">
            <Field label="Title"><input value={milestone.title} placeholder="Example: MVP Launch" onChange={(e) => setMilestone({ ...milestone, title: e.target.value })} /></Field>
            <Field label="Due Date"><input type="date" value={milestone.due_date} onChange={(e) => setMilestone({ ...milestone, due_date: e.target.value })} /></Field>
            <Field label="Status">
              <select value={milestone.status} onChange={(e) => setMilestone({ ...milestone, status: e.target.value })}>
                <option value="planned">Planned</option>
                <option value="in_progress">In Progress</option>
                <option value="done">Done</option>
                <option value="closed">Closed</option>
              </select>
            </Field>
            <Field label="Notes" full><textarea value={milestone.notes} onChange={(e) => setMilestone({ ...milestone, notes: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={() => setMilestone(null)} onSave={saveMilestone} saveLabel="Save Milestone" />
        </Modal>
      ) : null}

      {workItem ? (
        <Modal id="workItemModal" onClose={() => setWorkItem(null)}>
          <ModalHead title="Add Work Item" onClose={() => setWorkItem(null)} />
          <div className="form-grid">
            <Field label="Title"><input value={workItem.title} placeholder="Example: Build landing page" onChange={(e) => setWorkItem({ ...workItem, title: e.target.value })} /></Field>
            <Field label="Owner">
              <select value={workItem.owner_user_id} onChange={(e) => setWorkItem({ ...workItem, owner_user_id: e.target.value })}>
                <option value="">Select owner</option>
                {users.map((u) => <option value={u.id} key={u.id}>{u.name}</option>)}
              </select>
            </Field>
            <Field label="Priority">
              <select value={workItem.priority} onChange={(e) => setWorkItem({ ...workItem, priority: e.target.value })}>
                <option value="medium">Medium</option>
                <option value="high">High</option>
                <option value="low">Low</option>
              </select>
            </Field>
            <Field label="Due Date"><input type="date" value={workItem.due_date} onChange={(e) => setWorkItem({ ...workItem, due_date: e.target.value })} /></Field>
            <Field label="Depends On">
              <select value={workItem.dependency_work_item_id} onChange={(e) => setWorkItem({ ...workItem, dependency_work_item_id: e.target.value })}>
                <option value="">No dependency</option>
                {workItems.map((w) => <option value={w.id} key={w.id}>#{w.id} · {w.title}</option>)}
              </select>
            </Field>
            <Field label="Milestone">
              <select value={workItem.milestone_id} onChange={(e) => setWorkItem({ ...workItem, milestone_id: e.target.value })}>
                <option value="">No milestone</option>
                {milestones.map((m) => <option value={m.id} key={m.id}>{m.title}</option>)}
              </select>
            </Field>
            <Field label="Description" full><textarea value={workItem.description} placeholder="Add details, expected outcome, blockers, etc." onChange={(e) => setWorkItem({ ...workItem, description: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={() => setWorkItem(null)} onSave={createWorkItem} saveLabel="Create Work Item" />
        </Modal>
      ) : null}

      {workItemDetail ? (
        <Modal id="workItemDetailModal" onClose={() => setWorkItemDetail(null)}>
          <ModalHead
            title={workItemDetail.form ? "#" + workItemDetail.form.id + " — Edit Work Item" : "Work Item"}
            onClose={() => setWorkItemDetail(null)}
          />
          {workItemDetail.loading ? (
            <div className="meta">Loading...</div>
          ) : workItemDetail.error ? (
            <div className="meta">{workItemDetail.error}</div>
          ) : (
            <>
              <div className="form-grid">
                <Field label="Title"><input value={workItemDetail.form.title} onChange={(e) => setWorkItemDetail({ ...workItemDetail, form: { ...workItemDetail.form, title: e.target.value } })} /></Field>
                <Field label="Status">
                  <select value={workItemDetail.form.status} onChange={(e) => setWorkItemDetail({ ...workItemDetail, form: { ...workItemDetail.form, status: e.target.value } })}>
                    <option value="todo">Todo</option>
                    <option value="in_progress">In Progress</option>
                    <option value="done">Done</option>
                  </select>
                </Field>
                <Field label="Priority">
                  <select value={workItemDetail.form.priority} onChange={(e) => setWorkItemDetail({ ...workItemDetail, form: { ...workItemDetail.form, priority: e.target.value } })}>
                    <option value="low">Low</option>
                    <option value="medium">Medium</option>
                    <option value="high">High</option>
                  </select>
                </Field>
                <Field label="Due Date"><input type="date" value={workItemDetail.form.due_date} onChange={(e) => setWorkItemDetail({ ...workItemDetail, form: { ...workItemDetail.form, due_date: e.target.value } })} /></Field>
                <Field label="Owner">
                  <select value={workItemDetail.form.owner_user_id} onChange={(e) => setWorkItemDetail({ ...workItemDetail, form: { ...workItemDetail.form, owner_user_id: e.target.value } })}>
                    <option value="">No owner</option>
                    {users.map((u) => <option value={u.id} key={u.id}>{u.name}</option>)}
                  </select>
                </Field>
                <Field label="Depends On">
                  <select value={workItemDetail.form.dependency_work_item_id} onChange={(e) => setWorkItemDetail({ ...workItemDetail, form: { ...workItemDetail.form, dependency_work_item_id: e.target.value } })}>
                    <option value="">No dependency</option>
                    {workItems.filter((item) => Number(item.id) !== Number(workItemDetail.form.id)).map((item) => (
                      <option value={item.id} key={item.id}>#{item.id} · {item.title} ({item.status || "todo"})</option>
                    ))}
                  </select>
                </Field>
                <Field label="Milestone">
                  <select value={workItemDetail.form.milestone_id} onChange={(e) => setWorkItemDetail({ ...workItemDetail, form: { ...workItemDetail.form, milestone_id: e.target.value } })}>
                    <option value="">No milestone</option>
                    {milestones.map((m) => <option value={m.id} key={m.id}>{m.title}</option>)}
                  </select>
                </Field>
                <Field label="Description" full><textarea value={workItemDetail.form.description} onChange={(e) => setWorkItemDetail({ ...workItemDetail, form: { ...workItemDetail.form, description: e.target.value } })} /></Field>
              </div>
              <div style={{ marginTop: "14px", paddingTop: "14px", borderTop: "1px solid rgba(255,255,255,0.10)" }}>
                <div><strong>Created:</strong> {workItemDetail.meta.created_at}</div>
                <div><strong>Last Updated:</strong> {workItemDetail.meta.updated_at}</div>
              </div>
              <div style={{ display: "flex", justifyContent: "flex-end", gap: "10px", marginTop: "16px", flexWrap: "wrap" }}>
                <button className="btn" type="button" onClick={() => setWorkItemDetail(null)}>Cancel</button>
                <button className="btn" type="button" onClick={() => archiveWorkItem(Number(workItemDetail.form.id))}>Archive</button>
                <button className="btn btn-primary" type="button" onClick={saveWorkItemChanges}>Save Changes</button>
              </div>
            </>
          )}
        </Modal>
      ) : null}

      {lead ? (
        <Modal id="clientLeadModal" onClose={() => setLead(null)}>
          <ModalHead title={lead.id ? "Edit Lead" : "Add Lead"} onClose={() => setLead(null)} />
          <div className="form-grid">
            <Field label="Company / Business Name"><input value={lead.company} placeholder="Acme Manufacturing" onChange={(e) => setLead({ ...lead, company: e.target.value })} /></Field>
            <Field label="Contact Name"><input value={lead.contact_name} placeholder="Jane Doe" onChange={(e) => setLead({ ...lead, contact_name: e.target.value })} /></Field>
            <Field label="Phone"><input value={lead.phone} placeholder="+91..." onChange={(e) => setLead({ ...lead, phone: e.target.value })} /></Field>
            <Field label="Email"><input value={lead.email} placeholder="name@company.com" onChange={(e) => setLead({ ...lead, email: e.target.value })} /></Field>
            <Field label="City"><input value={lead.city} onChange={(e) => setLead({ ...lead, city: e.target.value })} /></Field>
            <Field label="State"><input value={lead.state} onChange={(e) => setLead({ ...lead, state: e.target.value })} /></Field>
            <Field label="Website"><input value={lead.website} placeholder="https://" onChange={(e) => setLead({ ...lead, website: e.target.value })} /></Field>
            <Field label="Lead Category">
              <select value={lead.lead_category} onChange={(e) => setLead({ ...lead, lead_category: e.target.value })}>
                <option value="b2b">B2B</option>
                <option value="b2c">B2C</option>
              </select>
            </Field>
            <Field label="Lead Source">
              <select value={lead.lead_source} onChange={(e) => setLead({ ...lead, lead_source: e.target.value })}>
                <option value="manual">Manual</option>
                <option value="apollo">Apollo</option>
                <option value="apify">Apify</option>
                <option value="linkedin">LinkedIn</option>
                <option value="website">Website</option>
                <option value="google_map">Google Maps</option>
              </select>
            </Field>
            <Field label="Pipeline Stage">
              <select value={lead.pipeline_stage} onChange={(e) => setLead({ ...lead, pipeline_stage: e.target.value })}>
                {PIPELINE.map((s) => <option value={s.key} key={s.key}>{s.label}</option>)}
              </select>
            </Field>
            <Field label="Outreach Status">
              <select value={lead.outreach_status} onChange={(e) => setLead({ ...lead, outreach_status: e.target.value })}>
                {OUTREACH.map((s) => <option value={s.key} key={s.key}>{s.label}</option>)}
              </select>
            </Field>
            <Field label="Demo Status">
              <select value={lead.demo_status} onChange={(e) => setLead({ ...lead, demo_status: e.target.value })}>
                {DEMO.map((s) => <option value={s.key} key={s.key}>{s.label}</option>)}
              </select>
            </Field>
            <Field label="Status">
              <select value={lead.status} onChange={(e) => setLead({ ...lead, status: e.target.value })}>
                <option value="new">New</option>
                <option value="in_progress">In Progress</option>
                <option value="completed">Completed</option>
              </select>
            </Field>
            <Field label="Assigned To"><input value={lead.assigned_to} placeholder="Team member name" onChange={(e) => setLead({ ...lead, assigned_to: e.target.value })} /></Field>
            <Field label="Notes history" full>
              <div className="meta" style={{ display: "flex", flexDirection: "column", gap: "8px", maxHeight: "220px", overflowY: "auto" }}>
                <NotesList history={lead.notesHistory} />
              </div>
            </Field>
            <Field label="Add a note" full><textarea value={lead.newNote} placeholder="Saved with your name and the current date/time." onChange={(e) => setLead({ ...lead, newNote: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={() => setLead(null)} onSave={saveClientLead} saveLabel="Save Lead" />
        </Modal>
      ) : null}

      {blocker ? (
        <Modal id="blockerModal" onClose={() => setBlocker(null)}>
          <ModalHead title={blocker.id ? "Edit Blocker" : "Add Blocker"} onClose={() => setBlocker(null)} />
          <div className="form-grid">
            <Field label="Title" full><input value={blocker.title} placeholder="Example: Waiting on client API credentials" onChange={(e) => setBlocker({ ...blocker, title: e.target.value })} /></Field>
            <Field label="Side">
              <select value={blocker.blocker_side} onChange={(e) => setBlocker({ ...blocker, blocker_side: e.target.value })}>
                <option value="internal">Internal</option>
                <option value="client_side">Client-side</option>
              </select>
            </Field>
            <Field label="Priority">
              <select value={blocker.priority} onChange={(e) => setBlocker({ ...blocker, priority: e.target.value })}>
                <option value="medium">Medium</option>
                <option value="high">High</option>
                <option value="low">Low</option>
              </select>
            </Field>
            <Field label="Owner">
              <select value={blocker.owner_user_id} onChange={(e) => setBlocker({ ...blocker, owner_user_id: e.target.value })}>
                <option value="">Select owner</option>
                {users.map((u) => <option value={u.id} key={u.id}>{u.name}</option>)}
              </select>
            </Field>
            <Field label="Related Work Item">
              <select value={blocker.related_work_item_id} onChange={(e) => setBlocker({ ...blocker, related_work_item_id: e.target.value })}>
                <option value="">No related work item</option>
                {workItems.map((w) => <option value={w.id} key={w.id}>#{w.id} · {w.title}</option>)}
              </select>
            </Field>
            {blocker.isEdit ? (
              <Field label="Resolution Status">
                <select value={blocker.resolution_status} onChange={(e) => setBlocker({ ...blocker, resolution_status: e.target.value })}>
                  <option value="open">Open</option>
                  <option value="in_progress">In Progress</option>
                  <option value="resolved">Resolved</option>
                </select>
              </Field>
            ) : null}
            <Field label="Description" full><textarea value={blocker.description} placeholder="What is blocked and why" onChange={(e) => setBlocker({ ...blocker, description: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={() => setBlocker(null)} onSave={saveBlocker} saveLabel="Save Blocker" />
        </Modal>
      ) : null}

      {meeting ? (
        <Modal id="meetingModal" onClose={() => setMeeting(null)}>
          <ModalHead title={meeting.isEdit ? "Edit Meeting" : "Log Meeting"} onClose={() => setMeeting(null)} />
          <div className="form-field" style={{ marginBottom: "14px" }}>
            <label>Meeting Details (AI Quick Fill)</label>
            <textarea rows={6} style={{ minHeight: "120px" }} value={meeting.aiNotes} placeholder="Write or paste the full meeting details here — what was discussed, who joined, decisions, action items — then click Auto-fill." onChange={(e) => setMeeting({ ...meeting, aiNotes: e.target.value })} />
            <div style={{ display: "flex", justifyContent: "flex-end", marginTop: "8px" }}>
              <button className="btn btn-primary" type="button" disabled={meeting.aiBusy} onClick={aiFillMeetingFromNotes}>{meeting.aiBusy ? "Filling..." : "✨ Auto-fill with AI"}</button>
            </div>
          </div>
          <div className="form-grid">
            <Field label="Title"><input value={meeting.title} placeholder="Example: Weekly sync call" onChange={(e) => setMeeting({ ...meeting, title: e.target.value })} /></Field>
            <Field label="Date"><input type="date" value={meeting.meeting_date} onChange={(e) => setMeeting({ ...meeting, meeting_date: e.target.value })} /></Field>
            <Field label="Type">
              <select value={meeting.meeting_type} onChange={(e) => setMeeting({ ...meeting, meeting_type: e.target.value })}>
                <option value="sync_call">Sync Call</option>
                <option value="review">Review</option>
                <option value="internal">Internal</option>
                <option value="adhoc">Ad-hoc</option>
              </select>
            </Field>
            <Field label="Participants"><input value={meeting.participants} placeholder="Names, comma-separated" onChange={(e) => setMeeting({ ...meeting, participants: e.target.value })} /></Field>
            <Field label="Summary" full><textarea value={meeting.summary} placeholder="Brief call summary" onChange={(e) => setMeeting({ ...meeting, summary: e.target.value })} /></Field>
            {meeting.isEdit ? (
              <>
                <Field label="Discussion Points" full><textarea value={meeting.discussion_points} placeholder="Key points discussed" onChange={(e) => setMeeting({ ...meeting, discussion_points: e.target.value })} /></Field>
                <Field label="Decisions Taken" full><textarea value={meeting.decisions} placeholder="Decisions made" onChange={(e) => setMeeting({ ...meeting, decisions: e.target.value })} /></Field>
                <Field label="Deliverables" full><textarea value={meeting.deliverables} placeholder="Agreed deliverables" onChange={(e) => setMeeting({ ...meeting, deliverables: e.target.value })} /></Field>
              </>
            ) : null}
            <Field label="Action Items" full><textarea value={meeting.action_items} placeholder="Who does what" onChange={(e) => setMeeting({ ...meeting, action_items: e.target.value })} /></Field>
            {meeting.isEdit ? (
              <Field label="Follow-ups" full><textarea value={meeting.follow_ups} placeholder="Follow-up items" onChange={(e) => setMeeting({ ...meeting, follow_ups: e.target.value })} /></Field>
            ) : null}
            <Field label="Next Steps" full><textarea value={meeting.next_steps} placeholder="Next steps" onChange={(e) => setMeeting({ ...meeting, next_steps: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={() => setMeeting(null)} onSave={saveMeeting} saveLabel="Save Meeting" />
        </Modal>
      ) : null}

      {campaign ? (
        <Modal id="campaignModal" onClose={() => setCampaign(null)}>
          <ModalHead title={campaign.id ? "Edit Campaign" : "Add Campaign"} onClose={() => setCampaign(null)} />
          <div className="form-grid">
            <Field label="Name" full><input value={campaign.name} placeholder="Example: Q3 cold email blast" onChange={(e) => setCampaign({ ...campaign, name: e.target.value })} /></Field>
            <Field label="Type">
              <select value={campaign.campaign_type} onChange={(e) => setCampaign({ ...campaign, campaign_type: e.target.value })}>
                <option value="email">Email</option>
                <option value="calling">Calling</option>
                <option value="linkedin">LinkedIn</option>
                <option value="whatsapp">WhatsApp</option>
                <option value="sms">SMS</option>
                <option value="events">Events / Webinar</option>
                <option value="ads">Paid Ads</option>
                <option value="content">Content / SEO</option>
                <option value="referral">Referral</option>
                <option value="other">Other</option>
              </select>
            </Field>
            <Field label="Channel / Tool"><input value={campaign.channel} placeholder="e.g. Apollo, Instantly" onChange={(e) => setCampaign({ ...campaign, channel: e.target.value })} /></Field>
            <Field label="Status">
              <select value={campaign.status} onChange={(e) => setCampaign({ ...campaign, status: e.target.value })}>
                <option value="planned">Planned</option>
                <option value="active">Active</option>
                <option value="paused">Paused</option>
                <option value="completed">Completed</option>
              </select>
            </Field>
            <Field label="Sent"><input type="number" min="0" value={campaign.sent_count} onChange={(e) => setCampaign({ ...campaign, sent_count: e.target.value })} /></Field>
            <Field label="Responses"><input type="number" min="0" value={campaign.response_count} onChange={(e) => setCampaign({ ...campaign, response_count: e.target.value })} /></Field>
            <Field label="Positive replies"><input type="number" min="0" value={campaign.positive_replies} onChange={(e) => setCampaign({ ...campaign, positive_replies: e.target.value })} /></Field>
            <Field label="Notes" full><textarea value={campaign.notes} placeholder="Performance, segments, etc." onChange={(e) => setCampaign({ ...campaign, notes: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={() => setCampaign(null)} onSave={saveCampaign} saveLabel="Save Campaign" />
        </Modal>
      ) : null}

      {incentive ? (
        <Modal id="incentiveModal" onClose={() => setIncentive(null)}>
          <ModalHead title={incentive.id ? "Edit Incentive" : "Add Incentive"} onClose={() => setIncentive(null)} />
          <div className="form-grid">
            <Field label="Title" full><input value={incentive.title} placeholder="Example: Converted Acme deal commission" onChange={(e) => setIncentive({ ...incentive, title: e.target.value })} /></Field>
            <Field label="GTM (attribution)">
              <select value={incentive.gtm_user_id} onChange={(e) => setIncentive({ ...incentive, gtm_user_id: e.target.value })}>
                <option value="">Select team member</option>
                {users.map((u) => <option value={u.id} key={u.id}>{u.name}</option>)}
              </select>
            </Field>
            <Field label="Related Lead">
              <select value={incentive.related_lead_id} onChange={(e) => setIncentive({ ...incentive, related_lead_id: e.target.value })}>
                <option value="">No lead</option>
                {incentiveLeadOptions.map((o) => <option value={o.id} key={o.id}>{o.label}</option>)}
              </select>
            </Field>
            <Field label="Amount"><input type="number" min="0" step="0.01" value={incentive.amount} onChange={(e) => setIncentive({ ...incentive, amount: e.target.value })} /></Field>
            <Field label="Status">
              <select value={incentive.status} onChange={(e) => setIncentive({ ...incentive, status: e.target.value })}>
                <option value="pending">Pending</option>
                <option value="approved">Approved</option>
                <option value="paid">Paid</option>
              </select>
            </Field>
            <Field label="Credit log / Notes" full><textarea value={incentive.notes} placeholder="Attribution details, calculation, etc." onChange={(e) => setIncentive({ ...incentive, notes: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={() => setIncentive(null)} onSave={saveIncentive} saveLabel="Save Incentive" />
        </Modal>
      ) : null}

      {report ? (
        <Modal id="reportModal" onClose={() => setReport(null)}>
          <ModalHead title={report.id ? "Edit Weekly Report" : "New Weekly Report"} onClose={() => setReport(null)} />
          <div className="form-grid">
            <Field label="Period Label"><input value={report.period_label} placeholder="e.g. Week 23 · Jun 3–9" onChange={(e) => setReport({ ...report, period_label: e.target.value })} /></Field>
            <Field label="Week Start"><input type="date" value={report.week_start} onChange={(e) => setReport({ ...report, week_start: e.target.value })} /></Field>
            <Field label="Summary" full><textarea value={report.summary} placeholder="Overall progress this week" onChange={(e) => setReport({ ...report, summary: e.target.value })} /></Field>
            <Field label="Highlights" full><textarea value={report.highlights} placeholder="Wins, milestones hit" onChange={(e) => setReport({ ...report, highlights: e.target.value })} /></Field>
            <Field label="Lowlights / Risks" full><textarea value={report.lowlights} placeholder="Risks, blockers, misses" onChange={(e) => setReport({ ...report, lowlights: e.target.value })} /></Field>
            <Field label="Next Week Plan" full><textarea value={report.next_week_plan} placeholder="Plan for next week" onChange={(e) => setReport({ ...report, next_week_plan: e.target.value })} /></Field>
            <div className="form-field" style={{ gridColumn: "1 / -1" }}>
              <label><input type="checkbox" checked={report.is_client_visible} onChange={(e) => setReport({ ...report, is_client_visible: e.target.checked })} /> Visible to client (when published)</label>
            </div>
          </div>
          <ModalActions onCancel={() => setReport(null)} onSave={saveReport} saveLabel="Save Report" />
        </Modal>
      ) : null}

      {clientUpdate ? (
        <Modal id="clientUpdateModal" onClose={() => setClientUpdate(null)}>
          <ModalHead title="Add Client Update" onClose={() => setClientUpdate(null)} />
          <div className="form-grid">
            <Field label="Title"><input value={clientUpdate.title} placeholder="Example: Weekly progress update" onChange={(e) => setClientUpdate({ ...clientUpdate, title: e.target.value })} /></Field>
            <Field label="Related Work Item">
              <select value={clientUpdate.related_work_item_id} onChange={(e) => setClientUpdate({ ...clientUpdate, related_work_item_id: e.target.value })}>
                <option value="">No related work item</option>
                {workItems.map((w) => <option value={w.id} key={w.id}>#{w.id} · {w.title}</option>)}
              </select>
            </Field>
            <Field label="Update Type">
              <select value={clientUpdate.update_type} onChange={(e) => setClientUpdate({ ...clientUpdate, update_type: e.target.value })}>
                <option value="general">General</option>
                <option value="progress">Progress</option>
                <option value="blocker">Blocker</option>
                <option value="client_call">Client Call</option>
                <option value="delivery">Delivery</option>
              </select>
            </Field>
            <Field label="Visibility">
              <select value={clientUpdate.visibility} onChange={(e) => setClientUpdate({ ...clientUpdate, visibility: e.target.value })}>
                <option value="internal">Internal only</option>
                <option value="client">Client visible later</option>
              </select>
            </Field>
            <Field label="Update" full><textarea value={clientUpdate.update_text} placeholder="Write what happened, what changed, next step, blocker, etc." onChange={(e) => setClientUpdate({ ...clientUpdate, update_text: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={() => setClientUpdate(null)} onSave={createClientUpdate} saveLabel="Save Update" />
        </Modal>
      ) : null}

      {leadNotesHistory ? (
        <Modal id="leadNotesHistoryModal" onClose={() => setLeadNotesHistory(null)}>
          <ModalHead title="Notes History" onClose={() => setLeadNotesHistory(null)} />
          <div style={{ display: "flex", flexDirection: "column", gap: "8px", maxHeight: "60vh", overflowY: "auto" }}>
            <NotesList history={leadNotesHistory.notes} />
          </div>
        </Modal>
      ) : null}

      {leadNote ? (
        <Modal id="leadNoteModal" onClose={() => setLeadNote(null)}>
          <ModalHead title="Add Note" onClose={() => setLeadNote(null)} />
          <div className="form-grid">
            <Field label="Note" full><textarea autoFocus value={leadNote.text} placeholder="Write a note. Saved with your name and the current date/time." onChange={(e) => setLeadNote({ ...leadNote, text: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={() => setLeadNote(null)} onSave={confirmLeadNote} saveLabel="Save Note" />
        </Modal>
      ) : null}

      {leadDemoNote ? (
        <Modal id="leadDemoNotesModal" onClose={cancelLeadDemoNote}>
          <ModalHead title="Demo Status Note" onClose={cancelLeadDemoNote} />
          <div className="form-grid">
            <Field label="Add a note for this demo status change (required)" full><textarea autoFocus value={leadDemoNote.text} placeholder="What changed? Outcome, next step, etc." onChange={(e) => setLeadDemoNote({ ...leadDemoNote, text: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={cancelLeadDemoNote} onSave={confirmLeadDemoNote} saveLabel="Save" />
        </Modal>
      ) : null}

      {leadStageNote ? (
        <Modal id="leadStageNotesModal" onClose={cancelLeadStageNote}>
          <ModalHead title="Status Change Note" onClose={cancelLeadStageNote} />
          <div className="form-grid">
            <Field label="Add a note for this status change (required)" full><textarea autoFocus value={leadStageNote.text} placeholder="What changed? Outcome, next step, etc." onChange={(e) => setLeadStageNote({ ...leadStageNote, text: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={cancelLeadStageNote} onSave={confirmLeadStageNote} saveLabel="Save" />
        </Modal>
      ) : null}

      {actionModal ? (
        <Modal id="actionModal" onClose={() => setActionModal(null)}>
          <ModalHead title={actionModal.id ? "Edit Action" : "Add Action"} onClose={() => setActionModal(null)} />
          <div className="form-grid">
            <Field label="Title"><input value={actionModal.title} placeholder="Need logo from client" onChange={(e) => setActionModal({ ...actionModal, title: e.target.value })} /></Field>
            <Field label="Owner Type">
              <select value={actionModal.owner_type} onChange={(e) => setActionModal({ ...actionModal, owner_type: e.target.value })}>
                <option value="WeSolve">WeSolve</option>
                <option value="Client">Client</option>
              </select>
            </Field>
            <Field label="Owner Name"><input value={actionModal.owner_name} placeholder="Aj / Malikah / Client" onChange={(e) => setActionModal({ ...actionModal, owner_name: e.target.value })} /></Field>
            <Field label="Due Date"><input type="date" value={actionModal.due_date} onChange={(e) => setActionModal({ ...actionModal, due_date: e.target.value })} /></Field>
            <Field label="Status">
              <select value={actionModal.status} onChange={(e) => setActionModal({ ...actionModal, status: e.target.value })}>
                <option>Open</option>
                <option>In Progress</option>
                <option>Waiting</option>
                <option>Done</option>
              </select>
            </Field>
            <Field label="Priority">
              <select value={actionModal.priority} onChange={(e) => setActionModal({ ...actionModal, priority: e.target.value })}>
                <option>Low</option>
                <option>Medium</option>
                <option>High</option>
                <option>Urgent</option>
              </select>
            </Field>
            <Field label="Notes" full><textarea value={actionModal.notes} onChange={(e) => setActionModal({ ...actionModal, notes: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={() => setActionModal(null)} onSave={saveAction} saveLabel="Save Action" />
        </Modal>
      ) : null}

      {contributor ? (
        <Modal id="contributorModal" onClose={() => setContributor(null)}>
          <ModalHead title={contributor.id ? "Edit Contributor" : "Add Contributor"} onClose={() => setContributor(null)} />
          <div className="form-grid">
            <Field label="Person Type">
              <select value={contributor.person_type} onChange={(e) => setContributor({ ...contributor, person_type: e.target.value })}>
                <option>Internal</option>
                <option>Contractor</option>
                <option>Client</option>
              </select>
            </Field>
            <Field label="Name"><input value={contributor.name} placeholder="Name" onChange={(e) => setContributor({ ...contributor, name: e.target.value })} /></Field>
            <Field label="Email"><input value={contributor.email} placeholder="email@example.com" onChange={(e) => setContributor({ ...contributor, email: e.target.value })} /></Field>
            <Field label="Phone"><input value={contributor.phone} placeholder="+91..." onChange={(e) => setContributor({ ...contributor, phone: e.target.value })} /></Field>
            <Field label="Role"><input value={contributor.role} placeholder="Developer / Designer / Client Contact" onChange={(e) => setContributor({ ...contributor, role: e.target.value })} /></Field>
            <Field label="Status">
              <select value={contributor.status} onChange={(e) => setContributor({ ...contributor, status: e.target.value })}>
                <option>Active</option>
                <option>Inactive</option>
              </select>
            </Field>
            <div className="form-field">
              <label><input type="checkbox" style={{ width: "auto" }} checked={contributor.can_update_work} onChange={(e) => setContributor({ ...contributor, can_update_work: e.target.checked })} /> Can update work</label>
            </div>
            <div className="form-field">
              <label><input type="checkbox" style={{ width: "auto" }} checked={contributor.can_view_client_dashboard} onChange={(e) => setContributor({ ...contributor, can_view_client_dashboard: e.target.checked })} /> Can view client dashboard</label>
            </div>
            <Field label="Notes" full><textarea value={contributor.notes} onChange={(e) => setContributor({ ...contributor, notes: e.target.value })} /></Field>
          </div>
          <ModalActions onCancel={() => setContributor(null)} onSave={saveContributor} saveLabel="Save Contributor" />
        </Modal>
      ) : null}

      {loading ? (
        <div className="work-modal open" style={{ zIndex: 2000 }}>
          <div className="work-modal-card" style={{ width: "min(360px, 100%)", textAlign: "center" }}>
            <div style={{ fontSize: "18px", fontWeight: 800, marginBottom: "8px" }}>Please wait...</div>
            <div className="meta">{loading}</div>
          </div>
        </div>
      ) : null}
    </div>
  );

  // Report sub-view toggle. Returns true when the views aren't on the page
  // (so the flyout link should navigate), false when toggled in place.
  function changeReportView(v) {
    if (activeTab !== "report") return true;
    setReportView(v === "week" ? "week" : "daily");
    try {
      history.replaceState(null, "", v === "week" ? "#week1" : "#daily");
    } catch (e) {
      /* ignore */
    }
    return false;
  }
}

// ---------------------------------------------------------------------------
// Small shared modal building blocks (mirror .work-modal markup).
// ---------------------------------------------------------------------------
function Modal({ id, onClose, children }) {
  return (
    <div
      id={id}
      className="work-modal open"
      onClick={(e) => {
        if (e.target.id === id) onClose();
      }}
    >
      <div className="work-modal-card" onClick={(e) => e.stopPropagation()}>
        {children}
      </div>
    </div>
  );
}

function ModalHead({ title, onClose }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
      <div style={{ fontSize: "22px", fontWeight: 800 }}>{title}</div>
      <button className="btn" type="button" onClick={onClose}>Close</button>
    </div>
  );
}

function ModalActions({ onCancel, onSave, saveLabel }) {
  return (
    <div style={{ display: "flex", justifyContent: "flex-end", gap: "10px", marginTop: "16px" }}>
      <button className="btn" type="button" onClick={onCancel}>Cancel</button>
      <button className="btn btn-primary" type="button" onClick={onSave}>{saveLabel}</button>
    </div>
  );
}

function Field({ label, full, children }) {
  return (
    <div className="form-field" style={full ? { gridColumn: "1 / -1" } : undefined}>
      <label>{label}</label>
      {children}
    </div>
  );
}

function NotesList({ history }) {
  const list = history || [];
  if (!list.length) return <div className="meta">No notes yet.</div>;
  return (
    <>
      {list
        .slice()
        .reverse()
        .map((n, i) => {
          const when = n.at
            ? new Date(n.at).toLocaleString("en-IN", {
                timeZone: "Asia/Kolkata",
                year: "numeric",
                month: "short",
                day: "numeric",
                hour: "numeric",
                minute: "2-digit",
                hour12: true,
              })
            : "";
          const byline = [n.by || "", when].filter(Boolean).join(" · ");
          return (
            <div key={i} style={{ padding: "8px 10px", border: "1px solid var(--line)", borderRadius: "8px" }}>
              <div style={{ whiteSpace: "pre-wrap", color: "var(--text, inherit)" }}>{n.text}</div>
              {byline ? <div className="meta" style={{ fontSize: "11px", marginTop: "4px" }}>{byline}</div> : null}
            </div>
          );
        })}
    </>
  );
}
