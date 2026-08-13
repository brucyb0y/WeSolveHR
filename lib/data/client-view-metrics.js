// Derived values for /client-view/:token, lifted out of
// renderClientViewOnlyPage()'s prelude. Pure functions over the loaded data, so
// the page can compute them server-side and hand plain values to the client.

import { CLIENT_LEAD_PIPELINE_STAGES } from "@/lib/server/app.js";

// Lead funnel — mirrors the internal Leads tab exactly.
export function buildLeadMetrics(leads) {
  const stageCount = {};
  CLIENT_LEAD_PIPELINE_STAGES.forEach((s) => {
    stageCount[s.key] = 0;
  });

  leads.forEach((l) => {
    const st = l.pipeline_stage || "prospect_identified";
    if (stageCount[st] !== undefined) stageCount[st] += 1;
  });

  return {
    stageCount,
    totalLeads: leads.length,
    qualifiedLeads:
      (stageCount.qualified_opportunity || 0) +
      (stageCount.pilot_evaluation || 0) +
      (stageCount.commercial_discussion || 0) +
      (stageCount.converted || 0),
    convertedLeads: stageCount.converted || 0,
    meetingLeads:
      (stageCount.meeting_scheduled || 0) + (stageCount.meeting_completed || 0),
  };
}

// Project manager, GTM associates, then active contributors — in that order.
//
// The account manager is deliberately absent: that push was commented out in
// the original and is left out rather than quietly reinstated.
export function buildTeamMembers({ client, users, contributors }) {
  const userNameById = {};
  (users || []).forEach((u) => {
    userNameById[String(u.id)] = u.name || "";
  });

  const teamMembers = [];

  if (client.project_manager_name) {
    teamMembers.push({
      name: client.project_manager_name,
      role: "Project Manager",
    });
  }

  (Array.isArray(client.gtm_associate_user_ids)
    ? client.gtm_associate_user_ids
    : []
  ).forEach((id) => {
    const name = userNameById[String(id)];
    if (name) teamMembers.push({ name, role: "GTM Associate" });
  });

  (contributors || [])
    .filter((c) => (c.status || "Active") === "Active")
    .forEach((c) => {
      if (c.name) {
        teamMembers.push({
          name: c.name,
          role: c.role || c.person_type || "Contributor",
        });
      }
    });

  return teamMembers;
}

// Status pill tone on the header badge.
export function statusTone(status) {
  const s = String(status || "").toLowerCase();
  if (/active|live|ongoing|won/.test(s)) return "ok";
  if (/paus|hold|onboard|pending/.test(s)) return "warn";
  if (/churn|lost|cancel|inactiv|closed/.test(s)) return "danger";
  return "info";
}

export const initialOf = (value) =>
  String(value || "?")
    .trim()
    .charAt(0)
    .toUpperCase() || "?";
