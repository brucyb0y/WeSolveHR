// Markup for GET /help.
//
// Body markup extracted verbatim from renderHelpPage() (lib/server/app.js
// lines 36134-36779). The document shell now comes from
// app/layout.jsx, the <style> block from ./help.css, and the inline
// <script> from public/js/.

import { isManagerOrAdmin } from "@/lib/server/users.js";
import { escapeHtml } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderHelpPage(user = {}) {
  const isAdmin = isManagerOrAdmin(user);

  // Command guide data — each section groups related commands. A command is
  // either a plain string or { cmd, desc } when it needs an explanation.
  const sections = [
    {
      id: "basics",
      icon: "❓",
      title: "Basic Help",
      blurb: "Type these anytime to get oriented.",
      subs: [
        {
          commands: ["help", "commands", "help attendance", "help tasks", "help manager"],
          note: "help manager works only for manager / admin users.",
        },
      ],
    },
    {
      id: "attendance-self",
      icon: "🕒",
      title: "Personal Attendance",
      blurb: "Manage your own day.",
      subs: [
        { label: "Start work", commands: ["login"] },
        {
          label: "Take a break",
          commands: [
            { cmd: "break", desc: "Starts a break." },
            { cmd: "break 15", desc: "Break with an expected 15-minute duration." },
            { cmd: "break 15 lunch", desc: "Break with duration and reason." },
            { cmd: "break lunch", desc: "Break with a reason." },
          ],
        },
        { label: "Return from break", commands: ["back"] },
        { label: "Logout", commands: ["logout", "logout done for today"] },
        { label: "Check your status", commands: ["status", "now", "now summary"] },
        {
          label: "Mark yourself late",
          commands: ["late 10:45 am", "late 11:00 am", "late unsure"],
          note: 'Use an actual clock time — "late 30 min" is not supported.',
        },
        {
          label: "Mark yourself off / on leave",
          commands: [
            "leave today",
            "leave tomorrow",
            "leave on today",
            "off today",
            "off tomorrow",
            "off on 15 april",
          ],
          note: "Supported date styles: today · tomorrow · friday · 15 april · april 15 · 15th april",
        },
      ],
    },
    {
      id: "attendance-others",
      icon: "👥",
      title: "Attendance for Others",
      blurb: "Mark attendance on behalf of teammates.",
      admin: true,
      subs: [
        {
          label: "Login / logout / back",
          commands: [
            "login Zoya",
            "logout Aj",
            "logout Aj 6:30 pm",
            "back Mahesh",
            "back Mahesh 4:30 pm",
          ],
        },
        {
          label: "Break",
          commands: [
            "break Ruhab",
            "break Ruhab 15",
            "break Ruhab 15 1:30 pm",
            "break Ruhab 1:30 pm",
          ],
        },
        {
          label: "Alternative mark format",
          commands: [
            "mark Zoya login",
            "mark Zoya login 10:30 am",
            "mark Zoya logout",
            "mark Zoya logout 6:30 pm",
            "mark Zoya back",
            "mark Zoya back 2:30 pm",
            "mark Zoya break",
            "mark Zoya break 15",
            "mark Zoya break 15 1:30 pm",
            "mark Zoya break 1:30 pm",
          ],
        },
        { label: "Mark late", commands: ["late Zoya 11:00 am", "late Ruhab unsure"] },
        {
          label: "Off / leave",
          commands: [
            "leave Zoya today",
            "leave Zoya tomorrow",
            "leave Zoya on 15 april",
            "off Zoya today",
            "off Zoya tomorrow",
            "off Zoya on 15 april",
          ],
        },
        {
          label: "Company-wide off day",
          commands: [
            "company off today",
            "company off tomorrow",
            "company off 15 april",
            "company off april 15",
          ],
        },
        {
          label: "Override a working day (one person)",
          commands: [
            "day on today Zoya",
            "day on tomorrow Zoya",
            "day on 15 april Zoya",
            "day half today Zoya",
            "day half sunday Ruhab",
            "day half 15 april Ruhab",
          ],
          note: "day on marks a working day · day half marks a half working day.",
        },
        {
          label: "Company-wide working day override",
          commands: [
            "company day on today",
            "company day on tomorrow",
            "company day on 18 april",
            "company day half today",
            "company day half sunday",
            "company day half 18 april",
          ],
        },
      ],
    },
    {
      id: "attendance-fix",
      icon: "🛠️",
      title: "Attendance Corrections",
      blurb: "Fix, reset, and lock attendance records.",
      admin: true,
      subs: [
        { label: "Undo attendance", commands: ["undo my attendance", "undo attendance Aj"] },
        {
          label: "Reset a user / date",
          commands: ["reset Aj today", "reset Aj tomorrow", "reset Aj 15 april"],
        },
        {
          label: "Force logout / back",
          commands: [
            "force logout Aj",
            "force logout Aj 6:30 pm",
            "force back Aj",
            "force back Aj 2:30 pm",
          ],
        },
        {
          label: "Fix a specific event time",
          commands: [
            "fix Aj login 10:30 am",
            "fix Aj logout 6:30 pm",
            "fix Aj break 1:00 pm",
            "fix Aj back 1:30 pm",
          ],
        },
        {
          label: "Remove an event",
          commands: ["remove Aj login", "remove Aj logout", "remove Aj break", "remove Aj back"],
        },
        {
          label: "Auto-fix",
          commands: ["auto fix Aj", "auto fix Aj today", "auto fix Aj tomorrow", "auto fix Aj 15 april"],
        },
        {
          label: "Lock / unlock a day",
          commands: ["lock Aj today", "unlock Aj today", "lock Aj 15 april", "unlock Aj 15 april"],
        },
      ],
    },
    {
      id: "people-view",
      icon: "📊",
      title: "People & Attendance Views",
      blurb: "See who's where at a glance.",
      subs: [
        { label: "Who is off", commands: ["who is off today", "who all are on leave today"] },
        { label: "Who is on break", commands: ["who is on break"] },
        {
          label: "Today's summary",
          commands: ["summary today", "attendance summary today", "now", "now summary"],
        },
        { label: "Employee summary", commands: ["employee summary Aj"] },
        { label: "Timeline", commands: ["timeline Aj", "timeline Aj today", "timeline Aj 15 april"] },
        { label: "Audit", commands: ["audit Aj", "audit Aj today", "audit Aj 15 april"] },
      ],
    },
    {
      id: "identity",
      icon: "🔐",
      title: "Identity & Account",
      subs: [
        { label: "Who am I", commands: ["who am i"] },
        {
          label: "Change password",
          commands: ["change password newPasswordHere"],
          note: "Dashboard login uses your phone number + password.",
        },
      ],
    },
    {
      id: "tasks-view",
      icon: "📋",
      title: "Viewing Tasks",
      subs: [
        { label: "My open tasks", commands: ["my tasks"] },
        { label: "Someone else's tasks", commands: ["tasks Ruhab", "tasks Aj", "tasks Zoya"] },
        { label: "Show one task", commands: ["show task 2"] },
        { label: "Show overdue", commands: ["show overdue"] },
      ],
    },
    {
      id: "tasks-create",
      icon: "➕",
      title: "Creating Tasks",
      blurb: "Strict format is most reliable; free text also works.",
      subs: [
        {
          label: "Strict format (recommended)",
          commands: [
            "create task <title> business <business> area <area> owner <names> priority <low|medium|high|urgent> due <date>",
          ],
        },
        {
          label: "Examples",
          commands: [
            "create task fix landing page business joolian area marketing owner aj priority high due tomorrow",
            "create task test login flow business wesolvehr area qa owner aj, zoya priority medium due friday",
          ],
        },
        {
          label: "AI / free-text",
          commands: [
            "task Ruhab high present progress on Rasset by today",
            "Add a high priority task for Ruhab to test VPN by tomorrow",
          ],
          note: "Free text is parsed by AI. Use the strict create task … format when accuracy matters.",
        },
      ],
    },
    {
      id: "tasks-update",
      icon: "✏️",
      title: "Updating Tasks",
      subs: [
        {
          label: "Progress update",
          commands: [
            "progress 2 50% 20 mails sent no positive response",
            "progress 2 50 finished API testing",
            "progress task 2 75 completed dashboard wiring",
          ],
        },
        {
          label: "Mark done",
          commands: ["done 2 tested and verified properly"],
          note: 'done 2 alone is not enough — a detailed note is required.',
        },
        { label: "Change deadline", commands: ["deadline 2 tomorrow", "deadline 2 friday", "deadline 2 15 april"] },
        {
          label: "Edit task fields",
          commands: [
            "edit task 2 title final parents pitch v2",
            "edit task 2 detail add more detail here",
            "edit task 2 priority high",
            "edit task 2 priority urgent",
            "edit task 2 business joolian",
            "edit task 2 area marketing",
            "edit task 2 deadline tomorrow",
            "edit task 2 status in_progress",
            "edit task 2 status done",
            "edit task 2 status cancelled",
            "edit task 2 blocker waiting on dependency",
            "edit task 2 owner zoya, aj",
          ],
        },
        {
          label: "Clear fields",
          commands: [
            "edit task 2 clear detail",
            "edit task 2 clear blocker",
            "edit task 2 clear business",
            "edit task 2 clear area",
            "edit task 2 clear deadline",
          ],
        },
        {
          label: "Waiting / blocked",
          commands: [
            "wait 23 on aj for API response",
            "waiting 23 on aj for API response",
            "blocked 23 on aj for API response",
          ],
        },
        {
          label: "Clear wait / blocker",
          commands: ["clear wait 23", "clear wait 23 aj responded", "unwait 23", "unwait 23 dependency cleared"],
        },
        { label: "Undo last change", commands: ["undo last task change"] },
        {
          label: "Cancel / delete (admin)",
          commands: ["cancel 2", "delete 2", "cancel task 2", "delete task 2"],
        },
      ],
    },
    {
      id: "extra-work",
      icon: "💪",
      title: "Extra Work",
      blurb: "Log work that lives outside tasks.",
      subs: [
        {
          commands: [
            "extra work helped aj debug org id issue",
            "extra work created client onboarding notes",
          ],
        },
      ],
    },
    {
      id: "feedback",
      icon: "🗣️",
      title: "Feedback & HR Notes",
      blurb: "Recognise, coach, and appraise your team.",
      admin: true,
      subs: [
        { label: "General feedback", commands: ["feedback Aj good ownership on dashboard work"] },
        { label: "Appreciation", commands: ["appreciation Ruhab handled client issue very well"] },
        { label: "Coaching", commands: ["coaching Zoya needs to improve update quality"] },
        { label: "1-on-1 note", commands: ["1on1 Aj discussed workload and blockers"] },
        {
          label: "Appraisal",
          commands: [
            "appraisal Aj rating 4 strengths strong ownership improve communication comment good quarter overall",
          ],
          note: "Format: appraisal <name> rating <number> strengths <text> improve <text> comment <text>",
        },
      ],
    },
    {
      id: "dashboard-login",
      icon: "🔑",
      title: "Dashboard Login",
      subs: [
        {
          commands: ["+12133081594", "+919891517965"],
          note: "Log in with your phone number (including country code) + password. First-time users use the admin-assigned default password.",
        },
      ],
    },
  ];

  const cmdHtml = (c) => {
    const text = typeof c === "string" ? c : c.cmd;
    const desc = typeof c === "string" ? "" : c.desc;
    return `
      <div class="help-cmd-row">
        <button class="help-cmd" type="button" data-cmd="${escapeHtml(text)}" title="Click to copy">${escapeHtml(text)}</button>
        ${desc ? `<span class="help-cmd-desc">${escapeHtml(desc)}</span>` : ""}
      </div>`;
  };

  const subHtml = (s) => `
    ${s.label ? `<div class="help-sub-label">${escapeHtml(s.label)}</div>` : ""}
    <div class="help-cmd-list">${s.commands.map(cmdHtml).join("")}</div>
    ${s.note ? `<div class="help-note">${escapeHtml(s.note)}</div>` : ""}`;

  const visibleSections = sections.filter((s) => !s.admin || isAdmin);

  const tocHtml = visibleSections
    .map(
      (s) =>
        `<a class="help-toc-link" href="#${s.id}"><span class="help-toc-ico">${s.icon}</span>${escapeHtml(s.title)}${s.admin ? `<span class="help-toc-badge">Admin</span>` : ""}</a>`,
    )
    .join("");

  const sectionsHtml = visibleSections
    .map(
      (sec) => `
    <section class="help-section" id="${sec.id}" data-title="${escapeHtml(sec.title)}">
      <div class="help-section-head">
        <div class="help-section-icon" aria-hidden="true">${sec.icon}</div>
        <div class="help-section-titles">
          <h2>${escapeHtml(sec.title)}</h2>
          ${sec.blurb ? `<div class="help-section-blurb">${escapeHtml(sec.blurb)}</div>` : ""}
        </div>
        ${sec.admin ? `<span class="help-admin-badge">Manager / Admin</span>` : ""}
      </div>
      <div class="help-section-body">${sec.subs.map(subHtml).join("")}</div>
    </section>`,
    )
    .join("");

  return `
            ${renderTopNav("help")}
        <div class="wrap">
          <div class="help-hero">
            <div class="eyebrow">Help Center</div>
            <h1>WeSolveHR Command Guide</h1>
            <p>Everything you can do by chatting with the assistant — attendance, tasks, feedback and more. Tap any command to copy it.</p>
            <div class="help-search-box">
              <span class="help-search-ico" aria-hidden="true">🔍</span>
              <input id="helpSearch" type="search" placeholder="Search commands… (e.g. break, leave, create task)" autocomplete="off" />
            </div>
          </div>

          <div class="help-layout">
            <nav class="help-toc" aria-label="Sections">${tocHtml}</nav>
            <div>
              <div class="help-sections">${sectionsHtml}</div>
              <div class="help-empty" id="helpEmpty">No commands match your search.</div>
            </div>
          </div>
        </div>

        <script src="/js/help.js"></script>
      
  `;
}

export {
  renderHelpPage,
};
