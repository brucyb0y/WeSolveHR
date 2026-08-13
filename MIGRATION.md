# React / App Router migration

Status: **24 of 25 page routes converted.** The app builds and every route works
at every point — this is a strangler migration, not a big-bang rewrite.

## How it works

`lib/server/app.js` still holds the whole Express application: all ~70 JSON API
routes, the `/whatsapp` webhook, and every data loader. What has moved is the
**page rendering**: each converted route is a React Server Component under
`app/`, and its old `render*Page()` HTML-string builder is gone.

Unconverted routes still run through the Express→Web adapter
(`lib/server/adapter.js`) via auto-generated `app/**/route.js` wrappers.

Three rules keep the two halves coexisting:

1. **`scripts/gen-routes.mjs` skips converted segments.** A folder holding a
   `page.jsx`, or a hand-written `route.js`, is never overwritten. Next.js
   rejects `route.js` and `page.jsx` in the same segment, so without this the
   next `node scripts/gen-routes.mjs` would break the build.
2. **One session store.** `lib/server/session.js` backs both the adapter and the
   React tree, so a login through either is valid on both.
3. **RSCs call data loaders directly** (`lib/server/app.js` exports them) rather
   than fetching the app's own HTTP endpoints.

## Conventions

- `app/globals.css` is **generated** — run `node scripts/gen-css.mjs`, never edit
  it. It is the theme/base/top-nav CSS the old pages inlined into every `<style>`.
- Page-specific CSS lives in a CSS module beside the page.
- **Check for class-name collisions before converting.** Several pages define a
  class that also exists in `globals.css` (e.g. `.brand` on /login rendered as a
  *merged* rule). Compare against globals and write the merged result explicitly
  rather than relying on stylesheet order.
- **Module classes nested under other module classes** (`.a .b`) must both be
  applied from `styles.*`. Applying `"b"` as a plain string silently does
  nothing. This has caused two real bugs during the migration.
- **Functions cannot be passed from a server component to a client component.**
  Pass rendered elements down, and keep the handler's state inside the client
  component (see `app/dashboard/TaskLoadTable.jsx`).
- Links use plain `<a>`, not `next/link`, while any target is still adapter-
  backed — the client router cannot read an HTML document as an RSC payload.
  Switch to `<Link>` once the last page converts.
- Public routes must pass `authenticated={false}` to `TopNav`, or its
  server-rendered Clients dropdown leaks the client roster to anonymous
  visitors. This applies to `/`. It does NOT apply to `/client-view/[token]`:
  that page renders no navigation at all (verified — `renderTopNav` never
  appears inside `renderClientViewOnlyPage`).

## Remaining work

**24 of 25 done.** One left: `/clients/[id]`.

| Route | Lines | Notes |
| --- | --- | --- |
### `/clients/[id]` — the last page

| Section | Lines | State |
| --- | --- | --- |
| CSS | 822 | **done** — `app/clients/[id]/workspace.module.css` (817 lines, 82 classes) |
| Data prep + helper renderers | 1,651 | to do |
| Markup (14 tabs) | 1,649 | to do |
| Client JS | **2,614 across 135 functions** | to do |
| Route data loading | ~470 | to do — 17 parallel queries, lead filtering, business resolution, conditional report/goals load |

More client JS than `/leads/[business]` and `/client-view` combined. Convert tab
by tab: Overview, then Leads (reuse `lib/data/client-view-leads.js` — same
filter semantics), then the CRUD-heavy tabs (milestones / actions /
contributors / work items / updates / blockers / meetings / campaigns /
incentives / reports / goals), then audio recording, bulk lead ops and Excel
import.

**Decide before converting:** the page loads **SweetAlert2 from a CDN**
(`<script src="https://cdn.jsdelivr.net/npm/sweetalert2@11">`, app.js:6543) and
calls `window.Swal` for confirms and toasts. It is an undeclared runtime
dependency — not in package.json. Either add it properly or replace those calls
with the app's own modal. `buildSweetAlertCss()` already ships the styling.

**Chart kit:** converting this page's Report tab is where `arKpiCard` /
`arBars` / `arStackedBars` / `arDonut` / `arFunnelChart` /
`buildClientAutoReportSections` (~350 lines, app.js:4052) should become a shared
component. Doing so retires the one `dangerouslySetInnerHTML` bridge in
`app/client-view/[token]/ReportTab.jsx`, which is the only non-JSX spot left in
the migration.

It embeds `/leads/[business]?embed=1` in an iframe for
`INLINE_CLIENT_LEADS_BUSINESSES` — that contract is already honoured by the
converted leads page, so the iframe keeps working either way.

Client-visible filters in `lib/data/client-view.js` are load-bearing (updates
`is_client_visible`, actions `owner_type=Client`, blockers `blocker_side=client_side`,
reports `is_published`). They are what keeps the customer page from showing
internal data — do not relax them when refactoring.

## Pre-existing defects found

Fifteen, all verified against the running app. Twelve are **preserved
deliberately** so the migration stays visually faithful; four were fixed because
preserving them was not possible or not defensible.

### Fixed

1. **`/leads/:business/intelligence?timeframe=cumulative` returned a 500.**
   `renderBusinessLeadIntelligencePage` built `cumulativeHtml` using a
   `renderList` helper declared (with `const`) further down the function —
   read-before-initialise. Only the cumulative branch used it before the
   declaration, so only that timeframe broke. Now 200.
2. **Add/Edit Lead enrichment silently half-failed.** `enrichLeadUrl()` assigned
   to `#leadCompany`, an id the form never had, so `null.value = ...` threw a
   TypeError *before* the 17 assignments after it. Any enrichment that returned
   a company name left website / maps / email / city / phone / owner unfilled.
3. **Smart-paste capability detection was dead.** It wrote into
   `#leadManufacturingCapabilities`; the field is `#leadCapabilities`. Guarded,
   so it failed silently and never selected anything.
4. **`/attendance` tab bar did not switch tabs.** There was no `#tab-overview`
   element, so the overview content was never hidden and appeared under every
   tab; `.wrap` also closed early, leaving the other three panels outside the
   max-width container. All four panels are now siblings inside `.wrap`.

### Preserved (each documented in the relevant CSS module)

5. `.badge` / `.badge-*` are undefined on `/bugs`, `/leads` and
   `/tasks/user/[userId]` — severity/status pills render unstyled. `.badge` is
   defined per-page elsewhere and looks like it was meant to live in
   `buildBasePageCss`.
6. `/leads` and `/leads/:business/imports` read `req.session?.user?.org_id`,
   but the session only ever stores `userId` — always `undefined`, so both
   silently fall back to `DASHBOARD_ORG_ID`.
7. `.controls` on `/tasks` has only media-query overrides and no base rule, so
   `grid-template-columns` applies to a plain block element and does nothing.
8. `.task-link` on `/tasks` has no rule at all.
9. `--dangerSoft` on `/attendance` is a typo for `--danger-soft`; the
   `no_update` and `unknown` status pills get no fill.
10. `/attendance`'s exceptions table declares 9 headers but renders 7 cells in a
   different order, so data sits under the wrong headings. Ambiguous which side
   is wrong, so left alone.
11. `.btn` is undefined on `/attendance/[userId]`, so the three month-navigation
   links render as plain anchors.
12. The multi-day report view emits `.report-card-missing/-partial/-off/-leave`
    and `.report-reason` but never defined them — hence two CSS modules under
    `app/reports/` rather than one shared file.
13. `/dashboard` summary cards carry a tone class (`info`/`danger`/`warn`/
    `success`) that was never defined. This one was **dropped** rather than
    preserved, since an unused class name carries nothing forward.

## Other notes

- `requireUserLogin` in `lib/server/app.js` references an undefined
  `isApiRoute` and throws when a session outlives its user row. `lib/auth.js`
  does not carry this forward, but the Express copy still has it.
- Converted pages redirect with **307** where the adapter used **302**.
  Equivalent for GET.
- `server.js` at the repo root is a stale copy of the app (last touched
  2026-06-11) and is not part of this migration.
- `/reports` used to run the same `getDailyNarrativeReport` query three times
  per view (once server-side, then twice more via `/api/reports/summary` and
  `/api/reports/cards`, which returned HTML strings for `innerHTML`). It runs
  once now; both endpoints still exist and are unchanged.

12. `.hint` is used three times on `/leads/[business]` and defined nowhere, so
    the multiselect hints render unstyled. Rendered as a plain global class so
    it stays that way.
13. `qualification_done` / `worth_talking` on the lead form read
    `#leadQualificationDone` / `#leadWorthTalking`, neither of which exists, so
    both have always submitted `false`. Preserved — whether those checkboxes
    were meant to exist is a product question.
14. `renderConversationRows()` in `renderBusinessLeadsPage` is defined and never
    called (dead code); `.conversationRow` / `.speakerPill` are its orphaned
    styles. Not ported.
