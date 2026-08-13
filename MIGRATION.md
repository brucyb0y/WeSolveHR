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

**All 25 pages are converted and serving from React.** `route.js` is gone from
every page segment; the Express adapter (`lib/server/adapter.js`) now serves
only the `/api/*` endpoints, which the React pages call.

Verified at runtime, not just at build: all 14 tabs of `/clients/[id]` return
200 with their real content present, the lead filters discriminate correctly
(`has_phone=yes` keeps a row that `has_phone=no` drops), and the other 13 pages
still respond. `/my-dashboard`, `/account` and `/help` return 307 to `/login`
under Basic auth — expected, since they use `requireUser` (session) rather than
`requireDashboardUser`.

### Still outstanding

**Only cleanup.** `lib/server/app.js` still holds the page renderers
(`renderClientWorkspacePage` and siblings), the `ar*` HTML builders, and
`renderSummaryWithGoals` / `renderReportSummaryPanel` / `renderClientGoalsPanel`
— all now unused by the React pages, along with the `*Html` fields on
`buildClientAutoReportSections`' return (the `reportData` aggregates replaced
them). Removing them is safe but noisy; left for a separate pass so this
migration's diff stays reviewable.

**There is no `dangerouslySetInnerHTML` anywhere in `app/`.**


## Gotcha: module-level state is duplicated across Next's bundle layers

`lib/server/session.js` keeps the session store on `globalThis`, not in a
module-level `const`. This is load-bearing.

Next bundles **server components / server actions** and **route handlers** into
separate module graphs, so `session.js` is instantiated more than once per
process (verified by probe: two "module init" hits per boot, and before the fix
the second instance could not see the first's entries).

With a plain `const store = new Map()` the effect was:

* login (a server action) wrote the session into graph A's Map;
* pages rendered correctly — RSC auth also reads graph A;
* **every `/api/*` call 302'd to `/login`** — the Express adapter's route
  handlers read graph B's Map, which was always empty;
* client components then tried to `JSON.parse` the login page's HTML, giving
  `Unexpected token '<', "<!DOCTYPE "... is not valid JSON`.

That broke all 30 files across 10 page areas that fetch `/api/*` from the
client: every modal save, the bulk lead operations, notes, audio upload and the
Excel import.

**It was invisible to Basic-auth testing.** `requireDashboardAuth` checks
`req.session?.userId` first and falls back to `DASHBOARD_USERNAME`/`PASSWORD`.
Every curl check in this migration used `-u`, which takes the fallback branch
and never touches the session store, so the whole API surface reported 200 while
being broken for anyone actually logged in. **Verify authenticated flows with a
real session login, not Basic auth.**

Any other module-level mutable state shared between an RSC/server action and a
route handler needs the same `globalThis` treatment.

## Chart kit

`components/charts/` is shared by `/clients/[id]` (staff) and
`/client-view/[token]` (customer) — the same components render both, so the
customer-facing report cannot drift from the internal one.

| File | What |
| --- | --- |
| `AutoReport.jsx` | Primitives: `ArKpiCard`, `ArBars`, `ArStackedBars`, `ArDonut`, `ArFunnelChart`, `ArHighlightTable`, `ArLegend`, `ArCard` |
| `ActivityReport.jsx` | One window's activity panel — KPIs, team-contribution bars, activity-mix donut, per-member table |
| `FunnelReport.jsx` | Pipeline funnel panel — snapshot funnel + movement charts |
| `icons.js` | KPI icon path data, extracted from `CLIENT_REPORT_ICONS` |
| `SummaryPanel.jsx` | AI summary + curated goals panels, side by side |

`SummaryPanel` handles **three stored summary shapes**, none of which are
migrated in the database: structured (`summary_json` with sections carrying
`description`/`stats`), legacy list (sections carrying `items[]` bullets), and
plain `summary_text`. Falling through to "no summary yet" on the older two would
silently blank historical weeks that do have content. Its `editable` prop is the
only difference between the staff and customer renderings — verified at runtime
that the customer page emits none of Generate now / Regenerate / Add goals /
Edit goals.

**Aggregation was NOT duplicated.** The ~400 lines of window/attribution logic
stay in `buildClientAutoReportSections()`; it now returns `reportData` (daily
aggregate, funnel snapshot) and an `agg` per week alongside the legacy `*Html`
fields. One aggregation, two renderers.

**Styling** lives in `app/globals.css`, generated by `scripts/gen-css.mjs` from
`buildAutoReportCss()` — which reads the same `CLIENT_REPORT_STYLES` constant
the legacy pages use, so the two cannot diverge. These are deliberately global
class names (`ar-*`), not CSS modules: two pages and a generator share them.

## Pre-existing defects found

Seventeen, all verified against the running app. Twelve are **preserved
deliberately** so the migration stays visually faithful; five were fixed because
preserving them was not possible or not defensible.

Two more were found during the `/clients/[id]` conversion and preserved:
the Meetings table's fourth/fifth column headers ("Status" / "MOM") sit over the
meeting *type* and MOM-filled columns respectively — the labels are off by one
against their contents; and `lead-call-icon` is emitted as a class with no rule
anywhere and nothing querying it (all its styling is inline), so it was dropped
rather than given a rule the original never had.

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
