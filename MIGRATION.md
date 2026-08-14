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

Nothing. `lib/server/app.js` has been cleaned: **50,100 -> 24,489 lines (-51%)**.

Removed:

* the 28 non-API Express page routes (every one is now a React `page.jsx`);
* 29 page-render functions — `renderClientWorkspacePage`,
  `renderClientViewOnlyPage`, `renderLoginPage`, `renderTopNav`,
  `renderQuickActionModal`, the `ar*` HTML chart kit, `buildActivityReport`,
  and the rest of the `render*Page` family;
* the legacy `*Html` fields on `buildClientAutoReportSections`
  (`dailyAutoReportHtml`, `leadFunnelReportDailyHtml`, per-week `activityHtml` /
  `funnelHtml`). Those kept the whole `ar*` HTML kit alive; with them gone the
  aggregation has exactly one consumer, the React kit in `components/charts`.

Kept, deliberately: all **94 `/api/*` handlers**, plus `/health/live`,
`/health/ready` and `/whatsapp` — the only non-API routes the Express adapter
still dispatches.

**How it was done, if this needs repeating:** with an AST (acorn), not regex.
Two hand-rolled attempts corrupted the file — the page renderers contain huge
template literals holding CSS and client JS, so both "match braces" and "top
level constructs start at column 0" are false here. A col-0 `}` inside an
embedded CSS block silently truncated a route. acorn was installed as a one-off
devDependency for the codemod and removed afterwards.

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


## Phase 2 (in progress): native API route handlers

The UI is fully migrated. The API is not: **1 of 83 route files is native**, the
other 82 are still AUTO-GENERATED shims forwarding into ~97 Express handlers in
`lib/server/app.js` via `lib/server/dispatch.js`.

### Foundation (done, verified)

* `lib/api/respond.js` — `apiSuccess` / `apiError` / `withApiErrors`, plus
  `readJsonBody`, `searchParamsToQuery` and `routeParams`. The
  `{ok:true,data}` / `{ok:false,error}` envelope is a hard contract: dozens of
  fetch call sites branch on `json.ok`.
* `lib/api/auth.js` — `requireApiUser(request)`, mirroring
  `requireDashboardAuth`'s precedence (session -> basic auth -> unprotected)
  but **never redirecting**. It returns `{user}` or `{response}` with a 401.
  That is structurally the fix for the failure that broke the tasks console:
  the Express version redirected to /login and clients JSON.parsed the HTML.

### Converting one route

1. Write `app/api/<path>/route.js` exporting the verb(s). `scripts/gen-routes.mjs`
   skips any folder holding a route.js it did not generate (it only deletes
   files whose header says AUTO-GENERATED), so a hand-written file is safe and
   the shim disappears on the next run.
2. Auth first: `const {user, response} = await requireApiUser(request); if (response) return response;`
3. Translate: `req.params` -> `await routeParams(ctx)`; `req.query` ->
   `searchParamsToQuery(request)`; `req.body` -> `await readJsonBody(request)`;
   `sendApiError(res,s,m)` -> `return apiError(s,m)`;
   `sendApiSuccess(res,d)` -> `return apiSuccess(d)`.
4. Delete the Express handler from `lib/server/app.js` once the native one is
   verified. Leaving both is harmless (the shim is gone so Express is
   unreachable) but keeps dead code around.

Done so far — **5 of 83 route files native**, each verified against a live
server (correct envelope, 401 JSON when unauthenticated, shimmed routes and
pages unaffected):

| Route | Note |
| --- | --- |
| `GET /api/users` | dashboard-org scoped, as the original |
| `GET /api/clients/nav-list` | caller-org scoped; name falls back to company_name |
| `GET /api/logs` | five optional filters; loader error message passed through |
| `GET /api/top-nav-summary` | non-managers get zeroes, not 403 |
| `GET /api/tasks` | repeated `progressBucket` must stay an array |
| `POST /api/clients/[clientId]/goals` | caps preserved (300/100/20k/100 goals); blank rows dropped |
| `POST /api/clients/[clientId]/actions` | see schema note below |
| `PATCH|PUT .../actions/[actionId]` | **method + envelope both fixed** |
| `POST .../actions/[actionId]/archive` | **envelope fixed** |
| `POST .../blockers` | invalid enums fall back, never reject |
| `PATCH .../blockers/[blockerId]` | archive + partial update share the verb |
| `POST/PATCH .../campaigns[/id]` | merge-with-existing before the payload builder |
| `POST/GET/PATCH .../meetings[/id]` | GET reads archived rows, PATCH does not |
| `POST/PUT/PATCH .../milestones[/id]`, `.../archive` | key-by-key; separate archive endpoint |
| `POST/PATCH .../incentives[/id]` | payload-builder family (merge required) |
| `POST/PUT .../updates[/id]`, `.../archive` | `archived_at`, not `deleted_at` |
| `POST/PATCH .../reports[/id]` | four exclusive ops on one PATCH |
| `GET/POST/PATCH .../contributors[/id]`, `.../archive` | **method + envelope both fixed** |
| `POST .../documents` | form post — NOT the JSON envelope |
| `POST .../client-view-link` | idempotent; token reused, never regenerated |
| `PATCH .../linked-tasks/[taskId]` | authorizes by name match before writing |
| `POST .../report-summary/generate` | week_start normalised to a Monday key |
| `POST .../leads` | goes through createBusinessLead, not a direct insert |
| `GET/PATCH/DELETE .../leads/[leadId]` | two PATCH paths, both log transitions |
| `GET /api/reports/task/[taskNo]` | three callers share this response shape |
| `GET /api/team-work`, `.../logs` | bad date falls back to today, never errors |

**DELETED — `/api/reports/summary` and `/api/reports/cards`.** Both returned
server-rendered HTML that the OLD /reports page injected with innerHTML; the
converted React page builds that markup itself and called neither. Removed with
acorn (routes + the now-orphaned `renderReportsSummaryHtml` /
`renderReportCardsHtml` builders); both now 404. The only remaining mentions
are explanatory comments in app/reports/*.jsx.

| `POST /api/team-work/hours` | clamps rather than rejects; logs only real changes |
| `POST /api/team-work/members` | appends with a sort_order gap of 10 |
| `PATCH/DELETE /api/team-work/members/[id]` | hard delete; name read before removal |
| `POST/DELETE /api/team-work/columns[/id]` | org-wide; label read before removal |
| `GET /api/attendance`, `.../insights`, `.../[userId]/red-reports` | duplicate registration collapsed |
| `POST /api/account/profile-field` | **session-only auth — no basic-auth fallback** |
| `PATCH /api/bugs/[id]` | assignee validated against users, not a bare FK |
| `POST /api/client-work-items` | status always starts "todo" |
| `GET/PATCH /api/client-work-items/[id]` | **dependency guard — read this one** |
| `POST /api/cron/generate-report-summaries` | secret auth, fail-closed |
| `GET /api/business-leads/[business]/check-phone`, `.../call-summaries` | normalised phone matching |
| `GET/PUT/DELETE /api/business-leads/[business]/[id]` | delegates to the shared lead engine |
| `PATCH .../[id]/status` | separate from PUT — status has its own side effects |
| `PATCH .../[id]/quick-toggle` | **allow-list on the field name** |

`quick-toggle` takes a FIELD NAME from the body and uses it as a column. The
allow-list (`l2_done`, `qualified`, `lead_stage`) is the only thing stopping
that from being an arbitrary column write — verified that `field: "org_id"` is
rejected. Checkbox values are compared with `=== true`, so a truthy string
cannot tick a box (verified: `"yes"` left it False, `true` set it).

`cron/generate-report-summaries` is NOT user-authenticated — a scheduler calls
it with a shared CRON_SECRET, accepted from a header, the query string OR the
body because different schedulers can only send one. It is **fail-closed**: an
unset CRON_SECRET rejects everything rather than running open (verified: 401
with no secret and with a wrong one).

Both business-leads reads match phones on a NORMALISED key in JS, not with
`.eq("phone", …)`. The same number is stored with different punctuation and
prefixes across imports, so an exact query match would silently miss most real
duplicates. They differ on the empty case: check-phone returns
`{duplicate:false}` (the form calls it on every keystroke and must not error
mid-typing) while call-summaries 400s (it is opened deliberately for one lead).

Both also preserve the `req.session?.user?.org_id` defect — that expression was
always undefined, so DASHBOARD_ORG_ID is kept rather than silently switching to
the caller's org.

**The work-item dependency guard is the subtlest rule in the API.** An item
cannot be marked done while its prerequisite is not. Two things make it work:

* `effectiveDependencyId` falls back to the STORED dependency when the body
  omits the field. A PATCH of just `{status:"done"}` carries no dependency, so
  without the fallback the check is skipped entirely — verified live that such
  a request IS blocked.
* `""`, `null` and `undefined` all mean "no dependency"; only a real value is
  coerced with `Number()`. Truthiness alone would treat `""` as an id.

Self-dependency is rejected (an item blocking itself could never complete), and
`completed_at` is derived from status — set on done, cleared otherwise —
never read from the body.

**`requireSessionUser()` exists for a reason — use it for self-service routes.**
`/api/account/profile-field` writes to `user.id`. Under the normal
`requireApiUser`, basic auth resolves to a fallback admin, so anyone holding the
shared DASHBOARD_PASSWORD could edit that admin's profile. The Express original
enforced this by using `requireUserLogin` rather than `requireDashboardAuth`.
Verified: basic-auth credentials get 401 on this route while working everywhere
else.

`PATCH /api/bugs/[id]` validates the assignee against the users table (exists,
active, same org) rather than trusting the id — a bare foreign key would accept
another org's user and assign a bug to someone the board cannot display.

**`/api/attendance/insights` was registered TWICE** in app.js with two
functionally identical handlers. Express serves the first match, so the second
was unreachable dead code; one native route resolves it by construction.

**PERFORMANCE — `/api/attendance/insights` takes ~117 seconds.** Verified live
(200, valid payload, 117s application time). Pre-existing: the route is a thin
wrapper over `getAttendanceInsightsData`, which is where the time goes. Not
introduced by the migration, but it will hit a proxy/gateway timeout in
production and is worth profiling separately.

`red-reports` scans a DIFFERENT range depending on the month: the current month
only up to today, a past month in full — otherwise every future day of this
month counts as a missing report. Verified: current month returned 12 days,
July returned 27.

`team-work/members` encodes two deliberate asymmetries:

* **sort_order jumps by 10, not 1** — the gap leaves room to reorder by
  rewriting one row instead of renumbering the whole team.
* **`responsibility: ""` clears the note, but `name: "  "` is rejected.** An
  empty note is a real edit; a nameless member would be unidentifiable on the
  board. An empty patch overall returns "Nothing to update" rather than a no-op
  write, because the hover-card saves on blur.

`team-work/hours` is worth copying from: hours are CLAMPED (non-numeric or
negative -> 0, above 24 -> 24) rather than rejected, because the board's inputs
are free-text and erroring mid-edit is worse than correcting. And the activity
log is written only when the value actually CHANGED — verified that re-saving
an unchanged cell leaves the log count identical.

**FIXED — `POST .../leads` accepted a nonexistent client.** It only checks
`if (!clientId)`, so `/api/clients/999999/leads` returns 200 and inserts a row
with `client_id: 999999` that no client owns (reproduced, then cleaned up).
It was inconsistent with its siblings — `.../goals` and `.../client-view-link`
both look the client up and 404. The route now does the same lookup before
`resolveClientLeadBusiness`; verified a bad id 404s and valid creates still
work.

**`.../leads/[leadId]` (GET/PATCH/DELETE) — the largest handler, now native.**
Three things it encodes:

* **Lead storage is not uniform.** A client mapped to an inline static business
  writes to that business's own table with NO client_id filter; everyone else
  uses per-client `client_leads` rows. `resolveLeadSource()` decides, and the
  client_id filter is applied only when there is one — adding it
  unconditionally matches zero rows for static businesses.
* **PATCH has two paths.** "Light" when every key is an inline
  dropdown/toggle/note (validated and written here), "full" otherwise (through
  `updateBusinessLead`, used by the lead form).
* **BOTH paths must log `client_lead_status_changed`.** The funnel report is
  built from those events, so a form edit that changes company AND stage has to
  emit the transition too — otherwise the lead moves while the funnel silently
  misses it. Verified live: light stage change, light demo change, and a
  combined form edit all produced correct from -> to events.

Three that break the usual mould:

* **`.../documents` is a form POST.** Form-encoded body (`is_client_visible`
  arrives as the string `"on"`), plain-text errors, and a 303 redirect on
  success. Wrapping it in `{ok, data}` would break whatever form posts to it.
  Its URL allow-list (drive.google.com / docs.google.com only) is a real guard —
  verified that `https://evil.example/x` is rejected.
* **`.../client-view-link` is idempotent.** It REUSES an existing token;
  regenerating on each click would silently invalidate a link the customer has
  already bookmarked (verified: two calls returned the same token). The absolute
  URL is built from forwarded host/proto headers, because behind a proxy
  `request.url` is the internal origin.
* **`.../linked-tasks/[taskId]` has no foreign key to authorize against.** A
  task belongs to a client only when its free-text `business` matches the
  client's name or company_name, so the route re-checks that match before
  writing — otherwise any client's URL could edit any task by id. The check runs
  BEFORE field validation, so probing with a bad payload reveals nothing.

**The PUT-vs-PATCH + `{success}`-envelope pair has now appeared twice** — on
actions and on contributors, in both cases making edit silently return 405 while
create/archive reported failure on success. When converting any remaining
route, check the verb the React caller actually sends and the field it checks,
not just what Express registered. Both were verified live as 405 before the
fix.

Soft-delete column names differ per table and must not be copy-pasted:
`is_active` + `deleted_at` (blockers, campaigns, meetings, milestones,
incentives, reports), `archived_at` (client_updates), `archived` + status
"Inactive" (client_contributors), `archived` + status "Archived"
(client_actions).

Two boolean rules that are security-relevant, both verified live:

* **`is_client_visible === true`** on updates — never coerced. Posting the
  string `"yes"` must NOT publish an internal update to the customer
  (verified: stored `False`).
* **`is_client_visible !== false`** on reports — the OPPOSITE default. A report
  is customer-visible once published unless someone opts out. These two look
  like a copy-paste error and are not.

`PATCH .../reports/[reportId]` carries four mutually exclusive operations —
archive / publish / unpublish / edit — checked in that order. Publish and
unpublish touch only the publish fields, so publishing never rewrites the
report body (verified: `summary` survived a publish).

**Payload-builder families need `{...existing, ...body}`.** Campaigns, meetings
and incentives map fields through a shared `build*PayloadFromBody` helper that
applies a default to every field it knows. Feeding it only the changed keys
therefore RESETS everything else — a PATCH of `{status}` would zero
`sent_count`. Verified: patching status alone left `sent_count` at 10, and
patching `decisions` left `summary` intact. Blockers and actions build their
patch key-by-key instead and must NOT be merged this way.

`PATCH .../blockers/[blockerId]` is the template for the remaining CRUD
families. Two rules it encodes, both verified live:

* **`!== undefined`, never truthiness.** `owner_user_id: null` must CLEAR the
  owner while omitting the key must PRESERVE it. Truthiness collapses those two
  into one and silently discards data.
* **`resolved_at` is derived, never taken from the body** — stamped when status
  becomes "resolved", cleared when it moves back, so it cannot drift out of step
  with `resolution_status`.

### Three defects found while converting the actions family

1. **PATCH vs PUT.** The route was registered `app.put(...)` but `ActionModal`
   sends `PATCH`, so every edit returned **405** and silently did nothing.
   Verified live before the fix. Both verbs are exported now.
2. **Non-standard envelope.** `.../actions/:actionId` replied
   `{success, action}` and `.../archive` replied `{success}`, while
   `ActionsTab`/`ActionModal` check `json.ok`. A successful archive reported
   "Failed to archive action" while having archived the row. The original
   client hedged with `if (!json.success && !json.ok)`; the converted
   components did not. Both endpoints now return `{ok, data}` — they have a
   single caller each, so nothing else depended on the old shape.
3. **Missing columns — `migrations/001_client_actions_missing_columns.sql`.**
   The create handler has always written `owner_name`, `priority` and `notes`;
   none exist on `client_actions`, so every insert failed with PGRST204 and the
   table is empty. **Creating an action has never worked.** Run that migration
   before expecting the Actions tab to function; the route code is already
   correct and needs no change once the columns exist.

That last one is the trap worth repeating: `progressBucket` is sent once per
checked bucket. `searchParams.get()` returns only the first, which silently
narrows the result set — verified live as 24 rows for one bucket vs 113 for
three. Use `searchParamsToQuery`, never `.get()`, for any repeatable filter.

### Order worth taking

Highest-traffic and simplest first: the plain reads (`/api/tasks`,
`/api/logs`, `/api/clients/nav-list`, `/api/top-nav-summary`), then the CRUD
families under `/api/clients/[clientId]/*`, and last the multipart/upload
endpoints (`note-audio`, `import-excel`, `upload-call`, `call-recording`) —
those use multer in Express and need `request.formData()` instead, so they are
the only ones that are not a mechanical translation.

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
