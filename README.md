# WeSolveHR

A Next.js (App Router) app. Every user-facing screen is now a real page in the
App Router tree; the JSON API still runs the original Express handlers behind a
thin adapter while it is migrated.

## Running

```bash
npm install
npm run dev      # Next.js dev server (http://localhost:3000)
npm run build    # production build
npm start        # production server (next start)
```

Environment variables are read from `.env` (Supabase, OpenAI, Twilio, dashboard
basic-auth, `PORT`, etc.) exactly as before.

`npm run start:legacy` boots the original `node server.js`. Note that
`server.js` has drifted behind `lib/server/app.js` and is reference material
only — it is not a current copy of the app.

## Structure

```
app/
  layout.jsx                 the document shell (<html>/<body>) every page shares
  page.jsx                   GET /
  <route>/
    page.jsx                 server component: auth -> load data -> render
    <Name>Page.js            the markup builder for that screen
    <name>.css               that screen's own stylesheet
  form-post/                 POST handlers for URLs that are also pages
  api/**/route.js            JSON API — still forwards to lib/server/app.js
  health/{live,ready}/route.js
  logout/route.js
  whatsapp/route.js          Twilio webhook

lib/
  server/
    supabase.js              Supabase / OpenAI / ffmpeg clients
    auth.js                  page + route-handler guards
    session.js               signed "connect.sid" cookie over an in-memory store
    constants.js runtime.js time.js users.js phone.js parse.js
    form-post.js login-errors.js
    adapter.js dispatch.js   Express shim, still used by app/api/**
    app.js                   the original monolith; now only the API needs it
  data/                      one module per domain (clients, leads, attendance,
                             reports, tasks, team-work, bugs, …)
  ui/
    RawHtml.jsx              renders a markup string without disturbing layout
    nav.js html.js gtm-multiselect.js account-fields.js
    css/                     theme.css base.css top-nav.css sweetalert.css

public/
  js/                        client scripts lifted out of the old inline <script>
  css/                       stylesheets a route picks at runtime (see /reports)
middleware.js                rewrites POSTs aimed at page URLs to /form-post/*
```

## How a page renders

1. `app/<route>/page.jsx` calls a guard from `lib/server/auth.js`
   (`requireDashboardAuthPage` / `requireUserLoginPage`), which returns the user
   or redirects to `/login`.
2. It loads what it needs from a `lib/data/*` module.
3. It renders the screen's markup builder through `lib/ui/RawHtml.jsx`.
   The builders still produce HTML strings — the same strings the Express
   handlers produced — so this migration changed the file layout, not the
   output. Converting them to real JSX elements is a separate, later step.

CSS and client JS that were embedded in those strings now live in real files:
each page imports only the shared stylesheets its original document included,
and static `<script>` bodies were moved to `public/js/`. A `<script>` that
interpolates server data stays inline, so nothing had to be rewritten.

## Conventions worth knowing

- **A page and a route handler cannot share a path.** `/login`,
  `/clients/:id/edit` and `/clients/:id/reset` answer both GET and POST, so
  `middleware.js` rewrites the POST to `/form-post/<same path>`. The browser
  still posts to the original URL and no form markup changed; the handler
  rejects requests that did not come through the rewrite.
- **`RawHtml` uses `display: contents`** so the wrapper never becomes a layout
  box — important for pages whose CSS lays out `<body>` itself.
- **`<body>` attributes** must be applied by a parse-time script, since a page
  cannot add them to the shared shell. `/leads/:business?embed=1` does this.
- **`/reports`** renders one of two screens whose stylesheets define the same
  selectors differently, so both live in `public/css/` and the page links
  whichever it rendered.

## Behaviour that intentionally differs

- Redirects answer `307` (Next's `redirect()`) rather than Express's `302`.
- Next always emits `<meta name="viewport">` and a `<title>`.
- Handler branches that used to answer `400`/`404`/`500` with a short text body
  now render that same text with a `200`, because a rendered page carries the
  page status.
- A failed login redirects to `/login?error=<key>` instead of answering the POST
  with a `400`/`401` body. The rendered message is unchanged, and a refresh no
  longer resubmits the password.
- Three pages had malformed markup that browsers silently repaired, and it is
  now corrected: `/tasks` and `/clients/:id` never closed `<div class="wrap">`,
  and `/attendance` emitted one `</div>` too many before `#pageLoadingOverlay`.
  This mattered once React was involved — an unclosed div swallows everything
  that follows it, including Next's own trailing nodes, which broke hydration.
  Each fix is exactly one tag; the verification harness knows about them (see
  `divFix` in its manifest) so the rest of the markup stays strictly compared.

## What still runs on the monolith

`app/api/**/route.js` (83 routes) forward to `lib/server/app.js` via
`dispatch()`. `lib/server/app.js` also still starts the nightly report-summary
scheduler on import. Both go away when the API handlers are ported; that work
should move each handler into the matching `route.js`, backed by the same
`lib/data/*` modules the pages already use, and then re-home the scheduler in
`instrumentation.js`.

## Regenerating API route files

`app/api/**/route.js` are still generated from the Express registry:

```bash
node scripts/gen-routes.mjs
```

This only regenerates API wrappers; the page routes are hand-maintained and the
script will not touch them.
