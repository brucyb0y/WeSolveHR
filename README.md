# WeSolveHR

A Next.js (App Router) app. The application logic is the original Express
monolith, ported **verbatim** behind a thin Express→Web adapter so the rendered
HTML and all API responses are byte-for-byte identical to the previous
`server.js`.

## Running

```bash
npm install
npm run dev      # Next.js dev server (http://localhost:3000)
npm run build    # production build
npm start        # production server (next start)
```

Environment variables are read from `.env` (Supabase, OpenAI, Twilio, dashboard
basic-auth, `PORT`, etc.) exactly as before.

`npm run start:legacy` still boots the original `node server.js` if you ever
need to compare behavior.

## Structure

```
app/                         App Router route handlers (one route.js per route).
                             Auto-generated thin wrappers that forward to the
                             original Express handlers via dispatch().
lib/server/
  app.js                     The original server.js, verbatim, minus the Express
                             bootstrap. Builds the route registry + holds every
                             render*/data*/auth function unchanged.
  adapter.js                 Express→Web adapter: route collector, multer-compatible
                             upload, and the req/res shim that lets the original
                             handlers run unchanged inside Next.js route handlers.
  session.js                 In-memory session + signed "connect.sid" cookie,
                             mirroring the old express-session config.
  dispatch.js                Bridges a generated route file to the handler chain.
scripts/gen-routes.mjs       Regenerates app/**/route.js from the route registry.
server.js                    Original Express entrypoint (kept for reference).
```

## How a request flows

1. A URL hits the matching `app/**/route.js`, which calls
   `dispatch(method, "<original express path>", request)`.
2. `dispatch` → `runRoute` finds the route in the registry, builds an
   Express-like `req`/`res` from the Web `Request`, runs any mounted middleware
   (e.g. `app.use("/api", requireDashboardAuth)`) and the route's own handler
   chain, then returns a Web `Response`.
3. The handler bodies — including every `render*Page` HTML builder — are the
   original code, untouched.

## Regenerating routes

If you add/remove an `app.get/post/...` in `lib/server/app.js`, regenerate the
App Router tree:

```bash
node scripts/gen-routes.mjs
```

## Notes

- Runs as a long-lived Node process (`next start`), matching the original
  deployment model. The in-memory rate limiter, sessions, and ffmpeg processing
  behave as before.
- `express`, `express-session`, and `multer` are no longer used at runtime
  (replaced by the adapter) and can be removed from `dependencies` once you're
  confident in the migration.
