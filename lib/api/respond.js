// Response helpers for native Next.js route handlers.
//
// The JSON envelope is fixed by every existing client: success is
// `{ ok: true, data }` and failure is `{ ok: false, error }` with a real HTTP
// status. Dozens of fetch() call sites branch on `json.ok`, so these shapes are
// a contract, not a preference — the Express handlers produced them via
// sendApiSuccess / sendApiError and these are the direct equivalents.

export function apiSuccess(data, init) {
  return Response.json({ ok: true, data }, init);
}

export function apiError(status, error) {
  return Response.json({ ok: false, error }, { status });
}

// Wraps a handler so an unexpected throw becomes a logged 500 in the same
// envelope rather than Next's HTML error page — a client doing
// `(await res.json()).ok` must never receive markup.
export function withApiErrors(label, handler) {
  return async (...args) => {
    try {
      return await handler(...args);
    } catch (err) {
      console.error(`${label} fatal error:`, err);
      return apiError(500, "Internal server error");
    }
  };
}

// Route handlers receive params as a promise in Next 15+.
export async function routeParams(ctx) {
  return (await ctx?.params) || {};
}

// Query string as a plain object, with repeated keys collapsed to arrays —
// matching what Express's `req.query` produced, which several handlers rely on
// (e.g. the tasks console sends progressBucket multiple times).
export function searchParamsToQuery(request) {
  const out = {};
  for (const [k, v] of new URL(request.url).searchParams) {
    if (k in out) out[k] = Array.isArray(out[k]) ? [...out[k], v] : [out[k], v];
    else out[k] = v;
  }
  return out;
}

// Body parsing that tolerates an empty body, which Express's json middleware
// treated as {} rather than a parse error.
export async function readJsonBody(request) {
  try {
    const text = await request.text();
    if (!text) return {};
    return JSON.parse(text);
  } catch {
    return {};
  }
}
