// Shared session primitives used by BOTH the Express adapter (lib/server/session.js)
// and the idiomatic Next.js auth helpers (lib/auth/session.js).
//
// Keeping the signed-cookie format, secret, and in-memory store in one place means
// a session created by a converted page (Server Action) is recognized by routes
// still served through the Express dispatch shim, and vice-versa. Like
// express-session's MemoryStore, sessions live for the lifetime of the Node
// process and are lost on restart.

import crypto from "crypto";

export const SESSION_COOKIE = "connect.sid";
export const SESSION_SECRET = process.env.SESSION_SECRET || "wesolve-secret";
export const MAX_AGE_MS = 1000 * 60 * 60 * 24 * 30; // 30 days

// In-memory store (process-lifetime), matching express-session MemoryStore.
//
// IMPORTANT: pinned to globalThis. Next.js compiles Server Actions, RSC pages,
// and Route Handlers (the Express dispatch shim) into separate server bundles,
// each of which would otherwise get its OWN instance of this module — and thus
// its own Map. A session created by the login Server Action would then be
// invisible to /my-dashboard (served by the shim) and the user would bounce
// back to /login. A single globalThis-scoped Map guarantees all bundles in the
// process share one store.
const STORE_KEY = Symbol.for("wesolvehr.session.store");
export const store = (globalThis[STORE_KEY] ??= new Map());

// cookie-signature compatible sign/unsign (HMAC-SHA256, base64, trimmed "=").
export function sign(value, secret = SESSION_SECRET) {
  return (
    value +
    "." +
    crypto
      .createHmac("sha256", secret)
      .update(value)
      .digest("base64")
      .replace(/=+$/, "")
  );
}

export function unsign(signed, secret = SESSION_SECRET) {
  const idx = signed.lastIndexOf(".");
  if (idx < 0) return false;
  const value = signed.slice(0, idx);
  const expected = sign(value, secret);
  const a = Buffer.from(expected);
  const b = Buffer.from(signed);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b) ? value : false;
}

export function newSid() {
  return crypto.randomBytes(24).toString("hex");
}
