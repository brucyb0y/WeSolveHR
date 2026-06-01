// Session handling that mirrors the original express-session setup:
//   session({ secret, resave:false, saveUninitialized:false,
//             cookie:{ secure:false, httpOnly:true, maxAge: 30 days } })
//
// Like express-session's default MemoryStore, sessions live for the lifetime
// of the Node process and are lost on restart. Only the signed session id is
// stored in the cookie (name "connect.sid"), exactly as before.

import crypto from "crypto";

const SESSION_COOKIE = "connect.sid";
const SESSION_SECRET = process.env.SESSION_SECRET || "wesolve-secret";
const MAX_AGE_MS = 1000 * 60 * 60 * 24 * 30; // 30 days

// In-memory store (process-lifetime), matching express-session MemoryStore.
const store = new Map();

// cookie-signature compatible sign/unsign (HMAC-SHA256, base64, trimmed "=").
function sign(value, secret) {
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

function unsign(signed, secret) {
  const idx = signed.lastIndexOf(".");
  if (idx < 0) return false;
  const value = signed.slice(0, idx);
  const expected = sign(value, secret);
  const a = Buffer.from(expected);
  const b = Buffer.from(signed);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b) ? value : false;
}

function parseCookies(header) {
  const out = {};
  if (!header) return out;
  for (const part of header.split(";")) {
    const i = part.indexOf("=");
    if (i < 0) continue;
    const k = part.slice(0, i).trim();
    if (!k) continue;
    out[k] = decodeURIComponent(part.slice(i + 1).trim());
  }
  return out;
}

function newSid() {
  return crypto.randomBytes(24).toString("hex");
}

// Build req.session from the incoming cookie header.
// Returns { session, meta } where session carries a non-enumerable destroy(cb).
export function loadSession(cookieHeader) {
  const cookies = parseCookies(cookieHeader);
  let sid = null;
  const raw = cookies[SESSION_COOKIE];
  if (raw && raw.startsWith("s:")) {
    const unsigned = unsign(raw.slice(2), SESSION_SECRET);
    if (unsigned) sid = unsigned;
  }

  const data = sid && store.has(sid) ? { ...store.get(sid) } : {};
  const meta = { sid, destroyed: false, original: JSON.stringify(data) };

  const session = { ...data };
  Object.defineProperty(session, "destroy", {
    enumerable: false,
    value: function destroy(cb) {
      meta.destroyed = true;
      for (const k of Object.keys(session)) delete session[k];
      if (typeof cb === "function") cb();
      return undefined;
    },
  });

  return { session, meta };
}

// Persist the session after the handler ran. Returns a Set-Cookie string or null.
export function commitSession(session, meta) {
  if (meta.destroyed) {
    if (meta.sid) {
      store.delete(meta.sid);
      return `${SESSION_COOKIE}=; Path=/; HttpOnly; Max-Age=0`;
    }
    return null;
  }

  const data = {};
  for (const k of Object.keys(session)) data[k] = session[k];
  const hasData = Object.keys(data).length > 0;

  if (meta.sid) {
    if (hasData) {
      // sid unchanged -> express (resave:false, rolling:false) would not resend.
      store.set(meta.sid, data);
      return null;
    }
    store.delete(meta.sid);
    return `${SESSION_COOKIE}=; Path=/; HttpOnly; Max-Age=0`;
  }

  // New session.
  if (!hasData) return null; // saveUninitialized:false -> no cookie until modified
  const sid = newSid();
  store.set(sid, data);
  const cookieVal = "s:" + sign(sid, SESSION_SECRET);
  const maxAgeSec = Math.floor(MAX_AGE_MS / 1000);
  return `${SESSION_COOKIE}=${encodeURIComponent(
    cookieVal,
  )}; Path=/; HttpOnly; Max-Age=${maxAgeSec}`;
}
