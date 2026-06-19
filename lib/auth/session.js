// Idiomatic Next.js session helpers built on the cookies() API, sharing the
// signed-cookie format + in-memory store with the Express adapter
// (lib/server/session-store.js). A session created here is therefore recognized
// by pages still served through the dispatch shim, and vice-versa.

import { cookies } from "next/headers";
import {
  SESSION_COOKIE,
  SESSION_SECRET,
  MAX_AGE_MS,
  store,
  sign,
  unsign,
  newSid,
} from "@/lib/server/session-store.js";

// Returns the session data object ({ userId, ... }) or null.
export async function getSession() {
  const jar = await cookies();
  const raw = jar.get(SESSION_COOKIE)?.value;
  if (!raw || !raw.startsWith("s:")) return null;

  const sid = unsign(raw.slice(2), SESSION_SECRET);
  if (!sid || !store.has(sid)) return null;

  return store.get(sid) || null;
}

export async function getSessionUserId() {
  const session = await getSession();
  return session?.userId ?? null;
}

// Create a fresh session for a user and set the signed cookie.
export async function createUserSession(userId) {
  const sid = newSid();
  store.set(sid, { userId });

  const jar = await cookies();
  jar.set(SESSION_COOKIE, "s:" + sign(sid, SESSION_SECRET), {
    httpOnly: true,
    sameSite: "lax",
    path: "/",
    maxAge: Math.floor(MAX_AGE_MS / 1000),
  });
}

// Destroy the current session (logout).
export async function destroySession() {
  const jar = await cookies();
  const raw = jar.get(SESSION_COOKIE)?.value;
  if (raw && raw.startsWith("s:")) {
    const sid = unsign(raw.slice(2), SESSION_SECRET);
    if (sid) store.delete(sid);
  }
  jar.delete(SESSION_COOKIE);
}
