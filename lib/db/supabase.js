// Single Supabase client for server-side code (Server Components, Server Actions,
// route handlers). Uses the service-role key when present, falling back to anon —
// identical to how lib/server/app.js constructs its client.

import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error("Missing SUPABASE_URL or usable Supabase key in .env");
}

export const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
