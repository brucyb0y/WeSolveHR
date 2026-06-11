-- "Show on client dashboard" toggle for org-wide tasks.
--
-- A task is linked to a single client via its free-text `business`; this flag
-- controls whether that task surfaces on the client's external (client-view)
-- dashboard. Mirrors the per-lead is_client_visible flag on client_leads.
-- Tasks stay hidden from the client dashboard by default.
--
-- IMPORTANT: run this once against EVERY Supabase database the app uses — the
-- local .env project AND the Railway/production project are SEPARATE databases.
-- Until the column exists, the workspace "Linked Tasks" list will fail to load
-- (PostgREST rejects selecting an unknown column).

ALTER TABLE tasks
  ADD COLUMN IF NOT EXISTS is_client_visible boolean NOT NULL DEFAULT false;
