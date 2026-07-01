-- Upgrade the client Goals block from a single free-text field to a structured
-- list of { title, value } goal items plus a separate free-text Notes field.
--
-- The internal client workspace (Report tab) and the external client-view
-- dashboard now render each goal as a bold title with its number beside it,
-- followed by the notes. Legacy rows keep working: normalizeClientGoalsData()
-- surfaces the old goals_text as the notes until the row is re-saved.
--
-- IMPORTANT: run this once against EVERY Supabase database the app uses — the
-- local .env project AND the Railway/production project are SEPARATE databases.
-- Until these columns exist, getClientGoals()/normalizeClientGoalsData() degrade
-- gracefully (fall back to goals_text), so the Report tab keeps working either way.

ALTER TABLE client_goals
  ADD COLUMN IF NOT EXISTS goals_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS notes      text  NOT NULL DEFAULT '';
