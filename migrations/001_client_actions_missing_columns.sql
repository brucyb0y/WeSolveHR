-- client_actions: add the three columns the application has always written.
--
-- WHY THIS EXISTS
-- The create-action handler (originally app.post("/api/clients/:id/actions"),
-- now app/api/clients/[clientId]/actions/route.js) has always inserted
-- owner_name, priority and notes. None of them exist on the table, so every
-- insert failed with PostgREST error PGRST204:
--
--   Could not find the 'notes' column of 'client_actions' in the schema cache
--
-- The feature therefore never worked — the table is empty. The Actions modal
-- collects all three values from the user, so they are added here rather than
-- dropped from the UI.
--
-- Types and defaults match what the handler writes:
--   owner_name  nullable text  (handler sends `body.owner_name || null`)
--   priority    text, default 'Medium'  (handler sends `body.priority || "Medium"`)
--   notes       nullable text  (handler sends `body.notes || null`)
--
-- Safe to re-run: every statement uses IF NOT EXISTS.

ALTER TABLE public.client_actions
  ADD COLUMN IF NOT EXISTS owner_name text,
  ADD COLUMN IF NOT EXISTS priority   text NOT NULL DEFAULT 'Medium',
  ADD COLUMN IF NOT EXISTS notes      text;

-- Existing rows (if any) inherit the default priority; owner_name and notes
-- stay NULL, which is what the handler writes when the form leaves them blank.
