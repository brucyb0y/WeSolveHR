-- Client internal ownership: "GTM Associate" (multi-select).
--
-- Unlike account_manager_user_id / project_manager_user_id (a single user FK
-- each), a client can have SEVERAL GTM associates. They are stored as a JSON
-- array of user ids directly on the clients row (no join table needed at this
-- scale). App code reads/writes this via clients.gtm_associate_user_ids.
--
-- IMPORTANT: run this once against EVERY Supabase database the app uses — the
-- local .env project AND the Railway/production project are SEPARATE databases.
-- Until the column exists, saving the client edit form will fail (PostgREST
-- rejects writes to an unknown column).

ALTER TABLE clients
  ADD COLUMN IF NOT EXISTS gtm_associate_user_ids jsonb NOT NULL DEFAULT '[]'::jsonb;
