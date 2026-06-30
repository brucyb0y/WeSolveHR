-- Manually-curated "Goals" block shown next to the AI Weekly/Daily Summary on
-- both the internal client workspace (Report tab) and the external client-view
-- dashboard. One free-text block per client; staff edit it, the client sees it
-- read-only. We track who last changed it and when so the panel can show
-- "Last updated <date> by <name>".
--
-- IMPORTANT: run this once against EVERY Supabase database the app uses — the
-- local .env project AND the Railway/production project are SEPARATE databases.
-- Until the table exists, getClientGoals() degrades gracefully (renders the
-- empty-goals placeholder), so the Report tab keeps working either way.

CREATE TABLE IF NOT EXISTS client_goals (
  id                 bigserial PRIMARY KEY,
  org_id             integer     NOT NULL DEFAULT 1,
  client_id          bigint      NOT NULL,
  goals_text         text        NOT NULL DEFAULT '',
  updated_by_user_id bigint,
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, client_id)
);
