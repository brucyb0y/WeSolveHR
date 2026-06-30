-- "Team Work" page (calendar icon in the top nav). A manually-maintained,
-- date-scoped grid of how many hours each team member spent on each
-- project/client for a given day — mirroring the Google Sheet the team kept by
-- hand. Staff edit cells inline; every change is recorded so the Logs panel can
-- show "X increased their Navii hours from 4 to 8.5".
--
-- Four tables:
--   team_work_columns  - the project/client column headers (Navii, Rasset, ...)
--   team_work_members  - the people, grouped by team (LEADS / GTM), each with an
--                        optional free-text responsibility note shown under name
--   team_work_hours    - one row per (date, member, column) holding the hours
--   team_work_logs     - append-only change history for the Logs panel
--
-- IMPORTANT: run this once against EVERY Supabase database the app uses — the
-- local .env project AND the Railway/production project are SEPARATE databases.
-- Until the tables exist, the /team-work page degrades gracefully (renders an
-- empty grid with a "run the migration" hint) so the rest of the app keeps
-- working either way.

CREATE TABLE IF NOT EXISTS team_work_columns (
  id          bigserial PRIMARY KEY,
  org_id      integer     NOT NULL DEFAULT 1,
  label       text        NOT NULL,
  sort_order  integer     NOT NULL DEFAULT 0,
  is_active   boolean     NOT NULL DEFAULT true,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS team_work_members (
  id             bigserial PRIMARY KEY,
  org_id         integer     NOT NULL DEFAULT 1,
  name           text        NOT NULL,
  team           text        NOT NULL DEFAULT 'LEADS', -- 'LEADS' | 'GTM'
  responsibility text        NOT NULL DEFAULT '',
  sort_order     integer     NOT NULL DEFAULT 0,
  is_active      boolean     NOT NULL DEFAULT true,
  created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS team_work_hours (
  id          bigserial PRIMARY KEY,
  org_id      integer     NOT NULL DEFAULT 1,
  work_date   date        NOT NULL,
  member_id   bigint      NOT NULL REFERENCES team_work_members(id) ON DELETE CASCADE,
  column_id   bigint      NOT NULL REFERENCES team_work_columns(id) ON DELETE CASCADE,
  hours       numeric(6,2) NOT NULL DEFAULT 0,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, work_date, member_id, column_id)
);

CREATE INDEX IF NOT EXISTS team_work_hours_date_idx
  ON team_work_hours (org_id, work_date);

CREATE TABLE IF NOT EXISTS team_work_logs (
  id             bigserial PRIMARY KEY,
  org_id         integer     NOT NULL DEFAULT 1,
  work_date      date        NOT NULL,
  member_id      bigint,
  column_id      bigint,
  member_name    text        NOT NULL DEFAULT '',
  column_label   text        NOT NULL DEFAULT '',
  action         text        NOT NULL DEFAULT 'hours_changed',
  old_hours      numeric(6,2),
  new_hours      numeric(6,2),
  detail         text        NOT NULL DEFAULT '',
  actor_user_id  bigint,
  actor_name     text        NOT NULL DEFAULT '',
  created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS team_work_logs_recent_idx
  ON team_work_logs (org_id, created_at DESC);

-- Seed the columns and members from the team's existing sheet so the page is
-- useful immediately. Guarded so re-running the migration does not duplicate.
INSERT INTO team_work_columns (org_id, label, sort_order)
SELECT 1, v.label, v.sort_order
FROM (VALUES
  ('Navii', 10),
  ('RebusAI', 20),
  ('Joolian', 30),
  ('Rasset', 40),
  ('WS 01', 50),
  ('WS02', 60),
  ('WS03', 70),
  ('WS', 80),
  ('Team Meetings', 90)
) AS v(label, sort_order)
WHERE NOT EXISTS (SELECT 1 FROM team_work_columns WHERE org_id = 1);

INSERT INTO team_work_members (org_id, name, team, sort_order)
SELECT 1, v.name, v.team, v.sort_order
FROM (VALUES
  ('Samar',    'LEADS', 10),
  ('Aabru',    'LEADS', 20),
  ('Mahesh',   'LEADS', 30),
  ('Jahan',    'LEADS', 40),
  ('Kavita',   'LEADS', 50),
  ('Mehnoor',  'LEADS', 60),
  ('Sibat',    'LEADS', 70),
  ('Marish',   'GTM',   10),
  ('Shabana',  'GTM',   20),
  ('Matiba',   'GTM',   30),
  ('Ruhab',    'GTM',   40),
  ('Zoya',     'GTM',   50),
  ('Sumit',    'GTM',   60),
  ('Pranav',   'GTM',   70),
  ('Mujtaba',  'GTM',   80),
  ('Khateeba', 'GTM',   90),
  ('Ainan',    'GTM',   100),
  ('Malikah',  'GTM',   110)
) AS v(name, team, sort_order)
WHERE NOT EXISTS (SELECT 1 FROM team_work_members WHERE org_id = 1);
