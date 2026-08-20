-- Client leads: two columns for the Revivflow "Final Format" sheet.
--
-- That sheet carries a company-level email and an Instagram engagement string
-- that previously had nowhere to land and were folded into the free-text notes:
--
--   Company Email          -> company_email            ("info@acme.com")
--   Last Instagram Activity -> last_instagram_activity  ("Posted 1 day ago, 36.4K followers, Active")
--
-- Both are free text and mirror columns the table already has — company_email
-- sits beside company_hq_phone / company_website, and last_instagram_activity
-- pairs with the existing last_linkedin_activity. They surface on the client
-- lead form's optional-sheet fields, same as persona / mode_of_payment / etc.
--
-- WITHOUT THIS MIGRATION every client's Excel import fails, not just
-- Revivflow's: the shared mapper (mapExcelRowToClientLead in lib/server/app.js)
-- now emits these two keys on every row, and PostgREST rejects an insert naming
-- a column the table does not have. Run this once, before deploying.

ALTER TABLE client_leads
  ADD COLUMN IF NOT EXISTS company_email text,
  ADD COLUMN IF NOT EXISTS last_instagram_activity text;

-- PostgREST answers from a cached schema, so a brand-new column reads back as
-- "column ... does not exist" until it reloads. Supabase reloads on DDL by
-- itself; this makes it immediate when running the file by hand.

NOTIFY pgrst, 'reload schema';
