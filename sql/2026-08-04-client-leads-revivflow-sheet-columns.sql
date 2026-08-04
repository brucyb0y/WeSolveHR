-- Client leads: five columns for the Revivflow sheet (R_Leads.csv).
--
-- Revivflow is a normal per-client workspace backed by client_leads (filtered by
-- client_id), like Navii and Rebus AI — it has no table of its own. Its import
-- sheet just carries five fields with nowhere to land:
--
--   Persona                -> persona                 ("FOUNDER/CHAIRMAN")
--   Last Linkedin Activity -> last_linkedin_activity   ("Posted 2 days ago")
--   Monthly Chargebacks    -> monthly_chargebacks      ("High")
--   Mode of Payment        -> mode_of_payment          ("Credit Cards, PayPal")
--   ICP Category           -> icp_category             ("Travel Companies")
--
-- All are free text: the sheet's values are ranges, prose and comma-separated
-- lists, and ICP Category is the client's own segment naming (distinct from the
-- app's category_type dropdown, which the import dialog stamps on every row).
--
-- The rest of the sheet already maps onto existing columns: Full name ->
-- full_name/contact_name, Number -> phone, Person LinkedIn URL ->
-- person_linkedin_url, Company LinkedIn -> company_linkedin_url, Size of the
-- company -> company_size/number_of_employees, Country/State/City -> country/
-- state/city. See mapExcelRowToClientLead in lib/server/app.js.
--
-- WITHOUT THIS MIGRATION every client's Excel import fails, not just
-- Revivflow's: the shared mapper emits these five keys on every row, and
-- PostgREST rejects an insert naming a column the table does not have.
--
-- Run this once against the Supabase Postgres database, before deploying.

ALTER TABLE client_leads
  ADD COLUMN IF NOT EXISTS persona text,
  ADD COLUMN IF NOT EXISTS last_linkedin_activity text,
  ADD COLUMN IF NOT EXISTS monthly_chargebacks text,
  ADD COLUMN IF NOT EXISTS mode_of_payment text,
  ADD COLUMN IF NOT EXISTS icp_category text;

-- PostgREST answers from a cached schema, so a brand-new column reads back as
-- "column ... does not exist" until it reloads. Supabase reloads on DDL by
-- itself; this makes it immediate when running the file by hand.

NOTIFY pgrst, 'reload schema';

-- Verify:
--
--   SELECT column_name FROM information_schema.columns
--   WHERE table_name = 'client_leads'
--     AND column_name IN ('persona', 'last_linkedin_activity',
--                         'monthly_chargebacks', 'mode_of_payment',
--                         'icp_category');
