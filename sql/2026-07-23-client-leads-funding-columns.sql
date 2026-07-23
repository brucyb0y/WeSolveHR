-- Navii / client lead import: two more columns off the Navii lead sheet.
--
-- Both are stored as text, matching the other imported company_* columns on
-- client_leads (company_funding_total_amount, company_last_round_amount, ...):
-- the sheets carry raw values like "Unknown", "-", "Series A", or a date in
-- whatever format the export used, and we preserve them verbatim rather than
-- risk a cast failing mid-import.
--
-- Sheet headers mapped to these columns (see mapExcelRowToClientLead in
-- lib/server/app.js):
--   company_last_funding_date <- "Funding Date / Month" | "Last funding date"
--   company_funding_round     <- "Funding Round (Seed, Series A, B, etc.)"
--
-- WITHOUT THIS MIGRATION every row of the Navii sheet fails to import: PostgREST
-- rejects an insert/update naming a column the table does not have.
--
-- Run this once against the Supabase Postgres database.

ALTER TABLE client_leads
  ADD COLUMN IF NOT EXISTS company_last_funding_date text;

ALTER TABLE client_leads
  ADD COLUMN IF NOT EXISTS company_funding_round text;
