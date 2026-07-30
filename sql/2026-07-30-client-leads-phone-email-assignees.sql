-- Navii / client leads: two extra assignee columns alongside assigned_to.
--
-- "Assign for Phone" and "Assign for Email" split the outreach work for a lead
-- between two team members (one dials, one writes) while assigned_to stays the
-- overall owner. Both store a team member's NAME verbatim — exactly like
-- assigned_to and verified_by do — so the Leads filter popup can match them
-- against the same user-name dropdown without a join.
--
--   phone_assigned_to <- "Assign for Phone" (lead edit modal)
--   email_assigned_to <- "Assign for Email" (lead edit modal)
--
-- WITHOUT THIS MIGRATION saving a lead from the edit modal fails: PostgREST
-- rejects an insert/update naming a column the table does not have.
--
-- Run this once against the Supabase Postgres database.

ALTER TABLE client_leads
  ADD COLUMN IF NOT EXISTS phone_assigned_to text;

ALTER TABLE client_leads
  ADD COLUMN IF NOT EXISTS email_assigned_to text;

-- Rasset renders the same inline client-leads UI (see
-- INLINE_CLIENT_LEADS_BUSINESSES) but writes to its own table, so the edit
-- modal would otherwise fail there on save.

ALTER TABLE IF EXISTS rasset_leads
  ADD COLUMN IF NOT EXISTS phone_assigned_to text;

ALTER TABLE IF EXISTS rasset_leads
  ADD COLUMN IF NOT EXISTS email_assigned_to text;
