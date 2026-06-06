-- Navii / client lead management: track outreach status and demo status
-- separately from the conversion pipeline_stage.
--
-- These columns live ONLY on client_leads (the per-client lead table that backs
-- the client workspace "Leads" tab). The rasset_leads / joolian_leads tables are
-- intentionally left untouched — the lead engine only writes these fields when a
-- request explicitly provides them (see buildBusinessLeadPayloadFromBody in
-- lib/server/app.js).
--
-- Run this once against the Supabase Postgres database.

ALTER TABLE client_leads
  ADD COLUMN IF NOT EXISTS outreach_status text NOT NULL DEFAULT 'not_started';

ALTER TABLE client_leads
  ADD COLUMN IF NOT EXISTS demo_status text NOT NULL DEFAULT 'not_scheduled';

-- Allowed values (enforced in app code, listed here for reference):
--   outreach_status: not_started | contacted | follow_up | responded | no_response
--   demo_status:     not_scheduled | scheduled | completed | no_show | cancelled
