-- Navii / client lead management: call-recording integration + positive-reply
-- tracking for the client-facing dashboard.
--
-- These columns live ONLY on client_leads / client_campaigns (the per-client
-- tables that back the client workspace). The rasset_leads / joolian_leads
-- tables are intentionally left untouched — the lead engine only writes these
-- fields when a request explicitly provides them.
--
-- Run this once against the Supabase Postgres database.

-- Lead call recordings: an uploaded audio URL (stored in the existing
-- "lead-call-recordings" storage bucket) and a star flag GTMs use to highlight
-- high-quality / important conversations for the Navii team to review.
ALTER TABLE client_leads
  ADD COLUMN IF NOT EXISTS call_recording_url text;

ALTER TABLE client_leads
  ADD COLUMN IF NOT EXISTS is_starred boolean NOT NULL DEFAULT false;

-- Positive / interested replies tracked per campaign, surfaced as an outreach
-- momentum metric on the client dashboard ("X positive replies").
ALTER TABLE client_campaigns
  ADD COLUMN IF NOT EXISTS positive_replies integer NOT NULL DEFAULT 0;
