-- All client leads: copy each lead's owner ("Assigned To") into the
-- "Assign for Phone" column.
--
-- Run this AFTER 2026-07-30-client-leads-phone-email-assignees.sql, which adds
-- phone_assigned_to / email_assigned_to.
--
-- Why: the lead edit modal and the Leads tab's bulk bar no longer expose an
-- "Assigned To" control at all — ownership is now set per channel through
-- "Assign for Phone" / "Assign for Email". Every existing owner therefore has
-- to land in phone_assigned_to, or those leads read as unassigned in the UI.
--
-- This supersedes the Navii-only 2026-07-30-navii-move-assigned-to-into-phone-assignee.sql
-- by doing the same thing for every client on the table. Navii's rows were
-- already moved by that migration (assigned_to cleared, phone_assigned_to set),
-- so they no longer match the WHERE clause and are left alone.
--
-- COPY, not move: assigned_to is deliberately left in place. Reporting still
-- attributes converted leads to a user by matching assigned_to against the
-- team-member name list (see the GTM stats block and the leads report table in
-- lib/server/app.js) — clearing it would zero out those numbers. Nothing writes
-- assigned_to from the UI any more, so the leftover value just goes stale
-- rather than diverging.
--
-- Notes on the WHERE clause:
--   * rows that already have an "Assign for Phone" value are skipped, so an
--     existing phone assignee is never clobbered and re-running is safe;
--   * updated_at is deliberately NOT touched — bumping it would re-sort the
--     whole Leads tab and land every lead inside the "Updated from/to" filter.

-- Preview first — this should list the rows about to change:
--
--   SELECT id, client_id, company, assigned_to, phone_assigned_to
--   FROM client_leads
--   WHERE coalesce(btrim(assigned_to), '') <> ''
--     AND coalesce(btrim(phone_assigned_to), '') = '';

UPDATE client_leads
SET phone_assigned_to = btrim(assigned_to)
WHERE coalesce(btrim(assigned_to), '') <> ''
  AND coalesce(btrim(phone_assigned_to), '') = '';

-- Rasset renders the same inline client-leads UI (INLINE_CLIENT_LEADS_BUSINESSES)
-- against its own table, and that table got phone_assigned_to in the same
-- 2026-07-30 migration, so its rows need the identical copy. Skip this statement
-- if rasset_leads does not exist in your environment. joolian_leads is
-- deliberately left out: it never got the phone/email columns because it does
-- not render this UI.

UPDATE rasset_leads
SET phone_assigned_to = btrim(assigned_to)
WHERE coalesce(btrim(assigned_to), '') <> ''
  AND coalesce(btrim(phone_assigned_to), '') = '';
