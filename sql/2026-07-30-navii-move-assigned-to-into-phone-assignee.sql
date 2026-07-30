-- Navii: move each lead's existing owner ("Assigned To") into the new
-- "Assign for Phone" column.
--
-- Run this AFTER 2026-07-30-client-leads-phone-email-assignees.sql, which adds
-- phone_assigned_to / email_assigned_to.
--
-- Scoped to Navii's client_leads rows only — every other client on this table
-- keeps using assigned_to as-is.
--
-- Notes on the WHERE clause:
--   * rows that already have an "Assign for Phone" value are skipped entirely
--     (owner and phone assignee both stay put) so re-running is safe;
--   * assigned_to is cleared only on the rows actually moved;
--   * updated_at is deliberately NOT touched — bumping it would re-sort the
--     whole Leads tab and land every lead inside the "Updated from/to" filter.
--
-- The "My leads only" toggle now matches phone_assigned_to / email_assigned_to
-- as well as assigned_to (see getBusinessLeadsData in lib/server/app.js), so
-- moved leads still show up for the person who owns them.

-- Preview first — this should list the rows about to change:
--
--   SELECT id, company, assigned_to, phone_assigned_to
--   FROM client_leads
--   WHERE client_id IN (
--           SELECT id FROM clients
--           WHERE lower(btrim(name)) = 'navii'
--              OR lower(btrim(company_name)) = 'navii'
--         )
--     AND coalesce(btrim(assigned_to), '') <> ''
--     AND coalesce(btrim(phone_assigned_to), '') = '';

UPDATE client_leads
SET phone_assigned_to = btrim(assigned_to),
    assigned_to = NULL
WHERE client_id IN (
        SELECT id FROM clients
        WHERE lower(btrim(name)) = 'navii'
           OR lower(btrim(company_name)) = 'navii'
      )
  AND coalesce(btrim(assigned_to), '') <> ''
  AND coalesce(btrim(phone_assigned_to), '') = '';
