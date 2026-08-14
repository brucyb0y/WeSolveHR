-- clients.notebook_url — per-client NotebookLM (or any notebook) link.
--
-- Before this, the client workspace's "Notebook" button pointed at ONE
-- hardcoded NotebookLM URL for every client, so it always opened the same
-- notebook regardless of which client you were viewing.
--
-- Nullable with no default: a client without a notebook simply does not show
-- the button, matching how the Google Drive link already behaves.

ALTER TABLE public.clients
  ADD COLUMN IF NOT EXISTS notebook_url text;
