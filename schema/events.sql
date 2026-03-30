-- Claim Check–friendly event row: heavy blobs live in object storage; this table is control-plane sized.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    correlation_id text NOT NULL,
    kind text NOT NULL,
    claim_uri text NOT NULL,
    claim_meta jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_created_at ON public.events (created_at);
