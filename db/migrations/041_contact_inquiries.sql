-- Migration 041: Contact form inquiries
--
-- Backing store for the /contact form submission. Email forwarding via a
-- transactional provider (Resend / Postmark) is layered on top in a
-- subsequent change — the table is the system of record so an outbound
-- send failure never loses an inquiry.
--
-- Design notes:
--   • inet column for client_ip enables cheap per-IP rate-limit lookups
--     without a separate rate-limit table.
--   • forwarded_at / forward_error capture the email delivery state;
--     NULL forwarded_at + NULL forward_error means "queued, not attempted".
--   • RLS is enabled and only the service role can read/write — the table
--     contains PII (visitor email + free-form message) and must never be
--     reachable from the anon JWT used by the web app.
--
-- Run once in the Supabase SQL editor.

CREATE TABLE IF NOT EXISTS public.contact_inquiries (
    id              bigserial PRIMARY KEY,
    created_at      timestamptz NOT NULL DEFAULT now(),
    name            text        NOT NULL,
    email           text        NOT NULL,
    inquiry_type    text        NOT NULL DEFAULT 'general',
    message         text        NOT NULL,
    client_ip       inet,
    user_agent      text,
    forwarded_at    timestamptz,
    forward_error   text,
    CONSTRAINT contact_inquiries_name_length    CHECK (char_length(name)    BETWEEN 1 AND 200),
    CONSTRAINT contact_inquiries_email_length   CHECK (char_length(email)   BETWEEN 3 AND 320),
    CONSTRAINT contact_inquiries_message_length CHECK (char_length(message) BETWEEN 1 AND 10000),
    CONSTRAINT contact_inquiries_type_allowed   CHECK (
        inquiry_type IN ('general', 'sales', 'feedback', 'support', 'partnership', 'careers')
    )
);

-- Most-recent-first listing in the admin UI.
CREATE INDEX IF NOT EXISTS idx_contact_inquiries_created_at
    ON public.contact_inquiries (created_at DESC);

-- Per-IP rate-limit lookup: "how many inquiries from this IP in the last hour?"
CREATE INDEX IF NOT EXISTS idx_contact_inquiries_client_ip_created_at
    ON public.contact_inquiries (client_ip, created_at DESC)
    WHERE client_ip IS NOT NULL;

-- Drains the retry queue: rows that have never been forwarded and have no
-- recorded error are eligible for a (re)send attempt.
CREATE INDEX IF NOT EXISTS idx_contact_inquiries_pending_forward
    ON public.contact_inquiries (created_at)
    WHERE forwarded_at IS NULL AND forward_error IS NULL;

-- RLS — service role only. Contains PII; the anon key must not see it.
ALTER TABLE public.contact_inquiries ENABLE ROW LEVEL SECURITY;

-- (No policies are created. With RLS enabled and zero policies, only the
-- service_role bypass — used by the FastHTML backend — can read or write.)

COMMENT ON TABLE public.contact_inquiries IS
    'Submissions from the /contact form. PII; service-role only. '
    'forwarded_at is set when the transactional email send succeeds; '
    'forward_error captures the most recent failure.';
