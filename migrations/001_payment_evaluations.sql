-- A durable key reservation survives a process crash or a downstream failure.
CREATE TABLE idempotency_requests (
    idempotency_key VARCHAR(128) PRIMARY KEY,
    request_hash CHAR(64) NOT NULL,
    attempt_id UUID NOT NULL UNIQUE,
    response_body JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- These are risk evaluations, NOT bank charges or settlement records.
CREATE TABLE payment_evaluations (
    id UUID PRIMARY KEY REFERENCES idempotency_requests(attempt_id),
    customer_id VARCHAR(64) NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    decision TEXT NOT NULL CHECK (decision IN ('ALLOW', 'BLOCK')),
    tx_count_5m BIGINT NOT NULL CHECK (tx_count_5m > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX payment_evaluations_customer_created_idx
    ON payment_evaluations (customer_id, created_at DESC);
