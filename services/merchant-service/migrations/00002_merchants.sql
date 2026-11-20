-- +goose Up
-- Merchants and their API keys (SRS 4.1.10, UC-06, UC-07).
CREATE TABLE merchants (
    id                     UUID        PRIMARY KEY,
    name                   TEXT        NOT NULL,
    email                  TEXT        NOT NULL,
    -- Only an extended public key; private keys are refused by the service.
    xpub                   TEXT,
    webhook_url            TEXT,
    tolerance_bps          INTEGER     NOT NULL CHECK (tolerance_bps BETWEEN 0 AND 500),
    webhook_secret_version INTEGER     NOT NULL DEFAULT 1 CHECK (webhook_secret_version >= 1),
    -- Grows with every change of the state published in merchant.updated.
    version                BIGINT      NOT NULL CHECK (version >= 1),
    created_at             TIMESTAMPTZ NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL
);

-- API keys are stored only as SHA-256 hashes.
CREATE TABLE api_keys (
    id          UUID        PRIMARY KEY,
    merchant_id UUID        NOT NULL REFERENCES merchants (id),
    key_hash    BYTEA       NOT NULL UNIQUE CHECK (length(key_hash) = 32),
    created_at  TIMESTAMPTZ NOT NULL,
    -- Set on the previous key by a planned rotation: the key works until then.
    expires_at  TIMESTAMPTZ,
    revoked_at  TIMESTAMPTZ
);

CREATE INDEX api_keys_merchant_idx ON api_keys (merchant_id, created_at) WHERE revoked_at IS NULL;

-- +goose Down
DROP TABLE api_keys;
DROP TABLE merchants;
