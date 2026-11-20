-- +goose Up
-- Projection of merchant.events (SRS 4.1.2) and the derivation counter of every merchant (SRS 4.1.3).
CREATE TABLE merchants (
    id                    UUID        PRIMARY KEY,
    -- Version of merchant.updated the row holds; an event with a lower or equal version is ignored.
    version               BIGINT      NOT NULL CHECK (version >= 1),
    -- Only an extended public key: merchant-service refuses private keys.
    xpub                  TEXT        NOT NULL,
    tolerance_bps         INTEGER     NOT NULL CHECK (tolerance_bps BETWEEN 0 AND 500),
    -- Next derivation index of the merchant. Taken and incremented under the row lock, never by events.
    next_derivation_index BIGINT      NOT NULL DEFAULT 0 CHECK (next_derivation_index BETWEEN 0 AND 2147483648),
    updated_at            TIMESTAMPTZ NOT NULL
);

-- Invoices (SRS 4.1.4, 4.1.6). Amounts are integers in minimal units of the asset, NUMERIC(78,0).
CREATE TABLE invoices (
    id               UUID          PRIMARY KEY,
    merchant_id      UUID          NOT NULL REFERENCES merchants (id),
    idempotency_key  TEXT          NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    -- SHA-256 of the canonical creation request: the same key with another body is refused.
    request_hash     BYTEA         NOT NULL CHECK (length(request_hash) = 32),
    order_id         TEXT          NOT NULL CHECK (length(order_id) BETWEEN 1 AND 128),
    amount_usd_cents BIGINT        NOT NULL CHECK (amount_usd_cents > 0),
    asset_kind       TEXT          NOT NULL CHECK (asset_kind IN ('ETH', 'ERC20')),
    asset_contract   TEXT,
    asset_decimals   INTEGER       NOT NULL CHECK (asset_decimals BETWEEN 0 AND 77),
    -- USD for one whole unit of the asset, fixed at creation.
    rate             NUMERIC       NOT NULL CHECK (rate > 0),
    rate_quoted_at   TIMESTAMPTZ   NOT NULL,
    amount           NUMERIC(78,0) NOT NULL CHECK (amount > 0),
    address          TEXT          NOT NULL,
    derivation_index BIGINT        NOT NULL CHECK (derivation_index BETWEEN 0 AND 2147483647),
    status           TEXT          NOT NULL CHECK (status IN
                         ('NEW', 'DETECTED', 'CONFIRMING', 'PAID', 'UNDERPAID', 'EXPIRED', 'LATE_PAID')),
    -- Version of the invoice, the sequence of its invoice.events.
    sequence         INTEGER       NOT NULL CHECK (sequence >= 1),
    confirmations    INTEGER       NOT NULL DEFAULT 0 CHECK (confirmations >= 0),
    received_amount  NUMERIC(78,0) NOT NULL DEFAULT 0 CHECK (received_amount >= 0),
    overpaid         BOOLEAN       NOT NULL DEFAULT false,
    created_at       TIMESTAMPTZ   NOT NULL,
    expires_at       TIMESTAMPTZ   NOT NULL,
    updated_at       TIMESTAMPTZ   NOT NULL,
    CHECK ((asset_kind = 'ETH') = (asset_contract IS NULL))
);

-- FR-01: one invoice per idempotency key of a merchant.
CREATE UNIQUE INDEX invoices_idempotency_idx ON invoices (merchant_id, idempotency_key);
-- SRS 4.1.3: an address is never given to two invoices of a merchant.
CREATE UNIQUE INDEX invoices_derivation_idx ON invoices (merchant_id, derivation_index);
-- List of a merchant, newest first (keyset pagination).
CREATE INDEX invoices_merchant_created_idx ON invoices (merchant_id, created_at DESC, id DESC);
-- Transfers are matched by address.
CREATE INDEX invoices_address_idx ON invoices (address);

-- +goose Down
DROP TABLE invoices;
DROP TABLE merchants;
