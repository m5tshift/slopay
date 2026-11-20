-- +goose Up
-- Canonical chain of chain.blocks (SRS 4.1.4, 4.1.5). The row is the chain lock: block events take it
-- FOR UPDATE, transfers FOR SHARE.
CREATE TABLE chain_state (
    chain_id      BIGINT      PRIMARY KEY CHECK (chain_id > 0),
    -- A block was processed: head_number, head_hash and chain_time hold it.
    known         BOOLEAN     NOT NULL DEFAULT false,
    head_number   BIGINT      NOT NULL DEFAULT 0 CHECK (head_number >= 0),
    head_hash     TEXT        NOT NULL DEFAULT '',
    -- Chain time: the largest processed block timestamp. Every deadline of the lifecycle uses it.
    chain_time    TIMESTAMPTZ,
    -- Offset of the last applied chain.blocks record (one partition): a redelivered record is skipped.
    blocks_offset BIGINT      NOT NULL DEFAULT -1 CHECK (blocks_offset >= -1),
    updated_at    TIMESTAMPTZ NOT NULL
);

-- Recent canonical blocks (number -> hash), more than the deepest reorg.
CREATE TABLE chain_blocks (
    chain_id    BIGINT      NOT NULL REFERENCES chain_state (chain_id),
    number      BIGINT      NOT NULL CHECK (number >= 0),
    hash        TEXT        NOT NULL,
    parent_hash TEXT        NOT NULL,
    block_time  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (chain_id, number)
);

-- Transfers to invoice addresses (chain.transfers). The key is blockHash + txHash + logIndex; logIndex is -1
-- for native ETH (null in the event).
CREATE TABLE transfers (
    block_hash     TEXT          NOT NULL,
    tx_hash        TEXT          NOT NULL,
    log_index      INTEGER       NOT NULL CHECK (log_index >= -1),
    invoice_id     UUID          NOT NULL REFERENCES invoices (id),
    block_number   BIGINT        NOT NULL CHECK (block_number >= 0),
    block_time     TIMESTAMPTZ   NOT NULL,
    amount         NUMERIC(78,0) NOT NULL CHECK (amount >= 0),
    asset_kind     TEXT          NOT NULL CHECK (asset_kind IN ('ETH', 'ERC20')),
    asset_contract TEXT,
    asset_decimals INTEGER       NOT NULL CHECK (asset_decimals BETWEEN 0 AND 77),
    -- The block number -> hash of the transfer is in the canonical chain.
    canonical      BOOLEAN       NOT NULL DEFAULT false,
    -- '' not counted, 'counted' in the invoice sum, 'late' reported as a late transfer.
    role           TEXT          NOT NULL DEFAULT '' CHECK (role IN ('', 'counted', 'late')),
    -- Set when a reorg after expiresAt removed the counted transfer: the same txHash is on time up to it.
    grace_until    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (block_hash, tx_hash, log_index),
    CHECK ((asset_kind = 'ETH') = (asset_contract IS NULL)),
    CHECK ((asset_kind = 'ETH') = (log_index = -1))
);

CREATE INDEX transfers_invoice_idx ON transfers (invoice_id);
CREATE INDEX transfers_block_idx ON transfers (block_number);

ALTER TABLE invoices
    -- Chain time of the PAID transition: the tracking window ends 24 h after the later of it and expires_at.
    ADD COLUMN paid_at  TIMESTAMPTZ,
    -- Evaluated on every block: confirmations are being counted.
    ADD COLUMN watch    BOOLEAN NOT NULL DEFAULT false,
    -- Evaluated when the chain time passes it (expiry, the wait after a revert, the UNDERPAID finality).
    ADD COLUMN deadline TIMESTAMPTZ;

UPDATE invoices SET deadline = expires_at WHERE status = 'NEW';

CREATE INDEX invoices_watch_idx ON invoices (id) WHERE watch;
CREATE INDEX invoices_deadline_idx ON invoices (deadline) WHERE deadline IS NOT NULL;

-- +goose Down
DROP INDEX invoices_deadline_idx;
DROP INDEX invoices_watch_idx;
ALTER TABLE invoices DROP COLUMN deadline, DROP COLUMN watch, DROP COLUMN paid_at;
DROP TABLE transfers;
DROP TABLE chain_blocks;
DROP TABLE chain_state;
