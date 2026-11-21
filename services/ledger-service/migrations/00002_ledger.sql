-- +goose Up
-- Double-entry ledger (SRS 4.1.3, FR-12): one posting per transfer with N confirmations, four lines each.

-- Merchants seen in invoice.events. The row is locked while postings of the merchant are written, so the
-- outbox events of one merchant (the Kafka key of ledger.events) grow in commit order.
CREATE TABLE merchants (
    id            UUID        PRIMARY KEY,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- The journal entry of one transfer. id = receipt:<blockHash>:<txHash>:<logIndex or empty> is the
-- idempotency key; COLLATE "C" keeps the page order byte-wise, as in the service.
CREATE TABLE postings (
    id             TEXT COLLATE "C" PRIMARY KEY CHECK (id ~ '^receipt:0x[0-9a-f]{64}:0x[0-9a-f]{64}:[0-9]*$'),
    merchant_id    UUID          NOT NULL REFERENCES merchants (id),
    invoice_id     UUID          NOT NULL,
    address        TEXT          NOT NULL CHECK (address ~ '^0x[0-9a-f]{40}$'),
    block_number   BIGINT        NOT NULL CHECK (block_number >= 0),
    block_hash     TEXT          NOT NULL,
    tx_hash        TEXT          NOT NULL,
    log_index      INTEGER       CHECK (log_index >= 0),
    asset_kind     TEXT          NOT NULL CHECK (asset_kind IN ('ETH', 'ERC20')),
    -- Token contract; empty for ETH.
    asset_contract TEXT          NOT NULL DEFAULT '',
    asset_decimals INTEGER       NOT NULL CHECK (asset_decimals BETWEEN 0 AND 77),
    amount         NUMERIC(78,0) NOT NULL CHECK (amount >= 0),
    fee_bps        INTEGER       NOT NULL CHECK (fee_bps BETWEEN 0 AND 10000),
    fee            NUMERIC(78,0) NOT NULL CHECK (fee >= 0 AND fee <= amount),
    posted_at      TIMESTAMPTZ   NOT NULL,
    CHECK ((asset_kind = 'ETH') = (log_index IS NULL)),
    CHECK ((asset_kind = 'ETH') = (asset_contract = ''))
);

CREATE INDEX postings_merchant_idx ON postings (merchant_id, posted_at, id);
CREATE INDEX postings_address_idx ON postings (address, posted_at, id);

-- Lines of a posting, all in the asset of the posting.
CREATE TABLE entries (
    posting_id TEXT COLLATE "C" NOT NULL REFERENCES postings (id),
    line       SMALLINT      NOT NULL CHECK (line >= 1),
    account    TEXT          NOT NULL,
    side       CHAR(1)       NOT NULL CHECK (side IN ('D', 'C')),
    amount     NUMERIC(78,0) NOT NULL CHECK (amount >= 0),
    PRIMARY KEY (posting_id, line)
);

CREATE INDEX entries_account_idx ON entries (account);

-- At commit, every posting written in the transaction has at least two lines and its debits equal its
-- credits. TG_ARGV[0] names the column with the posting id.
-- +goose StatementBegin
CREATE FUNCTION ledger_check_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    pid   TEXT := to_jsonb(NEW) ->> TG_ARGV[0];
    lines BIGINT;
    bal   NUMERIC;
BEGIN
    SELECT count(*), COALESCE(sum(CASE side WHEN 'D' THEN amount ELSE -amount END), 0)
      INTO lines, bal FROM entries WHERE posting_id = pid;
    IF lines < 2 OR bal <> 0 THEN
        RAISE EXCEPTION 'ledger: posting % is not balanced (lines %, debit - credit %)', pid, lines, bal;
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER postings_balanced AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_check_balanced('id');
CREATE CONSTRAINT TRIGGER entries_balanced AFTER INSERT ON entries
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_check_balanced('posting_id');

-- There are no reversing postings (SRS 4.1.3): the journal is append-only.
-- +goose StatementBegin
CREATE FUNCTION ledger_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ledger: % is append-only', TG_TABLE_NAME;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER postings_append_only BEFORE UPDATE OR DELETE ON postings
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
CREATE TRIGGER postings_no_truncate BEFORE TRUNCATE ON postings
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_append_only();
CREATE TRIGGER entries_append_only BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
CREATE TRIGGER entries_no_truncate BEFORE TRUNCATE ON entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_append_only();

-- +goose Down
DROP TABLE entries;
DROP TABLE postings;
DROP TABLE merchants;
DROP FUNCTION ledger_append_only();
DROP FUNCTION ledger_check_balanced();
