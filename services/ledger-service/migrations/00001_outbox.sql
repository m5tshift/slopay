-- +goose Up
-- Transactional outbox (SRS 4.1.5). Copy this file into services/<service>/migrations/ under the
-- next free version number; pkg/outbox reads and writes the table "outbox".
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_id     TEXT        NOT NULL UNIQUE,
    topic        TEXT        NOT NULL,
    key          TEXT        NOT NULL,
    event        JSONB       NOT NULL,
    traceparent  TEXT        NOT NULL DEFAULT '',
    tracestate   TEXT        NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ
);

-- The relay reads unpublished rows in id order.
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
-- Cleanup of published rows.
CREATE INDEX outbox_published_idx ON outbox (published_at) WHERE published_at IS NOT NULL;

-- +goose Down
DROP TABLE outbox;
