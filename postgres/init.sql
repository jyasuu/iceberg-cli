-- postgres/init.sql
-- Seed data for iceberg-cli sync integration tests (section 11).
-- Loaded automatically by the postgres image on first start.

-- ── Tables ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS products (
    id         BIGSERIAL PRIMARY KEY,
    sku        TEXT        NOT NULL,
    name       TEXT        NOT NULL,
    category   TEXT,
    unit_price NUMERIC(10,2),
    stock_qty  INT         NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id           BIGSERIAL PRIMARY KEY,
    user_id      BIGINT      NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'pending',
    total_amount NUMERIC(12,2),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    id         BIGSERIAL PRIMARY KEY,
    order_id   BIGINT        NOT NULL REFERENCES orders(id),
    product_id BIGINT        NOT NULL REFERENCES products(id),
    quantity   INT           NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    line_total NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);

-- ── Seed data ─────────────────────────────────────────────────────────────────

INSERT INTO products (sku, name, category, unit_price, stock_qty, updated_at) VALUES
    ('SKU-001', 'Widget',      'hardware', 9.99,  100, now() - interval '2 days'),
    ('SKU-002', 'Gadget',      'hardware', 19.99,  50, now() - interval '1 day'),
    ('SKU-003', 'Doohickey',   'parts',     4.49, 200, now() - interval '1 day'),
    ('SKU-004', 'Thingamajig', 'parts',    14.99,  75, now()),
    ('SKU-005', 'Whatsit',     'misc',      2.99, 500, now());

INSERT INTO orders (user_id, status, total_amount, created_at, updated_at) VALUES
    (1, 'shipped',  19.98, now() - interval '3 days', now() - interval '2 days'),
    (2, 'pending',  19.99, now() - interval '1 day',  now() - interval '1 day'),
    (3, 'shipped',  22.45, now() - interval '1 day',  now()),
    (1, 'cancelled', 4.49, now() - interval '4 days', now() - interval '3 days'),
    (2, 'pending',  44.97, now(),                     now());

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 2,  9.99),
    (2, 2, 1, 19.99),
    (3, 3, 5,  4.49),
    (4, 3, 1,  4.49),
    (5, 2, 1, 19.99),
    (5, 4, 1, 14.99),
    (5, 5, 3,  2.99);

-- ── Watermark index (speeds up incremental queries) ───────────────────────────

CREATE INDEX IF NOT EXISTS orders_updated_at_idx    ON orders(updated_at);
CREATE INDEX IF NOT EXISTS products_updated_at_idx  ON products(updated_at);


-- ── Write-strategy integration test tables ────────────────────────────────────
-- These tables are used by the §14 write-strategy integration tests.
-- Each table is keyed by a test-unique suffix injected at runtime; the
-- DDL here creates the shared "template" used as a documentation reference.

-- Generic event log for APPEND tests.
CREATE TABLE IF NOT EXISTS ws_events (
    id         BIGSERIAL PRIMARY KEY,
    label      TEXT        NOT NULL,
    value      BIGINT      NOT NULL,
    event_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Daily snapshot table for OVERWRITE tests.
CREATE TABLE IF NOT EXISTS ws_daily_snapshot (
    id         BIGSERIAL PRIMARY KEY,
    snap_date  TEXT        NOT NULL,  -- 'YYYY-MM-DD'
    metric     TEXT        NOT NULL,
    amount     NUMERIC(14,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Mutable entity table for UPSERT tests (soft-delete via is_deleted flag).
CREATE TABLE IF NOT EXISTS ws_entities (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  TEXT        NOT NULL DEFAULT 'default',
    name       TEXT        NOT NULL,
    status     TEXT        NOT NULL DEFAULT 'active',
    is_deleted BOOLEAN     NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- CDC outbox table for MERGE INTO tests (_op carries I / U / D).
CREATE TABLE IF NOT EXISTS ws_cdc_outbox (
    id         BIGSERIAL PRIMARY KEY,
    entity_id  BIGINT      NOT NULL,
    name       TEXT,
    status     TEXT,
    op         TEXT        NOT NULL,  -- 'I', 'U', 'D'
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ws_events_updated_at_idx     ON ws_events(updated_at);
CREATE INDEX IF NOT EXISTS ws_entities_updated_at_idx   ON ws_entities(updated_at);
CREATE INDEX IF NOT EXISTS ws_cdc_outbox_occurred_at_idx ON ws_cdc_outbox(occurred_at);