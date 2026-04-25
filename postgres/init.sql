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
