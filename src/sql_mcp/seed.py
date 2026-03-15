"""
sql_mcp/seed.py
────────────────────────────────────────────────────────────────────
One-shot script: creates the demo e-commerce schema and seeds sample
data into the sql-mcp-db Postgres container.

Run once:
    uv run python -m sql_mcp.seed

Tables created:
    users        — registered customers
    products     — product catalog
    orders       — purchase orders (one per user per checkout)
    order_items  — line items linking orders to products

Why a separate seed script?
  The MCP server itself is read-only.  Keeping the write path in a
  separate script means the server can never mutate the database.
"""

import asyncio
from dotenv import load_dotenv
from sql_mcp.database import build_engine

load_dotenv()

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_TABLES = """
-- Drop in reverse dependency order so we can re-run safely
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders      CASCADE;
DROP TABLE IF EXISTS products    CASCADE;
DROP TABLE IF EXISTS users       CASCADE;

-- users: people who have registered on the platform
CREATE TABLE users (
    id         SERIAL       PRIMARY KEY,
    name       TEXT         NOT NULL,
    email      TEXT         NOT NULL UNIQUE,
    country    TEXT         NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- products: items available for purchase
CREATE TABLE products (
    id          SERIAL          PRIMARY KEY,
    name        TEXT            NOT NULL,
    category    TEXT            NOT NULL,
    price_cents INTEGER         NOT NULL CHECK (price_cents >= 0),
    stock       INTEGER         NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- orders: a single checkout session for one user
CREATE TABLE orders (
    id          SERIAL          PRIMARY KEY,
    user_id     INTEGER         NOT NULL REFERENCES users(id),
    status      TEXT            NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending','paid','shipped','cancelled')),
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- order_items: each product line in an order
CREATE TABLE order_items (
    id          SERIAL          PRIMARY KEY,
    order_id    INTEGER         NOT NULL REFERENCES orders(id),
    product_id  INTEGER         NOT NULL REFERENCES products(id),
    quantity    INTEGER         NOT NULL CHECK (quantity > 0),
    unit_price_cents INTEGER    NOT NULL CHECK (unit_price_cents >= 0)
);
"""

# ── seed data ─────────────────────────────────────────────────────────────────

SEED_USERS = """
INSERT INTO users (name, email, country) VALUES
    ('Alice Martin',   'alice@example.com',   'US'),
    ('Bob Singh',      'bob@example.com',     'IN'),
    ('Chloe Dupont',   'chloe@example.com',   'FR'),
    ('David Kim',      'david@example.com',   'KR'),
    ('Eva Müller',     'eva@example.com',     'DE'),
    ('Frank Torres',   'frank@example.com',   'MX'),
    ('Grace Liu',      'grace@example.com',   'CN'),
    ('Hiro Tanaka',    'hiro@example.com',    'JP');
"""

SEED_PRODUCTS = """
INSERT INTO products (name, category, price_cents, stock) VALUES
    ('Wireless Keyboard',   'Electronics',  4999,  120),
    ('USB-C Hub 7-in-1',    'Electronics',  3499,   85),
    ('Noise-Cancelling Headphones', 'Electronics', 14999, 40),
    ('Mechanical Pencil Set','Stationery',   899,  300),
    ('Notebook A5 Pack',    'Stationery',   1299,  250),
    ('Desk Lamp LED',       'Furniture',    2999,   60),
    ('Ergonomic Mouse Pad', 'Accessories',   799,  200),
    ('Standing Desk Mat',   'Furniture',    4499,   35),
    ('Cable Organiser Kit', 'Accessories',   599,  400),
    ('Webcam 1080p',        'Electronics',  7999,   55);
"""

SEED_ORDERS = """
INSERT INTO orders (user_id, status, created_at) VALUES
    (1, 'paid',       '2026-01-05 10:23:00+00'),
    (1, 'shipped',    '2026-01-18 14:11:00+00'),
    (2, 'paid',       '2026-01-07 09:00:00+00'),
    (3, 'cancelled',  '2026-01-10 16:45:00+00'),
    (4, 'shipped',    '2026-01-12 11:30:00+00'),
    (5, 'paid',       '2026-01-14 08:55:00+00'),
    (6, 'pending',    '2026-01-20 20:10:00+00'),
    (7, 'shipped',    '2026-01-22 13:00:00+00'),
    (8, 'paid',       '2026-01-25 17:30:00+00'),
    (2, 'shipped',    '2026-02-01 09:15:00+00');
"""

SEED_ORDER_ITEMS = """
INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents) VALUES
    -- order 1 (Alice)
    (1, 1,  1, 4999),   -- Keyboard
    (1, 7,  2,  799),   -- 2x Mouse Pad
    -- order 2 (Alice)
    (2, 3,  1, 14999),  -- Headphones
    -- order 3 (Bob)
    (3, 2,  1, 3499),   -- USB-C Hub
    (3, 9,  3,  599),   -- 3x Cable Organiser
    -- order 4 (Chloe — cancelled)
    (4, 5,  2, 1299),   -- 2x Notebook
    -- order 5 (David)
    (5, 6,  1, 2999),   -- Desk Lamp
    (5, 8,  1, 4499),   -- Standing Mat
    -- order 6 (Eva)
    (6, 4,  4,  899),   -- 4x Pencil Set
    -- order 7 (Frank — pending)
    (7, 10, 1, 7999),   -- Webcam
    -- order 8 (Grace)
    (8, 1,  2, 4999),   -- 2x Keyboard
    (8, 2,  1, 3499),   -- USB-C Hub
    -- order 9 (Hiro)
    (9, 3,  1, 14999),  -- Headphones
    (9, 9,  2,  599),   -- 2x Cable Organiser
    -- order 10 (Bob again)
    (10, 6, 1, 2999),   -- Desk Lamp
    (10, 7, 1,  799);   -- Mouse Pad
"""


# ── main ──────────────────────────────────────────────────────────────────────

async def seed() -> None:
    engine = build_engine()
    # asyncpg does not support multi-statement scripts via the SQLAlchemy
    # layer because it uses prepared statements internally.  We work around
    # this by grabbing the raw asyncpg connection and calling its execute()
    # method directly, which sends the SQL as a simple query (not a prepared
    # statement) and supports multiple commands in one call.
    #
    # We still use engine.connect() to borrow from the pool; we just bypass
    # SQLAlchemy's execution layer for these DDL/DML seed blocks.
    async with engine.connect() as sa_conn:
        # Unwrap to the underlying asyncpg Connection object
        raw_conn = await sa_conn.get_raw_connection()
        pg = raw_conn.driver_connection  # actual asyncpg.Connection
        assert pg is not None, "Expected asyncpg driver connection"

        print("Creating tables…")
        await pg.execute(CREATE_TABLES)
        print("Seeding users…")
        await pg.execute(SEED_USERS)
        print("Seeding products…")
        await pg.execute(SEED_PRODUCTS)
        print("Seeding orders…")
        await pg.execute(SEED_ORDERS)
        print("Seeding order_items…")
        await pg.execute(SEED_ORDER_ITEMS)

        # Commit explicitly (asyncpg raw connections are in autocommit by
        # default but SQLAlchemy wraps them in a transaction — we need to
        # commit or the changes will be rolled back when the context exits)
        await sa_conn.commit()

    await engine.dispose()
    print("Done — demo schema ready.")


if __name__ == "__main__":
    asyncio.run(seed())
