"""
smoke_test.py — sql-mcp
────────────────────────────────────────────────────────────────────
Validates the two core modules (database.py) without starting the
full MCP server.  Run with:

    uv run python smoke_test.py
"""

import asyncio
import sys

SEP = "=" * 60


def section(title: str) -> None:
    print(f"\n{SEP}\n  {title}\n{SEP}")


def ok(msg: str) -> None:
    print(f"    PASS: {msg}")


def fail(msg: str) -> None:
    print(f"    FAIL: {msg}")
    sys.exit(1)


# ── import under test ─────────────────────────────────────────────────────────
try:
    from sql_mcp.database import build_engine, get_schema, run_select
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)


async def run_tests() -> None:
    engine = build_engine()

    # ── 1. connection ─────────────────────────────────────────────────────────
    section("1. Database — connection")
    try:
        async with engine.connect() as conn:
            from sqlalchemy import text
            row = (await conn.execute(text("SELECT version()"))).scalar()
        ok(f"Connected: {str(row)[:60]}")
    except Exception as e:
        fail(f"Connection failed: {e}")

    # ── 2. schema introspection ───────────────────────────────────────────────
    section("2. Database — schema introspection")
    async with engine.connect() as conn:
        schema = await get_schema(conn)

    expected_tables = {"public.users", "public.products", "public.orders", "public.order_items"}
    found = set(schema.keys())
    for t in expected_tables:
        if t in found:
            ok(f"Table present: {t}")
        else:
            fail(f"Expected table missing: {t}")

    # Check columns, PKs, and FKs
    users_info = schema["public.users"]
    col_names = [c["name"] for c in users_info["columns"]]
    assert "id" in col_names,    "users.id missing"
    assert "email" in col_names, "users.email missing"
    ok(f"users columns: {col_names}")
    ok(f"users PK: {users_info['primary_keys']}")

    oi_info = schema["public.order_items"]
    fk_cols = [fk["column"] for fk in oi_info["foreign_keys"]]
    assert "order_id" in fk_cols,   "order_items.order_id FK missing"
    assert "product_id" in fk_cols, "order_items.product_id FK missing"
    ok(f"order_items FKs: {oi_info['foreign_keys']}")

    # ── 3. run_select — basic query ───────────────────────────────────────────
    section("3. Database — run_select (basic)")
    async with engine.connect() as conn:
        result = await run_select(conn, "SELECT id, name, country FROM users ORDER BY id")
    assert result["columns"] == ["id", "name", "country"], f"Unexpected columns: {result['columns']}"
    assert result["row_count"] == 8, f"Expected 8 users, got {result['row_count']}"
    assert result["truncated"] is False
    ok(f"Fetched {result['row_count']} users, columns: {result['columns']}")
    for row in result["rows"]:
        print(f"      [{row[0]}] {row[1]:<20} {row[2]}")

    # ── 4. run_select — join query ────────────────────────────────────────────
    section("4. Database — run_select (JOIN)")
    join_sql = """
        SELECT u.name, COUNT(o.id) AS order_count
        FROM users u
        LEFT JOIN orders o ON o.user_id = u.id
        GROUP BY u.name
        ORDER BY order_count DESC, u.name
    """
    async with engine.connect() as conn:
        result = await run_select(conn, join_sql)
    assert result["row_count"] == 8, f"Expected 8 rows, got {result['row_count']}"
    ok(f"JOIN query returned {result['row_count']} rows")
    for row in result["rows"]:
        print(f"      {row[0]:<20} orders={row[1]}")

    # ── 5. run_select — blocked mutation ──────────────────────────────────────
    section("5. Database — run_select (mutation blocked)")
    try:
        async with engine.connect() as conn:
            await run_select(conn, "DELETE FROM users WHERE id = 1")
        fail("DELETE should have been blocked")
    except ValueError as e:
        ok(f"DELETE correctly blocked: {e}")

    try:
        async with engine.connect() as conn:
            await run_select(conn, "DROP TABLE users")
        fail("DROP TABLE should have been blocked")
    except ValueError as e:
        ok(f"DROP TABLE correctly blocked: {e}")

    # ── 6. run_select — LIMIT injection ───────────────────────────────────────
    section("6. Database — run_select (LIMIT injection)")
    async with engine.connect() as conn:
        # This query has no LIMIT — our code should inject one
        result = await run_select(conn, "SELECT * FROM products")
    ok(f"No-LIMIT query returned {result['row_count']} rows (capped at MAX_QUERY_ROWS)")
    assert result["row_count"] <= 500, "Row cap not applied"
    ok("Row cap applied correctly")

    await engine.dispose()
    print(f"\n{SEP}")
    print("  ALL TESTS PASSED")
    print(SEP)


if __name__ == "__main__":
    asyncio.run(run_tests())
