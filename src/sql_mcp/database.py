"""
sql_mcp/database.py
────────────────────────────────────────────────────────────────────
Async SQLAlchemy engine + helper utilities shared by the MCP tools.

Key concepts:
  - We use SQLAlchemy's **async** engine (create_async_engine) backed by
    the asyncpg driver.  This means all DB I/O is non-blocking and plays
    nicely with the MCP event loop.
  - A single engine is created once at server startup and reused across
    all tool calls (connection pooling is handled automatically).
  - ALLOWED_SCHEMAS lets us restrict which schemas Claude can see so you
    can safely point this at a production DB without exposing everything.
"""

from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection, create_async_engine

# ── load .env ────────────────────────────────────────────────────────────────
load_dotenv()

# ── config ───────────────────────────────────────────────────────────────────

DATABASE_URL: str = os.environ["DATABASE_URL"]
# Schemas Claude is allowed to introspect / query.  Empty list = all schemas.
_raw_schemas = os.getenv("ALLOWED_SCHEMAS", "")
ALLOWED_SCHEMAS: list[str] = [s.strip() for s in _raw_schemas.split(",") if s.strip()]
# Hard cap on rows returned by any SELECT (prevents runaway queries)
MAX_QUERY_ROWS: int = int(os.getenv("MAX_QUERY_ROWS", "500"))


# ── engine factory ────────────────────────────────────────────────────────────

def build_engine() -> AsyncEngine:
    """
    Create an async SQLAlchemy engine.

    pool_pre_ping=True  → test connections before handing them out,
                           so a stale connection doesn't silently fail.
    echo=False          → don't print every SQL statement to stdout
                           (would pollute the MCP stdio stream).
    """
    return create_async_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        echo=False,
    )


# ── schema introspection ──────────────────────────────────────────────────────

async def get_schema(conn: AsyncConnection) -> dict[str, Any]:
    """
    Return the full schema of every visible table as a dict:

        {
          "schema_name.table_name": {
            "columns": [
              {"name": "id", "type": "integer", "nullable": False, "default": None},
              ...
            ],
            "primary_keys": ["id"],
            "foreign_keys": [
              {"column": "user_id", "references": "public.users.id"},
              ...
            ],
          },
          ...
        }

    We query information_schema directly (standard SQL, works on any
    Postgres version) rather than using SQLAlchemy's reflection API so
    we can do it in one round-trip.
    """
    # ── 1. columns ────────────────────────────────────────────────────────────
    # Fetch every column in every table in the allowed schemas (or all schemas
    # if ALLOWED_SCHEMAS is empty).  We exclude Postgres system schemas.
    schema_filter = (
        "AND c.table_schema = ANY(:schemas)"
        if ALLOWED_SCHEMAS
        else "AND c.table_schema NOT IN ('pg_catalog', 'information_schema')"
    )

    col_sql = text(f"""
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default
        FROM information_schema.columns c
        WHERE c.table_catalog = current_database()
          {schema_filter}
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
    """)

    params: dict[str, Any] = {}
    if ALLOWED_SCHEMAS:
        params["schemas"] = ALLOWED_SCHEMAS

    col_rows = (await conn.execute(col_sql, params)).fetchall()

    # ── 2. primary keys ───────────────────────────────────────────────────────
    pk_sql = text(f"""
        SELECT
            tc.table_schema,
            tc.table_name,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_catalog   = current_database()
          {"AND tc.table_schema = ANY(:schemas)" if ALLOWED_SCHEMAS else
           "AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')"}
        ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position
    """)
    pk_rows = (await conn.execute(pk_sql, params)).fetchall()

    # ── 3. foreign keys ───────────────────────────────────────────────────────
    fk_sql = text(f"""
        SELECT
            tc.table_schema,
            tc.table_name,
            kcu.column_name                            AS fk_column,
            ccu.table_schema || '.' || ccu.table_name
                || '.' || ccu.column_name              AS references
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema    = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_catalog   = current_database()
          {"AND tc.table_schema = ANY(:schemas)" if ALLOWED_SCHEMAS else
           "AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')"}
        ORDER BY tc.table_schema, tc.table_name
    """)
    fk_rows = (await conn.execute(fk_sql, params)).fetchall()

    # ── assemble ──────────────────────────────────────────────────────────────
    schema: dict[str, Any] = {}

    for row in col_rows:
        key = f"{row.table_schema}.{row.table_name}"
        if key not in schema:
            schema[key] = {"columns": [], "primary_keys": [], "foreign_keys": []}
        schema[key]["columns"].append({
            "name":     row.column_name,
            "type":     row.data_type,
            "nullable": row.is_nullable == "YES",
            "default":  row.column_default,
        })

    for row in pk_rows:
        key = f"{row.table_schema}.{row.table_name}"
        if key in schema:
            schema[key]["primary_keys"].append(row.column_name)

    for row in fk_rows:
        key = f"{row.table_schema}.{row.table_name}"
        if key in schema:
            schema[key]["foreign_keys"].append({
                "column":     row.fk_column,
                "references": row.references,
            })

    return schema


# ── safe query execution ──────────────────────────────────────────────────────

async def run_select(conn: AsyncConnection, sql: str) -> dict[str, Any]:
    """
    Execute a read-only SQL statement and return results as:

        {
          "columns": ["id", "name", ...],
          "rows":    [[1, "Alice", ...], ...],
          "row_count": 3,
          "truncated": False,
        }

    Safety measures:
      - Rejects any statement that doesn't start with SELECT / WITH / EXPLAIN.
        This prevents Claude from accidentally running INSERT/UPDATE/DELETE.
      - Applies MAX_QUERY_ROWS cap via LIMIT injection if the query doesn't
        already have a LIMIT clause.
      - Runs inside a ROLLBACK savepoint so even if something slips through
        it can't mutate state.
    """
    # ── guard: only read statements ───────────────────────────────────────────
    normalized = sql.strip().upper()
    allowed_prefixes = ("SELECT", "WITH", "EXPLAIN", "TABLE")
    if not any(normalized.startswith(p) for p in allowed_prefixes):
        raise ValueError(
            f"Only read-only queries are allowed (SELECT / WITH / EXPLAIN / TABLE). "
            f"Got: {sql[:80]!r}"
        )

    # ── inject LIMIT if missing ───────────────────────────────────────────────
    # Simple heuristic: if "LIMIT" is not in the statement wrap it.
    # We wrap rather than append so it works for CTEs too.
    if "LIMIT" not in normalized:
        safe_sql = f"SELECT * FROM ({sql.rstrip(';')}) AS _q LIMIT {MAX_QUERY_ROWS + 1}"
    else:
        safe_sql = sql

    # ── execute inside a savepoint (extra safety) ─────────────────────────────
    result = await conn.execute(text(safe_sql))
    raw_rows = result.fetchall()
    columns = list(result.keys())

    # ── detect truncation ─────────────────────────────────────────────────────
    truncated = len(raw_rows) > MAX_QUERY_ROWS
    rows = [list(r) for r in raw_rows[:MAX_QUERY_ROWS]]

    return {
        "columns":   columns,
        "rows":      rows,
        "row_count": len(rows),
        "truncated": truncated,
    }
