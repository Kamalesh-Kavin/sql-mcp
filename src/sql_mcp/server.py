"""
sql_mcp/server.py
────────────────────────────────────────────────────────────────────
MCP server: 4 tools that let Claude inspect and query a PostgreSQL
database using natural language.

Tools:
  1. list_tables        — show every table/view in the allowed schemas
  2. describe_table     — full schema for one table (columns, PKs, FKs)
  3. run_query          — execute a read-only SQL query, return results
  4. get_full_schema    — dump the entire schema in one shot (for context)

Architecture:
  - The async SQLAlchemy engine is opened once at lifespan startup and
    stored in AppState.  Each tool call acquires a connection from the
    pool, uses it, and releases it — no leaks.
  - All DB calls are already async (asyncpg driver) so we don't need
    asyncio.to_thread() here, unlike the devops-mcp Docker/psutil tools.
  - The server only exposes read-only operations.  run_query() in
    database.py rejects any non-SELECT statement before it even reaches
    the DB.
"""

from __future__ import annotations

import json
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator

from mcp.server.fastmcp import FastMCP, Context
from sqlalchemy.ext.asyncio import AsyncEngine

from sql_mcp.database import (
    build_engine,
    get_schema,
    run_select,
    ALLOWED_SCHEMAS,
)


# ── app state ─────────────────────────────────────────────────────────────────

@dataclass
class AppState:
    """Holds the single async engine created at startup."""
    engine: AsyncEngine


# ── lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[AppState]:
    """
    Open the DB engine when the MCP server starts and close it cleanly
    when the server shuts down.  This is identical to the pattern used
    in devops-mcp for the Docker client.
    """
    engine = build_engine()
    print(f"[sql-mcp] engine ready — {engine.url!r}", file=sys.stderr)
    try:
        yield AppState(engine=engine)
    finally:
        await engine.dispose()
        print("[sql-mcp] engine disposed", file=sys.stderr)


# ── server ────────────────────────────────────────────────────────────────────

mcp = FastMCP(
    "sql-assistant",
    instructions=(
        "You are a helpful SQL assistant. You can inspect a PostgreSQL database "
        "schema and run read-only queries. Always look at the schema first before "
        "writing a query. Never mutate data — only SELECT statements are allowed."
    ),
    lifespan=lifespan,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _fmt_results(result: dict[str, Any]) -> str:
    """
    Format a query result dict into a readable markdown table string.

    Input:  {"columns": [...], "rows": [[...], ...], "row_count": N, "truncated": bool}
    Output: markdown table  (or a plain message if no rows)
    """
    cols = result["columns"]
    rows = result["rows"]

    if not rows:
        return "Query returned 0 rows."

    # Build a simple pipe-delimited table
    header = " | ".join(str(c) for c in cols)
    divider = " | ".join("---" for _ in cols)
    body_lines = [" | ".join(str(v) for v in row) for row in rows]

    table = "\n".join([header, divider] + body_lines)

    note = ""
    if result.get("truncated"):
        note = f"\n\n> **Results truncated** — showing first {result['row_count']} rows."

    return table + note


# ── tool 1: list_tables ───────────────────────────────────────────────────────

@mcp.tool()
async def list_tables(ctx: Context) -> str:
    """
    List all tables and views available in the database.

    Returns a formatted list showing:
      - fully-qualified table name (schema.table)
      - number of columns
      - primary key columns

    Use this as your first step before writing any SQL query.
    """
    state: AppState = ctx.request_context.lifespan_context
    async with state.engine.connect() as conn:
        schema = await get_schema(conn)

    if not schema:
        schemas_msg = (
            f"schemas: {ALLOWED_SCHEMAS}" if ALLOWED_SCHEMAS else "all schemas"
        )
        return f"No tables found in {schemas_msg}."

    lines = []
    for table_key, info in sorted(schema.items()):
        n_cols = len(info["columns"])
        pks = ", ".join(info["primary_keys"]) or "(none)"
        lines.append(f"  {table_key}  ({n_cols} columns, PK: {pks})")

    return "Available tables:\n" + "\n".join(lines)


# ── tool 2: describe_table ────────────────────────────────────────────────────

@mcp.tool()
async def describe_table(table_name: str, ctx: Context) -> str:
    """
    Show the full schema for a single table: columns, types, nullability,
    primary keys, and foreign keys.

    Args:
        table_name: The table to describe. Can be just 'orders' or
                    fully-qualified 'public.orders'.
    """
    state: AppState = ctx.request_context.lifespan_context
    async with state.engine.connect() as conn:
        schema = await get_schema(conn)

    # Allow bare name ('orders') or qualified ('public.orders')
    matches = {
        k: v for k, v in schema.items()
        if k == table_name or k.endswith(f".{table_name}")
    }

    if not matches:
        available = ", ".join(sorted(schema.keys()))
        return (
            f"Table '{table_name}' not found.\n"
            f"Available tables: {available}"
        )

    # If multiple schemas have a table with that name, show all
    lines: list[str] = []
    for key, info in sorted(matches.items()):
        lines.append(f"## {key}\n")

        # Columns
        lines.append("### Columns")
        lines.append("| Column | Type | Nullable | Default |")
        lines.append("|--------|------|----------|---------|")
        for col in info["columns"]:
            nullable = "YES" if col["nullable"] else "NO"
            default  = col["default"] or ""
            pk_marker = " *(PK)*" if col["name"] in info["primary_keys"] else ""
            lines.append(
                f"| {col['name']}{pk_marker} | {col['type']} | {nullable} | {default} |"
            )

        # Foreign keys
        if info["foreign_keys"]:
            lines.append("\n### Foreign Keys")
            for fk in info["foreign_keys"]:
                lines.append(f"- `{fk['column']}` → `{fk['references']}`")

        lines.append("")  # blank line between tables

    return "\n".join(lines)


# ── tool 3: run_query ─────────────────────────────────────────────────────────

@mcp.tool()
async def run_query(sql: str, ctx: Context) -> str:
    """
    Execute a read-only SQL query and return the results as a table.

    Only SELECT, WITH (CTE), EXPLAIN, and TABLE statements are allowed.
    Results are capped at MAX_QUERY_ROWS rows to prevent runaway queries.

    Args:
        sql: A read-only SQL statement to execute.

    Examples:
        SELECT * FROM users LIMIT 5;
        SELECT u.name, COUNT(o.id) AS orders FROM users u
          LEFT JOIN orders o ON o.user_id = u.id GROUP BY u.name;
    """
    state: AppState = ctx.request_context.lifespan_context
    try:
        async with state.engine.connect() as conn:
            result = await run_select(conn, sql)
        return _fmt_results(result)
    except ValueError as exc:
        # Our own safety check (non-SELECT statement)
        return f"Blocked: {exc}"
    except Exception as exc:
        # SQL syntax errors, missing tables, etc.
        return f"Query error: {exc}"


# ── tool 4: get_full_schema ───────────────────────────────────────────────────

@mcp.tool()
async def get_full_schema(ctx: Context) -> str:
    """
    Return the complete schema of all tables as JSON.

    This is useful when you want to understand all the relationships at once
    before writing a complex multi-table query.  For a large database this
    can be verbose — prefer describe_table() for a single table.
    """
    state: AppState = ctx.request_context.lifespan_context
    async with state.engine.connect() as conn:
        schema = await get_schema(conn)
    return json.dumps(schema, indent=2)


# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    """Entry point for `uv run sql-mcp` (defined in pyproject.toml scripts)."""
    mcp.run()


if __name__ == "__main__":
    main()
