# sql-mcp

A Model Context Protocol (MCP) server that lets Claude inspect a PostgreSQL
database schema and run read-only queries using natural language.

## What it does

| Tool | Description |
|---|---|
| `list_tables` | List all tables/views (name, column count, primary key) |
| `describe_table` | Full schema for one table: columns, types, PKs, FKs |
| `run_query` | Execute a read-only SQL query, return results as a table |
| `get_full_schema` | Dump the entire schema as JSON (for complex multi-table queries) |

## Architecture

```
Claude Desktop
     │  MCP (stdio)
     ▼
sql_mcp/server.py     ← MCP server, 4 tools, async SQLAlchemy engine
     └── database.py  ← async engine, schema introspection, safe run_select
```

**Key design decisions:**

- **Async end-to-end** — SQLAlchemy async engine + asyncpg driver means every DB
  call is non-blocking.  No `asyncio.to_thread()` needed (unlike devops-mcp).
- **Read-only enforcement** — `run_select()` rejects any statement that doesn't
  start with `SELECT / WITH / EXPLAIN / TABLE` before it ever reaches the DB.
- **Row cap** — results are capped at `MAX_QUERY_ROWS` (default 500) to prevent
  runaway queries from flooding the context window.
- **Schema filtering** — `ALLOWED_SCHEMAS` restricts which schemas Claude can
  see, so you can safely point this at a production database.

## Requirements

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager
- Docker (for the demo Postgres container)

## Setup

```bash
git clone https://github.com/Kamalesh-Kavin/sql-mcp
cd sql-mcp

# Install dependencies
uv sync

# Copy and fill in your DB URL
cp .env.example .env
```

### Start the demo database (one-time)

```bash
# Create network + volume
docker network create sql_mcp_network
docker volume create sql_mcp_data

# Start Postgres on port 5434
# (5433 may be taken by a local Postgres instance on macOS)
docker run -d \
  --name sql-mcp-db \
  --network sql_mcp_network \
  -v sql_mcp_data:/var/lib/postgresql/data \
  -e POSTGRES_USER=sql_mcp_user \
  -e POSTGRES_PASSWORD=sql_mcp_pass \
  -e POSTGRES_DB=sql_mcp_demo \
  -p 5434:5432 \
  postgres:16

# Seed the e-commerce demo schema
uv run python -m sql_mcp.seed
```

## Run the smoke test

```bash
uv run python smoke_test.py
```

All 6 test sections should print `PASS`.

## Wire into Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
"sql-assistant": {
  "command": "/Users/yourname/.local/bin/uv",
  "args": [
    "--directory", "/path/to/sql-mcp",
    "run", "sql-mcp"
  ]
}
```

Restart Claude Desktop, then try:

- *"List all tables in the database"*
- *"Describe the orders table"*
- *"Show me the top 5 customers by total spend"*
- *"How many orders are in each status?"*
- *"Which products have never been ordered?"*

## Demo schema

```
users ──< orders ──< order_items >── products
```

| Table | Description |
|---|---|
| `users` | 8 customers from 8 countries |
| `products` | 10 products across 4 categories |
| `orders` | 10 orders with status (pending/paid/shipped/cancelled) |
| `order_items` | 16 line items linking orders to products |

## Environment variables (`.env`)

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | — | asyncpg connection URL (`postgresql+asyncpg://...`) |
| `ALLOWED_SCHEMAS` | `public` | Comma-separated schemas to expose; blank = all |
| `MAX_QUERY_ROWS` | `500` | Hard cap on rows returned per query |
