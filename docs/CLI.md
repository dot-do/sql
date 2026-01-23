# DoSQL CLI Reference

Comprehensive documentation for the DoSQL command-line interface.

## Table of Contents

- [Installation](#installation)
- [Commands Overview](#commands-overview)
- [dosql init](#dosql-init)
- [dosql migrate](#dosql-migrate)
- [dosql shell](#dosql-shell)
- [dosql query](#dosql-query)
- [dosql inspect](#dosql-inspect)
- [dosql export](#dosql-export)
- [dosql import](#dosql-import)
- [Configuration File](#configuration-file)
- [Environment Variables](#environment-variables)
- [Examples and Common Workflows](#examples-and-common-workflows)

---

## Installation

### Global Installation

```bash
npm install -g dosql-cli
```

### Local Installation (Project)

```bash
npm install --save-dev dosql-cli
```

When installed locally, use `npx dosql` or add scripts to your `package.json`:

```json
{
  "scripts": {
    "db:migrate": "dosql migrate",
    "db:shell": "dosql shell",
    "db:inspect": "dosql inspect"
  }
}
```

### Verify Installation

```bash
dosql --version
# dosql-cli v0.1.0

dosql --help
# Usage: dosql <command> [options]
# ...
```

### Requirements

- Node.js 18.0.0 or later
- npm 9.0.0 or later (or pnpm/yarn equivalent)

---

## Commands Overview

| Command | Description |
|---------|-------------|
| `dosql init` | Initialize a new DoSQL project |
| `dosql migrate` | Run database migrations |
| `dosql shell` | Start interactive SQL shell |
| `dosql query` | Execute a single SQL query |
| `dosql inspect` | Inspect database schema and structure |
| `dosql export` | Export data to various formats |
| `dosql import` | Import data from files |

### Global Options

All commands support these global options:

| Option | Short | Description |
|--------|-------|-------------|
| `--config <path>` | `-c` | Path to config file (default: `.dosqlrc.json`) |
| `--env <name>` | `-e` | Environment name (default: `development`) |
| `--verbose` | `-v` | Enable verbose output |
| `--quiet` | `-q` | Suppress non-essential output |
| `--help` | `-h` | Show help for command |
| `--version` | | Show version number |

---

## dosql init

Initialize a new DoSQL project with recommended directory structure and configuration.

### Usage

```bash
dosql init [directory] [options]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `directory` | Target directory (default: current directory) |

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--template <name>` | `-t` | Project template (`basic`, `multi-tenant`, `analytics`) |
| `--migrations-dir <path>` | `-m` | Migrations directory (default: `.do/migrations`) |
| `--typescript` | | Generate TypeScript configuration |
| `--drizzle` | | Include Drizzle ORM integration |
| `--force` | `-f` | Overwrite existing files |

### Examples

```bash
# Initialize in current directory
dosql init

# Initialize with specific template
dosql init --template multi-tenant

# Initialize in a new directory
dosql init my-project --typescript

# Initialize with Drizzle integration
dosql init --drizzle --typescript
```

### Generated Structure

```
your-project/
├── .do/
│   └── migrations/
│       └── 001_initial_schema.sql
├── .dosqlrc.json
├── src/
│   └── database.ts (if --typescript)
└── wrangler.jsonc (if Cloudflare project detected)
```

### Templates

#### basic (default)

Simple single-database setup with users table example.

#### multi-tenant

Multi-tenant configuration with tenant isolation patterns.

#### analytics

Analytics-focused setup with time-series patterns and partitioning examples.

---

## dosql migrate

Run, create, or manage database migrations.

### Usage

```bash
dosql migrate <subcommand> [options]
```

### Subcommands

| Subcommand | Description |
|------------|-------------|
| `run` | Apply pending migrations (default) |
| `create` | Create a new migration file |
| `status` | Show migration status |
| `rollback` | Rollback migrations |
| `reset` | Reset database (rollback all, then migrate) |

### dosql migrate run

Apply pending migrations to the database.

```bash
dosql migrate run [options]
```

| Option | Short | Description |
|--------|-------|-------------|
| `--target <id>` | `-t` | Migrate to specific migration ID |
| `--dry-run` | | Show what would be executed without applying |
| `--step <n>` | `-s` | Apply only N migrations |
| `--database <name>` | `-d` | Target database name |

```bash
# Apply all pending migrations
dosql migrate run

# Apply with dry run
dosql migrate run --dry-run

# Apply only next 2 migrations
dosql migrate run --step 2

# Migrate to specific version
dosql migrate run --target 003_add_indexes
```

### dosql migrate create

Create a new migration file.

```bash
dosql migrate create <name> [options]
```

| Option | Short | Description |
|--------|-------|-------------|
| `--sql` | | Create SQL migration (default) |
| `--typescript` | | Create TypeScript migration |
| `--empty` | | Create empty migration file |

```bash
# Create SQL migration
dosql migrate create add_users_table

# Create TypeScript migration
dosql migrate create add_audit_triggers --typescript

# Output: Created .do/migrations/004_add_users_table.sql
```

### dosql migrate status

Show current migration status.

```bash
dosql migrate status [options]
```

| Option | Short | Description |
|--------|-------|-------------|
| `--json` | | Output as JSON |
| `--pending` | | Show only pending migrations |

```bash
dosql migrate status

# Output:
# Migration Status
# ================
# Total: 5, Applied: 3, Pending: 2
#
# ID                      Status    Applied At
# ----------------------- --------- -------------------
# 001_create_users        applied   2024-01-15 10:30:00
# 002_add_posts           applied   2024-01-15 10:30:01
# 003_add_indexes         applied   2024-01-16 09:15:22
# 004_add_comments        pending   -
# 005_add_likes           pending   -
```

### dosql migrate rollback

Rollback applied migrations.

```bash
dosql migrate rollback [options]
```

| Option | Short | Description |
|--------|-------|-------------|
| `--step <n>` | `-s` | Rollback N migrations (default: 1) |
| `--target <id>` | `-t` | Rollback to specific migration |
| `--all` | | Rollback all migrations |
| `--force` | `-f` | Force rollback without confirmation |

```bash
# Rollback last migration
dosql migrate rollback

# Rollback last 3 migrations
dosql migrate rollback --step 3

# Rollback to specific point
dosql migrate rollback --target 002_add_posts
```

### dosql migrate reset

Reset database by rolling back all migrations and re-applying them.

```bash
dosql migrate reset [options]
```

| Option | Short | Description |
|--------|-------|-------------|
| `--seed` | | Run seed data after migrations |
| `--force` | `-f` | Skip confirmation prompt |

```bash
# Reset database
dosql migrate reset

# Reset and seed
dosql migrate reset --seed
```

---

## dosql shell

Start an interactive SQL shell connected to your database.

### Usage

```bash
dosql shell [options]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--database <name>` | `-d` | Database name to connect to |
| `--tenant <id>` | `-t` | Tenant ID (for multi-tenant setups) |
| `--read-only` | `-r` | Open in read-only mode |
| `--history <path>` | | Path to history file |
| `--no-history` | | Disable command history |
| `--format <fmt>` | `-f` | Output format (`table`, `json`, `csv`) |
| `--time-travel <point>` | | Query at specific time point |

### Examples

```bash
# Start interactive shell
dosql shell

# Connect to specific database
dosql shell --database analytics

# Connect to specific tenant
dosql shell --tenant acme-corp

# Read-only mode
dosql shell --read-only

# Query historical data
dosql shell --time-travel "2024-01-01T00:00:00Z"
```

### Shell Commands

Once in the shell, you can use these special commands:

| Command | Description |
|---------|-------------|
| `.help` | Show available commands |
| `.tables` | List all tables |
| `.schema [table]` | Show table schema |
| `.indexes [table]` | Show indexes |
| `.quit` or `.exit` | Exit the shell |
| `.format <fmt>` | Change output format |
| `.timer on/off` | Toggle query timing |
| `.output <file>` | Redirect output to file |
| `.read <file>` | Execute SQL from file |
| `.dump [table]` | Dump table as SQL |
| `.branches` | List database branches |
| `.checkout <branch>` | Switch to branch |
| `.migrations` | Show migration status |

### Shell Session Example

```
$ dosql shell --database myapp

DoSQL Shell v0.1.0
Connected to: myapp
Type .help for commands, .quit to exit

dosql> .tables
users
posts
comments

dosql> .schema users
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

dosql> SELECT COUNT(*) as total FROM users;
+-------+
| total |
+-------+
| 1234  |
+-------+
1 row (0.003s)

dosql> .format json
Output format: json

dosql> SELECT * FROM users LIMIT 2;
[
  {"id": 1, "name": "Alice", "email": "alice@example.com", "created_at": "2024-01-15T10:30:00Z"},
  {"id": 2, "name": "Bob", "email": "bob@example.com", "created_at": "2024-01-15T11:45:00Z"}
]

dosql> .quit
Goodbye!
```

---

## dosql query

Execute a single SQL query from the command line.

### Usage

```bash
dosql query <sql> [options]
dosql query -f <file> [options]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--file <path>` | `-f` | Execute SQL from file |
| `--database <name>` | `-d` | Target database |
| `--tenant <id>` | `-t` | Tenant ID |
| `--params <json>` | `-p` | Query parameters as JSON |
| `--format <fmt>` | `-o` | Output format (`table`, `json`, `csv`, `tsv`) |
| `--no-headers` | | Omit column headers (for csv/tsv) |
| `--time-travel <point>` | | Query at specific time point |
| `--branch <name>` | `-b` | Query specific branch |

### Examples

```bash
# Simple query
dosql query "SELECT * FROM users LIMIT 10"

# Query with parameters
dosql query "SELECT * FROM users WHERE id = :id" --params '{"id": 42}'

# Query from file
dosql query -f reports/monthly-stats.sql

# Output as JSON
dosql query "SELECT * FROM users" --format json

# Output as CSV
dosql query "SELECT * FROM users" --format csv > users.csv

# Query specific tenant
dosql query "SELECT COUNT(*) FROM orders" --tenant acme-corp

# Time travel query
dosql query "SELECT * FROM users" --time-travel "2024-01-01T00:00:00Z"

# Query specific branch
dosql query "SELECT * FROM products" --branch feature-new-pricing
```

### Piping and Scripting

```bash
# Pipe query from stdin
echo "SELECT COUNT(*) FROM users" | dosql query -

# Use in scripts
USER_COUNT=$(dosql query "SELECT COUNT(*) as count FROM users" --format json | jq '.[0].count')
echo "Total users: $USER_COUNT"

# Chain queries
dosql query "SELECT id FROM inactive_users" --format csv --no-headers | \
  xargs -I {} dosql query "DELETE FROM users WHERE id = {}"
```

---

## dosql inspect

Inspect database schema, tables, indexes, and metadata.

### Usage

```bash
dosql inspect [subcommand] [options]
```

### Subcommands

| Subcommand | Description |
|------------|-------------|
| `schema` | Show full database schema (default) |
| `tables` | List all tables |
| `table <name>` | Show details for specific table |
| `indexes` | List all indexes |
| `stats` | Show database statistics |
| `branches` | List database branches |
| `migrations` | Show migration history |

### dosql inspect schema

Show the complete database schema.

```bash
dosql inspect schema [options]
```

| Option | Short | Description |
|--------|-------|-------------|
| `--format <fmt>` | `-f` | Output format (`sql`, `json`, `typescript`) |
| `--tables <list>` | `-t` | Comma-separated list of tables |
| `--output <file>` | `-o` | Write to file |

```bash
# Show full schema as SQL
dosql inspect schema

# Export schema as TypeScript types
dosql inspect schema --format typescript > schema.d.ts

# Export specific tables
dosql inspect schema --tables users,posts --format json
```

### dosql inspect tables

List all tables in the database.

```bash
dosql inspect tables [options]
```

| Option | Short | Description |
|--------|-------|-------------|
| `--format <fmt>` | `-f` | Output format (`table`, `json`) |
| `--details` | `-d` | Include row counts and sizes |

```bash
dosql inspect tables

# Output:
# Tables in database 'myapp'
# ==========================
# Name          Columns   Indexes
# ------------- --------- --------
# users         5         2
# posts         7         3
# comments      6         2
```

### dosql inspect table

Show detailed information about a specific table.

```bash
dosql inspect table <name> [options]
```

```bash
dosql inspect table users

# Output:
# Table: users
# ============
#
# Columns:
# Name        Type      Nullable  Default            PK
# ----------- --------- --------- ------------------ ---
# id          INTEGER   NO        AUTOINCREMENT      YES
# name        TEXT      NO        -                  NO
# email       TEXT      YES       -                  NO
# active      BOOLEAN   YES       true               NO
# created_at  TEXT      YES       CURRENT_TIMESTAMP  NO
#
# Indexes:
# Name              Columns    Unique
# ----------------- ---------- ------
# PRIMARY           id         YES
# idx_users_email   email      YES
#
# Foreign Keys: None
#
# Row Count: 1,234
# Table Size: 256 KB
```

### dosql inspect stats

Show database statistics.

```bash
dosql inspect stats [options]
```

```bash
dosql inspect stats

# Output:
# Database Statistics
# ===================
#
# General:
#   Database Size: 4.2 MB
#   Page Size: 4096 bytes
#   Page Count: 1075
#   WAL Size: 128 KB
#
# Tables:
#   Total Tables: 8
#   Total Rows: 45,678
#   Largest Table: events (32,100 rows)
#
# Indexes:
#   Total Indexes: 15
#   Index Size: 1.1 MB
#
# Migrations:
#   Applied: 12
#   Pending: 0
#
# Branches:
#   Total: 3
#   Current: main
```

### dosql inspect branches

List all database branches.

```bash
dosql inspect branches [options]
```

| Option | Short | Description |
|--------|-------|-------------|
| `--format <fmt>` | `-f` | Output format (`table`, `json`) |
| `--all` | `-a` | Include merged/deleted branches |

```bash
dosql inspect branches

# Output:
# Database Branches
# =================
# Name          Created           LSN        Status
# ------------- ----------------- ---------- --------
# main          2024-01-01 00:00  12345      current
# feature-x     2024-01-10 14:30  12400      active
# experiment    2024-01-12 09:00  12450      active
```

---

## dosql export

Export database tables or query results to various formats.

### Usage

```bash
dosql export <target> [options]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `target` | Table name, SQL query, or `--all` for full database |

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--format <fmt>` | `-f` | Output format (`csv`, `json`, `ndjson`, `parquet`, `sql`) |
| `--output <path>` | `-o` | Output file path (default: stdout) |
| `--database <name>` | `-d` | Source database |
| `--tenant <id>` | `-t` | Tenant ID |
| `--where <clause>` | `-w` | WHERE clause for filtering |
| `--limit <n>` | `-l` | Limit number of rows |
| `--compress` | `-z` | Compress output (gzip) |
| `--schema-only` | | Export schema without data |
| `--data-only` | | Export data without schema |
| `--time-travel <point>` | | Export from specific time point |

### Examples

```bash
# Export table to CSV
dosql export users --format csv --output users.csv

# Export table to JSON
dosql export users --format json --output users.json

# Export query results
dosql export "SELECT * FROM orders WHERE status = 'pending'" -f csv -o pending.csv

# Export with filter
dosql export users --where "active = true" --format json

# Export full database as SQL dump
dosql export --all --format sql --output backup.sql

# Export with compression
dosql export users --format csv --compress --output users.csv.gz

# Export to Parquet (for analytics)
dosql export events --format parquet --output events.parquet

# Export historical data
dosql export users --time-travel "2024-01-01T00:00:00Z" --format json

# Export schema only
dosql export --all --schema-only --format sql --output schema.sql
```

### Format Details

#### csv

Standard comma-separated values with headers.

```bash
dosql export users --format csv
# id,name,email,created_at
# 1,Alice,alice@example.com,2024-01-15T10:30:00Z
# 2,Bob,bob@example.com,2024-01-15T11:45:00Z
```

#### json

Array of JSON objects.

```bash
dosql export users --format json
# [
#   {"id": 1, "name": "Alice", "email": "alice@example.com"},
#   {"id": 2, "name": "Bob", "email": "bob@example.com"}
# ]
```

#### ndjson

Newline-delimited JSON (one object per line).

```bash
dosql export users --format ndjson
# {"id": 1, "name": "Alice", "email": "alice@example.com"}
# {"id": 2, "name": "Bob", "email": "bob@example.com"}
```

#### parquet

Apache Parquet format for analytics workloads.

```bash
dosql export events --format parquet --output events.parquet
```

#### sql

SQL INSERT statements or full dump.

```bash
dosql export users --format sql
# INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
# INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com');
```

---

## dosql import

Import data from external files into the database.

### Usage

```bash
dosql import <file> [options]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `file` | Path to file to import (or `-` for stdin) |

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--table <name>` | `-t` | Target table name |
| `--format <fmt>` | `-f` | Input format (`csv`, `json`, `ndjson`, `parquet`, `sql`) |
| `--database <name>` | `-d` | Target database |
| `--tenant <id>` | | Tenant ID |
| `--create-table` | `-c` | Create table if not exists |
| `--truncate` | | Truncate table before import |
| `--upsert` | `-u` | Upsert on conflict (requires --on-conflict) |
| `--on-conflict <cols>` | | Columns for conflict detection |
| `--batch-size <n>` | `-b` | Batch size for inserts (default: 1000) |
| `--skip-errors` | | Continue on row errors |
| `--dry-run` | | Validate without importing |
| `--csv-delimiter <char>` | | CSV delimiter (default: `,`) |
| `--csv-headers` | | CSV has header row (default: true) |
| `--csv-quote <char>` | | CSV quote character (default: `"`) |

### Examples

```bash
# Import CSV to existing table
dosql import users.csv --table users

# Import JSON array
dosql import data.json --table events --format json

# Import and create table
dosql import products.csv --table products --create-table

# Import with upsert
dosql import users.csv --table users --upsert --on-conflict id

# Import SQL dump
dosql import backup.sql --format sql

# Import from stdin
cat data.json | dosql import - --table events --format json

# Import with custom CSV settings
dosql import data.tsv --table logs --csv-delimiter '\t'

# Dry run to validate
dosql import large-file.csv --table data --dry-run

# Import Parquet file
dosql import analytics.parquet --table events --format parquet

# Import to specific tenant
dosql import tenant-data.json --table users --tenant acme-corp

# Truncate before import
dosql import fresh-data.csv --table cache --truncate
```

### Import from URL

```bash
# Import from HTTP URL
dosql import "https://example.com/data.csv" --table external_data

# Import from R2
dosql import "r2://bucket/exports/data.parquet" --table analytics
```

### Bulk Import Performance

For large imports, consider these options:

```bash
# Large CSV import with optimal settings
dosql import large-file.csv \
  --table events \
  --batch-size 5000 \
  --skip-errors \
  2>&1 | tee import.log
```

---

## Configuration File

DoSQL CLI uses `.dosqlrc.json` for project configuration.

### File Location

The CLI searches for configuration in this order:

1. Path specified with `--config` flag
2. `.dosqlrc.json` in current directory
3. `.dosqlrc.json` in parent directories (up to root)
4. `~/.dosqlrc.json` (global config)

### Configuration Schema

```json
{
  "$schema": "https://dosql.dev/schema/dosqlrc.json",
  "version": 1,

  "defaults": {
    "database": "myapp",
    "format": "table"
  },

  "migrations": {
    "directory": ".do/migrations",
    "table": "__dosql_migrations",
    "drizzle": false
  },

  "environments": {
    "development": {
      "database": "myapp-dev",
      "url": "http://localhost:8787"
    },
    "staging": {
      "database": "myapp-staging",
      "url": "https://staging.example.com",
      "tenant": "staging-tenant"
    },
    "production": {
      "database": "myapp-prod",
      "url": "https://api.example.com",
      "readOnly": true
    }
  },

  "shell": {
    "historyFile": "~/.dosql_history",
    "historySize": 10000,
    "format": "table",
    "timing": true,
    "multiline": true
  },

  "export": {
    "defaultFormat": "csv",
    "compress": false,
    "batchSize": 10000
  },

  "import": {
    "batchSize": 1000,
    "skipErrors": false
  },

  "inspect": {
    "defaultFormat": "table"
  }
}
```

### Configuration Options

#### defaults

| Property | Type | Description |
|----------|------|-------------|
| `database` | `string` | Default database name |
| `format` | `string` | Default output format |
| `tenant` | `string` | Default tenant ID |

#### migrations

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `directory` | `string` | `.do/migrations` | Migrations folder path |
| `table` | `string` | `__dosql_migrations` | Migration tracking table |
| `drizzle` | `boolean` | `false` | Use Drizzle Kit format |

#### environments

Environment-specific configuration overrides. Selected with `--env` flag.

| Property | Type | Description |
|----------|------|-------------|
| `database` | `string` | Database name |
| `url` | `string` | API endpoint URL |
| `tenant` | `string` | Tenant identifier |
| `readOnly` | `boolean` | Restrict to read-only operations |

#### shell

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `historyFile` | `string` | `~/.dosql_history` | Command history file |
| `historySize` | `number` | `10000` | Max history entries |
| `format` | `string` | `table` | Default output format |
| `timing` | `boolean` | `true` | Show query timing |
| `multiline` | `boolean` | `true` | Enable multiline input |

#### export

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `defaultFormat` | `string` | `csv` | Default export format |
| `compress` | `boolean` | `false` | Enable compression by default |
| `batchSize` | `number` | `10000` | Rows per batch |

#### import

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `batchSize` | `number` | `1000` | Rows per insert batch |
| `skipErrors` | `boolean` | `false` | Continue on errors |

---

## Environment Variables

DoSQL CLI respects these environment variables:

### Connection

| Variable | Description |
|----------|-------------|
| `DOSQL_DATABASE` | Default database name |
| `DOSQL_TENANT` | Default tenant ID |
| `DOSQL_URL` | API endpoint URL |
| `DOSQL_API_KEY` | Authentication API key |
| `DOSQL_TOKEN` | Authentication bearer token |

### Configuration

| Variable | Description |
|----------|-------------|
| `DOSQL_CONFIG` | Path to configuration file |
| `DOSQL_ENV` | Environment name (e.g., `production`) |
| `DOSQL_MIGRATIONS_DIR` | Migrations directory path |

### Output

| Variable | Description |
|----------|-------------|
| `DOSQL_FORMAT` | Default output format |
| `DOSQL_NO_COLOR` | Disable colored output |
| `DOSQL_VERBOSE` | Enable verbose output |

### Shell

| Variable | Description |
|----------|-------------|
| `DOSQL_HISTORY` | Path to shell history file |
| `DOSQL_HISTORY_SIZE` | Maximum history entries |

### Example .env File

```bash
# .env
DOSQL_DATABASE=myapp
DOSQL_URL=https://api.example.com
DOSQL_API_KEY=your-api-key
DOSQL_ENV=development
DOSQL_FORMAT=json
```

### Precedence

Configuration is resolved in this order (later overrides earlier):

1. Built-in defaults
2. Global config (`~/.dosqlrc.json`)
3. Project config (`.dosqlrc.json`)
4. Environment variables
5. Command-line flags

---

## Examples and Common Workflows

### Project Setup

```bash
# Create new project
mkdir my-dosql-project && cd my-dosql-project

# Initialize DoSQL
dosql init --typescript

# Create first migration
dosql migrate create initial_schema

# Edit migration file, then apply
dosql migrate run

# Verify
dosql inspect tables
```

### Development Workflow

```bash
# Start shell for exploration
dosql shell

# Create migration for new feature
dosql migrate create add_orders_table

# Apply migration
dosql migrate run

# Check status
dosql migrate status

# If something went wrong, rollback
dosql migrate rollback
```

### Multi-Tenant Operations

```bash
# Query specific tenant
dosql query "SELECT * FROM users" --tenant acme-corp

# Export tenant data
dosql export users --tenant acme-corp --format json --output acme-users.json

# Shell connected to tenant
dosql shell --tenant acme-corp
```

### Data Migration

```bash
# Export from one database
dosql export users --database source --format json --output users.json

# Import to another
dosql import users.json --database target --table users --format json

# Or pipe directly
dosql export users --database source --format ndjson | \
  dosql import - --database target --table users --format ndjson
```

### Backup and Restore

```bash
# Full backup
dosql export --all --format sql --output backup-$(date +%Y%m%d).sql

# Compressed backup
dosql export --all --format sql --compress --output backup-$(date +%Y%m%d).sql.gz

# Restore
dosql import backup-20240115.sql --format sql
```

### CI/CD Integration

```yaml
# .github/workflows/migrate.yml
name: Database Migrations

on:
  push:
    branches: [main]
    paths:
      - '.do/migrations/**'

jobs:
  migrate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install DoSQL CLI
        run: npm install -g dosql-cli

      - name: Run Migrations (Staging)
        run: dosql migrate run --env staging
        env:
          DOSQL_API_KEY: ${{ secrets.STAGING_API_KEY }}

      - name: Run Migrations (Production)
        run: dosql migrate run --env production
        env:
          DOSQL_API_KEY: ${{ secrets.PROD_API_KEY }}
```

### Analytics Export

```bash
# Export to Parquet for analytics
dosql export events \
  --where "created_at >= '2024-01-01'" \
  --format parquet \
  --output events-2024.parquet

# Upload to data warehouse
aws s3 cp events-2024.parquet s3://analytics-bucket/events/
```

### Database Branching Workflow

```bash
# Create feature branch
dosql query "CREATE BRANCH 'feature-new-schema'"

# Shell on branch
dosql shell --branch feature-new-schema

# Make changes
dosql migrate create new_schema_changes --branch feature-new-schema
dosql migrate run --branch feature-new-schema

# Test
dosql query "SELECT * FROM new_table" --branch feature-new-schema

# Merge back
dosql query "MERGE BRANCH 'feature-new-schema' INTO 'main'"

# Clean up
dosql query "DROP BRANCH 'feature-new-schema'"
```

### Time Travel Queries

```bash
# Query data from specific point in time
dosql query "SELECT * FROM users" --time-travel "2024-01-01T00:00:00Z"

# Export historical state
dosql export users --time-travel "2024-01-15T12:00:00Z" --format json

# Compare current vs historical
dosql query "SELECT COUNT(*) FROM users"
dosql query "SELECT COUNT(*) FROM users" --time-travel "2024-01-01T00:00:00Z"
```

### Scripting Examples

```bash
#!/bin/bash
# backup.sh - Daily backup script

set -e

DATE=$(date +%Y%m%d)
BACKUP_DIR="/backups"

# Export each database
for db in users analytics orders; do
  echo "Backing up $db..."
  dosql export --all \
    --database $db \
    --format sql \
    --compress \
    --output "$BACKUP_DIR/$db-$DATE.sql.gz"
done

# Clean old backups (keep 30 days)
find $BACKUP_DIR -name "*.sql.gz" -mtime +30 -delete

echo "Backup complete!"
```

```bash
#!/bin/bash
# health-check.sh - Database health check

set -e

# Check migration status
PENDING=$(dosql migrate status --json | jq '.pending | length')
if [ "$PENDING" -gt 0 ]; then
  echo "Warning: $PENDING pending migrations"
  exit 1
fi

# Check database connectivity
dosql query "SELECT 1" > /dev/null 2>&1 || {
  echo "Error: Cannot connect to database"
  exit 1
}

# Check table counts
USER_COUNT=$(dosql query "SELECT COUNT(*) as c FROM users" --format json | jq '.[0].c')
echo "Users: $USER_COUNT"

echo "Health check passed!"
```

---

## See Also

- [Getting Started Guide](./getting-started.md) - Basic usage and setup
- [API Reference](./api-reference.md) - Programmatic API documentation
- [Advanced Features](./advanced.md) - Time travel, branching, CDC
- [Architecture](./architecture.md) - Understanding DoSQL internals
