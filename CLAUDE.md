# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DoSQL & DoLake: A SQL database for Cloudflare Workers using Durable Objects. Written entirely in TypeScript (no WASM), achieving a 7KB bundle size vs 500KB+ for SQLite-WASM alternatives.

**Packages:**
- `dosql` - SQL database engine (server-side Durable Object)
- `dolake` - Lakehouse for CDC streaming to Iceberg/Parquet on R2
- `sql.do` - Client SDK for DoSQL
- `lake.do` - Client SDK for DoLake
- `@dotdo/sql-types` - Shared TypeScript types

## Development Commands

```bash
pnpm install          # Install dependencies
pnpm build            # Build all packages
pnpm test             # Run all tests
pnpm typecheck        # Type check all packages
pnpm lint             # Lint all packages

# Package-specific commands
pnpm --filter dosql test                    # Run dosql tests only
pnpm --filter dolake test                   # Run dolake tests only
pnpm --filter dosql test -- --testNamePattern="parser"  # Run tests matching pattern

# Run a single test file
cd packages/dosql && npx vitest run src/parser/dml.test.ts

# Run tests in watch mode
cd packages/dosql && npx vitest src/btree/btree.test.ts

# Benchmarks
pnpm --filter dosql benchmark               # Run benchmarks
pnpm --filter dosql benchmark:quick         # Quick benchmark run
```

## Architecture

### DoSQL Query Execution Pipeline

```
SQL String → Parser → Planner → Executor → Storage
              ↓         ↓          ↓          ↓
            AST    QueryPlan   Operators   B-tree/Columnar
```

**Key modules in `packages/dosql/src/`:**
- `parser/` - SQL parser (tokenizer in `shared/tokenizer.ts`, unified parser in `unified.ts`)
- `planner/` - Query planner and cost-based optimizer
- `engine/` - Execution engine with pull-based operators (`operators/`)
- `btree/` - B+ tree for OLTP row storage (pages sized for DO's 2MB limit)
- `columnar/` - Columnar storage for OLAP analytics
- `fsx/` - File system abstraction (DO storage, R2, tiered)
- `wal/` - Write-ahead log for durability
- `transaction/` - ACID transactions with MVCC
- `cdc/` - Change Data Capture streaming
- `sharding/` - Query routing with vindexes (hash, range, lookup, region)
- `rpc/` - CapnWeb RPC client/server
- `worker/` - Durable Object entry point (`database.ts`)

### DoLake Architecture

CDC events flow: DoSQL WAL → CDC Capture → DoLake DO → Parquet files on R2

**Key modules in `packages/dolake/src/`:**
- `dolake.ts` - Main Durable Object
- `compaction.ts` - Merges small Parquet files
- `partitioning.ts` - Table partitioning logic
- `iceberg.ts` - Iceberg table format support
- `parquet.ts` - Parquet file handling (uses hyparquet)

### Storage Tiers

| Tier | Backend | Latency | Use |
|------|---------|---------|-----|
| Hot | DO Storage | ~1ms | Active data, B-tree pages |
| Warm | R2 + Cache | ~50ms | Overflow, archived indexes |
| Cold | R2 Parquet | ~100ms | Analytics, historical data |

## Testing Philosophy

**TDD with NO MOCKS** - Tests run in actual Cloudflare Workers environment via `@cloudflare/vitest-pool-workers`. Use real Durable Objects and real SQLite, not mocks. Integration tests over unit tests.

Test config in `vitest.config.ts` sets up Durable Object bindings via miniflare.

## Code Style

- TypeScript strict mode
- Branded types for IDs (`TransactionId`, `LSN`)
- Prefer `unknown` over `any`
- Direct imports to avoid heavy transitive dependencies in Worker code

---

## Beads Issue Tracking

This project uses **bd** (beads) for issue tracking.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync               # Sync with git
```

### Hierarchical Issue IDs

```
sql-a3f8      (Epic)
sql-a3f8.1    (Task under epic)
sql-a3f8.1.1  (Sub-task under task)
```

### Creating Issues

```bash
bd create --title="..." --type=task|bug|feature --priority=N
# Priority: 0=critical, 1=high, 2=medium, 3=low, 4=backlog
```

### Dependencies

```bash
bd dep add <issue> <depends-on>   # issue depends on depends-on
bd blocked                        # Show blocked issues
```

### Session Completion Workflow

**MANDATORY** - Work is NOT complete until `git push` succeeds:

```bash
git pull --rebase
bd sync
git push
git status  # MUST show "up to date with origin"
```
