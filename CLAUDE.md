# DoSQL & DoLake

SQL database for Cloudflare Workers using Durable Objects.

## Project Structure

```
packages/
├── dosql/          # SQL database engine
│   ├── src/
│   │   ├── parser/     # SQL parser (SELECT, INSERT, UPDATE, DELETE, DDL)
│   │   ├── planner/    # Query planner and optimizer
│   │   ├── engine/     # Execution engine
│   │   ├── sharding/   # Horizontal scaling and query routing
│   │   ├── btree/      # B-tree index implementation
│   │   ├── fsx/        # File system abstraction (memory, DO storage, R2)
│   │   ├── wal/        # Write-ahead log
│   │   ├── transaction/# Transaction management and isolation
│   │   ├── database/   # Database operations and lock management
│   │   └── worker/     # Durable Object entry point
│   └── wrangler.jsonc
│
└── dolake/         # Lakehouse for CDC and analytics
    ├── src/
    │   ├── dolake.ts       # Main Durable Object
    │   ├── compaction.ts   # Parquet file compaction
    │   ├── partitioning.ts # Table partitioning
    │   ├── query-engine.ts # Analytical queries
    │   ├── rate-limiter.ts # WebSocket rate limiting
    │   └── schemas.ts      # Zod validation schemas
    └── wrangler.jsonc
```

## Development Commands

```bash
# Install dependencies
pnpm install

# Build all packages
pnpm build

# Run tests (uses workers-vitest-pool - NO MOCKS)
pnpm test

# Run specific package tests
pnpm --filter @dotdo/dosql test
pnpm --filter @dotdo/dolake test
```

## Testing Philosophy

This project follows **TDD with NO MOCKS**:

- Tests run in actual Cloudflare Workers environment via `workers-vitest-pool`
- Use real Durable Objects, not mocks
- Use real SQLite, not mocks
- Integration tests over unit tests

## Code Style

- TypeScript strict mode
- Branded types for IDs (TransactionId, LSN)
- Prefer `unknown` over `any`
- No unnecessary abstractions

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

Beads supports hierarchical IDs for organizing work:

```
sql-a3f8      (Epic)
sql-a3f8.1    (Task under epic)
sql-a3f8.1.1  (Sub-task under task)
```

Use hierarchical IDs to break down large features:

```bash
# Create an epic
bd create --title="Add sharding support" --type=epic --priority=1

# Create tasks under the epic (use .N suffix)
bd create --title="Implement shard router" --type=task --id=sql-a3f8.1
bd create --title="Add query rewriting" --type=task --id=sql-a3f8.2

# Create sub-tasks
bd create --title="Parse shard key" --type=task --id=sql-a3f8.1.1
```

### Creating Issues

```bash
# Create a task
bd create --title="Fix SQL injection" --type=task --priority=0

# Create a feature
bd create --title="Add window functions" --type=feature --priority=1

# Create a bug
bd create --title="Memory leak in B-tree" --type=bug --priority=1

# Priority: 0=critical, 1=high, 2=medium, 3=low, 4=backlog
```

### Dependencies

```bash
# Add dependency (issue depends on depends-on)
bd dep add sql-abc sql-xyz

# View blocked issues
bd blocked

# View issue with dependencies
bd show sql-abc
```

### Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
