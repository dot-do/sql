# DoSQL Migrations Spike

**Status:** SPIKE / RESEARCH
**Date:** 2026-01-21
**Author:** DoSQL Team

## Executive Summary

This document details the research and spike implementation for DoSQL migrations, focusing on the unique requirements of DoSQL's DB-per-tenant model with clone-on-demand.

## 1. DoSQL Native Convention

### 1.1 `.do/migrations/*.sql` Structure (Recommended)

DoSQL uses a simple, portable migration convention:

```
📂 .do/
  └── 📂 migrations/
      ├── 001_create_users.sql
      ├── 002_add_posts.sql
      ├── 003_add_comments.sql
      └── 004_add_indexes.sql
```

**Benefits:**
- Simple: Just numbered `.sql` files
- Git-friendly: Easy to review, merge, and track
- Portable: Works anywhere (no special tools required)
- Fast: No JSON parsing, just read SQL files
- Compatible: Drizzle Kit migrations also supported

**Naming Convention:**
- Format: `NNN_descriptive_name.sql`
- Prefix: 3-digit number for ordering (001, 002, etc.)
- Separator: Underscore
- Extension: `.sql`

**Example Migration File (`001_create_users.sql`):**
```sql
-- Create users table
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
```

### 1.2 Usage with DB()

```typescript
// Auto-loads from .do/migrations/
const db = DB('tenants');

// Or explicit path
const db = DB('tenants', {
  migrations: { folder: '.do/migrations' }
});

// With Drizzle Kit compatibility
const db = DB('tenants', {
  migrations: { drizzle: 'drizzle' }
});
```

## 2. Drizzle Kit Compatibility

### 1.1 Folder Structure

Drizzle Kit uses two migration formats:

#### V3 Format (Current - Recommended)
```
📂 drizzle/
  ├── 📂 20240823160430_create_users/
  │   ├── migration.sql
  │   └── snapshot.json
  ├── 📂 20240823160431_add_posts/
  │   ├── migration.sql
  │   └── snapshot.json
  └── 📂 20240824120000_add_comments/
      ├── migration.sql
      └── snapshot.json
```

- Timestamp-based folder names: `YYYYMMDDHHMMSS_descriptive_name`
- Each migration is self-contained in its own folder
- Lexicographic ordering determines application sequence

#### V2 Format (Legacy)
```
📂 drizzle/
  ├── 📂 meta/
  │   └── _journal.json
  ├── 📂 0000_warm_stone_men/
  │   ├── migration.sql
  │   └── snapshot.json
  └── 📂 0001_cool_dust_devil/
      ├── migration.sql
      └── snapshot.json
```

- Sequential numeric prefixes: `0000`, `0001`, etc.
- Central journal file tracks migration order
- Prone to merge conflicts in team environments

### 1.2 Migration File Format

**migration.sql:**
```sql
CREATE TABLE "users" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT NOT NULL,
  "email" TEXT UNIQUE
);

CREATE INDEX "users_email_idx" ON "users" ("email");
```

**snapshot.json:**
```json
{
  "version": "5",
  "dialect": "postgresql",
  "tables": {
    "users": {
      "name": "users",
      "columns": {
        "id": {
          "name": "id",
          "type": "serial",
          "notNull": true,
          "primaryKey": true
        },
        "name": {
          "name": "name",
          "type": "text",
          "notNull": true
        }
      }
    }
  }
}
```

### 1.3 Migration Tracking

Drizzle Kit tracks applied migrations in a database table:

```sql
CREATE TABLE "__drizzle_migrations" (
  id SERIAL PRIMARY KEY,
  hash TEXT NOT NULL,
  created_at BIGINT
);
```

DoSQL uses a similar approach with enhanced tracking:

```sql
CREATE TABLE "__dosql_migrations" (
  id TEXT PRIMARY KEY,         -- Migration ID (timestamp_name)
  applied_at TEXT NOT NULL,    -- ISO timestamp
  checksum TEXT NOT NULL,      -- SHA-256 of SQL
  duration_ms INTEGER NOT NULL -- Execution time
);
```

### 1.4 Drizzle Kit CLI

Key commands:
- `drizzle-kit generate` - Generate migrations from schema changes
- `drizzle-kit migrate` - Apply pending migrations
- `drizzle-kit push` - Push schema directly (dev only)
- `drizzle-kit studio` - Visual database browser

**Recommendation:** DoSQL can use `drizzle-kit generate` directly for schema diffing. No need to reimplement schema comparison logic.

## 2. DoSQL DB() Integration Design

### 2.1 API Design

```typescript
// Option A: Migrations as DB() parameter
const db = DB('tenants', {
  migrations: [
    { id: '20240101000000_init', sql: 'CREATE TABLE ...' },
    { id: '20240101000001_add_email', sql: 'ALTER TABLE ...' },
  ],
  autoMigrate: true, // Apply on first access
});

// Option B: Drizzle folder reference
const db = DB('tenants', {
  migrations: { drizzle: './drizzle' },
  autoMigrate: true,
});

// Option C: Lazy loader function
const db = DB('tenants', {
  migrations: async () => loadMigrationsFromR2(),
  autoMigrate: true,
});
```

**Recommendation:** Support all three options via `MigrationSource` union type.

### 2.2 Migration Options

```typescript
interface DatabaseMigrationOptions {
  // Migration source
  migrations?: MigrationSource;

  // Auto-migrate on first access (default: true)
  autoMigrate?: boolean;

  // Migration runner options
  migrationOptions?: {
    // Table name for tracking (default: __dosql_migrations)
    migrationsTable?: string;

    // Dry run mode
    dryRun?: boolean;

    // Wrap in transaction (default: true)
    transactional?: boolean;

    // Timeout per migration (default: 30000ms)
    timeout?: number;

    // Logger
    logger?: MigrationLogger;
  };
}
```

### 2.3 Internal Flow

```
DB('tenants') called
       │
       ▼
┌─────────────────────────┐
│ Check if DB is clone    │
│ (clone_source marker)   │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Get current version     │
│ from DO storage         │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Compare with latest     │
│ migration version       │
└───────────┬─────────────┘
            │
    ┌───────┴───────┐
    │               │
    ▼               ▼
 Up to date    Needs migration
    │               │
    ▼               ▼
 Return DB    Apply pending
              migrations
                  │
                  ▼
              Record in
              DO storage
                  │
                  ▼
              Clear clone
              marker (if set)
                  │
                  ▼
              Return DB
```

## 3. Migration-on-Clone Strategy

### 3.1 Problem Statement

When a DB clone is created from a template:
1. Clone starts at version N (template's version)
2. Current schema may be at version M (M > N)
3. Clone needs migrations N+1 through M applied on first load
4. Must be fast and transactional

### 3.2 Solution: Clone Context

```typescript
interface CloneMigrationContext {
  // Template database version
  sourceVersion: string | null;

  // Target version to migrate to
  targetVersion: string;

  // Migrations to apply (from source+1 to target)
  pendingMigrations: Migration[];

  // Whether clone needs migration
  needsMigration: boolean;
}
```

### 3.3 Clone Workflow

```
Template DB (v3)              Tenant Clone
┌──────────────┐              ┌──────────────┐
│ users        │   CLONE      │ users        │
│ posts        │ ──────────►  │ posts        │
│ _migrations  │              │ _migrations  │
│   v1, v2, v3 │              │   v1, v2, v3 │
└──────────────┘              │ _clone_src=v3│
                              └──────────────┘
                                    │
                                    │ First access
                                    ▼
                              ┌──────────────┐
                              │ Load current │
                              │ migrations   │
                              │ (v1-v5)      │
                              └──────────────┘
                                    │
                                    ▼
                              ┌──────────────┐
                              │ Apply v4, v5 │
                              │ to clone     │
                              └──────────────┘
                                    │
                                    ▼
                              ┌──────────────┐
                              │ Clear clone  │
                              │ marker       │
                              │ Now at v5    │
                              └──────────────┘
```

### 3.4 Batched Application

For performance, multiple migrations are batched when possible:

```typescript
// Pseudo-code
async function applyPendingMigrations(migrations: Migration[]) {
  if (migrations.length === 0) return;

  // Option 1: Transactional batch (if supported)
  await db.beginTransaction();
  try {
    for (const m of migrations) {
      await db.exec(m.sql);
      await recordMigration(m);
    }
    await db.commit();
  } catch (e) {
    await db.rollback();
    throw e;
  }

  // Option 2: Sequential with savepoints
  for (const m of migrations) {
    await db.savepoint(`migration_${m.id}`);
    try {
      await db.exec(m.sql);
      await recordMigration(m);
      await db.release(`migration_${m.id}`);
    } catch (e) {
      await db.rollbackTo(`migration_${m.id}`);
      throw e;
    }
  }
}
```

## 4. Spike Implementation

The spike implementation is in `packages/dosql/src/migrations/`:

### 4.1 Files Created

| File | Purpose |
|------|---------|
| `types.ts` | Core type definitions, checksums, utilities |
| `runner.ts` | Migration application logic |
| `drizzle-compat.ts` | Drizzle Kit format parsing |
| `schema-tracker.ts` | DO storage version tracking |
| `migrations.test.ts` | Test suite (workers-vitest-pool) |
| `index.ts` | Module exports |

### 4.2 Key APIs

```typescript
// Create migrations
const migrations = createMigrations([
  { id: '20240101000000_init', sql: 'CREATE TABLE users (id INT)' },
  { id: '20240101000001_posts', sql: 'CREATE TABLE posts (id INT)' },
]);

// Initialize with migrations
const status = await initializeWithMigrations({
  migrations,
  storage: ctx.storage,  // DO storage
  db: database,          // Database executor
  autoMigrate: true,
});

// Load from Drizzle folder
const drizzleMigrations = await loadDrizzleMigrations({
  basePath: './drizzle',
  fs: nodeFs,  // or R2 adapter
  includeSnapshots: true,
});

// Track schema in DO storage
const tracker = createSchemaTracker(ctx.storage);
const context = await tracker.getCloneMigrationContext(migrations);
```

### 4.3 Test Coverage

The test suite covers:
- Checksum calculation
- Migration ID comparison/ordering
- Type guards
- Runner status and migration
- Drizzle folder parsing (v2 and v3)
- Schema tracker version management
- Clone workflow
- DB() integration

## 5. Drizzle Kit CLI Integration

### 5.1 Can We Use drizzle-kit Directly?

**Yes, for generation.** The workflow:

```bash
# 1. Define schema in TypeScript (Drizzle format)
# src/schema.ts
export const users = sqliteTable('users', {
  id: integer('id').primaryKey(),
  name: text('name').notNull(),
});

# 2. Generate migrations with drizzle-kit
npx drizzle-kit generate

# 3. Load generated migrations in DoSQL
const db = DB('tenants', {
  migrations: { drizzle: './drizzle' },
});
```

### 5.2 Should DoSQL Have Its Own Generator?

**Optional but valuable.** Benefits:
- Diff DoSQL schema DSL directly
- No Drizzle dependency for generation
- Could generate from runtime schema

```typescript
// Future: DoSQL schema diffing
const schema = {
  users: {
    id: 'uuid!',
    name: 'string',
    email: 'string?#',
  },
};

// Generate migration from schema change
const migration = generateMigration(previousSchema, schema);
```

### 5.3 Hybrid Approach (Recommended)

1. **Generation:** Use `drizzle-kit generate` (battle-tested)
2. **Loading:** DoSQL compatibility layer parses Drizzle format
3. **Application:** DoSQL migration runner
4. **Tracking:** DoSQL schema tracker in DO storage

## 6. Code Examples

### 6.1 Basic Usage

```typescript
import { DB } from '@dotdo/dosql';

// Define migrations inline
const db = await DB('my-tenant', {
  migrations: [
    {
      id: '20240101000000_init',
      sql: `
        CREATE TABLE users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          email TEXT UNIQUE
        );
        CREATE INDEX users_email_idx ON users (email);
      `,
    },
    {
      id: '20240101000001_add_posts',
      sql: `
        CREATE TABLE posts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER REFERENCES users(id),
          title TEXT NOT NULL,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
      `,
    },
  ],
  autoMigrate: true,
});

// Use the database
const users = await db.query('SELECT * FROM users');
```

### 6.2 Using Drizzle Migrations

```typescript
import { DB } from '@dotdo/dosql';

// Load from Drizzle folder
const db = await DB('my-tenant', {
  migrations: { drizzle: './drizzle' },
  autoMigrate: true,
});
```

### 6.3 Custom Loader

```typescript
import { DB } from '@dotdo/dosql';

// Load migrations from R2 or API
const db = await DB('my-tenant', {
  migrations: async () => {
    const response = await fetch('/api/migrations');
    return response.json();
  },
  autoMigrate: true,
});
```

### 6.4 Manual Control

```typescript
import {
  createSchemaTracker,
  createMigrationRunner,
  createMigrations
} from '@dotdo/dosql/migrations';

// In Durable Object
export class TenantDB implements DurableObject {
  private tracker: SchemaTracker;
  private runner: MigrationRunner;

  constructor(state: DurableObjectState, env: Env) {
    this.tracker = createSchemaTracker(state.storage);
    this.runner = createMigrationRunner(this.db, {
      logger: console,
    });
  }

  async migrate() {
    const migrations = createMigrations([
      { id: '20240101000000_init', sql: 'CREATE TABLE test (id INT)' },
    ]);

    // Check status
    const status = await this.runner.getStatus(migrations);
    console.log('Pending:', status.pending.length);

    // Apply if needed
    if (status.needsMigration) {
      const result = await this.runner.migrate(migrations);
      console.log('Applied:', result.applied.length);
    }
  }
}
```

### 6.5 Reversible Migrations

```typescript
import { createReversibleMigration } from '@dotdo/dosql/migrations';

const migration = createReversibleMigration(
  // Up
  `
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL
    )
  `,
  // Down
  `
    DROP TABLE users
  `,
  'create_users'
);

// Rollback support
await runner.rollbackTo(migrations, '20240101000000_init');
```

## 7. Migration-on-Clone Workflow Diagram

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         TEMPLATE DATABASE                                   │
│                                                                            │
│   Schema Version: v5                                                       │
│   ┌─────────────────────────────────────────────────────────────────────┐ │
│   │ __dosql_migrations                                                   │ │
│   │ ┌──────────────────────────────────────────────────────────────────┐│ │
│   │ │ id                        │ applied_at          │ checksum       ││ │
│   │ ├──────────────────────────────────────────────────────────────────┤│ │
│   │ │ 20240101000000_init       │ 2024-01-01 00:00:00│ abc123         ││ │
│   │ │ 20240102000000_users      │ 2024-01-02 00:00:00│ def456         ││ │
│   │ │ 20240103000000_posts      │ 2024-01-03 00:00:00│ ghi789         ││ │
│   │ │ 20240104000000_comments   │ 2024-01-04 00:00:00│ jkl012         ││ │
│   │ │ 20240105000000_likes      │ 2024-01-05 00:00:00│ mno345         ││ │
│   │ └──────────────────────────────────────────────────────────────────┘│ │
│   └─────────────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ CLONE (Copy-on-Write)
                                    │ Template at v3
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                         TENANT CLONE (Initial State)                        │
│                                                                            │
│   Clone Source: v3 (20240103000000_posts)                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐ │
│   │ __dosql_migrations                                                   │ │
│   │ ┌──────────────────────────────────────────────────────────────────┐│ │
│   │ │ id                        │ applied_at          │ checksum       ││ │
│   │ ├──────────────────────────────────────────────────────────────────┤│ │
│   │ │ 20240101000000_init       │ 2024-01-01 00:00:00│ abc123         ││ │
│   │ │ 20240102000000_users      │ 2024-01-02 00:00:00│ def456         ││ │
│   │ │ 20240103000000_posts      │ 2024-01-03 00:00:00│ ghi789         ││ │
│   │ └──────────────────────────────────────────────────────────────────┘│ │
│   │                                                                      │ │
│   │ _schema:clone_source = "20240103000000_posts"                       │ │
│   └─────────────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ FIRST ACCESS
                                    │ Current migrations: v1-v5
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                         MIGRATION DETECTION                                 │
│                                                                            │
│   getCloneMigrationContext() returns:                                      │
│   {                                                                        │
│     sourceVersion: "20240103000000_posts",                                 │
│     targetVersion: "20240105000000_likes",                                 │
│     pendingMigrations: [                                                   │
│       { id: "20240104000000_comments", ... },                              │
│       { id: "20240105000000_likes", ... }                                  │
│     ],                                                                     │
│     needsMigration: true                                                   │
│   }                                                                        │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ APPLY MIGRATIONS
                                    │ (Batched, Transactional)
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                         TENANT CLONE (Final State)                          │
│                                                                            │
│   Schema Version: v5 (Up to date)                                          │
│   ┌─────────────────────────────────────────────────────────────────────┐ │
│   │ __dosql_migrations                                                   │ │
│   │ ┌──────────────────────────────────────────────────────────────────┐│ │
│   │ │ id                        │ applied_at          │ checksum       ││ │
│   │ ├──────────────────────────────────────────────────────────────────┤│ │
│   │ │ 20240101000000_init       │ 2024-01-01 00:00:00│ abc123         ││ │
│   │ │ 20240102000000_users      │ 2024-01-02 00:00:00│ def456         ││ │
│   │ │ 20240103000000_posts      │ 2024-01-03 00:00:00│ ghi789         ││ │
│   │ │ 20240104000000_comments   │ 2024-01-21 14:30:00│ jkl012         ││ │
│   │ │ 20240105000000_likes      │ 2024-01-21 14:30:01│ mno345         ││ │
│   │ └──────────────────────────────────────────────────────────────────┘│ │
│   │                                                                      │ │
│   │ _schema:clone_source = (deleted)                                     │ │
│   └─────────────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────────┘
```

## 8. Recommendations

### 8.1 Immediate Actions

1. **Adopt V3 folder format** - Timestamp-based, git-friendly
2. **Use Drizzle Kit for generation** - No need to reimplement schema diffing
3. **Store tracking in DO storage** - Native to Cloudflare platform
4. **Auto-migrate by default** - Best DX for DB-per-tenant model

### 8.2 Future Enhancements

1. **DoSQL Schema Diffing** - Generate migrations from DoSQL DSL
2. **Migration Squashing** - Combine old migrations for performance
3. **Branch Support** - Handle migrations in parallel branches
4. **R2 Migration Storage** - Centralized migration storage
5. **Migration Preview** - Dry-run with SQL output

### 8.3 Testing Strategy

1. Use `@cloudflare/vitest-pool-workers` for all tests
2. No mocks - test real implementations
3. Use in-memory storage/executor for unit tests
4. Integration tests with actual DO instances

## 9. Sources

- [Drizzle ORM - Migrations](https://orm.drizzle.team/docs/migrations)
- [Drizzle Kit - generate](https://orm.drizzle.team/docs/drizzle-kit-generate)
- [Drizzle Kit - migrate](https://orm.drizzle.team/docs/drizzle-kit-migrate)
- [Drizzle Config File](https://orm.drizzle.team/docs/drizzle-config-file)
- [Migrations for Teams](https://orm.drizzle.team/docs/kit-migrations-for-teams)
- [V3 Folder Structure Discussion](https://github.com/drizzle-team/drizzle-orm/discussions/2832)

## 10. Appendix: Type Definitions

See `packages/dosql/src/migrations/types.ts` for complete type definitions.

Key types:
- `Migration` - Core migration structure
- `MigrationSnapshot` - Schema state after migration
- `AppliedMigration` - Record of applied migration
- `MigrationStatus` - Current database migration state
- `CloneMigrationContext` - Clone-specific migration context
- `MigrationSource` - Union type for migration sources
