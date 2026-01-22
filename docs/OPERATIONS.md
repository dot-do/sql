# DoSQL & DoLake Deployment & Operations Guide

**Version**: 1.0.0
**Last Updated**: 2026-01-22
**Maintainer**: Platform Team

---

## Table of Contents

1. [Deployment](#deployment)
   - [Wrangler Configuration](#wrangler-configuration)
   - [Environment Variables](#environment-variables)
   - [Multi-Environment Setup](#multi-environment-setup)
2. [Durable Object Configuration](#durable-object-configuration)
   - [DO Bindings](#do-bindings)
   - [Migration Configuration](#migration-configuration)
   - [SQLite Storage Settings](#sqlite-storage-settings)
3. [R2 Configuration](#r2-configuration)
   - [Bucket Setup for DoLake](#bucket-setup-for-dolake)
   - [Lifecycle Policies](#lifecycle-policies)
   - [Cross-Region Considerations](#cross-region-considerations)
4. [Monitoring](#monitoring)
   - [Key Metrics to Track](#key-metrics-to-track)
   - [Cloudflare Analytics Integration](#cloudflare-analytics-integration)
   - [Custom Logging Patterns](#custom-logging-patterns)
5. [Scaling](#scaling)
   - [Sharding Configuration](#sharding-configuration)
   - [When to Add Shards](#when-to-add-shards)
   - [Load Balancing Strategies](#load-balancing-strategies)
6. [Backup & Recovery](#backup--recovery)
   - [Time Travel for Point-in-Time Recovery](#time-travel-for-point-in-time-recovery)
   - [R2 Backup Strategies](#r2-backup-strategies)
   - [Disaster Recovery Procedures](#disaster-recovery-procedures)
7. [Troubleshooting](#troubleshooting)
   - [Common Issues and Solutions](#common-issues-and-solutions)
   - [Debug Logging](#debug-logging)
   - [Performance Diagnostics](#performance-diagnostics)
8. [Maintenance](#maintenance)
   - [WAL Compaction](#wal-compaction)
   - [DoLake Compaction](#dolake-compaction)
   - [Storage Cleanup](#storage-cleanup)

---

## Deployment

### Wrangler Configuration

DoSQL and DoLake are deployed as Cloudflare Workers with Durable Objects. The primary configuration file is `wrangler.toml` (or `wrangler.jsonc` for JSON format).

#### Basic wrangler.toml

```toml
name = "dosql-production"
main = "src/worker.ts"
compatibility_date = "2024-12-01"
compatibility_flags = ["nodejs_compat"]

# Worker limits
limits = { cpu_ms = 30000 }

# Durable Object bindings
[durable_objects]
bindings = [
  { name = "DOSQL", class_name = "DoSQL" },
  { name = "DOLAKE", class_name = "DoLake" }
]

# SQLite-enabled Durable Objects (required for DoSQL)
[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQL"]

[[migrations]]
tag = "v2"
new_classes = ["DoLake"]

# R2 bucket for lakehouse storage
[[r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "dosql-lakehouse-prod"

# Optional: Analytics Engine for metrics
[[analytics_engine_datasets]]
binding = "METRICS"
dataset = "dosql_metrics"
```

#### Equivalent wrangler.jsonc

```jsonc
{
  "name": "dosql-production",
  "main": "src/worker.ts",
  "compatibility_date": "2024-12-01",
  "compatibility_flags": ["nodejs_compat"],
  "limits": {
    "cpu_ms": 30000
  },
  "durable_objects": {
    "bindings": [
      { "name": "DOSQL", "class_name": "DoSQL" },
      { "name": "DOLAKE", "class_name": "DoLake" }
    ]
  },
  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["DoSQL"] },
    { "tag": "v2", "new_classes": ["DoLake"] }
  ],
  "r2_buckets": [
    { "binding": "LAKEHOUSE_BUCKET", "bucket_name": "dosql-lakehouse-prod" }
  ],
  "analytics_engine_datasets": [
    { "binding": "METRICS", "dataset": "dosql_metrics" }
  ]
}
```

### Environment Variables

Configure environment-specific settings using Wrangler secrets and variables.

#### Setting Secrets

```bash
# API keys and sensitive configuration
wrangler secret put ENCRYPTION_KEY
wrangler secret put API_SECRET_KEY
wrangler secret put DOLAKE_AUTH_TOKEN
```

#### Environment Variables in wrangler.toml

```toml
[vars]
# General settings
LOG_LEVEL = "info"
ENVIRONMENT = "production"

# DoSQL settings
DOSQL_MAX_CONNECTIONS = "100"
DOSQL_QUERY_TIMEOUT_MS = "30000"
DOSQL_WAL_SEGMENT_SIZE = "10485760"  # 10MB

# DoLake settings
DOLAKE_FLUSH_INTERVAL_MS = "60000"    # 1 minute
DOLAKE_MAX_BUFFER_SIZE = "67108864"   # 64MB
DOLAKE_EVENT_THRESHOLD = "10000"
DOLAKE_PARQUET_ROW_GROUP_SIZE = "100000"

# CDC settings
CDC_BATCH_SIZE = "100"
CDC_BATCH_TIMEOUT_MS = "5000"
CDC_MAX_RETRY_COUNT = "5"
CDC_RETRY_DELAY_MS = "1000"

# Sharding settings
SHARD_COUNT = "16"
SHARD_TIMEOUT_MS = "10000"
MAX_PARALLEL_SHARDS = "8"
```

### Multi-Environment Setup

Manage multiple environments (dev, staging, production) using Wrangler's environment feature.

#### wrangler.toml with Environments

```toml
# Base configuration (shared)
name = "dosql"
main = "src/worker.ts"
compatibility_date = "2024-12-01"
compatibility_flags = ["nodejs_compat"]

[durable_objects]
bindings = [
  { name = "DOSQL", class_name = "DoSQL" },
  { name = "DOLAKE", class_name = "DoLake" }
]

[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQL"]

[[migrations]]
tag = "v2"
new_classes = ["DoLake"]

# =============================================================================
# Development Environment
# =============================================================================
[env.dev]
name = "dosql-dev"
route = "dev-db.example.com/*"

[env.dev.vars]
LOG_LEVEL = "debug"
ENVIRONMENT = "development"
DOSQL_WAL_SEGMENT_SIZE = "1048576"  # 1MB (smaller for dev)
DOLAKE_FLUSH_INTERVAL_MS = "10000"  # 10 seconds
DOLAKE_EVENT_THRESHOLD = "100"

[[env.dev.r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "dosql-lakehouse-dev"

# =============================================================================
# Staging Environment
# =============================================================================
[env.staging]
name = "dosql-staging"
route = "staging-db.example.com/*"

[env.staging.vars]
LOG_LEVEL = "info"
ENVIRONMENT = "staging"
DOSQL_WAL_SEGMENT_SIZE = "5242880"  # 5MB
DOLAKE_FLUSH_INTERVAL_MS = "30000"  # 30 seconds
DOLAKE_EVENT_THRESHOLD = "1000"

[[env.staging.r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "dosql-lakehouse-staging"

# =============================================================================
# Production Environment
# =============================================================================
[env.production]
name = "dosql-production"
route = "db.example.com/*"
workers_dev = false

[env.production.vars]
LOG_LEVEL = "warn"
ENVIRONMENT = "production"
DOSQL_WAL_SEGMENT_SIZE = "10485760"  # 10MB
DOLAKE_FLUSH_INTERVAL_MS = "60000"   # 1 minute
DOLAKE_EVENT_THRESHOLD = "10000"

[[env.production.r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "dosql-lakehouse-prod"
```

#### Deployment Commands

```bash
# Deploy to development
wrangler deploy --env dev

# Deploy to staging
wrangler deploy --env staging

# Deploy to production (with confirmation)
wrangler deploy --env production

# Tail logs for specific environment
wrangler tail --env production

# View secrets for environment
wrangler secret list --env production
```

---

## Durable Object Configuration

### DO Bindings

Durable Objects are bound to your Worker in the wrangler configuration. Each DO class requires:

1. A binding name (used in code)
2. A class name (the exported class)

#### Worker Code Structure

```typescript
// src/worker.ts
import { DoSQL } from './dosql';
import { DoLake } from './dolake';

export interface Env {
  DOSQL: DurableObjectNamespace;
  DOLAKE: DurableObjectNamespace;
  LAKEHOUSE_BUCKET: R2Bucket;
  METRICS?: AnalyticsEngineDataset;

  // Environment variables
  LOG_LEVEL: string;
  ENVIRONMENT: string;
  DOSQL_WAL_SEGMENT_SIZE: string;
  DOLAKE_FLUSH_INTERVAL_MS: string;
  DOLAKE_EVENT_THRESHOLD: string;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    // Route to appropriate Durable Object
    if (url.pathname.startsWith('/sql')) {
      const shardId = extractShardId(request);
      const doId = env.DOSQL.idFromName(shardId);
      const stub = env.DOSQL.get(doId);
      return stub.fetch(request);
    }

    if (url.pathname.startsWith('/lake')) {
      const doId = env.DOLAKE.idFromName('default');
      const stub = env.DOLAKE.get(doId);
      return stub.fetch(request);
    }

    return new Response('Not Found', { status: 404 });
  },
};

// Export Durable Object classes
export { DoSQL, DoLake };
```

### Migration Configuration

Migrations are critical for Durable Objects, especially when using SQLite storage.

#### Migration Tags

```toml
# Initial deployment with SQLite-enabled DoSQL
[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQL"]

# Add DoLake (non-SQLite DO)
[[migrations]]
tag = "v2"
new_classes = ["DoLake"]

# Rename a class (if needed)
[[migrations]]
tag = "v3"
renamed_classes = [
  { from = "OldDoSQL", to = "DoSQL" }
]

# Delete a deprecated class (use with caution)
[[migrations]]
tag = "v4"
deleted_classes = ["DeprecatedDO"]
```

#### Migration Best Practices

1. **Never reuse migration tags** - Each tag must be unique
2. **Test migrations in staging first** - Always validate before production
3. **Backup data before migrations** - Especially for schema changes
4. **Use rolling deployments** - Minimize downtime during migrations

```bash
# Check current migrations status
wrangler durable-objects migrations list

# Apply migrations (happens automatically on deploy)
wrangler deploy
```

### SQLite Storage Settings

DoSQL uses Durable Object SQLite storage for persistence. Understanding the constraints is critical for operations.

#### Storage Limits

| Resource | Limit | Notes |
|----------|-------|-------|
| Total storage per DO | 10 GB | Charged at $0.20/GB-month |
| Key-value entry size | 128 KB | Use chunking for larger values |
| Blob size | 2 MB | Auto-chunked by DoSQL |
| Maximum keys | No limit | Performance degrades at ~100K keys |
| SQLite database size | 10 GB | Subject to total storage limit |

#### Storage Configuration in Code

```typescript
export class DoSQL implements DurableObject {
  private sql: SqlStorage;

  constructor(state: DurableObjectState, env: Env) {
    // Access SQLite storage
    this.sql = state.storage.sql;

    // Configure pragmas on initialization
    this.sql.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA cache_size = -64000;  -- 64MB cache
      PRAGMA temp_store = MEMORY;
      PRAGMA mmap_size = 268435456;  -- 256MB mmap
    `);
  }

  async initializeSchema(): Promise<void> {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS _meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS _wal_segments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lsn_start INTEGER NOT NULL,
        lsn_end INTEGER NOT NULL,
        data BLOB NOT NULL,
        created_at INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_wal_lsn ON _wal_segments(lsn_start, lsn_end);
    `);
  }
}
```

---

## R2 Configuration

### Bucket Setup for DoLake

R2 buckets store the lakehouse data in Iceberg format. Configure buckets for each environment.

#### Creating Buckets

```bash
# Create production bucket
wrangler r2 bucket create dosql-lakehouse-prod

# Create staging bucket
wrangler r2 bucket create dosql-lakehouse-staging

# Create development bucket
wrangler r2 bucket create dosql-lakehouse-dev

# List existing buckets
wrangler r2 bucket list
```

#### Directory Structure

DoLake organizes data following the Iceberg table format:

```
dosql-lakehouse-prod/
├── warehouse/                          # Base path
│   ├── default/                        # Namespace
│   │   ├── users/                      # Table
│   │   │   ├── metadata/
│   │   │   │   ├── version-hint.text   # Current version pointer
│   │   │   │   ├── v1.metadata.json    # Initial metadata
│   │   │   │   ├── v2.metadata.json    # After first snapshot
│   │   │   │   ├── snap-{id}-manifest-list.avro
│   │   │   │   └── {uuid}-manifest.avro
│   │   │   └── data/
│   │   │       ├── dt=2026-01-20/
│   │   │       │   ├── {uuid}.parquet
│   │   │       │   └── {uuid}.parquet
│   │   │       └── dt=2026-01-21/
│   │   │           └── {uuid}.parquet
│   │   └── orders/                     # Another table
│   │       ├── metadata/
│   │       └── data/
│   └── analytics/                      # Another namespace
│       └── events/
│           ├── metadata/
│           └── data/
├── _archive/                           # Archived WAL segments
│   └── shard-001/
│       └── wal-segment-0001.bin
└── _backups/                           # Point-in-time backups
    └── 2026-01-21T00:00:00Z/
        └── snapshot.json
```

### Lifecycle Policies

Configure R2 lifecycle policies to manage storage costs and compliance.

#### Setting Lifecycle Rules via API

```typescript
// lifecycle-config.ts
import { S3Client, PutBucketLifecycleConfigurationCommand } from '@aws-sdk/client-s3';

const s3 = new S3Client({
  region: 'auto',
  endpoint: `https://${ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
});

const lifecycleConfig = {
  Rules: [
    {
      ID: 'archive-old-parquet',
      Status: 'Enabled',
      Filter: {
        Prefix: 'warehouse/',
      },
      Transitions: [
        {
          Days: 90,
          StorageClass: 'INFREQUENT_ACCESS',  // R2 IA when available
        },
      ],
    },
    {
      ID: 'delete-old-wal-archives',
      Status: 'Enabled',
      Filter: {
        Prefix: '_archive/',
      },
      Expiration: {
        Days: 30,  // Delete archived WAL after 30 days
      },
    },
    {
      ID: 'cleanup-incomplete-uploads',
      Status: 'Enabled',
      Filter: {
        Prefix: '',
      },
      AbortIncompleteMultipartUpload: {
        DaysAfterInitiation: 7,
      },
    },
  ],
};

await s3.send(new PutBucketLifecycleConfigurationCommand({
  Bucket: 'dosql-lakehouse-prod',
  LifecycleConfiguration: lifecycleConfig,
}));
```

#### Recommended Lifecycle Policies

| Rule | Prefix | Action | Days | Rationale |
|------|--------|--------|------|-----------|
| Archive old data | `warehouse/` | Transition to IA | 90 | Reduce costs for historical data |
| Delete old WAL | `_archive/` | Expire | 30 | WAL only needed for recovery |
| Cleanup backups | `_backups/` | Expire | 365 | Keep 1 year of backups |
| Abort uploads | (all) | Abort incomplete | 7 | Cleanup failed multipart uploads |

### Cross-Region Considerations

R2 automatically replicates data globally, but there are considerations for multi-region deployments.

#### Data Locality

```typescript
// Configure R2 with location hints
const bucket = env.LAKEHOUSE_BUCKET;

// Write with location hint (if supported)
await bucket.put('warehouse/default/users/data/file.parquet', data, {
  customMetadata: {
    'cf-location-hint': 'wnam',  // Western North America
  },
});
```

#### Multi-Region Architecture

```
                    ┌─────────────────────────────────────────────────┐
                    │              Cloudflare Global Network           │
                    └─────────────────────────────────────────────────┘
                                          │
          ┌───────────────────────────────┼───────────────────────────┐
          │                               │                           │
          ▼                               ▼                           ▼
   ┌─────────────┐                 ┌─────────────┐             ┌─────────────┐
   │  US Region  │                 │  EU Region  │             │ APAC Region │
   │   Workers   │                 │   Workers   │             │   Workers   │
   └──────┬──────┘                 └──────┬──────┘             └──────┬──────┘
          │                               │                           │
          ▼                               ▼                           ▼
   ┌─────────────┐                 ┌─────────────┐             ┌─────────────┐
   │ DoSQL DO    │                 │ DoSQL DO    │             │ DoSQL DO    │
   │ (Primary)   │───── CDC ──────▶│ (Replica)   │───── CDC ──▶│ (Replica)   │
   └──────┬──────┘                 └──────┬──────┘             └──────┬──────┘
          │                               │                           │
          └───────────────────────────────┼───────────────────────────┘
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │      R2 Bucket        │
                              │ (Global Replication)  │
                              └───────────────────────┘
```

---

## Monitoring

### Key Metrics to Track

Monitor these critical metrics for healthy DoSQL/DoLake operations.

#### DoSQL Metrics

| Metric | Description | Warning Threshold | Critical Threshold |
|--------|-------------|-------------------|-------------------|
| `dosql_query_duration_ms` | Query execution time | > 100ms | > 1000ms |
| `dosql_active_transactions` | Concurrent transactions | > 50 | > 100 |
| `dosql_wal_segment_count` | Active WAL segments | > 100 | > 500 |
| `dosql_storage_used_bytes` | DO storage consumption | > 5GB | > 8GB |
| `dosql_connection_count` | Active WebSocket connections | > 500 | > 900 |
| `dosql_error_rate` | Errors per minute | > 10 | > 100 |

#### DoLake Metrics

| Metric | Description | Warning Threshold | Critical Threshold |
|--------|-------------|-------------------|-------------------|
| `dolake_buffer_size_bytes` | CDC buffer size | > 32MB | > 56MB |
| `dolake_buffer_event_count` | Events in buffer | > 5000 | > 9000 |
| `dolake_flush_duration_ms` | Time to flush to R2 | > 5000ms | > 30000ms |
| `dolake_parquet_file_size` | Average Parquet file size | < 10MB | < 1MB |
| `dolake_connected_sources` | DoSQL instances connected | varies | 0 |
| `dolake_snapshot_lag_ms` | Time since last snapshot | > 300000ms | > 600000ms |

#### Sharding Metrics

| Metric | Description | Warning Threshold | Critical Threshold |
|--------|-------------|-------------------|-------------------|
| `shard_query_scatter_ratio` | % of scatter queries | > 30% | > 50% |
| `shard_imbalance_ratio` | Data distribution skew | > 1.5 | > 2.0 |
| `shard_latency_p99_ms` | Cross-shard query latency | > 200ms | > 500ms |
| `shard_health_score` | Aggregate shard health | < 0.9 | < 0.7 |

### Cloudflare Analytics Integration

Use Analytics Engine to track custom metrics.

#### Metric Emission

```typescript
// src/metrics.ts
export function emitMetric(
  env: Env,
  name: string,
  value: number,
  dimensions: Record<string, string> = {}
): void {
  if (!env.METRICS) return;

  env.METRICS.writeDataPoint({
    blobs: [name],
    doubles: [value],
    indexes: [
      env.ENVIRONMENT,
      dimensions.shard_id ?? 'unknown',
      dimensions.operation ?? 'unknown',
    ],
  });
}

// Usage in DoSQL
export class DoSQL {
  async query(sql: string, params: unknown[]): Promise<QueryResult> {
    const start = Date.now();

    try {
      const result = await this.executeQuery(sql, params);

      emitMetric(this.env, 'dosql_query_duration_ms', Date.now() - start, {
        shard_id: this.shardId,
        operation: 'query',
      });

      return result;
    } catch (error) {
      emitMetric(this.env, 'dosql_error_count', 1, {
        shard_id: this.shardId,
        operation: 'query',
      });
      throw error;
    }
  }
}
```

#### Querying Metrics with GraphQL

```graphql
# Query DoSQL metrics
query DoSQLMetrics($accountTag: string!, $since: Time!, $until: Time!) {
  viewer {
    accounts(filter: { accountTag: $accountTag }) {
      dosqlMetrics: analyticsEngineAdaptiveGroups(
        filter: {
          datetime_geq: $since
          datetime_lt: $until
        }
        limit: 1000
      ) {
        sum { count }
        avg { double1 }
        quantiles {
          double1P50
          double1P90
          double1P99
        }
        dimensions {
          metric: blob1
          environment: index1
          shardId: index2
        }
      }
    }
  }
}
```

### Custom Logging Patterns

Implement structured logging for operational visibility.

#### Logger Configuration

```typescript
// src/logger.ts
export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
}

export interface LogContext {
  requestId?: string;
  shardId?: string;
  operation?: string;
  userId?: string;
  [key: string]: unknown;
}

export class Logger {
  private level: LogLevel;
  private context: LogContext;

  constructor(env: Env, context: LogContext = {}) {
    this.level = LogLevel[env.LOG_LEVEL?.toUpperCase() as keyof typeof LogLevel] ?? LogLevel.INFO;
    this.context = context;
  }

  private log(level: LogLevel, message: string, data?: Record<string, unknown>): void {
    if (level < this.level) return;

    const entry = {
      timestamp: new Date().toISOString(),
      level: LogLevel[level],
      message,
      ...this.context,
      ...data,
    };

    console.log(JSON.stringify(entry));
  }

  debug(message: string, data?: Record<string, unknown>): void {
    this.log(LogLevel.DEBUG, message, data);
  }

  info(message: string, data?: Record<string, unknown>): void {
    this.log(LogLevel.INFO, message, data);
  }

  warn(message: string, data?: Record<string, unknown>): void {
    this.log(LogLevel.WARN, message, data);
  }

  error(message: string, error?: Error, data?: Record<string, unknown>): void {
    this.log(LogLevel.ERROR, message, {
      ...data,
      error: error ? {
        name: error.name,
        message: error.message,
        stack: error.stack,
      } : undefined,
    });
  }

  child(context: LogContext): Logger {
    const logger = new Logger({ LOG_LEVEL: LogLevel[this.level] } as Env);
    logger.context = { ...this.context, ...context };
    return logger;
  }
}
```

#### Usage Examples

```typescript
export class DoSQL implements DurableObject {
  private logger: Logger;

  constructor(state: DurableObjectState, env: Env) {
    this.logger = new Logger(env, {
      shardId: state.id.toString(),
      component: 'DoSQL',
    });
  }

  async fetch(request: Request): Promise<Response> {
    const requestId = crypto.randomUUID();
    const log = this.logger.child({ requestId });

    log.info('Request received', {
      method: request.method,
      url: request.url,
    });

    try {
      const result = await this.handleRequest(request);
      log.info('Request completed', { status: 200 });
      return result;
    } catch (error) {
      log.error('Request failed', error as Error);
      return new Response('Internal Error', { status: 500 });
    }
  }
}
```

---

## Scaling

### Sharding Configuration

DoSQL supports horizontal scaling through Vitess-inspired sharding.

#### VSchema Definition

```typescript
// src/vschema.ts
import {
  createVSchema,
  shardedTable,
  unshardedTable,
  referenceTable,
  hashVindex,
  consistentHashVindex,
  rangeVindex,
  shard,
  createShardId,
} from '@dotdo/dosql/sharding';

export const vschema = createVSchema(
  {
    // Sharded by tenant_id for multi-tenancy
    users: shardedTable('tenant_id', hashVindex()),
    orders: shardedTable('tenant_id', hashVindex()),
    sessions: shardedTable('tenant_id', consistentHashVindex()),

    // Time-series data with range sharding
    events: shardedTable('event_date', rangeVindex([
      { shard: createShardId('events-2025'), min: '2025-01-01', max: '2026-01-01' },
      { shard: createShardId('events-2026'), min: '2026-01-01', max: '2027-01-01' },
    ])),

    // Small lookup tables replicated everywhere
    countries: referenceTable(),
    currencies: referenceTable(),

    // Configuration in single shard
    system_config: unshardedTable(createShardId('shard-0')),
  },
  [
    shard(createShardId('shard-0'), 'DOSQL', { metadata: { region: 'us-west' } }),
    shard(createShardId('shard-1'), 'DOSQL', { metadata: { region: 'us-west' } }),
    shard(createShardId('shard-2'), 'DOSQL', { metadata: { region: 'us-east' } }),
    shard(createShardId('shard-3'), 'DOSQL', { metadata: { region: 'us-east' } }),
    shard(createShardId('shard-4'), 'DOSQL', { metadata: { region: 'eu-west' } }),
    shard(createShardId('shard-5'), 'DOSQL', { metadata: { region: 'eu-west' } }),
    shard(createShardId('shard-6'), 'DOSQL', { metadata: { region: 'apac' } }),
    shard(createShardId('shard-7'), 'DOSQL', { metadata: { region: 'apac' } }),
  ],
  {
    defaultShard: createShardId('shard-0'),
    settings: {
      maxParallelShards: 8,
      shardTimeoutMs: 10000,
      enableCaching: true,
    },
  }
);
```

### When to Add Shards

Monitor these indicators to determine when to scale out.

#### Scaling Triggers

| Indicator | Threshold | Action |
|-----------|-----------|--------|
| DO storage usage | > 7 GB per shard | Add shards, redistribute |
| Query latency P99 | > 500ms sustained | Add shards or optimize queries |
| CPU utilization | > 80% of limit | Add shards for workload distribution |
| Scatter query ratio | > 50% of queries | Review shard key selection |
| Write throughput | > 1000 writes/sec/shard | Add shards |

#### Adding a New Shard

```typescript
// 1. Update VSchema with new shard
const newShards = [
  ...vschema.shards,
  shard(createShardId('shard-8'), 'DOSQL', { metadata: { region: 'us-west' } }),
];

// 2. Deploy updated configuration
// (Deploy via wrangler)

// 3. Trigger rebalancing for consistent-hash sharded tables
await rebalanceShards({
  tables: ['sessions'],
  fromShards: vschema.shards.map(s => s.id),
  toShards: newShards.map(s => s.id),
  batchSize: 1000,
  throttleMs: 100,
});
```

#### Rebalancing Strategy

```typescript
// src/rebalance.ts
export async function rebalanceShards(config: RebalanceConfig): Promise<RebalanceResult> {
  const { tables, fromShards, toShards, batchSize, throttleMs } = config;

  const results: RebalanceResult = {
    movedRows: 0,
    duration: 0,
    errors: [],
  };

  const start = Date.now();

  for (const table of tables) {
    console.log(`Rebalancing table: ${table}`);

    // Get rows that need to move
    for (const fromShard of fromShards) {
      const rows = await getRowsToMove(fromShard, table, toShards, batchSize);

      while (rows.length > 0) {
        // Move batch to new shard
        const batch = rows.splice(0, batchSize);

        for (const row of batch) {
          const targetShard = calculateTargetShard(row, toShards);

          if (targetShard !== fromShard) {
            try {
              await moveRow(fromShard, targetShard, table, row);
              results.movedRows++;
            } catch (error) {
              results.errors.push({ table, row, error: error.message });
            }
          }
        }

        // Throttle to avoid overwhelming the system
        await new Promise(resolve => setTimeout(resolve, throttleMs));
      }
    }
  }

  results.duration = Date.now() - start;
  return results;
}
```

### Load Balancing Strategies

DoSQL implements intelligent query routing for optimal performance.

#### Read Preference Configuration

```typescript
// Configure read preference per query
const users = await executor.query(
  'SELECT * FROM users WHERE tenant_id = ?',
  [tenantId],
  {
    readPreference: 'replicaPreferred',  // Prefer replicas for reads
    maxStalenessMs: 5000,                // Allow 5 seconds staleness
  }
);

// For analytics queries, use dedicated analytics replicas
const report = await executor.query(
  'SELECT COUNT(*), SUM(amount) FROM orders WHERE tenant_id = ?',
  [tenantId],
  {
    readPreference: 'analytics',  // Route to analytics replica
    timeout: 60000,               // Allow longer timeout
  }
);
```

#### Replica Selection Algorithm

```
                    ┌─────────────────────────────────────────┐
                    │           Query Router                   │
                    └─────────────────────┬───────────────────┘
                                          │
                              ┌───────────┴───────────┐
                              │   Read Preference?    │
                              └───────────┬───────────┘
                                          │
          ┌───────────────────────────────┼───────────────────────────┐
          │                               │                           │
          ▼                               ▼                           ▼
   ┌─────────────┐                 ┌─────────────┐             ┌─────────────┐
   │   primary   │                 │   replica   │             │  analytics  │
   │             │                 │             │             │             │
   │ Route to    │                 │ Select from │             │ Route to    │
   │ primary DO  │                 │ healthy     │             │ analytics   │
   │             │                 │ replicas    │             │ replica     │
   └─────────────┘                 └──────┬──────┘             └─────────────┘
                                          │
                              ┌───────────┴───────────┐
                              │  Selection Strategy   │
                              └───────────┬───────────┘
                                          │
          ┌───────────────────────────────┼───────────────────────────┐
          │                               │                           │
          ▼                               ▼                           ▼
   ┌─────────────┐                 ┌─────────────┐             ┌─────────────┐
   │   nearest   │                 │   weighted  │             │ round-robin │
   │             │                 │             │             │             │
   │ Lowest      │                 │ Based on    │             │ Distribute  │
   │ latency     │                 │ capacity    │             │ evenly      │
   └─────────────┘                 └─────────────┘             └─────────────┘
```

---

## Backup & Recovery

### Time Travel for Point-in-Time Recovery

DoSQL's WAL enables querying data at any historical point.

#### Time Travel Query Syntax

```sql
-- Query users table as of specific timestamp
SELECT * FROM users
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-20 14:30:00';

-- Query at specific LSN (Log Sequence Number)
SELECT * FROM users
FOR SYSTEM_TIME AS OF LSN 12345678;

-- Query at specific snapshot
SELECT * FROM users
FOR SYSTEM_TIME AS OF SNAPSHOT 'main@42';

-- Compare data between two points
SELECT
  current.id,
  current.name AS current_name,
  historical.name AS old_name
FROM users AS current
INNER JOIN (
  SELECT * FROM users
  FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-15'
) AS historical ON current.id = historical.id
WHERE current.name != historical.name;
```

#### Programmatic Point-in-Time Recovery

```typescript
// src/recovery.ts
import { createTimeTravelSession, timestamp, lsn } from '@dotdo/dosql/timetravel';

export async function recoverToPointInTime(
  db: Database,
  targetTime: Date,
  tables: string[]
): Promise<RecoveryResult> {
  const session = await createTimeTravelSession(db, {
    asOf: timestamp(targetTime.toISOString()),
    scope: 'global',
  });

  try {
    // Create recovery snapshots
    for (const table of tables) {
      // Read historical data
      const historicalData = await session.query(
        `SELECT * FROM ${table}`
      );

      // Write to recovery table
      await db.run(`CREATE TABLE IF NOT EXISTS ${table}_recovered AS
        SELECT * FROM ${table} WHERE 1=0`);

      for (const row of historicalData) {
        await db.run(
          `INSERT INTO ${table}_recovered VALUES (${Object.values(row).map(() => '?').join(',')})`,
          Object.values(row)
        );
      }
    }

    return { success: true, tables, targetTime };
  } finally {
    await session.close();
  }
}
```

### R2 Backup Strategies

Implement automated backups to R2 for disaster recovery.

#### Backup Configuration

```typescript
// src/backup.ts
export interface BackupConfig {
  /** Backup frequency in hours */
  intervalHours: number;
  /** Number of backups to retain */
  retentionCount: number;
  /** Tables to backup (empty = all) */
  tables?: string[];
  /** Include WAL segments */
  includeWal: boolean;
  /** Compression algorithm */
  compression: 'gzip' | 'zstd' | 'none';
}

export const defaultBackupConfig: BackupConfig = {
  intervalHours: 24,
  retentionCount: 30,
  includeWal: true,
  compression: 'gzip',
};
```

#### Automated Backup Process

```typescript
// src/backup-worker.ts
export class BackupScheduler {
  private config: BackupConfig;
  private bucket: R2Bucket;

  constructor(bucket: R2Bucket, config: BackupConfig = defaultBackupConfig) {
    this.bucket = bucket;
    this.config = config;
  }

  async createBackup(db: Database, shardId: string): Promise<BackupManifest> {
    const timestamp = new Date().toISOString();
    const backupPath = `_backups/${timestamp}/${shardId}`;

    // Export current snapshot
    const snapshot = await db.createSnapshot();

    // Backup metadata
    const manifest: BackupManifest = {
      timestamp,
      shardId,
      snapshotId: snapshot.id,
      lsn: snapshot.lsn,
      tables: [],
      walSegments: [],
      totalSizeBytes: 0,
    };

    // Backup each table
    const tables = this.config.tables ?? await this.listTables(db);

    for (const table of tables) {
      const data = await db.query(`SELECT * FROM ${table}`);
      const json = JSON.stringify(data);
      const compressed = await this.compress(json);

      const key = `${backupPath}/tables/${table}.json.gz`;
      await this.bucket.put(key, compressed);

      manifest.tables.push({
        name: table,
        rowCount: data.length,
        sizeBytes: compressed.byteLength,
        path: key,
      });
      manifest.totalSizeBytes += compressed.byteLength;
    }

    // Backup WAL segments if configured
    if (this.config.includeWal) {
      const walSegments = await this.backupWalSegments(db, backupPath);
      manifest.walSegments = walSegments;
    }

    // Write manifest
    await this.bucket.put(
      `${backupPath}/manifest.json`,
      JSON.stringify(manifest, null, 2)
    );

    // Cleanup old backups
    await this.cleanupOldBackups(shardId);

    return manifest;
  }

  async restoreBackup(
    db: Database,
    manifestPath: string,
    options: RestoreOptions = {}
  ): Promise<RestoreResult> {
    // Read manifest
    const manifestObj = await this.bucket.get(manifestPath);
    if (!manifestObj) {
      throw new Error(`Backup manifest not found: ${manifestPath}`);
    }

    const manifest: BackupManifest = JSON.parse(await manifestObj.text());

    // Begin restore transaction
    await db.run('BEGIN EXCLUSIVE');

    try {
      // Restore tables
      for (const table of manifest.tables) {
        if (options.tables && !options.tables.includes(table.name)) {
          continue;
        }

        const dataObj = await this.bucket.get(table.path);
        if (!dataObj) {
          throw new Error(`Backup data not found: ${table.path}`);
        }

        const compressed = await dataObj.arrayBuffer();
        const json = await this.decompress(new Uint8Array(compressed));
        const rows = JSON.parse(json);

        // Drop and recreate table (or truncate based on options)
        if (options.dropExisting) {
          await db.run(`DROP TABLE IF EXISTS ${table.name}`);
          await db.run(`CREATE TABLE ${table.name} AS SELECT * FROM (${
            rows.length > 0 ? `VALUES ${rows.map(() => '(?)').join(',')}` : 'SELECT 1 WHERE 0'
          })`);
        } else {
          await db.run(`DELETE FROM ${table.name}`);
        }

        // Insert restored data
        for (const row of rows) {
          const columns = Object.keys(row);
          const values = Object.values(row);
          await db.run(
            `INSERT INTO ${table.name} (${columns.join(',')}) VALUES (${values.map(() => '?').join(',')})`,
            values
          );
        }
      }

      await db.run('COMMIT');

      return {
        success: true,
        manifest,
        tablesRestored: manifest.tables.length,
      };
    } catch (error) {
      await db.run('ROLLBACK');
      throw error;
    }
  }

  private async cleanupOldBackups(shardId: string): Promise<void> {
    const prefix = `_backups/`;
    const list = await this.bucket.list({ prefix });

    // Group by timestamp
    const backups = list.objects
      .filter(obj => obj.key.includes(`/${shardId}/manifest.json`))
      .sort((a, b) => b.uploaded.getTime() - a.uploaded.getTime());

    // Delete old backups beyond retention count
    const toDelete = backups.slice(this.config.retentionCount);

    for (const backup of toDelete) {
      const backupPrefix = backup.key.replace('/manifest.json', '/');
      const backupObjects = await this.bucket.list({ prefix: backupPrefix });

      for (const obj of backupObjects.objects) {
        await this.bucket.delete(obj.key);
      }
    }
  }
}
```

### Disaster Recovery Procedures

Follow these procedures for different disaster scenarios.

#### Runbook: Single Shard Failure

```markdown
## Single Shard Failure Recovery

### Symptoms
- Queries to specific shard returning errors
- DO not responding or timing out
- Storage corruption detected

### Recovery Steps

1. **Identify affected shard**
   ```bash
   # Check shard health via API
   curl https://db.example.com/admin/shards/health
   ```

2. **Isolate the shard**
   ```typescript
   // Mark shard as read-only in VSchema
   await markShardReadOnly(shardId);
   ```

3. **Find latest backup**
   ```bash
   # List backups for shard
   wrangler r2 object list dosql-lakehouse-prod --prefix="_backups/" | grep shard-X
   ```

4. **Create new DO instance**
   ```typescript
   // New DO will be created automatically with fresh ID
   const newDoId = env.DOSQL.newUniqueId();
   const newStub = env.DOSQL.get(newDoId);
   ```

5. **Restore from backup**
   ```typescript
   await newStub.fetch('https://internal/restore', {
     method: 'POST',
     body: JSON.stringify({
       backupPath: '_backups/2026-01-21T00:00:00Z/shard-X/manifest.json',
       dropExisting: true,
     }),
   });
   ```

6. **Update shard mapping**
   ```typescript
   // Update VSchema to point to new DO
   await updateShardMapping(shardId, newDoId.toString());
   ```

7. **Verify data integrity**
   ```bash
   # Run consistency checks
   curl https://db.example.com/admin/shards/shard-X/verify
   ```

8. **Re-enable shard**
   ```typescript
   await markShardWritable(shardId);
   ```

### Estimated Recovery Time
- Small shard (< 1GB): 5-10 minutes
- Medium shard (1-5GB): 15-30 minutes
- Large shard (> 5GB): 30-60 minutes
```

#### Runbook: Complete Region Failure

```markdown
## Complete Region Failure Recovery

### Prerequisites
- Multi-region deployment configured
- Cross-region replication enabled
- Backup data available in R2 (globally replicated)

### Recovery Steps

1. **Assess impact**
   - Identify affected shards
   - Verify backup availability
   - Check CDC lag to DoLake

2. **Failover to healthy region**
   ```typescript
   // Update DNS/routing to healthy region
   await updateRegionRouting({
     from: 'us-west',
     to: 'us-east',
     shards: affectedShards,
   });
   ```

3. **Promote replicas to primary**
   ```typescript
   for (const shard of affectedShards) {
     const replica = getHealthyReplica(shard, 'us-east');
     await promoteReplicaToPrimary(replica);
   }
   ```

4. **Catch up from DoLake (if needed)**
   ```typescript
   // Replay CDC events from last checkpoint
   await replayCDCFromLakehouse({
     shards: affectedShards,
     fromSnapshot: lastKnownGoodSnapshot,
   });
   ```

5. **Verify data consistency**
   ```bash
   # Compare row counts and checksums
   curl https://db.example.com/admin/verify-consistency
   ```

6. **Update monitoring**
   - Clear alerts for failed region
   - Update dashboards
   - Notify stakeholders

### Post-Recovery
- Document incident
- Review backup/replication strategy
- Consider adding more redundancy
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue: Query Timeout

**Symptoms:**
- Queries taking longer than configured timeout
- `TimeoutError` in logs

**Diagnosis:**
```typescript
// Enable query profiling
const result = await db.query(sql, params, {
  profile: true,
  explain: true,
});

console.log('Query plan:', result.queryPlan);
console.log('Execution stats:', result.stats);
```

**Solutions:**
1. Add missing indexes
2. Reduce result set size with LIMIT
3. Optimize WHERE clause
4. Consider sharding for large tables
5. Increase timeout for complex queries

---

#### Issue: WAL Segment Buildup

**Symptoms:**
- `dosql_wal_segment_count` metric increasing
- Storage usage growing unexpectedly
- Checkpoint taking longer

**Diagnosis:**
```sql
-- Check WAL status
SELECT
  COUNT(*) as segment_count,
  MIN(lsn_start) as oldest_lsn,
  MAX(lsn_end) as newest_lsn,
  SUM(LENGTH(data)) as total_bytes
FROM _wal_segments;
```

**Solutions:**
1. Force checkpoint: `await db.checkpoint()`
2. Increase checkpoint frequency
3. Check for long-running transactions
4. Verify CDC consumer is caught up

---

#### Issue: DoLake Buffer Full

**Symptoms:**
- NACKs with `buffer_full` reason
- CDC events backing up in DoSQL
- Increased retry attempts

**Diagnosis:**
```bash
# Check DoLake status
curl https://lake.example.com/status
```

**Solutions:**
1. Trigger manual flush: `POST /flush`
2. Increase buffer size in configuration
3. Add more DoLake instances
4. Check R2 write performance
5. Reduce CDC batch size

---

#### Issue: Shard Imbalance

**Symptoms:**
- Some shards hot while others idle
- Uneven storage distribution
- Increased scatter query ratio

**Diagnosis:**
```typescript
// Get shard statistics
const stats = await getShardStats();
console.log('Distribution:', stats.rowDistribution);
console.log('Query distribution:', stats.queryDistribution);
```

**Solutions:**
1. Review shard key selection
2. Use consistent hash vindex for better distribution
3. Trigger manual rebalancing
4. Split hot shards

---

### Debug Logging

Enable detailed logging for troubleshooting.

#### Enable Debug Mode

```toml
# wrangler.toml
[env.debug]
[env.debug.vars]
LOG_LEVEL = "debug"
DEBUG_QUERIES = "true"
DEBUG_WAL = "true"
DEBUG_CDC = "true"
TRACE_REQUESTS = "true"
```

#### Debug Query Execution

```typescript
export class DoSQL {
  async query(sql: string, params: unknown[]): Promise<QueryResult> {
    const debug = this.env.DEBUG_QUERIES === 'true';

    if (debug) {
      console.log('[DEBUG] Query:', sql);
      console.log('[DEBUG] Params:', JSON.stringify(params));
      console.log('[DEBUG] Transaction:', this.currentTxn?.id);
    }

    const start = performance.now();
    const result = await this.executeQuery(sql, params);
    const duration = performance.now() - start;

    if (debug) {
      console.log('[DEBUG] Duration:', duration.toFixed(2), 'ms');
      console.log('[DEBUG] Rows:', result.rows.length);
    }

    return result;
  }
}
```

### Performance Diagnostics

#### Query Analysis

```sql
-- Explain query plan
EXPLAIN QUERY PLAN SELECT * FROM users WHERE tenant_id = 42;

-- Analyze table statistics
ANALYZE users;

-- Check index usage
SELECT * FROM sqlite_stat1 WHERE tbl = 'users';
```

#### Performance Profiling Endpoint

```typescript
// GET /admin/profile
async getPerformanceProfile(): Promise<PerformanceProfile> {
  return {
    // Query statistics
    queries: {
      total: this.stats.queryCount,
      avgDurationMs: this.stats.totalQueryTime / this.stats.queryCount,
      slowQueries: this.stats.slowQueries,
    },

    // Storage statistics
    storage: {
      totalBytes: await this.getStorageSize(),
      walBytes: await this.getWalSize(),
      indexBytes: await this.getIndexSize(),
    },

    // Transaction statistics
    transactions: {
      active: this.transactionManager.activeCount,
      committed: this.stats.committedTxns,
      rolledBack: this.stats.rolledBackTxns,
      avgDurationMs: this.stats.avgTxnDuration,
    },

    // CDC statistics
    cdc: {
      pendingEvents: this.cdcBuffer.length,
      lastFlushedLsn: this.lastFlushedLsn,
      connected: this.dolakeWs !== null,
    },
  };
}
```

---

## Maintenance

### WAL Compaction

Regular WAL compaction prevents unbounded storage growth.

#### Automatic Checkpointing

```typescript
// src/checkpoint.ts
export class CheckpointManager {
  private config: CheckpointConfig;
  private lastCheckpoint: number = 0;

  constructor(config: CheckpointConfig) {
    this.config = config;
  }

  shouldCheckpoint(stats: WalStats): boolean {
    const now = Date.now();

    // Time-based trigger
    if (now - this.lastCheckpoint > this.config.intervalMs) {
      return true;
    }

    // Size-based trigger
    if (stats.totalBytes > this.config.maxWalBytes) {
      return true;
    }

    // Entry-based trigger
    if (stats.entryCount > this.config.maxWalEntries) {
      return true;
    }

    return false;
  }

  async checkpoint(db: Database, wal: WalWriter): Promise<CheckpointResult> {
    const start = Date.now();

    // Write all pending WAL to SQLite
    await wal.flush();

    // Run SQLite checkpoint
    const result = db.exec('PRAGMA wal_checkpoint(TRUNCATE)');

    // Archive old segments
    const archivedSegments = await this.archiveOldSegments(wal);

    // Update checkpoint marker
    this.lastCheckpoint = Date.now();

    return {
      duration: Date.now() - start,
      archivedSegments: archivedSegments.length,
      freedBytes: archivedSegments.reduce((sum, s) => sum + s.sizeBytes, 0),
    };
  }

  private async archiveOldSegments(wal: WalWriter): Promise<WalSegment[]> {
    const segments = await wal.getSegments();
    const toArchive = segments.filter(
      s => s.lsnEnd < wal.checkpointLsn
    );

    for (const segment of toArchive) {
      // Upload to R2
      await this.bucket.put(
        `_archive/${this.shardId}/wal-segment-${segment.id}.bin`,
        segment.data
      );

      // Delete from DO storage
      await wal.deleteSegment(segment.id);
    }

    return toArchive;
  }
}
```

#### Manual Checkpoint Command

```bash
# Trigger checkpoint via API
curl -X POST https://db.example.com/admin/shards/shard-0/checkpoint

# Force aggressive checkpoint
curl -X POST https://db.example.com/admin/shards/shard-0/checkpoint?mode=full
```

### DoLake Compaction

Compact small Parquet files for optimal query performance.

#### Compaction Configuration

```typescript
// src/compaction-config.ts
export interface CompactionConfig {
  /** Minimum files before compaction triggers */
  minFilesThreshold: number;

  /** Target file size after compaction */
  targetFileSizeBytes: number;

  /** Maximum files to compact in one operation */
  maxFilesPerCompaction: number;

  /** Partition age before compaction (ms) */
  minPartitionAgeMs: number;

  /** Schedule compaction during low-traffic hours */
  preferredHours?: number[];
}

export const defaultCompactionConfig: CompactionConfig = {
  minFilesThreshold: 10,
  targetFileSizeBytes: 128 * 1024 * 1024,  // 128MB
  maxFilesPerCompaction: 50,
  minPartitionAgeMs: 24 * 60 * 60 * 1000,  // 24 hours
  preferredHours: [2, 3, 4, 5],  // 2-6 AM UTC
};
```

#### Compaction Process

```typescript
// src/compaction.ts
export class CompactionManager {
  async runCompaction(table: string, partition?: string): Promise<CompactionResult> {
    // Find partitions needing compaction
    const partitions = partition
      ? [partition]
      : await this.findPartitionsNeedingCompaction(table);

    const results: PartitionCompactionResult[] = [];

    for (const part of partitions) {
      // Get files in partition
      const files = await this.getPartitionFiles(table, part);

      if (files.length < this.config.minFilesThreshold) {
        continue;
      }

      // Read all data
      const allData: Record<string, unknown>[] = [];
      for (const file of files) {
        const data = await this.readParquetFile(file.path);
        allData.push(...data);
      }

      // Write compacted file
      const compactedPath = await this.writeCompactedFile(table, part, allData);

      // Update Iceberg manifest
      await this.updateManifest(table, {
        added: [compactedPath],
        removed: files.map(f => f.path),
      });

      // Delete old files
      for (const file of files) {
        await this.bucket.delete(file.path);
      }

      results.push({
        partition: part,
        filesCompacted: files.length,
        originalSizeBytes: files.reduce((sum, f) => sum + f.sizeBytes, 0),
        compactedSizeBytes: (await this.bucket.head(compactedPath))!.size,
      });
    }

    return {
      table,
      partitionsProcessed: results.length,
      results,
    };
  }
}
```

#### Schedule Compaction

```typescript
// Schedule daily compaction via Cron Trigger
export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    if (event.cron === '0 3 * * *') {  // 3 AM UTC daily
      const dolake = env.DOLAKE.get(env.DOLAKE.idFromName('default'));

      // Trigger compaction for all tables
      await dolake.fetch('https://internal/compact', {
        method: 'POST',
        body: JSON.stringify({
          tables: ['users', 'orders', 'events'],
          mode: 'auto',
        }),
      });
    }
  },
};
```

### Storage Cleanup

Regular cleanup of orphaned and temporary files.

#### Cleanup Tasks

```typescript
// src/cleanup.ts
export class StorageCleanup {
  async runCleanup(): Promise<CleanupResult> {
    const results: CleanupResult = {
      deletedFiles: 0,
      freedBytes: 0,
      errors: [],
    };

    // 1. Clean orphaned data files (not in any manifest)
    const orphanedFiles = await this.findOrphanedDataFiles();
    for (const file of orphanedFiles) {
      try {
        await this.bucket.delete(file.key);
        results.deletedFiles++;
        results.freedBytes += file.size;
      } catch (error) {
        results.errors.push({ file: file.key, error: error.message });
      }
    }

    // 2. Clean old metadata versions (keep last N)
    const oldMetadata = await this.findOldMetadataVersions(10);
    for (const meta of oldMetadata) {
      await this.bucket.delete(meta.key);
      results.deletedFiles++;
      results.freedBytes += meta.size;
    }

    // 3. Clean incomplete multipart uploads
    const incompleteUploads = await this.listIncompleteUploads();
    for (const upload of incompleteUploads) {
      if (Date.now() - upload.initiated.getTime() > 7 * 24 * 60 * 60 * 1000) {
        await this.abortMultipartUpload(upload);
        results.deletedFiles++;
      }
    }

    // 4. Clean expired CDC fallback storage
    const expiredFallback = await this.findExpiredFallbackEvents(7);
    for (const event of expiredFallback) {
      await this.storage.delete(event.key);
      results.deletedFiles++;
    }

    return results;
  }

  private async findOrphanedDataFiles(): Promise<R2Object[]> {
    // Get all data files
    const dataFiles = await this.bucket.list({ prefix: 'warehouse/' });

    // Get all files referenced in manifests
    const referencedFiles = new Set<string>();
    const manifests = await this.bucket.list({ prefix: 'warehouse/', suffix: '-manifest.avro' });

    for (const manifest of manifests.objects) {
      const content = await this.bucket.get(manifest.key);
      const files = await this.parseManifestFiles(content);
      files.forEach(f => referencedFiles.add(f));
    }

    // Return unreferenced data files
    return dataFiles.objects.filter(
      obj => obj.key.endsWith('.parquet') && !referencedFiles.has(obj.key)
    );
  }
}
```

#### Cleanup Schedule

```toml
# wrangler.toml
[triggers]
crons = [
  "0 4 * * *",   # Daily cleanup at 4 AM UTC
  "0 3 * * 0"    # Weekly deep cleanup on Sunday 3 AM UTC
]
```

```typescript
// Handle cron triggers
export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const dolake = env.DOLAKE.get(env.DOLAKE.idFromName('default'));

    switch (event.cron) {
      case '0 4 * * *':
        // Daily cleanup
        await dolake.fetch('https://internal/cleanup', {
          method: 'POST',
          body: JSON.stringify({ mode: 'standard' }),
        });
        break;

      case '0 3 * * 0':
        // Weekly deep cleanup
        await dolake.fetch('https://internal/cleanup', {
          method: 'POST',
          body: JSON.stringify({ mode: 'deep', includeOrphanScan: true }),
        });
        break;
    }
  },
};
```

---

## Appendix

### Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `info` | Logging level (debug, info, warn, error) |
| `DOSQL_WAL_SEGMENT_SIZE` | `10485760` | WAL segment size in bytes |
| `DOSQL_QUERY_TIMEOUT_MS` | `30000` | Query timeout in milliseconds |
| `DOLAKE_FLUSH_INTERVAL_MS` | `60000` | CDC flush interval |
| `DOLAKE_MAX_BUFFER_SIZE` | `67108864` | Maximum buffer size (64MB) |
| `DOLAKE_EVENT_THRESHOLD` | `10000` | Events before flush |
| `SHARD_COUNT` | `16` | Number of shards |
| `MAX_PARALLEL_SHARDS` | `8` | Max parallel shard requests |

### Health Check Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Basic health check |
| `/health/deep` | GET | Deep health check with dependencies |
| `/admin/shards/health` | GET | All shards health status |
| `/admin/metrics` | GET | Prometheus-format metrics |
| `/admin/status` | GET | Detailed system status |

### Useful Commands

```bash
# Deploy to production
wrangler deploy --env production

# Tail production logs
wrangler tail --env production

# Check DO storage usage
wrangler durable-objects storage list DOSQL

# View R2 bucket contents
wrangler r2 object list dosql-lakehouse-prod --prefix="warehouse/"

# Run manual backup
curl -X POST https://db.example.com/admin/backup

# Trigger compaction
curl -X POST https://db.example.com/admin/compact
```

---

*Last updated: 2026-01-22*
*Maintained by: Platform Team*
