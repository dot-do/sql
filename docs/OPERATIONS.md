# DoSQL & DoLake Deployment & Operations Guide

**Version**: 2.0.0
**Last Updated**: 2026-01-22
**Maintainer**: Platform Team

---

## Table of Contents

1. [Deployment Checklist](#deployment-checklist)
   - [Pre-Deployment Checklist](#pre-deployment-checklist)
   - [Deployment Steps](#deployment-steps)
   - [Post-Deployment Verification](#post-deployment-verification)
   - [Rollback Procedures](#rollback-procedures)
2. [Deployment](#deployment)
   - [Wrangler Configuration](#wrangler-configuration)
   - [Environment Variables](#environment-variables)
   - [Multi-Environment Setup](#multi-environment-setup)
3. [Durable Object Configuration](#durable-object-configuration)
   - [DO Bindings](#do-bindings)
   - [Migration Configuration](#migration-configuration)
   - [SQLite Storage Settings](#sqlite-storage-settings)
4. [R2 Configuration](#r2-configuration)
   - [Bucket Setup for DoLake](#bucket-setup-for-dolake)
   - [Lifecycle Policies](#lifecycle-policies)
   - [Cross-Region Considerations](#cross-region-considerations)
5. [Monitoring and Observability](#monitoring-and-observability)
   - [Key Metrics to Track](#key-metrics-to-track)
   - [Cloudflare Analytics Integration](#cloudflare-analytics-integration)
   - [Custom Logging Patterns](#custom-logging-patterns)
   - [Distributed Tracing](#distributed-tracing)
6. [Alerting Recommendations](#alerting-recommendations)
   - [Alert Tiers](#alert-tiers)
   - [Alert Configuration](#alert-configuration)
   - [On-Call Procedures](#on-call-procedures)
   - [Alert Suppression and Maintenance Mode](#alert-suppression-and-maintenance-mode)
7. [Scaling](#scaling)
   - [Sharding Configuration](#sharding-configuration)
   - [When to Add Shards](#when-to-add-shards)
   - [Load Balancing Strategies](#load-balancing-strategies)
8. [Backup & Recovery](#backup--recovery)
   - [Time Travel for Point-in-Time Recovery](#time-travel-for-point-in-time-recovery)
   - [R2 Backup Strategies](#r2-backup-strategies)
   - [Disaster Recovery Procedures](#disaster-recovery-procedures)
9. [Maintenance Windows](#maintenance-windows)
   - [Scheduled Maintenance](#scheduled-maintenance)
   - [Maintenance Procedures](#maintenance-procedures)
   - [Zero-Downtime Operations](#zero-downtime-operations)
10. [Incident Response](#incident-response)
    - [Incident Classification](#incident-classification)
    - [Response Procedures](#response-procedures)
    - [Communication Templates](#communication-templates)
    - [Post-Incident Review](#post-incident-review)
11. [Capacity Planning](#capacity-planning)
    - [Resource Limits](#resource-limits)
    - [Growth Forecasting](#growth-forecasting)
    - [Scaling Triggers](#scaling-triggers)
    - [Capacity Review Schedule](#capacity-review-schedule)
12. [Cost Optimization](#cost-optimization)
    - [Pricing Model](#pricing-model)
    - [Cost Reduction Strategies](#cost-reduction-strategies)
    - [Budget Monitoring](#budget-monitoring)
    - [TCO Analysis](#tco-analysis)
13. [Troubleshooting](#troubleshooting)
    - [Common Issues and Solutions](#common-issues-and-solutions)
    - [Debug Logging](#debug-logging)
    - [Performance Diagnostics](#performance-diagnostics)
14. [Maintenance](#maintenance)
    - [WAL Compaction](#wal-compaction)
    - [DoLake Compaction](#dolake-compaction)
    - [Storage Cleanup](#storage-cleanup)

---

## Deployment Checklist

### Pre-Deployment Checklist

Complete all items before initiating deployment to production.

#### Code and Build Verification

- [ ] **All tests pass** - Run `pnpm test` and verify 100% pass rate
- [ ] **TypeScript compiles** - Run `pnpm typecheck` with no errors
- [ ] **Build succeeds** - Run `pnpm build` with no errors
- [ ] **Bundle size checked** - Verify Worker bundle < 10MB (warn at 5MB)
- [ ] **No security vulnerabilities** - Run `pnpm audit` with no high/critical issues
- [ ] **Code review approved** - PR approved by at least one reviewer
- [ ] **CHANGELOG updated** - Document all user-facing changes

#### Configuration Verification

- [ ] **wrangler.toml valid** - Run `wrangler check` for configuration errors
- [ ] **Secrets configured** - All required secrets set via `wrangler secret list`
- [ ] **Environment variables set** - Verify all required vars in wrangler.toml
- [ ] **DO migrations defined** - All migration tags properly configured
- [ ] **R2 buckets exist** - Verify bucket names match configuration
- [ ] **Routes configured** - Verify route patterns are correct

#### Infrastructure Verification

- [ ] **R2 buckets accessible** - Test R2 read/write permissions
- [ ] **D1 bindings (if any)** - Verify D1 database accessibility
- [ ] **KV namespaces (if any)** - Verify KV read/write access
- [ ] **Analytics Engine enabled** - Verify dataset configured

#### Staging Verification

- [ ] **Staging deployment successful** - `wrangler deploy --env staging`
- [ ] **Smoke tests pass** - Run automated smoke test suite
- [ ] **Manual testing complete** - Critical paths verified manually
- [ ] **Performance baseline met** - Latency P95 within acceptable range
- [ ] **No regressions detected** - Compare metrics against previous release

### Deployment Steps

#### Standard Deployment Process

```bash
#!/bin/bash
# deployment.sh - Standard deployment script

set -euo pipefail

ENVIRONMENT="${1:-production}"
VERSION=$(git describe --tags --always)

echo "=== DoSQL/DoLake Deployment ==="
echo "Environment: $ENVIRONMENT"
echo "Version: $VERSION"
echo ""

# Step 1: Pre-flight checks
echo "[1/7] Running pre-flight checks..."
pnpm test
pnpm typecheck
pnpm build

# Step 2: Backup current state
echo "[2/7] Creating pre-deployment backup..."
BACKUP_TIMESTAMP=$(date -u +"%Y%m%d_%H%M%S")
wrangler r2 object put "dosql-lakehouse-${ENVIRONMENT}/_backups/pre-deploy-${BACKUP_TIMESTAMP}/marker.json" \
  --file=<(echo "{\"version\":\"$VERSION\",\"timestamp\":\"$BACKUP_TIMESTAMP\"}")

# Step 3: Deploy to Cloudflare
echo "[3/7] Deploying to Cloudflare..."
wrangler deploy --env "$ENVIRONMENT"

# Step 4: Run migrations (if any)
echo "[4/7] Running migrations..."
# Migrations are handled automatically by wrangler for DO classes

# Step 5: Health check
echo "[5/7] Running health checks..."
sleep 5  # Allow time for deployment propagation

HEALTH_URL="https://$(wrangler domains get --env $ENVIRONMENT | head -1)/health"
HEALTH_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$HEALTH_URL")

if [ "$HEALTH_STATUS" != "200" ]; then
  echo "ERROR: Health check failed with status $HEALTH_STATUS"
  echo "Initiating rollback..."
  wrangler rollback --env "$ENVIRONMENT"
  exit 1
fi

# Step 6: Smoke tests
echo "[6/7] Running smoke tests..."
npm run test:smoke -- --env "$ENVIRONMENT"

# Step 7: Notify
echo "[7/7] Sending deployment notification..."
curl -X POST "${SLACK_WEBHOOK_URL}" \
  -H "Content-Type: application/json" \
  -d "{\"text\":\"DoSQL deployed to ${ENVIRONMENT}: ${VERSION}\"}"

echo ""
echo "=== Deployment Complete ==="
echo "Version: $VERSION"
echo "Environment: $ENVIRONMENT"
echo "Health: $HEALTH_STATUS"
```

#### Blue-Green Deployment Pattern

```typescript
// blue-green-deploy.ts
interface DeploymentConfig {
  environment: 'production';
  blueVersion: string;
  greenVersion: string;
  trafficSplitPercent: number;
}

async function blueGreenDeploy(config: DeploymentConfig): Promise<void> {
  // Step 1: Deploy green version (new code)
  console.log('Deploying green version...');
  await deployVersion(config.greenVersion, 'green');

  // Step 2: Warm up green environment
  console.log('Warming up green environment...');
  await warmUp('green', { requests: 100, concurrency: 10 });

  // Step 3: Verify green health
  console.log('Verifying green health...');
  const greenHealth = await checkHealth('green');
  if (!greenHealth.healthy) {
    throw new Error(`Green deployment unhealthy: ${greenHealth.error}`);
  }

  // Step 4: Gradual traffic shift
  console.log('Shifting traffic to green...');
  for (const percent of [10, 25, 50, 75, 100]) {
    await setTrafficSplit({ blue: 100 - percent, green: percent });
    await sleep(60000);  // Wait 1 minute between shifts

    const metrics = await getRealtimeMetrics();
    if (metrics.errorRate > 0.01) {  // > 1% error rate
      console.log('Error rate elevated, rolling back...');
      await setTrafficSplit({ blue: 100, green: 0 });
      throw new Error('Deployment rolled back due to elevated error rate');
    }
  }

  // Step 5: Decommission blue
  console.log('Decommissioning blue version...');
  // Keep blue available for quick rollback for 24 hours
  await scheduleDecommission('blue', { delayHours: 24 });

  console.log('Blue-green deployment complete');
}
```

### Post-Deployment Verification

#### Verification Checklist

```bash
#!/bin/bash
# post-deploy-verify.sh

ENVIRONMENT="${1:-production}"
BASE_URL="https://db.example.com"

echo "=== Post-Deployment Verification ==="

# 1. Health endpoints
echo "[1/6] Checking health endpoints..."
curl -sf "${BASE_URL}/health" || { echo "FAIL: /health"; exit 1; }
curl -sf "${BASE_URL}/health/deep" || { echo "FAIL: /health/deep"; exit 1; }

# 2. Query functionality
echo "[2/6] Testing query functionality..."
QUERY_RESULT=$(curl -sf -X POST "${BASE_URL}/sql/query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_TOKEN}" \
  -d '{"sql":"SELECT 1 as test"}')
echo "$QUERY_RESULT" | jq -e '.rows[0].test == 1' || { echo "FAIL: Query test"; exit 1; }

# 3. Write functionality
echo "[3/6] Testing write functionality..."
WRITE_RESULT=$(curl -sf -X POST "${BASE_URL}/sql/exec" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_TOKEN}" \
  -d '{"sql":"INSERT INTO _health_check (id, ts) VALUES (?, ?)", "params":["deploy-check", '$(date +%s)']}')

# 4. CDC connectivity
echo "[4/6] Checking CDC connectivity..."
WS_CHECK=$(curl -sf "${BASE_URL}/lake/status" | jq -e '.connectedSources >= 0')

# 5. Metrics emission
echo "[5/6] Verifying metrics emission..."
# Check Analytics Engine for recent data points
METRICS_CHECK=$(curl -sf "https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/analytics_engine/sql" \
  -H "Authorization: Bearer ${CF_API_TOKEN}" \
  -d "SELECT COUNT(*) as c FROM dosql_metrics WHERE timestamp > NOW() - INTERVAL '5' MINUTE")

# 6. Error rate check
echo "[6/6] Checking error rates..."
ERROR_RATE=$(curl -sf "https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/workers/analytics" \
  -H "Authorization: Bearer ${CF_API_TOKEN}" \
  | jq '.result.totals.errorRate')

if (( $(echo "$ERROR_RATE > 0.01" | bc -l) )); then
  echo "WARNING: Error rate elevated: $ERROR_RATE"
fi

echo ""
echo "=== Verification Complete ==="
echo "All checks passed"
```

### Rollback Procedures

#### Automatic Rollback

```typescript
// rollback-manager.ts
interface RollbackTrigger {
  metric: string;
  threshold: number;
  window: string;
  action: 'immediate' | 'gradual';
}

const ROLLBACK_TRIGGERS: RollbackTrigger[] = [
  { metric: 'error_rate', threshold: 0.05, window: '5m', action: 'immediate' },
  { metric: 'latency_p99', threshold: 5000, window: '5m', action: 'gradual' },
  { metric: 'do_failures', threshold: 10, window: '1m', action: 'immediate' },
];

class RollbackManager {
  private previousVersion: string | null = null;

  async recordDeployment(version: string): Promise<void> {
    this.previousVersion = await this.getCurrentVersion();
    await this.storage.put('current_version', version);
    await this.storage.put('previous_version', this.previousVersion);
  }

  async checkRollbackNeeded(): Promise<boolean> {
    for (const trigger of ROLLBACK_TRIGGERS) {
      const value = await this.getMetricValue(trigger.metric, trigger.window);
      if (value > trigger.threshold) {
        console.error(`Rollback trigger: ${trigger.metric}=${value} > ${trigger.threshold}`);
        return true;
      }
    }
    return false;
  }

  async executeRollback(): Promise<RollbackResult> {
    if (!this.previousVersion) {
      throw new Error('No previous version available for rollback');
    }

    console.log(`Rolling back to version: ${this.previousVersion}`);

    // Use wrangler rollback
    const result = await execAsync(`wrangler rollback --env production`);

    // Notify on-call
    await this.notifyOnCall({
      type: 'rollback',
      previousVersion: this.previousVersion,
      reason: 'Automated rollback triggered',
    });

    return {
      success: result.exitCode === 0,
      version: this.previousVersion,
      timestamp: new Date().toISOString(),
    };
  }
}
```

#### Manual Rollback Commands

```bash
# Immediate rollback to previous version
wrangler rollback --env production

# Rollback to specific deployment
wrangler rollback --env production --deployment-id="abc123"

# List recent deployments
wrangler deployments list --env production

# View deployment details
wrangler deployments status --deployment-id="abc123"
```

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
                    +---------------------------------------------+
                    |          Cloudflare Global Network          |
                    +---------------------------------------------+
                                          |
          +-------------------------------+-------------------------------+
          |                               |                               |
          v                               v                               v
   +-------------+                 +-------------+                 +-------------+
   |  US Region  |                 |  EU Region  |                 | APAC Region |
   |   Workers   |                 |   Workers   |                 |   Workers   |
   +------+------+                 +------+------+                 +------+------+
          |                               |                               |
          v                               v                               v
   +-------------+                 +-------------+                 +-------------+
   | DoSQL DO    |                 | DoSQL DO    |                 | DoSQL DO    |
   | (Primary)   |------ CDC ----->| (Replica)   |------ CDC ----->| (Replica)   |
   +------+------+                 +------+------+                 +------+------+
          |                               |                               |
          +-------------------------------+-------------------------------+
                                          |
                                          v
                              +-----------------------+
                              |      R2 Bucket        |
                              | (Global Replication)  |
                              +-----------------------+
```

---

## Monitoring and Observability

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

### Distributed Tracing

Implement request tracing across Workers and Durable Objects.

```typescript
// src/tracing.ts
interface TraceContext {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  startTime: number;
}

export function extractTraceContext(request: Request): TraceContext {
  const traceHeader = request.headers.get('X-Trace-ID');
  const parentSpan = request.headers.get('X-Parent-Span-ID');

  return {
    traceId: traceHeader ?? crypto.randomUUID(),
    spanId: crypto.randomUUID(),
    parentSpanId: parentSpan ?? undefined,
    startTime: Date.now(),
  };
}

export function injectTraceContext(request: Request, trace: TraceContext): Request {
  const headers = new Headers(request.headers);
  headers.set('X-Trace-ID', trace.traceId);
  headers.set('X-Parent-Span-ID', trace.spanId);

  return new Request(request.url, {
    method: request.method,
    headers,
    body: request.body,
  });
}

export function recordSpan(
  env: Env,
  trace: TraceContext,
  name: string,
  attributes: Record<string, unknown>
): void {
  const duration = Date.now() - trace.startTime;

  env.METRICS?.writeDataPoint({
    blobs: ['trace', trace.traceId, trace.spanId, name],
    doubles: [duration],
    indexes: [trace.parentSpanId ?? 'root', attributes.status as string ?? 'ok'],
  });
}
```

---

## Alerting Recommendations

### Alert Tiers

Configure alerts based on severity and response requirements.

#### Tier 1 - Critical (Page Immediately)

| Alert | Condition | Response Time |
|-------|-----------|---------------|
| Service Down | Health check fails 3x consecutively | 5 minutes |
| Data Loss Risk | WAL flush failures | 5 minutes |
| Storage Critical | DO storage > 95% | 15 minutes |
| High Error Rate | Error rate > 5% for 5 minutes | 5 minutes |
| Total Outage | All shards unreachable | Immediate |

#### Tier 2 - High (Page During Hours)

| Alert | Condition | Response Time |
|-------|-----------|---------------|
| Performance Degraded | P99 latency > 5x baseline for 15 min | 30 minutes |
| CDC Lag High | Consumer lag > 10000 events | 30 minutes |
| Storage Warning | DO storage > 80% | 1 hour |
| Elevated Error Rate | Error rate > 1% for 15 minutes | 30 minutes |
| Replication Lag | CDC replication > 5 minutes behind | 30 minutes |

#### Tier 3 - Medium (Next Business Day)

| Alert | Condition | Response Time |
|-------|-----------|---------------|
| Slow Queries | Slow query count > 100/hour | 8 hours |
| Resource Usage | CPU consistently > 70% | 24 hours |
| Certificate Expiry | TLS cert expiring in < 30 days | 24 hours |
| Backup Failure | Scheduled backup failed | 24 hours |
| Disk Space Trend | Storage growth rate unsustainable | 24 hours |

### Alert Configuration

#### Cloudflare Workers Alerts

```typescript
// alert-rules.ts
interface AlertRule {
  name: string;
  tier: 1 | 2 | 3;
  condition: string;
  window: string;
  threshold: number;
  channels: string[];
}

const ALERT_RULES: AlertRule[] = [
  // Tier 1 - Critical
  {
    name: 'service_down',
    tier: 1,
    condition: 'health_check_success',
    window: '5m',
    threshold: 0,  // Alert if 0 successful checks
    channels: ['pagerduty', 'slack-critical'],
  },
  {
    name: 'high_error_rate',
    tier: 1,
    condition: 'error_rate',
    window: '5m',
    threshold: 0.05,  // 5%
    channels: ['pagerduty', 'slack-critical'],
  },
  {
    name: 'storage_critical',
    tier: 1,
    condition: 'storage_utilization',
    window: '5m',
    threshold: 0.95,  // 95%
    channels: ['pagerduty', 'slack-critical'],
  },

  // Tier 2 - High
  {
    name: 'latency_degraded',
    tier: 2,
    condition: 'latency_p99',
    window: '15m',
    threshold: 5000,  // 5 seconds
    channels: ['pagerduty-low', 'slack-alerts'],
  },
  {
    name: 'cdc_lag_high',
    tier: 2,
    condition: 'cdc_consumer_lag',
    window: '10m',
    threshold: 10000,  // events
    channels: ['slack-alerts'],
  },

  // Tier 3 - Medium
  {
    name: 'slow_queries',
    tier: 3,
    condition: 'slow_query_count',
    window: '1h',
    threshold: 100,
    channels: ['slack-ops', 'email'],
  },
];
```

#### External Alerting Integration

```typescript
// alert-manager.ts
export class AlertManager {
  private lastAlerts = new Map<string, number>();
  private cooldownMs = 300000;  // 5 minute cooldown

  async checkAndAlert(
    metrics: Record<string, number>,
    rules: AlertRule[]
  ): Promise<void> {
    for (const rule of rules) {
      const value = metrics[rule.condition];
      if (value === undefined) continue;

      const shouldAlert = this.evaluateRule(rule, value);
      if (shouldAlert && this.shouldSendAlert(rule.name)) {
        await this.sendAlert(rule, value);
      }
    }
  }

  private evaluateRule(rule: AlertRule, value: number): boolean {
    switch (rule.condition) {
      case 'health_check_success':
        return value === 0;
      case 'error_rate':
      case 'storage_utilization':
        return value >= rule.threshold;
      case 'latency_p99':
      case 'cdc_consumer_lag':
      case 'slow_query_count':
        return value > rule.threshold;
      default:
        return false;
    }
  }

  private shouldSendAlert(ruleName: string): boolean {
    const lastAlert = this.lastAlerts.get(ruleName);
    if (!lastAlert) return true;

    return Date.now() - lastAlert > this.cooldownMs;
  }

  private async sendAlert(rule: AlertRule, value: number): Promise<void> {
    this.lastAlerts.set(rule.name, Date.now());

    for (const channel of rule.channels) {
      await this.sendToChannel(channel, {
        rule: rule.name,
        tier: rule.tier,
        condition: rule.condition,
        value,
        threshold: rule.threshold,
        timestamp: new Date().toISOString(),
      });
    }
  }

  private async sendToChannel(channel: string, alert: unknown): Promise<void> {
    switch (channel) {
      case 'pagerduty':
        await this.sendPagerDuty(alert, 'critical');
        break;
      case 'pagerduty-low':
        await this.sendPagerDuty(alert, 'warning');
        break;
      case 'slack-critical':
        await this.sendSlack(alert, '#critical-alerts');
        break;
      case 'slack-alerts':
        await this.sendSlack(alert, '#alerts');
        break;
      case 'slack-ops':
        await this.sendSlack(alert, '#ops');
        break;
      case 'email':
        await this.sendEmail(alert);
        break;
    }
  }
}
```

### On-Call Procedures

#### On-Call Runbook

```markdown
## On-Call Engineer Responsibilities

### Shift Start
1. Review open alerts and incidents
2. Check deployment history for recent changes
3. Verify you have access to all required systems
4. Update on-call status in communication channels

### During Shift
1. Respond to pages within SLA (Tier 1: 5 min, Tier 2: 30 min)
2. Follow runbooks for known issues
3. Escalate unknown issues appropriately
4. Document all actions taken

### Alert Response Flow
1. Acknowledge alert within SLA
2. Assess severity and impact
3. Communicate status to stakeholders
4. Begin investigation/remediation
5. Update incident timeline
6. Resolve or escalate

### Escalation Path
1. Primary on-call engineer
2. Secondary on-call engineer
3. Engineering lead
4. VP of Engineering
5. CTO

### Contact Information
- Primary On-Call: [PagerDuty Schedule]
- Engineering Lead: engineering-lead@example.com
- Emergency Contact: +1-555-0123
```

### Alert Suppression and Maintenance Mode

```typescript
// maintenance-mode.ts
interface MaintenanceWindow {
  id: string;
  start: Date;
  end: Date;
  reason: string;
  suppressedAlerts: string[];
  createdBy: string;
}

class MaintenanceManager {
  private windows: MaintenanceWindow[] = [];

  async createWindow(window: MaintenanceWindow): Promise<void> {
    this.windows.push(window);
    await this.storage.put(`maintenance:${window.id}`, window);

    // Notify about maintenance
    await this.notifyMaintenanceStart(window);
  }

  isInMaintenance(): boolean {
    const now = new Date();
    return this.windows.some(w =>
      now >= w.start && now <= w.end
    );
  }

  shouldSuppressAlert(alertName: string): boolean {
    const now = new Date();
    for (const window of this.windows) {
      if (now >= window.start && now <= window.end) {
        if (window.suppressedAlerts.includes('*') ||
            window.suppressedAlerts.includes(alertName)) {
          return true;
        }
      }
    }
    return false;
  }

  async endWindow(windowId: string): Promise<void> {
    const index = this.windows.findIndex(w => w.id === windowId);
    if (index >= 0) {
      const window = this.windows[index];
      this.windows.splice(index, 1);
      await this.storage.delete(`maintenance:${windowId}`);
      await this.notifyMaintenanceEnd(window);
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
} from 'dosql/sharding';

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
import { createTimeTravelSession, timestamp, lsn } from 'dosql/timetravel';

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
   curl https://db.example.com/admin/shards/health
   ```

2. **Isolate the shard**
   ```typescript
   await markShardReadOnly(shardId);
   ```

3. **Find latest backup**
   ```bash
   wrangler r2 object list dosql-lakehouse-prod --prefix="_backups/" | grep shard-X
   ```

4. **Create new DO instance**
   ```typescript
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
   await updateShardMapping(shardId, newDoId.toString());
   ```

7. **Verify data integrity**
   ```bash
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

---

## Maintenance Windows

### Scheduled Maintenance

#### Maintenance Types and Windows

| Type | Frequency | Duration | Window (UTC) | Notice Required |
|------|-----------|----------|--------------|-----------------|
| Routine Updates | Weekly | 15-30 min | Sunday 03:00-05:00 | 24 hours |
| Schema Migrations | As needed | 30-60 min | Sunday 03:00-05:00 | 72 hours |
| Major Upgrades | Quarterly | 1-2 hours | Sunday 02:00-06:00 | 1 week |
| Emergency Patches | As needed | Variable | ASAP | Best effort |
| Compaction | Daily | Background | Daily 04:00 | None |

#### Maintenance Calendar Template

```typescript
// maintenance-calendar.ts
interface ScheduledMaintenance {
  id: string;
  type: 'routine' | 'schema' | 'major' | 'emergency';
  description: string;
  scheduledStart: Date;
  scheduledEnd: Date;
  affectedServices: string[];
  expectedImpact: 'none' | 'degraded' | 'outage';
  notificationSent: boolean;
  status: 'scheduled' | 'in_progress' | 'completed' | 'cancelled';
}

const MAINTENANCE_SCHEDULE: ScheduledMaintenance[] = [
  {
    id: 'maint-2026-01-26',
    type: 'routine',
    description: 'Weekly security updates and dependency refresh',
    scheduledStart: new Date('2026-01-26T03:00:00Z'),
    scheduledEnd: new Date('2026-01-26T03:30:00Z'),
    affectedServices: ['dosql', 'dolake'],
    expectedImpact: 'none',
    notificationSent: false,
    status: 'scheduled',
  },
];
```

### Maintenance Procedures

#### Pre-Maintenance Checklist

```bash
#!/bin/bash
# pre-maintenance.sh

echo "=== Pre-Maintenance Checklist ==="

# 1. Verify maintenance window
echo "[1/7] Verifying maintenance window..."
CURRENT_HOUR=$(date -u +%H)
if [ "$CURRENT_HOUR" -lt 3 ] || [ "$CURRENT_HOUR" -gt 5 ]; then
  echo "WARNING: Outside standard maintenance window"
  read -p "Continue? (y/n) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
  fi
fi

# 2. Create backup
echo "[2/7] Creating pre-maintenance backup..."
./scripts/backup.sh production

# 3. Check system health
echo "[3/7] Checking system health..."
HEALTH=$(curl -sf https://db.example.com/health/deep)
if [ "$(echo $HEALTH | jq -r '.status')" != "healthy" ]; then
  echo "ERROR: System not healthy, aborting maintenance"
  exit 1
fi

# 4. Enable maintenance mode
echo "[4/7] Enabling maintenance mode..."
curl -X POST https://db.example.com/admin/maintenance/enable \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"reason":"Scheduled maintenance","duration_minutes":30}'

# 5. Drain connections (gracefully)
echo "[5/7] Draining connections..."
curl -X POST https://db.example.com/admin/connections/drain \
  -H "Authorization: Bearer $ADMIN_TOKEN"
sleep 30  # Wait for connections to drain

# 6. Verify no active transactions
echo "[6/7] Verifying no active transactions..."
ACTIVE_TXN=$(curl -sf https://db.example.com/admin/transactions/active | jq '.count')
if [ "$ACTIVE_TXN" -gt 0 ]; then
  echo "WARNING: $ACTIVE_TXN active transactions"
fi

# 7. Notify stakeholders
echo "[7/7] Sending maintenance notification..."
./scripts/notify-maintenance-start.sh

echo "=== Pre-Maintenance Complete ==="
echo "System ready for maintenance"
```

#### Post-Maintenance Checklist

```bash
#!/bin/bash
# post-maintenance.sh

echo "=== Post-Maintenance Checklist ==="

# 1. Disable maintenance mode
echo "[1/6] Disabling maintenance mode..."
curl -X POST https://db.example.com/admin/maintenance/disable \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# 2. Run health checks
echo "[2/6] Running health checks..."
for i in {1..5}; do
  HEALTH=$(curl -sf https://db.example.com/health/deep)
  if [ "$(echo $HEALTH | jq -r '.status')" = "healthy" ]; then
    echo "Health check passed"
    break
  fi
  echo "Health check $i failed, retrying..."
  sleep 10
done

# 3. Verify shard health
echo "[3/6] Verifying shard health..."
curl -sf https://db.example.com/admin/shards/health | jq '.'

# 4. Run smoke tests
echo "[4/6] Running smoke tests..."
npm run test:smoke -- --env production

# 5. Check metrics
echo "[5/6] Checking metrics..."
# Verify no elevated error rates

# 6. Notify stakeholders
echo "[6/6] Sending maintenance complete notification..."
./scripts/notify-maintenance-complete.sh

echo "=== Post-Maintenance Complete ==="
```

### Zero-Downtime Operations

#### Rolling Deployment Strategy

```typescript
// rolling-deploy.ts
interface RollingDeployConfig {
  batchSize: number;          // Shards to update per batch
  healthCheckWaitMs: number;  // Wait time between batches
  rollbackOnFailure: boolean; // Auto-rollback on failure
}

async function rollingDeploy(
  shards: string[],
  config: RollingDeployConfig
): Promise<void> {
  const batches = chunkArray(shards, config.batchSize);
  const deployedShards: string[] = [];

  for (const batch of batches) {
    console.log(`Deploying batch: ${batch.join(', ')}`);

    try {
      // Deploy to batch
      for (const shard of batch) {
        await deployShard(shard);
        deployedShards.push(shard);
      }

      // Health check wait
      await sleep(config.healthCheckWaitMs);

      // Verify batch health
      const healthy = await checkBatchHealth(batch);
      if (!healthy) {
        throw new Error(`Batch health check failed: ${batch.join(', ')}`);
      }

      console.log(`Batch healthy: ${batch.join(', ')}`);
    } catch (error) {
      console.error(`Deployment failed: ${error.message}`);

      if (config.rollbackOnFailure) {
        console.log('Rolling back deployed shards...');
        for (const shard of deployedShards) {
          await rollbackShard(shard);
        }
      }

      throw error;
    }
  }

  console.log('Rolling deployment complete');
}
```

#### Connection Draining

```typescript
// connection-drain.ts
class ConnectionDrainer {
  private draining = false;
  private activeConnections = new Set<string>();

  startDrain(): void {
    this.draining = true;
  }

  canAcceptConnection(): boolean {
    return !this.draining;
  }

  registerConnection(id: string): void {
    this.activeConnections.add(id);
  }

  unregisterConnection(id: string): void {
    this.activeConnections.delete(id);
  }

  async waitForDrain(timeoutMs: number): Promise<boolean> {
    const start = Date.now();

    while (this.activeConnections.size > 0) {
      if (Date.now() - start > timeoutMs) {
        console.warn(`Drain timeout: ${this.activeConnections.size} connections remaining`);
        return false;
      }

      await sleep(1000);
    }

    return true;
  }

  getStatus(): { draining: boolean; activeCount: number } {
    return {
      draining: this.draining,
      activeCount: this.activeConnections.size,
    };
  }
}
```

---

## Incident Response

### Incident Classification

#### Severity Levels

| Severity | Impact | Examples | Response Time | Resolution Target |
|----------|--------|----------|---------------|-------------------|
| **SEV-1** | Complete outage or data loss | All shards down, data corruption | 5 minutes | 1 hour |
| **SEV-2** | Major feature unavailable | CDC streaming down, queries failing | 15 minutes | 4 hours |
| **SEV-3** | Minor feature degraded | Slow queries, elevated latency | 1 hour | 8 hours |
| **SEV-4** | Minimal impact | Cosmetic issues, minor bugs | 24 hours | 72 hours |

### Response Procedures

#### SEV-1 Response Playbook

```markdown
## SEV-1 Incident Response

### Immediate Actions (0-5 minutes)
1. **Acknowledge** - Confirm receipt of alert
2. **Assess** - Quick impact assessment
3. **Communicate** - Open incident channel (#incident-YYYYMMDD)
4. **Escalate** - Page engineering lead and stakeholders

### Investigation (5-15 minutes)
1. Check recent deployments: `wrangler deployments list`
2. Check infrastructure status: Cloudflare status page
3. Review logs: `wrangler tail --env production`
4. Check metrics dashboard for anomalies

### Mitigation (15-60 minutes)
1. If deployment-related: Execute rollback
2. If traffic-related: Enable rate limiting
3. If infrastructure: Engage Cloudflare support
4. Document all actions in incident channel

### Communication Template
"""
**INCIDENT: [Title]**
**Severity**: SEV-1
**Status**: Investigating / Identified / Monitoring / Resolved
**Impact**: [Describe user impact]
**Started**: [Timestamp]
**Last Update**: [Timestamp]
**Next Update**: [Estimated time]

**Current Status**:
[Brief description of current state]

**Actions Taken**:
- [Action 1]
- [Action 2]
"""

### Resolution
1. Verify service restored
2. Monitor for 30 minutes
3. Post resolution update
4. Schedule post-incident review
```

#### Incident Timeline Template

```typescript
// incident-timeline.ts
interface IncidentEvent {
  timestamp: Date;
  type: 'detection' | 'acknowledgment' | 'investigation' | 'mitigation' | 'resolution' | 'update';
  description: string;
  actor: string;
}

interface Incident {
  id: string;
  title: string;
  severity: 1 | 2 | 3 | 4;
  status: 'open' | 'investigating' | 'identified' | 'monitoring' | 'resolved';
  startTime: Date;
  resolveTime?: Date;
  impact: string;
  rootCause?: string;
  timeline: IncidentEvent[];
  affectedServices: string[];
  customerImpact: {
    usersAffected: number;
    dataLoss: boolean;
    financialImpact?: number;
  };
}
```

### Communication Templates

#### Customer-Facing Status Updates

```typescript
// status-update-templates.ts
const STATUS_TEMPLATES = {
  investigating: `
**Service Disruption - Investigating**

We are currently investigating reports of [issue description].

**Impact**: [Describe what customers may experience]
**Started**: [Time]

We are working to identify the cause and will provide updates every 30 minutes.
  `,

  identified: `
**Service Disruption - Cause Identified**

We have identified the cause of [issue description] as [root cause].

**Impact**: [Describe what customers may experience]
**Started**: [Time]
**Cause**: [Brief technical explanation]

Our team is working on a fix. We expect to resolve this within [ETA].
  `,

  monitoring: `
**Service Disruption - Fix Deployed**

We have deployed a fix for [issue description] and are monitoring the results.

**Impact**: Service should be returning to normal
**Started**: [Time]
**Duration**: [Duration so far]

We will continue monitoring and provide a final update once confirmed stable.
  `,

  resolved: `
**Service Disruption - Resolved**

The [issue description] has been resolved.

**Total Duration**: [Duration]
**Root Cause**: [Brief explanation]
**Resolution**: [What was done to fix it]

We apologize for any inconvenience. A full incident report will be published within 48 hours.
  `,
};
```

### Post-Incident Review

#### Post-Incident Review Template

```markdown
# Post-Incident Review: [Incident Title]

**Date**: [Date of incident]
**Duration**: [Total duration]
**Severity**: SEV-[N]
**Author**: [Name]
**Review Date**: [Date of this review]

## Summary
[2-3 sentence summary of what happened]

## Timeline
| Time (UTC) | Event |
|------------|-------|
| HH:MM | Initial alert triggered |
| HH:MM | On-call engineer acknowledged |
| HH:MM | Root cause identified |
| HH:MM | Mitigation deployed |
| HH:MM | Service restored |
| HH:MM | Incident resolved |

## Impact
- **Users affected**: [Number or percentage]
- **Data loss**: [Yes/No - describe if yes]
- **Financial impact**: [If applicable]
- **SLA breach**: [Yes/No]

## Root Cause
[Detailed explanation of what caused the incident]

## Contributing Factors
1. [Factor 1]
2. [Factor 2]

## What Went Well
1. [Positive 1]
2. [Positive 2]

## What Could Be Improved
1. [Improvement area 1]
2. [Improvement area 2]

## Action Items
| Action | Owner | Due Date | Status |
|--------|-------|----------|--------|
| [Action 1] | [Name] | [Date] | Open |
| [Action 2] | [Name] | [Date] | Open |

## Lessons Learned
[Key takeaways that should inform future operations]
```

---

## Capacity Planning

### Resource Limits

#### Cloudflare Workers Limits

| Resource | Limit | Notes |
|----------|-------|-------|
| CPU time per request | 30 seconds (paid) | Configurable via `limits.cpu_ms` |
| Memory per Worker | 128 MB | Includes V8 heap |
| Request body size | 100 MB | For uploads |
| Subrequest limit | 1000 per request | DO fetches count |
| Environment variables | 64 | Combined text length < 32 KB |
| Worker size | 10 MB (compressed) | After gzip |

#### Durable Object Limits

| Resource | Limit | Notes |
|----------|-------|-------|
| Storage per DO | 10 GB | Hard limit |
| SQLite database | 10 GB | Part of DO storage |
| Key size | 2 KB | For KV operations |
| Value size | 128 KB | For KV operations |
| Concurrent requests | 1000+ | Soft limit, scales |
| WebSocket connections | 32,000 | Per DO instance |
| Alarm frequency | Every 12 hours (min) | For hibernating DOs |

#### R2 Limits

| Resource | Limit | Notes |
|----------|-------|-------|
| Object size | 5 TB | Per object |
| PUT request body | 5 GB | Single-part upload |
| Multipart upload | 10,000 parts | 5 GB per part |
| Bucket objects | Unlimited | No object count limit |
| Custom metadata | 2 KB | Per object |
| List results | 1000 | Per API call |

### Growth Forecasting

#### Capacity Projection Model

```typescript
// capacity-forecast.ts
interface UsageMetrics {
  timestamp: Date;
  storageBytes: number;
  dailyQueries: number;
  dailyWrites: number;
  uniqueUsers: number;
  shardCount: number;
}

interface CapacityForecast {
  projectedDate: Date;
  metric: string;
  currentValue: number;
  projectedValue: number;
  limit: number;
  daysUntilLimit: number;
  recommendation: string;
}

function forecastCapacity(
  metrics: UsageMetrics[],
  forecastDays: number
): CapacityForecast[] {
  const forecasts: CapacityForecast[] = [];

  // Storage forecast
  const storageGrowthRate = calculateGrowthRate(metrics.map(m => m.storageBytes));
  const currentStorage = metrics[metrics.length - 1].storageBytes;
  const projectedStorage = currentStorage * Math.pow(1 + storageGrowthRate, forecastDays);
  const storageLimit = 10 * 1024 * 1024 * 1024;  // 10 GB per shard

  forecasts.push({
    projectedDate: addDays(new Date(), forecastDays),
    metric: 'storage_per_shard',
    currentValue: currentStorage,
    projectedValue: projectedStorage,
    limit: storageLimit,
    daysUntilLimit: calculateDaysUntilLimit(currentStorage, storageGrowthRate, storageLimit),
    recommendation: projectedStorage > storageLimit * 0.8
      ? 'Add shards within 30 days'
      : 'No action needed',
  });

  // Query volume forecast
  const queryGrowthRate = calculateGrowthRate(metrics.map(m => m.dailyQueries));
  const currentQueries = metrics[metrics.length - 1].dailyQueries;
  const projectedQueries = currentQueries * Math.pow(1 + queryGrowthRate, forecastDays);
  const queryCapacity = 10000000;  // 10M queries/day target

  forecasts.push({
    projectedDate: addDays(new Date(), forecastDays),
    metric: 'daily_queries',
    currentValue: currentQueries,
    projectedValue: projectedQueries,
    limit: queryCapacity,
    daysUntilLimit: calculateDaysUntilLimit(currentQueries, queryGrowthRate, queryCapacity),
    recommendation: projectedQueries > queryCapacity * 0.8
      ? 'Scale shards or optimize queries'
      : 'No action needed',
  });

  return forecasts;
}

function calculateGrowthRate(values: number[]): number {
  if (values.length < 2) return 0;

  // Simple linear regression for daily growth rate
  const n = values.length;
  const periods = values.map((_, i) => i);

  const sumX = periods.reduce((a, b) => a + b, 0);
  const sumY = values.reduce((a, b) => a + b, 0);
  const sumXY = periods.reduce((sum, x, i) => sum + x * values[i], 0);
  const sumXX = periods.reduce((sum, x) => sum + x * x, 0);

  const slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
  const intercept = (sumY - slope * sumX) / n;

  // Convert to daily growth rate
  return slope / intercept;
}
```

### Scaling Triggers

#### Automated Scaling Rules

```typescript
// auto-scaling.ts
interface ScalingRule {
  metric: string;
  threshold: number;
  operator: '>' | '<' | '>=' | '<=';
  duration: string;  // e.g., '15m', '1h'
  action: 'scale_up' | 'scale_down' | 'alert';
  cooldownMinutes: number;
}

const SCALING_RULES: ScalingRule[] = [
  // Scale up rules
  {
    metric: 'storage_utilization',
    threshold: 0.75,
    operator: '>=',
    duration: '15m',
    action: 'scale_up',
    cooldownMinutes: 60,
  },
  {
    metric: 'query_latency_p99',
    threshold: 500,
    operator: '>',
    duration: '30m',
    action: 'scale_up',
    cooldownMinutes: 60,
  },
  {
    metric: 'cpu_utilization',
    threshold: 0.80,
    operator: '>=',
    duration: '15m',
    action: 'scale_up',
    cooldownMinutes: 60,
  },

  // Alert rules (manual scaling decision)
  {
    metric: 'storage_utilization',
    threshold: 0.60,
    operator: '>=',
    duration: '1d',
    action: 'alert',
    cooldownMinutes: 1440,  // 24 hours
  },
];

class AutoScaler {
  private lastScaleAction = new Map<string, number>();

  async evaluate(metrics: Record<string, number>): Promise<void> {
    for (const rule of SCALING_RULES) {
      const value = metrics[rule.metric];
      if (value === undefined) continue;

      const triggered = this.checkRule(rule, value);
      if (triggered && this.canTakeAction(rule)) {
        await this.executeAction(rule, value);
      }
    }
  }

  private checkRule(rule: ScalingRule, value: number): boolean {
    switch (rule.operator) {
      case '>': return value > rule.threshold;
      case '<': return value < rule.threshold;
      case '>=': return value >= rule.threshold;
      case '<=': return value <= rule.threshold;
    }
  }

  private canTakeAction(rule: ScalingRule): boolean {
    const lastAction = this.lastScaleAction.get(rule.metric);
    if (!lastAction) return true;

    const cooldownMs = rule.cooldownMinutes * 60 * 1000;
    return Date.now() - lastAction > cooldownMs;
  }

  private async executeAction(rule: ScalingRule, value: number): Promise<void> {
    this.lastScaleAction.set(rule.metric, Date.now());

    if (rule.action === 'alert') {
      await this.sendAlert(rule, value);
    } else {
      console.log(`Auto-scaling: ${rule.action} triggered by ${rule.metric}=${value}`);
      // In practice, scaling requires manual approval or automated shard addition
      await this.sendAlert(rule, value);
    }
  }
}
```

### Capacity Review Schedule

#### Monthly Capacity Review Checklist

```markdown
## Monthly Capacity Review

### Storage Analysis
- [ ] Current storage utilization per shard
- [ ] Storage growth rate (MB/day)
- [ ] Projected storage at current growth
- [ ] Days until 80% capacity per shard
- [ ] Compaction effectiveness

### Query Performance
- [ ] Query volume trends (QPS)
- [ ] Latency percentiles (P50, P95, P99)
- [ ] Slow query analysis
- [ ] Index effectiveness

### CDC and Lakehouse
- [ ] CDC throughput (events/second)
- [ ] Consumer lag trends
- [ ] R2 storage growth
- [ ] Parquet file sizes

### Cost Analysis
- [ ] Current monthly spend
- [ ] Cost per query/write
- [ ] Projected costs at growth rate
- [ ] Optimization opportunities

### Recommendations
- [ ] Scaling actions needed (30/60/90 day)
- [ ] Optimization opportunities identified
- [ ] Budget adjustments required
```

---

## Cost Optimization

### Pricing Model

#### Cloudflare Workers Pricing

| Resource | Free Tier | Paid (Bundled) | Paid (Unbound) |
|----------|-----------|----------------|----------------|
| Requests | 100K/day | 10M/month included | $0.15/million |
| CPU time | 10ms/request | 50ms/request | $0.02/million ms |
| Duration | N/A | N/A | $12.50/million GB-s |

#### Durable Objects Pricing

| Resource | Price | Notes |
|----------|-------|-------|
| Requests | $0.15/million | Each DO fetch |
| Duration | $12.50/million GB-s | Wall-clock time |
| Storage | $0.20/GB-month | Includes SQLite |
| Reads | $0.20/million | KV read operations |
| Writes | $1.00/million | KV write operations |
| Deletes | $1.00/million | KV delete operations |

#### R2 Pricing

| Operation | Price | Notes |
|-----------|-------|-------|
| Storage | $0.015/GB-month | Standard storage |
| Class A (write) | $4.50/million | PUT, POST, COPY, LIST |
| Class B (read) | $0.36/million | GET, HEAD |
| Egress | Free | No egress fees |

### Cost Reduction Strategies

#### 1. Query Optimization

```typescript
// Reduce requests through batching
// Before: 1000 individual queries = 1000 requests
const results = [];
for (const id of ids) {
  results.push(await db.query('SELECT * FROM users WHERE id = ?', [id]));
}

// After: 1 batched query = 1 request
const results = await db.query(
  `SELECT * FROM users WHERE id IN (${ids.map(() => '?').join(',')})`,
  ids
);

// Cost savings: ~1000x fewer DO requests
```

#### 2. Hibernation for Idle DOs

```typescript
// Enable WebSocket hibernation for CDC connections
// Cost impact: 95% reduction in DO duration charges during idle

// Without hibernation:
// - Active DO consuming duration even when idle
// - $12.50/million GB-s * 24/7 = significant cost

// With hibernation:
// - DO sleeps between messages
// - Only charged for actual message processing
```

#### 3. Efficient CDC Batching

```typescript
// Batch CDC events to reduce write operations
// Before: 10,000 events/hour = 10,000 writes
for (const event of events) {
  await lakeBucket.put(`events/${event.id}`, JSON.stringify(event));
}

// After: 100 batched writes/hour = 100 writes
const batches = chunkArray(events, 100);
for (const batch of batches) {
  await flushToParquet(batch);  // Single R2 write per batch
}

// Cost savings: 100x fewer Class A operations
```

#### 4. Compaction for Storage

```typescript
// Regular compaction reduces storage costs
// Before compaction:
// - 1000 small Parquet files (10MB each) = 10 GB
// - Higher metadata overhead
// - More LIST operations needed

// After compaction:
// - 100 optimal Parquet files (100MB each) = 10 GB
// - Lower metadata overhead
// - Fewer LIST operations

const COMPACTION_CONFIG = {
  minFilesThreshold: 10,
  targetFileSizeBytes: 128 * 1024 * 1024,  // 128MB
  schedule: 'daily',
};
```

### Budget Monitoring

#### Cost Tracking Dashboard

```typescript
// cost-monitor.ts
interface CostBreakdown {
  period: string;
  total: number;
  breakdown: {
    workerRequests: number;
    workerCpu: number;
    doRequests: number;
    doDuration: number;
    doStorage: number;
    doReads: number;
    doWrites: number;
    r2Storage: number;
    r2ClassA: number;
    r2ClassB: number;
  };
}

async function getCostEstimate(
  metrics: UsageMetrics,
  pricingTier: 'free' | 'bundled' | 'unbound'
): Promise<CostBreakdown> {
  const prices = getPricing(pricingTier);

  return {
    period: 'monthly',
    total: 0,  // Calculate below
    breakdown: {
      workerRequests: metrics.requests * prices.workerRequest,
      workerCpu: metrics.cpuMs * prices.cpuMs,
      doRequests: metrics.doRequests * prices.doRequest,
      doDuration: metrics.doDurationGbS * prices.doDuration,
      doStorage: metrics.doStorageGb * prices.doStorage,
      doReads: metrics.doReads * prices.doRead,
      doWrites: metrics.doWrites * prices.doWrite,
      r2Storage: metrics.r2StorageGb * prices.r2Storage,
      r2ClassA: metrics.r2ClassA * prices.r2ClassA,
      r2ClassB: metrics.r2ClassB * prices.r2ClassB,
    },
  };
}

// Budget alert thresholds
const BUDGET_ALERTS = {
  daily: {
    warning: 100,    // $100/day
    critical: 200,   // $200/day
  },
  monthly: {
    warning: 2500,   // $2,500/month
    critical: 5000,  // $5,000/month
  },
};
```

### TCO Analysis

#### Total Cost of Ownership Model

```typescript
// tco-model.ts
interface TCOModel {
  // Direct costs
  infrastructure: {
    workers: number;
    durableObjects: number;
    r2Storage: number;
    analytics: number;
  };

  // Indirect costs
  operations: {
    engineeringHours: number;
    oncallHours: number;
    trainingHours: number;
  };

  // Comparison alternatives
  alternatives: {
    selfHosted: number;
    d1: number;
    planetscale: number;
    turso: number;
  };
}

function calculateTCO(
  usage: UsageMetrics,
  teamSize: number,
  hourlyRate: number
): TCOModel {
  // Infrastructure costs (monthly)
  const infrastructure = {
    workers: calculateWorkerCost(usage),
    durableObjects: calculateDOCost(usage),
    r2Storage: calculateR2Cost(usage),
    analytics: 0,  // Included in Workers
  };

  // Operations costs (monthly)
  const operations = {
    engineeringHours: teamSize * 10 * hourlyRate,  // 10 hrs/month maintenance
    oncallHours: 4 * 8 * hourlyRate,               // 4 weekends, 8 hrs each
    trainingHours: teamSize * 2 * hourlyRate,      // 2 hrs/month training
  };

  // Alternative costs for comparison
  const alternatives = {
    selfHosted: calculateSelfHostedCost(usage),
    d1: calculateD1Cost(usage),
    planetscale: calculatePlanetScaleCost(usage),
    turso: calculateTursoCost(usage),
  };

  return { infrastructure, operations, alternatives };
}
```

#### Cost Comparison Table

| Solution | 1M Queries/month | 10M Queries/month | 100M Queries/month |
|----------|------------------|-------------------|---------------------|
| DoSQL | $50-100 | $200-400 | $1,500-3,000 |
| D1 | $0 (free tier) | $5 | $50 |
| PlanetScale | $29 (starter) | $29-99 | $99-499 |
| Turso | $0-29 | $29-99 | Contact |
| Self-hosted | $50-200 | $200-500 | $500-2,000 |

**Note**: DoSQL includes edge compute, real-time CDC, and lakehouse features not available in other solutions. Direct cost comparison should consider feature parity.

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
}
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

### Storage Cleanup

Regular cleanup of orphaned and temporary files.

#### Cleanup Schedule

```toml
# wrangler.toml
[triggers]
crons = [
  "0 4 * * *",   # Daily cleanup at 4 AM UTC
  "0 3 * * 0"    # Weekly deep cleanup on Sunday 3 AM UTC
]
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
