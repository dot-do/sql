# DoSQL Deployment and Operations Guide

This guide covers deploying DoSQL to Cloudflare Workers, production configuration, monitoring, scaling strategies, and troubleshooting deployment issues.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Basic Deployment](#basic-deployment)
- [Production Configuration](#production-configuration)
- [Monitoring](#monitoring)
- [Scaling](#scaling)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Cloudflare Account Setup

1. **Create a Cloudflare Account**
   - Sign up at [dash.cloudflare.com](https://dash.cloudflare.com)
   - Enable Workers & Pages in your dashboard
   - Note your Account ID (found in Workers & Pages > Overview)

2. **Enable Durable Objects**
   - Durable Objects require a paid Workers plan (Workers Paid or higher)
   - Navigate to Workers & Pages > Plans and upgrade if necessary

3. **Create R2 Bucket (Optional)**
   - For lakehouse/cold storage integration, create an R2 bucket
   - Navigate to R2 > Create bucket
   - Note the bucket name for wrangler configuration

### Wrangler CLI Installation

```bash
# Install wrangler globally
npm install -g wrangler

# Or as a project dependency (recommended)
npm install wrangler --save-dev

# Authenticate with Cloudflare
wrangler login

# Verify authentication
wrangler whoami
```

### Node.js Requirements

- **Node.js**: 18.0.0 or higher (recommended: 20.x LTS)
- **npm**: 9.0.0 or higher

```bash
# Check versions
node --version  # Should be >= 18.0.0
npm --version   # Should be >= 9.0.0

# Install dependencies
npm install @dotdo/dosql
npm install wrangler @cloudflare/workers-types typescript --save-dev
```

---

## Basic Deployment

### Project Structure

```
my-dosql-app/
├── src/
│   ├── index.ts           # Worker entry point
│   └── database.ts        # DoSQL Durable Object
├── .do/
│   └── migrations/        # SQL migration files
│       ├── 001_init.sql
│       └── 002_indexes.sql
├── wrangler.jsonc         # Wrangler configuration
├── tsconfig.json          # TypeScript configuration
└── package.json
```

### Wrangler Configuration

Create `wrangler.jsonc`:

```jsonc
{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "my-dosql-app",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-01",

  // Durable Objects configuration
  "durable_objects": {
    "bindings": [
      {
        "name": "DOSQL_DB",
        "class_name": "DoSQLDatabase"
      }
    ]
  },

  // Migrations for DO classes
  "migrations": [
    {
      "tag": "v1",
      "new_sqlite_classes": ["DoSQLDatabase"]
    }
  ]
}
```

### Durable Object Class Setup

Create `src/database.ts`:

```typescript
import { DB } from '@dotdo/dosql';

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  R2_STORAGE?: R2Bucket;
}

export class DoSQLDatabase implements DurableObject {
  private db: Awaited<ReturnType<typeof DB>> | null = null;
  private state: DurableObjectState;
  private env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  private async getDB() {
    if (!this.db) {
      this.db = await DB('app', {
        migrations: { folder: '.do/migrations' },
        storage: {
          hot: this.state.storage,
          cold: this.env.R2_STORAGE,
        },
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();
    const url = new URL(request.url);

    try {
      // Route requests to appropriate handlers
      if (url.pathname === '/query' && request.method === 'POST') {
        const { sql, params } = await request.json() as {
          sql: string;
          params?: unknown[];
        };
        const result = await db.query(sql, params);
        return Response.json(result);
      }

      if (url.pathname === '/execute' && request.method === 'POST') {
        const { sql, params } = await request.json() as {
          sql: string;
          params?: unknown[];
        };
        const result = await db.run(sql, params);
        return Response.json(result);
      }

      return new Response('Not Found', { status: 404 });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return Response.json({ error: message }, { status: 500 });
    }
  }
}
```

Create `src/index.ts`:

```typescript
import { Env } from './database';

export { DoSQLDatabase } from './database';

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Extract database ID from path or use default
    const dbId = url.searchParams.get('db') || 'default';
    const id = env.DOSQL_DB.idFromName(dbId);
    const stub = env.DOSQL_DB.get(id);

    // Forward request to Durable Object
    return stub.fetch(request);
  },
};
```

### Deployment Commands

```bash
# Deploy to Cloudflare
npm run deploy
# Or directly with wrangler
wrangler deploy

# Deploy to staging environment
wrangler deploy --env staging

# Deploy to production
wrangler deploy --env production

# Tail logs in real-time
wrangler tail

# View deployment status
wrangler deployments list
```

### Package.json Scripts

```json
{
  "scripts": {
    "dev": "wrangler dev",
    "deploy": "wrangler deploy",
    "deploy:staging": "wrangler deploy --env staging",
    "deploy:production": "wrangler deploy --env production",
    "tail": "wrangler tail",
    "logs": "wrangler tail --format pretty"
  }
}
```

---

## Production Configuration

### Environment-Specific Configuration

Create a comprehensive `wrangler.jsonc` with multiple environments:

```jsonc
{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "dosql-app",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-01",
  "compatibility_flags": ["nodejs_compat"],

  // Base Durable Objects (inherited by all envs)
  "durable_objects": {
    "bindings": [
      { "name": "DOSQL_DB", "class_name": "DoSQLDatabase" }
    ]
  },

  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["DoSQLDatabase"] }
  ],

  // Development environment (default)
  "vars": {
    "ENVIRONMENT": "development",
    "LOG_LEVEL": "debug"
  },

  // Staging environment
  "env": {
    "staging": {
      "name": "dosql-app-staging",
      "vars": {
        "ENVIRONMENT": "staging",
        "LOG_LEVEL": "info"
      },
      "r2_buckets": [
        {
          "binding": "R2_STORAGE",
          "bucket_name": "dosql-staging-bucket"
        }
      ]
    },

    // Production environment
    "production": {
      "name": "dosql-app-production",
      "vars": {
        "ENVIRONMENT": "production",
        "LOG_LEVEL": "warn"
      },
      "r2_buckets": [
        {
          "binding": "R2_STORAGE",
          "bucket_name": "dosql-production-bucket"
        }
      ],
      // Production-specific limits
      "limits": {
        "cpu_ms": 50
      }
    }
  }
}
```

### Environment Variables

Configure environment variables in `wrangler.jsonc`:

```jsonc
{
  "vars": {
    // Application settings
    "ENVIRONMENT": "production",
    "LOG_LEVEL": "warn",

    // DoSQL settings
    "DOSQL_MAX_CONNECTIONS": "100",
    "DOSQL_QUERY_TIMEOUT_MS": "30000",
    "DOSQL_WAL_CHECKPOINT_THRESHOLD": "1000",

    // Feature flags
    "ENABLE_CDC": "true",
    "ENABLE_VECTOR_SEARCH": "true",
    "ENABLE_TIME_TRAVEL": "true"
  }
}
```

### Secrets Management

Secrets should never be stored in `wrangler.jsonc`. Use Wrangler CLI or the dashboard:

```bash
# Add secrets via CLI
wrangler secret put API_KEY
wrangler secret put DATABASE_ENCRYPTION_KEY
wrangler secret put JWT_SECRET

# Add secrets for specific environment
wrangler secret put API_KEY --env production

# List secrets (names only, not values)
wrangler secret list

# Delete a secret
wrangler secret delete API_KEY
```

Access secrets in your Worker:

```typescript
export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  R2_STORAGE: R2Bucket;
  // Secrets
  API_KEY: string;
  DATABASE_ENCRYPTION_KEY: string;
  JWT_SECRET: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Verify API key
    const apiKey = request.headers.get('X-API-Key');
    if (apiKey !== env.API_KEY) {
      return new Response('Unauthorized', { status: 401 });
    }
    // ...
  },
};
```

### Resource Limits

Cloudflare Workers and Durable Objects have specific limits:

| Resource | Limit | Notes |
|----------|-------|-------|
| Worker CPU time | 50ms (paid) | Per request |
| Worker memory | 128 MB | Per isolate |
| DO storage | 128 KB per key | SQLite storage |
| DO total storage | Unlimited | Paid plans |
| DO CPU time | 30s | Per request |
| R2 object size | 5 GB | Per object |
| Subrequest limit | 1000 | Per request |

Configure limits in `wrangler.jsonc`:

```jsonc
{
  "limits": {
    "cpu_ms": 50
  },

  // For Durable Objects with larger storage needs
  "durable_objects": {
    "bindings": [
      {
        "name": "DOSQL_DB",
        "class_name": "DoSQLDatabase"
      }
    ]
  }
}
```

### R2 Bucket Configuration

For cold storage and lakehouse integration:

```jsonc
{
  "r2_buckets": [
    {
      "binding": "R2_STORAGE",
      "bucket_name": "dosql-data",
      "preview_bucket_name": "dosql-data-preview"
    },
    {
      "binding": "R2_LAKEHOUSE",
      "bucket_name": "dosql-lakehouse"
    }
  ]
}
```

---

## Monitoring

### Cloudflare Dashboard

Access monitoring through the Cloudflare dashboard:

1. **Workers Metrics**
   - Navigate to Workers & Pages > Your Worker > Metrics
   - View requests, errors, CPU time, and latency

2. **Durable Objects Analytics**
   - Navigate to Workers & Pages > Durable Objects
   - View storage usage, requests, and WebSocket connections

3. **R2 Metrics**
   - Navigate to R2 > Your Bucket > Metrics
   - View storage usage, operations, and bandwidth

### Custom Metrics with Workers Analytics Engine

```typescript
import { DB } from '@dotdo/dosql';

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  ANALYTICS: AnalyticsEngineDataset;
}

export class DoSQLDatabase implements DurableObject {
  private state: DurableObjectState;
  private env: Env;
  private db: Awaited<ReturnType<typeof DB>> | null = null;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const startTime = Date.now();
    const url = new URL(request.url);

    try {
      const db = await this.getDB();
      const { sql, params } = await request.json() as {
        sql: string;
        params?: unknown[];
      };

      const result = await db.query(sql, params);
      const duration = Date.now() - startTime;

      // Record metrics
      this.env.ANALYTICS.writeDataPoint({
        blobs: [
          url.pathname,           // endpoint
          sql.split(' ')[0],      // query type (SELECT, INSERT, etc.)
          'success',              // status
        ],
        doubles: [
          duration,               // latency_ms
          result.length,          // row_count
        ],
        indexes: [
          this.state.id.toString(), // shard_id
        ],
      });

      return Response.json(result);
    } catch (error) {
      const duration = Date.now() - startTime;

      // Record error metrics
      this.env.ANALYTICS.writeDataPoint({
        blobs: [
          url.pathname,
          'unknown',
          'error',
        ],
        doubles: [duration, 0],
        indexes: [this.state.id.toString()],
      });

      throw error;
    }
  }

  private async getDB() {
    if (!this.db) {
      this.db = await DB('app', {
        storage: { hot: this.state.storage },
      });
    }
    return this.db;
  }
}
```

Configure Analytics Engine in `wrangler.jsonc`:

```jsonc
{
  "analytics_engine_datasets": [
    {
      "binding": "ANALYTICS",
      "dataset": "dosql_metrics"
    }
  ]
}
```

### Query Analytics Data

```bash
# Query metrics via GraphQL API
curl -X POST "https://api.cloudflare.com/client/v4/accounts/{account_id}/analytics_engine/sql" \
  -H "Authorization: Bearer {api_token}" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT blob1 as endpoint, AVG(double1) as avg_latency, COUNT() as requests FROM dosql_metrics WHERE timestamp > NOW() - INTERVAL '\''1'\'' HOUR GROUP BY blob1"
  }'
```

### Alerting Setup

Configure alerts in the Cloudflare dashboard:

1. **Navigate to Notifications** in your Cloudflare dashboard
2. **Create notification** for:
   - Worker errors exceeding threshold
   - High latency (p99 > 500ms)
   - Storage approaching limits
   - Request rate spikes

Example alert configurations:

```
Alert: High Error Rate
Condition: Worker error rate > 1% over 5 minutes
Action: Email + PagerDuty

Alert: High Latency
Condition: p99 latency > 500ms over 5 minutes
Action: Slack notification

Alert: Storage Warning
Condition: R2 storage > 80% of quota
Action: Email notification
```

### Structured Logging

```typescript
interface LogEntry {
  timestamp: string;
  level: 'debug' | 'info' | 'warn' | 'error';
  message: string;
  context: {
    requestId: string;
    shardId: string;
    operation: string;
    duration?: number;
    error?: string;
  };
}

function log(entry: LogEntry): void {
  console.log(JSON.stringify(entry));
}

// Usage in Durable Object
async fetch(request: Request): Promise<Response> {
  const requestId = crypto.randomUUID();
  const startTime = Date.now();

  log({
    timestamp: new Date().toISOString(),
    level: 'info',
    message: 'Request received',
    context: {
      requestId,
      shardId: this.state.id.toString(),
      operation: new URL(request.url).pathname,
    },
  });

  try {
    // ... handle request
    log({
      timestamp: new Date().toISOString(),
      level: 'info',
      message: 'Request completed',
      context: {
        requestId,
        shardId: this.state.id.toString(),
        operation: new URL(request.url).pathname,
        duration: Date.now() - startTime,
      },
    });
  } catch (error) {
    log({
      timestamp: new Date().toISOString(),
      level: 'error',
      message: 'Request failed',
      context: {
        requestId,
        shardId: this.state.id.toString(),
        operation: new URL(request.url).pathname,
        duration: Date.now() - startTime,
        error: error instanceof Error ? error.message : 'Unknown error',
      },
    });
    throw error;
  }
}
```

---

## Scaling

### Durable Object Distribution Patterns

#### Single Database Pattern

For small to medium applications with a single logical database:

```typescript
// All requests go to one DO instance
const id = env.DOSQL_DB.idFromName('main');
const stub = env.DOSQL_DB.get(id);
```

#### Tenant-Based Sharding

For multi-tenant applications:

```typescript
// Each tenant gets their own DO
function getDatabaseForTenant(env: Env, tenantId: string): DurableObjectStub {
  const id = env.DOSQL_DB.idFromName(`tenant:${tenantId}`);
  return env.DOSQL_DB.get(id);
}

// Router worker
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Extract tenant from JWT, header, or subdomain
    const tenantId = extractTenantId(request);
    const stub = getDatabaseForTenant(env, tenantId);
    return stub.fetch(request);
  },
};
```

#### Geographic Sharding

For latency-sensitive applications:

```typescript
// Route to nearest region
function getDatabaseForRegion(env: Env, region: string): DurableObjectStub {
  // Use location hints for Durable Objects
  const id = env.DOSQL_DB.idFromName(`region:${region}`);
  return env.DOSQL_DB.get(id, { locationHint: region as DurableObjectLocationHint });
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    // Get client's region from CF headers
    const region = request.cf?.region || 'wnam';
    const stub = getDatabaseForRegion(env, region);
    return stub.fetch(request);
  },
};
```

### Native Sharding Configuration

DoSQL includes native sharding support. Configure in your application:

```typescript
import {
  createShardingClient,
  createVSchema,
  shardedTable,
  referenceTable,
  hashVindex,
  consistentHashVindex,
  rangeVindex,
  shard,
  replica,
} from '@dotdo/dosql/sharding';

// Define VSchema (sharding topology)
const vschema = createVSchema(
  {
    // Users table sharded by tenant_id
    users: shardedTable('tenant_id', hashVindex({ shardCount: 4 })),

    // Orders table sharded by user_id with consistent hashing
    orders: shardedTable('user_id', consistentHashVindex({
      shardCount: 4,
      virtualNodes: 150,
    })),

    // Time-series data with range sharding
    events: shardedTable('created_at', rangeVindex({
      boundaries: [
        { value: '2024-01-01', shardId: 'events-2024-q1' },
        { value: '2024-04-01', shardId: 'events-2024-q2' },
        { value: '2024-07-01', shardId: 'events-2024-q3' },
        { value: '2024-10-01', shardId: 'events-2024-q4' },
      ],
    })),

    // Reference tables replicated to all shards
    countries: referenceTable(),
    currencies: referenceTable(),
  },
  [
    // Define shards with replicas
    shard('shard-0', 'dosql-db', [
      replica('primary', 'wnam', 'primary'),
      replica('replica-eu', 'weur', 'replica'),
    ]),
    shard('shard-1', 'dosql-db', [
      replica('primary', 'wnam', 'primary'),
      replica('replica-eu', 'weur', 'replica'),
    ]),
    shard('shard-2', 'dosql-db', [
      replica('primary', 'wnam', 'primary'),
    ]),
    shard('shard-3', 'dosql-db', [
      replica('primary', 'wnam', 'primary'),
    ]),
  ],
  {
    defaultReadPreference: 'nearest',
  }
);

// Create sharding client
const shardingClient = createShardingClient({
  vschema,
  rpc: createDoRPC(env),  // Your RPC implementation
  currentRegion: 'wnam',
});

// Execute queries (automatically routed)
const users = await shardingClient.query(
  'SELECT * FROM users WHERE tenant_id = ?',
  [123]
);
```

### Wrangler Configuration for Sharding

```jsonc
{
  "name": "dosql-sharded",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-01",

  "durable_objects": {
    "bindings": [
      // Shard 0
      { "name": "SHARD_0", "class_name": "DoSQLDatabase" },
      // Shard 1
      { "name": "SHARD_1", "class_name": "DoSQLDatabase" },
      // Shard 2
      { "name": "SHARD_2", "class_name": "DoSQLDatabase" },
      // Shard 3
      { "name": "SHARD_3", "class_name": "DoSQLDatabase" },
      // Router
      { "name": "ROUTER", "class_name": "ShardRouter" }
    ]
  },

  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["DoSQLDatabase"] },
    { "tag": "v2", "new_classes": ["ShardRouter"] }
  ]
}
```

### Load Balancing Strategies

#### Round-Robin for Read Replicas

```typescript
import { createReplicaSelector, type ReadPreference } from '@dotdo/dosql/sharding';

const replicaSelector = createReplicaSelector(vschema.shards, {
  healthCheckIntervalMs: 5000,
  unhealthyThreshold: 3,
  healthyThreshold: 2,
});

// Select replica based on preference
const replica = replicaSelector.select('shard-0', 'nearest');  // or 'primary', 'replica', 'nearest'
```

#### Weighted Load Balancing

```typescript
// Configure replica weights
const shards = [
  shard('shard-0', 'dosql-db', [
    replica('primary', 'wnam', 'primary', { weight: 1 }),
    replica('replica-1', 'weur', 'replica', { weight: 2 }),  // Handle 2x traffic
    replica('replica-2', 'apac', 'replica', { weight: 1 }),
  ]),
];
```

### Connection Pooling

While Durable Objects handle their own connections, you can implement request queuing:

```typescript
class RequestQueue {
  private queue: Array<{
    request: Request;
    resolve: (response: Response) => void;
    reject: (error: Error) => void;
  }> = [];
  private processing = false;
  private maxConcurrent = 10;
  private currentCount = 0;

  async enqueue(request: Request): Promise<Response> {
    return new Promise((resolve, reject) => {
      this.queue.push({ request, resolve, reject });
      this.processQueue();
    });
  }

  private async processQueue(): Promise<void> {
    if (this.processing || this.currentCount >= this.maxConcurrent) return;
    this.processing = true;

    while (this.queue.length > 0 && this.currentCount < this.maxConcurrent) {
      const item = this.queue.shift()!;
      this.currentCount++;

      this.handleRequest(item.request)
        .then(item.resolve)
        .catch(item.reject)
        .finally(() => {
          this.currentCount--;
          this.processQueue();
        });
    }

    this.processing = false;
  }

  private async handleRequest(request: Request): Promise<Response> {
    // Process the request
    return new Response('OK');
  }
}
```

---

## Troubleshooting

### Common Deployment Errors

#### Error: "Durable Object class not found"

**Cause:** The DO class is not exported from the entry point.

**Solution:**

```typescript
// src/index.ts - Make sure to export the DO class
export { DoSQLDatabase } from './database';

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // ...
  },
};
```

#### Error: "Migration required"

**Cause:** New DO class added without migration tag.

**Solution:** Add migration in `wrangler.jsonc`:

```jsonc
{
  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["DoSQLDatabase"] },
    { "tag": "v2", "new_sqlite_classes": ["NewDOClass"] }  // Add new migration
  ]
}
```

#### Error: "Script too large"

**Cause:** Bundle size exceeds 10MB limit (free) or 25MB (paid).

**Solution:**

```bash
# Analyze bundle size
npx wrangler deploy --dry-run --outdir dist

# Check output size
ls -lh dist/

# Use dynamic imports for large dependencies
const heavyLib = await import('./heavy-lib');
```

#### Error: "R2 bucket not found"

**Cause:** Bucket doesn't exist or binding is misconfigured.

**Solution:**

```bash
# Create the bucket
wrangler r2 bucket create dosql-data

# Verify bucket exists
wrangler r2 bucket list

# Check wrangler.jsonc binding
{
  "r2_buckets": [
    {
      "binding": "R2_STORAGE",
      "bucket_name": "dosql-data"  // Must match created bucket
    }
  ]
}
```

### Debugging Techniques

#### Local Development

```bash
# Start local dev server with Durable Objects
wrangler dev --local --persist

# Enable verbose logging
wrangler dev --log-level debug

# Test with specific data directory
wrangler dev --persist-to ./data
```

#### Remote Debugging

```bash
# Tail live logs
wrangler tail

# Tail with filters
wrangler tail --format pretty --status error
wrangler tail --search "DoSQLDatabase"

# Tail specific environment
wrangler tail --env production
```

#### Durable Object State Inspection

```typescript
// Add a debug endpoint to your DO
async fetch(request: Request): Promise<Response> {
  const url = new URL(request.url);

  if (url.pathname === '/__debug/state' && this.isDebugEnabled()) {
    const storage = this.state.storage;
    const keys = await storage.list();
    const state: Record<string, unknown> = {};

    for (const [key, value] of keys) {
      state[key] = value;
    }

    return Response.json({
      id: this.state.id.toString(),
      keyCount: keys.size,
      // Don't expose all state in production!
      state: this.env.ENVIRONMENT === 'development' ? state : undefined,
    });
  }

  // ... normal handling
}
```

#### Query Debugging

```typescript
// Enable query logging
const db = await DB('app', {
  debug: {
    logQueries: true,
    logQueryPlans: true,
    logSlowQueries: 100,  // Log queries taking > 100ms
  },
});

// Or use EXPLAIN
const plan = await db.query('EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = ?', [1]);
console.log('Query plan:', plan);
```

### Performance Issues

#### Slow Cold Starts

**Symptoms:** First request takes > 500ms

**Solutions:**

```typescript
// 1. Minimize initialization work
class DoSQLDatabase implements DurableObject {
  private db: Promise<ReturnType<typeof DB>> | null = null;

  // Lazy initialization
  private getDB(): Promise<ReturnType<typeof DB>> {
    if (!this.db) {
      this.db = DB('app', { /* config */ });
    }
    return this.db;
  }
}

// 2. Use blockConcurrencyWhile for critical init
constructor(state: DurableObjectState, env: Env) {
  this.state = state;
  this.env = env;

  // Block until critical initialization is complete
  state.blockConcurrencyWhile(async () => {
    await this.initialize();
  });
}
```

#### Memory Pressure

**Symptoms:** `OutOfMemory` errors, slow responses

**Solutions:**

```typescript
// 1. Stream large result sets
async *queryStream(sql: string, params?: unknown[]): AsyncIterable<unknown[]> {
  const db = await this.getDB();
  const cursor = await db.queryStream(sql, params);

  for await (const batch of cursor) {
    yield batch;
  }
}

// 2. Implement pagination
const pageSize = 100;
const results = await db.query(
  'SELECT * FROM large_table LIMIT ? OFFSET ?',
  [pageSize, page * pageSize]
);

// 3. Clean up unused data
await db.run('DELETE FROM temp_data WHERE created_at < ?', [cutoffDate]);
await db.run('VACUUM');  // Reclaim space
```

### Support Resources

- **Cloudflare Workers Documentation**: [developers.cloudflare.com/workers](https://developers.cloudflare.com/workers)
- **Durable Objects Documentation**: [developers.cloudflare.com/durable-objects](https://developers.cloudflare.com/durable-objects)
- **Cloudflare Discord**: [discord.cloudflare.com](https://discord.cloudflare.com)
- **DoSQL GitHub Issues**: Report bugs and request features
- **Wrangler GitHub**: [github.com/cloudflare/workers-sdk](https://github.com/cloudflare/workers-sdk)

### Health Check Endpoint

Implement a health check for monitoring:

```typescript
async fetch(request: Request): Promise<Response> {
  const url = new URL(request.url);

  if (url.pathname === '/health') {
    try {
      const db = await this.getDB();
      // Quick query to verify DB is responsive
      await db.query('SELECT 1');

      return Response.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        id: this.state.id.toString(),
      });
    } catch (error) {
      return Response.json({
        status: 'unhealthy',
        error: error instanceof Error ? error.message : 'Unknown error',
        timestamp: new Date().toISOString(),
        id: this.state.id.toString(),
      }, { status: 503 });
    }
  }

  // ... normal handling
}
```

---

## Next Steps

- [Getting Started Guide](./getting-started.md) - Basic usage and CRUD operations
- [Architecture Overview](./architecture.md) - System design and components
- [API Reference](./api-reference.md) - Complete API documentation
- [Troubleshooting Guide](./TROUBLESHOOTING.md) - Detailed error resolution
- [Security Guide](./SECURITY.md) - Security best practices
