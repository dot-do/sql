# DoSQL Deployment Guide

Deploy DoSQL to Cloudflare Workers with confidence. This guide covers everything from initial setup to production-ready deployments, including environment management, rollback procedures, and operational best practices.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Deployment Process](#deployment-process)
- [Wrangler Configuration](#wrangler-configuration)
- [R2 Storage Setup](#r2-storage-setup)
- [Environment Management](#environment-management)
- [Production Checklist](#production-checklist)
- [Monitoring and Health Checks](#monitoring-and-health-checks)
- [Rollback Procedures](#rollback-procedures)
- [Troubleshooting](#troubleshooting)
- [Resource Limits Reference](#resource-limits-reference)

---

## Prerequisites

### Account Requirements

| Requirement | Details |
|-------------|---------|
| Cloudflare Account | [Sign up](https://dash.cloudflare.com) if you do not have one |
| Workers Paid Plan | Required for Durable Objects ($5/month minimum) |
| Account ID | Found in Workers & Pages > Overview in the dashboard |

### Development Tools

| Tool | Minimum Version | Installation |
|------|-----------------|--------------|
| Node.js | 20.0.0+ | [nodejs.org](https://nodejs.org) |
| npm | 10.0.0+ | Included with Node.js |
| Wrangler CLI | 3.0.0+ | `npm install -g wrangler` |

### Verify Your Environment

Run these commands to confirm your setup:

```bash
# Check Node.js version (must be 20+)
node --version

# Check Wrangler version (must be 3+)
wrangler --version

# Authenticate with Cloudflare
wrangler login

# Verify authentication succeeded
wrangler whoami
```

Expected output from `wrangler whoami`:

```
 wrangler 3.x.x
--------------------
Getting User settings...
You are logged in with an OAuth Token, associated with the email: you@example.com!
```

---

## Deployment Process

### Step 1: Install Dependencies

```bash
cd your-dosql-project

# Install DoSQL
npm install dosql

# Install development dependencies
npm install -D wrangler @cloudflare/workers-types typescript
```

### Step 2: Create Project Structure

Your project should follow this structure:

```
your-dosql-project/
├── src/
│   ├── index.ts           # Worker entry point
│   └── database.ts        # DoSQL Durable Object
├── .do/
│   └── migrations/        # SQL migration files
│       └── 001_init.sql
├── wrangler.toml          # Wrangler configuration
├── tsconfig.json          # TypeScript configuration
└── package.json
```

### Step 3: Configure TypeScript

Create `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": true,
    "skipLibCheck": true,
    "types": ["@cloudflare/workers-types"],
    "lib": ["ES2022"],
    "outDir": "./dist"
  },
  "include": ["src/**/*"]
}
```

### Step 4: Create Your First Migration

Create the migrations directory and your initial schema:

```bash
mkdir -p .do/migrations
```

Create `.do/migrations/001_init.sql`:

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
```

### Step 5: Implement the Durable Object

Create `src/database.ts`:

```typescript
import { DB } from 'dosql';

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
      // Health check endpoint
      if (url.pathname === '/health') {
        const start = Date.now();
        await db.query('SELECT 1');
        return Response.json({
          status: 'healthy',
          latency_ms: Date.now() - start,
          timestamp: new Date().toISOString(),
        });
      }

      // Query endpoint
      if (url.pathname === '/query' && request.method === 'POST') {
        const { sql, params } = await request.json() as {
          sql: string;
          params?: unknown[];
        };
        const result = await db.query(sql, params);
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
    const dbId = url.searchParams.get('db') || 'default';
    const id = env.DOSQL_DB.idFromName(dbId);
    const stub = env.DOSQL_DB.get(id);
    return stub.fetch(request);
  },
};
```

### Step 6: Configure Wrangler

Create `wrangler.toml`:

```toml
name = "my-dosql-app"
main = "src/index.ts"
compatibility_date = "2024-12-01"
compatibility_flags = ["nodejs_compat"]

[durable_objects]
bindings = [
  { name = "DOSQL_DB", class_name = "DoSQLDatabase" }
]

[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQLDatabase"]
```

### Step 7: Deploy

```bash
# Deploy to Cloudflare
wrangler deploy

# Verify deployment
curl https://my-dosql-app.<your-subdomain>.workers.dev/health
```

Expected response:

```json
{"status":"healthy","latency_ms":1,"timestamp":"2024-01-15T10:30:00.000Z"}
```

---

## Wrangler Configuration

### TOML Configuration (Recommended)

```toml
# wrangler.toml
name = "my-dosql-app"
main = "src/index.ts"
compatibility_date = "2024-12-01"
compatibility_flags = ["nodejs_compat"]

# Durable Objects bindings
[durable_objects]
bindings = [
  { name = "DOSQL_DB", class_name = "DoSQLDatabase" }
]

# Durable Object migrations (required for SQLite storage)
[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQLDatabase"]

# Environment variables
[vars]
ENVIRONMENT = "development"
LOG_LEVEL = "debug"
```

### JSON Configuration (Alternative)

Create `wrangler.jsonc`:

```jsonc
{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "my-dosql-app",
  "main": "src/index.ts",
  "compatibility_date": "2024-12-01",
  "compatibility_flags": ["nodejs_compat"],

  "durable_objects": {
    "bindings": [
      { "name": "DOSQL_DB", "class_name": "DoSQLDatabase" }
    ]
  },

  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["DoSQLDatabase"] }
  ]
}
```

### Configuration Reference

| Setting | Required | Description |
|---------|----------|-------------|
| `name` | Yes | Worker name (used in deployment URL) |
| `main` | Yes | Entry point file |
| `compatibility_date` | Yes | Workers runtime version date |
| `compatibility_flags` | Recommended | Enable `nodejs_compat` for Node.js APIs |
| `durable_objects.bindings` | Yes | Durable Object namespace bindings |
| `migrations` | Yes | DO class migrations with `new_sqlite_classes` |
| `r2_buckets` | Optional | R2 bucket bindings for cold storage |
| `vars` | Optional | Environment variables |

### Adding New Durable Object Classes

When you add a new Durable Object class, create a new migration tag:

```toml
# Initial migration
[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQLDatabase"]

# Adding another class later
[[migrations]]
tag = "v2"
new_sqlite_classes = ["AnotherDatabase"]
```

Migrations are applied in order based on tags. Never modify existing migration entries.

---

## R2 Storage Setup

R2 provides cold storage for DoSQL's tiered architecture. While optional for development, it is recommended for production.

### Step 1: Create R2 Buckets

```bash
# Create buckets for each environment
wrangler r2 bucket create dosql-staging
wrangler r2 bucket create dosql-production

# Verify creation
wrangler r2 bucket list
```

### Step 2: Add R2 Binding to Wrangler

```toml
# Add to wrangler.toml
[[r2_buckets]]
binding = "R2_STORAGE"
bucket_name = "dosql-production"
```

### Step 3: Update Your Environment Interface

```typescript
export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  R2_STORAGE: R2Bucket;  // Now required
}
```

### Step 4: Configure Tiered Storage

```typescript
const db = await DB('app', {
  migrations: { folder: '.do/migrations' },
  storage: {
    hot: this.state.storage,   // DO storage (~1ms latency)
    cold: this.env.R2_STORAGE, // R2 storage (~50-100ms latency)
  },
});
```

### R2 Best Practices

| Practice | Rationale |
|----------|-----------|
| Separate buckets per environment | Prevents data leakage between staging and production |
| Enable lifecycle rules | Automatically expire old temporary data |
| Monitor storage usage | Set up alerts before hitting quotas |
| Use regional hints | Choose regions closest to your users |

---

## Environment Management

### Multi-Environment Configuration

```toml
# wrangler.toml
name = "dosql-app"
main = "src/index.ts"
compatibility_date = "2024-12-01"
compatibility_flags = ["nodejs_compat"]

[durable_objects]
bindings = [
  { name = "DOSQL_DB", class_name = "DoSQLDatabase" }
]

[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQLDatabase"]

# Default (development)
[vars]
ENVIRONMENT = "development"
LOG_LEVEL = "debug"

# Staging environment
[env.staging]
name = "dosql-app-staging"
vars = { ENVIRONMENT = "staging", LOG_LEVEL = "info" }

[[env.staging.r2_buckets]]
binding = "R2_STORAGE"
bucket_name = "dosql-staging"

# Production environment
[env.production]
name = "dosql-app-production"
vars = { ENVIRONMENT = "production", LOG_LEVEL = "warn" }

[[env.production.r2_buckets]]
binding = "R2_STORAGE"
bucket_name = "dosql-production"
```

### Deploy to Each Environment

```bash
# Development (default)
wrangler deploy

# Staging
wrangler deploy --env staging

# Production
wrangler deploy --env production
```

### Manage Secrets

Secrets are encrypted environment variables that never appear in logs or the dashboard.

```bash
# Add secrets
wrangler secret put API_KEY --env production
wrangler secret put JWT_SECRET --env production

# List secrets (shows names only, not values)
wrangler secret list --env production

# Delete a secret
wrangler secret delete OLD_SECRET --env production
```

### Package.json Scripts

Add these scripts for convenience:

```json
{
  "scripts": {
    "dev": "wrangler dev",
    "deploy": "wrangler deploy",
    "deploy:staging": "wrangler deploy --env staging",
    "deploy:production": "wrangler deploy --env production",
    "logs": "wrangler tail --format pretty",
    "logs:staging": "wrangler tail --env staging --format pretty",
    "logs:production": "wrangler tail --env production --format pretty"
  }
}
```

---

## Production Checklist

Complete this checklist before deploying to production.

### Configuration

- [ ] `compatibility_date` is set to a recent stable date
- [ ] `nodejs_compat` flag is enabled (if using Node.js APIs)
- [ ] All Durable Object classes are exported from the entry point
- [ ] Migrations are correctly defined with `new_sqlite_classes`
- [ ] Production environment is configured in `wrangler.toml`
- [ ] R2 bucket is created and bound for cold storage

### Security

- [ ] API authentication is implemented (API keys, JWT, or OAuth)
- [ ] Secrets are stored via `wrangler secret put` (never hardcoded)
- [ ] Input validation is implemented for all endpoints
- [ ] SQL queries use parameterized statements (never string interpolation)
- [ ] Error responses do not leak internal details
- [ ] CORS headers are properly configured
- [ ] Rate limiting is implemented

### Database

- [ ] All migration files are in `.do/migrations/`
- [ ] Migrations follow naming convention (`NNN_description.sql`)
- [ ] Migrations have been tested in staging
- [ ] Indexes are created for frequently queried columns
- [ ] Large result sets use pagination

### Monitoring

- [ ] `/health` endpoint is implemented and tested
- [ ] External uptime monitoring is configured (e.g., Cloudflare Health Checks)
- [ ] Log level is set to `warn` or `error` for production
- [ ] Error alerting is configured

### Testing

- [ ] All migrations run successfully in staging
- [ ] API endpoints are tested with representative data
- [ ] Error handling paths are verified
- [ ] Load testing has been performed (if expecting high traffic)

### Deployment Verification

After deploying, verify with these commands:

```bash
# Deploy
wrangler deploy --env production

# Verify health
curl https://dosql-app-production.<subdomain>.workers.dev/health

# Check deployment status
wrangler deployments list --env production

# Monitor logs (filter for errors)
wrangler tail --env production --format pretty --status error
```

---

## Monitoring and Health Checks

### Health Check Implementation

A robust health check should verify database connectivity:

```typescript
if (url.pathname === '/health') {
  try {
    const db = await this.getDB();
    const start = Date.now();
    await db.query('SELECT 1');
    const latency = Date.now() - start;

    return Response.json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      latency_ms: latency,
      instance_id: this.state.id.toString(),
    });
  } catch (error) {
    return Response.json({
      status: 'unhealthy',
      timestamp: new Date().toISOString(),
      error: error instanceof Error ? error.message : 'Unknown error',
    }, { status: 503 });
  }
}
```

### Setting Up Cloudflare Health Checks

1. Go to the Cloudflare dashboard
2. Navigate to Traffic > Health Checks
3. Create a new health check:
   - **URL**: `https://your-worker.workers.dev/health`
   - **Method**: GET
   - **Expected response codes**: 200
   - **Interval**: 60 seconds
   - **Retries**: 2

### Viewing Logs

```bash
# Real-time logs
wrangler tail --env production --format pretty

# Filter by status
wrangler tail --env production --format pretty --status error

# Filter by search term
wrangler tail --env production --format pretty --search "database"
```

---

## Rollback Procedures

### View Deployment History

```bash
wrangler deployments list --env production
```

Output:

```
Deployment ID                          Created                    Source
12345678-abcd-1234-efgh-123456789abc   2024-01-15T10:00:00.000Z   Upload
87654321-dcba-4321-hgfe-987654321cba   2024-01-14T15:30:00.000Z   Upload
```

### Rollback to Previous Version

```bash
# Rollback to the most recent previous deployment
wrangler rollback --env production

# Rollback to a specific deployment
wrangler rollback --deployment-id 87654321-dcba-4321-hgfe-987654321cba --env production
```

### Emergency Rollback Procedure

If a deployment causes issues:

1. **Rollback immediately**:
   ```bash
   wrangler rollback --env production
   ```

2. **Verify recovery**:
   ```bash
   curl https://your-worker.workers.dev/health
   ```

3. **Check error logs**:
   ```bash
   wrangler tail --env production --format pretty --status error
   ```

4. **Investigate** the failed deployment before redeploying.

### Blue-Green Deployments

For zero-downtime deployments with additional safety:

1. Deploy to a new worker name:
   ```bash
   wrangler deploy --name dosql-app-production-v2 --env production
   ```

2. Test the new deployment thoroughly:
   ```bash
   curl https://dosql-app-production-v2.workers.dev/health
   ```

3. Update your DNS or route configuration to point to the new worker.

4. Keep the old worker running until you are confident in the new deployment.

5. Delete the old worker when ready:
   ```bash
   wrangler delete --name dosql-app-production-v1
   ```

---

## Troubleshooting

### "Durable Object class not found"

The Durable Object class must be exported from your entry point:

```typescript
// src/index.ts
export { DoSQLDatabase } from './database';  // This export is required
```

### "Migration required"

Add a migration tag in `wrangler.toml` for new DO classes:

```toml
[[migrations]]
tag = "v2"
new_sqlite_classes = ["NewDOClass"]
```

### "R2 bucket not found"

Create the bucket before deploying:

```bash
wrangler r2 bucket create my-bucket
```

### "Script too large"

Analyze your bundle to identify large dependencies:

```bash
wrangler deploy --dry-run --outdir dist
ls -lh dist/
```

Consider removing unused dependencies or using dynamic imports.

### Debug Deployment Issues

```bash
# Dry run to check for errors without deploying
wrangler deploy --dry-run --env production

# Enable verbose logging
WRANGLER_LOG=debug wrangler deploy --env production
```

---

## Resource Limits Reference

### Cloudflare Workers Limits

| Resource | Free Plan | Paid Plan |
|----------|-----------|-----------|
| CPU time per request | 10ms | 50ms |
| Memory | 128 MB | 128 MB |
| Bundle size | 1 MB | 10 MB (25 MB compressed) |
| Subrequests per request | 50 | 1,000 |
| Environment variables | 64 | 128 |

### Durable Objects Limits

| Resource | Limit |
|----------|-------|
| CPU time per request | 30 seconds |
| Memory | 128 MB |
| Storage key size | 2 KB |
| Storage value size | 128 KB |
| SQLite database size | Unlimited (paid) |
| Concurrent WebSocket connections | 32,768 |

### R2 Limits

| Resource | Limit |
|----------|-------|
| Object size (single PUT) | 5 GB |
| Object size (multipart) | 5 TB |
| Buckets per account | 1,000 |
| Free tier operations | 10,000,000/month |

---

## Next Steps

- [Getting Started Guide](./getting-started.md) - Basic usage and CRUD operations
- [Architecture Overview](./architecture.md) - System design and components
- [API Reference](./api-reference.md) - Complete API documentation
- [Security Guide](./SECURITY.md) - Authentication and authorization patterns
- [Troubleshooting Guide](./TROUBLESHOOTING.md) - Detailed error resolution
