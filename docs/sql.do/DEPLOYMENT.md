# DoSQL Deployment Guide

This guide provides step-by-step instructions for deploying DoSQL to Cloudflare Workers in production environments.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Step-by-Step Deployment Process](#step-by-step-deployment-process)
- [Wrangler Configuration](#wrangler-configuration)
- [R2 Bucket Setup](#r2-bucket-setup)
- [Production Checklist](#production-checklist)
- [Environment-Specific Deployments](#environment-specific-deployments)
- [Rollback Procedures](#rollback-procedures)

---

## Prerequisites

Before deploying, ensure you have the following:

### Account Requirements

| Requirement | Details |
|-------------|---------|
| Cloudflare Account | Sign up at [dash.cloudflare.com](https://dash.cloudflare.com) |
| Workers Paid Plan | Required for Durable Objects ($5/month minimum) |
| Account ID | Found in Workers & Pages > Overview |

### Development Tools

| Tool | Minimum Version | Installation |
|------|-----------------|--------------|
| Node.js | 18.0.0+ | [nodejs.org](https://nodejs.org) |
| npm | 9.0.0+ | Included with Node.js |
| Wrangler CLI | 3.0.0+ | `npm install -g wrangler` |

### Verify Your Setup

```bash
# Check Node.js version
node --version  # Should be >= 18.0.0

# Check Wrangler version
wrangler --version  # Should be >= 3.0.0

# Authenticate with Cloudflare
wrangler login

# Verify authentication
wrangler whoami
```

---

## Step-by-Step Deployment Process

### Step 1: Install Dependencies

```bash
# Navigate to your project directory
cd your-dosql-project

# Install DoSQL and development dependencies
npm install @dotdo/dosql
npm install wrangler @cloudflare/workers-types typescript --save-dev
```

### Step 2: Create Project Structure

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
    "types": ["@cloudflare/workers-types"],
    "lib": ["ES2022"],
    "outDir": "./dist",
    "skipLibCheck": true
  },
  "include": ["src/**/*"]
}
```

### Step 4: Create Migration Files

Create `.do/migrations/001_init.sql`:

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
```

### Step 5: Implement the Durable Object

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
      if (url.pathname === '/query' && request.method === 'POST') {
        const { sql, params } = await request.json() as {
          sql: string;
          params?: unknown[];
        };
        const result = await db.query(sql, params);
        return Response.json(result);
      }

      if (url.pathname === '/health') {
        await db.query('SELECT 1');
        return Response.json({ status: 'healthy', timestamp: new Date().toISOString() });
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

### Step 6: Deploy

```bash
# Deploy to Cloudflare
wrangler deploy

# Verify deployment
curl https://your-worker.your-subdomain.workers.dev/health
```

---

## Wrangler Configuration

### Basic Configuration (wrangler.toml)

```toml
name = "my-dosql-app"
main = "src/index.ts"
compatibility_date = "2026-01-01"
compatibility_flags = ["nodejs_compat"]

# Durable Objects bindings
[durable_objects]
bindings = [
  { name = "DOSQL_DB", class_name = "DoSQLDatabase" }
]

# Durable Object migrations (required for new DO classes)
[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQLDatabase"]
```

### Alternative: JSON Configuration (wrangler.jsonc)

```jsonc
{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "my-dosql-app",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-01",
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

### Multi-Environment Configuration

```toml
# wrangler.toml
name = "dosql-app"
main = "src/index.ts"
compatibility_date = "2026-01-01"
compatibility_flags = ["nodejs_compat"]

[durable_objects]
bindings = [
  { name = "DOSQL_DB", class_name = "DoSQLDatabase" }
]

[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQLDatabase"]

# Development (default)
[vars]
ENVIRONMENT = "development"
LOG_LEVEL = "debug"

# Staging environment
[env.staging]
name = "dosql-app-staging"
vars = { ENVIRONMENT = "staging", LOG_LEVEL = "info" }

[[env.staging.r2_buckets]]
binding = "R2_STORAGE"
bucket_name = "dosql-staging-bucket"

# Production environment
[env.production]
name = "dosql-app-production"
vars = { ENVIRONMENT = "production", LOG_LEVEL = "warn" }

[[env.production.r2_buckets]]
binding = "R2_STORAGE"
bucket_name = "dosql-production-bucket"
```

### Configuration Reference

| Setting | Required | Description |
|---------|----------|-------------|
| `name` | Yes | Worker name (appears in dashboard and deployment URL) |
| `main` | Yes | Entry point TypeScript/JavaScript file |
| `compatibility_date` | Yes | Workers runtime compatibility date |
| `compatibility_flags` | Recommended | Enable features like `nodejs_compat` |
| `durable_objects.bindings` | Yes | Durable Object namespace bindings |
| `migrations` | Yes | DO class migrations for SQLite storage |
| `r2_buckets` | Optional | R2 bucket bindings for cold storage |
| `vars` | Optional | Environment variables |

### Adding New Durable Object Classes

When adding a new Durable Object class, you must add a new migration:

```toml
# Initial migration
[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQLDatabase"]

# Adding a new class later
[[migrations]]
tag = "v2"
new_sqlite_classes = ["AnotherDatabase"]
```

---

## R2 Bucket Setup

R2 provides cold storage for DoSQL's tiered storage architecture. This is optional but recommended for production deployments.

### Step 1: Create R2 Bucket

```bash
# Create bucket for staging
wrangler r2 bucket create dosql-staging-bucket

# Create bucket for production
wrangler r2 bucket create dosql-production-bucket

# Verify buckets were created
wrangler r2 bucket list
```

### Step 2: Configure R2 Binding

Add to `wrangler.toml`:

```toml
[[r2_buckets]]
binding = "R2_STORAGE"
bucket_name = "dosql-production-bucket"

# Optional: Preview bucket for local development
# preview_bucket_name = "dosql-dev-bucket"
```

### Step 3: Update Environment Interface

```typescript
export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  R2_STORAGE: R2Bucket;  // Now required
}
```

### Step 4: Configure Storage Tiers

```typescript
const db = await DB('app', {
  migrations: { folder: '.do/migrations' },
  storage: {
    hot: this.state.storage,  // Durable Object storage (~1ms latency)
    cold: this.env.R2_STORAGE, // R2 bucket (50-100ms latency)
  },
});
```

### R2 Best Practices

| Practice | Description |
|----------|-------------|
| Separate buckets per environment | Prevents data leakage between staging and production |
| Enable lifecycle rules | Automatically clean up old data |
| Monitor storage usage | Set up alerts for quota warnings |
| Use regional buckets | Choose region closest to your users |

### R2 Lifecycle Rules (via Dashboard)

1. Navigate to R2 > Your Bucket > Settings > Object Lifecycle Rules
2. Create rules for:
   - Delete incomplete multipart uploads after 7 days
   - Transition old data to Infrequent Access after 30 days
   - Delete expired backups after retention period

---

## Production Checklist

Complete this checklist before deploying to production.

### Pre-Deployment

- [ ] **Wrangler Configuration**
  - [ ] `compatibility_date` is set to a recent date
  - [ ] `nodejs_compat` flag is enabled if using Node.js APIs
  - [ ] All Durable Object classes are exported from entry point
  - [ ] Migrations are correctly defined for all DO classes

- [ ] **Environment Setup**
  - [ ] Production environment is configured in wrangler.toml
  - [ ] Environment variables are set (not hardcoded)
  - [ ] Secrets are configured via `wrangler secret put`

- [ ] **R2 Storage**
  - [ ] Production R2 bucket is created
  - [ ] R2 binding is configured in wrangler.toml
  - [ ] Lifecycle rules are set up

- [ ] **Database Migrations**
  - [ ] All migration files are in `.do/migrations/`
  - [ ] Migrations follow naming convention (`NNN_description.sql`)
  - [ ] Migrations have been tested in staging

### Security

- [ ] **Authentication**
  - [ ] API authentication is implemented
  - [ ] API keys or JWT tokens are validated
  - [ ] Secrets are stored via `wrangler secret`

- [ ] **Input Validation**
  - [ ] SQL parameters are properly sanitized
  - [ ] Request body validation is implemented
  - [ ] Error messages don't leak internal details

- [ ] **Access Control**
  - [ ] Tenant isolation is enforced
  - [ ] Rate limiting is configured
  - [ ] CORS headers are properly set

### Secrets Management

```bash
# Add secrets for production
wrangler secret put API_KEY --env production
wrangler secret put JWT_SECRET --env production
wrangler secret put ENCRYPTION_KEY --env production

# List secrets (names only)
wrangler secret list --env production

# Delete a secret
wrangler secret delete OLD_SECRET --env production
```

### Monitoring

- [ ] **Health Checks**
  - [ ] `/health` endpoint is implemented
  - [ ] Health check queries the database
  - [ ] External uptime monitoring is configured

- [ ] **Logging**
  - [ ] Structured logging is implemented
  - [ ] Log level is appropriate for production (`warn` or `error`)
  - [ ] Request IDs are included in logs

- [ ] **Alerting**
  - [ ] Error rate alerts are configured
  - [ ] Latency alerts are configured
  - [ ] Storage usage alerts are configured

### Health Check Implementation

```typescript
async fetch(request: Request): Promise<Response> {
  const url = new URL(request.url);

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
        id: this.state.id.toString(),
      });
    } catch (error) {
      return Response.json({
        status: 'unhealthy',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : 'Unknown error',
      }, { status: 503 });
    }
  }

  // ... rest of handler
}
```

### Performance

- [ ] **Database Optimization**
  - [ ] Indexes are created for frequently queried columns
  - [ ] Queries use parameterized statements
  - [ ] Large result sets use pagination

- [ ] **Resource Limits**
  - [ ] Query timeouts are configured
  - [ ] Connection limits are appropriate
  - [ ] Bundle size is under 25MB (paid plans)

### Testing

- [ ] **Pre-Production Testing**
  - [ ] All migrations run successfully in staging
  - [ ] API endpoints are tested
  - [ ] Error handling is verified
  - [ ] Load testing has been performed

### Deployment Verification

```bash
# Deploy to production
wrangler deploy --env production

# Verify deployment
curl https://dosql-app-production.your-subdomain.workers.dev/health

# Check deployment status
wrangler deployments list --env production

# Tail production logs
wrangler tail --env production --format pretty
```

---

## Environment-Specific Deployments

### Deploy to Staging

```bash
# Deploy to staging
wrangler deploy --env staging

# Tail staging logs
wrangler tail --env staging

# Add staging secret
wrangler secret put API_KEY --env staging
```

### Deploy to Production

```bash
# Deploy to production
wrangler deploy --env production

# Tail production logs (filtered)
wrangler tail --env production --format pretty --status error

# View deployment history
wrangler deployments list --env production
```

### Package.json Scripts

```json
{
  "scripts": {
    "dev": "wrangler dev",
    "deploy:staging": "wrangler deploy --env staging",
    "deploy:production": "wrangler deploy --env production",
    "logs:staging": "wrangler tail --env staging --format pretty",
    "logs:production": "wrangler tail --env production --format pretty",
    "health:staging": "curl https://dosql-app-staging.your-subdomain.workers.dev/health",
    "health:production": "curl https://dosql-app-production.your-subdomain.workers.dev/health"
  }
}
```

---

## Rollback Procedures

### View Deployment History

```bash
# List recent deployments
wrangler deployments list --env production
```

Output:
```
Deployment ID                          Created                    Source
12345678-abcd-1234-efgh-123456789abc   2026-01-23T10:00:00.000Z   Upload
87654321-dcba-4321-hgfe-987654321cba   2026-01-22T15:30:00.000Z   Upload
```

### Rollback to Previous Version

```bash
# Rollback to a specific deployment ID
wrangler rollback --deployment-id 87654321-dcba-4321-hgfe-987654321cba --env production
```

### Emergency Rollback

If a deployment causes issues:

1. **Immediate Rollback**
   ```bash
   wrangler rollback --env production
   ```

2. **Verify Health**
   ```bash
   curl https://your-worker.workers.dev/health
   ```

3. **Check Logs**
   ```bash
   wrangler tail --env production --format pretty --status error
   ```

### Blue-Green Deployment Pattern

For zero-downtime deployments:

1. Deploy to a new worker name:
   ```bash
   wrangler deploy --name dosql-app-production-v2 --env production
   ```

2. Test the new deployment:
   ```bash
   curl https://dosql-app-production-v2.workers.dev/health
   ```

3. Update DNS/routes to point to new worker

4. Keep old worker running until verified

5. Delete old worker when confident:
   ```bash
   wrangler delete --name dosql-app-production-v1
   ```

---

## Resource Limits

### Cloudflare Workers Limits

| Resource | Free Plan | Paid Plan |
|----------|-----------|-----------|
| Worker CPU time | 10ms | 50ms |
| Worker memory | 128 MB | 128 MB |
| Bundle size | 1 MB | 10 MB (25 MB with compression) |
| Subrequests | 50 | 1000 |
| Environment variables | 64 | 128 |

### Durable Objects Limits

| Resource | Limit |
|----------|-------|
| CPU time per request | 30 seconds |
| Storage key size | 2 KB |
| Storage value size | 128 KB |
| SQLite database size | Unlimited (paid) |
| Concurrent connections | 32,768 |

### R2 Limits

| Resource | Limit |
|----------|-------|
| Object size | 5 GB (single PUT) |
| Object size (multipart) | 5 TB |
| Bucket count | 1000 per account |
| Operations | 10,000,000/month (free tier) |

---

## Troubleshooting Deployment Issues

### Common Errors

**"Durable Object class not found"**
```typescript
// Solution: Export the DO class from entry point
export { DoSQLDatabase } from './database';
```

**"Migration required"**
```toml
# Solution: Add migration tag in wrangler.toml
[[migrations]]
tag = "v2"
new_sqlite_classes = ["NewDOClass"]
```

**"R2 bucket not found"**
```bash
# Solution: Create the bucket first
wrangler r2 bucket create my-bucket
```

**"Script too large"**
```bash
# Solution: Analyze and reduce bundle size
npx wrangler deploy --dry-run --outdir dist
ls -lh dist/
```

### Debug Deployment

```bash
# Dry run to check for issues
wrangler deploy --dry-run --env production

# Check configuration
wrangler config

# Verbose logging
WRANGLER_LOG=debug wrangler deploy --env production
```

---

## Next Steps

- [Getting Started Guide](./getting-started.md) - Basic usage and CRUD operations
- [Architecture Overview](./architecture.md) - System design and components
- [API Reference](./api-reference.md) - Complete API documentation
- [Security Guide](./SECURITY.md) - Security best practices
- [Troubleshooting Guide](./TROUBLESHOOTING.md) - Detailed error resolution
