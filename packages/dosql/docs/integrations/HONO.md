# Hono Integration Guide

Build type-safe APIs with DoSQL and Hono on Cloudflare Workers. This guide covers middleware patterns, route handlers, Hono RPC, real-time features, and production deployment.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Setup](#setup)
- [Database Client](#database-client)
- [Middleware Patterns](#middleware-patterns)
- [Route Handlers](#route-handlers)
- [CRUD API Example](#crud-api-example)
- [Advanced Patterns](#advanced-patterns)
- [Hono RPC](#hono-rpc)
- [Real-Time Features](#real-time-features)
- [Error Handling](#error-handling)
- [Testing](#testing)
- [Deployment](#deployment)

---

## Overview

Hono and DoSQL are a natural pairing for Cloudflare Workers. Both are lightweight, edge-native, and type-safe by design.

### Key Benefits

| Feature | Description |
|---------|-------------|
| **Edge-Native** | Both run natively on Cloudflare Workers with zero cold starts |
| **Type-Safe** | Hono's typed routing pairs with DoSQL's typed queries |
| **Lightweight** | Combined bundle size under 50KB |
| **Multi-Runtime** | Hono works across Workers, Deno, Bun, and Node.js |
| **Middleware System** | Perfect for database connection and tenant management |

### Architecture

```
                         Client Request
                              |
                              v
+------------------------------------------------------------------+
|                    Cloudflare Worker                              |
|  +------------------------------------------------------------+  |
|  |                     Hono Application                        |  |
|  |                                                             |  |
|  |  +------------------+  +------------------+                 |  |
|  |  |  Middleware      |  |  Route Handlers  |                 |  |
|  |  |  - dbMiddleware  |  |  - /api/users    |                 |  |
|  |  |  - authMiddleware|  |  - /api/posts    |                 |  |
|  |  +--------+---------+  +--------+---------+                 |  |
|  |           |                     |                           |  |
|  |           +----------+----------+                           |  |
|  |                      |                                      |  |
|  |                      v                                      |  |
|  |            +------------------+                             |  |
|  |            |   c.get('db')    |                             |  |
|  |            +--------+---------+                             |  |
|  +------------------------------------------------------------+  |
+------------------------------------------------------------------+
                              |
              +---------------+---------------+
              |                               |
              v                               v
+------------------------+     +------------------------+
|    DoSQL Durable       |     |    DoSQL Durable       |
|    Object (Tenant A)   |     |    Object (Tenant B)   |
|  +------------------+  |     |  +------------------+  |
|  |   SQLite DB      |  |     |   SQLite DB        |  |
|  +------------------+  |     |  +------------------+  |
+------------------------+     +------------------------+
```

---

## Quick Start

Get a Hono + DoSQL API running in under 5 minutes.

```bash
npm create hono@latest my-api -- --template cloudflare-workers
cd my-api
npm install dosql
```

Create `src/index.ts`:

```typescript
import { Hono } from 'hono';
import { DB } from 'dosql';

interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

// Durable Object for your database
export class DoSQLDatabase implements DurableObject {
  private db: Awaited<ReturnType<typeof DB>> | null = null;

  constructor(private state: DurableObjectState) {}

  async fetch(request: Request): Promise<Response> {
    if (!this.db) {
      this.db = await DB('app', { storage: { hot: this.state.storage } });
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS todos (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          title TEXT NOT NULL,
          done INTEGER DEFAULT 0
        )
      `);
    }

    const url = new URL(request.url);
    const { sql, params } = await request.json() as { sql: string; params?: unknown[] };

    if (url.pathname === '/query') {
      const rows = this.db.prepare(sql).all(...(params || []));
      return Response.json(rows);
    }

    if (url.pathname === '/run') {
      const result = this.db.run(sql, params);
      return Response.json(result);
    }

    return new Response('Not Found', { status: 404 });
  }
}

// Hono app
const app = new Hono<{ Bindings: Env }>();

const query = async (env: Env, sql: string, params?: unknown[]) => {
  const stub = env.DOSQL_DB.get(env.DOSQL_DB.idFromName('default'));
  const res = await stub.fetch('http://do/query', {
    method: 'POST',
    body: JSON.stringify({ sql, params }),
  });
  return res.json();
};

app.get('/todos', async (c) => {
  const todos = await query(c.env, 'SELECT * FROM todos ORDER BY id DESC');
  return c.json(todos);
});

app.post('/todos', async (c) => {
  const { title } = await c.req.json();
  await query(c.env, 'INSERT INTO todos (title) VALUES (?)', [title]);
  return c.json({ success: true }, 201);
});

export default app;
```

Add to `wrangler.toml`:

```toml
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]
```

Run it:

```bash
npm run dev
curl -X POST http://localhost:8787/todos -H "Content-Type: application/json" -d '{"title":"Learn DoSQL"}'
curl http://localhost:8787/todos
```

---

## Setup

### Installation

```bash
npm create hono@latest my-hono-app
cd my-hono-app

# Select "cloudflare-workers" template

npm install dosql
npm install -D @cloudflare/workers-types wrangler
```

### Project Structure

```
my-hono-app/
├── .do/
│   └── migrations/
│       ├── 001_create_users.sql
│       └── 002_create_posts.sql
├── src/
│   ├── index.ts              # App entry point
│   ├── db/
│   │   ├── client.ts         # Database client
│   │   └── durable-object.ts # DoSQL Durable Object
│   ├── middleware/
│   │   ├── db.ts             # Database middleware
│   │   └── auth.ts           # Authentication
│   └── routes/
│       ├── users.ts
│       └── posts.ts
├── wrangler.toml
├── package.json
└── tsconfig.json
```

### TypeScript Configuration

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": true,
    "types": ["@cloudflare/workers-types"],
    "lib": ["ES2022"]
  },
  "include": ["src/**/*"]
}
```

### Wrangler Configuration

```toml
name = "my-hono-app"
main = "src/index.ts"
compatibility_date = "2025-01-01"

[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

# Optional: R2 for cold storage
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-data-bucket"
```

### Environment Types

```typescript
// src/types.ts
import type { DBClient } from './db/client';

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  DATA_BUCKET?: R2Bucket;
}

// Extend Hono's context variables
declare module 'hono' {
  interface ContextVariableMap {
    db: DBClient;
    tenantId: string;
  }
}
```

---

## Database Client

### Durable Object

```typescript
// src/db/durable-object.ts
import { DB } from 'dosql';

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
          cold: this.env.DATA_BUCKET,
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
        const { sql, params } = await request.json() as { sql: string; params?: unknown[] };
        const rows = await db.query(sql, params);
        return Response.json({ rows });
      }

      if (url.pathname === '/queryOne' && request.method === 'POST') {
        const { sql, params } = await request.json() as { sql: string; params?: unknown[] };
        const row = await db.queryOne(sql, params);
        return Response.json({ row });
      }

      if (url.pathname === '/run' && request.method === 'POST') {
        const { sql, params } = await request.json() as { sql: string; params?: unknown[] };
        const result = await db.run(sql, params);
        return Response.json(result);
      }

      if (url.pathname === '/transaction' && request.method === 'POST') {
        const { operations } = await request.json() as {
          operations: Array<{ type: 'query' | 'run'; sql: string; params?: unknown[] }>
        };

        const results = await db.transaction(async (tx) => {
          const txResults: unknown[] = [];
          for (const op of operations) {
            if (op.type === 'query') {
              txResults.push(await tx.query(op.sql, op.params));
            } else {
              txResults.push(await tx.run(op.sql, op.params));
            }
          }
          return txResults;
        });

        return Response.json({ results });
      }

      return new Response('Not Found', { status: 404 });
    } catch (error) {
      return Response.json({ error: (error as Error).message }, { status: 500 });
    }
  }
}
```

### Client Wrapper

```typescript
// src/db/client.ts
export interface RunResult {
  changes: number;
  lastInsertRowid: number;
}

export interface DBClient {
  query<T>(sql: string, params?: unknown[]): Promise<T[]>;
  queryOne<T>(sql: string, params?: unknown[]): Promise<T | null>;
  run(sql: string, params?: unknown[]): Promise<RunResult>;
  transaction<T>(fn: (tx: DBClient) => Promise<T>): Promise<T>;
}

export function createDBClient(stub: DurableObjectStub): DBClient {
  const rpc = async <T>(endpoint: string, body: unknown): Promise<T> => {
    const response = await stub.fetch(`http://internal${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const error = await response.json() as { error: string };
      throw new Error(error.error || 'Database error');
    }

    return response.json() as Promise<T>;
  };

  return {
    async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
      const result = await rpc<{ rows: T[] }>('/query', { sql, params });
      return result.rows;
    },

    async queryOne<T>(sql: string, params?: unknown[]): Promise<T | null> {
      const result = await rpc<{ row: T | null }>('/queryOne', { sql, params });
      return result.row;
    },

    async run(sql: string, params?: unknown[]): Promise<RunResult> {
      return rpc<RunResult>('/run', { sql, params });
    },

    async transaction<T>(fn: (tx: DBClient) => Promise<T>): Promise<T> {
      const operations: Array<{ type: 'query' | 'run'; sql: string; params?: unknown[] }> = [];

      const txClient: DBClient = {
        async query<U>(sql: string, params?: unknown[]): Promise<U[]> {
          operations.push({ type: 'query', sql, params });
          return [] as U[];
        },
        async queryOne<U>(sql: string, params?: unknown[]): Promise<U | null> {
          operations.push({ type: 'query', sql, params });
          return null;
        },
        async run(sql: string, params?: unknown[]): Promise<RunResult> {
          operations.push({ type: 'run', sql, params });
          return { changes: 0, lastInsertRowid: 0 };
        },
        async transaction<U>(innerFn: (tx: DBClient) => Promise<U>): Promise<U> {
          return innerFn(txClient);
        },
      };

      await fn(txClient);
      const result = await rpc<{ results: unknown[] }>('/transaction', { operations });
      return result.results[result.results.length - 1] as T;
    },
  };
}
```

---

## Middleware Patterns

### Database Middleware

```typescript
// src/middleware/db.ts
import { createMiddleware } from 'hono/factory';
import { createDBClient, type DBClient } from '../db/client';
import type { Env } from '../types';

export const dbMiddleware = createMiddleware<{
  Bindings: Env;
  Variables: { db: DBClient; tenantId: string };
}>(async (c, next) => {
  const tenantId = getTenantId(c);
  const id = c.env.DOSQL_DB.idFromName(tenantId);
  const stub = c.env.DOSQL_DB.get(id);
  const db = createDBClient(stub);

  c.set('db', db);
  c.set('tenantId', tenantId);

  await next();
});

function getTenantId(c: any): string {
  // From header
  const headerTenant = c.req.header('X-Tenant-ID');
  if (headerTenant) return headerTenant;

  // From subdomain
  const host = c.req.header('Host') || '';
  const subdomain = host.split('.')[0];
  if (subdomain && subdomain !== 'www' && subdomain !== 'api') {
    return subdomain;
  }

  // From path parameter
  const pathTenant = c.req.param('tenant');
  if (pathTenant) return pathTenant;

  return 'default';
}
```

### Authentication Middleware

```typescript
// src/middleware/auth.ts
import { createMiddleware } from 'hono/factory';
import { HTTPException } from 'hono/http-exception';
import type { DBClient } from '../db/client';

interface User {
  id: number;
  name: string;
  email: string;
  role: string;
}

export const authMiddleware = createMiddleware<{
  Variables: { db: DBClient; user: User };
}>(async (c, next) => {
  const authHeader = c.req.header('Authorization');

  if (!authHeader?.startsWith('Bearer ')) {
    throw new HTTPException(401, { message: 'Missing authorization header' });
  }

  const token = authHeader.slice(7);
  const db = c.get('db');

  const session = await db.queryOne<{ user_id: number; expires_at: string }>(
    'SELECT user_id, expires_at FROM sessions WHERE token = ?',
    [token]
  );

  if (!session || new Date(session.expires_at) < new Date()) {
    throw new HTTPException(401, { message: 'Invalid or expired token' });
  }

  const user = await db.queryOne<User>(
    'SELECT id, name, email, role FROM users WHERE id = ?',
    [session.user_id]
  );

  if (!user) {
    throw new HTTPException(401, { message: 'User not found' });
  }

  c.set('user', user);
  await next();
});

// Role-based access control
export const requireRole = (...roles: string[]) => {
  return createMiddleware<{ Variables: { user: User } }>(async (c, next) => {
    const user = c.get('user');

    if (!roles.includes(user.role)) {
      throw new HTTPException(403, {
        message: `Requires one of roles: ${roles.join(', ')}`,
      });
    }

    await next();
  });
};
```

### Rate Limiting Middleware

```typescript
// src/middleware/rate-limit.ts
import { createMiddleware } from 'hono/factory';
import { HTTPException } from 'hono/http-exception';
import type { DBClient } from '../db/client';

interface RateLimitConfig {
  windowMs: number;
  maxRequests: number;
  keyFn?: (c: any) => string;
}

export const rateLimit = (config: RateLimitConfig) => {
  return createMiddleware<{ Variables: { db: DBClient } }>(async (c, next) => {
    const db = c.get('db');
    const key = config.keyFn?.(c) || c.req.header('CF-Connecting-IP') || 'unknown';
    const now = Date.now();
    const windowStart = now - config.windowMs;

    const result = await db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM rate_limits WHERE key = ? AND timestamp > ?',
      [key, windowStart]
    );

    if ((result?.count || 0) >= config.maxRequests) {
      throw new HTTPException(429, { message: 'Too many requests' });
    }

    await db.run('INSERT INTO rate_limits (key, timestamp) VALUES (?, ?)', [key, now]);

    // Add rate limit headers
    c.header('X-RateLimit-Limit', String(config.maxRequests));
    c.header('X-RateLimit-Remaining', String(config.maxRequests - (result?.count || 0) - 1));

    await next();
  });
};
```

---

## Route Handlers

### Validation Options

Hono supports multiple validation approaches:

**Option 1: Zod Validator (recommended)**

```typescript
import { zValidator } from '@hono/zod-validator';
import { z } from 'zod';

const schema = z.object({
  name: z.string().min(1).max(100),
  email: z.string().email(),
});

app.post('/users', zValidator('json', schema), async (c) => {
  const { name, email } = c.req.valid('json');
  // name and email are typed and validated
});
```

**Option 2: Hono's Built-in Validator (zero dependencies)**

```typescript
import { validator } from 'hono/validator';

app.post(
  '/users',
  validator('json', (value, c) => {
    const { name, email } = value as { name?: string; email?: string };

    if (!name || name.length < 1) {
      return c.json({ error: 'Name required' }, 400);
    }
    if (!email?.includes('@')) {
      return c.json({ error: 'Valid email required' }, 400);
    }

    return { name, email };
  }),
  async (c) => {
    const { name, email } = c.req.valid('json');
  }
);
```

**Option 3: Valibot (smaller bundle)**

```typescript
import { vValidator } from '@hono/valibot-validator';
import * as v from 'valibot';

const schema = v.object({
  name: v.pipe(v.string(), v.minLength(1), v.maxLength(100)),
  email: v.pipe(v.string(), v.email()),
});

app.post('/users', vValidator('json', schema), async (c) => {
  const { name, email } = c.req.valid('json');
});
```

### User Routes

```typescript
// src/routes/users.ts
import { Hono } from 'hono';
import { zValidator } from '@hono/zod-validator';
import { z } from 'zod';
import type { DBClient } from '../db/client';

interface User {
  id: number;
  name: string;
  email: string;
  bio: string | null;
  created_at: string;
}

const createUserSchema = z.object({
  name: z.string().min(1).max(100),
  email: z.string().email(),
  bio: z.string().max(500).optional(),
});

const updateUserSchema = createUserSchema.partial();

export const usersRouter = new Hono<{ Variables: { db: DBClient } }>()
  // GET /users
  .get('/', async (c) => {
    const db = c.get('db');
    const { limit = '20', offset = '0', search } = c.req.query();

    let sql = 'SELECT * FROM users';
    const params: unknown[] = [];

    if (search) {
      sql += ' WHERE name LIKE ? OR email LIKE ?';
      params.push(`%${search}%`, `%${search}%`);
    }

    sql += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
    params.push(Math.min(parseInt(limit), 100), parseInt(offset));

    const [users, countResult] = await Promise.all([
      db.query<User>(sql, params),
      db.queryOne<{ total: number }>(
        `SELECT COUNT(*) as total FROM users${search ? ' WHERE name LIKE ? OR email LIKE ?' : ''}`,
        search ? [`%${search}%`, `%${search}%`] : []
      ),
    ]);

    return c.json({
      data: users,
      pagination: { total: countResult?.total || 0, limit: parseInt(limit), offset: parseInt(offset) },
    });
  })

  // GET /users/:id
  .get('/:id', async (c) => {
    const db = c.get('db');
    const id = parseInt(c.req.param('id'));

    const user = await db.queryOne<User>('SELECT * FROM users WHERE id = ?', [id]);

    if (!user) {
      return c.json({ error: 'User not found' }, 404);
    }

    return c.json(user);
  })

  // POST /users
  .post('/', zValidator('json', createUserSchema), async (c) => {
    const db = c.get('db');
    const { name, email, bio } = c.req.valid('json');

    try {
      const result = await db.run(
        'INSERT INTO users (name, email, bio) VALUES (?, ?, ?)',
        [name, email, bio || null]
      );

      const user = await db.queryOne<User>(
        'SELECT * FROM users WHERE id = ?',
        [result.lastInsertRowid]
      );

      return c.json(user, 201);
    } catch (error) {
      if ((error as Error).message.includes('UNIQUE constraint')) {
        return c.json({ error: 'Email already exists' }, 409);
      }
      throw error;
    }
  })

  // PATCH /users/:id
  .patch('/:id', zValidator('json', updateUserSchema), async (c) => {
    const db = c.get('db');
    const id = parseInt(c.req.param('id'));
    const updates = c.req.valid('json');

    const fields: string[] = [];
    const values: unknown[] = [];

    Object.entries(updates).forEach(([key, value]) => {
      if (value !== undefined) {
        fields.push(`${key} = ?`);
        values.push(value);
      }
    });

    if (fields.length === 0) {
      return c.json({ error: 'No fields to update' }, 400);
    }

    fields.push('updated_at = CURRENT_TIMESTAMP');
    values.push(id);

    const result = await db.run(
      `UPDATE users SET ${fields.join(', ')} WHERE id = ?`,
      values
    );

    if (result.changes === 0) {
      return c.json({ error: 'User not found' }, 404);
    }

    const user = await db.queryOne<User>('SELECT * FROM users WHERE id = ?', [id]);
    return c.json(user);
  })

  // DELETE /users/:id
  .delete('/:id', async (c) => {
    const db = c.get('db');
    const id = parseInt(c.req.param('id'));

    const result = await db.run('DELETE FROM users WHERE id = ?', [id]);

    if (result.changes === 0) {
      return c.json({ error: 'User not found' }, 404);
    }

    return c.json({ success: true });
  });
```

---

## CRUD API Example

### Complete Application

```typescript
// src/index.ts
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import { prettyJSON } from 'hono/pretty-json';
import { secureHeaders } from 'hono/secure-headers';
import { HTTPException } from 'hono/http-exception';

import { dbMiddleware } from './middleware/db';
import { authMiddleware, requireRole } from './middleware/auth';
import { rateLimit } from './middleware/rate-limit';
import { usersRouter } from './routes/users';
import { postsRouter } from './routes/posts';
import { DoSQLDatabase } from './db/durable-object';
import type { Env } from './types';
import type { DBClient } from './db/client';

export { DoSQLDatabase };

const app = new Hono<{
  Bindings: Env;
  Variables: {
    db: DBClient;
    tenantId: string;
    user: { id: number; name: string; email: string; role: string };
  };
}>();

// Global middleware
app.use('*', logger());
app.use('*', cors());
app.use('*', prettyJSON());
app.use('*', secureHeaders());
app.use('*', dbMiddleware);

// Rate limiting for API routes
app.use('/api/*', rateLimit({ windowMs: 60_000, maxRequests: 100 }));

// Health check
app.get('/health', async (c) => {
  const db = c.get('db');
  try {
    await db.queryOne('SELECT 1');
    return c.json({ status: 'healthy', timestamp: new Date().toISOString() });
  } catch (error) {
    return c.json({ status: 'unhealthy', error: (error as Error).message }, 503);
  }
});

// Public routes
app.route('/api/v1/users', usersRouter);

// Protected routes
const protectedRoutes = new Hono<{
  Variables: { db: DBClient; user: { id: number; role: string } };
}>();

protectedRoutes.use('*', authMiddleware);
protectedRoutes.route('/posts', postsRouter);

// Admin-only routes
protectedRoutes.get('/admin/stats', requireRole('admin'), async (c) => {
  const db = c.get('db');
  const stats = await db.queryOne<{ users: number; posts: number }>(`
    SELECT
      (SELECT COUNT(*) FROM users) as users,
      (SELECT COUNT(*) FROM posts) as posts
  `);
  return c.json(stats);
});

app.route('/api/v1', protectedRoutes);

// Global error handler
app.onError((err, c) => {
  console.error('Error:', err);

  if (err instanceof HTTPException) {
    return c.json({ error: err.message }, err.status);
  }

  if ((err as Error).message.includes('UNIQUE constraint')) {
    return c.json({ error: 'Resource already exists' }, 409);
  }

  return c.json({ error: 'Internal server error' }, 500);
});

// 404 handler
app.notFound((c) => c.json({ error: 'Not found' }, 404));

export default app;
```

### Database Migrations

```sql
-- .do/migrations/001_create_users.sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE NOT NULL,
  bio TEXT,
  role TEXT DEFAULT 'user',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
```

```sql
-- .do/migrations/002_create_posts.sql
CREATE TABLE posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  published INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_published ON posts(published);
```

```sql
-- .do/migrations/003_create_sessions.sql
CREATE TABLE sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  token TEXT UNIQUE NOT NULL,
  expires_at TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX idx_sessions_token ON sessions(token);
```

```sql
-- .do/migrations/004_create_rate_limits.sql
CREATE TABLE rate_limits (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  key TEXT NOT NULL,
  timestamp INTEGER NOT NULL
);

CREATE INDEX idx_rate_limits_key_timestamp ON rate_limits(key, timestamp);
```

---

## Advanced Patterns

### Multi-Tenant by Subdomain

```typescript
app.use('*', async (c, next) => {
  const host = c.req.header('Host') || '';
  const subdomain = host.split('.')[0];

  if (subdomain && subdomain !== 'www' && subdomain !== 'api') {
    const tenantId = `tenant:${subdomain}`;
    const id = c.env.DOSQL_DB.idFromName(tenantId);
    const stub = c.env.DOSQL_DB.get(id);
    c.set('db', createDBClient(stub));
    c.set('tenantId', subdomain);
  }

  await next();
});
```

### Transactions

```typescript
// src/routes/orders.ts
ordersRouter.post('/', async (c) => {
  const db = c.get('db');
  const user = c.get('user');
  const { items } = await c.req.json<{ items: Array<{ productId: number; quantity: number; price: number }> }>();

  try {
    const orderId = await db.transaction(async (tx) => {
      const total = items.reduce((sum, item) => sum + item.price * item.quantity, 0);

      const orderResult = await tx.run(
        `INSERT INTO orders (user_id, total, status) VALUES (?, ?, 'pending')`,
        [user.id, total]
      );

      for (const item of items) {
        const product = await tx.queryOne<{ stock: number }>(
          'SELECT stock FROM products WHERE id = ?',
          [item.productId]
        );

        if (!product || product.stock < item.quantity) {
          throw new Error(`Insufficient stock for product ${item.productId}`);
        }

        await tx.run(
          'INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (?, ?, ?, ?)',
          [orderResult.lastInsertRowid, item.productId, item.quantity, item.price]
        );

        await tx.run(
          'UPDATE products SET stock = stock - ? WHERE id = ?',
          [item.quantity, item.productId]
        );
      }

      return orderResult.lastInsertRowid;
    });

    return c.json({ orderId }, 201);
  } catch (error) {
    if ((error as Error).message.includes('Insufficient stock')) {
      return c.json({ error: (error as Error).message }, 400);
    }
    throw error;
  }
});
```

### Batch Operations

```typescript
bulkRouter.post('/users', async (c) => {
  const db = c.get('db');
  const { users } = await c.req.json<{ users: Array<{ name: string; email: string }> }>();

  const results = await db.transaction(async (tx) => {
    const inserted: number[] = [];
    const errors: Array<{ index: number; error: string }> = [];

    for (let i = 0; i < users.length; i++) {
      try {
        const result = await tx.run(
          'INSERT INTO users (name, email) VALUES (?, ?)',
          [users[i].name, users[i].email]
        );
        inserted.push(Number(result.lastInsertRowid));
      } catch (error) {
        errors.push({ index: i, error: (error as Error).message });
      }
    }

    return { inserted, errors };
  });

  return c.json({
    success: results.inserted.length,
    failed: results.errors.length,
    insertedIds: results.inserted,
    errors: results.errors,
  });
});
```

### Route Grouping

```typescript
// Group related routes
const api = new Hono();

// Version 1
const v1 = new Hono();
v1.route('/users', usersRouter);
v1.route('/posts', postsRouter);
v1.route('/comments', commentsRouter);

// Version 2 (with breaking changes)
const v2 = new Hono();
v2.route('/users', usersRouterV2);
v2.route('/articles', articlesRouter); // renamed from posts

api.route('/v1', v1);
api.route('/v2', v2);

app.route('/api', api);
```

---

## Hono RPC

Hono RPC provides end-to-end type safety between your API and client.

### Define Type-Safe Routes

```typescript
// src/routes/users.ts
import { Hono } from 'hono';
import { zValidator } from '@hono/zod-validator';
import { z } from 'zod';

const createUserSchema = z.object({
  name: z.string().min(1).max(100),
  email: z.string().email(),
});

// Use method chaining for type inference
export const usersApi = new Hono<{ Variables: { db: DBClient } }>()
  .get('/', async (c) => {
    const db = c.get('db');
    const users = await db.query<User>('SELECT * FROM users ORDER BY id DESC');
    return c.json({ users });
  })

  .get('/:id', async (c) => {
    const db = c.get('db');
    const id = parseInt(c.req.param('id'));
    const user = await db.queryOne<User>('SELECT * FROM users WHERE id = ?', [id]);

    if (!user) return c.json({ error: 'User not found' }, 404);
    return c.json({ user });
  })

  .post('/', zValidator('json', createUserSchema), async (c) => {
    const db = c.get('db');
    const { name, email } = c.req.valid('json');

    const result = await db.run(
      'INSERT INTO users (name, email) VALUES (?, ?)',
      [name, email]
    );

    const user = await db.queryOne<User>(
      'SELECT * FROM users WHERE id = ?',
      [result.lastInsertRowid]
    );

    return c.json({ user }, 201);
  })

  .delete('/:id', async (c) => {
    const db = c.get('db');
    const id = parseInt(c.req.param('id'));
    await db.run('DELETE FROM users WHERE id = ?', [id]);
    return c.json({ success: true });
  });

export type UsersApi = typeof usersApi;
```

### Create Main App with Type Export

```typescript
// src/index.ts
import { Hono } from 'hono';
import { dbMiddleware } from './middleware/db';
import { usersApi } from './routes/users';
import { DoSQLDatabase } from './db/durable-object';

export { DoSQLDatabase };

const app = new Hono<{ Bindings: Env }>()
  .use('*', dbMiddleware)
  .route('/api/users', usersApi);

export default app;
export type AppType = typeof app;
```

### Use the RPC Client

```typescript
// client/api.ts
import { hc } from 'hono/client';
import type { AppType } from '../src/index';

const client = hc<AppType>('https://my-api.workers.dev');

// Fully typed!
async function examples() {
  // GET /api/users
  const listRes = await client.api.users.$get();
  const { users } = await listRes.json();
  console.log(users[0].name); // TypeScript knows this is string

  // POST /api/users
  const createRes = await client.api.users.$post({
    json: { name: 'Alice', email: 'alice@example.com' },
  });
  const { user } = await createRes.json();
  console.log(user.id); // TypeScript knows this is number

  // GET /api/users/:id
  const getRes = await client.api.users[':id'].$get({
    param: { id: '1' },
  });

  // DELETE /api/users/:id
  await client.api.users[':id'].$delete({
    param: { id: '1' },
  });
}
```

### React Hook Example

```typescript
// client/hooks/useUsers.ts
import { hc } from 'hono/client';
import { useState, useEffect } from 'react';
import type { AppType } from '../../src/index';

const client = hc<AppType>(import.meta.env.VITE_API_URL);

export function useUsers() {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    client.api.users.$get()
      .then((res) => res.json())
      .then((data) => {
        setUsers(data.users);
        setLoading(false);
      });
  }, []);

  const createUser = async (name: string, email: string) => {
    const res = await client.api.users.$post({ json: { name, email } });
    const { user } = await res.json();
    setUsers((prev) => [user, ...prev]);
    return user;
  };

  const deleteUser = async (id: number) => {
    await client.api.users[':id'].$delete({ param: { id: String(id) } });
    setUsers((prev) => prev.filter((u) => u.id !== id));
  };

  return { users, loading, createUser, deleteUser };
}
```

---

## Real-Time Features

### Server-Sent Events

```typescript
// src/routes/events.ts
import { Hono } from 'hono';
import { streamSSE } from 'hono/streaming';

export const eventsRouter = new Hono<{ Variables: { db: DBClient } }>()
  .get('/stream', async (c) => {
    const db = c.get('db');
    let lastId = 0;

    return streamSSE(c, async (stream) => {
      while (true) {
        const events = await db.query<{ id: number; type: string; data: string }>(
          'SELECT * FROM events WHERE id > ? ORDER BY id ASC LIMIT 10',
          [lastId]
        );

        for (const event of events) {
          await stream.writeSSE({
            id: String(event.id),
            event: event.type,
            data: event.data,
          });
          lastId = event.id;
        }

        await stream.sleep(1000);
      }
    });
  })

  .post('/emit', async (c) => {
    const db = c.get('db');
    const { type, data } = await c.req.json<{ type: string; data: unknown }>();

    await db.run(
      'INSERT INTO events (type, data) VALUES (?, ?)',
      [type, JSON.stringify(data)]
    );

    return c.json({ success: true });
  });
```

### WebSocket Hub

```typescript
// src/durable-objects/websocket-hub.ts
import { DB } from 'dosql';

export class WebSocketHub implements DurableObject {
  private db: Awaited<ReturnType<typeof DB>> | null = null;
  private sessions = new Map<string, { socket: WebSocket; channels: Set<string> }>();
  private state: DurableObjectState;

  constructor(state: DurableObjectState) {
    this.state = state;
  }

  private async getDB() {
    if (!this.db) {
      this.db = await DB('websocket-hub', {
        storage: { hot: this.state.storage },
      });
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          channel TEXT NOT NULL,
          data TEXT NOT NULL,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel);
      `);
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/connect') {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);

      const sessionId = crypto.randomUUID();
      this.sessions.set(sessionId, { socket: server, channels: new Set() });

      server.accept();

      server.addEventListener('message', (event) => {
        this.handleMessage(sessionId, event.data as string);
      });

      server.addEventListener('close', () => {
        this.sessions.delete(sessionId);
      });

      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response('Not Found', { status: 404 });
  }

  private async handleMessage(sessionId: string, data: string) {
    const session = this.sessions.get(sessionId);
    if (!session) return;

    const message = JSON.parse(data);
    const db = await this.getDB();

    switch (message.type) {
      case 'subscribe':
        session.channels.add(message.channel);
        break;

      case 'unsubscribe':
        session.channels.delete(message.channel);
        break;

      case 'publish':
        await db.run(
          'INSERT INTO messages (channel, data) VALUES (?, ?)',
          [message.channel, JSON.stringify(message.data)]
        );

        for (const [, sess] of this.sessions) {
          if (sess.channels.has(message.channel)) {
            sess.socket.send(JSON.stringify({
              type: 'message',
              channel: message.channel,
              data: message.data,
            }));
          }
        }
        break;
    }
  }
}
```

---

## Error Handling

### Custom Error Classes

```typescript
// src/errors.ts
import { HTTPException } from 'hono/http-exception';

export class NotFoundError extends HTTPException {
  constructor(resource: string, id?: string | number) {
    const message = id ? `${resource} with id ${id} not found` : `${resource} not found`;
    super(404, { message });
  }
}

export class ConflictError extends HTTPException {
  constructor(message: string) {
    super(409, { message });
  }
}

export class ValidationError extends HTTPException {
  constructor(message: string) {
    super(400, { message });
  }
}
```

### Global Error Handler

```typescript
// src/index.ts
app.onError((err, c) => {
  console.error('Error:', {
    name: err.name,
    message: err.message,
    path: c.req.path,
    method: c.req.method,
  });

  if (err instanceof HTTPException) {
    return c.json({ error: err.message, code: err.status }, err.status);
  }

  // Handle SQLite constraint errors
  if (err.message.includes('UNIQUE constraint')) {
    return c.json({ error: 'Resource already exists', code: 'CONFLICT' }, 409);
  }

  if (err.message.includes('FOREIGN KEY constraint')) {
    return c.json({ error: 'Referenced resource not found', code: 'INVALID_REFERENCE' }, 400);
  }

  return c.json({ error: 'Internal server error' }, 500);
});
```

---

## Testing

### Test Setup

```typescript
// test/setup.ts
export function createMockDB() {
  const data: Map<string, unknown[]> = new Map();

  return {
    async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
      const table = extractTableName(sql);
      return (data.get(table) || []) as T[];
    },

    async queryOne<T>(sql: string, params?: unknown[]): Promise<T | null> {
      const rows = await this.query<T>(sql, params);
      return rows[0] || null;
    },

    async run(sql: string, params?: unknown[]) {
      const table = extractTableName(sql);
      if (sql.includes('INSERT')) {
        const existing = data.get(table) || [];
        const id = existing.length + 1;
        existing.push({ id });
        data.set(table, existing);
        return { changes: 1, lastInsertRowid: id };
      }
      return { changes: 1, lastInsertRowid: 0 };
    },

    async transaction<T>(fn: (tx: any) => Promise<T>): Promise<T> {
      return fn(this);
    },

    _seed(table: string, rows: unknown[]) {
      data.set(table, rows);
    },

    _clear() {
      data.clear();
    },
  };
}

function extractTableName(sql: string): string {
  const match = sql.match(/(?:FROM|INTO|UPDATE)\s+(\w+)/i);
  return match?.[1] || 'unknown';
}
```

### Route Tests

```typescript
// test/routes/users.test.ts
import { describe, it, expect, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { usersRouter } from '../../src/routes/users';
import { createMockDB } from '../setup';

describe('Users API', () => {
  let app: Hono;
  let mockDB: ReturnType<typeof createMockDB>;

  beforeEach(() => {
    mockDB = createMockDB();
    app = new Hono();

    app.use('*', async (c, next) => {
      c.set('db', mockDB);
      await next();
    });

    app.route('/users', usersRouter);
  });

  describe('GET /users', () => {
    it('returns empty array when no users', async () => {
      const res = await app.request('/users');
      expect(res.status).toBe(200);

      const data = await res.json();
      expect(data.data).toEqual([]);
    });

    it('returns users with pagination', async () => {
      mockDB._seed('users', [
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: 'bob@example.com' },
      ]);

      const res = await app.request('/users?limit=10&offset=0');
      expect(res.status).toBe(200);

      const data = await res.json();
      expect(data.data).toHaveLength(2);
    });
  });

  describe('POST /users', () => {
    it('creates a new user', async () => {
      const res = await app.request('/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Alice', email: 'alice@example.com' }),
      });

      expect(res.status).toBe(201);
    });

    it('returns 400 for invalid email', async () => {
      const res = await app.request('/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Alice', email: 'invalid' }),
      });

      expect(res.status).toBe(400);
    });
  });

  describe('GET /users/:id', () => {
    it('returns 404 for non-existent user', async () => {
      const res = await app.request('/users/999');
      expect(res.status).toBe(404);
    });
  });
});
```

### Integration Tests

```typescript
// test/integration/app.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { unstable_dev } from 'wrangler';
import type { UnstableDevWorker } from 'wrangler';

describe('Integration Tests', () => {
  let worker: UnstableDevWorker;

  beforeAll(async () => {
    worker = await unstable_dev('src/index.ts', {
      experimental: { disableExperimentalWarning: true },
    });
  });

  afterAll(async () => {
    await worker.stop();
  });

  it('health check returns healthy', async () => {
    const resp = await worker.fetch('/health');
    expect(resp.status).toBe(200);

    const data = await resp.json();
    expect(data.status).toBe('healthy');
  });

  it('creates and retrieves a user', async () => {
    const createResp = await worker.fetch('/api/v1/users', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'Test User', email: 'test@example.com' }),
    });

    expect(createResp.status).toBe(201);
    const created = await createResp.json();
    expect(created.id).toBeDefined();

    const getResp = await worker.fetch(`/api/v1/users/${created.id}`);
    expect(getResp.status).toBe(200);

    const user = await getResp.json();
    expect(user.name).toBe('Test User');
  });
});
```

---

## Deployment

### Production Configuration

```toml
# wrangler.toml
name = "my-hono-app"
main = "src/index.ts"
compatibility_date = "2025-01-01"

[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-data-bucket"

# Production environment
[env.production]
name = "my-hono-app-production"

[[env.production.r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-data-bucket-prod"
```

### Build Scripts

```json
{
  "scripts": {
    "dev": "wrangler dev",
    "deploy": "wrangler deploy",
    "deploy:production": "wrangler deploy --env production",
    "test": "vitest run",
    "test:integration": "vitest run --config vitest.integration.config.ts",
    "typecheck": "tsc --noEmit"
  }
}
```

### CI/CD with GitHub Actions

```yaml
# .github/workflows/deploy.yml
name: Deploy

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'
      - run: npm ci
      - run: npm run typecheck
      - run: npm test

  deploy:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'
      - run: npm ci
      - name: Deploy to Cloudflare Workers
        uses: cloudflare/wrangler-action@v3
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          command: deploy --env production
```

### Production Checklist

- [ ] Configure separate Durable Object namespaces per environment
- [ ] Set up R2 buckets for each environment
- [ ] Enable Cloudflare Workers Analytics
- [ ] Configure CORS with specific origins
- [ ] Implement rate limiting
- [ ] Add authentication to protected routes
- [ ] Test migrations in staging first
- [ ] Set up error alerting

---

## Next Steps

- [Getting Started](../getting-started.md) - DoSQL basics and setup
- [API Reference](../api-reference.md) - Complete API documentation
- [Advanced Features](../advanced.md) - Time travel, branching, CDC
- [Architecture](../architecture.md) - Understanding DoSQL internals
