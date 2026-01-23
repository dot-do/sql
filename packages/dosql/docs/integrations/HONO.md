# DoSQL + Hono Integration Guide

A comprehensive guide for integrating DoSQL with Hono, the ultrafast web framework for Cloudflare Workers. This guide covers middleware patterns, route handlers, and production-ready examples.

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Basic Usage](#basic-usage)
- [Middleware Patterns](#middleware-patterns)
- [Route Handlers](#route-handlers)
- [CRUD API Example](#crud-api-example)
- [Advanced Patterns](#advanced-patterns)
- [Real-time Features](#real-time-features)
- [Error Handling](#error-handling)
- [Testing](#testing)
- [Deployment](#deployment)

---

## Overview

### Why DoSQL + Hono

Hono and DoSQL are a natural pairing for Cloudflare Workers:

| Hono Feature | DoSQL Benefit |
|--------------|---------------|
| **Edge-first design** | Both run natively on Cloudflare Workers |
| **Type-safe routing** | DoSQL provides type-safe SQL queries |
| **Middleware system** | Perfect for database connection management |
| **Multi-runtime** | DoSQL's RPC client works across all Hono targets |
| **Lightweight** | Combined bundle size under 50KB |

### Architecture

```
+------------------------------------------------------------------+
|                    HONO + DOSQL ARCHITECTURE                      |
+------------------------------------------------------------------+

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
|  |   SQLite DB      |  |     |   SQLite DB      |    |
|  +------------------+  |     |  +------------------+  |
+------------------------+     +------------------------+
```

---

## Setup

### Installation

```bash
# Create a new Hono project
npm create hono@latest my-hono-app
cd my-hono-app

# Select "cloudflare-workers" as the template

# Install DoSQL
npm install @dotdo/dosql

# Install development dependencies
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
│   ├── index.ts           # Hono app entry point
│   ├── db/
│   │   ├── client.ts      # Database client utilities
│   │   └── schema.ts      # Type definitions
│   ├── middleware/
│   │   ├── db.ts          # Database middleware
│   │   └── auth.ts        # Authentication middleware
│   ├── routes/
│   │   ├── users.ts       # User routes
│   │   └── posts.ts       # Post routes
│   └── durable-objects/
│       └── database.ts    # DoSQL Durable Object
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
# wrangler.toml
name = "my-hono-app"
main = "src/index.ts"
compatibility_date = "2024-01-01"

# DoSQL Durable Object binding
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

# Durable Object migrations
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
import type { Database } from '@dotdo/dosql';

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  DATA_BUCKET?: R2Bucket;
}

// Extend Hono's context variables
declare module 'hono' {
  interface ContextVariableMap {
    db: Database;
    tenantId: string;
  }
}
```

---

## Basic Usage

### Durable Object Setup

```typescript
// src/durable-objects/database.ts
import { DB, type Database } from '@dotdo/dosql';

export class DoSQLDatabase implements DurableObject {
  private db: Database | null = null;
  private state: DurableObjectState;
  private env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  private async getDB(): Promise<Database> {
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
      return Response.json(
        { error: (error as Error).message },
        { status: 500 }
      );
    }
  }
}
```

### Database Client

```typescript
// src/db/client.ts
import type { Database, RunResult } from '@dotdo/dosql';

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
      // Collect operations and send as batch
      const operations: Array<{ type: 'query' | 'run'; sql: string; params?: unknown[] }> = [];

      const txClient: DBClient = {
        async query<U>(sql: string, params?: unknown[]): Promise<U[]> {
          operations.push({ type: 'query', sql, params });
          return [] as U[]; // Placeholder - actual results come from batch
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
          // Nested transactions become part of the same batch
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

### Simple Hono App

```typescript
// src/index.ts
import { Hono } from 'hono';
import { createDBClient, type DBClient } from './db/client';
import { DoSQLDatabase } from './durable-objects/database';
import type { Env } from './types';

// Export the Durable Object class
export { DoSQLDatabase };

// Create Hono app with type-safe environment
const app = new Hono<{ Bindings: Env; Variables: { db: DBClient } }>();

// Database middleware
app.use('*', async (c, next) => {
  const tenantId = c.req.header('X-Tenant-ID') || 'default';
  const id = c.env.DOSQL_DB.idFromName(tenantId);
  const stub = c.env.DOSQL_DB.get(id);
  const db = createDBClient(stub);
  c.set('db', db);
  await next();
});

// Routes
app.get('/users', async (c) => {
  const db = c.get('db');
  const users = await db.query('SELECT * FROM users ORDER BY created_at DESC');
  return c.json(users);
});

app.post('/users', async (c) => {
  const db = c.get('db');
  const { name, email } = await c.req.json();

  const result = await db.run(
    'INSERT INTO users (name, email) VALUES (?, ?)',
    [name, email]
  );

  return c.json({ id: result.lastInsertRowid, name, email }, 201);
});

export default app;
```

---

## Middleware Patterns

### Database Middleware

```typescript
// src/middleware/db.ts
import { createMiddleware } from 'hono/factory';
import { createDBClient, type DBClient } from '../db/client';
import type { Env } from '../types';

// Type-safe middleware for database access
export const dbMiddleware = createMiddleware<{
  Bindings: Env;
  Variables: { db: DBClient; tenantId: string };
}>(async (c, next) => {
  // Extract tenant ID from header, subdomain, or path
  const tenantId = getTenantId(c);

  // Get Durable Object stub for this tenant
  const id = c.env.DOSQL_DB.idFromName(tenantId);
  const stub = c.env.DOSQL_DB.get(id);

  // Create database client
  const db = createDBClient(stub);

  // Set variables on context
  c.set('db', db);
  c.set('tenantId', tenantId);

  await next();
});

function getTenantId(c: any): string {
  // Option 1: From header
  const headerTenant = c.req.header('X-Tenant-ID');
  if (headerTenant) return headerTenant;

  // Option 2: From subdomain
  const host = c.req.header('Host') || '';
  const subdomain = host.split('.')[0];
  if (subdomain && subdomain !== 'www' && subdomain !== 'api') {
    return subdomain;
  }

  // Option 3: From path parameter
  const pathTenant = c.req.param('tenant');
  if (pathTenant) return pathTenant;

  // Default tenant
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

  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    throw new HTTPException(401, { message: 'Missing authorization header' });
  }

  const token = authHeader.slice(7);
  const db = c.get('db');

  // Look up session and user
  const session = await db.queryOne<{ user_id: number; expires_at: string }>(
    'SELECT user_id, expires_at FROM sessions WHERE token = ?',
    [token]
  );

  if (!session) {
    throw new HTTPException(401, { message: 'Invalid token' });
  }

  if (new Date(session.expires_at) < new Date()) {
    throw new HTTPException(401, { message: 'Token expired' });
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

// Role-based access control middleware
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

### Request Logging Middleware

```typescript
// src/middleware/logging.ts
import { createMiddleware } from 'hono/factory';
import type { DBClient } from '../db/client';

export const loggingMiddleware = createMiddleware<{
  Variables: { db: DBClient; tenantId: string };
}>(async (c, next) => {
  const start = Date.now();
  const requestId = crypto.randomUUID();

  // Add request ID to response headers
  c.header('X-Request-ID', requestId);

  await next();

  const duration = Date.now() - start;
  const db = c.get('db');
  const tenantId = c.get('tenantId');

  // Log request to database (fire-and-forget)
  db.run(
    `INSERT INTO request_logs (request_id, tenant_id, method, path, status, duration_ms, created_at)
     VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
    [requestId, tenantId, c.req.method, c.req.path, c.res.status, duration]
  ).catch(console.error);
});
```

### Rate Limiting Middleware

```typescript
// src/middleware/rateLimit.ts
import { createMiddleware } from 'hono/factory';
import { HTTPException } from 'hono/http-exception';
import type { DBClient } from '../db/client';

interface RateLimitConfig {
  windowMs: number;    // Time window in milliseconds
  maxRequests: number; // Max requests per window
  keyFn?: (c: any) => string; // Custom key extraction
}

export const rateLimit = (config: RateLimitConfig) => {
  const { windowMs, maxRequests, keyFn } = config;

  return createMiddleware<{ Variables: { db: DBClient } }>(async (c, next) => {
    const db = c.get('db');
    const key = keyFn ? keyFn(c) : c.req.header('CF-Connecting-IP') || 'unknown';
    const now = Date.now();
    const windowStart = now - windowMs;

    // Count requests in current window
    const result = await db.queryOne<{ count: number }>(
      `SELECT COUNT(*) as count FROM rate_limits
       WHERE key = ? AND timestamp > ?`,
      [key, windowStart]
    );

    const currentCount = result?.count || 0;

    if (currentCount >= maxRequests) {
      throw new HTTPException(429, {
        message: 'Too many requests',
      });
    }

    // Record this request
    await db.run(
      'INSERT INTO rate_limits (key, timestamp) VALUES (?, ?)',
      [key, now]
    );

    // Cleanup old entries periodically
    if (Math.random() < 0.01) {
      db.run('DELETE FROM rate_limits WHERE timestamp < ?', [windowStart])
        .catch(console.error);
    }

    // Add rate limit headers
    c.header('X-RateLimit-Limit', String(maxRequests));
    c.header('X-RateLimit-Remaining', String(maxRequests - currentCount - 1));
    c.header('X-RateLimit-Reset', String(Math.ceil((windowStart + windowMs) / 1000)));

    await next();
  });
};
```

---

## Route Handlers

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

// Validation schemas
const createUserSchema = z.object({
  name: z.string().min(1).max(100),
  email: z.string().email(),
  bio: z.string().max(500).optional(),
});

const updateUserSchema = z.object({
  name: z.string().min(1).max(100).optional(),
  email: z.string().email().optional(),
  bio: z.string().max(500).nullable().optional(),
});

const usersRouter = new Hono<{ Variables: { db: DBClient } }>();

// GET /users - List all users with pagination
usersRouter.get('/', async (c) => {
  const db = c.get('db');
  const { limit = '20', offset = '0', search } = c.req.query();

  const limitNum = Math.min(parseInt(limit, 10), 100);
  const offsetNum = parseInt(offset, 10);

  let sql = 'SELECT * FROM users';
  const params: unknown[] = [];

  if (search) {
    sql += ' WHERE name LIKE ? OR email LIKE ?';
    params.push(`%${search}%`, `%${search}%`);
  }

  sql += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
  params.push(limitNum, offsetNum);

  const [users, countResult] = await Promise.all([
    db.query<User>(sql, params),
    db.queryOne<{ total: number }>(
      `SELECT COUNT(*) as total FROM users${search ? ' WHERE name LIKE ? OR email LIKE ?' : ''}`,
      search ? [`%${search}%`, `%${search}%`] : []
    ),
  ]);

  return c.json({
    data: users,
    pagination: {
      total: countResult?.total || 0,
      limit: limitNum,
      offset: offsetNum,
    },
  });
});

// GET /users/:id - Get single user
usersRouter.get('/:id', async (c) => {
  const db = c.get('db');
  const id = parseInt(c.req.param('id'), 10);

  const user = await db.queryOne<User>(
    'SELECT * FROM users WHERE id = ?',
    [id]
  );

  if (!user) {
    return c.json({ error: 'User not found' }, 404);
  }

  return c.json(user);
});

// POST /users - Create user
usersRouter.post('/', zValidator('json', createUserSchema), async (c) => {
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
});

// PATCH /users/:id - Update user
usersRouter.patch('/:id', zValidator('json', updateUserSchema), async (c) => {
  const db = c.get('db');
  const id = parseInt(c.req.param('id'), 10);
  const updates = c.req.valid('json');

  // Build dynamic update query
  const fields: string[] = [];
  const values: unknown[] = [];

  if (updates.name !== undefined) {
    fields.push('name = ?');
    values.push(updates.name);
  }
  if (updates.email !== undefined) {
    fields.push('email = ?');
    values.push(updates.email);
  }
  if (updates.bio !== undefined) {
    fields.push('bio = ?');
    values.push(updates.bio);
  }

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

  const user = await db.queryOne<User>(
    'SELECT * FROM users WHERE id = ?',
    [id]
  );

  return c.json(user);
});

// DELETE /users/:id - Delete user
usersRouter.delete('/:id', async (c) => {
  const db = c.get('db');
  const id = parseInt(c.req.param('id'), 10);

  const result = await db.run('DELETE FROM users WHERE id = ?', [id]);

  if (result.changes === 0) {
    return c.json({ error: 'User not found' }, 404);
  }

  return c.json({ success: true });
});

export { usersRouter };
```

### Post Routes with Relationships

```typescript
// src/routes/posts.ts
import { Hono } from 'hono';
import { zValidator } from '@hono/zod-validator';
import { z } from 'zod';
import type { DBClient } from '../db/client';

interface Post {
  id: number;
  user_id: number;
  title: string;
  content: string;
  published: boolean;
  created_at: string;
  updated_at: string;
}

interface PostWithAuthor extends Post {
  author_name: string;
  author_email: string;
}

const createPostSchema = z.object({
  title: z.string().min(1).max(200),
  content: z.string().min(1),
  published: z.boolean().default(false),
});

interface User {
  id: number;
}

const postsRouter = new Hono<{ Variables: { db: DBClient; user: User } }>();

// GET /posts - List published posts
postsRouter.get('/', async (c) => {
  const db = c.get('db');
  const { limit = '20', offset = '0' } = c.req.query();

  const posts = await db.query<PostWithAuthor>(
    `SELECT p.*, u.name as author_name, u.email as author_email
     FROM posts p
     JOIN users u ON u.id = p.user_id
     WHERE p.published = 1
     ORDER BY p.created_at DESC
     LIMIT ? OFFSET ?`,
    [parseInt(limit, 10), parseInt(offset, 10)]
  );

  return c.json(posts);
});

// GET /posts/:id - Get single post with author
postsRouter.get('/:id', async (c) => {
  const db = c.get('db');
  const id = parseInt(c.req.param('id'), 10);

  const post = await db.queryOne<PostWithAuthor>(
    `SELECT p.*, u.name as author_name, u.email as author_email
     FROM posts p
     JOIN users u ON u.id = p.user_id
     WHERE p.id = ?`,
    [id]
  );

  if (!post) {
    return c.json({ error: 'Post not found' }, 404);
  }

  // Don't show unpublished posts to non-authors
  const user = c.get('user');
  if (!post.published && post.user_id !== user?.id) {
    return c.json({ error: 'Post not found' }, 404);
  }

  return c.json(post);
});

// POST /posts - Create post (requires auth)
postsRouter.post('/', zValidator('json', createPostSchema), async (c) => {
  const db = c.get('db');
  const user = c.get('user');
  const { title, content, published } = c.req.valid('json');

  const result = await db.run(
    `INSERT INTO posts (user_id, title, content, published, created_at, updated_at)
     VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
    [user.id, title, content, published ? 1 : 0]
  );

  const post = await db.queryOne<Post>(
    'SELECT * FROM posts WHERE id = ?',
    [result.lastInsertRowid]
  );

  return c.json(post, 201);
});

// PUT /posts/:id/publish - Publish a post
postsRouter.put('/:id/publish', async (c) => {
  const db = c.get('db');
  const user = c.get('user');
  const id = parseInt(c.req.param('id'), 10);

  // Check ownership
  const post = await db.queryOne<Post>(
    'SELECT * FROM posts WHERE id = ?',
    [id]
  );

  if (!post) {
    return c.json({ error: 'Post not found' }, 404);
  }

  if (post.user_id !== user.id) {
    return c.json({ error: 'Not authorized' }, 403);
  }

  await db.run(
    'UPDATE posts SET published = 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
    [id]
  );

  return c.json({ success: true, published: true });
});

export { postsRouter };
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
import { rateLimit } from './middleware/rateLimit';
import { usersRouter } from './routes/users';
import { postsRouter } from './routes/posts';
import { DoSQLDatabase } from './durable-objects/database';
import type { Env } from './types';
import type { DBClient } from './db/client';

// Export Durable Object
export { DoSQLDatabase };

// Create app with typed bindings and variables
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

// Database middleware for all routes
app.use('*', dbMiddleware);

// Rate limiting for API routes
app.use('/api/*', rateLimit({
  windowMs: 60 * 1000, // 1 minute
  maxRequests: 100,
}));

// Health check (no auth required)
app.get('/health', async (c) => {
  const db = c.get('db');

  try {
    await db.queryOne('SELECT 1');
    return c.json({ status: 'healthy', timestamp: new Date().toISOString() });
  } catch (error) {
    return c.json(
      { status: 'unhealthy', error: (error as Error).message },
      503
    );
  }
});

// Public routes
app.route('/api/v1/users', usersRouter);

// Protected routes (require authentication)
const protectedRoutes = new Hono<{
  Variables: {
    db: DBClient;
    user: { id: number; name: string; email: string; role: string };
  };
}>();

protectedRoutes.use('*', authMiddleware);
protectedRoutes.route('/posts', postsRouter);

// Admin-only routes
protectedRoutes.get('/admin/stats', requireRole('admin'), async (c) => {
  const db = c.get('db');

  const stats = await db.queryOne<{
    users: number;
    posts: number;
    published_posts: number;
  }>(`
    SELECT
      (SELECT COUNT(*) FROM users) as users,
      (SELECT COUNT(*) FROM posts) as posts,
      (SELECT COUNT(*) FROM posts WHERE published = 1) as published_posts
  `);

  return c.json(stats);
});

app.route('/api/v1', protectedRoutes);

// Global error handler
app.onError((err, c) => {
  console.error('Error:', err);

  if (err instanceof HTTPException) {
    return c.json(
      { error: err.message },
      err.status
    );
  }

  return c.json(
    { error: 'Internal server error' },
    500
  );
});

// 404 handler
app.notFound((c) => {
  return c.json({ error: 'Not found' }, 404);
});

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
CREATE INDEX idx_users_role ON users(role);
```

```sql
-- .do/migrations/002_create_posts.sql
CREATE TABLE posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  published BOOLEAN DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_published ON posts(published);
CREATE INDEX idx_posts_created_at ON posts(created_at);
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
CREATE INDEX idx_sessions_user_id ON sessions(user_id);
CREATE INDEX idx_sessions_expires_at ON sessions(expires_at);
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

### Multi-Tenant Application

```typescript
// src/index.ts
import { Hono } from 'hono';
import { dbMiddleware } from './middleware/db';
import type { Env } from './types';

const app = new Hono<{ Bindings: Env }>();

// Tenant isolation via subdomain
app.use('*', async (c, next) => {
  const host = c.req.header('Host') || '';
  const subdomain = host.split('.')[0];

  // Validate tenant exists
  if (subdomain && subdomain !== 'www' && subdomain !== 'api') {
    // Each tenant gets their own Durable Object
    const tenantId = `tenant:${subdomain}`;
    const id = c.env.DOSQL_DB.idFromName(tenantId);
    const stub = c.env.DOSQL_DB.get(id);
    c.set('db', createDBClient(stub));
    c.set('tenantId', subdomain);
  }

  await next();
});

// Tenant-specific routes
app.get('/api/settings', async (c) => {
  const db = c.get('db');
  const tenantId = c.get('tenantId');

  const settings = await db.queryOne(
    'SELECT * FROM tenant_settings WHERE tenant_id = ?',
    [tenantId]
  );

  return c.json(settings);
});

export default app;
```

### Transaction Patterns

```typescript
// src/routes/orders.ts
import { Hono } from 'hono';
import type { DBClient } from '../db/client';

interface OrderItem {
  productId: number;
  quantity: number;
  price: number;
}

const ordersRouter = new Hono<{
  Variables: { db: DBClient; user: { id: number } };
}>();

// Create order with transaction
ordersRouter.post('/', async (c) => {
  const db = c.get('db');
  const user = c.get('user');
  const { items } = await c.req.json<{ items: OrderItem[] }>();

  try {
    const orderId = await db.transaction(async (tx) => {
      // Calculate total
      const total = items.reduce((sum, item) => sum + item.price * item.quantity, 0);

      // Create order
      const orderResult = await tx.run(
        `INSERT INTO orders (user_id, total, status, created_at)
         VALUES (?, ?, 'pending', CURRENT_TIMESTAMP)`,
        [user.id, total]
      );

      const newOrderId = orderResult.lastInsertRowid;

      // Create order items and update inventory
      for (const item of items) {
        // Check stock
        const product = await tx.queryOne<{ stock: number }>(
          'SELECT stock FROM products WHERE id = ?',
          [item.productId]
        );

        if (!product || product.stock < item.quantity) {
          throw new Error(`Insufficient stock for product ${item.productId}`);
        }

        // Create order item
        await tx.run(
          `INSERT INTO order_items (order_id, product_id, quantity, price)
           VALUES (?, ?, ?, ?)`,
          [newOrderId, item.productId, item.quantity, item.price]
        );

        // Deduct from inventory
        await tx.run(
          'UPDATE products SET stock = stock - ? WHERE id = ?',
          [item.quantity, item.productId]
        );
      }

      return newOrderId;
    });

    // Fetch complete order
    const order = await db.queryOne(
      `SELECT o.*,
              json_group_array(json_object(
                'productId', oi.product_id,
                'quantity', oi.quantity,
                'price', oi.price
              )) as items
       FROM orders o
       JOIN order_items oi ON oi.order_id = o.id
       WHERE o.id = ?
       GROUP BY o.id`,
      [orderId]
    );

    return c.json(order, 201);
  } catch (error) {
    if ((error as Error).message.includes('Insufficient stock')) {
      return c.json({ error: (error as Error).message }, 400);
    }
    throw error;
  }
});

export { ordersRouter };
```

### Batch Operations

```typescript
// src/routes/bulk.ts
import { Hono } from 'hono';
import type { DBClient } from '../db/client';

const bulkRouter = new Hono<{ Variables: { db: DBClient } }>();

// Bulk insert users
bulkRouter.post('/users', async (c) => {
  const db = c.get('db');
  const { users } = await c.req.json<{
    users: Array<{ name: string; email: string }>;
  }>();

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

// Bulk update
bulkRouter.patch('/users/status', async (c) => {
  const db = c.get('db');
  const { userIds, status } = await c.req.json<{
    userIds: number[];
    status: string;
  }>();

  const placeholders = userIds.map(() => '?').join(', ');
  const result = await db.run(
    `UPDATE users SET status = ?, updated_at = CURRENT_TIMESTAMP
     WHERE id IN (${placeholders})`,
    [status, ...userIds]
  );

  return c.json({ updated: result.changes });
});

export { bulkRouter };
```

---

## Real-time Features

### Server-Sent Events (SSE)

```typescript
// src/routes/events.ts
import { Hono } from 'hono';
import { streamSSE } from 'hono/streaming';
import type { DBClient } from '../db/client';

const eventsRouter = new Hono<{ Variables: { db: DBClient } }>();

// SSE endpoint for real-time updates
eventsRouter.get('/stream', async (c) => {
  const db = c.get('db');
  let lastId = 0;

  return streamSSE(c, async (stream) => {
    // Poll for new events
    while (true) {
      const events = await db.query<{
        id: number;
        type: string;
        data: string;
        created_at: string;
      }>(
        `SELECT * FROM events
         WHERE id > ?
         ORDER BY id ASC
         LIMIT 10`,
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

      // Wait before polling again
      await stream.sleep(1000);
    }
  });
});

// Emit an event
eventsRouter.post('/emit', async (c) => {
  const db = c.get('db');
  const { type, data } = await c.req.json<{ type: string; data: unknown }>();

  await db.run(
    'INSERT INTO events (type, data, created_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
    [type, JSON.stringify(data)]
  );

  return c.json({ success: true });
});

export { eventsRouter };
```

### WebSocket Support

```typescript
// src/routes/ws.ts
import { Hono } from 'hono';
import type { Env } from '../types';

const wsRouter = new Hono<{ Bindings: Env }>();

wsRouter.get('/connect', async (c) => {
  const upgradeHeader = c.req.header('Upgrade');

  if (upgradeHeader !== 'websocket') {
    return c.text('Expected WebSocket', 426);
  }

  // Get Durable Object for WebSocket handling
  const id = c.env.DOSQL_DB.idFromName('websocket-hub');
  const stub = c.env.DOSQL_DB.get(id);

  // Forward the WebSocket upgrade to the Durable Object
  return stub.fetch(c.req.raw);
});

export { wsRouter };
```

```typescript
// src/durable-objects/websocket-hub.ts
import { DB, type Database } from '@dotdo/dosql';

interface WebSocketSession {
  socket: WebSocket;
  userId?: string;
  subscribedChannels: Set<string>;
}

export class WebSocketHub implements DurableObject {
  private db: Database | null = null;
  private sessions = new Map<string, WebSocketSession>();
  private state: DurableObjectState;

  constructor(state: DurableObjectState) {
    this.state = state;
  }

  private async getDB(): Promise<Database> {
    if (!this.db) {
      this.db = await DB('websocket-hub', {
        migrations: [
          {
            id: '001_messages',
            sql: `
              CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                data TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
              );
              CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel);
            `,
          },
        ],
        storage: { hot: this.state.storage },
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/connect') {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);

      const sessionId = crypto.randomUUID();
      const session: WebSocketSession = {
        socket: server,
        subscribedChannels: new Set(),
      };

      this.sessions.set(sessionId, session);
      server.accept();

      server.addEventListener('message', async (event) => {
        await this.handleMessage(sessionId, event.data as string);
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
        session.subscribedChannels.add(message.channel);
        break;

      case 'unsubscribe':
        session.subscribedChannels.delete(message.channel);
        break;

      case 'publish':
        // Store message
        await db.run(
          'INSERT INTO messages (channel, data) VALUES (?, ?)',
          [message.channel, JSON.stringify(message.data)]
        );

        // Broadcast to subscribers
        for (const [, sess] of this.sessions) {
          if (sess.subscribedChannels.has(message.channel)) {
            sess.socket.send(JSON.stringify({
              type: 'message',
              channel: message.channel,
              data: message.data,
            }));
          }
        }
        break;

      case 'query':
        // Execute query and return results
        const rows = await db.query(message.sql, message.params);
        session.socket.send(JSON.stringify({
          type: 'result',
          id: message.id,
          rows,
        }));
        break;
    }
  }
}
```

---

## Error Handling

### Global Error Handler

```typescript
// src/errors.ts
import { HTTPException } from 'hono/http-exception';

export class ValidationError extends HTTPException {
  constructor(message: string, field?: string) {
    super(400, { message });
    this.name = 'ValidationError';
  }
}

export class NotFoundError extends HTTPException {
  constructor(resource: string, id?: string | number) {
    const message = id ? `${resource} with id ${id} not found` : `${resource} not found`;
    super(404, { message });
    this.name = 'NotFoundError';
  }
}

export class ConflictError extends HTTPException {
  constructor(message: string) {
    super(409, { message });
    this.name = 'ConflictError';
  }
}

export class UnauthorizedError extends HTTPException {
  constructor(message = 'Unauthorized') {
    super(401, { message });
    this.name = 'UnauthorizedError';
  }
}

export class ForbiddenError extends HTTPException {
  constructor(message = 'Forbidden') {
    super(403, { message });
    this.name = 'ForbiddenError';
  }
}
```

```typescript
// src/index.ts
import { Hono } from 'hono';
import { HTTPException } from 'hono/http-exception';
import { DoSQLError } from '@dotdo/dosql';

const app = new Hono();

// Global error handler
app.onError((err, c) => {
  // Log error for debugging
  console.error('Error:', {
    name: err.name,
    message: err.message,
    stack: err.stack,
    path: c.req.path,
    method: c.req.method,
  });

  // Handle HTTP exceptions
  if (err instanceof HTTPException) {
    return c.json(
      {
        error: err.message,
        code: err.status,
      },
      err.status
    );
  }

  // Handle DoSQL errors
  if (err instanceof DoSQLError) {
    const status = getStatusFromDoSQLError(err);
    return c.json(
      {
        error: err.toUserMessage(),
        code: err.code,
        retryable: err.isRetryable(),
      },
      status
    );
  }

  // Handle constraint violations
  if (err.message.includes('UNIQUE constraint')) {
    return c.json(
      { error: 'Resource already exists', code: 'CONFLICT' },
      409
    );
  }

  if (err.message.includes('FOREIGN KEY constraint')) {
    return c.json(
      { error: 'Referenced resource not found', code: 'INVALID_REFERENCE' },
      400
    );
  }

  // Generic server error
  return c.json(
    { error: 'Internal server error', code: 'INTERNAL_ERROR' },
    500
  );
});

function getStatusFromDoSQLError(err: DoSQLError): number {
  switch (err.category) {
    case 'VALIDATION':
      return 400;
    case 'CONNECTION':
      return 503;
    case 'TIMEOUT':
      return 504;
    case 'CONFLICT':
      return 409;
    default:
      return 500;
  }
}

export default app;
```

---

## Testing

### Test Setup

```typescript
// test/setup.ts
import { Hono } from 'hono';
import { testClient } from 'hono/testing';

// Mock database client for testing
export function createMockDB() {
  const data: Map<string, unknown[]> = new Map();

  return {
    async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
      // Simple mock implementation
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
        existing.push({ id, ...extractValues(sql, params) });
        data.set(table, existing);
        return { changes: 1, lastInsertRowid: id };
      }
      return { changes: 1, lastInsertRowid: 0 };
    },

    async transaction<T>(fn: (tx: any) => Promise<T>): Promise<T> {
      return fn(this);
    },

    // Test helper to seed data
    _seed(table: string, rows: unknown[]) {
      data.set(table, rows);
    },

    // Test helper to clear data
    _clear() {
      data.clear();
    },
  };
}

function extractTableName(sql: string): string {
  const match = sql.match(/(?:FROM|INTO|UPDATE)\s+(\w+)/i);
  return match?.[1] || 'unknown';
}

function extractValues(sql: string, params?: unknown[]): Record<string, unknown> {
  // Simplified for testing
  return {};
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

    // Inject mock database
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
      expect(data.pagination.total).toBe(2);
    });
  });

  describe('POST /users', () => {
    it('creates a new user', async () => {
      const res = await app.request('/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'Alice',
          email: 'alice@example.com',
        }),
      });

      expect(res.status).toBe(201);
    });

    it('returns 400 for invalid email', async () => {
      const res = await app.request('/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'Alice',
          email: 'invalid-email',
        }),
      });

      expect(res.status).toBe(400);
    });
  });

  describe('GET /users/:id', () => {
    it('returns 404 for non-existent user', async () => {
      const res = await app.request('/users/999');
      expect(res.status).toBe(404);
    });

    it('returns user by id', async () => {
      mockDB._seed('users', [
        { id: 1, name: 'Alice', email: 'alice@example.com' },
      ]);

      const res = await app.request('/users/1');
      expect(res.status).toBe(200);

      const user = await res.json();
      expect(user.name).toBe('Alice');
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
    // Create user
    const createResp = await worker.fetch('/api/v1/users', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'Test User',
        email: 'test@example.com',
      }),
    });

    expect(createResp.status).toBe(201);
    const created = await createResp.json();
    expect(created.id).toBeDefined();

    // Retrieve user
    const getResp = await worker.fetch(`/api/v1/users/${created.id}`);
    expect(getResp.status).toBe(200);

    const user = await getResp.json();
    expect(user.name).toBe('Test User');
    expect(user.email).toBe('test@example.com');
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
compatibility_date = "2024-01-01"

# Durable Objects
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

# R2 for cold storage
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-data-bucket"

# Production environment
[env.production]
name = "my-hono-app-production"

[[env.production.r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-data-bucket-prod"

# Staging environment
[env.staging]
name = "my-hono-app-staging"

[[env.staging.r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-data-bucket-staging"
```

### Build Scripts

```json
{
  "scripts": {
    "dev": "wrangler dev",
    "build": "wrangler deploy --dry-run",
    "deploy": "wrangler deploy",
    "deploy:staging": "wrangler deploy --env staging",
    "deploy:production": "wrangler deploy --env production",
    "test": "vitest run",
    "test:watch": "vitest",
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

1. **Environment Configuration**
   - Set up separate Durable Object namespaces for staging/production
   - Configure R2 buckets for each environment
   - Set environment-specific secrets

2. **Database Migrations**
   - Test migrations in staging first
   - Include migration files in deployment bundle
   - Monitor migration execution on first request

3. **Monitoring**
   - Enable Cloudflare Workers Analytics
   - Set up error alerting
   - Monitor Durable Object metrics

4. **Security**
   - Enable CORS with specific origins
   - Use secure headers middleware
   - Implement rate limiting
   - Validate all input with Zod or similar

5. **Performance**
   - Use connection pooling via Durable Objects
   - Implement caching where appropriate
   - Monitor query performance

---

## Next Steps

- [Getting Started](../getting-started.md) - DoSQL basics and setup
- [API Reference](../api-reference.md) - Complete API documentation
- [Advanced Features](../advanced.md) - Time travel, branching, CDC
- [Architecture](../architecture.md) - Understanding DoSQL internals
