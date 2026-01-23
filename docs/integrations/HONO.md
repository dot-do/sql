# Hono Integration Guide

**Version**: 1.0.0
**Last Updated**: 2026-01-22
**Maintainer**: Platform Team

This guide provides comprehensive documentation for integrating DoSQL with the [Hono](https://hono.dev) web framework on Cloudflare Workers.

---

## Table of Contents

1. [Installation and Setup](#installation-and-setup)
2. [Middleware for Database Context](#middleware-for-database-context)
3. [Route Handlers with DoSQL](#route-handlers-with-dosql)
4. [Request Validation with Zod](#request-validation-with-zod)
5. [Cloudflare Workers Deployment](#cloudflare-workers-deployment)
6. [Type-Safe Queries with Hono's TypeScript](#type-safe-queries-with-honos-typescript)
7. [WebSocket Routes for CDC Streaming](#websocket-routes-for-cdc-streaming)
8. [Example: REST API with CRUD Operations](#example-rest-api-with-crud-operations)
9. [Example: GraphQL-like Queries](#example-graphql-like-queries)
10. [Performance Optimization Tips](#performance-optimization-tips)

---

## Installation and Setup

### Prerequisites

- Node.js 18+ or Bun
- Cloudflare Workers account
- Wrangler CLI (`npm install -g wrangler`)

### Install Dependencies

```bash
# Create a new Hono project
npm create hono@latest my-dosql-app
cd my-dosql-app

# Install DoSQL and dependencies
npm install sql.do hono zod
npm install -D @cloudflare/workers-types wrangler typescript
```

### Project Structure

```
my-dosql-app/
├── src/
│   ├── index.ts           # Main Hono app
│   ├── middleware/
│   │   ├── database.ts    # DoSQL middleware
│   │   ├── auth.ts        # Authentication middleware
│   │   └── validation.ts  # Zod validation helpers
│   ├── routes/
│   │   ├── users.ts       # User routes
│   │   ├── posts.ts       # Post routes
│   │   └── cdc.ts         # CDC WebSocket routes
│   ├── db/
│   │   ├── schema.ts      # Database schema definitions
│   │   ├── types.ts       # TypeScript interfaces
│   │   └── queries.ts     # Reusable query functions
│   └── lib/
│       └── errors.ts      # Error handling utilities
├── wrangler.toml          # Cloudflare configuration
├── package.json
└── tsconfig.json
```

### Basic Setup

```typescript
// src/index.ts
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import { secureHeaders } from 'hono/secure-headers';
import { prettyJSON } from 'hono/pretty-json';
import { createSQLClient, type SQLClient } from 'sql.do';

// Environment bindings
type Env = {
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
  ENVIRONMENT: 'development' | 'staging' | 'production';
};

// Context variables available in route handlers
type Variables = {
  db: SQLClient;
  requestId: string;
  startTime: number;
};

// Create typed Hono app
const app = new Hono<{ Bindings: Env; Variables: Variables }>();

// Global middleware
app.use('*', logger());
app.use('*', secureHeaders());
app.use('*', prettyJSON());
app.use('/api/*', cors({
  origin: ['https://app.example.com'],
  credentials: true,
}));

// Request ID and timing
app.use('*', async (c, next) => {
  c.set('requestId', crypto.randomUUID());
  c.set('startTime', Date.now());
  await next();
});

// Database middleware
app.use('/api/*', async (c, next) => {
  const db = createSQLClient({
    url: c.env.DOSQL_URL,
    token: c.env.DOSQL_TOKEN,
  });
  c.set('db', db);

  try {
    await next();
  } finally {
    // Cleanup if needed
  }
});

// Health check
app.get('/health', (c) => c.json({ status: 'healthy' }));

// Export for Cloudflare Workers
export default app;
```

---

## Middleware for Database Context

### Basic Database Middleware

```typescript
// src/middleware/database.ts
import { createMiddleware } from 'hono/factory';
import { createSQLClient, type SQLClient } from 'sql.do';

type Env = {
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
};

type Variables = {
  db: SQLClient;
};

/**
 * Database middleware - creates a DoSQL client per request
 */
export const dbMiddleware = createMiddleware<{
  Bindings: Env;
  Variables: Variables;
}>(async (c, next) => {
  const db = createSQLClient({
    url: c.env.DOSQL_URL,
    token: c.env.DOSQL_TOKEN,
  });

  c.set('db', db);
  await next();
});
```

### Tenant-Aware Database Middleware

```typescript
// src/middleware/tenant-database.ts
import { createMiddleware } from 'hono/factory';
import { createSQLClient, type SQLClient } from 'sql.do';
import { HTTPException } from 'hono/http-exception';

type Env = {
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
};

type Variables = {
  db: SQLClient;
  tenantId: string;
};

/**
 * Tenant-aware database middleware
 * Routes requests to tenant-specific Durable Objects
 */
export const tenantDbMiddleware = createMiddleware<{
  Bindings: Env;
  Variables: Variables;
}>(async (c, next) => {
  // Extract tenant from header, subdomain, or JWT
  const tenantId = c.req.header('X-Tenant-ID')
    || extractTenantFromHost(c.req.header('Host'))
    || extractTenantFromJWT(c.req.header('Authorization'));

  if (!tenantId) {
    throw new HTTPException(400, { message: 'Tenant identification required' });
  }

  const db = createSQLClient({
    url: c.env.DOSQL_URL,
    token: c.env.DOSQL_TOKEN,
    headers: {
      'X-Tenant-ID': tenantId,
    },
  });

  c.set('db', db);
  c.set('tenantId', tenantId);
  await next();
});

function extractTenantFromHost(host?: string): string | null {
  if (!host) return null;
  const match = host.match(/^([a-z0-9-]+)\./);
  return match ? match[1] : null;
}

function extractTenantFromJWT(auth?: string): string | null {
  if (!auth?.startsWith('Bearer ')) return null;
  try {
    const token = auth.slice(7);
    const payload = JSON.parse(atob(token.split('.')[1]));
    return payload.tenant_id || null;
  } catch {
    return null;
  }
}
```

### Transaction Middleware

```typescript
// src/middleware/transaction.ts
import { createMiddleware } from 'hono/factory';
import type { SQLClient } from 'sql.do';

type Variables = {
  db: SQLClient;
  inTransaction: boolean;
};

/**
 * Transaction middleware - wraps request in a database transaction
 * Automatically commits on success, rolls back on error
 */
export const transactionMiddleware = createMiddleware<{
  Variables: Variables;
}>(async (c, next) => {
  const db = c.get('db');

  try {
    await db.exec('BEGIN TRANSACTION');
    c.set('inTransaction', true);

    await next();

    // Commit if successful
    if (c.res.status < 400) {
      await db.exec('COMMIT');
    } else {
      await db.exec('ROLLBACK');
    }
  } catch (error) {
    await db.exec('ROLLBACK');
    throw error;
  }
});
```

### Read Replica Middleware

```typescript
// src/middleware/read-replica.ts
import { createMiddleware } from 'hono/factory';
import { createSQLClient, type SQLClient } from 'sql.do';

type Env = {
  DOSQL_URL: string;
  DOSQL_REPLICA_URL: string;
  DOSQL_TOKEN: string;
};

type Variables = {
  db: SQLClient;
  dbReplica: SQLClient;
};

/**
 * Read replica middleware - provides separate connections for reads/writes
 */
export const readReplicaMiddleware = createMiddleware<{
  Bindings: Env;
  Variables: Variables;
}>(async (c, next) => {
  // Primary for writes
  const db = createSQLClient({
    url: c.env.DOSQL_URL,
    token: c.env.DOSQL_TOKEN,
  });

  // Replica for reads (optional, falls back to primary)
  const dbReplica = c.env.DOSQL_REPLICA_URL
    ? createSQLClient({
        url: c.env.DOSQL_REPLICA_URL,
        token: c.env.DOSQL_TOKEN,
      })
    : db;

  c.set('db', db);
  c.set('dbReplica', dbReplica);
  await next();
});
```

---

## Route Handlers with DoSQL

### Basic Route Handlers

```typescript
// src/routes/users.ts
import { Hono } from 'hono';
import type { SQLClient } from 'sql.do';

type Env = {
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
};

type Variables = {
  db: SQLClient;
};

interface User {
  id: string;
  name: string;
  email: string;
  created_at: number;
}

const users = new Hono<{ Bindings: Env; Variables: Variables }>();

// GET /users - List all users
users.get('/', async (c) => {
  const db = c.get('db');
  const { page = '1', limit = '20' } = c.req.query();

  const pageNum = Math.max(1, parseInt(page));
  const limitNum = Math.min(100, Math.max(1, parseInt(limit)));
  const offset = (pageNum - 1) * limitNum;

  const [usersResult, countResult] = await Promise.all([
    db.query<User>(
      'SELECT * FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?',
      [limitNum, offset]
    ),
    db.query<{ total: number }>('SELECT COUNT(*) as total FROM users'),
  ]);

  return c.json({
    users: usersResult.rows,
    pagination: {
      page: pageNum,
      limit: limitNum,
      total: countResult.rows[0].total,
      totalPages: Math.ceil(countResult.rows[0].total / limitNum),
    },
  });
});

// GET /users/:id - Get single user
users.get('/:id', async (c) => {
  const db = c.get('db');
  const id = c.req.param('id');

  const result = await db.query<User>(
    'SELECT * FROM users WHERE id = ?',
    [id]
  );

  if (result.rows.length === 0) {
    return c.json({ error: 'User not found' }, 404);
  }

  return c.json({ user: result.rows[0] });
});

// POST /users - Create user
users.post('/', async (c) => {
  const db = c.get('db');
  const body = await c.req.json<{ name: string; email: string }>();

  const id = crypto.randomUUID();
  const now = Date.now();

  await db.exec(
    'INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)',
    [id, body.name, body.email, now]
  );

  return c.json({ id }, 201);
});

// PUT /users/:id - Update user
users.put('/:id', async (c) => {
  const db = c.get('db');
  const id = c.req.param('id');
  const body = await c.req.json<{ name?: string; email?: string }>();

  const result = await db.query<User>(
    `UPDATE users SET
      name = COALESCE(?, name),
      email = COALESCE(?, email)
     WHERE id = ?
     RETURNING *`,
    [body.name, body.email, id]
  );

  if (result.rows.length === 0) {
    return c.json({ error: 'User not found' }, 404);
  }

  return c.json({ user: result.rows[0] });
});

// DELETE /users/:id - Delete user
users.delete('/:id', async (c) => {
  const db = c.get('db');
  const id = c.req.param('id');

  const result = await db.exec(
    'DELETE FROM users WHERE id = ?',
    [id]
  );

  if (result.changes === 0) {
    return c.json({ error: 'User not found' }, 404);
  }

  return c.json({ success: true });
});

export default users;
```

### Route Groups with Shared Logic

```typescript
// src/routes/posts.ts
import { Hono } from 'hono';
import type { SQLClient } from 'sql.do';

type Variables = {
  db: SQLClient;
  userId: string;
};

interface Post {
  id: string;
  title: string;
  content: string;
  author_id: string;
  published: boolean;
  created_at: number;
  updated_at: number;
}

const posts = new Hono<{ Variables: Variables }>();

// Middleware to verify post ownership
const verifyOwnership = async (
  db: SQLClient,
  postId: string,
  userId: string
): Promise<Post | null> => {
  const result = await db.query<Post>(
    'SELECT * FROM posts WHERE id = ? AND author_id = ?',
    [postId, userId]
  );
  return result.rows[0] || null;
};

// GET /posts - List posts with filtering
posts.get('/', async (c) => {
  const db = c.get('db');
  const { author, published, search, sort = 'created_at', order = 'desc' } = c.req.query();

  // Build dynamic query
  const conditions: string[] = [];
  const params: unknown[] = [];

  if (author) {
    conditions.push('author_id = ?');
    params.push(author);
  }

  if (published !== undefined) {
    conditions.push('published = ?');
    params.push(published === 'true');
  }

  if (search) {
    conditions.push('(title LIKE ? OR content LIKE ?)');
    params.push(`%${search}%`, `%${search}%`);
  }

  const whereClause = conditions.length > 0
    ? `WHERE ${conditions.join(' AND ')}`
    : '';

  // Validate sort column to prevent SQL injection
  const allowedSorts = ['created_at', 'updated_at', 'title'];
  const sortColumn = allowedSorts.includes(sort) ? sort : 'created_at';
  const sortOrder = order.toLowerCase() === 'asc' ? 'ASC' : 'DESC';

  const result = await db.query<Post>(
    `SELECT * FROM posts ${whereClause} ORDER BY ${sortColumn} ${sortOrder} LIMIT 50`,
    params
  );

  return c.json({ posts: result.rows });
});

// POST /posts - Create post
posts.post('/', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId');
  const body = await c.req.json<{ title: string; content: string; published?: boolean }>();

  const id = crypto.randomUUID();
  const now = Date.now();

  await db.exec(
    `INSERT INTO posts (id, title, content, author_id, published, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [id, body.title, body.content, userId, body.published ?? false, now, now]
  );

  return c.json({ id }, 201);
});

// PATCH /posts/:id/publish - Publish/unpublish post
posts.patch('/:id/publish', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId');
  const postId = c.req.param('id');
  const { published } = await c.req.json<{ published: boolean }>();

  const post = await verifyOwnership(db, postId, userId);
  if (!post) {
    return c.json({ error: 'Post not found or not authorized' }, 404);
  }

  await db.exec(
    'UPDATE posts SET published = ?, updated_at = ? WHERE id = ?',
    [published, Date.now(), postId]
  );

  return c.json({ success: true, published });
});

export default posts;
```

---

## Request Validation with Zod

### Setting Up Zod Validation

```typescript
// src/middleware/validation.ts
import { z, ZodSchema, ZodError } from 'zod';
import { createMiddleware } from 'hono/factory';
import { validator } from 'hono/validator';
import { HTTPException } from 'hono/http-exception';

/**
 * Custom Zod validator middleware for Hono
 */
export function zodValidator<T extends ZodSchema>(
  target: 'json' | 'query' | 'param',
  schema: T
) {
  return validator(target, (value, c) => {
    const result = schema.safeParse(value);

    if (!result.success) {
      const errors = result.error.errors.map((err) => ({
        path: err.path.join('.'),
        message: err.message,
        code: err.code,
      }));

      throw new HTTPException(400, {
        message: 'Validation failed',
        cause: { errors },
      });
    }

    return result.data as z.infer<T>;
  });
}

/**
 * Global error handler for Zod validation errors
 */
export function handleZodError(err: unknown) {
  if (err instanceof ZodError) {
    return {
      error: 'Validation failed',
      details: err.errors.map((e) => ({
        path: e.path.join('.'),
        message: e.message,
      })),
    };
  }
  throw err;
}
```

### Schema Definitions

```typescript
// src/db/schema.ts
import { z } from 'zod';

// User schemas
export const CreateUserSchema = z.object({
  name: z.string().min(1).max(100),
  email: z.string().email(),
  password: z.string().min(8).max(100).optional(),
  role: z.enum(['user', 'admin', 'moderator']).default('user'),
});

export const UpdateUserSchema = z.object({
  name: z.string().min(1).max(100).optional(),
  email: z.string().email().optional(),
  role: z.enum(['user', 'admin', 'moderator']).optional(),
});

export const UserIdSchema = z.object({
  id: z.string().uuid(),
});

// Post schemas
export const CreatePostSchema = z.object({
  title: z.string().min(1).max(200),
  content: z.string().min(1).max(50000),
  published: z.boolean().default(false),
  tags: z.array(z.string()).max(10).optional(),
});

export const UpdatePostSchema = z.object({
  title: z.string().min(1).max(200).optional(),
  content: z.string().min(1).max(50000).optional(),
  published: z.boolean().optional(),
  tags: z.array(z.string()).max(10).optional(),
});

// Query parameter schemas
export const PaginationSchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  limit: z.coerce.number().int().min(1).max(100).default(20),
});

export const SearchSchema = z.object({
  q: z.string().min(1).max(100).optional(),
  sort: z.enum(['created_at', 'updated_at', 'name', 'title']).default('created_at'),
  order: z.enum(['asc', 'desc']).default('desc'),
}).merge(PaginationSchema);

// Infer TypeScript types from schemas
export type CreateUser = z.infer<typeof CreateUserSchema>;
export type UpdateUser = z.infer<typeof UpdateUserSchema>;
export type CreatePost = z.infer<typeof CreatePostSchema>;
export type UpdatePost = z.infer<typeof UpdatePostSchema>;
export type PaginationParams = z.infer<typeof PaginationSchema>;
export type SearchParams = z.infer<typeof SearchSchema>;
```

### Using Validation in Routes

```typescript
// src/routes/users-validated.ts
import { Hono } from 'hono';
import { zodValidator } from '../middleware/validation';
import {
  CreateUserSchema,
  UpdateUserSchema,
  UserIdSchema,
  SearchSchema,
  type CreateUser,
  type UpdateUser,
} from '../db/schema';
import type { SQLClient } from 'sql.do';

type Variables = {
  db: SQLClient;
};

const users = new Hono<{ Variables: Variables }>();

// GET /users - with validated query params
users.get(
  '/',
  zodValidator('query', SearchSchema),
  async (c) => {
    const db = c.get('db');
    const { page, limit, q, sort, order } = c.req.valid('query');

    const offset = (page - 1) * limit;
    const params: unknown[] = [];
    let whereClause = '';

    if (q) {
      whereClause = 'WHERE name LIKE ? OR email LIKE ?';
      params.push(`%${q}%`, `%${q}%`);
    }

    params.push(limit, offset);

    const result = await db.query(
      `SELECT * FROM users ${whereClause} ORDER BY ${sort} ${order.toUpperCase()} LIMIT ? OFFSET ?`,
      params
    );

    return c.json({ users: result.rows, page, limit });
  }
);

// POST /users - with validated body
users.post(
  '/',
  zodValidator('json', CreateUserSchema),
  async (c) => {
    const db = c.get('db');
    const data: CreateUser = c.req.valid('json');

    const id = crypto.randomUUID();
    const now = Date.now();

    await db.exec(
      'INSERT INTO users (id, name, email, role, created_at) VALUES (?, ?, ?, ?, ?)',
      [id, data.name, data.email, data.role, now]
    );

    return c.json({ id }, 201);
  }
);

// PUT /users/:id - with validated params and body
users.put(
  '/:id',
  zodValidator('param', UserIdSchema),
  zodValidator('json', UpdateUserSchema),
  async (c) => {
    const db = c.get('db');
    const { id } = c.req.valid('param');
    const data: UpdateUser = c.req.valid('json');

    // Build dynamic update
    const updates: string[] = [];
    const params: unknown[] = [];

    if (data.name !== undefined) {
      updates.push('name = ?');
      params.push(data.name);
    }
    if (data.email !== undefined) {
      updates.push('email = ?');
      params.push(data.email);
    }
    if (data.role !== undefined) {
      updates.push('role = ?');
      params.push(data.role);
    }

    if (updates.length === 0) {
      return c.json({ error: 'No fields to update' }, 400);
    }

    params.push(id);

    const result = await db.exec(
      `UPDATE users SET ${updates.join(', ')}, updated_at = ${Date.now()} WHERE id = ?`,
      params
    );

    if (result.changes === 0) {
      return c.json({ error: 'User not found' }, 404);
    }

    return c.json({ success: true });
  }
);

export default users;
```

### Advanced Validation Patterns

```typescript
// src/middleware/advanced-validation.ts
import { z } from 'zod';
import { createMiddleware } from 'hono/factory';
import type { SQLClient } from 'sql.do';

// Custom refinements for database validation
export const UniqueEmailSchema = z.object({
  email: z.string().email(),
});

/**
 * Middleware to validate email uniqueness in database
 */
export const validateUniqueEmail = createMiddleware<{
  Variables: { db: SQLClient };
}>(async (c, next) => {
  const db = c.get('db');
  const body = await c.req.json();

  if (body.email) {
    const existing = await db.query(
      'SELECT id FROM users WHERE email = ? LIMIT 1',
      [body.email]
    );

    if (existing.rows.length > 0) {
      return c.json(
        { error: 'Validation failed', details: [{ path: 'email', message: 'Email already in use' }] },
        400
      );
    }
  }

  await next();
});

// Conditional validation based on user role
export const CreateOrderSchema = z.object({
  items: z.array(z.object({
    productId: z.string().uuid(),
    quantity: z.number().int().min(1).max(100),
  })).min(1).max(50),
  shippingAddress: z.object({
    street: z.string().min(1).max(200),
    city: z.string().min(1).max(100),
    country: z.string().length(2), // ISO country code
    postalCode: z.string().min(3).max(20),
  }),
  // Coupon code with async validation
  couponCode: z.string().optional(),
  // Notes for admin users
  adminNotes: z.string().optional(),
}).refine(
  (data) => {
    // Ensure admin notes are only provided by admins (checked in route)
    return true;
  },
  { message: 'Invalid order data' }
);

// Schema for date range queries
export const DateRangeSchema = z.object({
  startDate: z.coerce.date().optional(),
  endDate: z.coerce.date().optional(),
}).refine(
  (data) => {
    if (data.startDate && data.endDate) {
      return data.startDate <= data.endDate;
    }
    return true;
  },
  { message: 'Start date must be before end date', path: ['startDate'] }
);
```

---

## Cloudflare Workers Deployment

### Wrangler Configuration

```toml
# wrangler.toml
name = "dosql-hono-api"
main = "src/index.ts"
compatibility_date = "2024-01-01"
compatibility_flags = ["nodejs_compat"]

[vars]
ENVIRONMENT = "production"

# Secrets (set via wrangler secret put)
# DOSQL_URL
# DOSQL_TOKEN

# Durable Objects for DoSQL
[[durable_objects.bindings]]
name = "SQL_DO"
class_name = "SQLDurableObject"
script_name = "dosql-worker"

# KV for caching (optional)
[[kv_namespaces]]
binding = "CACHE"
id = "your-kv-namespace-id"

# R2 for storage (optional)
[[r2_buckets]]
binding = "STORAGE"
bucket_name = "your-bucket-name"

# Environment-specific configuration
[env.staging]
name = "dosql-hono-api-staging"
vars = { ENVIRONMENT = "staging" }

[env.production]
name = "dosql-hono-api-production"
vars = { ENVIRONMENT = "production" }
```

### TypeScript Configuration

```json
// tsconfig.json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "lib": ["ES2022"],
    "strict": true,
    "skipLibCheck": true,
    "types": ["@cloudflare/workers-types"],
    "jsx": "react-jsx",
    "jsxImportSource": "hono/jsx",
    "outDir": "./dist",
    "rootDir": "./src"
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules"]
}
```

### Environment Type Definitions

```typescript
// src/env.d.ts
import type { DurableObjectNamespace, KVNamespace, R2Bucket } from '@cloudflare/workers-types';

declare global {
  interface Env {
    // DoSQL configuration
    DOSQL_URL: string;
    DOSQL_TOKEN: string;

    // Durable Object binding
    SQL_DO: DurableObjectNamespace;

    // Optional bindings
    CACHE?: KVNamespace;
    STORAGE?: R2Bucket;

    // Environment
    ENVIRONMENT: 'development' | 'staging' | 'production';
  }
}

export {};
```

### Deployment Commands

```bash
# Development
wrangler dev

# Deploy to staging
wrangler deploy --env staging

# Deploy to production
wrangler deploy --env production

# Set secrets
wrangler secret put DOSQL_URL --env production
wrangler secret put DOSQL_TOKEN --env production

# View logs
wrangler tail --env production
```

### Production-Ready Main File

```typescript
// src/index.ts
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import { secureHeaders } from 'hono/secure-headers';
import { compress } from 'hono/compress';
import { etag } from 'hono/etag';
import { HTTPException } from 'hono/http-exception';
import { dbMiddleware } from './middleware/database';
import users from './routes/users';
import posts from './routes/posts';

type Env = {
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
  ENVIRONMENT: string;
};

const app = new Hono<{ Bindings: Env }>();

// Production middleware
app.use('*', logger());
app.use('*', secureHeaders());
app.use('*', compress());
app.use('*', etag());

// CORS configuration
app.use('/api/*', cors({
  origin: (origin) => {
    const allowedOrigins = [
      'https://app.example.com',
      'https://admin.example.com',
    ];
    return allowedOrigins.includes(origin) ? origin : null;
  },
  credentials: true,
  maxAge: 86400,
}));

// Database middleware
app.use('/api/*', dbMiddleware);

// Health and readiness checks
app.get('/health', (c) => c.json({ status: 'healthy' }));
app.get('/ready', async (c) => {
  try {
    const db = c.get('db');
    await db.query('SELECT 1');
    return c.json({ status: 'ready' });
  } catch {
    return c.json({ status: 'not ready' }, 503);
  }
});

// API routes
app.route('/api/users', users);
app.route('/api/posts', posts);

// Global error handler
app.onError((err, c) => {
  console.error('Error:', err);

  if (err instanceof HTTPException) {
    return c.json(
      { error: err.message, ...(err.cause || {}) },
      err.status
    );
  }

  // Don't expose internal errors in production
  const isProduction = c.env.ENVIRONMENT === 'production';
  return c.json(
    {
      error: isProduction ? 'Internal server error' : err.message,
      requestId: c.get('requestId'),
    },
    500
  );
});

// 404 handler
app.notFound((c) => {
  return c.json({ error: 'Not found' }, 404);
});

export default app;
```

---

## Type-Safe Queries with Hono's TypeScript

### Defining Database Types

```typescript
// src/db/types.ts
import type { SQLClient } from 'sql.do';

// Entity types
export interface User {
  id: string;
  name: string;
  email: string;
  role: 'user' | 'admin' | 'moderator';
  avatar_url: string | null;
  created_at: number;
  updated_at: number;
}

export interface Post {
  id: string;
  title: string;
  content: string;
  author_id: string;
  published: boolean;
  view_count: number;
  created_at: number;
  updated_at: number;
}

export interface Comment {
  id: string;
  post_id: string;
  author_id: string;
  content: string;
  created_at: number;
}

// Join result types
export interface PostWithAuthor extends Post {
  author_name: string;
  author_email: string;
}

export interface CommentWithAuthor extends Comment {
  author_name: string;
}

// Aggregate types
export interface UserStats {
  user_id: string;
  post_count: number;
  comment_count: number;
  total_views: number;
}
```

### Type-Safe Query Functions

```typescript
// src/db/queries.ts
import type { SQLClient } from 'sql.do';
import type { User, Post, PostWithAuthor, UserStats } from './types';

/**
 * Type-safe user queries
 */
export class UserQueries {
  constructor(private db: SQLClient) {}

  async findById(id: string): Promise<User | null> {
    const result = await this.db.query<User>(
      'SELECT * FROM users WHERE id = ?',
      [id]
    );
    return result.rows[0] || null;
  }

  async findByEmail(email: string): Promise<User | null> {
    const result = await this.db.query<User>(
      'SELECT * FROM users WHERE email = ?',
      [email]
    );
    return result.rows[0] || null;
  }

  async create(data: Omit<User, 'id' | 'created_at' | 'updated_at'>): Promise<User> {
    const id = crypto.randomUUID();
    const now = Date.now();

    await this.db.exec(
      `INSERT INTO users (id, name, email, role, avatar_url, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [id, data.name, data.email, data.role, data.avatar_url, now, now]
    );

    const result = await this.db.query<User>(
      'SELECT * FROM users WHERE id = ?',
      [id]
    );
    return result.rows[0];
  }

  async update(id: string, data: Partial<Omit<User, 'id' | 'created_at'>>): Promise<User | null> {
    const result = await this.db.query<User>(
      `UPDATE users SET
        name = COALESCE(?, name),
        email = COALESCE(?, email),
        role = COALESCE(?, role),
        avatar_url = COALESCE(?, avatar_url),
        updated_at = ?
       WHERE id = ?
       RETURNING *`,
      [data.name, data.email, data.role, data.avatar_url, Date.now(), id]
    );
    return result.rows[0] || null;
  }

  async delete(id: string): Promise<boolean> {
    const result = await this.db.exec(
      'DELETE FROM users WHERE id = ?',
      [id]
    );
    return result.changes > 0;
  }

  async getStats(id: string): Promise<UserStats | null> {
    const result = await this.db.query<UserStats>(`
      SELECT
        u.id as user_id,
        COUNT(DISTINCT p.id) as post_count,
        COUNT(DISTINCT c.id) as comment_count,
        COALESCE(SUM(p.view_count), 0) as total_views
      FROM users u
      LEFT JOIN posts p ON u.id = p.author_id
      LEFT JOIN comments c ON u.id = c.author_id
      WHERE u.id = ?
      GROUP BY u.id
    `, [id]);
    return result.rows[0] || null;
  }
}

/**
 * Type-safe post queries
 */
export class PostQueries {
  constructor(private db: SQLClient) {}

  async findById(id: string): Promise<PostWithAuthor | null> {
    const result = await this.db.query<PostWithAuthor>(`
      SELECT p.*, u.name as author_name, u.email as author_email
      FROM posts p
      JOIN users u ON p.author_id = u.id
      WHERE p.id = ?
    `, [id]);
    return result.rows[0] || null;
  }

  async findPublished(options: {
    limit?: number;
    offset?: number;
    authorId?: string;
  } = {}): Promise<PostWithAuthor[]> {
    const { limit = 20, offset = 0, authorId } = options;
    const params: unknown[] = [];
    let whereClause = 'WHERE p.published = true';

    if (authorId) {
      whereClause += ' AND p.author_id = ?';
      params.push(authorId);
    }

    params.push(limit, offset);

    const result = await this.db.query<PostWithAuthor>(`
      SELECT p.*, u.name as author_name, u.email as author_email
      FROM posts p
      JOIN users u ON p.author_id = u.id
      ${whereClause}
      ORDER BY p.created_at DESC
      LIMIT ? OFFSET ?
    `, params);

    return result.rows;
  }

  async incrementViewCount(id: string): Promise<void> {
    await this.db.exec(
      'UPDATE posts SET view_count = view_count + 1 WHERE id = ?',
      [id]
    );
  }

  async search(query: string, limit = 20): Promise<PostWithAuthor[]> {
    const result = await this.db.query<PostWithAuthor>(`
      SELECT p.*, u.name as author_name, u.email as author_email
      FROM posts p
      JOIN users u ON p.author_id = u.id
      WHERE p.published = true
        AND (p.title LIKE ? OR p.content LIKE ?)
      ORDER BY p.created_at DESC
      LIMIT ?
    `, [`%${query}%`, `%${query}%`, limit]);

    return result.rows;
  }
}
```

### Using Query Classes in Routes

```typescript
// src/routes/users-typed.ts
import { Hono } from 'hono';
import { UserQueries } from '../db/queries';
import type { SQLClient } from 'sql.do';

type Variables = {
  db: SQLClient;
  userQueries: UserQueries;
};

const users = new Hono<{ Variables: Variables }>();

// Initialize queries middleware
users.use('*', async (c, next) => {
  const db = c.get('db');
  c.set('userQueries', new UserQueries(db));
  await next();
});

users.get('/:id', async (c) => {
  const queries = c.get('userQueries');
  const id = c.req.param('id');

  const user = await queries.findById(id);
  if (!user) {
    return c.json({ error: 'User not found' }, 404);
  }

  return c.json({ user });
});

users.get('/:id/stats', async (c) => {
  const queries = c.get('userQueries');
  const id = c.req.param('id');

  const stats = await queries.getStats(id);
  if (!stats) {
    return c.json({ error: 'User not found' }, 404);
  }

  return c.json({ stats });
});

export default users;
```

---

## WebSocket Routes for CDC Streaming

### WebSocket CDC Handler

```typescript
// src/routes/cdc.ts
import { Hono } from 'hono';
import { upgradeWebSocket } from 'hono/cloudflare-workers';
import type { SQLClient } from 'sql.do';
import type { CDCEvent, ChangeEvent } from 'sql.do';

type Env = {
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
};

type Variables = {
  db: SQLClient;
};

const cdc = new Hono<{ Bindings: Env; Variables: Variables }>();

/**
 * WebSocket endpoint for CDC streaming
 *
 * Clients can subscribe to real-time database changes
 * Protocol:
 * - Client sends: { type: 'subscribe', tables?: string[], fromLSN?: string }
 * - Server sends: { type: 'change', event: CDCEvent }
 * - Client sends: { type: 'ack', lsn: string }
 * - Server sends: { type: 'heartbeat', timestamp: number }
 */
cdc.get(
  '/stream',
  upgradeWebSocket((c) => {
    let subscription: AsyncIterableIterator<CDCEvent> | null = null;
    let isActive = true;

    return {
      onOpen(event, ws) {
        console.log('CDC WebSocket connection opened');

        // Send initial connection confirmation
        ws.send(JSON.stringify({
          type: 'connected',
          timestamp: Date.now(),
        }));
      },

      async onMessage(event, ws) {
        try {
          const message = JSON.parse(event.data as string);

          switch (message.type) {
            case 'subscribe': {
              const db = c.get('db');
              const { tables, fromLSN } = message;

              // Create CDC subscription
              const filter = tables ? { tables } : undefined;
              const startLSN = fromLSN ? BigInt(fromLSN) : 0n;

              // Start streaming changes
              subscription = db.subscribeCDC(startLSN, filter);

              // Stream events to client
              (async () => {
                try {
                  for await (const event of subscription!) {
                    if (!isActive) break;

                    ws.send(JSON.stringify({
                      type: 'change',
                      event: serializeCDCEvent(event),
                    }));
                  }
                } catch (error) {
                  if (isActive) {
                    ws.send(JSON.stringify({
                      type: 'error',
                      message: 'Stream error',
                      details: String(error),
                    }));
                  }
                }
              })();

              ws.send(JSON.stringify({
                type: 'subscribed',
                tables: tables || ['*'],
                fromLSN: startLSN.toString(),
              }));
              break;
            }

            case 'ack': {
              // Client acknowledges processing up to LSN
              const { lsn } = message;
              // Store checkpoint for exactly-once delivery
              console.log('Client acknowledged LSN:', lsn);
              break;
            }

            case 'ping': {
              ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
              break;
            }

            default:
              ws.send(JSON.stringify({
                type: 'error',
                message: `Unknown message type: ${message.type}`,
              }));
          }
        } catch (error) {
          ws.send(JSON.stringify({
            type: 'error',
            message: 'Invalid message format',
          }));
        }
      },

      onClose(event, ws) {
        isActive = false;
        subscription?.return?.();
        console.log('CDC WebSocket connection closed');
      },

      onError(event, ws) {
        isActive = false;
        subscription?.return?.();
        console.error('CDC WebSocket error:', event);
      },
    };
  })
);

/**
 * Serialize CDC event for JSON transport
 */
function serializeCDCEvent(event: CDCEvent): object {
  return {
    ...event,
    lsn: event.lsn.toString(),
    timestamp: event.timestamp instanceof Date
      ? event.timestamp.toISOString()
      : event.timestamp,
  };
}

/**
 * REST endpoint to get CDC events (for polling clients)
 */
cdc.get('/events', async (c) => {
  const db = c.get('db');
  const { fromLSN, tables, limit = '100' } = c.req.query();

  const startLSN = fromLSN ? BigInt(fromLSN) : 0n;
  const maxEvents = Math.min(1000, parseInt(limit));
  const filter = tables ? { tables: tables.split(',') } : undefined;

  const events: object[] = [];
  const subscription = db.subscribeCDC(startLSN, filter);

  try {
    for await (const event of subscription) {
      events.push(serializeCDCEvent(event));
      if (events.length >= maxEvents) break;
    }
  } finally {
    subscription.return?.();
  }

  return c.json({
    events,
    lastLSN: events.length > 0
      ? (events[events.length - 1] as any).lsn
      : fromLSN || '0',
  });
});

export default cdc;
```

### Client-Side CDC Integration

```typescript
// Example client code for CDC WebSocket
class CDCClient {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private reconnectDelay = 1000;
  private lastLSN: string | null = null;
  private handlers: Map<string, (event: any) => void> = new Map();

  constructor(private url: string, private token: string) {}

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(`${this.url}?token=${this.token}`);

      this.ws.onopen = () => {
        this.reconnectAttempts = 0;
        resolve();
      };

      this.ws.onmessage = (event) => {
        const message = JSON.parse(event.data);
        this.handleMessage(message);
      };

      this.ws.onclose = () => {
        this.handleDisconnect();
      };

      this.ws.onerror = (error) => {
        reject(error);
      };
    });
  }

  subscribe(tables?: string[], fromLSN?: string): void {
    this.send({
      type: 'subscribe',
      tables,
      fromLSN: fromLSN || this.lastLSN,
    });
  }

  onChange(handler: (event: any) => void): void {
    this.handlers.set('change', handler);
  }

  onError(handler: (error: any) => void): void {
    this.handlers.set('error', handler);
  }

  private handleMessage(message: any): void {
    switch (message.type) {
      case 'change':
        this.lastLSN = message.event.lsn;
        this.handlers.get('change')?.(message.event);
        // Acknowledge receipt
        this.send({ type: 'ack', lsn: message.event.lsn });
        break;

      case 'error':
        this.handlers.get('error')?.(message);
        break;

      case 'heartbeat':
        // Connection is alive
        break;
    }
  }

  private handleDisconnect(): void {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      setTimeout(() => {
        this.connect().then(() => {
          // Resubscribe from last position
          this.subscribe(undefined, this.lastLSN || undefined);
        });
      }, this.reconnectDelay * this.reconnectAttempts);
    }
  }

  private send(message: object): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
    }
  }

  close(): void {
    this.ws?.close();
    this.ws = null;
  }
}

// Usage
const cdc = new CDCClient('wss://api.example.com/cdc/stream', 'your-token');

await cdc.connect();

cdc.onChange((event) => {
  console.log('Change:', event.type, event.table, event.data);

  // Update local state/UI
  if (event.table === 'users') {
    updateUsersList(event);
  }
});

cdc.subscribe(['users', 'posts']);
```

---

## Example: REST API with CRUD Operations

### Complete REST API Implementation

```typescript
// src/index.ts - Complete REST API Example
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import { secureHeaders } from 'hono/secure-headers';
import { HTTPException } from 'hono/http-exception';
import { z } from 'zod';
import { createSQLClient, type SQLClient } from 'sql.do';

// =============================================================================
// Types
// =============================================================================

type Env = {
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
};

type Variables = {
  db: SQLClient;
  userId?: string;
};

interface Todo {
  id: string;
  user_id: string;
  title: string;
  description: string | null;
  completed: boolean;
  priority: 'low' | 'medium' | 'high';
  due_date: number | null;
  created_at: number;
  updated_at: number;
}

// =============================================================================
// Schemas
// =============================================================================

const CreateTodoSchema = z.object({
  title: z.string().min(1).max(200),
  description: z.string().max(2000).optional(),
  priority: z.enum(['low', 'medium', 'high']).default('medium'),
  due_date: z.number().optional(),
});

const UpdateTodoSchema = z.object({
  title: z.string().min(1).max(200).optional(),
  description: z.string().max(2000).nullable().optional(),
  completed: z.boolean().optional(),
  priority: z.enum(['low', 'medium', 'high']).optional(),
  due_date: z.number().nullable().optional(),
});

const QuerySchema = z.object({
  completed: z.enum(['true', 'false']).optional(),
  priority: z.enum(['low', 'medium', 'high']).optional(),
  sort: z.enum(['created_at', 'due_date', 'priority']).default('created_at'),
  order: z.enum(['asc', 'desc']).default('desc'),
  limit: z.coerce.number().min(1).max(100).default(20),
  offset: z.coerce.number().min(0).default(0),
});

// =============================================================================
// App Setup
// =============================================================================

const app = new Hono<{ Bindings: Env; Variables: Variables }>();

// Middleware
app.use('*', logger());
app.use('*', secureHeaders());
app.use('/api/*', cors());

// Database middleware
app.use('/api/*', async (c, next) => {
  const db = createSQLClient({
    url: c.env.DOSQL_URL,
    token: c.env.DOSQL_TOKEN,
  });
  c.set('db', db);
  await next();
});

// Auth middleware (simplified - use proper auth in production)
app.use('/api/todos/*', async (c, next) => {
  const userId = c.req.header('X-User-ID');
  if (!userId) {
    throw new HTTPException(401, { message: 'Authentication required' });
  }
  c.set('userId', userId);
  await next();
});

// =============================================================================
// Routes
// =============================================================================

// Health check
app.get('/health', (c) => c.json({ status: 'ok', timestamp: Date.now() }));

// List todos
app.get('/api/todos', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId')!;

  // Parse and validate query parameters
  const queryResult = QuerySchema.safeParse(c.req.query());
  if (!queryResult.success) {
    return c.json({ error: 'Invalid query parameters', details: queryResult.error.errors }, 400);
  }

  const { completed, priority, sort, order, limit, offset } = queryResult.data;

  // Build query
  const conditions: string[] = ['user_id = ?'];
  const params: unknown[] = [userId];

  if (completed !== undefined) {
    conditions.push('completed = ?');
    params.push(completed === 'true');
  }

  if (priority) {
    conditions.push('priority = ?');
    params.push(priority);
  }

  params.push(limit, offset);

  // Execute query
  const [todosResult, countResult] = await Promise.all([
    db.query<Todo>(
      `SELECT * FROM todos
       WHERE ${conditions.join(' AND ')}
       ORDER BY ${sort} ${order.toUpperCase()}
       LIMIT ? OFFSET ?`,
      params
    ),
    db.query<{ total: number }>(
      `SELECT COUNT(*) as total FROM todos WHERE ${conditions.join(' AND ')}`,
      params.slice(0, -2)
    ),
  ]);

  return c.json({
    todos: todosResult.rows,
    pagination: {
      total: countResult.rows[0].total,
      limit,
      offset,
      hasMore: offset + todosResult.rows.length < countResult.rows[0].total,
    },
  });
});

// Get single todo
app.get('/api/todos/:id', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId')!;
  const id = c.req.param('id');

  const result = await db.query<Todo>(
    'SELECT * FROM todos WHERE id = ? AND user_id = ?',
    [id, userId]
  );

  if (result.rows.length === 0) {
    throw new HTTPException(404, { message: 'Todo not found' });
  }

  return c.json({ todo: result.rows[0] });
});

// Create todo
app.post('/api/todos', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId')!;

  // Validate body
  const bodyResult = CreateTodoSchema.safeParse(await c.req.json());
  if (!bodyResult.success) {
    return c.json({ error: 'Validation failed', details: bodyResult.error.errors }, 400);
  }

  const { title, description, priority, due_date } = bodyResult.data;
  const id = crypto.randomUUID();
  const now = Date.now();

  await db.exec(
    `INSERT INTO todos (id, user_id, title, description, completed, priority, due_date, created_at, updated_at)
     VALUES (?, ?, ?, ?, false, ?, ?, ?, ?)`,
    [id, userId, title, description || null, priority, due_date || null, now, now]
  );

  // Return created todo
  const result = await db.query<Todo>(
    'SELECT * FROM todos WHERE id = ?',
    [id]
  );

  return c.json({ todo: result.rows[0] }, 201);
});

// Update todo
app.patch('/api/todos/:id', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId')!;
  const id = c.req.param('id');

  // Validate body
  const bodyResult = UpdateTodoSchema.safeParse(await c.req.json());
  if (!bodyResult.success) {
    return c.json({ error: 'Validation failed', details: bodyResult.error.errors }, 400);
  }

  const data = bodyResult.data;

  // Build dynamic update
  const updates: string[] = ['updated_at = ?'];
  const params: unknown[] = [Date.now()];

  if (data.title !== undefined) {
    updates.push('title = ?');
    params.push(data.title);
  }
  if (data.description !== undefined) {
    updates.push('description = ?');
    params.push(data.description);
  }
  if (data.completed !== undefined) {
    updates.push('completed = ?');
    params.push(data.completed);
  }
  if (data.priority !== undefined) {
    updates.push('priority = ?');
    params.push(data.priority);
  }
  if (data.due_date !== undefined) {
    updates.push('due_date = ?');
    params.push(data.due_date);
  }

  params.push(id, userId);

  const result = await db.query<Todo>(
    `UPDATE todos SET ${updates.join(', ')} WHERE id = ? AND user_id = ? RETURNING *`,
    params
  );

  if (result.rows.length === 0) {
    throw new HTTPException(404, { message: 'Todo not found' });
  }

  return c.json({ todo: result.rows[0] });
});

// Delete todo
app.delete('/api/todos/:id', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId')!;
  const id = c.req.param('id');

  const result = await db.exec(
    'DELETE FROM todos WHERE id = ? AND user_id = ?',
    [id, userId]
  );

  if (result.changes === 0) {
    throw new HTTPException(404, { message: 'Todo not found' });
  }

  return c.json({ success: true });
});

// Bulk operations
app.post('/api/todos/bulk/complete', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId')!;
  const { ids } = await c.req.json<{ ids: string[] }>();

  if (!Array.isArray(ids) || ids.length === 0) {
    return c.json({ error: 'ids array required' }, 400);
  }

  const placeholders = ids.map(() => '?').join(', ');
  const result = await db.exec(
    `UPDATE todos SET completed = true, updated_at = ? WHERE id IN (${placeholders}) AND user_id = ?`,
    [Date.now(), ...ids, userId]
  );

  return c.json({ updated: result.changes });
});

app.delete('/api/todos/bulk', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId')!;
  const { ids } = await c.req.json<{ ids: string[] }>();

  if (!Array.isArray(ids) || ids.length === 0) {
    return c.json({ error: 'ids array required' }, 400);
  }

  const placeholders = ids.map(() => '?').join(', ');
  const result = await db.exec(
    `DELETE FROM todos WHERE id IN (${placeholders}) AND user_id = ?`,
    [...ids, userId]
  );

  return c.json({ deleted: result.changes });
});

// =============================================================================
// Error Handler
// =============================================================================

app.onError((err, c) => {
  console.error('Error:', err);

  if (err instanceof HTTPException) {
    return c.json({ error: err.message }, err.status);
  }

  return c.json({ error: 'Internal server error' }, 500);
});

app.notFound((c) => c.json({ error: 'Not found' }, 404));

export default app;
```

---

## Example: GraphQL-like Queries

### Flexible Query API

```typescript
// src/routes/query.ts
import { Hono } from 'hono';
import { z } from 'zod';
import type { SQLClient } from 'sql.do';

type Variables = {
  db: SQLClient;
  userId: string;
};

const query = new Hono<{ Variables: Variables }>();

// =============================================================================
// GraphQL-like Query Schema
// =============================================================================

const FieldSelectionSchema = z.array(z.string()).optional();

const FilterSchema = z.object({
  field: z.string(),
  op: z.enum(['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'like', 'in', 'is_null']),
  value: z.union([z.string(), z.number(), z.boolean(), z.array(z.any()), z.null()]),
});

const QueryRequestSchema = z.object({
  // Table to query
  from: z.string(),
  // Fields to select (default: all)
  select: FieldSelectionSchema,
  // Filter conditions
  where: z.array(FilterSchema).optional(),
  // Sorting
  orderBy: z.object({
    field: z.string(),
    direction: z.enum(['asc', 'desc']).default('asc'),
  }).optional(),
  // Pagination
  limit: z.number().min(1).max(1000).default(100),
  offset: z.number().min(0).default(0),
  // Include related data
  include: z.record(z.object({
    from: z.string(),
    foreignKey: z.string(),
    select: FieldSelectionSchema,
    where: z.array(FilterSchema).optional(),
  })).optional(),
});

type QueryRequest = z.infer<typeof QueryRequestSchema>;
type Filter = z.infer<typeof FilterSchema>;

// =============================================================================
// Query Builder
// =============================================================================

// Allowed tables for security
const ALLOWED_TABLES = ['users', 'posts', 'comments', 'todos', 'tags'];

// Allowed fields per table (prevents accessing sensitive columns)
const ALLOWED_FIELDS: Record<string, string[]> = {
  users: ['id', 'name', 'avatar_url', 'created_at'],
  posts: ['id', 'title', 'content', 'author_id', 'published', 'created_at', 'updated_at'],
  comments: ['id', 'post_id', 'author_id', 'content', 'created_at'],
  todos: ['id', 'user_id', 'title', 'description', 'completed', 'priority', 'due_date', 'created_at'],
  tags: ['id', 'name', 'color'],
};

function buildFilter(filter: Filter): { sql: string; params: unknown[] } {
  const { field, op, value } = filter;
  const params: unknown[] = [];
  let sql: string;

  switch (op) {
    case 'eq':
      sql = `${field} = ?`;
      params.push(value);
      break;
    case 'ne':
      sql = `${field} != ?`;
      params.push(value);
      break;
    case 'gt':
      sql = `${field} > ?`;
      params.push(value);
      break;
    case 'gte':
      sql = `${field} >= ?`;
      params.push(value);
      break;
    case 'lt':
      sql = `${field} < ?`;
      params.push(value);
      break;
    case 'lte':
      sql = `${field} <= ?`;
      params.push(value);
      break;
    case 'like':
      sql = `${field} LIKE ?`;
      params.push(`%${value}%`);
      break;
    case 'in':
      if (!Array.isArray(value)) throw new Error('IN operator requires array value');
      const placeholders = value.map(() => '?').join(', ');
      sql = `${field} IN (${placeholders})`;
      params.push(...value);
      break;
    case 'is_null':
      sql = value ? `${field} IS NULL` : `${field} IS NOT NULL`;
      break;
    default:
      throw new Error(`Unknown operator: ${op}`);
  }

  return { sql, params };
}

function validateFields(table: string, fields?: string[]): string[] {
  const allowed = ALLOWED_FIELDS[table];
  if (!allowed) throw new Error(`Unknown table: ${table}`);

  if (!fields || fields.length === 0) {
    return allowed;
  }

  for (const field of fields) {
    if (!allowed.includes(field)) {
      throw new Error(`Field '${field}' not allowed for table '${table}'`);
    }
  }

  return fields;
}

async function executeQuery(
  db: SQLClient,
  request: QueryRequest,
  userId: string
): Promise<{ data: unknown[]; total: number }> {
  const { from, select, where, orderBy, limit, offset, include } = request;

  // Validate table
  if (!ALLOWED_TABLES.includes(from)) {
    throw new Error(`Table '${from}' not allowed`);
  }

  // Validate and get fields
  const fields = validateFields(from, select);

  // Build WHERE clause
  const conditions: string[] = [];
  const params: unknown[] = [];

  // Add ownership filter for user-scoped tables
  if (['todos', 'posts'].includes(from)) {
    conditions.push('user_id = ?');
    params.push(userId);
  }

  // Add custom filters
  if (where) {
    for (const filter of where) {
      // Validate filter field
      if (!ALLOWED_FIELDS[from]?.includes(filter.field)) {
        throw new Error(`Filter field '${filter.field}' not allowed`);
      }
      const { sql, params: filterParams } = buildFilter(filter);
      conditions.push(sql);
      params.push(...filterParams);
    }
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

  // Build ORDER BY
  let orderClause = '';
  if (orderBy) {
    if (!ALLOWED_FIELDS[from]?.includes(orderBy.field)) {
      throw new Error(`Order field '${orderBy.field}' not allowed`);
    }
    orderClause = `ORDER BY ${orderBy.field} ${orderBy.direction.toUpperCase()}`;
  }

  // Execute main query
  const mainQuery = `
    SELECT ${fields.join(', ')}
    FROM ${from}
    ${whereClause}
    ${orderClause}
    LIMIT ? OFFSET ?
  `;
  params.push(limit, offset);

  const countQuery = `SELECT COUNT(*) as total FROM ${from} ${whereClause}`;

  const [dataResult, countResult] = await Promise.all([
    db.query(mainQuery, params),
    db.query<{ total: number }>(countQuery, params.slice(0, -2)),
  ]);

  let data = dataResult.rows;

  // Handle includes (related data)
  if (include && data.length > 0) {
    for (const [key, includeSpec] of Object.entries(include)) {
      if (!ALLOWED_TABLES.includes(includeSpec.from)) {
        throw new Error(`Include table '${includeSpec.from}' not allowed`);
      }

      const includeFields = validateFields(includeSpec.from, includeSpec.select);
      const parentIds = data.map((row: any) => row.id);
      const placeholders = parentIds.map(() => '?').join(', ');

      const includeQuery = `
        SELECT ${includeFields.join(', ')}, ${includeSpec.foreignKey}
        FROM ${includeSpec.from}
        WHERE ${includeSpec.foreignKey} IN (${placeholders})
      `;

      const includeResult = await db.query(includeQuery, parentIds);

      // Group by foreign key
      const grouped = new Map<string, unknown[]>();
      for (const row of includeResult.rows as any[]) {
        const fkValue = row[includeSpec.foreignKey];
        if (!grouped.has(fkValue)) {
          grouped.set(fkValue, []);
        }
        grouped.get(fkValue)!.push(row);
      }

      // Attach to parent records
      data = data.map((row: any) => ({
        ...row,
        [key]: grouped.get(row.id) || [],
      }));
    }
  }

  return { data, total: countResult.rows[0].total };
}

// =============================================================================
// Routes
// =============================================================================

/**
 * POST /query - Execute GraphQL-like query
 *
 * Example request:
 * {
 *   "from": "posts",
 *   "select": ["id", "title", "content", "created_at"],
 *   "where": [
 *     { "field": "published", "op": "eq", "value": true }
 *   ],
 *   "orderBy": { "field": "created_at", "direction": "desc" },
 *   "limit": 10,
 *   "include": {
 *     "comments": {
 *       "from": "comments",
 *       "foreignKey": "post_id",
 *       "select": ["id", "content", "author_id"]
 *     }
 *   }
 * }
 */
query.post('/', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId');

  const bodyResult = QueryRequestSchema.safeParse(await c.req.json());
  if (!bodyResult.success) {
    return c.json({
      error: 'Invalid query',
      details: bodyResult.error.errors,
    }, 400);
  }

  try {
    const result = await executeQuery(db, bodyResult.data, userId);
    return c.json(result);
  } catch (error) {
    return c.json({
      error: 'Query execution failed',
      message: error instanceof Error ? error.message : 'Unknown error',
    }, 400);
  }
});

/**
 * POST /query/batch - Execute multiple queries
 */
query.post('/batch', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId');
  const { queries } = await c.req.json<{ queries: QueryRequest[] }>();

  if (!Array.isArray(queries) || queries.length === 0 || queries.length > 10) {
    return c.json({ error: 'queries must be an array with 1-10 items' }, 400);
  }

  const results = await Promise.all(
    queries.map(async (q, index) => {
      const parsed = QueryRequestSchema.safeParse(q);
      if (!parsed.success) {
        return { index, error: 'Invalid query', details: parsed.error.errors };
      }
      try {
        const result = await executeQuery(db, parsed.data, userId);
        return { index, ...result };
      } catch (error) {
        return {
          index,
          error: 'Query failed',
          message: error instanceof Error ? error.message : 'Unknown error',
        };
      }
    })
  );

  return c.json({ results });
});

/**
 * POST /query/aggregate - Run aggregation queries
 */
const AggregateSchema = z.object({
  from: z.string(),
  groupBy: z.array(z.string()).optional(),
  aggregations: z.array(z.object({
    function: z.enum(['count', 'sum', 'avg', 'min', 'max']),
    field: z.string().optional(),
    alias: z.string(),
  })),
  where: z.array(FilterSchema).optional(),
  having: z.array(FilterSchema).optional(),
  orderBy: z.object({
    field: z.string(),
    direction: z.enum(['asc', 'desc']).default('desc'),
  }).optional(),
  limit: z.number().min(1).max(1000).default(100),
});

query.post('/aggregate', async (c) => {
  const db = c.get('db');
  const userId = c.get('userId');

  const bodyResult = AggregateSchema.safeParse(await c.req.json());
  if (!bodyResult.success) {
    return c.json({ error: 'Invalid aggregation', details: bodyResult.error.errors }, 400);
  }

  const { from, groupBy, aggregations, where, having, orderBy, limit } = bodyResult.data;

  // Validate table
  if (!ALLOWED_TABLES.includes(from)) {
    return c.json({ error: `Table '${from}' not allowed` }, 400);
  }

  // Build SELECT clause
  const selectParts: string[] = [];
  if (groupBy) {
    selectParts.push(...groupBy);
  }

  for (const agg of aggregations) {
    switch (agg.function) {
      case 'count':
        selectParts.push(`COUNT(${agg.field || '*'}) as ${agg.alias}`);
        break;
      case 'sum':
        selectParts.push(`SUM(${agg.field}) as ${agg.alias}`);
        break;
      case 'avg':
        selectParts.push(`AVG(${agg.field}) as ${agg.alias}`);
        break;
      case 'min':
        selectParts.push(`MIN(${agg.field}) as ${agg.alias}`);
        break;
      case 'max':
        selectParts.push(`MAX(${agg.field}) as ${agg.alias}`);
        break;
    }
  }

  // Build query
  const conditions: string[] = [];
  const params: unknown[] = [];

  if (['todos', 'posts'].includes(from)) {
    conditions.push('user_id = ?');
    params.push(userId);
  }

  if (where) {
    for (const filter of where) {
      const { sql, params: filterParams } = buildFilter(filter);
      conditions.push(sql);
      params.push(...filterParams);
    }
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  const groupByClause = groupBy ? `GROUP BY ${groupBy.join(', ')}` : '';

  let havingClause = '';
  if (having && having.length > 0) {
    const havingConditions = having.map((filter) => {
      const { sql, params: filterParams } = buildFilter(filter);
      params.push(...filterParams);
      return sql;
    });
    havingClause = `HAVING ${havingConditions.join(' AND ')}`;
  }

  const orderClause = orderBy ? `ORDER BY ${orderBy.field} ${orderBy.direction.toUpperCase()}` : '';

  const query = `
    SELECT ${selectParts.join(', ')}
    FROM ${from}
    ${whereClause}
    ${groupByClause}
    ${havingClause}
    ${orderClause}
    LIMIT ?
  `;
  params.push(limit);

  const result = await db.query(query, params);
  return c.json({ data: result.rows });
});

export default query;
```

---

## Performance Optimization Tips

### 1. Connection Reuse

```typescript
// src/middleware/optimized-database.ts
import { createMiddleware } from 'hono/factory';
import { createSQLClient, type SQLClient } from 'sql.do';

type Env = {
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
};

type Variables = {
  db: SQLClient;
};

// Connection cache per request context
const connectionCache = new Map<string, SQLClient>();

/**
 * Optimized database middleware with connection caching
 */
export const optimizedDbMiddleware = createMiddleware<{
  Bindings: Env;
  Variables: Variables;
}>(async (c, next) => {
  const cacheKey = `${c.env.DOSQL_URL}:${c.env.DOSQL_TOKEN}`;

  let db = connectionCache.get(cacheKey);
  if (!db) {
    db = createSQLClient({
      url: c.env.DOSQL_URL,
      token: c.env.DOSQL_TOKEN,
      // Enable connection pooling hints
      headers: {
        'X-Connection-Pool': 'true',
      },
    });
    connectionCache.set(cacheKey, db);
  }

  c.set('db', db);
  await next();
});
```

### 2. Query Batching

```typescript
// src/lib/query-batcher.ts
import type { SQLClient } from 'sql.do';

interface PendingQuery {
  sql: string;
  params: unknown[];
  resolve: (result: any) => void;
  reject: (error: Error) => void;
}

/**
 * Query batcher - combines multiple queries into batch requests
 */
export class QueryBatcher {
  private pendingQueries: PendingQuery[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly maxBatchSize = 50;
  private readonly maxWaitMs = 5;

  constructor(private db: SQLClient) {}

  async query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    return new Promise((resolve, reject) => {
      this.pendingQueries.push({ sql, params, resolve, reject });

      if (this.pendingQueries.length >= this.maxBatchSize) {
        this.flush();
      } else if (!this.flushTimer) {
        this.flushTimer = setTimeout(() => this.flush(), this.maxWaitMs);
      }
    });
  }

  private async flush(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    const queries = this.pendingQueries.splice(0, this.maxBatchSize);
    if (queries.length === 0) return;

    try {
      const results = await this.db.batchQuery(
        queries.map((q) => ({ sql: q.sql, params: q.params }))
      );

      queries.forEach((query, index) => {
        query.resolve(results[index].rows);
      });
    } catch (error) {
      queries.forEach((query) => {
        query.reject(error as Error);
      });
    }
  }
}
```

### 3. Response Caching with KV

```typescript
// src/middleware/cache.ts
import { createMiddleware } from 'hono/factory';
import type { KVNamespace } from '@cloudflare/workers-types';

type Env = {
  CACHE: KVNamespace;
};

interface CacheOptions {
  ttl?: number; // seconds
  staleWhileRevalidate?: number; // seconds
  vary?: string[]; // headers to vary on
}

/**
 * Response caching middleware using KV
 */
export function cacheMiddleware(options: CacheOptions = {}) {
  const { ttl = 60, staleWhileRevalidate = 300, vary = [] } = options;

  return createMiddleware<{ Bindings: Env }>(async (c, next) => {
    // Only cache GET requests
    if (c.req.method !== 'GET') {
      return next();
    }

    const kv = c.env.CACHE;
    if (!kv) {
      return next();
    }

    // Build cache key
    const url = new URL(c.req.url);
    const varyParts = vary.map((h) => c.req.header(h) || '').join('|');
    const cacheKey = `cache:${url.pathname}${url.search}:${varyParts}`;

    // Check cache
    const cached = await kv.getWithMetadata<{ timestamp: number }>(cacheKey, 'json');

    if (cached.value !== null && cached.metadata) {
      const age = Math.floor((Date.now() - cached.metadata.timestamp) / 1000);

      if (age < ttl) {
        // Fresh cache hit
        return c.json(cached.value, {
          headers: {
            'X-Cache': 'HIT',
            'Age': String(age),
            'Cache-Control': `max-age=${ttl - age}`,
          },
        });
      } else if (age < ttl + staleWhileRevalidate) {
        // Stale cache - return immediately, revalidate in background
        c.executionCtx.waitUntil(revalidate(c, kv, cacheKey, ttl));

        return c.json(cached.value, {
          headers: {
            'X-Cache': 'STALE',
            'Age': String(age),
          },
        });
      }
    }

    // Cache miss - execute request
    await next();

    // Cache successful responses
    if (c.res.status === 200) {
      const body = await c.res.clone().json();
      c.executionCtx.waitUntil(
        kv.put(cacheKey, JSON.stringify(body), {
          expirationTtl: ttl + staleWhileRevalidate,
          metadata: { timestamp: Date.now() },
        })
      );

      c.res.headers.set('X-Cache', 'MISS');
    }
  });
}

async function revalidate(c: any, kv: KVNamespace, key: string, ttl: number) {
  // Re-execute the request logic and update cache
  // This would need access to the route handler
}
```

### 4. Streaming Large Results

```typescript
// src/routes/export.ts
import { Hono } from 'hono';
import { stream } from 'hono/streaming';
import type { SQLClient } from 'sql.do';

type Variables = {
  db: SQLClient;
};

const exportRoutes = new Hono<{ Variables: Variables }>();

/**
 * Stream large query results as JSON Lines
 */
exportRoutes.get('/todos/export', async (c) => {
  const db = c.get('db');
  const userId = c.req.header('X-User-ID')!;

  return stream(c, async (stream) => {
    let offset = 0;
    const batchSize = 100;
    let hasMore = true;

    while (hasMore) {
      const result = await db.query(
        'SELECT * FROM todos WHERE user_id = ? ORDER BY created_at LIMIT ? OFFSET ?',
        [userId, batchSize, offset]
      );

      for (const row of result.rows) {
        await stream.write(JSON.stringify(row) + '\n');
      }

      hasMore = result.rows.length === batchSize;
      offset += batchSize;
    }
  }, {
    headers: {
      'Content-Type': 'application/x-ndjson',
      'Content-Disposition': 'attachment; filename="todos.jsonl"',
    },
  });
});

/**
 * Stream as CSV
 */
exportRoutes.get('/todos/export.csv', async (c) => {
  const db = c.get('db');
  const userId = c.req.header('X-User-ID')!;

  return stream(c, async (stream) => {
    // Write header
    await stream.write('id,title,completed,priority,created_at\n');

    let offset = 0;
    const batchSize = 100;
    let hasMore = true;

    while (hasMore) {
      const result = await db.query<{
        id: string;
        title: string;
        completed: boolean;
        priority: string;
        created_at: number;
      }>(
        'SELECT id, title, completed, priority, created_at FROM todos WHERE user_id = ? ORDER BY created_at LIMIT ? OFFSET ?',
        [userId, batchSize, offset]
      );

      for (const row of result.rows) {
        const line = [
          row.id,
          `"${row.title.replace(/"/g, '""')}"`,
          row.completed,
          row.priority,
          new Date(row.created_at).toISOString(),
        ].join(',');
        await stream.write(line + '\n');
      }

      hasMore = result.rows.length === batchSize;
      offset += batchSize;
    }
  }, {
    headers: {
      'Content-Type': 'text/csv',
      'Content-Disposition': 'attachment; filename="todos.csv"',
    },
  });
});

export default exportRoutes;
```

### 5. Query Optimization Patterns

```typescript
// src/lib/query-optimizer.ts
import type { SQLClient } from 'sql.do';

/**
 * Query optimizer utilities
 */
export class QueryOptimizer {
  constructor(private db: SQLClient) {}

  /**
   * Use covering indexes - select only indexed columns when possible
   */
  async getIdsOnly(table: string, where: string, params: unknown[]): Promise<string[]> {
    const result = await this.db.query<{ id: string }>(
      `SELECT id FROM ${table} ${where}`,
      params
    );
    return result.rows.map((r) => r.id);
  }

  /**
   * Use EXISTS instead of COUNT for existence checks
   */
  async exists(table: string, where: string, params: unknown[]): Promise<boolean> {
    const result = await this.db.query<{ e: number }>(
      `SELECT EXISTS(SELECT 1 FROM ${table} ${where} LIMIT 1) as e`,
      params
    );
    return result.rows[0].e === 1;
  }

  /**
   * Paginate with keyset pagination for large datasets
   */
  async keysetPaginate<T>(
    table: string,
    options: {
      select: string;
      orderBy: string;
      direction: 'asc' | 'desc';
      cursor?: string | number;
      limit: number;
      where?: string;
      params?: unknown[];
    }
  ): Promise<{ rows: T[]; nextCursor: string | number | null }> {
    const { select, orderBy, direction, cursor, limit, where, params = [] } = options;

    const conditions: string[] = [];
    const queryParams = [...params];

    if (where) {
      conditions.push(where);
    }

    if (cursor !== undefined) {
      const op = direction === 'asc' ? '>' : '<';
      conditions.push(`${orderBy} ${op} ?`);
      queryParams.push(cursor);
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    queryParams.push(limit + 1); // Fetch one extra to check for more

    const result = await this.db.query<T>(
      `SELECT ${select} FROM ${table} ${whereClause} ORDER BY ${orderBy} ${direction.toUpperCase()} LIMIT ?`,
      queryParams
    );

    const hasMore = result.rows.length > limit;
    const rows = hasMore ? result.rows.slice(0, -1) : result.rows;
    const nextCursor = hasMore ? (rows[rows.length - 1] as any)[orderBy] : null;

    return { rows, nextCursor };
  }

  /**
   * Use UNION ALL for OR conditions on different columns
   */
  async searchMultipleColumns<T>(
    table: string,
    columns: string[],
    searchTerm: string,
    limit: number
  ): Promise<T[]> {
    // More efficient than: WHERE col1 LIKE ? OR col2 LIKE ?
    const unions = columns.map(
      (col) => `SELECT * FROM ${table} WHERE ${col} LIKE ? LIMIT ?`
    );

    const result = await this.db.query<T>(
      `SELECT DISTINCT * FROM (${unions.join(' UNION ALL ')}) LIMIT ?`,
      columns.flatMap(() => [`%${searchTerm}%`, limit]).concat(limit)
    );

    return result.rows;
  }
}
```

### 6. Request Coalescing

```typescript
// src/lib/request-coalescer.ts
type Resolver<T> = {
  resolve: (value: T) => void;
  reject: (error: Error) => void;
};

/**
 * Coalesces identical concurrent requests into a single database call
 */
export class RequestCoalescer {
  private pending = new Map<string, Promise<unknown>>();
  private waiters = new Map<string, Resolver<unknown>[]>();

  async coalesce<T>(
    key: string,
    executor: () => Promise<T>
  ): Promise<T> {
    // Check if this request is already in flight
    const existing = this.pending.get(key);
    if (existing) {
      return existing as Promise<T>;
    }

    // Execute the request
    const promise = executor()
      .then((result) => {
        this.pending.delete(key);
        return result;
      })
      .catch((error) => {
        this.pending.delete(key);
        throw error;
      });

    this.pending.set(key, promise);
    return promise;
  }
}

// Usage in routes
const coalescer = new RequestCoalescer();

app.get('/api/users/:id', async (c) => {
  const db = c.get('db');
  const id = c.req.param('id');

  const user = await coalescer.coalesce(
    `user:${id}`,
    () => db.query('SELECT * FROM users WHERE id = ?', [id]).then((r) => r.rows[0])
  );

  if (!user) {
    return c.json({ error: 'User not found' }, 404);
  }

  return c.json({ user });
});
```

---

## Summary

This guide covered:

1. **Installation and Setup** - Project structure, dependencies, and basic configuration
2. **Middleware** - Database context, tenant isolation, transactions, and read replicas
3. **Route Handlers** - Type-safe CRUD operations and dynamic queries
4. **Validation** - Zod integration for request/response validation
5. **Deployment** - Cloudflare Workers configuration and production setup
6. **Type Safety** - Full TypeScript integration with Hono's type system
7. **CDC Streaming** - Real-time change notifications via WebSockets
8. **REST API** - Complete CRUD example with pagination and filtering
9. **GraphQL-like Queries** - Flexible query API with field selection and includes
10. **Performance** - Caching, batching, streaming, and query optimization

For more information, see:

- [DoSQL API Reference](../api-reference.md)
- [Performance Tuning Guide](../PERFORMANCE_TUNING.md)
- [Security Guide](../SECURITY.md)
- [Error Codes Reference](../ERROR_CODES.md)
- [Hono Documentation](https://hono.dev)
