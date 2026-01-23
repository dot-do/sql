# DoSQL + Remix Integration Guide

Build full-stack applications with DoSQL and Remix on Cloudflare Workers. This guide covers loaders, actions, resource routes, real-time features, and deployment patterns for production-ready applications.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Setup](#setup)
- [Database Module](#database-module)
- [Loaders](#loaders)
- [Actions](#actions)
- [Resource Routes](#resource-routes)
- [Patterns](#patterns)
- [Type Safety](#type-safety)
- [Advanced Features](#advanced-features)
- [Testing](#testing)
- [Deployment](#deployment)

---

## Overview

### Why DoSQL + Remix

Remix and DoSQL are a natural fit for building full-stack applications on Cloudflare Workers:

| Remix Feature | DoSQL Benefit |
|---------------|---------------|
| Loaders | Direct database queries at the edge with zero cold starts |
| Actions | Transactional mutations with automatic rollback on errors |
| Nested Routes | Parallel data loading per route segment |
| `defer()` | Streaming deferred database results for optimal UX |
| Resource Routes | JSON APIs, webhooks, and file downloads |
| Forms | Progressive enhancement with optimistic UI |

### Architecture

```
+------------------------------------------------------------------+
|                    REMIX + DOSQL ARCHITECTURE                     |
+------------------------------------------------------------------+

                         Browser Request
                              |
                              v
+------------------------------------------------------------------+
|                    Cloudflare Worker                              |
|  +------------------------------------------------------------+  |
|  |                   Remix Application                         |  |
|  |  +------------------+  +------------------+                 |  |
|  |  |    Loaders       |  |    Actions       |                 |  |
|  |  |  (data fetching) |  |  (mutations)     |                 |  |
|  |  +--------+---------+  +--------+---------+                 |  |
|  |           |                     |                           |  |
|  |           +----------+----------+                           |  |
|  |                      |                                      |  |
|  |                      v                                      |  |
|  |            +------------------+                             |  |
|  |            |   getDB(env)     |                             |  |
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
|  |   SQLite DB      |  |     |  |   SQLite DB      |  |
|  |   - users        |  |     |  |   - users        |  |
|  |   - posts        |  |     |  |   - posts        |  |
|  +------------------+  |     |  +------------------+  |
+------------------------+     +------------------------+
```

**Request flow:**

1. Browser sends request to Cloudflare Worker
2. Remix router matches the URL and runs loaders/actions
3. Loaders and actions access DoSQL via Durable Object RPC
4. DoSQL executes queries against SQLite with full ACID guarantees
5. Remix renders the response (HTML for pages, JSON for resource routes)
6. Browser receives the response with zero cold start latency

---

## Quick Start

Get a Remix + DoSQL application running in under 5 minutes.

```bash
# Create a new Remix project with Cloudflare template
npx create-remix@latest my-remix-app --template remix-run/remix/templates/cloudflare

cd my-remix-app

# Install DoSQL
npm install @dotdo/dosql

# Create migrations folder
mkdir -p .do/migrations
```

Create your first migration at `.do/migrations/001_init.sql`:

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
```

Create `app/db.server.ts`:

```typescript
import { DB } from '@dotdo/dosql';

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export async function getDB(env: Env, tenantId: string = 'default') {
  const id = env.DOSQL_DB.idFromName(tenantId);
  const stub = env.DOSQL_DB.get(id);
  return createDBClient(stub);
}

function createDBClient(stub: DurableObjectStub) {
  return {
    async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
      const res = await stub.fetch('http://internal/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params }),
      });
      const data = await res.json() as { rows: T[] };
      return data.rows;
    },

    async queryOne<T>(sql: string, params?: unknown[]): Promise<T | null> {
      const rows = await this.query<T>(sql, params);
      return rows[0] ?? null;
    },

    async run(sql: string, params?: unknown[]) {
      const res = await stub.fetch('http://internal/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params }),
      });
      return res.json() as Promise<{ rowsAffected: number; lastInsertRowId: number }>;
    },
  };
}
```

Create a simple route at `app/routes/users._index.tsx`:

```typescript
import { json, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { useLoaderData, Link } from '@remix-run/react';
import { getDB } from '~/db.server';

interface User {
  id: number;
  name: string;
  email: string;
}

export async function loader({ context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const users = await db.query<User>('SELECT * FROM users ORDER BY id DESC');
  return json({ users });
}

export default function UsersIndex() {
  const { users } = useLoaderData<typeof loader>();

  return (
    <div>
      <h1>Users</h1>
      <Link to="/users/new">Add User</Link>
      <ul>
        {users.map((user) => (
          <li key={user.id}>
            <Link to={`/users/${user.id}`}>{user.name}</Link> - {user.email}
          </li>
        ))}
      </ul>
    </div>
  );
}
```

Run locally:

```bash
npm run dev
```

---

## Setup

### Installation

```bash
# Create Remix project with Cloudflare Workers template
npx create-remix@latest my-remix-app --template remix-run/remix/templates/cloudflare

cd my-remix-app

# Install DoSQL and dev dependencies
npm install @dotdo/dosql
npm install -D @cloudflare/workers-types wrangler
```

### Project Structure

```
my-remix-app/
├── .do/
│   └── migrations/
│       ├── 001_create_users.sql
│       └── 002_create_posts.sql
├── app/
│   ├── db.server.ts          # Database client (server-only)
│   ├── types/
│   │   └── database.ts       # Shared type definitions
│   ├── routes/
│   │   ├── _index.tsx
│   │   ├── users._index.tsx
│   │   ├── users.new.tsx
│   │   ├── users.$id.tsx
│   │   ├── users.$id.edit.tsx
│   │   └── api.users.tsx     # Resource route
│   ├── root.tsx
│   └── entry.server.tsx
├── server/
│   └── durable-object.ts     # DoSQL Durable Object
├── load-context.ts           # Cloudflare context types
├── wrangler.toml
├── package.json
├── vite.config.ts
└── tsconfig.json
```

### Environment Configuration

Create `load-context.ts` for type-safe environment bindings:

```typescript
// load-context.ts
import { type PlatformProxy } from 'wrangler';

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  DATA_BUCKET?: R2Bucket;
  SESSION_KV?: KVNamespace;
}

type Cloudflare = Omit<PlatformProxy<Env>, 'dispose'>;

declare module '@remix-run/cloudflare' {
  interface AppLoadContext {
    cloudflare: Cloudflare;
  }
}

export function getLoadContext({
  context,
}: {
  request: Request;
  context: { cloudflare: Cloudflare };
}) {
  return context;
}
```

### Wrangler Configuration

Configure `wrangler.toml`:

```toml
name = "my-remix-app"
main = "build/server/index.js"
compatibility_date = "2025-01-01"
compatibility_flags = ["nodejs_compat"]

# Static assets
[site]
bucket = "./build/client"

# DoSQL Durable Object
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

# Optional: R2 for cold storage tier
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-remix-data"

# Optional: KV for sessions
[[kv_namespaces]]
binding = "SESSION_KV"
id = "your-kv-id"
```

### Vite Configuration

Update `vite.config.ts` to copy migrations:

```typescript
import { vitePlugin as remix } from '@remix-run/dev';
import { defineConfig } from 'vite';
import { copyFileSync, mkdirSync, readdirSync } from 'fs';

export default defineConfig({
  plugins: [
    remix({
      future: {
        v3_fetcherPersist: true,
        v3_relativeSplatPath: true,
        v3_throwAbortReason: true,
        v3_singleFetch: true,
        v3_lazyRouteDiscovery: true,
      },
    }),
    {
      name: 'copy-migrations',
      buildEnd() {
        const srcDir = '.do/migrations';
        const destDir = 'build/server/.do/migrations';
        mkdirSync(destDir, { recursive: true });

        for (const file of readdirSync(srcDir)) {
          if (file.endsWith('.sql')) {
            copyFileSync(`${srcDir}/${file}`, `${destDir}/${file}`);
          }
        }
      },
    },
  ],
});
```

---

## Database Module

### Durable Object Implementation

Create `server/durable-object.ts`:

```typescript
import { DB } from '@dotdo/dosql';

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  DATA_BUCKET?: R2Bucket;
}

interface Operation {
  type: 'query' | 'run';
  sql: string;
  params?: unknown[];
}

export class DoSQLDatabase implements DurableObject {
  private db: Awaited<ReturnType<typeof DB>> | null = null;

  constructor(
    private state: DurableObjectState,
    private env: Env
  ) {}

  private async getDB() {
    if (!this.db) {
      this.db = await DB('remix-app', {
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
    const url = new URL(request.url);
    const db = await this.getDB();

    try {
      switch (url.pathname) {
        case '/query': {
          const { sql, params } = await request.json() as { sql: string; params?: unknown[] };
          const rows = await db.query(sql, params);
          return Response.json({ rows });
        }

        case '/queryOne': {
          const { sql, params } = await request.json() as { sql: string; params?: unknown[] };
          const rows = await db.query(sql, params);
          return Response.json({ row: rows[0] ?? null });
        }

        case '/run': {
          const { sql, params } = await request.json() as { sql: string; params?: unknown[] };
          const result = await db.run(sql, params);
          return Response.json(result);
        }

        case '/transaction': {
          const { operations } = await request.json() as { operations: Operation[] };
          const result = await db.transaction(async (tx) => {
            const results: unknown[] = [];
            for (const op of operations) {
              if (op.type === 'query') {
                results.push(await tx.query(op.sql, op.params));
              } else {
                results.push(await tx.run(op.sql, op.params));
              }
            }
            return results;
          });
          return Response.json({ results: result });
        }

        default:
          return new Response('Not Found', { status: 404 });
      }
    } catch (error) {
      console.error('Database error:', error);
      return Response.json(
        { error: (error as Error).message },
        { status: 500 }
      );
    }
  }
}
```

### Database Client

Create `app/db.server.ts`:

```typescript
// app/db.server.ts
// This file has the .server suffix so Remix excludes it from client bundles

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  DATA_BUCKET?: R2Bucket;
}

export interface DBClient {
  query<T>(sql: string, params?: unknown[]): Promise<T[]>;
  queryOne<T>(sql: string, params?: unknown[]): Promise<T | null>;
  run(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowId: number }>;
  transaction<T>(fn: (tx: TransactionClient) => Promise<T>): Promise<T>;
}

export interface TransactionClient {
  query<T>(sql: string, params?: unknown[]): Promise<T[]>;
  run(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowId: number }>;
}

export async function getDB(env: Env, tenantId: string = 'default'): Promise<DBClient> {
  const id = env.DOSQL_DB.idFromName(tenantId);
  const stub = env.DOSQL_DB.get(id);
  return createDBClient(stub);
}

function createDBClient(stub: DurableObjectStub): DBClient {
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

    async run(sql: string, params?: unknown[]) {
      return rpc<{ rowsAffected: number; lastInsertRowId: number }>('/run', { sql, params });
    },

    async transaction<T>(fn: (tx: TransactionClient) => Promise<T>): Promise<T> {
      // Collect operations from the transaction function
      const operations: Array<{ type: 'query' | 'run'; sql: string; params?: unknown[] }> = [];

      const txClient: TransactionClient = {
        async query<U>(sql: string, params?: unknown[]): Promise<U[]> {
          operations.push({ type: 'query', sql, params });
          return [] as U[];
        },
        async run(sql: string, params?: unknown[]) {
          operations.push({ type: 'run', sql, params });
          return { rowsAffected: 0, lastInsertRowId: 0 };
        },
      };

      await fn(txClient);

      const result = await rpc<{ results: unknown[] }>('/transaction', { operations });
      return result.results[result.results.length - 1] as T;
    },
  };
}

// Multi-tenant helper
export function getTenantId(request: Request): string {
  // Option 1: From header
  const headerTenant = request.headers.get('X-Tenant-ID');
  if (headerTenant) return headerTenant;

  // Option 2: From subdomain
  const url = new URL(request.url);
  const subdomain = url.hostname.split('.')[0];
  if (subdomain && subdomain !== 'www' && subdomain !== 'app') {
    return subdomain;
  }

  // Option 3: From path (e.g., /tenant/acme/...)
  const pathMatch = url.pathname.match(/^\/tenant\/([^/]+)/);
  if (pathMatch) return pathMatch[1];

  return 'default';
}
```

---

## Loaders

Loaders fetch data on the server before rendering. They run on every navigation and can access the database directly.

### Basic Loader

```typescript
// app/routes/users._index.tsx
import { json, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { useLoaderData, Link } from '@remix-run/react';
import { getDB } from '~/db.server';

interface User {
  id: number;
  name: string;
  email: string;
  created_at: string;
}

export async function loader({ context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  const users = await db.query<User>(`
    SELECT id, name, email, created_at
    FROM users
    ORDER BY created_at DESC
    LIMIT 50
  `);

  return json({ users });
}

export default function UsersIndex() {
  const { users } = useLoaderData<typeof loader>();

  return (
    <div>
      <h1>Users ({users.length})</h1>
      <Link to="/users/new">Add User</Link>
      <ul>
        {users.map((user) => (
          <li key={user.id}>
            <Link to={`/users/${user.id}`}>{user.name}</Link>
            <span> - {user.email}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
```

### Loader with URL Parameters

```typescript
// app/routes/users.$id.tsx
import { json, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { useLoaderData, Link } from '@remix-run/react';
import { getDB } from '~/db.server';

interface User {
  id: number;
  name: string;
  email: string;
  bio: string | null;
  created_at: string;
}

export async function loader({ params, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  const user = await db.queryOne<User>(
    'SELECT * FROM users WHERE id = ?',
    [params.id]
  );

  if (!user) {
    throw json({ message: 'User not found' }, { status: 404 });
  }

  return json({ user });
}

export default function UserProfile() {
  const { user } = useLoaderData<typeof loader>();

  return (
    <div>
      <Link to="/users">Back to Users</Link>
      <h1>{user.name}</h1>
      <p>Email: {user.email}</p>
      {user.bio && <p>Bio: {user.bio}</p>}
      <p>Joined: {new Date(user.created_at).toLocaleDateString()}</p>
      <Link to={`/users/${user.id}/edit`}>Edit</Link>
    </div>
  );
}
```

### Loader with Search and Pagination

```typescript
// app/routes/users._index.tsx
import { json, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { useLoaderData, useSearchParams, Link, Form } from '@remix-run/react';
import { getDB } from '~/db.server';

interface User {
  id: number;
  name: string;
  email: string;
}

export async function loader({ request, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const url = new URL(request.url);

  const search = url.searchParams.get('q') || '';
  const page = parseInt(url.searchParams.get('page') || '1', 10);
  const limit = 20;
  const offset = (page - 1) * limit;

  let sql = 'SELECT id, name, email FROM users';
  let countSql = 'SELECT COUNT(*) as total FROM users';
  const params: unknown[] = [];
  const countParams: unknown[] = [];

  if (search) {
    const whereClause = ' WHERE name LIKE ? OR email LIKE ?';
    sql += whereClause;
    countSql += whereClause;
    params.push(`%${search}%`, `%${search}%`);
    countParams.push(`%${search}%`, `%${search}%`);
  }

  sql += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);

  const [users, countResult] = await Promise.all([
    db.query<User>(sql, params),
    db.queryOne<{ total: number }>(countSql, countParams),
  ]);

  const total = countResult?.total || 0;
  const totalPages = Math.ceil(total / limit);

  return json({ users, page, totalPages, search, total });
}

export default function UsersIndex() {
  const { users, page, totalPages, search, total } = useLoaderData<typeof loader>();
  const [searchParams] = useSearchParams();

  return (
    <div>
      <h1>Users ({total})</h1>

      <Form method="get">
        <input
          type="search"
          name="q"
          placeholder="Search users..."
          defaultValue={search}
        />
        <button type="submit">Search</button>
      </Form>

      <ul>
        {users.map((user) => (
          <li key={user.id}>
            <Link to={`/users/${user.id}`}>{user.name}</Link> - {user.email}
          </li>
        ))}
      </ul>

      <nav>
        {page > 1 && (
          <Link to={`?${new URLSearchParams({ ...Object.fromEntries(searchParams), page: String(page - 1) })}`}>
            Previous
          </Link>
        )}
        <span>Page {page} of {totalPages}</span>
        {page < totalPages && (
          <Link to={`?${new URLSearchParams({ ...Object.fromEntries(searchParams), page: String(page + 1) })}`}>
            Next
          </Link>
        )}
      </nav>
    </div>
  );
}
```

### Deferred Loading with defer()

Use `defer()` to stream non-critical data for better perceived performance:

```typescript
// app/routes/dashboard.tsx
import { defer, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { Await, useLoaderData } from '@remix-run/react';
import { Suspense } from 'react';
import { getDB } from '~/db.server';

interface Stats {
  totalUsers: number;
  totalPosts: number;
  activeToday: number;
}

interface RecentUser {
  id: number;
  name: string;
  created_at: string;
}

export async function loader({ context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  // Critical data - await immediately for initial render
  const stats = await db.queryOne<Stats>(`
    SELECT
      (SELECT COUNT(*) FROM users) as totalUsers,
      (SELECT COUNT(*) FROM posts) as totalPosts,
      (SELECT COUNT(DISTINCT user_id) FROM activity
       WHERE created_at > datetime('now', '-24 hours')) as activeToday
  `);

  // Non-critical data - defer and stream
  const recentUsersPromise = db.query<RecentUser>(`
    SELECT id, name, created_at
    FROM users
    ORDER BY created_at DESC
    LIMIT 10
  `);

  const topPostsPromise = db.query<{ id: number; title: string; likes: number }>(`
    SELECT p.id, p.title, COUNT(l.id) as likes
    FROM posts p
    LEFT JOIN likes l ON l.post_id = p.id
    WHERE p.published = 1
    GROUP BY p.id
    ORDER BY likes DESC
    LIMIT 5
  `);

  return defer({
    stats,
    recentUsers: recentUsersPromise,
    topPosts: topPostsPromise,
  });
}

export default function Dashboard() {
  const { stats, recentUsers, topPosts } = useLoaderData<typeof loader>();

  return (
    <div>
      <h1>Dashboard</h1>

      {/* Critical data renders immediately */}
      <section className="stats">
        <div>Total Users: {stats?.totalUsers ?? 0}</div>
        <div>Total Posts: {stats?.totalPosts ?? 0}</div>
        <div>Active Today: {stats?.activeToday ?? 0}</div>
      </section>

      {/* Deferred data streams in */}
      <div className="panels">
        <section>
          <h2>Recent Users</h2>
          <Suspense fallback={<div>Loading recent users...</div>}>
            <Await resolve={recentUsers}>
              {(users) => (
                <ul>
                  {users.map((user) => (
                    <li key={user.id}>{user.name}</li>
                  ))}
                </ul>
              )}
            </Await>
          </Suspense>
        </section>

        <section>
          <h2>Top Posts</h2>
          <Suspense fallback={<div>Loading top posts...</div>}>
            <Await resolve={topPosts}>
              {(posts) => (
                <ul>
                  {posts.map((post) => (
                    <li key={post.id}>
                      {post.title} ({post.likes} likes)
                    </li>
                  ))}
                </ul>
              )}
            </Await>
          </Suspense>
        </section>
      </div>
    </div>
  );
}
```

---

## Actions

Actions handle form submissions and mutations. They run on POST, PUT, PATCH, and DELETE requests.

### Basic Action

```typescript
// app/routes/users.new.tsx
import {
  json,
  redirect,
  type ActionFunctionArgs,
} from '@remix-run/cloudflare';
import { Form, useActionData, useNavigation } from '@remix-run/react';
import { getDB } from '~/db.server';

interface ActionData {
  errors?: {
    name?: string;
    email?: string;
  };
}

export async function action({ request, context }: ActionFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const formData = await request.formData();

  const name = formData.get('name') as string;
  const email = formData.get('email') as string;

  // Validation
  const errors: ActionData['errors'] = {};

  if (!name || name.length < 2) {
    errors.name = 'Name must be at least 2 characters';
  }
  if (!email || !email.includes('@')) {
    errors.email = 'Valid email is required';
  }

  if (Object.keys(errors).length > 0) {
    return json<ActionData>({ errors }, { status: 400 });
  }

  try {
    const result = await db.run(
      'INSERT INTO users (name, email) VALUES (?, ?)',
      [name, email]
    );

    return redirect(`/users/${result.lastInsertRowId}`);
  } catch (error) {
    if ((error as Error).message.includes('UNIQUE constraint')) {
      return json<ActionData>(
        { errors: { email: 'Email already exists' } },
        { status: 400 }
      );
    }
    throw error;
  }
}

export default function NewUser() {
  const actionData = useActionData<ActionData>();
  const navigation = useNavigation();
  const isSubmitting = navigation.state === 'submitting';

  return (
    <div>
      <h1>New User</h1>
      <Form method="post">
        <div>
          <label htmlFor="name">Name</label>
          <input
            type="text"
            id="name"
            name="name"
            required
            minLength={2}
          />
          {actionData?.errors?.name && (
            <span className="error">{actionData.errors.name}</span>
          )}
        </div>

        <div>
          <label htmlFor="email">Email</label>
          <input
            type="email"
            id="email"
            name="email"
            required
          />
          {actionData?.errors?.email && (
            <span className="error">{actionData.errors.email}</span>
          )}
        </div>

        <button type="submit" disabled={isSubmitting}>
          {isSubmitting ? 'Creating...' : 'Create User'}
        </button>
      </Form>
    </div>
  );
}
```

### Action with Multiple Intents

```typescript
// app/routes/users.$id.tsx
import {
  json,
  redirect,
  type ActionFunctionArgs,
  type LoaderFunctionArgs,
} from '@remix-run/cloudflare';
import { Form, useLoaderData, useNavigation } from '@remix-run/react';
import { getDB } from '~/db.server';

interface User {
  id: number;
  name: string;
  email: string;
}

export async function loader({ params, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  const user = await db.queryOne<User>(
    'SELECT * FROM users WHERE id = ?',
    [params.id]
  );

  if (!user) {
    throw json({ message: 'User not found' }, { status: 404 });
  }

  return json({ user });
}

export async function action({ request, params, context }: ActionFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const formData = await request.formData();
  const intent = formData.get('intent');

  switch (intent) {
    case 'update': {
      const name = formData.get('name') as string;
      const email = formData.get('email') as string;

      await db.run(
        'UPDATE users SET name = ?, email = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
        [name, email, params.id]
      );

      return json({ success: true });
    }

    case 'delete': {
      await db.run('DELETE FROM users WHERE id = ?', [params.id]);
      return redirect('/users');
    }

    default:
      return json({ error: 'Invalid intent' }, { status: 400 });
  }
}

export default function UserDetail() {
  const { user } = useLoaderData<typeof loader>();
  const navigation = useNavigation();
  const isUpdating = navigation.formData?.get('intent') === 'update';
  const isDeleting = navigation.formData?.get('intent') === 'delete';

  return (
    <div>
      <h1>Edit User</h1>

      <Form method="post">
        <input type="hidden" name="intent" value="update" />
        <div>
          <label htmlFor="name">Name</label>
          <input
            type="text"
            id="name"
            name="name"
            defaultValue={user.name}
            required
          />
        </div>
        <div>
          <label htmlFor="email">Email</label>
          <input
            type="email"
            id="email"
            name="email"
            defaultValue={user.email}
            required
          />
        </div>
        <button type="submit" disabled={isUpdating}>
          {isUpdating ? 'Saving...' : 'Save Changes'}
        </button>
      </Form>

      <Form method="post">
        <input type="hidden" name="intent" value="delete" />
        <button
          type="submit"
          disabled={isDeleting}
          onClick={(e) => {
            if (!confirm('Are you sure you want to delete this user?')) {
              e.preventDefault();
            }
          }}
        >
          {isDeleting ? 'Deleting...' : 'Delete User'}
        </button>
      </Form>
    </div>
  );
}
```

### Transactional Actions

```typescript
// app/routes/api.transfer.tsx
import { json, type ActionFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';

export async function action({ request, context }: ActionFunctionArgs) {
  if (request.method !== 'POST') {
    return json({ error: 'Method not allowed' }, { status: 405 });
  }

  const db = await getDB(context.cloudflare.env);
  const { fromAccountId, toAccountId, amount } = await request.json() as {
    fromAccountId: number;
    toAccountId: number;
    amount: number;
  };

  if (amount <= 0) {
    return json({ error: 'Amount must be positive' }, { status: 400 });
  }

  try {
    const result = await db.transaction(async (tx) => {
      // Check source balance
      const source = await tx.query<{ balance: number }>(
        'SELECT balance FROM accounts WHERE id = ?',
        [fromAccountId]
      );

      if (!source[0] || source[0].balance < amount) {
        throw new Error('Insufficient funds');
      }

      // Debit source account
      await tx.run(
        'UPDATE accounts SET balance = balance - ? WHERE id = ?',
        [amount, fromAccountId]
      );

      // Credit destination account
      await tx.run(
        'UPDATE accounts SET balance = balance + ? WHERE id = ?',
        [amount, toAccountId]
      );

      // Record the transfer
      await tx.run(
        `INSERT INTO transfers (from_account_id, to_account_id, amount)
         VALUES (?, ?, ?)`,
        [fromAccountId, toAccountId, amount]
      );

      return { success: true };
    });

    return json(result);
  } catch (error) {
    return json(
      { error: (error as Error).message },
      { status: 400 }
    );
  }
}
```

---

## Resource Routes

Resource routes return non-HTML responses like JSON, files, or streams. They have no default export (no UI component).

### JSON API Endpoint

```typescript
// app/routes/api.users.tsx
import { json, type ActionFunctionArgs, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';

interface User {
  id: number;
  name: string;
  email: string;
  created_at: string;
}

// GET /api/users
export async function loader({ request, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const url = new URL(request.url);

  const limit = Math.min(parseInt(url.searchParams.get('limit') || '50', 10), 100);
  const offset = parseInt(url.searchParams.get('offset') || '0', 10);
  const search = url.searchParams.get('q');

  let sql = 'SELECT id, name, email, created_at FROM users';
  const params: unknown[] = [];

  if (search) {
    sql += ' WHERE name LIKE ? OR email LIKE ?';
    params.push(`%${search}%`, `%${search}%`);
  }

  sql += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);

  const [users, countResult] = await Promise.all([
    db.query<User>(sql, params),
    db.queryOne<{ total: number }>(
      'SELECT COUNT(*) as total FROM users' +
        (search ? ' WHERE name LIKE ? OR email LIKE ?' : ''),
      search ? [`%${search}%`, `%${search}%`] : []
    ),
  ]);

  return json({
    data: users,
    pagination: {
      total: countResult?.total || 0,
      limit,
      offset,
    },
  });
}

// POST /api/users
export async function action({ request, context }: ActionFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  switch (request.method) {
    case 'POST': {
      const body = await request.json() as { name: string; email: string };

      if (!body.name || !body.email) {
        return json({ error: 'Name and email are required' }, { status: 400 });
      }

      try {
        const result = await db.run(
          'INSERT INTO users (name, email) VALUES (?, ?)',
          [body.name, body.email]
        );

        const user = await db.queryOne<User>(
          'SELECT * FROM users WHERE id = ?',
          [result.lastInsertRowId]
        );

        return json({ data: user }, { status: 201 });
      } catch (error) {
        if ((error as Error).message.includes('UNIQUE constraint')) {
          return json({ error: 'Email already exists' }, { status: 409 });
        }
        throw error;
      }
    }

    default:
      return json({ error: 'Method not allowed' }, { status: 405 });
  }
}
```

### Individual Resource Endpoint

```typescript
// app/routes/api.users.$id.tsx
import { json, type ActionFunctionArgs, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';

interface User {
  id: number;
  name: string;
  email: string;
  bio: string | null;
  created_at: string;
}

// GET /api/users/:id
export async function loader({ params, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  const user = await db.queryOne<User>(
    'SELECT * FROM users WHERE id = ?',
    [params.id]
  );

  if (!user) {
    return json({ error: 'User not found' }, { status: 404 });
  }

  return json({ data: user });
}

// PATCH/DELETE /api/users/:id
export async function action({ request, params, context }: ActionFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  switch (request.method) {
    case 'PATCH': {
      const body = await request.json() as Partial<{ name: string; email: string; bio: string }>;

      const updates: string[] = [];
      const values: unknown[] = [];

      if (body.name !== undefined) {
        updates.push('name = ?');
        values.push(body.name);
      }
      if (body.email !== undefined) {
        updates.push('email = ?');
        values.push(body.email);
      }
      if (body.bio !== undefined) {
        updates.push('bio = ?');
        values.push(body.bio);
      }

      if (updates.length === 0) {
        return json({ error: 'No fields to update' }, { status: 400 });
      }

      updates.push('updated_at = CURRENT_TIMESTAMP');
      values.push(params.id);

      const result = await db.run(
        `UPDATE users SET ${updates.join(', ')} WHERE id = ?`,
        values
      );

      if (result.rowsAffected === 0) {
        return json({ error: 'User not found' }, { status: 404 });
      }

      const user = await db.queryOne<User>(
        'SELECT * FROM users WHERE id = ?',
        [params.id]
      );

      return json({ data: user });
    }

    case 'DELETE': {
      const result = await db.run(
        'DELETE FROM users WHERE id = ?',
        [params.id]
      );

      if (result.rowsAffected === 0) {
        return json({ error: 'User not found' }, { status: 404 });
      }

      return json({ success: true });
    }

    default:
      return json({ error: 'Method not allowed' }, { status: 405 });
  }
}
```

### Server-Sent Events (SSE)

```typescript
// app/routes/api.events.users.tsx
import type { LoaderFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';

export async function loader({ request, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  let lastId = 0;

  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      // Send initial connection message
      controller.enqueue(encoder.encode('event: connected\ndata: {}\n\n'));

      const poll = async () => {
        // Check if client disconnected
        if (request.signal.aborted) {
          controller.close();
          return;
        }

        try {
          const events = await db.query<{
            id: number;
            type: string;
            data: string;
          }>(
            'SELECT * FROM events WHERE id > ? ORDER BY id ASC LIMIT 10',
            [lastId]
          );

          for (const event of events) {
            const sseMessage = `id: ${event.id}\nevent: ${event.type}\ndata: ${event.data}\n\n`;
            controller.enqueue(encoder.encode(sseMessage));
            lastId = event.id;
          }
        } catch (error) {
          console.error('SSE poll error:', error);
        }

        // Poll every second
        setTimeout(poll, 1000);
      };

      poll();
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    },
  });
}
```

### File Download

```typescript
// app/routes/api.export.users.tsx
import type { LoaderFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';

export async function loader({ context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  const users = await db.query<{ name: string; email: string; created_at: string }>(
    'SELECT name, email, created_at FROM users ORDER BY created_at DESC'
  );

  // Generate CSV
  const header = 'Name,Email,Created At\n';
  const rows = users
    .map((u) => `"${u.name}","${u.email}","${u.created_at}"`)
    .join('\n');
  const csv = header + rows;

  return new Response(csv, {
    headers: {
      'Content-Type': 'text/csv',
      'Content-Disposition': 'attachment; filename="users.csv"',
    },
  });
}
```

---

## Patterns

### Optimistic UI

```typescript
// app/routes/tasks._index.tsx
import { json, type ActionFunctionArgs, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { useFetcher, useLoaderData } from '@remix-run/react';
import { getDB } from '~/db.server';

interface Task {
  id: number;
  title: string;
  completed: boolean;
}

export async function loader({ context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const tasks = await db.query<Task>('SELECT * FROM tasks ORDER BY id DESC');
  return json({ tasks });
}

export async function action({ request, context }: ActionFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const formData = await request.formData();
  const intent = formData.get('intent');

  switch (intent) {
    case 'toggle': {
      const id = formData.get('id');
      const completed = formData.get('completed') === 'true';
      await db.run(
        'UPDATE tasks SET completed = ? WHERE id = ?',
        [!completed ? 1 : 0, id]
      );
      return json({ success: true });
    }

    case 'delete': {
      const id = formData.get('id');
      await db.run('DELETE FROM tasks WHERE id = ?', [id]);
      return json({ success: true });
    }

    case 'create': {
      const title = formData.get('title') as string;
      await db.run('INSERT INTO tasks (title) VALUES (?)', [title]);
      return json({ success: true });
    }

    default:
      return json({ error: 'Unknown intent' }, { status: 400 });
  }
}

export default function TasksIndex() {
  const { tasks } = useLoaderData<typeof loader>();
  const createFetcher = useFetcher();

  return (
    <div>
      <h1>Tasks</h1>

      <createFetcher.Form method="post">
        <input type="hidden" name="intent" value="create" />
        <input type="text" name="title" placeholder="New task..." required />
        <button type="submit">Add</button>
      </createFetcher.Form>

      <ul>
        {tasks.map((task) => (
          <TaskItem key={task.id} task={task} />
        ))}
      </ul>
    </div>
  );
}

function TaskItem({ task }: { task: Task }) {
  const fetcher = useFetcher();

  // Optimistic state
  const isToggling = fetcher.formData?.get('intent') === 'toggle';
  const isDeleting = fetcher.formData?.get('intent') === 'delete';

  // Optimistically compute completed state
  const optimisticCompleted = isToggling
    ? fetcher.formData?.get('completed') !== 'true'
    : task.completed;

  // Optimistically hide if deleting
  if (isDeleting) {
    return null;
  }

  return (
    <li style={{ opacity: fetcher.state === 'submitting' ? 0.5 : 1 }}>
      <fetcher.Form method="post" style={{ display: 'inline' }}>
        <input type="hidden" name="id" value={task.id} />
        <input type="hidden" name="completed" value={String(task.completed)} />
        <button type="submit" name="intent" value="toggle">
          {optimisticCompleted ? '[x]' : '[ ]'}
        </button>
      </fetcher.Form>

      <span style={{ textDecoration: optimisticCompleted ? 'line-through' : 'none' }}>
        {task.title}
      </span>

      <fetcher.Form method="post" style={{ display: 'inline' }}>
        <input type="hidden" name="id" value={task.id} />
        <button type="submit" name="intent" value="delete">
          Delete
        </button>
      </fetcher.Form>
    </li>
  );
}
```

### Error Boundary

```typescript
// app/routes/users.$id.tsx
import { json, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { useLoaderData, isRouteErrorResponse, useRouteError, Link } from '@remix-run/react';
import { getDB } from '~/db.server';

interface User {
  id: number;
  name: string;
  email: string;
}

export async function loader({ params, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  const user = await db.queryOne<User>(
    'SELECT * FROM users WHERE id = ?',
    [params.id]
  );

  if (!user) {
    throw json({ message: 'User not found' }, { status: 404 });
  }

  return json({ user });
}

export default function UserProfile() {
  const { user } = useLoaderData<typeof loader>();

  return (
    <div>
      <h1>{user.name}</h1>
      <p>{user.email}</p>
    </div>
  );
}

export function ErrorBoundary() {
  const error = useRouteError();

  if (isRouteErrorResponse(error)) {
    if (error.status === 404) {
      return (
        <div>
          <h1>User Not Found</h1>
          <p>The user you are looking for does not exist.</p>
          <Link to="/users">Back to Users</Link>
        </div>
      );
    }

    return (
      <div>
        <h1>Error {error.status}</h1>
        <p>{error.data?.message || 'Something went wrong'}</p>
        <Link to="/users">Back to Users</Link>
      </div>
    );
  }

  return (
    <div>
      <h1>Unexpected Error</h1>
      <p>An unexpected error occurred. Please try again.</p>
      <Link to="/users">Back to Users</Link>
    </div>
  );
}
```

### Nested Routes with Parallel Data Loading

```typescript
// app/routes/users.$id.tsx (parent)
import { json, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { useLoaderData, Outlet, Link, NavLink } from '@remix-run/react';
import { getDB } from '~/db.server';

interface User {
  id: number;
  name: string;
  email: string;
}

export async function loader({ params, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  const user = await db.queryOne<User>(
    'SELECT id, name, email FROM users WHERE id = ?',
    [params.id]
  );

  if (!user) {
    throw json({ message: 'User not found' }, { status: 404 });
  }

  return json({ user });
}

export default function UserLayout() {
  const { user } = useLoaderData<typeof loader>();

  return (
    <div>
      <header>
        <h1>{user.name}</h1>
        <nav>
          <NavLink to={`/users/${user.id}`} end>Profile</NavLink>
          <NavLink to={`/users/${user.id}/posts`}>Posts</NavLink>
          <NavLink to={`/users/${user.id}/activity`}>Activity</NavLink>
        </nav>
      </header>

      {/* Child routes render here */}
      <Outlet />
    </div>
  );
}
```

```typescript
// app/routes/users.$id.posts.tsx (child)
import { json, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { useLoaderData } from '@remix-run/react';
import { getDB } from '~/db.server';

interface Post {
  id: number;
  title: string;
  created_at: string;
}

export async function loader({ params, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  // This runs in parallel with the parent loader
  const posts = await db.query<Post>(
    'SELECT id, title, created_at FROM posts WHERE user_id = ? ORDER BY created_at DESC',
    [params.id]
  );

  return json({ posts });
}

export default function UserPosts() {
  const { posts } = useLoaderData<typeof loader>();

  return (
    <section>
      <h2>Posts ({posts.length})</h2>
      <ul>
        {posts.map((post) => (
          <li key={post.id}>{post.title}</li>
        ))}
      </ul>
    </section>
  );
}
```

---

## Type Safety

### Database Types

Create `app/types/database.ts` for shared type definitions:

```typescript
// app/types/database.ts

export interface User {
  id: number;
  name: string;
  email: string;
  bio: string | null;
  role: 'user' | 'admin';
  created_at: string;
  updated_at: string;
}

export interface Post {
  id: number;
  user_id: number;
  title: string;
  content: string;
  published: boolean;
  created_at: string;
  updated_at: string;
}

export interface Comment {
  id: number;
  post_id: number;
  user_id: number;
  content: string;
  created_at: string;
}

// Joined query results
export interface PostWithAuthor extends Post {
  author_name: string;
  author_email: string;
}

export interface CommentWithUser extends Comment {
  user_name: string;
}

// Input types for mutations
export type CreateUserInput = Pick<User, 'name' | 'email'> & Partial<Pick<User, 'bio'>>;
export type UpdateUserInput = Partial<CreateUserInput>;
```

### Zod Validation

```typescript
// app/schemas/user.ts
import { z } from 'zod';

export const CreateUserSchema = z.object({
  name: z
    .string()
    .min(2, 'Name must be at least 2 characters')
    .max(100, 'Name must be under 100 characters'),
  email: z
    .string()
    .email('Invalid email address'),
  bio: z
    .string()
    .max(500, 'Bio must be under 500 characters')
    .optional(),
});

export const UpdateUserSchema = CreateUserSchema.partial();

export type CreateUserInput = z.infer<typeof CreateUserSchema>;
export type UpdateUserInput = z.infer<typeof UpdateUserSchema>;
```

```typescript
// app/routes/api.users.tsx (using Zod validation)
import { json, type ActionFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';
import { CreateUserSchema } from '~/schemas/user';

export async function action({ request, context }: ActionFunctionArgs) {
  if (request.method !== 'POST') {
    return json({ error: 'Method not allowed' }, { status: 405 });
  }

  const db = await getDB(context.cloudflare.env);
  const body = await request.json();

  // Validate with Zod
  const result = CreateUserSchema.safeParse(body);

  if (!result.success) {
    return json(
      { errors: result.error.flatten().fieldErrors },
      { status: 400 }
    );
  }

  const { name, email, bio } = result.data;

  try {
    const dbResult = await db.run(
      'INSERT INTO users (name, email, bio) VALUES (?, ?, ?)',
      [name, email, bio || null]
    );

    return json({ id: dbResult.lastInsertRowId }, { status: 201 });
  } catch (error) {
    if ((error as Error).message.includes('UNIQUE constraint')) {
      return json(
        { errors: { email: ['Email already exists'] } },
        { status: 409 }
      );
    }
    throw error;
  }
}
```

---

## Advanced Features

### Real-Time with WebSocket

```typescript
// app/routes/ws.tsx
import type { LoaderFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';

export async function loader({ request, context }: LoaderFunctionArgs) {
  const upgradeHeader = request.headers.get('Upgrade');

  if (upgradeHeader !== 'websocket') {
    return new Response('Expected WebSocket upgrade', { status: 426 });
  }

  const { 0: client, 1: server } = new WebSocketPair();
  server.accept();

  const db = await getDB(context.cloudflare.env);

  server.addEventListener('message', async (event) => {
    try {
      const message = JSON.parse(event.data as string);

      switch (message.type) {
        case 'query': {
          const result = await db.query(message.sql, message.params);
          server.send(JSON.stringify({ id: message.id, data: result }));
          break;
        }

        case 'subscribe': {
          // Implement subscription logic
          break;
        }
      }
    } catch (error) {
      server.send(JSON.stringify({ error: (error as Error).message }));
    }
  });

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}
```

### Session Management

```typescript
// app/sessions.server.ts
import { createCookieSessionStorage } from '@remix-run/cloudflare';

export const sessionStorage = createCookieSessionStorage({
  cookie: {
    name: '__session',
    httpOnly: true,
    maxAge: 60 * 60 * 24 * 7, // 1 week
    path: '/',
    sameSite: 'lax',
    secrets: ['s3cr3t'], // Use environment variable in production
    secure: process.env.NODE_ENV === 'production',
  },
});

export const { getSession, commitSession, destroySession } = sessionStorage;
```

```typescript
// app/routes/login.tsx
import {
  json,
  redirect,
  type ActionFunctionArgs,
  type LoaderFunctionArgs,
} from '@remix-run/cloudflare';
import { Form, useActionData } from '@remix-run/react';
import { getDB } from '~/db.server';
import { getSession, commitSession } from '~/sessions.server';

export async function loader({ request }: LoaderFunctionArgs) {
  const session = await getSession(request.headers.get('Cookie'));

  if (session.has('userId')) {
    return redirect('/dashboard');
  }

  return json({});
}

export async function action({ request, context }: ActionFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const session = await getSession(request.headers.get('Cookie'));
  const formData = await request.formData();

  const email = formData.get('email') as string;
  const password = formData.get('password') as string;

  const user = await db.queryOne<{ id: number; password_hash: string }>(
    'SELECT id, password_hash FROM users WHERE email = ?',
    [email]
  );

  if (!user) {
    return json({ error: 'Invalid email or password' }, { status: 401 });
  }

  // Verify password (implement proper password hashing)
  // const isValid = await verifyPassword(password, user.password_hash);
  // if (!isValid) { ... }

  session.set('userId', user.id);

  return redirect('/dashboard', {
    headers: {
      'Set-Cookie': await commitSession(session),
    },
  });
}

export default function Login() {
  const actionData = useActionData<typeof action>();

  return (
    <Form method="post">
      {actionData?.error && <p className="error">{actionData.error}</p>}
      <input type="email" name="email" placeholder="Email" required />
      <input type="password" name="password" placeholder="Password" required />
      <button type="submit">Log In</button>
    </Form>
  );
}
```

### Multi-Tenant Routes

```typescript
// app/routes/tenant.$tenantId.tsx
import { json, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { Outlet, useLoaderData } from '@remix-run/react';
import { getDB } from '~/db.server';

export async function loader({ params, context }: LoaderFunctionArgs) {
  // Each tenant gets their own isolated database
  const db = await getDB(context.cloudflare.env, params.tenantId);

  const settings = await db.queryOne<{ name: string; plan: string }>(
    'SELECT name, plan FROM tenant_settings LIMIT 1'
  );

  return json({ tenantId: params.tenantId, settings });
}

export default function TenantLayout() {
  const { tenantId, settings } = useLoaderData<typeof loader>();

  return (
    <div>
      <header>
        <span>Tenant: {settings?.name || tenantId}</span>
        <span>Plan: {settings?.plan || 'free'}</span>
      </header>
      <Outlet />
    </div>
  );
}
```

---

## Testing

### Loader/Action Tests

```typescript
// app/routes/users._index.test.ts
import { describe, it, expect, vi } from 'vitest';
import { loader } from './users._index';

// Mock the database client
vi.mock('~/db.server', () => ({
  getDB: vi.fn().mockResolvedValue({
    query: vi.fn().mockResolvedValue([
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ]),
  }),
}));

describe('users._index loader', () => {
  it('returns users', async () => {
    const response = await loader({
      request: new Request('http://localhost/users'),
      params: {},
      context: {
        cloudflare: {
          env: { DOSQL_DB: {} as DurableObjectNamespace },
        },
      },
    } as any);

    const data = await response.json();
    expect(data.users).toHaveLength(2);
    expect(data.users[0].name).toBe('Alice');
  });
});
```

### Integration Tests with Miniflare

```typescript
// test/integration/users.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { unstable_dev } from 'wrangler';
import type { UnstableDevWorker } from 'wrangler';

describe('Users API', () => {
  let worker: UnstableDevWorker;

  beforeAll(async () => {
    worker = await unstable_dev('build/server/index.js', {
      experimental: { disableExperimentalWarning: true },
    });
  });

  afterAll(async () => {
    await worker.stop();
  });

  it('lists users', async () => {
    const response = await worker.fetch('/api/users');
    expect(response.status).toBe(200);

    const data = await response.json();
    expect(data).toHaveProperty('data');
    expect(Array.isArray(data.data)).toBe(true);
  });

  it('creates a user', async () => {
    const response = await worker.fetch('/api/users', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'Test User', email: 'test@example.com' }),
    });

    expect(response.status).toBe(201);

    const data = await response.json();
    expect(data.data.name).toBe('Test User');
  });
});
```

---

## Deployment

### Production Wrangler Configuration

```toml
# wrangler.toml
name = "remix-dosql-app"
main = "build/server/index.js"
compatibility_date = "2025-01-01"
compatibility_flags = ["nodejs_compat"]

[site]
bucket = "./build/client"

[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "remix-data"

[[kv_namespaces]]
binding = "SESSION_KV"
id = "your-kv-id"

# Production environment
[env.production]
name = "remix-dosql-app-prod"

[[env.production.r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "remix-data-prod"

[[env.production.kv_namespaces]]
binding = "SESSION_KV"
id = "your-prod-kv-id"
```

### Package Scripts

```json
{
  "scripts": {
    "build": "remix vite:build",
    "dev": "remix vite:dev",
    "start": "wrangler pages dev ./build/client",
    "deploy": "npm run build && wrangler deploy",
    "deploy:production": "npm run build && wrangler deploy --env production",
    "typecheck": "tsc --noEmit",
    "test": "vitest run",
    "test:e2e": "vitest run --config vitest.e2e.config.ts"
  }
}
```

### GitHub Actions CI/CD

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
      - run: npm run build
      - name: Deploy to Cloudflare Workers
        uses: cloudflare/wrangler-action@v3
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          command: deploy --env production
```

### Production Checklist

- [ ] Configure environment-specific Durable Object namespaces
- [ ] Set up R2 buckets for each environment
- [ ] Configure KV namespaces for sessions
- [ ] Use environment variables for secrets (session secrets, API keys)
- [ ] Enable Cloudflare Analytics
- [ ] Set up error tracking (Sentry, LogRocket)
- [ ] Configure rate limiting for API routes
- [ ] Test migrations in staging before production
- [ ] Set up monitoring and alerting
- [ ] Configure custom domains

---

## Next Steps

- [Getting Started](../getting-started.md) - DoSQL fundamentals
- [API Reference](../api-reference.md) - Complete API documentation
- [Advanced Features](../advanced.md) - Time travel, branching, CDC
- [Architecture](../architecture.md) - Understanding DoSQL internals
- [Hono Integration](./HONO.md) - Alternative lightweight framework
