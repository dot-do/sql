# DoSQL + Remix Integration Guide

This guide covers how to integrate DoSQL with Remix applications running on Cloudflare Workers, providing a seamless full-stack development experience with server-side rendering and edge-native data persistence.

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Basic Usage](#basic-usage)
- [Patterns](#patterns)
- [Type Safety](#type-safety)
- [Advanced](#advanced)
- [Deployment](#deployment)

---

## Overview

### Why DoSQL + Remix

Remix and DoSQL are a natural fit for Cloudflare Workers:

| Remix Feature | DoSQL Benefit |
|---------------|---------------|
| Loaders | Direct database queries at the edge |
| Actions | Transactional mutations with automatic rollback |
| Nested routes | Scoped data loading per route segment |
| defer() | Streaming database results |
| Resource routes | REST/JSON API endpoints |

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
|  |                   Remix Server                              |  |
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
|  |   SQLite DB      |  |     |   SQLite DB      |    |
|  |   - users        |  |     |   - users        |    |
|  |   - posts        |  |     |   - posts        |    |
|  +------------------+  |     |  +------------------+  |
+------------------------+     +------------------------+
```

**Data flow:**

1. Browser requests a page
2. Remix loader runs on Cloudflare Worker
3. Loader accesses DoSQL via Durable Object binding
4. DoSQL executes queries against SQLite
5. Remix renders the page with data
6. Browser receives fully-rendered HTML

---

## Setup

### Installation

```bash
# Create a new Remix project with Cloudflare template
npx create-remix@latest --template remix-run/remix/templates/cloudflare

# Install DoSQL
npm install @dotdo/dosql

# Install dev dependencies
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
│   ├── db.server.ts          # Database utilities (server only)
│   ├── routes/
│   │   ├── _index.tsx
│   │   ├── users._index.tsx
│   │   ├── users.$id.tsx
│   │   └── api.users.tsx     # Resource route
│   ├── root.tsx
│   └── entry.server.tsx
├── public/
├── wrangler.toml
├── package.json
└── tsconfig.json
```

### Environment Configuration

Create a `load-context.ts` file:

```typescript
// load-context.ts
import { type PlatformProxy } from 'wrangler';

// Define your environment bindings
export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  DATA_BUCKET: R2Bucket;
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

### Worker Bindings

Configure `wrangler.toml`:

```toml
name = "my-remix-app"
main = "build/server/index.js"
compatibility_date = "2024-01-01"
compatibility_flags = ["nodejs_compat"]

# Assets for Remix client-side
[site]
bucket = "./build/client"

# DoSQL Durable Object
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

# Optional: R2 for cold storage
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-remix-data"
```

### Database Server Module

Create `app/db.server.ts`:

```typescript
// app/db.server.ts
import { DB, type Database } from '@dotdo/dosql';
import type { Env } from '../load-context';

// Cache database connection per request
let dbPromise: Promise<Database> | null = null;

export async function getDB(env: Env, tenantId: string = 'default'): Promise<Database> {
  // Get Durable Object stub
  const id = env.DOSQL_DB.idFromName(tenantId);
  const stub = env.DOSQL_DB.get(id);

  // Return RPC client to the DO
  return createDBClient(stub);
}

// Simple RPC client for database operations
function createDBClient(stub: DurableObjectStub): Database {
  return {
    async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
      const response = await stub.fetch('http://internal/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params }),
      });
      const data = await response.json() as { rows: T[] };
      return data.rows;
    },

    async queryOne<T>(sql: string, params?: unknown[]): Promise<T | null> {
      const rows = await this.query<T>(sql, params);
      return rows[0] ?? null;
    },

    async run(sql: string, params?: unknown[]) {
      const response = await stub.fetch('http://internal/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params }),
      });
      return response.json() as Promise<{ rowsAffected: number; lastInsertRowId: number }>;
    },

    async transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
      // For complex transactions, use the RPC transaction endpoint
      const response = await stub.fetch('http://internal/transaction', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ operations: await collectOperations(fn) }),
      });
      return response.json() as Promise<T>;
    },
  } as Database;
}

// DoSQL Durable Object class (export in your worker entry)
export class DoSQLDatabase implements DurableObject {
  private db: Database | null = null;

  constructor(
    private state: DurableObjectState,
    private env: Env
  ) {}

  private async getDB(): Promise<Database> {
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
      if (url.pathname === '/query' && request.method === 'POST') {
        const { sql, params } = await request.json() as { sql: string; params?: unknown[] };
        const rows = await db.query(sql, params);
        return Response.json({ rows });
      }

      if (url.pathname === '/run' && request.method === 'POST') {
        const { sql, params } = await request.json() as { sql: string; params?: unknown[] };
        const result = await db.run(sql, params);
        return Response.json(result);
      }

      if (url.pathname === '/transaction' && request.method === 'POST') {
        const { operations } = await request.json() as { operations: Operation[] };
        const result = await db.transaction(async (tx) => {
          let lastResult;
          for (const op of operations) {
            if (op.type === 'query') {
              lastResult = await tx.query(op.sql, op.params);
            } else {
              lastResult = await tx.run(op.sql, op.params);
            }
          }
          return lastResult;
        });
        return Response.json(result);
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

interface Operation {
  type: 'query' | 'run';
  sql: string;
  params?: unknown[];
}
```

---

## Basic Usage

### Loader with Database Queries

```typescript
// app/routes/users._index.tsx
import { json, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { useLoaderData } from '@remix-run/react';
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
      <h1>Users</h1>
      <ul>
        {users.map((user) => (
          <li key={user.id}>
            <a href={`/users/${user.id}`}>{user.name}</a> - {user.email}
          </li>
        ))}
      </ul>
    </div>
  );
}
```

### Action with Mutations

```typescript
// app/routes/users.new.tsx
import {
  json,
  redirect,
  type ActionFunctionArgs,
  type LoaderFunctionArgs,
} from '@remix-run/cloudflare';
import { Form, useActionData, useNavigation } from '@remix-run/react';
import { getDB } from '~/db.server';

export async function action({ request, context }: ActionFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const formData = await request.formData();

  const name = formData.get('name') as string;
  const email = formData.get('email') as string;

  // Validation
  const errors: Record<string, string> = {};
  if (!name || name.length < 2) {
    errors.name = 'Name must be at least 2 characters';
  }
  if (!email || !email.includes('@')) {
    errors.email = 'Valid email is required';
  }

  if (Object.keys(errors).length > 0) {
    return json({ errors }, { status: 400 });
  }

  try {
    const result = await db.run(
      'INSERT INTO users (name, email) VALUES (?, ?)',
      [name, email]
    );

    return redirect(`/users/${result.lastInsertRowId}`);
  } catch (error) {
    if ((error as Error).message.includes('UNIQUE constraint')) {
      return json(
        { errors: { email: 'Email already exists' } },
        { status: 400 }
      );
    }
    throw error;
  }
}

export default function NewUser() {
  const actionData = useActionData<typeof action>();
  const navigation = useNavigation();
  const isSubmitting = navigation.state === 'submitting';

  return (
    <Form method="post">
      <div>
        <label htmlFor="name">Name</label>
        <input type="text" name="name" id="name" required />
        {actionData?.errors?.name && (
          <span className="error">{actionData.errors.name}</span>
        )}
      </div>

      <div>
        <label htmlFor="email">Email</label>
        <input type="email" name="email" id="email" required />
        {actionData?.errors?.email && (
          <span className="error">{actionData.errors.email}</span>
        )}
      </div>

      <button type="submit" disabled={isSubmitting}>
        {isSubmitting ? 'Creating...' : 'Create User'}
      </button>
    </Form>
  );
}
```

### Error Handling

```typescript
// app/routes/users.$id.tsx
import {
  json,
  type LoaderFunctionArgs,
} from '@remix-run/cloudflare';
import { useLoaderData, isRouteErrorResponse, useRouteError } from '@remix-run/react';
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
    throw json(
      { message: 'User not found' },
      { status: 404 }
    );
  }

  return json({ user });
}

export default function UserProfile() {
  const { user } = useLoaderData<typeof loader>();

  return (
    <div>
      <h1>{user.name}</h1>
      <p>{user.email}</p>
      {user.bio && <p>{user.bio}</p>}
      <p>Joined: {new Date(user.created_at).toLocaleDateString()}</p>
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
          <p>The user you're looking for doesn't exist.</p>
        </div>
      );
    }
  }

  return (
    <div>
      <h1>Error</h1>
      <p>Something went wrong loading this user.</p>
    </div>
  );
}
```

---

## Patterns

### Resource Routes for API Endpoints

```typescript
// app/routes/api.users.tsx
import { json, type ActionFunctionArgs, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';

// GET /api/users - List users
export async function loader({ request, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const url = new URL(request.url);

  const limit = parseInt(url.searchParams.get('limit') || '50', 10);
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

  const users = await db.query(sql, params);
  const countResult = await db.queryOne<{ total: number }>(
    'SELECT COUNT(*) as total FROM users' + (search ? ' WHERE name LIKE ? OR email LIKE ?' : ''),
    search ? [`%${search}%`, `%${search}%`] : []
  );

  return json({
    users,
    total: countResult?.total || 0,
    limit,
    offset,
  });
}

// POST /api/users - Create user
export async function action({ request, context }: ActionFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  if (request.method === 'POST') {
    const body = await request.json() as { name: string; email: string };

    const result = await db.run(
      'INSERT INTO users (name, email) VALUES (?, ?)',
      [body.name, body.email]
    );

    return json(
      { id: result.lastInsertRowId, ...body },
      { status: 201 }
    );
  }

  return json({ error: 'Method not allowed' }, { status: 405 });
}
```

### Optimistic UI with Mutations

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

  if (intent === 'toggle') {
    const id = formData.get('id');
    const completed = formData.get('completed') === 'true';

    await db.run(
      'UPDATE tasks SET completed = ? WHERE id = ?',
      [!completed, id]
    );

    return json({ success: true });
  }

  if (intent === 'delete') {
    const id = formData.get('id');
    await db.run('DELETE FROM tasks WHERE id = ?', [id]);
    return json({ success: true });
  }

  return json({ error: 'Unknown intent' }, { status: 400 });
}

export default function TasksIndex() {
  const { tasks } = useLoaderData<typeof loader>();

  return (
    <ul>
      {tasks.map((task) => (
        <TaskItem key={task.id} task={task} />
      ))}
    </ul>
  );
}

function TaskItem({ task }: { task: Task }) {
  const fetcher = useFetcher();

  // Optimistic UI: compute the optimistic state
  const isToggling = fetcher.formData?.get('intent') === 'toggle';
  const optimisticCompleted = isToggling
    ? fetcher.formData?.get('completed') !== 'true'
    : task.completed;

  const isDeleting = fetcher.formData?.get('intent') === 'delete';
  if (isDeleting) {
    return null; // Optimistically remove from UI
  }

  return (
    <li style={{ opacity: fetcher.state === 'submitting' ? 0.5 : 1 }}>
      <fetcher.Form method="post">
        <input type="hidden" name="id" value={task.id} />
        <input type="hidden" name="completed" value={String(task.completed)} />
        <button type="submit" name="intent" value="toggle">
          {optimisticCompleted ? '[x]' : '[ ]'}
        </button>
        <span style={{ textDecoration: optimisticCompleted ? 'line-through' : 'none' }}>
          {task.title}
        </span>
        <button type="submit" name="intent" value="delete">
          Delete
        </button>
      </fetcher.Form>
    </li>
  );
}
```

### Streaming with defer()

```typescript
// app/routes/dashboard.tsx
import { defer, type LoaderFunctionArgs } from '@remix-run/cloudflare';
import { Await, useLoaderData } from '@remix-run/react';
import { Suspense } from 'react';
import { getDB } from '~/db.server';

interface DashboardStats {
  totalUsers: number;
  totalPosts: number;
  recentActivity: number;
}

interface RecentUser {
  id: number;
  name: string;
  created_at: string;
}

export async function loader({ context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  // Critical data - await immediately
  const stats = db.queryOne<DashboardStats>(`
    SELECT
      (SELECT COUNT(*) FROM users) as totalUsers,
      (SELECT COUNT(*) FROM posts) as totalPosts,
      (SELECT COUNT(*) FROM activity WHERE created_at > datetime('now', '-24 hours')) as recentActivity
  `);

  // Non-critical data - defer and stream
  const recentUsersPromise = db.query<RecentUser>(`
    SELECT id, name, created_at
    FROM users
    ORDER BY created_at DESC
    LIMIT 10
  `);

  const popularPostsPromise = db.query(`
    SELECT p.id, p.title, COUNT(l.id) as likes
    FROM posts p
    LEFT JOIN likes l ON l.post_id = p.id
    GROUP BY p.id
    ORDER BY likes DESC
    LIMIT 5
  `);

  return defer({
    stats: await stats,
    recentUsers: recentUsersPromise,
    popularPosts: popularPostsPromise,
  });
}

export default function Dashboard() {
  const { stats, recentUsers, popularPosts } = useLoaderData<typeof loader>();

  return (
    <div>
      <h1>Dashboard</h1>

      {/* Critical data rendered immediately */}
      <div className="stats">
        <div>Users: {stats?.totalUsers}</div>
        <div>Posts: {stats?.totalPosts}</div>
        <div>Recent Activity: {stats?.recentActivity}</div>
      </div>

      {/* Deferred data streams in */}
      <div className="panels">
        <Suspense fallback={<div>Loading recent users...</div>}>
          <Await resolve={recentUsers}>
            {(users) => (
              <div>
                <h2>Recent Users</h2>
                <ul>
                  {users.map((user) => (
                    <li key={user.id}>{user.name}</li>
                  ))}
                </ul>
              </div>
            )}
          </Await>
        </Suspense>

        <Suspense fallback={<div>Loading popular posts...</div>}>
          <Await resolve={popularPosts}>
            {(posts) => (
              <div>
                <h2>Popular Posts</h2>
                <ul>
                  {posts.map((post: { id: number; title: string; likes: number }) => (
                    <li key={post.id}>
                      {post.title} ({post.likes} likes)
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </Await>
        </Suspense>
      </div>
    </div>
  );
}
```

---

## Type Safety

### TypeScript Integration

Define your database types in a shared module:

```typescript
// app/types/database.ts
export interface User {
  id: number;
  name: string;
  email: string;
  bio: string | null;
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

// Joined types for queries
export interface PostWithAuthor extends Post {
  author_name: string;
  author_email: string;
}

export interface CommentWithUser extends Comment {
  user_name: string;
}
```

### Typed Loaders and Actions

```typescript
// app/routes/posts.$id.tsx
import { json, type LoaderFunctionArgs, type ActionFunctionArgs } from '@remix-run/cloudflare';
import { useLoaderData, Form } from '@remix-run/react';
import { getDB } from '~/db.server';
import type { Post, CommentWithUser } from '~/types/database';

interface LoaderData {
  post: Post;
  comments: CommentWithUser[];
}

export async function loader({ params, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  const post = await db.queryOne<Post>(
    'SELECT * FROM posts WHERE id = ?',
    [params.id]
  );

  if (!post) {
    throw json({ message: 'Post not found' }, { status: 404 });
  }

  const comments = await db.query<CommentWithUser>(`
    SELECT c.*, u.name as user_name
    FROM comments c
    JOIN users u ON c.user_id = u.id
    WHERE c.post_id = ?
    ORDER BY c.created_at DESC
  `, [params.id]);

  return json<LoaderData>({ post, comments });
}

interface ActionData {
  errors?: {
    content?: string;
  };
}

export async function action({ request, params, context }: ActionFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const formData = await request.formData();

  const content = formData.get('content') as string;
  const userId = formData.get('userId') as string;

  if (!content || content.length < 1) {
    return json<ActionData>(
      { errors: { content: 'Comment cannot be empty' } },
      { status: 400 }
    );
  }

  await db.run(
    'INSERT INTO comments (post_id, user_id, content) VALUES (?, ?, ?)',
    [params.id, userId, content]
  );

  return json({ success: true });
}

export default function PostDetail() {
  const { post, comments } = useLoaderData<LoaderData>();

  return (
    <article>
      <h1>{post.title}</h1>
      <div dangerouslySetInnerHTML={{ __html: post.content }} />

      <section>
        <h2>Comments ({comments.length})</h2>
        {comments.map((comment) => (
          <div key={comment.id}>
            <strong>{comment.user_name}</strong>
            <p>{comment.content}</p>
          </div>
        ))}

        <Form method="post">
          <input type="hidden" name="userId" value="1" />
          <textarea name="content" placeholder="Add a comment..." />
          <button type="submit">Post Comment</button>
        </Form>
      </section>
    </article>
  );
}
```

### Zod Schema Validation

```typescript
// app/schemas/user.ts
import { z } from 'zod';

export const CreateUserSchema = z.object({
  name: z.string().min(2, 'Name must be at least 2 characters'),
  email: z.string().email('Invalid email address'),
  bio: z.string().max(500, 'Bio must be under 500 characters').optional(),
});

export const UpdateUserSchema = CreateUserSchema.partial();

export type CreateUserInput = z.infer<typeof CreateUserSchema>;
export type UpdateUserInput = z.infer<typeof UpdateUserSchema>;
```

```typescript
// app/routes/api.users.$id.tsx
import { json, type ActionFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';
import { UpdateUserSchema } from '~/schemas/user';

export async function action({ request, params, context }: ActionFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  if (request.method === 'PATCH') {
    const body = await request.json();

    // Validate with Zod
    const result = UpdateUserSchema.safeParse(body);
    if (!result.success) {
      return json(
        { errors: result.error.flatten().fieldErrors },
        { status: 400 }
      );
    }

    const { name, email, bio } = result.data;

    // Build dynamic update query
    const updates: string[] = [];
    const values: unknown[] = [];

    if (name !== undefined) {
      updates.push('name = ?');
      values.push(name);
    }
    if (email !== undefined) {
      updates.push('email = ?');
      values.push(email);
    }
    if (bio !== undefined) {
      updates.push('bio = ?');
      values.push(bio);
    }

    if (updates.length === 0) {
      return json({ error: 'No fields to update' }, { status: 400 });
    }

    values.push(params.id);

    await db.run(
      `UPDATE users SET ${updates.join(', ')}, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
      values
    );

    return json({ success: true });
  }

  return json({ error: 'Method not allowed' }, { status: 405 });
}
```

---

## Advanced

### CDC Subscriptions in Routes

Stream real-time changes to the client using Server-Sent Events:

```typescript
// app/routes/api.events.users.tsx
import type { LoaderFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';

export async function loader({ request, context }: LoaderFunctionArgs) {
  const db = await getDB(context.cloudflare.env);

  // Create a readable stream for SSE
  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      // Subscribe to CDC events
      const subscription = await db.subscribeCDC({
        tables: ['users'],
        fromLSN: 0n,
      });

      // Handle client disconnect
      request.signal.addEventListener('abort', () => {
        subscription.close();
      });

      // Stream events to client
      for await (const event of subscription) {
        const data = JSON.stringify({
          type: event.type,
          table: event.table,
          data: event.after || event.before,
          timestamp: event.timestamp,
        });

        controller.enqueue(encoder.encode(`data: ${data}\n\n`));
      }

      controller.close();
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

Client-side subscription:

```typescript
// app/hooks/useRealtimeUsers.ts
import { useEffect, useState } from 'react';
import type { User } from '~/types/database';

interface CDCEvent {
  type: 'insert' | 'update' | 'delete';
  table: string;
  data: User;
  timestamp: string;
}

export function useRealtimeUsers(initialUsers: User[]) {
  const [users, setUsers] = useState(initialUsers);

  useEffect(() => {
    const eventSource = new EventSource('/api/events/users');

    eventSource.onmessage = (event) => {
      const change: CDCEvent = JSON.parse(event.data);

      setUsers((current) => {
        switch (change.type) {
          case 'insert':
            return [change.data, ...current];
          case 'update':
            return current.map((u) =>
              u.id === change.data.id ? change.data : u
            );
          case 'delete':
            return current.filter((u) => u.id !== change.data.id);
          default:
            return current;
        }
      });
    };

    return () => eventSource.close();
  }, []);

  return users;
}
```

### Real-time Updates with WebSocket

```typescript
// app/routes/ws.tsx
import type { LoaderFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';

export async function loader({ request, context }: LoaderFunctionArgs) {
  // Check for WebSocket upgrade
  const upgradeHeader = request.headers.get('Upgrade');
  if (upgradeHeader !== 'websocket') {
    return new Response('Expected WebSocket', { status: 426 });
  }

  // Create WebSocket pair
  const { 0: client, 1: server } = new WebSocketPair();

  // Handle WebSocket connection
  server.accept();

  const db = await getDB(context.cloudflare.env);

  server.addEventListener('message', async (event) => {
    try {
      const message = JSON.parse(event.data as string);

      switch (message.type) {
        case 'query': {
          const result = await db.query(message.sql, message.params);
          server.send(JSON.stringify({ id: message.id, result }));
          break;
        }
        case 'subscribe': {
          const subscription = await db.subscribeCDC({
            tables: message.tables,
          });

          for await (const change of subscription) {
            server.send(JSON.stringify({ type: 'change', data: change }));
          }
          break;
        }
      }
    } catch (error) {
      server.send(JSON.stringify({
        error: (error as Error).message,
      }));
    }
  });

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}
```

### Transaction Handling

```typescript
// app/routes/api.transfer.tsx
import { json, type ActionFunctionArgs } from '@remix-run/cloudflare';
import { getDB } from '~/db.server';

export async function action({ request, context }: ActionFunctionArgs) {
  const db = await getDB(context.cloudflare.env);
  const { fromAccountId, toAccountId, amount } = await request.json() as {
    fromAccountId: number;
    toAccountId: number;
    amount: number;
  };

  try {
    const result = await db.transaction(async (tx) => {
      // Check source balance
      const source = await tx.queryOne<{ balance: number }>(
        'SELECT balance FROM accounts WHERE id = ?',
        [fromAccountId]
      );

      if (!source || source.balance < amount) {
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
      const transfer = await tx.run(
        `INSERT INTO transfers (from_account_id, to_account_id, amount)
         VALUES (?, ?, ?)`,
        [fromAccountId, toAccountId, amount]
      );

      return { transferId: transfer.lastInsertRowId };
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

## Deployment

### Cloudflare Pages Deployment

For Remix on Cloudflare Pages:

```toml
# wrangler.toml
name = "my-remix-app"
pages_build_output_dir = "./build/client"
compatibility_date = "2024-01-01"
compatibility_flags = ["nodejs_compat"]

# Functions configuration
[build]
command = "npm run build"

# Durable Objects (requires Workers paid plan)
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"
script_name = "dosql-worker"  # Separate worker for DO

# R2 binding
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "remix-app-data"

# Environment variables
[vars]
ENVIRONMENT = "production"
```

### Wrangler Configuration for Remix

Complete `wrangler.toml` for Remix Workers:

```toml
name = "remix-dosql-app"
main = "build/server/index.js"
compatibility_date = "2024-01-01"
compatibility_flags = ["nodejs_compat"]

# Worker sites for static assets
[site]
bucket = "./build/client"

# Durable Objects
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

# DO migrations
[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

# R2 for data
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "remix-data"

# KV for sessions (optional)
[[kv_namespaces]]
binding = "SESSIONS"
id = "your-kv-namespace-id"

# Production environment
[env.production]
name = "remix-dosql-app-prod"

[[env.production.r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "remix-data-prod"

# Staging environment
[env.staging]
name = "remix-dosql-app-staging"

[[env.staging.r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "remix-data-staging"
```

### Build and Deploy Scripts

```json
{
  "scripts": {
    "build": "remix vite:build",
    "dev": "remix vite:dev",
    "start": "wrangler pages dev ./build/client",
    "deploy": "npm run build && wrangler pages deploy ./build/client",
    "deploy:staging": "npm run build && wrangler pages deploy ./build/client --env staging",
    "deploy:production": "npm run build && wrangler pages deploy ./build/client --env production",
    "typecheck": "tsc"
  }
}
```

### Migrations in Production

Include migrations in your build:

```typescript
// vite.config.ts
import { vitePlugin as remix } from '@remix-run/dev';
import { defineConfig } from 'vite';
import { copyFileSync, mkdirSync } from 'fs';
import { glob } from 'glob';

export default defineConfig({
  plugins: [
    remix(),
    {
      name: 'copy-migrations',
      buildEnd() {
        // Copy SQL migrations to build output
        mkdirSync('build/server/.do/migrations', { recursive: true });
        const files = glob.sync('.do/migrations/*.sql');
        files.forEach((file) => {
          const dest = file.replace('.do', 'build/server/.do');
          copyFileSync(file, dest);
        });
      },
    },
  ],
});
```

### Production Checklist

1. **Environment bindings configured** - DOSQL_DB, DATA_BUCKET
2. **Migrations bundled** - SQL files included in build
3. **Error boundaries** - Graceful error handling in routes
4. **Rate limiting** - Consider adding rate limits for API routes
5. **Monitoring** - Set up Cloudflare analytics and logging

---

## Next Steps

- [Getting Started](../getting-started.md) - DoSQL basics
- [API Reference](../api-reference.md) - Complete API documentation
- [Advanced Features](../advanced.md) - CDC, time travel, branching
- [Architecture](../architecture.md) - Understanding DoSQL internals
