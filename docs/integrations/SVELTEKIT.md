# SvelteKit Integration Guide

**Version**: 1.0.0
**Last Updated**: 2026-01-22
**Maintainer**: Platform Team

This guide provides comprehensive documentation for integrating DoSQL with SvelteKit applications, including server-side rendering, form actions, API routes, and real-time updates.

---

## Table of Contents

1. [Installation and Setup](#installation-and-setup)
   - [Prerequisites](#prerequisites)
   - [Installing Dependencies](#installing-dependencies)
   - [Environment Configuration](#environment-configuration)
   - [Creating the Database Client](#creating-the-database-client)
2. [Server Load Functions](#server-load-functions)
   - [Basic Data Loading](#basic-data-loading)
   - [Parallel Data Loading](#parallel-data-loading)
   - [Error Handling in Load Functions](#error-handling-in-load-functions)
   - [Type-Safe Load Functions](#type-safe-load-functions)
3. [Form Actions](#form-actions)
   - [Basic Form Actions](#basic-form-actions)
   - [Validation with Form Actions](#validation-with-form-actions)
   - [Progressive Enhancement](#progressive-enhancement)
   - [Optimistic Updates](#optimistic-updates)
4. [API Routes](#api-routes)
   - [RESTful API Endpoints](#restful-api-endpoints)
   - [Streaming Responses](#streaming-responses)
   - [Request Validation](#request-validation)
5. [Edge Deployment with Cloudflare](#edge-deployment-with-cloudflare)
   - [Cloudflare Adapter Setup](#cloudflare-adapter-setup)
   - [Platform Bindings](#platform-bindings)
   - [Wrangler Configuration](#wrangler-configuration)
6. [Type-Safe Queries](#type-safe-queries)
   - [Defining Database Types](#defining-database-types)
   - [Type Inference Patterns](#type-inference-patterns)
   - [Generic Query Functions](#generic-query-functions)
7. [Real-Time Updates with CDC](#real-time-updates-with-cdc)
   - [Setting Up CDC Subscriptions](#setting-up-cdc-subscriptions)
   - [Svelte Stores for Real-Time Data](#svelte-stores-for-real-time-data)
   - [Server-Sent Events](#server-sent-events)
8. [Example: Todo App with Real-Time Sync](#example-todo-app-with-real-time-sync)
9. [Example: Dashboard with Live Data](#example-dashboard-with-live-data)
10. [Performance Optimization](#performance-optimization)
    - [Connection Pooling](#connection-pooling)
    - [Query Caching](#query-caching)
    - [Prefetching Strategies](#prefetching-strategies)

---

## Installation and Setup

### Prerequisites

Before starting, ensure you have:

- Node.js 18+ or Bun
- SvelteKit 2.0+
- A DoSQL instance running (local or production)
- TypeScript 5.0+ (recommended)

### Installing Dependencies

```bash
# Create a new SvelteKit project (if starting fresh)
npx sv create my-app
cd my-app

# Install DoSQL client SDK
npm install sql.do

# For Cloudflare deployment
npm install -D @sveltejs/adapter-cloudflare wrangler

# For real-time features
npm install lake.do
```

### Environment Configuration

Create your environment files:

```bash
# .env.local (development)
DOSQL_URL=http://localhost:8787
DOSQL_TOKEN=dev-token

# .env.production (production)
DOSQL_URL=https://sql.your-domain.com
DOSQL_TOKEN=your-production-token
```

Define environment types in `src/app.d.ts`:

```typescript
// src/app.d.ts
declare global {
  namespace App {
    interface Locals {
      db: import('sql.do').DoSQLClient;
    }

    interface Platform {
      env: {
        DOSQL_URL: string;
        DOSQL_TOKEN: string;
        // Cloudflare-specific bindings
        DOSQL: DurableObjectNamespace;
        KV: KVNamespace;
      };
    }

    interface Error {
      message: string;
      code?: string;
    }
  }
}

export {};
```

### Creating the Database Client

Create a reusable database client module:

```typescript
// src/lib/server/db.ts
import { createSQLClient, type DoSQLClient, type SQLClientConfig } from 'sql.do';
import { building } from '$app/environment';
import { env } from '$env/dynamic/private';

let client: DoSQLClient | null = null;

export function getDB(): DoSQLClient {
  if (building) {
    // Return a dummy client during build
    return {} as DoSQLClient;
  }

  if (!client) {
    const config: SQLClientConfig = {
      url: env.DOSQL_URL ?? 'http://localhost:8787',
      token: env.DOSQL_TOKEN,
      timeout: 30000,
      retry: {
        maxRetries: 3,
        baseDelayMs: 100,
        maxDelayMs: 5000,
      },
      idempotency: {
        enabled: true,
        keyPrefix: 'sveltekit',
      },
    };

    client = createSQLClient(config);
  }

  return client;
}

// For use in hooks and middleware
export function createDBFromPlatform(platform: App.Platform | undefined): DoSQLClient {
  if (!platform?.env) {
    return getDB();
  }

  return createSQLClient({
    url: platform.env.DOSQL_URL,
    token: platform.env.DOSQL_TOKEN,
    timeout: 30000,
  });
}
```

Set up the client in hooks for request-scoped access:

```typescript
// src/hooks.server.ts
import type { Handle } from '@sveltejs/kit';
import { createDBFromPlatform } from '$lib/server/db';

export const handle: Handle = async ({ event, resolve }) => {
  // Create database client for this request
  event.locals.db = createDBFromPlatform(event.platform);

  const response = await resolve(event);

  return response;
};
```

---

## Server Load Functions

### Basic Data Loading

Load data in `+page.server.ts` files:

```typescript
// src/routes/users/+page.server.ts
import type { PageServerLoad } from './$types';
import { error } from '@sveltejs/kit';
import { SQLError, RPCErrorCode } from 'sql.do';

interface User {
  id: string;
  name: string;
  email: string;
  created_at: number;
}

export const load: PageServerLoad = async ({ locals }) => {
  try {
    const result = await locals.db.query<User>(
      'SELECT id, name, email, created_at FROM users ORDER BY created_at DESC LIMIT 50'
    );

    return {
      users: result.rows,
    };
  } catch (err) {
    if (err instanceof SQLError) {
      if (err.code === RPCErrorCode.TABLE_NOT_FOUND) {
        error(404, { message: 'Users table not found' });
      }
      error(500, { message: `Database error: ${err.message}` });
    }
    throw err;
  }
};
```

Display data in the page component:

```svelte
<!-- src/routes/users/+page.svelte -->
<script lang="ts">
  import type { PageData } from './$types';

  export let data: PageData;
</script>

<h1>Users ({data.users.length})</h1>

<ul>
  {#each data.users as user (user.id)}
    <li>
      <a href="/users/{user.id}">{user.name}</a>
      <span class="email">{user.email}</span>
    </li>
  {/each}
</ul>

<style>
  .email {
    color: #666;
    font-size: 0.875rem;
  }
</style>
```

### Parallel Data Loading

Load multiple queries in parallel for better performance:

```typescript
// src/routes/dashboard/+page.server.ts
import type { PageServerLoad } from './$types';

interface DashboardStats {
  total_users: number;
  active_users: number;
  total_orders: number;
  revenue: number;
}

interface RecentOrder {
  id: string;
  user_name: string;
  total: number;
  created_at: number;
}

interface TopProduct {
  id: string;
  name: string;
  sales: number;
}

export const load: PageServerLoad = async ({ locals }) => {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const todayStart = today.getTime();

  // Execute multiple queries in parallel
  const [statsResult, ordersResult, productsResult] = await Promise.all([
    // Dashboard stats - batch query for efficiency
    locals.db.batchQuery([
      { sql: 'SELECT COUNT(*) as count FROM users' },
      { sql: 'SELECT COUNT(DISTINCT user_id) as count FROM sessions WHERE last_active > ?', params: [todayStart] },
      { sql: 'SELECT COUNT(*) as count FROM orders WHERE created_at > ?', params: [todayStart] },
      { sql: 'SELECT COALESCE(SUM(total), 0) as sum FROM orders WHERE created_at > ?', params: [todayStart] },
    ]),

    // Recent orders
    locals.db.query<RecentOrder>(`
      SELECT o.id, u.name as user_name, o.total, o.created_at
      FROM orders o
      JOIN users u ON o.user_id = u.id
      ORDER BY o.created_at DESC
      LIMIT 10
    `),

    // Top products
    locals.db.query<TopProduct>(`
      SELECT p.id, p.name, COUNT(oi.id) as sales
      FROM products p
      JOIN order_items oi ON p.id = oi.product_id
      GROUP BY p.id, p.name
      ORDER BY sales DESC
      LIMIT 5
    `),
  ]);

  const stats: DashboardStats = {
    total_users: statsResult[0].rows[0].count as number,
    active_users: statsResult[1].rows[0].count as number,
    total_orders: statsResult[2].rows[0].count as number,
    revenue: statsResult[3].rows[0].sum as number,
  };

  return {
    stats,
    recentOrders: ordersResult.rows,
    topProducts: productsResult.rows,
  };
};
```

### Error Handling in Load Functions

Create a reusable error handler:

```typescript
// src/lib/server/errors.ts
import { error } from '@sveltejs/kit';
import { SQLError, RPCErrorCode } from 'sql.do';

export function handleDBError(err: unknown, context: string): never {
  if (err instanceof SQLError) {
    console.error(`Database error in ${context}:`, err.code, err.message);

    switch (err.code) {
      case RPCErrorCode.TABLE_NOT_FOUND:
        error(404, { message: 'Resource not found', code: err.code });
      case RPCErrorCode.CONSTRAINT_VIOLATION:
        error(400, { message: 'Constraint violation', code: err.code });
      case RPCErrorCode.TIMEOUT:
        error(504, { message: 'Request timeout', code: err.code });
      case RPCErrorCode.UNAUTHORIZED:
        error(401, { message: 'Unauthorized', code: err.code });
      default:
        error(500, { message: 'Database error', code: err.code });
    }
  }

  console.error(`Unexpected error in ${context}:`, err);
  error(500, { message: 'Internal server error' });
}
```

Usage in load functions:

```typescript
// src/routes/users/[id]/+page.server.ts
import type { PageServerLoad } from './$types';
import { error } from '@sveltejs/kit';
import { handleDBError } from '$lib/server/errors';

interface User {
  id: string;
  name: string;
  email: string;
  bio: string | null;
  created_at: number;
}

export const load: PageServerLoad = async ({ locals, params }) => {
  try {
    const result = await locals.db.query<User>(
      'SELECT * FROM users WHERE id = ?',
      [params.id]
    );

    if (result.rows.length === 0) {
      error(404, { message: 'User not found' });
    }

    return {
      user: result.rows[0],
    };
  } catch (err) {
    handleDBError(err, `load user ${params.id}`);
  }
};
```

### Type-Safe Load Functions

Leverage SvelteKit's type generation for full type safety:

```typescript
// src/routes/posts/+page.server.ts
import type { PageServerLoad } from './$types';

interface Post {
  id: string;
  title: string;
  content: string;
  author_id: string;
  author_name: string;
  published_at: number | null;
  created_at: number;
}

export const load = (async ({ locals, url }) => {
  const page = parseInt(url.searchParams.get('page') ?? '1', 10);
  const pageSize = 20;
  const offset = (page - 1) * pageSize;

  const [postsResult, countResult] = await Promise.all([
    locals.db.query<Post>(`
      SELECT
        p.id, p.title, p.content, p.author_id, p.published_at, p.created_at,
        u.name as author_name
      FROM posts p
      JOIN users u ON p.author_id = u.id
      WHERE p.published_at IS NOT NULL
      ORDER BY p.published_at DESC
      LIMIT ? OFFSET ?
    `, [pageSize, offset]),

    locals.db.query<{ total: number }>(`
      SELECT COUNT(*) as total FROM posts WHERE published_at IS NOT NULL
    `),
  ]);

  const total = countResult.rows[0].total;
  const totalPages = Math.ceil(total / pageSize);

  return {
    posts: postsResult.rows,
    pagination: {
      page,
      pageSize,
      total,
      totalPages,
      hasNext: page < totalPages,
      hasPrev: page > 1,
    },
  };
}) satisfies PageServerLoad;
```

---

## Form Actions

### Basic Form Actions

Handle form submissions with type safety:

```typescript
// src/routes/users/new/+page.server.ts
import type { Actions, PageServerLoad } from './$types';
import { fail, redirect } from '@sveltejs/kit';
import { SQLError, RPCErrorCode } from 'sql.do';

export const load: PageServerLoad = async () => {
  return {};
};

export const actions: Actions = {
  default: async ({ locals, request }) => {
    const formData = await request.formData();
    const name = formData.get('name')?.toString().trim();
    const email = formData.get('email')?.toString().trim().toLowerCase();

    // Validation
    if (!name || name.length < 2) {
      return fail(400, {
        error: 'Name must be at least 2 characters',
        values: { name, email },
      });
    }

    if (!email || !email.includes('@')) {
      return fail(400, {
        error: 'Valid email is required',
        values: { name, email },
      });
    }

    try {
      const result = await locals.db.query<{ id: string }>(
        `INSERT INTO users (id, name, email, created_at)
         VALUES (?, ?, ?, ?)
         RETURNING id`,
        [crypto.randomUUID(), name, email, Date.now()]
      );

      const userId = result.rows[0].id;

      redirect(303, `/users/${userId}`);
    } catch (err) {
      if (err instanceof SQLError) {
        if (err.code === RPCErrorCode.CONSTRAINT_VIOLATION) {
          return fail(400, {
            error: 'A user with this email already exists',
            values: { name, email },
          });
        }
      }

      console.error('Failed to create user:', err);
      return fail(500, {
        error: 'Failed to create user. Please try again.',
        values: { name, email },
      });
    }
  },
};
```

Form component with error handling:

```svelte
<!-- src/routes/users/new/+page.svelte -->
<script lang="ts">
  import type { ActionData } from './$types';
  import { enhance } from '$app/forms';

  export let form: ActionData;
</script>

<h1>Create New User</h1>

{#if form?.error}
  <div class="error" role="alert">
    {form.error}
  </div>
{/if}

<form method="POST" use:enhance>
  <label>
    Name
    <input
      type="text"
      name="name"
      value={form?.values?.name ?? ''}
      required
      minlength="2"
    />
  </label>

  <label>
    Email
    <input
      type="email"
      name="email"
      value={form?.values?.email ?? ''}
      required
    />
  </label>

  <button type="submit">Create User</button>
</form>

<style>
  .error {
    background: #fee;
    border: 1px solid #c00;
    color: #c00;
    padding: 1rem;
    border-radius: 4px;
    margin-bottom: 1rem;
  }

  label {
    display: block;
    margin-bottom: 1rem;
  }

  input {
    display: block;
    width: 100%;
    padding: 0.5rem;
    margin-top: 0.25rem;
  }
</style>
```

### Validation with Form Actions

Create a validation library for forms:

```typescript
// src/lib/server/validation.ts
import { fail } from '@sveltejs/kit';

export interface ValidationError {
  field: string;
  message: string;
}

export interface ValidationResult<T> {
  success: boolean;
  data?: T;
  errors?: ValidationError[];
}

export function validate<T>(
  data: FormData,
  schema: Record<keyof T, (value: string | null) => string | null>
): ValidationResult<T> {
  const errors: ValidationError[] = [];
  const result: Partial<T> = {};

  for (const [field, validator] of Object.entries(schema)) {
    const value = data.get(field)?.toString() ?? null;
    const error = (validator as (v: string | null) => string | null)(value);

    if (error) {
      errors.push({ field, message: error });
    } else {
      (result as Record<string, unknown>)[field] = value;
    }
  }

  if (errors.length > 0) {
    return { success: false, errors };
  }

  return { success: true, data: result as T };
}

// Common validators
export const validators = {
  required: (msg = 'This field is required') => (v: string | null) =>
    v && v.trim() ? null : msg,

  email: (msg = 'Invalid email address') => (v: string | null) =>
    v && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v) ? null : msg,

  minLength: (min: number, msg?: string) => (v: string | null) =>
    v && v.length >= min ? null : msg ?? `Must be at least ${min} characters`,

  maxLength: (max: number, msg?: string) => (v: string | null) =>
    !v || v.length <= max ? null : msg ?? `Must be at most ${max} characters`,

  numeric: (msg = 'Must be a number') => (v: string | null) =>
    v && !isNaN(Number(v)) ? null : msg,

  url: (msg = 'Invalid URL') => (v: string | null) => {
    if (!v) return null;
    try {
      new URL(v);
      return null;
    } catch {
      return msg;
    }
  },
};

// Usage helper for fail responses
export function validationFail<T>(
  errors: ValidationError[],
  values: Partial<T>
) {
  return fail(400, {
    errors: errors.reduce(
      (acc, e) => ({ ...acc, [e.field]: e.message }),
      {} as Record<string, string>
    ),
    values,
  });
}
```

Using validation in actions:

```typescript
// src/routes/posts/new/+page.server.ts
import type { Actions } from './$types';
import { fail, redirect } from '@sveltejs/kit';
import { validate, validators, validationFail } from '$lib/server/validation';

interface PostInput {
  title: string;
  content: string;
  slug: string;
}

export const actions: Actions = {
  default: async ({ locals, request }) => {
    const formData = await request.formData();

    const validation = validate<PostInput>(formData, {
      title: (v) =>
        validators.required()(v) ?? validators.minLength(3)(v) ?? validators.maxLength(100)(v),
      content: validators.required('Post content is required'),
      slug: (v) =>
        validators.required()(v) ?? (v && /^[a-z0-9-]+$/.test(v) ? null : 'Invalid slug format'),
    });

    if (!validation.success) {
      return validationFail(validation.errors!, {
        title: formData.get('title')?.toString(),
        content: formData.get('content')?.toString(),
        slug: formData.get('slug')?.toString(),
      });
    }

    const { title, content, slug } = validation.data!;

    try {
      await locals.db.exec(
        `INSERT INTO posts (id, title, content, slug, created_at)
         VALUES (?, ?, ?, ?, ?)`,
        [crypto.randomUUID(), title, content, slug, Date.now()]
      );

      redirect(303, `/posts/${slug}`);
    } catch (err) {
      return fail(500, {
        error: 'Failed to create post',
        values: { title, content, slug },
      });
    }
  },
};
```

### Progressive Enhancement

Use SvelteKit's enhance for better UX:

```svelte
<!-- src/routes/todos/+page.svelte -->
<script lang="ts">
  import type { PageData, ActionData } from './$types';
  import { enhance } from '$app/forms';
  import { invalidateAll } from '$app/navigation';

  export let data: PageData;
  export let form: ActionData;

  let submitting = false;
</script>

<h1>Todos</h1>

<form
  method="POST"
  action="?/add"
  use:enhance={() => {
    submitting = true;
    return async ({ update }) => {
      await update();
      submitting = false;
    };
  }}
>
  <input
    type="text"
    name="title"
    placeholder="Add a new todo..."
    disabled={submitting}
    required
  />
  <button type="submit" disabled={submitting}>
    {submitting ? 'Adding...' : 'Add'}
  </button>
</form>

{#if form?.error}
  <p class="error">{form.error}</p>
{/if}

<ul>
  {#each data.todos as todo (todo.id)}
    <li class:completed={todo.completed}>
      <form
        method="POST"
        action="?/toggle"
        use:enhance={() => {
          // Optimistic update
          todo.completed = !todo.completed;
          return async ({ update }) => {
            await update({ reset: false });
          };
        }}
      >
        <input type="hidden" name="id" value={todo.id} />
        <input
          type="checkbox"
          checked={todo.completed}
          on:change={(e) => e.currentTarget.form?.requestSubmit()}
        />
        <span>{todo.title}</span>
      </form>

      <form
        method="POST"
        action="?/delete"
        use:enhance={() => {
          // Optimistic removal
          const index = data.todos.indexOf(todo);
          data.todos.splice(index, 1);
          data.todos = data.todos;
          return async ({ update }) => {
            await update({ reset: false });
          };
        }}
      >
        <input type="hidden" name="id" value={todo.id} />
        <button type="submit" aria-label="Delete">x</button>
      </form>
    </li>
  {/each}
</ul>

<style>
  .completed span {
    text-decoration: line-through;
    opacity: 0.6;
  }
</style>
```

### Optimistic Updates

Implement optimistic updates with rollback:

```svelte
<!-- src/lib/components/LikeButton.svelte -->
<script lang="ts">
  import { enhance } from '$app/forms';

  export let postId: string;
  export let likes: number;
  export let liked: boolean;

  let optimisticLikes = likes;
  let optimisticLiked = liked;
  let submitting = false;
</script>

<form
  method="POST"
  action="/posts/{postId}?/like"
  use:enhance={() => {
    submitting = true;

    // Store original values for rollback
    const originalLikes = optimisticLikes;
    const originalLiked = optimisticLiked;

    // Optimistic update
    optimisticLiked = !optimisticLiked;
    optimisticLikes += optimisticLiked ? 1 : -1;

    return async ({ result, update }) => {
      submitting = false;

      if (result.type === 'failure' || result.type === 'error') {
        // Rollback on failure
        optimisticLikes = originalLikes;
        optimisticLiked = originalLiked;
      } else {
        // Sync with server
        await update({ reset: false });
        likes = optimisticLikes;
        liked = optimisticLiked;
      }
    };
  }}
>
  <button type="submit" disabled={submitting} class:liked={optimisticLiked}>
    {optimisticLiked ? 'heart-filled' : 'heart'}
    {optimisticLikes}
  </button>
</form>

<style>
  .liked {
    color: red;
  }
</style>
```

---

## API Routes

### RESTful API Endpoints

Create API routes in `+server.ts` files:

```typescript
// src/routes/api/users/+server.ts
import type { RequestHandler } from './$types';
import { json, error } from '@sveltejs/kit';
import { getDB } from '$lib/server/db';
import { SQLError, RPCErrorCode } from 'sql.do';

interface User {
  id: string;
  name: string;
  email: string;
  created_at: number;
}

// GET /api/users - List users
export const GET: RequestHandler = async ({ url }) => {
  const db = getDB();
  const limit = Math.min(parseInt(url.searchParams.get('limit') ?? '50', 10), 100);
  const offset = parseInt(url.searchParams.get('offset') ?? '0', 10);
  const search = url.searchParams.get('search');

  try {
    let query = 'SELECT id, name, email, created_at FROM users';
    const params: (string | number)[] = [];

    if (search) {
      query += ' WHERE name LIKE ? OR email LIKE ?';
      params.push(`%${search}%`, `%${search}%`);
    }

    query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
    params.push(limit, offset);

    const result = await db.query<User>(query, params);

    return json({
      users: result.rows,
      pagination: { limit, offset },
    });
  } catch (err) {
    if (err instanceof SQLError) {
      error(500, { message: err.message });
    }
    throw err;
  }
};

// POST /api/users - Create user
export const POST: RequestHandler = async ({ request }) => {
  const db = getDB();

  let body: { name?: string; email?: string };
  try {
    body = await request.json();
  } catch {
    error(400, { message: 'Invalid JSON body' });
  }

  const { name, email } = body;

  if (!name || !email) {
    error(400, { message: 'Name and email are required' });
  }

  try {
    const id = crypto.randomUUID();
    const createdAt = Date.now();

    await db.exec(
      `INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)`,
      [id, name, email.toLowerCase(), createdAt]
    );

    return json(
      { id, name, email, created_at: createdAt },
      { status: 201 }
    );
  } catch (err) {
    if (err instanceof SQLError) {
      if (err.code === RPCErrorCode.CONSTRAINT_VIOLATION) {
        error(409, { message: 'User with this email already exists' });
      }
      error(500, { message: err.message });
    }
    throw err;
  }
};
```

Individual resource endpoints:

```typescript
// src/routes/api/users/[id]/+server.ts
import type { RequestHandler } from './$types';
import { json, error } from '@sveltejs/kit';
import { getDB } from '$lib/server/db';

interface User {
  id: string;
  name: string;
  email: string;
  bio: string | null;
  created_at: number;
  updated_at: number;
}

// GET /api/users/:id
export const GET: RequestHandler = async ({ params }) => {
  const db = getDB();

  const result = await db.query<User>(
    'SELECT * FROM users WHERE id = ?',
    [params.id]
  );

  if (result.rows.length === 0) {
    error(404, { message: 'User not found' });
  }

  return json(result.rows[0]);
};

// PATCH /api/users/:id
export const PATCH: RequestHandler = async ({ params, request }) => {
  const db = getDB();

  const body = await request.json();
  const updates: string[] = [];
  const values: (string | number | null)[] = [];

  if (body.name !== undefined) {
    updates.push('name = ?');
    values.push(body.name);
  }

  if (body.email !== undefined) {
    updates.push('email = ?');
    values.push(body.email.toLowerCase());
  }

  if (body.bio !== undefined) {
    updates.push('bio = ?');
    values.push(body.bio);
  }

  if (updates.length === 0) {
    error(400, { message: 'No fields to update' });
  }

  updates.push('updated_at = ?');
  values.push(Date.now());
  values.push(params.id);

  const result = await db.query<User>(
    `UPDATE users SET ${updates.join(', ')} WHERE id = ? RETURNING *`,
    values
  );

  if (result.rows.length === 0) {
    error(404, { message: 'User not found' });
  }

  return json(result.rows[0]);
};

// DELETE /api/users/:id
export const DELETE: RequestHandler = async ({ params }) => {
  const db = getDB();

  const result = await db.exec(
    'DELETE FROM users WHERE id = ?',
    [params.id]
  );

  if (result.changes === 0) {
    error(404, { message: 'User not found' });
  }

  return new Response(null, { status: 204 });
};
```

### Streaming Responses

Stream large result sets:

```typescript
// src/routes/api/export/users/+server.ts
import type { RequestHandler } from './$types';
import { getDB } from '$lib/server/db';

export const GET: RequestHandler = async () => {
  const db = getDB();

  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      // Write CSV header
      controller.enqueue(encoder.encode('id,name,email,created_at\n'));

      // Stream users in batches
      let offset = 0;
      const batchSize = 1000;

      while (true) {
        const result = await db.query<{
          id: string;
          name: string;
          email: string;
          created_at: number;
        }>(
          'SELECT id, name, email, created_at FROM users ORDER BY id LIMIT ? OFFSET ?',
          [batchSize, offset]
        );

        if (result.rows.length === 0) {
          break;
        }

        for (const row of result.rows) {
          const line = `${row.id},${row.name},${row.email},${row.created_at}\n`;
          controller.enqueue(encoder.encode(line));
        }

        offset += batchSize;

        // Yield to prevent blocking
        await new Promise((resolve) => setTimeout(resolve, 0));
      }

      controller.close();
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/csv',
      'Content-Disposition': 'attachment; filename="users.csv"',
    },
  });
};
```

### Request Validation

Use Zod for request validation:

```typescript
// src/routes/api/posts/+server.ts
import type { RequestHandler } from './$types';
import { json, error } from '@sveltejs/kit';
import { z } from 'zod';
import { getDB } from '$lib/server/db';

const CreatePostSchema = z.object({
  title: z.string().min(3).max(100),
  content: z.string().min(10),
  slug: z.string().regex(/^[a-z0-9-]+$/).optional(),
  published: z.boolean().default(false),
  tags: z.array(z.string()).optional(),
});

export const POST: RequestHandler = async ({ request, locals }) => {
  const db = getDB();

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    error(400, { message: 'Invalid JSON' });
  }

  const parsed = CreatePostSchema.safeParse(body);

  if (!parsed.success) {
    error(400, {
      message: 'Validation failed',
      errors: parsed.error.flatten().fieldErrors,
    });
  }

  const { title, content, slug, published, tags } = parsed.data;
  const id = crypto.randomUUID();
  const createdAt = Date.now();
  const generatedSlug = slug ?? title.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');

  await db.transaction(async (tx) => {
    await tx.exec(
      `INSERT INTO posts (id, title, content, slug, published, created_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [id, title, content, generatedSlug, published ? 1 : 0, createdAt]
    );

    if (tags && tags.length > 0) {
      for (const tag of tags) {
        await tx.exec(
          `INSERT INTO post_tags (post_id, tag) VALUES (?, ?)
           ON CONFLICT DO NOTHING`,
          [id, tag]
        );
      }
    }
  });

  return json({ id, slug: generatedSlug }, { status: 201 });
};
```

---

## Edge Deployment with Cloudflare

### Cloudflare Adapter Setup

Install and configure the Cloudflare adapter:

```typescript
// svelte.config.js
import adapter from '@sveltejs/adapter-cloudflare';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: vitePreprocess(),

  kit: {
    adapter: adapter({
      // Cloudflare Pages options
      routes: {
        include: ['/*'],
        exclude: ['<all>'],
      },
      // Enable platform emulation in dev
      platformProxy: {
        configPath: 'wrangler.toml',
        experimentalJsonConfig: false,
        persist: true,
      },
    }),
  },
};

export default config;
```

### Platform Bindings

Access Cloudflare bindings in your app:

```typescript
// src/lib/server/db.ts
import { createSQLClient, type DoSQLClient, type SQLClientConfig } from 'sql.do';

export function createDBFromPlatform(platform: App.Platform | undefined): DoSQLClient {
  // In production, use Cloudflare environment bindings
  if (platform?.env?.DOSQL_URL) {
    return createSQLClient({
      url: platform.env.DOSQL_URL,
      token: platform.env.DOSQL_TOKEN,
      timeout: 30000,
    });
  }

  // Fallback for local development
  return createSQLClient({
    url: 'http://localhost:8787',
    timeout: 30000,
  });
}

// Access other Cloudflare bindings
export function getKV(platform: App.Platform | undefined): KVNamespace | null {
  return platform?.env?.KV ?? null;
}

export function getR2(platform: App.Platform | undefined): R2Bucket | null {
  return platform?.env?.BUCKET ?? null;
}
```

Using platform in hooks:

```typescript
// src/hooks.server.ts
import type { Handle } from '@sveltejs/kit';
import { createDBFromPlatform } from '$lib/server/db';

export const handle: Handle = async ({ event, resolve }) => {
  // Database client with platform bindings
  event.locals.db = createDBFromPlatform(event.platform);

  // Access KV for caching
  const kv = event.platform?.env?.KV;
  if (kv) {
    event.locals.cache = {
      get: async (key: string) => kv.get(key, 'json'),
      set: async (key: string, value: unknown, ttl?: number) =>
        kv.put(key, JSON.stringify(value), ttl ? { expirationTtl: ttl } : undefined),
    };
  }

  return resolve(event);
};
```

### Wrangler Configuration

Create `wrangler.toml` for development:

```toml
# wrangler.toml
name = "my-sveltekit-app"
compatibility_date = "2026-01-22"
pages_build_output_dir = ".svelte-kit/cloudflare"

[vars]
DOSQL_URL = "https://sql.your-domain.com"

# Secrets (set via wrangler secret put)
# DOSQL_TOKEN

[[kv_namespaces]]
binding = "KV"
id = "your-kv-namespace-id"

[[r2_buckets]]
binding = "BUCKET"
bucket_name = "your-bucket"

[[durable_objects.bindings]]
name = "DOSQL"
class_name = "DoSQL"
script_name = "dosql-worker"

[dev]
port = 5173
```

---

## Type-Safe Queries

### Defining Database Types

Create a central types file:

```typescript
// src/lib/types/database.ts

// Core entity types
export interface User {
  id: string;
  name: string;
  email: string;
  avatar_url: string | null;
  bio: string | null;
  role: 'admin' | 'user' | 'guest';
  created_at: number;
  updated_at: number;
}

export interface Post {
  id: string;
  author_id: string;
  title: string;
  slug: string;
  content: string;
  excerpt: string | null;
  published: boolean;
  published_at: number | null;
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

// Join/aggregate types
export interface PostWithAuthor extends Post {
  author_name: string;
  author_avatar: string | null;
}

export interface PostWithCommentCount extends Post {
  comment_count: number;
}

export interface UserStats {
  user_id: string;
  post_count: number;
  comment_count: number;
  total_likes: number;
}

// Insert/Update types (omit auto-generated fields)
export type UserInsert = Omit<User, 'created_at' | 'updated_at'>;
export type UserUpdate = Partial<Omit<User, 'id' | 'created_at'>>;

export type PostInsert = Omit<Post, 'id' | 'created_at' | 'updated_at'>;
export type PostUpdate = Partial<Omit<Post, 'id' | 'author_id' | 'created_at'>>;
```

### Type Inference Patterns

Create type-safe query builders:

```typescript
// src/lib/server/queries.ts
import type { DoSQLClient, QueryResult } from 'sql.do';
import type { User, Post, PostWithAuthor, UserStats } from '$lib/types/database';

// Type-safe repository pattern
export class UserRepository {
  constructor(private db: DoSQLClient) {}

  async findById(id: string): Promise<User | null> {
    const result = await this.db.query<User>(
      'SELECT * FROM users WHERE id = ?',
      [id]
    );
    return result.rows[0] ?? null;
  }

  async findByEmail(email: string): Promise<User | null> {
    const result = await this.db.query<User>(
      'SELECT * FROM users WHERE email = ?',
      [email.toLowerCase()]
    );
    return result.rows[0] ?? null;
  }

  async findAll(options: {
    limit?: number;
    offset?: number;
    role?: User['role'];
  } = {}): Promise<User[]> {
    const { limit = 50, offset = 0, role } = options;
    const params: (string | number)[] = [];
    let query = 'SELECT * FROM users';

    if (role) {
      query += ' WHERE role = ?';
      params.push(role);
    }

    query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
    params.push(limit, offset);

    const result = await this.db.query<User>(query, params);
    return result.rows;
  }

  async create(user: Omit<User, 'created_at' | 'updated_at'>): Promise<User> {
    const now = Date.now();
    const result = await this.db.query<User>(
      `INSERT INTO users (id, name, email, avatar_url, bio, role, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       RETURNING *`,
      [user.id, user.name, user.email, user.avatar_url, user.bio, user.role, now, now]
    );
    return result.rows[0];
  }

  async update(id: string, updates: Partial<Omit<User, 'id' | 'created_at'>>): Promise<User | null> {
    const fields: string[] = [];
    const values: (string | number | null)[] = [];

    for (const [key, value] of Object.entries(updates)) {
      fields.push(`${key} = ?`);
      values.push(value as string | number | null);
    }

    if (fields.length === 0) return this.findById(id);

    fields.push('updated_at = ?');
    values.push(Date.now());
    values.push(id);

    const result = await this.db.query<User>(
      `UPDATE users SET ${fields.join(', ')} WHERE id = ? RETURNING *`,
      values
    );
    return result.rows[0] ?? null;
  }

  async delete(id: string): Promise<boolean> {
    const result = await this.db.exec('DELETE FROM users WHERE id = ?', [id]);
    return result.changes > 0;
  }

  async getStats(userId: string): Promise<UserStats | null> {
    const result = await this.db.query<UserStats>(`
      SELECT
        ? as user_id,
        (SELECT COUNT(*) FROM posts WHERE author_id = ?) as post_count,
        (SELECT COUNT(*) FROM comments WHERE author_id = ?) as comment_count,
        (SELECT COUNT(*) FROM likes l JOIN posts p ON l.post_id = p.id WHERE p.author_id = ?) as total_likes
    `, [userId, userId, userId, userId]);
    return result.rows[0] ?? null;
  }
}

export class PostRepository {
  constructor(private db: DoSQLClient) {}

  async findBySlug(slug: string): Promise<PostWithAuthor | null> {
    const result = await this.db.query<PostWithAuthor>(`
      SELECT p.*, u.name as author_name, u.avatar_url as author_avatar
      FROM posts p
      JOIN users u ON p.author_id = u.id
      WHERE p.slug = ?
    `, [slug]);
    return result.rows[0] ?? null;
  }

  async findPublished(options: { limit?: number; offset?: number } = {}): Promise<PostWithAuthor[]> {
    const { limit = 20, offset = 0 } = options;

    const result = await this.db.query<PostWithAuthor>(`
      SELECT p.*, u.name as author_name, u.avatar_url as author_avatar
      FROM posts p
      JOIN users u ON p.author_id = u.id
      WHERE p.published = 1
      ORDER BY p.published_at DESC
      LIMIT ? OFFSET ?
    `, [limit, offset]);

    return result.rows;
  }
}
```

### Generic Query Functions

Create generic helpers for common patterns:

```typescript
// src/lib/server/db-helpers.ts
import type { DoSQLClient, QueryResult } from 'sql.do';

// Paginated query helper
export interface PaginatedResult<T> {
  data: T[];
  pagination: {
    page: number;
    pageSize: number;
    total: number;
    totalPages: number;
    hasNext: boolean;
    hasPrev: boolean;
  };
}

export async function paginate<T>(
  db: DoSQLClient,
  query: string,
  countQuery: string,
  params: unknown[],
  page: number,
  pageSize: number
): Promise<PaginatedResult<T>> {
  const offset = (page - 1) * pageSize;

  const [dataResult, countResult] = await Promise.all([
    db.query<T>(`${query} LIMIT ? OFFSET ?`, [...params, pageSize, offset]),
    db.query<{ count: number }>(countQuery, params),
  ]);

  const total = countResult.rows[0]?.count ?? 0;
  const totalPages = Math.ceil(total / pageSize);

  return {
    data: dataResult.rows,
    pagination: {
      page,
      pageSize,
      total,
      totalPages,
      hasNext: page < totalPages,
      hasPrev: page > 1,
    },
  };
}

// Transaction helper with typed context
export async function withTransaction<T>(
  db: DoSQLClient,
  fn: (tx: DoSQLClient) => Promise<T>
): Promise<T> {
  return db.transaction(fn);
}

// Single result helper
export async function findOne<T>(
  db: DoSQLClient,
  query: string,
  params?: unknown[]
): Promise<T | null> {
  const result = await db.query<T>(query, params);
  return result.rows[0] ?? null;
}

// Exists check helper
export async function exists(
  db: DoSQLClient,
  table: string,
  field: string,
  value: unknown
): Promise<boolean> {
  const result = await db.query<{ exists: number }>(
    `SELECT EXISTS(SELECT 1 FROM ${table} WHERE ${field} = ?) as exists`,
    [value]
  );
  return result.rows[0]?.exists === 1;
}
```

---

## Real-Time Updates with CDC

### Setting Up CDC Subscriptions

Create a CDC subscription service:

```typescript
// src/lib/server/cdc.ts
import { createLakeClient, type CDCEvent, type LakeClient } from 'lake.do';
import type { RequestEvent } from '@sveltejs/kit';

let lakeClient: LakeClient | null = null;

export function getLakeClient(event?: RequestEvent): LakeClient {
  if (!lakeClient) {
    const url = event?.platform?.env?.LAKE_URL ?? process.env.LAKE_URL ?? 'http://localhost:8788';
    const token = event?.platform?.env?.LAKE_TOKEN ?? process.env.LAKE_TOKEN;

    lakeClient = createLakeClient({ url, token });
  }
  return lakeClient;
}

export interface CDCSubscription {
  unsubscribe: () => void;
}

export interface CDCOptions {
  tables: string[];
  startLSN?: number;
  onEvent: (event: CDCEvent) => void;
  onError?: (error: Error) => void;
}

export async function subscribeToCDC(options: CDCOptions): Promise<CDCSubscription> {
  const lake = getLakeClient();

  const subscription = await lake.subscribeToCDC({
    tables: options.tables,
    startLSN: options.startLSN ?? 0,
    onEvent: options.onEvent,
    onError: options.onError ?? console.error,
  });

  return {
    unsubscribe: () => subscription.close(),
  };
}
```

### Svelte Stores for Real-Time Data

Create reactive stores that sync with CDC:

```typescript
// src/lib/stores/realtime.ts
import { writable, type Writable } from 'svelte/store';
import { browser } from '$app/environment';

interface RealtimeStoreOptions<T> {
  table: string;
  initialData: T[];
  getKey: (item: T) => string;
}

export function createRealtimeStore<T>(options: RealtimeStoreOptions<T>) {
  const { table, initialData, getKey } = options;
  const store = writable<T[]>(initialData);
  let eventSource: EventSource | null = null;

  function connect() {
    if (!browser) return;

    eventSource = new EventSource(`/api/cdc/stream?table=${table}`);

    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);

      store.update((items) => {
        switch (data.type) {
          case 'INSERT':
            return [...items, data.newValue as T];

          case 'UPDATE':
            return items.map((item) =>
              getKey(item) === getKey(data.newValue as T) ? (data.newValue as T) : item
            );

          case 'DELETE':
            return items.filter((item) => getKey(item) !== getKey(data.oldValue as T));

          default:
            return items;
        }
      });
    };

    eventSource.onerror = () => {
      // Reconnect after delay
      eventSource?.close();
      setTimeout(connect, 5000);
    };
  }

  function disconnect() {
    eventSource?.close();
    eventSource = null;
  }

  return {
    subscribe: store.subscribe,
    connect,
    disconnect,
    set: store.set,
  };
}

// Usage in component:
// const todos = createRealtimeStore({ table: 'todos', initialData: data.todos, getKey: (t) => t.id });
// onMount(() => todos.connect());
// onDestroy(() => todos.disconnect());
```

### Server-Sent Events

Create an SSE endpoint for CDC streaming:

```typescript
// src/routes/api/cdc/stream/+server.ts
import type { RequestHandler } from './$types';
import { getLakeClient } from '$lib/server/cdc';

export const GET: RequestHandler = async ({ url, request }) => {
  const table = url.searchParams.get('table');

  if (!table) {
    return new Response('Missing table parameter', { status: 400 });
  }

  const lake = getLakeClient();

  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      // Send initial connection message
      controller.enqueue(encoder.encode(': connected\n\n'));

      // Subscribe to CDC events
      const subscription = await lake.subscribeToCDC({
        tables: [table],
        onEvent: (event) => {
          const data = JSON.stringify({
            type: event.type,
            table: event.table,
            oldValue: event.oldValue,
            newValue: event.newValue,
            lsn: event.lsn,
            timestamp: event.timestamp,
          });

          controller.enqueue(encoder.encode(`data: ${data}\n\n`));
        },
        onError: (error) => {
          controller.enqueue(
            encoder.encode(`event: error\ndata: ${JSON.stringify({ message: error.message })}\n\n`)
          );
        },
      });

      // Handle client disconnect
      request.signal.addEventListener('abort', () => {
        subscription.close();
        controller.close();
      });

      // Keep-alive ping every 30 seconds
      const keepAlive = setInterval(() => {
        controller.enqueue(encoder.encode(': ping\n\n'));
      }, 30000);

      request.signal.addEventListener('abort', () => {
        clearInterval(keepAlive);
      });
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    },
  });
};
```

Client-side SSE consumption:

```svelte
<!-- src/lib/components/RealtimeList.svelte -->
<script lang="ts">
  import { onMount, onDestroy } from 'svelte';

  export let table: string;
  export let initialItems: unknown[];
  export let getKey: (item: unknown) => string;

  let items = initialItems;
  let eventSource: EventSource | null = null;
  let connected = false;
  let error: string | null = null;

  onMount(() => {
    eventSource = new EventSource(`/api/cdc/stream?table=${table}`);

    eventSource.onopen = () => {
      connected = true;
      error = null;
    };

    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);

      switch (data.type) {
        case 'INSERT':
          items = [...items, data.newValue];
          break;
        case 'UPDATE':
          items = items.map((item) =>
            getKey(item) === getKey(data.newValue) ? data.newValue : item
          );
          break;
        case 'DELETE':
          items = items.filter((item) => getKey(item) !== getKey(data.oldValue));
          break;
      }
    };

    eventSource.addEventListener('error', (e) => {
      connected = false;
      error = 'Connection lost. Reconnecting...';

      // EventSource auto-reconnects
    });
  });

  onDestroy(() => {
    eventSource?.close();
  });
</script>

<div class="realtime-status" class:connected class:error={!!error}>
  {#if error}
    <span class="error-message">{error}</span>
  {:else if connected}
    <span class="connected-indicator">Live</span>
  {:else}
    <span class="connecting">Connecting...</span>
  {/if}
</div>

<slot {items} />

<style>
  .connected-indicator {
    color: green;
  }
  .error-message {
    color: orange;
  }
  .connecting {
    color: gray;
  }
</style>
```

---

## Example: Todo App with Real-Time Sync

A complete todo application with real-time synchronization across clients.

### Database Schema

```sql
-- Run this on your DoSQL instance
CREATE TABLE IF NOT EXISTS todos (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  completed INTEGER DEFAULT 0,
  position INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_todos_position ON todos(position);
```

### Server Implementation

```typescript
// src/routes/todos/+page.server.ts
import type { PageServerLoad, Actions } from './$types';
import { fail } from '@sveltejs/kit';

interface Todo {
  id: string;
  title: string;
  completed: boolean;
  position: number;
  created_at: number;
  updated_at: number;
}

export const load: PageServerLoad = async ({ locals }) => {
  const result = await locals.db.query<Todo>(
    'SELECT id, title, completed, position, created_at, updated_at FROM todos ORDER BY position ASC'
  );

  // Convert SQLite integers to booleans
  const todos = result.rows.map((row) => ({
    ...row,
    completed: Boolean(row.completed),
  }));

  return { todos };
};

export const actions: Actions = {
  add: async ({ locals, request }) => {
    const formData = await request.formData();
    const title = formData.get('title')?.toString().trim();

    if (!title) {
      return fail(400, { error: 'Title is required', action: 'add' });
    }

    const id = crypto.randomUUID();
    const now = Date.now();

    // Get next position
    const posResult = await locals.db.query<{ max_pos: number | null }>(
      'SELECT MAX(position) as max_pos FROM todos'
    );
    const position = (posResult.rows[0]?.max_pos ?? -1) + 1;

    await locals.db.exec(
      `INSERT INTO todos (id, title, completed, position, created_at, updated_at)
       VALUES (?, ?, 0, ?, ?, ?)`,
      [id, title, position, now, now]
    );

    return { success: true, action: 'add' };
  },

  toggle: async ({ locals, request }) => {
    const formData = await request.formData();
    const id = formData.get('id')?.toString();

    if (!id) {
      return fail(400, { error: 'ID is required', action: 'toggle' });
    }

    await locals.db.exec(
      `UPDATE todos SET completed = NOT completed, updated_at = ? WHERE id = ?`,
      [Date.now(), id]
    );

    return { success: true, action: 'toggle' };
  },

  update: async ({ locals, request }) => {
    const formData = await request.formData();
    const id = formData.get('id')?.toString();
    const title = formData.get('title')?.toString().trim();

    if (!id || !title) {
      return fail(400, { error: 'ID and title are required', action: 'update' });
    }

    await locals.db.exec(
      `UPDATE todos SET title = ?, updated_at = ? WHERE id = ?`,
      [title, Date.now(), id]
    );

    return { success: true, action: 'update' };
  },

  delete: async ({ locals, request }) => {
    const formData = await request.formData();
    const id = formData.get('id')?.toString();

    if (!id) {
      return fail(400, { error: 'ID is required', action: 'delete' });
    }

    await locals.db.exec('DELETE FROM todos WHERE id = ?', [id]);

    return { success: true, action: 'delete' };
  },

  reorder: async ({ locals, request }) => {
    const formData = await request.formData();
    const order = formData.get('order')?.toString();

    if (!order) {
      return fail(400, { error: 'Order is required', action: 'reorder' });
    }

    const ids = order.split(',');
    const now = Date.now();

    await locals.db.transaction(async (tx) => {
      for (let i = 0; i < ids.length; i++) {
        await tx.exec(
          'UPDATE todos SET position = ?, updated_at = ? WHERE id = ?',
          [i, now, ids[i]]
        );
      }
    });

    return { success: true, action: 'reorder' };
  },

  clearCompleted: async ({ locals }) => {
    await locals.db.exec('DELETE FROM todos WHERE completed = 1');
    return { success: true, action: 'clearCompleted' };
  },
};
```

### Client Component

```svelte
<!-- src/routes/todos/+page.svelte -->
<script lang="ts">
  import type { PageData, ActionData } from './$types';
  import { enhance } from '$app/forms';
  import { onMount, onDestroy } from 'svelte';
  import { flip } from 'svelte/animate';
  import { fade, slide } from 'svelte/transition';

  export let data: PageData;
  export let form: ActionData;

  let todos = data.todos;
  let newTodoTitle = '';
  let filter: 'all' | 'active' | 'completed' = 'all';
  let editingId: string | null = null;
  let editingTitle = '';
  let eventSource: EventSource | null = null;

  // Real-time sync
  onMount(() => {
    eventSource = new EventSource('/api/cdc/stream?table=todos');

    eventSource.onmessage = (event) => {
      const { type, newValue, oldValue } = JSON.parse(event.data);

      switch (type) {
        case 'INSERT':
          if (!todos.find((t) => t.id === newValue.id)) {
            todos = [...todos, { ...newValue, completed: Boolean(newValue.completed) }];
            todos.sort((a, b) => a.position - b.position);
          }
          break;

        case 'UPDATE':
          todos = todos.map((t) =>
            t.id === newValue.id ? { ...newValue, completed: Boolean(newValue.completed) } : t
          );
          todos.sort((a, b) => a.position - b.position);
          break;

        case 'DELETE':
          todos = todos.filter((t) => t.id !== oldValue.id);
          break;
      }
    };
  });

  onDestroy(() => {
    eventSource?.close();
  });

  $: filteredTodos = todos.filter((todo) => {
    if (filter === 'active') return !todo.completed;
    if (filter === 'completed') return todo.completed;
    return true;
  });

  $: remaining = todos.filter((t) => !t.completed).length;
  $: hasCompleted = todos.some((t) => t.completed);

  function startEditing(todo: (typeof todos)[0]) {
    editingId = todo.id;
    editingTitle = todo.title;
  }

  function cancelEditing() {
    editingId = null;
    editingTitle = '';
  }

  // Drag and drop
  let draggedId: string | null = null;

  function handleDragStart(event: DragEvent, id: string) {
    draggedId = id;
    if (event.dataTransfer) {
      event.dataTransfer.effectAllowed = 'move';
    }
  }

  function handleDragOver(event: DragEvent, targetId: string) {
    event.preventDefault();
    if (draggedId && draggedId !== targetId) {
      const draggedIndex = todos.findIndex((t) => t.id === draggedId);
      const targetIndex = todos.findIndex((t) => t.id === targetId);

      const newTodos = [...todos];
      const [removed] = newTodos.splice(draggedIndex, 1);
      newTodos.splice(targetIndex, 0, removed);
      todos = newTodos;
    }
  }

  function handleDragEnd() {
    if (draggedId) {
      // Submit reorder form
      const form = document.getElementById('reorder-form') as HTMLFormElement;
      const input = form.querySelector('input[name="order"]') as HTMLInputElement;
      input.value = todos.map((t) => t.id).join(',');
      form.requestSubmit();
    }
    draggedId = null;
  }
</script>

<svelte:head>
  <title>Todos - Real-Time Sync</title>
</svelte:head>

<main>
  <h1>Todos</h1>

  <form
    method="POST"
    action="?/add"
    use:enhance={() => {
      const title = newTodoTitle;
      newTodoTitle = '';

      // Optimistic add
      const tempId = crypto.randomUUID();
      todos = [
        ...todos,
        {
          id: tempId,
          title,
          completed: false,
          position: todos.length,
          created_at: Date.now(),
          updated_at: Date.now(),
        },
      ];

      return async ({ result, update }) => {
        if (result.type === 'failure') {
          // Rollback
          todos = todos.filter((t) => t.id !== tempId);
          newTodoTitle = title;
        }
        await update({ reset: false });
      };
    }}
  >
    <input
      type="text"
      name="title"
      bind:value={newTodoTitle}
      placeholder="What needs to be done?"
      autofocus
    />
    <button type="submit" disabled={!newTodoTitle.trim()}>Add</button>
  </form>

  {#if form?.error}
    <p class="error" transition:fade>{form.error}</p>
  {/if}

  <nav class="filters">
    <button class:active={filter === 'all'} on:click={() => (filter = 'all')}>All</button>
    <button class:active={filter === 'active'} on:click={() => (filter = 'active')}>Active</button>
    <button class:active={filter === 'completed'} on:click={() => (filter = 'completed')}>
      Completed
    </button>
  </nav>

  <ul class="todo-list">
    {#each filteredTodos as todo (todo.id)}
      <li
        class:completed={todo.completed}
        class:editing={editingId === todo.id}
        class:dragging={draggedId === todo.id}
        animate:flip={{ duration: 200 }}
        transition:slide
        draggable="true"
        on:dragstart={(e) => handleDragStart(e, todo.id)}
        on:dragover={(e) => handleDragOver(e, todo.id)}
        on:dragend={handleDragEnd}
      >
        {#if editingId === todo.id}
          <form
            method="POST"
            action="?/update"
            use:enhance={() => {
              cancelEditing();
              return async ({ update }) => update({ reset: false });
            }}
          >
            <input type="hidden" name="id" value={todo.id} />
            <input
              type="text"
              name="title"
              bind:value={editingTitle}
              on:blur={cancelEditing}
              on:keydown={(e) => e.key === 'Escape' && cancelEditing()}
              autofocus
            />
          </form>
        {:else}
          <form
            method="POST"
            action="?/toggle"
            use:enhance={() => {
              todo.completed = !todo.completed;
              return async ({ update }) => update({ reset: false });
            }}
          >
            <input type="hidden" name="id" value={todo.id} />
            <input
              type="checkbox"
              checked={todo.completed}
              on:change={(e) => e.currentTarget.form?.requestSubmit()}
            />
          </form>

          <span class="title" on:dblclick={() => startEditing(todo)}>
            {todo.title}
          </span>

          <form
            method="POST"
            action="?/delete"
            use:enhance={() => {
              const index = todos.indexOf(todo);
              todos = todos.filter((t) => t !== todo);

              return async ({ result, update }) => {
                if (result.type === 'failure') {
                  todos.splice(index, 0, todo);
                  todos = todos;
                }
                await update({ reset: false });
              };
            }}
          >
            <input type="hidden" name="id" value={todo.id} />
            <button type="submit" class="delete" aria-label="Delete">x</button>
          </form>
        {/if}
      </li>
    {/each}
  </ul>

  <form id="reorder-form" method="POST" action="?/reorder" use:enhance hidden>
    <input type="hidden" name="order" value="" />
  </form>

  <footer>
    <span>{remaining} item{remaining !== 1 ? 's' : ''} left</span>

    {#if hasCompleted}
      <form method="POST" action="?/clearCompleted" use:enhance>
        <button type="submit">Clear completed</button>
      </form>
    {/if}
  </footer>
</main>

<style>
  main {
    max-width: 600px;
    margin: 2rem auto;
    padding: 0 1rem;
  }

  .todo-list {
    list-style: none;
    padding: 0;
  }

  .todo-list li {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.75rem;
    border-bottom: 1px solid #eee;
    cursor: grab;
  }

  .todo-list li.dragging {
    opacity: 0.5;
  }

  .todo-list li.completed .title {
    text-decoration: line-through;
    opacity: 0.6;
  }

  .title {
    flex: 1;
  }

  .delete {
    opacity: 0;
    background: none;
    border: none;
    color: #cc0000;
    cursor: pointer;
    font-size: 1.25rem;
  }

  li:hover .delete {
    opacity: 1;
  }

  .filters {
    display: flex;
    gap: 0.5rem;
    margin: 1rem 0;
  }

  .filters button {
    background: none;
    border: 1px solid transparent;
    padding: 0.25rem 0.5rem;
    cursor: pointer;
  }

  .filters button.active {
    border-color: #999;
    border-radius: 3px;
  }

  footer {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.5rem 0;
    color: #666;
  }

  .error {
    background: #fee;
    color: #c00;
    padding: 0.5rem;
    border-radius: 4px;
  }
</style>
```

---

## Example: Dashboard with Live Data

A real-time analytics dashboard showing live metrics.

### Server Implementation

```typescript
// src/routes/dashboard/+page.server.ts
import type { PageServerLoad } from './$types';

interface DashboardMetrics {
  totalUsers: number;
  activeUsers: number;
  totalOrders: number;
  revenue: number;
  conversionRate: number;
}

interface RecentActivity {
  id: string;
  type: 'signup' | 'order' | 'refund';
  description: string;
  amount: number | null;
  timestamp: number;
}

interface ChartData {
  date: string;
  orders: number;
  revenue: number;
}

export const load: PageServerLoad = async ({ locals }) => {
  const now = Date.now();
  const todayStart = new Date();
  todayStart.setHours(0, 0, 0, 0);
  const weekAgo = now - 7 * 24 * 60 * 60 * 1000;

  // Parallel data fetching
  const [metricsResults, activityResult, chartResult] = await Promise.all([
    // Dashboard metrics batch
    locals.db.batchQuery([
      { sql: 'SELECT COUNT(*) as count FROM users' },
      {
        sql: 'SELECT COUNT(DISTINCT user_id) as count FROM sessions WHERE last_active > ?',
        params: [now - 15 * 60 * 1000], // Active in last 15 minutes
      },
      { sql: 'SELECT COUNT(*) as count FROM orders WHERE created_at > ?', params: [todayStart.getTime()] },
      { sql: 'SELECT COALESCE(SUM(total), 0) as sum FROM orders WHERE created_at > ?', params: [todayStart.getTime()] },
      { sql: 'SELECT COUNT(*) as visitors FROM sessions WHERE created_at > ?', params: [todayStart.getTime()] },
      { sql: 'SELECT COUNT(*) as conversions FROM orders WHERE created_at > ?', params: [todayStart.getTime()] },
    ]),

    // Recent activity
    locals.db.query<RecentActivity>(`
      SELECT
        id,
        'signup' as type,
        name || ' signed up' as description,
        NULL as amount,
        created_at as timestamp
      FROM users
      WHERE created_at > ?
      UNION ALL
      SELECT
        o.id,
        'order' as type,
        u.name || ' placed an order' as description,
        o.total as amount,
        o.created_at as timestamp
      FROM orders o
      JOIN users u ON o.user_id = u.id
      WHERE o.created_at > ?
      ORDER BY timestamp DESC
      LIMIT 20
    `, [weekAgo, weekAgo]),

    // Chart data for last 7 days
    locals.db.query<ChartData>(`
      WITH RECURSIVE dates(date) AS (
        SELECT date(?, 'unixepoch', 'localtime')
        UNION ALL
        SELECT date(date, '+1 day')
        FROM dates
        WHERE date < date('now', 'localtime')
      )
      SELECT
        d.date,
        COUNT(o.id) as orders,
        COALESCE(SUM(o.total), 0) as revenue
      FROM dates d
      LEFT JOIN orders o ON date(o.created_at / 1000, 'unixepoch', 'localtime') = d.date
      GROUP BY d.date
      ORDER BY d.date
    `, [Math.floor(weekAgo / 1000)]),
  ]);

  const visitors = (metricsResults[4].rows[0]?.visitors as number) || 1;
  const conversions = (metricsResults[5].rows[0]?.conversions as number) || 0;

  const metrics: DashboardMetrics = {
    totalUsers: metricsResults[0].rows[0].count as number,
    activeUsers: metricsResults[1].rows[0].count as number,
    totalOrders: metricsResults[2].rows[0].count as number,
    revenue: metricsResults[3].rows[0].sum as number,
    conversionRate: (conversions / visitors) * 100,
  };

  return {
    metrics,
    recentActivity: activityResult.rows,
    chartData: chartResult.rows,
  };
};
```

### Dashboard Component

```svelte
<!-- src/routes/dashboard/+page.svelte -->
<script lang="ts">
  import type { PageData } from './$types';
  import { onMount, onDestroy } from 'svelte';
  import { tweened } from 'svelte/motion';
  import { cubicOut } from 'svelte/easing';

  export let data: PageData;

  // Animated metrics
  const totalUsers = tweened(0, { duration: 1000, easing: cubicOut });
  const activeUsers = tweened(0, { duration: 1000, easing: cubicOut });
  const totalOrders = tweened(0, { duration: 1000, easing: cubicOut });
  const revenue = tweened(0, { duration: 1000, easing: cubicOut });

  $: $totalUsers = data.metrics.totalUsers;
  $: $activeUsers = data.metrics.activeUsers;
  $: $totalOrders = data.metrics.totalOrders;
  $: $revenue = data.metrics.revenue;

  let activity = data.recentActivity;
  let eventSource: EventSource | null = null;

  onMount(() => {
    // Real-time updates for activity feed
    eventSource = new EventSource('/api/cdc/stream?table=users,orders');

    eventSource.onmessage = (event) => {
      const { type, table, newValue } = JSON.parse(event.data);

      if (type === 'INSERT') {
        let newActivity: (typeof activity)[0];

        if (table === 'users') {
          newActivity = {
            id: newValue.id,
            type: 'signup',
            description: `${newValue.name} signed up`,
            amount: null,
            timestamp: newValue.created_at,
          };
          $totalUsers++;
        } else if (table === 'orders') {
          newActivity = {
            id: newValue.id,
            type: 'order',
            description: `Order #${newValue.id.slice(0, 8)} placed`,
            amount: newValue.total,
            timestamp: newValue.created_at,
          };
          $totalOrders++;
          $revenue += newValue.total;
        } else {
          return;
        }

        activity = [newActivity, ...activity.slice(0, 19)];
      }
    };
  });

  onDestroy(() => {
    eventSource?.close();
  });

  function formatCurrency(value: number): string {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
    }).format(value);
  }

  function formatTime(timestamp: number): string {
    const diff = Date.now() - timestamp;
    if (diff < 60000) return 'Just now';
    if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
    return new Date(timestamp).toLocaleDateString();
  }
</script>

<svelte:head>
  <title>Dashboard - Live Metrics</title>
</svelte:head>

<main>
  <h1>Dashboard</h1>

  <div class="metrics-grid">
    <div class="metric-card">
      <h3>Total Users</h3>
      <p class="value">{Math.round($totalUsers).toLocaleString()}</p>
    </div>

    <div class="metric-card">
      <h3>Active Now</h3>
      <p class="value live">{Math.round($activeUsers).toLocaleString()}</p>
    </div>

    <div class="metric-card">
      <h3>Orders Today</h3>
      <p class="value">{Math.round($totalOrders).toLocaleString()}</p>
    </div>

    <div class="metric-card">
      <h3>Revenue Today</h3>
      <p class="value">{formatCurrency($revenue)}</p>
    </div>

    <div class="metric-card">
      <h3>Conversion Rate</h3>
      <p class="value">{data.metrics.conversionRate.toFixed(1)}%</p>
    </div>
  </div>

  <div class="dashboard-grid">
    <section class="chart-section">
      <h2>Revenue & Orders (7 days)</h2>
      <div class="chart">
        {#each data.chartData as day}
          <div class="bar-group">
            <div
              class="bar revenue"
              style="height: {(day.revenue / Math.max(...data.chartData.map((d) => d.revenue || 1))) * 100}%"
              title="{formatCurrency(day.revenue)}"
            />
            <div
              class="bar orders"
              style="height: {(day.orders / Math.max(...data.chartData.map((d) => d.orders || 1))) * 100}%"
              title="{day.orders} orders"
            />
            <span class="label">{new Date(day.date).toLocaleDateString('en-US', { weekday: 'short' })}</span>
          </div>
        {/each}
      </div>
      <div class="legend">
        <span class="legend-item"><span class="dot revenue" /> Revenue</span>
        <span class="legend-item"><span class="dot orders" /> Orders</span>
      </div>
    </section>

    <section class="activity-section">
      <h2>Recent Activity</h2>
      <ul class="activity-feed">
        {#each activity as item (item.id)}
          <li class="activity-item {item.type}">
            <span class="icon">
              {#if item.type === 'signup'}
                +
              {:else if item.type === 'order'}
                $
              {:else}
                -
              {/if}
            </span>
            <span class="description">{item.description}</span>
            {#if item.amount}
              <span class="amount">{formatCurrency(item.amount)}</span>
            {/if}
            <span class="time">{formatTime(item.timestamp)}</span>
          </li>
        {/each}
      </ul>
    </section>
  </div>
</main>

<style>
  main {
    max-width: 1200px;
    margin: 0 auto;
    padding: 2rem;
  }

  .metrics-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin-bottom: 2rem;
  }

  .metric-card {
    background: white;
    border-radius: 8px;
    padding: 1.5rem;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }

  .metric-card h3 {
    margin: 0;
    font-size: 0.875rem;
    color: #666;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .metric-card .value {
    margin: 0.5rem 0 0;
    font-size: 2rem;
    font-weight: 600;
  }

  .metric-card .value.live {
    color: #22c55e;
  }

  .metric-card .value.live::after {
    content: '';
    display: inline-block;
    width: 8px;
    height: 8px;
    background: #22c55e;
    border-radius: 50%;
    margin-left: 0.5rem;
    animation: pulse 2s infinite;
  }

  @keyframes pulse {
    0%,
    100% {
      opacity: 1;
    }
    50% {
      opacity: 0.5;
    }
  }

  .dashboard-grid {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 2rem;
  }

  @media (max-width: 768px) {
    .dashboard-grid {
      grid-template-columns: 1fr;
    }
  }

  .chart-section,
  .activity-section {
    background: white;
    border-radius: 8px;
    padding: 1.5rem;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }

  .chart {
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    height: 200px;
    margin: 1rem 0;
  }

  .bar-group {
    display: flex;
    flex-direction: column;
    align-items: center;
    flex: 1;
    height: 100%;
  }

  .bar {
    width: 20px;
    margin: 0 2px;
    border-radius: 4px 4px 0 0;
    transition: height 0.3s ease;
  }

  .bar.revenue {
    background: #3b82f6;
  }

  .bar.orders {
    background: #22c55e;
  }

  .bar-group .label {
    margin-top: 0.5rem;
    font-size: 0.75rem;
    color: #666;
  }

  .legend {
    display: flex;
    gap: 1rem;
    justify-content: center;
  }

  .legend-item {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.875rem;
    color: #666;
  }

  .dot {
    width: 12px;
    height: 12px;
    border-radius: 50%;
  }

  .dot.revenue {
    background: #3b82f6;
  }

  .dot.orders {
    background: #22c55e;
  }

  .activity-feed {
    list-style: none;
    padding: 0;
    margin: 0;
    max-height: 400px;
    overflow-y: auto;
  }

  .activity-item {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.75rem 0;
    border-bottom: 1px solid #eee;
  }

  .activity-item .icon {
    width: 32px;
    height: 32px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: bold;
    flex-shrink: 0;
  }

  .activity-item.signup .icon {
    background: #dbeafe;
    color: #3b82f6;
  }

  .activity-item.order .icon {
    background: #dcfce7;
    color: #22c55e;
  }

  .activity-item .description {
    flex: 1;
    font-size: 0.875rem;
  }

  .activity-item .amount {
    font-weight: 600;
    color: #22c55e;
  }

  .activity-item .time {
    font-size: 0.75rem;
    color: #999;
  }
</style>
```

---

## Performance Optimization

### Connection Pooling

Reuse database connections efficiently:

```typescript
// src/lib/server/db-pool.ts
import { createSQLClient, type DoSQLClient } from 'sql.do';

class ConnectionPool {
  private static instance: ConnectionPool;
  private client: DoSQLClient | null = null;
  private lastUsed = 0;
  private readonly maxIdleTime = 60000; // 1 minute

  private constructor() {}

  static getInstance(): ConnectionPool {
    if (!ConnectionPool.instance) {
      ConnectionPool.instance = new ConnectionPool();
    }
    return ConnectionPool.instance;
  }

  async getClient(): Promise<DoSQLClient> {
    const now = Date.now();

    // Check if connection is stale
    if (this.client && now - this.lastUsed > this.maxIdleTime) {
      await this.client.close();
      this.client = null;
    }

    if (!this.client) {
      this.client = createSQLClient({
        url: process.env.DOSQL_URL ?? 'http://localhost:8787',
        token: process.env.DOSQL_TOKEN,
        timeout: 30000,
        retry: {
          maxRetries: 3,
          baseDelayMs: 100,
          maxDelayMs: 5000,
        },
      });
    }

    this.lastUsed = now;
    return this.client;
  }

  async close(): Promise<void> {
    if (this.client) {
      await this.client.close();
      this.client = null;
    }
  }
}

export const pool = ConnectionPool.getInstance();
```

### Query Caching

Implement caching for frequently accessed data:

```typescript
// src/lib/server/cache.ts
import type { RequestEvent } from '@sveltejs/kit';

interface CacheEntry<T> {
  data: T;
  expiresAt: number;
}

const memoryCache = new Map<string, CacheEntry<unknown>>();

export async function cached<T>(
  key: string,
  fn: () => Promise<T>,
  ttlSeconds = 60,
  event?: RequestEvent
): Promise<T> {
  // Try Cloudflare KV first (if available)
  const kv = event?.platform?.env?.KV;
  if (kv) {
    const cached = await kv.get<T>(key, 'json');
    if (cached !== null) {
      return cached;
    }

    const data = await fn();
    await kv.put(key, JSON.stringify(data), { expirationTtl: ttlSeconds });
    return data;
  }

  // Fallback to memory cache
  const now = Date.now();
  const entry = memoryCache.get(key) as CacheEntry<T> | undefined;

  if (entry && entry.expiresAt > now) {
    return entry.data;
  }

  const data = await fn();
  memoryCache.set(key, {
    data,
    expiresAt: now + ttlSeconds * 1000,
  });

  return data;
}

export function invalidate(pattern: string): void {
  for (const key of memoryCache.keys()) {
    if (key.startsWith(pattern) || key.includes(pattern)) {
      memoryCache.delete(key);
    }
  }
}
```

Using cache in load functions:

```typescript
// src/routes/products/+page.server.ts
import type { PageServerLoad } from './$types';
import { cached } from '$lib/server/cache';

export const load: PageServerLoad = async ({ locals, url, ...event }) => {
  const category = url.searchParams.get('category');
  const cacheKey = `products:${category ?? 'all'}`;

  const products = await cached(
    cacheKey,
    async () => {
      let query = 'SELECT * FROM products WHERE active = 1';
      const params: string[] = [];

      if (category) {
        query += ' AND category = ?';
        params.push(category);
      }

      query += ' ORDER BY name LIMIT 100';

      const result = await locals.db.query(query, params);
      return result.rows;
    },
    300, // Cache for 5 minutes
    event
  );

  return { products };
};
```

### Prefetching Strategies

Use SvelteKit's prefetching for better UX:

```svelte
<!-- src/routes/+layout.svelte -->
<script>
  import { afterNavigate } from '$app/navigation';
  import { prefetch } from '$app/navigation';

  // Prefetch common routes on hover
  function handleMouseEnter(event: MouseEvent) {
    const target = event.target as HTMLElement;
    const link = target.closest('a');
    if (link?.href) {
      prefetch(link.href);
    }
  }

  // Prefetch likely next pages after navigation
  afterNavigate(({ to }) => {
    if (to?.route.id === '/') {
      // User is on home, likely to visit dashboard
      prefetch('/dashboard');
    }
    if (to?.route.id?.startsWith('/users')) {
      // User is browsing users, prefetch user list
      prefetch('/users');
    }
  });
</script>

<nav on:mouseenter={handleMouseEnter}>
  <a href="/">Home</a>
  <a href="/dashboard">Dashboard</a>
  <a href="/users">Users</a>
  <a href="/settings">Settings</a>
</nav>

<slot />
```

Batch data loading with dependencies:

```typescript
// src/routes/users/[id]/+page.server.ts
import type { PageServerLoad } from './$types';
import { error } from '@sveltejs/kit';

export const load: PageServerLoad = async ({ locals, params, depends }) => {
  // Register dependency for invalidation
  depends(`user:${params.id}`);

  const [userResult, postsResult, statsResult] = await Promise.all([
    locals.db.query('SELECT * FROM users WHERE id = ?', [params.id]),
    locals.db.query(
      'SELECT id, title, published_at FROM posts WHERE author_id = ? ORDER BY published_at DESC LIMIT 10',
      [params.id]
    ),
    locals.db.query(
      `SELECT
        (SELECT COUNT(*) FROM posts WHERE author_id = ?) as post_count,
        (SELECT COUNT(*) FROM comments WHERE author_id = ?) as comment_count`,
      [params.id, params.id]
    ),
  ]);

  if (userResult.rows.length === 0) {
    error(404, 'User not found');
  }

  return {
    user: userResult.rows[0],
    recentPosts: postsResult.rows,
    stats: statsResult.rows[0],
  };
};
```

Invalidate cached data when needed:

```typescript
// src/routes/users/[id]/+page.server.ts
import type { Actions } from './$types';
import { invalidateAll } from '$app/navigation';

export const actions: Actions = {
  updateProfile: async ({ locals, params, request }) => {
    const formData = await request.formData();
    // ... update user ...

    // Client-side: call invalidateAll() or invalidate('user:id')
    return { success: true };
  },
};
```

```svelte
<!-- In the component -->
<script lang="ts">
  import { invalidate } from '$app/navigation';
  import { enhance } from '$app/forms';

  export let data;
</script>

<form
  method="POST"
  action="?/updateProfile"
  use:enhance={() => {
    return async ({ result, update }) => {
      if (result.type === 'success') {
        // Invalidate this user's data
        await invalidate(`user:${data.user.id}`);
      }
      await update();
    };
  }}
>
  <!-- form fields -->
</form>
```

---

## Summary

This guide covered the essential patterns for integrating DoSQL with SvelteKit:

1. **Setup**: Install `sql.do`, configure environment variables, create a database client module
2. **Load Functions**: Use `+page.server.ts` for server-side data loading with type safety
3. **Form Actions**: Handle mutations with progressive enhancement and optimistic updates
4. **API Routes**: Build RESTful endpoints with proper validation and error handling
5. **Edge Deployment**: Configure the Cloudflare adapter and access platform bindings
6. **Type Safety**: Define database types and use typed repositories
7. **Real-Time**: Use CDC and Server-Sent Events for live updates
8. **Performance**: Implement connection pooling, caching, and prefetching

For more information, see:
- [DoSQL Client SDK Documentation](../../packages/sql.do/README.md)
- [Performance Tuning Guide](../PERFORMANCE_TUNING.md)
- [Error Codes Reference](../ERROR_CODES.md)
