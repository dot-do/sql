# SvelteKit Integration Guide

A comprehensive guide for using DoSQL with SvelteKit applications, covering server load functions, form actions, API routes, and real-time features on Cloudflare Workers.

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Server Load Functions](#server-load-functions)
- [Form Actions](#form-actions)
- [API Routes](#api-routes)
- [Real-Time Updates](#real-time-updates)
- [Multi-Tenancy](#multi-tenancy)
- [Authentication](#authentication)
- [Type Safety](#type-safety)
- [Testing](#testing)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)

---

## Overview

### Why DoSQL + SvelteKit

SvelteKit and DoSQL are an excellent combination for building full-stack applications on Cloudflare Workers:

| Feature | Benefit |
|---------|---------|
| **Edge-Native** | Both SvelteKit and DoSQL run on Cloudflare Workers for minimal latency |
| **Server Load Functions** | Direct database queries in `+page.server.ts` with zero API overhead |
| **Form Actions** | Type-safe mutations with built-in progressive enhancement |
| **Streaming** | Stream database results with SvelteKit's streaming support |
| **Real-Time** | WebSocket and SSE support for live database subscriptions |

### Architecture

```
+------------------------------------------------------------------+
|                   SVELTEKIT + DOSQL ARCHITECTURE                  |
+------------------------------------------------------------------+

                         Browser Request
                              |
                              v
+------------------------------------------------------------------+
|                    Cloudflare Worker                              |
|  +------------------------------------------------------------+  |
|  |                   SvelteKit Server                          |  |
|  |  +------------------+  +------------------+                 |  |
|  |  |  Load Functions  |  |   Form Actions   |                 |  |
|  |  |  (+page.server)  |  |  (+page.server)  |                 |  |
|  |  +--------+---------+  +--------+---------+                 |  |
|  |           |                     |                           |  |
|  |           +----------+----------+                           |  |
|  |                      |                                      |  |
|  |                      v                                      |  |
|  |            +------------------+                             |  |
|  |            |   getDB(platform)|                             |  |
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

**Data flow:**

1. Browser requests a page
2. SvelteKit load function runs on Cloudflare Worker
3. Load function accesses DoSQL via Durable Object binding
4. DoSQL executes queries against SQLite
5. SvelteKit renders the page with data
6. Browser receives fully-rendered HTML

---

## Setup

### Installation

```bash
# Create a new SvelteKit project
npx sv create my-sveltekit-app
cd my-sveltekit-app

# When prompted, select:
# - SvelteKit minimal
# - TypeScript
# - Cloudflare adapter (or add it manually)

# Install DoSQL
npm install @dotdo/dosql

# Install dev dependencies
npm install -D @cloudflare/workers-types wrangler
```

If you did not select the Cloudflare adapter during setup:

```bash
npm install @sveltejs/adapter-cloudflare
```

### Project Structure

```
my-sveltekit-app/
├── .do/
│   └── migrations/
│       ├── 001_create_users.sql
│       └── 002_create_posts.sql
├── src/
│   ├── lib/
│   │   ├── server/
│   │   │   ├── db.ts           # Database utilities (server only)
│   │   │   └── auth.ts         # Auth utilities (server only)
│   │   └── types.ts            # Shared type definitions
│   ├── routes/
│   │   ├── +layout.server.ts   # Root layout server load
│   │   ├── +layout.svelte
│   │   ├── +page.svelte
│   │   ├── +page.server.ts
│   │   ├── users/
│   │   │   ├── +page.svelte
│   │   │   ├── +page.server.ts
│   │   │   └── [id]/
│   │   │       ├── +page.svelte
│   │   │       └── +page.server.ts
│   │   └── api/
│   │       └── users/
│   │           └── +server.ts  # API endpoint
│   ├── app.d.ts                # Type declarations
│   └── hooks.server.ts         # Server hooks
├── wrangler.toml
├── svelte.config.js
├── vite.config.ts
└── package.json
```

### Environment Configuration

Update `src/app.d.ts` with your environment bindings:

```typescript
// src/app.d.ts
import type { DatabaseClient } from '$lib/server/db';

declare global {
  namespace App {
    interface Platform {
      env: {
        DOSQL_DB: DurableObjectNamespace;
        DATA_BUCKET?: R2Bucket;
      };
      context: ExecutionContext;
      caches: CacheStorage;
    }

    interface Locals {
      db: DatabaseClient;
      tenantId: string;
      user?: {
        id: number;
        name: string;
        email: string;
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

### Worker Bindings

Configure `wrangler.toml`:

```toml
name = "my-sveltekit-app"
compatibility_date = "2024-01-01"
compatibility_flags = ["nodejs_compat"]

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
bucket_name = "sveltekit-data"
```

### Database Server Module

Create `src/lib/server/db.ts`:

```typescript
// src/lib/server/db.ts
import { DB, type Database } from '@dotdo/dosql';

export interface DatabaseClient {
  query<T>(sql: string, params?: unknown[]): Promise<T[]>;
  queryOne<T>(sql: string, params?: unknown[]): Promise<T | null>;
  run(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowId: number }>;
  transaction<T>(fn: (tx: TransactionClient) => Promise<T>): Promise<T>;
}

export interface TransactionClient {
  query<T>(sql: string, params?: unknown[]): Promise<T[]>;
  queryOne<T>(sql: string, params?: unknown[]): Promise<T | null>;
  run(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowId: number }>;
}

export function getDB(platform: App.Platform, tenantId: string = 'default'): DatabaseClient {
  const id = platform.env.DOSQL_DB.idFromName(tenantId);
  const stub = platform.env.DOSQL_DB.get(id);

  return createDBClient(stub);
}

function createDBClient(stub: DurableObjectStub): DatabaseClient {
  return {
    async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
      const response = await stub.fetch('http://internal/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params }),
      });

      if (!response.ok) {
        const error = await response.json() as { error: string };
        throw new Error(error.error);
      }

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

      if (!response.ok) {
        const error = await response.json() as { error: string };
        throw new Error(error.error);
      }

      return response.json() as Promise<{ rowsAffected: number; lastInsertRowId: number }>;
    },

    async transaction<T>(fn: (tx: TransactionClient) => Promise<T>): Promise<T> {
      const operations: Array<{ type: 'query' | 'run'; sql: string; params?: unknown[] }> = [];
      const txClient: TransactionClient = {
        async query<R>(sql: string, params?: unknown[]): Promise<R[]> {
          operations.push({ type: 'query', sql, params });
          return [] as R[];
        },
        async queryOne<R>(sql: string, params?: unknown[]): Promise<R | null> {
          operations.push({ type: 'query', sql, params });
          return null;
        },
        async run(sql: string, params?: unknown[]) {
          operations.push({ type: 'run', sql, params });
          return { rowsAffected: 0, lastInsertRowId: 0 };
        },
      };

      await fn(txClient);

      const response = await stub.fetch('http://internal/transaction', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ operations }),
      });

      if (!response.ok) {
        const error = await response.json() as { error: string };
        throw new Error(error.error);
      }

      return response.json() as Promise<T>;
    },
  };
}

// Export the Durable Object class for wrangler
export class DoSQLDatabase implements DurableObject {
  private db: Database | null = null;

  constructor(
    private state: DurableObjectState,
    private env: App.Platform['env']
  ) {}

  private async getDB(): Promise<Database> {
    if (!this.db) {
      this.db = await DB('sveltekit-app', {
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
        const { operations } = await request.json() as {
          operations: Array<{ type: 'query' | 'run'; sql: string; params?: unknown[] }>;
        };
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
```

### Server Hooks

Set up the database in server hooks for easy access:

```typescript
// src/hooks.server.ts
import { getDB } from '$lib/server/db';
import type { Handle } from '@sveltejs/kit';

export const handle: Handle = async ({ event, resolve }) => {
  // Make database available in all server-side code
  if (event.platform) {
    event.locals.db = getDB(event.platform);
    event.locals.tenantId = 'default';
  }

  return resolve(event);
};
```

### SvelteKit Configuration

Update `svelte.config.js` for Cloudflare:

```javascript
// svelte.config.js
import adapter from '@sveltejs/adapter-cloudflare';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: vitePreprocess(),

  kit: {
    adapter: adapter({
      routes: {
        include: ['/*'],
        exclude: ['<all>'],
      },
      platformProxy: {
        configPath: 'wrangler.toml',
        persist: { path: '.wrangler/state/v3' },
      },
    }),
  },
};

export default config;
```

---

## Server Load Functions

Server load functions run on the server and can directly query the database.

### Basic Data Loading

```typescript
// src/routes/users/+page.server.ts
import type { PageServerLoad } from './$types';

interface User {
  id: number;
  name: string;
  email: string;
  created_at: string;
}

export const load: PageServerLoad = async ({ locals }) => {
  const users = await locals.db.query<User>(`
    SELECT id, name, email, created_at
    FROM users
    ORDER BY created_at DESC
    LIMIT 50
  `);

  return { users };
};
```

```svelte
<!-- src/routes/users/+page.svelte -->
<script lang="ts">
  import type { PageData } from './$types';

  let { data }: { data: PageData } = $props();
</script>

<h1>Users</h1>

<ul>
  {#each data.users as user (user.id)}
    <li>
      <a href="/users/{user.id}">{user.name}</a> - {user.email}
    </li>
  {/each}
</ul>
```

### Loading with Parameters

```typescript
// src/routes/users/[id]/+page.server.ts
import { error } from '@sveltejs/kit';
import type { PageServerLoad } from './$types';

interface User {
  id: number;
  name: string;
  email: string;
  bio: string | null;
  created_at: string;
}

interface Post {
  id: number;
  title: string;
  created_at: string;
}

export const load: PageServerLoad = async ({ params, locals }) => {
  const userId = parseInt(params.id, 10);

  if (isNaN(userId)) {
    error(400, 'Invalid user ID');
  }

  const user = await locals.db.queryOne<User>(
    'SELECT * FROM users WHERE id = ?',
    [userId]
  );

  if (!user) {
    error(404, 'User not found');
  }

  const posts = await locals.db.query<Post>(
    `SELECT id, title, created_at
     FROM posts
     WHERE user_id = ?
     ORDER BY created_at DESC
     LIMIT 10`,
    [userId]
  );

  return { user, posts };
};
```

```svelte
<!-- src/routes/users/[id]/+page.svelte -->
<script lang="ts">
  import type { PageData } from './$types';

  let { data }: { data: PageData } = $props();
</script>

<article>
  <h1>{data.user.name}</h1>
  <p>{data.user.email}</p>
  {#if data.user.bio}
    <p>{data.user.bio}</p>
  {/if}
  <p>Joined: {new Date(data.user.created_at).toLocaleDateString()}</p>

  <h2>Recent Posts</h2>
  {#if data.posts.length > 0}
    <ul>
      {#each data.posts as post (post.id)}
        <li>
          <a href="/posts/{post.id}">{post.title}</a>
        </li>
      {/each}
    </ul>
  {:else}
    <p>No posts yet.</p>
  {/if}
</article>
```

### Parallel Data Loading

Fetch multiple data sources in parallel for better performance:

```typescript
// src/routes/dashboard/+page.server.ts
import type { PageServerLoad } from './$types';

interface Stats {
  totalUsers: number;
  totalPosts: number;
  recentActivity: number;
}

interface RecentUser {
  id: number;
  name: string;
  created_at: string;
}

interface PopularPost {
  id: number;
  title: string;
  likes: number;
}

export const load: PageServerLoad = async ({ locals }) => {
  // Parallel data fetching for better performance
  const [stats, recentUsers, popularPosts] = await Promise.all([
    locals.db.queryOne<Stats>(`
      SELECT
        (SELECT COUNT(*) FROM users) as totalUsers,
        (SELECT COUNT(*) FROM posts) as totalPosts,
        (SELECT COUNT(*) FROM activity WHERE created_at > datetime('now', '-24 hours')) as recentActivity
    `),
    locals.db.query<RecentUser>(`
      SELECT id, name, created_at
      FROM users
      ORDER BY created_at DESC
      LIMIT 5
    `),
    locals.db.query<PopularPost>(`
      SELECT p.id, p.title, COUNT(l.id) as likes
      FROM posts p
      LEFT JOIN likes l ON l.post_id = p.id
      GROUP BY p.id
      ORDER BY likes DESC
      LIMIT 5
    `),
  ]);

  return {
    stats: stats ?? { totalUsers: 0, totalPosts: 0, recentActivity: 0 },
    recentUsers,
    popularPosts,
  };
};
```

### Pagination

```typescript
// src/routes/posts/+page.server.ts
import type { PageServerLoad } from './$types';

interface Post {
  id: number;
  title: string;
  excerpt: string;
  author_name: string;
  created_at: string;
}

const PAGE_SIZE = 20;

export const load: PageServerLoad = async ({ url, locals }) => {
  const page = Math.max(1, parseInt(url.searchParams.get('page') || '1', 10));
  const offset = (page - 1) * PAGE_SIZE;

  const [posts, countResult] = await Promise.all([
    locals.db.query<Post>(`
      SELECT p.id, p.title, SUBSTR(p.content, 1, 200) as excerpt,
             u.name as author_name, p.created_at
      FROM posts p
      JOIN users u ON p.user_id = u.id
      WHERE p.published = 1
      ORDER BY p.created_at DESC
      LIMIT ? OFFSET ?
    `, [PAGE_SIZE, offset]),
    locals.db.queryOne<{ total: number }>(
      'SELECT COUNT(*) as total FROM posts WHERE published = 1'
    ),
  ]);

  const total = countResult?.total ?? 0;
  const totalPages = Math.ceil(total / PAGE_SIZE);

  return {
    posts,
    pagination: {
      page,
      pageSize: PAGE_SIZE,
      total,
      totalPages,
      hasNext: page < totalPages,
      hasPrev: page > 1,
    },
  };
};
```

```svelte
<!-- src/routes/posts/+page.svelte -->
<script lang="ts">
  import type { PageData } from './$types';

  let { data }: { data: PageData } = $props();
</script>

<h1>Posts</h1>

<div class="posts">
  {#each data.posts as post (post.id)}
    <article>
      <h2><a href="/posts/{post.id}">{post.title}</a></h2>
      <p>{post.excerpt}...</p>
      <footer>
        By {post.author_name} on {new Date(post.created_at).toLocaleDateString()}
      </footer>
    </article>
  {/each}
</div>

<nav class="pagination">
  {#if data.pagination.hasPrev}
    <a href="?page={data.pagination.page - 1}">Previous</a>
  {/if}

  <span>Page {data.pagination.page} of {data.pagination.totalPages}</span>

  {#if data.pagination.hasNext}
    <a href="?page={data.pagination.page + 1}">Next</a>
  {/if}
</nav>
```

### Search with Query Parameters

```typescript
// src/routes/search/+page.server.ts
import type { PageServerLoad } from './$types';

interface SearchResult {
  id: number;
  title: string;
  type: 'user' | 'post';
  snippet: string;
}

export const load: PageServerLoad = async ({ url, locals }) => {
  const query = url.searchParams.get('q')?.trim();

  if (!query || query.length < 2) {
    return { results: [], query: query ?? '' };
  }

  const searchPattern = `%${query}%`;

  const results = await locals.db.query<SearchResult>(`
    SELECT id, name as title, 'user' as type, email as snippet
    FROM users
    WHERE name LIKE ? OR email LIKE ?
    UNION ALL
    SELECT id, title, 'post' as type, SUBSTR(content, 1, 100) as snippet
    FROM posts
    WHERE title LIKE ? OR content LIKE ?
    LIMIT 50
  `, [searchPattern, searchPattern, searchPattern, searchPattern]);

  return { results, query };
};
```

---

## Form Actions

Form actions provide type-safe server-side mutations with progressive enhancement.

### Basic Form Action

```typescript
// src/routes/users/new/+page.server.ts
import { fail, redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';

export const load: PageServerLoad = async () => {
  return {};
};

export const actions: Actions = {
  default: async ({ request, locals }) => {
    const formData = await request.formData();

    const name = formData.get('name')?.toString().trim();
    const email = formData.get('email')?.toString().trim();

    // Validation
    const errors: Record<string, string> = {};

    if (!name || name.length < 2) {
      errors.name = 'Name must be at least 2 characters';
    }

    if (!email || !email.includes('@')) {
      errors.email = 'Valid email is required';
    }

    if (Object.keys(errors).length > 0) {
      return fail(400, { errors, name, email });
    }

    try {
      const result = await locals.db.run(
        'INSERT INTO users (name, email) VALUES (?, ?)',
        [name, email]
      );

      redirect(303, `/users/${result.lastInsertRowId}`);
    } catch (error) {
      if ((error as Error).message.includes('UNIQUE constraint')) {
        return fail(400, {
          errors: { email: 'Email already exists' },
          name,
          email,
        });
      }
      throw error;
    }
  },
};
```

```svelte
<!-- src/routes/users/new/+page.svelte -->
<script lang="ts">
  import { enhance } from '$app/forms';
  import type { ActionData } from './$types';

  let { form }: { form: ActionData } = $props();
</script>

<h1>Create User</h1>

<form method="POST" use:enhance>
  <div class="field">
    <label for="name">Name</label>
    <input
      type="text"
      id="name"
      name="name"
      value={form?.name ?? ''}
      class:error={form?.errors?.name}
      required
    />
    {#if form?.errors?.name}
      <span class="error-message">{form.errors.name}</span>
    {/if}
  </div>

  <div class="field">
    <label for="email">Email</label>
    <input
      type="email"
      id="email"
      name="email"
      value={form?.email ?? ''}
      class:error={form?.errors?.email}
      required
    />
    {#if form?.errors?.email}
      <span class="error-message">{form.errors.email}</span>
    {/if}
  </div>

  <button type="submit">Create User</button>
</form>

<style>
  .error {
    border-color: red;
  }
  .error-message {
    color: red;
    font-size: 0.875rem;
  }
</style>
```

### Multiple Named Actions

```typescript
// src/routes/posts/[id]/+page.server.ts
import { fail, redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';

interface Post {
  id: number;
  title: string;
  content: string;
  published: boolean;
  user_id: number;
}

export const load: PageServerLoad = async ({ params, locals }) => {
  const post = await locals.db.queryOne<Post>(
    'SELECT * FROM posts WHERE id = ?',
    [params.id]
  );

  if (!post) {
    redirect(303, '/posts');
  }

  return { post };
};

export const actions: Actions = {
  update: async ({ request, params, locals }) => {
    const formData = await request.formData();

    const title = formData.get('title')?.toString().trim();
    const content = formData.get('content')?.toString().trim();
    const published = formData.get('published') === 'true';

    if (!title || title.length < 1) {
      return fail(400, { error: 'Title is required', title, content });
    }

    await locals.db.run(
      `UPDATE posts
       SET title = ?, content = ?, published = ?, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [title, content, published, params.id]
    );

    return { success: true };
  },

  delete: async ({ params, locals }) => {
    await locals.db.transaction(async (tx) => {
      await tx.run('DELETE FROM comments WHERE post_id = ?', [params.id]);
      await tx.run('DELETE FROM posts WHERE id = ?', [params.id]);
    });

    redirect(303, '/posts');
  },

  publish: async ({ params, locals }) => {
    await locals.db.run(
      'UPDATE posts SET published = 1, published_at = CURRENT_TIMESTAMP WHERE id = ?',
      [params.id]
    );

    return { success: true, message: 'Post published' };
  },

  unpublish: async ({ params, locals }) => {
    await locals.db.run(
      'UPDATE posts SET published = 0, published_at = NULL WHERE id = ?',
      [params.id]
    );

    return { success: true, message: 'Post unpublished' };
  },
};
```

```svelte
<!-- src/routes/posts/[id]/+page.svelte -->
<script lang="ts">
  import { enhance } from '$app/forms';
  import type { PageData, ActionData } from './$types';

  let { data, form }: { data: PageData; form: ActionData } = $props();
  let isDeleting = $state(false);
</script>

<article>
  <h1>Edit Post</h1>

  {#if form?.success}
    <div class="success">{form.message ?? 'Changes saved'}</div>
  {/if}

  {#if form?.error}
    <div class="error">{form.error}</div>
  {/if}

  <form method="POST" action="?/update" use:enhance>
    <div class="field">
      <label for="title">Title</label>
      <input
        type="text"
        id="title"
        name="title"
        value={form?.title ?? data.post.title}
        required
      />
    </div>

    <div class="field">
      <label for="content">Content</label>
      <textarea
        id="content"
        name="content"
        rows="10"
      >{form?.content ?? data.post.content}</textarea>
    </div>

    <div class="field">
      <label>
        <input
          type="checkbox"
          name="published"
          value="true"
          checked={data.post.published}
        />
        Published
      </label>
    </div>

    <button type="submit">Save Changes</button>
  </form>

  <hr />

  <div class="actions">
    {#if data.post.published}
      <form method="POST" action="?/unpublish" use:enhance>
        <button type="submit">Unpublish</button>
      </form>
    {:else}
      <form method="POST" action="?/publish" use:enhance>
        <button type="submit">Publish</button>
      </form>
    {/if}

    <form
      method="POST"
      action="?/delete"
      use:enhance={() => {
        if (!confirm('Are you sure you want to delete this post?')) {
          return () => {};
        }
        isDeleting = true;
        return async ({ update }) => {
          await update();
          isDeleting = false;
        };
      }}
    >
      <button type="submit" class="danger" disabled={isDeleting}>
        {isDeleting ? 'Deleting...' : 'Delete Post'}
      </button>
    </form>
  </div>
</article>
```

### Transactions in Actions

```typescript
// src/routes/orders/+page.server.ts
import { fail, redirect } from '@sveltejs/kit';
import type { Actions } from './$types';

interface CartItem {
  product_id: number;
  quantity: number;
  price: number;
}

export const actions: Actions = {
  checkout: async ({ request, locals }) => {
    const formData = await request.formData();
    const userId = formData.get('user_id')?.toString();
    const cartJson = formData.get('cart')?.toString();

    if (!userId || !cartJson) {
      return fail(400, { error: 'Invalid request' });
    }

    const cart: CartItem[] = JSON.parse(cartJson);

    if (cart.length === 0) {
      return fail(400, { error: 'Cart is empty' });
    }

    try {
      const total = cart.reduce((sum, item) => sum + item.price * item.quantity, 0);

      // Transaction ensures all-or-nothing
      const orderId = await locals.db.transaction(async (tx) => {
        // Create order
        const orderResult = await tx.run(
          `INSERT INTO orders (user_id, total, status, created_at)
           VALUES (?, ?, 'pending', CURRENT_TIMESTAMP)`,
          [userId, total]
        );

        const orderId = orderResult.lastInsertRowId;

        // Add order items and update inventory
        for (const item of cart) {
          // Check stock
          const product = await tx.queryOne<{ stock: number }>(
            'SELECT stock FROM products WHERE id = ?',
            [item.product_id]
          );

          if (!product || product.stock < item.quantity) {
            throw new Error(`Insufficient stock for product ${item.product_id}`);
          }

          // Add order item
          await tx.run(
            `INSERT INTO order_items (order_id, product_id, quantity, price)
             VALUES (?, ?, ?, ?)`,
            [orderId, item.product_id, item.quantity, item.price]
          );

          // Decrement stock
          await tx.run(
            'UPDATE products SET stock = stock - ? WHERE id = ?',
            [item.quantity, item.product_id]
          );
        }

        return orderId;
      });

      redirect(303, `/orders/${orderId}/confirmation`);
    } catch (error) {
      if (error instanceof Response) throw error; // Re-throw redirects
      return fail(400, { error: (error as Error).message });
    }
  },
};
```

### Form Enhancement with Loading States

```svelte
<!-- src/routes/comments/+page.svelte -->
<script lang="ts">
  import { enhance } from '$app/forms';
  import type { PageData, ActionData } from './$types';

  let { data, form }: { data: PageData; form: ActionData } = $props();
  let isSubmitting = $state(false);
</script>

<section>
  <h2>Comments ({data.comments.length})</h2>

  {#each data.comments as comment (comment.id)}
    <div class="comment">
      <strong>{comment.author_name}</strong>
      <p>{comment.content}</p>
      <time>{new Date(comment.created_at).toLocaleString()}</time>
    </div>
  {/each}

  <form
    method="POST"
    action="?/addComment"
    use:enhance={() => {
      isSubmitting = true;
      return async ({ update, result }) => {
        await update();
        isSubmitting = false;
        if (result.type === 'success') {
          // Clear the form on success
          const formElement = document.querySelector('form') as HTMLFormElement;
          formElement?.reset();
        }
      };
    }}
  >
    <input type="hidden" name="post_id" value={data.postId} />

    <div class="field">
      <label for="content">Your comment</label>
      <textarea
        id="content"
        name="content"
        rows="3"
        required
        disabled={isSubmitting}
      ></textarea>
    </div>

    {#if form?.error}
      <p class="error">{form.error}</p>
    {/if}

    <button type="submit" disabled={isSubmitting}>
      {isSubmitting ? 'Posting...' : 'Post Comment'}
    </button>
  </form>
</section>
```

---

## API Routes

Create standalone API endpoints with `+server.ts` files.

### REST API Endpoint

```typescript
// src/routes/api/users/+server.ts
import { json, error } from '@sveltejs/kit';
import type { RequestHandler } from './$types';

interface User {
  id: number;
  name: string;
  email: string;
  created_at: string;
}

// GET /api/users
export const GET: RequestHandler = async ({ url, locals }) => {
  const limit = Math.min(100, Math.max(1, parseInt(url.searchParams.get('limit') || '50', 10)));
  const offset = Math.max(0, parseInt(url.searchParams.get('offset') || '0', 10));
  const search = url.searchParams.get('q');

  let sql = 'SELECT id, name, email, created_at FROM users';
  const params: unknown[] = [];

  if (search) {
    sql += ' WHERE name LIKE ? OR email LIKE ?';
    params.push(`%${search}%`, `%${search}%`);
  }

  sql += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);

  const users = await locals.db.query<User>(sql, params);

  const countResult = await locals.db.queryOne<{ total: number }>(
    'SELECT COUNT(*) as total FROM users' + (search ? ' WHERE name LIKE ? OR email LIKE ?' : ''),
    search ? [`%${search}%`, `%${search}%`] : []
  );

  return json({
    users,
    total: countResult?.total ?? 0,
    limit,
    offset,
  });
};

// POST /api/users
export const POST: RequestHandler = async ({ request, locals }) => {
  const body = await request.json() as { name: string; email: string };

  if (!body.name || !body.email) {
    error(400, 'Name and email are required');
  }

  try {
    const result = await locals.db.run(
      'INSERT INTO users (name, email) VALUES (?, ?)',
      [body.name, body.email]
    );

    return json(
      { id: result.lastInsertRowId, ...body },
      { status: 201 }
    );
  } catch (err) {
    if ((err as Error).message.includes('UNIQUE constraint')) {
      error(409, 'Email already exists');
    }
    throw err;
  }
};
```

### Dynamic API Route

```typescript
// src/routes/api/users/[id]/+server.ts
import { json, error } from '@sveltejs/kit';
import type { RequestHandler } from './$types';

interface User {
  id: number;
  name: string;
  email: string;
  bio: string | null;
  created_at: string;
}

// GET /api/users/:id
export const GET: RequestHandler = async ({ params, locals }) => {
  const user = await locals.db.queryOne<User>(
    'SELECT * FROM users WHERE id = ?',
    [params.id]
  );

  if (!user) {
    error(404, 'User not found');
  }

  return json(user);
};

// PATCH /api/users/:id
export const PATCH: RequestHandler = async ({ params, request, locals }) => {
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
    error(400, 'No fields to update');
  }

  values.push(params.id);

  const result = await locals.db.run(
    `UPDATE users SET ${updates.join(', ')}, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
    values
  );

  if (result.rowsAffected === 0) {
    error(404, 'User not found');
  }

  return json({ success: true });
};

// DELETE /api/users/:id
export const DELETE: RequestHandler = async ({ params, locals }) => {
  const result = await locals.db.run(
    'DELETE FROM users WHERE id = ?',
    [params.id]
  );

  if (result.rowsAffected === 0) {
    error(404, 'User not found');
  }

  return json({ success: true });
};
```

### Streaming Response (CSV Export)

```typescript
// src/routes/api/export/users/+server.ts
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ locals }) => {
  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      // Write CSV header
      controller.enqueue(encoder.encode('id,name,email,created_at\n'));

      // Stream users
      const users = await locals.db.query<{
        id: number;
        name: string;
        email: string;
        created_at: string;
      }>('SELECT * FROM users ORDER BY id');

      for (const user of users) {
        const row = `${user.id},"${escapeCSV(user.name)}","${escapeCSV(user.email)}",${user.created_at}\n`;
        controller.enqueue(encoder.encode(row));
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

function escapeCSV(str: string): string {
  return str.replace(/"/g, '""');
}
```

---

## Real-Time Updates

### Server-Sent Events for CDC

```typescript
// src/routes/api/events/users/+server.ts
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ platform, request }) => {
  if (!platform) {
    return new Response('Platform not available', { status: 500 });
  }

  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      // Get database stub
      const id = platform.env.DOSQL_DB.idFromName('default');
      const stub = platform.env.DOSQL_DB.get(id);

      // Subscribe to CDC
      const response = await stub.fetch('http://internal/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tables: ['users'] }),
      });

      const reader = response.body?.getReader();
      if (!reader) {
        controller.close();
        return;
      }

      // Handle client disconnect
      request.signal.addEventListener('abort', () => {
        reader.cancel();
      });

      // Stream changes to client
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const text = new TextDecoder().decode(value);
        controller.enqueue(encoder.encode(`data: ${text}\n\n`));
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
};
```

### Client-Side SSE Store

```typescript
// src/lib/stores/realtimeUsers.svelte.ts
import { browser } from '$app/environment';

interface User {
  id: number;
  name: string;
  email: string;
}

interface CDCEvent {
  type: 'insert' | 'update' | 'delete';
  data: User;
}

export function createRealtimeUsers(initialUsers: User[]) {
  let users = $state<User[]>(initialUsers);
  let connected = $state(false);
  let eventSource: EventSource | null = null;

  function connect() {
    if (!browser || eventSource) return;

    eventSource = new EventSource('/api/events/users');

    eventSource.onopen = () => {
      connected = true;
    };

    eventSource.onmessage = (event) => {
      const change: CDCEvent = JSON.parse(event.data);

      switch (change.type) {
        case 'insert':
          users = [change.data, ...users];
          break;
        case 'update':
          users = users.map((u) =>
            u.id === change.data.id ? change.data : u
          );
          break;
        case 'delete':
          users = users.filter((u) => u.id !== change.data.id);
          break;
      }
    };

    eventSource.onerror = () => {
      connected = false;
      eventSource?.close();
      eventSource = null;
      // Reconnect after 5 seconds
      setTimeout(connect, 5000);
    };
  }

  function disconnect() {
    eventSource?.close();
    eventSource = null;
    connected = false;
  }

  return {
    get users() {
      return users;
    },
    get connected() {
      return connected;
    },
    connect,
    disconnect,
  };
}
```

```svelte
<!-- src/routes/users/live/+page.svelte -->
<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { createRealtimeUsers } from '$lib/stores/realtimeUsers.svelte';
  import type { PageData } from './$types';

  let { data }: { data: PageData } = $props();

  const store = createRealtimeUsers(data.users);

  onMount(() => {
    store.connect();
  });

  onDestroy(() => {
    store.disconnect();
  });
</script>

<h1>Live Users</h1>
<p class="status">
  {store.connected ? 'Connected - Updates in real-time' : 'Connecting...'}
</p>

<ul>
  {#each store.users as user (user.id)}
    <li>{user.name} - {user.email}</li>
  {/each}
</ul>
```

---

## Multi-Tenancy

### Tenant-Based Routing

```typescript
// src/hooks.server.ts
import { getDB } from '$lib/server/db';
import type { Handle } from '@sveltejs/kit';

export const handle: Handle = async ({ event, resolve }) => {
  if (!event.platform) {
    return resolve(event);
  }

  // Extract tenant from subdomain, header, or path
  const tenantId = getTenantId(event);

  // Each tenant gets their own isolated database
  event.locals.db = getDB(event.platform, tenantId);
  event.locals.tenantId = tenantId;

  return resolve(event);
};

function getTenantId(event: Parameters<Handle>[0]['event']): string {
  // Option 1: From subdomain (tenant.example.com)
  const host = event.request.headers.get('host') || '';
  const subdomain = host.split('.')[0];
  if (subdomain && subdomain !== 'www' && subdomain !== 'app') {
    return subdomain;
  }

  // Option 2: From header (X-Tenant-ID)
  const headerTenant = event.request.headers.get('x-tenant-id');
  if (headerTenant) {
    return headerTenant;
  }

  // Option 3: From path (/tenant/[slug]/...)
  const pathMatch = event.url.pathname.match(/^\/tenant\/([^/]+)/);
  if (pathMatch) {
    return pathMatch[1];
  }

  // Default tenant
  return 'default';
}
```

### Tenant-Scoped Routes

```typescript
// src/routes/tenant/[tenant]/+layout.server.ts
import { error } from '@sveltejs/kit';
import type { LayoutServerLoad } from './$types';

interface Tenant {
  id: string;
  name: string;
  plan: string;
}

export const load: LayoutServerLoad = async ({ params, locals }) => {
  // Verify tenant exists and user has access
  const tenant = await locals.db.queryOne<Tenant>(
    'SELECT id, name, plan FROM tenants WHERE id = ?',
    [params.tenant]
  );

  if (!tenant) {
    error(404, 'Tenant not found');
  }

  return { tenant };
};
```

---

## Authentication

### Auth Hook Integration

```typescript
// src/hooks.server.ts
import { getDB } from '$lib/server/db';
import { verifySession } from '$lib/server/auth';
import type { Handle } from '@sveltejs/kit';

export const handle: Handle = async ({ event, resolve }) => {
  if (!event.platform) {
    return resolve(event);
  }

  // Initialize database
  event.locals.db = getDB(event.platform);

  // Verify session from cookie
  const sessionToken = event.cookies.get('session');
  if (sessionToken) {
    const user = await verifySession(event.locals.db, sessionToken);
    if (user) {
      event.locals.user = user;
    }
  }

  return resolve(event);
};
```

```typescript
// src/lib/server/auth.ts
import type { DatabaseClient } from './db';

interface User {
  id: number;
  name: string;
  email: string;
}

export async function verifySession(
  db: DatabaseClient,
  token: string
): Promise<User | null> {
  const session = await db.queryOne<{ user_id: number; expires_at: string }>(
    'SELECT user_id, expires_at FROM sessions WHERE token = ?',
    [token]
  );

  if (!session || new Date(session.expires_at) < new Date()) {
    return null;
  }

  const user = await db.queryOne<User>(
    'SELECT id, name, email FROM users WHERE id = ?',
    [session.user_id]
  );

  return user;
}

export async function createSession(
  db: DatabaseClient,
  userId: number
): Promise<string> {
  const token = crypto.randomUUID();
  const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000); // 7 days

  await db.run(
    `INSERT INTO sessions (token, user_id, expires_at, created_at)
     VALUES (?, ?, ?, CURRENT_TIMESTAMP)`,
    [token, userId, expiresAt.toISOString()]
  );

  return token;
}

export async function hashPassword(password: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(password);
  const hash = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}
```

### Protected Routes

```typescript
// src/routes/dashboard/+page.server.ts
import { redirect } from '@sveltejs/kit';
import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ locals }) => {
  if (!locals.user) {
    redirect(303, '/login');
  }

  // User is authenticated
  const data = await locals.db.query(
    'SELECT * FROM user_data WHERE user_id = ?',
    [locals.user.id]
  );

  return { user: locals.user, data };
};
```

### Login Action

```typescript
// src/routes/login/+page.server.ts
import { fail, redirect } from '@sveltejs/kit';
import { createSession, hashPassword } from '$lib/server/auth';
import type { Actions } from './$types';

export const actions: Actions = {
  default: async ({ request, cookies, locals }) => {
    const formData = await request.formData();
    const email = formData.get('email')?.toString();
    const password = formData.get('password')?.toString();

    if (!email || !password) {
      return fail(400, { error: 'Email and password are required' });
    }

    const hashedPassword = await hashPassword(password);

    const user = await locals.db.queryOne<{ id: number }>(
      'SELECT id FROM users WHERE email = ? AND password_hash = ?',
      [email, hashedPassword]
    );

    if (!user) {
      return fail(401, { error: 'Invalid email or password' });
    }

    const sessionToken = await createSession(locals.db, user.id);

    cookies.set('session', sessionToken, {
      path: '/',
      httpOnly: true,
      sameSite: 'lax',
      secure: true,
      maxAge: 60 * 60 * 24 * 7, // 7 days
    });

    redirect(303, '/dashboard');
  },
};
```

---

## Type Safety

### Database Type Definitions

```typescript
// src/lib/types.ts
export interface User {
  id: number;
  name: string;
  email: string;
  bio: string | null;
  avatar_url: string | null;
  created_at: string;
  updated_at: string;
}

export interface Post {
  id: number;
  user_id: number;
  title: string;
  content: string;
  published: boolean;
  published_at: string | null;
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

// Joined types for queries with JOINs
export interface PostWithAuthor extends Post {
  author_name: string;
  author_email: string;
}

export interface CommentWithUser extends Comment {
  user_name: string;
  user_avatar_url: string | null;
}
```

### Zod Schema Validation

```typescript
// src/lib/schemas.ts
import { z } from 'zod';

export const createUserSchema = z.object({
  name: z.string().min(2, 'Name must be at least 2 characters').max(100),
  email: z.string().email('Invalid email address'),
  bio: z.string().max(500, 'Bio must be under 500 characters').optional(),
});

export const updateUserSchema = createUserSchema.partial();

export const createPostSchema = z.object({
  title: z.string().min(1, 'Title is required').max(200),
  content: z.string().min(1, 'Content is required'),
  published: z.boolean().default(false),
});

export type CreateUserInput = z.infer<typeof createUserSchema>;
export type UpdateUserInput = z.infer<typeof updateUserSchema>;
export type CreatePostInput = z.infer<typeof createPostSchema>;
```

### Type-Safe Actions with Validation

```typescript
// src/routes/users/new/+page.server.ts
import { fail, redirect } from '@sveltejs/kit';
import { createUserSchema } from '$lib/schemas';
import type { Actions } from './$types';

export const actions: Actions = {
  default: async ({ request, locals }) => {
    const formData = await request.formData();

    const data = {
      name: formData.get('name')?.toString(),
      email: formData.get('email')?.toString(),
      bio: formData.get('bio')?.toString() || undefined,
    };

    const result = createUserSchema.safeParse(data);

    if (!result.success) {
      const errors = result.error.flatten().fieldErrors;
      return fail(400, {
        errors: {
          name: errors.name?.[0],
          email: errors.email?.[0],
          bio: errors.bio?.[0],
        },
        data,
      });
    }

    try {
      const insertResult = await locals.db.run(
        'INSERT INTO users (name, email, bio) VALUES (?, ?, ?)',
        [result.data.name, result.data.email, result.data.bio ?? null]
      );

      redirect(303, `/users/${insertResult.lastInsertRowId}`);
    } catch (error) {
      if (error instanceof Response) throw error;
      if ((error as Error).message.includes('UNIQUE constraint')) {
        return fail(400, {
          errors: { email: 'Email already exists' },
          data,
        });
      }
      throw error;
    }
  },
};
```

---

## Testing

### Unit Testing with Vitest

```typescript
// src/lib/server/db.test.ts
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { getDB, type DatabaseClient } from './db';

describe('DatabaseClient', () => {
  let mockStub: { fetch: ReturnType<typeof vi.fn> };
  let db: DatabaseClient;

  beforeEach(() => {
    mockStub = {
      fetch: vi.fn(),
    };

    const mockPlatform = {
      env: {
        DOSQL_DB: {
          idFromName: vi.fn().mockReturnValue('test-id'),
          get: vi.fn().mockReturnValue(mockStub),
        },
      },
    } as unknown as App.Platform;

    db = getDB(mockPlatform, 'test-tenant');
  });

  it('should query data', async () => {
    mockStub.fetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ rows: [{ id: 1, name: 'Test' }] }),
    });

    const result = await db.query('SELECT * FROM users');

    expect(result).toEqual([{ id: 1, name: 'Test' }]);
    expect(mockStub.fetch).toHaveBeenCalledWith(
      'http://internal/query',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({ sql: 'SELECT * FROM users', params: undefined }),
      })
    );
  });

  it('should handle errors', async () => {
    mockStub.fetch.mockResolvedValue({
      ok: false,
      json: () => Promise.resolve({ error: 'Database error' }),
    });

    await expect(db.query('SELECT * FROM users')).rejects.toThrow('Database error');
  });
});
```

### Integration Testing with Playwright

```typescript
// tests/users.test.ts
import { test, expect } from '@playwright/test';

test.describe('Users', () => {
  test('should list users', async ({ page }) => {
    await page.goto('/users');

    await expect(page.getByRole('heading', { name: 'Users' })).toBeVisible();
    await expect(page.getByRole('list')).toBeVisible();
  });

  test('should create a new user', async ({ page }) => {
    await page.goto('/users/new');

    await page.fill('#name', 'Test User');
    await page.fill('#email', 'test@example.com');
    await page.click('button[type="submit"]');

    // Should redirect to user page
    await expect(page).toHaveURL(/\/users\/\d+/);
    await expect(page.getByText('Test User')).toBeVisible();
  });

  test('should show validation errors', async ({ page }) => {
    await page.goto('/users/new');

    await page.fill('#name', 'A');
    await page.fill('#email', 'invalid-email');
    await page.click('button[type="submit"]');

    await expect(page.getByText('Name must be at least 2 characters')).toBeVisible();
    await expect(page.getByText('Valid email is required')).toBeVisible();
  });
});
```

### Mocking the Database in Tests

```typescript
// tests/helpers/mockDb.ts
import { vi } from 'vitest';
import type { DatabaseClient } from '$lib/server/db';

export function createMockDb(overrides: Partial<DatabaseClient> = {}): DatabaseClient {
  return {
    query: vi.fn().mockResolvedValue([]),
    queryOne: vi.fn().mockResolvedValue(null),
    run: vi.fn().mockResolvedValue({ rowsAffected: 0, lastInsertRowId: 0 }),
    transaction: vi.fn().mockImplementation((fn) => fn({
      query: vi.fn().mockResolvedValue([]),
      queryOne: vi.fn().mockResolvedValue(null),
      run: vi.fn().mockResolvedValue({ rowsAffected: 0, lastInsertRowId: 0 }),
    })),
    ...overrides,
  };
}
```

---

## Deployment

### Build Configuration

```json
// package.json
{
  "scripts": {
    "dev": "vite dev",
    "build": "vite build",
    "preview": "wrangler pages dev .svelte-kit/cloudflare",
    "deploy": "npm run build && wrangler pages deploy .svelte-kit/cloudflare",
    "deploy:staging": "npm run build && wrangler pages deploy .svelte-kit/cloudflare --env staging",
    "deploy:production": "npm run build && wrangler pages deploy .svelte-kit/cloudflare --env production",
    "typecheck": "svelte-kit sync && svelte-check --tsconfig ./tsconfig.json",
    "test": "vitest run",
    "test:e2e": "playwright test"
  }
}
```

### Wrangler Configuration for Production

```toml
# wrangler.toml
name = "my-sveltekit-app"
compatibility_date = "2024-01-01"
compatibility_flags = ["nodejs_compat"]

# Durable Objects
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

# R2 for data
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "sveltekit-data"

# Production environment
[env.production]
name = "my-sveltekit-app-prod"

[[env.production.r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "sveltekit-data-prod"

# Staging environment
[env.staging]
name = "my-sveltekit-app-staging"

[[env.staging.r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "sveltekit-data-staging"
```

### Including Migrations in Build

```typescript
// vite.config.ts
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { copyFileSync, mkdirSync, readdirSync } from 'fs';

export default defineConfig({
  plugins: [
    sveltekit(),
    {
      name: 'copy-migrations',
      buildEnd() {
        // Copy SQL migrations to build output
        const migrationsDir = '.do/migrations';
        const outputDir = '.svelte-kit/cloudflare/.do/migrations';

        try {
          mkdirSync(outputDir, { recursive: true });
          const files = readdirSync(migrationsDir).filter((f) => f.endsWith('.sql'));
          files.forEach((file) => {
            copyFileSync(`${migrationsDir}/${file}`, `${outputDir}/${file}`);
          });
        } catch (error) {
          console.warn('No migrations to copy:', error);
        }
      },
    },
  ],
});
```

### Production Checklist

Before deploying to production, ensure you have:

1. **Environment bindings configured** - DOSQL_DB, DATA_BUCKET in wrangler.toml
2. **Migrations bundled** - SQL files included in build output
3. **Error handling** - Proper error boundaries in layouts
4. **Rate limiting** - Consider adding rate limits for form actions and API routes
5. **CORS headers** - Configure CORS for API endpoints if accessed from other domains
6. **Monitoring** - Set up Cloudflare analytics and logging
7. **Session security** - Secure cookie settings (httpOnly, secure, sameSite)

---

## Troubleshooting

### Common Issues

#### "Platform not available in hooks"

The platform object is only available when running on Cloudflare Workers. During development, use the Cloudflare adapter's platform proxy:

```javascript
// svelte.config.js
adapter: adapter({
  platformProxy: {
    configPath: 'wrangler.toml',
    persist: { path: '.wrangler/state/v3' },
  },
}),
```

#### "Durable Objects require a Workers Paid plan"

Durable Objects require the Workers Paid plan ($5/month). Upgrade at [dash.cloudflare.com](https://dash.cloudflare.com) under Workers & Pages > Plans.

#### "class_name 'DoSQLDatabase' not found in exports"

Ensure the Durable Object class is exported from your entry point:

```typescript
// src/lib/server/db.ts
export class DoSQLDatabase implements DurableObject {
  // ...
}
```

And verify the `class_name` in `wrangler.toml` matches exactly (case-sensitive):

```toml
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"
```

#### "Migration folder not found"

Create the migrations directory and ensure it contains at least one `.sql` file:

```bash
mkdir -p .do/migrations
```

#### "form?.errors is undefined in template"

When using Svelte 5 runes, ensure you use the correct props syntax and optional chaining:

```svelte
<script lang="ts">
  import type { ActionData } from './$types';

  let { form }: { form: ActionData } = $props();
</script>

<!-- Use optional chaining -->
{#if form?.errors?.email}
  <span>{form.errors.email}</span>
{/if}
```

#### Local Development Database Persistence

Data persists between restarts when using the platform proxy with the persist option:

```javascript
platformProxy: {
  persist: { path: '.wrangler/state/v3' },
},
```

To reset the database during development:

```bash
rm -rf .wrangler/state
```

### Performance Tips

1. **Use parallel queries** - Fetch independent data with `Promise.all()`
2. **Limit result sets** - Always use `LIMIT` in queries
3. **Create indexes** - Add indexes for frequently queried columns
4. **Use pagination** - Avoid loading all data at once
5. **Cache where appropriate** - Use SvelteKit's caching for static content

---

## Next Steps

- [Getting Started](../getting-started.md) - DoSQL basics
- [API Reference](../api-reference.md) - Complete API documentation
- [Advanced Features](../advanced.md) - CDC, time travel, branching
- [Architecture](../architecture.md) - Understanding DoSQL internals
