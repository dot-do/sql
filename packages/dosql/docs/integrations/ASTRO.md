# DoSQL + Astro Integration Guide

This guide covers how to integrate DoSQL with Astro applications, including SSR/hybrid rendering, API endpoints, and deployment to Cloudflare Pages with Durable Objects.

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [SSR with DoSQL](#ssr-with-dosql)
- [Hybrid Rendering](#hybrid-rendering)
- [API Endpoints](#api-endpoints)
- [Client-Side Integration](#client-side-integration)
- [Content Collections with DoSQL](#content-collections-with-dosql)
- [Type Safety](#type-safety)
- [Advanced Patterns](#advanced-patterns)
- [Deployment](#deployment)

---

## Overview

### Why DoSQL + Astro

Astro's content-focused architecture pairs excellently with DoSQL's edge-native database:

| Astro Feature | DoSQL Benefit |
|---------------|---------------|
| **SSR/Hybrid Rendering** | Server-side data fetching at the edge with Durable Objects |
| **API Endpoints** | Build RESTful APIs with transactional database access |
| **Island Architecture** | Load interactive components with real-time database sync |
| **Content Collections** | Combine static content with dynamic database queries |
| **Edge Deployment** | Native Cloudflare Workers/Pages integration |

### Architecture

```
+------------------------------------------------------------------+
|                    ASTRO + DOSQL ARCHITECTURE                     |
+------------------------------------------------------------------+

                         Browser Request
                              |
                              v
+------------------------------------------------------------------+
|                    Cloudflare Pages                               |
|  +------------------------------------------------------------+  |
|  |                   Astro SSR                                 |  |
|  |  +------------------+  +------------------+                 |  |
|  |  |  .astro Pages    |  |  API Endpoints   |                 |  |
|  |  |  (SSR/Hybrid)    |  |  (/api/*)        |                 |  |
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
|  |   - posts        |  |     |   - posts        |    |
|  |   - comments     |  |     |   - comments     |    |
|  +------------------+  |     |  +------------------+  |
+------------------------+     +------------------------+
```

### Rendering Modes Comparison

| Mode | Description | DoSQL Access |
|------|-------------|--------------|
| **Static (SSG)** | Build-time rendering | Build-time queries only |
| **Server (SSR)** | On-demand rendering | Full database access |
| **Hybrid** | Per-page control | SSR pages get database access |

---

## Setup

### Installation

```bash
# Create a new Astro project with Cloudflare adapter
npm create astro@latest my-astro-app -- --template minimal
cd my-astro-app

# Install DoSQL and Cloudflare adapter
npm install @dotdo/dosql
npm install @astrojs/cloudflare
npm install -D @cloudflare/workers-types wrangler
```

### Project Structure

```
my-astro-app/
├── .do/
│   └── migrations/
│       ├── 001_create_posts.sql
│       └── 002_create_comments.sql
├── src/
│   ├── lib/
│   │   ├── db.ts           # Database utilities
│   │   └── types.ts        # TypeScript types
│   ├── pages/
│   │   ├── index.astro
│   │   ├── posts/
│   │   │   ├── index.astro
│   │   │   └── [id].astro
│   │   └── api/
│   │       ├── posts.ts
│   │       └── posts/[id].ts
│   ├── components/
│   │   └── PostList.astro
│   └── layouts/
│       └── Layout.astro
├── astro.config.mjs
├── wrangler.toml
├── package.json
└── tsconfig.json
```

### Astro Configuration

Configure Astro for Cloudflare deployment with SSR:

```javascript
// astro.config.mjs
import { defineConfig } from 'astro/config';
import cloudflare from '@astrojs/cloudflare';

export default defineConfig({
  output: 'server', // Enable SSR
  adapter: cloudflare({
    mode: 'directory', // Use directory mode for Pages Functions
    runtime: {
      mode: 'local',
      type: 'pages',
      bindings: {
        // Local development bindings (from wrangler.toml)
        DOSQL_DB: {
          type: 'durable-object-namespace',
          className: 'DoSQLDatabase',
        },
      },
    },
  }),

  vite: {
    build: {
      minify: false, // Easier debugging
    },
    ssr: {
      // Ensure DoSQL is bundled correctly
      external: [],
    },
  },
});
```

### Wrangler Configuration

Create `wrangler.toml`:

```toml
name = "my-astro-app"
compatibility_date = "2024-01-01"
compatibility_flags = ["nodejs_compat"]

# Output directory for Astro build
pages_build_output_dir = "./dist"

# Durable Object bindings
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

# DO migrations
[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

# Optional: R2 for cold storage
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-astro-data"

# Environment variables
[vars]
ENVIRONMENT = "development"
```

### Environment Types

Create `src/env.d.ts`:

```typescript
/// <reference types="astro/client" />

interface Env {
  DOSQL_DB: DurableObjectNamespace;
  DATA_BUCKET?: R2Bucket;
  ENVIRONMENT?: string;
}

type Runtime = import('@astrojs/cloudflare').Runtime<Env>;

declare namespace App {
  interface Locals extends Runtime {}
}
```

### Database Server Module

Create `src/lib/db.ts`:

```typescript
// src/lib/db.ts
import { DB, type Database } from '@dotdo/dosql';
import type { Env } from '../env';

// Type for the database client
export type DBClient = {
  query<T = unknown>(sql: string, params?: unknown[]): Promise<T[]>;
  queryOne<T = unknown>(sql: string, params?: unknown[]): Promise<T | null>;
  run(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowId: number }>;
  transaction<T>(fn: (tx: DBClient) => Promise<T>): Promise<T>;
};

/**
 * Get a database client for the given tenant
 */
export async function getDB(env: Env, tenantId: string = 'default'): Promise<DBClient> {
  // Get Durable Object stub
  const id = env.DOSQL_DB.idFromName(tenantId);
  const stub = env.DOSQL_DB.get(id);

  return createDBClient(stub);
}

/**
 * Create an RPC client that communicates with the Durable Object
 */
function createDBClient(stub: DurableObjectStub): DBClient {
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

    async transaction<T>(fn: (tx: DBClient) => Promise<T>): Promise<T> {
      // For complex transactions, collect operations and send as batch
      const response = await stub.fetch('http://internal/transaction', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'begin' }),
      });

      if (!response.ok) {
        throw new Error('Failed to begin transaction');
      }

      const txClient = createDBClient(stub);
      return fn(txClient);
    },
  };
}

/**
 * DoSQL Durable Object class - export in your worker entry
 */
export class DoSQLDatabase implements DurableObject {
  private db: Database | null = null;

  constructor(
    private state: DurableObjectState,
    private env: Env
  ) {}

  private async getDB(): Promise<Database> {
    if (!this.db) {
      this.db = await DB('astro-app', {
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

### Initial Migration

Create `.do/migrations/001_create_posts.sql`:

```sql
-- Create posts table
CREATE TABLE posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  slug TEXT UNIQUE NOT NULL,
  content TEXT NOT NULL,
  excerpt TEXT,
  published BOOLEAN DEFAULT false,
  author_id INTEGER,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Create index for published posts
CREATE INDEX idx_posts_published ON posts(published, created_at DESC);
CREATE INDEX idx_posts_slug ON posts(slug);

-- Create comments table
CREATE TABLE comments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  post_id INTEGER NOT NULL,
  author_name TEXT NOT NULL,
  content TEXT NOT NULL,
  approved BOOLEAN DEFAULT false,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
);

CREATE INDEX idx_comments_post ON comments(post_id, approved);
```

---

## SSR with DoSQL

### Basic Page with Database Query

```astro
---
// src/pages/posts/index.astro
import Layout from '../../layouts/Layout.astro';
import { getDB } from '../../lib/db';

interface Post {
  id: number;
  title: string;
  slug: string;
  excerpt: string | null;
  created_at: string;
}

// Get environment from Astro's runtime
const { env } = Astro.locals.runtime;

// Fetch posts from database
const db = await getDB(env);
const posts = await db.query<Post>(`
  SELECT id, title, slug, excerpt, created_at
  FROM posts
  WHERE published = ?
  ORDER BY created_at DESC
  LIMIT 20
`, [true]);
---

<Layout title="Blog Posts">
  <main>
    <h1>Blog Posts</h1>

    {posts.length === 0 ? (
      <p>No posts yet.</p>
    ) : (
      <ul class="posts-list">
        {posts.map((post) => (
          <li>
            <a href={`/posts/${post.slug}`}>
              <h2>{post.title}</h2>
              {post.excerpt && <p>{post.excerpt}</p>}
              <time datetime={post.created_at}>
                {new Date(post.created_at).toLocaleDateString()}
              </time>
            </a>
          </li>
        ))}
      </ul>
    )}
  </main>
</Layout>

<style>
  .posts-list {
    list-style: none;
    padding: 0;
  }

  .posts-list li {
    margin-bottom: 2rem;
    padding-bottom: 2rem;
    border-bottom: 1px solid #eee;
  }

  .posts-list h2 {
    margin: 0 0 0.5rem;
  }

  .posts-list time {
    color: #666;
    font-size: 0.875rem;
  }
</style>
```

### Dynamic Routes with Database

```astro
---
// src/pages/posts/[slug].astro
import Layout from '../../layouts/Layout.astro';
import { getDB } from '../../lib/db';

interface Post {
  id: number;
  title: string;
  slug: string;
  content: string;
  created_at: string;
  updated_at: string;
}

interface Comment {
  id: number;
  author_name: string;
  content: string;
  created_at: string;
}

const { slug } = Astro.params;
const { env } = Astro.locals.runtime;

if (!slug) {
  return Astro.redirect('/posts');
}

const db = await getDB(env);

// Fetch post and comments in parallel
const [post, comments] = await Promise.all([
  db.queryOne<Post>(
    'SELECT * FROM posts WHERE slug = ? AND published = ?',
    [slug, true]
  ),
  db.query<Comment>(`
    SELECT id, author_name, content, created_at
    FROM comments
    WHERE post_id = (SELECT id FROM posts WHERE slug = ?)
    AND approved = ?
    ORDER BY created_at ASC
  `, [slug, true]),
]);

// Return 404 if post not found
if (!post) {
  return Astro.redirect('/404');
}
---

<Layout title={post.title}>
  <article>
    <header>
      <h1>{post.title}</h1>
      <time datetime={post.created_at}>
        Published: {new Date(post.created_at).toLocaleDateString()}
      </time>
    </header>

    <div class="content" set:html={post.content} />

    <section class="comments">
      <h2>Comments ({comments.length})</h2>

      {comments.length === 0 ? (
        <p>No comments yet. Be the first to comment!</p>
      ) : (
        <ul>
          {comments.map((comment) => (
            <li class="comment">
              <header>
                <strong>{comment.author_name}</strong>
                <time datetime={comment.created_at}>
                  {new Date(comment.created_at).toLocaleDateString()}
                </time>
              </header>
              <p>{comment.content}</p>
            </li>
          ))}
        </ul>
      )}

      <form method="POST" action={`/api/posts/${post.id}/comments`}>
        <h3>Leave a Comment</h3>
        <div>
          <label for="author_name">Name</label>
          <input type="text" id="author_name" name="author_name" required />
        </div>
        <div>
          <label for="content">Comment</label>
          <textarea id="content" name="content" rows="4" required></textarea>
        </div>
        <button type="submit">Submit Comment</button>
      </form>
    </section>
  </article>
</Layout>

<style>
  article {
    max-width: 800px;
    margin: 0 auto;
  }

  .content {
    line-height: 1.8;
    margin: 2rem 0;
  }

  .comments {
    margin-top: 4rem;
    padding-top: 2rem;
    border-top: 1px solid #eee;
  }

  .comment {
    margin-bottom: 1.5rem;
    padding: 1rem;
    background: #f9f9f9;
    border-radius: 4px;
  }

  .comment header {
    display: flex;
    justify-content: space-between;
    margin-bottom: 0.5rem;
  }
</style>
```

### Pagination

```astro
---
// src/pages/posts/page/[page].astro
import Layout from '../../../layouts/Layout.astro';
import { getDB } from '../../../lib/db';

interface Post {
  id: number;
  title: string;
  slug: string;
  excerpt: string | null;
  created_at: string;
}

const { page: pageParam } = Astro.params;
const page = parseInt(pageParam || '1', 10);
const pageSize = 10;
const offset = (page - 1) * pageSize;

const { env } = Astro.locals.runtime;
const db = await getDB(env);

// Fetch posts and total count in parallel
const [posts, countResult] = await Promise.all([
  db.query<Post>(`
    SELECT id, title, slug, excerpt, created_at
    FROM posts
    WHERE published = ?
    ORDER BY created_at DESC
    LIMIT ? OFFSET ?
  `, [true, pageSize, offset]),
  db.queryOne<{ total: number }>(
    'SELECT COUNT(*) as total FROM posts WHERE published = ?',
    [true]
  ),
]);

const total = countResult?.total || 0;
const totalPages = Math.ceil(total / pageSize);
const hasPrev = page > 1;
const hasNext = page < totalPages;
---

<Layout title={`Blog Posts - Page ${page}`}>
  <main>
    <h1>Blog Posts</h1>

    <ul class="posts-list">
      {posts.map((post) => (
        <li>
          <a href={`/posts/${post.slug}`}>
            <h2>{post.title}</h2>
            {post.excerpt && <p>{post.excerpt}</p>}
          </a>
        </li>
      ))}
    </ul>

    <nav class="pagination">
      {hasPrev && (
        <a href={page === 2 ? '/posts' : `/posts/page/${page - 1}`}>
          Previous
        </a>
      )}

      <span>Page {page} of {totalPages}</span>

      {hasNext && (
        <a href={`/posts/page/${page + 1}`}>
          Next
        </a>
      )}
    </nav>
  </main>
</Layout>

<style>
  .pagination {
    display: flex;
    justify-content: center;
    gap: 2rem;
    margin-top: 2rem;
    padding-top: 2rem;
    border-top: 1px solid #eee;
  }
</style>
```

---

## Hybrid Rendering

Astro's hybrid rendering allows you to mix static and server-rendered pages.

### Configuring Hybrid Mode

```javascript
// astro.config.mjs
import { defineConfig } from 'astro/config';
import cloudflare from '@astrojs/cloudflare';

export default defineConfig({
  output: 'hybrid', // Enable hybrid rendering
  adapter: cloudflare({
    mode: 'directory',
  }),
});
```

### Static Pages with Dynamic Data

For pages that can be pre-rendered but need occasional updates:

```astro
---
// src/pages/about.astro
// This page is statically generated by default in hybrid mode

export const prerender = true; // Explicitly mark as static
---

<Layout title="About">
  <h1>About Our Blog</h1>
  <p>This is a statically generated page.</p>
</Layout>
```

### Server-Rendered Pages

```astro
---
// src/pages/dashboard.astro
// Opt into SSR for pages that need fresh data

export const prerender = false; // Force server rendering

import Layout from '../layouts/Layout.astro';
import { getDB } from '../lib/db';

const { env } = Astro.locals.runtime;
const db = await getDB(env);

// Get real-time stats
const stats = await db.queryOne<{
  posts: number;
  comments: number;
  views: number;
}>(`
  SELECT
    (SELECT COUNT(*) FROM posts) as posts,
    (SELECT COUNT(*) FROM comments) as comments,
    (SELECT SUM(view_count) FROM posts) as views
`);
---

<Layout title="Dashboard">
  <h1>Dashboard</h1>
  <div class="stats">
    <div class="stat">
      <span class="value">{stats?.posts || 0}</span>
      <span class="label">Posts</span>
    </div>
    <div class="stat">
      <span class="value">{stats?.comments || 0}</span>
      <span class="label">Comments</span>
    </div>
    <div class="stat">
      <span class="value">{stats?.views || 0}</span>
      <span class="label">Views</span>
    </div>
  </div>
</Layout>
```

### Mixing Static and Dynamic Content

```astro
---
// src/pages/index.astro
// Home page with static shell and dynamic content

export const prerender = false; // SSR for fresh data

import Layout from '../layouts/Layout.astro';
import { getDB } from '../lib/db';

const { env } = Astro.locals.runtime;
const db = await getDB(env);

// Fetch featured posts
const featuredPosts = await db.query<{
  id: number;
  title: string;
  slug: string;
  excerpt: string;
}>(`
  SELECT id, title, slug, excerpt
  FROM posts
  WHERE published = ? AND featured = ?
  ORDER BY created_at DESC
  LIMIT 3
`, [true, true]);

// Fetch recent posts
const recentPosts = await db.query<{
  id: number;
  title: string;
  slug: string;
  created_at: string;
}>(`
  SELECT id, title, slug, created_at
  FROM posts
  WHERE published = ?
  ORDER BY created_at DESC
  LIMIT 5
`, [true]);
---

<Layout title="Home">
  <section class="hero">
    <h1>Welcome to Our Blog</h1>
    <p>Discover the latest articles and insights.</p>
  </section>

  {featuredPosts.length > 0 && (
    <section class="featured">
      <h2>Featured Posts</h2>
      <div class="featured-grid">
        {featuredPosts.map((post) => (
          <article class="featured-card">
            <h3><a href={`/posts/${post.slug}`}>{post.title}</a></h3>
            <p>{post.excerpt}</p>
          </article>
        ))}
      </div>
    </section>
  )}

  <section class="recent">
    <h2>Recent Posts</h2>
    <ul>
      {recentPosts.map((post) => (
        <li>
          <a href={`/posts/${post.slug}`}>{post.title}</a>
          <time>{new Date(post.created_at).toLocaleDateString()}</time>
        </li>
      ))}
    </ul>
  </section>
</Layout>
```

---

## API Endpoints

Astro API endpoints provide RESTful API functionality with full database access.

### Basic CRUD Endpoints

```typescript
// src/pages/api/posts.ts
import type { APIRoute } from 'astro';
import { getDB } from '../../lib/db';

// GET /api/posts - List posts
export const GET: APIRoute = async ({ locals, url }) => {
  const { env } = locals.runtime;
  const db = await getDB(env);

  const page = parseInt(url.searchParams.get('page') || '1', 10);
  const limit = parseInt(url.searchParams.get('limit') || '10', 10);
  const offset = (page - 1) * limit;
  const published = url.searchParams.get('published') !== 'false';

  const [posts, countResult] = await Promise.all([
    db.query(
      `SELECT id, title, slug, excerpt, created_at
       FROM posts
       WHERE published = ?
       ORDER BY created_at DESC
       LIMIT ? OFFSET ?`,
      [published, limit, offset]
    ),
    db.queryOne<{ total: number }>(
      'SELECT COUNT(*) as total FROM posts WHERE published = ?',
      [published]
    ),
  ]);

  return new Response(JSON.stringify({
    posts,
    pagination: {
      page,
      limit,
      total: countResult?.total || 0,
      pages: Math.ceil((countResult?.total || 0) / limit),
    },
  }), {
    headers: { 'Content-Type': 'application/json' },
  });
};

// POST /api/posts - Create post
export const POST: APIRoute = async ({ locals, request }) => {
  const { env } = locals.runtime;
  const db = await getDB(env);

  try {
    const body = await request.json() as {
      title: string;
      slug: string;
      content: string;
      excerpt?: string;
      published?: boolean;
    };

    // Validation
    if (!body.title || !body.slug || !body.content) {
      return new Response(JSON.stringify({
        error: 'Missing required fields: title, slug, content',
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const result = await db.run(
      `INSERT INTO posts (title, slug, content, excerpt, published)
       VALUES (?, ?, ?, ?, ?)`,
      [body.title, body.slug, body.content, body.excerpt || null, body.published || false]
    );

    return new Response(JSON.stringify({
      id: result.lastInsertRowId,
      message: 'Post created successfully',
    }), {
      status: 201,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    const message = (error as Error).message;

    if (message.includes('UNIQUE constraint')) {
      return new Response(JSON.stringify({
        error: 'A post with this slug already exists',
      }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    return new Response(JSON.stringify({
      error: message,
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};
```

### Dynamic API Routes

```typescript
// src/pages/api/posts/[id].ts
import type { APIRoute } from 'astro';
import { getDB } from '../../../lib/db';

// GET /api/posts/:id
export const GET: APIRoute = async ({ params, locals }) => {
  const { env } = locals.runtime;
  const db = await getDB(env);
  const { id } = params;

  const post = await db.queryOne(
    'SELECT * FROM posts WHERE id = ?',
    [id]
  );

  if (!post) {
    return new Response(JSON.stringify({ error: 'Post not found' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  return new Response(JSON.stringify(post), {
    headers: { 'Content-Type': 'application/json' },
  });
};

// PUT /api/posts/:id
export const PUT: APIRoute = async ({ params, locals, request }) => {
  const { env } = locals.runtime;
  const db = await getDB(env);
  const { id } = params;

  try {
    const body = await request.json() as {
      title?: string;
      content?: string;
      excerpt?: string;
      published?: boolean;
    };

    // Build dynamic update query
    const updates: string[] = [];
    const values: unknown[] = [];

    if (body.title !== undefined) {
      updates.push('title = ?');
      values.push(body.title);
    }
    if (body.content !== undefined) {
      updates.push('content = ?');
      values.push(body.content);
    }
    if (body.excerpt !== undefined) {
      updates.push('excerpt = ?');
      values.push(body.excerpt);
    }
    if (body.published !== undefined) {
      updates.push('published = ?');
      values.push(body.published);
    }

    if (updates.length === 0) {
      return new Response(JSON.stringify({ error: 'No fields to update' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    updates.push('updated_at = CURRENT_TIMESTAMP');
    values.push(id);

    const result = await db.run(
      `UPDATE posts SET ${updates.join(', ')} WHERE id = ?`,
      values
    );

    if (result.rowsAffected === 0) {
      return new Response(JSON.stringify({ error: 'Post not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};

// DELETE /api/posts/:id
export const DELETE: APIRoute = async ({ params, locals }) => {
  const { env } = locals.runtime;
  const db = await getDB(env);
  const { id } = params;

  const result = await db.run('DELETE FROM posts WHERE id = ?', [id]);

  if (result.rowsAffected === 0) {
    return new Response(JSON.stringify({ error: 'Post not found' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  return new Response(JSON.stringify({ success: true }), {
    headers: { 'Content-Type': 'application/json' },
  });
};
```

### Comment API

```typescript
// src/pages/api/posts/[id]/comments.ts
import type { APIRoute } from 'astro';
import { getDB } from '../../../../lib/db';

// GET /api/posts/:id/comments
export const GET: APIRoute = async ({ params, locals, url }) => {
  const { env } = locals.runtime;
  const db = await getDB(env);
  const { id } = params;
  const approved = url.searchParams.get('approved') !== 'false';

  const comments = await db.query(
    `SELECT id, author_name, content, created_at, approved
     FROM comments
     WHERE post_id = ? AND (approved = ? OR ? = false)
     ORDER BY created_at ASC`,
    [id, approved, approved]
  );

  return new Response(JSON.stringify(comments), {
    headers: { 'Content-Type': 'application/json' },
  });
};

// POST /api/posts/:id/comments
export const POST: APIRoute = async ({ params, locals, request }) => {
  const { env } = locals.runtime;
  const db = await getDB(env);
  const { id } = params;

  // Check if post exists
  const post = await db.queryOne('SELECT id FROM posts WHERE id = ?', [id]);
  if (!post) {
    return new Response(JSON.stringify({ error: 'Post not found' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // Parse form data or JSON
  let body: { author_name: string; content: string };

  const contentType = request.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    body = await request.json();
  } else {
    const formData = await request.formData();
    body = {
      author_name: formData.get('author_name') as string,
      content: formData.get('content') as string,
    };
  }

  if (!body.author_name || !body.content) {
    return new Response(JSON.stringify({
      error: 'Missing required fields: author_name, content',
    }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const result = await db.run(
    `INSERT INTO comments (post_id, author_name, content, approved)
     VALUES (?, ?, ?, ?)`,
    [id, body.author_name, body.content, false]
  );

  // If the request came from a form, redirect back to the post
  if (!contentType.includes('application/json')) {
    const postData = await db.queryOne<{ slug: string }>(
      'SELECT slug FROM posts WHERE id = ?',
      [id]
    );
    return new Response(null, {
      status: 302,
      headers: {
        'Location': `/posts/${postData?.slug}?comment=pending`,
      },
    });
  }

  return new Response(JSON.stringify({
    id: result.lastInsertRowId,
    message: 'Comment submitted and pending approval',
  }), {
    status: 201,
    headers: { 'Content-Type': 'application/json' },
  });
};
```

### Search API

```typescript
// src/pages/api/search.ts
import type { APIRoute } from 'astro';
import { getDB } from '../../lib/db';

export const GET: APIRoute = async ({ locals, url }) => {
  const { env } = locals.runtime;
  const db = await getDB(env);

  const query = url.searchParams.get('q');
  const limit = parseInt(url.searchParams.get('limit') || '10', 10);

  if (!query || query.trim().length < 2) {
    return new Response(JSON.stringify({
      error: 'Search query must be at least 2 characters',
    }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const searchTerm = `%${query}%`;

  const results = await db.query(
    `SELECT id, title, slug, excerpt,
            CASE
              WHEN title LIKE ? THEN 2
              WHEN content LIKE ? THEN 1
              ELSE 0
            END as relevance
     FROM posts
     WHERE published = ?
       AND (title LIKE ? OR content LIKE ? OR excerpt LIKE ?)
     ORDER BY relevance DESC, created_at DESC
     LIMIT ?`,
    [searchTerm, searchTerm, true, searchTerm, searchTerm, searchTerm, limit]
  );

  return new Response(JSON.stringify({
    query,
    results,
    count: results.length,
  }), {
    headers: { 'Content-Type': 'application/json' },
  });
};
```

---

## Client-Side Integration

### Fetching Data from Components

```astro
---
// src/components/SearchBox.astro
---

<div class="search-box">
  <input type="search" id="search-input" placeholder="Search posts..." />
  <div id="search-results" class="results"></div>
</div>

<style>
  .search-box {
    position: relative;
  }

  .results {
    position: absolute;
    top: 100%;
    left: 0;
    right: 0;
    background: white;
    border: 1px solid #ddd;
    border-radius: 4px;
    max-height: 300px;
    overflow-y: auto;
    display: none;
  }

  .results.active {
    display: block;
  }

  .result-item {
    padding: 0.75rem 1rem;
    border-bottom: 1px solid #eee;
  }

  .result-item:hover {
    background: #f5f5f5;
  }
</style>

<script>
  const input = document.getElementById('search-input') as HTMLInputElement;
  const results = document.getElementById('search-results')!;

  let debounceTimer: number;

  input.addEventListener('input', () => {
    clearTimeout(debounceTimer);

    const query = input.value.trim();

    if (query.length < 2) {
      results.classList.remove('active');
      return;
    }

    debounceTimer = setTimeout(async () => {
      try {
        const response = await fetch(`/api/search?q=${encodeURIComponent(query)}`);
        const data = await response.json();

        if (data.results.length === 0) {
          results.innerHTML = '<div class="result-item">No results found</div>';
        } else {
          results.innerHTML = data.results.map((post: any) => `
            <a href="/posts/${post.slug}" class="result-item">
              <strong>${post.title}</strong>
              ${post.excerpt ? `<p>${post.excerpt}</p>` : ''}
            </a>
          `).join('');
        }

        results.classList.add('active');
      } catch (error) {
        console.error('Search error:', error);
      }
    }, 300);
  });

  // Close results when clicking outside
  document.addEventListener('click', (e) => {
    if (!results.contains(e.target as Node) && e.target !== input) {
      results.classList.remove('active');
    }
  });
</script>
```

### React/Vue/Svelte Islands with DoSQL

Using Astro's island architecture with client-side data fetching:

```tsx
// src/components/LiveComments.tsx
import { useState, useEffect } from 'react';

interface Comment {
  id: number;
  author_name: string;
  content: string;
  created_at: string;
}

interface Props {
  postId: number;
  initialComments: Comment[];
}

export default function LiveComments({ postId, initialComments }: Props) {
  const [comments, setComments] = useState<Comment[]>(initialComments);
  const [newComment, setNewComment] = useState({ author_name: '', content: '' });
  const [submitting, setSubmitting] = useState(false);

  // Poll for new comments every 30 seconds
  useEffect(() => {
    const interval = setInterval(async () => {
      try {
        const response = await fetch(`/api/posts/${postId}/comments`);
        const data = await response.json();
        setComments(data);
      } catch (error) {
        console.error('Failed to fetch comments:', error);
      }
    }, 30000);

    return () => clearInterval(interval);
  }, [postId]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);

    try {
      const response = await fetch(`/api/posts/${postId}/comments`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newComment),
      });

      if (response.ok) {
        // Optimistically add the comment (pending approval notice)
        setComments([...comments, {
          id: Date.now(),
          ...newComment,
          created_at: new Date().toISOString(),
        }]);
        setNewComment({ author_name: '', content: '' });
      }
    } catch (error) {
      console.error('Failed to submit comment:', error);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <section className="live-comments">
      <h3>Comments ({comments.length})</h3>

      <ul>
        {comments.map((comment) => (
          <li key={comment.id}>
            <strong>{comment.author_name}</strong>
            <time>{new Date(comment.created_at).toLocaleDateString()}</time>
            <p>{comment.content}</p>
          </li>
        ))}
      </ul>

      <form onSubmit={handleSubmit}>
        <input
          type="text"
          placeholder="Your name"
          value={newComment.author_name}
          onChange={(e) => setNewComment({ ...newComment, author_name: e.target.value })}
          required
        />
        <textarea
          placeholder="Your comment"
          value={newComment.content}
          onChange={(e) => setNewComment({ ...newComment, content: e.target.value })}
          required
        />
        <button type="submit" disabled={submitting}>
          {submitting ? 'Submitting...' : 'Submit Comment'}
        </button>
      </form>
    </section>
  );
}
```

Using the React component in Astro:

```astro
---
// src/pages/posts/[slug].astro
import Layout from '../../layouts/Layout.astro';
import LiveComments from '../../components/LiveComments';
import { getDB } from '../../lib/db';

const { slug } = Astro.params;
const { env } = Astro.locals.runtime;
const db = await getDB(env);

const post = await db.queryOne('SELECT * FROM posts WHERE slug = ?', [slug]);
const initialComments = await db.query(
  'SELECT * FROM comments WHERE post_id = ? AND approved = ? ORDER BY created_at',
  [post?.id, true]
);
---

<Layout title={post?.title || 'Post'}>
  <article>
    <h1>{post?.title}</h1>
    <div set:html={post?.content} />

    <!-- Hydrate with React for interactivity -->
    <LiveComments
      client:visible
      postId={post?.id}
      initialComments={initialComments}
    />
  </article>
</Layout>
```

---

## Content Collections with DoSQL

Combine Astro Content Collections with dynamic database data.

### Hybrid Content Strategy

```typescript
// src/content/config.ts
import { defineCollection, z } from 'astro:content';

// Static content in markdown
const staticPosts = defineCollection({
  type: 'content',
  schema: z.object({
    title: z.string(),
    description: z.string(),
    pubDate: z.coerce.date(),
    author: z.string(),
    tags: z.array(z.string()).optional(),
  }),
});

export const collections = { staticPosts };
```

```astro
---
// src/pages/blog/index.astro
// Combine static content collections with dynamic database posts

import Layout from '../../layouts/Layout.astro';
import { getCollection } from 'astro:content';
import { getDB } from '../../lib/db';

// Get static posts from content collections
const staticPosts = await getCollection('staticPosts');

// Get dynamic posts from database
const { env } = Astro.locals.runtime;
const db = await getDB(env);
const dynamicPosts = await db.query<{
  id: number;
  title: string;
  slug: string;
  excerpt: string;
  created_at: string;
}>(`
  SELECT id, title, slug, excerpt, created_at
  FROM posts
  WHERE published = ?
  ORDER BY created_at DESC
`, [true]);

// Merge and sort all posts
const allPosts = [
  ...staticPosts.map(post => ({
    type: 'static' as const,
    slug: post.slug,
    title: post.data.title,
    description: post.data.description,
    date: post.data.pubDate,
  })),
  ...dynamicPosts.map(post => ({
    type: 'dynamic' as const,
    slug: post.slug,
    title: post.title,
    description: post.excerpt,
    date: new Date(post.created_at),
  })),
].sort((a, b) => b.date.getTime() - a.date.getTime());
---

<Layout title="Blog">
  <h1>All Posts</h1>
  <ul>
    {allPosts.map((post) => (
      <li>
        <a href={post.type === 'static' ? `/blog/${post.slug}` : `/posts/${post.slug}`}>
          <h2>{post.title}</h2>
          <p>{post.description}</p>
          <time>{post.date.toLocaleDateString()}</time>
          {post.type === 'dynamic' && <span class="badge">Live</span>}
        </a>
      </li>
    ))}
  </ul>
</Layout>
```

---

## Type Safety

### Database Types

```typescript
// src/lib/types.ts
export interface Post {
  id: number;
  title: string;
  slug: string;
  content: string;
  excerpt: string | null;
  published: boolean;
  author_id: number | null;
  created_at: string;
  updated_at: string;
}

export interface Comment {
  id: number;
  post_id: number;
  author_name: string;
  content: string;
  approved: boolean;
  created_at: string;
}

export interface Author {
  id: number;
  name: string;
  email: string;
  bio: string | null;
}

// Query result types
export interface PostWithAuthor extends Post {
  author_name: string | null;
  author_email: string | null;
}

export interface PostStats {
  total_posts: number;
  published_posts: number;
  total_comments: number;
  approved_comments: number;
}
```

### Typed Database Queries

```typescript
// src/lib/queries.ts
import type { DBClient } from './db';
import type { Post, Comment, PostWithAuthor, PostStats } from './types';

export async function getPublishedPosts(
  db: DBClient,
  limit = 10,
  offset = 0
): Promise<Post[]> {
  return db.query<Post>(
    `SELECT * FROM posts
     WHERE published = ?
     ORDER BY created_at DESC
     LIMIT ? OFFSET ?`,
    [true, limit, offset]
  );
}

export async function getPostBySlug(
  db: DBClient,
  slug: string
): Promise<PostWithAuthor | null> {
  return db.queryOne<PostWithAuthor>(
    `SELECT p.*, a.name as author_name, a.email as author_email
     FROM posts p
     LEFT JOIN authors a ON p.author_id = a.id
     WHERE p.slug = ?`,
    [slug]
  );
}

export async function getPostComments(
  db: DBClient,
  postId: number,
  approvedOnly = true
): Promise<Comment[]> {
  return db.query<Comment>(
    `SELECT * FROM comments
     WHERE post_id = ? ${approvedOnly ? 'AND approved = ?' : ''}
     ORDER BY created_at ASC`,
    approvedOnly ? [postId, true] : [postId]
  );
}

export async function getStats(db: DBClient): Promise<PostStats> {
  const result = await db.queryOne<PostStats>(`
    SELECT
      (SELECT COUNT(*) FROM posts) as total_posts,
      (SELECT COUNT(*) FROM posts WHERE published = 1) as published_posts,
      (SELECT COUNT(*) FROM comments) as total_comments,
      (SELECT COUNT(*) FROM comments WHERE approved = 1) as approved_comments
  `);

  return result || {
    total_posts: 0,
    published_posts: 0,
    total_comments: 0,
    approved_comments: 0,
  };
}

export async function createPost(
  db: DBClient,
  post: Omit<Post, 'id' | 'created_at' | 'updated_at'>
): Promise<{ id: number }> {
  const result = await db.run(
    `INSERT INTO posts (title, slug, content, excerpt, published, author_id)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [post.title, post.slug, post.content, post.excerpt, post.published, post.author_id]
  );

  return { id: result.lastInsertRowId };
}
```

Using typed queries in pages:

```astro
---
// src/pages/posts/[slug].astro
import Layout from '../../layouts/Layout.astro';
import { getDB } from '../../lib/db';
import { getPostBySlug, getPostComments } from '../../lib/queries';

const { slug } = Astro.params;
const { env } = Astro.locals.runtime;
const db = await getDB(env);

const post = await getPostBySlug(db, slug!);
if (!post) return Astro.redirect('/404');

const comments = await getPostComments(db, post.id);
---

<Layout title={post.title}>
  <article>
    <h1>{post.title}</h1>
    {post.author_name && (
      <p class="author">By {post.author_name}</p>
    )}
    <div set:html={post.content} />
  </article>
</Layout>
```

---

## Advanced Patterns

### Middleware for Database Access

```typescript
// src/middleware.ts
import { defineMiddleware } from 'astro:middleware';
import { getDB, type DBClient } from './lib/db';

// Extend locals type
declare global {
  namespace App {
    interface Locals {
      db: DBClient;
    }
  }
}

export const onRequest = defineMiddleware(async ({ locals, request }, next) => {
  const { env } = locals.runtime;

  // Extract tenant ID from subdomain or header
  const url = new URL(request.url);
  const tenantId = url.hostname.split('.')[0] || 'default';

  // Attach database client to locals
  locals.db = await getDB(env, tenantId);

  return next();
});
```

Using middleware-provided database:

```astro
---
// src/pages/posts/index.astro
import Layout from '../../layouts/Layout.astro';

// Database is now available via middleware
const { db } = Astro.locals;

const posts = await db.query('SELECT * FROM posts WHERE published = ?', [true]);
---

<Layout title="Posts">
  <h1>Posts</h1>
  <!-- ... -->
</Layout>
```

### Caching with Cache-Control

```typescript
// src/pages/api/posts.ts
import type { APIRoute } from 'astro';
import { getDB } from '../../lib/db';

export const GET: APIRoute = async ({ locals, url, request }) => {
  const { env } = locals.runtime;
  const db = await getDB(env);

  // Check for conditional request
  const ifNoneMatch = request.headers.get('if-none-match');

  const posts = await db.query(
    'SELECT * FROM posts WHERE published = ? ORDER BY created_at DESC LIMIT 20',
    [true]
  );

  // Generate ETag from content
  const contentHash = await generateHash(JSON.stringify(posts));
  const etag = `"${contentHash}"`;

  if (ifNoneMatch === etag) {
    return new Response(null, { status: 304 });
  }

  return new Response(JSON.stringify(posts), {
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=60, stale-while-revalidate=300',
      'ETag': etag,
    },
  });
};

async function generateHash(content: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(content);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('').slice(0, 16);
}
```

### Transactions in API Endpoints

```typescript
// src/pages/api/admin/posts/publish-batch.ts
import type { APIRoute } from 'astro';
import { getDB } from '../../../../lib/db';

export const POST: APIRoute = async ({ locals, request }) => {
  const { env } = locals.runtime;
  const db = await getDB(env);

  const { postIds } = await request.json() as { postIds: number[] };

  if (!postIds || !Array.isArray(postIds) || postIds.length === 0) {
    return new Response(JSON.stringify({
      error: 'postIds must be a non-empty array',
    }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  try {
    // Use transaction for atomic batch update
    await db.transaction(async (tx) => {
      for (const id of postIds) {
        await tx.run(
          'UPDATE posts SET published = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
          [true, id]
        );
      }
    });

    return new Response(JSON.stringify({
      success: true,
      published: postIds.length,
    }), {
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    return new Response(JSON.stringify({
      error: (error as Error).message,
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};
```

### CDC for Real-Time Updates

```typescript
// src/pages/api/events.ts
import type { APIRoute } from 'astro';
import { getDB } from '../../lib/db';

export const GET: APIRoute = async ({ locals, request }) => {
  const { env } = locals.runtime;
  const db = await getDB(env);

  // Server-Sent Events for real-time updates
  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      // Send initial connection event
      controller.enqueue(encoder.encode('event: connected\ndata: {}\n\n'));

      // Subscribe to CDC changes
      const subscription = await (db as any).subscribeCDC({
        tables: ['posts', 'comments'],
        fromLSN: 0n,
      });

      // Handle client disconnect
      request.signal.addEventListener('abort', () => {
        subscription?.close?.();
        controller.close();
      });

      // Stream events
      for await (const event of subscription) {
        const data = JSON.stringify({
          table: event.table,
          operation: event.op,
          data: event.after || event.before,
          timestamp: event.timestamp,
        });

        controller.enqueue(encoder.encode(`event: change\ndata: ${data}\n\n`));
      }
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

---

## Deployment

### Cloudflare Pages Deployment

#### Build Configuration

```json
// package.json
{
  "scripts": {
    "dev": "astro dev",
    "build": "astro build",
    "preview": "wrangler pages dev dist",
    "deploy": "npm run build && wrangler pages deploy dist",
    "deploy:production": "npm run build && wrangler pages deploy dist --branch main"
  }
}
```

#### Wrangler Configuration for Pages

```toml
# wrangler.toml
name = "my-astro-blog"
compatibility_date = "2024-01-01"
compatibility_flags = ["nodejs_compat"]

pages_build_output_dir = "./dist"

# Durable Objects
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"
script_name = "dosql-worker"  # Separate worker for DO

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

# R2 for media/assets
[[r2_buckets]]
binding = "MEDIA_BUCKET"
bucket_name = "astro-media"

# Environment variables
[vars]
ENVIRONMENT = "production"
SITE_URL = "https://myblog.pages.dev"

# Production overrides
[env.production]
name = "my-astro-blog-prod"

[env.production.vars]
ENVIRONMENT = "production"
SITE_URL = "https://myblog.com"

[[env.production.r2_buckets]]
binding = "MEDIA_BUCKET"
bucket_name = "astro-media-prod"
```

#### Deployment Steps

```bash
# 1. Build the Astro project
npm run build

# 2. Deploy to Cloudflare Pages
wrangler pages deploy dist

# 3. For production deployment
wrangler pages deploy dist --branch main

# 4. Check deployment status
wrangler pages deployment list
```

### Environment Variables

Set secrets via Wrangler CLI:

```bash
# Set secrets for the Pages project
wrangler pages secret put DATABASE_SECRET

# Or set in the Cloudflare dashboard:
# 1. Go to Workers & Pages > your project > Settings > Environment variables
# 2. Add variables for Production and Preview environments
```

### Production Checklist

1. **Configure custom domain** in Cloudflare Pages settings
2. **Set environment variables** for each environment (production/preview)
3. **Enable Durable Objects** on Workers Paid plan
4. **Create R2 buckets** for media storage if needed
5. **Set up monitoring** with Cloudflare Analytics
6. **Configure caching rules** for static assets

### GitHub Actions Deployment

```yaml
# .github/workflows/deploy.yml
name: Deploy to Cloudflare Pages

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      deployments: write
    steps:
      - uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'

      - name: Install dependencies
        run: npm ci

      - name: Build
        run: npm run build

      - name: Deploy to Cloudflare Pages
        uses: cloudflare/pages-action@v1
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          accountId: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
          projectName: my-astro-blog
          directory: dist
          gitHubToken: ${{ secrets.GITHUB_TOKEN }}
```

---

## Next Steps

- [Getting Started](../getting-started.md) - DoSQL basics
- [API Reference](../api-reference.md) - Complete API documentation
- [Advanced Features](../advanced.md) - CDC, time travel, branching
- [Architecture](../architecture.md) - Understanding DoSQL internals
- [Next.js Integration](./NEXTJS.md) - Next.js specific patterns
- [Remix Integration](./REMIX.md) - Remix specific patterns
