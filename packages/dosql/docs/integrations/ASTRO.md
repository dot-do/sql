# DoSQL + Astro Integration Guide

Build edge-native, database-driven applications with Astro and DoSQL. This guide covers SSR, hybrid rendering, API endpoints, and deployment to Cloudflare Pages.

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [SSR with DoSQL](#ssr-with-dosql)
- [Hybrid Rendering](#hybrid-rendering)
- [API Endpoints](#api-endpoints)
- [Client-Side Integration](#client-side-integration)
- [Content Collections with DoSQL](#content-collections-with-dosql)
- [Type Safety](#type-safety)
- [View Transitions](#view-transitions)
- [Error Handling](#error-handling)
- [Local Development](#local-development)
- [Advanced Patterns](#advanced-patterns)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)

---

## Overview

### Why DoSQL + Astro

Astro's content-focused architecture pairs naturally with DoSQL's edge-native database:

| Astro Feature | DoSQL Benefit |
|---------------|---------------|
| **SSR/Hybrid Rendering** | Server-side data fetching at the edge |
| **API Endpoints** | RESTful APIs with transactional database access |
| **Island Architecture** | Interactive components with real-time data |
| **Content Collections** | Combine static content with dynamic queries |
| **Edge Deployment** | Native Cloudflare Pages integration |

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
|  |   SQLite DB      |  |     |  |   SQLite DB      |  |
|  |   - posts        |  |     |  |   - posts        |  |
|  |   - comments     |  |     |  |   - comments     |  |
|  +------------------+  |     |  +------------------+  |
+------------------------+     +------------------------+
```

### Rendering Modes

| Mode | Description | DoSQL Access |
|------|-------------|--------------|
| **Static (SSG)** | Build-time rendering | Build-time queries only |
| **Server (SSR)** | On-demand rendering | Full database access |
| **Hybrid** | Per-page control | SSR pages get database access |

---

## Setup

### Installation

```bash
# Create a new Astro project
npm create astro@latest my-astro-app -- --template minimal
cd my-astro-app

# Install dependencies
npm install @dotdo/dosql
npm install @astrojs/cloudflare
npm install -D @cloudflare/workers-types wrangler
```

### Project Structure

```
my-astro-app/
├── .do/
│   └── migrations/
│       └── 001_init.sql
├── src/
│   ├── lib/
│   │   ├── db.ts
│   │   └── types.ts
│   ├── pages/
│   │   ├── index.astro
│   │   ├── posts/
│   │   │   ├── index.astro
│   │   │   └── [slug].astro
│   │   └── api/
│   │       ├── posts.ts
│   │       └── posts/[id].ts
│   ├── components/
│   │   └── PostList.astro
│   ├── layouts/
│   │   └── Layout.astro
│   └── env.d.ts
├── astro.config.mjs
├── wrangler.toml
└── tsconfig.json
```

### Astro Configuration

```javascript
// astro.config.mjs
import { defineConfig } from 'astro/config';
import cloudflare from '@astrojs/cloudflare';

export default defineConfig({
  output: 'server',
  adapter: cloudflare({
    platformProxy: {
      enabled: true,
      persist: {
        path: '.wrangler/state',
      },
    },
  }),
});
```

### Wrangler Configuration

```toml
# wrangler.toml
name = "my-astro-app"
compatibility_date = "2024-12-01"
compatibility_flags = ["nodejs_compat"]

pages_build_output_dir = "./dist"

[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

[vars]
ENVIRONMENT = "development"
```

### TypeScript Types

Generate Cloudflare types and extend them:

```bash
npx wrangler types
```

Create `src/env.d.ts`:

```typescript
/// <reference types="astro/client" />

interface Env {
  DOSQL_DB: DurableObjectNamespace;
  ENVIRONMENT?: string;
}

type Runtime = import('@astrojs/cloudflare').Runtime<Env>;

declare namespace App {
  interface Locals extends Runtime {}
}
```

### Database Client

Create `src/lib/db.ts`:

```typescript
import type { Env } from '../env';

export interface DBClient {
  query<T = unknown>(sql: string, params?: unknown[]): Promise<T[]>;
  queryOne<T = unknown>(sql: string, params?: unknown[]): Promise<T | null>;
  run(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowId: number }>;
}

export function getDB(env: Env, tenantId = 'default'): DBClient {
  const id = env.DOSQL_DB.idFromName(tenantId);
  const stub = env.DOSQL_DB.get(id);

  return {
    async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
      const response = await stub.fetch('http://do/query', {
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
      const response = await stub.fetch('http://do/run', {
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
  };
}
```

### Initial Migration

Create `.do/migrations/001_init.sql`:

```sql
CREATE TABLE posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  slug TEXT UNIQUE NOT NULL,
  content TEXT NOT NULL,
  excerpt TEXT,
  published INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_posts_published ON posts(published, created_at DESC);
CREATE INDEX idx_posts_slug ON posts(slug);

CREATE TABLE comments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  author_name TEXT NOT NULL,
  content TEXT NOT NULL,
  approved INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
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

const { env } = Astro.locals.runtime;
const db = getDB(env);

const posts = await db.query<Post>(`
  SELECT id, title, slug, excerpt, created_at
  FROM posts
  WHERE published = 1
  ORDER BY created_at DESC
  LIMIT 20
`);
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

### Dynamic Routes

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
}

interface Comment {
  id: number;
  author_name: string;
  content: string;
  created_at: string;
}

const { slug } = Astro.params;
const { env } = Astro.locals.runtime;
const db = getDB(env);

if (!slug) {
  return Astro.redirect('/posts');
}

// Fetch post and comments in parallel
const [post, comments] = await Promise.all([
  db.queryOne<Post>(
    'SELECT * FROM posts WHERE slug = ? AND published = 1',
    [slug]
  ),
  db.query<Comment>(`
    SELECT id, author_name, content, created_at
    FROM comments
    WHERE post_id = (SELECT id FROM posts WHERE slug = ?)
      AND approved = 1
    ORDER BY created_at ASC
  `, [slug]),
]);

if (!post) {
  return Astro.redirect('/404');
}
---

<Layout title={post.title}>
  <article>
    <header>
      <h1>{post.title}</h1>
      <time datetime={post.created_at}>
        {new Date(post.created_at).toLocaleDateString()}
      </time>
    </header>

    <div class="content" set:html={post.content} />

    <section class="comments">
      <h2>Comments ({comments.length})</h2>

      {comments.length === 0 ? (
        <p>No comments yet.</p>
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
        <button type="submit">Submit</button>
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
}

const { page: pageParam } = Astro.params;
const page = parseInt(pageParam || '1', 10);
const pageSize = 10;
const offset = (page - 1) * pageSize;

const { env } = Astro.locals.runtime;
const db = getDB(env);

const [posts, countResult] = await Promise.all([
  db.query<Post>(`
    SELECT id, title, slug, excerpt
    FROM posts
    WHERE published = 1
    ORDER BY created_at DESC
    LIMIT ? OFFSET ?
  `, [pageSize, offset]),
  db.queryOne<{ total: number }>(
    'SELECT COUNT(*) as total FROM posts WHERE published = 1'
  ),
]);

const total = countResult?.total ?? 0;
const totalPages = Math.ceil(total / pageSize);
---

<Layout title={`Posts - Page ${page}`}>
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
      {page > 1 && (
        <a href={page === 2 ? '/posts' : `/posts/page/${page - 1}`}>
          Previous
        </a>
      )}

      <span>Page {page} of {totalPages}</span>

      {page < totalPages && (
        <a href={`/posts/page/${page + 1}`}>Next</a>
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

Astro hybrid mode lets you mix static and server-rendered pages.

### Configuration

```javascript
// astro.config.mjs
import { defineConfig } from 'astro/config';
import cloudflare from '@astrojs/cloudflare';

export default defineConfig({
  output: 'hybrid',
  adapter: cloudflare({
    platformProxy: { enabled: true },
  }),
});
```

### Static Pages

```astro
---
// src/pages/about.astro
export const prerender = true;

import Layout from '../layouts/Layout.astro';
---

<Layout title="About">
  <h1>About Us</h1>
  <p>This page is statically generated at build time.</p>
</Layout>
```

### Server-Rendered Pages

```astro
---
// src/pages/dashboard.astro
export const prerender = false;

import Layout from '../layouts/Layout.astro';
import { getDB } from '../lib/db';

const { env } = Astro.locals.runtime;
const db = getDB(env);

const stats = await db.queryOne<{
  posts: number;
  comments: number;
}>(`
  SELECT
    (SELECT COUNT(*) FROM posts) as posts,
    (SELECT COUNT(*) FROM comments) as comments
`);
---

<Layout title="Dashboard">
  <h1>Dashboard</h1>
  <div class="stats">
    <div class="stat">
      <span class="value">{stats?.posts ?? 0}</span>
      <span class="label">Posts</span>
    </div>
    <div class="stat">
      <span class="value">{stats?.comments ?? 0}</span>
      <span class="label">Comments</span>
    </div>
  </div>
</Layout>
```

---

## API Endpoints

### CRUD Endpoints

```typescript
// src/pages/api/posts.ts
import type { APIRoute } from 'astro';
import { getDB } from '../../lib/db';

export const GET: APIRoute = async ({ locals, url }) => {
  const { env } = locals.runtime;
  const db = getDB(env);

  const page = parseInt(url.searchParams.get('page') || '1', 10);
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '10', 10), 100);
  const offset = (page - 1) * limit;

  const [posts, countResult] = await Promise.all([
    db.query(
      `SELECT id, title, slug, excerpt, created_at
       FROM posts
       WHERE published = 1
       ORDER BY created_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    ),
    db.queryOne<{ total: number }>(
      'SELECT COUNT(*) as total FROM posts WHERE published = 1'
    ),
  ]);

  return Response.json({
    posts,
    pagination: {
      page,
      limit,
      total: countResult?.total ?? 0,
      pages: Math.ceil((countResult?.total ?? 0) / limit),
    },
  });
};

export const POST: APIRoute = async ({ locals, request }) => {
  const { env } = locals.runtime;
  const db = getDB(env);

  const body = await request.json() as {
    title?: string;
    slug?: string;
    content?: string;
    excerpt?: string;
    published?: boolean;
  };

  if (!body.title || !body.slug || !body.content) {
    return Response.json(
      { error: 'Missing required fields: title, slug, content' },
      { status: 400 }
    );
  }

  try {
    const result = await db.run(
      `INSERT INTO posts (title, slug, content, excerpt, published)
       VALUES (?, ?, ?, ?, ?)`,
      [body.title, body.slug, body.content, body.excerpt ?? null, body.published ? 1 : 0]
    );

    return Response.json(
      { id: result.lastInsertRowId },
      { status: 201 }
    );
  } catch (error) {
    const message = (error as Error).message;

    if (message.includes('UNIQUE constraint')) {
      return Response.json(
        { error: 'A post with this slug already exists' },
        { status: 409 }
      );
    }

    return Response.json({ error: message }, { status: 500 });
  }
};
```

### Dynamic API Routes

```typescript
// src/pages/api/posts/[id].ts
import type { APIRoute } from 'astro';
import { getDB } from '../../../lib/db';

export const GET: APIRoute = async ({ params, locals }) => {
  const { env } = locals.runtime;
  const db = getDB(env);
  const { id } = params;

  const post = await db.queryOne('SELECT * FROM posts WHERE id = ?', [id]);

  if (!post) {
    return Response.json({ error: 'Post not found' }, { status: 404 });
  }

  return Response.json(post);
};

export const PUT: APIRoute = async ({ params, locals, request }) => {
  const { env } = locals.runtime;
  const db = getDB(env);
  const { id } = params;

  const body = await request.json() as {
    title?: string;
    content?: string;
    excerpt?: string;
    published?: boolean;
  };

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
    values.push(body.published ? 1 : 0);
  }

  if (updates.length === 0) {
    return Response.json({ error: 'No fields to update' }, { status: 400 });
  }

  updates.push('updated_at = CURRENT_TIMESTAMP');
  values.push(id);

  const result = await db.run(
    `UPDATE posts SET ${updates.join(', ')} WHERE id = ?`,
    values
  );

  if (result.rowsAffected === 0) {
    return Response.json({ error: 'Post not found' }, { status: 404 });
  }

  return Response.json({ success: true });
};

export const DELETE: APIRoute = async ({ params, locals }) => {
  const { env } = locals.runtime;
  const db = getDB(env);
  const { id } = params;

  const result = await db.run('DELETE FROM posts WHERE id = ?', [id]);

  if (result.rowsAffected === 0) {
    return Response.json({ error: 'Post not found' }, { status: 404 });
  }

  return Response.json({ success: true });
};
```

### Comments API

```typescript
// src/pages/api/posts/[id]/comments.ts
import type { APIRoute } from 'astro';
import { getDB } from '../../../../lib/db';

export const GET: APIRoute = async ({ params, locals }) => {
  const { env } = locals.runtime;
  const db = getDB(env);
  const { id } = params;

  const comments = await db.query(
    `SELECT id, author_name, content, created_at
     FROM comments
     WHERE post_id = ? AND approved = 1
     ORDER BY created_at ASC`,
    [id]
  );

  return Response.json(comments);
};

export const POST: APIRoute = async ({ params, locals, request }) => {
  const { env } = locals.runtime;
  const db = getDB(env);
  const { id } = params;

  // Verify post exists
  const post = await db.queryOne<{ id: number; slug: string }>(
    'SELECT id, slug FROM posts WHERE id = ?',
    [id]
  );

  if (!post) {
    return Response.json({ error: 'Post not found' }, { status: 404 });
  }

  // Parse form data or JSON
  const contentType = request.headers.get('content-type') || '';
  let body: { author_name: string; content: string };

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
    return Response.json(
      { error: 'Missing required fields: author_name, content' },
      { status: 400 }
    );
  }

  const result = await db.run(
    `INSERT INTO comments (post_id, author_name, content, approved)
     VALUES (?, ?, ?, 0)`,
    [id, body.author_name, body.content]
  );

  // Redirect form submissions back to the post
  if (!contentType.includes('application/json')) {
    return new Response(null, {
      status: 302,
      headers: { Location: `/posts/${post.slug}?comment=pending` },
    });
  }

  return Response.json(
    { id: result.lastInsertRowId, message: 'Comment pending approval' },
    { status: 201 }
  );
};
```

### Search API

```typescript
// src/pages/api/search.ts
import type { APIRoute } from 'astro';
import { getDB } from '../../lib/db';

export const GET: APIRoute = async ({ locals, url }) => {
  const { env } = locals.runtime;
  const db = getDB(env);

  const query = url.searchParams.get('q')?.trim();
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '10', 10), 50);

  if (!query || query.length < 2) {
    return Response.json(
      { error: 'Query must be at least 2 characters' },
      { status: 400 }
    );
  }

  const searchTerm = `%${query}%`;

  const results = await db.query(
    `SELECT id, title, slug, excerpt
     FROM posts
     WHERE published = 1
       AND (title LIKE ? OR content LIKE ? OR excerpt LIKE ?)
     ORDER BY
       CASE WHEN title LIKE ? THEN 0 ELSE 1 END,
       created_at DESC
     LIMIT ?`,
    [searchTerm, searchTerm, searchTerm, searchTerm, limit]
  );

  return Response.json({ query, results, count: results.length });
};
```

---

## Client-Side Integration

### Search Component

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
    display: block;
    padding: 0.75rem 1rem;
    border-bottom: 1px solid #eee;
    text-decoration: none;
    color: inherit;
  }

  .result-item:hover {
    background: #f5f5f5;
  }
</style>

<script>
  const input = document.getElementById('search-input') as HTMLInputElement;
  const results = document.getElementById('search-results')!;

  let debounceTimer: ReturnType<typeof setTimeout>;

  input.addEventListener('input', () => {
    clearTimeout(debounceTimer);

    const query = input.value.trim();
    if (query.length < 2) {
      results.classList.remove('active');
      return;
    }

    debounceTimer = setTimeout(async () => {
      const response = await fetch(`/api/search?q=${encodeURIComponent(query)}`);
      const data = await response.json();

      if (data.results.length === 0) {
        results.innerHTML = '<div class="result-item">No results found</div>';
      } else {
        results.innerHTML = data.results
          .map((post: { title: string; slug: string; excerpt?: string }) => `
            <a href="/posts/${post.slug}" class="result-item">
              <strong>${post.title}</strong>
              ${post.excerpt ? `<p>${post.excerpt}</p>` : ''}
            </a>
          `)
          .join('');
      }

      results.classList.add('active');
    }, 300);
  });

  document.addEventListener('click', (e) => {
    if (!results.contains(e.target as Node) && e.target !== input) {
      results.classList.remove('active');
    }
  });
</script>
```

### React Island with Live Data

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
  const [comments, setComments] = useState(initialComments);
  const [form, setForm] = useState({ author_name: '', content: '' });
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    const interval = setInterval(async () => {
      const response = await fetch(`/api/posts/${postId}/comments`);
      if (response.ok) {
        setComments(await response.json());
      }
    }, 30000);

    return () => clearInterval(interval);
  }, [postId]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);

    const response = await fetch(`/api/posts/${postId}/comments`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(form),
    });

    if (response.ok) {
      setForm({ author_name: '', content: '' });
    }

    setSubmitting(false);
  };

  return (
    <section>
      <h3>Comments ({comments.length})</h3>

      <ul>
        {comments.map((c) => (
          <li key={c.id}>
            <strong>{c.author_name}</strong>
            <p>{c.content}</p>
          </li>
        ))}
      </ul>

      <form onSubmit={handleSubmit}>
        <input
          type="text"
          placeholder="Name"
          value={form.author_name}
          onChange={(e) => setForm({ ...form, author_name: e.target.value })}
          required
        />
        <textarea
          placeholder="Comment"
          value={form.content}
          onChange={(e) => setForm({ ...form, content: e.target.value })}
          required
        />
        <button type="submit" disabled={submitting}>
          {submitting ? 'Submitting...' : 'Submit'}
        </button>
      </form>
    </section>
  );
}
```

Use the React component with hydration:

```astro
---
// src/pages/posts/[slug].astro
import Layout from '../../layouts/Layout.astro';
import LiveComments from '../../components/LiveComments';
import { getDB } from '../../lib/db';

const { slug } = Astro.params;
const { env } = Astro.locals.runtime;
const db = getDB(env);

const post = await db.queryOne('SELECT * FROM posts WHERE slug = ?', [slug]);
const comments = await db.query(
  'SELECT * FROM comments WHERE post_id = ? AND approved = 1',
  [post?.id]
);
---

<Layout title={post?.title || 'Post'}>
  <article>
    <h1>{post?.title}</h1>
    <div set:html={post?.content} />

    <LiveComments
      client:visible
      postId={post?.id}
      initialComments={comments}
    />
  </article>
</Layout>
```

---

## Content Collections with DoSQL

Combine static Content Collections with dynamic database content.

### Content Collection Schema

```typescript
// src/content/config.ts
import { defineCollection, z } from 'astro:content';

const staticPosts = defineCollection({
  type: 'content',
  schema: z.object({
    title: z.string(),
    description: z.string(),
    pubDate: z.coerce.date(),
    tags: z.array(z.string()).optional(),
  }),
});

export const collections = { staticPosts };
```

### Merged Content Page

```astro
---
// src/pages/blog/index.astro
import Layout from '../../layouts/Layout.astro';
import { getCollection } from 'astro:content';
import { getDB } from '../../lib/db';

const staticPosts = await getCollection('staticPosts');

const { env } = Astro.locals.runtime;
const db = getDB(env);

const dynamicPosts = await db.query<{
  id: number;
  title: string;
  slug: string;
  excerpt: string;
  created_at: string;
}>(`
  SELECT id, title, slug, excerpt, created_at
  FROM posts
  WHERE published = 1
`);

type MergedPost = {
  type: 'static' | 'dynamic';
  slug: string;
  title: string;
  description: string;
  date: Date;
};

const allPosts: MergedPost[] = [
  ...staticPosts.map((p) => ({
    type: 'static' as const,
    slug: p.slug,
    title: p.data.title,
    description: p.data.description,
    date: p.data.pubDate,
  })),
  ...dynamicPosts.map((p) => ({
    type: 'dynamic' as const,
    slug: p.slug,
    title: p.title,
    description: p.excerpt,
    date: new Date(p.created_at),
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
  published: number;
  created_at: string;
  updated_at: string;
}

export interface Comment {
  id: number;
  post_id: number;
  author_name: string;
  content: string;
  approved: number;
  created_at: string;
}

export interface PostWithComments extends Post {
  comment_count: number;
}
```

### Typed Query Functions

```typescript
// src/lib/queries.ts
import type { DBClient } from './db';
import type { Post, Comment, PostWithComments } from './types';

export async function getPublishedPosts(
  db: DBClient,
  limit = 10,
  offset = 0
): Promise<Post[]> {
  return db.query<Post>(
    `SELECT * FROM posts
     WHERE published = 1
     ORDER BY created_at DESC
     LIMIT ? OFFSET ?`,
    [limit, offset]
  );
}

export async function getPostBySlug(
  db: DBClient,
  slug: string
): Promise<Post | null> {
  return db.queryOne<Post>(
    'SELECT * FROM posts WHERE slug = ? AND published = 1',
    [slug]
  );
}

export async function getPostComments(
  db: DBClient,
  postId: number
): Promise<Comment[]> {
  return db.query<Comment>(
    `SELECT * FROM comments
     WHERE post_id = ? AND approved = 1
     ORDER BY created_at ASC`,
    [postId]
  );
}

export async function createPost(
  db: DBClient,
  data: Omit<Post, 'id' | 'created_at' | 'updated_at'>
): Promise<number> {
  const result = await db.run(
    `INSERT INTO posts (title, slug, content, excerpt, published)
     VALUES (?, ?, ?, ?, ?)`,
    [data.title, data.slug, data.content, data.excerpt, data.published]
  );
  return result.lastInsertRowId;
}
```

---

## View Transitions

### Layout with View Transitions

```astro
---
// src/layouts/Layout.astro
import { ViewTransitions } from 'astro:transitions';

interface Props {
  title: string;
}

const { title } = Astro.props;
---

<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width" />
    <title>{title}</title>
    <ViewTransitions />
  </head>
  <body>
    <slot />
  </body>
</html>
```

### Named Transitions for Posts

```astro
---
// src/pages/posts/[slug].astro
import Layout from '../../layouts/Layout.astro';
import { getDB } from '../../lib/db';

const { slug } = Astro.params;
const { env } = Astro.locals.runtime;
const db = getDB(env);

const post = await db.queryOne(
  'SELECT * FROM posts WHERE slug = ? AND published = 1',
  [slug]
);

if (!post) return Astro.redirect('/404');
---

<Layout title={post.title}>
  <article transition:name={`post-${post.id}`}>
    <h1 transition:name={`title-${post.id}`}>{post.title}</h1>
    <div class="content" set:html={post.content} />
  </article>
</Layout>
```

---

## Error Handling

### Error Utilities

```typescript
// src/lib/errors.ts
export class AppError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly status: number = 500
  ) {
    super(message);
    this.name = 'AppError';
  }

  static notFound(resource: string) {
    return new AppError(`${resource} not found`, 'NOT_FOUND', 404);
  }

  static validation(message: string) {
    return new AppError(message, 'VALIDATION', 400);
  }

  static conflict(message: string) {
    return new AppError(message, 'CONFLICT', 409);
  }
}

export function handleError(error: unknown): Response {
  if (error instanceof AppError) {
    return Response.json(
      { error: error.message, code: error.code },
      { status: error.status }
    );
  }

  const message = error instanceof Error ? error.message : 'Unknown error';

  if (message.includes('UNIQUE constraint')) {
    return Response.json(
      { error: 'Record already exists', code: 'UNIQUE_VIOLATION' },
      { status: 409 }
    );
  }

  console.error('Unhandled error:', error);
  return Response.json(
    { error: 'Internal server error', code: 'INTERNAL' },
    { status: 500 }
  );
}
```

### Error Page

```astro
---
// src/pages/404.astro
import Layout from '../layouts/Layout.astro';
import { getDB } from '../lib/db';

let suggestions: { title: string; slug: string }[] = [];

try {
  const { env } = Astro.locals.runtime;
  const db = getDB(env);
  suggestions = await db.query(
    'SELECT title, slug FROM posts WHERE published = 1 ORDER BY created_at DESC LIMIT 5'
  );
} catch {
  // Suggestions are optional
}
---

<Layout title="Not Found">
  <main>
    <h1>Page Not Found</h1>
    <p>The page you're looking for doesn't exist.</p>

    {suggestions.length > 0 && (
      <section>
        <h2>Recent Posts</h2>
        <ul>
          {suggestions.map((post) => (
            <li><a href={`/posts/${post.slug}`}>{post.title}</a></li>
          ))}
        </ul>
      </section>
    )}

    <a href="/">Go home</a>
  </main>
</Layout>
```

---

## Local Development

### Development Commands

```bash
# Start dev server with Cloudflare bindings
npm run dev

# Build and preview locally
npm run build
npx wrangler pages dev dist
```

### Seed Script

```typescript
// scripts/seed.ts
import { getDB } from '../src/lib/db';

async function seed(env: Env) {
  const db = getDB(env);

  await db.run('DELETE FROM comments');
  await db.run('DELETE FROM posts');

  const posts = [
    {
      title: 'Getting Started with DoSQL',
      slug: 'getting-started',
      content: '<p>Welcome to DoSQL...</p>',
      excerpt: 'Learn how to build edge-native apps.',
      published: 1,
    },
    {
      title: 'Advanced Patterns',
      slug: 'advanced-patterns',
      content: '<p>Explore advanced DoSQL patterns...</p>',
      excerpt: 'Deep dive into DoSQL features.',
      published: 1,
    },
  ];

  for (const post of posts) {
    const result = await db.run(
      `INSERT INTO posts (title, slug, content, excerpt, published)
       VALUES (?, ?, ?, ?, ?)`,
      [post.title, post.slug, post.content, post.excerpt, post.published]
    );

    await db.run(
      `INSERT INTO comments (post_id, author_name, content, approved)
       VALUES (?, ?, ?, 1)`,
      [result.lastInsertRowId, 'Demo User', 'Great post!']
    );
  }

  console.log('Seed complete');
}
```

---

## Advanced Patterns

### Middleware for Database Access

```typescript
// src/middleware.ts
import { defineMiddleware } from 'astro:middleware';
import { getDB, type DBClient } from './lib/db';

declare global {
  namespace App {
    interface Locals {
      db: DBClient;
    }
  }
}

export const onRequest = defineMiddleware(async ({ locals }, next) => {
  const { env } = locals.runtime;
  locals.db = getDB(env);
  return next();
});
```

Use in pages:

```astro
---
// src/pages/posts/index.astro
import Layout from '../../layouts/Layout.astro';

const { db } = Astro.locals;
const posts = await db.query('SELECT * FROM posts WHERE published = 1');
---

<Layout title="Posts">
  <!-- ... -->
</Layout>
```

### Response Caching

```typescript
// src/pages/api/posts.ts
import type { APIRoute } from 'astro';
import { getDB } from '../../lib/db';

export const GET: APIRoute = async ({ locals }) => {
  const { env } = locals.runtime;
  const db = getDB(env);

  const posts = await db.query(
    'SELECT * FROM posts WHERE published = 1 ORDER BY created_at DESC LIMIT 20'
  );

  return Response.json(posts, {
    headers: {
      'Cache-Control': 'public, max-age=60, stale-while-revalidate=300',
    },
  });
};
```

---

## Deployment

### Build and Deploy

```json
{
  "scripts": {
    "dev": "astro dev",
    "build": "astro build",
    "preview": "wrangler pages dev dist",
    "deploy": "npm run build && wrangler pages deploy dist"
  }
}
```

### Production Wrangler Config

```toml
# wrangler.toml
name = "my-astro-blog"
compatibility_date = "2024-12-01"
compatibility_flags = ["nodejs_compat"]

pages_build_output_dir = "./dist"

[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"
script_name = "dosql-worker"

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]

[vars]
ENVIRONMENT = "production"
```

### GitHub Actions

```yaml
# .github/workflows/deploy.yml
name: Deploy

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'

      - run: npm ci
      - run: npm run build

      - uses: cloudflare/pages-action@v1
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          accountId: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
          projectName: my-astro-blog
          directory: dist
```

---

## Troubleshooting

### "Cannot find runtime" Error

Ensure `platformProxy` is enabled:

```javascript
// astro.config.mjs
export default defineConfig({
  output: 'server',
  adapter: cloudflare({
    platformProxy: { enabled: true },
  }),
});
```

Verify types in `src/env.d.ts`:

```typescript
/// <reference types="astro/client" />

type Runtime = import('@astrojs/cloudflare').Runtime<Env>;

declare namespace App {
  interface Locals extends Runtime {}
}
```

### Static Pages Accessing Database

Static pages cannot access runtime bindings. Mark database pages as server-rendered:

```astro
---
export const prerender = false;
// ... database queries
---
```

### Durable Object Not Found

Check wrangler.toml bindings:

```toml
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "DoSQLDatabase"

[[migrations]]
tag = "v1"
new_classes = ["DoSQLDatabase"]
```

### API Routes 404 in Dev

Use wrangler to serve the built output:

```bash
npm run build
npx wrangler pages dev dist
```

### Performance Tips

1. **Parallel queries** for independent data:

```typescript
const [posts, tags] = await Promise.all([
  db.query('SELECT * FROM posts'),
  db.query('SELECT * FROM tags'),
]);
```

2. **Proper indexes** for common queries:

```sql
CREATE INDEX idx_posts_published ON posts(published, created_at DESC);
```

3. **Use `queryOne`** for single results:

```typescript
const post = await db.queryOne('SELECT * FROM posts WHERE id = ?', [id]);
```

4. **Limit result sets**:

```typescript
const posts = await db.query('SELECT * FROM posts LIMIT 20');
```

---

## Next Steps

- [Getting Started](../getting-started.md) - DoSQL fundamentals
- [API Reference](../api-reference.md) - Complete API documentation
- [Advanced Features](../advanced.md) - CDC, time travel, branching
- [Next.js Integration](./NEXTJS.md) - Next.js patterns
- [Remix Integration](./REMIX.md) - Remix patterns
- [SvelteKit Integration](./SVELTEKIT.md) - SvelteKit patterns
