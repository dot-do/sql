# Astro Integration Guide

**Version**: 1.0.0
**Last Updated**: 2026-01-22
**Maintainer**: Platform Team

This guide covers integrating DoSQL with [Astro](https://astro.build), the modern web framework for content-focused websites with optional interactivity.

---

## Table of Contents

1. [Installation and Setup](#installation-and-setup)
2. [Server-Side Data Loading](#server-side-data-loading)
3. [API Routes with DoSQL](#api-routes-with-dosql)
4. [SSR vs SSG Considerations](#ssr-vs-ssg-considerations)
5. [Edge Deployment with Cloudflare Adapter](#edge-deployment-with-cloudflare-adapter)
6. [Type-Safe Queries](#type-safe-queries)
7. [Real-Time Updates with CDC](#real-time-updates-with-cdc)
8. [Example: Blog with Categories and Tags](#example-blog-with-categories-and-tags)
9. [Example: E-commerce Product Catalog](#example-e-commerce-product-catalog)
10. [Performance Optimization Tips](#performance-optimization-tips)

---

## Installation and Setup

### Prerequisites

- Astro 4.0 or later
- Node.js 18+ or Cloudflare Workers runtime
- DoSQL instance (self-hosted or managed)

### Install Dependencies

```bash
# Using npm
npm install sql.do

# Using pnpm
pnpm add sql.do

# Using yarn
yarn add sql.do
```

### Project Configuration

Create a DoSQL client configuration file:

```typescript
// src/lib/db.ts
import { createSQLClient, type SQLClientConfig } from 'sql.do';

const config: SQLClientConfig = {
  url: import.meta.env.DOSQL_URL,
  token: import.meta.env.DOSQL_TOKEN,
  timeout: 30000,
  retry: {
    maxRetries: 3,
    baseDelayMs: 100,
    maxDelayMs: 5000,
  },
};

// Create a singleton client for SSR
let client: ReturnType<typeof createSQLClient> | null = null;

export function getClient() {
  if (!client) {
    client = createSQLClient(config);
  }
  return client;
}

// For Cloudflare Workers with bindings
export function getClientFromEnv(env: { DOSQL: DurableObjectStub }) {
  return createSQLClient({
    durableObject: env.DOSQL,
  });
}
```

### Environment Variables

Add your DoSQL credentials to `.env`:

```bash
# .env
DOSQL_URL=https://sql.your-domain.com
DOSQL_TOKEN=your-auth-token
```

Update `astro.config.mjs` to expose environment variables:

```javascript
// astro.config.mjs
import { defineConfig } from 'astro/config';

export default defineConfig({
  vite: {
    envPrefix: ['DOSQL_', 'PUBLIC_'],
  },
});
```

---

## Server-Side Data Loading

### Basic Data Fetching in .astro Files

Astro components run on the server by default, making them ideal for database queries:

```astro
---
// src/pages/users.astro
import { getClient } from '../lib/db';
import Layout from '../layouts/Layout.astro';

interface User {
  id: string;
  name: string;
  email: string;
  created_at: number;
}

const client = getClient();
const result = await client.query<User>(
  'SELECT * FROM users WHERE active = ? ORDER BY created_at DESC LIMIT 50',
  [true]
);
const users = result.rows;
---

<Layout title="Users">
  <h1>Active Users</h1>
  <ul>
    {users.map((user) => (
      <li>
        <strong>{user.name}</strong> - {user.email}
        <span class="date">
          Joined: {new Date(user.created_at).toLocaleDateString()}
        </span>
      </li>
    ))}
  </ul>
</Layout>
```

### Fetching Related Data

```astro
---
// src/pages/posts/[slug].astro
import { getClient } from '../../lib/db';
import Layout from '../../layouts/Layout.astro';

interface Post {
  id: string;
  title: string;
  content: string;
  author_name: string;
  published_at: number;
}

interface Comment {
  id: string;
  author_name: string;
  content: string;
  created_at: number;
}

const { slug } = Astro.params;
const client = getClient();

// Fetch post with author information
const postResult = await client.query<Post>(`
  SELECT p.*, u.name as author_name
  FROM posts p
  JOIN users u ON p.author_id = u.id
  WHERE p.slug = ? AND p.published = ?
`, [slug, true]);

if (postResult.rows.length === 0) {
  return Astro.redirect('/404');
}

const post = postResult.rows[0];

// Fetch comments
const commentsResult = await client.query<Comment>(`
  SELECT c.*, u.name as author_name
  FROM comments c
  JOIN users u ON c.user_id = u.id
  WHERE c.post_id = ?
  ORDER BY c.created_at DESC
`, [post.id]);

const comments = commentsResult.rows;
---

<Layout title={post.title}>
  <article>
    <h1>{post.title}</h1>
    <p class="meta">By {post.author_name} on {new Date(post.published_at).toLocaleDateString()}</p>
    <div class="content" set:html={post.content} />
  </article>

  <section class="comments">
    <h2>Comments ({comments.length})</h2>
    {comments.map((comment) => (
      <div class="comment">
        <strong>{comment.author_name}</strong>
        <p>{comment.content}</p>
        <time>{new Date(comment.created_at).toLocaleString()}</time>
      </div>
    ))}
  </section>
</Layout>
```

### Parallel Data Fetching

For better performance, fetch independent data in parallel:

```astro
---
// src/pages/dashboard.astro
import { getClient } from '../lib/db';
import Layout from '../layouts/Layout.astro';

interface Stats {
  total_users: number;
  total_orders: number;
  total_revenue: number;
}

interface RecentOrder {
  id: string;
  customer_name: string;
  total: number;
  created_at: number;
}

interface TopProduct {
  id: string;
  name: string;
  sales_count: number;
}

const client = getClient();

// Fetch all data in parallel
const [statsResult, recentOrdersResult, topProductsResult] = await Promise.all([
  client.query<Stats>(`
    SELECT
      (SELECT COUNT(*) FROM users) as total_users,
      (SELECT COUNT(*) FROM orders) as total_orders,
      (SELECT COALESCE(SUM(total), 0) FROM orders) as total_revenue
  `),
  client.query<RecentOrder>(`
    SELECT o.id, u.name as customer_name, o.total, o.created_at
    FROM orders o
    JOIN users u ON o.user_id = u.id
    ORDER BY o.created_at DESC
    LIMIT 10
  `),
  client.query<TopProduct>(`
    SELECT p.id, p.name, COUNT(oi.id) as sales_count
    FROM products p
    JOIN order_items oi ON p.id = oi.product_id
    GROUP BY p.id, p.name
    ORDER BY sales_count DESC
    LIMIT 5
  `),
]);

const stats = statsResult.rows[0];
const recentOrders = recentOrdersResult.rows;
const topProducts = topProductsResult.rows;
---

<Layout title="Dashboard">
  <div class="dashboard">
    <section class="stats">
      <div class="stat-card">
        <h3>Total Users</h3>
        <p>{stats.total_users.toLocaleString()}</p>
      </div>
      <div class="stat-card">
        <h3>Total Orders</h3>
        <p>{stats.total_orders.toLocaleString()}</p>
      </div>
      <div class="stat-card">
        <h3>Revenue</h3>
        <p>${stats.total_revenue.toLocaleString()}</p>
      </div>
    </section>

    <section class="recent-orders">
      <h2>Recent Orders</h2>
      <table>
        <thead>
          <tr>
            <th>Order ID</th>
            <th>Customer</th>
            <th>Total</th>
            <th>Date</th>
          </tr>
        </thead>
        <tbody>
          {recentOrders.map((order) => (
            <tr>
              <td>{order.id}</td>
              <td>{order.customer_name}</td>
              <td>${order.total.toFixed(2)}</td>
              <td>{new Date(order.created_at).toLocaleDateString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>

    <section class="top-products">
      <h2>Top Products</h2>
      <ol>
        {topProducts.map((product) => (
          <li>
            {product.name} - {product.sales_count} sales
          </li>
        ))}
      </ol>
    </section>
  </div>
</Layout>
```

---

## API Routes with DoSQL

### Basic API Endpoint

Create API routes in the `src/pages/api` directory:

```typescript
// src/pages/api/users.ts
import type { APIRoute } from 'astro';
import { getClient } from '../../lib/db';

interface User {
  id: string;
  name: string;
  email: string;
}

export const GET: APIRoute = async ({ url }) => {
  const client = getClient();
  const limit = Number(url.searchParams.get('limit')) || 20;
  const offset = Number(url.searchParams.get('offset')) || 0;

  try {
    const result = await client.query<User>(
      'SELECT id, name, email FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?',
      [limit, offset]
    );

    return new Response(JSON.stringify({
      users: result.rows,
      pagination: { limit, offset, total: result.rowCount },
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Failed to fetch users:', error);
    return new Response(JSON.stringify({ error: 'Internal server error' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};

export const POST: APIRoute = async ({ request }) => {
  const client = getClient();

  try {
    const body = await request.json();
    const { name, email } = body;

    if (!name || !email) {
      return new Response(JSON.stringify({ error: 'Name and email are required' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const result = await client.query<User>(
      'INSERT INTO users (name, email) VALUES (?, ?) RETURNING id, name, email',
      [name, email]
    );

    return new Response(JSON.stringify({ user: result.rows[0] }), {
      status: 201,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Failed to create user:', error);
    return new Response(JSON.stringify({ error: 'Internal server error' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};
```

### Transactional API Routes

```typescript
// src/pages/api/orders.ts
import type { APIRoute } from 'astro';
import { getClient } from '../../lib/db';

interface OrderItem {
  product_id: string;
  quantity: number;
  price: number;
}

interface CreateOrderRequest {
  user_id: string;
  items: OrderItem[];
}

export const POST: APIRoute = async ({ request }) => {
  const client = getClient();

  try {
    const body: CreateOrderRequest = await request.json();
    const { user_id, items } = body;

    if (!user_id || !items || items.length === 0) {
      return new Response(JSON.stringify({ error: 'Invalid order data' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // Calculate total
    const total = items.reduce((sum, item) => sum + item.price * item.quantity, 0);

    // Execute transaction
    const order = await client.transaction(async (tx) => {
      // Create order
      const orderResult = await tx.query<{ id: string; created_at: number }>(
        'INSERT INTO orders (user_id, total, status) VALUES (?, ?, ?) RETURNING id, created_at',
        [user_id, total, 'pending']
      );
      const orderId = orderResult.rows[0].id;

      // Create order items
      for (const item of items) {
        await tx.exec(
          'INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (?, ?, ?, ?)',
          [orderId, item.product_id, item.quantity, item.price]
        );

        // Update inventory
        await tx.exec(
          'UPDATE products SET inventory = inventory - ? WHERE id = ?',
          [item.quantity, item.product_id]
        );
      }

      return orderResult.rows[0];
    });

    return new Response(JSON.stringify({ order }), {
      status: 201,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Failed to create order:', error);
    return new Response(JSON.stringify({ error: 'Failed to create order' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};
```

### Search API with Full-Text

```typescript
// src/pages/api/search.ts
import type { APIRoute } from 'astro';
import { getClient } from '../../lib/db';

interface SearchResult {
  id: string;
  title: string;
  excerpt: string;
  type: 'post' | 'product' | 'page';
  url: string;
}

export const GET: APIRoute = async ({ url }) => {
  const client = getClient();
  const query = url.searchParams.get('q')?.trim();
  const type = url.searchParams.get('type');
  const limit = Math.min(Number(url.searchParams.get('limit')) || 20, 100);

  if (!query || query.length < 2) {
    return new Response(JSON.stringify({ error: 'Query must be at least 2 characters' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  try {
    // Build search query with optional type filter
    let sql = `
      SELECT id, title, excerpt, type, url
      FROM search_index
      WHERE title LIKE ? OR content LIKE ?
    `;
    const params: unknown[] = [`%${query}%`, `%${query}%`];

    if (type && ['post', 'product', 'page'].includes(type)) {
      sql += ' AND type = ?';
      params.push(type);
    }

    sql += ' ORDER BY relevance DESC LIMIT ?';
    params.push(limit);

    const result = await client.query<SearchResult>(sql, params);

    return new Response(JSON.stringify({
      results: result.rows,
      query,
      count: result.rows.length,
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Search failed:', error);
    return new Response(JSON.stringify({ error: 'Search failed' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};
```

---

## SSR vs SSG Considerations

### Static Site Generation (SSG)

Use `getStaticPaths` for pre-rendered pages with database data:

```astro
---
// src/pages/posts/[slug].astro
import { getClient } from '../../lib/db';
import Layout from '../../layouts/Layout.astro';

export async function getStaticPaths() {
  const client = getClient();
  const result = await client.query<{ slug: string }>(
    'SELECT slug FROM posts WHERE published = ?',
    [true]
  );

  return result.rows.map((post) => ({
    params: { slug: post.slug },
  }));
}

interface Post {
  id: string;
  title: string;
  content: string;
  slug: string;
  published_at: number;
}

const { slug } = Astro.params;
const client = getClient();
const result = await client.query<Post>(
  'SELECT * FROM posts WHERE slug = ?',
  [slug]
);
const post = result.rows[0];
---

<Layout title={post.title}>
  <article>
    <h1>{post.title}</h1>
    <div set:html={post.content} />
  </article>
</Layout>
```

### When to Use SSG vs SSR

| Scenario | Recommended Mode | Reason |
|----------|-----------------|--------|
| Blog posts | SSG | Content rarely changes, benefits from CDN caching |
| Product catalog | SSG with ISR | Products change infrequently |
| User dashboard | SSR | Personalized, real-time data |
| Shopping cart | SSR | Session-specific data |
| Search results | SSR | Dynamic, user-specific |
| Marketing pages | SSG | Static content, fast loading |

### Hybrid Mode Configuration

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

Mark pages for SSR explicitly:

```astro
---
// src/pages/dashboard.astro
export const prerender = false; // Force SSR for this page

import { getClient } from '../lib/db';
// ... rest of component
---
```

### Incremental Static Regeneration (ISR)

Implement ISR-like behavior with cache headers:

```typescript
// src/pages/api/products/[id].ts
import type { APIRoute } from 'astro';
import { getClient } from '../../../lib/db';

export const GET: APIRoute = async ({ params }) => {
  const client = getClient();
  const result = await client.query(
    'SELECT * FROM products WHERE id = ?',
    [params.id]
  );

  if (result.rows.length === 0) {
    return new Response(null, { status: 404 });
  }

  return new Response(JSON.stringify(result.rows[0]), {
    status: 200,
    headers: {
      'Content-Type': 'application/json',
      // Cache for 1 hour, revalidate in background
      'Cache-Control': 'public, s-maxage=3600, stale-while-revalidate=86400',
    },
  });
};
```

---

## Edge Deployment with Cloudflare Adapter

### Install Cloudflare Adapter

```bash
npm install @astrojs/cloudflare
```

### Configure for Cloudflare Workers

```javascript
// astro.config.mjs
import { defineConfig } from 'astro/config';
import cloudflare from '@astrojs/cloudflare';

export default defineConfig({
  output: 'server',
  adapter: cloudflare({
    mode: 'directory',
    runtime: {
      mode: 'local',
      type: 'pages',
    },
  }),
});
```

### Access Cloudflare Bindings

```typescript
// src/lib/db.ts
import { createSQLClient } from 'sql.do';

export function getClientFromRuntime(runtime: App.Locals['runtime']) {
  const env = runtime.env;

  // Use Durable Object binding directly
  if (env.DOSQL) {
    return createSQLClient({
      durableObject: env.DOSQL,
    });
  }

  // Fallback to HTTP
  return createSQLClient({
    url: env.DOSQL_URL,
    token: env.DOSQL_TOKEN,
  });
}
```

### Using Bindings in Components

```astro
---
// src/pages/users.astro
import { getClientFromRuntime } from '../lib/db';
import Layout from '../layouts/Layout.astro';

const runtime = Astro.locals.runtime;
const client = getClientFromRuntime(runtime);

const result = await client.query('SELECT * FROM users LIMIT 50');
const users = result.rows;
---

<Layout title="Users">
  <ul>
    {users.map((user) => (
      <li>{user.name}</li>
    ))}
  </ul>
</Layout>
```

### Wrangler Configuration

```toml
# wrangler.toml
name = "my-astro-app"
main = "dist/_worker.js"
compatibility_date = "2026-01-15"
compatibility_flags = ["nodejs_compat"]

[[durable_objects.bindings]]
name = "DOSQL"
class_name = "DoSQL"
script_name = "dosql-worker"

[vars]
ENVIRONMENT = "production"
```

### TypeScript Types for Bindings

```typescript
// src/env.d.ts
/// <reference types="astro/client" />

type Runtime = import('@astrojs/cloudflare').Runtime<Env>;

interface Env {
  DOSQL: DurableObjectNamespace;
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
  ENVIRONMENT: string;
}

declare namespace App {
  interface Locals extends Runtime {}
}
```

---

## Type-Safe Queries

### Define Database Types

Create a central types file for your schema:

```typescript
// src/types/database.ts

// Table types
export interface User {
  id: string;
  name: string;
  email: string;
  password_hash: string;
  role: 'admin' | 'user' | 'guest';
  created_at: number;
  updated_at: number;
}

export interface Post {
  id: string;
  title: string;
  slug: string;
  content: string;
  excerpt: string | null;
  author_id: string;
  category_id: string;
  published: boolean;
  published_at: number | null;
  created_at: number;
  updated_at: number;
}

export interface Category {
  id: string;
  name: string;
  slug: string;
  description: string | null;
  parent_id: string | null;
}

export interface Tag {
  id: string;
  name: string;
  slug: string;
}

export interface PostTag {
  post_id: string;
  tag_id: string;
}

// View/query result types
export interface PostWithAuthor extends Post {
  author_name: string;
  author_email: string;
}

export interface PostWithDetails extends PostWithAuthor {
  category_name: string;
  tags: string[];
}

export interface UserWithStats extends User {
  post_count: number;
  comment_count: number;
}
```

### Type-Safe Query Functions

```typescript
// src/lib/queries.ts
import { getClient } from './db';
import type {
  User,
  Post,
  PostWithAuthor,
  PostWithDetails,
  Category,
  Tag,
} from '../types/database';

const client = getClient();

// Users
export async function getUserById(id: string): Promise<User | null> {
  const result = await client.query<User>(
    'SELECT * FROM users WHERE id = ?',
    [id]
  );
  return result.rows[0] ?? null;
}

export async function getUserByEmail(email: string): Promise<User | null> {
  const result = await client.query<User>(
    'SELECT * FROM users WHERE email = ?',
    [email]
  );
  return result.rows[0] ?? null;
}

export async function createUser(
  data: Omit<User, 'id' | 'created_at' | 'updated_at'>
): Promise<User> {
  const result = await client.query<User>(
    `INSERT INTO users (name, email, password_hash, role)
     VALUES (?, ?, ?, ?)
     RETURNING *`,
    [data.name, data.email, data.password_hash, data.role]
  );
  return result.rows[0];
}

// Posts
export async function getPublishedPosts(
  limit = 20,
  offset = 0
): Promise<PostWithAuthor[]> {
  const result = await client.query<PostWithAuthor>(
    `SELECT p.*, u.name as author_name, u.email as author_email
     FROM posts p
     JOIN users u ON p.author_id = u.id
     WHERE p.published = ?
     ORDER BY p.published_at DESC
     LIMIT ? OFFSET ?`,
    [true, limit, offset]
  );
  return result.rows;
}

export async function getPostBySlug(slug: string): Promise<PostWithDetails | null> {
  const postResult = await client.query<PostWithAuthor>(
    `SELECT p.*, u.name as author_name, u.email as author_email,
            c.name as category_name
     FROM posts p
     JOIN users u ON p.author_id = u.id
     JOIN categories c ON p.category_id = c.id
     WHERE p.slug = ? AND p.published = ?`,
    [slug, true]
  );

  if (postResult.rows.length === 0) {
    return null;
  }

  const post = postResult.rows[0];

  // Fetch tags
  const tagsResult = await client.query<{ name: string }>(
    `SELECT t.name
     FROM tags t
     JOIN post_tags pt ON t.id = pt.tag_id
     WHERE pt.post_id = ?`,
    [post.id]
  );

  return {
    ...post,
    tags: tagsResult.rows.map((t) => t.name),
  };
}

export async function getPostsByCategory(
  categorySlug: string,
  limit = 20
): Promise<PostWithAuthor[]> {
  const result = await client.query<PostWithAuthor>(
    `SELECT p.*, u.name as author_name, u.email as author_email
     FROM posts p
     JOIN users u ON p.author_id = u.id
     JOIN categories c ON p.category_id = c.id
     WHERE c.slug = ? AND p.published = ?
     ORDER BY p.published_at DESC
     LIMIT ?`,
    [categorySlug, true, limit]
  );
  return result.rows;
}

export async function getPostsByTag(
  tagSlug: string,
  limit = 20
): Promise<PostWithAuthor[]> {
  const result = await client.query<PostWithAuthor>(
    `SELECT p.*, u.name as author_name, u.email as author_email
     FROM posts p
     JOIN users u ON p.author_id = u.id
     JOIN post_tags pt ON p.id = pt.post_id
     JOIN tags t ON pt.tag_id = t.id
     WHERE t.slug = ? AND p.published = ?
     ORDER BY p.published_at DESC
     LIMIT ?`,
    [tagSlug, true, limit]
  );
  return result.rows;
}

// Categories
export async function getAllCategories(): Promise<Category[]> {
  const result = await client.query<Category>(
    'SELECT * FROM categories ORDER BY name'
  );
  return result.rows;
}

// Tags
export async function getAllTags(): Promise<Tag[]> {
  const result = await client.query<Tag>(
    'SELECT * FROM tags ORDER BY name'
  );
  return result.rows;
}

export async function getPopularTags(limit = 10): Promise<(Tag & { count: number })[]> {
  const result = await client.query<Tag & { count: number }>(
    `SELECT t.*, COUNT(pt.post_id) as count
     FROM tags t
     JOIN post_tags pt ON t.id = pt.tag_id
     GROUP BY t.id
     ORDER BY count DESC
     LIMIT ?`,
    [limit]
  );
  return result.rows;
}
```

### Using Type-Safe Queries in Components

```astro
---
// src/pages/blog/[slug].astro
import { getPostBySlug, getPopularTags } from '../../lib/queries';
import Layout from '../../layouts/Layout.astro';

const { slug } = Astro.params;

const [post, popularTags] = await Promise.all([
  getPostBySlug(slug!),
  getPopularTags(5),
]);

if (!post) {
  return Astro.redirect('/404');
}
---

<Layout title={post.title}>
  <article>
    <header>
      <h1>{post.title}</h1>
      <p class="meta">
        By <a href={`/author/${post.author_id}`}>{post.author_name}</a>
        in <a href={`/category/${post.category_name.toLowerCase()}`}>{post.category_name}</a>
      </p>
      <div class="tags">
        {post.tags.map((tag) => (
          <a href={`/tag/${tag.toLowerCase()}`} class="tag">{tag}</a>
        ))}
      </div>
    </header>
    <div class="content" set:html={post.content} />
  </article>

  <aside>
    <h3>Popular Tags</h3>
    <ul>
      {popularTags.map((tag) => (
        <li>
          <a href={`/tag/${tag.slug}`}>{tag.name}</a>
          <span>({tag.count})</span>
        </li>
      ))}
    </ul>
  </aside>
</Layout>
```

---

## Real-Time Updates with CDC

### CDC Client Setup

```typescript
// src/lib/cdc.ts
import { createCDCClient, type CDCEvent } from 'sql.do';

export function createCDCSubscription(
  tables: string[],
  onEvent: (event: CDCEvent) => void
) {
  const client = createCDCClient({
    url: import.meta.env.DOSQL_CDC_URL || import.meta.env.DOSQL_URL,
    token: import.meta.env.DOSQL_TOKEN,
  });

  const subscription = client.subscribe({
    tables,
    startFrom: 'latest',
    onEvent,
    onError: (error) => {
      console.error('CDC error:', error);
    },
  });

  return subscription;
}
```

### Server-Sent Events Endpoint

```typescript
// src/pages/api/events/posts.ts
import type { APIRoute } from 'astro';
import { createCDCSubscription } from '../../../lib/cdc';

export const GET: APIRoute = async ({ request }) => {
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    start(controller) {
      const subscription = createCDCSubscription(['posts'], (event) => {
        const data = JSON.stringify({
          type: event.operation,
          table: event.table,
          data: event.after || event.before,
          timestamp: event.timestamp,
        });

        controller.enqueue(encoder.encode(`data: ${data}\n\n`));
      });

      // Handle client disconnect
      request.signal.addEventListener('abort', () => {
        subscription.unsubscribe();
        controller.close();
      });
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

### Client-Side Event Listener

```astro
---
// src/pages/admin/posts.astro
import { getPublishedPosts } from '../../lib/queries';
import Layout from '../../layouts/Layout.astro';

const posts = await getPublishedPosts(50);
---

<Layout title="Manage Posts">
  <h1>Posts</h1>
  <div id="posts-list">
    {posts.map((post) => (
      <div class="post-item" data-id={post.id}>
        <h3>{post.title}</h3>
        <p>By {post.author_name}</p>
      </div>
    ))}
  </div>

  <div id="toast" class="toast hidden"></div>
</Layout>

<script>
  const postsList = document.getElementById('posts-list');
  const toast = document.getElementById('toast');

  function showToast(message: string) {
    if (toast) {
      toast.textContent = message;
      toast.classList.remove('hidden');
      setTimeout(() => toast.classList.add('hidden'), 3000);
    }
  }

  // Connect to CDC stream
  const eventSource = new EventSource('/api/events/posts');

  eventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);

    switch (data.type) {
      case 'INSERT':
        showToast(`New post: ${data.data.title}`);
        // Optionally refresh the list
        window.location.reload();
        break;

      case 'UPDATE':
        const postEl = document.querySelector(`[data-id="${data.data.id}"]`);
        if (postEl) {
          const titleEl = postEl.querySelector('h3');
          if (titleEl) titleEl.textContent = data.data.title;
        }
        showToast(`Post updated: ${data.data.title}`);
        break;

      case 'DELETE':
        const deletedEl = document.querySelector(`[data-id="${data.data.id}"]`);
        if (deletedEl) deletedEl.remove();
        showToast('Post deleted');
        break;
    }
  };

  eventSource.onerror = () => {
    console.error('CDC connection error');
    // Reconnect logic handled by browser
  };

  // Cleanup on page unload
  window.addEventListener('beforeunload', () => {
    eventSource.close();
  });
</script>

<style>
  .toast {
    position: fixed;
    bottom: 20px;
    right: 20px;
    padding: 12px 24px;
    background: #333;
    color: white;
    border-radius: 4px;
    transition: opacity 0.3s;
  }
  .toast.hidden {
    opacity: 0;
    pointer-events: none;
  }
</style>
```

### Real-Time Dashboard with Islands

```astro
---
// src/pages/dashboard.astro
import Layout from '../layouts/Layout.astro';
import LiveStats from '../components/LiveStats';
import RecentActivity from '../components/RecentActivity';

export const prerender = false;
---

<Layout title="Dashboard">
  <h1>Real-Time Dashboard</h1>

  <div class="dashboard-grid">
    <LiveStats client:load />
    <RecentActivity client:load />
  </div>
</Layout>
```

```tsx
// src/components/LiveStats.tsx
import { useEffect, useState } from 'react';

interface Stats {
  totalUsers: number;
  totalOrders: number;
  activeUsers: number;
}

export default function LiveStats() {
  const [stats, setStats] = useState<Stats>({
    totalUsers: 0,
    totalOrders: 0,
    activeUsers: 0,
  });

  useEffect(() => {
    // Initial fetch
    fetch('/api/stats')
      .then((res) => res.json())
      .then(setStats);

    // Subscribe to updates
    const eventSource = new EventSource('/api/events/stats');

    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);
      setStats((prev) => ({ ...prev, ...data }));
    };

    return () => eventSource.close();
  }, []);

  return (
    <div className="stats-grid">
      <div className="stat-card">
        <h3>Total Users</h3>
        <p className="stat-value">{stats.totalUsers.toLocaleString()}</p>
      </div>
      <div className="stat-card">
        <h3>Total Orders</h3>
        <p className="stat-value">{stats.totalOrders.toLocaleString()}</p>
      </div>
      <div className="stat-card">
        <h3>Active Users</h3>
        <p className="stat-value">{stats.activeUsers.toLocaleString()}</p>
      </div>
    </div>
  );
}
```

---

## Example: Blog with Categories and Tags

### Database Schema

```sql
-- Create tables
CREATE TABLE categories (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  description TEXT,
  parent_id TEXT REFERENCES categories(id)
);

CREATE TABLE tags (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE
);

CREATE TABLE posts (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  content TEXT NOT NULL,
  excerpt TEXT,
  author_id TEXT NOT NULL REFERENCES users(id),
  category_id TEXT NOT NULL REFERENCES categories(id),
  featured_image TEXT,
  published INTEGER DEFAULT 0,
  published_at INTEGER,
  created_at INTEGER DEFAULT (unixepoch() * 1000),
  updated_at INTEGER DEFAULT (unixepoch() * 1000)
);

CREATE TABLE post_tags (
  post_id TEXT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  tag_id TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
  PRIMARY KEY (post_id, tag_id)
);

-- Indexes
CREATE INDEX idx_posts_published ON posts(published, published_at DESC);
CREATE INDEX idx_posts_category ON posts(category_id);
CREATE INDEX idx_posts_author ON posts(author_id);
CREATE INDEX idx_post_tags_tag ON post_tags(tag_id);
```

### Blog Layout Component

```astro
---
// src/layouts/BlogLayout.astro
import { getAllCategories, getPopularTags } from '../lib/queries';
import BaseLayout from './BaseLayout.astro';

interface Props {
  title: string;
  description?: string;
}

const { title, description } = Astro.props;

const [categories, popularTags] = await Promise.all([
  getAllCategories(),
  getPopularTags(10),
]);
---

<BaseLayout title={title} description={description}>
  <div class="blog-layout">
    <main class="content">
      <slot />
    </main>

    <aside class="sidebar">
      <section class="categories">
        <h3>Categories</h3>
        <ul>
          {categories.map((cat) => (
            <li>
              <a href={`/category/${cat.slug}`}>{cat.name}</a>
            </li>
          ))}
        </ul>
      </section>

      <section class="tags">
        <h3>Popular Tags</h3>
        <div class="tag-cloud">
          {popularTags.map((tag) => (
            <a href={`/tag/${tag.slug}`} class="tag" data-count={tag.count}>
              {tag.name}
            </a>
          ))}
        </div>
      </section>
    </aside>
  </div>
</BaseLayout>

<style>
  .blog-layout {
    display: grid;
    grid-template-columns: 1fr 300px;
    gap: 2rem;
    max-width: 1200px;
    margin: 0 auto;
    padding: 2rem;
  }

  @media (max-width: 768px) {
    .blog-layout {
      grid-template-columns: 1fr;
    }
  }

  .sidebar section {
    margin-bottom: 2rem;
  }

  .tag-cloud {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
  }

  .tag {
    padding: 0.25rem 0.75rem;
    background: #f0f0f0;
    border-radius: 4px;
    text-decoration: none;
    font-size: 0.875rem;
  }

  .tag:hover {
    background: #e0e0e0;
  }
</style>
```

### Blog Index Page

```astro
---
// src/pages/blog/index.astro
import { getPublishedPosts } from '../../lib/queries';
import BlogLayout from '../../layouts/BlogLayout.astro';
import PostCard from '../../components/PostCard.astro';
import Pagination from '../../components/Pagination.astro';

const page = Number(Astro.url.searchParams.get('page')) || 1;
const pageSize = 12;

const posts = await getPublishedPosts(pageSize + 1, (page - 1) * pageSize);
const hasMore = posts.length > pageSize;
const displayPosts = posts.slice(0, pageSize);
---

<BlogLayout title="Blog" description="Latest articles and tutorials">
  <h1>Latest Posts</h1>

  <div class="posts-grid">
    {displayPosts.map((post) => (
      <PostCard post={post} />
    ))}
  </div>

  <Pagination
    currentPage={page}
    hasNext={hasMore}
    hasPrev={page > 1}
    baseUrl="/blog"
  />
</BlogLayout>

<style>
  .posts-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 1.5rem;
  }
</style>
```

### Category Page

```astro
---
// src/pages/category/[slug].astro
import { getPostsByCategory, getAllCategories } from '../../lib/queries';
import BlogLayout from '../../layouts/BlogLayout.astro';
import PostCard from '../../components/PostCard.astro';

export async function getStaticPaths() {
  const categories = await getAllCategories();
  return categories.map((cat) => ({
    params: { slug: cat.slug },
    props: { category: cat },
  }));
}

const { slug } = Astro.params;
const { category } = Astro.props;

const posts = await getPostsByCategory(slug!, 50);
---

<BlogLayout title={`${category.name} - Blog`} description={category.description}>
  <header class="category-header">
    <h1>{category.name}</h1>
    {category.description && <p>{category.description}</p>}
    <p class="post-count">{posts.length} posts</p>
  </header>

  <div class="posts-grid">
    {posts.map((post) => (
      <PostCard post={post} />
    ))}
  </div>
</BlogLayout>
```

### Tag Page

```astro
---
// src/pages/tag/[slug].astro
import { getPostsByTag, getAllTags } from '../../lib/queries';
import BlogLayout from '../../layouts/BlogLayout.astro';
import PostCard from '../../components/PostCard.astro';

export async function getStaticPaths() {
  const tags = await getAllTags();
  return tags.map((tag) => ({
    params: { slug: tag.slug },
    props: { tag },
  }));
}

const { slug } = Astro.params;
const { tag } = Astro.props;

const posts = await getPostsByTag(slug!, 50);
---

<BlogLayout title={`#${tag.name} - Blog`}>
  <header class="tag-header">
    <h1>#{tag.name}</h1>
    <p class="post-count">{posts.length} posts tagged</p>
  </header>

  <div class="posts-grid">
    {posts.map((post) => (
      <PostCard post={post} />
    ))}
  </div>
</BlogLayout>
```

### Post Card Component

```astro
---
// src/components/PostCard.astro
import type { PostWithAuthor } from '../types/database';

interface Props {
  post: PostWithAuthor;
}

const { post } = Astro.props;
const publishedDate = new Date(post.published_at!).toLocaleDateString('en-US', {
  year: 'numeric',
  month: 'long',
  day: 'numeric',
});
---

<article class="post-card">
  {post.featured_image && (
    <img
      src={post.featured_image}
      alt={post.title}
      class="featured-image"
      loading="lazy"
    />
  )}
  <div class="post-content">
    <h2>
      <a href={`/blog/${post.slug}`}>{post.title}</a>
    </h2>
    {post.excerpt && <p class="excerpt">{post.excerpt}</p>}
    <footer class="post-meta">
      <span class="author">By {post.author_name}</span>
      <time datetime={new Date(post.published_at!).toISOString()}>
        {publishedDate}
      </time>
    </footer>
  </div>
</article>

<style>
  .post-card {
    background: white;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    transition: transform 0.2s, box-shadow 0.2s;
  }

  .post-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  }

  .featured-image {
    width: 100%;
    height: 200px;
    object-fit: cover;
  }

  .post-content {
    padding: 1rem;
  }

  h2 {
    margin: 0 0 0.5rem;
    font-size: 1.25rem;
  }

  h2 a {
    color: inherit;
    text-decoration: none;
  }

  h2 a:hover {
    color: #0066cc;
  }

  .excerpt {
    color: #666;
    font-size: 0.9rem;
    line-height: 1.5;
    margin: 0 0 1rem;
  }

  .post-meta {
    display: flex;
    justify-content: space-between;
    font-size: 0.8rem;
    color: #888;
  }
</style>
```

### Single Post Page

```astro
---
// src/pages/blog/[slug].astro
import { getPostBySlug, getPublishedPosts } from '../../lib/queries';
import BlogLayout from '../../layouts/BlogLayout.astro';

export async function getStaticPaths() {
  const posts = await getPublishedPosts(1000);
  return posts.map((post) => ({
    params: { slug: post.slug },
  }));
}

const { slug } = Astro.params;
const post = await getPostBySlug(slug!);

if (!post) {
  return Astro.redirect('/404');
}

const publishedDate = new Date(post.published_at!).toLocaleDateString('en-US', {
  year: 'numeric',
  month: 'long',
  day: 'numeric',
});
---

<BlogLayout title={post.title} description={post.excerpt || undefined}>
  <article class="post">
    <header>
      <h1>{post.title}</h1>
      <div class="meta">
        <span class="author">
          By <a href={`/author/${post.author_id}`}>{post.author_name}</a>
        </span>
        <time datetime={new Date(post.published_at!).toISOString()}>
          {publishedDate}
        </time>
        <a href={`/category/${post.category_name.toLowerCase()}`} class="category">
          {post.category_name}
        </a>
      </div>
      <div class="tags">
        {post.tags.map((tag) => (
          <a href={`/tag/${tag.toLowerCase()}`} class="tag">#{tag}</a>
        ))}
      </div>
    </header>

    {post.featured_image && (
      <img
        src={post.featured_image}
        alt={post.title}
        class="featured-image"
      />
    )}

    <div class="content" set:html={post.content} />
  </article>
</BlogLayout>

<style>
  .post {
    max-width: 800px;
  }

  header {
    margin-bottom: 2rem;
  }

  h1 {
    font-size: 2.5rem;
    margin-bottom: 1rem;
    line-height: 1.2;
  }

  .meta {
    display: flex;
    gap: 1rem;
    color: #666;
    margin-bottom: 1rem;
  }

  .tags {
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
  }

  .tag {
    color: #0066cc;
    text-decoration: none;
  }

  .tag:hover {
    text-decoration: underline;
  }

  .featured-image {
    width: 100%;
    max-height: 500px;
    object-fit: cover;
    border-radius: 8px;
    margin-bottom: 2rem;
  }

  .content {
    font-size: 1.1rem;
    line-height: 1.8;
  }

  .content :global(h2) {
    margin-top: 2rem;
  }

  .content :global(pre) {
    background: #f5f5f5;
    padding: 1rem;
    border-radius: 4px;
    overflow-x: auto;
  }

  .content :global(img) {
    max-width: 100%;
    border-radius: 4px;
  }
</style>
```

---

## Example: E-commerce Product Catalog

### Database Schema

```sql
-- Products
CREATE TABLE products (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  description TEXT,
  price REAL NOT NULL,
  compare_at_price REAL,
  sku TEXT UNIQUE,
  inventory INTEGER DEFAULT 0,
  category_id TEXT REFERENCES product_categories(id),
  brand_id TEXT REFERENCES brands(id),
  status TEXT DEFAULT 'draft' CHECK (status IN ('draft', 'active', 'archived')),
  featured INTEGER DEFAULT 0,
  created_at INTEGER DEFAULT (unixepoch() * 1000),
  updated_at INTEGER DEFAULT (unixepoch() * 1000)
);

-- Product images
CREATE TABLE product_images (
  id TEXT PRIMARY KEY,
  product_id TEXT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  alt_text TEXT,
  position INTEGER DEFAULT 0
);

-- Product variants
CREATE TABLE product_variants (
  id TEXT PRIMARY KEY,
  product_id TEXT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  sku TEXT UNIQUE,
  price REAL NOT NULL,
  inventory INTEGER DEFAULT 0,
  options TEXT -- JSON: {"size": "M", "color": "Blue"}
);

-- Product categories
CREATE TABLE product_categories (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  description TEXT,
  parent_id TEXT REFERENCES product_categories(id),
  image_url TEXT
);

-- Brands
CREATE TABLE brands (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  logo_url TEXT,
  description TEXT
);

-- Product attributes for filtering
CREATE TABLE product_attributes (
  id TEXT PRIMARY KEY,
  product_id TEXT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  value TEXT NOT NULL
);

-- Indexes
CREATE INDEX idx_products_status ON products(status);
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_brand ON products(brand_id);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_product_attributes ON product_attributes(name, value);
```

### Product Types

```typescript
// src/types/ecommerce.ts

export interface Product {
  id: string;
  name: string;
  slug: string;
  description: string | null;
  price: number;
  compare_at_price: number | null;
  sku: string | null;
  inventory: number;
  category_id: string | null;
  brand_id: string | null;
  status: 'draft' | 'active' | 'archived';
  featured: boolean;
  created_at: number;
  updated_at: number;
}

export interface ProductImage {
  id: string;
  product_id: string;
  url: string;
  alt_text: string | null;
  position: number;
}

export interface ProductVariant {
  id: string;
  product_id: string;
  name: string;
  sku: string | null;
  price: number;
  inventory: number;
  options: Record<string, string>;
}

export interface ProductCategory {
  id: string;
  name: string;
  slug: string;
  description: string | null;
  parent_id: string | null;
  image_url: string | null;
}

export interface Brand {
  id: string;
  name: string;
  slug: string;
  logo_url: string | null;
  description: string | null;
}

export interface ProductWithDetails extends Product {
  category_name: string | null;
  brand_name: string | null;
  images: ProductImage[];
  variants: ProductVariant[];
}

export interface ProductFilter {
  category?: string;
  brand?: string;
  minPrice?: number;
  maxPrice?: number;
  inStock?: boolean;
  attributes?: Record<string, string[]>;
  search?: string;
  sort?: 'price_asc' | 'price_desc' | 'newest' | 'name';
}
```

### Product Query Functions

```typescript
// src/lib/ecommerce-queries.ts
import { getClient } from './db';
import type {
  Product,
  ProductWithDetails,
  ProductCategory,
  Brand,
  ProductFilter,
  ProductImage,
  ProductVariant,
} from '../types/ecommerce';

const client = getClient();

export async function getProducts(
  filter: ProductFilter = {},
  limit = 24,
  offset = 0
): Promise<{ products: Product[]; total: number }> {
  const conditions: string[] = ["status = 'active'"];
  const params: unknown[] = [];

  if (filter.category) {
    conditions.push('c.slug = ?');
    params.push(filter.category);
  }

  if (filter.brand) {
    conditions.push('b.slug = ?');
    params.push(filter.brand);
  }

  if (filter.minPrice !== undefined) {
    conditions.push('p.price >= ?');
    params.push(filter.minPrice);
  }

  if (filter.maxPrice !== undefined) {
    conditions.push('p.price <= ?');
    params.push(filter.maxPrice);
  }

  if (filter.inStock) {
    conditions.push('p.inventory > 0');
  }

  if (filter.search) {
    conditions.push('(p.name LIKE ? OR p.description LIKE ?)');
    params.push(`%${filter.search}%`, `%${filter.search}%`);
  }

  const whereClause = conditions.join(' AND ');

  let orderBy = 'p.created_at DESC';
  switch (filter.sort) {
    case 'price_asc':
      orderBy = 'p.price ASC';
      break;
    case 'price_desc':
      orderBy = 'p.price DESC';
      break;
    case 'name':
      orderBy = 'p.name ASC';
      break;
  }

  // Get total count
  const countResult = await client.query<{ count: number }>(
    `SELECT COUNT(*) as count
     FROM products p
     LEFT JOIN product_categories c ON p.category_id = c.id
     LEFT JOIN brands b ON p.brand_id = b.id
     WHERE ${whereClause}`,
    params
  );

  // Get products
  const productsResult = await client.query<Product>(
    `SELECT p.*
     FROM products p
     LEFT JOIN product_categories c ON p.category_id = c.id
     LEFT JOIN brands b ON p.brand_id = b.id
     WHERE ${whereClause}
     ORDER BY ${orderBy}
     LIMIT ? OFFSET ?`,
    [...params, limit, offset]
  );

  return {
    products: productsResult.rows,
    total: countResult.rows[0].count,
  };
}

export async function getProductBySlug(slug: string): Promise<ProductWithDetails | null> {
  const productResult = await client.query<Product & { category_name: string | null; brand_name: string | null }>(
    `SELECT p.*, c.name as category_name, b.name as brand_name
     FROM products p
     LEFT JOIN product_categories c ON p.category_id = c.id
     LEFT JOIN brands b ON p.brand_id = b.id
     WHERE p.slug = ? AND p.status = 'active'`,
    [slug]
  );

  if (productResult.rows.length === 0) {
    return null;
  }

  const product = productResult.rows[0];

  // Fetch images and variants in parallel
  const [imagesResult, variantsResult] = await Promise.all([
    client.query<ProductImage>(
      'SELECT * FROM product_images WHERE product_id = ? ORDER BY position',
      [product.id]
    ),
    client.query<ProductVariant & { options: string }>(
      'SELECT * FROM product_variants WHERE product_id = ?',
      [product.id]
    ),
  ]);

  return {
    ...product,
    images: imagesResult.rows,
    variants: variantsResult.rows.map((v) => ({
      ...v,
      options: JSON.parse(v.options || '{}'),
    })),
  };
}

export async function getFeaturedProducts(limit = 8): Promise<Product[]> {
  const result = await client.query<Product>(
    `SELECT * FROM products
     WHERE status = 'active' AND featured = 1
     ORDER BY updated_at DESC
     LIMIT ?`,
    [limit]
  );
  return result.rows;
}

export async function getRelatedProducts(
  productId: string,
  categoryId: string | null,
  limit = 4
): Promise<Product[]> {
  if (!categoryId) {
    return [];
  }

  const result = await client.query<Product>(
    `SELECT * FROM products
     WHERE category_id = ? AND id != ? AND status = 'active'
     ORDER BY RANDOM()
     LIMIT ?`,
    [categoryId, productId, limit]
  );
  return result.rows;
}

export async function getCategories(): Promise<ProductCategory[]> {
  const result = await client.query<ProductCategory>(
    'SELECT * FROM product_categories ORDER BY name'
  );
  return result.rows;
}

export async function getCategoryBySlug(slug: string): Promise<ProductCategory | null> {
  const result = await client.query<ProductCategory>(
    'SELECT * FROM product_categories WHERE slug = ?',
    [slug]
  );
  return result.rows[0] ?? null;
}

export async function getBrands(): Promise<Brand[]> {
  const result = await client.query<Brand>(
    'SELECT * FROM brands ORDER BY name'
  );
  return result.rows;
}

export async function getPriceRange(): Promise<{ min: number; max: number }> {
  const result = await client.query<{ min_price: number; max_price: number }>(
    `SELECT MIN(price) as min_price, MAX(price) as max_price
     FROM products WHERE status = 'active'`
  );
  return {
    min: result.rows[0].min_price,
    max: result.rows[0].max_price,
  };
}
```

### Product Listing Page

```astro
---
// src/pages/products/index.astro
import { getProducts, getCategories, getBrands, getPriceRange } from '../../lib/ecommerce-queries';
import ShopLayout from '../../layouts/ShopLayout.astro';
import ProductGrid from '../../components/ProductGrid.astro';
import ProductFilters from '../../components/ProductFilters.astro';
import Pagination from '../../components/Pagination.astro';

export const prerender = false; // SSR for dynamic filtering

const url = Astro.url;
const page = Number(url.searchParams.get('page')) || 1;
const pageSize = 24;

const filter = {
  category: url.searchParams.get('category') || undefined,
  brand: url.searchParams.get('brand') || undefined,
  minPrice: url.searchParams.get('min_price') ? Number(url.searchParams.get('min_price')) : undefined,
  maxPrice: url.searchParams.get('max_price') ? Number(url.searchParams.get('max_price')) : undefined,
  inStock: url.searchParams.get('in_stock') === 'true',
  search: url.searchParams.get('q') || undefined,
  sort: (url.searchParams.get('sort') as any) || undefined,
};

const [{ products, total }, categories, brands, priceRange] = await Promise.all([
  getProducts(filter, pageSize, (page - 1) * pageSize),
  getCategories(),
  getBrands(),
  getPriceRange(),
]);

const totalPages = Math.ceil(total / pageSize);
---

<ShopLayout title="Products">
  <div class="products-page">
    <aside class="filters-sidebar">
      <ProductFilters
        categories={categories}
        brands={brands}
        priceRange={priceRange}
        currentFilter={filter}
      />
    </aside>

    <main class="products-main">
      <header class="products-header">
        <h1>
          {filter.search ? `Search: "${filter.search}"` : 'All Products'}
        </h1>
        <p class="results-count">{total} products</p>

        <div class="sort-options">
          <label for="sort">Sort by:</label>
          <select id="sort" onchange="updateSort(this.value)">
            <option value="" selected={!filter.sort}>Featured</option>
            <option value="price_asc" selected={filter.sort === 'price_asc'}>Price: Low to High</option>
            <option value="price_desc" selected={filter.sort === 'price_desc'}>Price: High to Low</option>
            <option value="newest" selected={filter.sort === 'newest'}>Newest</option>
            <option value="name" selected={filter.sort === 'name'}>Name</option>
          </select>
        </div>
      </header>

      {products.length > 0 ? (
        <>
          <ProductGrid products={products} />
          <Pagination
            currentPage={page}
            totalPages={totalPages}
            baseUrl="/products"
            preserveParams={true}
          />
        </>
      ) : (
        <div class="no-results">
          <p>No products found matching your criteria.</p>
          <a href="/products">Clear filters</a>
        </div>
      )}
    </main>
  </div>
</ShopLayout>

<script>
  function updateSort(value: string) {
    const url = new URL(window.location.href);
    if (value) {
      url.searchParams.set('sort', value);
    } else {
      url.searchParams.delete('sort');
    }
    url.searchParams.delete('page');
    window.location.href = url.toString();
  }

  // Make function available globally
  (window as any).updateSort = updateSort;
</script>

<style>
  .products-page {
    display: grid;
    grid-template-columns: 250px 1fr;
    gap: 2rem;
    max-width: 1400px;
    margin: 0 auto;
    padding: 2rem;
  }

  @media (max-width: 768px) {
    .products-page {
      grid-template-columns: 1fr;
    }
  }

  .products-header {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 1rem;
    margin-bottom: 2rem;
  }

  .products-header h1 {
    margin: 0;
    flex: 1;
  }

  .results-count {
    color: #666;
    margin: 0;
  }

  .sort-options {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .sort-options select {
    padding: 0.5rem;
    border: 1px solid #ddd;
    border-radius: 4px;
  }

  .no-results {
    text-align: center;
    padding: 4rem 2rem;
  }
</style>
```

### Product Detail Page

```astro
---
// src/pages/products/[slug].astro
import { getProductBySlug, getRelatedProducts } from '../../lib/ecommerce-queries';
import ShopLayout from '../../layouts/ShopLayout.astro';
import ProductGallery from '../../components/ProductGallery.astro';
import ProductInfo from '../../components/ProductInfo.astro';
import ProductGrid from '../../components/ProductGrid.astro';
import AddToCartButton from '../../components/AddToCartButton';

const { slug } = Astro.params;
const product = await getProductBySlug(slug!);

if (!product) {
  return Astro.redirect('/404');
}

const relatedProducts = await getRelatedProducts(product.id, product.category_id, 4);

const structuredData = {
  '@context': 'https://schema.org',
  '@type': 'Product',
  name: product.name,
  description: product.description,
  image: product.images[0]?.url,
  sku: product.sku,
  brand: product.brand_name ? { '@type': 'Brand', name: product.brand_name } : undefined,
  offers: {
    '@type': 'Offer',
    price: product.price,
    priceCurrency: 'USD',
    availability: product.inventory > 0
      ? 'https://schema.org/InStock'
      : 'https://schema.org/OutOfStock',
  },
};
---

<ShopLayout title={product.name}>
  <script type="application/ld+json" set:html={JSON.stringify(structuredData)} />

  <div class="product-page">
    <div class="product-main">
      <ProductGallery images={product.images} productName={product.name} />

      <div class="product-details">
        <ProductInfo product={product} />

        {product.variants.length > 0 && (
          <div class="variants">
            <h3>Options</h3>
            {product.variants.map((variant) => (
              <button
                class="variant-option"
                data-variant-id={variant.id}
                data-price={variant.price}
              >
                {variant.name} - ${variant.price.toFixed(2)}
                {variant.inventory === 0 && <span class="out-of-stock">(Out of stock)</span>}
              </button>
            ))}
          </div>
        )}

        <AddToCartButton
          client:load
          productId={product.id}
          price={product.price}
          inStock={product.inventory > 0}
        />
      </div>
    </div>

    {product.description && (
      <section class="product-description">
        <h2>Description</h2>
        <div set:html={product.description} />
      </section>
    )}

    {relatedProducts.length > 0 && (
      <section class="related-products">
        <h2>Related Products</h2>
        <ProductGrid products={relatedProducts} columns={4} />
      </section>
    )}
  </div>
</ShopLayout>

<style>
  .product-page {
    max-width: 1200px;
    margin: 0 auto;
    padding: 2rem;
  }

  .product-main {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 3rem;
    margin-bottom: 3rem;
  }

  @media (max-width: 768px) {
    .product-main {
      grid-template-columns: 1fr;
    }
  }

  .variants {
    margin: 1.5rem 0;
  }

  .variant-option {
    display: block;
    width: 100%;
    padding: 0.75rem;
    margin-bottom: 0.5rem;
    border: 1px solid #ddd;
    border-radius: 4px;
    background: white;
    cursor: pointer;
    text-align: left;
  }

  .variant-option:hover {
    border-color: #333;
  }

  .variant-option.selected {
    border-color: #0066cc;
    background: #f0f7ff;
  }

  .out-of-stock {
    color: #cc0000;
    font-size: 0.875rem;
  }

  .product-description,
  .related-products {
    margin-top: 3rem;
    padding-top: 2rem;
    border-top: 1px solid #eee;
  }
</style>
```

### Add to Cart Component (React Island)

```tsx
// src/components/AddToCartButton.tsx
import { useState } from 'react';

interface Props {
  productId: string;
  price: number;
  inStock: boolean;
}

export default function AddToCartButton({ productId, price, inStock }: Props) {
  const [quantity, setQuantity] = useState(1);
  const [loading, setLoading] = useState(false);
  const [added, setAdded] = useState(false);

  async function handleAddToCart() {
    if (!inStock || loading) return;

    setLoading(true);
    try {
      const response = await fetch('/api/cart/add', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ productId, quantity }),
      });

      if (response.ok) {
        setAdded(true);
        setTimeout(() => setAdded(false), 2000);

        // Dispatch custom event for cart update
        window.dispatchEvent(new CustomEvent('cart-updated'));
      }
    } catch (error) {
      console.error('Failed to add to cart:', error);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="add-to-cart">
      <div className="quantity-selector">
        <button
          onClick={() => setQuantity((q) => Math.max(1, q - 1))}
          disabled={quantity <= 1}
        >
          -
        </button>
        <input
          type="number"
          value={quantity}
          onChange={(e) => setQuantity(Math.max(1, parseInt(e.target.value) || 1))}
          min="1"
        />
        <button onClick={() => setQuantity((q) => q + 1)}>+</button>
      </div>

      <button
        className={`add-button ${added ? 'added' : ''}`}
        onClick={handleAddToCart}
        disabled={!inStock || loading}
      >
        {loading ? 'Adding...' : added ? 'Added!' : inStock ? `Add to Cart - $${(price * quantity).toFixed(2)}` : 'Out of Stock'}
      </button>
    </div>
  );
}
```

### Cart API Route

```typescript
// src/pages/api/cart/add.ts
import type { APIRoute } from 'astro';
import { getClient } from '../../../lib/db';

export const POST: APIRoute = async ({ request, cookies }) => {
  const client = getClient();

  try {
    const { productId, quantity } = await request.json();

    if (!productId || !quantity || quantity < 1) {
      return new Response(JSON.stringify({ error: 'Invalid request' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // Get or create session cart
    let cartId = cookies.get('cart_id')?.value;

    if (!cartId) {
      const cartResult = await client.query<{ id: string }>(
        'INSERT INTO carts (created_at) VALUES (?) RETURNING id',
        [Date.now()]
      );
      cartId = cartResult.rows[0].id;
      cookies.set('cart_id', cartId, {
        path: '/',
        maxAge: 60 * 60 * 24 * 30, // 30 days
        httpOnly: true,
        secure: import.meta.env.PROD,
      });
    }

    // Check product availability
    const productResult = await client.query<{ inventory: number; price: number }>(
      'SELECT inventory, price FROM products WHERE id = ?',
      [productId]
    );

    if (productResult.rows.length === 0) {
      return new Response(JSON.stringify({ error: 'Product not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const product = productResult.rows[0];
    if (product.inventory < quantity) {
      return new Response(JSON.stringify({ error: 'Insufficient inventory' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // Add or update cart item
    await client.exec(
      `INSERT INTO cart_items (cart_id, product_id, quantity, price)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(cart_id, product_id) DO UPDATE SET
         quantity = cart_items.quantity + excluded.quantity`,
      [cartId, productId, quantity, product.price]
    );

    return new Response(JSON.stringify({ success: true }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Failed to add to cart:', error);
    return new Response(JSON.stringify({ error: 'Internal server error' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};
```

---

## Performance Optimization Tips

### 1. Connection Pooling and Reuse

```typescript
// src/lib/db.ts
import { createSQLClient } from 'sql.do';

// Singleton pattern for connection reuse
let clientInstance: ReturnType<typeof createSQLClient> | null = null;

export function getClient() {
  if (!clientInstance) {
    clientInstance = createSQLClient({
      url: import.meta.env.DOSQL_URL,
      token: import.meta.env.DOSQL_TOKEN,
      // Enable connection pooling
      pool: {
        maxConnections: 10,
        idleTimeoutMs: 30000,
      },
      // Enable query caching
      cache: {
        enabled: true,
        maxSize: 100,
        ttlMs: 60000,
      },
    });
  }
  return clientInstance;
}
```

### 2. Query Optimization

```typescript
// Avoid N+1 queries with JOINs
// Bad: N+1 queries
async function getPostsWithAuthorsBad() {
  const posts = await client.query('SELECT * FROM posts');
  for (const post of posts.rows) {
    const author = await client.query('SELECT * FROM users WHERE id = ?', [post.author_id]);
    post.author = author.rows[0];
  }
  return posts;
}

// Good: Single query with JOIN
async function getPostsWithAuthorsGood() {
  return client.query(`
    SELECT p.*, u.name as author_name, u.email as author_email
    FROM posts p
    JOIN users u ON p.author_id = u.id
  `);
}

// Use indexes for frequent queries
// Ensure these indexes exist:
// CREATE INDEX idx_posts_author ON posts(author_id);
// CREATE INDEX idx_posts_category ON posts(category_id);
// CREATE INDEX idx_posts_published ON posts(published, published_at DESC);
```

### 3. Parallel Data Fetching

```astro
---
// Fetch independent data in parallel
const [posts, categories, tags, stats] = await Promise.all([
  getPublishedPosts(10),
  getAllCategories(),
  getPopularTags(10),
  getDashboardStats(),
]);
---
```

### 4. Static Generation for Stable Content

```javascript
// astro.config.mjs
import { defineConfig } from 'astro/config';

export default defineConfig({
  output: 'hybrid',
  // Pre-render as much as possible
});
```

```astro
---
// Mark dynamic pages explicitly
export const prerender = false;

// Static pages are pre-rendered by default
---
```

### 5. Caching Strategies

```typescript
// src/lib/cache.ts

// In-memory cache for frequently accessed data
const cache = new Map<string, { data: unknown; expires: number }>();

export async function cachedQuery<T>(
  key: string,
  queryFn: () => Promise<T>,
  ttlMs = 60000
): Promise<T> {
  const cached = cache.get(key);

  if (cached && cached.expires > Date.now()) {
    return cached.data as T;
  }

  const data = await queryFn();
  cache.set(key, { data, expires: Date.now() + ttlMs });

  return data;
}

// Usage
const categories = await cachedQuery(
  'all-categories',
  () => getAllCategories(),
  300000 // 5 minutes
);
```

### 6. Response Caching with Headers

```typescript
// src/pages/api/products.ts
export const GET: APIRoute = async ({ url }) => {
  const products = await getProducts();

  return new Response(JSON.stringify(products), {
    headers: {
      'Content-Type': 'application/json',
      // Cache for 5 minutes on CDN
      'Cache-Control': 'public, s-maxage=300, stale-while-revalidate=600',
      // Set ETag for conditional requests
      'ETag': `"${hashProducts(products)}"`,
    },
  });
};
```

### 7. Lazy Loading for Heavy Components

```astro
---
import HeavyChart from '../components/HeavyChart';
---

<!-- Only load on client when visible -->
<HeavyChart client:visible data={chartData} />

<!-- Only load when idle -->
<HeavyChart client:idle data={chartData} />

<!-- Only load on specific media query -->
<HeavyChart client:media="(min-width: 768px)" data={chartData} />
```

### 8. Pagination Best Practices

```typescript
// Use cursor-based pagination for large datasets
async function getProductsCursor(cursor?: string, limit = 20) {
  let sql = `
    SELECT * FROM products
    WHERE status = 'active'
  `;
  const params: unknown[] = [];

  if (cursor) {
    sql += ' AND id > ?';
    params.push(cursor);
  }

  sql += ' ORDER BY id LIMIT ?';
  params.push(limit + 1);

  const result = await client.query(sql, params);

  const hasMore = result.rows.length > limit;
  const products = result.rows.slice(0, limit);
  const nextCursor = hasMore ? products[products.length - 1].id : null;

  return { products, nextCursor, hasMore };
}
```

### 9. Database Query Monitoring

```typescript
// src/lib/db.ts
import { createSQLClient } from 'sql.do';

const client = createSQLClient({
  url: import.meta.env.DOSQL_URL,
  // Enable query logging in development
  logging: {
    enabled: import.meta.env.DEV,
    slowQueryThresholdMs: 100,
    onSlowQuery: (sql, duration) => {
      console.warn(`Slow query (${duration}ms): ${sql.substring(0, 100)}...`);
    },
  },
});
```

### 10. Image Optimization with Cloudflare

```astro
---
// Use Cloudflare Image Resizing
function getOptimizedImageUrl(url: string, width: number) {
  return `/cdn-cgi/image/width=${width},quality=80,format=auto/${url}`;
}
---

<img
  src={getOptimizedImageUrl(product.image, 400)}
  srcset={`
    ${getOptimizedImageUrl(product.image, 400)} 400w,
    ${getOptimizedImageUrl(product.image, 800)} 800w,
    ${getOptimizedImageUrl(product.image, 1200)} 1200w
  `}
  sizes="(max-width: 768px) 100vw, 400px"
  alt={product.name}
  loading="lazy"
/>
```

---

## Summary

This guide covered integrating DoSQL with Astro, including:

1. **Installation and Setup** - Setting up the DoSQL client with proper configuration
2. **Server-Side Data Loading** - Fetching data in `.astro` files with type safety
3. **API Routes** - Building REST APIs with transactions and error handling
4. **SSR vs SSG** - Choosing the right rendering mode for each page
5. **Edge Deployment** - Deploying to Cloudflare Workers with Durable Object bindings
6. **Type-Safe Queries** - Using TypeScript for schema types and query functions
7. **Real-Time Updates** - Implementing CDC subscriptions with Server-Sent Events
8. **Blog Example** - Complete blog with categories, tags, and pagination
9. **E-commerce Example** - Product catalog with filtering, variants, and cart
10. **Performance Tips** - Caching, parallel fetching, and query optimization

For more information, see:
- [DoSQL Performance Tuning Guide](../PERFORMANCE_TUNING.md)
- [DoSQL Security Guide](../SECURITY.md)
- [Astro Documentation](https://docs.astro.build)
- [Cloudflare Workers Documentation](https://developers.cloudflare.com/workers/)
