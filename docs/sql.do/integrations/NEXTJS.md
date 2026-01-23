# Next.js Integration Guide

A comprehensive guide for using DoSQL with Next.js applications, covering Server Components, Server Actions, API Routes, and real-time features.

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Server Components](#server-components)
- [Server Actions](#server-actions)
- [API Routes](#api-routes)
- [Client-Side](#client-side)
- [Edge Runtime](#edge-runtime)
- [Deployment](#deployment)

---

## Overview

### Why DoSQL + Next.js

DoSQL pairs well with Next.js for several reasons:

| Feature | Benefit |
|---------|---------|
| **Edge-Native** | DoSQL runs on Cloudflare Workers, enabling low-latency database access at the edge alongside Next.js Edge Runtime |
| **Type-Safe SQL** | Full TypeScript support with compile-time query validation matches Next.js's TypeScript-first approach |
| **Real-Time CDC** | Stream database changes to React components for live updates |
| **Serverless** | No connection pooling needed - each request gets a dedicated Durable Object instance |
| **Time Travel** | Built-in versioning enables audit logs and undo functionality |

### App Router vs Pages Router

DoSQL works with both routing paradigms, but the App Router unlocks additional capabilities:

| Feature | App Router | Pages Router |
|---------|------------|--------------|
| Server Components | Native support | Not available |
| Server Actions | Native mutations | API routes required |
| Streaming | ReadableStream support | Limited |
| Edge Runtime | Per-route configuration | Global or per-page |
| Caching | Fine-grained revalidation | Page-level caching |

**Recommendation**: Use the App Router for new projects to take full advantage of Server Components and Server Actions with DoSQL.

---

## Setup

### Installation

```bash
# Install DoSQL
npm install dosql

# Install peer dependencies
npm install @cloudflare/workers-types --save-dev

# Optional: Install SWR or React Query for client-side data fetching
npm install swr
# or
npm install @tanstack/react-query
```

### next.config.js Configuration

Configure Next.js to work with DoSQL and external service bindings:

```javascript
// next.config.js
/** @type {import('next').NextConfig} */
const nextConfig = {
  // Enable experimental features for edge compatibility
  experimental: {
    // Required for Cloudflare bindings
    runtime: 'edge',
  },

  // Webpack configuration for DoSQL
  webpack: (config, { isServer }) => {
    if (isServer) {
      // Handle DoSQL's WASM dependencies
      config.experiments = {
        ...config.experiments,
        asyncWebAssembly: true,
      };
    }

    return config;
  },

  // External packages that should not be bundled
  serverExternalPackages: ['dosql'],

  // Headers for CORS if needed
  async headers() {
    return [
      {
        source: '/api/:path*',
        headers: [
          { key: 'Access-Control-Allow-Origin', value: '*' },
          { key: 'Access-Control-Allow-Methods', value: 'GET, POST, PUT, DELETE, OPTIONS' },
        ],
      },
    ];
  },
};

module.exports = nextConfig;
```

### Environment Variables

Create a `.env.local` file with your DoSQL configuration:

```bash
# .env.local

# DoSQL Connection URL (Cloudflare Worker endpoint)
DOSQL_URL=https://your-worker.your-subdomain.workers.dev

# Authentication (if using)
DOSQL_API_KEY=your-api-key

# WebSocket URL for real-time features
DOSQL_WS_URL=wss://your-worker.your-subdomain.workers.dev/ws

# Optional: R2 bucket for cold storage
DOSQL_R2_BUCKET=your-bucket-name

# Cloudflare bindings (when deploying to Cloudflare Pages)
# These are configured in wrangler.toml, not here
```

### Database Client Setup

Create a reusable database client:

```typescript
// lib/db.ts
import { createHttpClient, createWebSocketClient } from 'dosql/rpc';

// HTTP client for Server Components and Server Actions
export function getDB() {
  if (!process.env.DOSQL_URL) {
    throw new Error('DOSQL_URL environment variable is required');
  }

  return createHttpClient({
    url: process.env.DOSQL_URL,
    headers: {
      Authorization: `Bearer ${process.env.DOSQL_API_KEY}`,
    },
    batch: true, // Enable request batching for better performance
  });
}

// WebSocket client factory for real-time features (client-side)
export function createRealtimeClient() {
  if (!process.env.NEXT_PUBLIC_DOSQL_WS_URL) {
    throw new Error('NEXT_PUBLIC_DOSQL_WS_URL environment variable is required');
  }

  return createWebSocketClient({
    url: process.env.NEXT_PUBLIC_DOSQL_WS_URL,
    reconnect: true,
    reconnectDelay: 1000,
    maxReconnectAttempts: 10,
  });
}
```

### Type Definitions

Define your database schema types:

```typescript
// lib/types.ts
export interface User {
  id: number;
  name: string;
  email: string;
  avatar_url: string | null;
  created_at: string;
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
```

---

## Server Components

Server Components enable direct database queries without API routes, reducing latency and bundle size.

### Database Queries in Server Components

```typescript
// app/users/page.tsx
import { getDB } from '@/lib/db';
import type { User } from '@/lib/types';

export default async function UsersPage() {
  const db = getDB();

  // Direct database query in Server Component
  const users = await db.query<User>('SELECT * FROM users ORDER BY created_at DESC');

  return (
    <div>
      <h1>Users</h1>
      <ul>
        {users.map((user) => (
          <li key={user.id}>
            <span>{user.name}</span>
            <span className="text-gray-500">{user.email}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
```

### Parallel Data Fetching

Fetch multiple data sources in parallel for better performance:

```typescript
// app/dashboard/page.tsx
import { getDB } from '@/lib/db';
import type { User, Post } from '@/lib/types';

async function getUsers() {
  const db = getDB();
  return db.query<User>('SELECT * FROM users WHERE active = ?', [true]);
}

async function getRecentPosts() {
  const db = getDB();
  return db.query<Post>(
    'SELECT * FROM posts WHERE published = ? ORDER BY created_at DESC LIMIT ?',
    [true, 10]
  );
}

async function getStats() {
  const db = getDB();
  return db.queryOne<{ users: number; posts: number; comments: number }>(`
    SELECT
      (SELECT COUNT(*) FROM users) as users,
      (SELECT COUNT(*) FROM posts WHERE published = 1) as posts,
      (SELECT COUNT(*) FROM comments) as comments
  `);
}

export default async function DashboardPage() {
  // Parallel data fetching
  const [users, posts, stats] = await Promise.all([
    getUsers(),
    getRecentPosts(),
    getStats(),
  ]);

  return (
    <div className="grid grid-cols-3 gap-6">
      <div>
        <h2>Active Users ({stats?.users})</h2>
        <UserList users={users} />
      </div>
      <div>
        <h2>Recent Posts ({stats?.posts})</h2>
        <PostList posts={posts} />
      </div>
      <div>
        <h2>Stats</h2>
        <StatsCard stats={stats} />
      </div>
    </div>
  );
}
```

### Caching Strategies

Use Next.js caching with DoSQL queries:

```typescript
// app/posts/[id]/page.tsx
import { getDB } from '@/lib/db';
import { unstable_cache } from 'next/cache';
import type { Post, Comment } from '@/lib/types';

// Cache the post query with tags for targeted revalidation
const getPost = unstable_cache(
  async (id: number) => {
    const db = getDB();
    return db.queryOne<Post>('SELECT * FROM posts WHERE id = ?', [id]);
  },
  ['post'],
  {
    tags: ['posts'],
    revalidate: 60, // Revalidate every 60 seconds
  }
);

// Cache comments separately
const getComments = unstable_cache(
  async (postId: number) => {
    const db = getDB();
    return db.query<Comment>(
      'SELECT * FROM comments WHERE post_id = ? ORDER BY created_at ASC',
      [postId]
    );
  },
  ['comments'],
  {
    tags: ['comments'],
    revalidate: 30,
  }
);

export default async function PostPage({ params }: { params: { id: string } }) {
  const postId = parseInt(params.id, 10);

  const [post, comments] = await Promise.all([
    getPost(postId),
    getComments(postId),
  ]);

  if (!post) {
    notFound();
  }

  return (
    <article>
      <h1>{post.title}</h1>
      <div dangerouslySetInnerHTML={{ __html: post.content }} />
      <CommentSection comments={comments} postId={postId} />
    </article>
  );
}
```

### Revalidation Patterns

Implement on-demand revalidation when data changes:

```typescript
// app/api/revalidate/route.ts
import { revalidateTag, revalidatePath } from 'next/cache';
import { NextRequest, NextResponse } from 'next/server';

export async function POST(request: NextRequest) {
  const { type, id } = await request.json();

  switch (type) {
    case 'post':
      // Revalidate specific post
      revalidateTag('posts');
      revalidatePath(`/posts/${id}`);
      break;
    case 'comment':
      // Revalidate comments for a post
      revalidateTag('comments');
      break;
    case 'user':
      // Revalidate user-related pages
      revalidateTag('users');
      revalidatePath('/users');
      break;
  }

  return NextResponse.json({ revalidated: true });
}
```

### Dynamic vs Static Rendering

Control rendering behavior based on data requirements:

```typescript
// Static: Generate at build time
// app/posts/page.tsx
export const dynamic = 'force-static';
export const revalidate = 3600; // Revalidate every hour

// Dynamic: Always fetch fresh data
// app/dashboard/page.tsx
export const dynamic = 'force-dynamic';

// Auto: Let Next.js decide based on data fetching
// app/profile/page.tsx
// (no export, Next.js decides automatically)
```

---

## Server Actions

Server Actions provide type-safe mutations directly from React components.

### Mutations with Server Actions

```typescript
// app/posts/actions.ts
'use server';

import { getDB } from '@/lib/db';
import { revalidateTag } from 'next/cache';
import { z } from 'zod';

// Input validation schema
const CreatePostSchema = z.object({
  title: z.string().min(1).max(200),
  content: z.string().min(1),
  published: z.boolean().default(false),
});

export async function createPost(formData: FormData) {
  const db = getDB();

  // Validate input
  const validated = CreatePostSchema.parse({
    title: formData.get('title'),
    content: formData.get('content'),
    published: formData.get('published') === 'true',
  });

  // Insert into database
  const result = await db.run(
    `INSERT INTO posts (title, content, published, user_id, created_at, updated_at)
     VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
    [validated.title, validated.content, validated.published, getUserId()]
  );

  // Revalidate cached data
  revalidateTag('posts');

  return { id: result.lastInsertRowId };
}

export async function updatePost(id: number, formData: FormData) {
  const db = getDB();

  const title = formData.get('title') as string;
  const content = formData.get('content') as string;
  const published = formData.get('published') === 'true';

  await db.run(
    `UPDATE posts
     SET title = ?, content = ?, published = ?, updated_at = CURRENT_TIMESTAMP
     WHERE id = ?`,
    [title, content, published, id]
  );

  revalidateTag('posts');
}

export async function deletePost(id: number) {
  const db = getDB();

  // Use transaction to delete post and related comments
  await db.transaction(async (tx) => {
    await tx.run('DELETE FROM comments WHERE post_id = ?', [id]);
    await tx.run('DELETE FROM posts WHERE id = ?', [id]);
  });

  revalidateTag('posts');
  revalidateTag('comments');
}
```

### Form Handling

Create forms that use Server Actions:

```tsx
// app/posts/new/page.tsx
import { createPost } from '../actions';

export default function NewPostPage() {
  return (
    <form action={createPost} className="space-y-4">
      <div>
        <label htmlFor="title" className="block text-sm font-medium">
          Title
        </label>
        <input
          type="text"
          id="title"
          name="title"
          required
          className="mt-1 block w-full rounded-md border-gray-300 shadow-sm"
        />
      </div>

      <div>
        <label htmlFor="content" className="block text-sm font-medium">
          Content
        </label>
        <textarea
          id="content"
          name="content"
          rows={10}
          required
          className="mt-1 block w-full rounded-md border-gray-300 shadow-sm"
        />
      </div>

      <div className="flex items-center">
        <input
          type="checkbox"
          id="published"
          name="published"
          value="true"
          className="h-4 w-4 rounded border-gray-300"
        />
        <label htmlFor="published" className="ml-2 block text-sm">
          Publish immediately
        </label>
      </div>

      <button
        type="submit"
        className="rounded-md bg-blue-600 px-4 py-2 text-white hover:bg-blue-700"
      >
        Create Post
      </button>
    </form>
  );
}
```

### Optimistic Updates

Implement optimistic UI updates with useOptimistic:

```tsx
// components/LikeButton.tsx
'use client';

import { useOptimistic, useTransition } from 'react';
import { likePost } from '@/app/posts/actions';

interface LikeButtonProps {
  postId: number;
  initialLikes: number;
  isLiked: boolean;
}

export function LikeButton({ postId, initialLikes, isLiked }: LikeButtonProps) {
  const [isPending, startTransition] = useTransition();

  const [optimisticState, addOptimistic] = useOptimistic(
    { likes: initialLikes, isLiked },
    (state, newIsLiked: boolean) => ({
      likes: newIsLiked ? state.likes + 1 : state.likes - 1,
      isLiked: newIsLiked,
    })
  );

  const handleClick = () => {
    const newIsLiked = !optimisticState.isLiked;

    startTransition(async () => {
      addOptimistic(newIsLiked);
      await likePost(postId, newIsLiked);
    });
  };

  return (
    <button
      onClick={handleClick}
      disabled={isPending}
      className={`flex items-center gap-2 rounded-full px-4 py-2 ${
        optimisticState.isLiked
          ? 'bg-red-100 text-red-600'
          : 'bg-gray-100 text-gray-600'
      }`}
    >
      <span>{optimisticState.isLiked ? 'Liked' : 'Like'}</span>
      <span>{optimisticState.likes}</span>
    </button>
  );
}
```

### Error Handling in Actions

Handle errors gracefully in Server Actions:

```typescript
// app/posts/actions.ts
'use server';

import { getDB } from '@/lib/db';
import { revalidateTag } from 'next/cache';

export type ActionResult<T = void> =
  | { success: true; data: T }
  | { success: false; error: string };

export async function createPost(formData: FormData): Promise<ActionResult<{ id: number }>> {
  try {
    const db = getDB();

    const title = formData.get('title') as string;
    const content = formData.get('content') as string;

    if (!title || title.trim().length === 0) {
      return { success: false, error: 'Title is required' };
    }

    if (!content || content.trim().length === 0) {
      return { success: false, error: 'Content is required' };
    }

    const result = await db.run(
      `INSERT INTO posts (title, content, user_id, created_at)
       VALUES (?, ?, ?, CURRENT_TIMESTAMP)`,
      [title, content, getUserId()]
    );

    revalidateTag('posts');

    return { success: true, data: { id: Number(result.lastInsertRowId) } };
  } catch (error) {
    console.error('Failed to create post:', error);
    return { success: false, error: 'Failed to create post. Please try again.' };
  }
}
```

```tsx
// components/CreatePostForm.tsx
'use client';

import { useActionState } from 'react';
import { createPost, type ActionResult } from '@/app/posts/actions';

export function CreatePostForm() {
  const [state, formAction, isPending] = useActionState(
    createPost,
    null as ActionResult<{ id: number }> | null
  );

  return (
    <form action={formAction} className="space-y-4">
      {state && !state.success && (
        <div className="rounded-md bg-red-50 p-4 text-red-700">
          {state.error}
        </div>
      )}

      {/* Form fields... */}

      <button type="submit" disabled={isPending}>
        {isPending ? 'Creating...' : 'Create Post'}
      </button>
    </form>
  );
}
```

---

## API Routes

Use API Routes (Route Handlers) for complex operations, webhooks, and third-party integrations.

### Route Handlers with DoSQL

```typescript
// app/api/posts/route.ts
import { getDB } from '@/lib/db';
import { NextRequest, NextResponse } from 'next/server';
import type { Post } from '@/lib/types';

// GET /api/posts
export async function GET(request: NextRequest) {
  const db = getDB();
  const searchParams = request.nextUrl.searchParams;

  const page = parseInt(searchParams.get('page') || '1', 10);
  const limit = parseInt(searchParams.get('limit') || '10', 10);
  const offset = (page - 1) * limit;

  const [posts, countResult] = await Promise.all([
    db.query<Post>(
      `SELECT * FROM posts
       WHERE published = ?
       ORDER BY created_at DESC
       LIMIT ? OFFSET ?`,
      [true, limit, offset]
    ),
    db.queryOne<{ total: number }>(
      'SELECT COUNT(*) as total FROM posts WHERE published = ?',
      [true]
    ),
  ]);

  return NextResponse.json({
    posts,
    pagination: {
      page,
      limit,
      total: countResult?.total || 0,
      pages: Math.ceil((countResult?.total || 0) / limit),
    },
  });
}

// POST /api/posts
export async function POST(request: NextRequest) {
  const db = getDB();
  const body = await request.json();

  const { title, content, published = false } = body;

  if (!title || !content) {
    return NextResponse.json(
      { error: 'Title and content are required' },
      { status: 400 }
    );
  }

  const result = await db.run(
    `INSERT INTO posts (title, content, published, created_at, updated_at)
     VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
    [title, content, published]
  );

  return NextResponse.json(
    { id: result.lastInsertRowId },
    { status: 201 }
  );
}
```

### Dynamic Route Handlers

```typescript
// app/api/posts/[id]/route.ts
import { getDB } from '@/lib/db';
import { NextRequest, NextResponse } from 'next/server';

interface Params {
  params: { id: string };
}

// GET /api/posts/[id]
export async function GET(request: NextRequest, { params }: Params) {
  const db = getDB();
  const id = parseInt(params.id, 10);

  const post = await db.queryOne('SELECT * FROM posts WHERE id = ?', [id]);

  if (!post) {
    return NextResponse.json({ error: 'Post not found' }, { status: 404 });
  }

  return NextResponse.json(post);
}

// PUT /api/posts/[id]
export async function PUT(request: NextRequest, { params }: Params) {
  const db = getDB();
  const id = parseInt(params.id, 10);
  const body = await request.json();

  const { title, content, published } = body;

  const result = await db.run(
    `UPDATE posts
     SET title = COALESCE(?, title),
         content = COALESCE(?, content),
         published = COALESCE(?, published),
         updated_at = CURRENT_TIMESTAMP
     WHERE id = ?`,
    [title, content, published, id]
  );

  if (result.rowsAffected === 0) {
    return NextResponse.json({ error: 'Post not found' }, { status: 404 });
  }

  return NextResponse.json({ success: true });
}

// DELETE /api/posts/[id]
export async function DELETE(request: NextRequest, { params }: Params) {
  const db = getDB();
  const id = parseInt(params.id, 10);

  await db.transaction(async (tx) => {
    await tx.run('DELETE FROM comments WHERE post_id = ?', [id]);
    await tx.run('DELETE FROM posts WHERE id = ?', [id]);
  });

  return NextResponse.json({ success: true });
}
```

### Streaming Responses

Stream large datasets to the client:

```typescript
// app/api/export/posts/route.ts
import { getDB } from '@/lib/db';
import { NextRequest } from 'next/server';

export async function GET(request: NextRequest) {
  const db = getDB();

  // Create a streaming response
  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      // Write CSV header
      controller.enqueue(encoder.encode('id,title,content,created_at\n'));

      // Stream posts using DoSQL's streaming interface
      const stmt = db.prepare('SELECT * FROM posts ORDER BY id');

      for await (const post of stmt.iterate()) {
        const row = `${post.id},"${escapeCSV(post.title)}","${escapeCSV(post.content)}",${post.created_at}\n`;
        controller.enqueue(encoder.encode(row));
      }

      controller.close();
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/csv',
      'Content-Disposition': 'attachment; filename="posts.csv"',
    },
  });
}

function escapeCSV(str: string): string {
  return str.replace(/"/g, '""');
}
```

### Webhook Handler with Time Travel

Use DoSQL's time travel for audit logging:

```typescript
// app/api/webhooks/stripe/route.ts
import { getDB } from '@/lib/db';
import { NextRequest, NextResponse } from 'next/server';

export async function POST(request: NextRequest) {
  const db = getDB();
  const payload = await request.text();
  const signature = request.headers.get('stripe-signature');

  // Verify webhook signature (implementation depends on your setup)
  const event = verifyStripeWebhook(payload, signature);

  // Store the webhook event with timestamp for auditing
  await db.run(
    `INSERT INTO webhook_events (provider, event_type, payload, received_at)
     VALUES (?, ?, ?, CURRENT_TIMESTAMP)`,
    ['stripe', event.type, payload]
  );

  // Process the event
  switch (event.type) {
    case 'payment_intent.succeeded':
      await handlePaymentSuccess(db, event.data.object);
      break;
    case 'customer.subscription.updated':
      await handleSubscriptionUpdate(db, event.data.object);
      break;
  }

  return NextResponse.json({ received: true });
}

async function handlePaymentSuccess(db: ReturnType<typeof getDB>, paymentIntent: any) {
  await db.transaction(async (tx) => {
    // Update order status
    await tx.run(
      `UPDATE orders SET status = ?, paid_at = CURRENT_TIMESTAMP WHERE payment_intent_id = ?`,
      ['paid', paymentIntent.id]
    );

    // Record payment
    await tx.run(
      `INSERT INTO payments (order_id, amount, currency, payment_intent_id, created_at)
       SELECT id, ?, ?, ?, CURRENT_TIMESTAMP FROM orders WHERE payment_intent_id = ?`,
      [paymentIntent.amount, paymentIntent.currency, paymentIntent.id, paymentIntent.id]
    );
  });
}
```

---

## Client-Side

Integrate DoSQL with client-side data fetching and real-time updates.

### WebSocket Connection for Real-Time

```typescript
// hooks/useDoSQLRealtime.ts
'use client';

import { useEffect, useState, useCallback, useRef } from 'react';
import { createWebSocketClient } from 'dosql/rpc';

interface UseRealtimeOptions {
  url?: string;
  reconnect?: boolean;
}

export function useDoSQLRealtime(options: UseRealtimeOptions = {}) {
  const {
    url = process.env.NEXT_PUBLIC_DOSQL_WS_URL!,
    reconnect = true,
  } = options;

  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const clientRef = useRef<ReturnType<typeof createWebSocketClient> | null>(null);

  useEffect(() => {
    const client = createWebSocketClient({
      url,
      reconnect,
      onConnect: () => setIsConnected(true),
      onDisconnect: () => setIsConnected(false),
      onError: (err) => setError(err),
    });

    clientRef.current = client;

    return () => {
      client.close();
    };
  }, [url, reconnect]);

  const query = useCallback(async <T>(sql: string, params?: unknown[]): Promise<T[]> => {
    if (!clientRef.current) {
      throw new Error('WebSocket not connected');
    }
    return clientRef.current.query<T>(sql, params);
  }, []);

  const run = useCallback(async (sql: string, params?: unknown[]) => {
    if (!clientRef.current) {
      throw new Error('WebSocket not connected');
    }
    return clientRef.current.run(sql, params);
  }, []);

  return { isConnected, error, query, run, client: clientRef.current };
}
```

### CDC Subscriptions

Subscribe to real-time database changes:

```typescript
// hooks/useCDCSubscription.ts
'use client';

import { useEffect, useState, useCallback } from 'react';
import { createWebSocketClient, type ChangeEvent } from 'dosql/rpc';

interface CDCOptions {
  tables?: string[];
  operations?: ('INSERT' | 'UPDATE' | 'DELETE')[];
}

export function useCDCSubscription<T>(
  options: CDCOptions,
  onEvent: (event: ChangeEvent) => void
) {
  const [isSubscribed, setIsSubscribed] = useState(false);
  const [lastLSN, setLastLSN] = useState<bigint>(0n);

  useEffect(() => {
    const client = createWebSocketClient({
      url: process.env.NEXT_PUBLIC_DOSQL_WS_URL!,
      reconnect: true,
    });

    let subscription: AsyncIterable<ChangeEvent> | null = null;

    async function subscribe() {
      subscription = await client.subscribeCDC({
        fromLSN: lastLSN,
        tables: options.tables,
        operations: options.operations,
      });

      setIsSubscribed(true);

      for await (const event of subscription) {
        setLastLSN(event.lsn);
        onEvent(event);
      }
    }

    subscribe().catch(console.error);

    return () => {
      client.close();
      setIsSubscribed(false);
    };
  }, [options.tables?.join(','), options.operations?.join(',')]);

  return { isSubscribed, lastLSN };
}
```

### Real-Time Component Example

```tsx
// components/LiveComments.tsx
'use client';

import { useState, useEffect } from 'react';
import { useCDCSubscription } from '@/hooks/useCDCSubscription';
import type { Comment } from '@/lib/types';

interface LiveCommentsProps {
  postId: number;
  initialComments: Comment[];
}

export function LiveComments({ postId, initialComments }: LiveCommentsProps) {
  const [comments, setComments] = useState<Comment[]>(initialComments);

  useCDCSubscription(
    { tables: ['comments'], operations: ['INSERT', 'UPDATE', 'DELETE'] },
    (event) => {
      // Filter for this post's comments
      if (event.after?.post_id !== postId && event.before?.post_id !== postId) {
        return;
      }

      switch (event.type) {
        case 'insert':
          setComments((prev) => [...prev, event.after as Comment]);
          break;
        case 'update':
          setComments((prev) =>
            prev.map((c) => (c.id === event.after.id ? (event.after as Comment) : c))
          );
          break;
        case 'delete':
          setComments((prev) => prev.filter((c) => c.id !== event.before.id));
          break;
      }
    }
  );

  return (
    <div className="space-y-4">
      <h3>Comments ({comments.length})</h3>
      {comments.map((comment) => (
        <div key={comment.id} className="border-l-2 border-gray-200 pl-4">
          <p>{comment.content}</p>
          <span className="text-sm text-gray-500">
            {new Date(comment.created_at).toLocaleString()}
          </span>
        </div>
      ))}
    </div>
  );
}
```

### SWR Integration

Use SWR for client-side data fetching with caching:

```typescript
// hooks/useDoSQL.ts
'use client';

import useSWR, { type SWRConfiguration } from 'swr';

interface QueryOptions extends SWRConfiguration {
  params?: unknown[];
}

export function useQuery<T>(sql: string, options: QueryOptions = {}) {
  const { params = [], ...swrOptions } = options;

  const fetcher = async () => {
    const response = await fetch('/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql, params }),
    });

    if (!response.ok) {
      throw new Error('Query failed');
    }

    return response.json();
  };

  // Create cache key from SQL and params
  const key = JSON.stringify({ sql, params });

  return useSWR<T[]>(key, fetcher, {
    revalidateOnFocus: false,
    ...swrOptions,
  });
}

// Usage
function PostsList() {
  const { data: posts, error, isLoading, mutate } = useQuery<Post>(
    'SELECT * FROM posts WHERE published = ? ORDER BY created_at DESC',
    { params: [true], refreshInterval: 30000 }
  );

  if (isLoading) return <div>Loading...</div>;
  if (error) return <div>Error loading posts</div>;

  return (
    <ul>
      {posts?.map((post) => (
        <li key={post.id}>{post.title}</li>
      ))}
    </ul>
  );
}
```

### React Query Integration

Use React Query for more advanced caching scenarios:

```typescript
// hooks/useDoSQLQuery.ts
'use client';

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';

async function queryDB<T>(sql: string, params?: unknown[]): Promise<T[]> {
  const response = await fetch('/api/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  });

  if (!response.ok) {
    throw new Error('Query failed');
  }

  return response.json();
}

// Query hook
export function useDoSQLQuery<T>(
  key: string[],
  sql: string,
  params?: unknown[]
) {
  return useQuery({
    queryKey: key,
    queryFn: () => queryDB<T>(sql, params),
  });
}

// Mutation hook
export function useDoSQLMutation(
  sql: string,
  options?: {
    invalidateKeys?: string[][];
  }
) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (params: unknown[]) => {
      const response = await fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params }),
      });

      if (!response.ok) {
        throw new Error('Mutation failed');
      }

      return response.json();
    },
    onSuccess: () => {
      // Invalidate related queries
      options?.invalidateKeys?.forEach((key) => {
        queryClient.invalidateQueries({ queryKey: key });
      });
    },
  });
}

// Usage
function UserProfile({ userId }: { userId: number }) {
  const { data: user, isLoading } = useDoSQLQuery<User>(
    ['user', userId.toString()],
    'SELECT * FROM users WHERE id = ?',
    [userId]
  );

  const updateUser = useDoSQLMutation(
    'UPDATE users SET name = ? WHERE id = ?',
    { invalidateKeys: [['user', userId.toString()], ['users']] }
  );

  if (isLoading) return <div>Loading...</div>;

  return (
    <div>
      <h2>{user?.[0]?.name}</h2>
      <button
        onClick={() => updateUser.mutate(['New Name', userId])}
        disabled={updateUser.isPending}
      >
        Update Name
      </button>
    </div>
  );
}
```

---

## Edge Runtime

Run DoSQL queries at the edge for minimal latency.

### Using DoSQL on Edge

Configure routes to run on the Edge Runtime:

```typescript
// app/api/edge/posts/route.ts
import { createHttpClient } from 'dosql/rpc';
import { NextRequest, NextResponse } from 'next/server';

// Force Edge Runtime
export const runtime = 'edge';

export async function GET(request: NextRequest) {
  const db = createHttpClient({
    url: process.env.DOSQL_URL!,
    headers: {
      Authorization: `Bearer ${process.env.DOSQL_API_KEY}`,
    },
  });

  const posts = await db.query('SELECT * FROM posts WHERE published = ? LIMIT 10', [true]);

  return NextResponse.json(posts);
}
```

### Vercel Edge Functions

Deploy DoSQL-powered Edge Functions to Vercel:

```typescript
// app/api/geo-posts/route.ts
import { createHttpClient } from 'dosql/rpc';
import { NextRequest, NextResponse } from 'next/server';

export const runtime = 'edge';

export async function GET(request: NextRequest) {
  // Get geo information from Vercel Edge
  const country = request.geo?.country || 'US';
  const city = request.geo?.city || 'Unknown';

  const db = createHttpClient({
    url: process.env.DOSQL_URL!,
    headers: {
      Authorization: `Bearer ${process.env.DOSQL_API_KEY}`,
    },
  });

  // Query posts relevant to the user's region
  const posts = await db.query(
    `SELECT * FROM posts
     WHERE published = ?
     AND (region = ? OR region IS NULL)
     ORDER BY created_at DESC
     LIMIT 10`,
    [true, country]
  );

  return NextResponse.json({
    posts,
    geo: { country, city },
  });
}
```

### Cloudflare Workers Integration

When deploying to Cloudflare Pages, you can access Durable Objects directly:

```typescript
// functions/api/posts/[[route]].ts (Cloudflare Pages Function)
import { DB } from 'dosql';

interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export const onRequest: PagesFunction<Env> = async (context) => {
  const { request, env } = context;

  // Get Durable Object stub
  const id = env.DOSQL_DB.idFromName('default');
  const stub = env.DOSQL_DB.get(id);

  // Forward request to Durable Object
  return stub.fetch(request);
};
```

### Hybrid Approach: Edge + Origin

Use edge for reads, origin for writes:

```typescript
// app/api/posts/route.ts
import { createHttpClient } from 'dosql/rpc';
import { NextRequest, NextResponse } from 'next/server';

// Edge for fast reads
export const runtime = 'edge';

export async function GET(request: NextRequest) {
  const db = createHttpClient({ url: process.env.DOSQL_URL! });
  const posts = await db.query('SELECT * FROM posts LIMIT 10');
  return NextResponse.json(posts);
}

// app/api/posts/create/route.ts
// Origin for writes (no runtime export = Node.js runtime)
import { getDB } from '@/lib/db';
import { revalidateTag } from 'next/cache';
import { NextRequest, NextResponse } from 'next/server';

export async function POST(request: NextRequest) {
  const db = getDB();
  const body = await request.json();

  await db.run(
    'INSERT INTO posts (title, content) VALUES (?, ?)',
    [body.title, body.content]
  );

  revalidateTag('posts');

  return NextResponse.json({ success: true });
}
```

---

## Deployment

### Vercel Deployment

Deploy your Next.js + DoSQL application to Vercel:

#### 1. Environment Variables

Set environment variables in Vercel Dashboard or via CLI:

```bash
vercel env add DOSQL_URL
vercel env add DOSQL_API_KEY
vercel env add NEXT_PUBLIC_DOSQL_WS_URL
```

#### 2. Build Configuration

```json
// vercel.json
{
  "buildCommand": "next build",
  "framework": "nextjs",
  "regions": ["iad1", "sfo1", "cdg1"],
  "env": {
    "DOSQL_URL": "@dosql-url",
    "DOSQL_API_KEY": "@dosql-api-key"
  }
}
```

#### 3. Middleware for Edge Routing

```typescript
// middleware.ts
import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

export function middleware(request: NextRequest) {
  // Add custom headers or route to nearest DoSQL region
  const response = NextResponse.next();

  // Example: Route to nearest Cloudflare region
  const region = request.geo?.country === 'US' ? 'us' : 'eu';
  response.headers.set('x-dosql-region', region);

  return response;
}

export const config = {
  matcher: '/api/:path*',
};
```

### Cloudflare Pages Deployment

Deploy to Cloudflare Pages with native Durable Objects access:

#### 1. wrangler.toml Configuration

```toml
# wrangler.toml
name = "my-nextjs-app"
compatibility_date = "2024-01-01"

[build]
command = "npx @cloudflare/next-on-pages"

[[d1_databases]]
binding = "DB"
database_name = "my-database"
database_id = "xxxxx"

[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "TenantDatabase"

[[migrations]]
tag = "v1"
new_classes = ["TenantDatabase"]
```

#### 2. Pages Functions

```typescript
// functions/_middleware.ts
import type { PagesFunction } from '@cloudflare/workers-types';

interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export const onRequest: PagesFunction<Env> = async (context) => {
  // Add bindings to the request context
  context.data.db = context.env.DOSQL_DB;
  return context.next();
};
```

### Self-Hosted Options

Deploy Next.js with DoSQL on your own infrastructure:

#### Docker Compose Setup

```yaml
# docker-compose.yml
version: '3.8'

services:
  nextjs:
    build: .
    ports:
      - '3000:3000'
    environment:
      - DOSQL_URL=http://dosql-worker:8787
      - NODE_ENV=production
    depends_on:
      - dosql-worker

  dosql-worker:
    image: cloudflare/workers-runtime:latest
    volumes:
      - ./worker:/app
    environment:
      - WORKER_SCRIPT=/app/index.js

  # Optional: Local R2-compatible storage
  minio:
    image: minio/minio
    ports:
      - '9000:9000'
    volumes:
      - minio-data:/data
    command: server /data

volumes:
  minio-data:
```

#### Dockerfile

```dockerfile
# Dockerfile
FROM node:20-alpine AS base

FROM base AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM base AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM base AS runner
WORKDIR /app
ENV NODE_ENV=production

COPY --from=builder /app/public ./public
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static

EXPOSE 3000
ENV PORT=3000

CMD ["node", "server.js"]
```

### Performance Optimization

#### Connection Pooling Proxy

When self-hosting, use a connection pooling proxy:

```typescript
// lib/db-pool.ts
import { createHttpClient } from 'dosql/rpc';

// Singleton connection pool
let dbPool: ReturnType<typeof createHttpClient> | null = null;

export function getPooledDB() {
  if (!dbPool) {
    dbPool = createHttpClient({
      url: process.env.DOSQL_URL!,
      headers: {
        Authorization: `Bearer ${process.env.DOSQL_API_KEY}`,
      },
      // Connection pool settings
      maxConnections: 10,
      idleTimeout: 60000,
      batch: true,
      batchMaxSize: 100,
      batchMaxWait: 10,
    });
  }

  return dbPool;
}
```

#### Request Batching

Batch multiple queries in a single request:

```typescript
// lib/batch.ts
import { createHttpClient } from 'dosql/rpc';

const db = createHttpClient({
  url: process.env.DOSQL_URL!,
  batch: true,
  batchMaxSize: 50,
  batchMaxWait: 5, // ms
});

// These queries are automatically batched
async function getDashboardData() {
  const [users, posts, comments] = await Promise.all([
    db.query('SELECT COUNT(*) as count FROM users'),
    db.query('SELECT COUNT(*) as count FROM posts'),
    db.query('SELECT COUNT(*) as count FROM comments'),
  ]);

  return { users, posts, comments };
}
```

---

## Next Steps

- [API Reference](../api-reference.md) - Complete DoSQL API documentation
- [Advanced Features](../advanced.md) - Time travel, branching, CDC
- [Getting Started](../getting-started.md) - DoSQL basics and setup
