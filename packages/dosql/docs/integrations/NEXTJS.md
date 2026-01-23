# Next.js Integration Guide

Build full-stack applications with DoSQL and Next.js App Router. This guide covers Server Components, Server Actions, Route Handlers, real-time features, and deployment strategies.

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Server Components](#server-components)
- [Server Actions](#server-actions)
- [Route Handlers](#route-handlers)
- [Client-Side Patterns](#client-side-patterns)
- [Real-Time Features](#real-time-features)
- [Edge Runtime](#edge-runtime)
- [Deployment](#deployment)

---

## Overview

DoSQL and Next.js complement each other well. DoSQL runs on Cloudflare Workers with Durable Objects, providing edge-native SQL databases. Next.js App Router provides Server Components and Server Actions that enable direct database access without intermediate API layers.

### Key Benefits

| Feature | Description |
|---------|-------------|
| **Edge-Native** | DoSQL runs at the edge, minimizing latency when paired with Next.js Edge Runtime |
| **Type-Safe** | Full TypeScript support with compile-time query validation |
| **Real-Time** | Stream database changes to React components via CDC |
| **Serverless** | No connection pooling required - each request uses a dedicated Durable Object |
| **Time Travel** | Built-in versioning for audit logs and undo functionality |

### App Router vs Pages Router

This guide focuses on the App Router, which provides:

- Server Components for direct database queries
- Server Actions for type-safe mutations
- Streaming and Suspense support
- Per-route runtime configuration
- Fine-grained cache revalidation

The Pages Router is supported but requires API routes for all database operations. New projects should use the App Router.

---

## Setup

### Installation

```bash
npm install @dotdo/dosql

# Optional: Client-side data fetching libraries
npm install swr
# or
npm install @tanstack/react-query
```

### Environment Variables

Create `.env.local` with your DoSQL configuration:

```bash
# DoSQL Connection (Cloudflare Worker endpoint)
DOSQL_URL=https://your-worker.your-subdomain.workers.dev
DOSQL_API_KEY=your-api-key

# WebSocket URL for real-time features (client-accessible)
NEXT_PUBLIC_DOSQL_WS_URL=wss://your-worker.your-subdomain.workers.dev/ws
```

### Next.js Configuration

Configure Next.js for DoSQL compatibility:

```javascript
// next.config.js
/** @type {import('next').NextConfig} */
const nextConfig = {
  // Enable WebAssembly for DoSQL dependencies
  webpack: (config, { isServer }) => {
    if (isServer) {
      config.experiments = {
        ...config.experiments,
        asyncWebAssembly: true,
      };
    }
    return config;
  },

  // Exclude DoSQL from server bundling
  serverExternalPackages: ['@dotdo/dosql'],
};

module.exports = nextConfig;
```

### Database Client

Create a reusable database client:

```typescript
// lib/db.ts
import { createHttpClient } from '@dotdo/dosql/rpc';

export function getDB() {
  if (!process.env.DOSQL_URL) {
    throw new Error('DOSQL_URL environment variable is required');
  }

  return createHttpClient({
    url: process.env.DOSQL_URL,
    headers: {
      Authorization: `Bearer ${process.env.DOSQL_API_KEY}`,
    },
    batch: true,
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

Server Components query the database directly without API routes, reducing latency and client bundle size.

### Basic Query

```typescript
// app/users/page.tsx
import { getDB } from '@/lib/db';
import type { User } from '@/lib/types';

export default async function UsersPage() {
  const db = getDB();
  const users = await db.query<User>(
    'SELECT * FROM users ORDER BY created_at DESC'
  );

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

Fetch multiple data sources concurrently:

```typescript
// app/dashboard/page.tsx
import { getDB } from '@/lib/db';
import type { User, Post } from '@/lib/types';

async function getActiveUsers() {
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
  const [users, posts, stats] = await Promise.all([
    getActiveUsers(),
    getRecentPosts(),
    getStats(),
  ]);

  return (
    <div className="grid grid-cols-3 gap-6">
      <section>
        <h2>Active Users ({stats?.users})</h2>
        <UserList users={users} />
      </section>
      <section>
        <h2>Recent Posts ({stats?.posts})</h2>
        <PostList posts={posts} />
      </section>
      <section>
        <h2>Statistics</h2>
        <StatsCard stats={stats} />
      </section>
    </div>
  );
}
```

### Dynamic Routes with Async Params

Next.js 15 uses Promise-based params. Await them before use:

```typescript
// app/posts/[id]/page.tsx
import { getDB } from '@/lib/db';
import { notFound } from 'next/navigation';
import type { Post, Comment } from '@/lib/types';

interface PageProps {
  params: Promise<{ id: string }>;
}

export default async function PostPage({ params }: PageProps) {
  const { id } = await params;
  const postId = parseInt(id, 10);

  const db = getDB();
  const [post, comments] = await Promise.all([
    db.queryOne<Post>('SELECT * FROM posts WHERE id = ?', [postId]),
    db.query<Comment>(
      'SELECT * FROM comments WHERE post_id = ? ORDER BY created_at ASC',
      [postId]
    ),
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

### Caching with unstable_cache

Cache database queries with targeted revalidation:

```typescript
// app/posts/[id]/page.tsx
import { getDB } from '@/lib/db';
import { unstable_cache } from 'next/cache';
import { notFound } from 'next/navigation';
import type { Post, Comment } from '@/lib/types';

const getPost = unstable_cache(
  async (id: number) => {
    const db = getDB();
    return db.queryOne<Post>('SELECT * FROM posts WHERE id = ?', [id]);
  },
  ['post'],
  { tags: ['posts'], revalidate: 60 }
);

const getComments = unstable_cache(
  async (postId: number) => {
    const db = getDB();
    return db.query<Comment>(
      'SELECT * FROM comments WHERE post_id = ? ORDER BY created_at ASC',
      [postId]
    );
  },
  ['comments'],
  { tags: ['comments'], revalidate: 30 }
);

interface PageProps {
  params: Promise<{ id: string }>;
}

export default async function PostPage({ params }: PageProps) {
  const { id } = await params;
  const postId = parseInt(id, 10);

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

### Rendering Modes

Control rendering behavior per route:

```typescript
// Force static generation with periodic revalidation
// app/posts/page.tsx
export const dynamic = 'force-static';
export const revalidate = 3600; // Revalidate hourly

// Force dynamic rendering (fresh data on every request)
// app/dashboard/page.tsx
export const dynamic = 'force-dynamic';

// Let Next.js decide based on data fetching patterns
// (default behavior - no export needed)
```

---

## Server Actions

Server Actions provide type-safe mutations directly from React components. They run on the server and can revalidate cached data.

### Basic Mutation

```typescript
// app/posts/actions.ts
'use server';

import { getDB } from '@/lib/db';
import { revalidateTag } from 'next/cache';
import { redirect } from 'next/navigation';

export async function createPost(formData: FormData) {
  const db = getDB();

  const title = formData.get('title') as string;
  const content = formData.get('content') as string;
  const published = formData.get('published') === 'true';

  const result = await db.run(
    `INSERT INTO posts (title, content, published, created_at, updated_at)
     VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
    [title, content, published]
  );

  revalidateTag('posts');
  redirect(`/posts/${result.lastInsertRowId}`);
}

export async function deletePost(id: number) {
  const db = getDB();

  await db.transaction(async (tx) => {
    await tx.run('DELETE FROM comments WHERE post_id = ?', [id]);
    await tx.run('DELETE FROM posts WHERE id = ?', [id]);
  });

  revalidateTag('posts');
  revalidateTag('comments');
  redirect('/posts');
}
```

### Form with Server Action

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

### Error Handling with useActionState

Use `useActionState` for form validation and error display:

```typescript
// app/posts/actions.ts
'use server';

import { getDB } from '@/lib/db';
import { revalidateTag } from 'next/cache';

export type ActionState = {
  success: boolean;
  error?: string;
  data?: { id: number };
} | null;

export async function createPost(
  prevState: ActionState,
  formData: FormData
): Promise<ActionState> {
  const title = formData.get('title') as string;
  const content = formData.get('content') as string;

  // Validation
  if (!title?.trim()) {
    return { success: false, error: 'Title is required' };
  }
  if (!content?.trim()) {
    return { success: false, error: 'Content is required' };
  }
  if (title.length > 200) {
    return { success: false, error: 'Title must be 200 characters or less' };
  }

  try {
    const db = getDB();
    const result = await db.run(
      `INSERT INTO posts (title, content, created_at, updated_at)
       VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
      [title, content]
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
import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { createPost, type ActionState } from '@/app/posts/actions';

export function CreatePostForm() {
  const router = useRouter();
  const [state, formAction, isPending] = useActionState<ActionState, FormData>(
    createPost,
    null
  );

  useEffect(() => {
    if (state?.success && state.data?.id) {
      router.push(`/posts/${state.data.id}`);
    }
  }, [state, router]);

  return (
    <form action={formAction} className="space-y-4">
      {state?.error && (
        <div className="rounded-md bg-red-50 p-4 text-red-700">
          {state.error}
        </div>
      )}

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

      <button
        type="submit"
        disabled={isPending}
        className="rounded-md bg-blue-600 px-4 py-2 text-white hover:bg-blue-700 disabled:opacity-50"
      >
        {isPending ? 'Creating...' : 'Create Post'}
      </button>
    </form>
  );
}
```

### Optimistic Updates

Show immediate feedback while mutations complete:

```tsx
// components/LikeButton.tsx
'use client';

import { useOptimistic, useTransition } from 'react';
import { toggleLike } from '@/app/posts/actions';

interface LikeButtonProps {
  postId: number;
  initialLikes: number;
  initialIsLiked: boolean;
}

export function LikeButton({ postId, initialLikes, initialIsLiked }: LikeButtonProps) {
  const [isPending, startTransition] = useTransition();

  const [optimistic, addOptimistic] = useOptimistic(
    { likes: initialLikes, isLiked: initialIsLiked },
    (state, newIsLiked: boolean) => ({
      likes: newIsLiked ? state.likes + 1 : state.likes - 1,
      isLiked: newIsLiked,
    })
  );

  const handleClick = () => {
    const newIsLiked = !optimistic.isLiked;
    startTransition(async () => {
      addOptimistic(newIsLiked);
      await toggleLike(postId, newIsLiked);
    });
  };

  return (
    <button
      onClick={handleClick}
      disabled={isPending}
      className={`flex items-center gap-2 rounded-full px-4 py-2 ${
        optimistic.isLiked
          ? 'bg-red-100 text-red-600'
          : 'bg-gray-100 text-gray-600'
      }`}
    >
      <span>{optimistic.isLiked ? 'Liked' : 'Like'}</span>
      <span>{optimistic.likes}</span>
    </button>
  );
}
```

---

## Route Handlers

Use Route Handlers for REST APIs, webhooks, and operations that need HTTP method control.

### Basic CRUD Routes

```typescript
// app/api/posts/route.ts
import { getDB } from '@/lib/db';
import { NextRequest, NextResponse } from 'next/server';
import type { Post } from '@/lib/types';

export async function GET(request: NextRequest) {
  const db = getDB();
  const searchParams = request.nextUrl.searchParams;

  const page = parseInt(searchParams.get('page') || '1', 10);
  const limit = parseInt(searchParams.get('limit') || '10', 10);
  const offset = (page - 1) * limit;

  const [posts, countResult] = await Promise.all([
    db.query<Post>(
      `SELECT * FROM posts WHERE published = ?
       ORDER BY created_at DESC LIMIT ? OFFSET ?`,
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

  return NextResponse.json({ id: result.lastInsertRowId }, { status: 201 });
}
```

### Dynamic Route Handlers

Handle individual resources with async params:

```typescript
// app/api/posts/[id]/route.ts
import { getDB } from '@/lib/db';
import { NextRequest, NextResponse } from 'next/server';

interface RouteContext {
  params: Promise<{ id: string }>;
}

export async function GET(request: NextRequest, context: RouteContext) {
  const { id } = await context.params;
  const db = getDB();

  const post = await db.queryOne(
    'SELECT * FROM posts WHERE id = ?',
    [parseInt(id, 10)]
  );

  if (!post) {
    return NextResponse.json({ error: 'Post not found' }, { status: 404 });
  }

  return NextResponse.json(post);
}

export async function PUT(request: NextRequest, context: RouteContext) {
  const { id } = await context.params;
  const db = getDB();
  const body = await request.json();

  const { title, content, published } = body;

  const result = await db.run(
    `UPDATE posts
     SET title = COALESCE(?, title),
         content = COALESCE(?, content),
         published = COALESCE(?, published),
         updated_at = CURRENT_TIMESTAMP
     WHERE id = ?`,
    [title, content, published, parseInt(id, 10)]
  );

  if (result.rowsAffected === 0) {
    return NextResponse.json({ error: 'Post not found' }, { status: 404 });
  }

  return NextResponse.json({ success: true });
}

export async function DELETE(request: NextRequest, context: RouteContext) {
  const { id } = await context.params;
  const db = getDB();
  const postId = parseInt(id, 10);

  await db.transaction(async (tx) => {
    await tx.run('DELETE FROM comments WHERE post_id = ?', [postId]);
    await tx.run('DELETE FROM posts WHERE id = ?', [postId]);
  });

  return NextResponse.json({ success: true });
}
```

### Streaming Response

Stream large datasets to the client:

```typescript
// app/api/export/posts/route.ts
import { getDB } from '@/lib/db';

export async function GET() {
  const db = getDB();

  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      controller.enqueue(encoder.encode('id,title,content,created_at\n'));

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

### Revalidation Endpoint

Trigger cache revalidation from external services:

```typescript
// app/api/revalidate/route.ts
import { revalidateTag, revalidatePath } from 'next/cache';
import { NextRequest, NextResponse } from 'next/server';

export async function POST(request: NextRequest) {
  const { type, id, secret } = await request.json();

  // Verify webhook secret
  if (secret !== process.env.REVALIDATE_SECRET) {
    return NextResponse.json({ error: 'Invalid secret' }, { status: 401 });
  }

  switch (type) {
    case 'post':
      revalidateTag('posts');
      if (id) revalidatePath(`/posts/${id}`);
      break;
    case 'comment':
      revalidateTag('comments');
      break;
    case 'user':
      revalidateTag('users');
      revalidatePath('/users');
      break;
    default:
      return NextResponse.json({ error: 'Unknown type' }, { status: 400 });
  }

  return NextResponse.json({ revalidated: true });
}
```

---

## Client-Side Patterns

### SWR Integration

Use SWR for client-side data fetching with caching:

```typescript
// hooks/useQuery.ts
'use client';

import useSWR, { type SWRConfiguration } from 'swr';

interface QueryOptions extends SWRConfiguration {
  params?: unknown[];
}

async function fetcher([sql, params]: [string, unknown[]]) {
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

export function useQuery<T>(sql: string, options: QueryOptions = {}) {
  const { params = [], ...swrOptions } = options;

  return useSWR<T[]>([sql, params], fetcher, {
    revalidateOnFocus: false,
    ...swrOptions,
  });
}
```

```tsx
// Usage in component
'use client';

import { useQuery } from '@/hooks/useQuery';
import type { Post } from '@/lib/types';

export function RecentPosts() {
  const { data: posts, error, isLoading } = useQuery<Post>(
    'SELECT * FROM posts WHERE published = ? ORDER BY created_at DESC LIMIT ?',
    { params: [true, 5], refreshInterval: 30000 }
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

Use React Query for more complex caching scenarios:

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

export function useDoSQLMutation(
  sql: string,
  options?: { invalidateKeys?: string[][] }
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
      options?.invalidateKeys?.forEach((key) => {
        queryClient.invalidateQueries({ queryKey: key });
      });
    },
  });
}
```

---

## Real-Time Features

### WebSocket Hook

Create a hook for real-time database connections:

```typescript
// hooks/useDoSQLRealtime.ts
'use client';

import { useEffect, useState, useCallback, useRef } from 'react';
import { createWebSocketClient } from '@dotdo/dosql/rpc';

export function useDoSQLRealtime() {
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const clientRef = useRef<ReturnType<typeof createWebSocketClient> | null>(null);

  useEffect(() => {
    const client = createWebSocketClient({
      url: process.env.NEXT_PUBLIC_DOSQL_WS_URL!,
      reconnect: true,
      onConnect: () => setIsConnected(true),
      onDisconnect: () => setIsConnected(false),
      onError: (err) => setError(err),
    });

    clientRef.current = client;

    return () => {
      client.close();
    };
  }, []);

  const query = useCallback(async <T>(sql: string, params?: unknown[]): Promise<T[]> => {
    if (!clientRef.current) {
      throw new Error('WebSocket not connected');
    }
    return clientRef.current.query<T>(sql, params);
  }, []);

  return { isConnected, error, query, client: clientRef.current };
}
```

### CDC Subscription Hook

Subscribe to real-time database changes:

```typescript
// hooks/useCDCSubscription.ts
'use client';

import { useEffect, useState } from 'react';
import { createWebSocketClient, type ChangeEvent } from '@dotdo/dosql/rpc';

interface CDCOptions {
  tables?: string[];
  operations?: ('INSERT' | 'UPDATE' | 'DELETE')[];
}

export function useCDCSubscription(
  options: CDCOptions,
  onEvent: (event: ChangeEvent) => void
) {
  const [isSubscribed, setIsSubscribed] = useState(false);

  useEffect(() => {
    const client = createWebSocketClient({
      url: process.env.NEXT_PUBLIC_DOSQL_WS_URL!,
      reconnect: true,
    });

    async function subscribe() {
      const subscription = await client.subscribeCDC({
        tables: options.tables,
        operations: options.operations,
      });

      setIsSubscribed(true);

      for await (const event of subscription) {
        onEvent(event);
      }
    }

    subscribe().catch(console.error);

    return () => {
      client.close();
      setIsSubscribed(false);
    };
  }, [options.tables?.join(','), options.operations?.join(','), onEvent]);

  return { isSubscribed };
}
```

### Live Comments Component

Display real-time comment updates:

```tsx
// components/LiveComments.tsx
'use client';

import { useState } from 'react';
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

---

## Edge Runtime

Run DoSQL queries at the edge for minimal latency.

### Edge Route Handler

```typescript
// app/api/edge/posts/route.ts
import { createHttpClient } from '@dotdo/dosql/rpc';
import { NextRequest, NextResponse } from 'next/server';

export const runtime = 'edge';

export async function GET(request: NextRequest) {
  const db = createHttpClient({
    url: process.env.DOSQL_URL!,
    headers: {
      Authorization: `Bearer ${process.env.DOSQL_API_KEY}`,
    },
  });

  const posts = await db.query(
    'SELECT * FROM posts WHERE published = ? LIMIT 10',
    [true]
  );

  return NextResponse.json(posts);
}
```

### Geo-Aware Queries

Use edge geolocation for personalized content:

```typescript
// app/api/nearby/route.ts
import { createHttpClient } from '@dotdo/dosql/rpc';
import { NextRequest, NextResponse } from 'next/server';

export const runtime = 'edge';

export async function GET(request: NextRequest) {
  const country = request.geo?.country || 'US';
  const city = request.geo?.city || 'Unknown';

  const db = createHttpClient({
    url: process.env.DOSQL_URL!,
    headers: {
      Authorization: `Bearer ${process.env.DOSQL_API_KEY}`,
    },
  });

  const posts = await db.query(
    `SELECT * FROM posts
     WHERE published = ? AND (region = ? OR region IS NULL)
     ORDER BY created_at DESC LIMIT 10`,
    [true, country]
  );

  return NextResponse.json({ posts, geo: { country, city } });
}
```

### Cloudflare Pages Integration

Access Durable Objects directly when deployed to Cloudflare Pages:

```typescript
// functions/api/posts/[[route]].ts
import { DB } from '@dotdo/dosql';

interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export const onRequest: PagesFunction<Env> = async (context) => {
  const { request, env } = context;

  const id = env.DOSQL_DB.idFromName('default');
  const stub = env.DOSQL_DB.get(id);

  return stub.fetch(request);
};
```

---

## Deployment

### Vercel

#### Environment Setup

```bash
vercel env add DOSQL_URL
vercel env add DOSQL_API_KEY
vercel env add NEXT_PUBLIC_DOSQL_WS_URL
```

#### vercel.json

```json
{
  "framework": "nextjs",
  "regions": ["iad1", "sfo1", "cdg1"],
  "env": {
    "DOSQL_URL": "@dosql-url",
    "DOSQL_API_KEY": "@dosql-api-key"
  }
}
```

#### Middleware for Region Routing

```typescript
// middleware.ts
import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

export function middleware(request: NextRequest) {
  const response = NextResponse.next();

  const region = request.geo?.country === 'US' ? 'us' : 'eu';
  response.headers.set('x-dosql-region', region);

  return response;
}

export const config = {
  matcher: '/api/:path*',
};
```

### Cloudflare Pages

#### wrangler.toml

```toml
name = "my-nextjs-app"
compatibility_date = "2024-01-01"

[build]
command = "npx @cloudflare/next-on-pages"

[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "TenantDatabase"

[[migrations]]
tag = "v1"
new_classes = ["TenantDatabase"]
```

#### Pages Function Middleware

```typescript
// functions/_middleware.ts
import type { PagesFunction } from '@cloudflare/workers-types';

interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export const onRequest: PagesFunction<Env> = async (context) => {
  context.data.db = context.env.DOSQL_DB;
  return context.next();
};
```

### Self-Hosted

#### Docker Compose

```yaml
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
```

#### Dockerfile

```dockerfile
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

### Performance Tips

**Request Batching**: Enable batching in the client to combine multiple queries:

```typescript
const db = createHttpClient({
  url: process.env.DOSQL_URL!,
  batch: true,
  batchMaxSize: 50,
  batchMaxWait: 5, // milliseconds
});
```

**Connection Reuse**: Use a singleton client in server code:

```typescript
// lib/db.ts
let dbInstance: ReturnType<typeof createHttpClient> | null = null;

export function getDB() {
  if (!dbInstance) {
    dbInstance = createHttpClient({
      url: process.env.DOSQL_URL!,
      headers: {
        Authorization: `Bearer ${process.env.DOSQL_API_KEY}`,
      },
      batch: true,
    });
  }
  return dbInstance;
}
```

---

## Related Documentation

- [API Reference](../api-reference.md) - Complete DoSQL API documentation
- [Advanced Features](../advanced.md) - Time travel, branching, CDC
- [Getting Started](../getting-started.md) - DoSQL basics and setup
