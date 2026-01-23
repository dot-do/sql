# DoSQL & DoLake Example Applications

**Version**: 1.0.0
**Last Updated**: 2026-01-22
**Maintainer**: Platform Team

This document provides comprehensive example applications and usage patterns for DoSQL and DoLake.

---

## Table of Contents

1. [Quick Examples](#quick-examples)
   - [Basic CRUD Operations](#basic-crud-operations)
   - [Transactions](#transactions)
   - [Batch Operations](#batch-operations)
2. [Real-World Patterns](#real-world-patterns)
   - [Multi-Tenant SaaS](#multi-tenant-saas)
   - [E-commerce Application](#e-commerce-application)
   - [Social Application](#social-application)
   - [Analytics Dashboard](#analytics-dashboard)
3. [Advanced Examples](#advanced-examples)
   - [CDC Streaming to Analytics](#cdc-streaming-to-analytics)
   - [Real-time Collaboration](#real-time-collaboration)
   - [Event Sourcing Pattern](#event-sourcing-pattern)
   - [CQRS with DoSQL + DoLake](#cqrs-with-dosql--dolake)
4. [Framework Examples](#framework-examples)
   - [Remix App Example](#remix-app-example)
   - [Next.js App Example](#nextjs-app-example)
   - [Hono API Example](#hono-api-example)
   - [Plain Workers Example](#plain-workers-example)
5. [Testing Examples](#testing-examples)
   - [Unit Testing with Mocks](#unit-testing-with-mocks)
   - [Integration Testing](#integration-testing)
   - [E2E Testing Patterns](#e2e-testing-patterns)
6. [Performance Examples](#performance-examples)
   - [Query Optimization](#query-optimization)
   - [Connection Pooling](#connection-pooling)
   - [Caching Strategies](#caching-strategies)

---

## Quick Examples

### Basic CRUD Operations

Fundamental database operations with DoSQL.

#### Create (INSERT)

```typescript
import { createSQLClient } from 'sql.do';

const client = createSQLClient({
  url: 'https://sql.example.com',
  token: process.env.DOSQL_TOKEN,
});

// Simple insert
await client.exec(
  'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
  ['user-123', 'Alice', 'alice@example.com']
);

// Insert with returning
const result = await client.query<{ id: string; created_at: number }>(
  `INSERT INTO users (name, email) VALUES (?, ?) RETURNING id, created_at`,
  ['Bob', 'bob@example.com']
);
console.log('New user ID:', result.rows[0].id);

// Bulk insert
const users = [
  ['user-1', 'Alice', 'alice@example.com'],
  ['user-2', 'Bob', 'bob@example.com'],
  ['user-3', 'Charlie', 'charlie@example.com'],
];

for (const [id, name, email] of users) {
  await client.exec(
    'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
    [id, name, email]
  );
}
```

**When to use**: Any time you need to insert new records. Use `RETURNING` when you need auto-generated values like IDs or timestamps.

#### Read (SELECT)

```typescript
interface User {
  id: string;
  name: string;
  email: string;
  created_at: number;
}

// Select all
const allUsers = await client.query<User>('SELECT * FROM users');
console.log(`Found ${allUsers.rows.length} users`);

// Select with WHERE clause
const activeUsers = await client.query<User>(
  'SELECT * FROM users WHERE status = ? AND created_at > ?',
  ['active', Date.now() - 86400000]
);

// Select specific columns
const emails = await client.query<{ email: string }>(
  'SELECT email FROM users WHERE verified = ?',
  [true]
);

// Select with JOIN
interface UserWithOrders {
  user_id: string;
  user_name: string;
  order_count: number;
  total_spent: number;
}

const userStats = await client.query<UserWithOrders>(`
  SELECT
    u.id as user_id,
    u.name as user_name,
    COUNT(o.id) as order_count,
    COALESCE(SUM(o.total), 0) as total_spent
  FROM users u
  LEFT JOIN orders o ON u.id = o.user_id
  WHERE u.tenant_id = ?
  GROUP BY u.id, u.name
`, [tenantId]);

// Pagination
const page = 1;
const pageSize = 20;
const pagedUsers = await client.query<User>(
  'SELECT * FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?',
  [pageSize, (page - 1) * pageSize]
);
```

**When to use**: Data retrieval. Use JOINs for related data, pagination for large result sets.

#### Update (UPDATE)

```typescript
// Simple update
await client.exec(
  'UPDATE users SET name = ? WHERE id = ?',
  ['Alice Smith', 'user-123']
);

// Update with conditions
const updateResult = await client.exec(
  'UPDATE users SET last_login = ? WHERE id = ? AND status = ?',
  [Date.now(), 'user-123', 'active']
);
console.log(`Updated ${updateResult.changes} rows`);

// Update with RETURNING
const updated = await client.query<User>(
  `UPDATE users SET email = ? WHERE id = ? RETURNING *`,
  ['newemail@example.com', 'user-123']
);

// Conditional update (upsert pattern)
await client.exec(`
  INSERT INTO user_settings (user_id, theme, notifications)
  VALUES (?, ?, ?)
  ON CONFLICT(user_id) DO UPDATE SET
    theme = excluded.theme,
    notifications = excluded.notifications
`, ['user-123', 'dark', true]);
```

**When to use**: Modifying existing records. Use `ON CONFLICT` for upsert operations.

#### Delete (DELETE)

```typescript
// Simple delete
await client.exec('DELETE FROM users WHERE id = ?', ['user-123']);

// Conditional delete
const deleteResult = await client.exec(
  'DELETE FROM sessions WHERE expires_at < ?',
  [Date.now()]
);
console.log(`Deleted ${deleteResult.changes} expired sessions`);

// Soft delete pattern
await client.exec(
  'UPDATE users SET deleted_at = ? WHERE id = ?',
  [Date.now(), 'user-123']
);

// Delete with RETURNING
const deleted = await client.query<User>(
  'DELETE FROM users WHERE id = ? RETURNING *',
  ['user-123']
);
console.log('Deleted user:', deleted.rows[0].name);
```

**When to use**: Removing records. Consider soft deletes for audit trails or recovery needs.

---

### Transactions

Atomic operations that maintain data consistency.

#### Basic Transaction

```typescript
import { createSQLClient } from 'sql.do';

const client = createSQLClient({ url: 'https://sql.example.com' });

// Automatic rollback on error
await client.transaction(async (tx) => {
  // Debit from sender
  await tx.exec(
    'UPDATE accounts SET balance = balance - ? WHERE id = ?',
    [100, 'account-sender']
  );

  // Credit to receiver
  await tx.exec(
    'UPDATE accounts SET balance = balance + ? WHERE id = ?',
    [100, 'account-receiver']
  );

  // Create audit log
  await tx.exec(
    'INSERT INTO transfers (from_id, to_id, amount, timestamp) VALUES (?, ?, ?, ?)',
    ['account-sender', 'account-receiver', 100, Date.now()]
  );

  // If any statement fails, all changes are rolled back
});
```

**When to use**: Any operation requiring atomicity - money transfers, inventory updates, or multi-table modifications.

#### Transaction with Isolation Levels

```typescript
// Serializable isolation for strict consistency
await client.transaction(
  async (tx) => {
    const result = await tx.query<{ balance: number }>(
      'SELECT balance FROM accounts WHERE id = ?',
      ['account-123']
    );

    const currentBalance = result.rows[0].balance;

    if (currentBalance >= 100) {
      await tx.exec(
        'UPDATE accounts SET balance = ? WHERE id = ?',
        [currentBalance - 100, 'account-123']
      );
    } else {
      throw new Error('Insufficient funds');
    }
  },
  { isolation: 'serializable' }
);

// Read committed for better concurrency
await client.transaction(
  async (tx) => {
    const users = await tx.query('SELECT * FROM users WHERE status = ?', ['pending']);
    for (const user of users.rows) {
      await tx.exec('UPDATE users SET status = ? WHERE id = ?', ['processed', user.id]);
    }
  },
  { isolation: 'read_committed' }
);
```

**When to use**: Serializable for financial operations or race-condition-sensitive code. Read committed for batch processing with better throughput.

#### Savepoints for Partial Rollback

```typescript
await client.transaction(async (tx) => {
  await tx.exec('INSERT INTO orders (id, user_id) VALUES (?, ?)', ['order-1', 'user-123']);

  try {
    await tx.savepoint('items');

    // Try to add items - may fail on stock check
    await tx.exec('INSERT INTO order_items (order_id, product_id, qty) VALUES (?, ?, ?)',
      ['order-1', 'product-1', 5]);
    await tx.exec('UPDATE inventory SET qty = qty - 5 WHERE product_id = ?', ['product-1']);

    await tx.release('items');
  } catch (e) {
    // Rollback just the items, keep the order
    await tx.rollbackTo('items');
    console.log('Could not add items, order created without items');
  }
});
```

**When to use**: Complex transactions where partial failure is acceptable and should be handled gracefully.

---

### Batch Operations

Efficient bulk operations for high-throughput scenarios.

#### Batch Inserts

```typescript
const client = createSQLClient({ url: 'https://sql.example.com' });

// Batch insert with prepared statements
const users = [
  { id: 'u1', name: 'Alice', email: 'alice@example.com' },
  { id: 'u2', name: 'Bob', email: 'bob@example.com' },
  { id: 'u3', name: 'Charlie', email: 'charlie@example.com' },
];

await client.batch(
  'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
  users.map(u => [u.id, u.name, u.email])
);

// Mixed batch operations
await client.batchExec([
  { sql: 'INSERT INTO users (id, name) VALUES (?, ?)', params: ['u1', 'Alice'] },
  { sql: 'INSERT INTO users (id, name) VALUES (?, ?)', params: ['u2', 'Bob'] },
  { sql: 'UPDATE counters SET value = value + ? WHERE name = ?', params: [2, 'user_count'] },
]);
```

**When to use**: Importing data, seeding databases, or any operation with many similar statements.

#### Batch Queries

```typescript
// Multiple queries in one round-trip
const results = await client.batchQuery([
  { sql: 'SELECT COUNT(*) as count FROM users' },
  { sql: 'SELECT COUNT(*) as count FROM orders' },
  { sql: 'SELECT SUM(total) as revenue FROM orders WHERE created_at > ?', params: [Date.now() - 86400000] },
]);

const [userCount, orderCount, dailyRevenue] = results;
console.log(`Users: ${userCount.rows[0].count}`);
console.log(`Orders: ${orderCount.rows[0].count}`);
console.log(`Daily Revenue: $${dailyRevenue.rows[0].revenue}`);
```

**When to use**: Dashboard queries, reports, or any scenario requiring multiple independent queries.

---

## Real-World Patterns

### Multi-Tenant SaaS

Complete multi-tenant architecture with tenant isolation.

```typescript
import { createSQLClient, SQLClient } from 'sql.do';

// Tenant-aware client wrapper
class TenantDatabase {
  private client: SQLClient;
  private tenantId: string;

  constructor(tenantId: string) {
    this.tenantId = tenantId;
    this.client = createSQLClient({
      url: 'https://sql.example.com',
      token: process.env.DOSQL_TOKEN,
      // Route to tenant-specific Durable Object
      headers: { 'X-Tenant-ID': tenantId },
    });
  }

  async query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    // Automatically inject tenant filter
    const result = await this.client.query<T>(sql, params);
    return result.rows;
  }

  // Tenant-scoped operations
  async createUser(user: { name: string; email: string }) {
    return this.client.exec(
      'INSERT INTO users (id, tenant_id, name, email) VALUES (?, ?, ?, ?)',
      [crypto.randomUUID(), this.tenantId, user.name, user.email]
    );
  }

  async getUsers() {
    return this.query<User>(
      'SELECT * FROM users WHERE tenant_id = ? AND deleted_at IS NULL',
      [this.tenantId]
    );
  }

  async close() {
    await this.client.close();
  }
}

// Usage in request handler
export default {
  async fetch(request: Request, env: Env) {
    const tenantId = request.headers.get('X-Tenant-ID');
    if (!tenantId) {
      return new Response('Tenant ID required', { status: 400 });
    }

    const db = new TenantDatabase(tenantId);

    try {
      const users = await db.getUsers();
      return Response.json({ users });
    } finally {
      await db.close();
    }
  },
};
```

**Schema with row-level tenant isolation:**

```sql
-- Users table with tenant isolation
CREATE TABLE users (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  role TEXT DEFAULT 'member',
  created_at INTEGER DEFAULT (unixepoch() * 1000),
  deleted_at INTEGER
);

CREATE INDEX idx_users_tenant ON users(tenant_id);
CREATE UNIQUE INDEX idx_users_tenant_email ON users(tenant_id, email) WHERE deleted_at IS NULL;

-- Tenant settings
CREATE TABLE tenant_settings (
  tenant_id TEXT PRIMARY KEY,
  plan TEXT DEFAULT 'free',
  max_users INTEGER DEFAULT 5,
  features TEXT DEFAULT '{}', -- JSON
  created_at INTEGER DEFAULT (unixepoch() * 1000)
);

-- Audit log for compliance
CREATE TABLE audit_log (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  user_id TEXT,
  action TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  resource_id TEXT,
  metadata TEXT, -- JSON
  timestamp INTEGER DEFAULT (unixepoch() * 1000)
);

CREATE INDEX idx_audit_tenant_time ON audit_log(tenant_id, timestamp DESC);
```

**When to use**: Any SaaS application requiring data isolation between customers, compliance requirements, or per-tenant billing.

---

### E-commerce Application

Products, orders, and inventory management.

```typescript
import { createSQLClient, SQLClient, SQLError, RPCErrorCode } from 'sql.do';

interface Product {
  id: string;
  name: string;
  price: number;
  stock: number;
  category: string;
}

interface Order {
  id: string;
  user_id: string;
  status: 'pending' | 'paid' | 'shipped' | 'delivered' | 'cancelled';
  total: number;
  created_at: number;
}

interface OrderItem {
  order_id: string;
  product_id: string;
  quantity: number;
  unit_price: number;
}

class EcommerceStore {
  constructor(private client: SQLClient) {}

  // Product catalog
  async getProducts(category?: string, page = 1, limit = 20): Promise<Product[]> {
    const offset = (page - 1) * limit;

    if (category) {
      const result = await this.client.query<Product>(
        'SELECT * FROM products WHERE category = ? AND stock > 0 ORDER BY name LIMIT ? OFFSET ?',
        [category, limit, offset]
      );
      return result.rows;
    }

    const result = await this.client.query<Product>(
      'SELECT * FROM products WHERE stock > 0 ORDER BY name LIMIT ? OFFSET ?',
      [limit, offset]
    );
    return result.rows;
  }

  async searchProducts(query: string): Promise<Product[]> {
    const result = await this.client.query<Product>(
      `SELECT * FROM products
       WHERE name LIKE ? OR description LIKE ?
       ORDER BY name LIMIT 50`,
      [`%${query}%`, `%${query}%`]
    );
    return result.rows;
  }

  // Shopping cart and checkout
  async createOrder(
    userId: string,
    items: Array<{ productId: string; quantity: number }>
  ): Promise<string> {
    const orderId = crypto.randomUUID();

    await this.client.transaction(async (tx) => {
      // Validate stock and calculate total
      let total = 0;
      const itemDetails: Array<{ productId: string; quantity: number; price: number }> = [];

      for (const item of items) {
        const result = await tx.query<Product>(
          'SELECT * FROM products WHERE id = ? FOR UPDATE',
          [item.productId]
        );

        if (result.rows.length === 0) {
          throw new Error(`Product ${item.productId} not found`);
        }

        const product = result.rows[0];

        if (product.stock < item.quantity) {
          throw new Error(`Insufficient stock for ${product.name}`);
        }

        itemDetails.push({
          productId: item.productId,
          quantity: item.quantity,
          price: product.price,
        });

        total += product.price * item.quantity;
      }

      // Create order
      await tx.exec(
        'INSERT INTO orders (id, user_id, status, total, created_at) VALUES (?, ?, ?, ?, ?)',
        [orderId, userId, 'pending', total, Date.now()]
      );

      // Create order items and update stock
      for (const item of itemDetails) {
        await tx.exec(
          'INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)',
          [orderId, item.productId, item.quantity, item.price]
        );

        await tx.exec(
          'UPDATE products SET stock = stock - ? WHERE id = ?',
          [item.quantity, item.productId]
        );
      }
    }, { isolation: 'serializable' });

    return orderId;
  }

  // Order management
  async getOrder(orderId: string): Promise<{ order: Order; items: OrderItem[] } | null> {
    const results = await this.client.batchQuery([
      { sql: 'SELECT * FROM orders WHERE id = ?', params: [orderId] },
      {
        sql: `SELECT oi.*, p.name as product_name
              FROM order_items oi
              JOIN products p ON oi.product_id = p.id
              WHERE oi.order_id = ?`,
        params: [orderId]
      },
    ]);

    if (results[0].rows.length === 0) return null;

    return {
      order: results[0].rows[0] as Order,
      items: results[1].rows as OrderItem[],
    };
  }

  async cancelOrder(orderId: string): Promise<void> {
    await this.client.transaction(async (tx) => {
      // Get order
      const orderResult = await tx.query<Order>(
        'SELECT * FROM orders WHERE id = ?',
        [orderId]
      );

      if (orderResult.rows.length === 0) {
        throw new Error('Order not found');
      }

      if (orderResult.rows[0].status !== 'pending') {
        throw new Error('Can only cancel pending orders');
      }

      // Restore stock
      const items = await tx.query<OrderItem>(
        'SELECT * FROM order_items WHERE order_id = ?',
        [orderId]
      );

      for (const item of items.rows) {
        await tx.exec(
          'UPDATE products SET stock = stock + ? WHERE id = ?',
          [item.quantity, item.product_id]
        );
      }

      // Update order status
      await tx.exec(
        'UPDATE orders SET status = ? WHERE id = ?',
        ['cancelled', orderId]
      );
    });
  }

  // Inventory management
  async restockProduct(productId: string, quantity: number): Promise<void> {
    await this.client.exec(
      'UPDATE products SET stock = stock + ? WHERE id = ?',
      [quantity, productId]
    );
  }

  async getLowStockProducts(threshold = 10): Promise<Product[]> {
    const result = await this.client.query<Product>(
      'SELECT * FROM products WHERE stock <= ? ORDER BY stock ASC',
      [threshold]
    );
    return result.rows;
  }
}

// Schema
const ecommerceSchema = `
CREATE TABLE products (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  price REAL NOT NULL,
  stock INTEGER NOT NULL DEFAULT 0,
  category TEXT NOT NULL,
  image_url TEXT,
  created_at INTEGER DEFAULT (unixepoch() * 1000)
);

CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_stock ON products(stock);

CREATE TABLE orders (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  total REAL NOT NULL,
  shipping_address TEXT,
  created_at INTEGER DEFAULT (unixepoch() * 1000),
  updated_at INTEGER
);

CREATE INDEX idx_orders_user ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);

CREATE TABLE order_items (
  order_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  quantity INTEGER NOT NULL,
  unit_price REAL NOT NULL,
  PRIMARY KEY (order_id, product_id),
  FOREIGN KEY (order_id) REFERENCES orders(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);
`;
```

**When to use**: Online stores, marketplaces, or any application with product catalogs and transactional ordering.

---

### Social Application

Posts, comments, likes, and feed generation.

```typescript
import { createSQLClient, SQLClient } from 'sql.do';

interface Post {
  id: string;
  author_id: string;
  author_name: string;
  content: string;
  media_url?: string;
  like_count: number;
  comment_count: number;
  created_at: number;
}

interface Comment {
  id: string;
  post_id: string;
  author_id: string;
  author_name: string;
  content: string;
  created_at: number;
}

class SocialApp {
  constructor(private client: SQLClient) {}

  // Posts
  async createPost(authorId: string, content: string, mediaUrl?: string): Promise<string> {
    const postId = crypto.randomUUID();

    await this.client.exec(
      `INSERT INTO posts (id, author_id, content, media_url, created_at)
       VALUES (?, ?, ?, ?, ?)`,
      [postId, authorId, content, mediaUrl, Date.now()]
    );

    return postId;
  }

  async getPost(postId: string): Promise<Post | null> {
    const result = await this.client.query<Post>(`
      SELECT
        p.*,
        u.name as author_name,
        (SELECT COUNT(*) FROM likes WHERE post_id = p.id) as like_count,
        (SELECT COUNT(*) FROM comments WHERE post_id = p.id) as comment_count
      FROM posts p
      JOIN users u ON p.author_id = u.id
      WHERE p.id = ?
    `, [postId]);

    return result.rows[0] || null;
  }

  async deletePost(postId: string, userId: string): Promise<void> {
    await this.client.transaction(async (tx) => {
      // Verify ownership
      const post = await tx.query<{ author_id: string }>(
        'SELECT author_id FROM posts WHERE id = ?',
        [postId]
      );

      if (post.rows.length === 0) {
        throw new Error('Post not found');
      }

      if (post.rows[0].author_id !== userId) {
        throw new Error('Not authorized to delete this post');
      }

      // Delete likes
      await tx.exec('DELETE FROM likes WHERE post_id = ?', [postId]);

      // Delete comments
      await tx.exec('DELETE FROM comments WHERE post_id = ?', [postId]);

      // Delete post
      await tx.exec('DELETE FROM posts WHERE id = ?', [postId]);
    });
  }

  // Comments
  async addComment(postId: string, authorId: string, content: string): Promise<string> {
    const commentId = crypto.randomUUID();

    await this.client.exec(
      `INSERT INTO comments (id, post_id, author_id, content, created_at)
       VALUES (?, ?, ?, ?, ?)`,
      [commentId, postId, authorId, content, Date.now()]
    );

    return commentId;
  }

  async getComments(postId: string, limit = 20, cursor?: number): Promise<Comment[]> {
    const result = await this.client.query<Comment>(`
      SELECT c.*, u.name as author_name
      FROM comments c
      JOIN users u ON c.author_id = u.id
      WHERE c.post_id = ?
      ${cursor ? 'AND c.created_at < ?' : ''}
      ORDER BY c.created_at DESC
      LIMIT ?
    `, cursor ? [postId, cursor, limit] : [postId, limit]);

    return result.rows;
  }

  // Likes
  async likePost(postId: string, userId: string): Promise<void> {
    await this.client.exec(
      `INSERT INTO likes (post_id, user_id, created_at)
       VALUES (?, ?, ?)
       ON CONFLICT(post_id, user_id) DO NOTHING`,
      [postId, userId, Date.now()]
    );
  }

  async unlikePost(postId: string, userId: string): Promise<void> {
    await this.client.exec(
      'DELETE FROM likes WHERE post_id = ? AND user_id = ?',
      [postId, userId]
    );
  }

  async hasUserLiked(postId: string, userId: string): Promise<boolean> {
    const result = await this.client.query<{ count: number }>(
      'SELECT COUNT(*) as count FROM likes WHERE post_id = ? AND user_id = ?',
      [postId, userId]
    );
    return result.rows[0].count > 0;
  }

  // Feed generation
  async getUserFeed(userId: string, limit = 20, cursor?: number): Promise<Post[]> {
    // Get posts from followed users and own posts
    const result = await this.client.query<Post>(`
      SELECT
        p.*,
        u.name as author_name,
        (SELECT COUNT(*) FROM likes WHERE post_id = p.id) as like_count,
        (SELECT COUNT(*) FROM comments WHERE post_id = p.id) as comment_count,
        EXISTS(SELECT 1 FROM likes WHERE post_id = p.id AND user_id = ?) as is_liked
      FROM posts p
      JOIN users u ON p.author_id = u.id
      WHERE p.author_id IN (
        SELECT followed_id FROM follows WHERE follower_id = ?
        UNION ALL
        SELECT ?
      )
      ${cursor ? 'AND p.created_at < ?' : ''}
      ORDER BY p.created_at DESC
      LIMIT ?
    `, cursor
      ? [userId, userId, userId, cursor, limit]
      : [userId, userId, userId, limit]
    );

    return result.rows;
  }

  // Following
  async followUser(followerId: string, followedId: string): Promise<void> {
    if (followerId === followedId) {
      throw new Error('Cannot follow yourself');
    }

    await this.client.exec(
      `INSERT INTO follows (follower_id, followed_id, created_at)
       VALUES (?, ?, ?)
       ON CONFLICT DO NOTHING`,
      [followerId, followedId, Date.now()]
    );
  }

  async unfollowUser(followerId: string, followedId: string): Promise<void> {
    await this.client.exec(
      'DELETE FROM follows WHERE follower_id = ? AND followed_id = ?',
      [followerId, followedId]
    );
  }

  async getFollowers(userId: string): Promise<Array<{ id: string; name: string }>> {
    const result = await this.client.query<{ id: string; name: string }>(`
      SELECT u.id, u.name
      FROM follows f
      JOIN users u ON f.follower_id = u.id
      WHERE f.followed_id = ?
      ORDER BY f.created_at DESC
    `, [userId]);
    return result.rows;
  }

  async getFollowing(userId: string): Promise<Array<{ id: string; name: string }>> {
    const result = await this.client.query<{ id: string; name: string }>(`
      SELECT u.id, u.name
      FROM follows f
      JOIN users u ON f.followed_id = u.id
      WHERE f.follower_id = ?
      ORDER BY f.created_at DESC
    `, [userId]);
    return result.rows;
  }
}

// Schema
const socialSchema = `
CREATE TABLE users (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  bio TEXT,
  avatar_url TEXT,
  created_at INTEGER DEFAULT (unixepoch() * 1000)
);

CREATE TABLE posts (
  id TEXT PRIMARY KEY,
  author_id TEXT NOT NULL REFERENCES users(id),
  content TEXT NOT NULL,
  media_url TEXT,
  created_at INTEGER DEFAULT (unixepoch() * 1000)
);

CREATE INDEX idx_posts_author ON posts(author_id);
CREATE INDEX idx_posts_created ON posts(created_at DESC);

CREATE TABLE comments (
  id TEXT PRIMARY KEY,
  post_id TEXT NOT NULL REFERENCES posts(id),
  author_id TEXT NOT NULL REFERENCES users(id),
  content TEXT NOT NULL,
  created_at INTEGER DEFAULT (unixepoch() * 1000)
);

CREATE INDEX idx_comments_post ON comments(post_id, created_at DESC);

CREATE TABLE likes (
  post_id TEXT NOT NULL REFERENCES posts(id),
  user_id TEXT NOT NULL REFERENCES users(id),
  created_at INTEGER DEFAULT (unixepoch() * 1000),
  PRIMARY KEY (post_id, user_id)
);

CREATE INDEX idx_likes_user ON likes(user_id);

CREATE TABLE follows (
  follower_id TEXT NOT NULL REFERENCES users(id),
  followed_id TEXT NOT NULL REFERENCES users(id),
  created_at INTEGER DEFAULT (unixepoch() * 1000),
  PRIMARY KEY (follower_id, followed_id)
);

CREATE INDEX idx_follows_followed ON follows(followed_id);
`;
```

**When to use**: Social networks, content platforms, community applications, or any app with user-generated content and relationships.

---

### Analytics Dashboard

Aggregations, time-series data, and reporting.

```typescript
import { createSQLClient, SQLClient } from 'sql.do';
import { createLakeClient, LakeClient } from 'lake.do';

interface DailyMetrics {
  date: string;
  total_users: number;
  active_users: number;
  new_users: number;
  total_revenue: number;
  orders: number;
  avg_order_value: number;
}

interface TopProduct {
  product_id: string;
  product_name: string;
  units_sold: number;
  revenue: number;
}

class AnalyticsDashboard {
  constructor(
    private sql: SQLClient,
    private lake: LakeClient
  ) {}

  // Real-time metrics from DoSQL (hot data)
  async getTodayMetrics(): Promise<DailyMetrics> {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const todayStart = today.getTime();

    const results = await this.sql.batchQuery([
      { sql: 'SELECT COUNT(*) as count FROM users' },
      { sql: 'SELECT COUNT(DISTINCT user_id) as count FROM events WHERE timestamp > ?', params: [todayStart] },
      { sql: 'SELECT COUNT(*) as count FROM users WHERE created_at > ?', params: [todayStart] },
      { sql: 'SELECT COALESCE(SUM(total), 0) as sum FROM orders WHERE created_at > ?', params: [todayStart] },
      { sql: 'SELECT COUNT(*) as count FROM orders WHERE created_at > ?', params: [todayStart] },
    ]);

    const totalUsers = results[0].rows[0].count as number;
    const activeUsers = results[1].rows[0].count as number;
    const newUsers = results[2].rows[0].count as number;
    const revenue = results[3].rows[0].sum as number;
    const orders = results[4].rows[0].count as number;

    return {
      date: today.toISOString().split('T')[0],
      total_users: totalUsers,
      active_users: activeUsers,
      new_users: newUsers,
      total_revenue: revenue,
      orders,
      avg_order_value: orders > 0 ? revenue / orders : 0,
    };
  }

  // Historical metrics from DoLake (cold data)
  async getHistoricalMetrics(startDate: string, endDate: string): Promise<DailyMetrics[]> {
    const result = await this.lake.query<DailyMetrics>(`
      SELECT
        date,
        total_users,
        active_users,
        new_users,
        total_revenue,
        orders,
        avg_order_value
      FROM daily_metrics
      WHERE date >= ? AND date <= ?
      ORDER BY date
    `, [startDate, endDate]);

    return result.rows;
  }

  // Top products report
  async getTopProducts(days = 30, limit = 10): Promise<TopProduct[]> {
    const cutoff = Date.now() - days * 86400000;

    const result = await this.sql.query<TopProduct>(`
      SELECT
        p.id as product_id,
        p.name as product_name,
        SUM(oi.quantity) as units_sold,
        SUM(oi.quantity * oi.unit_price) as revenue
      FROM order_items oi
      JOIN products p ON oi.product_id = p.id
      JOIN orders o ON oi.order_id = o.id
      WHERE o.created_at > ? AND o.status != 'cancelled'
      GROUP BY p.id, p.name
      ORDER BY revenue DESC
      LIMIT ?
    `, [cutoff, limit]);

    return result.rows;
  }

  // User cohort analysis
  async getCohortRetention(cohortMonth: string): Promise<Array<{ month: number; retention: number }>> {
    const result = await this.lake.query<{ month: number; retention: number }>(`
      WITH cohort AS (
        SELECT user_id, MIN(DATE_TRUNC('month', created_at)) as cohort_month
        FROM users
        WHERE DATE_TRUNC('month', created_at) = ?
        GROUP BY user_id
      ),
      activity AS (
        SELECT DISTINCT user_id, DATE_TRUNC('month', timestamp) as activity_month
        FROM events
      )
      SELECT
        DATEDIFF('month', c.cohort_month, a.activity_month) as month,
        COUNT(DISTINCT a.user_id)::FLOAT / COUNT(DISTINCT c.user_id) * 100 as retention
      FROM cohort c
      LEFT JOIN activity a ON c.user_id = a.user_id
      GROUP BY month
      ORDER BY month
    `, [cohortMonth]);

    return result.rows;
  }

  // Funnel analysis
  async getFunnelMetrics(startDate: string, endDate: string): Promise<{
    visitors: number;
    signups: number;
    activations: number;
    purchases: number;
  }> {
    const results = await this.sql.batchQuery([
      {
        sql: `SELECT COUNT(DISTINCT session_id) as count FROM events
              WHERE event_type = 'page_view' AND timestamp BETWEEN ? AND ?`,
        params: [startDate, endDate]
      },
      {
        sql: `SELECT COUNT(*) as count FROM users
              WHERE created_at BETWEEN ? AND ?`,
        params: [startDate, endDate]
      },
      {
        sql: `SELECT COUNT(DISTINCT user_id) as count FROM events
              WHERE event_type = 'activation' AND timestamp BETWEEN ? AND ?`,
        params: [startDate, endDate]
      },
      {
        sql: `SELECT COUNT(DISTINCT user_id) as count FROM orders
              WHERE created_at BETWEEN ? AND ?`,
        params: [startDate, endDate]
      },
    ]);

    return {
      visitors: results[0].rows[0].count as number,
      signups: results[1].rows[0].count as number,
      activations: results[2].rows[0].count as number,
      purchases: results[3].rows[0].count as number,
    };
  }

  // Revenue by geography
  async getRevenueByRegion(): Promise<Array<{ region: string; revenue: number; orders: number }>> {
    const result = await this.sql.query<{ region: string; revenue: number; orders: number }>(`
      SELECT
        u.region,
        SUM(o.total) as revenue,
        COUNT(o.id) as orders
      FROM orders o
      JOIN users u ON o.user_id = u.id
      WHERE o.status = 'delivered'
      GROUP BY u.region
      ORDER BY revenue DESC
    `);

    return result.rows;
  }
}

// Usage
const dashboard = new AnalyticsDashboard(sqlClient, lakeClient);

// Get today's snapshot
const today = await dashboard.getTodayMetrics();
console.log(`Active users today: ${today.active_users}`);

// Get last 30 days historical data
const historical = await dashboard.getHistoricalMetrics('2026-01-01', '2026-01-22');

// Get top products
const topProducts = await dashboard.getTopProducts(30, 10);
```

**When to use**: Business intelligence dashboards, admin panels, reporting systems, or any application requiring aggregated metrics.

---

## Advanced Examples

### CDC Streaming to Analytics

Change Data Capture for real-time data pipelines.

```typescript
import { createSQLClient } from 'sql.do';
import { createLakeClient, CDCEvent } from 'lake.do';

interface User {
  id: string;
  name: string;
  email: string;
  plan: string;
}

// CDC subscription for real-time sync
async function setupCDCPipeline(env: Env) {
  const lake = createLakeClient({
    url: 'https://lake.example.com',
    token: env.LAKE_TOKEN,
  });

  // Subscribe to user changes
  const subscription = await lake.subscribeToCDC({
    tables: ['users', 'orders'],
    startLSN: 0, // Start from beginning, or provide checkpoint

    onEvent: async (event: CDCEvent) => {
      console.log(`CDC Event: ${event.type} on ${event.table}`);

      switch (event.type) {
        case 'INSERT':
          await handleInsert(event);
          break;
        case 'UPDATE':
          await handleUpdate(event);
          break;
        case 'DELETE':
          await handleDelete(event);
          break;
      }
    },

    onCheckpoint: async (lsn: number) => {
      // Persist checkpoint for recovery
      await env.KV.put('cdc_checkpoint', lsn.toString());
    },

    onError: (error) => {
      console.error('CDC error:', error);
    },
  });

  return subscription;
}

async function handleInsert(event: CDCEvent) {
  if (event.table === 'users') {
    // Sync to external analytics
    await fetch('https://analytics.example.com/users', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(event.newValue),
    });

    // Send welcome email
    await sendWelcomeEmail(event.newValue as User);
  }

  if (event.table === 'orders') {
    // Update real-time dashboard
    await updateDashboardMetrics(event.newValue);
  }
}

async function handleUpdate(event: CDCEvent) {
  if (event.table === 'users') {
    const oldUser = event.oldValue as User;
    const newUser = event.newValue as User;

    // Plan upgrade detection
    if (oldUser.plan !== newUser.plan) {
      await trackPlanChange(oldUser.id, oldUser.plan, newUser.plan);
    }
  }
}

async function handleDelete(event: CDCEvent) {
  // Soft delete in analytics
  await fetch(`https://analytics.example.com/${event.table}/${event.oldValue.id}`, {
    method: 'DELETE',
  });
}

// Worker entry point for CDC consumer
export default {
  async fetch(request: Request, env: Env) {
    // Health check endpoint
    if (request.url.endsWith('/health')) {
      return new Response('OK');
    }

    return new Response('CDC Consumer Running');
  },

  async scheduled(event: ScheduledEvent, env: Env) {
    // Run CDC sync on schedule
    const subscription = await setupCDCPipeline(env);

    // Process for limited time, then let alarm handle continuation
    await subscription.consume({ maxEvents: 1000, timeoutMs: 25000 });
  },
};
```

**When to use**: Data warehousing, search index updates, cache invalidation, audit logging, or any scenario requiring reaction to data changes.

---

### Real-time Collaboration

Live document editing with conflict resolution.

```typescript
import { createSQLClient, SQLClient } from 'sql.do';

interface Document {
  id: string;
  title: string;
  content: string;
  version: number;
  updated_at: number;
}

interface Operation {
  id: string;
  document_id: string;
  user_id: string;
  type: 'insert' | 'delete' | 'replace';
  position: number;
  content?: string;
  length?: number;
  version: number;
  timestamp: number;
}

class CollaborativeEditor {
  constructor(private client: SQLClient) {}

  // Create document
  async createDocument(title: string, creatorId: string): Promise<string> {
    const docId = crypto.randomUUID();

    await this.client.transaction(async (tx) => {
      await tx.exec(
        `INSERT INTO documents (id, title, content, version, created_by, created_at, updated_at)
         VALUES (?, ?, '', 0, ?, ?, ?)`,
        [docId, title, creatorId, Date.now(), Date.now()]
      );

      await tx.exec(
        `INSERT INTO document_access (document_id, user_id, role)
         VALUES (?, ?, 'owner')`,
        [docId, creatorId]
      );
    });

    return docId;
  }

  // Apply operation with optimistic concurrency
  async applyOperation(op: Omit<Operation, 'id' | 'timestamp'>): Promise<{
    success: boolean;
    currentVersion: number;
    operations?: Operation[];
  }> {
    const opId = crypto.randomUUID();

    try {
      return await this.client.transaction(async (tx) => {
        // Get current document state
        const docResult = await tx.query<Document>(
          'SELECT * FROM documents WHERE id = ?',
          [op.document_id]
        );

        if (docResult.rows.length === 0) {
          throw new Error('Document not found');
        }

        const doc = docResult.rows[0];

        // Check for version conflict
        if (op.version < doc.version) {
          // Get operations since client's version
          const missedOps = await tx.query<Operation>(
            `SELECT * FROM operations
             WHERE document_id = ? AND version > ?
             ORDER BY version`,
            [op.document_id, op.version]
          );

          return {
            success: false,
            currentVersion: doc.version,
            operations: missedOps.rows,
          };
        }

        // Apply operation to content
        let newContent = doc.content;

        switch (op.type) {
          case 'insert':
            newContent = doc.content.slice(0, op.position) +
                        op.content +
                        doc.content.slice(op.position);
            break;
          case 'delete':
            newContent = doc.content.slice(0, op.position) +
                        doc.content.slice(op.position + (op.length || 0));
            break;
          case 'replace':
            newContent = doc.content.slice(0, op.position) +
                        op.content +
                        doc.content.slice(op.position + (op.length || 0));
            break;
        }

        const newVersion = doc.version + 1;
        const timestamp = Date.now();

        // Update document
        await tx.exec(
          `UPDATE documents SET content = ?, version = ?, updated_at = ?
           WHERE id = ?`,
          [newContent, newVersion, timestamp, op.document_id]
        );

        // Store operation for history and sync
        await tx.exec(
          `INSERT INTO operations (id, document_id, user_id, type, position, content, length, version, timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [opId, op.document_id, op.user_id, op.type, op.position, op.content, op.length, newVersion, timestamp]
        );

        return {
          success: true,
          currentVersion: newVersion,
        };
      }, { isolation: 'serializable' });
    } catch (error) {
      console.error('Operation failed:', error);
      throw error;
    }
  }

  // Get operations since version (for sync)
  async getOperationsSince(documentId: string, sinceVersion: number): Promise<Operation[]> {
    const result = await this.client.query<Operation>(
      `SELECT * FROM operations
       WHERE document_id = ? AND version > ?
       ORDER BY version`,
      [documentId, sinceVersion]
    );
    return result.rows;
  }

  // Get document with access check
  async getDocument(documentId: string, userId: string): Promise<Document | null> {
    const result = await this.client.query<Document>(`
      SELECT d.*
      FROM documents d
      JOIN document_access a ON d.id = a.document_id
      WHERE d.id = ? AND a.user_id = ?
    `, [documentId, userId]);

    return result.rows[0] || null;
  }

  // Share document
  async shareDocument(documentId: string, ownerId: string, targetUserId: string, role: 'viewer' | 'editor') {
    // Verify ownership
    const access = await this.client.query<{ role: string }>(
      `SELECT role FROM document_access WHERE document_id = ? AND user_id = ?`,
      [documentId, ownerId]
    );

    if (access.rows[0]?.role !== 'owner') {
      throw new Error('Only owners can share documents');
    }

    await this.client.exec(
      `INSERT INTO document_access (document_id, user_id, role)
       VALUES (?, ?, ?)
       ON CONFLICT(document_id, user_id) DO UPDATE SET role = ?`,
      [documentId, targetUserId, role, role]
    );
  }
}

// Schema
const collaborationSchema = `
CREATE TABLE documents (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  content TEXT DEFAULT '',
  version INTEGER DEFAULT 0,
  created_by TEXT NOT NULL,
  created_at INTEGER DEFAULT (unixepoch() * 1000),
  updated_at INTEGER
);

CREATE TABLE document_access (
  document_id TEXT NOT NULL REFERENCES documents(id),
  user_id TEXT NOT NULL,
  role TEXT NOT NULL CHECK(role IN ('owner', 'editor', 'viewer')),
  PRIMARY KEY (document_id, user_id)
);

CREATE INDEX idx_access_user ON document_access(user_id);

CREATE TABLE operations (
  id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL REFERENCES documents(id),
  user_id TEXT NOT NULL,
  type TEXT NOT NULL,
  position INTEGER NOT NULL,
  content TEXT,
  length INTEGER,
  version INTEGER NOT NULL,
  timestamp INTEGER DEFAULT (unixepoch() * 1000)
);

CREATE INDEX idx_ops_doc_version ON operations(document_id, version);
`;
```

**When to use**: Collaborative editing (docs, spreadsheets), real-time whiteboards, multiplayer games, or any app requiring synchronized state.

---

### Event Sourcing Pattern

Store events as the source of truth, derive state.

```typescript
import { createSQLClient, SQLClient } from 'sql.do';

// Domain events
type AccountEvent =
  | { type: 'AccountOpened'; accountId: string; ownerId: string; initialDeposit: number; timestamp: number }
  | { type: 'MoneyDeposited'; accountId: string; amount: number; timestamp: number }
  | { type: 'MoneyWithdrawn'; accountId: string; amount: number; timestamp: number }
  | { type: 'AccountClosed'; accountId: string; timestamp: number };

// Aggregate state
interface AccountState {
  id: string;
  ownerId: string;
  balance: number;
  status: 'open' | 'closed';
  createdAt: number;
}

class EventSourcedAccount {
  constructor(private client: SQLClient) {}

  // Append event to stream
  private async appendEvent(streamId: string, event: AccountEvent, expectedVersion: number): Promise<number> {
    return await this.client.transaction(async (tx) => {
      // Check current version for optimistic concurrency
      const versionResult = await tx.query<{ max_version: number }>(`
        SELECT COALESCE(MAX(version), 0) as max_version
        FROM events WHERE stream_id = ?
      `, [streamId]);

      const currentVersion = versionResult.rows[0].max_version;

      if (currentVersion !== expectedVersion) {
        throw new Error(`Concurrency conflict: expected version ${expectedVersion}, got ${currentVersion}`);
      }

      const newVersion = currentVersion + 1;

      await tx.exec(
        `INSERT INTO events (stream_id, version, event_type, event_data, timestamp)
         VALUES (?, ?, ?, ?, ?)`,
        [streamId, newVersion, event.type, JSON.stringify(event), event.timestamp]
      );

      return newVersion;
    }, { isolation: 'serializable' });
  }

  // Rebuild state from events
  private async rebuildState(streamId: string): Promise<{ state: AccountState | null; version: number }> {
    const eventsResult = await this.client.query<{ event_type: string; event_data: string; version: number }>(
      `SELECT event_type, event_data, version FROM events
       WHERE stream_id = ? ORDER BY version`,
      [streamId]
    );

    if (eventsResult.rows.length === 0) {
      return { state: null, version: 0 };
    }

    let state: AccountState | null = null;
    let version = 0;

    for (const row of eventsResult.rows) {
      const event = JSON.parse(row.event_data) as AccountEvent;
      state = this.applyEvent(state, event);
      version = row.version;
    }

    return { state, version };
  }

  // Event application (pure function)
  private applyEvent(state: AccountState | null, event: AccountEvent): AccountState {
    switch (event.type) {
      case 'AccountOpened':
        return {
          id: event.accountId,
          ownerId: event.ownerId,
          balance: event.initialDeposit,
          status: 'open',
          createdAt: event.timestamp,
        };

      case 'MoneyDeposited':
        if (!state) throw new Error('Cannot deposit to non-existent account');
        return { ...state, balance: state.balance + event.amount };

      case 'MoneyWithdrawn':
        if (!state) throw new Error('Cannot withdraw from non-existent account');
        return { ...state, balance: state.balance - event.amount };

      case 'AccountClosed':
        if (!state) throw new Error('Cannot close non-existent account');
        return { ...state, status: 'closed' };

      default:
        return state!;
    }
  }

  // Commands
  async openAccount(ownerId: string, initialDeposit: number): Promise<string> {
    const accountId = crypto.randomUUID();

    await this.appendEvent(accountId, {
      type: 'AccountOpened',
      accountId,
      ownerId,
      initialDeposit,
      timestamp: Date.now(),
    }, 0);

    return accountId;
  }

  async deposit(accountId: string, amount: number): Promise<void> {
    if (amount <= 0) throw new Error('Amount must be positive');

    const { state, version } = await this.rebuildState(accountId);

    if (!state) throw new Error('Account not found');
    if (state.status === 'closed') throw new Error('Account is closed');

    await this.appendEvent(accountId, {
      type: 'MoneyDeposited',
      accountId,
      amount,
      timestamp: Date.now(),
    }, version);
  }

  async withdraw(accountId: string, amount: number): Promise<void> {
    if (amount <= 0) throw new Error('Amount must be positive');

    const { state, version } = await this.rebuildState(accountId);

    if (!state) throw new Error('Account not found');
    if (state.status === 'closed') throw new Error('Account is closed');
    if (state.balance < amount) throw new Error('Insufficient funds');

    await this.appendEvent(accountId, {
      type: 'MoneyWithdrawn',
      accountId,
      amount,
      timestamp: Date.now(),
    }, version);
  }

  async closeAccount(accountId: string): Promise<void> {
    const { state, version } = await this.rebuildState(accountId);

    if (!state) throw new Error('Account not found');
    if (state.status === 'closed') throw new Error('Account already closed');
    if (state.balance !== 0) throw new Error('Account balance must be zero to close');

    await this.appendEvent(accountId, {
      type: 'AccountClosed',
      accountId,
      timestamp: Date.now(),
    }, version);
  }

  // Query
  async getAccount(accountId: string): Promise<AccountState | null> {
    const { state } = await this.rebuildState(accountId);
    return state;
  }

  // Get event history
  async getAccountHistory(accountId: string): Promise<AccountEvent[]> {
    const result = await this.client.query<{ event_data: string }>(
      `SELECT event_data FROM events WHERE stream_id = ? ORDER BY version`,
      [accountId]
    );
    return result.rows.map(r => JSON.parse(r.event_data));
  }
}

// Schema
const eventSourcingSchema = `
CREATE TABLE events (
  stream_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  event_data TEXT NOT NULL,
  timestamp INTEGER DEFAULT (unixepoch() * 1000),
  PRIMARY KEY (stream_id, version)
);

CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_events_timestamp ON events(timestamp);

-- Optional: Snapshots for performance
CREATE TABLE snapshots (
  stream_id TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  state_data TEXT NOT NULL,
  created_at INTEGER DEFAULT (unixepoch() * 1000)
);
`;
```

**When to use**: Financial systems, audit-heavy applications, systems requiring complete history, or domains where "why" matters as much as "what".

---

### CQRS with DoSQL + DoLake

Command Query Responsibility Segregation using both databases.

```typescript
import { createSQLClient, SQLClient } from 'sql.do';
import { createLakeClient, LakeClient } from 'lake.do';

// Write model (DoSQL) - Commands
class OrderCommandHandler {
  constructor(private sql: SQLClient) {}

  async createOrder(command: {
    userId: string;
    items: Array<{ productId: string; quantity: number; price: number }>;
    shippingAddress: string;
  }): Promise<string> {
    const orderId = crypto.randomUUID();
    const total = command.items.reduce((sum, item) => sum + item.price * item.quantity, 0);

    await this.sql.transaction(async (tx) => {
      // Create order
      await tx.exec(
        `INSERT INTO orders (id, user_id, status, total, shipping_address, created_at)
         VALUES (?, ?, 'pending', ?, ?, ?)`,
        [orderId, command.userId, total, command.shippingAddress, Date.now()]
      );

      // Create items
      for (const item of command.items) {
        await tx.exec(
          `INSERT INTO order_items (order_id, product_id, quantity, unit_price)
           VALUES (?, ?, ?, ?)`,
          [orderId, item.productId, item.quantity, item.price]
        );
      }

      // Emit domain event (CDC will sync to read model)
      await tx.exec(
        `INSERT INTO domain_events (id, aggregate_type, aggregate_id, event_type, event_data, timestamp)
         VALUES (?, 'Order', ?, 'OrderCreated', ?, ?)`,
        [crypto.randomUUID(), orderId, JSON.stringify({
          orderId,
          userId: command.userId,
          items: command.items,
          total,
        }), Date.now()]
      );
    });

    return orderId;
  }

  async shipOrder(orderId: string, trackingNumber: string): Promise<void> {
    await this.sql.transaction(async (tx) => {
      const result = await tx.query<{ status: string }>(
        'SELECT status FROM orders WHERE id = ?',
        [orderId]
      );

      if (result.rows.length === 0) throw new Error('Order not found');
      if (result.rows[0].status !== 'paid') throw new Error('Can only ship paid orders');

      await tx.exec(
        `UPDATE orders SET status = 'shipped', tracking_number = ?, shipped_at = ?
         WHERE id = ?`,
        [trackingNumber, Date.now(), orderId]
      );

      await tx.exec(
        `INSERT INTO domain_events (id, aggregate_type, aggregate_id, event_type, event_data, timestamp)
         VALUES (?, 'Order', ?, 'OrderShipped', ?, ?)`,
        [crypto.randomUUID(), orderId, JSON.stringify({ orderId, trackingNumber }), Date.now()]
      );
    });
  }
}

// Read model (DoLake) - Queries
class OrderQueryHandler {
  constructor(private lake: LakeClient) {}

  // Complex analytical query - runs on DoLake
  async getOrderAnalytics(userId: string): Promise<{
    totalOrders: number;
    totalSpent: number;
    avgOrderValue: number;
    topProducts: Array<{ name: string; quantity: number }>;
  }> {
    const results = await this.lake.batchQuery([
      {
        sql: `SELECT COUNT(*) as total, SUM(total) as spent
              FROM orders_view WHERE user_id = ?`,
        params: [userId]
      },
      {
        sql: `SELECT p.name, SUM(oi.quantity) as quantity
              FROM order_items_view oi
              JOIN products_view p ON oi.product_id = p.id
              JOIN orders_view o ON oi.order_id = o.id
              WHERE o.user_id = ?
              GROUP BY p.id, p.name
              ORDER BY quantity DESC
              LIMIT 5`,
        params: [userId]
      },
    ]);

    const stats = results[0].rows[0] as { total: number; spent: number };
    const products = results[1].rows as Array<{ name: string; quantity: number }>;

    return {
      totalOrders: stats.total,
      totalSpent: stats.spent || 0,
      avgOrderValue: stats.total > 0 ? stats.spent / stats.total : 0,
      topProducts: products,
    };
  }

  // Search with full-text across historical data
  async searchOrders(userId: string, query: string): Promise<any[]> {
    const result = await this.lake.query(`
      SELECT o.*,
             array_agg(json_object('product', p.name, 'qty', oi.quantity)) as items
      FROM orders_view o
      JOIN order_items_view oi ON o.id = oi.order_id
      JOIN products_view p ON oi.product_id = p.id
      WHERE o.user_id = ?
        AND (o.id LIKE ? OR p.name ILIKE ?)
      GROUP BY o.id
      ORDER BY o.created_at DESC
      LIMIT 50
    `, [userId, `%${query}%`, `%${query}%`]);

    return result.rows;
  }

  // Time-series reporting
  async getRevenueByMonth(year: number): Promise<Array<{ month: number; revenue: number }>> {
    const result = await this.lake.query(`
      SELECT
        EXTRACT(MONTH FROM timestamp_to_date(created_at)) as month,
        SUM(total) as revenue
      FROM orders_view
      WHERE EXTRACT(YEAR FROM timestamp_to_date(created_at)) = ?
        AND status NOT IN ('cancelled', 'refunded')
      GROUP BY month
      ORDER BY month
    `, [year]);

    return result.rows as Array<{ month: number; revenue: number }>;
  }
}

// CDC Event Handler - Syncs write model to read model
class OrderProjectionHandler {
  constructor(private lake: LakeClient) {}

  async handleEvent(event: {
    eventType: string;
    aggregateId: string;
    eventData: string;
  }) {
    const data = JSON.parse(event.eventData);

    switch (event.eventType) {
      case 'OrderCreated':
        await this.projectOrderCreated(data);
        break;
      case 'OrderShipped':
        await this.projectOrderShipped(data);
        break;
    }
  }

  private async projectOrderCreated(data: any) {
    // Insert into read-optimized views in DoLake
    await this.lake.exec(
      `INSERT INTO orders_view (id, user_id, status, total, created_at)
       VALUES (?, ?, 'pending', ?, ?)`,
      [data.orderId, data.userId, data.total, Date.now()]
    );

    for (const item of data.items) {
      await this.lake.exec(
        `INSERT INTO order_items_view (order_id, product_id, quantity, unit_price)
         VALUES (?, ?, ?, ?)`,
        [data.orderId, item.productId, item.quantity, item.price]
      );
    }
  }

  private async projectOrderShipped(data: any) {
    await this.lake.exec(
      `UPDATE orders_view SET status = 'shipped', tracking_number = ?
       WHERE id = ?`,
      [data.trackingNumber, data.orderId]
    );
  }
}

// Usage example
const commandHandler = new OrderCommandHandler(sqlClient);
const queryHandler = new OrderQueryHandler(lakeClient);

// Write (goes to DoSQL)
const orderId = await commandHandler.createOrder({
  userId: 'user-123',
  items: [
    { productId: 'prod-1', quantity: 2, price: 29.99 },
    { productId: 'prod-2', quantity: 1, price: 49.99 },
  ],
  shippingAddress: '123 Main St',
});

// Read (goes to DoLake - optimized for analytics)
const analytics = await queryHandler.getOrderAnalytics('user-123');
```

**When to use**: High-scale applications with different read/write patterns, complex reporting requirements, or when read optimization needs to differ from write optimization.

---

## Framework Examples

### Remix App Example

Full-stack Remix application with DoSQL.

```typescript
// app/db.server.ts
import { createSQLClient } from 'sql.do';

let client: ReturnType<typeof createSQLClient>;

export function getDB() {
  if (!client) {
    client = createSQLClient({
      url: process.env.DOSQL_URL!,
      token: process.env.DOSQL_TOKEN!,
    });
  }
  return client;
}

// app/models/user.server.ts
import { getDB } from '~/db.server';

export interface User {
  id: string;
  email: string;
  name: string;
  created_at: number;
}

export async function getUserById(id: string): Promise<User | null> {
  const db = getDB();
  const result = await db.query<User>(
    'SELECT * FROM users WHERE id = ?',
    [id]
  );
  return result.rows[0] || null;
}

export async function getUserByEmail(email: string): Promise<User | null> {
  const db = getDB();
  const result = await db.query<User>(
    'SELECT * FROM users WHERE email = ?',
    [email]
  );
  return result.rows[0] || null;
}

export async function createUser(data: { email: string; name: string; passwordHash: string }): Promise<User> {
  const db = getDB();
  const id = crypto.randomUUID();

  await db.exec(
    `INSERT INTO users (id, email, name, password_hash, created_at)
     VALUES (?, ?, ?, ?, ?)`,
    [id, data.email, data.name, data.passwordHash, Date.now()]
  );

  return { id, email: data.email, name: data.name, created_at: Date.now() };
}

// app/routes/users.$userId.tsx
import type { LoaderFunctionArgs, ActionFunctionArgs } from '@remix-run/cloudflare';
import { json } from '@remix-run/cloudflare';
import { useLoaderData, Form } from '@remix-run/react';
import { getUserById } from '~/models/user.server';

export async function loader({ params }: LoaderFunctionArgs) {
  const user = await getUserById(params.userId!);

  if (!user) {
    throw new Response('User not found', { status: 404 });
  }

  return json({ user });
}

export async function action({ request, params }: ActionFunctionArgs) {
  const formData = await request.formData();
  const name = formData.get('name') as string;

  const db = getDB();
  await db.exec(
    'UPDATE users SET name = ? WHERE id = ?',
    [name, params.userId]
  );

  return json({ success: true });
}

export default function UserProfile() {
  const { user } = useLoaderData<typeof loader>();

  return (
    <div>
      <h1>{user.name}</h1>
      <p>{user.email}</p>

      <Form method="post">
        <input type="text" name="name" defaultValue={user.name} />
        <button type="submit">Update Name</button>
      </Form>
    </div>
  );
}

// app/routes/api.users.tsx - API route
import type { ActionFunctionArgs } from '@remix-run/cloudflare';
import { json } from '@remix-run/cloudflare';
import { createUser, getUserByEmail } from '~/models/user.server';
import bcrypt from 'bcryptjs';

export async function action({ request }: ActionFunctionArgs) {
  const data = await request.json();

  // Validate
  if (!data.email || !data.password || !data.name) {
    return json({ error: 'Missing required fields' }, { status: 400 });
  }

  // Check existing
  const existing = await getUserByEmail(data.email);
  if (existing) {
    return json({ error: 'Email already registered' }, { status: 409 });
  }

  // Create user
  const passwordHash = await bcrypt.hash(data.password, 10);
  const user = await createUser({
    email: data.email,
    name: data.name,
    passwordHash,
  });

  return json({ user: { id: user.id, email: user.email, name: user.name } }, { status: 201 });
}
```

**When to use**: Full-stack React applications with server-side rendering, forms, and data mutations.

---

### Next.js App Example

Next.js App Router with DoSQL.

```typescript
// lib/db.ts
import { createSQLClient, SQLClient } from 'sql.do';

declare global {
  var _db: SQLClient | undefined;
}

export function getDB(): SQLClient {
  if (!global._db) {
    global._db = createSQLClient({
      url: process.env.DOSQL_URL!,
      token: process.env.DOSQL_TOKEN!,
    });
  }
  return global._db;
}

// app/api/posts/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { getDB } from '@/lib/db';

interface Post {
  id: string;
  title: string;
  content: string;
  author_id: string;
  created_at: number;
}

export async function GET(request: NextRequest) {
  const db = getDB();
  const { searchParams } = new URL(request.url);
  const page = parseInt(searchParams.get('page') || '1');
  const limit = 20;

  const result = await db.query<Post>(
    'SELECT * FROM posts ORDER BY created_at DESC LIMIT ? OFFSET ?',
    [limit, (page - 1) * limit]
  );

  return NextResponse.json({ posts: result.rows });
}

export async function POST(request: NextRequest) {
  const db = getDB();
  const body = await request.json();

  const id = crypto.randomUUID();
  await db.exec(
    `INSERT INTO posts (id, title, content, author_id, created_at)
     VALUES (?, ?, ?, ?, ?)`,
    [id, body.title, body.content, body.authorId, Date.now()]
  );

  return NextResponse.json({ id }, { status: 201 });
}

// app/posts/[id]/page.tsx - Server Component
import { getDB } from '@/lib/db';
import { notFound } from 'next/navigation';

interface Post {
  id: string;
  title: string;
  content: string;
  author_name: string;
  created_at: number;
}

async function getPost(id: string): Promise<Post | null> {
  const db = getDB();
  const result = await db.query<Post>(`
    SELECT p.*, u.name as author_name
    FROM posts p
    JOIN users u ON p.author_id = u.id
    WHERE p.id = ?
  `, [id]);
  return result.rows[0] || null;
}

export default async function PostPage({ params }: { params: { id: string } }) {
  const post = await getPost(params.id);

  if (!post) {
    notFound();
  }

  return (
    <article>
      <h1>{post.title}</h1>
      <p>By {post.author_name}</p>
      <div>{post.content}</div>
    </article>
  );
}

// app/posts/[id]/comments/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { getDB } from '@/lib/db';

export async function GET(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  const db = getDB();

  const result = await db.query(`
    SELECT c.*, u.name as author_name
    FROM comments c
    JOIN users u ON c.author_id = u.id
    WHERE c.post_id = ?
    ORDER BY c.created_at DESC
  `, [params.id]);

  return NextResponse.json({ comments: result.rows });
}

export async function POST(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  const db = getDB();
  const body = await request.json();

  const id = crypto.randomUUID();
  await db.exec(
    `INSERT INTO comments (id, post_id, author_id, content, created_at)
     VALUES (?, ?, ?, ?, ?)`,
    [id, params.id, body.authorId, body.content, Date.now()]
  );

  return NextResponse.json({ id }, { status: 201 });
}
```

**When to use**: React applications with Next.js, API routes, and server components.

---

### Hono API Example

Lightweight API with Hono framework.

```typescript
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import { createSQLClient, SQLClient } from 'sql.do';

type Env = {
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
};

type Variables = {
  db: SQLClient;
};

const app = new Hono<{ Bindings: Env; Variables: Variables }>();

// Middleware
app.use('*', logger());
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

// Types
interface Todo {
  id: string;
  title: string;
  completed: boolean;
  user_id: string;
  created_at: number;
}

// Routes
app.get('/api/todos', async (c) => {
  const db = c.get('db');
  const userId = c.req.header('X-User-ID');

  if (!userId) {
    return c.json({ error: 'User ID required' }, 401);
  }

  const result = await db.query<Todo>(
    'SELECT * FROM todos WHERE user_id = ? ORDER BY created_at DESC',
    [userId]
  );

  return c.json({ todos: result.rows });
});

app.post('/api/todos', async (c) => {
  const db = c.get('db');
  const userId = c.req.header('X-User-ID');
  const body = await c.req.json<{ title: string }>();

  if (!userId) {
    return c.json({ error: 'User ID required' }, 401);
  }

  if (!body.title) {
    return c.json({ error: 'Title required' }, 400);
  }

  const id = crypto.randomUUID();
  await db.exec(
    `INSERT INTO todos (id, title, completed, user_id, created_at)
     VALUES (?, ?, false, ?, ?)`,
    [id, body.title, userId, Date.now()]
  );

  return c.json({ id }, 201);
});

app.patch('/api/todos/:id', async (c) => {
  const db = c.get('db');
  const userId = c.req.header('X-User-ID');
  const todoId = c.req.param('id');
  const body = await c.req.json<{ title?: string; completed?: boolean }>();

  // Build dynamic update
  const updates: string[] = [];
  const params: unknown[] = [];

  if (body.title !== undefined) {
    updates.push('title = ?');
    params.push(body.title);
  }

  if (body.completed !== undefined) {
    updates.push('completed = ?');
    params.push(body.completed);
  }

  if (updates.length === 0) {
    return c.json({ error: 'No updates provided' }, 400);
  }

  params.push(todoId, userId);

  const result = await db.exec(
    `UPDATE todos SET ${updates.join(', ')} WHERE id = ? AND user_id = ?`,
    params
  );

  if (result.changes === 0) {
    return c.json({ error: 'Todo not found' }, 404);
  }

  return c.json({ success: true });
});

app.delete('/api/todos/:id', async (c) => {
  const db = c.get('db');
  const userId = c.req.header('X-User-ID');
  const todoId = c.req.param('id');

  const result = await db.exec(
    'DELETE FROM todos WHERE id = ? AND user_id = ?',
    [todoId, userId]
  );

  if (result.changes === 0) {
    return c.json({ error: 'Todo not found' }, 404);
  }

  return c.json({ success: true });
});

// Batch operations
app.post('/api/todos/batch', async (c) => {
  const db = c.get('db');
  const userId = c.req.header('X-User-ID');
  const body = await c.req.json<{ ids: string[]; completed: boolean }>();

  await db.transaction(async (tx) => {
    for (const id of body.ids) {
      await tx.exec(
        'UPDATE todos SET completed = ? WHERE id = ? AND user_id = ?',
        [body.completed, id, userId]
      );
    }
  });

  return c.json({ success: true });
});

export default app;
```

**When to use**: Lightweight APIs, microservices, or when you want a minimal framework with excellent TypeScript support.

---

### Plain Workers Example

Direct Cloudflare Workers without frameworks.

```typescript
import { createSQLClient, SQLClient, SQLError, RPCErrorCode } from 'sql.do';

interface Env {
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
}

interface User {
  id: string;
  name: string;
  email: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    const client = createSQLClient({
      url: env.DOSQL_URL,
      token: env.DOSQL_TOKEN,
    });

    try {
      // Routing
      if (path === '/users' && method === 'GET') {
        return await handleGetUsers(client);
      }

      if (path === '/users' && method === 'POST') {
        return await handleCreateUser(client, request);
      }

      if (path.match(/^\/users\/[\w-]+$/) && method === 'GET') {
        const id = path.split('/')[2];
        return await handleGetUser(client, id);
      }

      if (path.match(/^\/users\/[\w-]+$/) && method === 'PUT') {
        const id = path.split('/')[2];
        return await handleUpdateUser(client, id, request);
      }

      if (path.match(/^\/users\/[\w-]+$/) && method === 'DELETE') {
        const id = path.split('/')[2];
        return await handleDeleteUser(client, id);
      }

      return new Response('Not Found', { status: 404 });
    } catch (error) {
      return handleError(error);
    } finally {
      await client.close();
    }
  },
};

async function handleGetUsers(client: SQLClient): Promise<Response> {
  const result = await client.query<User>('SELECT * FROM users ORDER BY name');
  return Response.json({ users: result.rows });
}

async function handleGetUser(client: SQLClient, id: string): Promise<Response> {
  const result = await client.query<User>(
    'SELECT * FROM users WHERE id = ?',
    [id]
  );

  if (result.rows.length === 0) {
    return Response.json({ error: 'User not found' }, { status: 404 });
  }

  return Response.json({ user: result.rows[0] });
}

async function handleCreateUser(client: SQLClient, request: Request): Promise<Response> {
  const body = await request.json() as { name: string; email: string };

  if (!body.name || !body.email) {
    return Response.json({ error: 'Name and email required' }, { status: 400 });
  }

  const id = crypto.randomUUID();
  await client.exec(
    'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
    [id, body.name, body.email]
  );

  return Response.json({ id }, { status: 201 });
}

async function handleUpdateUser(client: SQLClient, id: string, request: Request): Promise<Response> {
  const body = await request.json() as Partial<User>;

  const updates: string[] = [];
  const params: unknown[] = [];

  if (body.name) {
    updates.push('name = ?');
    params.push(body.name);
  }

  if (body.email) {
    updates.push('email = ?');
    params.push(body.email);
  }

  if (updates.length === 0) {
    return Response.json({ error: 'No updates provided' }, { status: 400 });
  }

  params.push(id);

  const result = await client.exec(
    `UPDATE users SET ${updates.join(', ')} WHERE id = ?`,
    params
  );

  if (result.changes === 0) {
    return Response.json({ error: 'User not found' }, { status: 404 });
  }

  return Response.json({ success: true });
}

async function handleDeleteUser(client: SQLClient, id: string): Promise<Response> {
  const result = await client.exec('DELETE FROM users WHERE id = ?', [id]);

  if (result.changes === 0) {
    return Response.json({ error: 'User not found' }, { status: 404 });
  }

  return Response.json({ success: true });
}

function handleError(error: unknown): Response {
  if (error instanceof SQLError) {
    console.error(`SQL Error [${error.code}]: ${error.message}`);

    switch (error.code) {
      case RPCErrorCode.CONSTRAINT_VIOLATION:
        return Response.json({ error: 'Constraint violation' }, { status: 409 });
      case RPCErrorCode.TIMEOUT:
        return Response.json({ error: 'Request timeout' }, { status: 504 });
      default:
        return Response.json({ error: 'Database error' }, { status: 500 });
    }
  }

  console.error('Unexpected error:', error);
  return Response.json({ error: 'Internal server error' }, { status: 500 });
}
```

**When to use**: When you want full control, minimal dependencies, or are building simple APIs.

---

## Testing Examples

### Unit Testing with Mocks

Testing business logic in isolation.

```typescript
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createMockSQLClient, MockSQLClient } from 'sql.do/testing';

// The service we're testing
class UserService {
  constructor(private db: any) {}

  async getUser(id: string) {
    const result = await this.db.query('SELECT * FROM users WHERE id = ?', [id]);
    return result.rows[0] || null;
  }

  async createUser(data: { name: string; email: string }) {
    const id = crypto.randomUUID();
    await this.db.exec(
      'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
      [id, data.name, data.email]
    );
    return id;
  }
}

describe('UserService', () => {
  let mockDb: MockSQLClient;
  let service: UserService;

  beforeEach(() => {
    mockDb = createMockSQLClient();
    service = new UserService(mockDb);
  });

  describe('getUser', () => {
    it('returns user when found', async () => {
      const mockUser = { id: 'user-1', name: 'Alice', email: 'alice@example.com' };
      mockDb.query.mockResolvedValue({ rows: [mockUser] });

      const user = await service.getUser('user-1');

      expect(user).toEqual(mockUser);
      expect(mockDb.query).toHaveBeenCalledWith(
        'SELECT * FROM users WHERE id = ?',
        ['user-1']
      );
    });

    it('returns null when not found', async () => {
      mockDb.query.mockResolvedValue({ rows: [] });

      const user = await service.getUser('nonexistent');

      expect(user).toBeNull();
    });
  });

  describe('createUser', () => {
    it('creates user and returns ID', async () => {
      mockDb.exec.mockResolvedValue({ changes: 1 });

      const id = await service.createUser({
        name: 'Bob',
        email: 'bob@example.com',
      });

      expect(id).toBeDefined();
      expect(mockDb.exec).toHaveBeenCalledWith(
        'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
        [expect.any(String), 'Bob', 'bob@example.com']
      );
    });
  });
});

// Testing error handling
describe('Error Handling', () => {
  it('handles constraint violations', async () => {
    const mockDb = createMockSQLClient();
    const service = new UserService(mockDb);

    mockDb.exec.mockRejectedValue(
      new SQLError('UNIQUE constraint failed', RPCErrorCode.CONSTRAINT_VIOLATION)
    );

    await expect(
      service.createUser({ name: 'Test', email: 'existing@example.com' })
    ).rejects.toThrow('UNIQUE constraint failed');
  });
});
```

**When to use**: Testing business logic in isolation, fast feedback loops, CI pipelines.

---

### Integration Testing

Testing with real database connections.

```typescript
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { createSQLClient, SQLClient } from 'sql.do';

describe('User Integration Tests', () => {
  let client: SQLClient;

  beforeAll(async () => {
    client = createSQLClient({
      url: process.env.TEST_DOSQL_URL!,
      token: process.env.TEST_DOSQL_TOKEN!,
    });

    // Setup test schema
    await client.exec(`
      CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        created_at INTEGER DEFAULT (unixepoch() * 1000)
      )
    `);
  });

  afterAll(async () => {
    // Cleanup
    await client.exec('DROP TABLE IF EXISTS users');
    await client.close();
  });

  beforeEach(async () => {
    // Clear data between tests
    await client.exec('DELETE FROM users');
  });

  it('creates and retrieves user', async () => {
    const userId = crypto.randomUUID();

    await client.exec(
      'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
      [userId, 'Alice', 'alice@test.com']
    );

    const result = await client.query<{ id: string; name: string; email: string }>(
      'SELECT * FROM users WHERE id = ?',
      [userId]
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].name).toBe('Alice');
    expect(result.rows[0].email).toBe('alice@test.com');
  });

  it('enforces unique email constraint', async () => {
    await client.exec(
      'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
      ['user-1', 'Alice', 'alice@test.com']
    );

    await expect(
      client.exec(
        'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
        ['user-2', 'Bob', 'alice@test.com']
      )
    ).rejects.toThrow();
  });

  it('handles transactions correctly', async () => {
    await expect(
      client.transaction(async (tx) => {
        await tx.exec(
          'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
          ['user-1', 'Alice', 'alice@test.com']
        );

        // This should fail and rollback
        throw new Error('Simulated failure');
      })
    ).rejects.toThrow('Simulated failure');

    // Verify rollback
    const result = await client.query('SELECT COUNT(*) as count FROM users');
    expect(result.rows[0].count).toBe(0);
  });

  it('handles concurrent updates with serializable isolation', async () => {
    await client.exec(
      'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
      ['user-1', 'Alice', 'alice@test.com']
    );

    // Simulate concurrent updates
    const results = await Promise.allSettled([
      client.transaction(async (tx) => {
        const user = await tx.query('SELECT name FROM users WHERE id = ?', ['user-1']);
        await tx.exec('UPDATE users SET name = ? WHERE id = ?', [user.rows[0].name + '-A', 'user-1']);
      }, { isolation: 'serializable' }),

      client.transaction(async (tx) => {
        const user = await tx.query('SELECT name FROM users WHERE id = ?', ['user-1']);
        await tx.exec('UPDATE users SET name = ? WHERE id = ?', [user.rows[0].name + '-B', 'user-1']);
      }, { isolation: 'serializable' }),
    ]);

    // One should succeed, one might fail due to serialization conflict
    const successCount = results.filter(r => r.status === 'fulfilled').length;
    expect(successCount).toBeGreaterThanOrEqual(1);
  });
});
```

**When to use**: Verifying database interactions, testing schema constraints, validating transaction behavior.

---

### E2E Testing Patterns

Full API testing with real requests.

```typescript
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { unstable_dev } from 'wrangler';
import type { UnstableDevWorker } from 'wrangler';

describe('API E2E Tests', () => {
  let worker: UnstableDevWorker;

  beforeAll(async () => {
    worker = await unstable_dev('src/index.ts', {
      experimental: { disableExperimentalWarning: true },
      vars: {
        DOSQL_URL: process.env.TEST_DOSQL_URL,
        DOSQL_TOKEN: process.env.TEST_DOSQL_TOKEN,
      },
    });
  });

  afterAll(async () => {
    await worker.stop();
  });

  describe('POST /api/users', () => {
    it('creates a new user', async () => {
      const response = await worker.fetch('/api/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'Test User',
          email: `test-${Date.now()}@example.com`,
        }),
      });

      expect(response.status).toBe(201);

      const data = await response.json();
      expect(data.id).toBeDefined();
    });

    it('returns 400 for missing fields', async () => {
      const response = await worker.fetch('/api/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Test' }), // Missing email
      });

      expect(response.status).toBe(400);
    });

    it('returns 409 for duplicate email', async () => {
      const email = `duplicate-${Date.now()}@example.com`;

      // Create first user
      await worker.fetch('/api/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'User 1', email }),
      });

      // Try to create second user with same email
      const response = await worker.fetch('/api/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'User 2', email }),
      });

      expect(response.status).toBe(409);
    });
  });

  describe('GET /api/users/:id', () => {
    it('returns user by ID', async () => {
      // Create user
      const createResponse = await worker.fetch('/api/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'Get Test',
          email: `get-test-${Date.now()}@example.com`,
        }),
      });

      const { id } = await createResponse.json();

      // Get user
      const response = await worker.fetch(`/api/users/${id}`);

      expect(response.status).toBe(200);

      const data = await response.json();
      expect(data.user.id).toBe(id);
      expect(data.user.name).toBe('Get Test');
    });

    it('returns 404 for non-existent user', async () => {
      const response = await worker.fetch('/api/users/nonexistent-id');
      expect(response.status).toBe(404);
    });
  });

  describe('Full user lifecycle', () => {
    it('creates, updates, and deletes user', async () => {
      // Create
      const createResponse = await worker.fetch('/api/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'Lifecycle Test',
          email: `lifecycle-${Date.now()}@example.com`,
        }),
      });

      expect(createResponse.status).toBe(201);
      const { id } = await createResponse.json();

      // Update
      const updateResponse = await worker.fetch(`/api/users/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Updated Name' }),
      });

      expect(updateResponse.status).toBe(200);

      // Verify update
      const getResponse = await worker.fetch(`/api/users/${id}`);
      const { user } = await getResponse.json();
      expect(user.name).toBe('Updated Name');

      // Delete
      const deleteResponse = await worker.fetch(`/api/users/${id}`, {
        method: 'DELETE',
      });

      expect(deleteResponse.status).toBe(200);

      // Verify deletion
      const finalResponse = await worker.fetch(`/api/users/${id}`);
      expect(finalResponse.status).toBe(404);
    });
  });
});
```

**When to use**: Full system validation, CI/CD pipelines, regression testing.

---

## Performance Examples

### Query Optimization

Techniques for faster queries.

```typescript
import { createSQLClient } from 'sql.do';

const client = createSQLClient({ url: 'https://sql.example.com' });

// ANTI-PATTERN: N+1 queries
async function getUsersWithOrdersBad(userIds: string[]) {
  const users = [];
  for (const id of userIds) {
    const user = await client.query('SELECT * FROM users WHERE id = ?', [id]);
    const orders = await client.query('SELECT * FROM orders WHERE user_id = ?', [id]);
    users.push({ ...user.rows[0], orders: orders.rows });
  }
  return users; // N+1 queries!
}

// GOOD: Single query with JOIN
async function getUsersWithOrdersGood(userIds: string[]) {
  const placeholders = userIds.map(() => '?').join(',');

  const result = await client.query(`
    SELECT
      u.id, u.name, u.email,
      json_group_array(
        json_object('id', o.id, 'total', o.total, 'status', o.status)
      ) as orders
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.id IN (${placeholders})
    GROUP BY u.id
  `, userIds);

  return result.rows.map(row => ({
    ...row,
    orders: JSON.parse(row.orders).filter(o => o.id !== null),
  }));
}

// Use EXPLAIN to analyze queries
async function analyzeQuery(sql: string, params: unknown[] = []) {
  const plan = await client.query(`EXPLAIN QUERY PLAN ${sql}`, params);

  for (const step of plan.rows) {
    console.log(step);

    // Check for full table scans
    if (step.detail?.includes('SCAN')) {
      console.warn('WARNING: Full table scan detected. Consider adding an index.');
    }
  }
}

// Index coverage - select only indexed columns
async function getOrderTotals(userId: string) {
  // If we have: CREATE INDEX idx_orders_user_total ON orders(user_id, total)
  // This query uses covering index (no table lookup needed)
  const result = await client.query(
    'SELECT user_id, SUM(total) as total FROM orders WHERE user_id = ? GROUP BY user_id',
    [userId]
  );
  return result.rows[0]?.total || 0;
}

// Pagination with cursor (better than OFFSET for large datasets)
interface PaginatedResult<T> {
  items: T[];
  nextCursor: string | null;
}

async function getUsersPaginated(
  cursor?: string,
  limit = 20
): Promise<PaginatedResult<{ id: string; name: string; created_at: number }>> {
  let sql = 'SELECT id, name, created_at FROM users';
  const params: unknown[] = [];

  if (cursor) {
    // Cursor is base64-encoded timestamp
    const timestamp = parseInt(Buffer.from(cursor, 'base64').toString());
    sql += ' WHERE created_at < ?';
    params.push(timestamp);
  }

  sql += ' ORDER BY created_at DESC LIMIT ?';
  params.push(limit + 1); // Fetch one extra to check if there's more

  const result = await client.query<{ id: string; name: string; created_at: number }>(sql, params);

  const hasMore = result.rows.length > limit;
  const items = hasMore ? result.rows.slice(0, -1) : result.rows;

  const nextCursor = hasMore
    ? Buffer.from(items[items.length - 1].created_at.toString()).toString('base64')
    : null;

  return { items, nextCursor };
}
```

**When to use**: When queries are slow, when dealing with large datasets, or when optimizing for scale.

---

### Connection Pooling

Efficient connection management.

```typescript
import { createSQLClient, SQLClient } from 'sql.do';

// Connection pool for Workers (simple approach)
class ConnectionPool {
  private connections: Map<string, SQLClient> = new Map();
  private lastUsed: Map<string, number> = new Map();
  private readonly maxConnections = 10;
  private readonly idleTimeoutMs = 60000;

  async getConnection(tenantId: string): Promise<SQLClient> {
    const key = tenantId;

    // Return existing connection
    if (this.connections.has(key)) {
      this.lastUsed.set(key, Date.now());
      return this.connections.get(key)!;
    }

    // Clean up idle connections
    await this.cleanupIdle();

    // Check pool limit
    if (this.connections.size >= this.maxConnections) {
      // Evict least recently used
      await this.evictLRU();
    }

    // Create new connection
    const client = createSQLClient({
      url: 'https://sql.example.com',
      token: process.env.DOSQL_TOKEN!,
      headers: { 'X-Tenant-ID': tenantId },
    });

    this.connections.set(key, client);
    this.lastUsed.set(key, Date.now());

    return client;
  }

  private async cleanupIdle(): Promise<void> {
    const now = Date.now();

    for (const [key, lastUsed] of this.lastUsed.entries()) {
      if (now - lastUsed > this.idleTimeoutMs) {
        const client = this.connections.get(key);
        if (client) {
          await client.close();
        }
        this.connections.delete(key);
        this.lastUsed.delete(key);
      }
    }
  }

  private async evictLRU(): Promise<void> {
    let lruKey: string | null = null;
    let lruTime = Infinity;

    for (const [key, lastUsed] of this.lastUsed.entries()) {
      if (lastUsed < lruTime) {
        lruTime = lastUsed;
        lruKey = key;
      }
    }

    if (lruKey) {
      const client = this.connections.get(lruKey);
      if (client) {
        await client.close();
      }
      this.connections.delete(lruKey);
      this.lastUsed.delete(lruKey);
    }
  }

  async closeAll(): Promise<void> {
    for (const client of this.connections.values()) {
      await client.close();
    }
    this.connections.clear();
    this.lastUsed.clear();
  }
}

// Global pool (for Workers, this persists across requests in same isolate)
const pool = new ConnectionPool();

// Usage in request handler
export default {
  async fetch(request: Request, env: Env) {
    const tenantId = request.headers.get('X-Tenant-ID') || 'default';

    const client = await pool.getConnection(tenantId);

    const result = await client.query('SELECT * FROM users LIMIT 10');

    return Response.json({ users: result.rows });
  },
};
```

**When to use**: High-traffic applications, multi-tenant systems, or when connection setup overhead is significant.

---

### Caching Strategies

Reduce database load with intelligent caching.

```typescript
import { createSQLClient, SQLClient } from 'sql.do';

interface CacheEntry<T> {
  data: T;
  expiresAt: number;
}

// In-memory cache with TTL
class QueryCache {
  private cache: Map<string, CacheEntry<unknown>> = new Map();

  get<T>(key: string): T | null {
    const entry = this.cache.get(key);

    if (!entry) return null;

    if (Date.now() > entry.expiresAt) {
      this.cache.delete(key);
      return null;
    }

    return entry.data as T;
  }

  set<T>(key: string, data: T, ttlMs: number): void {
    this.cache.set(key, {
      data,
      expiresAt: Date.now() + ttlMs,
    });
  }

  invalidate(pattern: string): void {
    for (const key of this.cache.keys()) {
      if (key.startsWith(pattern)) {
        this.cache.delete(key);
      }
    }
  }
}

// Cached repository
class CachedUserRepository {
  private cache = new QueryCache();

  constructor(private client: SQLClient) {}

  async getUser(id: string): Promise<User | null> {
    const cacheKey = `user:${id}`;

    // Check cache
    const cached = this.cache.get<User>(cacheKey);
    if (cached) {
      return cached;
    }

    // Query database
    const result = await this.client.query<User>(
      'SELECT * FROM users WHERE id = ?',
      [id]
    );

    const user = result.rows[0] || null;

    // Cache for 5 minutes
    if (user) {
      this.cache.set(cacheKey, user, 5 * 60 * 1000);
    }

    return user;
  }

  async updateUser(id: string, data: Partial<User>): Promise<void> {
    await this.client.exec(
      'UPDATE users SET name = ?, email = ? WHERE id = ?',
      [data.name, data.email, id]
    );

    // Invalidate cache
    this.cache.invalidate(`user:${id}`);
  }

  // Stale-while-revalidate pattern
  async getUserSWR(id: string): Promise<{ user: User | null; isStale: boolean }> {
    const cacheKey = `user:${id}`;
    const cached = this.cache.get<User>(cacheKey);

    if (cached) {
      // Return cached data, refresh in background
      this.refreshUserCache(id).catch(console.error);
      return { user: cached, isStale: true };
    }

    const user = await this.refreshUserCache(id);
    return { user, isStale: false };
  }

  private async refreshUserCache(id: string): Promise<User | null> {
    const result = await this.client.query<User>(
      'SELECT * FROM users WHERE id = ?',
      [id]
    );

    const user = result.rows[0] || null;

    if (user) {
      this.cache.set(`user:${id}`, user, 5 * 60 * 1000);
    }

    return user;
  }
}

// Using Cloudflare KV for distributed caching
class KVCachedRepository {
  constructor(
    private client: SQLClient,
    private kv: KVNamespace
  ) {}

  async getUser(id: string): Promise<User | null> {
    const cacheKey = `user:${id}`;

    // Check KV cache
    const cached = await this.kv.get(cacheKey, 'json');
    if (cached) {
      return cached as User;
    }

    // Query database
    const result = await this.client.query<User>(
      'SELECT * FROM users WHERE id = ?',
      [id]
    );

    const user = result.rows[0] || null;

    // Cache in KV with TTL
    if (user) {
      await this.kv.put(cacheKey, JSON.stringify(user), {
        expirationTtl: 300, // 5 minutes
      });
    }

    return user;
  }

  async invalidateUser(id: string): Promise<void> {
    await this.kv.delete(`user:${id}`);
  }
}

// Cache warming on startup
async function warmCache(client: SQLClient, cache: QueryCache): Promise<void> {
  // Pre-load frequently accessed data
  const hotUsers = await client.query<User>(
    `SELECT * FROM users ORDER BY last_login DESC LIMIT 100`
  );

  for (const user of hotUsers.rows) {
    cache.set(`user:${user.id}`, user, 10 * 60 * 1000);
  }

  console.log(`Warmed cache with ${hotUsers.rows.length} users`);
}
```

**When to use**: Frequently accessed data, expensive queries, read-heavy workloads, or when reducing database load is critical.

---

## Summary

This guide covered:

1. **Quick Examples** - Basic CRUD, transactions, and batch operations for everyday use
2. **Real-World Patterns** - Multi-tenant SaaS, e-commerce, social apps, and analytics dashboards
3. **Advanced Examples** - CDC streaming, real-time collaboration, event sourcing, and CQRS
4. **Framework Examples** - Integration with Remix, Next.js, Hono, and plain Workers
5. **Testing Examples** - Unit tests with mocks, integration tests, and E2E testing
6. **Performance Examples** - Query optimization, connection pooling, and caching strategies

For more information, see:
- [Performance Tuning Guide](./PERFORMANCE_TUNING.md)
- [Migration from D1](./MIGRATION_FROM_D1.md)
- [Error Codes Reference](./ERROR_CODES.md)
- [Security Guide](./SECURITY.md)
