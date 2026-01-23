# Getting Started with DoSQL

This guide covers installation, basic usage, CRUD operations, migrations, and deploying to Cloudflare Workers.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quickstart](#quickstart)
- [Hello World](#hello-world)
- [Installation](#installation)
- [Basic Usage with DB()](#basic-usage-with-db)
- [Creating Tables](#creating-tables)
- [CRUD Operations](#crud-operations)
- [Working with Transactions](#working-with-transactions)
- [CDC Streaming](#cdc-streaming)
- [Migrations](#migrations)
- [Deploying to Cloudflare Workers](#deploying-to-cloudflare-workers)
- [Error Handling for Common Setup Failures](#error-handling-for-common-setup-failures)
- [Troubleshooting](#troubleshooting)
- [Next Steps](#next-steps)

---

## Prerequisites

Before you begin, ensure you have the following requirements met:

### Cloudflare Account Requirements

1. **Cloudflare Account** - Create a free account at [dash.cloudflare.com](https://dash.cloudflare.com)
2. **Workers Paid Plan** - Durable Objects require at least a Workers Paid plan ($5/month)
   - Go to Workers & Pages > Plans to upgrade
3. **Durable Objects Enabled** - Should be automatically available on paid plans
   - If not visible, contact Cloudflare support

### R2 Bucket Setup (Optional for DoSQL, Required for DoLake)

If using cold storage with R2:

```bash
# Login to Cloudflare
wrangler login

# Create R2 bucket
wrangler r2 bucket create my-data-bucket
```

### Development Tools

| Tool | Version | Purpose |
|------|---------|---------|
| Node.js | 18+ | Runtime environment |
| npm/pnpm | Latest | Package manager |
| Wrangler | 3.0+ | Cloudflare CLI |
| TypeScript | 5.3+ | Type checking (optional) |

```bash
# Install Wrangler CLI
npm install -g wrangler

# Verify installation
wrangler --version

# Login to Cloudflare
wrangler login
```

### Verify Your Setup

```bash
# Check Wrangler authentication
wrangler whoami

# Expected output: Your Cloudflare account email and account ID
```

---

## Quickstart

Get up and running with DoSQL in under 5 minutes.

### Step 1: Create a New Project

```bash
# Create project directory
mkdir my-dosql-app && cd my-dosql-app

# Initialize npm project
npm init -y

# Install dependencies
npm install @dotdo/dosql
npm install wrangler @cloudflare/workers-types --save-dev
```

### Step 2: Configure TypeScript

Create `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": true,
    "types": ["@cloudflare/workers-types"],
    "lib": ["ES2022"]
  },
  "include": ["src/**/*"]
}
```

### Step 3: Create Your First Migration

```bash
# Create migrations directory
mkdir -p .do/migrations
```

Create `.do/migrations/001_init.sql`:

```sql
CREATE TABLE tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  completed BOOLEAN DEFAULT false,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

### Step 4: Create the Durable Object

Create `src/index.ts`:

```typescript
import { DB } from '@dotdo/dosql';

export interface Env {
  TASKS_DB: DurableObjectNamespace;
}

export class TasksDatabase implements DurableObject {
  private db: Awaited<ReturnType<typeof DB>> | null = null;
  private state: DurableObjectState;

  constructor(state: DurableObjectState) {
    this.state = state;
  }

  private async getDB() {
    if (!this.db) {
      this.db = await DB('tasks', {
        migrations: { folder: '.do/migrations' },
        storage: { hot: this.state.storage },
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();
    const url = new URL(request.url);

    // GET /tasks - List all tasks
    if (url.pathname === '/tasks' && request.method === 'GET') {
      const tasks = await db.query('SELECT * FROM tasks ORDER BY created_at DESC');
      return Response.json(tasks);
    }

    // POST /tasks - Create a task
    if (url.pathname === '/tasks' && request.method === 'POST') {
      const { title } = await request.json() as { title: string };
      const result = await db.run(
        'INSERT INTO tasks (title) VALUES (?)',
        [title]
      );
      return Response.json({ id: result.lastInsertRowId, title }, { status: 201 });
    }

    return new Response('Not Found', { status: 404 });
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const id = env.TASKS_DB.idFromName('default');
    const stub = env.TASKS_DB.get(id);
    return stub.fetch(request);
  },
};
```

### Step 5: Configure Wrangler

Create `wrangler.jsonc`:

```jsonc
{
  "name": "my-dosql-app",
  "main": "src/index.ts",
  "compatibility_date": "2024-01-01",
  "durable_objects": {
    "bindings": [
      { "name": "TASKS_DB", "class_name": "TasksDatabase" }
    ]
  },
  "migrations": [
    { "tag": "v1", "new_classes": ["TasksDatabase"] }
  ]
}
```

### Step 6: Run Locally

```bash
npx wrangler dev
```

### Step 7: Test Your API

```bash
# Create a task
curl -X POST http://localhost:8787/tasks \
  -H "Content-Type: application/json" \
  -d '{"title": "Learn DoSQL"}'

# List tasks
curl http://localhost:8787/tasks
```

You should see:

```json
[{"id": 1, "title": "Learn DoSQL", "completed": 0, "created_at": "2024-01-15T10:30:00.000Z"}]
```

---

## Hello World

The simplest possible DoSQL application:

```typescript
import { DB } from '@dotdo/dosql';

// Create an in-memory database
const db = await DB('hello-world');

// Execute a simple query
const result = await db.query('SELECT "Hello, DoSQL!" as message');
console.log(result[0].message); // "Hello, DoSQL!"

// Create a table and insert data
await db.run('CREATE TABLE greetings (id INTEGER PRIMARY KEY, message TEXT)');
await db.run('INSERT INTO greetings (message) VALUES (?)', ['Hello from DoSQL!']);

// Query the data
const greetings = await db.query('SELECT * FROM greetings');
console.log(greetings);
// [{ id: 1, message: "Hello from DoSQL!" }]
```

### Hello World with Migrations

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('hello-world', {
  migrations: [
    {
      id: '001_init',
      sql: `
        CREATE TABLE messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          content TEXT NOT NULL,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO messages (content) VALUES ('Welcome to DoSQL!');
      `,
    },
  ],
});

const messages = await db.query('SELECT * FROM messages');
console.log(messages);
// [{ id: 1, content: "Welcome to DoSQL!", created_at: "2024-01-15T10:30:00.000Z" }]
```

---

## Installation

```bash
npm install @dotdo/dosql
```

### Peer Dependencies

DoSQL has optional peer dependencies for Cloudflare Workers:

```bash
npm install @cloudflare/workers-types --save-dev
```

### TypeScript Configuration

Add to your `tsconfig.json`:

```json
{
  "compilerOptions": {
    "types": ["@cloudflare/workers-types"],
    "strict": true,
    "moduleResolution": "bundler"
  }
}
```

---

## Basic Usage with DB()

The `DB()` function is the primary interface for DoSQL. It creates or connects to a database with automatic migration support.

### Simple Database

```typescript
import { DB } from '@dotdo/dosql';

// Create a database with a name
const db = await DB('my-database');

// Execute queries
const result = await db.query('SELECT 1 + 1 as sum');
console.log(result); // [{ sum: 2 }]
```

### Database with Migrations

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('tenants', {
  migrations: { folder: '.do/migrations' },
  autoMigrate: true, // default: true
});
```

### Database with Inline Migrations

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('tenants', {
  migrations: [
    {
      id: '001_init',
      sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
    },
    {
      id: '002_add_email',
      sql: 'ALTER TABLE users ADD COLUMN email TEXT',
    },
  ],
});
```

### Database Options

```typescript
interface DBOptions {
  // Migration source: folder path, inline array, or async loader
  migrations?: MigrationSource;

  // Auto-apply migrations on first access (default: true)
  autoMigrate?: boolean;

  // Migration table name (default: '__dosql_migrations')
  migrationsTable?: string;

  // Storage tier configuration
  storage?: {
    hot: DurableObjectStorage;
    cold?: R2Bucket;
  };

  // Enable WAL for durability
  wal?: boolean;

  // Logging
  logger?: Logger;
}
```

---

## Creating Tables

### Basic Table Creation

```sql
-- .do/migrations/001_create_users.sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  active BOOLEAN DEFAULT true,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

### Table with Indexes

```sql
-- .do/migrations/002_create_posts.sql
CREATE TABLE posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  body TEXT,
  published_at TEXT,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_published ON posts(published_at);
```

### Supported Column Types

| SQL Type | TypeScript Type | Notes |
|----------|-----------------|-------|
| `INTEGER` | `number` | 64-bit signed integer |
| `REAL` | `number` | 64-bit floating point |
| `TEXT` | `string` | UTF-8 string |
| `BLOB` | `Uint8Array` | Binary data |
| `BOOLEAN` | `boolean` | Stored as 0/1 |
| `NULL` | `null` | Explicit null |

### Constraints

```sql
CREATE TABLE products (
  id INTEGER PRIMARY KEY,
  sku TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  price REAL CHECK (price >= 0),
  quantity INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

---

## CRUD Operations

### Complete CRUD Example

Here is a full example demonstrating all CRUD operations with a `products` table:

```typescript
import { DB } from '@dotdo/dosql';

// Initialize database with schema
const db = await DB('shop', {
  migrations: [
    {
      id: '001_products',
      sql: `
        CREATE TABLE products (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          description TEXT,
          price REAL NOT NULL CHECK (price >= 0),
          stock INTEGER DEFAULT 0,
          category TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX idx_products_category ON products(category);
        CREATE INDEX idx_products_price ON products(price);
      `,
    },
  ],
});

// ============================================
// CREATE - Insert new products
// ============================================

// Single insert
const result = await db.run(
  'INSERT INTO products (name, description, price, stock, category) VALUES (?, ?, ?, ?, ?)',
  ['Laptop', 'High-performance laptop', 999.99, 50, 'Electronics']
);
console.log('Created product ID:', result.lastInsertRowId); // 1

// Insert with named parameters
await db.run(
  'INSERT INTO products (name, price, stock, category) VALUES (:name, :price, :stock, :category)',
  { name: 'Mouse', price: 29.99, stock: 200, category: 'Electronics' }
);

// Insert multiple products in a transaction
await db.transaction(async (tx) => {
  const products = [
    { name: 'Keyboard', price: 79.99, stock: 100, category: 'Electronics' },
    { name: 'Monitor', price: 399.99, stock: 30, category: 'Electronics' },
    { name: 'Desk Chair', price: 249.99, stock: 25, category: 'Furniture' },
  ];

  for (const product of products) {
    await tx.run(
      'INSERT INTO products (name, price, stock, category) VALUES (?, ?, ?, ?)',
      [product.name, product.price, product.stock, product.category]
    );
  }
});

// ============================================
// READ - Query products
// ============================================

// Get all products
const allProducts = await db.query('SELECT * FROM products');
console.log('All products:', allProducts);

// Get single product by ID
const laptop = await db.queryOne('SELECT * FROM products WHERE id = ?', [1]);
console.log('Laptop:', laptop);

// Get products by category
const electronics = await db.query(
  'SELECT * FROM products WHERE category = ? ORDER BY price DESC',
  ['Electronics']
);
console.log('Electronics:', electronics);

// Get products within price range
const affordable = await db.query(
  'SELECT name, price FROM products WHERE price BETWEEN ? AND ? ORDER BY price',
  [20, 100]
);
console.log('Affordable products:', affordable);

// Search products by name (case-insensitive)
const searchTerm = '%lap%';
const searchResults = await db.query(
  'SELECT * FROM products WHERE LOWER(name) LIKE LOWER(?)',
  [searchTerm]
);
console.log('Search results:', searchResults);

// Aggregate queries
const stats = await db.queryOne(`
  SELECT
    COUNT(*) as total_products,
    SUM(stock) as total_stock,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
  FROM products
`);
console.log('Product stats:', stats);

// Group by category
const byCategory = await db.query(`
  SELECT
    category,
    COUNT(*) as count,
    AVG(price) as avg_price,
    SUM(stock) as total_stock
  FROM products
  GROUP BY category
  ORDER BY count DESC
`);
console.log('Products by category:', byCategory);

// Pagination
const page = 1;
const pageSize = 10;
const paginatedProducts = await db.query(
  'SELECT * FROM products ORDER BY created_at DESC LIMIT ? OFFSET ?',
  [pageSize, (page - 1) * pageSize]
);

// ============================================
// UPDATE - Modify products
// ============================================

// Update single field
await db.run('UPDATE products SET price = ? WHERE id = ?', [949.99, 1]);

// Update multiple fields
await db.run(
  'UPDATE products SET price = ?, stock = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
  [899.99, 45, 1]
);

// Update with conditions
const updateResult = await db.run(
  'UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?',
  [5, 1, 5]
);
console.log('Rows affected:', updateResult.rowsAffected);

// Bulk update by category
await db.run(
  'UPDATE products SET price = price * 0.9 WHERE category = ?', // 10% discount
  ['Electronics']
);

// Conditional update
await db.run(`
  UPDATE products
  SET category = 'Low Stock'
  WHERE stock < 10 AND category != 'Low Stock'
`);

// ============================================
// DELETE - Remove products
// ============================================

// Delete by ID
await db.run('DELETE FROM products WHERE id = ?', [5]);

// Delete with conditions
const deleteResult = await db.run(
  'DELETE FROM products WHERE stock = 0 AND created_at < ?',
  ['2024-01-01']
);
console.log('Deleted products:', deleteResult.rowsAffected);

// Delete all products in a category
await db.run('DELETE FROM products WHERE category = ?', ['Discontinued']);

// Soft delete pattern (recommended)
await db.run(`
  ALTER TABLE products ADD COLUMN deleted_at TEXT DEFAULT NULL
`);
// Then "delete" by setting deleted_at
await db.run(
  'UPDATE products SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?',
  [3]
);
// Query only active products
const activeProducts = await db.query(
  'SELECT * FROM products WHERE deleted_at IS NULL'
);
```

### Create (INSERT)

```typescript
// Single insert
await db.run(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Alice', 'alice@example.com']
);

// Get the last inserted ID
const result = await db.run(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Bob', 'bob@example.com']
);
console.log(result.lastInsertRowId); // 2

// Insert with named parameters
await db.run(
  'INSERT INTO users (name, email) VALUES (:name, :email)',
  { name: 'Carol', email: 'carol@example.com' }
);

// Bulk insert
await db.transaction(async (tx) => {
  for (const user of users) {
    await tx.run('INSERT INTO users (name, email) VALUES (?, ?)', [user.name, user.email]);
  }
});
```

### Read (SELECT)

```typescript
// Get all rows
const users = await db.query('SELECT * FROM users');

// Get with conditions
const activeUsers = await db.query(
  'SELECT * FROM users WHERE active = ?',
  [true]
);

// Get single row
const user = await db.queryOne('SELECT * FROM users WHERE id = ?', [1]);

// Get with LIMIT and OFFSET
const page = await db.query(
  'SELECT * FROM users ORDER BY id LIMIT ? OFFSET ?',
  [10, 20]
);

// Join tables
const postsWithUsers = await db.query(`
  SELECT p.*, u.name as author_name
  FROM posts p
  JOIN users u ON u.id = p.user_id
  WHERE p.published_at IS NOT NULL
`);

// Aggregate functions
const stats = await db.queryOne(`
  SELECT
    COUNT(*) as total,
    COUNT(CASE WHEN active THEN 1 END) as active,
    MAX(created_at) as newest
  FROM users
`);
```

### Update (UPDATE)

```typescript
// Update single row
await db.run('UPDATE users SET name = ? WHERE id = ?', ['Alicia', 1]);

// Update with multiple conditions
await db.run(
  'UPDATE users SET active = ? WHERE created_at < ? AND active = ?',
  [false, '2024-01-01', true]
);

// Update with expression
await db.run('UPDATE products SET quantity = quantity - ? WHERE id = ?', [1, 42]);

// Get affected row count
const result = await db.run('UPDATE users SET active = false WHERE active = true');
console.log(result.rowsAffected); // number of updated rows
```

### Delete (DELETE)

```typescript
// Delete single row
await db.run('DELETE FROM users WHERE id = ?', [1]);

// Delete with conditions
await db.run('DELETE FROM users WHERE active = ? AND created_at < ?', [false, '2023-01-01']);

// Delete all (use with caution)
await db.run('DELETE FROM users');

// Truncate equivalent (faster for large tables)
await db.run('DELETE FROM users');
await db.run('VACUUM'); // Reclaim space
```

### Prepared Statements

```typescript
// Create a prepared statement
const stmt = db.prepare('SELECT * FROM users WHERE active = ?');

// Execute multiple times with different parameters
const activeUsers = await stmt.all([true]);
const inactiveUsers = await stmt.all([false]);

// Get single row
const firstActive = await stmt.get([true]);

// Run for modifications
const insertStmt = db.prepare('INSERT INTO users (name) VALUES (?)');
await insertStmt.run(['Alice']);
await insertStmt.run(['Bob']);
```

---

## Working with Transactions

Transactions ensure data consistency by grouping multiple operations into an atomic unit.

### Basic Transaction

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('bank', {
  migrations: [
    {
      id: '001_accounts',
      sql: `
        CREATE TABLE accounts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          balance REAL NOT NULL DEFAULT 0 CHECK (balance >= 0)
        );

        CREATE TABLE transfers (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          from_account_id INTEGER NOT NULL,
          to_account_id INTEGER NOT NULL,
          amount REAL NOT NULL CHECK (amount > 0),
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (from_account_id) REFERENCES accounts(id),
          FOREIGN KEY (to_account_id) REFERENCES accounts(id)
        );
      `,
    },
  ],
});

// Create accounts
await db.run('INSERT INTO accounts (name, balance) VALUES (?, ?)', ['Alice', 1000]);
await db.run('INSERT INTO accounts (name, balance) VALUES (?, ?)', ['Bob', 500]);

// Transfer money atomically
async function transfer(fromId: number, toId: number, amount: number) {
  await db.transaction(async (tx) => {
    // Check balance
    const from = await tx.queryOne(
      'SELECT balance FROM accounts WHERE id = ?',
      [fromId]
    );

    if (!from || from.balance < amount) {
      throw new Error('Insufficient funds');
    }

    // Deduct from sender
    await tx.run(
      'UPDATE accounts SET balance = balance - ? WHERE id = ?',
      [amount, fromId]
    );

    // Add to receiver
    await tx.run(
      'UPDATE accounts SET balance = balance + ? WHERE id = ?',
      [amount, toId]
    );

    // Record transfer
    await tx.run(
      'INSERT INTO transfers (from_account_id, to_account_id, amount) VALUES (?, ?, ?)',
      [fromId, toId, amount]
    );
  });
}

// Execute transfer - either all operations succeed or none do
await transfer(1, 2, 200);

// Check balances
const accounts = await db.query('SELECT * FROM accounts');
console.log(accounts);
// [{ id: 1, name: 'Alice', balance: 800 }, { id: 2, name: 'Bob', balance: 700 }]
```

### Transaction with Rollback

```typescript
try {
  await db.transaction(async (tx) => {
    await tx.run('INSERT INTO orders (user_id, total) VALUES (?, ?)', [1, 99.99]);

    // This will fail if user doesn't have enough credit
    const user = await tx.queryOne('SELECT credits FROM users WHERE id = ?', [1]);
    if (user.credits < 99.99) {
      throw new Error('Insufficient credits');
    }

    await tx.run('UPDATE users SET credits = credits - ? WHERE id = ?', [99.99, 1]);
  });
} catch (error) {
  console.log('Transaction rolled back:', error.message);
  // The INSERT is also rolled back
}
```

### Nested Operations in Transactions

```typescript
async function processOrder(orderId: number) {
  await db.transaction(async (tx) => {
    // Get order details
    const order = await tx.queryOne('SELECT * FROM orders WHERE id = ?', [orderId]);

    // Get all order items
    const items = await tx.query(
      'SELECT * FROM order_items WHERE order_id = ?',
      [orderId]
    );

    // Update inventory for each item
    for (const item of items) {
      const result = await tx.run(
        'UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?',
        [item.quantity, item.product_id, item.quantity]
      );

      if (result.rowsAffected === 0) {
        throw new Error(`Insufficient stock for product ${item.product_id}`);
      }
    }

    // Mark order as processed
    await tx.run(
      'UPDATE orders SET status = ?, processed_at = CURRENT_TIMESTAMP WHERE id = ?',
      ['processed', orderId]
    );
  });
}
```

### Savepoints (Nested Transactions)

```typescript
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO logs (message) VALUES (?)', ['Starting batch']);

  // Try to process each item, but continue on individual failures
  for (const item of items) {
    try {
      await tx.savepoint(async (sp) => {
        await sp.run('INSERT INTO processed (item_id) VALUES (?)', [item.id]);

        if (item.invalid) {
          throw new Error('Invalid item');
        }
      });
    } catch (error) {
      // Savepoint rolled back, but outer transaction continues
      console.log(`Skipping item ${item.id}: ${error.message}`);
    }
  }

  await tx.run('INSERT INTO logs (message) VALUES (?)', ['Batch complete']);
});
```

### Transaction Options

```typescript
// Read-only transaction (optimized for queries)
const results = await db.transaction(async (tx) => {
  const users = await tx.query('SELECT * FROM users');
  const orders = await tx.query('SELECT * FROM orders');
  return { users, orders };
}, { readOnly: true });

// Transaction with timeout
await db.transaction(async (tx) => {
  // Long-running operations...
}, { timeout: 5000 }); // 5 second timeout

// Immediate transaction (acquire write lock immediately)
await db.transaction(async (tx) => {
  // Operations that need write lock from the start
}, { immediate: true });
```

---

## CDC Streaming

Change Data Capture (CDC) allows you to stream database changes in real-time.

### Basic CDC Setup

```typescript
import { DB, createCDCStream } from '@dotdo/dosql';

const db = await DB('app', {
  migrations: [
    {
      id: '001_init',
      sql: `
        CREATE TABLE users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          email TEXT UNIQUE,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
      `,
    },
  ],
  // Enable WAL for CDC support
  wal: true,
});

// Create a CDC stream
const stream = createCDCStream(db, {
  tables: ['users'],        // Tables to watch
  operations: ['INSERT', 'UPDATE', 'DELETE'], // Operations to capture
});

// Subscribe to changes
stream.subscribe((change) => {
  console.log('Change detected:', {
    table: change.table,
    operation: change.operation,
    data: change.data,
    oldData: change.oldData,     // For UPDATE/DELETE
    timestamp: change.timestamp,
    transactionId: change.txId,
  });
});

// Make some changes
await db.run('INSERT INTO users (name, email) VALUES (?, ?)', ['Alice', 'alice@example.com']);
// Output: Change detected: { table: 'users', operation: 'INSERT', data: { id: 1, name: 'Alice', ... } }

await db.run('UPDATE users SET name = ? WHERE id = ?', ['Alicia', 1]);
// Output: Change detected: { table: 'users', operation: 'UPDATE', data: { name: 'Alicia' }, oldData: { name: 'Alice' } }
```

### CDC with Webhooks

```typescript
import { DB, createCDCStream } from '@dotdo/dosql';

const db = await DB('orders', { wal: true });

const stream = createCDCStream(db, {
  tables: ['orders', 'order_items'],
});

// Forward changes to a webhook
stream.subscribe(async (change) => {
  if (change.table === 'orders' && change.operation === 'INSERT') {
    await fetch('https://api.example.com/webhooks/new-order', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        orderId: change.data.id,
        customerId: change.data.customer_id,
        total: change.data.total,
        timestamp: change.timestamp,
      }),
    });
  }
});
```

### CDC for Real-time Sync

```typescript
import { DB, createCDCStream } from '@dotdo/dosql';

export class SyncDatabase implements DurableObject {
  private db: Awaited<ReturnType<typeof DB>> | null = null;
  private connections = new Set<WebSocket>();

  async getDB() {
    if (!this.db) {
      this.db = await DB('sync', {
        migrations: { folder: '.do/migrations' },
        wal: true,
      });

      // Set up CDC to broadcast to all connected clients
      const stream = createCDCStream(this.db, {
        tables: ['*'], // Watch all tables
      });

      stream.subscribe((change) => {
        const message = JSON.stringify({
          type: 'change',
          ...change,
        });

        for (const ws of this.connections) {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(message);
          }
        }
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // WebSocket connection for real-time updates
    if (url.pathname === '/sync' && request.headers.get('Upgrade') === 'websocket') {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);

      server.accept();
      this.connections.add(server);

      server.addEventListener('close', () => {
        this.connections.delete(server);
      });

      // Send initial data
      const db = await this.getDB();
      const initialData = await db.query('SELECT * FROM items');
      server.send(JSON.stringify({ type: 'initial', data: initialData }));

      return new Response(null, { status: 101, webSocket: client });
    }

    // REST API...
    return new Response('Not Found', { status: 404 });
  }
}
```

### CDC with Filtering

```typescript
const stream = createCDCStream(db, {
  tables: ['orders'],
  operations: ['INSERT', 'UPDATE'],
  filter: (change) => {
    // Only capture high-value orders
    if (change.table === 'orders') {
      return change.data.total > 1000;
    }
    return true;
  },
});
```

### CDC Replay from Checkpoint

```typescript
// Save checkpoint
const checkpoint = await stream.getCheckpoint();
console.log('Checkpoint:', checkpoint); // { lsn: 12345, timestamp: '2024-01-15T10:30:00Z' }

// Later, replay from checkpoint
const replayStream = createCDCStream(db, {
  tables: ['orders'],
  fromCheckpoint: checkpoint,
});

replayStream.subscribe((change) => {
  console.log('Replayed change:', change);
});
```

### CDC to External Systems

```typescript
import { DB, createCDCStream } from '@dotdo/dosql';

const db = await DB('app', { wal: true });

const stream = createCDCStream(db, {
  tables: ['products'],
});

// Sync to Elasticsearch
stream.subscribe(async (change) => {
  const esClient = getElasticsearchClient();

  switch (change.operation) {
    case 'INSERT':
    case 'UPDATE':
      await esClient.index({
        index: 'products',
        id: change.data.id.toString(),
        body: change.data,
      });
      break;
    case 'DELETE':
      await esClient.delete({
        index: 'products',
        id: change.oldData.id.toString(),
      });
      break;
  }
});

// Sync to Redis cache
stream.subscribe(async (change) => {
  const redis = getRedisClient();

  if (change.operation === 'DELETE') {
    await redis.del(`product:${change.oldData.id}`);
  } else {
    await redis.set(`product:${change.data.id}`, JSON.stringify(change.data));
  }
});
```

---

## Migrations

### Migration File Convention

DoSQL uses a simple `.do/migrations/*.sql` convention:

```
your-project/
├── .do/
│   └── migrations/
│       ├── 001_create_users.sql
│       ├── 002_add_posts.sql
│       └── 003_add_indexes.sql
├── src/
└── package.json
```

### Migration File Format

```sql
-- .do/migrations/001_create_users.sql
-- Comments are supported

CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE
);

-- Multiple statements in one file
CREATE INDEX idx_users_email ON users(email);
```

### Naming Convention

- Format: `NNN_descriptive_name.sql`
- Prefix: 3-digit number for ordering (001, 002, etc.)
- Separator: Underscore
- Extension: `.sql`

### Migration Tracking

DoSQL tracks applied migrations in `__dosql_migrations`:

```sql
-- Created automatically
CREATE TABLE __dosql_migrations (
  id TEXT PRIMARY KEY,       -- Migration ID
  applied_at TEXT NOT NULL,  -- ISO timestamp
  checksum TEXT NOT NULL,    -- SHA-256 of SQL
  duration_ms INTEGER        -- Execution time
);
```

### Drizzle Kit Compatibility

DoSQL also supports Drizzle Kit migrations:

```typescript
// Load from Drizzle folder
const db = await DB('tenants', {
  migrations: { drizzle: './drizzle' },
});
```

### Manual Migration Control

```typescript
import { createMigrationRunner, createSchemaTracker } from '@dotdo/dosql/migrations';

// In a Durable Object
export class TenantDB {
  private tracker: SchemaTracker;
  private runner: MigrationRunner;

  constructor(state: DurableObjectState) {
    this.tracker = createSchemaTracker(state.storage);
    this.runner = createMigrationRunner(this.db);
  }

  async migrate(migrations: Migration[]) {
    const status = await this.runner.getStatus(migrations);

    if (status.needsMigration) {
      const result = await this.runner.migrate(migrations);
      console.log(`Applied ${result.applied.length} migrations`);
    }
  }
}
```

---

## Deploying to Cloudflare Workers

### Project Structure

```
your-project/
├── .do/
│   └── migrations/
│       └── 001_init.sql
├── src/
│   ├── index.ts        # Worker entry point
│   └── database.ts     # Database class
├── wrangler.jsonc
├── package.json
└── tsconfig.json
```

### Wrangler Configuration

DoSQL supports both `wrangler.toml` and `wrangler.jsonc` configuration formats. Choose whichever format you prefer.

#### Using wrangler.toml

```toml
# wrangler.toml
name = "my-app"
main = "src/index.ts"
compatibility_date = "2024-01-01"

# Durable Objects bindings
[[durable_objects.bindings]]
name = "DOSQL_DB"
class_name = "TenantDatabase"

# Durable Object migrations (required when adding new DO classes)
[[migrations]]
tag = "v1"
new_classes = ["TenantDatabase"]

# Optional: R2 bucket for cold storage (DoLake)
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-data-bucket"

# Optional: Multiple Durable Object classes
# [[durable_objects.bindings]]
# name = "USERS_DB"
# class_name = "UsersDatabase"
#
# [[durable_objects.bindings]]
# name = "ORDERS_DB"
# class_name = "OrdersDatabase"

# Optional: Environment-specific configuration
# [env.production]
# name = "my-app-production"
# [[env.production.r2_buckets]]
# binding = "DATA_BUCKET"
# bucket_name = "my-data-bucket-prod"
```

#### Using wrangler.jsonc

```jsonc
// wrangler.jsonc
{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "my-app",
  "main": "src/index.ts",
  "compatibility_date": "2024-01-01",

  // Durable Objects
  "durable_objects": {
    "bindings": [
      { "name": "DOSQL_DB", "class_name": "TenantDatabase" }
    ]
  },

  // DO migrations (not SQL migrations)
  "migrations": [
    { "tag": "v1", "new_classes": ["TenantDatabase"] }
  ],

  // Optional: R2 for cold storage
  "r2_buckets": [
    { "binding": "DATA_BUCKET", "bucket_name": "my-data-bucket" }
  ]
}
```

#### Configuration Reference

| Setting | Required | Description |
|---------|----------|-------------|
| `name` | Yes | Your worker name (used in deployment URL) |
| `main` | Yes | Entry point file for your worker |
| `compatibility_date` | Yes | Cloudflare Workers compatibility date |
| `durable_objects.bindings` | Yes | Durable Object namespace bindings for DoSQL |
| `migrations` | Yes | DO class migrations (tracks new/renamed/deleted classes) |
| `r2_buckets` | No | R2 bucket bindings for cold storage (DoLake) |

### Worker Implementation

```typescript
// src/index.ts
import { TenantDatabase } from './database';

export { TenantDatabase };

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  DATA_BUCKET?: R2Bucket;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const tenantId = url.searchParams.get('tenant') || 'default';

    // Get or create Durable Object for tenant
    const id = env.DOSQL_DB.idFromName(tenantId);
    const db = env.DOSQL_DB.get(id);

    // Forward request to DO
    return db.fetch(request);
  },
};
```

### Database Durable Object

```typescript
// src/database.ts
import { DB } from '@dotdo/dosql';

export class TenantDatabase implements DurableObject {
  private db: Database | null = null;
  private state: DurableObjectState;
  private env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  private async getDB(): Promise<Database> {
    if (!this.db) {
      this.db = await DB('tenant', {
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
    const db = await this.getDB();
    const url = new URL(request.url);

    if (url.pathname === '/users' && request.method === 'GET') {
      const users = await db.query('SELECT * FROM users');
      return Response.json(users);
    }

    if (url.pathname === '/users' && request.method === 'POST') {
      const body = await request.json() as { name: string; email: string };
      await db.run(
        'INSERT INTO users (name, email) VALUES (?, ?)',
        [body.name, body.email]
      );
      return Response.json({ success: true }, { status: 201 });
    }

    return new Response('Not Found', { status: 404 });
  }
}
```

### Deploying

```bash
# Deploy to Cloudflare
npx wrangler deploy

# Deploy to specific environment
npx wrangler deploy --env production

# Test locally
npx wrangler dev
```

### Environment Variables

```bash
# Set secrets
npx wrangler secret put DATABASE_KEY
```

### Monitoring

```bash
# View logs
npx wrangler tail

# View DO analytics
npx wrangler durable-objects list
```

---

## Error Handling for Common Setup Failures

This section covers errors you may encounter during initial setup and how to resolve them.

### Installation Errors

#### "Cannot find module '@dotdo/dosql'"

**Cause:** The package is not installed or there's a module resolution issue.

**Solutions:**

```bash
# Verify the package is installed
npm list @dotdo/dosql

# If not installed, install it
npm install @dotdo/dosql

# Clear npm cache if installation fails
npm cache clean --force
npm install @dotdo/dosql

# If using a monorepo, ensure you're in the correct workspace
npm install @dotdo/dosql --workspace=your-app
```

#### "Module not found: @cloudflare/workers-types"

**Cause:** TypeScript cannot find Cloudflare type definitions.

**Solution:**

```bash
# Install the types package
npm install @cloudflare/workers-types --save-dev

# Verify tsconfig.json includes the types
# Add to compilerOptions.types: ["@cloudflare/workers-types"]
```

### Wrangler Setup Errors

#### "Error: You must be logged in to use this command"

**Cause:** Wrangler is not authenticated with your Cloudflare account.

**Solution:**

```bash
# Login to Cloudflare
wrangler login

# Verify authentication
wrangler whoami

# If login fails, try with API token
wrangler login --api-token YOUR_API_TOKEN
```

#### "Error: A compatibility_date is required"

**Cause:** Your wrangler configuration is missing the required compatibility_date field.

**Solution:**

Add the compatibility_date to your wrangler.toml or wrangler.jsonc:

```toml
# wrangler.toml
compatibility_date = "2024-01-01"
```

```jsonc
// wrangler.jsonc
{
  "compatibility_date": "2024-01-01"
}
```

#### "Error: Could not find wrangler.toml"

**Cause:** Wrangler cannot find your configuration file.

**Solution:**

```bash
# Check if the file exists
ls -la wrangler.toml wrangler.jsonc

# Create wrangler.jsonc if missing
cat > wrangler.jsonc << 'EOF'
{
  "name": "my-dosql-app",
  "main": "src/index.ts",
  "compatibility_date": "2024-01-01"
}
EOF

# Or specify the config file path
npx wrangler dev --config ./path/to/wrangler.jsonc
```

### Durable Object Setup Errors

#### "Error: No Durable Object bindings found"

**Cause:** Durable Object bindings are not configured in wrangler configuration.

**Solution:**

Add the durable_objects section to your configuration:

```jsonc
// wrangler.jsonc
{
  "durable_objects": {
    "bindings": [
      { "name": "MY_DATABASE", "class_name": "MyDatabase" }
    ]
  },
  "migrations": [
    { "tag": "v1", "new_classes": ["MyDatabase"] }
  ]
}
```

#### "Error: class_name 'MyDatabase' not found in exports"

**Cause:** The Durable Object class is not exported from your worker entry point.

**Solution:**

```typescript
// src/index.ts
import { MyDatabase } from './database';

// Export the class so Wrangler can find it
export { MyDatabase };

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // ...
  },
};
```

#### "Error: Durable Objects require a Workers Paid plan"

**Cause:** Your Cloudflare account is on the free tier, which doesn't include Durable Objects.

**Solution:**

1. Go to [dash.cloudflare.com](https://dash.cloudflare.com)
2. Navigate to Workers & Pages > Plans
3. Upgrade to the Workers Paid plan ($5/month)
4. Wait a few minutes for the change to propagate
5. Run `wrangler deploy` again

### Migration Setup Errors

#### "Error: Migration folder not found: .do/migrations"

**Cause:** The migrations directory doesn't exist or the path is incorrect.

**Solution:**

```bash
# Create the migrations directory
mkdir -p .do/migrations

# Create your first migration file
cat > .do/migrations/001_init.sql << 'EOF'
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
EOF
```

#### "Error: Invalid migration file format"

**Cause:** Migration files don't follow the expected naming convention.

**Solution:**

Migration files must follow the pattern `NNN_description.sql`:

```bash
# Correct naming
001_init.sql
002_add_users.sql
003_add_indexes.sql

# Incorrect naming (will cause errors)
init.sql           # Missing number prefix
1_init.sql         # Number should be 3 digits
001-init.sql       # Use underscore, not hyphen
001_init.txt       # Must use .sql extension
```

### R2 Bucket Setup Errors

#### "Error: R2 bucket 'my-bucket' not found"

**Cause:** The R2 bucket doesn't exist or the binding name is incorrect.

**Solution:**

```bash
# List existing buckets
wrangler r2 bucket list

# Create the bucket if it doesn't exist
wrangler r2 bucket create my-data-bucket

# Verify the binding name matches your configuration
# wrangler.jsonc should have:
# "r2_buckets": [{ "binding": "DATA_BUCKET", "bucket_name": "my-data-bucket" }]
```

#### "Error: R2 is not enabled for this account"

**Cause:** R2 storage is not enabled on your Cloudflare account.

**Solution:**

1. Go to [dash.cloudflare.com](https://dash.cloudflare.com)
2. Navigate to R2 in the sidebar
3. Accept the R2 terms and enable the service
4. Create your bucket

### TypeScript Configuration Errors

#### "Cannot find name 'DurableObject'"

**Cause:** TypeScript doesn't know about Cloudflare Workers types.

**Solution:**

Update your tsconfig.json:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "types": ["@cloudflare/workers-types"],
    "lib": ["ES2022"]
  }
}
```

#### "Property 'storage' does not exist on type 'DurableObjectState'"

**Cause:** Outdated @cloudflare/workers-types version.

**Solution:**

```bash
# Update to the latest version
npm install @cloudflare/workers-types@latest --save-dev

# If using a specific version, ensure it's 4.0.0 or higher
npm install @cloudflare/workers-types@^4.0.0 --save-dev
```

### Local Development Errors

#### "Error: Port 8787 is already in use"

**Cause:** Another process is using the default wrangler dev port.

**Solution:**

```bash
# Find and kill the process using port 8787
lsof -i :8787
kill -9 <PID>

# Or use a different port
npx wrangler dev --port 8788
```

#### "Error: Could not connect to local Durable Object"

**Cause:** Local development environment is not properly initialized.

**Solution:**

```bash
# Clear wrangler's local state
rm -rf .wrangler

# Restart wrangler dev
npx wrangler dev

# If issues persist, try with --local flag
npx wrangler dev --local
```

### Quick Setup Validation Checklist

Run through this checklist to verify your setup is correct:

```bash
# 1. Verify Wrangler installation
wrangler --version
# Expected: 3.0.0 or higher

# 2. Verify authentication
wrangler whoami
# Expected: Your Cloudflare account email

# 3. Verify package installation
npm list @dotdo/dosql
# Expected: Package version listed

# 4. Verify migrations directory exists
ls -la .do/migrations/
# Expected: SQL migration files

# 5. Verify TypeScript compiles
npx tsc --noEmit
# Expected: No errors

# 6. Verify local dev server starts
npx wrangler dev
# Expected: Server running on localhost:8787
```

---

## Troubleshooting

### Common Issues

#### "Database not found" or "No such table"

**Cause:** Migrations have not been applied or the database name is incorrect.

**Solution:**

```typescript
// Ensure autoMigrate is enabled (default: true)
const db = await DB('my-database', {
  migrations: { folder: '.do/migrations' },
  autoMigrate: true, // Make sure this is true
});

// Or manually check migration status
const status = await db.getMigrationStatus();
console.log('Pending migrations:', status.pending);
```

#### "SQLITE_CONSTRAINT: UNIQUE constraint failed"

**Cause:** Attempting to insert a duplicate value in a UNIQUE column.

**Solution:**

```typescript
// Use INSERT OR REPLACE
await db.run(
  'INSERT OR REPLACE INTO users (email, name) VALUES (?, ?)',
  ['alice@example.com', 'Alice']
);

// Or use INSERT OR IGNORE
await db.run(
  'INSERT OR IGNORE INTO users (email, name) VALUES (?, ?)',
  ['alice@example.com', 'Alice']
);

// Or check before inserting
const existing = await db.queryOne('SELECT id FROM users WHERE email = ?', [email]);
if (!existing) {
  await db.run('INSERT INTO users (email, name) VALUES (?, ?)', [email, name]);
}
```

#### "SQLITE_BUSY: database is locked"

**Cause:** Another operation is holding a write lock on the database.

**Solution:**

```typescript
// Use transactions to group operations
await db.transaction(async (tx) => {
  // All operations in the same transaction
  await tx.run('INSERT INTO table1 ...');
  await tx.run('UPDATE table2 ...');
});

// Or increase timeout
const db = await DB('my-database', {
  busyTimeout: 5000, // 5 seconds
});
```

#### "Migration checksum mismatch"

**Cause:** A migration file was modified after being applied.

**Solution:**

```typescript
// Option 1: Revert to original migration content
// (Check your version control)

// Option 2: Force re-apply (development only!)
const db = await DB('my-database', {
  migrations: { folder: '.do/migrations' },
  validateChecksums: false, // Disable checksum validation
});

// Option 3: Reset the database (development only!)
await db.run('DROP TABLE __dosql_migrations');
// Then restart your application
```

#### "Cannot read properties of undefined"

**Cause:** Query returned no results and you're accessing properties on `undefined`.

**Solution:**

```typescript
// Always check if result exists
const user = await db.queryOne('SELECT * FROM users WHERE id = ?', [id]);
if (!user) {
  throw new Error('User not found');
}
console.log(user.name);

// Or use optional chaining
const userName = user?.name ?? 'Unknown';
```

#### Durable Object Errors

**"Durable Object storage is not available"**

```typescript
// Ensure you're using storage from the state object
export class MyDatabase implements DurableObject {
  constructor(state: DurableObjectState, env: Env) {
    // Correct: use state.storage
    this.db = await DB('app', {
      storage: { hot: state.storage },
    });
  }
}
```

**"Durable Object has been deleted"**

```typescript
// The DO was deleted or reset. Create a new instance
const id = env.MY_DO.idFromName('new-unique-name');
const stub = env.MY_DO.get(id);
```

### Performance Issues

#### Slow Queries

```typescript
// Add indexes for frequently queried columns
await db.run('CREATE INDEX idx_users_email ON users(email)');
await db.run('CREATE INDEX idx_orders_user_date ON orders(user_id, created_at)');

// Use EXPLAIN to analyze queries
const plan = await db.query('EXPLAIN QUERY PLAN SELECT * FROM users WHERE email = ?', ['test@example.com']);
console.log(plan);

// Avoid SELECT * when you only need specific columns
// Instead of:
const users = await db.query('SELECT * FROM users');
// Use:
const users = await db.query('SELECT id, name FROM users');
```

#### High Memory Usage

```typescript
// Use pagination for large result sets
const pageSize = 100;
let offset = 0;
let hasMore = true;

while (hasMore) {
  const batch = await db.query(
    'SELECT * FROM large_table LIMIT ? OFFSET ?',
    [pageSize, offset]
  );

  // Process batch
  processBatch(batch);

  offset += pageSize;
  hasMore = batch.length === pageSize;
}

// Or use streaming for very large datasets
for await (const row of db.stream('SELECT * FROM large_table')) {
  processRow(row);
}
```

### Debugging Tips

```typescript
// Enable debug logging
const db = await DB('my-database', {
  logger: {
    debug: console.debug,
    info: console.info,
    warn: console.warn,
    error: console.error,
  },
});

// Log all queries
const db = await DB('my-database', {
  onQuery: (sql, params, duration) => {
    console.log(`Query: ${sql}`);
    console.log(`Params: ${JSON.stringify(params)}`);
    console.log(`Duration: ${duration}ms`);
  },
});

// Check database schema
const tables = await db.query(`
  SELECT name, sql FROM sqlite_master
  WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
`);
console.log('Tables:', tables);

// Check indexes
const indexes = await db.query(`
  SELECT name, tbl_name, sql FROM sqlite_master
  WHERE type = 'index'
`);
console.log('Indexes:', indexes);
```

### Getting Help

If you're still experiencing issues:

1. Check the [GitHub Issues](https://github.com/dotdo/dosql/issues) for similar problems
2. Search the [Discussions](https://github.com/dotdo/dosql/discussions) for solutions
3. Join our [Discord community](https://discord.gg/dosql) for real-time help
4. Create a new issue with:
   - DoSQL version
   - Node.js/Wrangler version
   - Minimal reproduction code
   - Full error message and stack trace

---

## Next Steps

Now that you have DoSQL running, explore these guides to learn more:

### Core Concepts

- [API Reference](./api-reference.md) - Complete API documentation for all DoSQL functions and types
- [Architecture](./architecture.md) - Understanding DoSQL internals, storage tiers, and design decisions
- [Migrations Guide](./migrations.md) - Advanced migration patterns, rollbacks, and schema versioning

### Advanced Features

- [Advanced Features](./advanced.md) - Time travel queries, branching, and database snapshots
- [Transactions Deep Dive](./transactions.md) - Isolation levels, deadlock prevention, and performance tuning
- [CDC Streaming Guide](./cdc.md) - Building real-time applications with Change Data Capture

### Patterns and Best Practices

- [Multi-tenancy](./multi-tenancy.md) - Patterns for building multi-tenant SaaS applications
- [Performance Optimization](./performance.md) - Indexing strategies, query optimization, and caching
- [Testing Guide](./testing.md) - Unit testing, integration testing, and mocking DoSQL

### Integrations

- [Drizzle ORM Integration](./drizzle.md) - Using DoSQL with Drizzle for type-safe queries
- [Hono Integration](./hono.md) - Building APIs with Hono and DoSQL
- [React/Next.js Integration](./react.md) - Full-stack patterns with React and DoSQL

### Deployment

- [Production Checklist](./production.md) - Security, monitoring, and reliability best practices
- [Scaling Guide](./scaling.md) - Horizontal scaling, sharding, and read replicas
- [Backup and Recovery](./backup.md) - Backup strategies, point-in-time recovery, and disaster recovery

### Examples

- [Example: Todo App](./examples/todo-app.md) - Simple CRUD application
- [Example: E-commerce](./examples/ecommerce.md) - Complex multi-table application with transactions
- [Example: Real-time Chat](./examples/chat.md) - WebSockets and CDC for live updates
- [Example: Analytics Dashboard](./examples/analytics.md) - Aggregations and time-series data
