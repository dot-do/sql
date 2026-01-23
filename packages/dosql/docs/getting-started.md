# Getting Started with DoSQL

Build SQL-powered applications on Cloudflare Workers with type-safe queries and automatic migrations. This guide walks you through creating a working REST API in under 10 minutes.

## What is DoSQL?

DoSQL is a lightweight SQL database engine for Cloudflare Workers and Durable Objects. It provides:

- **Type-safe SQL queries** - Catch errors at compile time, not runtime
- **Automatic migrations** - Schema changes applied seamlessly on startup
- **Multi-tenant isolation** - Each Durable Object is an isolated database instance
- **Edge-native performance** - Data lives close to your users with zero cold starts

---

## Prerequisites

Before starting, ensure you have:

1. **Node.js 20+** installed (check with `node --version`)
2. **A Cloudflare account** with a Workers Paid plan ($5/month for Durable Objects)
3. **Wrangler CLI** installed and authenticated

```bash
# Install Wrangler globally
npm install -g wrangler

# Login to your Cloudflare account
wrangler login

# Verify you're logged in
wrangler whoami
```

> **Note**: Durable Objects require a Workers Paid plan. You can upgrade at [dash.cloudflare.com](https://dash.cloudflare.com) under Workers & Pages > Plans.

---

## Quick Start

### Step 1: Create Your Project

```bash
mkdir my-dosql-app && cd my-dosql-app
npm init -y
npm install @dotdo/dosql
npm install -D wrangler @cloudflare/workers-types typescript
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
    "skipLibCheck": true,
    "types": ["@cloudflare/workers-types"],
    "lib": ["ES2022"]
  },
  "include": ["src/**/*"]
}
```

### Step 3: Create Your Database Schema

Create the migrations folder and your first migration:

```bash
mkdir -p .do/migrations
```

Create `.do/migrations/001_init.sql`:

```sql
-- Create the tasks table
CREATE TABLE tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  completed INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Add an index for faster queries
CREATE INDEX idx_tasks_completed ON tasks(completed);
```

> **Note**: SQLite uses `INTEGER` (0/1) for boolean values. DoSQL handles the conversion automatically.

### Step 4: Write Your Worker

Create `src/index.ts`:

```typescript
import { DB } from '@dotdo/dosql';

export interface Env {
  TASKS_DB: DurableObjectNamespace;
}

// Define the Task type for type safety
interface Task {
  id: number;
  title: string;
  completed: number;
  created_at: string;
}

// The Durable Object that holds your database
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

    try {
      // GET /tasks - List all tasks
      if (url.pathname === '/tasks' && request.method === 'GET') {
        const tasks = await db.query<Task>('SELECT * FROM tasks ORDER BY created_at DESC');
        return Response.json(tasks);
      }

      // POST /tasks - Create a task
      if (url.pathname === '/tasks' && request.method === 'POST') {
        const { title } = await request.json() as { title: string };
        if (!title || typeof title !== 'string') {
          return Response.json({ error: 'Title is required' }, { status: 400 });
        }
        const result = await db.run(
          'INSERT INTO tasks (title) VALUES (?)',
          [title]
        );
        return Response.json({ id: result.lastInsertRowId, title }, { status: 201 });
      }

      // PUT /tasks/:id - Update a task
      if (url.pathname.startsWith('/tasks/') && request.method === 'PUT') {
        const id = parseInt(url.pathname.split('/')[2], 10);
        if (isNaN(id)) {
          return Response.json({ error: 'Invalid task ID' }, { status: 400 });
        }
        const { completed } = await request.json() as { completed: boolean };
        await db.run('UPDATE tasks SET completed = ? WHERE id = ?', [completed ? 1 : 0, id]);
        return Response.json({ success: true });
      }

      // DELETE /tasks/:id - Delete a task
      if (url.pathname.startsWith('/tasks/') && request.method === 'DELETE') {
        const id = parseInt(url.pathname.split('/')[2], 10);
        if (isNaN(id)) {
          return Response.json({ error: 'Invalid task ID' }, { status: 400 });
        }
        await db.run('DELETE FROM tasks WHERE id = ?', [id]);
        return Response.json({ success: true });
      }

      return new Response('Not Found', { status: 404 });
    } catch (error) {
      console.error('Database error:', error);
      return Response.json({ error: 'Internal server error' }, { status: 500 });
    }
  }
}

// The Worker entry point that routes to the Durable Object
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
  "compatibility_date": "2024-12-01",
  "durable_objects": {
    "bindings": [
      { "name": "TASKS_DB", "class_name": "TasksDatabase" }
    ]
  },
  "migrations": [
    { "tag": "v1", "new_classes": ["TasksDatabase"] }
  ],
  // Include migration files in the build
  "rules": [
    { "type": "Data", "globs": [".do/migrations/*.sql"] }
  ]
}
```

> **Tip**: The `compatibility_date` should be set to a recent date. Check the [Cloudflare docs](https://developers.cloudflare.com/workers/configuration/compatibility-dates/) for the latest recommended date.

### Step 6: Run Locally

```bash
npx wrangler dev
```

You should see output indicating the worker is running on `http://localhost:8787`.

### Step 7: Test Your API

Open a new terminal and test your API:

```bash
# Create a task
curl -X POST http://localhost:8787/tasks \
  -H "Content-Type: application/json" \
  -d '{"title": "Learn DoSQL"}'

# Expected response: {"id":1,"title":"Learn DoSQL"}
```

```bash
# List all tasks
curl http://localhost:8787/tasks

# Expected response: [{"id":1,"title":"Learn DoSQL","completed":0,"created_at":"..."}]
```

```bash
# Mark task as complete
curl -X PUT http://localhost:8787/tasks/1 \
  -H "Content-Type: application/json" \
  -d '{"completed": true}'

# Expected response: {"success":true}
```

```bash
# Delete a task
curl -X DELETE http://localhost:8787/tasks/1

# Expected response: {"success":true}
```

---

## Core Concepts

### The DB() Function

`DB()` is the main entry point for DoSQL. It creates or connects to a database with automatic migration support.

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('my-database', {
  migrations: { folder: '.do/migrations' },  // Path to SQL migrations
  storage: { hot: state.storage },           // Durable Object storage
});
```

The first argument is a database name (used for identification and logging). The second argument configures migrations and storage.

### Querying Data

Use `query()` to fetch data and `run()` to modify data:

```typescript
// Fetch multiple rows with type safety
interface User {
  id: number;
  name: string;
  active: number;
}

const users = await db.query<User>('SELECT * FROM users WHERE active = ?', [1]);
// users is typed as User[]

// Fetch a single row
const user = await db.queryOne<User>('SELECT * FROM users WHERE id = ?', [1]);
// user is typed as User | undefined

// Insert, update, or delete
const result = await db.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
console.log(result.lastInsertRowId);  // The new row's ID (e.g., 1)
console.log(result.rowsAffected);     // Number of rows changed (e.g., 1)
```

### Migrations

Migrations are SQL files in `.do/migrations/` that run automatically when the database initializes:

```
.do/migrations/
  001_init.sql
  002_add_users.sql
  003_add_indexes.sql
```

Each migration runs once and is tracked in a `__dosql_migrations` table.

### Transactions

Use `transaction()` to group multiple operations atomically:

```typescript
await db.transaction(async (tx) => {
  await tx.run('UPDATE accounts SET balance = balance - ? WHERE id = ?', [100, 1]);
  await tx.run('UPDATE accounts SET balance = balance + ? WHERE id = ?', [100, 2]);
  // If either fails, both are rolled back
});
```

---

## Deploy to Production

When you're ready to go live:

```bash
npx wrangler deploy
```

Your API is now running globally on Cloudflare's edge network.

---

## Common Patterns

### Multi-Tenant Applications

Create one Durable Object per tenant for complete data isolation:

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Extract tenant from subdomain, header, or path
    const tenantId = getTenantId(request);

    // Each tenant gets their own isolated database
    const id = env.TENANT_DB.idFromName(tenantId);
    const stub = env.TENANT_DB.get(id);
    return stub.fetch(request);
  },
};
```

### Type-Safe Queries

Add TypeScript types to your queries:

```typescript
interface User {
  id: number;
  name: string;
  email: string;
}

const users = await db.query<User>('SELECT id, name, email FROM users');
// users is typed as User[]

const user = await db.queryOne<User>('SELECT * FROM users WHERE id = ?', [1]);
// user is typed as User | undefined
```

### Parameterized Queries

Always use parameters to prevent SQL injection:

```typescript
// Good - uses parameters
await db.query('SELECT * FROM users WHERE email = ?', [userInput]);

// Bad - vulnerable to SQL injection
await db.query(`SELECT * FROM users WHERE email = '${userInput}'`);
```

---

## Troubleshooting

### "Durable Objects require a Workers Paid plan"

Upgrade to the Workers Paid plan ($5/month) at [dash.cloudflare.com](https://dash.cloudflare.com) > Workers & Pages > Plans.

### "class_name 'TasksDatabase' not found in exports"

Make sure you export your Durable Object class from `src/index.ts`:

```typescript
export class TasksDatabase implements DurableObject {
  // ...
}
```

### "Migration folder not found"

Create the migrations directory:

```bash
mkdir -p .do/migrations
```

### "Port 8787 is already in use"

Kill any existing wrangler processes:

```bash
pkill -f wrangler
# Or use a different port
npx wrangler dev --port 8788
```

---

## Next Steps

Now that you have DoSQL running, explore these topics:

- **[API Reference](./api-reference.md)** - Complete documentation for all functions
- **[Migrations Guide](./migrations.md)** - Advanced migration patterns
- **[Transactions](./transactions.md)** - Isolation levels and best practices
- **[CDC Streaming](./cdc.md)** - Real-time change data capture
- **[Multi-Tenancy](./multi-tenancy.md)** - Building SaaS applications
- **[Performance](./performance.md)** - Indexing and query optimization

---

## Example Applications

- **[Todo App](./examples/todo-app.md)** - Simple CRUD application
- **[E-commerce](./examples/ecommerce.md)** - Inventory and orders with transactions
- **[Real-time Chat](./examples/chat.md)** - WebSockets with CDC streaming
