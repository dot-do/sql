# Getting Started with DoSQL

Build SQL-powered applications on Cloudflare Workers with type-safe queries and automatic migrations. This guide will get you from zero to a working API in under 10 minutes.

## What is DoSQL?

DoSQL is a lightweight SQL database for Cloudflare Workers and Durable Objects. It provides:

- **Type-safe SQL queries** - Catch errors at compile time, not runtime
- **Automatic migrations** - Schema changes applied seamlessly
- **Multi-tenant isolation** - Each Durable Object is an isolated database
- **Zero cold starts** - Data lives at the edge, close to your users

---

## Prerequisites

Before starting, you need:

1. **Node.js 18+** installed
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
CREATE TABLE tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  completed BOOLEAN DEFAULT false,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

### Step 4: Write Your Worker

Create `src/index.ts`:

```typescript
import { DB } from '@dotdo/dosql';

export interface Env {
  TASKS_DB: DurableObjectNamespace;
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

    // PUT /tasks/:id - Update a task
    if (url.pathname.startsWith('/tasks/') && request.method === 'PUT') {
      const id = url.pathname.split('/')[2];
      const { completed } = await request.json() as { completed: boolean };
      await db.run('UPDATE tasks SET completed = ? WHERE id = ?', [completed, id]);
      return Response.json({ success: true });
    }

    // DELETE /tasks/:id - Delete a task
    if (url.pathname.startsWith('/tasks/') && request.method === 'DELETE') {
      const id = url.pathname.split('/')[2];
      await db.run('DELETE FROM tasks WHERE id = ?', [id]);
      return Response.json({ success: true });
    }

    return new Response('Not Found', { status: 404 });
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

# List all tasks
curl http://localhost:8787/tasks

# Mark task as complete
curl -X PUT http://localhost:8787/tasks/1 \
  -H "Content-Type: application/json" \
  -d '{"completed": true}'

# Delete a task
curl -X DELETE http://localhost:8787/tasks/1
```

You should see output like:

```json
[{"id": 1, "title": "Learn DoSQL", "completed": 0, "created_at": "2024-01-15T10:30:00.000Z"}]
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

### Querying Data

Use `query()` to fetch data and `run()` to modify data:

```typescript
// Fetch multiple rows
const users = await db.query('SELECT * FROM users WHERE active = ?', [true]);

// Fetch a single row
const user = await db.queryOne('SELECT * FROM users WHERE id = ?', [1]);

// Insert, update, or delete
const result = await db.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
console.log(result.lastInsertRowId);  // The new row's ID
console.log(result.rowsAffected);     // Number of rows changed
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
