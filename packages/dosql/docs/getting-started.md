# Getting Started with DoSQL

Build SQL-powered applications on Cloudflare Workers with type-safe queries and automatic migrations. This guide takes you from zero to a deployed REST API in under 10 minutes.

## What You Will Build

A fully functional task management API with:
- Create, read, update, and delete operations
- Persistent storage in Durable Objects
- Automatic database migrations
- Type-safe queries

## What is DoSQL?

DoSQL is a lightweight SQL database engine designed specifically for Cloudflare Workers and Durable Objects. It brings the familiarity of SQL to the edge with modern developer experience features:

| Feature | Description |
|---------|-------------|
| **Type-Safe Queries** | TypeScript catches query errors at compile time |
| **Auto Migrations** | Schema changes deploy automatically with your code |
| **Multi-Tenant Ready** | Each Durable Object instance is an isolated database |
| **Edge Performance** | Data lives close to users with sub-millisecond latency |

---

## Prerequisites

Before you begin, make sure you have:

- **Node.js 20 or later** - Check with `node --version`
- **A Cloudflare account** - [Sign up free](https://dash.cloudflare.com/sign-up)
- **Workers Paid plan** - Required for Durable Objects ($5/month)

### Install and Configure Wrangler

Wrangler is Cloudflare's CLI for managing Workers projects.

```bash
# Install Wrangler globally
npm install -g wrangler

# Log in to your Cloudflare account
wrangler login

# Verify your login
wrangler whoami
```

You should see your account email and name. If you see an error, make sure you completed the login flow in your browser.

---

## Step 1: Create Your Project

Start by creating a new directory and initializing the project:

```bash
# Create project directory
mkdir my-tasks-api && cd my-tasks-api

# Initialize package.json
npm init -y

# Install dependencies
npm install @dotdo/dosql
npm install -D wrangler @cloudflare/workers-types typescript
```

Your `package.json` should now include `@dotdo/dosql` in dependencies.

---

## Step 2: Configure TypeScript

Create a `tsconfig.json` file in your project root:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": true,
    "skipLibCheck": true,
    "types": ["@cloudflare/workers-types"],
    "lib": ["ES2022"],
    "outDir": "./dist"
  },
  "include": ["src/**/*"]
}
```

This configuration ensures TypeScript understands Cloudflare Workers globals like `DurableObject` and `Request`.

---

## Step 3: Define Your Database Schema

DoSQL uses SQL migration files to define and evolve your database schema. Create the migrations folder:

```bash
mkdir -p .do/migrations
```

Create your first migration file at `.do/migrations/001_create_tasks.sql`:

```sql
-- Create the tasks table
CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  completed INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Index for filtering by completion status
CREATE INDEX IF NOT EXISTS idx_tasks_completed ON tasks(completed);
```

**Key points:**
- Files are executed in alphabetical order (prefix with numbers like `001_`, `002_`)
- Use `IF NOT EXISTS` to make migrations idempotent
- SQLite uses `INTEGER` (0/1) for boolean values

---

## Step 4: Write Your Worker Code

Create `src/index.ts` with the following code:

```typescript
import { DB } from '@dotdo/dosql';

// Environment bindings
export interface Env {
  TASKS_DB: DurableObjectNamespace;
}

// Type definition for a task
interface Task {
  id: number;
  title: string;
  completed: number;  // 0 = false, 1 = true
  created_at: string;
}

// The Durable Object class that manages the database
export class TasksDatabase implements DurableObject {
  private db: Awaited<ReturnType<typeof DB>> | null = null;

  constructor(private state: DurableObjectState) {}

  // Lazy initialization of the database
  private async getDB() {
    if (!this.db) {
      this.db = await DB('tasks', {
        migrations: { folder: '.do/migrations' },
        storage: { hot: this.state.storage },
      });
    }
    return this.db;
  }

  // Handle incoming requests
  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    try {
      // GET /tasks - List all tasks
      if (path === '/tasks' && method === 'GET') {
        const tasks = await db.query<Task>(
          'SELECT * FROM tasks ORDER BY created_at DESC'
        );
        return Response.json(tasks);
      }

      // POST /tasks - Create a new task
      if (path === '/tasks' && method === 'POST') {
        const body = await request.json() as { title?: string };

        if (!body.title || typeof body.title !== 'string') {
          return Response.json(
            { error: 'Title is required and must be a string' },
            { status: 400 }
          );
        }

        const result = await db.run(
          'INSERT INTO tasks (title) VALUES (?)',
          [body.title]
        );

        return Response.json(
          { id: result.lastInsertRowId, title: body.title },
          { status: 201 }
        );
      }

      // GET /tasks/:id - Get a single task
      if (path.match(/^\/tasks\/\d+$/) && method === 'GET') {
        const id = parseInt(path.split('/')[2], 10);
        const task = await db.queryOne<Task>(
          'SELECT * FROM tasks WHERE id = ?',
          [id]
        );

        if (!task) {
          return Response.json({ error: 'Task not found' }, { status: 404 });
        }

        return Response.json(task);
      }

      // PUT /tasks/:id - Update a task
      if (path.match(/^\/tasks\/\d+$/) && method === 'PUT') {
        const id = parseInt(path.split('/')[2], 10);
        const body = await request.json() as { title?: string; completed?: boolean };

        // Build dynamic update query
        const updates: string[] = [];
        const params: (string | number)[] = [];

        if (body.title !== undefined) {
          updates.push('title = ?');
          params.push(body.title);
        }
        if (body.completed !== undefined) {
          updates.push('completed = ?');
          params.push(body.completed ? 1 : 0);
        }

        if (updates.length === 0) {
          return Response.json(
            { error: 'No fields to update' },
            { status: 400 }
          );
        }

        params.push(id);
        await db.run(
          `UPDATE tasks SET ${updates.join(', ')} WHERE id = ?`,
          params
        );

        return Response.json({ success: true });
      }

      // DELETE /tasks/:id - Delete a task
      if (path.match(/^\/tasks\/\d+$/) && method === 'DELETE') {
        const id = parseInt(path.split('/')[2], 10);
        const result = await db.run('DELETE FROM tasks WHERE id = ?', [id]);

        if (result.rowsAffected === 0) {
          return Response.json({ error: 'Task not found' }, { status: 404 });
        }

        return Response.json({ success: true });
      }

      // 404 for unmatched routes
      return Response.json({ error: 'Not found' }, { status: 404 });

    } catch (error) {
      console.error('Request error:', error);
      return Response.json(
        { error: 'Internal server error' },
        { status: 500 }
      );
    }
  }
}

// Worker entry point - routes requests to the Durable Object
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Get a reference to the Durable Object
    const id = env.TASKS_DB.idFromName('default');
    const stub = env.TASKS_DB.get(id);

    // Forward the request to the Durable Object
    return stub.fetch(request);
  },
};
```

---

## Step 5: Configure Wrangler

Create `wrangler.jsonc` in your project root:

```jsonc
{
  "name": "my-tasks-api",
  "main": "src/index.ts",
  "compatibility_date": "2024-12-01",

  // Durable Object configuration
  "durable_objects": {
    "bindings": [
      {
        "name": "TASKS_DB",
        "class_name": "TasksDatabase"
      }
    ]
  },

  // Required for Durable Objects - tracks class versions
  "migrations": [
    {
      "tag": "v1",
      "new_classes": ["TasksDatabase"]
    }
  ]
}
```

**Important notes:**
- The `name` in `durable_objects.bindings` must match the property name in your `Env` interface
- The `class_name` must match your exported class name exactly (case-sensitive)
- The `migrations` array tracks Durable Object class changes (separate from SQL migrations)

---

## Step 6: Run Locally

Start the local development server:

```bash
npx wrangler dev
```

You should see output like:

```
Starting local server...
Ready on http://localhost:8787
```

The first request may take a moment as the database initializes and runs migrations.

---

## Step 7: Test Your API

Open a new terminal and test each endpoint:

### Create a Task

```bash
curl -X POST http://localhost:8787/tasks \
  -H "Content-Type: application/json" \
  -d '{"title": "Learn DoSQL"}'
```

**Expected response:**
```json
{"id":1,"title":"Learn DoSQL"}
```

### List All Tasks

```bash
curl http://localhost:8787/tasks
```

**Expected response:**
```json
[{"id":1,"title":"Learn DoSQL","completed":0,"created_at":"2024-12-15 10:30:00"}]
```

### Get a Single Task

```bash
curl http://localhost:8787/tasks/1
```

**Expected response:**
```json
{"id":1,"title":"Learn DoSQL","completed":0,"created_at":"2024-12-15 10:30:00"}
```

### Update a Task

```bash
curl -X PUT http://localhost:8787/tasks/1 \
  -H "Content-Type: application/json" \
  -d '{"completed": true}'
```

**Expected response:**
```json
{"success":true}
```

### Delete a Task

```bash
curl -X DELETE http://localhost:8787/tasks/1
```

**Expected response:**
```json
{"success":true}
```

---

## Step 8: Deploy to Production

When you are ready to go live:

```bash
npx wrangler deploy
```

Wrangler will upload your code and output your production URL:

```
Published my-tasks-api (1.23 sec)
  https://my-tasks-api.<your-subdomain>.workers.dev
```

Your API is now running globally on Cloudflare's edge network.

---

## Core Concepts

### The DB() Function

`DB()` creates a database connection with automatic migration support:

```typescript
const db = await DB('database-name', {
  migrations: { folder: '.do/migrations' },
  storage: { hot: state.storage },
});
```

| Parameter | Description |
|-----------|-------------|
| First argument | A name for the database (used for logging and identification) |
| `migrations.folder` | Path to SQL migration files |
| `storage.hot` | Durable Object storage for fast reads/writes |

### Query Methods

DoSQL provides three primary methods for database operations:

```typescript
// query() - Fetch multiple rows
const users = await db.query<User>('SELECT * FROM users');
// Returns: User[]

// queryOne() - Fetch a single row
const user = await db.queryOne<User>('SELECT * FROM users WHERE id = ?', [1]);
// Returns: User | undefined

// run() - Execute INSERT, UPDATE, DELETE
const result = await db.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
// Returns: { lastInsertRowId: number; rowsAffected: number }
```

### Transactions

Use `transaction()` to execute multiple operations atomically:

```typescript
await db.transaction(async (tx) => {
  // Debit from one account
  await tx.run(
    'UPDATE accounts SET balance = balance - ? WHERE id = ?',
    [100, 1]
  );

  // Credit to another account
  await tx.run(
    'UPDATE accounts SET balance = balance + ? WHERE id = ?',
    [100, 2]
  );

  // If either fails, both are rolled back
});
```

### Migrations

Migration files in `.do/migrations/` run automatically when the database initializes:

```
.do/migrations/
  001_create_users.sql
  002_add_email_column.sql
  003_create_orders.sql
```

**Best practices:**
- Prefix files with numbers for ordering (`001_`, `002_`)
- Use descriptive names (`001_create_users.sql`)
- Include `IF NOT EXISTS` / `IF EXISTS` for safety
- Never modify a deployed migration; create a new one instead

DoSQL tracks completed migrations in `__dosql_migrations` to ensure each runs only once.

---

## Common Patterns

### Multi-Tenant Applications

Give each tenant their own isolated database:

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Extract tenant from request (subdomain, header, or path)
    const tenantId = getTenantId(request);

    // Each tenant gets a separate Durable Object instance
    const id = env.TENANT_DB.idFromName(tenantId);
    const stub = env.TENANT_DB.get(id);

    return stub.fetch(request);
  },
};

function getTenantId(request: Request): string {
  // Option 1: From subdomain (tenant.example.com)
  const host = new URL(request.url).hostname;
  const subdomain = host.split('.')[0];
  if (subdomain && subdomain !== 'www' && subdomain !== 'api') {
    return subdomain;
  }

  // Option 2: From header (X-Tenant-ID: acme)
  const header = request.headers.get('X-Tenant-ID');
  if (header) return header;

  // Option 3: From path (/tenant/acme/tasks)
  const path = new URL(request.url).pathname;
  const match = path.match(/^\/tenant\/([^/]+)/);
  if (match) return match[1];

  return 'default';
}
```

### Type-Safe Queries

Define interfaces for your data and use generics for compile-time safety:

```typescript
interface User {
  id: number;
  name: string;
  email: string;
  created_at: string;
}

// TypeScript knows the return type
const users = await db.query<User>('SELECT * FROM users');

// IDE autocompletion works
users.forEach(user => {
  console.log(user.name);   // OK
  console.log(user.invalid); // TypeScript error
});

// queryOne returns T | undefined
const user = await db.queryOne<User>('SELECT * FROM users WHERE id = ?', [1]);
if (user) {
  console.log(user.email);
}
```

### Parameterized Queries (SQL Injection Prevention)

Always use parameter placeholders instead of string interpolation:

```typescript
// SAFE: Parameters are escaped automatically
const email = "alice@example.com";
await db.query('SELECT * FROM users WHERE email = ?', [email]);

// DANGEROUS: Never do this!
// await db.query(`SELECT * FROM users WHERE email = '${email}'`);
```

---

## Troubleshooting

### "Durable Objects require a Workers Paid plan"

Durable Objects are only available on the Workers Paid plan ($5/month). Upgrade at:
[dash.cloudflare.com](https://dash.cloudflare.com) > Workers & Pages > Plans

### "class_name 'TasksDatabase' not found"

Make sure your Durable Object class is exported:

```typescript
// The 'export' keyword is required
export class TasksDatabase implements DurableObject {
  // ...
}
```

And verify the name matches exactly in `wrangler.jsonc` (case-sensitive).

### "Port 8787 is already in use"

Another process is using the port. Options:

```bash
# Kill existing wrangler processes
pkill -f wrangler

# Or use a different port
npx wrangler dev --port 8788
```

### Migrations Not Running

1. Verify the folder exists: `ls -la .do/migrations/`
2. Check file extensions are `.sql`
3. Look for errors in the console output when the worker starts

### "TypeError: Cannot read properties of undefined"

This usually means a Durable Object binding is misconfigured. Check:

1. The `name` in `wrangler.jsonc` bindings matches your `Env` interface
2. The `class_name` matches your exported class exactly
3. Your class is exported from the main entry file

---

## Next Steps

Now that you have DoSQL running, explore these topics:

| Guide | Description |
|-------|-------------|
| [API Reference](./api-reference.md) | Complete documentation for all methods |
| [Advanced Features](./advanced.md) | Time travel, branching, CDC streaming |
| [Architecture](./architecture.md) | Storage tiers and performance optimization |
| [Troubleshooting](./TROUBLESHOOTING.md) | Common issues and solutions |
| [Deployment](./DEPLOYMENT.md) | Production deployment patterns |

---

## Getting Help

- **GitHub Issues**: [github.com/dotdo/sql/issues](https://github.com/dotdo/sql/issues)
- **Discord**: Join the DoSQL community for real-time help
- **Stack Overflow**: Tag questions with `dosql`

---

## Quick Reference

### File Structure

```
my-tasks-api/
  .do/
    migrations/
      001_create_tasks.sql
  src/
    index.ts
  package.json
  tsconfig.json
  wrangler.jsonc
```

### Essential Commands

```bash
# Development
npx wrangler dev              # Start local server
npx wrangler dev --port 8788  # Custom port

# Deployment
npx wrangler deploy           # Deploy to production
npx wrangler tail             # View live logs

# Debugging
npx wrangler whoami           # Check auth status
```

### API Summary

```typescript
// Initialize
const db = await DB(name, { migrations, storage });

// Query
const rows = await db.query<T>(sql, params);      // T[]
const row = await db.queryOne<T>(sql, params);    // T | undefined

// Mutate
const result = await db.run(sql, params);
// { lastInsertRowId: number, rowsAffected: number }

// Transaction
await db.transaction(async (tx) => {
  await tx.run(sql, params);
  await tx.query(sql, params);
});
```
