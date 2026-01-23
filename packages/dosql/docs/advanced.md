# Advanced DoSQL Usage

This guide covers advanced patterns for experienced developers building complex applications with DoSQL. It assumes familiarity with basic DoSQL operations covered in the [Getting Started](./getting-started.md) guide.

## Table of Contents

- [Advanced Query Patterns](#advanced-query-patterns)
  - [Common Table Expressions (CTEs)](#common-table-expressions-ctes)
  - [Window Functions](#window-functions)
  - [Recursive Queries](#recursive-queries)
  - [Hybrid Search (Text + Vector)](#hybrid-search-text--vector)
  - [Time Travel Queries](#time-travel-queries)
  - [Virtual Tables](#virtual-tables)
- [Stored Procedures and Triggers](#stored-procedures-and-triggers)
  - [ESM Stored Procedures](#esm-stored-procedures)
  - [SQL Triggers](#sql-triggers)
  - [Programmatic Triggers](#programmatic-triggers)
  - [Trigger Execution Order](#trigger-execution-order)
- [CDC Streaming Patterns](#cdc-streaming-patterns)
  - [Basic CDC Subscription](#basic-cdc-subscription)
  - [Filtered Subscriptions](#filtered-subscriptions)
  - [Replication Slots](#replication-slots)
  - [Lakehouse Streaming](#lakehouse-streaming)
  - [Error Recovery Patterns](#error-recovery-patterns)
- [Sharding and Scaling](#sharding-and-scaling)
  - [VSchema Configuration](#vschema-configuration)
  - [Vindex Types](#vindex-types)
  - [Query Routing](#query-routing)
  - [Cross-Shard Transactions](#cross-shard-transactions)
  - [Replica Configuration](#replica-configuration)

---

## Advanced Query Patterns

### Common Table Expressions (CTEs)

CTEs provide readable, modular queries for complex data transformations. They act as temporary named result sets that exist only for the duration of the query.

```typescript
import { DB } from 'dosql';

const db = await DB('analytics');

// Basic CTE for hierarchical aggregation
const departmentStats = await db.query(`
  WITH dept_salaries AS (
    SELECT
      department_id,
      COUNT(*) as employee_count,
      AVG(salary) as avg_salary,
      SUM(salary) as total_salary
    FROM employees
    GROUP BY department_id
  ),
  dept_ranks AS (
    SELECT
      d.name as department_name,
      ds.*,
      RANK() OVER (ORDER BY ds.avg_salary DESC) as salary_rank
    FROM dept_salaries ds
    JOIN departments d ON d.id = ds.department_id
  )
  SELECT * FROM dept_ranks WHERE salary_rank <= 5
`);

// Multiple CTEs for complex reporting
const salesReport = await db.query(`
  WITH monthly_sales AS (
    SELECT
      strftime('%Y-%m', order_date) as month,
      product_id,
      SUM(quantity) as units_sold,
      SUM(total) as revenue
    FROM orders
    WHERE order_date >= date('now', '-12 months')
    GROUP BY month, product_id
  ),
  product_trends AS (
    SELECT
      product_id,
      month,
      revenue,
      LAG(revenue) OVER (PARTITION BY product_id ORDER BY month) as prev_revenue
    FROM monthly_sales
  ),
  growth_rates AS (
    SELECT
      p.name as product_name,
      pt.month,
      pt.revenue,
      CASE
        WHEN pt.prev_revenue > 0
        THEN ROUND((pt.revenue - pt.prev_revenue) / pt.prev_revenue * 100, 2)
        ELSE NULL
      END as growth_pct
    FROM product_trends pt
    JOIN products p ON p.id = pt.product_id
  )
  SELECT * FROM growth_rates ORDER BY month DESC, revenue DESC
`);
```

### Window Functions

Window functions enable sophisticated analytics without subqueries. They perform calculations across a set of rows related to the current row.

```typescript
// Running totals and moving averages
const orderAnalytics = await db.query(`
  SELECT
    order_id,
    customer_id,
    order_date,
    total,
    SUM(total) OVER (
      PARTITION BY customer_id
      ORDER BY order_date
      ROWS UNBOUNDED PRECEDING
    ) as customer_running_total,
    AVG(total) OVER (
      PARTITION BY customer_id
      ORDER BY order_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3_orders,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY order_date
    ) as order_sequence
  FROM orders
  WHERE order_date >= date('now', '-90 days')
`);

// Percentile rankings and distributions
const performanceRanking = await db.query(`
  SELECT
    employee_id,
    name,
    department_id,
    sales_total,
    PERCENT_RANK() OVER (ORDER BY sales_total) as overall_percentile,
    PERCENT_RANK() OVER (
      PARTITION BY department_id
      ORDER BY sales_total
    ) as dept_percentile,
    NTILE(4) OVER (ORDER BY sales_total DESC) as performance_quartile
  FROM employee_performance
  WHERE period = '2024-Q4'
`);

// Gap and island analysis for session detection
const sessionAnalysis = await db.query(`
  WITH event_gaps AS (
    SELECT
      user_id,
      event_time,
      LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) as prev_event,
      CASE
        WHEN julianday(event_time) - julianday(LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time)) > 0.0208
        THEN 1
        ELSE 0
      END as new_session
    FROM user_events
  ),
  session_markers AS (
    SELECT
      *,
      SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_time) as session_id
    FROM event_gaps
  )
  SELECT
    user_id,
    session_id,
    MIN(event_time) as session_start,
    MAX(event_time) as session_end,
    COUNT(*) as event_count
  FROM session_markers
  GROUP BY user_id, session_id
`);
```

> **Note**: The session gap threshold of `0.0208` days equals approximately 30 minutes (0.0208 * 24 * 60 = 30 minutes).

### Recursive Queries

Recursive CTEs handle hierarchical data like org charts, category trees, and graph traversal. They consist of a base case and a recursive case joined with `UNION ALL`.

```typescript
// Organizational hierarchy traversal
const orgChart = await db.query(`
  WITH RECURSIVE org_tree AS (
    -- Base case: top-level employees (no manager)
    SELECT
      id,
      name,
      manager_id,
      title,
      1 as level,
      name as path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive case: employees with managers
    SELECT
      e.id,
      e.name,
      e.manager_id,
      e.title,
      ot.level + 1,
      ot.path || ' > ' || e.name
    FROM employees e
    INNER JOIN org_tree ot ON e.manager_id = ot.id
    WHERE ot.level < 10  -- Prevent infinite recursion
  )
  SELECT * FROM org_tree ORDER BY path
`);

// Category tree with aggregated product metrics
const categoryTree = await db.query(`
  WITH RECURSIVE category_tree AS (
    SELECT
      id,
      name,
      parent_id,
      0 as depth,
      CAST(id AS TEXT) as path
    FROM categories
    WHERE parent_id IS NULL

    UNION ALL

    SELECT
      c.id,
      c.name,
      c.parent_id,
      ct.depth + 1,
      ct.path || '/' || CAST(c.id AS TEXT)
    FROM categories c
    INNER JOIN category_tree ct ON c.parent_id = ct.id
  ),
  category_products AS (
    SELECT
      ct.id,
      ct.name,
      ct.depth,
      ct.path,
      COUNT(p.id) as direct_products,
      SUM(p.price) as total_value
    FROM category_tree ct
    LEFT JOIN products p ON p.category_id = ct.id
    GROUP BY ct.id, ct.name, ct.depth, ct.path
  )
  SELECT * FROM category_products ORDER BY path
`);

// Bill of materials explosion with cumulative quantities
const bomExplosion = await db.query(`
  WITH RECURSIVE bom AS (
    SELECT
      component_id,
      quantity,
      1 as level,
      CAST(component_id AS TEXT) as assembly_path
    FROM bill_of_materials
    WHERE parent_id = :productId

    UNION ALL

    SELECT
      b.component_id,
      b.quantity * bom.quantity,
      bom.level + 1,
      bom.assembly_path || ' -> ' || CAST(b.component_id AS TEXT)
    FROM bill_of_materials b
    INNER JOIN bom ON b.parent_id = bom.component_id
    WHERE bom.level < 20
  )
  SELECT
    c.name as component_name,
    c.sku,
    SUM(bom.quantity) as total_required,
    c.unit_cost,
    SUM(bom.quantity) * c.unit_cost as total_cost
  FROM bom
  JOIN components c ON c.id = bom.component_id
  GROUP BY bom.component_id
  ORDER BY total_cost DESC
`, { productId: 123 });
```

> **Important**: Always include a recursion depth limit (e.g., `WHERE level < 10`) to prevent infinite loops in malformed hierarchical data.

### Hybrid Search (Text + Vector)

Combine full-text search with vector similarity for semantic search applications. This pattern is useful when you want both keyword matching and semantic understanding.

```typescript
import { DB } from 'dosql';

const db = await DB('documents');

// Create table with both FTS and vector capabilities
await db.run(`
  CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    embedding BLOB,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  );

  -- Full-text search index
  CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
    title, content,
    content='documents',
    content_rowid='id'
  );

  -- Vector index (HNSW) for semantic similarity
  CREATE INDEX IF NOT EXISTS idx_documents_embedding ON documents
  USING VECTOR (embedding)
  WITH (dimensions = 1536, metric = 'cosine', m = 16, ef_construction = 200);
`);

// Hybrid search combining keyword and semantic similarity
async function hybridSearch(
  query: string,
  embedding: Float32Array,
  options: { keywordWeight?: number; semanticWeight?: number; limit?: number } = {}
) {
  const { keywordWeight = 0.4, semanticWeight = 0.6, limit = 10 } = options;

  const results = await db.query(`
    WITH keyword_matches AS (
      SELECT
        rowid as id,
        bm25(documents_fts) as keyword_score
      FROM documents_fts
      WHERE documents_fts MATCH :query
      LIMIT 100
    ),
    semantic_matches AS (
      SELECT
        id,
        1 - vector_distance(embedding, :embedding) as semantic_score
      FROM documents
      WHERE embedding IS NOT NULL
      ORDER BY vector_distance(embedding, :embedding) ASC
      LIMIT 100
    ),
    combined AS (
      SELECT
        d.id,
        d.title,
        d.content,
        COALESCE(km.keyword_score, 0) * :keywordWeight +
        COALESCE(sm.semantic_score, 0) * :semanticWeight as combined_score
      FROM documents d
      LEFT JOIN keyword_matches km ON km.id = d.id
      LEFT JOIN semantic_matches sm ON sm.id = d.id
      WHERE km.id IS NOT NULL OR sm.id IS NOT NULL
    )
    SELECT * FROM combined
    ORDER BY combined_score DESC
    LIMIT :limit
  `, {
    query,
    embedding: new Uint8Array(embedding.buffer),
    keywordWeight,
    semanticWeight,
    limit,
  });

  return results;
}

// Filtered vector search with metadata constraints
async function searchWithFilters(
  embedding: Float32Array,
  filters: { category?: string; dateRange?: [string, string] }
) {
  let whereClause = 'WHERE embedding IS NOT NULL';
  const params: Record<string, unknown> = {
    embedding: new Uint8Array(embedding.buffer),
  };

  if (filters.category) {
    whereClause += ' AND category = :category';
    params.category = filters.category;
  }

  if (filters.dateRange) {
    whereClause += ' AND created_at BETWEEN :startDate AND :endDate';
    params.startDate = filters.dateRange[0];
    params.endDate = filters.dateRange[1];
  }

  return db.query(`
    SELECT
      id, title, content, category, created_at,
      vector_distance(embedding, :embedding) as distance
    FROM documents
    ${whereClause}
    ORDER BY distance ASC
    LIMIT 20
  `, params);
}
```

### Time Travel Queries

Query data at any point in time using DoSQL's time travel capabilities. This is useful for auditing, debugging, and historical analysis.

```typescript
import { DB } from 'dosql';
import {
  createTimeTravelSession,
  timestamp,
} from 'dosql/timetravel';

const db = await DB('analytics');

// Query at a specific timestamp
const historicalData = await db.query(`
  SELECT * FROM accounts
  FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00'
  WHERE balance > 10000
`);

// Query at a specific LSN (Log Sequence Number)
const atLsn = await db.query(`
  SELECT * FROM transactions
  FOR SYSTEM_TIME AS OF LSN 12345
  WHERE account_id = 42
`);

// Query at a named snapshot
const atSnapshot = await db.query(`
  SELECT * FROM inventory
  FOR SYSTEM_TIME AS OF SNAPSHOT 'main@5'
`);

// Programmatic time travel session for consistent multi-query audits
async function auditReport(asOfDate: Date) {
  const session = await createTimeTravelSession(db, {
    asOf: timestamp(asOfDate),
    scope: 'local',
  });

  try {
    // All queries within this session see data as of asOfDate
    const accounts = await session.query('SELECT * FROM accounts');
    const transactions = await session.query('SELECT * FROM transactions');
    const balances = await session.query(`
      SELECT
        a.id,
        a.name,
        a.balance as reported_balance,
        COALESCE(SUM(t.amount), 0) as calculated_balance
      FROM accounts a
      LEFT JOIN transactions t ON t.account_id = a.id
      GROUP BY a.id
    `);

    return { accounts, transactions, balances };
  } finally {
    await session.close();
  }
}

// Time range queries for viewing change history
const changeHistory = await db.query(`
  SELECT
    *,
    _dosql_version,
    _dosql_valid_from,
    _dosql_valid_to
  FROM accounts
  FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2024-06-01'
  WHERE id = 42
  ORDER BY _dosql_valid_from
`);

// Compare data between two points in time
async function comparePeriods(
  table: string,
  startDate: string,
  endDate: string
) {
  return db.query(`
    WITH start_state AS (
      SELECT * FROM ${table}
      FOR SYSTEM_TIME AS OF TIMESTAMP :startDate
    ),
    end_state AS (
      SELECT * FROM ${table}
      FOR SYSTEM_TIME AS OF TIMESTAMP :endDate
    )
    SELECT
      COALESCE(s.id, e.id) as id,
      s.balance as start_balance,
      e.balance as end_balance,
      COALESCE(e.balance, 0) - COALESCE(s.balance, 0) as change
    FROM start_state s
    FULL OUTER JOIN end_state e ON s.id = e.id
    WHERE s.balance != e.balance OR s.id IS NULL OR e.id IS NULL
  `, { startDate, endDate });
}
```

### Virtual Tables

Query external data sources directly using SQL with virtual tables. DoSQL supports URLs, APIs, and cloud storage as queryable tables.

```typescript
import { DB } from 'dosql';
import { createVirtualTableRegistry, createURLVirtualTable } from 'dosql/virtual';

const db = await DB('federation');
const registry = createVirtualTableRegistry();

// Register an API endpoint as a virtual table
registry.register('github_repos', createURLVirtualTable({
  url: 'https://api.github.com/users/{owner}/repos',
  format: 'json',
  urlParams: ['owner'],
  transform: (data) => data.map((repo: any) => ({
    id: repo.id,
    name: repo.name,
    full_name: repo.full_name,
    stars: repo.stargazers_count,
    language: repo.language,
    updated_at: repo.updated_at,
  })),
  cache: { ttl: 300000 }, // Cache for 5 minutes
}));

// Query the virtual table like any other table
const repos = await db.query(`
  SELECT name, stars, language
  FROM github_repos('octocat')
  WHERE stars > 100
  ORDER BY stars DESC
`);

// Join virtual table with local data
const enrichedData = await db.query(`
  SELECT
    u.id,
    u.username,
    u.github_handle,
    gr.name as repo_name,
    gr.stars
  FROM users u
  CROSS JOIN LATERAL github_repos(u.github_handle) gr
  WHERE u.active = true
  ORDER BY gr.stars DESC
`);

// Query Parquet files directly from R2
const salesData = await db.query(`
  SELECT
    region,
    SUM(revenue) as total_revenue,
    COUNT(*) as transaction_count
  FROM 'r2://analytics-bucket/sales/year=2024/*.parquet'
  WHERE month >= 10
  GROUP BY region
`);

// Query CSV with custom options
const importData = await db.query(`
  SELECT * FROM 'https://data.example.com/export.csv'
  WITH (
    headers = true,
    delimiter = ',',
    quote = '"',
    encoding = 'utf-8'
  )
  WHERE status = 'active'
`);
```

---

## Stored Procedures and Triggers

### ESM Stored Procedures

DoSQL supports ESM-based stored procedures for complex business logic with full TypeScript support. Procedures run in sandboxed V8 isolates for security.

```typescript
import {
  createProcedureRegistry,
  createProcedureExecutor,
  procedure,
  type ProcedureContext
} from 'dosql/proc';

// Define a procedure using the builder pattern
const transferFunds = procedure('transfer_funds')
  .input({
    fromAccountId: 'number',
    toAccountId: 'number',
    amount: 'number',
    description: 'string?', // Optional parameter
  })
  .output({
    transactionId: 'number',
    fromBalance: 'number',
    toBalance: 'number',
  })
  .handler(async (ctx: ProcedureContext, input) => {
    const { db } = ctx;

    return await db.transaction(async (tx) => {
      // Validate source account and lock the row
      const fromAccount = await tx.queryOne(
        'SELECT id, balance FROM accounts WHERE id = ? FOR UPDATE',
        [input.fromAccountId]
      );

      if (!fromAccount) {
        throw new Error(`Source account ${input.fromAccountId} not found`);
      }

      if (fromAccount.balance < input.amount) {
        throw new Error(`Insufficient funds: ${fromAccount.balance} < ${input.amount}`);
      }

      // Validate destination account
      const toAccount = await tx.queryOne(
        'SELECT id, balance FROM accounts WHERE id = ?',
        [input.toAccountId]
      );

      if (!toAccount) {
        throw new Error(`Destination account ${input.toAccountId} not found`);
      }

      // Perform the transfer
      await tx.run(
        'UPDATE accounts SET balance = balance - ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
        [input.amount, input.fromAccountId]
      );

      await tx.run(
        'UPDATE accounts SET balance = balance + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
        [input.amount, input.toAccountId]
      );

      // Record the transaction
      const result = await tx.run(
        `INSERT INTO transactions (from_account_id, to_account_id, amount, description, created_at)
         VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
        [input.fromAccountId, input.toAccountId, input.amount, input.description ?? null]
      );

      // Get updated balances for the response
      const updatedFrom = await tx.queryOne(
        'SELECT balance FROM accounts WHERE id = ?',
        [input.fromAccountId]
      );
      const updatedTo = await tx.queryOne(
        'SELECT balance FROM accounts WHERE id = ?',
        [input.toAccountId]
      );

      return {
        transactionId: result.lastInsertRowId,
        fromBalance: updatedFrom!.balance,
        toBalance: updatedTo!.balance,
      };
    });
  });

// Register and execute procedures
const registry = createProcedureRegistry();
registry.register(transferFunds);

const executor = createProcedureExecutor(db, registry);

const result = await executor.execute('transfer_funds', {
  fromAccountId: 1,
  toAccountId: 2,
  amount: 500,
  description: 'Payment for services',
});

console.log(result);
// { transactionId: 123, fromBalance: 500, toBalance: 1500 }
```

#### Functional Procedure Definitions

For simpler syntax, use the functional API:

```typescript
import { defineProcedures, withValidation, withRetry } from 'dosql/proc';

const procedures = defineProcedures({
  // Simple procedure with no validation
  getUserOrders: async ({ db }, userId: number) => {
    return db.query(
      'SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC',
      [userId]
    );
  },

  // Procedure with input validation
  createOrder: withValidation(
    async ({ db }, order: { userId: number; items: Array<{ productId: number; quantity: number }> }) => {
      return db.transaction(async (tx) => {
        const orderResult = await tx.run(
          'INSERT INTO orders (user_id, status, created_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
          [order.userId, 'pending']
        );
        const orderId = orderResult.lastInsertRowId;

        for (const item of order.items) {
          await tx.run(
            'INSERT INTO order_items (order_id, product_id, quantity) VALUES (?, ?, ?)',
            [orderId, item.productId, item.quantity]
          );
        }

        return { orderId };
      });
    },
    {
      userId: (v) => typeof v === 'number' && v > 0,
      items: (v) => Array.isArray(v) && v.length > 0,
    }
  ),

  // Procedure with automatic retry on transient failures
  syncExternalData: withRetry(
    async ({ db, env }, sourceId: string) => {
      const response = await fetch(`${env.EXTERNAL_API}/data/${sourceId}`);
      const data = await response.json();

      await db.run(
        'INSERT OR REPLACE INTO external_data (source_id, data, synced_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
        [sourceId, JSON.stringify(data)]
      );

      return { synced: true, recordCount: data.length };
    },
    { maxRetries: 3, baseDelayMs: 1000 }
  ),
});

// Execute procedures with type inference
const orders = await procedures.getUserOrders(42);
const newOrder = await procedures.createOrder({
  userId: 42,
  items: [
    { productId: 1, quantity: 2 },
    { productId: 3, quantity: 1 },
  ],
});
```

#### SQL Procedure Syntax

Create procedures using SQL syntax with embedded ESM:

```sql
-- Create a procedure with embedded ESM module
CREATE PROCEDURE calculate_customer_stats AS MODULE $$
  export default async ({ db }, customerId) => {
    const stats = await db.queryOne(`
      SELECT
        COUNT(*) as order_count,
        SUM(total) as total_spent,
        AVG(total) as avg_order,
        MAX(created_at) as last_order
      FROM orders
      WHERE customer_id = ?
    `, [customerId]);

    return {
      customerId,
      orderCount: stats.order_count,
      totalSpent: stats.total_spent,
      avgOrder: stats.avg_order,
      lastOrder: stats.last_order,
      tier: stats.total_spent > 10000 ? 'gold' :
            stats.total_spent > 1000 ? 'silver' : 'bronze'
    };
  }
$$;

-- Call the procedure with positional argument
CALL calculate_customer_stats(42);

-- Call with named parameter
CALL calculate_customer_stats(customerId => 42);
```

### SQL Triggers

DoSQL supports SQLite-compatible CREATE TRIGGER syntax for database-level automation.

```typescript
import { DB } from 'dosql';

const db = await DB('app');

// Audit trigger: log all changes to users table
await db.run(`
  CREATE TRIGGER audit_user_changes
  AFTER UPDATE ON users
  FOR EACH ROW
  BEGIN
    INSERT INTO audit_log (
      table_name,
      row_id,
      action,
      old_values,
      new_values,
      changed_at,
      changed_by
    )
    VALUES (
      'users',
      NEW.id,
      'UPDATE',
      json_object('name', OLD.name, 'email', OLD.email, 'role', OLD.role),
      json_object('name', NEW.name, 'email', NEW.email, 'role', NEW.role),
      CURRENT_TIMESTAMP,
      NEW.updated_by
    );
  END
`);

// Conditional trigger with WHEN clause
await db.run(`
  CREATE TRIGGER notify_large_orders
  AFTER INSERT ON orders
  FOR EACH ROW
  WHEN NEW.total > 1000
  BEGIN
    INSERT INTO notifications (type, payload, created_at)
    VALUES (
      'large_order',
      json_object('order_id', NEW.id, 'customer_id', NEW.customer_id, 'total', NEW.total),
      CURRENT_TIMESTAMP
    );
  END
`);

// BEFORE trigger for validation
await db.run(`
  CREATE TRIGGER validate_inventory
  BEFORE UPDATE ON products
  FOR EACH ROW
  WHEN NEW.stock < 0
  BEGIN
    SELECT RAISE(ABORT, 'Stock cannot be negative');
  END
`);

// INSTEAD OF trigger for updatable views
await db.run(`
  CREATE VIEW active_users AS
  SELECT id, name, email FROM users WHERE deleted_at IS NULL;

  CREATE TRIGGER delete_active_user
  INSTEAD OF DELETE ON active_users
  FOR EACH ROW
  BEGIN
    UPDATE users SET deleted_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
  END
`);

// Trigger for UPDATE OF specific columns only
await db.run(`
  CREATE TRIGGER track_price_changes
  AFTER UPDATE OF price ON products
  FOR EACH ROW
  WHEN OLD.price != NEW.price
  BEGIN
    INSERT INTO price_history (product_id, old_price, new_price, changed_at)
    VALUES (NEW.id, OLD.price, NEW.price, CURRENT_TIMESTAMP);
  END
`);
```

### Programmatic Triggers

For complex business logic, use TypeScript triggers with full access to the DoSQL context:

```typescript
import {
  createTriggerRegistry,
  createTriggerExecutor,
  type TriggerDefinition,
  type TriggerContext
} from 'dosql/triggers';

// Define a trigger with full type safety
const auditTrigger: TriggerDefinition<{ id: number; name: string; email: string }> = {
  name: 'audit_user_changes',
  table: 'users',
  timing: 'after',
  events: ['insert', 'update', 'delete'],
  priority: 100,

  async handler(ctx: TriggerContext<{ id: number; name: string; email: string }>) {
    const { event, old, new: newRow, db, meta } = ctx;

    await db.run(
      `INSERT INTO audit_log (table_name, row_id, action, old_values, new_values, changed_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        ctx.table,
        newRow?.id ?? old?.id,
        event.toUpperCase(),
        old ? JSON.stringify(old) : null,
        newRow ? JSON.stringify(newRow) : null,
        new Date().toISOString(),
      ]
    );
  },
};

// Validation trigger that modifies data before write
const validationTrigger: TriggerDefinition<{ email: string; role: string }> = {
  name: 'validate_user_email',
  table: 'users',
  timing: 'before',
  events: ['insert', 'update'],
  priority: 10, // Run early in the trigger chain

  handler(ctx) {
    const row = ctx.new!;

    // Email format validation
    if (row.email && !row.email.match(/^[^\s@]+@[^\s@]+\.[^\s@]+$/)) {
      throw new Error('Invalid email format');
    }

    // Role validation
    const validRoles = ['admin', 'user', 'guest'];
    if (row.role && !validRoles.includes(row.role)) {
      throw new Error(`Invalid role: ${row.role}. Must be one of: ${validRoles.join(', ')}`);
    }

    // Return modified row (normalizes email to lowercase)
    return {
      ...row,
      email: row.email?.toLowerCase(),
    };
  },
};

// Register triggers
const registry = createTriggerRegistry();
registry.register(auditTrigger);
registry.register(validationTrigger);

const executor = createTriggerExecutor(db, registry);

// Triggers fire automatically on database operations
// Or execute manually for testing:
const beforeResult = await executor.executeBefore(
  'users',
  'insert',
  undefined,
  { id: 1, name: 'Alice', email: 'ALICE@EXAMPLE.COM', role: 'user' }
);

console.log(beforeResult.row);
// { id: 1, name: 'Alice', email: 'alice@example.com', role: 'user' }
```

### Trigger Execution Order

Understanding trigger execution order is critical for complex applications. Triggers execute in priority order (lower numbers first).

```typescript
import { createTriggerRegistry, type TriggerDefinition } from 'dosql/triggers';

const registry = createTriggerRegistry();

// Triggers execute in priority order (lower numbers first)
const triggers: TriggerDefinition[] = [
  {
    name: 'validation',
    table: 'orders',
    timing: 'before',
    events: ['insert'],
    priority: 10, // Runs first
    handler: (ctx) => {
      if (ctx.new!.total < 0) throw new Error('Invalid total');
    },
  },
  {
    name: 'normalization',
    table: 'orders',
    timing: 'before',
    events: ['insert'],
    priority: 20, // Runs second
    handler: (ctx) => ({
      ...ctx.new!,
      status: ctx.new!.status || 'pending',
      created_at: new Date().toISOString(),
    }),
  },
  {
    name: 'audit_log',
    table: 'orders',
    timing: 'after',
    events: ['insert', 'update', 'delete'],
    priority: 100, // Runs after the database operation
    handler: async (ctx) => {
      await ctx.db.run('INSERT INTO audit_log ...');
    },
  },
  {
    name: 'notifications',
    table: 'orders',
    timing: 'after',
    events: ['insert'],
    priority: 200, // Runs last
    handler: async (ctx) => {
      await sendNotification(ctx.new!.customer_id, 'New order created');
    },
  },
];

triggers.forEach(t => registry.register(t));

// Execution flow for INSERT:
// 1. BEFORE triggers (priority order): validation (10) -> normalization (20)
// 2. Actual INSERT operation
// 3. AFTER triggers (priority order): audit_log (100) -> notifications (200)
```

> **Note**: BEFORE triggers can modify the row by returning a new object. AFTER triggers run after the operation commits and cannot modify data but are ideal for side effects like notifications.

---

## CDC Streaming Patterns

Change Data Capture (CDC) enables real-time streaming of database changes for event-driven architectures.

### Basic CDC Subscription

```typescript
import { createCDC, createCDCSubscription } from 'dosql/cdc';

const db = await DB('app', { wal: true });
const cdc = createCDC(db.backend);

// Subscribe to all changes starting from the beginning
for await (const entry of cdc.subscribe(0n)) {
  console.log('Change:', {
    operation: entry.op,
    table: entry.table,
    lsn: entry.lsn,
    timestamp: entry.timestamp,
  });
}

// Subscribe with typed events and automatic decoding
interface User {
  id: number;
  name: string;
  email: string;
}

const subscription = createCDCSubscription(db.backend, {
  fromLSN: 0n,
  pollInterval: 100, // Check for new entries every 100ms
  batchSize: 100,    // Process up to 100 entries per batch
});

for await (const event of subscription.subscribeChanges<User>(0n, undefined, JSON.parse)) {
  if (event.type === 'insert') {
    console.log('New user:', event.data);
  } else if (event.type === 'update') {
    console.log('User updated:', event.oldData, '->', event.data);
  } else if (event.type === 'delete') {
    console.log('User deleted:', event.oldData);
  }
}
```

### Filtered Subscriptions

Filter CDC events by table, operation, or custom predicates:

```typescript
import { createCDCSubscription, type CDCFilter } from 'dosql/cdc';

// Filter by tables and operations
const subscription = createCDCSubscription(db.backend, {
  fromLSN: 0n,
  filter: {
    tables: ['orders', 'order_items'],
    operations: ['INSERT', 'UPDATE'],
  },
});

// Custom predicate for complex filtering
const highValueFilter: CDCFilter = {
  tables: ['orders'],
  operations: ['INSERT'],
  predicate: (entry) => {
    const data = JSON.parse(new TextDecoder().decode(entry.after!));
    return data.total > 1000;
  },
};

const highValueSubscription = createCDCSubscription(db.backend, {
  fromLSN: 0n,
  filter: highValueFilter,
});

// Multiple consumers with different filters
async function startCDCConsumers(backend: Backend) {
  // Analytics: track all new records
  const analyticsConsumer = createCDCSubscription(backend, {
    fromLSN: 0n,
    filter: { operations: ['INSERT'] },
  });

  // Audit: track changes to sensitive tables
  const auditConsumer = createCDCSubscription(backend, {
    fromLSN: 0n,
    filter: {
      tables: ['users', 'payments', 'permissions'],
      operations: ['INSERT', 'UPDATE', 'DELETE'],
    },
  });

  // Cache invalidation: invalidate on updates and deletes
  const cacheConsumer = createCDCSubscription(backend, {
    fromLSN: 0n,
    filter: { operations: ['UPDATE', 'DELETE'] },
  });

  return { analyticsConsumer, auditConsumer, cacheConsumer };
}
```

### Replication Slots

Replication slots provide durable position tracking for reliable CDC consumption. They ensure no events are missed even if the consumer restarts.

```typescript
import { createCDC, createReplicationSlotManager } from 'dosql/cdc';

const cdc = createCDC(db.backend);

// Create a replication slot for persistent position tracking
await cdc.slots.createSlot('analytics-service', 0n, {
  tables: ['events', 'metrics'],
});

// Subscribe from slot position (automatically resumes where it left off)
const subscription = await cdc.slots.subscribeFromSlot('analytics-service');

let lastProcessedLSN = 0n;
let processedCount = 0;

for await (const event of subscription.subscribeChanges(0n)) {
  try {
    // Process the event
    await processEvent(event);

    // Update slot position periodically (every 100 events or 5 seconds)
    lastProcessedLSN = event.lsn;
    processedCount++;

    if (processedCount % 100 === 0) {
      await cdc.slots.updateSlot('analytics-service', lastProcessedLSN);
    }
  } catch (error) {
    console.error('Error processing event:', error);
    // Slot position not updated - will retry from last acknowledged LSN
    break;
  }
}

// List all active replication slots
const slots = await cdc.slots.listSlots();
console.log('Active slots:', slots.map(s => ({
  name: s.name,
  acknowledgedLSN: s.acknowledgedLSN,
  lastUsed: s.lastUsedAt,
})));

// Delete slot when consumer is decommissioned
await cdc.slots.deleteSlot('analytics-service');
```

### Lakehouse Streaming

Stream CDC events to a lakehouse for analytics and long-term storage:

```typescript
import {
  createLakehouseStreamer,
  type LakehouseStreamConfig,
  type CDCBatch
} from 'dosql/cdc';

const streamerConfig: LakehouseStreamConfig = {
  lakehouseUrl: 'wss://lakehouse.example.com/ingest',
  sourceDoId: 'tenant-123-orders',
  sourceShardName: 'orders-shard-0',
  maxBatchSize: 1000,
  maxBatchAge: 5000, // Flush batch after 5 seconds even if not full
  retry: {
    maxAttempts: 5,
    initialDelayMs: 100,
    maxDelayMs: 30000,
    backoffMultiplier: 2,
  },
  heartbeatInterval: 30000,
  exactlyOnce: true, // Enable deduplication
};

const streamer = createLakehouseStreamer({
  cdc: cdcSubscription,
  config: streamerConfig,
  onBatchSent: (batch: CDCBatch) => {
    console.log(`Batch ${batch.batchId} sent: ${batch.events.length} events`);
  },
  onAck: (ack) => {
    console.log(`Acknowledged LSN: ${ack.lsn}`);
  },
  onNack: (nack) => {
    console.error(`Batch rejected: ${nack.reason}`);
  },
  onBackpressure: (signal) => {
    if (signal.type === 'pause') {
      console.warn('Backpressure: pausing ingestion');
    } else if (signal.type === 'resume') {
      console.log('Backpressure: resuming ingestion');
    }
  },
});

// Start streaming
await streamer.start();

// Monitor status
setInterval(async () => {
  const status = await streamer.getStatus();
  console.log('Streamer status:', {
    state: status.state,
    lastAckLSN: status.lastAckLSN,
    pendingBatches: status.pendingBatches,
    totalEntriesSent: status.totalEntriesSent,
  });
}, 10000);

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('Shutting down streamer...');
  await streamer.stop();
  console.log('Streamer stopped');
});
```

### Error Recovery Patterns

Robust CDC consumers require comprehensive error handling:

```typescript
import {
  createCDC,
  CDCError,
  CDCErrorCode,
  type ChangeEvent
} from 'dosql/cdc';

// Retry configuration with exponential backoff
interface RetryConfig {
  maxRetries: number;
  baseDelayMs: number;
  maxDelayMs: number;
  jitterFactor: number;
}

const defaultRetryConfig: RetryConfig = {
  maxRetries: 5,
  baseDelayMs: 1000,
  maxDelayMs: 30000,
  jitterFactor: 0.2,
};

function calculateBackoff(attempt: number, config: RetryConfig): number {
  const exponentialDelay = Math.min(
    config.baseDelayMs * Math.pow(2, attempt),
    config.maxDelayMs
  );
  const jitter = exponentialDelay * config.jitterFactor * Math.random();
  return exponentialDelay + jitter;
}

async function createResilientCDCConsumer(
  backend: Backend,
  slotName: string,
  processor: (event: ChangeEvent) => Promise<void>,
  config: RetryConfig = defaultRetryConfig
) {
  const cdc = createCDC(backend);
  let retryCount = 0;
  let isShuttingDown = false;

  async function connect(): Promise<void> {
    while (!isShuttingDown && retryCount < config.maxRetries) {
      try {
        const subscription = await cdc.slots.subscribeFromSlot(slotName);
        retryCount = 0; // Reset on successful connection

        for await (const event of subscription.subscribeChanges(0n)) {
          if (isShuttingDown) break;

          await processWithRetry(event, processor, config);
          await cdc.slots.updateSlot(slotName, event.lsn);
        }
      } catch (error) {
        if (error instanceof CDCError && error.isRetryable()) {
          retryCount++;
          const delay = calculateBackoff(retryCount, config);
          console.error(
            `CDC connection failed (attempt ${retryCount}/${config.maxRetries}), retrying in ${delay}ms:`,
            error.message
          );
          await sleep(delay);
        } else {
          throw error; // Non-retryable error
        }
      }
    }

    if (!isShuttingDown) {
      throw new Error(`CDC connection failed after ${config.maxRetries} retries`);
    }
  }

  return {
    start: connect,
    stop: () => { isShuttingDown = true; },
  };
}

// Dead letter queue for events that fail processing
class CDCDeadLetterQueue {
  private queue: Array<{
    event: ChangeEvent;
    error: string;
    attempts: number;
    lastAttempt: Date;
  }> = [];

  add(event: ChangeEvent, error: Error, attempts: number): void {
    this.queue.push({
      event,
      error: error.message,
      attempts,
      lastAttempt: new Date(),
    });
  }

  async reprocess(
    processor: (event: ChangeEvent) => Promise<void>
  ): Promise<{ processed: number; failed: number }> {
    const results = { processed: 0, failed: 0 };
    const remaining = [];

    for (const item of this.queue) {
      try {
        await processor(item.event);
        results.processed++;
      } catch (error) {
        item.attempts++;
        item.lastAttempt = new Date();
        item.error = String(error);
        remaining.push(item);
        results.failed++;
      }
    }

    this.queue = remaining;
    return results;
  }

  getAll() {
    return [...this.queue];
  }

  clear() {
    this.queue = [];
  }
}

// Circuit breaker pattern for downstream services
enum CircuitState {
  CLOSED = 'CLOSED',
  OPEN = 'OPEN',
  HALF_OPEN = 'HALF_OPEN',
}

class CDCCircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED;
  private failures = 0;
  private lastFailureTime = 0;
  private halfOpenAttempts = 0;

  constructor(
    private readonly failureThreshold: number,
    private readonly resetTimeoutMs: number,
    private readonly halfOpenMaxAttempts: number
  ) {}

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    if (this.state === CircuitState.OPEN) {
      if (Date.now() - this.lastFailureTime >= this.resetTimeoutMs) {
        this.state = CircuitState.HALF_OPEN;
        this.halfOpenAttempts = 0;
      } else {
        throw new Error('Circuit breaker is OPEN');
      }
    }

    try {
      const result = await operation();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  private onSuccess(): void {
    if (this.state === CircuitState.HALF_OPEN) {
      this.halfOpenAttempts++;
      if (this.halfOpenAttempts >= this.halfOpenMaxAttempts) {
        this.state = CircuitState.CLOSED;
        this.failures = 0;
      }
    } else {
      this.failures = 0;
    }
  }

  private onFailure(): void {
    this.failures++;
    this.lastFailureTime = Date.now();

    if (this.state === CircuitState.HALF_OPEN || this.failures >= this.failureThreshold) {
      this.state = CircuitState.OPEN;
    }
  }

  getState(): CircuitState {
    return this.state;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
```

---

## Sharding and Scaling

DoSQL supports horizontal scaling through sharding, distributing data across multiple Durable Objects.

### VSchema Configuration

DoSQL uses a VSchema (Virtual Schema) to define sharding topology, inspired by Vitess but optimized for Durable Objects.

```typescript
import {
  createVSchema,
  shardedTable,
  unshardedTable,
  referenceTable,
  hashVindex,
  consistentHashVindex,
  rangeVindex,
  shard,
  replica,
  createShardId,
  type VSchema,
} from 'dosql/sharding';

// Define the complete sharding configuration
const vschema: VSchema = createVSchema(
  {
    // Sharded by user_id using hash distribution
    users: shardedTable('id', hashVindex('fnv1a')),

    // Sharded by user_id to colocate with users table
    orders: shardedTable('user_id', hashVindex('fnv1a')),

    // Sharded by user_id for colocation with user data
    user_preferences: shardedTable('user_id', hashVindex('fnv1a')),

    // Range sharding for time-series data
    events: shardedTable('timestamp', rangeVindex([
      { shard: createShardId('events-2024-q1'), min: '2024-01-01', max: '2024-04-01' },
      { shard: createShardId('events-2024-q2'), min: '2024-04-01', max: '2024-07-01' },
      { shard: createShardId('events-2024-q3'), min: '2024-07-01', max: '2024-10-01' },
      { shard: createShardId('events-2024-q4'), min: '2024-10-01', max: null },
    ])),

    // Unsharded table: all data in one shard
    system_config: unshardedTable(createShardId('config-shard')),

    // Reference tables: replicated to all shards for local joins
    countries: referenceTable(true),  // Read-only
    currencies: referenceTable(true),
    product_categories: referenceTable(false), // Writable (changes replicate to all shards)
  },
  [
    // Shard definitions with replicas
    shard(createShardId('shard-0'), 'DOSQL_DB', {
      replicas: [
        replica('shard-0-replica-1', 'DOSQL_DB_REPLICA', 'replica', { region: 'us-west' }),
        replica('shard-0-replica-2', 'DOSQL_DB_REPLICA', 'replica', { region: 'us-east' }),
        replica('shard-0-analytics', 'DOSQL_DB_ANALYTICS', 'analytics', { region: 'us-central' }),
      ],
    }),
    shard(createShardId('shard-1'), 'DOSQL_DB'),
    shard(createShardId('shard-2'), 'DOSQL_DB'),
    shard(createShardId('shard-3'), 'DOSQL_DB'),
    shard(createShardId('events-2024-q1'), 'DOSQL_EVENTS'),
    shard(createShardId('events-2024-q2'), 'DOSQL_EVENTS'),
    shard(createShardId('events-2024-q3'), 'DOSQL_EVENTS'),
    shard(createShardId('events-2024-q4'), 'DOSQL_EVENTS'),
    shard(createShardId('config-shard'), 'DOSQL_CONFIG'),
  ],
  {
    defaultShard: createShardId('shard-0'),
    settings: {
      defaultVindexType: 'hash',
      maxParallelShards: 8,
      shardTimeoutMs: 5000,
      enableCaching: true,
    },
  }
);
```

### Vindex Types

Vindexes (Virtual Indexes) determine how rows are distributed across shards:

```typescript
import {
  hashVindex,
  consistentHashVindex,
  rangeVindex,
  createShardId,
} from 'dosql/sharding';

// Hash vindex: uniform distribution via FNV-1a or xxHash
const userVindex = hashVindex('fnv1a');
// Best for: Primary keys, UUIDs, random IDs
// Pros: Even distribution, O(1) lookups
// Cons: Range queries require scatter-gather across all shards

// Consistent hash vindex: virtual nodes for smooth rebalancing
const orderVindex = consistentHashVindex(150, 'xxhash');
// Best for: High-churn data, frequent resharding scenarios
// Pros: Minimal data movement when adding/removing shards
// Cons: Slightly more memory for virtual node ring

// Range vindex: boundary-based partitioning
const timeSeriesVindex = rangeVindex([
  { shard: createShardId('hot'), min: '2024-01-01', max: null },    // Current data
  { shard: createShardId('warm'), min: '2023-01-01', max: '2024-01-01' },
  { shard: createShardId('cold'), min: null, max: '2023-01-01' },   // Historical
]);
// Best for: Time-series data, date-based partitioning, hot/cold data tiers
// Pros: Efficient range queries, natural data tiering
// Cons: Potential hotspots on the most recent partition
```

### Query Routing

The query router analyzes SQL and determines optimal shard routing:

```typescript
import {
  createShardRouter,
  createShardExecutor,
  type ExecutionPlan,
} from 'dosql/sharding';

const router = createShardRouter(vschema);
const executor = createShardExecutor(router, {
  getDO: (shardId) => env.DOSQL_DB.get(env.DOSQL_DB.idFromName(shardId)),
});

// Single-shard query: equality on shard key routes to one shard
const user = await executor.query(
  'SELECT * FROM users WHERE id = ?',
  [42]
);
// Routes to: specific shard based on hash(42)

// Scatter-gather query: no shard key requires querying all shards
const activeUsers = await executor.query(
  'SELECT COUNT(*) as count FROM users WHERE active = ?',
  [true]
);
// Routes to: all shards, results aggregated

// IN-list optimization: routes only to shards containing those IDs
const specificUsers = await executor.query(
  'SELECT * FROM users WHERE id IN (?, ?, ?, ?)',
  [1, 2, 3, 4]
);
// Routes to: subset of shards (may not be all shards)

// Colocated join: both tables sharded by same key
const userOrders = await executor.query(
  `SELECT u.name, o.total, o.created_at
   FROM users u
   JOIN orders o ON o.user_id = u.id
   WHERE u.id = ?`,
  [42]
);
// Routes to: single shard (both tables colocated by user_id)

// Join with reference table: reference tables are on all shards
const ordersWithCurrency = await executor.query(
  `SELECT o.*, c.symbol, c.name as currency_name
   FROM orders o
   JOIN currencies c ON c.code = o.currency_code
   WHERE o.user_id = ?`,
  [42]
);
// Routes to: single shard (currencies replicated everywhere)

// Analyze a query without executing it
const plan: ExecutionPlan = await router.analyze(
  'SELECT * FROM users WHERE created_at > ? ORDER BY created_at LIMIT 10',
  ['2024-01-01']
);
console.log(plan);
// {
//   sql: 'SELECT ...',
//   routing: {
//     queryType: 'scatter',
//     targetShards: ['shard-0', 'shard-1', 'shard-2', 'shard-3'],
//     readPreference: 'replica',
//     canUseReplica: true,
//     costEstimate: 4.0,
//     reason: 'No shard key in WHERE clause'
//   },
//   postProcessing: [
//     { type: 'merge' },
//     { type: 'sort', columns: [{ column: 'created_at', direction: 'ASC' }] },
//     { type: 'limit', count: 10 }
//   ],
//   totalCost: 4.5
// }
```

### Cross-Shard Transactions

DoSQL supports distributed transactions across shards using a two-phase commit protocol (2PC):

```typescript
import { createShardExecutor, createShardId } from 'dosql/sharding';

const executor = createShardExecutor(router, {
  getDO: (shardId) => env.DOSQL_DB.get(env.DOSQL_DB.idFromName(shardId)),
});

// Cross-shard transaction: transfer between users on different shards
async function transferBetweenUsers(
  fromUserId: number,
  toUserId: number,
  amount: number
): Promise<{ transactionId: string }> {
  return executor.transaction(async (tx) => {
    // These queries may hit different shards
    const fromAccount = await tx.queryOne(
      'SELECT balance FROM accounts WHERE user_id = ? FOR UPDATE',
      [fromUserId]
    );

    if (!fromAccount || fromAccount.balance < amount) {
      throw new Error('Insufficient funds');
    }

    const toAccount = await tx.queryOne(
      'SELECT id FROM accounts WHERE user_id = ?',
      [toUserId]
    );

    if (!toAccount) {
      throw new Error('Destination account not found');
    }

    // Updates are coordinated across shards via 2PC
    await tx.run(
      'UPDATE accounts SET balance = balance - ? WHERE user_id = ?',
      [amount, fromUserId]
    );

    await tx.run(
      'UPDATE accounts SET balance = balance + ? WHERE user_id = ?',
      [amount, toUserId]
    );

    // Record in ledger (may be on a different shard)
    const result = await tx.run(
      `INSERT INTO transactions (from_user, to_user, amount, created_at)
       VALUES (?, ?, ?, CURRENT_TIMESTAMP)`,
      [fromUserId, toUserId, amount]
    );

    return { transactionId: result.lastInsertRowId.toString() };
  });
}

// Transaction with explicit shard hints
await executor.transaction(async (tx) => {
  // Force query to a specific shard (advanced use case)
  const result = await tx.queryOnShard(
    createShardId('shard-0'),
    'SELECT * FROM system_stats'
  );

  // Batch operations to minimize round trips
  await tx.batch([
    { sql: 'UPDATE users SET last_seen = CURRENT_TIMESTAMP WHERE id = ?', params: [1] },
    { sql: 'UPDATE users SET last_seen = CURRENT_TIMESTAMP WHERE id = ?', params: [2] },
    { sql: 'UPDATE users SET last_seen = CURRENT_TIMESTAMP WHERE id = ?', params: [3] },
  ]);
}, {
  timeoutMs: 10000,
  readPreference: 'primary', // Force primary for writes
});
```

### Replica Configuration

Configure read replicas for scaling reads and geographic distribution:

```typescript
import {
  createShardExecutor,
  type ReadPreference,
} from 'dosql/sharding';

const executor = createShardExecutor(router, {
  getDO: (shardId) => env.DOSQL_DB.get(env.DOSQL_DB.idFromName(shardId)),
  getReplicaDO: (shardId, replicaId) => {
    const namespace = replicaId.includes('analytics')
      ? env.DOSQL_ANALYTICS
      : env.DOSQL_REPLICA;
    return namespace.get(namespace.idFromName(`${shardId}-${replicaId}`));
  },
});

// Query with read preference
const users = await executor.query(
  'SELECT * FROM users WHERE active = true',
  [],
  { readPreference: 'replica' } // Read from replica
);

// Read preference options:
// - 'primary':          Always read from primary (strongest consistency)
// - 'primaryPreferred': Primary if available, else replica
// - 'replica':          Always read from replica (may have replication lag)
// - 'replicaPreferred': Replica if available, else primary
// - 'nearest':          Lowest latency (considers geographic distance)
// - 'analytics':        Route to analytics replica (for heavy queries)

// Heavy analytics query routed to analytics replica
const report = await executor.query(
  `SELECT
     date(created_at) as day,
     COUNT(*) as orders,
     SUM(total) as revenue
   FROM orders
   WHERE created_at >= date('now', '-30 days')
   GROUP BY day
   ORDER BY day`,
  [],
  { readPreference: 'analytics' }
);

// Monitor cluster health
const health = await executor.getClusterHealth();
console.log('Cluster health:', {
  healthyShards: health.healthyShards,
  totalShards: health.totalShards,
  healthyReplicas: health.healthyReplicas,
  totalReplicas: health.totalReplicas,
  shards: health.shards.map(s => ({
    id: s.shardId,
    status: s.status,
    primaryHealth: s.primaryHealth,
    replicaHealths: s.replicaHealths,
    latencyMs: s.latencyMs,
  })),
});

// Event handlers for failover
executor.on('replicaDown', (shardId, replicaId) => {
  console.warn(`Replica ${replicaId} on shard ${shardId} is down`);
});

executor.on('primaryDown', (shardId) => {
  console.error(`Primary on shard ${shardId} is down - promoting replica`);
});

executor.on('shardRecovered', (shardId) => {
  console.info(`Shard ${shardId} has recovered`);
});
```

---

## Next Steps

- [Architecture](./architecture.md) - Understanding DoSQL internals and design decisions
- [API Reference](./api-reference.md) - Complete API documentation
- [Benchmarks](./BENCHMARKS.md) - Performance characteristics and optimization
- [Security](./SECURITY.md) - Security best practices and considerations
