/**
 * Complex JOIN Scenario
 *
 * Benchmarks multi-table JOIN queries with aggregation.
 * Tests query performance for relational patterns.
 */

import type {
  BenchmarkAdapter,
  ScenarioConfig,
  ScenarioResult,
  ScenarioExecutor,
} from '../types.js';
import { calculateLatencyStats, JOIN_BENCHMARK_SCHEMA } from '../types.js';

/**
 * Default configuration for complex JOIN scenario
 */
export const COMPLEX_JOIN_CONFIG: ScenarioConfig = {
  type: 'complex-join',
  name: 'Complex JOIN Query',
  description: 'Multi-table JOIN with aggregation (users -> orders -> order_items)',
  iterations: 50,
  warmupIterations: 5,
  rowCount: 500,
};

/**
 * Execute the complex JOIN scenario
 *
 * This scenario:
 * 1. Creates related tables (users, orders, order_items)
 * 2. Seeds with realistic relational data
 * 3. Runs JOIN queries with aggregation
 * 4. Measures query latency
 */
export const executeComplexJoin: ScenarioExecutor = async (
  adapter: BenchmarkAdapter,
  config: ScenarioConfig
): Promise<ScenarioResult> => {
  const timestamp = Date.now();
  const usersTable = `users_${timestamp}`;
  const ordersTable = `orders_${timestamp}`;
  const orderItemsTable = `order_items_${timestamp}`;

  const timings: number[] = [];
  const errors: string[] = [];
  let successCount = 0;
  let errorCount = 0;

  const startedAt = new Date().toISOString();

  // Setup: Create tables
  try {
    await adapter.dropTable(orderItemsTable);
    await adapter.dropTable(ordersTable);
    await adapter.dropTable(usersTable);
  } catch {
    // Ignore if tables don't exist
  }

  await adapter.createTable({
    ...JOIN_BENCHMARK_SCHEMA.users,
    tableName: usersTable,
  });
  await adapter.createTable({
    ...JOIN_BENCHMARK_SCHEMA.orders,
    tableName: ordersTable,
  });
  await adapter.createTable({
    ...JOIN_BENCHMARK_SCHEMA.order_items,
    tableName: orderItemsTable,
  });

  // Seed users
  const userCount = Math.min(config.rowCount, 100);
  const users: Record<string, unknown>[] = [];
  for (let i = 1; i <= userCount; i++) {
    users.push({
      id: i,
      name: `User ${i}`,
      email: `user${i}@example.com`,
      created_at: Date.now() - Math.floor(Math.random() * 86400000 * 365),
    });
  }
  await adapter.insertBatch(usersTable, users);

  // Seed orders (multiple per user)
  const orderCount = Math.min(config.rowCount * 2, 500);
  const orders: Record<string, unknown>[] = [];
  for (let i = 1; i <= orderCount; i++) {
    orders.push({
      id: i,
      user_id: Math.floor(Math.random() * userCount) + 1,
      total: Math.random() * 500 + 10,
      status: ['pending', 'completed', 'shipped', 'cancelled'][
        Math.floor(Math.random() * 4)
      ],
      created_at: Date.now() - Math.floor(Math.random() * 86400000 * 30),
    });
  }
  await adapter.insertBatch(ordersTable, orders);

  // Seed order items (multiple per order)
  const itemCount = Math.min(config.rowCount * 5, 2000);
  const items: Record<string, unknown>[] = [];
  for (let i = 1; i <= itemCount; i++) {
    items.push({
      id: i,
      order_id: Math.floor(Math.random() * orderCount) + 1,
      product_name: `Product ${Math.floor(Math.random() * 100)}`,
      quantity: Math.floor(Math.random() * 5) + 1,
      price: Math.random() * 100 + 5,
    });
  }

  // Insert items in chunks
  const chunkSize = 100;
  for (let i = 0; i < items.length; i += chunkSize) {
    const chunk = items.slice(i, i + chunkSize);
    await adapter.insertBatch(orderItemsTable, chunk);
  }

  // Complex JOIN query that gets user order summaries
  const joinQuery = `
    SELECT
      u.id as user_id,
      u.name as user_name,
      COUNT(DISTINCT o.id) as order_count,
      SUM(o.total) as total_spent,
      COUNT(oi.id) as item_count
    FROM ${usersTable} u
    LEFT JOIN ${ordersTable} o ON u.id = o.user_id
    LEFT JOIN ${orderItemsTable} oi ON o.id = oi.order_id
    WHERE o.status = :status
    GROUP BY u.id, u.name
    HAVING COUNT(o.id) > 0
    ORDER BY total_spent DESC
    LIMIT 10
  `;

  const statuses = ['pending', 'completed', 'shipped'];

  // Warmup phase
  for (let i = 0; i < config.warmupIterations; i++) {
    const status = statuses[i % statuses.length];
    await adapter.query(joinQuery, { status });
  }

  // Benchmark phase
  for (let i = 0; i < config.iterations; i++) {
    const status = statuses[i % statuses.length];
    const result = await adapter.query(joinQuery, { status });

    if (result.success) {
      timings.push(result.durationMs);
      successCount++;
    } else {
      errorCount++;
      if (result.error) {
        errors.push(result.error);
      }
    }
  }

  // Cleanup
  try {
    await adapter.dropTable(orderItemsTable);
    await adapter.dropTable(ordersTable);
    await adapter.dropTable(usersTable);
  } catch {
    // Ignore cleanup errors
  }

  const completedAt = new Date().toISOString();

  // Calculate metrics
  const latency = calculateLatencyStats(timings);
  const totalDurationMs = timings.reduce((a, b) => a + b, 0);
  const opsPerSecond = totalDurationMs > 0 ? (successCount / totalDurationMs) * 1000 : 0;

  return {
    config,
    adapter: adapter.name,
    latency,
    throughput: {
      opsPerSecond,
      totalOperations: successCount + errorCount,
      successCount,
      errorCount,
      errorRate: errorCount / (successCount + errorCount) || 0,
    },
    rawTimings: timings,
    startedAt,
    completedAt,
    errors: errors.slice(0, 10),
  };
};

/**
 * Create a complex JOIN scenario with custom config
 */
export function createComplexJoinScenario(
  overrides: Partial<ScenarioConfig> = {}
): ScenarioConfig {
  return {
    ...COMPLEX_JOIN_CONFIG,
    ...overrides,
  };
}
