/**
 * Prisma ORM Integration for DoSQL
 *
 * Provides a Prisma driver adapter that enables using Prisma ORM
 * with DoSQL on Cloudflare Workers edge runtime.
 *
 * @example
 * ```typescript
 * import { PrismaClient } from '@prisma/client';
 * import { PrismaDoSQLAdapter, createPrismaAdapter } from 'dosql/orm/prisma';
 *
 * // Using the class directly
 * const adapter = new PrismaDoSQLAdapter({ backend: dosqlBackend });
 *
 * // Or using the factory function
 * const adapter = createPrismaAdapter({
 *   backend: dosqlBackend,
 *   logging: true,
 * });
 *
 * // Create Prisma client with DoSQL adapter
 * const prisma = new PrismaClient({ adapter });
 *
 * // Use Prisma normally
 * const users = await prisma.user.findMany({
 *   where: { active: true },
 *   include: { posts: true },
 * });
 * ```
 *
 * @packageDocumentation
 */

// Main adapter class and factory
export {
  PrismaDoSQLAdapter,
  createPrismaAdapter,
} from './adapter.js';

// Configuration types
export type {
  PrismaDoSQLConfig,
} from './adapter.js';

// DoSQL backend interface (for implementers)
export type {
  DoSQLBackend,
  DoSQLTransaction,
} from './adapter.js';

// Prisma driver adapter types
export type {
  DriverAdapter,
  Query,
  Result,
  ResultSet,
  ColumnType,
  Transaction,
  TransactionOptions,
  IsolationLevel,
} from './adapter.js';

// Result utilities
export {
  isOk,
  isError,
  unwrap,
  unwrapOr,
} from './adapter.js';
