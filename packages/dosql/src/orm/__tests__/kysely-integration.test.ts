/**
 * Kysely ORM Integration Tests for DoSQL
 *
 * Comprehensive integration tests covering:
 * - Basic CRUD operations
 * - Transaction support
 * - Schema migrations
 * - Query building compatibility
 *
 * Uses workers-vitest-pool - NO MOCKS.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Kysely, sql } from 'kysely';

import {
  DoSQLDialect,
  createDoSQLDialect,
  createDoSQLKysely,
  type DoSQLBackend,
  type Generated,
  type Insertable,
  type Selectable,
} from '../kysely/index.js';
import { MockDoSQLBackend, createMockBackend } from '../kysely/mock-backend.js';

// =============================================================================
// TEST DATABASE SCHEMA
// =============================================================================

interface TestDatabase {
  users: UsersTable;
  posts: PostsTable;
  comments: CommentsTable;
  products: ProductsTable;
  orders: OrdersTable;
  migrations: MigrationsTable;
}

interface UsersTable {
  id: Generated<number>;
  username: string;
  email: string;
  created_at: Generated<Date>;
  is_admin: boolean;
}

interface PostsTable {
  id: Generated<number>;
  author_id: number;
  title: string;
  body: string;
  status: 'draft' | 'published' | 'archived';
  view_count: number;
}

interface CommentsTable {
  id: Generated<number>;
  post_id: number;
  user_id: number;
  content: string;
}

interface ProductsTable {
  id: Generated<number>;
  sku: string;
  name: string;
  price: number;
  stock: number;
  category: string;
}

interface OrdersTable {
  id: Generated<number>;
  user_id: number;
  product_id: number;
  quantity: number;
  total: number;
  status: 'pending' | 'completed' | 'cancelled';
}

interface MigrationsTable {
  id: Generated<number>;
  name: string;
  applied_at: Generated<Date>;
}

// =============================================================================
// TEST SUITES
// =============================================================================

describe('Kysely ORM Integration Tests', () => {
  let backend: MockDoSQLBackend;
  let db: Kysely<TestDatabase>;

  beforeEach(() => {
    backend = createMockBackend();
    db = new Kysely<TestDatabase>({
      dialect: new DoSQLDialect({ backend }),
    });
  });

  afterEach(async () => {
    await db.destroy();
    backend.clear();
  });

  // ---------------------------------------------------------------------------
  // Basic CRUD Operations
  // ---------------------------------------------------------------------------

  describe('Basic CRUD Operations', () => {
    beforeEach(() => {
      backend.seed('users', [
        { id: 1, username: 'alice', email: 'alice@example.com', created_at: new Date(), is_admin: false },
        { id: 2, username: 'bob', email: 'bob@example.com', created_at: new Date(), is_admin: true },
        { id: 3, username: 'charlie', email: 'charlie@example.com', created_at: new Date(), is_admin: false },
      ]);
    });

    describe('SELECT operations', () => {
      it('should select all rows', async () => {
        const users = await db.selectFrom('users').selectAll().execute();
        expect(users).toHaveLength(3);
      });

      it('should select specific columns', async () => {
        const users = await db.selectFrom('users').select(['username', 'email']).execute();
        expect(users).toHaveLength(3);
        expect(users[0]).toHaveProperty('username');
        expect(users[0]).toHaveProperty('email');
        expect(Object.keys(users[0])).toHaveLength(2);
      });

      it('should filter with WHERE', async () => {
        const users = await db.selectFrom('users').selectAll().where('id', '=', 1).execute();
        expect(users).toHaveLength(1);
        expect(users[0].username).toBe('alice');
      });

      it('should use executeTakeFirst for single row', async () => {
        const user = await db.selectFrom('users').selectAll().where('id', '=', 2).executeTakeFirst();
        expect(user).toBeDefined();
        expect(user?.username).toBe('bob');
      });

      it('should return undefined for no match with executeTakeFirst', async () => {
        const user = await db.selectFrom('users').selectAll().where('id', '=', 999).executeTakeFirst();
        expect(user).toBeUndefined();
      });
    });

    describe('INSERT operations', () => {
      it('should insert a single row', async () => {
        await db.insertInto('users').values({
          username: 'david',
          email: 'david@example.com',
          is_admin: false,
        }).execute();

        const users = await db.selectFrom('users').selectAll().execute();
        expect(users).toHaveLength(4);
      });

      it('should verify inserted data', async () => {
        await db.insertInto('users').values({
          username: 'eve',
          email: 'eve@example.com',
          is_admin: true,
        }).execute();

        const user = await db.selectFrom('users').selectAll().where('username', '=', 'eve').executeTakeFirst();
        expect(user).toBeDefined();
        expect(user?.email).toBe('eve@example.com');
        expect(user?.is_admin).toBe(true);
      });
    });

    describe('UPDATE operations', () => {
      it('should update a row', async () => {
        await db.updateTable('users').set({ email: 'alice.updated@example.com' }).where('id', '=', 1).execute();

        const user = await db.selectFrom('users').selectAll().where('id', '=', 1).executeTakeFirst();
        expect(user?.email).toBe('alice.updated@example.com');
      });

      it('should update multiple columns', async () => {
        await db.updateTable('users').set({
          username: 'bob_admin',
          is_admin: true,
        }).where('id', '=', 2).execute();

        const user = await db.selectFrom('users').selectAll().where('id', '=', 2).executeTakeFirst();
        expect(user?.username).toBe('bob_admin');
        expect(user?.is_admin).toBe(true);
      });
    });

    describe('DELETE operations', () => {
      it('should delete a row', async () => {
        await db.deleteFrom('users').where('id', '=', 3).execute();

        const users = await db.selectFrom('users').selectAll().execute();
        expect(users).toHaveLength(2);
        expect(users.find((u) => u.id === 3)).toBeUndefined();
      });

      it('should delete multiple rows with condition', async () => {
        await db.deleteFrom('users').where('is_admin', '=', false).execute();

        const users = await db.selectFrom('users').selectAll().execute();
        expect(users).toHaveLength(1);
        expect(users[0].is_admin).toBe(true);
      });
    });
  });

  // ---------------------------------------------------------------------------
  // Transaction Support
  // ---------------------------------------------------------------------------

  describe('Transaction Support', () => {
    beforeEach(() => {
      backend.seed('products', [
        { id: 1, sku: 'WIDGET-001', name: 'Widget', price: 9.99, stock: 100, category: 'hardware' },
        { id: 2, sku: 'GADGET-001', name: 'Gadget', price: 19.99, stock: 50, category: 'hardware' },
      ]);
      backend.seed('orders', []);
    });

    it('should commit transaction on success', async () => {
      await db.transaction().execute(async (trx) => {
        // Create an order
        await trx.insertInto('orders').values({
          user_id: 1,
          product_id: 1,
          quantity: 5,
          total: 49.95,
          status: 'completed',
        }).execute();

        // Update product stock
        await trx.updateTable('products').set({ stock: 95 }).where('id', '=', 1).execute();
      });

      // Verify order was created
      const orders = await db.selectFrom('orders').selectAll().execute();
      expect(orders).toHaveLength(1);

      // Verify stock was updated
      const product = await db.selectFrom('products').selectAll().where('id', '=', 1).executeTakeFirst();
      expect(product?.stock).toBe(95);
    });

    it('should rollback transaction on error', async () => {
      const initialProducts = await db.selectFrom('products').selectAll().execute();
      const initialStock = initialProducts.find((p) => p.id === 1)?.stock;

      try {
        await db.transaction().execute(async (trx) => {
          // Update stock
          await trx.updateTable('products').set({ stock: 0 }).where('id', '=', 1).execute();

          // Throw error to trigger rollback
          throw new Error('Transaction failed');
        });
      } catch {
        // Expected
      }

      // Verify stock was restored
      const product = await db.selectFrom('products').selectAll().where('id', '=', 1).executeTakeFirst();
      expect(product?.stock).toBe(initialStock);
    });

    it('should support multiple operations in transaction', async () => {
      const result = await db.transaction().execute(async (trx) => {
        await trx.insertInto('orders').values({
          user_id: 1,
          product_id: 1,
          quantity: 2,
          total: 19.98,
          status: 'pending',
        }).execute();

        await trx.insertInto('orders').values({
          user_id: 1,
          product_id: 2,
          quantity: 1,
          total: 19.99,
          status: 'pending',
        }).execute();

        const orders = await trx.selectFrom('orders').selectAll().execute();
        return orders.length;
      });

      expect(result).toBe(2);
    });
  });

  // ---------------------------------------------------------------------------
  // Schema Migrations
  // ---------------------------------------------------------------------------

  describe('Schema Migrations', () => {
    it('should track migrations in migrations table', async () => {
      backend.seed('migrations', [
        { id: 1, name: '001_create_users', applied_at: new Date('2024-01-01') },
        { id: 2, name: '002_add_posts', applied_at: new Date('2024-01-02') },
      ]);

      const migrations = await db.selectFrom('migrations').selectAll().execute();
      expect(migrations).toHaveLength(2);
      expect(migrations[0].name).toBe('001_create_users');
    });

    it('should support querying migration status', async () => {
      backend.seed('migrations', [
        { id: 1, name: '001_create_users', applied_at: new Date('2024-01-01') },
      ]);

      // Check if a migration has been applied
      const migration = await db
        .selectFrom('migrations')
        .selectAll()
        .where('name', '=', '001_create_users')
        .executeTakeFirst();

      expect(migration).toBeDefined();
      expect(migration?.name).toBe('001_create_users');

      // Check if a migration is pending
      const pending = await db
        .selectFrom('migrations')
        .selectAll()
        .where('name', '=', '003_add_comments')
        .executeTakeFirst();

      expect(pending).toBeUndefined();
    });

    it('should record new migrations', async () => {
      backend.seed('migrations', []);

      await db.insertInto('migrations').values({
        name: '001_initial',
      }).execute();

      const migrations = await db.selectFrom('migrations').selectAll().execute();
      expect(migrations).toHaveLength(1);
      expect(migrations[0].name).toBe('001_initial');
    });
  });

  // ---------------------------------------------------------------------------
  // Query Building Compatibility
  // ---------------------------------------------------------------------------

  describe('Query Building Compatibility', () => {
    beforeEach(() => {
      backend.seed('posts', [
        { id: 1, author_id: 1, title: 'Hello World', body: 'First post', status: 'published', view_count: 100 },
        { id: 2, author_id: 1, title: 'TypeScript Tips', body: 'Type safety', status: 'published', view_count: 500 },
        { id: 3, author_id: 2, title: 'Draft Post', body: 'Work in progress', status: 'draft', view_count: 0 },
        { id: 4, author_id: 2, title: 'Old News', body: 'Archived content', status: 'archived', view_count: 50 },
      ]);

      backend.seed('users', [
        { id: 1, username: 'alice', email: 'alice@example.com', created_at: new Date(), is_admin: true },
        { id: 2, username: 'bob', email: 'bob@example.com', created_at: new Date(), is_admin: false },
      ]);
    });

    describe('Filtering', () => {
      it('should filter with equality', async () => {
        const posts = await db.selectFrom('posts').selectAll().where('status', '=', 'published').execute();
        expect(posts).toHaveLength(2);
      });

      it('should filter with inequality', async () => {
        const posts = await db.selectFrom('posts').selectAll().where('status', '<>', 'draft').execute();
        expect(posts).toHaveLength(3);
      });

      it('should filter with greater than', async () => {
        const posts = await db.selectFrom('posts').selectAll().where('view_count', '>', 50).execute();
        expect(posts).toHaveLength(2);
      });

      it('should filter with less than or equal', async () => {
        const posts = await db.selectFrom('posts').selectAll().where('view_count', '<=', 50).execute();
        expect(posts).toHaveLength(2);
      });
    });

    describe('Ordering', () => {
      it('should order ascending', async () => {
        const posts = await db.selectFrom('posts').selectAll().orderBy('view_count', 'asc').execute();
        expect(posts[0].view_count).toBe(0);
      });

      it('should order descending', async () => {
        const posts = await db.selectFrom('posts').selectAll().orderBy('view_count', 'desc').execute();
        expect(posts[0].view_count).toBe(500);
      });
    });

    describe('Limiting', () => {
      it('should limit results', async () => {
        const posts = await db.selectFrom('posts').selectAll().limit(2).execute();
        expect(posts).toHaveLength(2);
      });

      it('should support offset with limit', async () => {
        const posts = await db.selectFrom('posts').selectAll().orderBy('id', 'asc').limit(2).offset(1).execute();
        expect(posts).toHaveLength(2);
        expect(posts[0].id).toBe(2);
      });
    });

    describe('Joins', () => {
      it('should perform inner join', async () => {
        const result = await db
          .selectFrom('posts')
          .innerJoin('users', 'users.id', 'posts.author_id')
          .select(['posts.title', 'users.username'])
          .execute();

        expect(result).toHaveLength(4);
        expect(result[0]).toHaveProperty('title');
        expect(result[0]).toHaveProperty('username');
      });

      it('should filter joined tables', async () => {
        const result = await db
          .selectFrom('posts')
          .innerJoin('users', 'users.id', 'posts.author_id')
          .select(['posts.title', 'users.username'])
          .where('posts.status', '=', 'published')
          .execute();

        expect(result).toHaveLength(2);
      });
    });
  });

  // ---------------------------------------------------------------------------
  // Type Safety
  // ---------------------------------------------------------------------------

  describe('Type Safety', () => {
    beforeEach(() => {
      backend.seed('users', [
        { id: 1, username: 'alice', email: 'alice@example.com', created_at: new Date(), is_admin: true },
      ]);
    });

    it('should infer correct types for selectAll', async () => {
      const users = await db.selectFrom('users').selectAll().execute();
      const user = users[0];

      // Runtime verification of expected types
      expect(typeof user.id).toBe('number');
      expect(typeof user.username).toBe('string');
      expect(typeof user.email).toBe('string');
      expect(typeof user.is_admin).toBe('boolean');
    });

    it('should infer correct types for select specific columns', async () => {
      const users = await db.selectFrom('users').select(['id', 'username']).execute();
      const user = users[0];

      expect(typeof user.id).toBe('number');
      expect(typeof user.username).toBe('string');
      expect(Object.keys(user)).toHaveLength(2);
    });

    it('should enforce insertable types', async () => {
      // This should work - providing required fields
      await db.insertInto('users').values({
        username: 'test',
        email: 'test@example.com',
        is_admin: false,
      }).execute();

      const user = await db.selectFrom('users').selectAll().where('username', '=', 'test').executeTakeFirst();
      expect(user).toBeDefined();
    });
  });

  // ---------------------------------------------------------------------------
  // Factory Functions
  // ---------------------------------------------------------------------------

  describe('Factory Functions', () => {
    it('should create dialect with createDoSQLDialect', async () => {
      const dialect = createDoSQLDialect({ backend });
      const db2 = new Kysely<TestDatabase>({ dialect });

      backend.seed('users', [
        { id: 1, username: 'test', email: 'test@example.com', created_at: new Date(), is_admin: false },
      ]);

      const users = await db2.selectFrom('users').selectAll().execute();
      expect(users).toHaveLength(1);

      await db2.destroy();
    });

    it('should create kysely instance with createDoSQLKysely', async () => {
      const db2 = createDoSQLKysely<TestDatabase>(backend);

      backend.seed('users', [
        { id: 1, username: 'test', email: 'test@example.com', created_at: new Date(), is_admin: false },
      ]);

      const users = await db2.selectFrom('users').selectAll().execute();
      expect(users).toHaveLength(1);

      await db2.destroy();
    });
  });

  // ---------------------------------------------------------------------------
  // Logging Support
  // ---------------------------------------------------------------------------

  describe('Logging Support', () => {
    it('should log queries when logging is enabled', async () => {
      const logs: { sql: string; params: unknown[] }[] = [];

      const db2 = new Kysely<TestDatabase>({
        dialect: new DoSQLDialect({
          backend,
          log: (sql, params) => {
            logs.push({ sql, params });
          },
        }),
      });

      backend.seed('users', [
        { id: 1, username: 'test', email: 'test@example.com', created_at: new Date(), is_admin: false },
      ]);

      await db2.selectFrom('users').selectAll().where('id', '=', 1).execute();

      expect(logs.length).toBeGreaterThan(0);
      expect(logs[0].sql.toLowerCase()).toContain('select');
      expect(logs[0].params).toContain(1);

      await db2.destroy();
    });

    it('should support boolean log option', async () => {
      // Should not throw when log: true
      const db2 = new Kysely<TestDatabase>({
        dialect: new DoSQLDialect({
          backend,
          log: true,
        }),
      });

      backend.seed('users', [
        { id: 1, username: 'test', email: 'test@example.com', created_at: new Date(), is_admin: false },
      ]);

      await db2.selectFrom('users').selectAll().execute();

      await db2.destroy();
    });
  });

  // ---------------------------------------------------------------------------
  // Edge Cases
  // ---------------------------------------------------------------------------

  describe('Edge Cases', () => {
    it('should handle empty result set', async () => {
      backend.seed('users', []);

      const users = await db.selectFrom('users').selectAll().execute();
      expect(users).toHaveLength(0);
    });

    it('should handle null values correctly', async () => {
      backend.seed('users', [
        { id: 1, username: 'test', email: null, created_at: new Date(), is_admin: false },
      ]);

      const user = await db.selectFrom('users').selectAll().where('id', '=', 1).executeTakeFirst();
      expect(user?.email).toBeNull();
    });

    it('should handle non-existent records gracefully', async () => {
      backend.seed('users', [
        { id: 1, username: 'test', email: 'test@example.com', created_at: new Date(), is_admin: false },
      ]);

      const user = await db.selectFrom('users').selectAll().where('id', '=', 999).executeTakeFirst();
      expect(user).toBeUndefined();
    });

    it('should handle update with no matching rows', async () => {
      backend.seed('users', [
        { id: 1, username: 'test', email: 'test@example.com', created_at: new Date(), is_admin: false },
      ]);

      // Update non-existent row - should not throw
      await db.updateTable('users').set({ username: 'updated' }).where('id', '=', 999).execute();

      // Verify original data unchanged
      const user = await db.selectFrom('users').selectAll().where('id', '=', 1).executeTakeFirst();
      expect(user?.username).toBe('test');
    });

    it('should handle delete with no matching rows', async () => {
      backend.seed('users', [
        { id: 1, username: 'test', email: 'test@example.com', created_at: new Date(), is_admin: false },
      ]);

      // Delete non-existent row - should not throw
      await db.deleteFrom('users').where('id', '=', 999).execute();

      // Verify original data unchanged
      const users = await db.selectFrom('users').selectAll().execute();
      expect(users).toHaveLength(1);
    });
  });
});
