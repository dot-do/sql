/**
 * DoSQL Integration Tests - Basic CRUD Operations
 *
 * Tests INSERT, SELECT, UPDATE, DELETE operations with type safety verification.
 * All fields use camelCase naming convention.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  createTestHarness,
  generateSampleUser,
  generateSampleOrder,
  generateSampleProduct,
  generateSampleCategory,
  type TestHarness,
  type UserRecord,
  type OrderRecord,
  type ProductRecord,
} from './setup.js';

describe('Basic CRUD Operations', () => {
  let harness: TestHarness;

  beforeEach(async () => {
    harness = await createTestHarness();
  });

  afterEach(async () => {
    await harness.cleanup();
  });

  // ===========================================================================
  // INSERT Operations
  // ===========================================================================

  describe('INSERT', () => {
    it('should insert a single user row', async () => {
      const userData = generateSampleUser({
        firstName: 'Alice',
        lastName: 'Johnson',
        emailAddress: 'alice@example.com',
      });

      const user = await harness.insertUser(userData);

      expect(user.id).toBeDefined();
      expect(user.id).toBeGreaterThan(0);
      expect(user.firstName).toBe('Alice');
      expect(user.lastName).toBe('Johnson');
      expect(user.emailAddress).toBe('alice@example.com');
    });

    it('should insert user with all camelCase fields preserved', async () => {
      const userData = generateSampleUser({
        firstName: 'Bob',
        lastName: 'Smith',
        emailAddress: 'bob@example.com',
        isActive: true,
        accountBalance: 150.50,
        createdAt: new Date('2024-01-15T10:30:00Z'),
      });

      const user = await harness.insertUser(userData);

      // Verify all camelCase fields
      expect(user.firstName).toBe('Bob');
      expect(user.lastName).toBe('Smith');
      expect(user.emailAddress).toBe('bob@example.com');
      expect(user.isActive).toBe(true);
      expect(user.accountBalance).toBe(150.50);
      expect(user.createdAt).toEqual(new Date('2024-01-15T10:30:00Z'));
    });

    it('should insert multiple users with unique IDs', async () => {
      const user1 = await harness.insertUser(generateSampleUser({ firstName: 'User1' }));
      const user2 = await harness.insertUser(generateSampleUser({ firstName: 'User2' }));
      const user3 = await harness.insertUser(generateSampleUser({ firstName: 'User3' }));

      expect(user1.id).toBe(1);
      expect(user2.id).toBe(2);
      expect(user3.id).toBe(3);

      // Verify uniqueness
      expect(new Set([user1.id, user2.id, user3.id]).size).toBe(3);
    });

    it('should insert an order with foreign key reference', async () => {
      const user = await harness.insertUser(generateSampleUser());

      const orderData = generateSampleOrder(user.id, {
        totalAmount: 99.99,
        shippingAddress: '123 Main St',
        orderStatus: 'pending',
      });

      const order = await harness.insertOrder(orderData);

      expect(order.id).toBeDefined();
      expect(order.userId).toBe(user.id);
      expect(order.totalAmount).toBe(99.99);
      expect(order.shippingAddress).toBe('123 Main St');
      expect(order.orderStatus).toBe('pending');
    });

    it('should insert product with numeric and boolean fields', async () => {
      const category = await harness.insertCategory(generateSampleCategory({
        categoryName: 'Electronics',
      }));

      const productData = generateSampleProduct(category.id, {
        productName: 'Laptop Pro',
        unitPrice: 1299.99,
        stockQuantity: 50,
        isAvailable: true,
      });

      const product = await harness.insertProduct(productData);

      expect(product.productName).toBe('Laptop Pro');
      expect(product.categoryId).toBe(category.id);
      expect(product.unitPrice).toBe(1299.99);
      expect(product.stockQuantity).toBe(50);
      expect(product.isAvailable).toBe(true);
    });

    it('should write to WAL on insert', async () => {
      const user = await harness.insertUser(generateSampleUser({
        firstName: 'WalTest',
      }));

      // Flush WAL to ensure write is persisted
      await harness.walWriter.flush();

      // Verify WAL entry exists by reading from WAL
      const entries: Array<{ table: string; op: string }> = [];
      for await (const entry of harness.walReader.iterate({ fromLSN: 0n })) {
        entries.push({ table: entry.table, op: entry.op });
      }

      expect(entries.length).toBeGreaterThan(0);
      expect(entries.some(e => e.table === 'users' && e.op === 'INSERT')).toBe(true);
    });
  });

  // ===========================================================================
  // SELECT Operations
  // ===========================================================================

  describe('SELECT', () => {
    it('should select user by primary key from B-tree', async () => {
      const user = await harness.insertUser(generateSampleUser({
        firstName: 'QueryTest',
        lastName: 'User',
      }));

      const result = await harness.usersBTree.get(`user:${user.id}`);

      expect(result).toBeDefined();
      expect(result?.id).toBe(user.id);
      expect(result?.firstName).toBe('QueryTest');
      expect(result?.lastName).toBe('User');
    });

    it('should return undefined for non-existent key', async () => {
      const result = await harness.usersBTree.get('user:999999');

      expect(result).toBeUndefined();
    });

    it('should select all users via B-tree iteration', async () => {
      await harness.insertUser(generateSampleUser({ firstName: 'Alice' }));
      await harness.insertUser(generateSampleUser({ firstName: 'Bob' }));
      await harness.insertUser(generateSampleUser({ firstName: 'Carol' }));

      const users: UserRecord[] = [];
      for await (const [key, user] of harness.usersBTree.entries()) {
        if (key.startsWith('user:')) {
          users.push(user);
        }
      }

      expect(users.length).toBe(3);
      expect(users.map(u => u.firstName).sort()).toEqual(['Alice', 'Bob', 'Carol']);
    });

    it('should select order by primary key', async () => {
      const user = await harness.insertUser(generateSampleUser());
      const order = await harness.insertOrder(generateSampleOrder(user.id, {
        totalAmount: 250.00,
        orderStatus: 'shipped',
      }));

      const result = await harness.ordersBTree.get(`order:${order.id}`);

      expect(result).toBeDefined();
      expect(result?.userId).toBe(user.id);
      expect(result?.totalAmount).toBe(250.00);
      expect(result?.orderStatus).toBe('shipped');
    });

    it('should select via in-memory adapter with filter', async () => {
      await harness.insertUser(generateSampleUser({ firstName: 'Alice', isActive: true }));
      await harness.insertUser(generateSampleUser({ firstName: 'Bob', isActive: false }));
      await harness.insertUser(generateSampleUser({ firstName: 'Carol', isActive: true }));

      const activeUsers = await harness.adapters.users.query({ isActive: true });

      expect(activeUsers.length).toBe(2);
      expect(activeUsers.every(u => u.isActive)).toBe(true);
    });

    it('should select with predicate function', async () => {
      await harness.insertUser(generateSampleUser({ firstName: 'Alice', accountBalance: 100 }));
      await harness.insertUser(generateSampleUser({ firstName: 'Bob', accountBalance: 500 }));
      await harness.insertUser(generateSampleUser({ firstName: 'Carol', accountBalance: 1000 }));

      const highBalanceUsers = await harness.adapters.users.queryWithPredicate(
        user => user.accountBalance > 250
      );

      expect(highBalanceUsers.length).toBe(2);
      expect(highBalanceUsers.map(u => u.firstName).sort()).toEqual(['Bob', 'Carol']);
    });

    it('should count records', async () => {
      await harness.insertUser(generateSampleUser());
      await harness.insertUser(generateSampleUser());
      await harness.insertUser(generateSampleUser());

      const count = await harness.adapters.users.count();

      expect(count).toBe(3);
    });

    it('should count with filter', async () => {
      await harness.insertUser(generateSampleUser({ isActive: true }));
      await harness.insertUser(generateSampleUser({ isActive: true }));
      await harness.insertUser(generateSampleUser({ isActive: false }));

      const activeCount = await harness.adapters.users.count({ isActive: true });

      expect(activeCount).toBe(2);
    });
  });

  // ===========================================================================
  // UPDATE Operations
  // ===========================================================================

  describe('UPDATE', () => {
    it('should update user by ID in B-tree', async () => {
      const user = await harness.insertUser(generateSampleUser({
        firstName: 'Original',
        accountBalance: 100,
      }));

      // Update in B-tree
      const updatedUser: UserRecord = {
        ...user,
        firstName: 'Updated',
        accountBalance: 200,
      };
      await harness.usersBTree.set(`user:${user.id}`, updatedUser);

      // Verify update
      const result = await harness.usersBTree.get(`user:${user.id}`);
      expect(result?.firstName).toBe('Updated');
      expect(result?.accountBalance).toBe(200);
    });

    it('should update multiple fields at once', async () => {
      const user = await harness.insertUser(generateSampleUser({
        firstName: 'Before',
        lastName: 'Update',
        isActive: true,
        accountBalance: 50,
      }));

      const updatedUser: UserRecord = {
        ...user,
        firstName: 'After',
        lastName: 'Change',
        isActive: false,
        accountBalance: 75.50,
      };
      await harness.usersBTree.set(`user:${user.id}`, updatedUser);

      const result = await harness.usersBTree.get(`user:${user.id}`);
      expect(result?.firstName).toBe('After');
      expect(result?.lastName).toBe('Change');
      expect(result?.isActive).toBe(false);
      expect(result?.accountBalance).toBe(75.50);
    });

    it('should update via in-memory adapter with filter', async () => {
      await harness.insertUser(generateSampleUser({ firstName: 'Alice', isActive: true }));
      await harness.insertUser(generateSampleUser({ firstName: 'Bob', isActive: true }));

      const updated = await harness.adapters.users.update(
        { firstName: 'Alice' },
        { accountBalance: 999 }
      );

      expect(updated).toBe(1);

      const alice = await harness.adapters.users.query({ firstName: 'Alice' });
      expect(alice[0]?.accountBalance).toBe(999);

      // Bob should be unchanged
      const bob = await harness.adapters.users.query({ firstName: 'Bob' });
      expect(bob[0]?.accountBalance).not.toBe(999);
    });

    it('should update via predicate function', async () => {
      await harness.insertUser(generateSampleUser({ firstName: 'User1', accountBalance: 100 }));
      await harness.insertUser(generateSampleUser({ firstName: 'User2', accountBalance: 200 }));
      await harness.insertUser(generateSampleUser({ firstName: 'User3', accountBalance: 300 }));

      const updated = await harness.adapters.users.updateWithPredicate(
        user => user.accountBalance >= 200,
        { isActive: false }
      );

      expect(updated).toBe(2);

      const inactiveUsers = await harness.adapters.users.query({ isActive: false });
      expect(inactiveUsers.length).toBe(2);
    });

    it('should update order status', async () => {
      const user = await harness.insertUser(generateSampleUser());
      const order = await harness.insertOrder(generateSampleOrder(user.id, {
        orderStatus: 'pending',
      }));

      const updatedOrder: OrderRecord = {
        ...order,
        orderStatus: 'shipped',
      };
      await harness.ordersBTree.set(`order:${order.id}`, updatedOrder);

      const result = await harness.ordersBTree.get(`order:${order.id}`);
      expect(result?.orderStatus).toBe('shipped');
    });

    it('should update product stock quantity', async () => {
      const category = await harness.insertCategory(generateSampleCategory());
      const product = await harness.insertProduct(generateSampleProduct(category.id, {
        stockQuantity: 100,
      }));

      const updatedProduct: ProductRecord = {
        ...product,
        stockQuantity: 90, // Sold 10 items
      };
      await harness.productsBTree.set(`product:${product.id}`, updatedProduct);

      const result = await harness.productsBTree.get(`product:${product.id}`);
      expect(result?.stockQuantity).toBe(90);
    });
  });

  // ===========================================================================
  // DELETE Operations
  // ===========================================================================

  describe('DELETE', () => {
    it('should delete user by primary key', async () => {
      const user = await harness.insertUser(generateSampleUser());

      // Delete from B-tree
      const deleted = await harness.usersBTree.delete(`user:${user.id}`);

      expect(deleted).toBe(true);

      // Verify deletion
      const result = await harness.usersBTree.get(`user:${user.id}`);
      expect(result).toBeUndefined();
    });

    it('should return false when deleting non-existent key', async () => {
      const deleted = await harness.usersBTree.delete('user:999999');

      expect(deleted).toBe(false);
    });

    it('should delete via in-memory adapter with filter', async () => {
      await harness.insertUser(generateSampleUser({ firstName: 'ToDelete', isActive: false }));
      await harness.insertUser(generateSampleUser({ firstName: 'ToKeep', isActive: true }));

      const deleted = await harness.adapters.users.delete({ isActive: false });

      expect(deleted).toBe(1);

      const remaining = await harness.adapters.users.query({});
      expect(remaining.length).toBe(1);
      expect(remaining[0].firstName).toBe('ToKeep');
    });

    it('should delete via predicate function', async () => {
      await harness.insertUser(generateSampleUser({ firstName: 'User1', accountBalance: 0 }));
      await harness.insertUser(generateSampleUser({ firstName: 'User2', accountBalance: 100 }));
      await harness.insertUser(generateSampleUser({ firstName: 'User3', accountBalance: 0 }));

      // Delete users with zero balance
      const deleted = await harness.adapters.users.deleteWithPredicate(
        user => user.accountBalance === 0
      );

      expect(deleted).toBe(2);

      const remaining = await harness.adapters.users.query({});
      expect(remaining.length).toBe(1);
      expect(remaining[0].firstName).toBe('User2');
    });

    it('should delete order and preserve referential context', async () => {
      const user = await harness.insertUser(generateSampleUser());
      const order = await harness.insertOrder(generateSampleOrder(user.id));

      // Delete order
      const deleted = await harness.ordersBTree.delete(`order:${order.id}`);
      expect(deleted).toBe(true);

      // User should still exist
      const userResult = await harness.usersBTree.get(`user:${user.id}`);
      expect(userResult).toBeDefined();
    });

    it('should clear all entries', async () => {
      await harness.insertUser(generateSampleUser());
      await harness.insertUser(generateSampleUser());
      await harness.insertUser(generateSampleUser());

      await harness.usersBTree.clear();

      const count = await harness.usersBTree.count();
      expect(count).toBe(0);
    });
  });

  // ===========================================================================
  // Type Safety Verification
  // ===========================================================================

  describe('Type Safety', () => {
    it('should preserve TypeScript types on insert and select', async () => {
      const userData = {
        firstName: 'TypeTest',
        lastName: 'User',
        emailAddress: 'type@test.com',
        createdAt: new Date('2024-01-01'),
        isActive: true,
        accountBalance: 123.45,
      };

      const user = await harness.insertUser(userData);
      const result = await harness.usersBTree.get(`user:${user.id}`);

      // These type assertions verify TypeScript typing
      const firstName: string = result!.firstName;
      const lastName: string = result!.lastName;
      const emailAddress: string = result!.emailAddress;
      const isActive: boolean = result!.isActive;
      const accountBalance: number = result!.accountBalance;
      const id: number = result!.id;

      expect(typeof firstName).toBe('string');
      expect(typeof lastName).toBe('string');
      expect(typeof emailAddress).toBe('string');
      expect(typeof isActive).toBe('boolean');
      expect(typeof accountBalance).toBe('number');
      expect(typeof id).toBe('number');
    });

    it('should handle null values for nullable fields', async () => {
      const category = await harness.insertCategory({
        categoryName: 'Root Category',
        parentCategoryId: null, // Explicit null
      });

      const result = await harness.categoriesBTree.get(`category:${category.id}`);

      expect(result?.parentCategoryId).toBeNull();
    });

    it('should handle Date serialization correctly', async () => {
      const specificDate = new Date('2024-06-15T14:30:00.000Z');

      const user = await harness.insertUser(generateSampleUser({
        createdAt: specificDate,
      }));

      const result = await harness.usersBTree.get(`user:${user.id}`);

      // JSON serialization converts Date to string, so we need to compare as string
      // or reconstruct the Date
      expect(new Date(result!.createdAt).toISOString()).toBe(specificDate.toISOString());
    });

    it('should handle floating point numbers accurately', async () => {
      const category = await harness.insertCategory(generateSampleCategory());
      const product = await harness.insertProduct(generateSampleProduct(category.id, {
        unitPrice: 19.99,
      }));

      const result = await harness.productsBTree.get(`product:${product.id}`);

      // Verify precision is maintained
      expect(result?.unitPrice).toBe(19.99);
      expect(result?.unitPrice).toBeCloseTo(19.99, 2);
    });

    it('should handle boolean values correctly', async () => {
      const activeUser = await harness.insertUser(generateSampleUser({ isActive: true }));
      const inactiveUser = await harness.insertUser(generateSampleUser({ isActive: false }));

      const activeResult = await harness.usersBTree.get(`user:${activeUser.id}`);
      const inactiveResult = await harness.usersBTree.get(`user:${inactiveUser.id}`);

      expect(activeResult?.isActive).toBe(true);
      expect(activeResult?.isActive).not.toBe('true');

      expect(inactiveResult?.isActive).toBe(false);
      expect(inactiveResult?.isActive).not.toBe('false');
    });
  });

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('Edge Cases', () => {
    it('should handle empty string values', async () => {
      const user = await harness.insertUser(generateSampleUser({
        firstName: '',
        lastName: '',
      }));

      const result = await harness.usersBTree.get(`user:${user.id}`);

      expect(result?.firstName).toBe('');
      expect(result?.lastName).toBe('');
    });

    it('should handle very long string values', async () => {
      const longName = 'A'.repeat(10000);

      const user = await harness.insertUser(generateSampleUser({
        firstName: longName,
      }));

      const result = await harness.usersBTree.get(`user:${user.id}`);

      expect(result?.firstName).toBe(longName);
      expect(result?.firstName.length).toBe(10000);
    });

    it('should handle special characters in strings', async () => {
      const specialChars = "Hello 'World' \"Test\" `Backtick` \n\t\\Escape";

      const user = await harness.insertUser(generateSampleUser({
        firstName: specialChars,
      }));

      const result = await harness.usersBTree.get(`user:${user.id}`);

      expect(result?.firstName).toBe(specialChars);
    });

    it('should handle unicode characters', async () => {
      const unicodeName = '  ';

      const user = await harness.insertUser(generateSampleUser({
        firstName: unicodeName,
      }));

      const result = await harness.usersBTree.get(`user:${user.id}`);

      expect(result?.firstName).toBe(unicodeName);
    });

    it('should handle zero values', async () => {
      const user = await harness.insertUser(generateSampleUser({
        accountBalance: 0,
      }));

      const result = await harness.usersBTree.get(`user:${user.id}`);

      expect(result?.accountBalance).toBe(0);
      expect(result?.accountBalance).not.toBeNull();
      expect(result?.accountBalance).not.toBeUndefined();
    });

    it('should handle negative numbers', async () => {
      const user = await harness.insertUser(generateSampleUser({
        accountBalance: -50.25,
      }));

      const result = await harness.usersBTree.get(`user:${user.id}`);

      expect(result?.accountBalance).toBe(-50.25);
    });

    it('should handle concurrent inserts', async () => {
      const insertPromises = [];

      for (let i = 0; i < 10; i++) {
        insertPromises.push(
          harness.insertUser(generateSampleUser({ firstName: `Concurrent${i}` }))
        );
      }

      const users = await Promise.all(insertPromises);

      // All should have unique IDs
      const ids = users.map(u => u.id);
      expect(new Set(ids).size).toBe(10);

      // All should be retrievable
      for (const user of users) {
        const result = await harness.usersBTree.get(`user:${user.id}`);
        expect(result).toBeDefined();
      }
    });
  });
});
