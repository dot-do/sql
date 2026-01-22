/**
 * DoSQL Integration Tests - Query Operations
 *
 * Tests WHERE, JOIN, aggregates (COUNT, SUM, AVG), GROUP BY, ORDER BY, LIMIT.
 * All fields use camelCase naming convention.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  createTestHarness,
  seedTestData,
  collectAsync,
  type TestHarness,
  type UserRecord,
  type OrderRecord,
  type ProductRecord,
  type OrderItemRecord,
} from './setup.js';

describe('Query Operations', () => {
  let harness: TestHarness;
  let seedData: Awaited<ReturnType<typeof seedTestData>>;

  beforeEach(async () => {
    harness = await createTestHarness();
    seedData = await seedTestData(harness);
  });

  afterEach(async () => {
    await harness.cleanup();
  });

  // ===========================================================================
  // SELECT with WHERE
  // ===========================================================================

  describe('SELECT with WHERE', () => {
    it('should filter users by isActive = true', async () => {
      const activeUsers = await harness.adapters.users.query({ isActive: true });

      expect(activeUsers.length).toBe(2); // Alice and Bob
      expect(activeUsers.every(u => u.isActive)).toBe(true);
    });

    it('should filter users by isActive = false', async () => {
      const inactiveUsers = await harness.adapters.users.query({ isActive: false });

      expect(inactiveUsers.length).toBe(1); // Carol
      expect(inactiveUsers[0].firstName).toBe('Carol');
    });

    it('should filter by exact string match', async () => {
      const aliceUsers = await harness.adapters.users.query({ firstName: 'Alice' });

      expect(aliceUsers.length).toBe(1);
      expect(aliceUsers[0].lastName).toBe('Johnson');
    });

    it('should filter by numeric equality', async () => {
      const aliceOrders = await harness.adapters.orders.query({ userId: seedData.users[0].id });

      expect(aliceOrders.length).toBe(1);
    });

    it('should filter with predicate for greater than', async () => {
      const highBalanceUsers = await harness.adapters.users.queryWithPredicate(
        user => user.accountBalance > 200
      );

      expect(highBalanceUsers.length).toBe(2); // Alice (500) and Bob (250)
    });

    it('should filter with predicate for less than', async () => {
      const lowBalanceUsers = await harness.adapters.users.queryWithPredicate(
        user => user.accountBalance < 100
      );

      expect(lowBalanceUsers.length).toBe(1); // Carol (0)
    });

    it('should filter with complex predicate (AND)', async () => {
      const activeHighBalance = await harness.adapters.users.queryWithPredicate(
        user => user.isActive && user.accountBalance > 300
      );

      expect(activeHighBalance.length).toBe(1); // Alice
      expect(activeHighBalance[0].firstName).toBe('Alice');
    });

    it('should filter with complex predicate (OR)', async () => {
      const aliceOrInactive = await harness.adapters.users.queryWithPredicate(
        user => user.firstName === 'Alice' || !user.isActive
      );

      expect(aliceOrInactive.length).toBe(2); // Alice and Carol
    });

    it('should filter products by categoryId', async () => {
      const electronicsId = seedData.categories.find(c => c.categoryName === 'Electronics')!.id;
      const electronics = await harness.adapters.products.query({ categoryId: electronicsId });

      expect(electronics.length).toBe(2); // Laptop and Headphones
      expect(electronics.map(p => p.productName).sort()).toEqual(['Laptop Pro 15', 'Wireless Headphones']);
    });

    it('should filter by isAvailable boolean', async () => {
      const availableProducts = await harness.adapters.products.query({ isAvailable: true });

      expect(availableProducts.length).toBe(5); // All products are available
    });

    it('should return empty array when no matches', async () => {
      const noMatches = await harness.adapters.users.query({ firstName: 'NonExistent' });

      expect(noMatches).toEqual([]);
    });

    it('should filter by multiple criteria', async () => {
      // First filter by isActive, then by predicate on result
      const active = await harness.adapters.users.query({ isActive: true });
      const filtered = active.filter(u => u.accountBalance >= 500);

      expect(filtered.length).toBe(1);
      expect(filtered[0].firstName).toBe('Alice');
    });
  });

  // ===========================================================================
  // SELECT with JOIN (simulated via application-level joins)
  // ===========================================================================

  describe('SELECT with JOIN', () => {
    it('should perform inner join between orders and users', async () => {
      const orders = await harness.adapters.orders.query({});
      const users = await harness.adapters.users.query({});

      // Application-level join
      const ordersWithUsers = orders.map(order => {
        const user = users.find(u => u.id === order.userId);
        return {
          orderId: order.id,
          orderStatus: order.orderStatus,
          totalAmount: order.totalAmount,
          userId: order.userId,
          customerName: user ? `${user.firstName} ${user.lastName}` : null,
          customerEmail: user?.emailAddress,
        };
      });

      expect(ordersWithUsers.length).toBe(2);
      expect(ordersWithUsers.find(o => o.customerName === 'Alice Johnson')).toBeDefined();
      expect(ordersWithUsers.find(o => o.customerName === 'Bob Smith')).toBeDefined();
    });

    it('should perform join between orderItems, orders, and products', async () => {
      const orderItems = await harness.adapters.orderItems.query({});
      const products = await harness.adapters.products.query({});
      const orders = await harness.adapters.orders.query({});

      // Three-way join
      const detailedItems = orderItems.map(item => {
        const product = products.find(p => p.id === item.productId);
        const order = orders.find(o => o.id === item.orderId);
        return {
          itemId: item.id,
          productName: product?.productName,
          quantity: item.quantity,
          unitPrice: item.unitPrice,
          itemTotal: item.itemTotal,
          orderStatus: order?.orderStatus,
        };
      });

      expect(detailedItems.length).toBe(5); // Total order items
      expect(detailedItems.every(i => i.productName !== undefined)).toBe(true);
    });

    it('should perform left join (include orders without items)', async () => {
      // Create an order without items
      const user = seedData.users[0];
      const emptyOrder = await harness.insertOrder({
        userId: user.id,
        orderDate: new Date(),
        totalAmount: 0,
        shippingAddress: 'Test Address',
        orderStatus: 'draft',
      });

      const orders = await harness.adapters.orders.query({});
      const orderItems = await harness.adapters.orderItems.query({});

      // Left join simulation
      const ordersWithItems = orders.map(order => {
        const items = orderItems.filter(item => item.orderId === order.id);
        return {
          orderId: order.id,
          orderStatus: order.orderStatus,
          itemCount: items.length,
          hasItems: items.length > 0,
        };
      });

      expect(ordersWithItems.length).toBe(3);
      expect(ordersWithItems.find(o => o.orderId === emptyOrder.id)?.hasItems).toBe(false);
    });

    it('should join products with categories', async () => {
      const products = await harness.adapters.products.query({});
      const categories = await harness.adapters.categories.query({});

      const productsWithCategories = products.map(product => {
        const category = categories.find(c => c.id === product.categoryId);
        return {
          productName: product.productName,
          categoryName: category?.categoryName,
          unitPrice: product.unitPrice,
        };
      });

      expect(productsWithCategories.length).toBe(5);
      const laptop = productsWithCategories.find(p => p.productName === 'Laptop Pro 15');
      expect(laptop?.categoryName).toBe('Electronics');
    });

    it('should calculate order totals via join', async () => {
      const orders = await harness.adapters.orders.query({});
      const orderItems = await harness.adapters.orderItems.query({});

      const orderTotals = orders.map(order => {
        const items = orderItems.filter(item => item.orderId === order.id);
        const calculatedTotal = items.reduce((sum, item) => sum + item.itemTotal, 0);
        return {
          orderId: order.id,
          storedTotal: order.totalAmount,
          calculatedTotal,
          itemCount: items.length,
        };
      });

      // Verify totals match
      for (const order of orderTotals) {
        if (order.itemCount > 0) {
          expect(order.storedTotal).toBeCloseTo(order.calculatedTotal, 2);
        }
      }
    });
  });

  // ===========================================================================
  // SELECT with Aggregates
  // ===========================================================================

  describe('SELECT with Aggregates', () => {
    describe('COUNT', () => {
      it('should count all users', async () => {
        const count = await harness.adapters.users.count();
        expect(count).toBe(3);
      });

      it('should count with filter', async () => {
        const activeCount = await harness.adapters.users.count({ isActive: true });
        expect(activeCount).toBe(2);
      });

      it('should count with predicate', async () => {
        const highBalanceCount = await harness.adapters.users.countWithPredicate(
          user => user.accountBalance > 100
        );
        expect(highBalanceCount).toBe(2); // Alice and Bob
      });

      it('should count order items per order', async () => {
        const orderItems = await harness.adapters.orderItems.query({});
        const orders = await harness.adapters.orders.query({});

        const itemCounts = orders.map(order => ({
          orderId: order.id,
          itemCount: orderItems.filter(i => i.orderId === order.id).length,
        }));

        expect(itemCounts.find(o => o.orderId === seedData.orders[0].id)?.itemCount).toBe(2);
        expect(itemCounts.find(o => o.orderId === seedData.orders[1].id)?.itemCount).toBe(3);
      });

      it('should count products per category', async () => {
        const products = await harness.adapters.products.query({});
        const categories = await harness.adapters.categories.query({});

        const productCounts = categories.map(cat => ({
          categoryName: cat.categoryName,
          productCount: products.filter(p => p.categoryId === cat.id).length,
        }));

        expect(productCounts.find(c => c.categoryName === 'Electronics')?.productCount).toBe(2);
        expect(productCounts.find(c => c.categoryName === 'Clothing')?.productCount).toBe(2);
        expect(productCounts.find(c => c.categoryName === 'Books')?.productCount).toBe(1);
      });
    });

    describe('SUM', () => {
      it('should sum all user account balances', async () => {
        const users = await harness.adapters.users.query({});
        const totalBalance = users.reduce((sum, u) => sum + u.accountBalance, 0);

        expect(totalBalance).toBe(750); // 500 + 250 + 0
      });

      it('should sum order totals', async () => {
        const orders = await harness.adapters.orders.query({});
        const totalOrderValue = orders.reduce((sum, o) => sum + o.totalAmount, 0);

        // Alice: 1299.99 + 199.99 = 1499.98
        // Bob: 74.97 + 119.98 + 14.99 = 209.94
        expect(totalOrderValue).toBeCloseTo(1499.98 + 209.94, 2);
      });

      it('should sum order item totals for specific order', async () => {
        const orderItems = await harness.adapters.orderItems.query({});
        const bobsOrderId = seedData.orders[1].id;

        const bobsItemsTotal = orderItems
          .filter(i => i.orderId === bobsOrderId)
          .reduce((sum, i) => sum + i.itemTotal, 0);

        // 3 * 24.99 + 2 * 59.99 + 1 * 14.99
        expect(bobsItemsTotal).toBeCloseTo(74.97 + 119.98 + 14.99, 2);
      });

      it('should sum stock quantities', async () => {
        const products = await harness.adapters.products.query({});
        const totalStock = products.reduce((sum, p) => sum + p.stockQuantity, 0);

        expect(totalStock).toBe(575); // 50 + 200 + 100 + 75 + 150
      });
    });

    describe('AVG', () => {
      it('should calculate average account balance', async () => {
        const users = await harness.adapters.users.query({});
        const avgBalance = users.reduce((sum, u) => sum + u.accountBalance, 0) / users.length;

        expect(avgBalance).toBe(250); // (500 + 250 + 0) / 3
      });

      it('should calculate average product price', async () => {
        const products = await harness.adapters.products.query({});
        const avgPrice = products.reduce((sum, p) => sum + p.unitPrice, 0) / products.length;

        // (1299.99 + 24.99 + 14.99 + 199.99 + 59.99) / 5
        expect(avgPrice).toBeCloseTo(319.99, 2);
      });

      it('should calculate average order total', async () => {
        const orders = await harness.adapters.orders.query({});
        const avgOrderTotal = orders.reduce((sum, o) => sum + o.totalAmount, 0) / orders.length;

        // (1499.98 + 209.94) / 2
        expect(avgOrderTotal).toBeCloseTo(854.96, 2);
      });

      it('should calculate average with filter', async () => {
        const users = await harness.adapters.users.query({ isActive: true });
        const avgActiveBalance = users.reduce((sum, u) => sum + u.accountBalance, 0) / users.length;

        expect(avgActiveBalance).toBe(375); // (500 + 250) / 2
      });
    });

    describe('MIN/MAX', () => {
      it('should find minimum account balance', async () => {
        const users = await harness.adapters.users.query({});
        const minBalance = Math.min(...users.map(u => u.accountBalance));

        expect(minBalance).toBe(0); // Carol
      });

      it('should find maximum account balance', async () => {
        const users = await harness.adapters.users.query({});
        const maxBalance = Math.max(...users.map(u => u.accountBalance));

        expect(maxBalance).toBe(500); // Alice
      });

      it('should find cheapest product', async () => {
        const products = await harness.adapters.products.query({});
        const cheapest = products.reduce((min, p) =>
          p.unitPrice < min.unitPrice ? p : min
        );

        expect(cheapest.productName).toBe('Mystery Novel');
        expect(cheapest.unitPrice).toBe(14.99);
      });

      it('should find most expensive product', async () => {
        const products = await harness.adapters.products.query({});
        const expensive = products.reduce((max, p) =>
          p.unitPrice > max.unitPrice ? p : max
        );

        expect(expensive.productName).toBe('Laptop Pro 15');
        expect(expensive.unitPrice).toBe(1299.99);
      });
    });
  });

  // ===========================================================================
  // SELECT with GROUP BY
  // ===========================================================================

  describe('SELECT with GROUP BY', () => {
    it('should group users by isActive status', async () => {
      const users = await harness.adapters.users.query({});

      const grouped = users.reduce((acc, user) => {
        const key = user.isActive ? 'active' : 'inactive';
        acc[key] = acc[key] || [];
        acc[key].push(user);
        return acc;
      }, {} as Record<string, UserRecord[]>);

      expect(grouped.active.length).toBe(2);
      expect(grouped.inactive.length).toBe(1);
    });

    it('should group products by categoryId with count', async () => {
      const products = await harness.adapters.products.query({});
      const categories = await harness.adapters.categories.query({});

      const grouped = products.reduce((acc, product) => {
        const catId = product.categoryId;
        acc[catId] = acc[catId] || { count: 0, products: [] };
        acc[catId].count++;
        acc[catId].products.push(product);
        return acc;
      }, {} as Record<number, { count: number; products: ProductRecord[] }>);

      const electronicsId = categories.find(c => c.categoryName === 'Electronics')!.id;
      expect(grouped[electronicsId].count).toBe(2);
    });

    it('should group orders by status with totals', async () => {
      const orders = await harness.adapters.orders.query({});

      const grouped = orders.reduce((acc, order) => {
        const status = order.orderStatus;
        acc[status] = acc[status] || { count: 0, totalAmount: 0 };
        acc[status].count++;
        acc[status].totalAmount += order.totalAmount;
        return acc;
      }, {} as Record<string, { count: number; totalAmount: number }>);

      expect(grouped.shipped?.count).toBe(1);
      expect(grouped.pending?.count).toBe(1);
    });

    it('should group order items by orderId with sum', async () => {
      const orderItems = await harness.adapters.orderItems.query({});

      const grouped = orderItems.reduce((acc, item) => {
        const orderId = item.orderId;
        acc[orderId] = acc[orderId] || { itemCount: 0, totalQuantity: 0, totalAmount: 0 };
        acc[orderId].itemCount++;
        acc[orderId].totalQuantity += item.quantity;
        acc[orderId].totalAmount += item.itemTotal;
        return acc;
      }, {} as Record<number, { itemCount: number; totalQuantity: number; totalAmount: number }>);

      const bobsOrderId = seedData.orders[1].id;
      expect(grouped[bobsOrderId].itemCount).toBe(3);
      expect(grouped[bobsOrderId].totalQuantity).toBe(6); // 3 + 2 + 1
    });

    it('should group by multiple fields', async () => {
      const users = await harness.adapters.users.query({});

      // Group by isActive and balance tier
      const grouped = users.reduce((acc, user) => {
        const key = `${user.isActive ? 'active' : 'inactive'}_${user.accountBalance >= 250 ? 'high' : 'low'}`;
        acc[key] = acc[key] || [];
        acc[key].push(user);
        return acc;
      }, {} as Record<string, UserRecord[]>);

      expect(grouped['active_high']?.length).toBe(2); // Alice, Bob
      expect(grouped['inactive_low']?.length).toBe(1); // Carol
    });
  });

  // ===========================================================================
  // SELECT with ORDER BY
  // ===========================================================================

  describe('SELECT with ORDER BY', () => {
    it('should order users by firstName ascending', async () => {
      const users = await harness.adapters.users.query({}, {
        orderBy: 'firstName',
        orderDirection: 'asc',
      });

      expect(users.map(u => u.firstName)).toEqual(['Alice', 'Bob', 'Carol']);
    });

    it('should order users by firstName descending', async () => {
      const users = await harness.adapters.users.query({}, {
        orderBy: 'firstName',
        orderDirection: 'desc',
      });

      expect(users.map(u => u.firstName)).toEqual(['Carol', 'Bob', 'Alice']);
    });

    it('should order by numeric field ascending', async () => {
      const users = await harness.adapters.users.query({}, {
        orderBy: 'accountBalance',
        orderDirection: 'asc',
      });

      expect(users.map(u => u.accountBalance)).toEqual([0, 250, 500]);
    });

    it('should order by numeric field descending', async () => {
      const users = await harness.adapters.users.query({}, {
        orderBy: 'accountBalance',
        orderDirection: 'desc',
      });

      expect(users.map(u => u.accountBalance)).toEqual([500, 250, 0]);
    });

    it('should order products by unitPrice', async () => {
      const products = await harness.adapters.products.query({}, {
        orderBy: 'unitPrice',
        orderDirection: 'asc',
      });

      expect(products[0].productName).toBe('Mystery Novel');
      expect(products[products.length - 1].productName).toBe('Laptop Pro 15');
    });

    it('should order with filter', async () => {
      const activeUsers = await harness.adapters.users.query({ isActive: true }, {
        orderBy: 'accountBalance',
        orderDirection: 'desc',
      });

      expect(activeUsers.map(u => u.firstName)).toEqual(['Alice', 'Bob']);
    });

    it('should maintain stable sort order', async () => {
      // Multiple runs should return same order
      const users1 = await harness.adapters.users.query({}, {
        orderBy: 'isActive',
        orderDirection: 'asc',
      });
      const users2 = await harness.adapters.users.query({}, {
        orderBy: 'isActive',
        orderDirection: 'asc',
      });

      expect(users1.map(u => u.id)).toEqual(users2.map(u => u.id));
    });
  });

  // ===========================================================================
  // SELECT with LIMIT and OFFSET
  // ===========================================================================

  describe('SELECT with LIMIT and OFFSET', () => {
    it('should limit results', async () => {
      const users = await harness.adapters.users.query({}, {
        limit: 2,
      });

      expect(users.length).toBe(2);
    });

    it('should limit with order', async () => {
      const topUsers = await harness.adapters.users.query({}, {
        orderBy: 'accountBalance',
        orderDirection: 'desc',
        limit: 2,
      });

      expect(topUsers.length).toBe(2);
      expect(topUsers[0].firstName).toBe('Alice');
      expect(topUsers[1].firstName).toBe('Bob');
    });

    it('should offset results', async () => {
      const users = await harness.adapters.users.query({}, {
        orderBy: 'firstName',
        orderDirection: 'asc',
        offset: 1,
      });

      expect(users.length).toBe(2);
      expect(users[0].firstName).toBe('Bob');
    });

    it('should combine limit and offset for pagination', async () => {
      const products = await harness.adapters.products.query({}, {
        orderBy: 'unitPrice',
        orderDirection: 'asc',
      });

      // Page 1: first 2
      const page1 = await harness.adapters.products.query({}, {
        orderBy: 'unitPrice',
        orderDirection: 'asc',
        limit: 2,
        offset: 0,
      });

      // Page 2: next 2
      const page2 = await harness.adapters.products.query({}, {
        orderBy: 'unitPrice',
        orderDirection: 'asc',
        limit: 2,
        offset: 2,
      });

      // Page 3: last 1
      const page3 = await harness.adapters.products.query({}, {
        orderBy: 'unitPrice',
        orderDirection: 'asc',
        limit: 2,
        offset: 4,
      });

      expect(page1.length).toBe(2);
      expect(page2.length).toBe(2);
      expect(page3.length).toBe(1);

      // No duplicates across pages
      const allIds = [...page1, ...page2, ...page3].map(p => p.id);
      expect(new Set(allIds).size).toBe(5);
    });

    it('should handle offset beyond result count', async () => {
      const users = await harness.adapters.users.query({}, {
        offset: 100,
      });

      expect(users.length).toBe(0);
    });

    it('should handle limit larger than result count', async () => {
      const users = await harness.adapters.users.query({}, {
        limit: 100,
      });

      expect(users.length).toBe(3);
    });

    it('should work with filter, order, limit, and offset together', async () => {
      const result = await harness.adapters.products.query({ isAvailable: true }, {
        orderBy: 'unitPrice',
        orderDirection: 'desc',
        limit: 2,
        offset: 1,
      });

      // Skip expensive (Laptop), get next 2 (Headphones, Jeans)
      expect(result.length).toBe(2);
      expect(result[0].productName).toBe('Wireless Headphones');
      expect(result[1].productName).toBe('Denim Jeans');
    });
  });

  // ===========================================================================
  // Complex Query Combinations
  // ===========================================================================

  describe('Complex Query Combinations', () => {
    it('should combine filter, join, and aggregate', async () => {
      // Get total order value per active user
      const users = await harness.adapters.users.query({ isActive: true });
      const orders = await harness.adapters.orders.query({});

      const userOrderTotals = users.map(user => {
        const userOrders = orders.filter(o => o.userId === user.id);
        const totalSpent = userOrders.reduce((sum, o) => sum + o.totalAmount, 0);
        return {
          userId: user.id,
          userName: `${user.firstName} ${user.lastName}`,
          orderCount: userOrders.length,
          totalSpent,
        };
      });

      expect(userOrderTotals.length).toBe(2);
      const alice = userOrderTotals.find(u => u.userName === 'Alice Johnson');
      expect(alice?.orderCount).toBe(1);
      expect(alice?.totalSpent).toBeCloseTo(1499.98, 2);
    });

    it('should find top spending users', async () => {
      const users = await harness.adapters.users.query({});
      const orders = await harness.adapters.orders.query({});

      // Calculate spending per user
      const spending = users.map(user => ({
        user,
        totalSpent: orders
          .filter(o => o.userId === user.id)
          .reduce((sum, o) => sum + o.totalAmount, 0),
      }));

      // Sort by spending and get top 2
      const topSpenders = spending
        .sort((a, b) => b.totalSpent - a.totalSpent)
        .slice(0, 2);

      expect(topSpenders[0].user.firstName).toBe('Alice');
      expect(topSpenders[1].user.firstName).toBe('Bob');
    });

    it('should find products with low stock in popular categories', async () => {
      const products = await harness.adapters.products.query({});
      const orderItems = await harness.adapters.orderItems.query({});

      // Count sales per product
      const salesByProduct = orderItems.reduce((acc, item) => {
        acc[item.productId] = (acc[item.productId] || 0) + item.quantity;
        return acc;
      }, {} as Record<number, number>);

      // Find products with sales but low stock
      const lowStockPopular = products
        .filter(p => (salesByProduct[p.id] || 0) > 0 && p.stockQuantity < 100)
        .map(p => ({
          productName: p.productName,
          stockQuantity: p.stockQuantity,
          soldQuantity: salesByProduct[p.id] || 0,
        }));

      expect(lowStockPopular.length).toBeGreaterThan(0);
    });

    it('should generate order summary report', async () => {
      const orders = await harness.adapters.orders.query({});
      const orderItems = await harness.adapters.orderItems.query({});
      const users = await harness.adapters.users.query({});
      const products = await harness.adapters.products.query({});

      const report = orders.map(order => {
        const user = users.find(u => u.id === order.userId);
        const items = orderItems.filter(i => i.orderId === order.id);
        const itemDetails = items.map(item => {
          const product = products.find(p => p.id === item.productId);
          return {
            productName: product?.productName,
            quantity: item.quantity,
            lineTotal: item.itemTotal,
          };
        });

        return {
          orderId: order.id,
          customerName: `${user?.firstName} ${user?.lastName}`,
          orderDate: order.orderDate,
          status: order.orderStatus,
          itemCount: items.length,
          totalQuantity: items.reduce((sum, i) => sum + i.quantity, 0),
          orderTotal: order.totalAmount,
          items: itemDetails,
        };
      });

      expect(report.length).toBe(2);
      expect(report.every(r => r.customerName !== undefined)).toBe(true);
      expect(report.every(r => r.items.length > 0)).toBe(true);
    });
  });
});
