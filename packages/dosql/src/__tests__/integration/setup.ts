/**
 * DoSQL Integration Test Harness
 *
 * Provides a complete test environment with:
 * - In-memory FSX backend
 * - B-tree for row storage
 * - Columnar storage for analytics
 * - WAL for durability
 * - CDC for streaming
 * - COW backend for time travel
 */

import {
  MemoryFSXBackend,
  createCOWBackend,
  type COWBackend,
  type FSXBackend,
} from '../../fsx/index.js';

import {
  createBTree,
  StringKeyCodec,
  JsonValueCodec,
  NumberKeyCodec,
  type BTree,
} from '../../btree/index.js';

import {
  ColumnarWriter,
  ColumnarReader,
  type ColumnarTableSchema,
  type RowGroup,
} from '../../columnar/index.js';

import {
  createWALWriter,
  createWALReader,
  createCheckpointManager,
  type WALWriter,
  type WALReader,
  type CheckpointManager,
  type WALEntry,
} from '../../wal/index.js';

import {
  createCDCSubscription,
  type CDCSubscription,
} from '../../cdc/index.js';

import {
  createInMemoryAdapter,
  createInMemorySqlExecutor,
  createInMemoryTransactionManager,
  createDatabaseContext,
  type StorageAdapter,
  type DatabaseContext,
} from '../../proc/context.js';

import type { DatabaseSchema, TableSchema } from '../../parser.js';

// =============================================================================
// Test Schema Definitions (using camelCase)
// =============================================================================

/**
 * User table schema with camelCase fields
 */
export interface UserRecord {
  id: number;
  firstName: string;
  lastName: string;
  emailAddress: string;
  createdAt: Date;
  isActive: boolean;
  accountBalance: number;
}

/**
 * Order table schema with camelCase fields
 */
export interface OrderRecord {
  id: number;
  userId: number;
  orderDate: Date;
  totalAmount: number;
  shippingAddress: string;
  orderStatus: string;
}

/**
 * Product table schema with camelCase fields
 */
export interface ProductRecord {
  id: number;
  productName: string;
  categoryId: number;
  unitPrice: number;
  stockQuantity: number;
  isAvailable: boolean;
}

/**
 * OrderItem table schema with camelCase fields
 */
export interface OrderItemRecord {
  id: number;
  orderId: number;
  productId: number;
  quantity: number;
  unitPrice: number;
  itemTotal: number;
}

/**
 * Category table schema with camelCase fields
 */
export interface CategoryRecord {
  id: number;
  categoryName: string;
  parentCategoryId: number | null;
}

/**
 * Database schema type for type-level SQL parsing
 */
export interface TestDBSchema extends DatabaseSchema {
  users: {
    id: 'number';
    firstName: 'string';
    lastName: 'string';
    emailAddress: 'string';
    createdAt: 'Date';
    isActive: 'boolean';
    accountBalance: 'number';
  };
  orders: {
    id: 'number';
    userId: 'number';
    orderDate: 'Date';
    totalAmount: 'number';
    shippingAddress: 'string';
    orderStatus: 'string';
  };
  products: {
    id: 'number';
    productName: 'string';
    categoryId: 'number';
    unitPrice: 'number';
    stockQuantity: 'number';
    isAvailable: 'boolean';
  };
  orderItems: {
    id: 'number';
    orderId: 'number';
    productId: 'number';
    quantity: 'number';
    unitPrice: 'number';
    itemTotal: 'number';
  };
  categories: {
    id: 'number';
    categoryName: 'string';
    parentCategoryId: 'number';
  };
}

// =============================================================================
// Columnar Schema Definitions
// =============================================================================

export const usersColumnarSchema: ColumnarTableSchema = {
  tableName: 'users',
  columns: [
    { name: 'id', dataType: 'int32', nullable: false },
    { name: 'firstName', dataType: 'string', nullable: false },
    { name: 'lastName', dataType: 'string', nullable: false },
    { name: 'emailAddress', dataType: 'string', nullable: false },
    { name: 'createdAt', dataType: 'timestamp', nullable: false },
    { name: 'isActive', dataType: 'boolean', nullable: false },
    { name: 'accountBalance', dataType: 'float64', nullable: false },
  ],
};

export const ordersColumnarSchema: ColumnarTableSchema = {
  tableName: 'orders',
  columns: [
    { name: 'id', dataType: 'int32', nullable: false },
    { name: 'userId', dataType: 'int32', nullable: false },
    { name: 'orderDate', dataType: 'timestamp', nullable: false },
    { name: 'totalAmount', dataType: 'float64', nullable: false },
    { name: 'shippingAddress', dataType: 'string', nullable: false },
    { name: 'orderStatus', dataType: 'string', nullable: false },
  ],
};

export const productsColumnarSchema: ColumnarTableSchema = {
  tableName: 'products',
  columns: [
    { name: 'id', dataType: 'int32', nullable: false },
    { name: 'productName', dataType: 'string', nullable: false },
    { name: 'categoryId', dataType: 'int32', nullable: false },
    { name: 'unitPrice', dataType: 'float64', nullable: false },
    { name: 'stockQuantity', dataType: 'int32', nullable: false },
    { name: 'isAvailable', dataType: 'boolean', nullable: false },
  ],
};

// =============================================================================
// Test Harness
// =============================================================================

/**
 * Complete test environment with all DoSQL components
 */
export interface TestHarness {
  // Storage backends
  fsx: MemoryFSXBackend;
  cowBackend: COWBackend;

  // B-tree stores (hot data)
  usersBTree: BTree<string, UserRecord>;
  ordersBTree: BTree<string, OrderRecord>;
  productsBTree: BTree<string, ProductRecord>;
  orderItemsBTree: BTree<string, OrderItemRecord>;
  categoriesBTree: BTree<string, CategoryRecord>;

  // Columnar writers (for compaction)
  usersColumnar: ColumnarWriter;
  ordersColumnar: ColumnarWriter;

  // WAL
  walWriter: WALWriter;
  walReader: WALReader;
  checkpointManager: CheckpointManager;

  // CDC
  cdcSubscription: CDCSubscription;

  // In-memory adapters for procedure context
  adapters: {
    users: StorageAdapter<UserRecord>;
    orders: StorageAdapter<OrderRecord>;
    products: StorageAdapter<ProductRecord>;
    orderItems: StorageAdapter<OrderItemRecord>;
    categories: StorageAdapter<CategoryRecord>;
  };

  // Database context for procedures
  dbContext: DatabaseContext<TestDBSchema>;

  // Helper methods
  insertUser: (user: Omit<UserRecord, 'id'>) => Promise<UserRecord>;
  insertOrder: (order: Omit<OrderRecord, 'id'>) => Promise<OrderRecord>;
  insertProduct: (product: Omit<ProductRecord, 'id'>) => Promise<ProductRecord>;
  insertOrderItem: (item: Omit<OrderItemRecord, 'id'>) => Promise<OrderItemRecord>;
  insertCategory: (category: Omit<CategoryRecord, 'id'>) => Promise<CategoryRecord>;

  // Cleanup
  cleanup: () => Promise<void>;
}

/**
 * Create a fresh test harness with all components initialized
 */
export async function createTestHarness(): Promise<TestHarness> {
  // Create base storage
  const fsx = new MemoryFSXBackend();

  // Create COW backend for time travel
  const cowBackend = await createCOWBackend(fsx);

  // Create B-trees for each table
  const usersBTree = createBTree<string, UserRecord>(fsx, StringKeyCodec, JsonValueCodec, {
    pagePrefix: 'btree/users/',
  });
  await usersBTree.init();

  const ordersBTree = createBTree<string, OrderRecord>(fsx, StringKeyCodec, JsonValueCodec, {
    pagePrefix: 'btree/orders/',
  });
  await ordersBTree.init();

  const productsBTree = createBTree<string, ProductRecord>(fsx, StringKeyCodec, JsonValueCodec, {
    pagePrefix: 'btree/products/',
  });
  await productsBTree.init();

  const orderItemsBTree = createBTree<string, OrderItemRecord>(fsx, StringKeyCodec, JsonValueCodec, {
    pagePrefix: 'btree/orderItems/',
  });
  await orderItemsBTree.init();

  const categoriesBTree = createBTree<string, CategoryRecord>(fsx, StringKeyCodec, JsonValueCodec, {
    pagePrefix: 'btree/categories/',
  });
  await categoriesBTree.init();

  // Create columnar writers
  const usersColumnar = new ColumnarWriter(usersColumnarSchema, {
    targetRowsPerGroup: 100, // Small for testing
  });

  const ordersColumnar = new ColumnarWriter(ordersColumnarSchema, {
    targetRowsPerGroup: 100,
  });

  // Create WAL components
  const walWriter = createWALWriter(fsx);
  const walReader = createWALReader(fsx);
  const checkpointManager = createCheckpointManager(fsx);

  // Create CDC subscription
  const cdcSubscription = createCDCSubscription(walReader);

  // Create in-memory adapters with initial empty data
  const usersAdapter = createInMemoryAdapter<UserRecord>([]);
  const ordersAdapter = createInMemoryAdapter<OrderRecord>([]);
  const productsAdapter = createInMemoryAdapter<ProductRecord>([]);
  const orderItemsAdapter = createInMemoryAdapter<OrderItemRecord>([]);
  const categoriesAdapter = createInMemoryAdapter<CategoryRecord>([]);

  const adapters = {
    users: usersAdapter,
    orders: ordersAdapter,
    products: productsAdapter,
    orderItems: orderItemsAdapter,
    categories: categoriesAdapter,
  };

  // Create adapter map for SQL executor
  const adapterMap = new Map<string, StorageAdapter<Record<string, unknown>>>(
    Object.entries(adapters) as [string, StorageAdapter<Record<string, unknown>>][]
  );

  // Create database context
  const dbContext = createDatabaseContext<TestDBSchema>({
    adapters: adapters as any,
    sqlExecutor: createInMemorySqlExecutor(adapterMap),
    transactionManager: createInMemoryTransactionManager(),
  });

  // ID counters for each table
  let userIdCounter = 0;
  let orderIdCounter = 0;
  let productIdCounter = 0;
  let orderItemIdCounter = 0;
  let categoryIdCounter = 0;

  // Helper functions
  const insertUser = async (userData: Omit<UserRecord, 'id'>): Promise<UserRecord> => {
    const user: UserRecord = { ...userData, id: ++userIdCounter };
    await usersBTree.set(`user:${user.id}`, user);

    // Also write to WAL
    await walWriter.append({
      timestamp: Date.now(),
      txnId: `txn_${Date.now()}`,
      op: 'INSERT',
      table: 'users',
      after: new TextEncoder().encode(JSON.stringify(user)),
    });

    // Sync to in-memory adapter
    await usersAdapter.insert(userData);

    return user;
  };

  const insertOrder = async (orderData: Omit<OrderRecord, 'id'>): Promise<OrderRecord> => {
    const order: OrderRecord = { ...orderData, id: ++orderIdCounter };
    await ordersBTree.set(`order:${order.id}`, order);

    await walWriter.append({
      timestamp: Date.now(),
      txnId: `txn_${Date.now()}`,
      op: 'INSERT',
      table: 'orders',
      after: new TextEncoder().encode(JSON.stringify(order)),
    });

    await ordersAdapter.insert(orderData);

    return order;
  };

  const insertProduct = async (productData: Omit<ProductRecord, 'id'>): Promise<ProductRecord> => {
    const product: ProductRecord = { ...productData, id: ++productIdCounter };
    await productsBTree.set(`product:${product.id}`, product);

    await walWriter.append({
      timestamp: Date.now(),
      txnId: `txn_${Date.now()}`,
      op: 'INSERT',
      table: 'products',
      after: new TextEncoder().encode(JSON.stringify(product)),
    });

    await productsAdapter.insert(productData);

    return product;
  };

  const insertOrderItem = async (
    itemData: Omit<OrderItemRecord, 'id'>
  ): Promise<OrderItemRecord> => {
    const item: OrderItemRecord = { ...itemData, id: ++orderItemIdCounter };
    await orderItemsBTree.set(`orderItem:${item.id}`, item);

    await walWriter.append({
      timestamp: Date.now(),
      txnId: `txn_${Date.now()}`,
      op: 'INSERT',
      table: 'orderItems',
      after: new TextEncoder().encode(JSON.stringify(item)),
    });

    await orderItemsAdapter.insert(itemData);

    return item;
  };

  const insertCategory = async (
    categoryData: Omit<CategoryRecord, 'id'>
  ): Promise<CategoryRecord> => {
    const category: CategoryRecord = { ...categoryData, id: ++categoryIdCounter };
    await categoriesBTree.set(`category:${category.id}`, category);

    await walWriter.append({
      timestamp: Date.now(),
      txnId: `txn_${Date.now()}`,
      op: 'INSERT',
      table: 'categories',
      after: new TextEncoder().encode(JSON.stringify(category)),
    });

    await categoriesAdapter.insert(categoryData);

    return category;
  };

  const cleanup = async (): Promise<void> => {
    await walWriter.flush();
    fsx.clear();
  };

  return {
    fsx,
    cowBackend,
    usersBTree,
    ordersBTree,
    productsBTree,
    orderItemsBTree,
    categoriesBTree,
    usersColumnar,
    ordersColumnar,
    walWriter,
    walReader,
    checkpointManager,
    cdcSubscription,
    adapters,
    dbContext,
    insertUser,
    insertOrder,
    insertProduct,
    insertOrderItem,
    insertCategory,
    cleanup,
  };
}

// =============================================================================
// Sample Data Generators
// =============================================================================

/**
 * Generate sample user data
 */
export function generateSampleUser(overrides?: Partial<Omit<UserRecord, 'id'>>): Omit<UserRecord, 'id'> {
  const timestamp = Date.now();
  return {
    firstName: 'John',
    lastName: 'Doe',
    emailAddress: `user_${timestamp}@example.com`,
    createdAt: new Date(),
    isActive: true,
    accountBalance: 100.0,
    ...overrides,
  };
}

/**
 * Generate sample order data
 */
export function generateSampleOrder(
  userId: number,
  overrides?: Partial<Omit<OrderRecord, 'id'>>
): Omit<OrderRecord, 'id'> {
  return {
    userId,
    orderDate: new Date(),
    totalAmount: 0,
    shippingAddress: '123 Main St, City, State 12345',
    orderStatus: 'pending',
    ...overrides,
  };
}

/**
 * Generate sample product data
 */
export function generateSampleProduct(
  categoryId: number,
  overrides?: Partial<Omit<ProductRecord, 'id'>>
): Omit<ProductRecord, 'id'> {
  const timestamp = Date.now();
  return {
    productName: `Product ${timestamp}`,
    categoryId,
    unitPrice: 29.99,
    stockQuantity: 100,
    isAvailable: true,
    ...overrides,
  };
}

/**
 * Generate sample order item data
 */
export function generateSampleOrderItem(
  orderId: number,
  productId: number,
  overrides?: Partial<Omit<OrderItemRecord, 'id'>>
): Omit<OrderItemRecord, 'id'> {
  const quantity = overrides?.quantity ?? 1;
  const unitPrice = overrides?.unitPrice ?? 29.99;
  return {
    orderId,
    productId,
    quantity,
    unitPrice,
    itemTotal: quantity * unitPrice,
    ...overrides,
  };
}

/**
 * Generate sample category data
 */
export function generateSampleCategory(
  overrides?: Partial<Omit<CategoryRecord, 'id'>>
): Omit<CategoryRecord, 'id'> {
  const timestamp = Date.now();
  return {
    categoryName: `Category ${timestamp}`,
    parentCategoryId: null,
    ...overrides,
  };
}

// =============================================================================
// Bulk Data Seeding
// =============================================================================

/**
 * Seed the harness with a complete dataset
 */
export async function seedTestData(harness: TestHarness): Promise<{
  users: UserRecord[];
  categories: CategoryRecord[];
  products: ProductRecord[];
  orders: OrderRecord[];
  orderItems: OrderItemRecord[];
}> {
  // Create categories
  const electronics = await harness.insertCategory({
    categoryName: 'Electronics',
    parentCategoryId: null,
  });
  const clothing = await harness.insertCategory({
    categoryName: 'Clothing',
    parentCategoryId: null,
  });
  const books = await harness.insertCategory({
    categoryName: 'Books',
    parentCategoryId: null,
  });
  const categories = [electronics, clothing, books];

  // Create products
  const laptop = await harness.insertProduct({
    productName: 'Laptop Pro 15',
    categoryId: electronics.id,
    unitPrice: 1299.99,
    stockQuantity: 50,
    isAvailable: true,
  });
  const tshirt = await harness.insertProduct({
    productName: 'Cotton T-Shirt',
    categoryId: clothing.id,
    unitPrice: 24.99,
    stockQuantity: 200,
    isAvailable: true,
  });
  const novel = await harness.insertProduct({
    productName: 'Mystery Novel',
    categoryId: books.id,
    unitPrice: 14.99,
    stockQuantity: 100,
    isAvailable: true,
  });
  const headphones = await harness.insertProduct({
    productName: 'Wireless Headphones',
    categoryId: electronics.id,
    unitPrice: 199.99,
    stockQuantity: 75,
    isAvailable: true,
  });
  const jeans = await harness.insertProduct({
    productName: 'Denim Jeans',
    categoryId: clothing.id,
    unitPrice: 59.99,
    stockQuantity: 150,
    isAvailable: true,
  });
  const products = [laptop, tshirt, novel, headphones, jeans];

  // Create users
  const alice = await harness.insertUser({
    firstName: 'Alice',
    lastName: 'Johnson',
    emailAddress: 'alice@example.com',
    createdAt: new Date('2024-01-15'),
    isActive: true,
    accountBalance: 500.0,
  });
  const bob = await harness.insertUser({
    firstName: 'Bob',
    lastName: 'Smith',
    emailAddress: 'bob@example.com',
    createdAt: new Date('2024-02-20'),
    isActive: true,
    accountBalance: 250.0,
  });
  const carol = await harness.insertUser({
    firstName: 'Carol',
    lastName: 'Williams',
    emailAddress: 'carol@example.com',
    createdAt: new Date('2024-03-10'),
    isActive: false,
    accountBalance: 0.0,
  });
  const users = [alice, bob, carol];

  // Create orders and order items
  const orders: OrderRecord[] = [];
  const orderItems: OrderItemRecord[] = [];

  // Alice's order
  const aliceOrder = await harness.insertOrder({
    userId: alice.id,
    orderDate: new Date('2024-06-01'),
    totalAmount: 0,
    shippingAddress: '123 Alice Lane, Wonderland, WL 12345',
    orderStatus: 'shipped',
  });

  const aliceItem1 = await harness.insertOrderItem({
    orderId: aliceOrder.id,
    productId: laptop.id,
    quantity: 1,
    unitPrice: laptop.unitPrice,
    itemTotal: laptop.unitPrice,
  });
  const aliceItem2 = await harness.insertOrderItem({
    orderId: aliceOrder.id,
    productId: headphones.id,
    quantity: 1,
    unitPrice: headphones.unitPrice,
    itemTotal: headphones.unitPrice,
  });

  // Update order total
  aliceOrder.totalAmount = aliceItem1.itemTotal + aliceItem2.itemTotal;
  await harness.ordersBTree.set(`order:${aliceOrder.id}`, aliceOrder);
  // Sync updated total to in-memory adapter
  await harness.adapters.orders.update({ id: aliceOrder.id }, { totalAmount: aliceOrder.totalAmount });
  orders.push(aliceOrder);
  orderItems.push(aliceItem1, aliceItem2);

  // Bob's order
  const bobOrder = await harness.insertOrder({
    userId: bob.id,
    orderDate: new Date('2024-06-15'),
    totalAmount: 0,
    shippingAddress: '456 Bob Street, Builder City, BC 67890',
    orderStatus: 'pending',
  });

  const bobItem1 = await harness.insertOrderItem({
    orderId: bobOrder.id,
    productId: tshirt.id,
    quantity: 3,
    unitPrice: tshirt.unitPrice,
    itemTotal: tshirt.unitPrice * 3,
  });
  const bobItem2 = await harness.insertOrderItem({
    orderId: bobOrder.id,
    productId: jeans.id,
    quantity: 2,
    unitPrice: jeans.unitPrice,
    itemTotal: jeans.unitPrice * 2,
  });
  const bobItem3 = await harness.insertOrderItem({
    orderId: bobOrder.id,
    productId: novel.id,
    quantity: 1,
    unitPrice: novel.unitPrice,
    itemTotal: novel.unitPrice,
  });

  bobOrder.totalAmount = bobItem1.itemTotal + bobItem2.itemTotal + bobItem3.itemTotal;
  await harness.ordersBTree.set(`order:${bobOrder.id}`, bobOrder);
  // Sync updated total to in-memory adapter
  await harness.adapters.orders.update({ id: bobOrder.id }, { totalAmount: bobOrder.totalAmount });
  orders.push(bobOrder);
  orderItems.push(bobItem1, bobItem2, bobItem3);

  // Flush WAL
  await harness.walWriter.flush();

  return { users, categories, products, orders, orderItems };
}

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Wait for a condition to be true
 */
export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  timeoutMs = 5000,
  intervalMs = 50
): Promise<void> {
  const startTime = Date.now();

  while (Date.now() - startTime < timeoutMs) {
    if (await condition()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error(`Condition not met within ${timeoutMs}ms`);
}

/**
 * Collect all items from an async iterator
 */
export async function collectAsync<T>(iterator: AsyncIterable<T>): Promise<T[]> {
  const results: T[] = [];
  for await (const item of iterator) {
    results.push(item);
  }
  return results;
}
