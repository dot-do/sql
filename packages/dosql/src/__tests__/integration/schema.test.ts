/**
 * DoSQL Integration Tests - Schema Definition and Type Inference
 *
 * Tests IceType DSL schema definition, TypeScript type inference,
 * SQL DDL generation, and relation handling.
 * All fields use camelCase naming convention.
 */

import { describe, it, expect } from 'vitest';
import {
  type ColumnType,
  type TableSchema,
  type DatabaseSchema,
  type QueryResult,
  type SQL,
  createDatabase,
  createQuery,
} from '../../parser.js';

import {
  type ColumnarTableSchema,
  type ColumnDefinition,
  type ColumnDataType,
  inferSchema,
} from '../../columnar/index.js';

import {
  type TestDBSchema,
  usersColumnarSchema,
  ordersColumnarSchema,
  productsColumnarSchema,
} from './setup.js';

// =============================================================================
// Schema Definition Tests
// =============================================================================

describe('Schema Definition', () => {
  describe('IceType DSL Schema Types', () => {
    it('should define table schema with camelCase fields', () => {
      // This is a compile-time test - if it compiles, the types work
      type UsersTable = {
        id: 'number';
        firstName: 'string';
        lastName: 'string';
        emailAddress: 'string';
        createdAt: 'Date';
        isActive: 'boolean';
        accountBalance: 'number';
      };

      // Verify the type is a valid TableSchema
      const _schema: TableSchema = {
        id: 'number',
        firstName: 'string',
        lastName: 'string',
        emailAddress: 'string',
        createdAt: 'Date',
        isActive: 'boolean',
        accountBalance: 'number',
      } satisfies UsersTable;

      expect(_schema.id).toBe('number');
      expect(_schema.firstName).toBe('string');
      expect(_schema.isActive).toBe('boolean');
    });

    it('should define database schema with multiple tables', () => {
      type MyDB = {
        users: {
          id: 'number';
          firstName: 'string';
          lastName: 'string';
        };
        orders: {
          id: 'number';
          userId: 'number';
          totalAmount: 'number';
        };
        products: {
          id: 'number';
          productName: 'string';
          unitPrice: 'number';
        };
      };

      const schema: DatabaseSchema = {
        users: { id: 'number', firstName: 'string', lastName: 'string' },
        orders: { id: 'number', userId: 'number', totalAmount: 'number' },
        products: { id: 'number', productName: 'string', unitPrice: 'number' },
      } satisfies MyDB;

      expect(Object.keys(schema)).toHaveLength(3);
      expect(schema.users.id).toBe('number');
      expect(schema.orders.userId).toBe('number');
      expect(schema.products.productName).toBe('string');
    });

    it('should support all ColumnType values', () => {
      type AllTypesTable = {
        stringField: 'string';
        numberField: 'number';
        booleanField: 'boolean';
        dateField: 'Date';
        nullField: 'null';
        unknownField: 'unknown';
      };

      const schema: TableSchema = {
        stringField: 'string',
        numberField: 'number',
        booleanField: 'boolean',
        dateField: 'Date',
        nullField: 'null',
        unknownField: 'unknown',
      } satisfies AllTypesTable;

      const validTypes: ColumnType[] = ['string', 'number', 'boolean', 'Date', 'null', 'unknown'];

      for (const [, value] of Object.entries(schema)) {
        expect(validTypes).toContain(value);
      }
    });
  });

  describe('Columnar Schema Definition', () => {
    it('should define columnar schema with proper data types', () => {
      const schema: ColumnarTableSchema = {
        tableName: 'testTable',
        columns: [
          { name: 'id', dataType: 'int32', nullable: false },
          { name: 'firstName', dataType: 'string', nullable: false },
          { name: 'lastName', dataType: 'string', nullable: true },
          { name: 'accountBalance', dataType: 'float64', nullable: false },
          { name: 'isActive', dataType: 'boolean', nullable: false },
          { name: 'createdAt', dataType: 'timestamp', nullable: false },
        ],
      };

      expect(schema.tableName).toBe('testTable');
      expect(schema.columns).toHaveLength(6);
    });

    it('should verify predefined columnar schemas', () => {
      // Users schema
      expect(usersColumnarSchema.tableName).toBe('users');
      expect(usersColumnarSchema.columns.find(c => c.name === 'firstName')?.dataType).toBe('string');
      expect(usersColumnarSchema.columns.find(c => c.name === 'accountBalance')?.dataType).toBe('float64');

      // Orders schema
      expect(ordersColumnarSchema.tableName).toBe('orders');
      expect(ordersColumnarSchema.columns.find(c => c.name === 'userId')?.dataType).toBe('int32');
      expect(ordersColumnarSchema.columns.find(c => c.name === 'totalAmount')?.dataType).toBe('float64');

      // Products schema
      expect(productsColumnarSchema.tableName).toBe('products');
      expect(productsColumnarSchema.columns.find(c => c.name === 'isAvailable')?.dataType).toBe('boolean');
    });

    it('should support all columnar data types', () => {
      const allTypes: ColumnDataType[] = [
        'int8', 'int16', 'int32', 'int64',
        'uint8', 'uint16', 'uint32', 'uint64',
        'float32', 'float64',
        'boolean',
        'string',
        'bytes',
        'timestamp',
      ];

      const columns: ColumnDefinition[] = allTypes.map((type, i) => ({
        name: `col_${type}`,
        dataType: type,
        nullable: i % 2 === 0,
      }));

      const schema: ColumnarTableSchema = {
        tableName: 'allTypes',
        columns,
      };

      expect(schema.columns).toHaveLength(allTypes.length);
      expect(schema.columns.every(c => allTypes.includes(c.dataType))).toBe(true);
    });

    it('should support preferred encoding hints', () => {
      const schema: ColumnarTableSchema = {
        tableName: 'encodedTable',
        columns: [
          { name: 'id', dataType: 'int32', nullable: false, preferredEncoding: 'delta' },
          { name: 'status', dataType: 'string', nullable: false, preferredEncoding: 'dict' },
          { name: 'flags', dataType: 'int8', nullable: false, preferredEncoding: 'rle' },
          { name: 'smallInt', dataType: 'int16', nullable: false, preferredEncoding: 'bitpack' },
        ],
      };

      expect(schema.columns[0].preferredEncoding).toBe('delta');
      expect(schema.columns[1].preferredEncoding).toBe('dict');
      expect(schema.columns[2].preferredEncoding).toBe('rle');
      expect(schema.columns[3].preferredEncoding).toBe('bitpack');
    });
  });

  describe('Schema Inference', () => {
    it('should infer schema from sample data', () => {
      const sampleData = [
        { id: 1, firstName: 'Alice', accountBalance: 100.5, isActive: true },
        { id: 2, firstName: 'Bob', accountBalance: 200.75, isActive: false },
      ];

      const schema = inferSchema('inferred', sampleData);

      expect(schema.tableName).toBe('inferred');
      expect(schema.columns.find(c => c.name === 'id')?.dataType).toBe('int32');
      expect(schema.columns.find(c => c.name === 'firstName')?.dataType).toBe('string');
      expect(schema.columns.find(c => c.name === 'accountBalance')?.dataType).toBe('float64');
      expect(schema.columns.find(c => c.name === 'isActive')?.dataType).toBe('boolean');
    });

    it('should detect nullable columns from sample data', () => {
      const sampleData = [
        { id: 1, firstName: 'Alice', lastName: null },
        { id: 2, firstName: 'Bob', lastName: 'Smith' },
      ];

      const schema = inferSchema('nullableTest', sampleData);

      expect(schema.columns.find(c => c.name === 'lastName')?.nullable).toBe(true);
      expect(schema.columns.find(c => c.name === 'firstName')?.nullable).toBe(false);
    });

    it('should infer bigint as int64', () => {
      const sampleData = [
        { id: 1n, value: 'test' },
        { id: 2n, value: 'data' },
      ];

      const schema = inferSchema('bigintTest', sampleData);

      expect(schema.columns.find(c => c.name === 'id')?.dataType).toBe('int64');
    });

    it('should infer Date as timestamp', () => {
      const sampleData = [
        { id: 1, createdAt: new Date() },
        { id: 2, createdAt: new Date() },
      ];

      const schema = inferSchema('dateTest', sampleData);

      expect(schema.columns.find(c => c.name === 'createdAt')?.dataType).toBe('timestamp');
    });

    it('should infer Uint8Array as bytes', () => {
      const sampleData = [
        { id: 1, data: new Uint8Array([1, 2, 3]) },
        { id: 2, data: new Uint8Array([4, 5, 6]) },
      ];

      const schema = inferSchema('bytesTest', sampleData);

      expect(schema.columns.find(c => c.name === 'data')?.dataType).toBe('bytes');
    });

    it('should throw on empty sample data', () => {
      expect(() => inferSchema('empty', [])).toThrow('Cannot infer schema from empty data');
    });
  });
});

// =============================================================================
// TypeScript Type Inference Tests
// =============================================================================

describe('TypeScript Type Inference', () => {
  describe('Query Result Type Inference', () => {
    // These are compile-time tests using the type system
    type TestDB = {
      users: {
        id: 'number';
        firstName: 'string';
        lastName: 'string';
        isActive: 'boolean';
      };
    };

    it('should infer SELECT * result type', () => {
      // Type-level test: SQL type helper
      type Result = SQL<'SELECT * FROM users', TestDB>;

      // This compiles only if Result is an array of the correct shape
      const _typeCheck: Result = [
        { id: 1, firstName: 'test', lastName: 'user', isActive: true },
      ];

      expect(_typeCheck).toBeDefined();
    });

    it('should infer single column selection type', () => {
      type Result = SQL<'SELECT firstName FROM users', TestDB>;

      // Result should be { firstName: string }[]
      const _typeCheck: Result = [{ firstName: 'Alice' }];

      expect(_typeCheck).toBeDefined();
    });

    it('should infer multiple column selection type', () => {
      type Result = SQL<'SELECT id, firstName FROM users', TestDB>;

      // Result should be { id: number; firstName: string }[]
      const _typeCheck: Result = [{ id: 1, firstName: 'Alice' }];

      expect(_typeCheck).toBeDefined();
    });

    it('should handle column alias in type inference', () => {
      type Result = SQL<'SELECT firstName AS name FROM users', TestDB>;

      // Result should be { name: string }[]
      const _typeCheck: Result = [{ name: 'Alice' }];

      expect(_typeCheck).toBeDefined();
    });

    it('should handle table alias', () => {
      type Result = SQL<'SELECT u.firstName FROM users u', TestDB>;

      // Result should be { firstName: string }[]
      const _typeCheck: Result = [{ firstName: 'Alice' }];

      expect(_typeCheck).toBeDefined();
    });
  });

  describe('createDatabase Type Safety', () => {
    it('should create typed database interface', () => {
      const db = createDatabase<TestDBSchema>();

      // This is a runtime check that the interface exists
      expect(db.sql).toBeDefined();
      expect(typeof db.sql).toBe('function');
    });

    it('should create typed query function', () => {
      const query = createQuery<TestDBSchema>();

      // The query function should exist
      expect(typeof query).toBe('function');
    });
  });

  describe('Case Sensitivity', () => {
    type CaseSensitiveDB = {
      userAccounts: {
        accountId: 'number';
        accountName: 'string';
        isVerified: 'boolean';
      };
    };

    it('should preserve camelCase table names', () => {
      type Result = SQL<'SELECT * FROM userAccounts', CaseSensitiveDB>;

      // This should compile if table name is case-sensitive
      const _typeCheck: Result = [
        { accountId: 1, accountName: 'test', isVerified: true },
      ];

      expect(_typeCheck).toBeDefined();
    });

    it('should preserve camelCase column names', () => {
      type Result = SQL<'SELECT accountId, accountName FROM userAccounts', CaseSensitiveDB>;

      // Column names should be preserved
      const _typeCheck: Result = [{ accountId: 1, accountName: 'test' }];

      expect(_typeCheck).toBeDefined();
    });
  });
});

// =============================================================================
// SQL DDL Generation Tests
// =============================================================================

describe('SQL DDL Generation', () => {
  describe('CREATE TABLE DDL', () => {
    function generateCreateTable(schema: ColumnarTableSchema): string {
      const columnDefs = schema.columns.map(col => {
        let sqlType: string;
        switch (col.dataType) {
          case 'int8':
          case 'int16':
          case 'int32':
            sqlType = 'INTEGER';
            break;
          case 'int64':
            sqlType = 'BIGINT';
            break;
          case 'uint8':
          case 'uint16':
          case 'uint32':
            sqlType = 'INTEGER UNSIGNED';
            break;
          case 'uint64':
            sqlType = 'BIGINT UNSIGNED';
            break;
          case 'float32':
            sqlType = 'REAL';
            break;
          case 'float64':
            sqlType = 'DOUBLE';
            break;
          case 'boolean':
            sqlType = 'BOOLEAN';
            break;
          case 'string':
            sqlType = 'TEXT';
            break;
          case 'bytes':
            sqlType = 'BLOB';
            break;
          case 'timestamp':
            sqlType = 'TIMESTAMP';
            break;
          default:
            sqlType = 'TEXT';
        }

        const nullable = col.nullable ? '' : ' NOT NULL';
        return `  "${col.name}" ${sqlType}${nullable}`;
      });

      return `CREATE TABLE "${schema.tableName}" (\n${columnDefs.join(',\n')}\n);`;
    }

    it('should generate CREATE TABLE for users', () => {
      const ddl = generateCreateTable(usersColumnarSchema);

      expect(ddl).toContain('CREATE TABLE "users"');
      expect(ddl).toContain('"firstName" TEXT NOT NULL');
      expect(ddl).toContain('"accountBalance" DOUBLE NOT NULL');
      expect(ddl).toContain('"isActive" BOOLEAN NOT NULL');
      expect(ddl).toContain('"createdAt" TIMESTAMP NOT NULL');
    });

    it('should generate CREATE TABLE for orders', () => {
      const ddl = generateCreateTable(ordersColumnarSchema);

      expect(ddl).toContain('CREATE TABLE "orders"');
      expect(ddl).toContain('"userId" INTEGER NOT NULL');
      expect(ddl).toContain('"totalAmount" DOUBLE NOT NULL');
      expect(ddl).toContain('"orderStatus" TEXT NOT NULL');
    });

    it('should handle nullable columns', () => {
      const schema: ColumnarTableSchema = {
        tableName: 'nullableTest',
        columns: [
          { name: 'id', dataType: 'int32', nullable: false },
          { name: 'optionalField', dataType: 'string', nullable: true },
        ],
      };

      const ddl = generateCreateTable(schema);

      expect(ddl).toContain('"id" INTEGER NOT NULL');
      expect(ddl).not.toContain('"optionalField" TEXT NOT NULL');
      expect(ddl).toMatch(/"optionalField" TEXT[^N]/); // No NOT NULL
    });

    it('should quote camelCase identifiers', () => {
      const schema: ColumnarTableSchema = {
        tableName: 'camelCaseTable',
        columns: [
          { name: 'userId', dataType: 'int32', nullable: false },
          { name: 'firstName', dataType: 'string', nullable: false },
          { name: 'lastName', dataType: 'string', nullable: false },
        ],
      };

      const ddl = generateCreateTable(schema);

      expect(ddl).toContain('"camelCaseTable"');
      expect(ddl).toContain('"userId"');
      expect(ddl).toContain('"firstName"');
      expect(ddl).toContain('"lastName"');
    });
  });

  describe('ALTER TABLE DDL', () => {
    function generateAlterAddColumn(tableName: string, column: ColumnDefinition): string {
      let sqlType: string;
      switch (column.dataType) {
        case 'int32':
          sqlType = 'INTEGER';
          break;
        case 'float64':
          sqlType = 'DOUBLE';
          break;
        case 'string':
          sqlType = 'TEXT';
          break;
        case 'boolean':
          sqlType = 'BOOLEAN';
          break;
        default:
          sqlType = 'TEXT';
      }
      const nullable = column.nullable ? '' : ' NOT NULL';
      return `ALTER TABLE "${tableName}" ADD COLUMN "${column.name}" ${sqlType}${nullable};`;
    }

    it('should generate ALTER TABLE ADD COLUMN', () => {
      const ddl = generateAlterAddColumn('users', {
        name: 'phoneNumber',
        dataType: 'string',
        nullable: true,
      });

      expect(ddl).toBe('ALTER TABLE "users" ADD COLUMN "phoneNumber" TEXT;');
    });

    it('should generate ALTER TABLE with NOT NULL', () => {
      const ddl = generateAlterAddColumn('users', {
        name: 'tenantId',
        dataType: 'int32',
        nullable: false,
      });

      expect(ddl).toBe('ALTER TABLE "users" ADD COLUMN "tenantId" INTEGER NOT NULL;');
    });
  });
});

// =============================================================================
// Relation Handling Tests
// =============================================================================

describe('Relation Handling', () => {
  describe('Foreign Key Relationships', () => {
    interface RelationSchema {
      tables: {
        users: ColumnarTableSchema;
        orders: ColumnarTableSchema;
        orderItems: ColumnarTableSchema;
        products: ColumnarTableSchema;
      };
      relations: {
        name: string;
        from: { table: string; column: string };
        to: { table: string; column: string };
        type: 'one-to-many' | 'many-to-one' | 'many-to-many';
      }[];
    }

    const testRelationSchema: RelationSchema = {
      tables: {
        users: usersColumnarSchema,
        orders: ordersColumnarSchema,
        orderItems: {
          tableName: 'orderItems',
          columns: [
            { name: 'id', dataType: 'int32', nullable: false },
            { name: 'orderId', dataType: 'int32', nullable: false },
            { name: 'productId', dataType: 'int32', nullable: false },
            { name: 'quantity', dataType: 'int32', nullable: false },
          ],
        },
        products: productsColumnarSchema,
      },
      relations: [
        {
          name: 'user_orders',
          from: { table: 'orders', column: 'userId' },
          to: { table: 'users', column: 'id' },
          type: 'many-to-one',
        },
        {
          name: 'order_items',
          from: { table: 'orderItems', column: 'orderId' },
          to: { table: 'orders', column: 'id' },
          type: 'many-to-one',
        },
        {
          name: 'item_products',
          from: { table: 'orderItems', column: 'productId' },
          to: { table: 'products', column: 'id' },
          type: 'many-to-one',
        },
      ],
    };

    it('should define relations with camelCase column names', () => {
      expect(testRelationSchema.relations).toHaveLength(3);

      const userOrdersRelation = testRelationSchema.relations.find(r => r.name === 'user_orders');
      expect(userOrdersRelation?.from.column).toBe('userId');
      expect(userOrdersRelation?.to.column).toBe('id');
    });

    it('should support one-to-many relationships', () => {
      // Users -> Orders (one user has many orders)
      const userOrdersRelation = testRelationSchema.relations.find(r => r.name === 'user_orders');
      expect(userOrdersRelation?.type).toBe('many-to-one');

      // From the perspective of orders, each order has one user
      // From the perspective of users, each user has many orders
    });

    it('should validate foreign key references exist', () => {
      for (const relation of testRelationSchema.relations) {
        const fromTable = testRelationSchema.tables[relation.from.table as keyof typeof testRelationSchema.tables];
        const toTable = testRelationSchema.tables[relation.to.table as keyof typeof testRelationSchema.tables];

        expect(fromTable).toBeDefined();
        expect(toTable).toBeDefined();

        const fromColumn = fromTable.columns.find(c => c.name === relation.from.column);
        const toColumn = toTable.columns.find(c => c.name === relation.to.column);

        expect(fromColumn).toBeDefined();
        expect(toColumn).toBeDefined();
      }
    });

    it('should generate foreign key constraints', () => {
      function generateForeignKey(relation: RelationSchema['relations'][0]): string {
        return `ALTER TABLE "${relation.from.table}" ` +
          `ADD CONSTRAINT "fk_${relation.name}" ` +
          `FOREIGN KEY ("${relation.from.column}") ` +
          `REFERENCES "${relation.to.table}" ("${relation.to.column}");`;
      }

      const fk = generateForeignKey(testRelationSchema.relations[0]);

      expect(fk).toContain('FOREIGN KEY ("userId")');
      expect(fk).toContain('REFERENCES "users" ("id")');
    });
  });

  describe('Self-Referential Relationships', () => {
    it('should support self-referential foreign keys', () => {
      const categorySchema: ColumnarTableSchema = {
        tableName: 'categories',
        columns: [
          { name: 'id', dataType: 'int32', nullable: false },
          { name: 'categoryName', dataType: 'string', nullable: false },
          { name: 'parentCategoryId', dataType: 'int32', nullable: true },
        ],
      };

      const selfRelation = {
        name: 'category_parent',
        from: { table: 'categories', column: 'parentCategoryId' },
        to: { table: 'categories', column: 'id' },
        type: 'many-to-one' as const,
      };

      expect(selfRelation.from.table).toBe(selfRelation.to.table);
      expect(categorySchema.columns.find(c => c.name === 'parentCategoryId')?.nullable).toBe(true);
    });
  });

  describe('Composite Keys', () => {
    it('should support composite primary keys', () => {
      interface CompositeKeySchema extends ColumnarTableSchema {
        primaryKey: string[];
      }

      const orderItemsWithCompositeKey: CompositeKeySchema = {
        tableName: 'orderProductMapping',
        columns: [
          { name: 'orderId', dataType: 'int32', nullable: false },
          { name: 'productId', dataType: 'int32', nullable: false },
          { name: 'quantity', dataType: 'int32', nullable: false },
        ],
        primaryKey: ['orderId', 'productId'],
      };

      expect(orderItemsWithCompositeKey.primaryKey).toHaveLength(2);
      expect(orderItemsWithCompositeKey.primaryKey).toContain('orderId');
      expect(orderItemsWithCompositeKey.primaryKey).toContain('productId');
    });

    it('should generate composite primary key constraint', () => {
      function generateCompositePK(tableName: string, columns: string[]): string {
        const quotedColumns = columns.map(c => `"${c}"`).join(', ');
        return `ALTER TABLE "${tableName}" ADD CONSTRAINT "pk_${tableName}" PRIMARY KEY (${quotedColumns});`;
      }

      const pk = generateCompositePK('orderProductMapping', ['orderId', 'productId']);

      expect(pk).toContain('PRIMARY KEY ("orderId", "productId")');
    });
  });

  describe('Index Definitions', () => {
    interface IndexDefinition {
      name: string;
      table: string;
      columns: string[];
      unique: boolean;
    }

    it('should define indexes on camelCase columns', () => {
      const indexes: IndexDefinition[] = [
        { name: 'idx_users_email', table: 'users', columns: ['emailAddress'], unique: true },
        { name: 'idx_orders_user', table: 'orders', columns: ['userId'], unique: false },
        { name: 'idx_products_category', table: 'products', columns: ['categoryId'], unique: false },
      ];

      expect(indexes[0].columns).toContain('emailAddress');
      expect(indexes[1].columns).toContain('userId');
    });

    it('should generate CREATE INDEX statements', () => {
      function generateCreateIndex(index: IndexDefinition): string {
        const unique = index.unique ? 'UNIQUE ' : '';
        const columns = index.columns.map(c => `"${c}"`).join(', ');
        return `CREATE ${unique}INDEX "${index.name}" ON "${index.table}" (${columns});`;
      }

      const idx = generateCreateIndex({
        name: 'idx_users_email',
        table: 'users',
        columns: ['emailAddress'],
        unique: true,
      });

      expect(idx).toBe('CREATE UNIQUE INDEX "idx_users_email" ON "users" ("emailAddress");');
    });

    it('should support composite indexes', () => {
      const compositeIndex: IndexDefinition = {
        name: 'idx_orders_user_date',
        table: 'orders',
        columns: ['userId', 'orderDate'],
        unique: false,
      };

      expect(compositeIndex.columns).toHaveLength(2);
    });
  });
});
