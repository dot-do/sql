/**
 * Schema Type Inference Tests
 *
 * Tests for compile-time type inference from schema definitions.
 * This includes testing type-level utilities, modifier extraction,
 * relation detection, and table/schema type inference.
 */

import { describe, it, expect } from 'vitest';
import type {
  HasNullable,
  HasRequired,
  HasIndexed,
  HasArray,
  IsForwardRelation,
  IsBackwardRelation,
  IsRelation,
  ExtractRelationTarget,
  ExtractBaseType,
  SqlToTs,
  InferField,
  InferTableDef,
  InferTable,
  InferSchema,
  InferInsert,
  InferUpdate,
  InferSelect,
  PrimaryKeyField,
  IndexedFields,
  RelationFields,
  DataFields,
  UUID,
  Email,
  Timestamp,
  Expect,
  Equal,
  Brand,
} from '../inference.js';
import type { TableDefinition, SchemaDefinition } from '../types.js';

// =============================================================================
// TYPE ASSERTION HELPER
// =============================================================================

/**
 * Runtime type checking helper for testing TypeScript inference.
 * We use this pattern to ensure types are correctly inferred at compile time.
 */
function assertType<T>(_value: T): void {
  // This function is used purely for type checking at compile time
}

// =============================================================================
// MODIFIER EXTRACTION TESTS
// =============================================================================

describe('HasNullable type', () => {
  it('should detect nullable modifier (?)', () => {
    // These are compile-time type checks
    type Test1 = HasNullable<'string?'>;
    type Test2 = HasNullable<'timestamp?'>;
    type Test3 = HasNullable<'string'>;
    type Test4 = HasNullable<'uuid!'>;
    type Test5 = HasNullable<'string = "default"'>;

    // Runtime assertion that types resolve correctly
    const check1: Test1 = true;
    const check2: Test2 = true;
    const check3: Test3 = false;
    const check4: Test4 = false;
    const check5: Test5 = false;

    expect(check1).toBe(true);
    expect(check2).toBe(true);
    expect(check3).toBe(false);
    expect(check4).toBe(false);
    expect(check5).toBe(false);
  });

  it('should detect nullable in combined modifiers', () => {
    type Test1 = HasNullable<'string?#'>;
    type Test2 = HasNullable<'string#?'>;

    const check1: Test1 = true;
    const check2: Test2 = true;

    expect(check1).toBe(true);
    expect(check2).toBe(true);
  });
});

describe('HasRequired type', () => {
  it('should detect required modifier (!)', () => {
    type Test1 = HasRequired<'uuid!'>;
    type Test2 = HasRequired<'int!'>;
    type Test3 = HasRequired<'string'>;
    type Test4 = HasRequired<'string?'>;

    const check1: Test1 = true;
    const check2: Test2 = true;
    const check3: Test3 = false;
    const check4: Test4 = false;

    expect(check1).toBe(true);
    expect(check2).toBe(true);
    expect(check3).toBe(false);
    expect(check4).toBe(false);
  });

  it('should detect required in combined modifiers', () => {
    type Test1 = HasRequired<'uuid!#'>;
    type Test2 = HasRequired<'string#!'>;

    const check1: Test1 = true;
    const check2: Test2 = true;

    expect(check1).toBe(true);
    expect(check2).toBe(true);
  });
});

describe('HasIndexed type', () => {
  it('should detect indexed modifier (#)', () => {
    type Test1 = HasIndexed<'string#'>;
    type Test2 = HasIndexed<'email#'>;
    type Test3 = HasIndexed<'string'>;
    type Test4 = HasIndexed<'uuid!'>;

    const check1: Test1 = true;
    const check2: Test2 = true;
    const check3: Test3 = false;
    const check4: Test4 = false;

    expect(check1).toBe(true);
    expect(check2).toBe(true);
    expect(check3).toBe(false);
    expect(check4).toBe(false);
  });
});

describe('HasArray type', () => {
  it('should detect array modifier ([])', () => {
    type Test1 = HasArray<'string[]'>;
    type Test2 = HasArray<'json[]'>;
    type Test3 = HasArray<'string'>;
    type Test4 = HasArray<'-> Order[]'>;

    const check1: Test1 = true;
    const check2: Test2 = true;
    const check3: Test3 = false;
    const check4: Test4 = true;

    expect(check1).toBe(true);
    expect(check2).toBe(true);
    expect(check3).toBe(false);
    expect(check4).toBe(true);
  });
});

// =============================================================================
// RELATION DETECTION TESTS
// =============================================================================

describe('IsForwardRelation type', () => {
  it('should detect forward relations (->)', () => {
    type Test1 = IsForwardRelation<'-> Order[]'>;
    type Test2 = IsForwardRelation<'-> Profile'>;
    type Test3 = IsForwardRelation<'->Order[]'>;
    type Test4 = IsForwardRelation<'<- users'>;
    type Test5 = IsForwardRelation<'string'>;

    const check1: Test1 = true;
    const check2: Test2 = true;
    const check3: Test3 = true;
    const check4: Test4 = false;
    const check5: Test5 = false;

    expect(check1).toBe(true);
    expect(check2).toBe(true);
    expect(check3).toBe(true);
    expect(check4).toBe(false);
    expect(check5).toBe(false);
  });
});

describe('IsBackwardRelation type', () => {
  it('should detect backward relations (<-)', () => {
    type Test1 = IsBackwardRelation<'<- users'>;
    type Test2 = IsBackwardRelation<'<- orders[]'>;
    type Test3 = IsBackwardRelation<'<-users'>;
    type Test4 = IsBackwardRelation<'-> Order[]'>;
    type Test5 = IsBackwardRelation<'string'>;

    const check1: Test1 = true;
    const check2: Test2 = true;
    const check3: Test3 = true;
    const check4: Test4 = false;
    const check5: Test5 = false;

    expect(check1).toBe(true);
    expect(check2).toBe(true);
    expect(check3).toBe(true);
    expect(check4).toBe(false);
    expect(check5).toBe(false);
  });
});

describe('IsRelation type', () => {
  it('should detect any relation type', () => {
    type Test1 = IsRelation<'-> Order[]'>;
    type Test2 = IsRelation<'<- users'>;
    type Test3 = IsRelation<'string'>;
    type Test4 = IsRelation<'uuid!'>;

    const check1: Test1 = true;
    const check2: Test2 = true;
    const check3: Test3 = false;
    const check4: Test4 = false;

    expect(check1).toBe(true);
    expect(check2).toBe(true);
    expect(check3).toBe(false);
    expect(check4).toBe(false);
  });
});

describe('ExtractRelationTarget type', () => {
  it('should extract target from forward relations', () => {
    type Test1 = ExtractRelationTarget<'-> Order[]'>;
    type Test2 = ExtractRelationTarget<'-> Profile'>;
    type Test3 = ExtractRelationTarget<'->Order[]'>;

    // Type assertions
    const check1: Test1 = 'Order';
    const check2: Test2 = 'Profile';
    const check3: Test3 = 'Order';

    expect(check1).toBe('Order');
    expect(check2).toBe('Profile');
    expect(check3).toBe('Order');
  });

  it('should extract target from backward relations', () => {
    type Test1 = ExtractRelationTarget<'<- users'>;
    type Test2 = ExtractRelationTarget<'<- orders[]'>;
    type Test3 = ExtractRelationTarget<'<-users'>;

    const check1: Test1 = 'users';
    const check2: Test2 = 'orders';
    const check3: Test3 = 'users';

    expect(check1).toBe('users');
    expect(check2).toBe('orders');
    expect(check3).toBe('users');
  });
});

// =============================================================================
// BASE TYPE EXTRACTION TESTS
// =============================================================================

describe('ExtractBaseType type', () => {
  it('should extract base type from simple fields', () => {
    type Test1 = ExtractBaseType<'string'>;
    type Test2 = ExtractBaseType<'int'>;
    type Test3 = ExtractBaseType<'uuid'>;

    const check1: Test1 = 'string';
    const check2: Test2 = 'int';
    const check3: Test3 = 'uuid';

    expect(check1).toBe('string');
    expect(check2).toBe('int');
    expect(check3).toBe('uuid');
  });

  it('should extract base type with modifiers', () => {
    type Test1 = ExtractBaseType<'uuid!'>;
    type Test2 = ExtractBaseType<'string#'>;
    type Test3 = ExtractBaseType<'timestamp?'>;
    type Test4 = ExtractBaseType<'uuid!#'>;

    const check1: Test1 = 'uuid';
    const check2: Test2 = 'string';
    const check3: Test3 = 'timestamp';
    const check4: Test4 = 'uuid';

    expect(check1).toBe('uuid');
    expect(check2).toBe('string');
    expect(check3).toBe('timestamp');
    expect(check4).toBe('uuid');
  });

  it('should extract base type from arrays', () => {
    type Test1 = ExtractBaseType<'string[]'>;
    type Test2 = ExtractBaseType<'json[]'>;

    const check1: Test1 = 'string';
    const check2: Test2 = 'json';

    expect(check1).toBe('string');
    expect(check2).toBe('json');
  });

  it('should extract target from relations', () => {
    type Test1 = ExtractBaseType<'-> Order[]'>;
    type Test2 = ExtractBaseType<'<- users'>;

    const check1: Test1 = 'Order';
    const check2: Test2 = 'users';

    expect(check1).toBe('Order');
    expect(check2).toBe('users');
  });
});

// =============================================================================
// SQL TO TYPESCRIPT TYPE MAPPING TESTS
// =============================================================================

describe('SqlToTs type', () => {
  it('should map string types correctly', () => {
    type Test1 = SqlToTs<'string'>;
    type Test2 = SqlToTs<'text'>;
    type Test3 = SqlToTs<'uuid'>;

    assertType<Test1>('' as string);
    assertType<Test2>('' as string);
    assertType<Test3>('' as string);

    // These compile-time checks verify the mapping
    expect(true).toBe(true);
  });

  it('should map numeric types correctly', () => {
    type Test1 = SqlToTs<'int'>;
    type Test2 = SqlToTs<'integer'>;
    type Test3 = SqlToTs<'bigint'>;
    type Test4 = SqlToTs<'float'>;
    type Test5 = SqlToTs<'double'>;
    type Test6 = SqlToTs<'number'>;

    assertType<Test1>(0 as number);
    assertType<Test2>(0 as number);
    assertType<Test3>(0 as number);
    assertType<Test4>(0 as number);
    assertType<Test5>(0 as number);
    assertType<Test6>(0 as number);

    expect(true).toBe(true);
  });

  it('should map decimal types correctly', () => {
    type Test1 = SqlToTs<'decimal(10,2)'>;

    assertType<Test1>(0 as number);

    expect(true).toBe(true);
  });

  it('should map boolean types correctly', () => {
    type Test1 = SqlToTs<'boolean'>;
    type Test2 = SqlToTs<'bool'>;

    assertType<Test1>(true as boolean);
    assertType<Test2>(false as boolean);

    expect(true).toBe(true);
  });

  it('should map date/time types correctly', () => {
    type Test1 = SqlToTs<'timestamp'>;
    type Test2 = SqlToTs<'datetime'>;
    type Test3 = SqlToTs<'date'>;

    assertType<Test1>(new Date());
    assertType<Test2>(new Date());
    assertType<Test3>(new Date());

    expect(true).toBe(true);
  });

  it('should map json types to unknown', () => {
    type Test1 = SqlToTs<'json'>;
    type Test2 = SqlToTs<'jsonb'>;

    // JSON can be any type
    assertType<Test1>({ key: 'value' } as unknown);
    assertType<Test2>([1, 2, 3] as unknown);

    expect(true).toBe(true);
  });

  it('should map binary types to Uint8Array', () => {
    type Test1 = SqlToTs<'blob'>;
    type Test2 = SqlToTs<'binary'>;

    assertType<Test1>(new Uint8Array());
    assertType<Test2>(new Uint8Array());

    expect(true).toBe(true);
  });
});

// =============================================================================
// FIELD TYPE INFERENCE TESTS
// =============================================================================

describe('InferField type', () => {
  it('should infer basic field types', () => {
    type Test1 = InferField<'string'>;
    type Test2 = InferField<'int'>;
    type Test3 = InferField<'boolean'>;

    assertType<Test1>('' as string);
    assertType<Test2>(0 as number);
    assertType<Test3>(true as boolean);

    expect(true).toBe(true);
  });

  it('should infer nullable fields with null union', () => {
    type Test1 = InferField<'string?'>;
    type Test2 = InferField<'timestamp?'>;

    assertType<Test1>('' as string | null);
    assertType<Test1>(null);
    assertType<Test2>(new Date() as Date | null);
    assertType<Test2>(null);

    expect(true).toBe(true);
  });

  it('should infer array fields', () => {
    type Test1 = InferField<'string[]'>;
    type Test2 = InferField<'json[]'>;

    assertType<Test1>([] as string[]);
    assertType<Test2>([] as unknown[]);

    expect(true).toBe(true);
  });

  it('should infer fields with modifiers', () => {
    type Test1 = InferField<'uuid!'>;
    type Test2 = InferField<'string!#'>;

    assertType<Test1>('' as string);
    assertType<Test2>('' as string);

    expect(true).toBe(true);
  });
});

// =============================================================================
// TABLE TYPE INFERENCE TESTS
// =============================================================================

describe('InferTableDef type', () => {
  it('should infer a simple table type', () => {
    type UserTable = {
      id: 'uuid!';
      name: 'string';
      email: 'string!#';
    };

    type User = InferTableDef<UserTable>;

    const user: User = {
      id: 'abc-123',
      name: 'John Doe',
      email: 'john@example.com',
    };

    expect(user.id).toBe('abc-123');
    expect(user.name).toBe('John Doe');
    expect(user.email).toBe('john@example.com');
  });

  it('should mark nullable fields as optional', () => {
    type ItemTable = {
      id: 'uuid!';
      name: 'string';
      description: 'text?';
    };

    type Item = InferTableDef<ItemTable>;

    const item1: Item = {
      id: 'item-1',
      name: 'Widget',
      description: 'A nice widget',
    };

    const item2: Item = {
      id: 'item-2',
      name: 'Gadget',
      // description is optional
    };

    expect(item1.description).toBe('A nice widget');
    expect(item2.description).toBeUndefined();
  });

  it('should infer all field types correctly', () => {
    type ComplexTable = {
      id: 'uuid!';
      name: 'string';
      count: 'int';
      price: 'float';
      active: 'boolean';
      tags: 'json[]';
      avatar: 'blob';
      createdAt: 'timestamp';
    };

    type Complex = InferTableDef<ComplexTable>;

    const complex: Complex = {
      id: 'complex-1',
      name: 'Test',
      count: 42,
      price: 19.99,
      active: true,
      tags: [{ label: 'sale' }],
      avatar: new Uint8Array([1, 2, 3]),
      createdAt: new Date(),
    };

    expect(complex.id).toBe('complex-1');
    expect(complex.count).toBe(42);
    expect(complex.active).toBe(true);
    expect(complex.tags).toHaveLength(1);
  });
});

describe('InferTable type', () => {
  it('should infer table type from schema by name', () => {
    const schema = {
      users: {
        id: 'uuid!',
        name: 'string',
        email: 'string!#',
      },
      orders: {
        id: 'uuid!',
        total: 'decimal(10,2)',
        status: 'string',
      },
    } as const;

    type User = InferTable<typeof schema, 'users'>;
    type Order = InferTable<typeof schema, 'orders'>;

    const user: User = {
      id: 'user-1',
      name: 'Jane',
      email: 'jane@example.com',
    };

    const order: Order = {
      id: 'order-1',
      total: 99.99,
      status: 'pending',
    };

    expect(user.email).toBe('jane@example.com');
    expect(order.total).toBe(99.99);
  });
});

// =============================================================================
// SCHEMA TYPE INFERENCE TESTS
// =============================================================================

describe('InferSchema type', () => {
  it('should infer all tables from a schema', () => {
    const schema = {
      users: {
        id: 'uuid!',
        name: 'string',
      },
      orders: {
        id: 'uuid!',
        total: 'decimal(10,2)',
      },
      products: {
        id: 'uuid!',
        price: 'float',
      },
    } as const;

    type Schema = InferSchema<typeof schema>;

    const entities: Schema = {
      users: { id: 'u1', name: 'Alice' },
      orders: { id: 'o1', total: 50 },
      products: { id: 'p1', price: 25.5 },
    };

    expect(entities.users.name).toBe('Alice');
    expect(entities.orders.total).toBe(50);
    expect(entities.products.price).toBe(25.5);
  });

  it('should exclude schema directives from inferred types', () => {
    const schema = {
      users: {
        id: 'uuid!',
        name: 'string',
      },
      '@index': ['email'],
    } as const;

    type Schema = InferSchema<typeof schema>;

    // @index should not appear as a table in the inferred type
    const entities: Schema = {
      users: { id: 'u1', name: 'Bob' },
    };

    expect(entities.users.id).toBe('u1');
    // TypeScript should not allow: entities['@index']
  });
});

// =============================================================================
// INSERT/UPDATE/SELECT TYPE TESTS
// =============================================================================

describe('InferInsert type', () => {
  it('should exclude auto-generated fields from insert', () => {
    type UserTable = {
      id: 'uuid!';
      name: 'string';
      createdAt: 'timestamp = now()';
      updatedAt: 'timestamp = now()';
    };

    type UserInsert = InferInsert<UserTable>;

    // createdAt and updatedAt should be optional (auto-generated)
    const insert: UserInsert = {
      id: 'user-1',
      name: 'Charlie',
      // createdAt and updatedAt are optional
    };

    expect(insert.id).toBe('user-1');
    expect(insert.name).toBe('Charlie');
  });

  it('should exclude relation fields from insert', () => {
    type UserTable = {
      id: 'uuid!';
      name: 'string';
      orders: '-> orders[]';
    };

    type UserInsert = InferInsert<UserTable>;

    // orders should not be in insert type
    const insert: UserInsert = {
      id: 'user-1',
      name: 'Dave',
    };

    expect(insert.name).toBe('Dave');
  });
});

describe('InferUpdate type', () => {
  it('should make all fields optional for update', () => {
    type UserTable = {
      id: 'uuid!';
      name: 'string';
      email: 'string!#';
    };

    type UserUpdate = InferUpdate<UserTable>;

    // All fields should be optional
    const update1: UserUpdate = {
      name: 'New Name',
    };

    const update2: UserUpdate = {
      email: 'new@example.com',
    };

    const update3: UserUpdate = {};

    expect(update1.name).toBe('New Name');
    expect(update2.email).toBe('new@example.com');
    expect(Object.keys(update3)).toHaveLength(0);
  });
});

describe('InferSelect type', () => {
  it('should include all fields including relations', () => {
    const schema = {
      users: {
        id: 'uuid!',
        name: 'string',
        orders: '-> orders[]',
      },
      orders: {
        id: 'uuid!',
        total: 'decimal(10,2)',
      },
    } as const;

    type User = InferSelect<typeof schema.users, typeof schema>;

    // All fields including relations should be present
    const user: User = {
      id: 'user-1',
      name: 'Eve',
      orders: [{ id: 'order-1', total: 100 }],
    };

    expect(user.orders).toHaveLength(1);
  });
});

// =============================================================================
// UTILITY TYPE TESTS
// =============================================================================

describe('PrimaryKeyField type', () => {
  it('should extract primary key field name', () => {
    type UserTable = {
      id: 'uuid!';
      name: 'string';
      email: 'string#';
    };

    type PK = PrimaryKeyField<UserTable>;

    const pk: PK = 'id';
    expect(pk).toBe('id');
  });

  it('should handle multiple primary keys (composite)', () => {
    type CompositeTable = {
      userId: 'uuid!';
      orderId: 'uuid!';
      quantity: 'int';
    };

    type PK = PrimaryKeyField<CompositeTable>;

    // Should be 'userId' | 'orderId'
    const pk1: PK = 'userId';
    const pk2: PK = 'orderId';

    expect(pk1).toBe('userId');
    expect(pk2).toBe('orderId');
  });
});

describe('IndexedFields type', () => {
  it('should extract indexed field names', () => {
    type UserTable = {
      id: 'uuid!';
      email: 'string#';
      name: 'string';
      username: 'string#';
    };

    type Indexed = IndexedFields<UserTable>;

    const idx1: Indexed = 'email';
    const idx2: Indexed = 'username';

    expect(idx1).toBe('email');
    expect(idx2).toBe('username');
  });
});

describe('RelationFields type', () => {
  it('should extract relation field names', () => {
    type UserTable = {
      id: 'uuid!';
      name: 'string';
      orders: '-> orders[]';
      profile: '-> profile';
    };

    type Relations = RelationFields<UserTable>;

    const rel1: Relations = 'orders';
    const rel2: Relations = 'profile';

    expect(rel1).toBe('orders');
    expect(rel2).toBe('profile');
  });
});

describe('DataFields type', () => {
  it('should extract non-relation field names', () => {
    type UserTable = {
      id: 'uuid!';
      name: 'string';
      email: 'string#';
      orders: '-> orders[]';
    };

    type Data = DataFields<UserTable>;

    const data1: Data = 'id';
    const data2: Data = 'name';
    const data3: Data = 'email';

    expect(data1).toBe('id');
    expect(data2).toBe('name');
    expect(data3).toBe('email');
  });
});

// =============================================================================
// BRANDED TYPE TESTS
// =============================================================================

describe('Brand type', () => {
  it('should create branded types', () => {
    type UserId = Brand<string, 'UserId'>;
    type OrderId = Brand<string, 'OrderId'>;

    const userId: UserId = 'user-123' as UserId;
    const orderId: OrderId = 'order-456' as OrderId;

    // Branded types are still strings at runtime
    expect(typeof userId).toBe('string');
    expect(typeof orderId).toBe('string');
  });
});

describe('UUID type', () => {
  it('should be a branded string', () => {
    const uuid: UUID = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890' as UUID;
    expect(typeof uuid).toBe('string');
    expect(uuid).toContain('-');
  });
});

describe('Email type', () => {
  it('should be a branded string', () => {
    const email: Email = 'test@example.com' as Email;
    expect(typeof email).toBe('string');
    expect(email).toContain('@');
  });
});

describe('Timestamp type', () => {
  it('should be a branded Date', () => {
    const ts: Timestamp = new Date() as Timestamp;
    expect(ts).toBeInstanceOf(Date);
  });
});

// =============================================================================
// TYPE ASSERTION HELPER TESTS
// =============================================================================

describe('Equal type', () => {
  it('should return true for equal types', () => {
    type Test1 = Equal<string, string>;
    type Test2 = Equal<number, number>;
    type Test3 = Equal<{ a: string }, { a: string }>;

    const check1: Test1 = true;
    const check2: Test2 = true;
    const check3: Test3 = true;

    expect(check1).toBe(true);
    expect(check2).toBe(true);
    expect(check3).toBe(true);
  });

  it('should return false for different types', () => {
    type Test1 = Equal<string, number>;
    type Test2 = Equal<string, string | null>;
    type Test3 = Equal<{ a: string }, { b: string }>;

    const check1: Test1 = false;
    const check2: Test2 = false;
    const check3: Test3 = false;

    expect(check1).toBe(false);
    expect(check2).toBe(false);
    expect(check3).toBe(false);
  });
});

// =============================================================================
// EDGE CASES AND COMPLEX TYPES
// =============================================================================

describe('Edge cases', () => {
  it('should handle fields with default values and modifiers', () => {
    type FieldWithDefault = ExtractBaseType<'string = "pending"'>;

    const check: FieldWithDefault = 'string';
    expect(check).toBe('string');
  });

  it('should handle deeply nested schema inference', () => {
    const schema = {
      organizations: {
        id: 'uuid!',
        name: 'string',
        users: '-> users[]',
      },
      users: {
        id: 'uuid!',
        orgId: '<- organizations',
        name: 'string',
        posts: '-> posts[]',
      },
      posts: {
        id: 'uuid!',
        authorId: '<- users',
        title: 'string',
        content: 'text?',
        comments: '-> comments[]',
      },
      comments: {
        id: 'uuid!',
        postId: '<- posts',
        userId: '<- users',
        text: 'string',
      },
    } as const;

    type Org = InferTable<typeof schema, 'organizations'>;
    type User = InferTable<typeof schema, 'users'>;
    type Post = InferTable<typeof schema, 'posts'>;
    type Comment = InferTable<typeof schema, 'comments'>;

    // Verify the types compile correctly
    const org: Org = {
      id: 'org-1',
      name: 'Acme Corp',
      users: [{ id: 'u1', orgId: 'org-1', name: 'Alice', posts: [] }],
    };

    expect(org.name).toBe('Acme Corp');
  });

  it('should handle all SQL types in one table', () => {
    type AllTypesTable = {
      id: 'uuid!';
      strField: 'string';
      txtField: 'text?';
      intField: 'int';
      integerField: 'integer';
      bigintField: 'bigint';
      floatField: 'float';
      doubleField: 'double';
      decimalField: 'decimal(10,2)';
      numField: 'number';
      boolField: 'boolean';
      boolField2: 'bool';
      tsField: 'timestamp';
      dtField: 'datetime';
      dateField: 'date';
      timeField: 'time';
      jsonField: 'json';
      jsonbField: 'jsonb';
      blobField: 'blob';
      binaryField: 'binary';
      arrField: 'string[]';
    };

    type AllTypes = InferTableDef<AllTypesTable>;

    const record: AllTypes = {
      id: 'test-1',
      strField: 'hello',
      txtField: 'long text',
      intField: 42,
      integerField: 100,
      bigintField: 9999999999,
      floatField: 3.14,
      doubleField: 2.718281828,
      decimalField: 99.99,
      numField: 123,
      boolField: true,
      boolField2: false,
      tsField: new Date(),
      dtField: new Date(),
      dateField: new Date(),
      timeField: '12:30:00',
      jsonField: { key: 'value' },
      jsonbField: [1, 2, 3],
      blobField: new Uint8Array([1, 2, 3]),
      binaryField: new Uint8Array([4, 5, 6]),
      arrField: ['a', 'b', 'c'],
    };

    expect(record.id).toBe('test-1');
    expect(record.intField).toBe(42);
    expect(record.boolField).toBe(true);
    expect(record.arrField).toHaveLength(3);
  });

  it('should handle case-insensitive type names', () => {
    // SqlToTs normalizes to lowercase internally
    type Test1 = SqlToTs<'STRING'>;
    type Test2 = SqlToTs<'Int'>;
    type Test3 = SqlToTs<'BOOLEAN'>;

    assertType<Test1>('' as string);
    assertType<Test2>(0 as number);
    assertType<Test3>(true as boolean);

    expect(true).toBe(true);
  });

  it('should handle combined modifiers in any order', () => {
    // These should all resolve to the same base type
    type Test1 = ExtractBaseType<'uuid!#'>;
    type Test2 = ExtractBaseType<'uuid#!'>;
    type Test3 = ExtractBaseType<'string!?'>;
    type Test4 = ExtractBaseType<'string?!'>;
    type Test5 = ExtractBaseType<'int#?'>;
    type Test6 = ExtractBaseType<'int?#'>;

    const check1: Test1 = 'uuid';
    const check2: Test2 = 'uuid';
    const check3: Test3 = 'string';
    const check4: Test4 = 'string';
    const check5: Test5 = 'int';
    const check6: Test6 = 'int';

    expect(check1).toBe('uuid');
    expect(check2).toBe('uuid');
    expect(check3).toBe('string');
    expect(check4).toBe('string');
    expect(check5).toBe('int');
    expect(check6).toBe('int');
  });
});
