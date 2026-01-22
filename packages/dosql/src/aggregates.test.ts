/**
 * Type Tests for SQL Aggregate Functions and Expressions
 *
 * These tests verify compile-time type inference for:
 * - Aggregate functions: COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col)
 * - Arithmetic expressions: total * 1.1, price + tax
 * - AS aliases for computed columns
 * - GROUP BY awareness
 */

import { describe, it, expectTypeOf } from 'vitest';
import type {
  IsAggregateFunction,
  ExtractAggregateFn,
  ExtractAggregateArg,
  AggregateResultType,
  IsArithmeticExpression,
  IsNumericLiteral,
  ParseExpressionWithAlias,
  GetExpressionOutputName,
  ResolveExpressionType,
  HasGroupBy,
  ExtractGroupByColumns,
  ParseSelectExpression,
  ParseSelectExpressions,
  AggregateFunctionName,
} from './aggregates.js';
import type { ColumnType, TableSchema } from './parser.js';

// =============================================================================
// TEST DATABASE SCHEMA
// =============================================================================

// Database schema type - uses Record<string, TableSchema> for compatibility
type TestDB = {
  orders: {
    id: 'number';
    user_id: 'number';
    total: 'number';
    status: 'string';
    created_at: 'Date';
  };
  users: {
    id: 'number';
    name: 'string';
    email: 'string';
    age: 'number';
  };
  products: {
    id: 'number';
    name: 'string';
    price: 'number';
    stock: 'number';
  };
};

type OrdersTable = TestDB['orders'];
type UsersTable = TestDB['users'];

// Flattened column types for single-table queries
type OrderColumns = {
  id: 'number';
  user_id: 'number';
  total: 'number';
  status: 'string';
  created_at: 'Date';
};

// =============================================================================
// AGGREGATE FUNCTION DETECTION TESTS
// =============================================================================

describe('IsAggregateFunction', () => {
  it('should detect COUNT(*)', () => {
    expectTypeOf<IsAggregateFunction<'COUNT(*)'>>().toEqualTypeOf<true>();
  });

  it('should detect COUNT(col)', () => {
    expectTypeOf<IsAggregateFunction<'COUNT(id)'>>().toEqualTypeOf<true>();
  });

  it('should detect SUM(col)', () => {
    expectTypeOf<IsAggregateFunction<'SUM(total)'>>().toEqualTypeOf<true>();
  });

  it('should detect AVG(col)', () => {
    expectTypeOf<IsAggregateFunction<'AVG(price)'>>().toEqualTypeOf<true>();
  });

  it('should detect MIN(col)', () => {
    expectTypeOf<IsAggregateFunction<'MIN(created_at)'>>().toEqualTypeOf<true>();
  });

  it('should detect MAX(col)', () => {
    expectTypeOf<IsAggregateFunction<'MAX(age)'>>().toEqualTypeOf<true>();
  });

  it('should be case-insensitive', () => {
    expectTypeOf<IsAggregateFunction<'count(*)'>>().toEqualTypeOf<true>();
    expectTypeOf<IsAggregateFunction<'Sum(total)'>>().toEqualTypeOf<true>();
    expectTypeOf<IsAggregateFunction<'avg(Price)'>>().toEqualTypeOf<true>();
  });

  it('should handle whitespace', () => {
    expectTypeOf<IsAggregateFunction<' COUNT(*) '>>().toEqualTypeOf<true>();
    expectTypeOf<IsAggregateFunction<'  SUM(total)  '>>().toEqualTypeOf<true>();
  });

  it('should reject non-aggregate expressions', () => {
    expectTypeOf<IsAggregateFunction<'id'>>().toEqualTypeOf<false>();
    expectTypeOf<IsAggregateFunction<'total * 1.1'>>().toEqualTypeOf<false>();
    expectTypeOf<IsAggregateFunction<'name'>>().toEqualTypeOf<false>();
  });
});

describe('ExtractAggregateFn', () => {
  it('should extract COUNT', () => {
    expectTypeOf<ExtractAggregateFn<'COUNT(*)'>>().toEqualTypeOf<'COUNT'>();
    expectTypeOf<ExtractAggregateFn<'count(id)'>>().toEqualTypeOf<'COUNT'>();
  });

  it('should extract SUM', () => {
    expectTypeOf<ExtractAggregateFn<'SUM(total)'>>().toEqualTypeOf<'SUM'>();
  });

  it('should extract AVG', () => {
    expectTypeOf<ExtractAggregateFn<'AVG(price)'>>().toEqualTypeOf<'AVG'>();
  });

  it('should extract MIN', () => {
    expectTypeOf<ExtractAggregateFn<'MIN(date)'>>().toEqualTypeOf<'MIN'>();
  });

  it('should extract MAX', () => {
    expectTypeOf<ExtractAggregateFn<'MAX(age)'>>().toEqualTypeOf<'MAX'>();
  });
});

describe('ExtractAggregateArg', () => {
  it('should extract * from COUNT(*)', () => {
    expectTypeOf<ExtractAggregateArg<'COUNT(*)'>>().toEqualTypeOf<'*'>();
  });

  it('should extract column name preserving original case', () => {
    // Preserves original case for correct column lookups
    expectTypeOf<ExtractAggregateArg<'SUM(total)'>>().toEqualTypeOf<'total'>();
    expectTypeOf<ExtractAggregateArg<'AVG(price)'>>().toEqualTypeOf<'price'>();
  });

  it('should handle whitespace in argument', () => {
    expectTypeOf<ExtractAggregateArg<'COUNT( id )'>>().toEqualTypeOf<'id'>();
  });
});

describe('AggregateResultType', () => {
  it('COUNT always returns number', () => {
    expectTypeOf<AggregateResultType<'COUNT', 'number'>>().toEqualTypeOf<'number'>();
    expectTypeOf<AggregateResultType<'COUNT', 'string'>>().toEqualTypeOf<'number'>();
    expectTypeOf<AggregateResultType<'COUNT', 'Date'>>().toEqualTypeOf<'number'>();
  });

  it('SUM returns number', () => {
    expectTypeOf<AggregateResultType<'SUM', 'number'>>().toEqualTypeOf<'number'>();
  });

  it('AVG returns number', () => {
    expectTypeOf<AggregateResultType<'AVG', 'number'>>().toEqualTypeOf<'number'>();
  });

  it('MIN preserves input type', () => {
    expectTypeOf<AggregateResultType<'MIN', 'number'>>().toEqualTypeOf<'number'>();
    expectTypeOf<AggregateResultType<'MIN', 'string'>>().toEqualTypeOf<'string'>();
    expectTypeOf<AggregateResultType<'MIN', 'Date'>>().toEqualTypeOf<'Date'>();
  });

  it('MAX preserves input type', () => {
    expectTypeOf<AggregateResultType<'MAX', 'number'>>().toEqualTypeOf<'number'>();
    expectTypeOf<AggregateResultType<'MAX', 'string'>>().toEqualTypeOf<'string'>();
    expectTypeOf<AggregateResultType<'MAX', 'Date'>>().toEqualTypeOf<'Date'>();
  });
});

// =============================================================================
// ARITHMETIC EXPRESSION TESTS
// =============================================================================

describe('IsArithmeticExpression', () => {
  it('should detect addition', () => {
    expectTypeOf<IsArithmeticExpression<'price + tax'>>().toEqualTypeOf<true>();
  });

  it('should detect subtraction', () => {
    expectTypeOf<IsArithmeticExpression<'total - discount'>>().toEqualTypeOf<true>();
  });

  it('should detect multiplication', () => {
    expectTypeOf<IsArithmeticExpression<'total * 1.1'>>().toEqualTypeOf<true>();
  });

  it('should detect division', () => {
    expectTypeOf<IsArithmeticExpression<'total / 2'>>().toEqualTypeOf<true>();
  });

  it('should reject aggregate functions', () => {
    // Aggregates contain parentheses but are not arithmetic
    expectTypeOf<IsArithmeticExpression<'COUNT(*)'>>().toEqualTypeOf<false>();
    expectTypeOf<IsArithmeticExpression<'SUM(total)'>>().toEqualTypeOf<false>();
  });

  it('should reject simple column references', () => {
    expectTypeOf<IsArithmeticExpression<'id'>>().toEqualTypeOf<false>();
    expectTypeOf<IsArithmeticExpression<'total'>>().toEqualTypeOf<false>();
  });
});

describe('IsNumericLiteral', () => {
  it('should detect integers', () => {
    expectTypeOf<IsNumericLiteral<'42'>>().toEqualTypeOf<true>();
    expectTypeOf<IsNumericLiteral<'0'>>().toEqualTypeOf<true>();
    expectTypeOf<IsNumericLiteral<'12345'>>().toEqualTypeOf<true>();
  });

  it('should detect decimals', () => {
    expectTypeOf<IsNumericLiteral<'3.14'>>().toEqualTypeOf<true>();
    expectTypeOf<IsNumericLiteral<'0.5'>>().toEqualTypeOf<true>();
    expectTypeOf<IsNumericLiteral<'1.0'>>().toEqualTypeOf<true>();
  });

  it('should reject non-numeric strings', () => {
    expectTypeOf<IsNumericLiteral<'abc'>>().toEqualTypeOf<false>();
    expectTypeOf<IsNumericLiteral<'total'>>().toEqualTypeOf<false>();
  });
});

// =============================================================================
// ALIAS PARSING TESTS
// =============================================================================

describe('ParseExpressionWithAlias', () => {
  it('should parse expression with AS alias', () => {
    type Result = ParseExpressionWithAlias<'COUNT(*) AS count'>;
    expectTypeOf<Result>().toEqualTypeOf<{
      expression: 'COUNT(*)';
      alias: 'count';
    }>();
  });

  it('should parse aggregate with AS alias', () => {
    type Result = ParseExpressionWithAlias<'SUM(total) AS total_sum'>;
    expectTypeOf<Result>().toEqualTypeOf<{
      expression: 'SUM(total)';
      alias: 'total_sum';
    }>();
  });

  it('should parse arithmetic with AS alias', () => {
    type Result = ParseExpressionWithAlias<'total * 1.1 AS with_tax'>;
    expectTypeOf<Result>().toEqualTypeOf<{
      expression: 'total * 1.1';
      alias: 'with_tax';
    }>();
  });

  it('should handle expression without alias', () => {
    type Result = ParseExpressionWithAlias<'total'>;
    expectTypeOf<Result>().toEqualTypeOf<{
      expression: 'total';
      alias: null;
    }>();
  });

  it('should handle case-insensitive AS', () => {
    type Result = ParseExpressionWithAlias<'COUNT(*) as cnt'>;
    expectTypeOf<Result>().toEqualTypeOf<{
      expression: 'COUNT(*)';
      alias: 'cnt';
    }>();
  });
});

describe('GetExpressionOutputName', () => {
  it('should return alias when present', () => {
    type Result = GetExpressionOutputName<{ expression: 'COUNT(*)'; alias: 'count' }>;
    expectTypeOf<Result>().toEqualTypeOf<'count'>();
  });

  it('should derive name from expression when no alias', () => {
    type Result = GetExpressionOutputName<{ expression: 'total'; alias: null }>;
    expectTypeOf<Result>().toEqualTypeOf<'total'>();
  });

  it('should derive name from table.col reference', () => {
    type Result = GetExpressionOutputName<{ expression: 'orders.total'; alias: null }>;
    expectTypeOf<Result>().toEqualTypeOf<'total'>();
  });
});

// =============================================================================
// EXPRESSION TYPE RESOLUTION TESTS
// =============================================================================

describe('ResolveExpressionType', () => {
  it('should resolve COUNT(*) as number', () => {
    type Result = ResolveExpressionType<'COUNT(*)', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<'number'>();
  });

  it('should resolve SUM(number) as number', () => {
    type Result = ResolveExpressionType<'SUM(total)', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<'number'>();
  });

  it('should resolve AVG(number) as number', () => {
    type Result = ResolveExpressionType<'AVG(total)', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<'number'>();
  });

  it('should resolve arithmetic as number', () => {
    type Result = ResolveExpressionType<'total * 1.1', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<'number'>();
  });

  it('should resolve column reference', () => {
    type Result = ResolveExpressionType<'status', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<'string'>();
  });

  it('should resolve numeric literal', () => {
    type Result = ResolveExpressionType<'42', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<'number'>();
  });

  it('should resolve string literal', () => {
    type Result = ResolveExpressionType<"'hello'", OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<'string'>();
  });
});

// =============================================================================
// GROUP BY TESTS
// =============================================================================

describe('HasGroupBy', () => {
  it('should detect GROUP BY clause', () => {
    type Result = HasGroupBy<'SELECT user_id, COUNT(*) FROM orders GROUP BY user_id'>;
    expectTypeOf<Result>().toEqualTypeOf<true>();
  });

  it('should handle case variations', () => {
    type Result = HasGroupBy<'SELECT user_id FROM orders group by user_id'>;
    expectTypeOf<Result>().toEqualTypeOf<true>();
  });

  it('should return false when no GROUP BY', () => {
    type Result = HasGroupBy<'SELECT * FROM orders'>;
    expectTypeOf<Result>().toEqualTypeOf<false>();
  });
});

describe('ExtractGroupByColumns', () => {
  it('should extract single GROUP BY column', () => {
    type Result = ExtractGroupByColumns<'SELECT user_id FROM orders GROUP BY user_id'>;
    expectTypeOf<Result>().toEqualTypeOf<['user_id']>();
  });

  it('should extract multiple GROUP BY columns', () => {
    type Result = ExtractGroupByColumns<'SELECT user_id, status FROM orders GROUP BY user_id, status'>;
    expectTypeOf<Result>().toEqualTypeOf<['user_id', 'status']>();
  });

  it('should return empty array when no GROUP BY', () => {
    type Result = ExtractGroupByColumns<'SELECT * FROM orders'>;
    expectTypeOf<Result>().toEqualTypeOf<[]>();
  });
});

// =============================================================================
// COMPLETE SELECT EXPRESSION PARSING TESTS
// =============================================================================

describe('ParseSelectExpression', () => {
  it('should parse COUNT(*) AS count', () => {
    type Result = ParseSelectExpression<'COUNT(*) AS count', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<{ name: 'count'; type: 'number' }>();
  });

  it('should parse SUM(total) AS total_sum', () => {
    type Result = ParseSelectExpression<'SUM(total) AS total_sum', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<{ name: 'total_sum'; type: 'number' }>();
  });

  it('should parse arithmetic expression with alias', () => {
    type Result = ParseSelectExpression<'total * 1.1 AS with_tax', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<{ name: 'with_tax'; type: 'number' }>();
  });

  it('should parse simple column', () => {
    type Result = ParseSelectExpression<'user_id', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<{ name: 'user_id'; type: 'number' }>();
  });

  it('should parse column with alias', () => {
    type Result = ParseSelectExpression<'user_id AS uid', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<{ name: 'uid'; type: 'number' }>();
  });
});

// =============================================================================
// MULTIPLE EXPRESSIONS PARSING TESTS
// =============================================================================

// Helper to simplify intersection types for testing
type Simplify<T> = { [K in keyof T]: T[K] };

describe('ParseSelectExpressions', () => {
  it('should parse multiple aggregates', () => {
    type Result = Simplify<ParseSelectExpressions<
      ['COUNT(*) AS count', 'SUM(total) AS total_sum'],
      OrderColumns
    >>;
    expectTypeOf<Result>().toEqualTypeOf<{
      count: number;
      total_sum: number;
    }>();
  });

  it('should parse mixed columns and aggregates', () => {
    type Result = Simplify<ParseSelectExpressions<
      ['user_id', 'COUNT(*) AS order_count', 'SUM(total) AS total_sum'],
      OrderColumns
    >>;
    expectTypeOf<Result>().toEqualTypeOf<{
      user_id: number;
      order_count: number;
      total_sum: number;
    }>();
  });

  it('should parse arithmetic expressions', () => {
    type Result = Simplify<ParseSelectExpressions<
      ['total', 'total * 1.1 AS with_tax'],
      OrderColumns
    >>;
    expectTypeOf<Result>().toEqualTypeOf<{
      total: number;
      with_tax: number;
    }>();
  });
});

// =============================================================================
// INTEGRATION TESTS - EXAMPLE QUERIES FROM TASK DESCRIPTION
// =============================================================================

describe('Integration Tests - Task Examples', () => {
  /**
   * Example 1: Simple COUNT(*) AS count
   * Should infer: { count: number }[]
   */
  it('should infer { count: number } for COUNT(*) as count', () => {
    type Result = Simplify<ParseSelectExpressions<['COUNT(*) as count'], OrderColumns>>;
    expectTypeOf<Result>().toEqualTypeOf<{ count: number }>();
  });

  /**
   * Example 2: GROUP BY with aggregates
   * SELECT user_id, SUM(total) as total_sum, COUNT(*) as order_count
   * Should infer: { user_id: number; total_sum: number; order_count: number }[]
   */
  it('should infer grouped result type with aggregates', () => {
    type Result = Simplify<ParseSelectExpressions<
      ['user_id', 'SUM(total) as total_sum', 'COUNT(*) as order_count'],
      OrderColumns
    >>;
    expectTypeOf<Result>().toEqualTypeOf<{
      user_id: number;
      total_sum: number;
      order_count: number;
    }>();
  });

  /**
   * Example 3: Arithmetic expression
   * SELECT total * 1.1 as with_tax
   * Should infer: { with_tax: number }[]
   */
  it('should infer { with_tax: number } for arithmetic expression', () => {
    type Result = Simplify<ParseSelectExpressions<['total * 1.1 as with_tax'], OrderColumns>>;
    expectTypeOf<Result>().toEqualTypeOf<{ with_tax: number }>();
  });

  /**
   * Complex example: Multiple expressions
   */
  it('should handle complex multi-expression query', () => {
    type Result = Simplify<ParseSelectExpressions<
      [
        'user_id',
        'status',
        'MIN(total) AS min_order',
        'MAX(total) AS max_order',
        'AVG(total) AS avg_order',
        'COUNT(*) AS num_orders'
      ],
      OrderColumns
    >>;
    expectTypeOf<Result>().toEqualTypeOf<{
      user_id: number;
      status: string;
      min_order: number;
      max_order: number;
      avg_order: number;
      num_orders: number;
    }>();
  });
});

// =============================================================================
// EDGE CASE TESTS
// =============================================================================

describe('Edge Cases', () => {
  it('should handle lowercase aggregate functions', () => {
    type Result = ParseSelectExpression<'count(*) as cnt', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<{ name: 'cnt'; type: 'number' }>();
  });

  it('should handle mixed case', () => {
    type Result = ParseSelectExpression<'Sum(total) As Total', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<{ name: 'total'; type: 'number' }>();
  });

  it('should handle spaces around operators', () => {
    type Result = ParseSelectExpression<'total  +  10 AS adjusted', OrderColumns>;
    expectTypeOf<Result>().toEqualTypeOf<{ name: 'adjusted'; type: 'number' }>();
  });

  it('should handle table.column reference', () => {
    type Result = ResolveExpressionType<'orders.total', {}, TestDB>;
    expectTypeOf<Result>().toEqualTypeOf<'number'>();
  });
});
