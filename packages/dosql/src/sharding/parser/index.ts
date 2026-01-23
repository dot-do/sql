/**
 * SQL Parser Module for Query Routing
 *
 * This module provides SQL parsing capabilities for the sharding router.
 * It extracts query metadata needed for shard routing decisions.
 *
 * Key features:
 * - State-aware tokenization
 * - Proper handling of comments and string literals
 * - WHERE clause extraction for shard key detection
 * - Support for SELECT, INSERT, UPDATE, DELETE operations
 *
 * @example Basic usage
 * ```typescript
 * import { SQLParser } from './parser/index.js';
 *
 * const parser = new SQLParser();
 * const parsed = parser.parse('SELECT * FROM users WHERE tenant_id = 123');
 *
 * console.log(parsed.operation); // 'SELECT'
 * console.log(parsed.tables[0].name); // 'users'
 * console.log(parsed.where?.conditions[0].column); // 'tenant_id'
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// SQL PARSER EXPORTS
// =============================================================================

export { SQLParser } from './sql-parser.js';

// =============================================================================
// TYPE EXPORTS
// =============================================================================

export type {
  ParsedQuery,
  TableReference,
  ColumnReference,
  WhereClause,
  WhereCondition,
  WhereOperator,
} from './types.js';

// =============================================================================
// TOKENIZER EXPORTS
// =============================================================================

export {
  // Types
  type TokenType,
  type SQLToken,

  // Tokenizer class
  SQLTokenizer,

  // Helper functions
  tokenizeSQL,
  stripComments,
  getMeaningfulTokens,
  findKeyword,
  findKeywordIndex,
  getTokensBetweenKeywords,

  // Shard key extraction
  extractShardKeyFromTokens,

  // Condition parsing
  type ParsedCondition,
  extractConditionsFromTokens,
} from './tokenizer.js';
