/**
 * SQL Tokenizer - Re-exports from sql-tokenizer module
 *
 * This module re-exports the SQL tokenizer functionality for use
 * within the parser module.
 *
 * @packageDocumentation
 */

// =============================================================================
// RE-EXPORT ALL TOKENIZER FUNCTIONALITY
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
} from '../sql-tokenizer.js';
