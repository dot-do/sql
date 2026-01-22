/**
 * Shared Parser Error Classes
 *
 * Re-exports from the unified error module.
 * Kept for backwards compatibility.
 *
 * @packageDocumentation
 */

// Re-export everything from the errors module for backwards compatibility
export {
  SQLSyntaxError,
  UnexpectedTokenError,
  UnexpectedEOFError,
  MissingKeywordError,
  InvalidIdentifierError,
  createErrorFromException,
  createUnexpectedTokenError,
  createUnexpectedEOFError,
  createMissingKeywordError,
  createInvalidIdentifierError,
  createSyntaxError,
  // Utility functions
  calculateLocation,
  formatErrorSnippet,
  getSuggestionForTypo,
  levenshteinDistance,
  COMMON_TYPOS,
  type SourceLocation,
} from '../../errors/index.js';
