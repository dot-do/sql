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
  createErrorFromException,
  type SourceLocation,
} from '../../errors/index.js';
