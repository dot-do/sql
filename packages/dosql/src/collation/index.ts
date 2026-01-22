/**
 * Collation Module for DoSQL
 *
 * Provides SQLite-compatible COLLATE support including:
 * - Built-in collations: BINARY, NOCASE, RTRIM
 * - Custom collation registration
 * - Unicode/ICU-style collations
 * - SQL parsing for COLLATE clauses
 */

// Types
export type {
  CollationFunction,
  CollationDefinition,
  CreateCollationOptions,
  CollateClause,
  ColumnCollation,
  IndexCollation,
  CreateCollationStatement,
  DropCollationStatement,
  UnicodeCollationOptions,
  BuiltinCollation,
  CaseSensitiveLike,
  CollationCompareResult,
} from './types.js';

export {
  CollationNotFoundError,
  CollationExistsError,
  BuiltinCollationError,
} from './types.js';

// Built-in collations
export {
  binaryCollation,
  nocaseCollation,
  rtrimCollation,
  BUILTIN_COLLATIONS,
  getBuiltinCollation,
  isBuiltinCollation,
  getBuiltinCollationNames,
  createUnicodeCollation,
  createCaseInsensitiveCollation,
  createNaturalSortCollation,
  createLocaleCollationDefinition,
} from './builtin.js';

// Registry
export type { CollationRegistry } from './registry.js';

export {
  createCollationRegistry,
  parseCollateClause,
  parseColumnCollation,
  isCreateCollation,
  parseCreateCollation,
  isDropCollation,
  parseDropCollation,
  parseIndexCollation,
  sortWithCollation,
  equalsWithCollation,
  createCollationComparator,
  registerUnicodeCollation,
} from './registry.js';
