/**
 * DoSQL ATTACH/DETACH Database Module
 *
 * Provides multi-database support following SQLite semantics:
 * - ATTACH DATABASE 'path' AS alias
 * - DETACH DATABASE alias
 * - Cross-database queries
 * - Schema qualification
 */

// Types
export type {
  DatabaseConnectionType,
  DatabaseConnectionOptions,
  AttachedDatabase,
  QualifiedTableRef,
  QualifiedColumnRef,
  ResolvedTableRef,
  ResolvedColumnRef,
  AttachOptions,
  AttachResult,
  DetachResult,
  DatabaseListEntry,
  MultiDatabaseTransaction,
  CrossDatabaseTransactionOptions,
  QueryScope,
  AmbiguityCheck,
  AttachError,
  AttachErrorCode,
  DatabaseManager,
  SchemaResolver,
} from './types.js';

export {
  RESERVED_DATABASE_ALIASES,
  MAX_ATTACHED_DATABASES,
} from './types.js';

// Manager
export {
  AttachmentManager,
  InMemoryDatabaseConnection,
  DefaultConnectionFactory,
  createDatabaseManager,
} from './manager.js';

export type {
  DatabaseConnectionFactory,
  DatabaseConnection,
} from './manager.js';

// Resolver
export {
  AttachSchemaResolver,
  createSchemaResolver,
  parseQualifiedTableName,
  parseQualifiedColumnName,
  formatQualifiedTableName,
  formatQualifiedColumnName,
  parseAttachStatement,
  parseDetachStatement,
  isAttachStatement,
  isDetachStatement,
  isDatabaseListPragma,
} from './resolver.js';
