/**
 * DoSQL Constraints Module
 *
 * Provides foreign key and constraint support for DoSQL.
 */

// Types
export * from './types.js';

// Validator
export {
  ConstraintError,
  ConstraintValidator,
  createConstraintValidator,
  evaluateCheckExpression,
  type DataAccessor,
  InMemoryDataAccessor,
  createInMemoryDataAccessor,
} from './validator.js';

// Foreign Key Handler
export {
  ForeignKeyHandler,
  createForeignKeyHandler,
  ForeignKeyBuilder,
  foreignKey,
  parseForeignKeyDefinition,
  parseColumnForeignKey,
  foreignKeyToSql,
  checkForeignKeyIntegrity,
  pragmaForeignKeyCheck,
  type DeleteCheckResult,
  type UpdateCheckResult,
  type CascadeOperation,
  type ForeignKeyCheckResult,
} from './foreign-key.js';
