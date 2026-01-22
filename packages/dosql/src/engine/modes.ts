/**
 * DoSQL Query Engine Mode Separation
 *
 * Provides two execution modes:
 * 1. READ_ONLY (Worker) - Fast queries against R2/cached data, no writes allowed
 * 2. READ_WRITE (Durable Object) - Full CRUD with transactional integrity
 *
 * @example
 * ```typescript
 * import { QueryMode, ModeEnforcer, ReadOnlyError, isWriteOperation } from './modes.js';
 *
 * // Check if a SQL statement is a write operation
 * isWriteOperation('SELECT * FROM users'); // false
 * isWriteOperation('INSERT INTO users ...'); // true
 *
 * // Enforce read-only mode
 * const enforcer = new ModeEnforcer(QueryMode.READ_ONLY);
 * enforcer.enforce('SELECT * FROM users'); // OK
 * enforcer.enforce('INSERT INTO users ...'); // throws ReadOnlyError
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// QUERY MODE
// =============================================================================

/**
 * Query execution mode
 *
 * - READ_ONLY: For Worker execution against R2/cached data. No writes allowed.
 * - READ_WRITE: For Durable Object execution with full CRUD and transactions.
 */
export enum QueryMode {
  /** Read-only mode for Worker execution */
  READ_ONLY = 'read-only',
  /** Read-write mode for Durable Object execution */
  READ_WRITE = 'read-write',
}

// =============================================================================
// ERRORS
// =============================================================================

/**
 * Error thrown when attempting a write operation in read-only mode.
 *
 * This error indicates that the query engine is running in a Worker context
 * where writes are not allowed. Write operations must be routed through
 * a Durable Object.
 */
export class ReadOnlyError extends Error {
  /** The operation that was attempted */
  public readonly operation: string;

  /** Error name for serialization */
  public override readonly name = 'ReadOnlyError';

  constructor(operation: string) {
    super(
      `Cannot execute ${operation} in read-only mode. Writes must go through a Durable Object.`
    );
    this.operation = operation;

    // Maintain proper stack trace for where our error was thrown (only available on V8)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, ReadOnlyError);
    }
  }

  /**
   * Convert to a plain object for JSON serialization
   */
  toJSON(): { name: string; message: string; operation: string } {
    return {
      name: this.name,
      message: this.message,
      operation: this.operation,
    };
  }
}

// =============================================================================
// WRITE OPERATION DETECTION
// =============================================================================

/**
 * SQL statements that modify data or schema
 */
const WRITE_OPERATIONS = [
  'INSERT',
  'UPDATE',
  'DELETE',
  'CREATE',
  'ALTER',
  'DROP',
  'TRUNCATE',
  'REPLACE',
  'UPSERT',
  'MERGE',
] as const;

/**
 * Regex pattern for detecting write operations.
 * Matches at the start of the SQL statement (after optional whitespace).
 */
const WRITE_OPERATION_PATTERN = new RegExp(
  `^\\s*(${WRITE_OPERATIONS.join('|')})\\b`,
  'i'
);

/**
 * Check if a SQL statement is a write operation.
 *
 * Detects INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, TRUNCATE,
 * REPLACE, UPSERT, and MERGE statements.
 *
 * @param sql - The SQL statement to check
 * @returns true if the statement is a write operation
 *
 * @example
 * ```typescript
 * isWriteOperation('SELECT * FROM users'); // false
 * isWriteOperation('INSERT INTO users (name) VALUES (\'Alice\')'); // true
 * isWriteOperation('  UPDATE users SET name = \'Bob\''); // true
 * isWriteOperation('CREATE TABLE users (id INT)'); // true
 * isWriteOperation('-- comment\nSELECT 1'); // false (comment is not a write)
 * ```
 */
export function isWriteOperation(sql: string): boolean {
  // Handle SQL comments at the start
  const trimmed = sql.replace(/^(\s*--[^\n]*\n)*\s*/i, '');
  return WRITE_OPERATION_PATTERN.test(trimmed);
}

/**
 * Extract the operation type from a SQL statement.
 *
 * @param sql - The SQL statement to parse
 * @returns The operation type (e.g., 'SELECT', 'INSERT') or null if not recognized
 */
export function extractOperation(sql: string): string | null {
  // Handle SQL comments at the start
  const trimmed = sql.replace(/^(\s*--[^\n]*\n)*\s*/i, '');
  const match = trimmed.match(/^(\w+)/i);
  return match ? match[1].toUpperCase() : null;
}

// =============================================================================
// MODE ENFORCER
// =============================================================================

/**
 * Enforces query mode restrictions.
 *
 * In READ_ONLY mode, write operations are rejected with a ReadOnlyError.
 * In READ_WRITE mode, all operations are allowed.
 *
 * @example
 * ```typescript
 * const enforcer = new ModeEnforcer(QueryMode.READ_ONLY);
 *
 * // This is OK
 * enforcer.enforce('SELECT * FROM users');
 *
 * // This throws ReadOnlyError
 * enforcer.enforce('INSERT INTO users (name) VALUES (\'Alice\')');
 * ```
 */
export class ModeEnforcer {
  /** The current query mode */
  private readonly mode: QueryMode;

  /**
   * Create a new mode enforcer.
   *
   * @param mode - The query mode to enforce
   */
  constructor(mode: QueryMode) {
    this.mode = mode;
  }

  /**
   * Get the current mode.
   */
  getMode(): QueryMode {
    return this.mode;
  }

  /**
   * Check if the enforcer is in read-only mode.
   */
  isReadOnly(): boolean {
    return this.mode === QueryMode.READ_ONLY;
  }

  /**
   * Check if a SQL statement is allowed in the current mode.
   *
   * @param sql - The SQL statement to check
   * @returns true if the statement is allowed
   */
  isAllowed(sql: string): boolean {
    if (this.mode === QueryMode.READ_WRITE) {
      return true;
    }
    return !isWriteOperation(sql);
  }

  /**
   * Enforce mode restrictions on a SQL statement.
   * Throws ReadOnlyError if the statement is not allowed in the current mode.
   *
   * @param sql - The SQL statement to enforce
   * @throws ReadOnlyError if the statement is a write in read-only mode
   */
  enforce(sql: string): void {
    if (this.mode === QueryMode.READ_ONLY && isWriteOperation(sql)) {
      const operation = extractOperation(sql) || 'UNKNOWN';
      throw new ReadOnlyError(operation);
    }
  }

  /**
   * Enforce mode restrictions and return whether the operation is a write.
   *
   * @param sql - The SQL statement to check
   * @returns Object with isWrite flag and operation type
   * @throws ReadOnlyError if the statement is a write in read-only mode
   */
  enforceAndClassify(sql: string): { isWrite: boolean; operation: string | null } {
    const operation = extractOperation(sql);
    const isWrite = isWriteOperation(sql);

    if (this.mode === QueryMode.READ_ONLY && isWrite) {
      throw new ReadOnlyError(operation || 'UNKNOWN');
    }

    return { isWrite, operation };
  }
}

// =============================================================================
// UTILITY TYPES
// =============================================================================

/**
 * Type guard for ReadOnlyError
 */
export function isReadOnlyError(error: unknown): error is ReadOnlyError {
  return error instanceof ReadOnlyError;
}

/**
 * Options for mode enforcement
 */
export interface ModeEnforcementOptions {
  /** The query mode to use */
  mode: QueryMode;

  /** Whether to allow DDL statements in read-only mode (default: false) */
  allowDDLInReadOnly?: boolean;

  /** Custom write operation patterns to detect */
  customWritePatterns?: RegExp[];
}

/**
 * Create a mode enforcer with options.
 *
 * @param options - Enforcement options
 * @returns A configured ModeEnforcer
 */
export function createModeEnforcer(options: ModeEnforcementOptions): ModeEnforcer {
  // Future: Support custom patterns and DDL options
  return new ModeEnforcer(options.mode);
}
