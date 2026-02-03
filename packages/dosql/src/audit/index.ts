/**
 * Audit Logging Module for DoSQL
 *
 * Provides SOC2/GDPR-compliant audit logging that tracks:
 * - WHO (user/role) performed an action
 * - WHAT (operation) was performed
 * - WHEN (timestamp) it occurred
 * - WHERE (table/row) the action was directed
 * - HOW (query) the action was executed
 *
 * Features:
 * - Immutable append-only log with SHA-256 hash chain for tamper detection
 * - Configurable retention policies (SOC2: 1 year minimum)
 * - Query filtering by actor, operation, table, time range, and more
 * - Automatic severity classification
 * - Sensitive parameter redaction for GDPR compliance
 * - Integrity verification for the entire log chain
 *
 * @example Basic Usage
 * ```typescript
 * import { createAuditLogger } from 'dosql/audit';
 *
 * const audit = createAuditLogger({ logReads: true });
 *
 * // Log a data modification
 * await audit.log({
 *   actor: { userId: 'user_123', role: 'admin' },
 *   operation: 'UPDATE',
 *   target: { table: 'users', rowIds: ['42'] },
 *   query: 'UPDATE users SET name = ? WHERE id = ?',
 *   queryParams: ['Alice', 42],
 *   outcome: 'success',
 *   rowsAffected: 1,
 * });
 *
 * // Verify integrity
 * const check = await audit.verifyIntegrity();
 * console.log(check.valid); // true
 *
 * // Query audit log
 * const entries = await audit.query({
 *   userId: 'user_123',
 *   operation: 'UPDATE',
 * });
 * ```
 *
 * @packageDocumentation
 */

// Types
export {
  // Branded types
  type AuditEntryId,

  // Core types
  type AuditEvent,
  type AuditEntry,
  type AuditActor,
  type AuditTarget,

  // Enums
  type AuditOperation,
  type AuditSeverity,
  type AuditOutcome,
  AuditErrorCode,

  // Configuration
  type AuditLoggerConfig,
  type AuditRetentionPolicy,
  DEFAULT_AUDIT_CONFIG,
  DEFAULT_RETENTION_POLICY,

  // Query
  type AuditQueryOptions,

  // Integrity
  type IntegrityCheckResult,

  // Stats
  type AuditStats,

  // Logger interface
  type AuditLoggerInterface,

  // Error class
  AuditError,
} from './types.js';

// Implementation
export {
  AuditLogger,
  createAuditLogger,
} from './audit-log.js';
