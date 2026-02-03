/**
 * Audit Logging Types for DoSQL
 *
 * Defines interfaces for SOC2/GDPR-compliant audit logging that tracks
 * WHO (user/role), WHAT (operation), WHEN (timestamp), WHERE (table/row),
 * and HOW (query) for all data access and modifications.
 *
 * @packageDocumentation
 */

// =============================================================================
// Branded Types
// =============================================================================

/**
 * Branded type for audit entry IDs to prevent mixing with other string IDs
 */
export type AuditEntryId = string & { readonly __brand: 'AuditEntryId' };

// =============================================================================
// Enums and Constants
// =============================================================================

/**
 * Categories of auditable operations
 */
export type AuditOperation =
  | 'SELECT'
  | 'INSERT'
  | 'UPDATE'
  | 'DELETE'
  | 'CREATE_TABLE'
  | 'DROP_TABLE'
  | 'ALTER_TABLE'
  | 'CREATE_INDEX'
  | 'DROP_INDEX'
  | 'BEGIN'
  | 'COMMIT'
  | 'ROLLBACK'
  | 'GRANT'
  | 'REVOKE'
  | 'LOGIN'
  | 'LOGOUT'
  | 'LOGIN_FAILED'
  | 'SCHEMA_CHANGE'
  | 'EXPORT'
  | 'ADMIN';

/**
 * Audit event severity levels for prioritizing review
 */
export type AuditSeverity = 'low' | 'medium' | 'high' | 'critical';

/**
 * Outcome of the audited operation
 */
export type AuditOutcome = 'success' | 'failure' | 'denied';

/**
 * Error codes specific to the audit module
 */
export enum AuditErrorCode {
  /** Log storage is full or unavailable */
  STORAGE_ERROR = 'AUDIT_STORAGE_ERROR',
  /** Tamper detection found integrity violation */
  INTEGRITY_VIOLATION = 'AUDIT_INTEGRITY_VIOLATION',
  /** Retention policy violation */
  RETENTION_VIOLATION = 'AUDIT_RETENTION_VIOLATION',
  /** Invalid audit entry format */
  INVALID_ENTRY = 'AUDIT_INVALID_ENTRY',
  /** Attempt to mutate an immutable log entry */
  IMMUTABLE_VIOLATION = 'AUDIT_IMMUTABLE_VIOLATION',
}

// =============================================================================
// Core Interfaces
// =============================================================================

/**
 * Identifies WHO performed the action
 */
export interface AuditActor {
  /** User ID or service account identifier */
  userId: string;
  /** Role or permission set of the actor */
  role?: string | undefined;
  /** IP address or origin of the request */
  sourceIp?: string | undefined;
  /** User agent or client identifier */
  userAgent?: string | undefined;
  /** Session or token identifier (redacted for security) */
  sessionId?: string | undefined;
}

/**
 * Identifies WHERE the action was performed
 */
export interface AuditTarget {
  /** Database name or identifier */
  database?: string | undefined;
  /** Table name */
  table?: string | undefined;
  /** Specific row identifier(s) affected */
  rowIds?: string[] | undefined;
  /** Column names accessed or modified */
  columns?: string[] | undefined;
}

/**
 * An audit event captures a single auditable action in the system.
 *
 * This is the input format used when logging an audit event. The AuditLogger
 * enriches this into a full AuditEntry with ID, hash chain, and metadata.
 */
export interface AuditEvent {
  /** WHO performed the action */
  actor: AuditActor;
  /** WHAT operation was performed */
  operation: AuditOperation;
  /** WHERE the action was performed (table, row, columns) */
  target: AuditTarget;
  /** HOW - the SQL query or command executed */
  query?: string | undefined;
  /** Query parameters (sensitive values should be redacted) */
  queryParams?: unknown[] | undefined;
  /** Result of the operation */
  outcome: AuditOutcome;
  /** Number of rows affected */
  rowsAffected?: number | undefined;
  /** Severity classification */
  severity?: AuditSeverity | undefined;
  /** Error message if outcome is failure or denied */
  errorMessage?: string | undefined;
  /** Additional metadata */
  metadata?: Record<string, unknown> | undefined;
  /** Trace ID for correlating with distributed tracing */
  traceId?: string | undefined;
  /** Transaction ID if within a transaction */
  transactionId?: string | undefined;
}

/**
 * A complete, immutable audit log entry.
 *
 * Extends AuditEvent with system-generated fields for integrity,
 * ordering, and tamper detection. Once created, entries cannot be
 * modified or deleted (within the retention period).
 */
export interface AuditEntry extends AuditEvent {
  /** Unique identifier for this audit entry */
  id: AuditEntryId;
  /** WHEN the action occurred (ISO 8601 with microsecond precision) */
  timestamp: string;
  /** Monotonically increasing sequence number for ordering */
  sequence: number;
  /** SHA-256 hash of this entry's content for integrity verification */
  hash: string;
  /** Hash of the previous entry for tamper-evident chain */
  previousHash: string;
  /** Version of the audit log format */
  version: number;
}

// =============================================================================
// Configuration
// =============================================================================

/**
 * Retention policy for audit logs.
 *
 * SOC2 typically requires 1 year minimum retention.
 * GDPR requires data minimization but audit logs for legitimate
 * compliance purposes are generally exempt from deletion requests.
 */
export interface AuditRetentionPolicy {
  /** Minimum retention period in days (SOC2: typically 365) */
  minRetentionDays: number;
  /** Maximum retention period in days (GDPR: data minimization) */
  maxRetentionDays: number;
  /** Maximum number of entries to retain (0 = unlimited) */
  maxEntries: number;
  /** Whether to archive entries before deletion */
  archiveBeforeDelete: boolean;
}

/**
 * Default retention policy meeting SOC2 requirements
 */
export const DEFAULT_RETENTION_POLICY: AuditRetentionPolicy = {
  minRetentionDays: 365,
  maxRetentionDays: 2555, // ~7 years
  maxEntries: 0, // unlimited
  archiveBeforeDelete: true,
};

/**
 * Configuration for the audit logger
 */
export interface AuditLoggerConfig {
  /** Whether audit logging is enabled */
  enabled: boolean;
  /** Retention policy */
  retention: AuditRetentionPolicy;
  /** Operations to include (empty = all operations) */
  includeOperations?: AuditOperation[] | undefined;
  /** Operations to exclude from logging */
  excludeOperations?: AuditOperation[] | undefined;
  /** Whether to log SELECT queries (can be high volume) */
  logReads: boolean;
  /** Whether to redact query parameters in the audit log */
  redactParams: boolean;
  /** Maximum query length to store (truncate longer queries) */
  maxQueryLength: number;
  /** Severity threshold - only log events at or above this severity */
  minSeverity?: AuditSeverity | undefined;
}

/**
 * Default audit logger configuration
 */
export const DEFAULT_AUDIT_CONFIG: AuditLoggerConfig = {
  enabled: true,
  retention: DEFAULT_RETENTION_POLICY,
  logReads: true,
  redactParams: true,
  maxQueryLength: 4096,
};

// =============================================================================
// Query and Filter Interfaces
// =============================================================================

/**
 * Options for querying audit log entries
 */
export interface AuditQueryOptions {
  /** Filter by actor user ID */
  userId?: string | undefined;
  /** Filter by operation type */
  operation?: AuditOperation | undefined;
  /** Filter by operations (multiple) */
  operations?: AuditOperation[] | undefined;
  /** Filter by target table */
  table?: string | undefined;
  /** Filter by outcome */
  outcome?: AuditOutcome | undefined;
  /** Filter by severity */
  severity?: AuditSeverity | undefined;
  /** Filter entries after this timestamp (ISO 8601) */
  startTime?: string | undefined;
  /** Filter entries before this timestamp (ISO 8601) */
  endTime?: string | undefined;
  /** Filter by trace ID */
  traceId?: string | undefined;
  /** Filter by transaction ID */
  transactionId?: string | undefined;
  /** Maximum number of entries to return */
  limit?: number | undefined;
  /** Offset for pagination */
  offset?: number | undefined;
}

// =============================================================================
// Integrity Verification
// =============================================================================

/**
 * Result of an integrity verification check
 */
export interface IntegrityCheckResult {
  /** Whether the audit log passed integrity verification */
  valid: boolean;
  /** Total entries checked */
  entriesChecked: number;
  /** First entry that failed verification (if any) */
  firstFailure?: {
    entryId: AuditEntryId;
    sequence: number;
    reason: string;
  } | undefined;
  /** Timestamp of the verification */
  checkedAt: string;
  /** Range of sequences verified */
  sequenceRange: {
    start: number;
    end: number;
  };
}

// =============================================================================
// Statistics
// =============================================================================

/**
 * Audit log statistics for monitoring and capacity planning
 */
export interface AuditStats {
  /** Total number of audit entries */
  totalEntries: number;
  /** Entries by operation type */
  entriesByOperation: Partial<Record<AuditOperation, number>>;
  /** Entries by outcome */
  entriesByOutcome: Record<AuditOutcome, number>;
  /** Oldest entry timestamp */
  oldestEntry?: string | undefined;
  /** Newest entry timestamp */
  newestEntry?: string | undefined;
  /** Current sequence number */
  currentSequence: number;
}

// =============================================================================
// Audit Logger Interface
// =============================================================================

/**
 * Core audit logger interface.
 *
 * Provides methods for logging audit events, querying the audit log,
 * and verifying integrity. All entries are append-only and form a
 * tamper-evident hash chain.
 */
export interface AuditLoggerInterface {
  /** Log an audit event */
  log(event: AuditEvent): Promise<AuditEntry>;

  /** Query audit log entries with filtering */
  query(options: AuditQueryOptions): Promise<AuditEntry[]>;

  /** Get a specific audit entry by ID */
  getEntry(id: AuditEntryId): Promise<AuditEntry | null>;

  /** Verify the integrity of the audit log hash chain */
  verifyIntegrity(options?: { startSequence?: number; endSequence?: number }): Promise<IntegrityCheckResult>;

  /** Get audit log statistics */
  getStats(): Promise<AuditStats>;

  /** Apply retention policy, removing entries older than the maximum retention period */
  applyRetention(): Promise<{ entriesRemoved: number; archivedTo?: string }>;

  /** Export audit entries for compliance reporting */
  export(options: AuditQueryOptions): Promise<AuditEntry[]>;
}

/**
 * Error class for audit-specific errors
 */
export class AuditError extends Error {
  readonly code: AuditErrorCode;

  constructor(code: AuditErrorCode, message: string) {
    super(message);
    this.name = 'AuditError';
    this.code = code;
  }
}
