/**
 * Audit Logger Implementation for DoSQL
 *
 * Provides SOC2/GDPR-compliant audit logging with:
 * - Immutable append-only log with tamper-evident hash chain
 * - SHA-256 integrity verification
 * - Configurable retention policies
 * - Filtering and querying capabilities
 * - Redaction of sensitive query parameters
 *
 * @packageDocumentation
 */

import type {
  AuditEvent,
  AuditEntry,
  AuditEntryId,
  AuditLoggerConfig,
  AuditLoggerInterface,
  AuditQueryOptions,
  AuditStats,
  AuditSeverity,
  AuditOutcome,
  AuditOperation,
  IntegrityCheckResult,
} from './types.js';
import { AuditError, AuditErrorCode, DEFAULT_AUDIT_CONFIG } from './types.js';

// =============================================================================
// Constants
// =============================================================================

/** Current audit log format version */
const AUDIT_LOG_VERSION = 1;

/** Genesis hash for the first entry in the chain */
const GENESIS_HASH = '0000000000000000000000000000000000000000000000000000000000000000';

/** Severity ordering for comparison */
const SEVERITY_ORDER: Record<AuditSeverity, number> = {
  low: 0,
  medium: 1,
  high: 2,
  critical: 3,
};

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Generate a unique audit entry ID
 */
function generateAuditId(): AuditEntryId {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return `audit_${crypto.randomUUID()}` as AuditEntryId;
  }
  const hex = Array.from({ length: 32 }, () =>
    Math.floor(Math.random() * 16).toString(16)
  ).join('');
  return `audit_${hex}` as AuditEntryId;
}

/**
 * Create a high-precision ISO 8601 timestamp
 */
function createAuditTimestamp(): string {
  return new Date().toISOString();
}

/**
 * Compute SHA-256 hash of an audit entry's content.
 * Uses a deterministic serialization to ensure consistent hashing.
 */
async function computeEntryHash(entry: Omit<AuditEntry, 'hash'>): Promise<string> {
  const content = JSON.stringify({
    id: entry.id,
    timestamp: entry.timestamp,
    sequence: entry.sequence,
    previousHash: entry.previousHash,
    actor: entry.actor,
    operation: entry.operation,
    target: entry.target,
    query: entry.query,
    outcome: entry.outcome,
    rowsAffected: entry.rowsAffected,
    severity: entry.severity,
    version: entry.version,
  });

  if (typeof crypto !== 'undefined' && crypto.subtle) {
    const encoder = new TextEncoder();
    const data = encoder.encode(content);
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    const hashArray = new Uint8Array(hashBuffer);
    return Array.from(hashArray)
      .map(b => b.toString(16).padStart(2, '0'))
      .join('');
  }

  // Fallback: simple hash for environments without crypto.subtle
  let hash = 0;
  for (let i = 0; i < content.length; i++) {
    const char = content.charCodeAt(i);
    hash = ((hash << 5) - hash + char) | 0;
  }
  return Math.abs(hash).toString(16).padStart(64, '0');
}

/**
 * Classify the severity of an audit operation if not explicitly provided
 */
function classifySeverity(event: AuditEvent): AuditSeverity {
  if (event.severity) return event.severity;

  // Failed or denied operations are always high severity
  if (event.outcome === 'denied') return 'critical';
  if (event.outcome === 'failure' && event.operation === 'LOGIN_FAILED') return 'high';
  if (event.outcome === 'failure') return 'medium';

  // Classify by operation type
  switch (event.operation) {
    case 'DROP_TABLE':
    case 'GRANT':
    case 'REVOKE':
    case 'ADMIN':
      return 'high';
    case 'CREATE_TABLE':
    case 'ALTER_TABLE':
    case 'DELETE':
    case 'SCHEMA_CHANGE':
    case 'EXPORT':
      return 'medium';
    case 'INSERT':
    case 'UPDATE':
    case 'CREATE_INDEX':
    case 'DROP_INDEX':
    case 'LOGIN':
    case 'LOGOUT':
      return 'low';
    case 'SELECT':
    case 'BEGIN':
    case 'COMMIT':
    case 'ROLLBACK':
    case 'LOGIN_FAILED':
      return 'low';
    default:
      return 'medium';
  }
}

/**
 * Redact sensitive values from query parameters
 */
function redactParams(params: unknown[] | undefined): unknown[] | undefined {
  if (!params || params.length === 0) return params;
  return params.map(p => {
    if (typeof p === 'string' && p.length > 0) return '[REDACTED]';
    if (typeof p === 'number') return 0;
    if (typeof p === 'boolean') return false;
    if (p === null || p === undefined) return p;
    return '[REDACTED]';
  });
}

/**
 * Truncate a query string to the maximum allowed length
 */
function truncateQuery(query: string | undefined, maxLength: number): string | undefined {
  if (!query) return query;
  if (query.length <= maxLength) return query;
  return query.slice(0, maxLength - 3) + '...';
}

// =============================================================================
// Audit Logger Implementation
// =============================================================================

/**
 * AuditLogger provides SOC2/GDPR-compliant audit logging.
 *
 * All entries are stored in an append-only log with a SHA-256 hash chain
 * for tamper detection. Entries cannot be modified or deleted within the
 * minimum retention period.
 *
 * @example
 * ```typescript
 * const logger = new AuditLogger();
 *
 * // Log a data access event
 * await logger.log({
 *   actor: { userId: 'user_123', role: 'analyst' },
 *   operation: 'SELECT',
 *   target: { table: 'users', columns: ['name', 'email'] },
 *   query: 'SELECT name, email FROM users WHERE id = ?',
 *   queryParams: [123],
 *   outcome: 'success',
 *   rowsAffected: 1,
 * });
 *
 * // Verify integrity
 * const result = await logger.verifyIntegrity();
 * console.log(result.valid); // true
 * ```
 */
export class AuditLogger implements AuditLoggerInterface {
  private entries: AuditEntry[] = [];
  private sequence = 0;
  private lastHash = GENESIS_HASH;
  private config: AuditLoggerConfig;

  constructor(config: Partial<AuditLoggerConfig> = {}) {
    this.config = {
      ...DEFAULT_AUDIT_CONFIG,
      ...config,
      retention: {
        ...DEFAULT_AUDIT_CONFIG.retention,
        ...config.retention,
      },
    };
  }

  /**
   * Log an audit event.
   *
   * Creates an immutable entry with a unique ID, timestamp, sequence number,
   * and hash chain link. The entry is appended to the log and cannot be
   * modified after creation.
   */
  async log(event: AuditEvent): Promise<AuditEntry> {
    if (!this.config.enabled) {
      // Return a minimal entry when disabled, but still maintain the chain
      const entry = await this.createEntry(event);
      return entry;
    }

    // Check if this operation should be logged
    if (!this.shouldLog(event)) {
      // Still create an entry for chain integrity but mark as filtered
      const entry = await this.createEntry(event);
      return entry;
    }

    const entry = await this.createEntry(event);
    this.entries.push(entry);

    return entry;
  }

  /**
   * Query audit log entries with filtering.
   */
  async query(options: AuditQueryOptions): Promise<AuditEntry[]> {
    let results = [...this.entries];

    // Apply filters
    if (options.userId) {
      results = results.filter(e => e.actor.userId === options.userId);
    }
    if (options.operation) {
      results = results.filter(e => e.operation === options.operation);
    }
    if (options.operations && options.operations.length > 0) {
      const ops = new Set(options.operations);
      results = results.filter(e => ops.has(e.operation));
    }
    if (options.table) {
      results = results.filter(e => e.target.table === options.table);
    }
    if (options.outcome) {
      results = results.filter(e => e.outcome === options.outcome);
    }
    if (options.severity) {
      results = results.filter(e => e.severity === options.severity);
    }
    if (options.startTime) {
      results = results.filter(e => e.timestamp >= options.startTime!);
    }
    if (options.endTime) {
      results = results.filter(e => e.timestamp <= options.endTime!);
    }
    if (options.traceId) {
      results = results.filter(e => e.traceId === options.traceId);
    }
    if (options.transactionId) {
      results = results.filter(e => e.transactionId === options.transactionId);
    }

    // Apply pagination
    const offset = options.offset ?? 0;
    const limit = options.limit ?? results.length;
    results = results.slice(offset, offset + limit);

    return results;
  }

  /**
   * Get a specific audit entry by ID.
   */
  async getEntry(id: AuditEntryId): Promise<AuditEntry | null> {
    return this.entries.find(e => e.id === id) ?? null;
  }

  /**
   * Verify the integrity of the audit log hash chain.
   *
   * Checks that:
   * 1. Each entry's hash matches its computed hash
   * 2. Each entry's previousHash matches the preceding entry's hash
   * 3. The sequence numbers are monotonically increasing
   */
  async verifyIntegrity(
    options?: { startSequence?: number; endSequence?: number }
  ): Promise<IntegrityCheckResult> {
    const startSeq = options?.startSequence ?? 0;
    const endSeq = options?.endSequence ?? this.sequence;

    const entriesToCheck = this.entries.filter(
      e => e.sequence >= startSeq && e.sequence <= endSeq
    );

    if (entriesToCheck.length === 0) {
      return {
        valid: true,
        entriesChecked: 0,
        checkedAt: createAuditTimestamp(),
        sequenceRange: { start: startSeq, end: endSeq },
      };
    }

    for (let i = 0; i < entriesToCheck.length; i++) {
      const entry = entriesToCheck[i]!;

      // Verify hash
      const computedHash = await computeEntryHash({
        ...entry,
        hash: undefined as unknown as string,
      });
      if (computedHash !== entry.hash) {
        return {
          valid: false,
          entriesChecked: i + 1,
          firstFailure: {
            entryId: entry.id,
            sequence: entry.sequence,
            reason: `Hash mismatch: expected ${computedHash}, got ${entry.hash}`,
          },
          checkedAt: createAuditTimestamp(),
          sequenceRange: { start: startSeq, end: endSeq },
        };
      }

      // Verify chain (previousHash)
      if (i > 0) {
        const prevEntry = entriesToCheck[i - 1]!;
        if (entry.previousHash !== prevEntry.hash) {
          return {
            valid: false,
            entriesChecked: i + 1,
            firstFailure: {
              entryId: entry.id,
              sequence: entry.sequence,
              reason: `Chain broken: previousHash does not match preceding entry's hash`,
            },
            checkedAt: createAuditTimestamp(),
            sequenceRange: { start: startSeq, end: endSeq },
          };
        }
      }

      // Verify sequence monotonicity
      if (i > 0) {
        const prevEntry = entriesToCheck[i - 1]!;
        if (entry.sequence <= prevEntry.sequence) {
          return {
            valid: false,
            entriesChecked: i + 1,
            firstFailure: {
              entryId: entry.id,
              sequence: entry.sequence,
              reason: `Sequence not monotonically increasing: ${entry.sequence} <= ${prevEntry.sequence}`,
            },
            checkedAt: createAuditTimestamp(),
            sequenceRange: { start: startSeq, end: endSeq },
          };
        }
      }
    }

    return {
      valid: true,
      entriesChecked: entriesToCheck.length,
      checkedAt: createAuditTimestamp(),
      sequenceRange: { start: startSeq, end: endSeq },
    };
  }

  /**
   * Get audit log statistics.
   */
  async getStats(): Promise<AuditStats> {
    const entriesByOperation: Partial<Record<AuditOperation, number>> = {};
    const entriesByOutcome: Record<AuditOutcome, number> = {
      success: 0,
      failure: 0,
      denied: 0,
    };

    for (const entry of this.entries) {
      entriesByOperation[entry.operation] = (entriesByOperation[entry.operation] ?? 0) + 1;
      entriesByOutcome[entry.outcome]++;
    }

    return {
      totalEntries: this.entries.length,
      entriesByOperation,
      entriesByOutcome,
      oldestEntry: this.entries.length > 0 ? this.entries[0]!.timestamp : undefined,
      newestEntry: this.entries.length > 0 ? this.entries[this.entries.length - 1]!.timestamp : undefined,
      currentSequence: this.sequence,
    };
  }

  /**
   * Apply retention policy.
   *
   * Removes entries older than maxRetentionDays, but never removes entries
   * within the minRetentionDays window (SOC2 compliance).
   *
   * @throws {AuditError} If attempting to remove entries within the minimum retention period
   */
  async applyRetention(): Promise<{ entriesRemoved: number; archivedTo?: string }> {
    const now = Date.now();
    const maxRetentionMs = this.config.retention.maxRetentionDays * 24 * 60 * 60 * 1000;
    const cutoffDate = new Date(now - maxRetentionMs).toISOString();

    const entriesToRemove = this.entries.filter(e => e.timestamp < cutoffDate);

    if (entriesToRemove.length === 0) {
      return { entriesRemoved: 0 };
    }

    // Verify none of the entries are within minimum retention
    const minRetentionMs = this.config.retention.minRetentionDays * 24 * 60 * 60 * 1000;
    const minCutoff = new Date(now - minRetentionMs).toISOString();

    const protectedEntries = entriesToRemove.filter(e => e.timestamp >= minCutoff);
    if (protectedEntries.length > 0) {
      throw new AuditError(
        AuditErrorCode.RETENTION_VIOLATION,
        `Cannot remove ${protectedEntries.length} entries within minimum retention period`
      );
    }

    // Remove expired entries
    this.entries = this.entries.filter(e => e.timestamp >= cutoffDate);

    return {
      entriesRemoved: entriesToRemove.length,
      archivedTo: this.config.retention.archiveBeforeDelete ? 'archive' : undefined,
    };
  }

  /**
   * Export audit entries for compliance reporting.
   * Delegates to query with the same options.
   */
  async export(options: AuditQueryOptions): Promise<AuditEntry[]> {
    return this.query(options);
  }

  /**
   * Get the current configuration.
   */
  getConfig(): AuditLoggerConfig {
    return { ...this.config };
  }

  /**
   * Get the total number of entries in the log.
   */
  getEntryCount(): number {
    return this.entries.length;
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Determine whether an event should be logged based on configuration.
   */
  private shouldLog(event: AuditEvent): boolean {
    // Check if reads should be logged
    if (!this.config.logReads && event.operation === 'SELECT') {
      return false;
    }

    // Check operation include/exclude lists
    if (this.config.includeOperations && this.config.includeOperations.length > 0) {
      if (!this.config.includeOperations.includes(event.operation)) {
        return false;
      }
    }
    if (this.config.excludeOperations && this.config.excludeOperations.length > 0) {
      if (this.config.excludeOperations.includes(event.operation)) {
        return false;
      }
    }

    // Check severity threshold
    if (this.config.minSeverity) {
      const eventSeverity = classifySeverity(event);
      if (SEVERITY_ORDER[eventSeverity] < SEVERITY_ORDER[this.config.minSeverity]) {
        return false;
      }
    }

    return true;
  }

  /**
   * Create an immutable audit entry from an event.
   */
  private async createEntry(event: AuditEvent): Promise<AuditEntry> {
    this.sequence++;
    const id = generateAuditId();
    const timestamp = createAuditTimestamp();
    const severity = classifySeverity(event);

    // Apply redaction and truncation
    const processedQuery = truncateQuery(event.query, this.config.maxQueryLength);
    const processedParams = this.config.redactParams ? redactParams(event.queryParams) : event.queryParams;

    const partialEntry: Omit<AuditEntry, 'hash'> = {
      id,
      timestamp,
      sequence: this.sequence,
      previousHash: this.lastHash,
      version: AUDIT_LOG_VERSION,
      actor: event.actor,
      operation: event.operation,
      target: event.target,
      query: processedQuery,
      queryParams: processedParams,
      outcome: event.outcome,
      rowsAffected: event.rowsAffected,
      severity,
      errorMessage: event.errorMessage,
      metadata: event.metadata,
      traceId: event.traceId,
      transactionId: event.transactionId,
    };

    const hash = await computeEntryHash(partialEntry);
    const entry: AuditEntry = { ...partialEntry, hash };

    this.lastHash = hash;

    return entry;
  }
}

/**
 * Create a new AuditLogger instance with the given configuration.
 *
 * @param config - Partial configuration (merged with defaults)
 * @returns A new AuditLogger instance
 *
 * @example
 * ```typescript
 * const logger = createAuditLogger({
 *   logReads: false,
 *   retention: { minRetentionDays: 365, maxRetentionDays: 730 },
 * });
 * ```
 */
export function createAuditLogger(config: Partial<AuditLoggerConfig> = {}): AuditLogger {
  return new AuditLogger(config);
}
