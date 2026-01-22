/**
 * DoSQL Distributed Transaction Error Types
 *
 * Error codes and error class for distributed transaction handling.
 *
 * @packageDocumentation
 */

// =============================================================================
// ERROR TYPES
// =============================================================================

/**
 * Error codes for distributed transactions
 */
export enum DistributedTransactionErrorCode {
  /** No active transaction */
  NO_ACTIVE_TRANSACTION = 'DTX_NO_ACTIVE',
  /** Transaction already active */
  TRANSACTION_ALREADY_ACTIVE = 'DTX_ALREADY_ACTIVE',
  /** Invalid state transition */
  INVALID_STATE = 'DTX_INVALID_STATE',
  /** Prepare phase failed */
  PREPARE_FAILED = 'DTX_PREPARE_FAILED',
  /** Commit phase failed */
  COMMIT_FAILED = 'DTX_COMMIT_FAILED',
  /** Rollback failed */
  ROLLBACK_FAILED = 'DTX_ROLLBACK_FAILED',
  /** Transaction timeout */
  TIMEOUT = 'DTX_TIMEOUT',
  /** Participant failure */
  PARTICIPANT_FAILURE = 'DTX_PARTICIPANT_FAILURE',
  /** Coordinator failure */
  COORDINATOR_FAILURE = 'DTX_COORDINATOR_FAILURE',
  /** Lock acquisition failed */
  LOCK_FAILED = 'DTX_LOCK_FAILED',
  /** Serialization conflict */
  SERIALIZATION_FAILURE = 'DTX_SERIALIZATION_FAILURE',
}

/**
 * Distributed transaction error
 */
export class DistributedTransactionError extends Error {
  constructor(
    public readonly code: DistributedTransactionErrorCode,
    message: string,
    public readonly txnId?: string,
    public readonly details?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'DistributedTransactionError';
  }
}
