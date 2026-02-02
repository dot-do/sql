/**
 * DoLake Error Hierarchy
 *
 * Unified error handling for DoLake package.
 * All errors extend BaseError from @dotdo/sql-types for consistency
 * across the DoSQL ecosystem.
 *
 * @packageDocumentation
 */

import {
  BaseError,
  ErrorCategory,
  registerErrorDeserializer,
  type ErrorContext,
  type SerializedError,
} from '@dotdo/sql-types';

// Re-export shared types for convenience
export {
  ErrorCategory,
  type ErrorContext,
  type SerializedError,
  type ErrorLogEntry,
} from '@dotdo/sql-types';

// =============================================================================
// Error Codes
// =============================================================================

/**
 * DoLake error codes following standardized naming convention.
 * Format: LAKE_SPECIFIC or CATEGORY_SPECIFIC
 */
export const DoLakeErrorCode = {
  // Connection errors
  CONNECTION_ERROR: 'LAKE_CONNECTION_ERROR',
  CONNECTION_CLOSED: 'LAKE_CONNECTION_CLOSED',

  // Buffer errors
  BUFFER_OVERFLOW: 'LAKE_BUFFER_OVERFLOW',

  // Flush errors
  FLUSH_ERROR: 'LAKE_FLUSH_ERROR',
  FLUSH_TIMEOUT: 'LAKE_FLUSH_TIMEOUT',

  // Storage/Write errors
  PARQUET_WRITE_ERROR: 'LAKE_PARQUET_WRITE_ERROR',
  ICEBERG_ERROR: 'LAKE_ICEBERG_ERROR',

  // Version errors
  VERSION_MISMATCH: 'LAKE_VERSION_MISMATCH',

  // Compaction errors
  COMPACTION_ERROR: 'LAKE_COMPACTION_ERROR',
  COMPACTION_INVALID_CONFIG: 'LAKE_COMPACTION_INVALID_CONFIG',

  // VFS errors
  VFS_STORAGE_ERROR: 'LAKE_VFS_STORAGE_ERROR',
  VFS_READ_ERROR: 'LAKE_VFS_READ_ERROR',
  VFS_WRITE_ERROR: 'LAKE_VFS_WRITE_ERROR',

  // Validation errors
  MESSAGE_VALIDATION_ERROR: 'LAKE_MESSAGE_VALIDATION_ERROR',
} as const;

export type DoLakeErrorCode = typeof DoLakeErrorCode[keyof typeof DoLakeErrorCode];

// =============================================================================
// Base DoLake Error
// =============================================================================

/**
 * Base error class for DoLake.
 *
 * Extends BaseError from @dotdo/sql-types with DoLake-specific functionality.
 * All DoLake errors should extend this class.
 */
export class DoLakeError extends BaseError {
  readonly code: string;
  readonly category: ErrorCategory;
  private readonly _retryable: boolean | undefined;

  constructor(
    message: string,
    code: string,
    category: ErrorCategory = ErrorCategory.INTERNAL,
    options?: { cause?: Error; context?: ErrorContext; retryable?: boolean }
  ) {
    const superOptions: { cause?: Error; context?: ErrorContext } = {};
    if (options?.cause) {
      superOptions.cause = options.cause;
    }
    if (options?.context) {
      superOptions.context = options.context;
    }
    super(message, Object.keys(superOptions).length > 0 ? superOptions : undefined);
    this.name = 'DoLakeError';
    this.code = code;
    this.category = category;
    this._retryable = options?.retryable !== undefined ? options.retryable : undefined;
  }

  override isRetryable(): boolean {
    if (this._retryable !== undefined) {
      return this._retryable;
    }
    return [ErrorCategory.CONNECTION, ErrorCategory.TIMEOUT].includes(this.category);
  }

  static fromJSON(json: SerializedError): DoLakeError {
    const opts = json.context ? { context: json.context } : undefined;
    return new DoLakeError(json.message, json.code, ErrorCategory.INTERNAL, opts);
  }
}

registerErrorDeserializer('DoLakeError', DoLakeError.fromJSON);

// =============================================================================
// Connection Error
// =============================================================================

export class ConnectionError extends DoLakeError {
  constructor(message: string, retryable: boolean = true) {
    super(message, DoLakeErrorCode.CONNECTION_ERROR, ErrorCategory.CONNECTION, { retryable });
    this.name = 'ConnectionError';
  }

  override isRetryable(): boolean {
    return true;
  }

  static override fromJSON(json: SerializedError): ConnectionError {
    return new ConnectionError(json.message);
  }
}

registerErrorDeserializer('ConnectionError', ConnectionError.fromJSON);

// =============================================================================
// Version Mismatch Error
// =============================================================================

export class VersionMismatchError extends DoLakeError {
  readonly snapshotVersion: number;
  readonly currentVersion: number;
  readonly minSupportedVersion: number;

  constructor(
    message: string,
    snapshotVersion: number,
    currentVersion: number,
    minSupportedVersion: number
  ) {
    super(message, DoLakeErrorCode.VERSION_MISMATCH, ErrorCategory.VALIDATION, {
      retryable: false,
      context: {
        metadata: { snapshotVersion, currentVersion, minSupportedVersion },
      },
    });
    this.name = 'VersionMismatchError';
    this.snapshotVersion = snapshotVersion;
    this.currentVersion = currentVersion;
    this.minSupportedVersion = minSupportedVersion;
  }

  override toUserMessage(): string {
    return `Snapshot version ${this.snapshotVersion} is not compatible. ` +
      `Current version is ${this.currentVersion}, minimum supported is ${this.minSupportedVersion}.`;
  }

  static override fromJSON(json: SerializedError): VersionMismatchError {
    const meta = json.context?.metadata ?? {};
    return new VersionMismatchError(
      json.message,
      (meta.snapshotVersion as number) ?? 0,
      (meta.currentVersion as number) ?? 0,
      (meta.minSupportedVersion as number) ?? 0
    );
  }
}

registerErrorDeserializer('VersionMismatchError', VersionMismatchError.fromJSON);

// =============================================================================
// Buffer Overflow Error
// =============================================================================

export class BufferOverflowError extends DoLakeError {
  readonly currentSizeBytes: number;
  readonly maxSizeBytes: number;
  readonly attemptedBatchSizeBytes: number;
  readonly utilization: number;

  constructor(options: {
    currentSizeBytes: number;
    maxSizeBytes: number;
    attemptedBatchSizeBytes: number;
  }) {
    const { currentSizeBytes, maxSizeBytes, attemptedBatchSizeBytes } = options;
    const utilization = currentSizeBytes / maxSizeBytes;

    const formatBytes = (bytes: number): string => {
      if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
      if (bytes >= 1024) return `${(bytes / 1024).toFixed(2)} KB`;
      return `${bytes} bytes`;
    };

    const message =
      `Buffer overflow: cannot add batch of ${formatBytes(attemptedBatchSizeBytes)}. ` +
      `Current buffer: ${formatBytes(currentSizeBytes)} / ${formatBytes(maxSizeBytes)} ` +
      `(${(utilization * 100).toFixed(1)}% utilized).`;

    super(message, DoLakeErrorCode.BUFFER_OVERFLOW, ErrorCategory.RESOURCE, {
      retryable: true,
      context: {
        metadata: { currentSizeBytes, maxSizeBytes, attemptedBatchSizeBytes, utilization },
      },
    });

    this.name = 'BufferOverflowError';
    this.currentSizeBytes = currentSizeBytes;
    this.maxSizeBytes = maxSizeBytes;
    this.attemptedBatchSizeBytes = attemptedBatchSizeBytes;
    this.utilization = utilization;
    this.recoveryHint = 'Wait for the buffer to flush before retrying, reduce batch size, ' +
      'or increase flushThresholdBytes to trigger more frequent flushes.';
  }

  override toUserMessage(): string {
    const pct = (this.utilization * 100).toFixed(1);
    return `Buffer is ${pct}% full. Please wait for the buffer to flush before sending more data.`;
  }

  static override fromJSON(json: SerializedError): BufferOverflowError {
    const meta = json.context?.metadata ?? {};
    return new BufferOverflowError({
      currentSizeBytes: (meta.currentSizeBytes as number) ?? 0,
      maxSizeBytes: (meta.maxSizeBytes as number) ?? 1,
      attemptedBatchSizeBytes: (meta.attemptedBatchSizeBytes as number) ?? 0,
    });
  }
}

registerErrorDeserializer('BufferOverflowError', BufferOverflowError.fromJSON);

// =============================================================================
// Flush Error
// =============================================================================

export class FlushError extends DoLakeError {
  readonly usedFallback: boolean;

  constructor(message: string, usedFallback: boolean) {
    super(message, DoLakeErrorCode.FLUSH_ERROR, ErrorCategory.EXECUTION, {
      retryable: true,
      context: { metadata: { usedFallback } },
    });
    this.name = 'FlushError';
    this.usedFallback = usedFallback;
  }

  static override fromJSON(json: SerializedError): FlushError {
    const usedFallback = (json.context?.metadata?.usedFallback as boolean) ?? false;
    return new FlushError(json.message, usedFallback);
  }
}

registerErrorDeserializer('FlushError', FlushError.fromJSON);

// =============================================================================
// Parquet Write Error
// =============================================================================

export class ParquetWriteError extends DoLakeError {
  constructor(message: string, options?: { cause?: Error }) {
    const superOpts: { retryable: boolean; cause?: Error } = { retryable: true };
    if (options?.cause) {
      superOpts.cause = options.cause;
    }
    super(message, DoLakeErrorCode.PARQUET_WRITE_ERROR, ErrorCategory.EXECUTION, superOpts);
    this.name = 'ParquetWriteError';
  }

  static override fromJSON(json: SerializedError): ParquetWriteError {
    return new ParquetWriteError(json.message);
  }
}

registerErrorDeserializer('ParquetWriteError', ParquetWriteError.fromJSON);

// =============================================================================
// Iceberg Error
// =============================================================================

export class IcebergError extends DoLakeError {
  constructor(message: string, options?: { cause?: Error }) {
    const superOpts: { retryable: boolean; cause?: Error } = { retryable: false };
    if (options?.cause) {
      superOpts.cause = options.cause;
    }
    super(message, DoLakeErrorCode.ICEBERG_ERROR, ErrorCategory.EXECUTION, superOpts);
    this.name = 'IcebergError';
  }

  static override fromJSON(json: SerializedError): IcebergError {
    return new IcebergError(json.message);
  }
}

registerErrorDeserializer('IcebergError', IcebergError.fromJSON);

// =============================================================================
// Compaction Error
// =============================================================================

export class CompactionError extends DoLakeError {
  constructor(
    message: string,
    code: string = DoLakeErrorCode.COMPACTION_ERROR,
    retryable: boolean = true
  ) {
    super(message, code, ErrorCategory.EXECUTION, { retryable });
    this.name = 'CompactionError';
  }

  static override fromJSON(json: SerializedError): CompactionError {
    return new CompactionError(json.message, json.code);
  }
}

registerErrorDeserializer('CompactionError', CompactionError.fromJSON);

// =============================================================================
// VFS Storage Error
// =============================================================================

export class VFSStorageError extends DoLakeError {
  constructor(
    message: string,
    code: string = DoLakeErrorCode.VFS_STORAGE_ERROR,
    storageContext?: Record<string, unknown>
  ) {
    const opts: { retryable: boolean; context?: ErrorContext } = { retryable: true };
    if (storageContext) {
      opts.context = { metadata: storageContext };
    }
    super(message, code, ErrorCategory.RESOURCE, opts);
    this.name = 'VFSStorageError';
  }

  static override fromJSON(json: SerializedError): VFSStorageError {
    return new VFSStorageError(json.message, json.code, json.context?.metadata);
  }
}

registerErrorDeserializer('VFSStorageError', VFSStorageError.fromJSON);

// =============================================================================
// Message Validation Error
// =============================================================================

export class MessageValidationError extends DoLakeError {
  readonly validationDetails: string | null;

  constructor(message: string, validationDetails?: string | null) {
    const opts: { retryable: boolean; context?: ErrorContext } = { retryable: false };
    if (validationDetails) {
      opts.context = { metadata: { validationDetails } };
    }
    super(message, DoLakeErrorCode.MESSAGE_VALIDATION_ERROR, ErrorCategory.VALIDATION, opts);
    this.name = 'MessageValidationError';
    this.validationDetails = validationDetails ?? null;
  }

  getErrorDetails(): string {
    return this.validationDetails ?? this.message;
  }

  static override fromJSON(json: SerializedError): MessageValidationError {
    const details = json.context?.metadata?.validationDetails as string | undefined;
    return new MessageValidationError(json.message, details);
  }
}

registerErrorDeserializer('MessageValidationError', MessageValidationError.fromJSON);
