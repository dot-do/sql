/**
 * Binding Error Classes
 *
 * Standardized error classes for parameter binding operations.
 *
 * @packageDocumentation
 */

import {
  DoSQLError,
  ErrorCategory,
  registerErrorClass,
  type ErrorContext,
  type SerializedError,
} from './base.js';
import { BindingErrorCode } from './codes.js';

// =============================================================================
// Binding Error
// =============================================================================

/**
 * Error thrown when parameter binding fails
 *
 * @example
 * ```typescript
 * try {
 *   stmt.bind(invalidValue);
 * } catch (error) {
 *   if (error instanceof BindingError) {
 *     console.log(error.code);  // 'BIND_INVALID_TYPE'
 *   }
 * }
 * ```
 */
export class BindingError extends DoSQLError {
  readonly code: BindingErrorCode;
  readonly category = ErrorCategory.VALIDATION;

  constructor(
    code: BindingErrorCode,
    message: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(message, options);
    this.name = 'BindingError';
    this.code = code;

    // Set recovery hints
    this.setRecoveryHint();
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case BindingErrorCode.MISSING_PARAM:
        this.recoveryHint = 'Ensure all required parameters are provided';
        break;
      case BindingErrorCode.INVALID_TYPE:
        this.recoveryHint = 'Use a supported type: string, number, bigint, boolean, null, Uint8Array, or Date';
        break;
      case BindingErrorCode.COUNT_MISMATCH:
        this.recoveryHint = 'Check that the number of parameters matches the placeholders in the SQL';
        break;
      case BindingErrorCode.NAMED_EXPECTED:
        this.recoveryHint = 'Use an object with named parameters instead of positional arguments';
        break;
    }
  }

  isRetryable(): boolean {
    // Binding errors are user errors, not retryable
    return false;
  }

  toUserMessage(): string {
    switch (this.code) {
      case BindingErrorCode.MISSING_PARAM:
        return 'A required parameter is missing.';
      case BindingErrorCode.INVALID_TYPE:
        return 'One of the parameters has an unsupported type.';
      case BindingErrorCode.TYPE_MISMATCH:
        return 'Parameter type does not match the expected type.';
      case BindingErrorCode.COUNT_MISMATCH:
        return 'The number of parameters does not match the SQL statement.';
      case BindingErrorCode.NAMED_EXPECTED:
        return 'This query uses named parameters. Please provide an object.';
      default:
        return this.message;
    }
  }

  /**
   * Deserialize from JSON
   */
  static fromJSON(json: SerializedError): BindingError {
    const error = new BindingError(
      json.code as BindingErrorCode,
      json.message,
      { context: json.context }
    );
    return error;
  }
}

// Register for deserialization
registerErrorClass('BindingError', BindingError);

// =============================================================================
// Missing Parameter Error
// =============================================================================

/**
 * Error when a required parameter is missing
 */
export class MissingParameterError extends BindingError {
  /** Name or index of the missing parameter */
  readonly parameterName: string | number;

  constructor(
    parameterName: string | number,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    const message = typeof parameterName === 'string'
      ? `Missing named parameter :${parameterName}`
      : `Missing positional parameter at index ${parameterName}`;

    super(BindingErrorCode.MISSING_PARAM, message, options);
    this.name = 'MissingParameterError';
    this.parameterName = parameterName;
    this.context = { ...this.context, metadata: { parameterName } };
  }
}

registerErrorClass('MissingParameterError', MissingParameterError);

// =============================================================================
// Type Coercion Error
// =============================================================================

/**
 * Error when a value cannot be coerced to a SQL type
 */
export class TypeCoercionError extends BindingError {
  /** The type that could not be coerced */
  readonly valueType: string;

  constructor(
    valueType: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(
      BindingErrorCode.INVALID_TYPE,
      `Cannot bind value of type ${valueType}`,
      options
    );
    this.name = 'TypeCoercionError';
    this.valueType = valueType;
    this.context = { ...this.context, metadata: { valueType } };
  }
}

registerErrorClass('TypeCoercionError', TypeCoercionError);

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a BindingError for missing named parameter
 */
export function createMissingNamedParamError(name: string): MissingParameterError {
  return new MissingParameterError(name);
}

/**
 * Create a BindingError for missing positional parameter
 */
export function createMissingPositionalParamError(index: number): MissingParameterError {
  return new MissingParameterError(index);
}

/**
 * Create a BindingError for invalid type
 */
export function createInvalidTypeError(valueType: string): TypeCoercionError {
  return new TypeCoercionError(valueType);
}

/**
 * Create a BindingError for parameter count mismatch
 */
export function createCountMismatchError(expected: number, got: number): BindingError {
  return new BindingError(
    BindingErrorCode.COUNT_MISMATCH,
    `Expected ${expected} parameters, got ${got}`,
    { context: { metadata: { expected, got } } }
  );
}

/**
 * Create a BindingError when named parameters expected but positional provided
 */
export function createNamedExpectedError(): BindingError {
  return new BindingError(
    BindingErrorCode.NAMED_EXPECTED,
    'SQL contains named parameters but positional parameters were provided'
  );
}

/**
 * Create a BindingError for non-finite number
 */
export function createNonFiniteNumberError(value: number): BindingError {
  return new BindingError(
    BindingErrorCode.INVALID_TYPE,
    `Cannot bind non-finite number: ${value}`,
    { context: { metadata: { value: String(value) } } }
  );
}
