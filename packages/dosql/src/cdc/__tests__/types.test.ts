/**
 * Tests for CDC types module
 *
 * Verifies the runtime exports from the CDC types module:
 * - Type guard functions (isInsertEvent, isUpdateEvent, isDeleteEvent, etc.)
 * - CDCError class (construction, error codes, retryability, user messages)
 * - CDCErrorCode enum values
 * - DEFAULT_LAKEHOUSE_CONFIG defaults
 */

import { describe, it, expect } from 'vitest';
import {
  // Type guards
  isInsertEvent,
  isUpdateEvent,
  isDeleteEvent,
  isTransactionEvent,
  isChangeEvent,
  // Error class
  CDCError,
  CDCErrorCode,
  // Default config
  DEFAULT_LAKEHOUSE_CONFIG,
  // Types
  type ChangeEvent,
  type TransactionEvent,
  type CDCEvent,
} from '../types.js';

// =============================================================================
// Test Fixtures
// =============================================================================

function makeInsertEvent(): ChangeEvent<{ id: number; name: string }> {
  return {
    id: 'evt_1',
    type: 'insert',
    table: 'users',
    txnId: 'txn_1',
    timestamp: new Date(),
    lsn: 1n,
    data: { id: 1, name: 'Alice' },
  };
}

function makeUpdateEvent(): ChangeEvent<{ id: number; name: string }> {
  return {
    id: 'evt_2',
    type: 'update',
    table: 'users',
    txnId: 'txn_2',
    timestamp: new Date(),
    lsn: 2n,
    data: { id: 1, name: 'Alice Updated' },
    oldData: { id: 1, name: 'Alice' },
  };
}

function makeDeleteEvent(): ChangeEvent<{ id: number; name: string }> {
  return {
    id: 'evt_3',
    type: 'delete',
    table: 'users',
    txnId: 'txn_3',
    timestamp: new Date(),
    lsn: 3n,
    oldData: { id: 1, name: 'Alice' },
  };
}

function makeBeginEvent(): TransactionEvent {
  return {
    type: 'begin',
    txnId: 'txn_1',
    timestamp: new Date(),
    lsn: 10n,
  };
}

function makeCommitEvent(): TransactionEvent {
  return {
    type: 'commit',
    txnId: 'txn_1',
    timestamp: new Date(),
    lsn: 20n,
  };
}

function makeRollbackEvent(): TransactionEvent {
  return {
    type: 'rollback',
    txnId: 'txn_1',
    timestamp: new Date(),
    lsn: 30n,
  };
}

// =============================================================================
// Type Guards
// =============================================================================

describe('CDC Types - Change Event Type Guards', () => {
  it('should identify insert events', () => {
    expect(isInsertEvent(makeInsertEvent())).toBe(true);
    expect(isInsertEvent(makeUpdateEvent())).toBe(false);
    expect(isInsertEvent(makeDeleteEvent())).toBe(false);
  });

  it('should identify update events', () => {
    expect(isUpdateEvent(makeUpdateEvent())).toBe(true);
    expect(isUpdateEvent(makeInsertEvent())).toBe(false);
    expect(isUpdateEvent(makeDeleteEvent())).toBe(false);
  });

  it('should identify delete events', () => {
    expect(isDeleteEvent(makeDeleteEvent())).toBe(true);
    expect(isDeleteEvent(makeInsertEvent())).toBe(false);
    expect(isDeleteEvent(makeUpdateEvent())).toBe(false);
  });
});

describe('CDC Types - Transaction Event Type Guards', () => {
  it('should identify begin as transaction event', () => {
    expect(isTransactionEvent(makeBeginEvent())).toBe(true);
  });

  it('should identify commit as transaction event', () => {
    expect(isTransactionEvent(makeCommitEvent())).toBe(true);
  });

  it('should identify rollback as transaction event', () => {
    expect(isTransactionEvent(makeRollbackEvent())).toBe(true);
  });

  it('should not identify change events as transaction events', () => {
    expect(isTransactionEvent(makeInsertEvent())).toBe(false);
    expect(isTransactionEvent(makeUpdateEvent())).toBe(false);
    expect(isTransactionEvent(makeDeleteEvent())).toBe(false);
  });
});

describe('CDC Types - Change vs Transaction Event Guards', () => {
  it('should identify change events', () => {
    expect(isChangeEvent(makeInsertEvent())).toBe(true);
    expect(isChangeEvent(makeUpdateEvent())).toBe(true);
    expect(isChangeEvent(makeDeleteEvent())).toBe(true);
  });

  it('should not identify transaction events as change events', () => {
    expect(isChangeEvent(makeBeginEvent() as CDCEvent)).toBe(false);
    expect(isChangeEvent(makeCommitEvent() as CDCEvent)).toBe(false);
    expect(isChangeEvent(makeRollbackEvent() as CDCEvent)).toBe(false);
  });
});

// =============================================================================
// CDCError Class
// =============================================================================

describe('CDC Types - CDCError', () => {
  it('should create error with code and message', () => {
    const error = new CDCError(CDCErrorCode.SUBSCRIPTION_FAILED, 'Failed to subscribe');
    expect(error).toBeInstanceOf(CDCError);
    expect(error.code).toBe(CDCErrorCode.SUBSCRIPTION_FAILED);
    expect(error.message).toBe('Failed to subscribe');
    expect(error.name).toBe('CDCError');
  });

  it('should create error with LSN context', () => {
    const error = new CDCError(CDCErrorCode.LSN_NOT_FOUND, 'LSN not found', {
      lsn: 42n,
    });
    expect(error.lsn).toBe(42n);
  });

  it('should create error with cause', () => {
    const cause = new Error('underlying error');
    const error = new CDCError(CDCErrorCode.DECODE_ERROR, 'Decode failed', {
      cause,
    });
    expect(error.cause).toBe(cause);
  });

  it('should identify retryable errors', () => {
    const retryableErrors = [
      CDCErrorCode.SUBSCRIPTION_FAILED,
      CDCErrorCode.BUFFER_OVERFLOW,
      CDCErrorCode.POOL_TIMEOUT,
      CDCErrorCode.BACKPRESSURE_LIMIT,
    ];

    for (const code of retryableErrors) {
      const error = new CDCError(code, 'test');
      expect(error.isRetryable()).toBe(true);
    }
  });

  it('should identify non-retryable errors', () => {
    const nonRetryableErrors = [
      CDCErrorCode.LSN_NOT_FOUND,
      CDCErrorCode.SLOT_NOT_FOUND,
      CDCErrorCode.SLOT_EXISTS,
      CDCErrorCode.DECODE_ERROR,
      CDCErrorCode.POOL_EXHAUSTED,
      CDCErrorCode.CONSUMER_NOT_FOUND,
    ];

    for (const code of nonRetryableErrors) {
      const error = new CDCError(code, 'test');
      expect(error.isRetryable()).toBe(false);
    }
  });

  it('should return user-friendly messages for all error codes', () => {
    const allCodes = Object.values(CDCErrorCode);
    for (const code of allCodes) {
      const error = new CDCError(code, 'internal message');
      const userMessage = error.toUserMessage();
      expect(typeof userMessage).toBe('string');
      expect(userMessage.length).toBeGreaterThan(0);
    }
  });

  it('should have recovery hints for all error codes', () => {
    const allCodes = Object.values(CDCErrorCode);
    for (const code of allCodes) {
      const error = new CDCError(code, 'test');
      expect(error.recoveryHint).toBeDefined();
      expect(typeof error.recoveryHint).toBe('string');
    }
  });

  it('should include LSN in context metadata', () => {
    const error = new CDCError(CDCErrorCode.LSN_NOT_FOUND, 'not found', {
      lsn: 100n,
    });
    expect(error.context?.metadata?.lsn).toBe('100');
  });

  it('should deserialize from JSON', () => {
    const original = new CDCError(CDCErrorCode.SLOT_NOT_FOUND, 'Slot missing', {
      lsn: 50n,
    });
    const json = original.toJSON();
    const deserialized = CDCError.fromJSON(json);

    expect(deserialized).toBeInstanceOf(CDCError);
    expect(deserialized.code).toBe(CDCErrorCode.SLOT_NOT_FOUND);
    expect(deserialized.message).toBe('Slot missing');
    expect(deserialized.lsn).toBe(50n);
  });
});

// =============================================================================
// CDCErrorCode Enum
// =============================================================================

describe('CDC Types - CDCErrorCode', () => {
  it('should have all expected error codes', () => {
    expect(CDCErrorCode.SUBSCRIPTION_FAILED).toBe('CDC_SUBSCRIPTION_FAILED');
    expect(CDCErrorCode.LSN_NOT_FOUND).toBe('CDC_LSN_NOT_FOUND');
    expect(CDCErrorCode.SLOT_NOT_FOUND).toBe('CDC_SLOT_NOT_FOUND');
    expect(CDCErrorCode.SLOT_EXISTS).toBe('CDC_SLOT_EXISTS');
    expect(CDCErrorCode.BUFFER_OVERFLOW).toBe('CDC_BUFFER_OVERFLOW');
    expect(CDCErrorCode.DECODE_ERROR).toBe('CDC_DECODE_ERROR');
    expect(CDCErrorCode.POOL_EXHAUSTED).toBe('CDC_POOL_EXHAUSTED');
    expect(CDCErrorCode.POOL_TIMEOUT).toBe('CDC_POOL_TIMEOUT');
    expect(CDCErrorCode.CONSUMER_NOT_FOUND).toBe('CDC_CONSUMER_NOT_FOUND');
    expect(CDCErrorCode.BACKPRESSURE_LIMIT).toBe('CDC_BACKPRESSURE_LIMIT');
  });
});

// =============================================================================
// Default Configuration
// =============================================================================

describe('CDC Types - Default Lakehouse Config', () => {
  it('should have sensible default values', () => {
    expect(DEFAULT_LAKEHOUSE_CONFIG.maxBatchSize).toBe(1000);
    expect(DEFAULT_LAKEHOUSE_CONFIG.maxBatchAge).toBe(5000);
    expect(DEFAULT_LAKEHOUSE_CONFIG.heartbeatInterval).toBe(30000);
    expect(DEFAULT_LAKEHOUSE_CONFIG.exactlyOnce).toBe(true);
  });

  it('should have retry configuration', () => {
    expect(DEFAULT_LAKEHOUSE_CONFIG.retry).toBeDefined();
    expect(DEFAULT_LAKEHOUSE_CONFIG.retry.maxAttempts).toBe(3);
    expect(DEFAULT_LAKEHOUSE_CONFIG.retry.initialDelayMs).toBe(100);
    expect(DEFAULT_LAKEHOUSE_CONFIG.retry.maxDelayMs).toBe(10000);
    expect(DEFAULT_LAKEHOUSE_CONFIG.retry.backoffMultiplier).toBe(2);
  });

  it('should have empty defaults for URL and DO ID', () => {
    expect(DEFAULT_LAKEHOUSE_CONFIG.lakehouseUrl).toBe('');
    expect(DEFAULT_LAKEHOUSE_CONFIG.sourceDoId).toBe('');
  });
});
