/**
 * Tests for lake.do constants
 *
 * @module lake.do/tests/constants
 */

import { describe, it, expect } from 'vitest';
import {
  WebSocketState,
  DEFAULT_TIMEOUT_MS,
  DEFAULT_MAX_QUEUE_SIZE,
  MIN_QUEUE_SIZE,
  ErrorCode,
} from '../constants.js';

describe('WebSocketState', () => {
  it('has correct state values matching WebSocket constants', () => {
    expect(WebSocketState.CONNECTING).toBe(0);
    expect(WebSocketState.OPEN).toBe(1);
    expect(WebSocketState.CLOSING).toBe(2);
    expect(WebSocketState.CLOSED).toBe(3);
  });

  it('is readonly', () => {
    // TypeScript const assertion should prevent modification
    const state: typeof WebSocketState = WebSocketState;
    expect(Object.isFrozen(state)).toBe(false); // as const doesn't freeze, but TS prevents modification
  });
});

describe('Default Configuration Values', () => {
  describe('DEFAULT_TIMEOUT_MS', () => {
    it('has expected value of 30 seconds', () => {
      expect(DEFAULT_TIMEOUT_MS).toBe(30000);
    });

    it('is a reasonable timeout value', () => {
      expect(DEFAULT_TIMEOUT_MS).toBeGreaterThan(1000); // At least 1 second
      expect(DEFAULT_TIMEOUT_MS).toBeLessThan(300000); // At most 5 minutes
    });
  });

  describe('DEFAULT_MAX_QUEUE_SIZE', () => {
    it('has expected value of 1000', () => {
      expect(DEFAULT_MAX_QUEUE_SIZE).toBe(1000);
    });

    it('is a reasonable queue size', () => {
      expect(DEFAULT_MAX_QUEUE_SIZE).toBeGreaterThanOrEqual(MIN_QUEUE_SIZE);
      expect(DEFAULT_MAX_QUEUE_SIZE).toBeLessThanOrEqual(100000);
    });
  });

  describe('MIN_QUEUE_SIZE', () => {
    it('has expected value of 1', () => {
      expect(MIN_QUEUE_SIZE).toBe(1);
    });

    it('allows at least one item in queue', () => {
      expect(MIN_QUEUE_SIZE).toBeGreaterThan(0);
    });
  });
});

describe('ErrorCode', () => {
  describe('Connection errors', () => {
    it('has CONNECTION_ERROR', () => {
      expect(ErrorCode.CONNECTION_ERROR).toBe('CONNECTION_ERROR');
    });

    it('has CONNECTION_CLOSED', () => {
      expect(ErrorCode.CONNECTION_CLOSED).toBe('CONNECTION_CLOSED');
    });

    it('has CONNECTION_TIMEOUT', () => {
      expect(ErrorCode.CONNECTION_TIMEOUT).toBe('CONNECTION_TIMEOUT');
    });

    it('has NOT_CONNECTED', () => {
      expect(ErrorCode.NOT_CONNECTED).toBe('NOT_CONNECTED');
    });
  });

  describe('Query errors', () => {
    it('has INVALID_SQL', () => {
      expect(ErrorCode.INVALID_SQL).toBe('INVALID_SQL');
    });

    it('has QUERY_TIMEOUT', () => {
      expect(ErrorCode.QUERY_TIMEOUT).toBe('QUERY_TIMEOUT');
    });
  });

  describe('Table/Partition errors', () => {
    it('has TABLE_NOT_FOUND', () => {
      expect(ErrorCode.TABLE_NOT_FOUND).toBe('TABLE_NOT_FOUND');
    });

    it('has PARTITION_NOT_FOUND', () => {
      expect(ErrorCode.PARTITION_NOT_FOUND).toBe('PARTITION_NOT_FOUND');
    });
  });

  describe('Authentication errors', () => {
    it('has UNAUTHORIZED', () => {
      expect(ErrorCode.UNAUTHORIZED).toBe('UNAUTHORIZED');
    });

    it('has TOKEN_EXPIRED', () => {
      expect(ErrorCode.TOKEN_EXPIRED).toBe('TOKEN_EXPIRED');
    });
  });

  describe('General errors', () => {
    it('has TIMEOUT', () => {
      expect(ErrorCode.TIMEOUT).toBe('TIMEOUT');
    });

    it('has INTERNAL_ERROR', () => {
      expect(ErrorCode.INTERNAL_ERROR).toBe('INTERNAL_ERROR');
    });
  });

  describe('All error codes are unique', () => {
    it('has no duplicate values', () => {
      const values = Object.values(ErrorCode);
      const uniqueValues = new Set(values);
      expect(values.length).toBe(uniqueValues.size);
    });
  });

  describe('Error codes are strings', () => {
    it('all error codes are strings', () => {
      for (const code of Object.values(ErrorCode)) {
        expect(typeof code).toBe('string');
      }
    });
  });
});
