/**
 * Tests for CDC Protocol - Formal contract between DoSQL and DoLake
 */

import { describe, it, expect } from 'vitest';
import {
  CDC_PROTOCOL_VERSION,
  CDC_PROTOCOL_MIN_VERSION,
  CDC_PROTOCOL_HISTORY,
  type CDCEventType,
  type VersionedCDCEvent,
  type CDCProtocolCapabilities,
  DEFAULT_PRODUCER_CAPABILITIES,
  DEFAULT_CONSUMER_CAPABILITIES,
  CDCValidationErrorCode,
  CDCValidationWarningCode,
  validateCDCEvent,
  isVersionedCDCEvent,
  negotiateProtocolVersion,
  createVersionedCDCEvent,
  upgradeToVersionedCDCEvent,
} from '../cdc-protocol.js';

describe('CDC Protocol', () => {
  describe('Protocol Constants', () => {
    it('should have correct protocol version', () => {
      expect(CDC_PROTOCOL_VERSION).toBe(1);
    });

    it('should have correct minimum version', () => {
      expect(CDC_PROTOCOL_MIN_VERSION).toBe(1);
    });

    it('should have protocol history', () => {
      expect(CDC_PROTOCOL_HISTORY).toBeDefined();
      expect(CDC_PROTOCOL_HISTORY.length).toBeGreaterThan(0);
      expect(CDC_PROTOCOL_HISTORY[0].version).toBe(1);
    });
  });

  describe('VersionedCDCEvent', () => {
    it('should create a valid INSERT event', () => {
      const event: VersionedCDCEvent<{ id: number; name: string }> = {
        version: 1,
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        after: { id: 1, name: 'Alice' },
      };

      expect(event.version).toBe(1);
      expect(event.type).toBe('INSERT');
      expect(event.after).toEqual({ id: 1, name: 'Alice' });
    });

    it('should create a valid UPDATE event', () => {
      const event: VersionedCDCEvent<{ id: number; name: string }> = {
        version: 1,
        type: 'UPDATE',
        table: 'users',
        lsn: 1002n,
        timestamp: Date.now(),
        before: { id: 1, name: 'Alice' },
        after: { id: 1, name: 'Alice Smith' },
      };

      expect(event.type).toBe('UPDATE');
      expect(event.before).toEqual({ id: 1, name: 'Alice' });
      expect(event.after).toEqual({ id: 1, name: 'Alice Smith' });
    });

    it('should create a valid DELETE event', () => {
      const event: VersionedCDCEvent<{ id: number; name: string }> = {
        version: 1,
        type: 'DELETE',
        table: 'users',
        lsn: 1003n,
        timestamp: Date.now(),
        before: { id: 1, name: 'Alice Smith' },
      };

      expect(event.type).toBe('DELETE');
      expect(event.before).toEqual({ id: 1, name: 'Alice Smith' });
    });
  });

  describe('validateCDCEvent', () => {
    it('should validate a complete INSERT event', () => {
      const event = {
        version: 1,
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        after: { id: 1, name: 'Alice' },
        schemaVersion: 1,
        primaryKey: { id: 1 },
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should fail for missing version', () => {
      const event = {
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        after: { id: 1 },
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.code === CDCValidationErrorCode.MISSING_VERSION)).toBe(true);
    });

    it('should fail for unsupported version', () => {
      const event = {
        version: 999,
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        after: { id: 1 },
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.code === CDCValidationErrorCode.UNSUPPORTED_VERSION)).toBe(true);
    });

    it('should fail for missing type', () => {
      const event = {
        version: 1,
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        after: { id: 1 },
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.code === CDCValidationErrorCode.MISSING_TYPE)).toBe(true);
    });

    it('should fail for invalid type', () => {
      const event = {
        version: 1,
        type: 'TRUNCATE', // Not allowed in versioned protocol
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.code === CDCValidationErrorCode.INVALID_TYPE)).toBe(true);
    });

    it('should fail for missing table', () => {
      const event = {
        version: 1,
        type: 'INSERT',
        lsn: 1001n,
        timestamp: Date.now(),
        after: { id: 1 },
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.code === CDCValidationErrorCode.MISSING_TABLE)).toBe(true);
    });

    it('should fail for missing LSN', () => {
      const event = {
        version: 1,
        type: 'INSERT',
        table: 'users',
        timestamp: Date.now(),
        after: { id: 1 },
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.code === CDCValidationErrorCode.MISSING_LSN)).toBe(true);
    });

    it('should fail for missing timestamp', () => {
      const event = {
        version: 1,
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        after: { id: 1 },
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.code === CDCValidationErrorCode.MISSING_TIMESTAMP)).toBe(true);
    });

    it('should fail INSERT without after data', () => {
      const event = {
        version: 1,
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.code === CDCValidationErrorCode.INSERT_MISSING_AFTER)).toBe(true);
    });

    it('should fail UPDATE without before data', () => {
      const event = {
        version: 1,
        type: 'UPDATE',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        after: { id: 1, name: 'Alice' },
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.code === CDCValidationErrorCode.UPDATE_MISSING_BEFORE)).toBe(true);
    });

    it('should fail UPDATE without after data', () => {
      const event = {
        version: 1,
        type: 'UPDATE',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        before: { id: 1, name: 'Alice' },
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.code === CDCValidationErrorCode.UPDATE_MISSING_AFTER)).toBe(true);
    });

    it('should fail DELETE without before data', () => {
      const event = {
        version: 1,
        type: 'DELETE',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.code === CDCValidationErrorCode.DELETE_MISSING_BEFORE)).toBe(true);
    });

    it('should warn for missing schema version', () => {
      const event = {
        version: 1,
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        after: { id: 1 },
        primaryKey: { id: 1 },
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(true);
      expect(result.warnings.some(w => w.code === CDCValidationWarningCode.MISSING_SCHEMA_VERSION)).toBe(true);
    });

    it('should warn for missing primary key', () => {
      const event = {
        version: 1,
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        after: { id: 1 },
        schemaVersion: 1,
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(true);
      expect(result.warnings.some(w => w.code === CDCValidationWarningCode.MISSING_PRIMARY_KEY)).toBe(true);
    });

    it('should accept number LSN', () => {
      const event = {
        version: 1,
        type: 'INSERT',
        table: 'users',
        lsn: 1001, // number instead of bigint
        timestamp: Date.now(),
        after: { id: 1 },
      };

      const result = validateCDCEvent(event);
      expect(result.valid).toBe(true);
    });

    it('should skip data validation when validateData is false', () => {
      const event = {
        version: 1,
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        // Missing after data, but validateData is false
      };

      const result = validateCDCEvent(event, { validateData: false });
      expect(result.valid).toBe(true);
    });
  });

  describe('isVersionedCDCEvent', () => {
    it('should return true for valid event', () => {
      const event = {
        version: 1,
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
      };

      expect(isVersionedCDCEvent(event)).toBe(true);
    });

    it('should return false for invalid event', () => {
      const event = {
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
      };

      expect(isVersionedCDCEvent(event)).toBe(false);
    });

    it('should return false for null', () => {
      expect(isVersionedCDCEvent(null)).toBe(false);
    });

    it('should return false for undefined', () => {
      expect(isVersionedCDCEvent(undefined)).toBe(false);
    });
  });

  describe('negotiateProtocolVersion', () => {
    it('should negotiate successfully when versions overlap', () => {
      const producer: CDCProtocolCapabilities = {
        version: 1,
        minVersion: 1,
        maxVersion: 2,
        supportsCompression: true,
        supportsBinary: true,
        supportsBatching: true,
      };

      const consumer: CDCProtocolCapabilities = {
        version: 1,
        minVersion: 1,
        maxVersion: 1,
        supportsCompression: true,
        supportsBinary: true,
        supportsBatching: true,
      };

      const result = negotiateProtocolVersion(producer, consumer);
      expect(result.success).toBe(true);
      expect(result.negotiatedVersion).toBe(1);
    });

    it('should negotiate to highest common version', () => {
      const producer: CDCProtocolCapabilities = {
        version: 2,
        minVersion: 1,
        maxVersion: 3,
        supportsCompression: true,
        supportsBinary: true,
        supportsBatching: true,
      };

      const consumer: CDCProtocolCapabilities = {
        version: 2,
        minVersion: 1,
        maxVersion: 2,
        supportsCompression: true,
        supportsBinary: true,
        supportsBatching: true,
      };

      const result = negotiateProtocolVersion(producer, consumer);
      expect(result.success).toBe(true);
      expect(result.negotiatedVersion).toBe(2);
    });

    it('should fail when no version overlap', () => {
      const producer: CDCProtocolCapabilities = {
        version: 2,
        minVersion: 2,
        maxVersion: 3,
        supportsCompression: true,
        supportsBinary: true,
        supportsBatching: true,
      };

      const consumer: CDCProtocolCapabilities = {
        version: 1,
        minVersion: 1,
        maxVersion: 1,
        supportsCompression: true,
        supportsBinary: true,
        supportsBatching: true,
      };

      const result = negotiateProtocolVersion(producer, consumer);
      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });

    it('should work with default capabilities', () => {
      const result = negotiateProtocolVersion(
        DEFAULT_PRODUCER_CAPABILITIES,
        DEFAULT_CONSUMER_CAPABILITIES
      );
      expect(result.success).toBe(true);
      expect(result.negotiatedVersion).toBe(CDC_PROTOCOL_VERSION);
    });
  });

  describe('createVersionedCDCEvent', () => {
    it('should add version field', () => {
      const event = createVersionedCDCEvent({
        type: 'INSERT',
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        after: { id: 1, name: 'Alice' },
      });

      expect(event.version).toBe(CDC_PROTOCOL_VERSION);
      expect(event.type).toBe('INSERT');
      expect(event.after).toEqual({ id: 1, name: 'Alice' });
    });

    it('should preserve all fields', () => {
      const timestamp = Date.now();
      const event = createVersionedCDCEvent<{ id: number }>({
        type: 'UPDATE',
        table: 'users',
        lsn: 1001n,
        timestamp,
        before: { id: 1 },
        after: { id: 2 },
        transactionId: 'tx-123',
        schemaVersion: 5,
        primaryKey: { id: 1 },
        metadata: { source: 'test' },
      });

      expect(event.version).toBe(CDC_PROTOCOL_VERSION);
      expect(event.timestamp).toBe(timestamp);
      expect(event.before).toEqual({ id: 1 });
      expect(event.after).toEqual({ id: 2 });
      expect(event.transactionId).toBe('tx-123');
      expect(event.schemaVersion).toBe(5);
      expect(event.primaryKey).toEqual({ id: 1 });
      expect(event.metadata).toEqual({ source: 'test' });
    });
  });

  describe('upgradeToVersionedCDCEvent', () => {
    it('should upgrade legacy event with operation field', () => {
      const legacyEvent = {
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        operation: 'insert',
        after: { id: 1 },
      };

      const event = upgradeToVersionedCDCEvent(legacyEvent);
      expect(event.version).toBe(CDC_PROTOCOL_VERSION);
      expect(event.type).toBe('INSERT');
    });

    it('should normalize Date timestamp', () => {
      const date = new Date();
      const legacyEvent = {
        table: 'users',
        lsn: 1001n,
        timestamp: date,
        type: 'INSERT',
        after: { id: 1 },
      };

      const event = upgradeToVersionedCDCEvent(legacyEvent);
      expect(event.timestamp).toBe(date.getTime());
    });

    it('should normalize number LSN', () => {
      const legacyEvent = {
        table: 'users',
        lsn: 1001,
        timestamp: Date.now(),
        type: 'INSERT',
        after: { id: 1 },
      };

      const event = upgradeToVersionedCDCEvent(legacyEvent);
      expect(event.lsn).toBe(1001n);
    });

    it('should use txId if transactionId not present', () => {
      const legacyEvent = {
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        type: 'INSERT',
        after: { id: 1 },
        txId: 'tx-123',
      };

      const event = upgradeToVersionedCDCEvent(legacyEvent);
      expect(event.transactionId).toBe('tx-123');
    });

    it('should allow specifying assumed version', () => {
      const legacyEvent = {
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        type: 'INSERT',
        after: { id: 1 },
      };

      const event = upgradeToVersionedCDCEvent(legacyEvent, 2);
      expect(event.version).toBe(2);
    });

    it('should omit undefined optional fields', () => {
      const legacyEvent = {
        table: 'users',
        lsn: 1001n,
        timestamp: Date.now(),
        type: 'INSERT',
        after: { id: 1 },
      };

      const event = upgradeToVersionedCDCEvent(legacyEvent);
      expect('before' in event).toBe(false);
      expect('transactionId' in event).toBe(false);
      expect('primaryKey' in event).toBe(false);
      expect('metadata' in event).toBe(false);
    });
  });

  describe('Default Capabilities', () => {
    it('should have matching default versions', () => {
      expect(DEFAULT_PRODUCER_CAPABILITIES.version).toBe(CDC_PROTOCOL_VERSION);
      expect(DEFAULT_CONSUMER_CAPABILITIES.version).toBe(CDC_PROTOCOL_VERSION);
    });

    it('should support common features', () => {
      expect(DEFAULT_PRODUCER_CAPABILITIES.supportsCompression).toBe(true);
      expect(DEFAULT_PRODUCER_CAPABILITIES.supportsBinary).toBe(true);
      expect(DEFAULT_PRODUCER_CAPABILITIES.supportsBatching).toBe(true);

      expect(DEFAULT_CONSUMER_CAPABILITIES.supportsCompression).toBe(true);
      expect(DEFAULT_CONSUMER_CAPABILITIES.supportsBinary).toBe(true);
      expect(DEFAULT_CONSUMER_CAPABILITIES.supportsBatching).toBe(true);
    });
  });
});
