/**
 * BigInt Serialization Tests
 *
 * Tests for custom JSON serialization/deserialization of bigint values
 * used in Iceberg metadata (snapshot IDs, sequence numbers, timestamps).
 */

import { describe, it, expect } from 'vitest';
import {
  serialize,
  deserialize,
  serializeMessage,
  deserializeMessage,
  bigintReplacer,
  bigintReviver,
  createReplacer,
  createReviver,
  serializeIcebergMetadata,
  deserializeIcebergMetadata,
  isBigInt,
  isSerializedBigInt,
  isNumericBigIntString,
  isIcebergBigIntField,
  isValidSnapshotId,
  isValidSequenceNumber,
  isValidTimestamp,
  parseSnapshotId,
  parseSequenceNumber,
  snapshotId,
  sequenceNumber,
  timestampMs,
  BIGINT_MARKER,
  ICEBERG_BIGINT_FIELDS,
} from '../serialization.js';

describe('BigInt Serialization', () => {
  // ===========================================================================
  // Basic Serialization Tests
  // ===========================================================================

  describe('serialize/deserialize', () => {
    it('should serialize bigint values with marker prefix', () => {
      const obj = { id: 1234567890123456789n };
      const json = serialize(obj);

      expect(json).toContain(BIGINT_MARKER);
      expect(json).toBe('{"id":"__bigint__:1234567890123456789"}');
    });

    it('should deserialize marked strings back to bigint', () => {
      const json = '{"id":"__bigint__:1234567890123456789"}';
      const obj = deserialize<{ id: bigint }>(json);

      expect(typeof obj.id).toBe('bigint');
      expect(obj.id).toBe(1234567890123456789n);
    });

    it('should handle roundtrip serialization', () => {
      const original = {
        'snapshot-id': 1234567890123456789n,
        'sequence-number': 42n,
        'timestamp-ms': 1705555555555n,
        name: 'test',
        count: 100,
      };

      const json = serialize(original);
      const restored = deserialize<typeof original>(json);

      expect(restored['snapshot-id']).toBe(original['snapshot-id']);
      expect(restored['sequence-number']).toBe(original['sequence-number']);
      expect(restored['timestamp-ms']).toBe(original['timestamp-ms']);
      expect(restored.name).toBe(original.name);
      expect(restored.count).toBe(original.count);
    });

    it('should handle null values', () => {
      const obj = { 'parent-snapshot-id': null };
      const json = serialize(obj);
      const restored = deserialize<typeof obj>(json);

      expect(restored['parent-snapshot-id']).toBeNull();
    });

    it('should handle nested objects with bigint', () => {
      const obj = {
        snapshot: {
          id: 123n,
          nested: {
            value: 456n,
          },
        },
      };

      const json = serialize(obj);
      const restored = deserialize<typeof obj>(json);

      expect(restored.snapshot.id).toBe(123n);
      expect(restored.snapshot.nested.value).toBe(456n);
    });

    it('should handle arrays with bigint', () => {
      const obj = {
        ids: [1n, 2n, 3n],
      };

      const json = serialize(obj);
      const restored = deserialize<typeof obj>(json);

      expect(restored.ids).toEqual([1n, 2n, 3n]);
    });

    it('should preserve non-bigint values', () => {
      const obj = {
        string: 'hello',
        number: 42,
        boolean: true,
        array: [1, 2, 3],
        object: { a: 1 },
      };

      const json = serialize(obj);
      const restored = deserialize<typeof obj>(json);

      expect(restored).toEqual(obj);
    });

    it('should handle pretty printing', () => {
      const obj = { id: 123n };
      const pretty = serialize(obj, 2);

      expect(pretty).toContain('\n');
      expect(pretty).toContain('  ');
    });
  });

  // ===========================================================================
  // Iceberg Field Handling
  // ===========================================================================

  describe('Iceberg bigint fields', () => {
    it('should recognize all known Iceberg bigint fields', () => {
      const expectedFields = [
        'snapshot-id',
        'parent-snapshot-id',
        'sequence-number',
        'timestamp-ms',
        'last-sequence-number',
        'last-updated-ms',
        'current-snapshot-id',
        'manifest-length',
        'min-sequence-number',
        'added-snapshot-id',
        'added-rows-count',
        'existing-rows-count',
        'deleted-rows-count',
        'record-count',
        'file-size-in-bytes',
      ];

      for (const field of expectedFields) {
        expect(isIcebergBigIntField(field)).toBe(true);
      }
    });

    it('should not recognize non-Iceberg fields as bigint fields', () => {
      expect(isIcebergBigIntField('name')).toBe(false);
      expect(isIcebergBigIntField('type')).toBe(false);
      expect(isIcebergBigIntField('id')).toBe(false);
    });

    it('should deserialize numeric strings for Iceberg fields without markers', () => {
      // This simulates data from external Iceberg readers that don't use markers
      const json = '{"snapshot-id":"1234567890123456789"}';
      const obj = deserialize<{ 'snapshot-id': bigint }>(json);

      expect(typeof obj['snapshot-id']).toBe('bigint');
      expect(obj['snapshot-id']).toBe(1234567890123456789n);
    });
  });

  // ===========================================================================
  // WebSocket Message Helpers
  // ===========================================================================

  describe('serializeMessage/deserializeMessage', () => {
    it('should serialize WebSocket messages with bigint', () => {
      const message = {
        type: 'snapshot',
        'snapshot-id': 123456789012345n,
      };

      const json = serializeMessage(message);
      expect(typeof json).toBe('string');
      expect(json).toContain(BIGINT_MARKER);
    });

    it('should deserialize string messages', () => {
      const json = '{"type":"snapshot","snapshot-id":"__bigint__:123456789012345"}';
      const message = deserializeMessage<{ type: string; 'snapshot-id': bigint }>(json);

      expect(message.type).toBe('snapshot');
      expect(message['snapshot-id']).toBe(123456789012345n);
    });

    it('should deserialize ArrayBuffer messages', () => {
      const json = '{"type":"snapshot","snapshot-id":"__bigint__:123456789012345"}';
      const buffer = new TextEncoder().encode(json).buffer;

      const message = deserializeMessage<{ type: string; 'snapshot-id': bigint }>(buffer);

      expect(message.type).toBe('snapshot');
      expect(message['snapshot-id']).toBe(123456789012345n);
    });
  });

  // ===========================================================================
  // Iceberg Metadata Serialization
  // ===========================================================================

  describe('serializeIcebergMetadata/deserializeIcebergMetadata', () => {
    const sampleMetadata = {
      'format-version': 2,
      'table-uuid': 'test-uuid',
      location: 's3://bucket/table',
      'last-sequence-number': 10n,
      'last-updated-ms': 1705555555555n,
      'current-snapshot-id': 1234567890123456789n,
      snapshots: [
        {
          'snapshot-id': 1234567890123456789n,
          'parent-snapshot-id': null,
          'sequence-number': 1n,
          'timestamp-ms': 1705555555555n,
        },
      ],
    };

    it('should serialize without markers for external compatibility', () => {
      const json = serializeIcebergMetadata(sampleMetadata, { useMarkers: false });

      // Should contain plain numeric strings, not markers
      expect(json).not.toContain(BIGINT_MARKER);
      expect(json).toContain('"1234567890123456789"');
    });

    it('should serialize with markers when requested', () => {
      const json = serializeIcebergMetadata(sampleMetadata, { useMarkers: true });

      expect(json).toContain(BIGINT_MARKER);
    });

    it('should pretty print when requested', () => {
      const json = serializeIcebergMetadata(sampleMetadata, { pretty: true });

      expect(json).toContain('\n');
    });

    it('should deserialize Iceberg metadata with bigint fields', () => {
      const json = serializeIcebergMetadata(sampleMetadata, { useMarkers: false });
      const restored = deserializeIcebergMetadata<typeof sampleMetadata>(json);

      expect(restored['last-sequence-number']).toBe(10n);
      expect(restored['last-updated-ms']).toBe(1705555555555n);
      expect(restored['current-snapshot-id']).toBe(1234567890123456789n);
      expect(restored.snapshots[0]['snapshot-id']).toBe(1234567890123456789n);
    });
  });

  // ===========================================================================
  // Type Guards
  // ===========================================================================

  describe('type guards', () => {
    describe('isBigInt', () => {
      it('should return true for bigint values', () => {
        expect(isBigInt(123n)).toBe(true);
        expect(isBigInt(0n)).toBe(true);
        expect(isBigInt(-123n)).toBe(true);
      });

      it('should return false for non-bigint values', () => {
        expect(isBigInt(123)).toBe(false);
        expect(isBigInt('123')).toBe(false);
        expect(isBigInt(null)).toBe(false);
        expect(isBigInt(undefined)).toBe(false);
      });
    });

    describe('isSerializedBigInt', () => {
      it('should return true for marker-prefixed strings', () => {
        expect(isSerializedBigInt('__bigint__:123')).toBe(true);
        expect(isSerializedBigInt('__bigint__:0')).toBe(true);
      });

      it('should return false for other values', () => {
        expect(isSerializedBigInt('123')).toBe(false);
        expect(isSerializedBigInt('bigint:123')).toBe(false);
        expect(isSerializedBigInt(123)).toBe(false);
      });
    });

    describe('isNumericBigIntString', () => {
      it('should return true for large numeric strings', () => {
        expect(isNumericBigIntString('123456789012345')).toBe(true);
        expect(isNumericBigIntString('1234567890123456789')).toBe(true);
      });

      it('should return false for small numeric strings', () => {
        expect(isNumericBigIntString('12345678901234')).toBe(false);
        expect(isNumericBigIntString('123')).toBe(false);
      });

      it('should return false for non-numeric strings', () => {
        expect(isNumericBigIntString('abc')).toBe(false);
        expect(isNumericBigIntString('123abc')).toBe(false);
      });
    });
  });

  // ===========================================================================
  // Validators
  // ===========================================================================

  describe('validators', () => {
    describe('isValidSnapshotId', () => {
      it('should return true for positive bigint', () => {
        expect(isValidSnapshotId(1n)).toBe(true);
        expect(isValidSnapshotId(1234567890123456789n)).toBe(true);
      });

      it('should return false for non-positive bigint', () => {
        expect(isValidSnapshotId(0n)).toBe(false);
        expect(isValidSnapshotId(-1n)).toBe(false);
      });

      it('should return false for non-bigint', () => {
        expect(isValidSnapshotId(123)).toBe(false);
        expect(isValidSnapshotId('123')).toBe(false);
      });
    });

    describe('isValidSequenceNumber', () => {
      it('should return true for non-negative bigint', () => {
        expect(isValidSequenceNumber(0n)).toBe(true);
        expect(isValidSequenceNumber(1n)).toBe(true);
        expect(isValidSequenceNumber(1234567890n)).toBe(true);
      });

      it('should return false for negative bigint', () => {
        expect(isValidSequenceNumber(-1n)).toBe(false);
      });

      it('should return false for non-bigint', () => {
        expect(isValidSequenceNumber(123)).toBe(false);
      });
    });

    describe('isValidTimestamp', () => {
      it('should return true for positive bigint', () => {
        expect(isValidTimestamp(1n)).toBe(true);
        expect(isValidTimestamp(1705555555555n)).toBe(true);
      });

      it('should return false for non-positive bigint', () => {
        expect(isValidTimestamp(0n)).toBe(false);
        expect(isValidTimestamp(-1n)).toBe(false);
      });
    });
  });

  // ===========================================================================
  // Parsers
  // ===========================================================================

  describe('parsers', () => {
    describe('parseSnapshotId', () => {
      it('should parse bigint values', () => {
        expect(parseSnapshotId(123n)).toBe(123n);
      });

      it('should parse safe integer numbers', () => {
        expect(parseSnapshotId(123)).toBe(123n);
      });

      it('should parse plain numeric strings', () => {
        expect(parseSnapshotId('123')).toBe(123n);
      });

      it('should parse marker-prefixed strings', () => {
        expect(parseSnapshotId('__bigint__:123')).toBe(123n);
      });

      it('should throw for invalid values', () => {
        expect(() => parseSnapshotId(0)).toThrow();
        expect(() => parseSnapshotId(-1)).toThrow();
        expect(() => parseSnapshotId('0')).toThrow();
      });
    });

    describe('parseSequenceNumber', () => {
      it('should parse bigint values', () => {
        expect(parseSequenceNumber(0n)).toBe(0n);
        expect(parseSequenceNumber(123n)).toBe(123n);
      });

      it('should parse safe integer numbers', () => {
        expect(parseSequenceNumber(0)).toBe(0n);
        expect(parseSequenceNumber(123)).toBe(123n);
      });

      it('should parse strings', () => {
        expect(parseSequenceNumber('0')).toBe(0n);
        expect(parseSequenceNumber('123')).toBe(123n);
        expect(parseSequenceNumber('__bigint__:123')).toBe(123n);
      });

      it('should throw for negative values', () => {
        expect(() => parseSequenceNumber(-1)).toThrow();
        expect(() => parseSequenceNumber('-1')).toThrow();
      });
    });
  });

  // ===========================================================================
  // Branded Types
  // ===========================================================================

  describe('branded types', () => {
    it('should create branded SnapshotId', () => {
      const id = snapshotId(123n);
      expect(id).toBe(123n);
      // The branding is compile-time only, runtime value is still bigint
      expect(typeof id).toBe('bigint');
    });

    it('should create branded SequenceNumber', () => {
      const seq = sequenceNumber(42n);
      expect(seq).toBe(42n);
      expect(typeof seq).toBe('bigint');
    });

    it('should create branded TimestampMs', () => {
      const ts = timestampMs(1705555555555n);
      expect(ts).toBe(1705555555555n);
      expect(typeof ts).toBe('bigint');
    });
  });

  // ===========================================================================
  // Custom Replacer/Reviver
  // ===========================================================================

  describe('createReplacer/createReviver', () => {
    it('should chain custom replacer with bigint handling', () => {
      const customReplacer = (_key: string, value: unknown) => {
        if (value === 'secret') return '[REDACTED]';
        return value;
      };

      const replacer = createReplacer(customReplacer);
      const obj = { id: 123n, password: 'secret' };
      const json = JSON.stringify(obj, replacer);

      expect(json).toContain(BIGINT_MARKER);
      expect(json).toContain('[REDACTED]');
      expect(json).not.toContain('secret');
    });

    it('should chain custom reviver with bigint handling', () => {
      const customReviver = (key: string, value: unknown) => {
        if (key === 'date' && typeof value === 'string') {
          return new Date(value);
        }
        return value;
      };

      const reviver = createReviver(customReviver);
      const json = '{"id":"__bigint__:123","date":"2024-01-18T00:00:00.000Z"}';
      const obj = JSON.parse(json, reviver);

      expect(obj.id).toBe(123n);
      expect(obj.date).toBeInstanceOf(Date);
    });
  });

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('should handle very large bigint values', () => {
      const veryLarge = 9999999999999999999999999999999999999n;
      const json = serialize({ value: veryLarge });
      const restored = deserialize<{ value: bigint }>(json);

      expect(restored.value).toBe(veryLarge);
    });

    it('should handle zero bigint', () => {
      const obj = { value: 0n };
      const json = serialize(obj);
      const restored = deserialize<typeof obj>(json);

      expect(restored.value).toBe(0n);
    });

    it('should handle negative bigint', () => {
      const obj = { value: -123n };
      const json = serialize(obj);
      const restored = deserialize<typeof obj>(json);

      expect(restored.value).toBe(-123n);
    });

    it('should handle empty objects', () => {
      const obj = {};
      const json = serialize(obj);
      const restored = deserialize<typeof obj>(json);

      expect(restored).toEqual({});
    });

    it('should handle empty arrays', () => {
      const obj = { arr: [] };
      const json = serialize(obj);
      const restored = deserialize<typeof obj>(json);

      expect(restored.arr).toEqual([]);
    });

    it('should not convert small numeric strings to bigint unless in Iceberg field', () => {
      const json = '{"other-field":"12345"}';
      const obj = deserialize<{ 'other-field': string }>(json);

      expect(typeof obj['other-field']).toBe('string');
      expect(obj['other-field']).toBe('12345');
    });
  });

  // ===========================================================================
  // Real-World Iceberg Snapshot Test
  // ===========================================================================

  describe('real-world Iceberg snapshot', () => {
    it('should handle complete Iceberg snapshot structure', () => {
      const snapshot = {
        'snapshot-id': 3776207205136740581n,
        'parent-snapshot-id': null as bigint | null,
        'sequence-number': 1n,
        'timestamp-ms': 1705555555555n,
        'manifest-list': 's3://bucket/table/metadata/snap-3776207205136740581-manifest-list.avro',
        summary: {
          operation: 'append',
          'added-data-files': '1',
          'added-records': '100',
          'added-files-size': '1024',
        },
        'schema-id': 0,
      };

      const json = serialize(snapshot);
      const restored = deserialize<typeof snapshot>(json);

      expect(restored['snapshot-id']).toBe(3776207205136740581n);
      expect(restored['parent-snapshot-id']).toBeNull();
      expect(restored['sequence-number']).toBe(1n);
      expect(restored['timestamp-ms']).toBe(1705555555555n);
      expect(restored['manifest-list']).toBe(snapshot['manifest-list']);
      expect(restored.summary).toEqual(snapshot.summary);
      expect(restored['schema-id']).toBe(0);
    });

    it('should handle manifest file with bigint fields', () => {
      const manifest = {
        'manifest-path': 's3://bucket/table/metadata/abc-manifest.avro',
        'manifest-length': 4096n,
        'partition-spec-id': 0,
        content: 'data',
        'sequence-number': 1n,
        'min-sequence-number': 1n,
        'added-snapshot-id': 3776207205136740581n,
        'added-files-count': 1,
        'existing-files-count': 0,
        'deleted-files-count': 0,
        'added-rows-count': 100n,
        'existing-rows-count': 0n,
        'deleted-rows-count': 0n,
      };

      const json = serialize(manifest);
      const restored = deserialize<typeof manifest>(json);

      expect(restored['manifest-length']).toBe(4096n);
      expect(restored['sequence-number']).toBe(1n);
      expect(restored['min-sequence-number']).toBe(1n);
      expect(restored['added-snapshot-id']).toBe(3776207205136740581n);
      expect(restored['added-rows-count']).toBe(100n);
      expect(restored['existing-rows-count']).toBe(0n);
      expect(restored['deleted-rows-count']).toBe(0n);
    });

    it('should handle data file with bigint fields', () => {
      const dataFile = {
        content: 0,
        'file-path': 's3://bucket/table/data/00000-0-abc.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 100n,
        'file-size-in-bytes': 1024n,
      };

      const json = serialize(dataFile);
      const restored = deserialize<typeof dataFile>(json);

      expect(restored['record-count']).toBe(100n);
      expect(restored['file-size-in-bytes']).toBe(1024n);
    });
  });
});
