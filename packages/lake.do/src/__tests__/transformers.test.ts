/**
 * Tests for lake.do data transformation utilities
 *
 * @module lake.do/tests/transformers
 */

import { describe, it, expect } from 'vitest';
import {
  transformSnapshot,
  transformSnapshots,
  transformPartitionInfo,
  transformPartitionInfos,
  transformCompactionJob,
  transformTableMetadata,
  isValidStringId,
  isValidDateLike,
  type RawSnapshot,
  type RawPartitionInfo,
  type RawCompactionJob,
  type RawTableMetadata,
} from '../transformers.js';

describe('isValidStringId', () => {
  it('returns true for valid non-empty strings', () => {
    expect(isValidStringId('abc')).toBe(true);
    expect(isValidStringId('snap_123')).toBe(true);
    expect(isValidStringId('a')).toBe(true);
  });

  it('returns false for empty strings', () => {
    expect(isValidStringId('')).toBe(false);
    expect(isValidStringId('   ')).toBe(false);
    expect(isValidStringId('\t\n')).toBe(false);
  });

  it('returns false for non-strings', () => {
    expect(isValidStringId(null)).toBe(false);
    expect(isValidStringId(undefined)).toBe(false);
    expect(isValidStringId(123)).toBe(false);
    expect(isValidStringId({})).toBe(false);
    expect(isValidStringId([])).toBe(false);
  });
});

describe('isValidDateLike', () => {
  it('returns true for valid Date objects', () => {
    expect(isValidDateLike(new Date())).toBe(true);
    expect(isValidDateLike(new Date('2024-01-15'))).toBe(true);
  });

  it('returns false for invalid Date objects', () => {
    expect(isValidDateLike(new Date('invalid'))).toBe(false);
  });

  it('returns true for valid date strings', () => {
    expect(isValidDateLike('2024-01-15')).toBe(true);
    expect(isValidDateLike('2024-01-15T12:00:00Z')).toBe(true);
    expect(isValidDateLike('January 15, 2024')).toBe(true);
  });

  it('returns false for invalid date strings', () => {
    expect(isValidDateLike('not-a-date')).toBe(false);
    expect(isValidDateLike('')).toBe(false);
  });

  it('returns true for valid timestamps', () => {
    expect(isValidDateLike(Date.now())).toBe(true);
    expect(isValidDateLike(0)).toBe(true);
    expect(isValidDateLike(1705276800000)).toBe(true);
  });

  it('returns false for non-date-like values', () => {
    expect(isValidDateLike(null)).toBe(false);
    expect(isValidDateLike(undefined)).toBe(false);
    expect(isValidDateLike({})).toBe(false);
    expect(isValidDateLike([])).toBe(false);
  });
});

describe('transformSnapshot', () => {
  it('transforms a valid raw snapshot', () => {
    const raw: RawSnapshot = {
      id: 'snap_123',
      timestamp: '2024-01-15T12:00:00Z',
      summary: {
        addedFiles: 10,
        deletedFiles: 2,
        addedRows: 1000,
        deletedRows: 50,
      },
      manifestList: '/path/to/manifest.json',
    };

    const snapshot = transformSnapshot(raw);

    expect(typeof snapshot.id).toBe('string');
    expect(snapshot.id).toBe('snap_123');
    expect(snapshot.timestamp).toBeInstanceOf(Date);
    expect(snapshot.timestamp.toISOString()).toBe('2024-01-15T12:00:00.000Z');
    expect(snapshot.summary).toEqual(raw.summary);
    expect(snapshot.manifestList).toBe(raw.manifestList);
    expect(snapshot.parentId).toBeUndefined();
  });

  it('transforms snapshot with parentId', () => {
    const raw: RawSnapshot = {
      id: 'snap_456',
      timestamp: Date.now(),
      parentId: 'snap_123',
      summary: {
        addedFiles: 5,
        deletedFiles: 0,
        addedRows: 500,
        deletedRows: 0,
      },
      manifestList: '/path/to/manifest.json',
    };

    const snapshot = transformSnapshot(raw);

    expect(snapshot.parentId).toBe('snap_123');
  });

  it('handles numeric timestamp', () => {
    const timestamp = 1705320000000;
    const raw: RawSnapshot = {
      id: 'snap_123',
      timestamp,
      summary: {
        addedFiles: 1,
        deletedFiles: 0,
        addedRows: 10,
        deletedRows: 0,
      },
      manifestList: '/manifest.json',
    };

    const snapshot = transformSnapshot(raw);

    expect(snapshot.timestamp.getTime()).toBe(timestamp);
  });

  it('handles Date timestamp', () => {
    const date = new Date('2024-01-15');
    const raw: RawSnapshot = {
      id: 'snap_123',
      timestamp: date,
      summary: {
        addedFiles: 1,
        deletedFiles: 0,
        addedRows: 10,
        deletedRows: 0,
      },
      manifestList: '/manifest.json',
    };

    const snapshot = transformSnapshot(raw);

    expect(snapshot.timestamp.toDateString()).toBe(date.toDateString());
  });

  it('throws for invalid snapshot ID', () => {
    const raw: RawSnapshot = {
      id: '' as unknown as string,
      timestamp: '2024-01-15',
      summary: { addedFiles: 0, deletedFiles: 0, addedRows: 0, deletedRows: 0 },
      manifestList: '/manifest.json',
    };

    expect(() => transformSnapshot(raw)).toThrow('Invalid snapshot ID');
  });

  it('throws for null snapshot ID', () => {
    const raw: RawSnapshot = {
      id: null as unknown as string,
      timestamp: '2024-01-15',
      summary: { addedFiles: 0, deletedFiles: 0, addedRows: 0, deletedRows: 0 },
      manifestList: '/manifest.json',
    };

    expect(() => transformSnapshot(raw)).toThrow('Invalid snapshot ID');
  });

  it('throws for invalid parent snapshot ID', () => {
    const raw: RawSnapshot = {
      id: 'snap_123',
      timestamp: '2024-01-15',
      parentId: 123 as unknown as string,
      summary: { addedFiles: 0, deletedFiles: 0, addedRows: 0, deletedRows: 0 },
      manifestList: '/manifest.json',
    };

    expect(() => transformSnapshot(raw)).toThrow('Invalid parent snapshot ID');
  });

  it('handles null parentId gracefully', () => {
    const raw: RawSnapshot = {
      id: 'snap_123',
      timestamp: '2024-01-15',
      parentId: null as unknown as undefined,
      summary: { addedFiles: 0, deletedFiles: 0, addedRows: 0, deletedRows: 0 },
      manifestList: '/manifest.json',
    };

    const snapshot = transformSnapshot(raw);
    expect(snapshot.parentId).toBeUndefined();
  });
});

describe('transformSnapshots', () => {
  it('transforms an array of raw snapshots', () => {
    const raws: RawSnapshot[] = [
      {
        id: 'snap_1',
        timestamp: '2024-01-15',
        summary: { addedFiles: 1, deletedFiles: 0, addedRows: 10, deletedRows: 0 },
        manifestList: '/manifest1.json',
      },
      {
        id: 'snap_2',
        timestamp: '2024-01-16',
        parentId: 'snap_1',
        summary: { addedFiles: 2, deletedFiles: 1, addedRows: 20, deletedRows: 5 },
        manifestList: '/manifest2.json',
      },
    ];

    const snapshots = transformSnapshots(raws);

    expect(snapshots).toHaveLength(2);
    expect(snapshots[0].id).toBe('snap_1');
    expect(snapshots[1].id).toBe('snap_2');
    expect(snapshots[1].parentId).toBe('snap_1');
  });

  it('returns empty array for empty input', () => {
    expect(transformSnapshots([])).toEqual([]);
  });
});

describe('transformPartitionInfo', () => {
  it('transforms a valid raw partition info', () => {
    const raw: RawPartitionInfo = {
      key: 'date=2024-01-15',
      strategy: 'time',
      fileCount: 10,
      rowCount: 10000,
      sizeBytes: 1024 * 1024 * 50,
      lastModified: '2024-01-15T12:00:00Z',
    };

    const partition = transformPartitionInfo(raw);

    expect(typeof partition.key).toBe('string');
    expect(partition.key).toBe('date=2024-01-15');
    expect(partition.strategy).toBe('time');
    expect(partition.fileCount).toBe(10);
    expect(partition.rowCount).toBe(10000);
    expect(partition.sizeBytes).toBe(1024 * 1024 * 50);
    expect(partition.lastModified).toBeInstanceOf(Date);
    expect(partition.range).toBeUndefined();
  });

  it('transforms partition info with range', () => {
    const raw: RawPartitionInfo = {
      key: 'id_range_1',
      strategy: 'range',
      range: { min: 0, max: 1000 },
      fileCount: 5,
      rowCount: 5000,
      sizeBytes: 1024 * 1024,
      lastModified: Date.now(),
    };

    const partition = transformPartitionInfo(raw);

    expect(partition.range).toEqual({ min: 0, max: 1000 });
  });

  it('handles different partition strategies', () => {
    const strategies: Array<'time' | 'hash' | 'list' | 'range'> = ['time', 'hash', 'list', 'range'];

    for (const strategy of strategies) {
      const raw: RawPartitionInfo = {
        key: `partition_${strategy}`,
        strategy,
        fileCount: 1,
        rowCount: 100,
        sizeBytes: 1024,
        lastModified: new Date(),
      };

      const partition = transformPartitionInfo(raw);
      expect(partition.strategy).toBe(strategy);
    }
  });

  it('throws for invalid partition key', () => {
    const raw: RawPartitionInfo = {
      key: '' as unknown as string,
      strategy: 'time',
      fileCount: 1,
      rowCount: 100,
      sizeBytes: 1024,
      lastModified: '2024-01-15',
    };

    expect(() => transformPartitionInfo(raw)).toThrow('Invalid partition key');
  });
});

describe('transformPartitionInfos', () => {
  it('transforms an array of raw partition infos', () => {
    const raws: RawPartitionInfo[] = [
      {
        key: 'date=2024-01-15',
        strategy: 'time',
        fileCount: 5,
        rowCount: 5000,
        sizeBytes: 1024 * 1024,
        lastModified: '2024-01-15',
      },
      {
        key: 'date=2024-01-16',
        strategy: 'time',
        fileCount: 3,
        rowCount: 3000,
        sizeBytes: 512 * 1024,
        lastModified: '2024-01-16',
      },
    ];

    const partitions = transformPartitionInfos(raws);

    expect(partitions).toHaveLength(2);
    expect(partitions[0].key).toBe('date=2024-01-15');
    expect(partitions[1].key).toBe('date=2024-01-16');
  });
});

describe('transformCompactionJob', () => {
  it('transforms a basic compaction job', () => {
    const raw: RawCompactionJob = {
      id: 'compact_123',
      partition: 'date=2024-01-15',
      status: 'pending',
      inputFiles: ['file1.parquet', 'file2.parquet'],
    };

    const job = transformCompactionJob(raw);

    expect(typeof job.id).toBe('string');
    expect(job.id).toBe('compact_123');
    expect(typeof job.partition).toBe('string');
    expect(job.partition).toBe('date=2024-01-15');
    expect(job.status).toBe('pending');
    expect(job.inputFiles).toEqual(['file1.parquet', 'file2.parquet']);
    expect(job.outputFiles).toBeUndefined();
    expect(job.error).toBeUndefined();
    expect(job.bytesRead).toBeUndefined();
    expect(job.bytesWritten).toBeUndefined();
    expect(job.startedAt).toBeUndefined();
    expect(job.completedAt).toBeUndefined();
  });

  it('transforms a completed compaction job with all fields', () => {
    const raw: RawCompactionJob = {
      id: 'compact_456',
      partition: 'date=2024-01-15',
      status: 'completed',
      inputFiles: ['file1.parquet', 'file2.parquet', 'file3.parquet'],
      outputFiles: ['merged.parquet'],
      bytesRead: 1024 * 1024 * 100,
      bytesWritten: 1024 * 1024 * 90,
      startedAt: '2024-01-15T12:00:00Z',
      completedAt: '2024-01-15T12:05:00Z',
    };

    const job = transformCompactionJob(raw);

    expect(job.status).toBe('completed');
    expect(job.outputFiles).toEqual(['merged.parquet']);
    expect(job.bytesRead).toBe(1024 * 1024 * 100);
    expect(job.bytesWritten).toBe(1024 * 1024 * 90);
    expect(job.startedAt).toBeInstanceOf(Date);
    expect(job.completedAt).toBeInstanceOf(Date);
  });

  it('transforms a failed compaction job', () => {
    const raw: RawCompactionJob = {
      id: 'compact_789',
      partition: 'date=2024-01-15',
      status: 'failed',
      inputFiles: ['file1.parquet'],
      error: 'Out of memory',
      startedAt: '2024-01-15T12:00:00Z',
    };

    const job = transformCompactionJob(raw);

    expect(job.status).toBe('failed');
    expect(job.error).toBe('Out of memory');
    expect(job.startedAt).toBeInstanceOf(Date);
    expect(job.completedAt).toBeUndefined();
  });

  it('handles all status values', () => {
    const statuses: Array<'pending' | 'running' | 'completed' | 'failed'> = [
      'pending',
      'running',
      'completed',
      'failed',
    ];

    for (const status of statuses) {
      const raw: RawCompactionJob = {
        id: `compact_${status}`,
        partition: 'partition',
        status,
        inputFiles: [],
      };

      const job = transformCompactionJob(raw);
      expect(job.status).toBe(status);
    }
  });

  it('throws for invalid job ID', () => {
    const raw: RawCompactionJob = {
      id: '' as unknown as string,
      partition: 'partition',
      status: 'pending',
      inputFiles: [],
    };

    expect(() => transformCompactionJob(raw)).toThrow('Invalid compaction job ID');
  });

  it('throws for invalid partition key', () => {
    const raw: RawCompactionJob = {
      id: 'compact_123',
      partition: null as unknown as string,
      status: 'pending',
      inputFiles: [],
    };

    expect(() => transformCompactionJob(raw)).toThrow('Invalid partition key');
  });

  it('handles empty outputFiles array', () => {
    const raw: RawCompactionJob = {
      id: 'compact_123',
      partition: 'partition',
      status: 'completed',
      inputFiles: ['file.parquet'],
      outputFiles: [],
    };

    const job = transformCompactionJob(raw);
    expect(job.outputFiles).toBeUndefined();
  });
});

describe('transformTableMetadata', () => {
  it('transforms basic table metadata', () => {
    const raw: RawTableMetadata = {
      tableId: 'orders',
      schema: {
        columns: [
          { name: 'id', type: 'string', nullable: false },
          { name: 'amount', type: 'number', nullable: false },
        ],
      },
      partitionSpec: {
        column: 'created_at',
        strategy: 'time',
        granularity: 'day',
      },
      snapshots: [],
      properties: { 'owner': 'data-team' },
    };

    const metadata = transformTableMetadata(raw);

    expect(metadata.tableId).toBe('orders');
    expect(metadata.schema).toEqual(raw.schema);
    expect(metadata.partitionSpec).toEqual(raw.partitionSpec);
    expect(metadata.properties).toEqual(raw.properties);
    expect(metadata.snapshots).toEqual([]);
    expect(metadata.currentSnapshotId).toBeUndefined();
  });

  it('transforms table metadata with snapshots', () => {
    const raw: RawTableMetadata = {
      tableId: 'orders',
      schema: { columns: [] },
      partitionSpec: { column: 'date', strategy: 'time', granularity: 'day' },
      currentSnapshotId: 'snap_2',
      snapshots: [
        {
          id: 'snap_1',
          timestamp: '2024-01-15',
          summary: { addedFiles: 1, deletedFiles: 0, addedRows: 10, deletedRows: 0 },
          manifestList: '/manifest1.json',
        },
        {
          id: 'snap_2',
          timestamp: '2024-01-16',
          parentId: 'snap_1',
          summary: { addedFiles: 2, deletedFiles: 0, addedRows: 20, deletedRows: 0 },
          manifestList: '/manifest2.json',
        },
      ],
      properties: {},
    };

    const metadata = transformTableMetadata(raw);

    expect(metadata.currentSnapshotId).toBe('snap_2');
    expect(metadata.snapshots).toHaveLength(2);
    expect(metadata.snapshots[0].id).toBe('snap_1');
    expect(metadata.snapshots[1].parentId).toBe('snap_1');
  });

  it('handles null currentSnapshotId', () => {
    const raw: RawTableMetadata = {
      tableId: 'new_table',
      schema: { columns: [] },
      partitionSpec: { column: 'id', strategy: 'hash', granularity: undefined },
      currentSnapshotId: null as unknown as undefined,
      snapshots: [],
      properties: {},
    };

    const metadata = transformTableMetadata(raw);
    expect(metadata.currentSnapshotId).toBeUndefined();
  });

  it('throws for invalid currentSnapshotId', () => {
    const raw: RawTableMetadata = {
      tableId: 'table',
      schema: { columns: [] },
      partitionSpec: { column: 'id', strategy: 'hash', granularity: undefined },
      currentSnapshotId: 123 as unknown as string,
      snapshots: [],
      properties: {},
    };

    expect(() => transformTableMetadata(raw)).toThrow('Invalid current snapshot ID');
  });
});
