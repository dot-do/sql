/**
 * Time Travel Tests for DoSQL
 *
 * Comprehensive tests covering multi-level time travel across storage tiers:
 * - In-memory B-tree (hot) - milliseconds of history
 * - DO blob storage (warm) - hours/days of history
 * - R2 lakehouse (cold) - months/years of history
 *
 * Tests cover:
 * - AS OF TIMESTAMP queries
 * - AS OF VERSION queries
 * - Time travel across storage tiers
 * - Point-in-time recovery
 * - SYSTEM_TIME period queries
 * - Historical diff queries
 * - Snapshot isolation
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  type TimePoint,
  type TimeRange,
  type ResolvedTimePoint,
  type TimeTravelScope,
  TimeTravelError,
  TimeTravelErrorCode,
  lsn,
  timestamp,
  snapshot,
  branch,
  relative,
  serializeTimePoint,
  deserializeTimePoint,
} from './types.js';

import {
  parseTimeTravelQuery,
  rewriteQuery,
  resolveTiers,
  buildDiffQuery,
  buildRecoveryQuery,
  validateTimeTravelQuery,
  createSnapshotContext,
  isSnapshotValid,
  applySnapshotContext,
  type ParsedTimeTravelQuery,
  type TierResolution,
  type TierResolutionConfig,
  DEFAULT_TIER_CONFIG,
} from './query.js';

// =============================================================================
// Test Utilities
// =============================================================================

const NOW = new Date('2024-06-15T12:00:00Z');
const ONE_MINUTE_AGO = new Date(NOW.getTime() - 60 * 1000);
const ONE_HOUR_AGO = new Date(NOW.getTime() - 60 * 60 * 1000);
const ONE_DAY_AGO = new Date(NOW.getTime() - 24 * 60 * 60 * 1000);
const ONE_WEEK_AGO = new Date(NOW.getTime() - 7 * 24 * 60 * 60 * 1000);
const ONE_MONTH_AGO = new Date(NOW.getTime() - 30 * 24 * 60 * 60 * 1000);

const testConfig: TierResolutionConfig = {
  hotTierMaxAge: 60 * 1000, // 1 minute
  warmTierMaxAge: 24 * 60 * 60 * 1000, // 24 hours
  now: NOW,
};

// =============================================================================
// Time Point Creation Tests
// =============================================================================

describe('Time Point Factory Functions', () => {
  it('should create LSN time point', () => {
    const point = lsn(100n);
    expect(point.type).toBe('lsn');
    expect(point.lsn).toBe(100n);
  });

  it('should create timestamp time point from Date', () => {
    const date = new Date('2024-01-01T00:00:00Z');
    const point = timestamp(date);
    expect(point.type).toBe('timestamp');
    expect(point.timestamp.getTime()).toBe(date.getTime());
  });

  it('should create timestamp time point from string', () => {
    const point = timestamp('2024-01-01T00:00:00Z');
    expect(point.type).toBe('timestamp');
    expect(point.timestamp.toISOString()).toBe('2024-01-01T00:00:00.000Z');
  });

  it('should create timestamp time point from number (unix ms)', () => {
    const ms = 1704067200000; // 2024-01-01T00:00:00Z
    const point = timestamp(ms);
    expect(point.type).toBe('timestamp');
    expect(point.timestamp.getTime()).toBe(ms);
  });

  it('should create snapshot time point', () => {
    const point = snapshot('main@123');
    expect(point.type).toBe('snapshot');
    expect(point.snapshotId).toBe('main@123');
  });

  it('should create branch time point without nested point', () => {
    const point = branch('feature-branch');
    expect(point.type).toBe('branch');
    expect(point.branch).toBe('feature-branch');
    expect(point.point).toBeUndefined();
  });

  it('should create branch time point with nested timestamp', () => {
    const date = new Date('2024-01-01');
    const point = branch('feature-branch', timestamp(date));
    expect(point.type).toBe('branch');
    expect(point.branch).toBe('feature-branch');
    expect(point.point?.type).toBe('timestamp');
  });

  it('should create relative time point with LSN offset', () => {
    const point = relative({ lsnOffset: -10n });
    expect(point.type).toBe('relative');
    expect(point.lsnOffset).toBe(-10n);
  });

  it('should create relative time point with time offset', () => {
    const point = relative({ timeOffset: -3600000 }); // -1 hour
    expect(point.type).toBe('relative');
    expect(point.timeOffset).toBe(-3600000);
  });

  it('should create relative time point with version offset', () => {
    const point = relative({ versionOffset: -5 });
    expect(point.type).toBe('relative');
    expect(point.versionOffset).toBe(-5);
  });

  it('should create relative time point with anchor', () => {
    const anchor = lsn(500n);
    const point = relative({ anchor, lsnOffset: -10n });
    expect(point.type).toBe('relative');
    expect(point.anchor).toBe(anchor);
  });
});

// =============================================================================
// Time Point Serialization Tests
// =============================================================================

describe('Time Point Serialization', () => {
  it('should serialize and deserialize LSN time point', () => {
    const original = lsn(12345n);
    const serialized = serializeTimePoint(original);
    const deserialized = deserializeTimePoint(serialized);

    expect(deserialized.type).toBe('lsn');
    expect((deserialized as { lsn: bigint }).lsn).toBe(12345n);
  });

  it('should serialize and deserialize timestamp time point', () => {
    const date = new Date('2024-01-15T10:30:00Z');
    const original = timestamp(date);
    const serialized = serializeTimePoint(original);
    const deserialized = deserializeTimePoint(serialized);

    expect(deserialized.type).toBe('timestamp');
    expect((deserialized as { timestamp: Date }).timestamp.getTime()).toBe(date.getTime());
  });

  it('should serialize and deserialize snapshot time point', () => {
    const original = snapshot('develop@456');
    const serialized = serializeTimePoint(original);
    const deserialized = deserializeTimePoint(serialized);

    expect(deserialized.type).toBe('snapshot');
    expect((deserialized as { snapshotId: string }).snapshotId).toBe('develop@456');
  });

  it('should serialize and deserialize branch time point with nested point', () => {
    const original = branch('feature', lsn(100n));
    const serialized = serializeTimePoint(original);
    const deserialized = deserializeTimePoint(serialized);

    expect(deserialized.type).toBe('branch');
    const branchPoint = deserialized as { branch: string; point?: TimePoint };
    expect(branchPoint.branch).toBe('feature');
    expect(branchPoint.point?.type).toBe('lsn');
  });

  it('should serialize and deserialize relative time point', () => {
    const original = relative({
      anchor: lsn(200n),
      lsnOffset: -50n,
      timeOffset: -3600000,
      versionOffset: -2,
    });
    const serialized = serializeTimePoint(original);
    const deserialized = deserializeTimePoint(serialized);

    expect(deserialized.type).toBe('relative');
    const relPoint = deserialized as {
      anchor?: TimePoint;
      lsnOffset?: bigint;
      timeOffset?: number;
      versionOffset?: number;
    };
    expect(relPoint.lsnOffset).toBe(-50n);
    expect(relPoint.timeOffset).toBe(-3600000);
    expect(relPoint.versionOffset).toBe(-2);
  });
});

// =============================================================================
// SQL Query Parsing Tests - AS OF TIMESTAMP
// =============================================================================

describe('SQL Query Parsing - AS OF TIMESTAMP', () => {
  it('should parse SELECT * FROM users AS OF TIMESTAMP', () => {
    const sql = "SELECT * FROM users AS OF TIMESTAMP '2024-01-01 00:00:00'";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.table).toBe('users');
    expect(parsed!.clauseType).toBe('AS_OF_TIMESTAMP');
    expect(parsed!.isRangeQuery).toBe(false);
  });

  it('should parse AS OF TIMESTAMP with ISO format', () => {
    const sql = "SELECT * FROM orders AS OF TIMESTAMP '2024-01-01T00:00:00Z'";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.table).toBe('orders');
    expect(parsed!.clauseType).toBe('AS_OF_TIMESTAMP');
    expect(parsed!.timeSpec).toHaveProperty('type', 'timestamp');
  });

  it('should remove time travel clause from rewritten SQL', () => {
    const sql = "SELECT * FROM users AS OF TIMESTAMP '2024-01-01'";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.rewrittenSQL).not.toContain('AS OF TIMESTAMP');
    expect(parsed!.rewrittenSQL).toContain('SELECT * FROM users');
  });

  it('should handle case-insensitive AS OF TIMESTAMP', () => {
    const sql = "select * from users as of timestamp '2024-01-01'";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.clauseType).toBe('AS_OF_TIMESTAMP');
  });
});

// =============================================================================
// SQL Query Parsing Tests - AS OF VERSION
// =============================================================================

describe('SQL Query Parsing - AS OF VERSION', () => {
  it('should parse SELECT * FROM users AS OF VERSION', () => {
    const sql = 'SELECT * FROM users AS OF VERSION 123';
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.table).toBe('users');
    expect(parsed!.clauseType).toBe('AS_OF_VERSION');
    expect(parsed!.timeSpec).toHaveProperty('type', 'lsn');
    expect((parsed!.timeSpec as { lsn: bigint }).lsn).toBe(123n);
  });

  it('should parse large version numbers', () => {
    const sql = 'SELECT * FROM logs AS OF VERSION 9007199254740992';
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect((parsed!.timeSpec as { lsn: bigint }).lsn).toBe(9007199254740992n);
  });

  it('should handle case-insensitive AS OF VERSION', () => {
    const sql = 'select * from events as of version 456';
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.clauseType).toBe('AS_OF_VERSION');
  });
});

// =============================================================================
// SQL Query Parsing Tests - SYSTEM_TIME Queries
// =============================================================================

describe('SQL Query Parsing - SYSTEM_TIME Queries', () => {
  it('should parse AS OF SYSTEM TIME', () => {
    const sql = "SELECT * FROM users AS OF SYSTEM TIME '2024-01-01 12:00:00'";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.clauseType).toBe('AS_OF_SYSTEM_TIME');
    expect(parsed!.isRangeQuery).toBe(false);
  });

  it('should parse FOR SYSTEM_TIME AS OF', () => {
    const sql = "SELECT * FROM users FOR SYSTEM_TIME AS OF '2024-01-01'";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.clauseType).toBe('FOR_SYSTEM_TIME_AS_OF');
  });

  it('should parse FOR SYSTEM_TIME BETWEEN ... AND ...', () => {
    const sql = "SELECT * FROM users FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2024-01-31'";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.clauseType).toBe('FOR_SYSTEM_TIME_BETWEEN');
    expect(parsed!.isRangeQuery).toBe(true);

    const range = parsed!.timeSpec as TimeRange;
    expect(range.from).toBeDefined();
    expect(range.to).toBeDefined();
  });

  it('should parse FOR SYSTEM_TIME FROM ... TO ...', () => {
    const sql = "SELECT * FROM audit_log FOR SYSTEM_TIME FROM '2024-01-01' TO '2024-06-01'";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.clauseType).toBe('FOR_SYSTEM_TIME_FROM_TO');
    expect(parsed!.isRangeQuery).toBe(true);
  });
});

// =============================================================================
// SQL Query Parsing Tests - Snapshot and Branch
// =============================================================================

describe('SQL Query Parsing - Snapshot and Branch', () => {
  it('should parse AS OF SNAPSHOT', () => {
    const sql = "SELECT * FROM users AS OF SNAPSHOT 'main@100'";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.clauseType).toBe('AS_OF_SNAPSHOT');
    expect((parsed!.timeSpec as { snapshotId: string }).snapshotId).toBe('main@100');
  });

  it('should parse AS OF BRANCH', () => {
    const sql = "SELECT * FROM users AS OF BRANCH 'feature-xyz'";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.clauseType).toBe('AS_OF_BRANCH');
    expect((parsed!.timeSpec as { branch: string }).branch).toBe('feature-xyz');
  });

  it('should parse AS OF LSN', () => {
    const sql = 'SELECT * FROM users AS OF LSN 12345';
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.clauseType).toBe('AS_OF_LSN');
    expect((parsed!.timeSpec as { lsn: bigint }).lsn).toBe(12345n);
  });
});

// =============================================================================
// Tier Resolution Tests
// =============================================================================

describe('Storage Tier Resolution', () => {
  it('should resolve recent timestamp to hot tier', () => {
    const point = timestamp(ONE_MINUTE_AGO);
    const resolution = resolveTiers(point, testConfig);

    expect(resolution.tiers).toHaveLength(1);
    expect(resolution.tiers[0].tier).toBe('memory');
    expect(resolution.estimatedAge).toBe('hot');
  });

  it('should resolve hour-old timestamp to warm tier', () => {
    const point = timestamp(ONE_HOUR_AGO);
    const resolution = resolveTiers(point, testConfig);

    expect(resolution.tiers).toHaveLength(1);
    expect(resolution.tiers[0].tier).toBe('do-blob');
    expect(resolution.estimatedAge).toBe('warm');
  });

  it('should resolve week-old timestamp to cold tier', () => {
    const point = timestamp(ONE_WEEK_AGO);
    const resolution = resolveTiers(point, testConfig);

    expect(resolution.tiers).toHaveLength(1);
    expect(resolution.tiers[0].tier).toBe('r2-lakehouse');
    expect(resolution.estimatedAge).toBe('cold');
  });

  it('should resolve month-old timestamp to cold tier', () => {
    const point = timestamp(ONE_MONTH_AGO);
    const resolution = resolveTiers(point, testConfig);

    expect(resolution.tiers[0].tier).toBe('r2-lakehouse');
    expect(resolution.estimatedAge).toBe('cold');
  });

  it('should resolve LSN to hot and warm tiers', () => {
    const point = lsn(100n);
    const resolution = resolveTiers(point, testConfig);

    expect(resolution.tiers.length).toBeGreaterThanOrEqual(2);
    expect(resolution.tiers.some(t => t.tier === 'memory')).toBe(true);
    expect(resolution.tiers.some(t => t.tier === 'do-blob')).toBe(true);
  });

  it('should resolve snapshot to warm and cold tiers', () => {
    const point = snapshot('main@50');
    const resolution = resolveTiers(point, testConfig);

    expect(resolution.tiers.some(t => t.tier === 'do-blob')).toBe(true);
    expect(resolution.tiers.some(t => t.tier === 'r2-lakehouse')).toBe(true);
    expect(resolution.tiers.some(t => t.snapshotId === 'main@50')).toBe(true);
  });

  it('should resolve branch to hot and warm tiers', () => {
    const point = branch('feature');
    const resolution = resolveTiers(point, testConfig);

    expect(resolution.tiers.some(t => t.tier === 'memory')).toBe(true);
    expect(resolution.tiers.some(t => t.tier === 'do-blob')).toBe(true);
  });

  it('should resolve range spanning multiple tiers', () => {
    const range: TimeRange = {
      from: timestamp(ONE_WEEK_AGO),
      to: timestamp(ONE_MINUTE_AGO),
    };
    const resolution = resolveTiers(range, testConfig);

    expect(resolution.requiresMerge).toBe(true);
    expect(resolution.tiers.length).toBeGreaterThan(1);
  });

  it('should resolve relative time point', () => {
    const point = relative({ anchor: lsn(500n), lsnOffset: -10n });
    const resolution = resolveTiers(point, testConfig);

    expect(resolution.tiers.length).toBeGreaterThan(0);
  });
});

// =============================================================================
// Query Rewriting Tests
// =============================================================================

describe('Query Rewriting', () => {
  it('should add snapshot hint for memory tier', () => {
    const sql = "SELECT * FROM users AS OF SNAPSHOT 'main@100'";
    const parsed = parseTimeTravelQuery(sql)!;
    const resolution: TierResolution = {
      tiers: [{ tier: 'memory', isPrimary: true, snapshotId: 'main@100' as any }],
      explanation: 'test',
      estimatedAge: 'hot',
      requiresMerge: false,
    };

    const rewritten = rewriteQuery(parsed, resolution);

    expect(rewritten.queries).toHaveLength(1);
    expect(rewritten.queries[0].sql).toContain('SNAPSHOT: main@100');
  });

  it('should add version filter for DO blob tier', () => {
    const sql = 'SELECT * FROM users AS OF LSN 500';
    const parsed = parseTimeTravelQuery(sql)!;
    const resolution: TierResolution = {
      tiers: [{ tier: 'do-blob', isPrimary: true }],
      explanation: 'test',
      estimatedAge: 'warm',
      requiresMerge: false,
    };

    const rewritten = rewriteQuery(parsed, resolution);

    expect(rewritten.queries[0].sql).toContain('_version');
    expect(rewritten.queries[0].sql).toContain('500');
  });

  it('should add timestamp filter for lakehouse tier', () => {
    const sql = "SELECT * FROM users AS OF TIMESTAMP '2024-01-01'";
    const parsed = parseTimeTravelQuery(sql)!;
    const resolution: TierResolution = {
      tiers: [{ tier: 'r2-lakehouse', isPrimary: true }],
      explanation: 'test',
      estimatedAge: 'cold',
      requiresMerge: false,
    };

    const rewritten = rewriteQuery(parsed, resolution);

    expect(rewritten.queries[0].sql).toContain('_timestamp');
  });

  it('should generate multiple queries for multi-tier resolution', () => {
    const sql = 'SELECT * FROM users AS OF VERSION 100';
    const parsed = parseTimeTravelQuery(sql)!;
    const resolution: TierResolution = {
      tiers: [
        { tier: 'memory', isPrimary: true },
        { tier: 'do-blob', isPrimary: false },
      ],
      explanation: 'test',
      estimatedAge: 'warm',
      requiresMerge: true,
    };

    const rewritten = rewriteQuery(parsed, resolution);

    expect(rewritten.queries).toHaveLength(2);
    expect(rewritten.requiresMerge).toBe(true);
  });
});

// =============================================================================
// Query Validation Tests
// =============================================================================

describe('Query Validation', () => {
  it('should reject future timestamps', () => {
    const futureDate = new Date(Date.now() + 86400000);
    const sql = `SELECT * FROM users AS OF TIMESTAMP '${futureDate.toISOString()}'`;
    const parsed = parseTimeTravelQuery(sql)!;
    const validation = validateTimeTravelQuery(parsed);

    expect(validation.valid).toBe(false);
    expect(validation.errors).toContain('Cannot query future timestamps');
  });

  it('should warn about global scope queries', () => {
    const sql = "SELECT * FROM users AS OF TIMESTAMP '2024-01-01'";
    const parsed = parseTimeTravelQuery(sql)!;
    const validation = validateTimeTravelQuery(parsed);

    expect(validation.warnings.some(w => w.includes('Global scope'))).toBe(true);
  });

  it('should warn about range queries', () => {
    const sql = "SELECT * FROM users FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2024-06-01'";
    const parsed = parseTimeTravelQuery(sql)!;
    const validation = validateTimeTravelQuery(parsed);

    expect(validation.warnings.some(w => w.includes('Range queries'))).toBe(true);
  });

  it('should pass validation for valid query', () => {
    const pastDate = new Date(Date.now() - 86400000);
    const sql = `SELECT * FROM users AS OF VERSION 100`;
    const parsed = parseTimeTravelQuery(sql)!;
    const validation = validateTimeTravelQuery(parsed);

    expect(validation.valid).toBe(true);
    expect(validation.errors).toHaveLength(0);
  });
});

// =============================================================================
// Historical Diff Query Tests
// =============================================================================

describe('Historical Diff Queries', () => {
  it('should build diff query between two LSNs', () => {
    const from = lsn(100n);
    const to = lsn(200n);
    const diff = buildDiffQuery('users', from, to);

    expect(diff.table).toBe('users');
    expect(diff.sql).toContain('FULL OUTER JOIN');
    expect(diff.sql).toContain('_change_type');
    expect(diff.sql).toContain('INSERT');
    expect(diff.sql).toContain('UPDATE');
    expect(diff.sql).toContain('DELETE');
  });

  it('should build diff query between timestamps', () => {
    const from = timestamp('2024-01-01');
    const to = timestamp('2024-06-01');
    const diff = buildDiffQuery('orders', from, to);

    expect(diff.table).toBe('orders');
    expect(diff.sql).toContain('AS OF VERSION');
  });

  it('should build diff query with snapshot references', () => {
    const from = snapshot('main@100');
    const to = snapshot('main@200');
    const diff = buildDiffQuery('products', from, to);

    expect(diff.fromVersion).toBe(from);
    expect(diff.toVersion).toBe(to);
  });
});

// =============================================================================
// Point-in-Time Recovery Tests
// =============================================================================

describe('Point-in-Time Recovery', () => {
  it('should build recovery query for single table', () => {
    const target = lsn(500n);
    const recovery = buildRecoveryQuery(['users'], target);

    expect(recovery.tables).toEqual(['users']);
    expect(recovery.recoveryType).toBe('full');
    expect(recovery.statements).toContain('BEGIN TRANSACTION');
    expect(recovery.statements).toContain('COMMIT');
    expect(recovery.statements.some(s => s.includes('_recovery_users'))).toBe(true);
  });

  it('should build recovery query for multiple tables', () => {
    const target = timestamp('2024-01-01');
    const recovery = buildRecoveryQuery(['users', 'orders', 'products'], target);

    expect(recovery.tables).toHaveLength(3);
    expect(recovery.statements.some(s => s.includes('_recovery_users'))).toBe(true);
    expect(recovery.statements.some(s => s.includes('_recovery_orders'))).toBe(true);
    expect(recovery.statements.some(s => s.includes('_recovery_products'))).toBe(true);
  });

  it('should build partial recovery query', () => {
    const target = lsn(300n);
    const recovery = buildRecoveryQuery(['users'], target, { partial: true });

    expect(recovery.recoveryType).toBe('partial');
    expect(recovery.statements.some(s => s.includes('DELETE FROM users'))).toBe(false);
  });

  it('should build dry-run recovery query', () => {
    const target = lsn(400n);
    const recovery = buildRecoveryQuery(['users'], target, { dryRun: true });

    expect(recovery.statements.some(s => s.includes('BEGIN'))).toBe(false);
    expect(recovery.statements.some(s => s.includes('COMMIT'))).toBe(false);
  });
});

// =============================================================================
// Snapshot Isolation Tests
// =============================================================================

describe('Snapshot Isolation', () => {
  it('should create snapshot isolation context', () => {
    const context = createSnapshotContext(1000n, 'main', 30000);

    expect(context.sessionId).toMatch(/^snap-/);
    expect(context.snapshotLSN).toBe(1000n);
    expect(context.branch).toBe('main');
    expect(context.isValid).toBe(true);
  });

  it('should check snapshot validity within TTL', () => {
    const context = createSnapshotContext(1000n, 'main', 30000);
    expect(isSnapshotValid(context)).toBe(true);
  });

  it('should invalidate expired snapshot', () => {
    const context = createSnapshotContext(1000n, 'main', 1);
    context.expiresAt = new Date(Date.now() - 1000); // Already expired
    expect(isSnapshotValid(context)).toBe(false);
  });

  it('should apply snapshot context to query', () => {
    const context = createSnapshotContext(1000n, 'main', 30000);
    const sql = 'SELECT * FROM users';
    const modified = applySnapshotContext(sql, context);

    expect(modified).toContain('SNAPSHOT_ISOLATION');
    expect(modified).toContain(context.sessionId);
    expect(modified).toContain('LSN=1000');
  });

  it('should throw on expired context', () => {
    const context = createSnapshotContext(1000n, 'main', 1);
    context.expiresAt = new Date(Date.now() - 1000);

    expect(() => applySnapshotContext('SELECT 1', context)).toThrow(TimeTravelError);
  });
});

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('Time Travel Errors', () => {
  it('should create TimeTravelError with code', () => {
    const error = new TimeTravelError(
      TimeTravelErrorCode.POINT_NOT_FOUND,
      'Point not found',
      lsn(999n)
    );

    expect(error.code).toBe(TimeTravelErrorCode.POINT_NOT_FOUND);
    expect(error.message).toBe('Point not found');
    expect(error.timePoint).toEqual(lsn(999n));
  });

  it('should include cause in error', () => {
    const cause = new Error('Original error');
    const error = new TimeTravelError(
      TimeTravelErrorCode.DATA_GAP,
      'Data gap detected',
      undefined,
      cause
    );

    expect(error.cause).toBe(cause);
  });

  it('should have all error codes defined', () => {
    expect(TimeTravelErrorCode.POINT_NOT_FOUND).toBe('TT_POINT_NOT_FOUND');
    expect(TimeTravelErrorCode.LSN_NOT_AVAILABLE).toBe('TT_LSN_NOT_AVAILABLE');
    expect(TimeTravelErrorCode.SNAPSHOT_NOT_FOUND).toBe('TT_SNAPSHOT_NOT_FOUND');
    expect(TimeTravelErrorCode.BRANCH_NOT_FOUND).toBe('TT_BRANCH_NOT_FOUND');
    expect(TimeTravelErrorCode.TIMEOUT).toBe('TT_TIMEOUT');
    expect(TimeTravelErrorCode.DATA_GAP).toBe('TT_DATA_GAP');
    expect(TimeTravelErrorCode.CONSISTENCY_ERROR).toBe('TT_CONSISTENCY_ERROR');
    expect(TimeTravelErrorCode.INVALID_SYNTAX).toBe('TT_INVALID_SYNTAX');
    expect(TimeTravelErrorCode.FUTURE_POINT).toBe('TT_FUTURE_POINT');
    expect(TimeTravelErrorCode.SESSION_EXPIRED).toBe('TT_SESSION_EXPIRED');
  });
});

// =============================================================================
// Scope Determination Tests
// =============================================================================

describe('Time Travel Scope Determination', () => {
  it('should determine local scope for LSN queries', () => {
    const sql = 'SELECT * FROM users AS OF LSN 100';
    const parsed = parseTimeTravelQuery(sql)!;
    expect(parsed.scope).toBe('local');
  });

  it('should determine branch scope for snapshot queries', () => {
    const sql = "SELECT * FROM users AS OF SNAPSHOT 'main@100'";
    const parsed = parseTimeTravelQuery(sql)!;
    expect(parsed.scope).toBe('branch');
  });

  it('should determine branch scope for branch queries', () => {
    const sql = "SELECT * FROM users AS OF BRANCH 'feature'";
    const parsed = parseTimeTravelQuery(sql)!;
    expect(parsed.scope).toBe('branch');
  });

  it('should determine global scope for timestamp queries', () => {
    const sql = "SELECT * FROM users AS OF TIMESTAMP '2024-01-01'";
    const parsed = parseTimeTravelQuery(sql)!;
    expect(parsed.scope).toBe('global');
  });

  it('should determine global scope for range queries', () => {
    const sql = "SELECT * FROM users FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2024-06-01'";
    const parsed = parseTimeTravelQuery(sql)!;
    expect(parsed.scope).toBe('global');
  });
});

// =============================================================================
// Edge Cases and Complex Scenarios
// =============================================================================

describe('Edge Cases and Complex Scenarios', () => {
  it('should return null for queries without time travel clauses', () => {
    const sql = 'SELECT * FROM users WHERE id = 1';
    const parsed = parseTimeTravelQuery(sql);
    expect(parsed).toBeNull();
  });

  it('should handle table aliases', () => {
    const sql = "SELECT u.* FROM users u AS OF TIMESTAMP '2024-01-01'";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.table).toBe('users');
  });

  it('should handle complex WHERE clauses', () => {
    const sql = "SELECT * FROM users AS OF VERSION 100 WHERE active = true AND age > 18";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.rewrittenSQL).toContain('WHERE active = true AND age > 18');
  });

  it('should handle queries with ORDER BY', () => {
    const sql = "SELECT * FROM users AS OF TIMESTAMP '2024-01-01' ORDER BY created_at DESC";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.rewrittenSQL).toContain('ORDER BY created_at DESC');
  });

  it('should handle queries with LIMIT', () => {
    const sql = 'SELECT * FROM users AS OF VERSION 50 LIMIT 10';
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.rewrittenSQL).toContain('LIMIT 10');
  });

  it('should handle queries with GROUP BY', () => {
    const sql = "SELECT status, COUNT(*) FROM users AS OF TIMESTAMP '2024-01-01' GROUP BY status";
    const parsed = parseTimeTravelQuery(sql);

    expect(parsed).not.toBeNull();
    expect(parsed!.rewrittenSQL).toContain('GROUP BY status');
  });
});

// =============================================================================
// Integration Scenarios
// =============================================================================

describe('Integration Scenarios', () => {
  it('should handle full workflow: parse -> resolve -> rewrite', () => {
    const sql = "SELECT * FROM orders AS OF TIMESTAMP '2024-01-15T10:30:00Z'";

    // Parse
    const parsed = parseTimeTravelQuery(sql);
    expect(parsed).not.toBeNull();

    // Resolve tiers
    const resolution = resolveTiers(parsed!.timeSpec as TimePoint, {
      ...DEFAULT_TIER_CONFIG,
      now: new Date('2024-01-15T12:00:00Z'),
    });
    expect(resolution.tiers.length).toBeGreaterThan(0);

    // Rewrite
    const rewritten = rewriteQuery(parsed!, resolution);
    expect(rewritten.queries.length).toBeGreaterThan(0);
  });

  it('should handle cross-tier range query workflow', () => {
    const sql = "SELECT * FROM audit_log FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2024-06-15'";

    // Parse
    const parsed = parseTimeTravelQuery(sql);
    expect(parsed).not.toBeNull();
    expect(parsed!.isRangeQuery).toBe(true);

    // Resolve tiers
    const resolution = resolveTiers(parsed!.timeSpec as TimeRange, {
      hotTierMaxAge: 60 * 1000,
      warmTierMaxAge: 24 * 60 * 60 * 1000,
      now: new Date('2024-06-15T12:00:00Z'),
    });

    // Should span multiple tiers
    expect(resolution.tiers.length).toBeGreaterThan(1);
    expect(resolution.requiresMerge).toBe(true);
  });

  it('should handle recovery workflow', () => {
    // 1. Parse time travel query to identify target
    const sql = 'SELECT * FROM users AS OF VERSION 100';
    const parsed = parseTimeTravelQuery(sql);
    expect(parsed).not.toBeNull();

    // 2. Build recovery query
    const recovery = buildRecoveryQuery(['users'], parsed!.timeSpec as TimePoint);
    expect(recovery.statements.length).toBeGreaterThan(0);

    // 3. Validate the target
    const validation = validateTimeTravelQuery(parsed!);
    expect(validation.valid).toBe(true);
  });

  it('should handle snapshot isolation workflow', () => {
    // 1. Create snapshot context
    const context = createSnapshotContext(1000n, 'main', 30000);
    expect(isSnapshotValid(context)).toBe(true);

    // 2. Apply to multiple queries
    const query1 = applySnapshotContext('SELECT * FROM users', context);
    const query2 = applySnapshotContext('SELECT * FROM orders', context);

    // Both should use same snapshot
    expect(query1).toContain(context.sessionId);
    expect(query2).toContain(context.sessionId);
    expect(query1).toContain('LSN=1000');
    expect(query2).toContain('LSN=1000');
  });
});
