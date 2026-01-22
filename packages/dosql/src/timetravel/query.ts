/**
 * Time Travel Query Parser and Rewriter for DoSQL
 *
 * Parses and rewrites SQL queries containing time travel clauses:
 * - AS OF TIMESTAMP '2024-01-01'
 * - AS OF VERSION 123
 * - AS OF SYSTEM TIME '2024-01-01'
 * - FOR SYSTEM_TIME BETWEEN ... AND ...
 * - FOR SYSTEM_TIME AS OF ...
 * - FOR SYSTEM_TIME FROM ... TO ...
 *
 * Supports multi-level time travel across storage tiers:
 * - Hot (in-memory B-tree): milliseconds of history
 * - Warm (DO blob storage): hours/days of history
 * - Cold (R2 lakehouse): months/years of history
 *
 * @packageDocumentation
 */

import type { SnapshotId } from '../fsx/cow-types.js';
import {
  type TimePoint,
  type TimeTravelQuery,
  type TimeTravelScope,
  type AsOfClause,
  type TimeRange,
  TimeTravelError,
  TimeTravelErrorCode,
  lsn,
  timestamp,
  snapshot,
  branch,
} from './types.js';

// =============================================================================
// Query Types
// =============================================================================

/**
 * Parsed time travel query
 */
export interface ParsedTimeTravelQuery {
  /** Original SQL query */
  originalSQL: string;
  /** SQL query with time travel clauses removed */
  rewrittenSQL: string;
  /** Base table being queried */
  table: string;
  /** Time travel clause type */
  clauseType: TimeTravelClauseType;
  /** Parsed time point or range */
  timeSpec: TimePoint | TimeRange;
  /** Table alias if present */
  tableAlias?: string;
  /** Is this a temporal range query */
  isRangeQuery: boolean;
  /** Scope of the time travel */
  scope: TimeTravelScope;
}

/**
 * Time travel clause types
 */
export type TimeTravelClauseType =
  | 'AS_OF_TIMESTAMP'
  | 'AS_OF_VERSION'
  | 'AS_OF_SYSTEM_TIME'
  | 'FOR_SYSTEM_TIME_AS_OF'
  | 'FOR_SYSTEM_TIME_BETWEEN'
  | 'FOR_SYSTEM_TIME_FROM_TO'
  | 'AS_OF_SNAPSHOT'
  | 'AS_OF_BRANCH'
  | 'AS_OF_LSN';

/**
 * Result of query tier resolution
 */
export interface TierResolution {
  /** Which tier(s) to query */
  tiers: StorageTierTarget[];
  /** Explanation of the tier selection */
  explanation: string;
  /** Estimated data age */
  estimatedAge: 'hot' | 'warm' | 'cold';
  /** Whether cross-tier merge is needed */
  requiresMerge: boolean;
}

/**
 * Storage tier target for query
 */
export interface StorageTierTarget {
  /** Tier name */
  tier: 'memory' | 'do-blob' | 'r2-lakehouse';
  /** Time range covered by this tier */
  timeRange?: TimeRange;
  /** Whether this is the primary source */
  isPrimary: boolean;
  /** Snapshot ID if applicable */
  snapshotId?: SnapshotId;
}

// =============================================================================
// Query Parser
// =============================================================================

/**
 * Regular expressions for parsing time travel clauses
 */
const PATTERNS = {
  // AS OF TIMESTAMP 'YYYY-MM-DD HH:MM:SS' or AS OF TIMESTAMP @param
  AS_OF_TIMESTAMP: /\bAS\s+OF\s+TIMESTAMP\s+['"]?([^'"]+)['"]?/gi,

  // AS OF VERSION 123
  AS_OF_VERSION: /\bAS\s+OF\s+VERSION\s+(\d+)/gi,

  // AS OF SYSTEM TIME 'YYYY-MM-DD HH:MM:SS'
  AS_OF_SYSTEM_TIME: /\bAS\s+OF\s+SYSTEM\s+TIME\s+['"]?([^'"]+)['"]?/gi,

  // FOR SYSTEM_TIME AS OF 'YYYY-MM-DD HH:MM:SS'
  FOR_SYSTEM_TIME_AS_OF: /\bFOR\s+SYSTEM_TIME\s+AS\s+OF\s+['"]?([^'"]+)['"]?/gi,

  // FOR SYSTEM_TIME BETWEEN 'start' AND 'end'
  FOR_SYSTEM_TIME_BETWEEN: /\bFOR\s+SYSTEM_TIME\s+BETWEEN\s+['"]?([^'"]+)['"]?\s+AND\s+['"]?([^'"]+)['"]?/gi,

  // FOR SYSTEM_TIME FROM 'start' TO 'end'
  FOR_SYSTEM_TIME_FROM_TO: /\bFOR\s+SYSTEM_TIME\s+FROM\s+['"]?([^'"]+)['"]?\s+TO\s+['"]?([^'"]+)['"]?/gi,

  // AS OF SNAPSHOT 'branch@version'
  AS_OF_SNAPSHOT: /\bAS\s+OF\s+SNAPSHOT\s+['"]?([^'"]+)['"]?/gi,

  // AS OF BRANCH 'branch-name'
  AS_OF_BRANCH: /\bAS\s+OF\s+BRANCH\s+['"]?([^'"]+)['"]?/gi,

  // AS OF LSN 123
  AS_OF_LSN: /\bAS\s+OF\s+LSN\s+(\d+)/gi,

  // Table name extraction: FROM table_name [AS] [alias] [AS OF...]
  TABLE_FROM: /\bFROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s*/gi,
};

/**
 * Parse a SQL query for time travel clauses
 */
export function parseTimeTravelQuery(sql: string): ParsedTimeTravelQuery | null {
  const trimmedSQL = sql.trim();

  // Try each pattern to find time travel clauses
  let clauseType: TimeTravelClauseType | null = null;
  let timeSpec: TimePoint | TimeRange | null = null;
  let match: RegExpExecArray | null = null;
  let matchedPattern: RegExp | null = null;

  // Check AS_OF_TIMESTAMP
  PATTERNS.AS_OF_TIMESTAMP.lastIndex = 0;
  match = PATTERNS.AS_OF_TIMESTAMP.exec(trimmedSQL);
  if (match) {
    clauseType = 'AS_OF_TIMESTAMP';
    timeSpec = timestamp(match[1].trim());
    matchedPattern = PATTERNS.AS_OF_TIMESTAMP;
  }

  // Check AS_OF_VERSION
  if (!clauseType) {
    PATTERNS.AS_OF_VERSION.lastIndex = 0;
    match = PATTERNS.AS_OF_VERSION.exec(trimmedSQL);
    if (match) {
      clauseType = 'AS_OF_VERSION';
      timeSpec = lsn(BigInt(match[1]));
      matchedPattern = PATTERNS.AS_OF_VERSION;
    }
  }

  // Check AS_OF_SYSTEM_TIME
  if (!clauseType) {
    PATTERNS.AS_OF_SYSTEM_TIME.lastIndex = 0;
    match = PATTERNS.AS_OF_SYSTEM_TIME.exec(trimmedSQL);
    if (match) {
      clauseType = 'AS_OF_SYSTEM_TIME';
      timeSpec = timestamp(match[1].trim());
      matchedPattern = PATTERNS.AS_OF_SYSTEM_TIME;
    }
  }

  // Check FOR_SYSTEM_TIME_AS_OF
  if (!clauseType) {
    PATTERNS.FOR_SYSTEM_TIME_AS_OF.lastIndex = 0;
    match = PATTERNS.FOR_SYSTEM_TIME_AS_OF.exec(trimmedSQL);
    if (match) {
      clauseType = 'FOR_SYSTEM_TIME_AS_OF';
      timeSpec = timestamp(match[1].trim());
      matchedPattern = PATTERNS.FOR_SYSTEM_TIME_AS_OF;
    }
  }

  // Check FOR_SYSTEM_TIME_BETWEEN
  if (!clauseType) {
    PATTERNS.FOR_SYSTEM_TIME_BETWEEN.lastIndex = 0;
    match = PATTERNS.FOR_SYSTEM_TIME_BETWEEN.exec(trimmedSQL);
    if (match) {
      clauseType = 'FOR_SYSTEM_TIME_BETWEEN';
      timeSpec = {
        from: timestamp(match[1].trim()),
        to: timestamp(match[2].trim()),
      };
      matchedPattern = PATTERNS.FOR_SYSTEM_TIME_BETWEEN;
    }
  }

  // Check FOR_SYSTEM_TIME_FROM_TO
  if (!clauseType) {
    PATTERNS.FOR_SYSTEM_TIME_FROM_TO.lastIndex = 0;
    match = PATTERNS.FOR_SYSTEM_TIME_FROM_TO.exec(trimmedSQL);
    if (match) {
      clauseType = 'FOR_SYSTEM_TIME_FROM_TO';
      timeSpec = {
        from: timestamp(match[1].trim()),
        to: timestamp(match[2].trim()),
      };
      matchedPattern = PATTERNS.FOR_SYSTEM_TIME_FROM_TO;
    }
  }

  // Check AS_OF_SNAPSHOT
  if (!clauseType) {
    PATTERNS.AS_OF_SNAPSHOT.lastIndex = 0;
    match = PATTERNS.AS_OF_SNAPSHOT.exec(trimmedSQL);
    if (match) {
      clauseType = 'AS_OF_SNAPSHOT';
      timeSpec = snapshot(match[1].trim() as SnapshotId);
      matchedPattern = PATTERNS.AS_OF_SNAPSHOT;
    }
  }

  // Check AS_OF_BRANCH
  if (!clauseType) {
    PATTERNS.AS_OF_BRANCH.lastIndex = 0;
    match = PATTERNS.AS_OF_BRANCH.exec(trimmedSQL);
    if (match) {
      clauseType = 'AS_OF_BRANCH';
      timeSpec = branch(match[1].trim());
      matchedPattern = PATTERNS.AS_OF_BRANCH;
    }
  }

  // Check AS_OF_LSN
  if (!clauseType) {
    PATTERNS.AS_OF_LSN.lastIndex = 0;
    match = PATTERNS.AS_OF_LSN.exec(trimmedSQL);
    if (match) {
      clauseType = 'AS_OF_LSN';
      timeSpec = lsn(BigInt(match[1]));
      matchedPattern = PATTERNS.AS_OF_LSN;
    }
  }

  // If no time travel clause found, return null
  if (!clauseType || !timeSpec || !matchedPattern) {
    return null;
  }

  // Extract table name and alias
  PATTERNS.TABLE_FROM.lastIndex = 0;
  const tableMatch = PATTERNS.TABLE_FROM.exec(trimmedSQL);
  if (!tableMatch) {
    throw new TimeTravelError(
      TimeTravelErrorCode.INVALID_SYNTAX,
      'Could not parse table name from query',
      timeSpec as TimePoint
    );
  }

  const table = tableMatch[1];
  const tableAlias = tableMatch[2];

  // Remove time travel clause from SQL
  matchedPattern.lastIndex = 0;
  const rewrittenSQL = trimmedSQL.replace(matchedPattern, '').replace(/\s+/g, ' ').trim();

  // Determine if this is a range query
  const isRangeQuery = clauseType === 'FOR_SYSTEM_TIME_BETWEEN' ||
                       clauseType === 'FOR_SYSTEM_TIME_FROM_TO';

  // Determine scope based on time point
  const scope = determineScope(timeSpec, isRangeQuery);

  return {
    originalSQL: trimmedSQL,
    rewrittenSQL,
    table,
    clauseType,
    timeSpec,
    tableAlias,
    isRangeQuery,
    scope,
  };
}

/**
 * Determine the time travel scope based on time point
 */
function determineScope(
  timeSpec: TimePoint | TimeRange,
  isRangeQuery: boolean
): TimeTravelScope {
  // Range queries typically need global scope for consistency
  if (isRangeQuery) {
    return 'global';
  }

  // Check if it's a TimePoint or TimeRange
  if ('type' in timeSpec) {
    const point = timeSpec as TimePoint;

    // Branch and snapshot queries need branch-level scope
    if (point.type === 'branch' || point.type === 'snapshot') {
      return 'branch';
    }

    // LSN queries can be local
    if (point.type === 'lsn') {
      return 'local';
    }

    // Timestamp queries need global scope for cross-DO consistency
    if (point.type === 'timestamp') {
      return 'global';
    }
  }

  return 'local';
}

// =============================================================================
// Query Rewriter
// =============================================================================

/**
 * Rewrite a SQL query for time travel execution
 */
export function rewriteQuery(
  parsed: ParsedTimeTravelQuery,
  resolution: TierResolution
): RewrittenQuery {
  const queries: TierQuery[] = [];

  for (const tier of resolution.tiers) {
    let sql = parsed.rewrittenSQL;

    // Add tier-specific modifications
    if (tier.tier === 'memory') {
      // For memory tier, add snapshot hint if available
      if (tier.snapshotId) {
        sql = addSnapshotHint(sql, tier.snapshotId);
      }
    } else if (tier.tier === 'do-blob') {
      // For DO blob tier, add version filter
      if (parsed.timeSpec && 'type' in parsed.timeSpec) {
        const point = parsed.timeSpec as TimePoint;
        if (point.type === 'lsn') {
          sql = addVersionFilter(sql, parsed.table, point.lsn);
        }
      }
    } else if (tier.tier === 'r2-lakehouse') {
      // For lakehouse, add partition pruning hints
      if (parsed.timeSpec && 'type' in parsed.timeSpec) {
        const point = parsed.timeSpec as TimePoint;
        if (point.type === 'timestamp') {
          sql = addTimestampFilter(sql, parsed.table, point.timestamp);
        }
      }
    }

    queries.push({
      tier: tier.tier,
      sql,
      isPrimary: tier.isPrimary,
      snapshotId: tier.snapshotId,
    });
  }

  return {
    originalSQL: parsed.originalSQL,
    queries,
    requiresMerge: resolution.requiresMerge,
    scope: parsed.scope,
    isRangeQuery: parsed.isRangeQuery,
  };
}

/**
 * Add snapshot hint to query
 */
function addSnapshotHint(sql: string, snapshotId: SnapshotId): string {
  // Add a comment hint for the snapshot
  return `/* SNAPSHOT: ${snapshotId} */ ${sql}`;
}

/**
 * Add version filter to query
 */
function addVersionFilter(sql: string, table: string, version: bigint): string {
  // For DO blob tier, we filter by _version column
  const whereClause = sql.toLowerCase().includes('where')
    ? `AND ${table}._version <= ${version}`
    : `WHERE ${table}._version <= ${version}`;

  // Insert before ORDER BY, GROUP BY, or LIMIT if present
  const insertPoint = findInsertPoint(sql);
  if (insertPoint > 0) {
    return sql.slice(0, insertPoint) + ' ' + whereClause + ' ' + sql.slice(insertPoint);
  }

  return sql + ' ' + whereClause;
}

/**
 * Add timestamp filter for lakehouse queries
 */
function addTimestampFilter(sql: string, table: string, ts: Date): string {
  const isoTimestamp = ts.toISOString();
  const whereClause = sql.toLowerCase().includes('where')
    ? `AND ${table}._timestamp <= '${isoTimestamp}'`
    : `WHERE ${table}._timestamp <= '${isoTimestamp}'`;

  const insertPoint = findInsertPoint(sql);
  if (insertPoint > 0) {
    return sql.slice(0, insertPoint) + ' ' + whereClause + ' ' + sql.slice(insertPoint);
  }

  return sql + ' ' + whereClause;
}

/**
 * Find insertion point for WHERE clause additions
 */
function findInsertPoint(sql: string): number {
  const lowerSQL = sql.toLowerCase();

  // Find ORDER BY, GROUP BY, HAVING, or LIMIT
  const keywords = ['order by', 'group by', 'having', 'limit'];
  let minIndex = sql.length;

  for (const keyword of keywords) {
    const index = lowerSQL.indexOf(keyword);
    if (index > 0 && index < minIndex) {
      minIndex = index;
    }
  }

  return minIndex < sql.length ? minIndex : -1;
}

/**
 * Rewritten query result
 */
export interface RewrittenQuery {
  /** Original SQL */
  originalSQL: string;
  /** Queries per tier */
  queries: TierQuery[];
  /** Whether results from multiple tiers need merging */
  requiresMerge: boolean;
  /** Query scope */
  scope: TimeTravelScope;
  /** Whether this is a range query */
  isRangeQuery: boolean;
}

/**
 * Query for a specific tier
 */
export interface TierQuery {
  /** Target tier */
  tier: 'memory' | 'do-blob' | 'r2-lakehouse';
  /** SQL query for this tier */
  sql: string;
  /** Whether this is the primary source */
  isPrimary: boolean;
  /** Snapshot ID if applicable */
  snapshotId?: SnapshotId;
}

// =============================================================================
// Tier Resolution
// =============================================================================

/**
 * Configuration for tier resolution
 */
export interface TierResolutionConfig {
  /** Maximum age (ms) for hot tier data */
  hotTierMaxAge: number;
  /** Maximum age (ms) for warm tier data */
  warmTierMaxAge: number;
  /** Current time (for testing) */
  now?: Date;
}

/**
 * Default tier resolution configuration
 */
export const DEFAULT_TIER_CONFIG: TierResolutionConfig = {
  hotTierMaxAge: 60 * 1000, // 1 minute
  warmTierMaxAge: 24 * 60 * 60 * 1000, // 24 hours
};

/**
 * Resolve which storage tiers to query
 */
export function resolveTiers(
  timeSpec: TimePoint | TimeRange,
  config: TierResolutionConfig = DEFAULT_TIER_CONFIG
): TierResolution {
  const now = config.now || new Date();
  const tiers: StorageTierTarget[] = [];
  let explanation = '';
  let estimatedAge: 'hot' | 'warm' | 'cold' = 'hot';
  let requiresMerge = false;

  // Handle time ranges
  if (!('type' in timeSpec)) {
    const range = timeSpec as TimeRange;
    const fromTs = getTimestampFromPoint(range.from);
    const toTs = getTimestampFromPoint(range.to);

    if (fromTs && toTs) {
      const fromAge = now.getTime() - fromTs.getTime();
      const toAge = now.getTime() - toTs.getTime();

      // Range may span multiple tiers
      if (toAge <= config.hotTierMaxAge) {
        tiers.push({ tier: 'memory', isPrimary: true });
      }
      if (fromAge > config.hotTierMaxAge || toAge > config.hotTierMaxAge) {
        if (fromAge <= config.warmTierMaxAge || toAge <= config.warmTierMaxAge) {
          tiers.push({ tier: 'do-blob', isPrimary: tiers.length === 0 });
        }
      }
      if (fromAge > config.warmTierMaxAge) {
        tiers.push({ tier: 'r2-lakehouse', isPrimary: tiers.length === 0 });
        estimatedAge = 'cold';
      } else if (fromAge > config.hotTierMaxAge) {
        estimatedAge = 'warm';
      }

      requiresMerge = tiers.length > 1;
      explanation = `Range query spanning ${tiers.length} tier(s)`;
    }

    return { tiers, explanation, estimatedAge, requiresMerge };
  }

  const point = timeSpec as TimePoint;

  switch (point.type) {
    case 'lsn':
      // LSN-based queries need to check all tiers
      tiers.push({ tier: 'memory', isPrimary: true });
      tiers.push({ tier: 'do-blob', isPrimary: false });
      explanation = 'LSN query - checking hot and warm tiers';
      break;

    case 'timestamp': {
      const age = now.getTime() - point.timestamp.getTime();

      if (age <= config.hotTierMaxAge) {
        tiers.push({ tier: 'memory', isPrimary: true });
        explanation = 'Recent timestamp - using hot tier';
        estimatedAge = 'hot';
      } else if (age <= config.warmTierMaxAge) {
        tiers.push({ tier: 'do-blob', isPrimary: true });
        explanation = 'Warm timestamp - using DO blob storage';
        estimatedAge = 'warm';
      } else {
        tiers.push({ tier: 'r2-lakehouse', isPrimary: true });
        explanation = 'Historical timestamp - using R2 lakehouse';
        estimatedAge = 'cold';
      }
      break;
    }

    case 'snapshot':
      // Snapshots can be in any tier
      tiers.push({ tier: 'do-blob', isPrimary: true, snapshotId: point.snapshotId });
      tiers.push({ tier: 'r2-lakehouse', isPrimary: false, snapshotId: point.snapshotId });
      explanation = 'Snapshot query - checking warm and cold tiers';
      estimatedAge = 'warm';
      break;

    case 'branch':
      // Branch queries start from the latest branch state
      tiers.push({ tier: 'memory', isPrimary: true });
      tiers.push({ tier: 'do-blob', isPrimary: false });
      explanation = 'Branch query - checking hot and warm tiers';
      break;

    case 'relative':
      // Relative queries depend on the anchor
      if (point.anchor) {
        const anchorResult = resolveTiers(point.anchor, config);
        tiers.push(...anchorResult.tiers);
        explanation = `Relative query from anchor: ${anchorResult.explanation}`;
        estimatedAge = anchorResult.estimatedAge;
      } else {
        tiers.push({ tier: 'memory', isPrimary: true });
        explanation = 'Relative query from HEAD';
      }
      break;
  }

  return { tiers, explanation, estimatedAge, requiresMerge };
}

/**
 * Extract timestamp from a time point
 */
function getTimestampFromPoint(point: TimePoint): Date | null {
  switch (point.type) {
    case 'timestamp':
      return point.timestamp;
    case 'lsn':
      return null; // LSN doesn't have a direct timestamp
    case 'snapshot':
      return null; // Would need to resolve
    case 'branch':
      return point.point ? getTimestampFromPoint(point.point) : null;
    case 'relative':
      if (point.timeOffset !== undefined && point.anchor) {
        const anchorTs = getTimestampFromPoint(point.anchor);
        if (anchorTs) {
          return new Date(anchorTs.getTime() + point.timeOffset);
        }
      }
      return null;
  }
}

// =============================================================================
// Diff Query Builder
// =============================================================================

/**
 * Build a query to get changes between two versions
 */
export interface DiffQuery {
  /** SQL query to get changes */
  sql: string;
  /** From version */
  fromVersion: TimePoint;
  /** To version */
  toVersion: TimePoint;
  /** Table being diffed */
  table: string;
}

/**
 * Build a diff query between two time points
 */
export function buildDiffQuery(
  table: string,
  from: TimePoint,
  to: TimePoint
): DiffQuery {
  // Build a query that returns changed rows between versions
  const sql = `
    SELECT
      COALESCE(old._pk, new._pk) as _pk,
      CASE
        WHEN old._pk IS NULL THEN 'INSERT'
        WHEN new._pk IS NULL THEN 'DELETE'
        ELSE 'UPDATE'
      END as _change_type,
      old.* as _old,
      new.* as _new
    FROM
      (SELECT * FROM ${table} AS OF VERSION ${formatTimePoint(from)}) old
    FULL OUTER JOIN
      (SELECT * FROM ${table} AS OF VERSION ${formatTimePoint(to)}) new
    ON old._pk = new._pk
    WHERE old._pk IS NULL
      OR new._pk IS NULL
      OR old._hash != new._hash
  `.trim();

  return {
    sql,
    fromVersion: from,
    toVersion: to,
    table,
  };
}

/**
 * Format a time point for SQL
 */
function formatTimePoint(point: TimePoint): string {
  switch (point.type) {
    case 'lsn':
      return point.lsn.toString();
    case 'timestamp':
      return `'${point.timestamp.toISOString()}'`;
    case 'snapshot':
      return `'${point.snapshotId}'`;
    case 'branch':
      return `'${point.branch}'`;
    case 'relative':
      return 'HEAD'; // Would need resolution
  }
}

// =============================================================================
// Point-in-Time Recovery Query Builder
// =============================================================================

/**
 * Build a recovery query to restore table state to a point in time
 */
export interface RecoveryQuery {
  /** Sequence of SQL statements to execute */
  statements: string[];
  /** Target time point */
  targetPoint: TimePoint;
  /** Tables being recovered */
  tables: string[];
  /** Whether this is a full or partial recovery */
  recoveryType: 'full' | 'partial';
}

/**
 * Build point-in-time recovery queries
 */
export function buildRecoveryQuery(
  tables: string[],
  targetPoint: TimePoint,
  options: { partial?: boolean; dryRun?: boolean } = {}
): RecoveryQuery {
  const statements: string[] = [];
  const recoveryType = options.partial ? 'partial' : 'full';

  for (const table of tables) {
    // Start transaction
    if (!options.dryRun) {
      statements.push('BEGIN TRANSACTION');
    }

    // Create temp table with historical data
    statements.push(`
      CREATE TEMP TABLE _recovery_${table} AS
      SELECT * FROM ${table} AS OF VERSION ${formatTimePoint(targetPoint)}
    `);

    // Clear current table
    if (!options.partial) {
      statements.push(`DELETE FROM ${table}`);
    }

    // Restore from temp table
    statements.push(`
      INSERT INTO ${table}
      SELECT * FROM _recovery_${table}
      ON CONFLICT (_pk) DO UPDATE SET *
    `);

    // Drop temp table
    statements.push(`DROP TABLE _recovery_${table}`);

    // Commit
    if (!options.dryRun) {
      statements.push('COMMIT');
    }
  }

  return {
    statements,
    targetPoint,
    tables,
    recoveryType,
  };
}

// =============================================================================
// Validation
// =============================================================================

/**
 * Validate a time travel query
 */
export function validateTimeTravelQuery(
  parsed: ParsedTimeTravelQuery
): ValidationResult {
  const errors: string[] = [];
  const warnings: string[] = [];

  // Check for future timestamps
  if ('type' in parsed.timeSpec) {
    const point = parsed.timeSpec as TimePoint;
    if (point.type === 'timestamp') {
      if (point.timestamp.getTime() > Date.now()) {
        errors.push('Cannot query future timestamps');
      }
    }
  }

  // Check for range query validity
  if (parsed.isRangeQuery && !('type' in parsed.timeSpec)) {
    const range = parsed.timeSpec as TimeRange;
    const fromTs = getTimestampFromPoint(range.from);
    const toTs = getTimestampFromPoint(range.to);

    if (fromTs && toTs && fromTs.getTime() > toTs.getTime()) {
      errors.push('Range start time must be before end time');
    }
  }

  // Check for global scope on local-only resources
  if (parsed.scope === 'global') {
    warnings.push('Global scope queries may have higher latency');
  }

  // Check for potentially expensive queries
  if (parsed.isRangeQuery) {
    warnings.push('Range queries may scan large amounts of historical data');
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

/**
 * Validation result
 */
export interface ValidationResult {
  /** Whether the query is valid */
  valid: boolean;
  /** Validation errors */
  errors: string[];
  /** Validation warnings */
  warnings: string[];
}

// =============================================================================
// Snapshot Isolation
// =============================================================================

/**
 * Snapshot isolation context for consistent reads
 */
export interface SnapshotIsolationContext {
  /** Session ID */
  sessionId: string;
  /** Snapshot timestamp */
  snapshotTime: Date;
  /** Snapshot LSN */
  snapshotLSN: bigint;
  /** Branch context */
  branch: string;
  /** Whether the snapshot is still valid */
  isValid: boolean;
  /** Expiry time */
  expiresAt: Date;
}

/**
 * Create a snapshot isolation context
 */
export function createSnapshotContext(
  currentLSN: bigint,
  branch: string = 'main',
  ttlMs: number = 30000
): SnapshotIsolationContext {
  const now = new Date();

  return {
    sessionId: `snap-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    snapshotTime: now,
    snapshotLSN: currentLSN,
    branch,
    isValid: true,
    expiresAt: new Date(now.getTime() + ttlMs),
  };
}

/**
 * Check if a snapshot context is still valid
 */
export function isSnapshotValid(context: SnapshotIsolationContext): boolean {
  return context.isValid && new Date() < context.expiresAt;
}

/**
 * Apply snapshot context to a query
 */
export function applySnapshotContext(
  sql: string,
  context: SnapshotIsolationContext
): string {
  if (!isSnapshotValid(context)) {
    throw new TimeTravelError(
      TimeTravelErrorCode.SESSION_EXPIRED,
      'Snapshot isolation context has expired',
      { type: 'lsn', lsn: context.snapshotLSN }
    );
  }

  // Add snapshot LSN filter to query
  return `/* SNAPSHOT_ISOLATION: ${context.sessionId} LSN=${context.snapshotLSN} */ ${sql}`;
}
