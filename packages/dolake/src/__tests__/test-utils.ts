/**
 * Test Utilities for dolake Package
 *
 * This module provides type-safe test helpers, mock factories, and fixture creators
 * to replace `as any` type assertions in test files.
 *
 * @packageDocumentation
 */

// =============================================================================
// Response Types
// =============================================================================

/**
 * Base WebSocket response message
 */
export interface WSResponseMessage {
  type: string;
  id?: string;
}

/**
 * Acknowledgment response
 */
export interface AckResponse extends WSResponseMessage {
  type: 'ack';
  id: string;
}

/**
 * Negative acknowledgment response
 */
export interface NackResponse extends WSResponseMessage {
  type: 'nack';
  id?: string;
  reason: string;
}

/**
 * Rate limited NACK response
 */
export interface RateLimitedResponse extends NackResponse {
  reason: 'rate_limited';
}

/**
 * Memory pressure NACK response
 */
export interface MemoryPressureResponse extends NackResponse {
  reason: 'memory_pressure';
}

/**
 * Pong response
 */
export interface PongResponse extends WSResponseMessage {
  type: 'pong';
}

/**
 * Type guard for ack response
 */
export function isAckResponse(response: WSResponseMessage): response is AckResponse {
  return response.type === 'ack';
}

/**
 * Type guard for nack response
 */
export function isNackResponse(response: WSResponseMessage): response is NackResponse {
  return response.type === 'nack';
}

/**
 * Type guard for rate limited response
 */
export function isRateLimitedResponse(response: WSResponseMessage): response is RateLimitedResponse {
  return isNackResponse(response) && response.reason === 'rate_limited';
}

/**
 * Type guard for memory pressure response
 */
export function isMemoryPressureResponse(response: WSResponseMessage): response is MemoryPressureResponse {
  return isNackResponse(response) && response.reason === 'memory_pressure';
}

/**
 * Type guard for pong response
 */
export function isPongResponse(response: WSResponseMessage): response is PongResponse {
  return response.type === 'pong';
}

/**
 * Filters responses to get only ack responses
 */
export function filterAckResponses(responses: WSResponseMessage[]): AckResponse[] {
  return responses.filter(isAckResponse);
}

/**
 * Filters responses to get only rate limited responses
 */
export function filterRateLimitedResponses(responses: WSResponseMessage[]): RateLimitedResponse[] {
  return responses.filter(isRateLimitedResponse);
}

/**
 * Filters responses to get only memory pressure responses
 */
export function filterMemoryPressureResponses(responses: WSResponseMessage[]): MemoryPressureResponse[] {
  return responses.filter(isMemoryPressureResponse);
}

/**
 * Checks if any response is a pong
 */
export function hasPongResponse(responses: WSResponseMessage[]): boolean {
  return responses.some(isPongResponse);
}

// =============================================================================
// CDC Event Types
// =============================================================================

/**
 * CDC event structure for testing
 */
export interface CDCEvent {
  id: string;
  timestamp: number;
  table: string;
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  before?: Record<string, unknown>;
  after?: Record<string, unknown>;
  lsn?: bigint;
  txId?: string;
}

/**
 * Creates a mock CDC event
 */
export function createCDCEvent(
  table: string,
  operation: CDCEvent['operation'],
  data: Partial<Omit<CDCEvent, 'table' | 'operation'>> = {}
): CDCEvent {
  return {
    id: data.id ?? `cdc-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    timestamp: data.timestamp ?? Date.now(),
    table,
    operation,
    before: data.before,
    after: data.after,
    lsn: data.lsn,
    txId: data.txId,
  };
}

/**
 * Type guard to check if an object is a CDC event
 */
export function isCDCEvent(obj: unknown): obj is CDCEvent {
  if (typeof obj !== 'object' || obj === null) return false;
  const event = obj as Record<string, unknown>;
  return (
    typeof event.id === 'string' &&
    typeof event.timestamp === 'number' &&
    typeof event.table === 'string' &&
    typeof event.operation === 'string' &&
    ['INSERT', 'UPDATE', 'DELETE'].includes(event.operation as string)
  );
}

/**
 * Gets the table name from a CDC event safely
 */
export function getCDCEventTable(event: unknown): string | undefined {
  if (isCDCEvent(event)) {
    return event.table;
  }
  return undefined;
}

/**
 * Gets the operation from a CDC event safely
 */
export function getCDCEventOperation(event: unknown): CDCEvent['operation'] | undefined {
  if (isCDCEvent(event)) {
    return event.operation;
  }
  return undefined;
}

// =============================================================================
// Lake Query Result Types
// =============================================================================

/**
 * Lake query result row
 */
export interface LakeRow {
  [column: string]: unknown;
}

/**
 * Lake query result with count
 */
export interface CountLakeRow extends LakeRow {
  count: number;
}

/**
 * Lake query result structure
 */
export interface LakeQueryResult {
  rows: LakeRow[];
  columns?: string[];
  rowCount?: number;
}

/**
 * Gets count value from lake row safely
 */
export function getLakeRowCount(row: LakeRow): number {
  const count = row['count'] ?? row['COUNT(*)'];
  if (typeof count === 'number') return count;
  if (typeof count === 'bigint') return Number(count);
  if (typeof count === 'string') return parseInt(count, 10);
  return 0;
}

/**
 * Type guard for count lake row
 */
export function isCountLakeRow(row: LakeRow): row is CountLakeRow {
  return 'count' in row && typeof row.count === 'number';
}

// =============================================================================
// Message Validation Types
// =============================================================================

/**
 * Validated message structure
 */
export interface ValidatedMessage {
  type: string;
  payload: unknown;
  isValid: boolean;
  errors?: string[];
}

/**
 * Prototype pollution test helper
 * Tests that prototype pollution attempts are detected
 */
export function testPrototypePollution(obj: object): boolean {
  // Check if object has been polluted
  const proto = Object.getPrototypeOf(obj);
  const emptyObj = {};
  const emptyProto = Object.getPrototypeOf(emptyObj);

  // Check for common pollution indicators
  return (
    (emptyObj as Record<string, unknown>)['isAdmin'] === undefined &&
    (emptyObj as Record<string, unknown>)['polluted'] === undefined &&
    proto === emptyProto
  );
}

// =============================================================================
// Lakehouse Types
// =============================================================================

/**
 * Time travel query options
 */
export interface TimeTravelOptions {
  asOfTimestamp?: number;
  asOfVersion?: number;
  asOfLSN?: bigint;
}

/**
 * Historical query result
 */
export interface HistoricalQueryResult extends LakeQueryResult {
  snapshotId?: string;
  snapshotTimestamp?: number;
}

/**
 * Compares row counts between historical and current results
 */
export function compareRowCounts(
  historical: LakeQueryResult,
  current: LakeQueryResult
): { historicalCount: number; currentCount: number; isHistoricalLess: boolean } {
  const historicalRow = historical.rows[0] ?? {};
  const currentRow = current.rows[0] ?? {};

  const historicalCount = getLakeRowCount(historicalRow);
  const currentCount = getLakeRowCount(currentRow);

  return {
    historicalCount,
    currentCount,
    isHistoricalLess: historicalCount < currentCount,
  };
}

// =============================================================================
// Sharded Aggregator Types
// =============================================================================

/**
 * Shard aggregation result
 */
export interface ShardAggregationResult {
  shardId: string;
  partialResult: unknown;
  rowCount: number;
  timestamp: number;
}

/**
 * Merged aggregation result
 */
export interface MergedAggregationResult {
  results: ShardAggregationResult[];
  totalRowCount: number;
  mergedAt: number;
}

/**
 * Creates a shard aggregation result
 */
export function createShardAggregationResult(
  shardId: string,
  partialResult: unknown,
  rowCount: number
): ShardAggregationResult {
  return {
    shardId,
    partialResult,
    rowCount,
    timestamp: Date.now(),
  };
}
