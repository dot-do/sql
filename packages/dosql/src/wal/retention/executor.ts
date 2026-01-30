/**
 * WAL Retention Executor Module
 *
 * Handles retention check logic, segment deletion, archival,
 * compaction, and truncation operations.
 *
 * @packageDocumentation
 */

import type { FSXBackend } from '../../fsx/types.js';
import type { WALReader, WALSegment, WALConfig, WALWriter, Checkpoint } from '../types.js';
import type { ReplicationSlotManager } from '../../cdc/types.js';
import type {
  RetentionPolicy,
  ActiveReader,
  RetentionCheckResult,
  RetentionCleanupResult,
  PolicyDecision,
  StorageStats,
  EntryStats,
  SegmentEntryStats,
  FragmentationInfo,
  MergeResult,
  CompactResult,
  CheckpointInfo,
  ExpiredEntry,
  WALStats,
  TruncateResult,
  CompactWALResult,
  ForceCleanupOptions,
  ForceCleanupResult,
  CleanupProgressEvent,
  ThrottleConfig,
} from '../retention-types.js';
import { checkStorageWarning } from './policy.js';
import { throttledProcess, sleep } from './scheduler.js';

// =============================================================================
// Checkpoint Manager Interface for Retention
// =============================================================================

/**
 * Interface for checkpoint manager that can have its createCheckpoint method wrapped.
 * This is a minimal interface used by the retention manager to register cleanup triggers.
 */
export interface CheckpointManagerForRetention {
  /**
   * Create a new checkpoint at the given LSN.
   * This method may be wrapped by the retention manager to trigger cleanup after checkpoints.
   */
  createCheckpoint(
    lsn: bigint,
    activeTransactions: string[]
  ): Promise<Checkpoint>;
}

// =============================================================================
// Executor Context
// =============================================================================

/**
 * Context for executor operations
 */
export interface ExecutorContext {
  backend: FSXBackend;
  reader: WALReader;
  slotManager: ReplicationSlotManager | null;
  policy: RetentionPolicy;
  config: WALConfig;
  activeReaders: Map<string, ActiveReader>;
  activeWriters: Set<WALWriter>;
  checkpointHistory: CheckpointInfo[];
  throttleConfig: ThrottleConfig;
  onProgress?: (event: CleanupProgressEvent) => void;
}

// =============================================================================
// Segment LSN Parsing
// =============================================================================

/**
 * Parse segment ID to extract start LSN
 *
 * @param segmentId - Segment ID in format "seg_XXXX"
 * @returns Parsed LSN value
 */
export function parseSegmentLSN(segmentId: string): bigint {
  const match = segmentId.match(/seg_(\d+)/);
  return match && match[1] ? BigInt(match[1]) : 0n;
}

// =============================================================================
// Reader Protection
// =============================================================================

/**
 * Check if a reader is idle (no activity for readerIdleTimeout)
 *
 * @param readerInfo - Reader information
 * @param readerIdleTimeout - Idle timeout in milliseconds
 * @returns True if reader is idle
 */
export function isReaderIdle(readerInfo: ActiveReader, readerIdleTimeout: number): boolean {
  return Date.now() - readerInfo.lastActivityAt >= readerIdleTimeout;
}

/**
 * Check if segment is protected by any active reader
 *
 * @param segmentId - Segment ID
 * @param endLSN - Segment end LSN
 * @param activeReaders - Map of active readers
 * @param readerIdleTimeout - Reader idle timeout
 * @returns True if protected
 */
export function isProtectedByReader(
  segmentId: string,
  endLSN: bigint,
  activeReaders: Map<string, ActiveReader>,
  readerIdleTimeout: number
): boolean {
  for (const readerInfo of activeReaders.values()) {
    // Skip idle readers
    if (isReaderIdle(readerInfo, readerIdleTimeout)) continue;

    // Segment is protected if reader is currently reading it
    // or if reader's position is within the segment's LSN range
    if (readerInfo.currentSegmentId === segmentId) {
      return true;
    }
    if (readerInfo.currentLSN <= endLSN) {
      return true;
    }
  }
  return false;
}

// =============================================================================
// Checkpoint LSN
// =============================================================================

/**
 * Get the latest checkpoint LSN from storage
 *
 * @param backend - Storage backend
 * @param checkpointPath - Path to checkpoint file
 * @returns Latest checkpoint LSN or null
 */
export async function getLatestCheckpointLSN(
  backend: FSXBackend,
  checkpointPath: string
): Promise<bigint | null> {
  try {
    const checkpointData = await backend.read(checkpointPath);
    if (!checkpointData) return null;
    const checkpoint = JSON.parse(new TextDecoder().decode(checkpointData));
    return BigInt(checkpoint.lsn);
  } catch {
    return null;
  }
}

// =============================================================================
// Segment Information
// =============================================================================

/**
 * Segment information for retention decisions
 */
export interface SegmentInfo {
  id: string;
  startLSN: bigint;
  endLSN: bigint;
  createdAt: number;
  entryCount: number;
  sizeBytes: number;
}

/**
 * Get segment information including timestamp
 *
 * @param ctx - Executor context
 * @param segmentId - Segment ID
 * @returns Segment info or null
 */
export async function getSegmentInfo(
  ctx: ExecutorContext,
  segmentId: string
): Promise<SegmentInfo | null> {
  try {
    const segment = await ctx.reader.readSegment(segmentId);
    if (!segment) return null;

    // Get segment size
    const path = `${ctx.config.segmentPrefix}${segmentId}`;
    const data = await ctx.backend.read(path);
    const sizeBytes = data ? data.length : 0;

    // Use the earliest entry timestamp as createdAt for more accurate age tracking.
    // This handles the case where entry timestamps differ from segment creation time.
    let effectiveCreatedAt = segment.createdAt;
    for (const entry of segment.entries) {
      if (entry.timestamp < effectiveCreatedAt) {
        effectiveCreatedAt = entry.timestamp;
      }
    }

    return {
      id: segmentId,
      startLSN: segment.startLSN,
      endLSN: segment.endLSN,
      createdAt: effectiveCreatedAt,
      entryCount: segment.entries.length,
      sizeBytes,
    };
  } catch {
    return null;
  }
}

// =============================================================================
// Retention Check
// =============================================================================

/**
 * Check which segments are eligible for deletion
 *
 * @param ctx - Executor context
 * @returns Retention check result
 */
export async function checkRetention(ctx: ExecutorContext): Promise<RetentionCheckResult> {
  const segments = await ctx.reader.listSegments(false);
  const now = Date.now();

  const result: RetentionCheckResult = {
    eligibleForDeletion: [],
    protectedBySlots: [],
    protectedByReaders: [],
    protectedByMinCount: [],
    minSlotLSN: null,
    oldestSegmentTime: null,
    totalSegmentCount: segments.length,
  };

  if (ctx.policy.verboseLogging) {
    result.decisions = [];
  }
  result.segmentsBeforeCheckpoint = [];
  result.emptySegments = [];
  result.skippedInvalid = [];
  result.protectedByCDC = [];

  if (segments.length === 0) {
    return result;
  }

  // Get min slot LSN
  result.minSlotLSN = await getMinSlotLSN(ctx.slotManager);

  // Get checkpoint LSN for checkpoint-based retention
  const checkpointLSN = await getLatestCheckpointLSN(ctx.backend, ctx.config.checkpointPath);

  // Get CDC pending LSN if configured
  let cdcPendingLSN: bigint | null = null;
  if (ctx.policy.cdcIntegration) {
    try {
      cdcPendingLSN = await ctx.policy.cdcIntegration.getPendingLSN();
    } catch {
      // Ignore CDC errors
    }
  }

  // Batch processing limit
  const maxToProcess = ctx.policy.maxSegmentsToProcess ?? segments.length;
  const segmentsToProcess = segments.slice(0, maxToProcess);
  if (segmentsToProcess.length < segments.length) {
    result.batchProcessed = true;
  }

  // Track size and entry stats
  let totalBytes = 0;
  let totalEntries = 0;
  const emptySegments: string[] = [];
  const skippedInvalid: string[] = [];
  const segmentsBeforeCheckpoint: string[] = [];
  const protectedByCDC: string[] = [];

  // Analyze each segment
  const segmentInfos: SegmentInfo[] = [];

  for (const segmentId of segmentsToProcess) {
    // Check for invalid segment ID
    if (!segmentId.match(/^seg_\d+$/)) {
      skippedInvalid.push(segmentId);
      continue;
    }

    const info = await getSegmentInfo(ctx, segmentId);
    if (info) {
      segmentInfos.push(info);
      totalBytes += info.sizeBytes;
      totalEntries += info.entryCount;

      if (info.entryCount === 0) {
        emptySegments.push(segmentId);
      }

      if (
        result.oldestSegmentTime === null ||
        info.createdAt < result.oldestSegmentTime
      ) {
        result.oldestSegmentTime = info.createdAt;
      }
    }
  }

  // Check for clock skew
  let clockSkewDetected = false;
  if (ctx.policy.handleClockSkew && segmentInfos.length > 1) {
    for (let i = 1; i < segmentInfos.length; i++) {
      const prev = segmentInfos[i - 1];
      const curr = segmentInfos[i];
      if (prev && curr && curr.startLSN > prev.startLSN && curr.createdAt < prev.createdAt) {
        clockSkewDetected = true;
        break;
      }
    }
  }

  result.emptySegments = emptySegments;
  result.skippedInvalid = skippedInvalid;
  result.clockSkewDetected = clockSkewDetected;

  // Calculate bytes/entries over limit
  if (ctx.policy.maxTotalBytes) {
    if (totalBytes > ctx.policy.maxTotalBytes) {
      result.bytesOverLimit = totalBytes - ctx.policy.maxTotalBytes;
    }

    // Check warning threshold (can trigger even when not over limit)
    const warning = checkStorageWarning(totalBytes, ctx.policy);
    if (warning) {
      if (ctx.policy.onWarning) {
        ctx.policy.onWarning(warning);
      }
      // Also call onSizeWarning callback if configured
      if (ctx.policy.onSizeWarning) {
        ctx.policy.onSizeWarning(totalBytes, ctx.policy.maxTotalBytes);
      }
    }
  }

  if (ctx.policy.maxEntryCount && totalEntries > ctx.policy.maxEntryCount) {
    result.entriesOverLimit = totalEntries - ctx.policy.maxEntryCount;
  }

  // Sort by LSN
  segmentInfos.sort((a, b) =>
    a.startLSN < b.startLSN ? -1 : a.startLSN > b.startLSN ? 1 : 0
  );

  // Determine minimum segments to keep
  const segmentsToKeep = Math.max(
    ctx.policy.minSegmentCount,
    segments.length - segmentInfos.length
  );
  const keepCount = Math.min(segmentsToKeep, segmentInfos.length);

  // Segments protected by min count (the most recent ones)
  const protectedByMinCount = new Set<string>();
  for (let i = segmentInfos.length - keepCount; i < segmentInfos.length; i++) {
    if (i >= 0) {
      const segment = segmentInfos[i];
      if (segment) {
        protectedByMinCount.add(segment.id);
        result.protectedByMinCount.push(segment.id);
      }
    }
  }

  // Check each segment
  for (const segmentInfo of segmentInfos) {
    const reasons: string[] = [];
    let eligible = true;

    // Skip if protected by min count
    if (protectedByMinCount.has(segmentInfo.id)) {
      if (ctx.policy.verboseLogging && result.decisions) {
        result.decisions.push({
          segmentId: segmentInfo.id,
          decision: 'keep',
          reasons: ['Protected by minimum segment count policy'],
        });
      }
      continue;
    }

    // Check slot protection
    if (
      ctx.policy.respectSlotPositions &&
      result.minSlotLSN !== null &&
      segmentInfo.endLSN >= result.minSlotLSN
    ) {
      result.protectedBySlots.push(segmentInfo.id);
      reasons.push('Protected by replication slot');
      eligible = false;
    }

    // Check reader protection
    if (
      eligible &&
      isProtectedByReader(
        segmentInfo.id,
        segmentInfo.endLSN,
        ctx.activeReaders,
        ctx.policy.readerIdleTimeout
      )
    ) {
      result.protectedByReaders.push(segmentInfo.id);
      reasons.push('Protected by active reader');
      eligible = false;
    }

    // Check CDC protection
    if (eligible && cdcPendingLSN !== null && segmentInfo.endLSN >= cdcPendingLSN) {
      protectedByCDC.push(segmentInfo.id);
      reasons.push('Protected by pending CDC events');
      eligible = false;
    }

    // Check checkpoint-based retention in strict mode
    if (
      ctx.policy.checkpointRetentionMode === 'strict' &&
      checkpointLSN !== null &&
      segmentInfo.endLSN < checkpointLSN
    ) {
      segmentsBeforeCheckpoint.push(segmentInfo.id);
    }

    // Check age (must be older than maxSegmentAge)
    if (eligible && !clockSkewDetected) {
      const age = now - segmentInfo.createdAt;
      if (age < ctx.policy.maxSegmentAge) {
        reasons.push(`Not old enough (age: ${age}ms, required: ${ctx.policy.maxSegmentAge}ms)`);
        eligible = false;
      }
    }

    if (eligible) {
      result.eligibleForDeletion.push(segmentInfo.id);
      reasons.push('Eligible for deletion');
    }

    if (ctx.policy.verboseLogging && result.decisions) {
      result.decisions.push({
        segmentId: segmentInfo.id,
        decision: eligible ? 'delete' : 'keep',
        reasons,
      });
    }
  }

  // Include empty segments as eligible
  for (const emptySegment of emptySegments) {
    if (!result.eligibleForDeletion.includes(emptySegment) && !protectedByMinCount.has(emptySegment)) {
      result.eligibleForDeletion.push(emptySegment);
    }
  }

  result.segmentsBeforeCheckpoint = segmentsBeforeCheckpoint;
  result.protectedByCDC = protectedByCDC;

  return result;
}

// =============================================================================
// Cleanup Execution
// =============================================================================

/**
 * Perform cleanup of eligible segments
 *
 * @param ctx - Executor context
 * @param segmentsToDelete - Segments to delete
 * @param dryRun - Whether this is a dry run
 * @returns Cleanup result
 */
export async function executeCleanup(
  ctx: ExecutorContext,
  segmentsToDelete: string[],
  dryRun: boolean
): Promise<RetentionCleanupResult> {
  const startTime = Date.now();

  const result: RetentionCleanupResult = {
    deleted: [],
    archived: [],
    failed: [],
    bytesFreed: 0,
    durationMs: 0,
    corruptedSegments: [],
  };

  if (dryRun) {
    result.deleted = [...segmentsToDelete];
    result.durationMs = Date.now() - startTime;
    return result;
  }

  const totalSegments = segmentsToDelete.length;

  await throttledProcess(
    segmentsToDelete,
    ctx.throttleConfig,
    async (segmentId, index) => {
      try {
        const sourcePath = `${ctx.config.segmentPrefix}${segmentId}`;

        // Get segment size for bytes freed calculation
        let data: Uint8Array | null = null;
        try {
          data = await ctx.backend.read(sourcePath);
        } catch (error) {
          // Segment might be corrupted
          result.corruptedSegments!.push(segmentId);
          return false;
        }

        if (!data) return false;

        const segmentSize = data.length;

        // Archive if configured
        if (ctx.policy.archiveBeforeDelete) {
          const archivePath = `${ctx.config.archivePrefix}${segmentId}`;
          await ctx.backend.write(archivePath, data);
          result.archived.push(segmentId);
        }

        // Delete segment
        await ctx.backend.delete(sourcePath);
        result.deleted.push(segmentId);
        result.bytesFreed += segmentSize;

        // Emit progress
        if (ctx.onProgress) {
          ctx.onProgress({
            type: 'progress',
            segmentsProcessed: index + 1,
            segmentsTotal: totalSegments,
            bytesFreed: result.bytesFreed,
          });
        }

        return true;
      } catch (error) {
        result.failed.push({
          segmentId,
          error: error instanceof Error ? error.message : String(error),
        });
        return false;
      }
    }
  );

  result.durationMs = Date.now() - startTime;
  return result;
}

// =============================================================================
// Slot LSN
// =============================================================================

/**
 * Get minimum LSN across all replication slots
 *
 * @param slotManager - Replication slot manager
 * @returns Minimum LSN or null
 */
export async function getMinSlotLSN(
  slotManager: ReplicationSlotManager | null
): Promise<bigint | null> {
  if (!slotManager) return null;

  const slots = await slotManager.listSlots();
  if (slots.length === 0) return null;

  const firstSlot = slots[0];
  if (!firstSlot) return null;

  let minLSN = firstSlot.acknowledgedLSN;
  for (const slot of slots) {
    if (slot.acknowledgedLSN < minLSN) {
      minLSN = slot.acknowledgedLSN;
    }
  }

  return minLSN;
}

// =============================================================================
// Storage Statistics
// =============================================================================

/**
 * Get storage statistics
 *
 * @param ctx - Executor context
 * @returns Storage stats
 */
export async function getStorageStats(ctx: ExecutorContext): Promise<StorageStats> {
  const segments = await ctx.reader.listSegments(false);
  let totalBytes = 0;

  for (const segmentId of segments) {
    const path = `${ctx.config.segmentPrefix}${segmentId}`;
    const data = await ctx.backend.read(path);
    if (data) {
      totalBytes += data.length;
    }
  }

  return {
    totalBytes,
    segmentCount: segments.length,
    averageSegmentSize: segments.length > 0 ? totalBytes / segments.length : 0,
  };
}

/**
 * Get entry statistics
 *
 * @param ctx - Executor context
 * @returns Entry stats
 */
export async function getEntryStats(ctx: ExecutorContext): Promise<EntryStats> {
  const segments = await ctx.reader.listSegments(false);
  let totalEntries = 0;

  for (const segmentId of segments) {
    const segment = await ctx.reader.readSegment(segmentId);
    if (segment) {
      totalEntries += segment.entries.length;
    }
  }

  return { totalEntries };
}

/**
 * Get per-segment entry statistics
 *
 * @param ctx - Executor context
 * @returns Array of segment stats
 */
export async function getSegmentEntryStats(ctx: ExecutorContext): Promise<SegmentEntryStats[]> {
  const segments = await ctx.reader.listSegments(false);
  const stats: SegmentEntryStats[] = [];

  for (const segmentId of segments) {
    const segment = await ctx.reader.readSegment(segmentId);
    const path = `${ctx.config.segmentPrefix}${segmentId}`;
    const data = await ctx.backend.read(path);

    if (segment && data) {
      stats.push({
        segmentId,
        entryCount: segment.entries.length,
        sizeBytes: data.length,
      });
    }
  }

  return stats;
}

// =============================================================================
// Fragmentation
// =============================================================================

/**
 * Calculate fragmentation ratio
 *
 * @param ctx - Executor context
 * @returns Fragmentation info
 */
export async function calculateFragmentation(ctx: ExecutorContext): Promise<FragmentationInfo> {
  const segments = await ctx.reader.listSegments(false);
  let totalEntries = 0;
  let rolledBackEntries = 0;

  // Track rolled-back transaction IDs
  const rolledBackTxns = new Set<string>();

  // First pass: identify rolled-back transactions
  for (const segmentId of segments) {
    const segment = await ctx.reader.readSegment(segmentId);
    if (!segment) continue;

    for (const entry of segment.entries) {
      if (entry.op === 'ROLLBACK') {
        rolledBackTxns.add(entry.txnId);
      }
    }
  }

  // Second pass: count entries
  for (const segmentId of segments) {
    const segment = await ctx.reader.readSegment(segmentId);
    if (!segment) continue;

    for (const entry of segment.entries) {
      totalEntries++;
      if (rolledBackTxns.has(entry.txnId)) {
        rolledBackEntries++;
      }
    }
  }

  const ratio = totalEntries > 0 ? rolledBackEntries / totalEntries : 0;

  return {
    ratio,
    rolledBackEntries,
    totalEntries,
  };
}

// =============================================================================
// Compaction
// =============================================================================

/**
 * Check if compaction is needed
 *
 * @param ctx - Executor context
 * @returns True if compaction is needed
 */
export async function checkCompactionNeeded(ctx: ExecutorContext): Promise<boolean> {
  if (!ctx.policy.compactionThreshold) return false;

  const fragmentation = await calculateFragmentation(ctx);
  const needed = fragmentation.ratio >= ctx.policy.compactionThreshold;

  if (needed && ctx.policy.onCompactionNeeded) {
    ctx.policy.onCompactionNeeded();
  }

  return needed;
}

/**
 * Merge multiple segments into one
 *
 * @param ctx - Executor context
 * @param segmentIds - Segments to merge
 * @returns Merge result
 */
export async function mergeSegments(
  ctx: ExecutorContext,
  segmentIds: string[]
): Promise<MergeResult> {
  if (segmentIds.length < 2) {
    return { newSegmentId: segmentIds[0] || '', mergedCount: 0, bytesSaved: 0 };
  }

  // Load all segments
  const segments: WALSegment[] = [];
  let totalOriginalSize = 0;

  for (const id of segmentIds) {
    const segment = await ctx.reader.readSegment(id);
    const path = `${ctx.config.segmentPrefix}${id}`;
    const data = await ctx.backend.read(path);
    if (segment && data) {
      segments.push(segment);
      totalOriginalSize += data.length;
    }
  }

  if (segments.length < 2) {
    return { newSegmentId: segmentIds[0] ?? '', mergedCount: 0, bytesSaved: 0 };
  }

  // Merge entries
  const allEntries = segments.flatMap((s) => s.entries);
  allEntries.sort((a, b) => (a.lsn < b.lsn ? -1 : a.lsn > b.lsn ? 1 : 0));

  const firstEntry = allEntries[0];
  const lastEntry = allEntries[allEntries.length - 1];
  if (!firstEntry || !lastEntry) {
    return { newSegmentId: segmentIds[0] ?? '', mergedCount: 0, bytesSaved: 0 };
  }

  const newSegmentId = `seg_${firstEntry.lsn.toString().padStart(20, '0')}`;
  const newSegment: WALSegment = {
    id: newSegmentId,
    startLSN: firstEntry.lsn,
    endLSN: lastEntry.lsn,
    entries: allEntries,
    checksum: 0, // Will be recalculated
    createdAt: Date.now(),
  };

  // Write new segment
  const encoder = new (await import('../writer.js')).DefaultWALEncoder();
  const newData = encoder.encodeSegment(newSegment);
  newSegment.checksum = encoder.calculateChecksum(newData);
  const finalData = encoder.encodeSegment(newSegment);

  await ctx.backend.write(`${ctx.config.segmentPrefix}${newSegmentId}`, finalData);

  // Delete old segments
  for (const id of segmentIds) {
    await ctx.backend.delete(`${ctx.config.segmentPrefix}${id}`);
  }

  return {
    newSegmentId,
    mergedCount: segments.length,
    bytesSaved: totalOriginalSize - finalData.length,
  };
}

/**
 * Compact a single segment
 *
 * @param ctx - Executor context
 * @param segmentId - Segment to compact
 * @returns Compact result
 */
export async function compactSegment(
  ctx: ExecutorContext,
  segmentId: string
): Promise<CompactResult> {
  const segment = await ctx.reader.readSegment(segmentId);
  if (!segment) {
    return { entriesRemoved: 0, bytesSaved: 0 };
  }

  const path = `${ctx.config.segmentPrefix}${segmentId}`;
  const originalData = await ctx.backend.read(path);
  if (!originalData) {
    return { entriesRemoved: 0, bytesSaved: 0 };
  }

  // Identify rolled-back transactions
  const rolledBackTxns = new Set<string>();
  for (const entry of segment.entries) {
    if (entry.op === 'ROLLBACK') {
      rolledBackTxns.add(entry.txnId);
    }
  }

  // Filter out rolled-back entries
  const keptEntries = segment.entries.filter(
    (e) => !rolledBackTxns.has(e.txnId)
  );

  const entriesRemoved = segment.entries.length - keptEntries.length;

  if (entriesRemoved === 0) {
    return { entriesRemoved: 0, bytesSaved: 0 };
  }

  // Create new compacted segment
  const lastKeptEntry = keptEntries[keptEntries.length - 1];
  const compactedSegment: WALSegment = {
    ...segment,
    entries: keptEntries,
    endLSN: lastKeptEntry ? lastKeptEntry.lsn : segment.startLSN,
  };

  // Write new segment
  const encoder = new (await import('../writer.js')).DefaultWALEncoder();
  const newData = encoder.encodeSegment(compactedSegment);
  compactedSegment.checksum = encoder.calculateChecksum(newData);
  const finalData = encoder.encodeSegment(compactedSegment);

  await ctx.backend.write(path, finalData);

  return {
    entriesRemoved,
    bytesSaved: originalData.length - finalData.length,
  };
}

// =============================================================================
// Expired Entries
// =============================================================================

/**
 * Get expired entries based on retention policy
 *
 * @param ctx - Executor context
 * @returns Array of expired entries
 */
export async function getExpiredEntries(ctx: ExecutorContext): Promise<ExpiredEntry[]> {
  const entries: ExpiredEntry[] = [];
  const now = Date.now();
  const maxAge = ctx.policy.retentionHours
    ? ctx.policy.retentionHours * 60 * 60 * 1000
    : ctx.policy.maxSegmentAge;

  const segments = await ctx.reader.listSegments(false);
  for (const segmentId of segments) {
    const segment = await ctx.reader.readSegment(segmentId);
    if (!segment) continue;

    for (const entry of segment.entries) {
      const age = now - entry.timestamp;
      if (age > maxAge) {
        entries.push({
          lsn: entry.lsn,
          segmentId,
          entryTimestamp: entry.timestamp,
          age,
          txnId: entry.txnId,
          table: entry.table,
          op: entry.op,
        });
      }
    }
  }

  return entries;
}

// =============================================================================
// WAL Statistics
// =============================================================================

/**
 * Get comprehensive WAL statistics
 *
 * @param ctx - Executor context
 * @returns WAL stats
 */
export async function getWALStats(ctx: ExecutorContext): Promise<WALStats> {
  const segments = await ctx.reader.listSegments(false);
  let totalBytes = 0;
  let totalEntries = 0;
  let oldestEntryTimestamp: number | null = null;
  let newestEntryTimestamp: number | null = null;
  let deadEntriesCount = 0;
  const activeTransactions = new Set<string>();
  const rolledBackTxns = new Set<string>();
  const committedTxns = new Set<string>();

  // First pass: identify transaction states and collect stats
  for (const segmentId of segments) {
    const segment = await ctx.reader.readSegment(segmentId);
    if (!segment) continue;

    const path = `${ctx.config.segmentPrefix}${segmentId}`;
    const data = await ctx.backend.read(path);
    if (data) {
      totalBytes += data.length;
    }

    for (const entry of segment.entries) {
      totalEntries++;

      // Track timestamps
      if (oldestEntryTimestamp === null || entry.timestamp < oldestEntryTimestamp) {
        oldestEntryTimestamp = entry.timestamp;
      }
      if (newestEntryTimestamp === null || entry.timestamp > newestEntryTimestamp) {
        newestEntryTimestamp = entry.timestamp;
      }

      // Track transaction states
      if (entry.op === 'BEGIN') {
        activeTransactions.add(entry.txnId);
      } else if (entry.op === 'COMMIT') {
        activeTransactions.delete(entry.txnId);
        committedTxns.add(entry.txnId);
      } else if (entry.op === 'ROLLBACK') {
        activeTransactions.delete(entry.txnId);
        rolledBackTxns.add(entry.txnId);
      }
    }
  }

  // Second pass: count dead entries (from rolled-back transactions)
  for (const segmentId of segments) {
    const segment = await ctx.reader.readSegment(segmentId);
    if (!segment) continue;

    for (const entry of segment.entries) {
      if (rolledBackTxns.has(entry.txnId)) {
        deadEntriesCount++;
      }
    }
  }

  const fragmentationRatio = totalEntries > 0 ? deadEntriesCount / totalEntries : 0;
  const averageSegmentSize = segments.length > 0 ? totalBytes / segments.length : 0;

  return {
    totalSegments: segments.length,
    totalEntries,
    totalBytes,
    oldestEntryTimestamp,
    newestEntryTimestamp,
    averageSegmentSize,
    fragmentationRatio,
    deadEntriesCount,
    activeTransactionCount: activeTransactions.size,
  };
}

// =============================================================================
// Truncate WAL
// =============================================================================

/**
 * Truncate WAL before a given LSN
 *
 * @param ctx - Executor context
 * @param beforeLSN - LSN before which to truncate
 * @returns Truncate result
 */
export async function truncateWAL(
  ctx: ExecutorContext,
  beforeLSN: bigint
): Promise<TruncateResult> {
  const result: TruncateResult = {
    truncatedSegments: [],
    bytesFreed: 0,
    truncatedLSN: beforeLSN,
  };

  const segments = await ctx.reader.listSegments(false);

  for (const segmentId of segments) {
    const segment = await ctx.reader.readSegment(segmentId);
    if (!segment) continue;

    // Only delete segments whose endLSN is less than beforeLSN
    if (segment.endLSN < beforeLSN) {
      const path = `${ctx.config.segmentPrefix}${segmentId}`;
      const data = await ctx.backend.read(path);
      if (data) {
        result.bytesFreed += data.length;
      }
      await ctx.backend.delete(path);
      result.truncatedSegments.push(segmentId);
    }
  }

  return result;
}

// =============================================================================
// Compact WAL
// =============================================================================

/**
 * Compact WAL by removing rolled-back transaction entries
 *
 * @param ctx - Executor context
 * @returns Compact WAL result
 */
export async function compactWAL(ctx: ExecutorContext): Promise<CompactWALResult> {
  const result: CompactWALResult = {
    segmentsCompacted: 0,
    entriesRemoved: 0,
    bytesReclaimed: 0,
  };

  const segments = await ctx.reader.listSegments(false);

  // First pass: identify all rolled-back transaction IDs across all segments
  const rolledBackTxns = new Set<string>();
  for (const segmentId of segments) {
    const segment = await ctx.reader.readSegment(segmentId);
    if (!segment) continue;

    for (const entry of segment.entries) {
      if (entry.op === 'ROLLBACK') {
        rolledBackTxns.add(entry.txnId);
      }
    }
  }

  // Second pass: compact each segment by removing rolled-back entries
  for (const segmentId of segments) {
    const segment = await ctx.reader.readSegment(segmentId);
    if (!segment) continue;

    const path = `${ctx.config.segmentPrefix}${segmentId}`;
    const originalData = await ctx.backend.read(path);
    if (!originalData) continue;

    // Filter out entries belonging to rolled-back transactions
    const keptEntries = segment.entries.filter(
      (e) => !rolledBackTxns.has(e.txnId)
    );

    const removedCount = segment.entries.length - keptEntries.length;
    if (removedCount === 0) continue;

    // If all entries are removed, delete the segment
    if (keptEntries.length === 0) {
      await ctx.backend.delete(path);
      result.segmentsCompacted++;
      result.entriesRemoved += removedCount;
      result.bytesReclaimed += originalData.length;
      continue;
    }

    // Create compacted segment
    const lastKeptEntry = keptEntries[keptEntries.length - 1];
    if (!lastKeptEntry) continue;

    const compactedSegment: WALSegment = {
      ...segment,
      entries: keptEntries,
      endLSN: lastKeptEntry.lsn,
    };

    // Re-encode and write
    const encoder = new (await import('../writer.js')).DefaultWALEncoder();
    const newData = encoder.encodeSegment(compactedSegment);
    compactedSegment.checksum = encoder.calculateChecksum(newData);
    const finalData = encoder.encodeSegment(compactedSegment);

    await ctx.backend.write(path, finalData);

    result.segmentsCompacted++;
    result.entriesRemoved += removedCount;
    result.bytesReclaimed += originalData.length - finalData.length;
  }

  return result;
}

// =============================================================================
// Force Cleanup
// =============================================================================

/**
 * Force cleanup ignoring safety checks
 *
 * @param ctx - Executor context
 * @param options - Force cleanup options
 * @returns Force cleanup result
 */
export async function forceCleanup(
  ctx: ExecutorContext,
  options: ForceCleanupOptions = {}
): Promise<ForceCleanupResult> {
  const result: ForceCleanupResult = {
    deleted: [],
    bytesFreed: 0,
  };

  const segments = await ctx.reader.listSegments(false);

  for (const segmentId of segments) {
    const path = `${ctx.config.segmentPrefix}${segmentId}`;
    const data = await ctx.backend.read(path);

    if (data) {
      result.bytesFreed += data.length;
      await ctx.backend.delete(path);
      result.deleted.push(segmentId);
    }
  }

  return result;
}
