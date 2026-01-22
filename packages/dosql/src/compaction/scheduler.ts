/**
 * Compaction Scheduler for DoSQL
 *
 * Manages background compaction scheduling with:
 * - DO alarm-based scheduling
 * - Adaptive scheduling based on write rate
 * - Pause during high load
 */

import type { BTree, KeyCodec, ValueCodec } from '../btree/types.js';
import type { FSXBackend } from '../fsx/types.js';
import type { ColumnarTableSchema, RowGroup } from '../columnar/types.js';
import type {
  CompactionConfig,
  CompactionJob,
  CompactionJobStatus,
  CompactionSummary,
  CompactionManifest,
  SchedulerState,
  SchedulerStatus,
  WriteRateStats,
  Compactor,
  CompactorDeps,
  CompactionCandidate,
} from './types.js';
import { DEFAULT_COMPACTION_CONFIG, CompactionError, CompactionErrorCode } from './types.js';
import { CompactionScanner, createScanner, collectBatch } from './scanner.js';
import { RowToColumnarConverter, createConverter, writeChunks } from './converter.js';
import { CompactionCleaner, createCleaner, loadManifest } from './cleaner.js';

// =============================================================================
// Job ID Generation
// =============================================================================

/**
 * Generate a unique job ID
 */
function generateJobId(): string {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substring(2, 8);
  return `cj_${timestamp}_${random}`;
}

// =============================================================================
// Write Rate Tracker
// =============================================================================

/**
 * Tracks write rate for adaptive scheduling
 */
class WriteRateTracker {
  private writes: number[] = [];
  private windowMs: number;
  private peakRate = 0;

  constructor(windowMs: number = 60_000) {
    this.windowMs = windowMs;
  }

  /**
   * Record a write operation
   */
  recordWrite(): void {
    const now = Date.now();
    this.writes.push(now);
    this.cleanup(now);
  }

  /**
   * Get current write rate statistics
   */
  getStats(): WriteRateStats {
    const now = Date.now();
    this.cleanup(now);

    // Calculate writes in last second
    const lastSecond = now - 1000;
    const recentWrites = this.writes.filter((t) => t >= lastSecond).length;

    // Calculate average rate over window
    const windowStart = now - this.windowMs;
    const windowWrites = this.writes.filter((t) => t >= windowStart).length;
    const windowSeconds = this.windowMs / 1000;
    const averageRate = windowWrites / windowSeconds;

    // Update peak
    if (recentWrites > this.peakRate) {
      this.peakRate = recentWrites;
    }

    return {
      currentRate: recentWrites,
      averageRate,
      peakRate: this.peakRate,
      lastMeasuredAt: now,
    };
  }

  /**
   * Clean up old write timestamps
   */
  private cleanup(now: number): void {
    const cutoff = now - this.windowMs;
    this.writes = this.writes.filter((t) => t >= cutoff);
  }

  /**
   * Reset the tracker
   */
  reset(): void {
    this.writes = [];
    this.peakRate = 0;
  }
}

// =============================================================================
// Scheduler Implementation
// =============================================================================

/**
 * Full compactor implementation with scheduling
 */
export class CompactorImpl<K, V> implements Compactor {
  private readonly btree: BTree<K, V>;
  private readonly fsx: FSXBackend;
  private readonly keyCodec: KeyCodec<K>;
  private readonly valueCodec: ValueCodec<V>;
  private readonly schema: ColumnarTableSchema;
  private readonly config: CompactionConfig;

  private readonly scanner: CompactionScanner<K, V>;
  private readonly converter: RowToColumnarConverter<K, V>;
  private readonly cleaner: CompactionCleaner<K>;
  private readonly writeRateTracker: WriteRateTracker;

  private state: SchedulerState = 'idle';
  private currentJob: CompactionJob | null = null;
  private jobHistory: CompactionJob[] = [];
  private nextRunAt?: number;
  private setAlarmFn?: (time: number) => Promise<void>;

  /** Interval ID for continuous mode */
  private continuousInterval?: ReturnType<typeof setInterval>;

  /** Flag to stop current job */
  private stopRequested = false;

  constructor(deps: CompactorDeps<K, V>, config: Partial<CompactionConfig> = {}) {
    this.btree = deps.btree;
    this.fsx = deps.fsx;
    this.keyCodec = deps.keyCodec;
    this.valueCodec = deps.valueCodec;
    this.schema = deps.schema;
    this.config = { ...DEFAULT_COMPACTION_CONFIG, ...config };
    this.setAlarmFn = deps.setAlarm;

    // Create components
    this.scanner = createScanner(
      this.btree,
      this.fsx,
      this.keyCodec,
      this.valueCodec,
      this.config
    );
    this.converter = createConverter<K, V>(this.schema, this.fsx, this.config);
    this.cleaner = createCleaner(
      this.btree,
      this.fsx,
      this.keyCodec,
      this.config,
      this.schema
    );

    this.writeRateTracker = new WriteRateTracker();

    // Wire up write tracking
    if (deps.onWrite) {
      const originalOnWrite = deps.onWrite;
      deps.onWrite = () => {
        originalOnWrite();
        this.writeRateTracker.recordWrite();
      };
    }
  }

  // ===========================================================================
  // Compactor Interface Implementation
  // ===========================================================================

  /**
   * Run a single compaction cycle
   */
  async runOnce(): Promise<CompactionJob> {
    if (this.state === 'running') {
      throw new CompactionError(
        CompactionErrorCode.SCHEDULER_ERROR,
        'Compaction already in progress',
        this.currentJob?.id
      );
    }

    const job = this.createJob();
    this.currentJob = job;
    this.state = 'running';

    try {
      await this.executeJob(job);
    } finally {
      this.state = 'idle';
      this.currentJob = null;
      this.jobHistory.push(job);
    }

    return job;
  }

  /**
   * Start background compaction
   */
  start(): void {
    if (this.state === 'running' || this.state === 'paused') {
      return;
    }

    this.stopRequested = false;

    switch (this.config.schedule) {
      case 'continuous':
        this.startContinuous();
        break;

      case 'scheduled':
        this.scheduleNext();
        break;

      case 'manual':
        // No-op for manual mode
        break;
    }
  }

  /**
   * Stop background compaction
   */
  async stop(): Promise<void> {
    this.stopRequested = true;

    // Stop continuous mode interval
    if (this.continuousInterval) {
      clearInterval(this.continuousInterval);
      this.continuousInterval = undefined;
    }

    // Wait for current job to complete
    if (this.state === 'running' && this.currentJob) {
      // Give it time to see the stop flag
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    this.state = 'stopped';
  }

  /**
   * Pause background compaction
   */
  pause(): void {
    if (this.state === 'running' && this.currentJob) {
      this.currentJob.status = 'cancelled';
    }

    if (this.continuousInterval) {
      clearInterval(this.continuousInterval);
      this.continuousInterval = undefined;
    }

    this.state = 'paused';
  }

  /**
   * Resume paused compaction
   */
  resume(): void {
    if (this.state !== 'paused') return;

    this.state = 'idle';
    this.start();
  }

  /**
   * Get current scheduler status
   */
  getStatus(): SchedulerStatus {
    const writeRate = this.writeRateTracker.getStats();
    const { needed, reasons } = this.checkShouldCompactSync();

    return {
      state: this.state,
      nextRunAt: this.nextRunAt,
      currentJob: this.currentJob ?? undefined,
      writeRate,
      compactionNeeded: needed,
      reasons,
    };
  }

  /**
   * Get compaction summary statistics
   */
  async getSummary(): Promise<CompactionSummary> {
    const successful = this.jobHistory.filter((j) => j.status === 'completed');
    const failed = this.jobHistory.filter((j) => j.status === 'failed');

    const durations = successful
      .filter((j) => j.startedAt && j.completedAt)
      .map((j) => j.completedAt! - j.startedAt!);

    const averageDuration =
      durations.length > 0
        ? durations.reduce((a, b) => a + b, 0) / durations.length
        : 0;

    return {
      totalJobs: this.jobHistory.length,
      successfulJobs: successful.length,
      failedJobs: failed.length,
      totalRowsCompacted: this.jobHistory.reduce(
        (sum, j) => sum + j.rowsCompacted,
        0
      ),
      totalChunksCreated: this.jobHistory.reduce(
        (sum, j) => sum + j.chunksCreated,
        0
      ),
      totalBytesWritten: this.jobHistory.reduce(
        (sum, j) => sum + j.bytesWritten,
        0
      ),
      lastCompactionAt: this.jobHistory.length > 0
        ? this.jobHistory[this.jobHistory.length - 1].completedAt
        : undefined,
      lastError: failed.length > 0 ? failed[failed.length - 1].error : undefined,
      averageDuration,
    };
  }

  /**
   * Get the current manifest
   */
  async getManifest(): Promise<CompactionManifest | null> {
    return loadManifest(this.fsx, this.config, this.schema.tableName);
  }

  /**
   * Check if compaction should run based on thresholds
   */
  async shouldCompact(): Promise<{ needed: boolean; reasons: string[] }> {
    const thresholds = await this.scanner.checkThresholds();
    const reasons: string[] = [];

    if (thresholds.rowThresholdMet) {
      reasons.push(
        `Row count (${thresholds.rowCount}) exceeds threshold (${this.config.rowThreshold})`
      );
    }

    if (thresholds.ageThresholdMet) {
      reasons.push(
        `${thresholds.eligibleCount} rows older than ${this.config.ageThreshold}ms`
      );
    }

    if (thresholds.sizeThresholdMet) {
      reasons.push(
        `Size (${thresholds.totalSize} bytes) exceeds threshold (${this.config.sizeThreshold} bytes)`
      );
    }

    // Check high load pause
    if (this.config.pauseDuringHighLoad) {
      const writeRate = this.writeRateTracker.getStats();
      if (writeRate.currentRate > this.config.highLoadThreshold) {
        return {
          needed: false,
          reasons: [
            `High write load (${writeRate.currentRate} writes/sec > ${this.config.highLoadThreshold})`,
          ],
        };
      }
    }

    return {
      needed: reasons.length > 0,
      reasons,
    };
  }

  /**
   * Force a checkpoint
   */
  async checkpoint(): Promise<void> {
    // This would typically call into the WAL checkpointing system
    // For now, just ensure manifest is persisted
    const manifest = await this.getManifest();
    if (manifest) {
      // Manifest is already persisted, nothing to do
    }
  }

  /**
   * Set DO alarm for next scheduled run
   */
  async setAlarm(alarmTime: number): Promise<void> {
    this.nextRunAt = alarmTime;
    if (this.setAlarmFn) {
      await this.setAlarmFn(alarmTime);
    }
  }

  /**
   * Handle DO alarm callback
   */
  async handleAlarm(): Promise<void> {
    this.nextRunAt = undefined;

    if (this.state === 'stopped' || this.state === 'paused') {
      return;
    }

    try {
      const { needed } = await this.shouldCompact();
      if (needed) {
        await this.runOnce();
      }
    } finally {
      // Schedule next run
      if (this.config.schedule === 'scheduled' && !this.stopRequested) {
        this.scheduleNext();
      }
    }
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Synchronous check for should compact (uses cached stats)
   */
  private checkShouldCompactSync(): { needed: boolean; reasons: string[] } {
    // This is a simplified sync check - the async version is more accurate
    const reasons: string[] = [];

    if (this.config.pauseDuringHighLoad) {
      const writeRate = this.writeRateTracker.getStats();
      if (writeRate.currentRate > this.config.highLoadThreshold) {
        return {
          needed: false,
          reasons: [
            `High write load (${writeRate.currentRate} writes/sec)`,
          ],
        };
      }
    }

    // Default to unknown - need async check
    reasons.push('Check thresholds for details');
    return { needed: true, reasons };
  }

  /**
   * Create a new compaction job
   */
  private createJob(): CompactionJob {
    return {
      id: generateJobId(),
      status: 'pending',
      createdAt: Date.now(),
      rowsScanned: 0,
      rowsCompacted: 0,
      chunksCreated: 0,
      bytesWritten: 0,
      rowsDeleted: 0,
      progress: 0,
      rowGroupIds: [],
    };
  }

  /**
   * Execute a compaction job
   */
  private async executeJob(job: CompactionJob): Promise<void> {
    try {
      job.startedAt = Date.now();

      // Phase 1: Scan for candidates
      job.status = 'scanning';
      const { candidates, totalSize, lsnRange } = await collectBatch(
        this.scanner,
        this.config.maxRowsPerRun,
        this.config.sizeThreshold,
      );

      if (this.stopRequested || candidates.length === 0) {
        job.status = 'completed';
        job.completedAt = Date.now();
        job.progress = 100;
        return;
      }

      job.rowsScanned = candidates.length;
      job.lsnRange = lsnRange;
      job.progress = 25;

      // Phase 2: Convert to columnar
      job.status = 'converting';
      this.converter.clearBuffer();
      this.converter.bufferRows(candidates);

      const conversionResult = await this.converter.convert();

      if (this.stopRequested) {
        job.status = 'cancelled';
        job.completedAt = Date.now();
        return;
      }

      job.rowsCompacted = conversionResult.rowCount;
      job.chunksCreated = conversionResult.rowGroups.length;
      job.progress = 50;

      // Phase 3: Write chunks to storage
      job.status = 'writing';
      const storagePaths = await writeChunks(
        conversionResult.rowGroups,
        conversionResult.serialized,
        this.fsx,
        this.config,
        this.schema.tableName
      );

      if (this.stopRequested) {
        job.status = 'cancelled';
        job.completedAt = Date.now();
        return;
      }

      job.bytesWritten = conversionResult.byteSize;
      job.rowGroupIds = conversionResult.rowGroups.map((rg) => rg.id);
      job.progress = 75;

      // Phase 4: Clean up B-tree
      job.status = 'cleaning';
      const keys = candidates.map((c) => c.key);

      const cleanupResult = await this.cleaner.cleanup(
        keys,
        conversionResult.rowGroups,
        storagePaths,
        job.id,
        lsnRange
      );

      job.rowsDeleted = cleanupResult.deletedRows;
      job.status = 'completed';
      job.completedAt = Date.now();
      job.progress = 100;

    } catch (error) {
      job.status = 'failed';
      job.completedAt = Date.now();
      job.error = error instanceof Error ? error.message : 'Unknown error';
      throw error;
    }
  }

  /**
   * Start continuous mode
   */
  private startContinuous(): void {
    const checkAndRun = async () => {
      if (this.state === 'running' || this.stopRequested) {
        return;
      }

      try {
        const { needed } = await this.shouldCompact();
        if (needed) {
          await this.runOnce();
        }
      } catch (error) {
        // Log error but continue
        console.error('Compaction error:', error);
      }
    };

    // Check immediately and then on interval
    checkAndRun();

    this.continuousInterval = setInterval(
      checkAndRun,
      this.config.minCompactionInterval
    );
  }

  /**
   * Schedule next compaction run
   */
  private scheduleNext(): void {
    const nextTime = Date.now() + this.config.minCompactionInterval;
    this.setAlarm(nextTime);
  }

  // ===========================================================================
  // Track Writes (for external integration)
  // ===========================================================================

  /**
   * Record a write for rate tracking
   */
  recordWrite(): void {
    this.writeRateTracker.recordWrite();
  }

  /**
   * Track a row for compaction metadata
   */
  async trackRow(key: K, value: V, lsn?: bigint): Promise<void> {
    await this.scanner.trackRow(key, value, lsn);
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a compactor instance
 */
export function createCompactor<K, V>(
  deps: CompactorDeps<K, V>,
  config?: Partial<CompactionConfig>
): CompactorImpl<K, V> {
  return new CompactorImpl(deps, config);
}

// =============================================================================
// DO Alarm Handler
// =============================================================================

/**
 * Create an alarm handler for a Durable Object
 */
export function createAlarmHandler<K, V>(
  compactor: CompactorImpl<K, V>
): () => Promise<void> {
  return () => compactor.handleAlarm();
}
