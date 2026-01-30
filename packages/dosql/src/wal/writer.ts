/**
 * WAL Writer for DoSQL
 *
 * Implements append-only write-ahead log with segment-based storage.
 * Provides fsync semantics via Durable Object storage guarantees.
 */

import type { FSXBackend } from '../fsx/types.js';
import {
  type WALEntry,
  type WALSegment,
  type WALConfig,
  type WALWriter,
  type WALEncoder,
  type AppendOptions,
  type AppendResult,
  type HLCTimestamp,
  DEFAULT_WAL_CONFIG,
  WALError,
  WALErrorCode,
} from './types.js';
import {
  createHLCClock,
  type HLCClock,
  type HLCConfig,
  type HLCReceiveOptions,
  type DriftMetrics,
  type HLCEventType,
  type HLCEventHandler,
} from '../hlc.js';
import { crc32 } from '../utils/crypto.js';
import { exactBase64Length } from '../utils/encoding.js';

// Re-export crc32 for backward compatibility
export { crc32 } from '../utils/crypto.js';

// =============================================================================
// WAL Encoder Implementation
// =============================================================================

/**
 * Default encoder for WAL entries and segments
 * Uses JSON for simplicity; can be replaced with binary format for production
 */
export class DefaultWALEncoder implements WALEncoder {
  private textEncoder = new TextEncoder();
  private textDecoder = new TextDecoder();

  encodeEntry(entry: WALEntry): Uint8Array {
    const obj = {
      lsn: entry.lsn.toString(),
      timestamp: entry.timestamp,
      txnId: entry.txnId,
      op: entry.op,
      table: entry.table,
      key: entry.key ? this.encodeBytes(entry.key) : undefined,
      before: entry.before ? this.encodeBytes(entry.before) : undefined,
      after: entry.after ? this.encodeBytes(entry.after) : undefined,
      hlc: entry.hlc ? {
        physicalTime: entry.hlc.physicalTime,
        logicalCounter: entry.hlc.logicalCounter,
        nodeId: entry.hlc.nodeId,
      } : undefined,
    };
    return this.textEncoder.encode(JSON.stringify(obj));
  }

  decodeEntry(data: Uint8Array): WALEntry {
    const json = this.textDecoder.decode(data);
    const obj = JSON.parse(json);
    return {
      lsn: BigInt(obj.lsn),
      timestamp: obj.timestamp,
      txnId: obj.txnId,
      op: obj.op,
      table: obj.table,
      key: obj.key ? this.decodeBytes(obj.key) : undefined,
      before: obj.before ? this.decodeBytes(obj.before) : undefined,
      after: obj.after ? this.decodeBytes(obj.after) : undefined,
      hlc: obj.hlc ? {
        physicalTime: obj.hlc.physicalTime,
        logicalCounter: obj.hlc.logicalCounter,
        nodeId: obj.hlc.nodeId,
      } : undefined,
    };
  }

  encodeSegment(segment: WALSegment): Uint8Array {
    const obj = {
      id: segment.id,
      startLSN: segment.startLSN.toString(),
      endLSN: segment.endLSN.toString(),
      entries: segment.entries.map((e) => ({
        lsn: e.lsn.toString(),
        timestamp: e.timestamp,
        txnId: e.txnId,
        op: e.op,
        table: e.table,
        key: e.key ? this.encodeBytes(e.key) : undefined,
        before: e.before ? this.encodeBytes(e.before) : undefined,
        after: e.after ? this.encodeBytes(e.after) : undefined,
        hlc: e.hlc ? {
          physicalTime: e.hlc.physicalTime,
          logicalCounter: e.hlc.logicalCounter,
          nodeId: e.hlc.nodeId,
        } : undefined,
      })),
      checksum: segment.checksum,
      createdAt: segment.createdAt,
      archived: segment.archived,
    };
    return this.textEncoder.encode(JSON.stringify(obj));
  }

  decodeSegment(data: Uint8Array): WALSegment {
    const json = this.textDecoder.decode(data);
    const obj = JSON.parse(json);
    return {
      id: obj.id,
      startLSN: BigInt(obj.startLSN),
      endLSN: BigInt(obj.endLSN),
      entries: obj.entries.map((e: any) => ({
        lsn: BigInt(e.lsn),
        timestamp: e.timestamp,
        txnId: e.txnId,
        op: e.op,
        table: e.table,
        key: e.key ? this.decodeBytes(e.key) : undefined,
        before: e.before ? this.decodeBytes(e.before) : undefined,
        after: e.after ? this.decodeBytes(e.after) : undefined,
        hlc: e.hlc ? {
          physicalTime: e.hlc.physicalTime,
          logicalCounter: e.hlc.logicalCounter,
          nodeId: e.hlc.nodeId,
        } : undefined,
      })),
      checksum: obj.checksum,
      createdAt: obj.createdAt,
      archived: obj.archived,
    };
  }

  calculateChecksum(data: Uint8Array): number {
    return crc32(data);
  }

  private encodeBytes(data: Uint8Array): string {
    // Base64 encode for JSON compatibility
    let binary = '';
    for (let i = 0; i < data.length; i++) {
      binary += String.fromCharCode(data[i]);
    }
    return btoa(binary);
  }

  private decodeBytes(base64: string): Uint8Array {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  }
}

// =============================================================================
// WAL Writer HLC Extension
// =============================================================================

/**
 * Extended WAL Writer interface with HLC support
 */
export interface WALWriterHLC {
  /**
   * Receive a remote HLC timestamp and update the local clock
   * @param hlc Remote HLC timestamp
   * @param options Optional HLC receive options
   */
  receiveHLC(hlc: HLCTimestamp, options?: HLCReceiveOptions): Promise<void>;

  /**
   * Register an event handler for HLC events
   * @param event Event type
   * @param callback Event handler
   */
  on(event: string, callback: (...args: unknown[]) => void): void;

  /**
   * Get drift metrics for monitoring
   */
  getDriftMetrics(): DriftMetrics;

  /**
   * Get the HLC clock instance
   */
  getHLCClock(): HLCClock;
}

// =============================================================================
// WAL Writer Implementation
// =============================================================================

/**
 * Configuration for HLC in WAL Writer
 */
export interface WALWriterHLCConfig {
  /** Node ID for HLC (default: auto-generated) */
  nodeId?: string;
  /** Maximum allowed drift from wall clock in milliseconds (default: 60000 = 1 minute) */
  maxDriftMs?: number;
  /** Warning threshold as fraction of maxDrift (default: 0.8 = 80%) */
  warningThreshold?: number;
}

/**
 * Creates a new WAL Writer with HLC support
 * @param backend FSX backend for storage
 * @param initialLSN Starting LSN (default: 0n)
 * @param config WAL configuration
 * @param encoder Custom encoder (optional)
 * @param hlcConfig HLC configuration (optional)
 */
export function createWALWriter(
  backend: FSXBackend,
  initialLSN: bigint = 0n,
  config: Partial<WALConfig> = {},
  encoder?: WALEncoder,
  hlcConfig?: WALWriterHLCConfig
): WALWriter & WALWriterHLC {
  const fullConfig: WALConfig = { ...DEFAULT_WAL_CONFIG, ...config };
  const walEncoder = encoder ?? new DefaultWALEncoder();

  // Generate a unique node ID if not provided
  const nodeId = hlcConfig?.nodeId ?? `node_${Date.now().toString(36)}_${Math.random().toString(36).substring(2, 8)}`;

  // Create HLC clock
  const hlcClock = createHLCClock({
    nodeId,
    maxDriftMs: hlcConfig?.maxDriftMs,
    warningThreshold: hlcConfig?.warningThreshold,
  });

  // Current state
  let currentLSN = initialLSN;
  let pendingEntries: WALEntry[] = [];
  let estimatedSize = 0;
  let closed = false;

  /**
   * Generate segment ID from start LSN
   */
  function generateSegmentId(startLSN: bigint): string {
    // Zero-padded LSN for lexicographic ordering
    return `seg_${startLSN.toString().padStart(20, '0')}`;
  }

  /**
   * Estimate entry size in bytes
   *
   * Uses exact Base64 length calculation: ceil(length / 3) * 4
   * This is the correct formula for Base64 expansion, not the approximate 1.34.
   * See: https://en.wikipedia.org/wiki/Base64#Output_padding
   */
  function estimateEntrySize(entry: WALEntry): number {
    let size = 100; // Base overhead for JSON structure
    size += entry.txnId.length;
    size += entry.table.length;
    // Use exact Base64 length calculation instead of approximate 1.34 multiplier
    if (entry.key) size += exactBase64Length(entry.key.length);
    if (entry.before) size += exactBase64Length(entry.before.length);
    if (entry.after) size += exactBase64Length(entry.after.length);
    return size; // No need for Math.ceil since exactBase64Length returns integers
  }

  /**
   * Build a segment from pending entries
   */
  function buildSegment(): WALSegment {
    if (pendingEntries.length === 0) {
      throw new WALError(
        WALErrorCode.FLUSH_FAILED,
        'No entries to flush'
      );
    }

    const startLSN = pendingEntries[0].lsn;
    const endLSN = pendingEntries[pendingEntries.length - 1].lsn;
    const segmentId = generateSegmentId(startLSN);
    // Capture timestamp once to ensure checksum consistency
    const createdAt = Date.now();

    // Calculate checksum over entries (without checksum field)
    const entriesData = walEncoder.encodeSegment({
      id: segmentId,
      startLSN,
      endLSN,
      entries: pendingEntries,
      checksum: 0, // Placeholder
      createdAt,
    });
    const checksum = walEncoder.calculateChecksum(entriesData);

    return {
      id: segmentId,
      startLSN,
      endLSN,
      entries: [...pendingEntries],
      checksum,
      createdAt,
    };
  }

  /**
   * Write segment to storage
   */
  async function writeSegment(segment: WALSegment): Promise<void> {
    const path = `${fullConfig.segmentPrefix}${segment.id}`;
    const data = walEncoder.encodeSegment(segment);

    try {
      await backend.write(path, data);
    } catch (error) {
      throw new WALError(
        WALErrorCode.FLUSH_FAILED,
        `Failed to write segment ${segment.id}`,
        segment.startLSN,
        segment.id,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Check if we should auto-flush based on size/count
   */
  function shouldFlush(): boolean {
    return (
      estimatedSize >= fullConfig.targetSegmentSize ||
      pendingEntries.length >= fullConfig.maxEntriesPerSegment
    );
  }

  // Public interface
  const writer: WALWriter & WALWriterHLC = {
    async append(
      entryWithoutLSN: Omit<WALEntry, 'lsn'>,
      options: AppendOptions = {}
    ): Promise<AppendResult> {
      if (closed) {
        throw new WALError(
          WALErrorCode.FLUSH_FAILED,
          'Writer has been closed'
        );
      }

      // Assign monotonically increasing LSN and HLC timestamp
      const entry: WALEntry = {
        ...entryWithoutLSN,
        lsn: currentLSN++,
        hlc: hlcClock.now(),
      };

      pendingEntries.push(entry);
      estimatedSize += estimateEntrySize(entry);

      let flushed = false;
      let segmentId: string | undefined;

      // Check if we need to flush
      if (shouldFlush() || options.sync) {
        const segment = await writer.flush();
        if (segment) {
          flushed = true;
          segmentId = segment.id;
        }
      }

      return {
        lsn: entry.lsn,
        flushed,
        segmentId,
      };
    },

    async flush(): Promise<WALSegment | null> {
      if (pendingEntries.length === 0) {
        return null;
      }

      const segment = buildSegment();
      await writeSegment(segment);

      // Reset state
      pendingEntries = [];
      estimatedSize = 0;

      return segment;
    },

    getCurrentLSN(): bigint {
      return currentLSN;
    },

    getPendingCount(): number {
      return pendingEntries.length;
    },

    getCurrentSegmentSize(): number {
      return estimatedSize;
    },

    async close(): Promise<void> {
      if (closed) return;

      // Flush any remaining entries
      if (pendingEntries.length > 0) {
        await writer.flush();
      }

      closed = true;
    },

    // HLC extension methods
    async receiveHLC(hlc: HLCTimestamp, options?: HLCReceiveOptions): Promise<void> {
      await hlcClock.receive(hlc, options);
    },

    on(event: string, callback: (...args: unknown[]) => void): void {
      if (event === 'drift-warning') {
        hlcClock.on('drift-warning', callback as HLCEventHandler);
      }
    },

    getDriftMetrics(): DriftMetrics {
      return hlcClock.getDriftMetrics();
    },

    getHLCClock(): HLCClock {
      return hlcClock;
    },
  };

  return writer;
}

// =============================================================================
// Transaction Helper
// =============================================================================

/**
 * Helper for creating transaction-scoped WAL entries
 */
export class WALTransaction {
  private entries: Array<Omit<WALEntry, 'lsn' | 'txnId' | 'timestamp'>> = [];
  private committed = false;
  private rolledBack = false;

  constructor(
    public readonly txnId: string,
    private readonly writer: WALWriter
  ) {}

  /**
   * Add an INSERT entry
   */
  insert(table: string, value: Uint8Array): this {
    this.assertActive();
    this.entries.push({
      op: 'INSERT',
      table,
      after: value,
    });
    return this;
  }

  /**
   * Add an UPDATE entry
   */
  update(
    table: string,
    key: Uint8Array,
    before: Uint8Array,
    after: Uint8Array
  ): this {
    this.assertActive();
    this.entries.push({
      op: 'UPDATE',
      table,
      key,
      before,
      after,
    });
    return this;
  }

  /**
   * Add a DELETE entry
   */
  delete(table: string, key: Uint8Array, before: Uint8Array): this {
    this.assertActive();
    this.entries.push({
      op: 'DELETE',
      table,
      key,
      before,
    });
    return this;
  }

  /**
   * Commit the transaction to the WAL
   */
  async commit(): Promise<bigint[]> {
    this.assertActive();

    const timestamp = Date.now();
    const lsns: bigint[] = [];

    // Write BEGIN
    const beginResult = await this.writer.append({
      timestamp,
      txnId: this.txnId,
      op: 'BEGIN',
      table: '',
    });
    lsns.push(beginResult.lsn);

    // Write all entries
    for (const entry of this.entries) {
      const result = await this.writer.append({
        ...entry,
        timestamp,
        txnId: this.txnId,
      });
      lsns.push(result.lsn);
    }

    // Write COMMIT
    const commitResult = await this.writer.append(
      {
        timestamp,
        txnId: this.txnId,
        op: 'COMMIT',
        table: '',
      },
      { sync: true } // Ensure commit is durable
    );
    lsns.push(commitResult.lsn);

    this.committed = true;
    return lsns;
  }

  /**
   * Rollback the transaction (no WAL entries written if not committed)
   */
  async rollback(): Promise<void> {
    if (this.committed) {
      // If already committed, write a ROLLBACK entry
      await this.writer.append(
        {
          timestamp: Date.now(),
          txnId: this.txnId,
          op: 'ROLLBACK',
          table: '',
        },
        { sync: true }
      );
    }
    this.rolledBack = true;
  }

  /**
   * Check if transaction is active
   */
  isActive(): boolean {
    return !this.committed && !this.rolledBack;
  }

  private assertActive(): void {
    if (this.committed) {
      throw new Error(`Transaction ${this.txnId} already committed`);
    }
    if (this.rolledBack) {
      throw new Error(`Transaction ${this.txnId} already rolled back`);
    }
  }
}

/**
 * Generate a unique transaction ID
 */
export function generateTxnId(): string {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substring(2, 10);
  return `txn_${timestamp}_${random}`;
}

/**
 * Create a new transaction
 *
 * @param writer - The WAL writer instance to use for recording transaction entries
 * @returns A new WALTransaction instance with an auto-generated transaction ID
 */
export function createTransaction(writer: WALWriter): WALTransaction {
  return new WALTransaction(generateTxnId(), writer);
}

