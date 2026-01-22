/**
 * Hybrid Logical Clock (HLC) Implementation for DoSQL
 *
 * Provides causal ordering guarantees for CDC events across distributed shards.
 * HLC combines physical wall-clock time with a logical counter to ensure:
 * - Monotonically increasing timestamps
 * - Causal ordering guarantees
 * - Bounded clock drift handling
 * - Globally consistent event ordering
 *
 * @see https://cse.buffalo.edu/tech-reports/2014-04.pdf - HLC Paper
 *
 * @packageDocumentation
 */

// =============================================================================
// HLC Types
// =============================================================================

/**
 * Hybrid Logical Clock timestamp
 * Combines wall-clock time with a logical counter for causal ordering
 */
export interface HLCTimestamp {
  /** Physical wall-clock time in milliseconds */
  physicalTime: number;
  /** Logical counter for ordering events with same physical time */
  logicalCounter: number;
  /** Node/shard identifier for tie-breaking */
  nodeId: string;
}

/**
 * HLC Clock configuration
 */
export interface HLCConfig {
  /** Node/shard identifier */
  nodeId: string;
  /** Maximum allowed drift from wall clock in milliseconds (default: 60000 = 1 minute) */
  maxDriftMs?: number;
  /** Warning threshold as fraction of maxDrift (default: 0.8 = 80%) */
  warningThreshold?: number;
  /** Custom wall clock function for testing */
  wallClock?: () => number;
}

/**
 * Drift recovery strategy
 */
export type DriftStrategy = 'reject' | 'log-and-accept' | 'wait-for-sync';

/**
 * HLC options for receiving remote timestamps
 */
export interface HLCReceiveOptions {
  /** Override max drift for this operation */
  maxDriftMs?: number;
  /** Warning threshold for this operation */
  warningThreshold?: number;
  /** Drift recovery strategy */
  driftStrategy?: DriftStrategy;
  /** Timeout for wait-for-sync strategy */
  syncTimeoutMs?: number;
}

/**
 * Drift metrics for monitoring
 */
export interface DriftMetrics {
  /** Maximum drift observed */
  maxDrift: number;
  /** Average drift observed */
  avgDrift: number;
  /** Total number of drift observations */
  driftCount: number;
  /** Sum of all drifts (for average calculation) */
  totalDrift: number;
}

/**
 * Drift warning event
 */
export interface DriftWarning {
  /** Observed drift in milliseconds */
  drift: number;
  /** Timestamp when warning was generated */
  timestamp: number;
  /** Remote HLC that caused the warning */
  remoteHLC: HLCTimestamp;
}

/**
 * HLC event types for event emitter
 */
export type HLCEventType = 'drift-warning';
export type HLCEventHandler = (warning: DriftWarning) => void;

// =============================================================================
// HLC Errors
// =============================================================================

/**
 * HLC-specific error codes
 */
export enum HLCErrorCode {
  /** Clock drift exceeded maximum allowed */
  EXCESSIVE_DRIFT = 'HLC_EXCESSIVE_DRIFT',
  /** Invalid HLC timestamp */
  INVALID_TIMESTAMP = 'HLC_INVALID_TIMESTAMP',
  /** Sync timeout exceeded */
  SYNC_TIMEOUT = 'HLC_SYNC_TIMEOUT',
}

/**
 * Custom error class for HLC operations
 */
export class HLCError extends Error {
  constructor(
    public readonly code: HLCErrorCode,
    message: string,
    public readonly drift?: number,
    public readonly remoteHLC?: HLCTimestamp
  ) {
    super(message);
    this.name = 'HLCError';
  }
}

// =============================================================================
// HLC Clock Implementation
// =============================================================================

/**
 * HLC Clock interface for generating and comparing timestamps
 */
export interface HLCClock {
  /** Get current HLC timestamp (advances the clock) */
  now(): HLCTimestamp;
  /** Update clock on receiving a message with higher timestamp */
  receive(remote: HLCTimestamp, options?: HLCReceiveOptions): Promise<HLCTimestamp>;
  /** Compare two HLC timestamps (-1, 0, 1) */
  compare(a: HLCTimestamp, b: HLCTimestamp): number;
  /** Get maximum allowed drift from wall clock */
  getMaxDrift(): number;
  /** Check if timestamp is within acceptable drift */
  isValidDrift(timestamp: HLCTimestamp): boolean;
  /** Get current node ID */
  getNodeId(): string;
  /** Get drift metrics */
  getDriftMetrics(): DriftMetrics;
  /** Register event handler */
  on(event: HLCEventType, handler: HLCEventHandler): void;
  /** Remove event handler */
  off(event: HLCEventType, handler: HLCEventHandler): void;
}

/**
 * Creates a new HLC Clock
 */
export function createHLCClock(config: HLCConfig): HLCClock {
  const {
    nodeId,
    maxDriftMs = 60000, // Default 1 minute max drift
    warningThreshold = 0.8, // Warn at 80% of max drift
    wallClock = () => Date.now(),
  } = config;

  // Current clock state
  let lastPhysicalTime = 0;
  let lastLogicalCounter = 0;

  // Drift metrics
  const driftMetrics: DriftMetrics = {
    maxDrift: 0,
    avgDrift: 0,
    driftCount: 0,
    totalDrift: 0,
  };

  // Event handlers
  const eventHandlers: Map<HLCEventType, Set<HLCEventHandler>> = new Map();

  /**
   * Emit an event to registered handlers
   */
  function emit(event: HLCEventType, data: DriftWarning): void {
    const handlers = eventHandlers.get(event);
    if (handlers) {
      for (const handler of handlers) {
        try {
          handler(data);
        } catch {
          // Ignore handler errors
        }
      }
    }
  }

  /**
   * Record drift observation for metrics
   */
  function recordDrift(drift: number): void {
    driftMetrics.driftCount++;
    driftMetrics.totalDrift += drift;
    driftMetrics.avgDrift = driftMetrics.totalDrift / driftMetrics.driftCount;
    if (drift > driftMetrics.maxDrift) {
      driftMetrics.maxDrift = drift;
    }
  }

  const clock: HLCClock = {
    now(): HLCTimestamp {
      const currentWallTime = wallClock();

      if (currentWallTime > lastPhysicalTime) {
        // Wall clock has advanced, use it and reset counter
        lastPhysicalTime = currentWallTime;
        lastLogicalCounter = 0;
      } else {
        // Wall clock hasn't advanced (or went backwards)
        // Increment logical counter
        lastLogicalCounter++;
      }

      return {
        physicalTime: lastPhysicalTime,
        logicalCounter: lastLogicalCounter,
        nodeId,
      };
    },

    async receive(remote: HLCTimestamp, options: HLCReceiveOptions = {}): Promise<HLCTimestamp> {
      const {
        maxDriftMs: optMaxDrift = maxDriftMs,
        warningThreshold: optWarningThreshold = warningThreshold,
        driftStrategy = 'reject',
        syncTimeoutMs = 5000,
      } = options;

      const currentWallTime = wallClock();
      const drift = remote.physicalTime - currentWallTime;

      // Record drift for metrics
      if (drift > 0) {
        recordDrift(drift);
      }

      // Check for excessive drift
      if (drift > optMaxDrift) {
        if (driftStrategy === 'reject') {
          throw new HLCError(
            HLCErrorCode.EXCESSIVE_DRIFT,
            `Clock drift ${drift}ms exceeds maximum allowed ${optMaxDrift}ms`,
            drift,
            remote
          );
        } else if (driftStrategy === 'wait-for-sync') {
          // Wait for wall clock to catch up
          const startTime = Date.now();
          while (wallClock() < remote.physicalTime) {
            if (Date.now() - startTime > syncTimeoutMs) {
              throw new HLCError(
                HLCErrorCode.SYNC_TIMEOUT,
                `Timeout waiting for clock sync after ${syncTimeoutMs}ms`,
                drift,
                remote
              );
            }
            await new Promise((resolve) => setTimeout(resolve, 10));
          }
        }
        // 'log-and-accept' - continue with the remote timestamp
      }

      // Check for warning threshold
      if (drift > optMaxDrift * optWarningThreshold) {
        emit('drift-warning', {
          drift,
          timestamp: Date.now(),
          remoteHLC: remote,
        });
      }

      // HLC receive algorithm
      const maxPhysical = Math.max(currentWallTime, lastPhysicalTime, remote.physicalTime);

      if (maxPhysical === lastPhysicalTime && maxPhysical === remote.physicalTime) {
        // All three equal, take max of counters + 1
        lastLogicalCounter = Math.max(lastLogicalCounter, remote.logicalCounter) + 1;
      } else if (maxPhysical === lastPhysicalTime) {
        // Our physical time is max, increment our counter
        lastLogicalCounter = lastLogicalCounter + 1;
      } else if (maxPhysical === remote.physicalTime) {
        // Remote physical time is max, use remote counter + 1
        lastLogicalCounter = remote.logicalCounter + 1;
      } else {
        // Wall clock is max, reset counter
        lastLogicalCounter = 0;
      }

      lastPhysicalTime = maxPhysical;

      return {
        physicalTime: lastPhysicalTime,
        logicalCounter: lastLogicalCounter,
        nodeId,
      };
    },

    compare(a: HLCTimestamp, b: HLCTimestamp): number {
      return compareHLC(a, b);
    },

    getMaxDrift(): number {
      return maxDriftMs;
    },

    isValidDrift(timestamp: HLCTimestamp): boolean {
      const currentWallTime = wallClock();
      const drift = timestamp.physicalTime - currentWallTime;
      return drift <= maxDriftMs;
    },

    getNodeId(): string {
      return nodeId;
    },

    getDriftMetrics(): DriftMetrics {
      return { ...driftMetrics };
    },

    on(event: HLCEventType, handler: HLCEventHandler): void {
      if (!eventHandlers.has(event)) {
        eventHandlers.set(event, new Set());
      }
      eventHandlers.get(event)!.add(handler);
    },

    off(event: HLCEventType, handler: HLCEventHandler): void {
      const handlers = eventHandlers.get(event);
      if (handlers) {
        handlers.delete(handler);
      }
    },
  };

  return clock;
}

// =============================================================================
// HLC Comparison Utilities
// =============================================================================

/**
 * Compare two HLC timestamps
 * Returns: -1 if a < b, 0 if a == b, 1 if a > b
 */
export function compareHLC(a: HLCTimestamp, b: HLCTimestamp): number {
  // First compare physical time
  if (a.physicalTime < b.physicalTime) return -1;
  if (a.physicalTime > b.physicalTime) return 1;

  // Physical times equal, compare logical counter
  if (a.logicalCounter < b.logicalCounter) return -1;
  if (a.logicalCounter > b.logicalCounter) return 1;

  // Both equal, use node ID as tiebreaker
  if (a.nodeId < b.nodeId) return -1;
  if (a.nodeId > b.nodeId) return 1;

  return 0;
}

/**
 * Check if HLC a is less than HLC b
 */
export function isHLCBefore(a: HLCTimestamp, b: HLCTimestamp): boolean {
  return compareHLC(a, b) < 0;
}

/**
 * Check if HLC a is greater than HLC b
 */
export function isHLCAfter(a: HLCTimestamp, b: HLCTimestamp): boolean {
  return compareHLC(a, b) > 0;
}

/**
 * Check if two HLCs are equal
 */
export function isHLCEqual(a: HLCTimestamp, b: HLCTimestamp): boolean {
  return compareHLC(a, b) === 0;
}

/**
 * Get the maximum (latest) of two HLC timestamps
 */
export function maxHLC(a: HLCTimestamp, b: HLCTimestamp): HLCTimestamp {
  return compareHLC(a, b) >= 0 ? a : b;
}

/**
 * Get the minimum (earliest) of two HLC timestamps
 */
export function minHLC(a: HLCTimestamp, b: HLCTimestamp): HLCTimestamp {
  return compareHLC(a, b) <= 0 ? a : b;
}

// =============================================================================
// HLC Serialization
// =============================================================================

/**
 * Serialize an HLC timestamp to a string representation
 * Format: "physicalTime:logicalCounter@nodeId"
 */
export function serializeHLC(hlc: HLCTimestamp): string {
  return `${hlc.physicalTime}:${hlc.logicalCounter}@${hlc.nodeId}`;
}

/**
 * Parse an HLC timestamp from a string representation
 */
export function parseHLC(str: string): HLCTimestamp {
  const match = str.match(/^(\d+):(\d+)@(.+)$/);
  if (!match) {
    throw new HLCError(
      HLCErrorCode.INVALID_TIMESTAMP,
      `Invalid HLC format: ${str}`
    );
  }
  return {
    physicalTime: parseInt(match[1], 10),
    logicalCounter: parseInt(match[2], 10),
    nodeId: match[3],
  };
}

/**
 * Serialize an HLC timestamp to a JSON-compatible object
 */
export function hlcToJSON(hlc: HLCTimestamp): {
  physicalTime: number;
  logicalCounter: number;
  nodeId: string;
} {
  return {
    physicalTime: hlc.physicalTime,
    logicalCounter: hlc.logicalCounter,
    nodeId: hlc.nodeId,
  };
}

/**
 * Create an HLC timestamp from a JSON-compatible object
 */
export function hlcFromJSON(obj: {
  physicalTime: number;
  logicalCounter: number;
  nodeId: string;
}): HLCTimestamp {
  return {
    physicalTime: obj.physicalTime,
    logicalCounter: obj.logicalCounter,
    nodeId: obj.nodeId,
  };
}

/**
 * Format HLC for debugging/logging
 */
export function formatHLC(hlc: HLCTimestamp): string {
  return `HLC(${hlc.physicalTime}:${hlc.logicalCounter}@${hlc.nodeId})`;
}

// =============================================================================
// HLC Sorting Utilities
// =============================================================================

/**
 * Sort an array of items by their HLC timestamp
 * @param items Array of items
 * @param getHLC Function to extract HLC from each item
 * @returns Sorted array (ascending by HLC)
 */
export function sortByHLC<T>(items: T[], getHLC: (item: T) => HLCTimestamp): T[] {
  return [...items].sort((a, b) => compareHLC(getHLC(a), getHLC(b)));
}

/**
 * Sort an array of HLC timestamps in ascending order
 */
export function sortHLCs(hlcs: HLCTimestamp[]): HLCTimestamp[] {
  return [...hlcs].sort(compareHLC);
}

// =============================================================================
// Global HLC Instance (for convenience)
// =============================================================================

let globalHLCClock: HLCClock | null = null;

/**
 * Initialize the global HLC clock
 * Should be called once at startup with the node/shard identifier
 */
export function initGlobalHLC(config: HLCConfig): HLCClock {
  globalHLCClock = createHLCClock(config);
  return globalHLCClock;
}

/**
 * Get the global HLC clock
 * @throws Error if global clock not initialized
 */
export function getGlobalHLC(): HLCClock {
  if (!globalHLCClock) {
    throw new Error('Global HLC clock not initialized. Call initGlobalHLC() first.');
  }
  return globalHLCClock;
}

/**
 * Check if global HLC clock is initialized
 */
export function hasGlobalHLC(): boolean {
  return globalHLCClock !== null;
}

/**
 * Reset the global HLC clock (for testing)
 */
export function resetGlobalHLC(): void {
  globalHLCClock = null;
}
