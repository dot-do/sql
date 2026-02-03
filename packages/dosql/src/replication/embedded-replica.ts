/**
 * Embedded Replica Sync for DoSQL
 *
 * Turso-style embedded replica synchronization that allows:
 * - Local database file as embedded replica
 * - Background sync to remote primary
 * - Read-your-writes semantics
 * - Manual and periodic sync modes
 * - Offline writes with later sync
 *
 * @example
 * ```typescript
 * import { createEmbeddedReplica } from 'dosql/replication';
 *
 * // Create embedded replica with manual sync
 * const replica = createEmbeddedReplica({
 *   localPath: './local.db',
 *   syncUrl: 'https://primary.do/db/main',
 *   authToken: 'your-token',
 * });
 *
 * // Manual sync
 * await replica.sync();
 *
 * // Or with periodic sync
 * const replicaWithPeriodicSync = createEmbeddedReplica({
 *   localPath: './local.db',
 *   syncUrl: 'https://primary.do/db/main',
 *   authToken: 'your-token',
 *   syncInterval: 60, // seconds
 * });
 *
 * // Read-your-writes: after a write, the replica immediately sees it
 * await replica.execute('INSERT INTO users VALUES (1, "Alice")');
 * const result = await replica.execute('SELECT * FROM users WHERE id = 1');
 * // result contains the new row immediately, even before sync()
 * ```
 *
 * @packageDocumentation
 */

import { createLogger } from '../logging/index.js';
import type { FSXBackend } from '../fsx/types.js';
import type { WALWriter, WALReader, WALEntry } from '../wal/types.js';
import { crc32 } from '../wal/writer.js';
import {
  type ReplicaId,
  type ReplicaInfo,
  type WALBatch,
  type WALAck,
  type ConsistencyLevel,
  type SessionState,
  type ReplicationConfig,
  DEFAULT_REPLICATION_CONFIG,
  ReplicationError,
  ReplicationErrorCode,
  serializeReplicaId,
} from './types.js';

const logger = createLogger({ defaultContext: { module: 'embedded-replica' } });

// =============================================================================
// EMBEDDED REPLICA CONFIGURATION
// =============================================================================

/**
 * Configuration for embedded replica
 */
export interface EmbeddedReplicaConfig {
  /** Path to local database file (for file-based backends) */
  localPath?: string;
  /** URL of the remote primary database for synchronization */
  syncUrl: string;
  /** Authentication token for remote access */
  authToken?: string;
  /** Sync interval in seconds (for periodic sync). If not set, manual sync only. */
  syncInterval?: number;
  /** Enable offline mode - allows local writes that are later synced */
  offline?: boolean;
  /** Read-your-writes mode (default: true) */
  readYourWrites?: boolean;
  /** Max offline WAL entries before forcing sync */
  maxOfflineEntries?: number;
  /** Timeout for sync operations in milliseconds */
  syncTimeoutMs?: number;
}

/**
 * Default embedded replica configuration
 */
export const DEFAULT_EMBEDDED_CONFIG: Required<Omit<EmbeddedReplicaConfig, 'localPath' | 'authToken'>> & Pick<EmbeddedReplicaConfig, 'localPath' | 'authToken'> = {
  localPath: undefined,
  syncUrl: '',
  authToken: undefined,
  syncInterval: undefined,
  offline: false,
  readYourWrites: true,
  maxOfflineEntries: 10000,
  syncTimeoutMs: 30000,
};

// =============================================================================
// SYNC STATE
// =============================================================================

/**
 * Sync state tracking
 */
export interface SyncState {
  /** Last synced LSN from remote */
  lastSyncedLSN: bigint;
  /** Last local LSN (may be ahead if offline writes occurred) */
  lastLocalLSN: bigint;
  /** Timestamp of last successful sync */
  lastSyncTimestamp: number;
  /** Whether sync is currently in progress */
  syncInProgress: boolean;
  /** Number of pending offline entries to push */
  pendingOfflineEntries: number;
  /** Whether we're currently online */
  online: boolean;
}

/**
 * Sync result
 */
export interface SyncResult {
  /** Whether sync was successful */
  success: boolean;
  /** Number of entries pulled from remote */
  entriesPulled: number;
  /** Number of offline entries pushed to remote */
  entriesPushed: number;
  /** New local LSN after sync */
  newLSN: bigint;
  /** Sync duration in milliseconds */
  durationMs: number;
  /** Any errors encountered */
  errors?: string[];
}

// =============================================================================
// EMBEDDED REPLICA INTERFACE
// =============================================================================

/**
 * Embedded replica interface
 */
export interface EmbeddedReplica {
  /**
   * Sync with remote primary
   * Pulls changes from remote and optionally pushes offline writes
   */
  sync(): Promise<SyncResult>;

  /**
   * Execute SQL query
   * Writes are stored locally and may be synced later
   */
  execute(sql: string, params?: unknown[]): Promise<unknown>;

  /**
   * Get current sync state
   */
  getSyncState(): SyncState;

  /**
   * Check if connected to remote
   */
  isOnline(): boolean;

  /**
   * Start periodic sync (if configured)
   */
  startPeriodicSync(): void;

  /**
   * Stop periodic sync
   */
  stopPeriodicSync(): void;

  /**
   * Close the embedded replica
   */
  close(): Promise<void>;

  /**
   * Get last sync timestamp
   */
  getLastSyncTimestamp(): number;

  /**
   * Get number of pending offline entries
   */
  getPendingOfflineEntries(): number;

  /**
   * Force push offline entries (when back online)
   */
  pushOfflineEntries(): Promise<SyncResult>;
}

// =============================================================================
// OFFLINE WAL TRACKING
// =============================================================================

/**
 * Offline WAL entry with metadata
 */
interface OfflineWALEntry {
  /** The WAL entry */
  entry: Omit<WALEntry, 'lsn'>;
  /** Local LSN assigned */
  localLSN: bigint;
  /** Timestamp when entry was created */
  createdAt: number;
  /** Whether this entry has been pushed to remote */
  pushed: boolean;
}

// =============================================================================
// EMBEDDED REPLICA IMPLEMENTATION
// =============================================================================

/**
 * Create an embedded replica
 */
export function createEmbeddedReplica(
  backend: FSXBackend,
  walWriter: WALWriter,
  walReader: WALReader,
  config: EmbeddedReplicaConfig,
  executeSql?: (sql: string, params?: unknown[]) => Promise<unknown>
): EmbeddedReplica {
  const fullConfig = { ...DEFAULT_EMBEDDED_CONFIG, ...config };
  const textEncoder = new TextEncoder();
  const textDecoder = new TextDecoder();

  // State
  let syncState: SyncState = {
    lastSyncedLSN: 0n,
    lastLocalLSN: 0n,
    lastSyncTimestamp: 0,
    syncInProgress: false,
    pendingOfflineEntries: 0,
    online: true,
  };

  // Offline WAL buffer
  const offlineWAL: OfflineWALEntry[] = [];

  // Periodic sync interval handle
  let periodicSyncInterval: ReturnType<typeof setInterval> | undefined;

  // Session tracking for read-your-writes
  let sessionLastWriteLSN = 0n;

  // ==========================================================================
  // INITIALIZATION
  // ==========================================================================

  async function initialize(): Promise<void> {
    // Load persisted sync state
    await loadSyncState();

    // Load any persisted offline WAL entries
    await loadOfflineWAL();

    // Check connectivity
    syncState.online = await checkConnectivity();

    logger.info('Embedded replica initialized', {
      syncUrl: fullConfig.syncUrl,
      lastSyncedLSN: syncState.lastSyncedLSN.toString(),
      pendingOfflineEntries: syncState.pendingOfflineEntries,
      online: syncState.online,
    });
  }

  async function loadSyncState(): Promise<void> {
    try {
      const data = await backend.read('_embedded/sync-state.json');
      if (data) {
        const parsed = JSON.parse(textDecoder.decode(data));
        syncState = {
          ...syncState,
          lastSyncedLSN: BigInt(parsed.lastSyncedLSN ?? '0'),
          lastLocalLSN: BigInt(parsed.lastLocalLSN ?? '0'),
          lastSyncTimestamp: parsed.lastSyncTimestamp ?? 0,
          pendingOfflineEntries: parsed.pendingOfflineEntries ?? 0,
        };
      }
    } catch (error) {
      logger.warn('Failed to load sync state, starting fresh', { error });
    }
  }

  async function persistSyncState(): Promise<void> {
    const data = {
      lastSyncedLSN: syncState.lastSyncedLSN.toString(),
      lastLocalLSN: syncState.lastLocalLSN.toString(),
      lastSyncTimestamp: syncState.lastSyncTimestamp,
      pendingOfflineEntries: syncState.pendingOfflineEntries,
    };

    await backend.write(
      '_embedded/sync-state.json',
      textEncoder.encode(JSON.stringify(data))
    );
  }

  async function loadOfflineWAL(): Promise<void> {
    try {
      const data = await backend.read('_embedded/offline-wal.json');
      if (data) {
        const parsed = JSON.parse(textDecoder.decode(data));
        for (const entry of parsed) {
          offlineWAL.push({
            entry: entry.entry,
            localLSN: BigInt(entry.localLSN),
            createdAt: entry.createdAt,
            pushed: entry.pushed,
          });
        }
        syncState.pendingOfflineEntries = offlineWAL.filter(e => !e.pushed).length;
      }
    } catch (error) {
      logger.warn('Failed to load offline WAL', { error });
    }
  }

  async function persistOfflineWAL(): Promise<void> {
    const data = offlineWAL.map(entry => ({
      entry: entry.entry,
      localLSN: entry.localLSN.toString(),
      createdAt: entry.createdAt,
      pushed: entry.pushed,
    }));

    await backend.write(
      '_embedded/offline-wal.json',
      textEncoder.encode(JSON.stringify(data))
    );
  }

  async function checkConnectivity(): Promise<boolean> {
    if (!fullConfig.syncUrl) {
      return false;
    }

    try {
      // Simple connectivity check - try to reach the sync URL
      // In real implementation, this would make an actual HTTP request
      return true;
    } catch {
      return false;
    }
  }

  // ==========================================================================
  // SYNC OPERATIONS
  // ==========================================================================

  async function sync(): Promise<SyncResult> {
    if (syncState.syncInProgress) {
      return {
        success: false,
        entriesPulled: 0,
        entriesPushed: 0,
        newLSN: syncState.lastLocalLSN,
        durationMs: 0,
        errors: ['Sync already in progress'],
      };
    }

    const startTime = performance.now();
    syncState.syncInProgress = true;

    try {
      let entriesPulled = 0;
      let entriesPushed = 0;
      const errors: string[] = [];

      // Step 1: Push offline entries if any
      if (fullConfig.offline && syncState.pendingOfflineEntries > 0) {
        const pushResult = await pushOfflineEntriesToRemote();
        entriesPushed = pushResult.entriesPushed;
        if (pushResult.errors) {
          errors.push(...pushResult.errors);
        }
      }

      // Step 2: Pull changes from remote
      const pullResult = await pullFromRemote();
      entriesPulled = pullResult.entriesPulled;
      if (pullResult.errors) {
        errors.push(...pullResult.errors);
      }

      // Update sync state
      syncState.lastSyncTimestamp = Date.now();
      await persistSyncState();

      const durationMs = performance.now() - startTime;

      logger.info('Sync completed', {
        entriesPulled,
        entriesPushed,
        durationMs,
        newLSN: syncState.lastLocalLSN.toString(),
      });

      return {
        success: errors.length === 0,
        entriesPulled,
        entriesPushed,
        newLSN: syncState.lastLocalLSN,
        durationMs,
        errors: errors.length > 0 ? errors : undefined,
      };
    } catch (error) {
      const durationMs = performance.now() - startTime;
      logger.error('Sync failed', error instanceof Error ? error : new Error(String(error)));

      return {
        success: false,
        entriesPulled: 0,
        entriesPushed: 0,
        newLSN: syncState.lastLocalLSN,
        durationMs,
        errors: [error instanceof Error ? error.message : 'Unknown sync error'],
      };
    } finally {
      syncState.syncInProgress = false;
    }
  }

  async function pullFromRemote(): Promise<{ entriesPulled: number; errors?: string[] }> {
    // In real implementation, this would:
    // 1. Call remote primary's pullWAL endpoint
    // 2. Apply received WAL entries to local storage
    // 3. Update sync state

    // Simulated implementation using WAL reader
    try {
      const entries = await walReader.readEntries({
        fromLSN: syncState.lastSyncedLSN,
        limit: 1000,
      });

      if (entries.length > 0) {
        // Apply entries to local storage
        for (const entry of entries) {
          await applyEntryLocally(entry);
        }

        const lastEntry = entries[entries.length - 1];
        syncState.lastSyncedLSN = lastEntry.lsn;
        syncState.lastLocalLSN = lastEntry.lsn;
      }

      return { entriesPulled: entries.length };
    } catch (error) {
      return {
        entriesPulled: 0,
        errors: [error instanceof Error ? error.message : 'Pull failed'],
      };
    }
  }

  async function pushOfflineEntriesToRemote(): Promise<{ entriesPushed: number; errors?: string[] }> {
    const pendingEntries = offlineWAL.filter(e => !e.pushed);
    let pushed = 0;

    for (const offlineEntry of pendingEntries) {
      try {
        // In real implementation, this would push to remote primary
        // For now, write to local WAL
        await walWriter.append(offlineEntry.entry);

        offlineEntry.pushed = true;
        pushed++;
      } catch (error) {
        logger.error('Failed to push offline entry', error instanceof Error ? error : new Error(String(error)));
        break; // Stop on first error to maintain ordering
      }
    }

    // Clean up pushed entries
    const unpushedEntries = offlineWAL.filter(e => !e.pushed);
    offlineWAL.length = 0;
    offlineWAL.push(...unpushedEntries);

    syncState.pendingOfflineEntries = offlineWAL.length;
    await persistOfflineWAL();

    return { entriesPushed: pushed };
  }

  async function applyEntryLocally(entry: WALEntry): Promise<void> {
    // In real implementation, this would apply the entry to local B-tree storage
    // For now, just log the application
    logger.debug('Applied entry locally', {
      lsn: entry.lsn.toString(),
      op: entry.op,
      table: entry.table,
    });
  }

  // ==========================================================================
  // EXECUTE OPERATIONS
  // ==========================================================================

  async function execute(sql: string, params?: unknown[]): Promise<unknown> {
    // Determine if this is a write operation
    const isWrite = isWriteOperation(sql);

    if (isWrite) {
      return executeWrite(sql, params);
    } else {
      return executeRead(sql, params);
    }
  }

  function isWriteOperation(sql: string): boolean {
    const trimmed = sql.trim().toUpperCase();
    return (
      trimmed.startsWith('INSERT') ||
      trimmed.startsWith('UPDATE') ||
      trimmed.startsWith('DELETE') ||
      trimmed.startsWith('CREATE') ||
      trimmed.startsWith('DROP') ||
      trimmed.startsWith('ALTER')
    );
  }

  async function executeWrite(sql: string, params?: unknown[]): Promise<unknown> {
    if (!fullConfig.offline && !syncState.online) {
      throw new ReplicationError(
        ReplicationErrorCode.PRIMARY_UNAVAILABLE,
        'Cannot write: offline mode disabled and remote is unavailable'
      );
    }

    // Execute locally if we have a SQL executor
    let result: unknown;
    if (executeSql) {
      result = await executeSql(sql, params);
    }

    if (fullConfig.offline || !syncState.online) {
      // Store in offline WAL for later sync
      const localLSN = syncState.lastLocalLSN + 1n;
      syncState.lastLocalLSN = localLSN;

      const offlineEntry: OfflineWALEntry = {
        entry: {
          timestamp: Date.now(),
          txnId: createTransactionId(`offline_${localLSN}`),
          op: 'INSERT' as const, // Use INSERT as generic operation type for offline writes
          table: '',
          after: new TextEncoder().encode(JSON.stringify({ sql, params })),
        },
        localLSN,
        createdAt: Date.now(),
        pushed: false,
      };

      offlineWAL.push(offlineEntry);
      syncState.pendingOfflineEntries++;

      // Check if we've exceeded max offline entries
      if (syncState.pendingOfflineEntries >= fullConfig.maxOfflineEntries) {
        logger.warn('Max offline entries reached, triggering sync');
        // Don't await - let it sync in background
        sync().catch(err => logger.error('Background sync failed', err instanceof Error ? err : new Error(String(err))));
      }

      await persistOfflineWAL();
      await persistSyncState();
    } else {
      // Write directly to WAL for immediate sync
      await walWriter.append({
        timestamp: Date.now(),
        txnId: createTransactionId(`direct_${Date.now()}`),
        op: 'INSERT' as const, // Use INSERT as generic operation type for direct writes
        table: '',
        after: new TextEncoder().encode(JSON.stringify({ sql, params })),
      });
    }

    // Update session LSN for read-your-writes
    sessionLastWriteLSN = syncState.lastLocalLSN;

    return result;
  }

  async function executeRead(sql: string, params?: unknown[]): Promise<unknown> {
    // For read-your-writes, no need to sync before reading
    // The local database already has our writes applied
    if (executeSql) {
      return executeSql(sql, params);
    }

    return { rows: [], rowCount: 0 };
  }

  // ==========================================================================
  // PERIODIC SYNC
  // ==========================================================================

  function startPeriodicSync(): void {
    if (!fullConfig.syncInterval) {
      logger.warn('No sync interval configured, periodic sync not started');
      return;
    }

    if (periodicSyncInterval) {
      logger.warn('Periodic sync already running');
      return;
    }

    const intervalMs = fullConfig.syncInterval * 1000;
    periodicSyncInterval = setInterval(async () => {
      try {
        await sync();
      } catch (error) {
        logger.error('Periodic sync failed', error instanceof Error ? error : new Error(String(error)));
      }
    }, intervalMs);

    logger.info('Periodic sync started', { intervalSeconds: fullConfig.syncInterval });
  }

  function stopPeriodicSync(): void {
    if (periodicSyncInterval) {
      clearInterval(periodicSyncInterval);
      periodicSyncInterval = undefined;
      logger.info('Periodic sync stopped');
    }
  }

  // ==========================================================================
  // STATUS & UTILITIES
  // ==========================================================================

  function getSyncState(): SyncState {
    return { ...syncState };
  }

  function isOnline(): boolean {
    return syncState.online;
  }

  function getLastSyncTimestamp(): number {
    return syncState.lastSyncTimestamp;
  }

  function getPendingOfflineEntries(): number {
    return syncState.pendingOfflineEntries;
  }

  async function pushOfflineEntries(): Promise<SyncResult> {
    if (!syncState.online) {
      return {
        success: false,
        entriesPulled: 0,
        entriesPushed: 0,
        newLSN: syncState.lastLocalLSN,
        durationMs: 0,
        errors: ['Cannot push: offline'],
      };
    }

    const startTime = performance.now();
    const result = await pushOfflineEntriesToRemote();
    const durationMs = performance.now() - startTime;

    await persistSyncState();

    return {
      success: !result.errors || result.errors.length === 0,
      entriesPulled: 0,
      entriesPushed: result.entriesPushed,
      newLSN: syncState.lastLocalLSN,
      durationMs,
      errors: result.errors,
    };
  }

  async function close(): Promise<void> {
    stopPeriodicSync();
    await persistSyncState();
    await persistOfflineWAL();
    logger.info('Embedded replica closed');
  }

  // Initialize on creation
  initialize().catch(err =>
    logger.error('Embedded replica initialization failed', err instanceof Error ? err : new Error(String(err)))
  );

  return {
    sync,
    execute,
    getSyncState,
    isOnline,
    startPeriodicSync,
    stopPeriodicSync,
    close,
    getLastSyncTimestamp,
    getPendingOfflineEntries,
    pushOfflineEntries,
  };
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Options for creating an embedded replica
 */
export interface CreateEmbeddedReplicaOptions {
  backend: FSXBackend;
  walWriter: WALWriter;
  walReader: WALReader;
  config: EmbeddedReplicaConfig;
  executeSql?: (sql: string, params?: unknown[]) => Promise<unknown>;
}

/**
 * Create an embedded replica instance
 */
export function createEmbeddedReplicaInstance(options: CreateEmbeddedReplicaOptions): EmbeddedReplica {
  return createEmbeddedReplica(
    options.backend,
    options.walWriter,
    options.walReader,
    options.config,
    options.executeSql
  );
}

// =============================================================================
// EMBEDDED REPLICA CLIENT (Turso-compatible API)
// =============================================================================

/**
 * Turso-compatible client interface for embedded replicas
 * Provides a familiar API for users migrating from Turso
 */
export interface EmbeddedReplicaClient {
  /** Execute a query */
  execute(sql: string): Promise<{ rows: unknown[]; rowsAffected: number; lastInsertRowid?: bigint }>;
  /** Execute a batch of queries */
  batch(queries: string[]): Promise<Array<{ rows: unknown[]; rowsAffected: number }>>;
  /** Sync with remote */
  sync(): Promise<void>;
  /** Close the client */
  close(): void;
}

/**
 * Create a Turso-compatible embedded replica client
 */
export function createClient(config: {
  url: string;
  syncUrl?: string;
  authToken?: string;
  syncInterval?: number;
}): EmbeddedReplicaClient {
  // This is a stub that would create the actual client
  // In real implementation, this would:
  // 1. Open/create local database file at `url`
  // 2. Set up sync with `syncUrl` if provided
  // 3. Configure periodic sync if `syncInterval` is set

  throw new Error(
    'createClient requires a backend implementation. ' +
    'Use createEmbeddedReplica with explicit backend, walWriter, and walReader.'
  );
}
