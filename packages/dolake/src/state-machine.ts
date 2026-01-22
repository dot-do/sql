/**
 * State Machine Module
 *
 * Manages DoLake state transitions and persistence.
 * Extracted from the DoLake monolith for better separation of concerns.
 */

import type { DoLakeState, CDCEvent, FlushResult } from './types.js';
import type { CDCBufferManager, BufferSnapshot } from './buffer.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Valid state transitions
 */
export type StateTransition = {
  from: DoLakeState;
  to: DoLakeState;
  trigger: string;
};

/**
 * State change event
 */
export interface StateChangeEvent {
  previousState: DoLakeState;
  currentState: DoLakeState;
  timestamp: number;
  trigger: string;
}

/**
 * State persistence interface
 */
export interface StatePersistence {
  saveBufferSnapshot(snapshot: BufferSnapshot): Promise<void>;
  loadBufferSnapshot(): Promise<BufferSnapshot | null>;
  clearBufferSnapshot(): Promise<void>;
  saveFallbackEvents(events: CDCEvent[]): Promise<void>;
  loadFallbackEvents(): Promise<CDCEvent[] | null>;
  clearFallbackEvents(): Promise<void>;
  saveScheduledCompaction(data: ScheduledCompaction): Promise<void>;
  loadScheduledCompaction(): Promise<ScheduledCompaction | null>;
  setAlarm(timestamp: number): Promise<void>;
}

/**
 * Scheduled compaction data
 */
export interface ScheduledCompaction {
  namespace: string[];
  tableName: string;
  scheduledAt: number;
}

/**
 * State machine configuration
 */
export interface StateMachineConfig {
  flushIntervalMs: number;
  enableFallback: boolean;
}

// =============================================================================
// Default Configuration
// =============================================================================

export const DEFAULT_STATE_MACHINE_CONFIG: StateMachineConfig = {
  flushIntervalMs: 60_000,
  enableFallback: true,
};

// =============================================================================
// State Machine Class
// =============================================================================

/**
 * Manages DoLake state transitions with validation
 */
export class DoLakeStateMachine {
  private currentState: DoLakeState = 'idle';
  private readonly config: StateMachineConfig;
  private readonly listeners: Set<(event: StateChangeEvent) => void> = new Set();
  private readonly stateHistory: StateChangeEvent[] = [];

  /**
   * Valid state transitions map
   */
  private static readonly VALID_TRANSITIONS: Map<DoLakeState, DoLakeState[]> = new Map([
    ['idle', ['receiving', 'flushing', 'recovering']],
    ['receiving', ['idle', 'flushing']],
    ['flushing', ['idle', 'receiving']],
    ['recovering', ['idle', 'receiving']],
  ]);

  constructor(config?: Partial<StateMachineConfig>) {
    this.config = { ...DEFAULT_STATE_MACHINE_CONFIG, ...config };
  }

  /**
   * Get current state
   */
  getState(): DoLakeState {
    return this.currentState;
  }

  /**
   * Set state with validation
   */
  setState(newState: DoLakeState, trigger: string = 'unknown'): boolean {
    const validTransitions = DoLakeStateMachine.VALID_TRANSITIONS.get(this.currentState);

    if (!validTransitions?.includes(newState)) {
      console.warn(
        `Invalid state transition: ${this.currentState} -> ${newState} (trigger: ${trigger})`
      );
      return false;
    }

    const previousState = this.currentState;
    this.currentState = newState;

    const event: StateChangeEvent = {
      previousState,
      currentState: newState,
      timestamp: Date.now(),
      trigger,
    };

    this.stateHistory.push(event);
    this.notifyListeners(event);

    return true;
  }

  /**
   * Force state (for recovery scenarios)
   */
  forceState(newState: DoLakeState, trigger: string = 'force'): void {
    const previousState = this.currentState;
    this.currentState = newState;

    const event: StateChangeEvent = {
      previousState,
      currentState: newState,
      timestamp: Date.now(),
      trigger,
    };

    this.stateHistory.push(event);
    this.notifyListeners(event);
  }

  /**
   * Check if a transition is valid
   */
  canTransitionTo(newState: DoLakeState): boolean {
    const validTransitions = DoLakeStateMachine.VALID_TRANSITIONS.get(this.currentState);
    return validTransitions?.includes(newState) ?? false;
  }

  /**
   * Get state history
   */
  getHistory(): readonly StateChangeEvent[] {
    return this.stateHistory;
  }

  /**
   * Add state change listener
   */
  addListener(listener: (event: StateChangeEvent) => void): void {
    this.listeners.add(listener);
  }

  /**
   * Remove state change listener
   */
  removeListener(listener: (event: StateChangeEvent) => void): void {
    this.listeners.delete(listener);
  }

  /**
   * Notify all listeners of state change
   */
  private notifyListeners(event: StateChangeEvent): void {
    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch (error) {
        console.error('State change listener error:', error);
      }
    }
  }

  /**
   * Is the system actively processing?
   */
  isActive(): boolean {
    return this.currentState === 'receiving' || this.currentState === 'flushing';
  }

  /**
   * Is the system idle?
   */
  isIdle(): boolean {
    return this.currentState === 'idle';
  }

  /**
   * Is the system recovering?
   */
  isRecovering(): boolean {
    return this.currentState === 'recovering';
  }
}

// =============================================================================
// State Persistence Implementation
// =============================================================================

/**
 * Creates a state persistence implementation using Durable Object storage
 */
export function createStatePersistence(ctx: DurableObjectState): StatePersistence {
  return {
    async saveBufferSnapshot(snapshot: BufferSnapshot): Promise<void> {
      await ctx.storage.put('buffer_snapshot', snapshot);
    },

    async loadBufferSnapshot(): Promise<BufferSnapshot | null> {
      return await ctx.storage.get<BufferSnapshot>('buffer_snapshot') ?? null;
    },

    async clearBufferSnapshot(): Promise<void> {
      await ctx.storage.delete('buffer_snapshot');
    },

    async saveFallbackEvents(events: CDCEvent[]): Promise<void> {
      await ctx.storage.put('fallback_events', events);
    },

    async loadFallbackEvents(): Promise<CDCEvent[] | null> {
      return await ctx.storage.get<CDCEvent[]>('fallback_events') ?? null;
    },

    async clearFallbackEvents(): Promise<void> {
      await ctx.storage.delete('fallback_events');
    },

    async saveScheduledCompaction(data: ScheduledCompaction): Promise<void> {
      await ctx.storage.put('scheduled_compaction', data);
    },

    async loadScheduledCompaction(): Promise<ScheduledCompaction | null> {
      return await ctx.storage.get<ScheduledCompaction>('scheduled_compaction') ?? null;
    },

    async setAlarm(timestamp: number): Promise<void> {
      await ctx.storage.setAlarm(timestamp);
    },
  };
}

// =============================================================================
// Alarm Handler
// =============================================================================

/**
 * Configuration for alarm handling
 */
export interface AlarmHandlerConfig {
  flushIntervalMs: number;
  enableFallback: boolean;
}

/**
 * Dependencies for alarm handling
 */
export interface AlarmHandlerDeps {
  stateMachine: DoLakeStateMachine;
  persistence: StatePersistence;
  buffer: CDCBufferManager;
  flush: (trigger: string) => Promise<FlushResult>;
  recoverFromFallback: (events: CDCEvent[]) => Promise<void>;
}

/**
 * Handle DO alarm for scheduled flushes and recovery
 */
export async function handleAlarm(
  deps: AlarmHandlerDeps,
  config: AlarmHandlerConfig
): Promise<void> {
  const stats = deps.buffer.getStats();

  if (stats.eventCount > 0) {
    await deps.flush('threshold_time');
  }

  // Recover from fallback if needed
  const fallbackEvents = await deps.persistence.loadFallbackEvents();
  if (fallbackEvents && fallbackEvents.length > 0) {
    await deps.recoverFromFallback(fallbackEvents);
  }

  // Schedule next alarm
  const nextAlarm = Date.now() + config.flushIntervalMs;
  await deps.persistence.setAlarm(nextAlarm);
}

/**
 * Schedule the next alarm
 */
export async function scheduleAlarm(
  persistence: StatePersistence,
  intervalMs: number
): Promise<void> {
  const nextAlarm = Date.now() + intervalMs;
  await persistence.setAlarm(nextAlarm);
}

// =============================================================================
// Recovery Operations
// =============================================================================

/**
 * Recovery result
 */
export interface RecoveryResult {
  success: boolean;
  eventsRecovered: number;
  tablesProcessed: number;
  error?: string;
}

/**
 * Create a recovery handler
 */
export function createRecoveryHandler(
  stateMachine: DoLakeStateMachine,
  persistence: StatePersistence,
  processTableEvents: (tableName: string, events: CDCEvent[]) => Promise<void>
): (events: CDCEvent[]) => Promise<RecoveryResult> {
  return async (events: CDCEvent[]): Promise<RecoveryResult> => {
    stateMachine.setState('recovering', 'fallback_recovery');

    try {
      // Group by table and flush
      const byTable = new Map<string, CDCEvent[]>();
      for (const event of events) {
        let tableEvents = byTable.get(event.table);
        if (!tableEvents) {
          tableEvents = [];
          byTable.set(event.table, tableEvents);
        }
        tableEvents.push(event);
      }

      for (const [tableName, tableEvents] of byTable) {
        await processTableEvents(tableName, tableEvents);
      }

      // Clear fallback storage
      await persistence.clearFallbackEvents();

      stateMachine.setState('idle', 'recovery_complete');

      return {
        success: true,
        eventsRecovered: events.length,
        tablesProcessed: byTable.size,
      };
    } catch (error) {
      console.error('Fallback recovery failed:', error);
      stateMachine.forceState('idle', 'recovery_failed');

      return {
        success: false,
        eventsRecovered: 0,
        tablesProcessed: 0,
        error: String(error),
      };
    }
  };
}
