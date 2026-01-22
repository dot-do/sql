/**
 * Trigger Registry
 *
 * Stores and manages both JavaScript and SQL triggers with support for:
 * - Priority-based ordering
 * - Enable/disable functionality
 * - Filtering by table, timing, and event
 * - SQL trigger definitions (CREATE TRIGGER syntax)
 */

import type {
  TriggerDefinition,
  TriggerConfig,
  TriggerRegistry,
  TriggerTiming,
  TriggerEvent,
  RegisterTriggerOptions,
  ListTriggersOptions,
  SQLTriggerDefinition,
  SQLTriggerTiming,
  SQLTriggerEvent,
  ParsedSQLTrigger,
} from './types.js';
import { TriggerError, TriggerErrorCode } from './types.js';

// =============================================================================
// Registry Storage Interface
// =============================================================================

/**
 * Storage backend for trigger registry
 */
export interface TriggerStorage {
  /** Get a trigger by name */
  get(name: string): TriggerConfig | undefined;

  /** Set a trigger */
  set(name: string, trigger: TriggerConfig): void;

  /** Delete a trigger */
  delete(name: string): boolean;

  /** List all triggers */
  list(): TriggerConfig[];

  /** Clear all triggers */
  clear(): void;
}

// =============================================================================
// In-Memory Storage Implementation
// =============================================================================

/**
 * Create an in-memory trigger storage
 */
export function createInMemoryTriggerStorage(): TriggerStorage {
  const store = new Map<string, TriggerConfig>();

  return {
    get(name: string): TriggerConfig | undefined {
      return store.get(name);
    },

    set(name: string, trigger: TriggerConfig): void {
      store.set(name, trigger);
    },

    delete(name: string): boolean {
      return store.delete(name);
    },

    list(): TriggerConfig[] {
      return Array.from(store.values());
    },

    clear(): void {
      store.clear();
    },
  };
}

// =============================================================================
// Registry Implementation
// =============================================================================

/**
 * Options for creating a trigger registry
 */
export interface TriggerRegistryOptions {
  /** Storage backend (default: in-memory) */
  storage?: TriggerStorage;

  /** Default priority for triggers (default: 100) */
  defaultPriority?: number;

  /** Whether new triggers are enabled by default (default: true) */
  enabledByDefault?: boolean;
}

/**
 * Create a trigger registry
 */
export function createTriggerRegistry(options: TriggerRegistryOptions = {}): TriggerRegistry {
  const {
    storage = createInMemoryTriggerStorage(),
    defaultPriority = 100,
    enabledByDefault = true,
  } = options;

  // Index for fast lookup by table/timing/event
  const tableEventIndex = new Map<string, Set<string>>();

  /**
   * Build index key for table/timing/event
   */
  function indexKey(table: string, timing: TriggerTiming, event: TriggerEvent): string {
    return `${table}:${timing}:${event}`;
  }

  /**
   * Add trigger to index
   */
  function addToIndex(trigger: TriggerConfig): void {
    for (const event of trigger.events) {
      const key = indexKey(trigger.table, trigger.timing, event);
      let names = tableEventIndex.get(key);
      if (!names) {
        names = new Set();
        tableEventIndex.set(key, names);
      }
      names.add(trigger.name);
    }
  }

  /**
   * Remove trigger from index
   */
  function removeFromIndex(trigger: TriggerConfig): void {
    for (const event of trigger.events) {
      const key = indexKey(trigger.table, trigger.timing, event);
      const names = tableEventIndex.get(key);
      if (names) {
        names.delete(trigger.name);
        if (names.size === 0) {
          tableEventIndex.delete(key);
        }
      }
    }
  }

  /**
   * Check if trigger is a SQL trigger (ParsedSQLTrigger)
   */
  function isSQLTrigger(trigger: any): trigger is ParsedSQLTrigger {
    return (
      trigger.timing &&
      ['BEFORE', 'AFTER', 'INSTEAD OF'].includes(trigger.timing) &&
      trigger.body !== undefined
    );
  }

  /**
   * Validate trigger definition (JavaScript triggers)
   */
  function validateTrigger<T extends Record<string, unknown>>(
    trigger: TriggerDefinition<T>
  ): void {
    if (!trigger.name || typeof trigger.name !== 'string') {
      throw new TriggerError(
        TriggerErrorCode.INVALID_DEFINITION,
        'Trigger name is required and must be a string'
      );
    }

    if (!trigger.table || typeof trigger.table !== 'string') {
      throw new TriggerError(
        TriggerErrorCode.INVALID_DEFINITION,
        'Trigger table is required and must be a string',
        trigger.name
      );
    }

    if (!['before', 'after'].includes(trigger.timing)) {
      throw new TriggerError(
        TriggerErrorCode.INVALID_DEFINITION,
        'Trigger timing must be "before" or "after"',
        trigger.name
      );
    }

    if (!Array.isArray(trigger.events) || trigger.events.length === 0) {
      throw new TriggerError(
        TriggerErrorCode.INVALID_DEFINITION,
        'Trigger events must be a non-empty array',
        trigger.name
      );
    }

    const validEvents: TriggerEvent[] = ['insert', 'update', 'delete'];
    for (const event of trigger.events) {
      if (!validEvents.includes(event)) {
        throw new TriggerError(
          TriggerErrorCode.INVALID_DEFINITION,
          `Invalid trigger event: ${event}. Must be one of: ${validEvents.join(', ')}`,
          trigger.name
        );
      }
    }

    if (typeof trigger.handler !== 'function') {
      throw new TriggerError(
        TriggerErrorCode.INVALID_DEFINITION,
        'Trigger handler must be a function',
        trigger.name
      );
    }
  }

  /**
   * Validate SQL trigger definition
   */
  function validateSQLTrigger(trigger: ParsedSQLTrigger): void {
    if (!trigger.name || typeof trigger.name !== 'string') {
      throw new TriggerError(
        TriggerErrorCode.INVALID_DEFINITION,
        'Trigger name is required and must be a string'
      );
    }

    if (!trigger.table || typeof trigger.table !== 'string') {
      throw new TriggerError(
        TriggerErrorCode.INVALID_DEFINITION,
        'Trigger table is required and must be a string',
        trigger.name
      );
    }

    if (!['BEFORE', 'AFTER', 'INSTEAD OF'].includes(trigger.timing)) {
      throw new TriggerError(
        TriggerErrorCode.INVALID_DEFINITION,
        'Trigger timing must be "BEFORE", "AFTER", or "INSTEAD OF"',
        trigger.name
      );
    }

    if (!trigger.body || typeof trigger.body !== 'string') {
      throw new TriggerError(
        TriggerErrorCode.INVALID_DEFINITION,
        'Trigger body is required',
        trigger.name
      );
    }
  }

  /**
   * Convert SQL trigger timing to lowercase for internal use
   */
  function normalizeTiming(timing: TriggerTiming | SQLTriggerTiming): TriggerTiming {
    const lower = timing.toLowerCase();
    if (lower === 'instead of') return 'before'; // Map INSTEAD OF to before for indexing
    return lower as TriggerTiming;
  }

  /**
   * Convert SQL trigger event to lowercase for internal use
   */
  function normalizeEvent(event: TriggerEvent | SQLTriggerEvent): TriggerEvent {
    return event.toLowerCase() as TriggerEvent;
  }

  // Build initial index from storage
  for (const trigger of storage.list()) {
    addToIndex(trigger);
  }

  return {
    register<T extends Record<string, unknown>>(
      trigger: TriggerDefinition<T> | ParsedSQLTrigger,
      options: RegisterTriggerOptions = {}
    ): TriggerConfig<T> {
      // Check if this is a SQL trigger
      if (isSQLTrigger(trigger)) {
        validateSQLTrigger(trigger);

        const existing = storage.get(trigger.name);
        if (existing && !options.replace) {
          throw new TriggerError(
            TriggerErrorCode.DUPLICATE_TRIGGER,
            `Trigger '${trigger.name}' already exists. Use replace: true to overwrite.`,
            trigger.name
          );
        }

        const now = new Date();

        // Convert SQL trigger to TriggerConfig format for storage
        const config: TriggerConfig<T> = {
          name: trigger.name,
          table: trigger.table,
          timing: normalizeTiming(trigger.timing),
          events: trigger.events.map(normalizeEvent),
          handler: (() => {}) as any, // Placeholder handler for SQL triggers
          priority: defaultPriority,
          enabled: enabledByDefault,
          version: existing ? existing.version + 1 : 1,
          createdAt: existing?.createdAt ?? now,
          updatedAt: now,
          author: options.author ?? existing?.author,
          tags: options.tags ?? existing?.tags,
          // Store SQL-specific properties
          condition: trigger.whenClause,
          description: `SQL Trigger: ${trigger.timing} ${trigger.event} ON ${trigger.table}`,
          // Store the full SQL trigger definition for execution
          ...(trigger as any),
        } as TriggerConfig<T>;

        // Remove old index if replacing
        if (existing) {
          removeFromIndex(existing);
        }

        storage.set(trigger.name, config as TriggerConfig);
        addToIndex(config as TriggerConfig);

        return config;
      }

      // Handle JavaScript triggers
      validateTrigger(trigger as TriggerDefinition<T>);

      const existing = storage.get(trigger.name);
      if (existing && !options.replace) {
        throw new TriggerError(
          TriggerErrorCode.DUPLICATE_TRIGGER,
          `Trigger '${trigger.name}' already exists. Use replace: true to overwrite.`,
          trigger.name
        );
      }

      const now = new Date();
      const config: TriggerConfig<T> = {
        ...(trigger as TriggerDefinition<T>),
        priority: (trigger as TriggerDefinition<T>).priority ?? defaultPriority,
        enabled: (trigger as TriggerDefinition<T>).enabled ?? enabledByDefault,
        version: existing ? existing.version + 1 : 1,
        createdAt: existing?.createdAt ?? now,
        updatedAt: now,
        author: options.author ?? existing?.author,
        tags: options.tags ?? existing?.tags,
      };

      // Remove old index if replacing
      if (existing) {
        removeFromIndex(existing);
      }

      storage.set(trigger.name, config as TriggerConfig);
      addToIndex(config as TriggerConfig);

      return config;
    },

    get(name: string): TriggerConfig | undefined {
      return storage.get(name);
    },

    list(options: ListTriggersOptions = {}): TriggerConfig[] {
      let triggers = storage.list();

      // Apply filters
      if (options.table !== undefined) {
        triggers = triggers.filter(t => t.table === options.table);
      }

      if (options.timing !== undefined) {
        // Handle both SQL trigger timing (uppercase) and JS trigger timing (lowercase)
        const normalizedTiming = options.timing.toLowerCase();
        triggers = triggers.filter(t => {
          const triggerTiming = t.timing.toLowerCase();
          // Handle INSTEAD OF mapping to before
          if (normalizedTiming === 'instead of' || normalizedTiming === 'instead_of') {
            return triggerTiming === 'before' && (t as any).timing === 'INSTEAD OF';
          }
          return triggerTiming === normalizedTiming ||
            (t as any).timing?.toUpperCase() === options.timing?.toUpperCase();
        });
      }

      if (options.event !== undefined) {
        // Handle both SQL trigger events (uppercase) and JS trigger events (lowercase)
        const normalizedEvent = options.event.toLowerCase() as TriggerEvent;
        triggers = triggers.filter(t =>
          t.events.includes(normalizedEvent) ||
          t.events.map(e => e.toUpperCase()).includes(options.event!.toUpperCase())
        );
      }

      if (options.enabled !== undefined) {
        triggers = triggers.filter(t => t.enabled === options.enabled);
      }

      if (options.tag !== undefined) {
        triggers = triggers.filter(t => t.tags?.includes(options.tag!));
      }

      // Sort by priority (lower first), then by name
      return triggers.sort((a, b) => {
        const priorityDiff = (a.priority ?? defaultPriority) - (b.priority ?? defaultPriority);
        if (priorityDiff !== 0) return priorityDiff;
        return a.name.localeCompare(b.name);
      });
    },

    getForTableEvent(
      table: string,
      timing: TriggerTiming,
      event: TriggerEvent
    ): TriggerConfig[] {
      const key = indexKey(table, timing, event);
      const names = tableEventIndex.get(key);

      if (!names || names.size === 0) {
        return [];
      }

      const triggers: TriggerConfig[] = [];
      for (const name of names) {
        const trigger = storage.get(name);
        if (trigger) {
          triggers.push(trigger);
        }
      }

      // Sort by priority (lower first), then by name
      return triggers.sort((a, b) => {
        const priorityDiff = (a.priority ?? defaultPriority) - (b.priority ?? defaultPriority);
        if (priorityDiff !== 0) return priorityDiff;
        return a.name.localeCompare(b.name);
      });
    },

    enable(name: string): boolean {
      const trigger = storage.get(name);
      if (!trigger) {
        return false;
      }

      trigger.enabled = true;
      trigger.updatedAt = new Date();
      storage.set(name, trigger);
      return true;
    },

    disable(name: string): boolean {
      const trigger = storage.get(name);
      if (!trigger) {
        return false;
      }

      trigger.enabled = false;
      trigger.updatedAt = new Date();
      storage.set(name, trigger);
      return true;
    },

    remove(name: string): boolean {
      const trigger = storage.get(name);
      if (!trigger) {
        return false;
      }

      removeFromIndex(trigger);
      return storage.delete(name);
    },

    clear(table?: string): void {
      if (table) {
        // Only clear triggers for specific table
        const toRemove: string[] = [];
        for (const trigger of storage.list()) {
          if (trigger.table === table) {
            toRemove.push(trigger.name);
          }
        }
        for (const name of toRemove) {
          const trigger = storage.get(name);
          if (trigger) {
            removeFromIndex(trigger);
            storage.delete(name);
          }
        }
      } else {
        // Clear all
        storage.clear();
        tableEventIndex.clear();
      }
    },

    setPriority(name: string, priority: number): boolean {
      const trigger = storage.get(name);
      if (!trigger) {
        return false;
      }

      trigger.priority = priority;
      trigger.updatedAt = new Date();
      storage.set(name, trigger);
      return true;
    },
  };
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Create a trigger registry pre-populated with triggers
 */
export function createRegistryWithTriggers(
  triggers: Array<TriggerDefinition<any>>,
  options: TriggerRegistryOptions = {}
): TriggerRegistry {
  const registry = createTriggerRegistry(options);

  for (const trigger of triggers) {
    registry.register(trigger);
  }

  return registry;
}

/**
 * Merge multiple registries into one
 */
export function mergeRegistries(
  ...registries: TriggerRegistry[]
): TriggerRegistry {
  const merged = createTriggerRegistry();

  for (const registry of registries) {
    for (const trigger of registry.list()) {
      merged.register(trigger, { replace: true });
    }
  }

  return merged;
}

/**
 * Export triggers from a registry as definitions
 */
export function exportTriggers(registry: TriggerRegistry): TriggerDefinition<any>[] {
  return registry.list().map(config => ({
    name: config.name,
    table: config.table,
    timing: config.timing,
    events: config.events,
    handler: config.handler,
    priority: config.priority,
    enabled: config.enabled,
    description: config.description,
    condition: config.condition,
  }));
}
