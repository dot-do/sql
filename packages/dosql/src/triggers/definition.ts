/**
 * Trigger Definition API
 *
 * Provides fluent APIs for defining JavaScript triggers:
 * - Object-based definition with defineTriggers()
 * - Builder pattern with TriggerBuilder
 * - Helper functions for common patterns
 */

import type {
  TriggerDefinition,
  TriggerConfig,
  TriggerRegistry,
  TriggerHandler,
  TriggerTiming,
  TriggerEvent,
  TriggerContext,
  RegisterTriggerOptions,
  DefinedTriggers,
} from './types.js';

// =============================================================================
// Object-Based Definition API
// =============================================================================

/**
 * Define multiple triggers using an object literal
 *
 * @example
 * ```typescript
 * const triggers = defineTriggers({
 *   validateUser: {
 *     table: 'users',
 *     timing: 'before',
 *     events: ['insert', 'update'],
 *     handler: ({ new: user }) => {
 *       if (!user.email.includes('@')) {
 *         throw new Error('Invalid email');
 *       }
 *       return { ...user, email: user.email.toLowerCase() };
 *     },
 *   },
 *   notifyNewUser: {
 *     table: 'users',
 *     timing: 'after',
 *     events: ['insert'],
 *     handler: async ({ new: user, db }) => {
 *       await db.notifications.insert({
 *         type: 'welcome',
 *         userId: user.id,
 *         createdAt: new Date(),
 *       });
 *     },
 *   },
 * });
 * ```
 */
export function defineTriggers<
  T extends Record<string, Omit<TriggerDefinition, 'name'>>
>(definitions: T): DefinedTriggers<{ [K in keyof T]: TriggerDefinition }> {
  // Convert to full definitions with names
  const fullDefinitions = Object.fromEntries(
    Object.entries(definitions).map(([name, def]) => [
      name,
      { ...def, name } as TriggerDefinition,
    ])
  ) as { [K in keyof T]: TriggerDefinition };

  return {
    definitions: fullDefinitions,

    registerAll(registry: TriggerRegistry, options?: RegisterTriggerOptions): void {
      for (const trigger of Object.values(fullDefinitions)) {
        registry.register(trigger, options);
      }
    },

    get<K extends keyof T>(name: K): TriggerDefinition {
      return fullDefinitions[name];
    },
  };
}

// =============================================================================
// Builder Pattern API
// =============================================================================

/**
 * Fluent builder for creating trigger definitions
 *
 * @example
 * ```typescript
 * const trigger = new TriggerBuilder('validateEmail')
 *   .on('users')
 *   .before('insert', 'update')
 *   .withPriority(10)
 *   .handler(({ new: user }) => {
 *     if (!user.email.includes('@')) {
 *       throw new Error('Invalid email');
 *     }
 *   })
 *   .build();
 * ```
 */
export class TriggerBuilder<T extends Record<string, unknown> = Record<string, unknown>> {
  private _name: string;
  private _table: string = '';
  private _timing: TriggerTiming = 'before';
  private _events: TriggerEvent[] = [];
  private _handler?: TriggerHandler<T>;
  private _priority?: number;
  private _enabled: boolean = true;
  private _description?: string;
  private _condition?: string | ((ctx: TriggerContext<T>) => boolean);

  constructor(name: string) {
    this._name = name;
  }

  /**
   * Set the table this trigger applies to
   */
  on(table: string): this {
    this._table = table;
    return this;
  }

  /**
   * Alias for on()
   */
  table(table: string): this {
    return this.on(table);
  }

  /**
   * Set timing to 'before' and specify events
   */
  before(...events: TriggerEvent[]): this {
    this._timing = 'before';
    this._events = events.length > 0 ? events : this._events;
    return this;
  }

  /**
   * Set timing to 'after' and specify events
   */
  after(...events: TriggerEvent[]): this {
    this._timing = 'after';
    this._events = events.length > 0 ? events : this._events;
    return this;
  }

  /**
   * Add events that trigger this
   */
  onEvent(...events: TriggerEvent[]): this {
    this._events = [...new Set([...this._events, ...events])];
    return this;
  }

  /**
   * Trigger on INSERT
   */
  onInsert(): this {
    return this.onEvent('insert');
  }

  /**
   * Trigger on UPDATE
   */
  onUpdate(): this {
    return this.onEvent('update');
  }

  /**
   * Trigger on DELETE
   */
  onDelete(): this {
    return this.onEvent('delete');
  }

  /**
   * Set the handler function
   */
  handler(fn: TriggerHandler<T>): this {
    this._handler = fn;
    return this;
  }

  /**
   * Alias for handler()
   */
  do(fn: TriggerHandler<T>): this {
    return this.handler(fn);
  }

  /**
   * Set the priority (lower runs first)
   */
  withPriority(priority: number): this {
    this._priority = priority;
    return this;
  }

  /**
   * Set whether the trigger is enabled
   */
  enabled(value: boolean = true): this {
    this._enabled = value;
    return this;
  }

  /**
   * Set whether the trigger is disabled
   */
  disabled(value: boolean = true): this {
    this._enabled = !value;
    return this;
  }

  /**
   * Set a description
   */
  describe(description: string): this {
    this._description = description;
    return this;
  }

  /**
   * Set a condition for when the trigger should fire
   */
  when(condition: string | ((ctx: TriggerContext<T>) => boolean)): this {
    this._condition = condition;
    return this;
  }

  /**
   * Build the trigger definition
   */
  build(): TriggerDefinition<T> {
    if (!this._table) {
      throw new Error('Trigger table is required');
    }
    if (this._events.length === 0) {
      throw new Error('At least one event is required');
    }
    if (!this._handler) {
      throw new Error('Trigger handler is required');
    }

    return {
      name: this._name,
      table: this._table,
      timing: this._timing,
      events: this._events,
      handler: this._handler,
      priority: this._priority,
      enabled: this._enabled,
      description: this._description,
      condition: this._condition,
    };
  }

  /**
   * Build and register the trigger
   */
  register(registry: TriggerRegistry, options?: RegisterTriggerOptions): TriggerConfig<T> {
    return registry.register(this.build(), options);
  }
}

/**
 * Create a new trigger builder
 */
export function trigger<T extends Record<string, unknown> = Record<string, unknown>>(
  name: string
): TriggerBuilder<T> {
  return new TriggerBuilder<T>(name);
}

// =============================================================================
// Helper Functions for Common Patterns
// =============================================================================

/**
 * Create a validation trigger
 *
 * @example
 * ```typescript
 * const validateEmail = validationTrigger('users', ({ new: user }) => {
 *   if (!user.email.includes('@')) {
 *     throw new Error('Invalid email');
 *   }
 * });
 * ```
 */
export function validationTrigger<T extends Record<string, unknown>>(
  table: string,
  validator: (ctx: TriggerContext<T>) => void,
  options: {
    name?: string;
    events?: TriggerEvent[];
    priority?: number;
  } = {}
): TriggerDefinition<T> {
  const { name = `validate_${table}`, events = ['insert', 'update'], priority = 10 } = options;

  return {
    name,
    table,
    timing: 'before',
    events,
    priority,
    handler: (ctx) => {
      validator(ctx);
      // Return undefined to proceed without modification
    },
  };
}

/**
 * Create a transform trigger that modifies the row
 *
 * @example
 * ```typescript
 * const normalizeEmail = transformTrigger('users', ({ new: user }) => ({
 *   ...user,
 *   email: user.email.toLowerCase().trim(),
 * }));
 * ```
 */
export function transformTrigger<T extends Record<string, unknown>>(
  table: string,
  transformer: (ctx: TriggerContext<T>) => T,
  options: {
    name?: string;
    events?: TriggerEvent[];
    priority?: number;
  } = {}
): TriggerDefinition<T> {
  const { name = `transform_${table}`, events = ['insert', 'update'], priority = 50 } = options;

  return {
    name,
    table,
    timing: 'before',
    events,
    priority,
    handler: transformer,
  };
}

/**
 * Create an audit trigger that logs changes
 *
 * @example
 * ```typescript
 * const auditUsers = auditTrigger('users', 'userAudit');
 * ```
 */
export function auditTrigger<T extends Record<string, unknown>>(
  table: string,
  auditTable: string,
  options: {
    name?: string;
    events?: TriggerEvent[];
    includeOld?: boolean;
    includeNew?: boolean;
  } = {}
): TriggerDefinition<T> {
  const {
    name = `audit_${table}`,
    events = ['insert', 'update', 'delete'],
    includeOld = true,
    includeNew = true,
  } = options;

  return {
    name,
    table,
    timing: 'after',
    events,
    priority: 1000, // Run last
    handler: async (ctx) => {
      const auditRecord: Record<string, unknown> = {
        tableName: ctx.table,
        event: ctx.event,
        timestamp: new Date(),
        txnId: ctx.txnId,
      };

      if (includeOld && ctx.old) {
        auditRecord.oldValue = JSON.stringify(ctx.old);
      }

      if (includeNew && ctx.new) {
        auditRecord.newValue = JSON.stringify(ctx.new);
      }

      // Extract primary key
      const row = ctx.new ?? ctx.old;
      if (row && 'id' in row) {
        auditRecord.recordId = row.id;
      }

      await ctx.db.tables[auditTable as keyof typeof ctx.db.tables]?.insert?.(auditRecord);
    },
  };
}

/**
 * Create a computed field trigger
 *
 * @example
 * ```typescript
 * const computeFullName = computedFieldTrigger('users', 'fullName', (user) =>
 *   `${user.firstName} ${user.lastName}`
 * );
 * ```
 */
export function computedFieldTrigger<T extends Record<string, unknown>, V>(
  table: string,
  fieldName: string,
  compute: (row: T) => V,
  options: {
    name?: string;
    events?: TriggerEvent[];
    priority?: number;
  } = {}
): TriggerDefinition<T> {
  const { name = `compute_${fieldName}`, events = ['insert', 'update'], priority = 90 } = options;

  return {
    name,
    table,
    timing: 'before',
    events,
    priority,
    handler: (ctx) => {
      if (!ctx.new) return;
      return {
        ...ctx.new,
        [fieldName]: compute(ctx.new),
      } as T;
    },
  };
}

/**
 * Create a cascade update trigger
 *
 * @example
 * ```typescript
 * const updateOrderTotal = cascadeUpdateTrigger({
 *   sourceTable: 'orderItems',
 *   targetTable: 'orders',
 *   foreignKey: 'orderId',
 *   updater: async (db, orderId) => {
 *     const items = await db.orderItems.where({ orderId });
 *     const total = items.reduce((sum, i) => sum + i.price * i.quantity, 0);
 *     await db.orders.update({ id: orderId }, { totalAmount: total });
 *   },
 * });
 * ```
 */
export function cascadeUpdateTrigger<T extends Record<string, unknown>>(
  config: {
    sourceTable: string;
    targetTable: string;
    foreignKey: string;
    updater: (db: TriggerContext<T>['db'], foreignKeyValue: unknown) => Promise<void>;
    name?: string;
    events?: TriggerEvent[];
  }
): TriggerDefinition<T> {
  const {
    sourceTable,
    targetTable,
    foreignKey,
    updater,
    name = `cascade_${sourceTable}_to_${targetTable}`,
    events = ['insert', 'update', 'delete'],
  } = config;

  return {
    name,
    table: sourceTable,
    timing: 'after',
    events,
    handler: async (ctx) => {
      const row = ctx.new ?? ctx.old;
      if (!row) return;

      const foreignKeyValue = row[foreignKey as keyof T];
      if (foreignKeyValue !== undefined) {
        await updater(ctx.db, foreignKeyValue);
      }
    },
  };
}

/**
 * Create a soft delete trigger
 *
 * @example
 * ```typescript
 * const softDeleteUsers = softDeleteTrigger('users', 'deletedAt');
 * ```
 */
export function softDeleteTrigger<T extends Record<string, unknown>>(
  table: string,
  deletedAtField: string = 'deletedAt',
  options: {
    name?: string;
  } = {}
): TriggerDefinition<T> {
  const { name = `soft_delete_${table}` } = options;

  return {
    name,
    table,
    timing: 'before',
    events: ['delete'],
    priority: 1, // Run first
    handler: (ctx) => {
      // Convert delete to update with deletedAt timestamp
      // Note: This requires special handling in the executor
      if (ctx.old) {
        return {
          ...ctx.old,
          [deletedAtField]: new Date(),
        } as T;
      }
    },
  };
}

/**
 * Create a timestamp trigger for createdAt/updatedAt
 *
 * @example
 * ```typescript
 * const timestamps = timestampTrigger('users');
 * ```
 */
export function timestampTrigger<T extends Record<string, unknown>>(
  table: string,
  options: {
    name?: string;
    createdAtField?: string;
    updatedAtField?: string;
  } = {}
): TriggerDefinition<T> {
  const {
    name = `timestamps_${table}`,
    createdAtField = 'createdAt',
    updatedAtField = 'updatedAt',
  } = options;

  return {
    name,
    table,
    timing: 'before',
    events: ['insert', 'update'],
    priority: 5, // Run early
    handler: (ctx) => {
      if (!ctx.new) return;

      const now = new Date();
      const updates: Record<string, unknown> = {
        [updatedAtField]: now,
      };

      if (ctx.event === 'insert') {
        updates[createdAtField] = now;
      }

      return { ...ctx.new, ...updates } as T;
    },
  };
}

// =============================================================================
// Trigger Group API
// =============================================================================

/**
 * Group multiple triggers together
 *
 * @example
 * ```typescript
 * const userTriggers = triggerGroup('users', [
 *   validationTrigger('users', validateUser),
 *   timestampTrigger('users'),
 *   auditTrigger('users', 'userAudit'),
 * ]);
 * ```
 */
export function triggerGroup<T extends Record<string, unknown>>(
  table: string,
  triggers: Array<TriggerDefinition<T>>
): {
  triggers: Array<TriggerDefinition<T>>;
  table: string;
  registerAll(registry: TriggerRegistry, options?: RegisterTriggerOptions): void;
} {
  // Ensure all triggers are for the same table
  const tableTriggers = triggers.map(t => ({ ...t, table }));

  return {
    triggers: tableTriggers,
    table,
    registerAll(registry: TriggerRegistry, options?: RegisterTriggerOptions): void {
      for (const trigger of tableTriggers) {
        registry.register(trigger, options);
      }
    },
  };
}
