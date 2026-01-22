/**
 * PlanningContext - Request-scoped ID generation for query planning
 *
 * This class solves the race condition problem with global planNodeIdCounter.
 * Each query planning operation should get its own PlanningContext, ensuring:
 *
 * 1. ID isolation: Each planning context has its own ID sequence starting from 1
 * 2. Deterministic testing: Tests can create deterministic contexts without global reset
 * 3. No global mutable state: Eliminates race conditions in concurrent planning
 * 4. Context identification: Each context has a unique ID for debugging/tracing
 *
 * @example
 * // Normal usage - each query gets its own context
 * const ctx = new PlanningContext();
 * const scanNode = createScanNodeWithContext(ctx, 'users', ['id']);
 * // scanNode.id === 1
 *
 * @example
 * // Deterministic testing
 * const ctx = PlanningContext.createDeterministic(0);
 * const id1 = ctx.nextId(); // Always 1
 * const id2 = ctx.nextId(); // Always 2
 */

/**
 * PlanningContext manages ID sequence for a single query planning operation.
 * This replaces the global planNodeIdCounter with request-scoped state.
 */
export class PlanningContext {
  private idCounter = 0;
  private readonly _contextId: string;

  /**
   * Create a new planning context.
   * @param contextId - Optional context identifier for debugging/tracing.
   *                    If not provided, a random UUID is generated.
   */
  constructor(contextId?: string) {
    this._contextId = contextId ?? generateContextId();
  }

  /**
   * Generate the next unique ID within this context.
   * IDs are 1-indexed to match the existing behavior where IDs start at 1.
   * @returns The next sequential ID (1, 2, 3, ...)
   */
  nextId(): number {
    return ++this.idCounter;
  }

  /**
   * Get the current ID counter value without incrementing.
   * Useful for debugging and testing.
   * @returns The current counter value (last ID generated, or 0 if none)
   */
  get currentId(): number {
    return this.idCounter;
  }

  /**
   * Get the context identifier.
   * @returns The context ID (either provided or auto-generated)
   */
  get contextId(): string {
    return this._contextId;
  }

  /**
   * Create a deterministic planning context for testing.
   * This allows tests to predict exact ID values without global state reset.
   *
   * @param startId - The starting ID counter value (default: 0, so first nextId() returns 1)
   * @param contextId - Optional fixed context ID (default: 'test')
   * @returns A planning context with deterministic behavior
   *
   * @example
   * const ctx = PlanningContext.createDeterministic();
   * expect(ctx.nextId()).toBe(1);
   * expect(ctx.nextId()).toBe(2);
   *
   * @example
   * const ctx = PlanningContext.createDeterministic(100);
   * expect(ctx.nextId()).toBe(101);
   */
  static createDeterministic(startId: number = 0, contextId: string = 'test'): PlanningContext {
    const ctx = new PlanningContext(contextId);
    ctx.idCounter = startId;
    return ctx;
  }
}

/**
 * Factory function to create a new planning context.
 * Preferred over direct constructor for cleaner API.
 *
 * @param contextId - Optional context identifier
 * @returns A new PlanningContext instance
 */
export function createPlanningContext(contextId?: string): PlanningContext {
  return new PlanningContext(contextId);
}

/**
 * Generate a unique context ID.
 * Uses crypto.randomUUID() if available, falls back to timestamp + random.
 */
function generateContextId(): string {
  // Use crypto.randomUUID() if available (modern browsers, Node 19+, Cloudflare Workers)
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  // Fallback for environments without crypto.randomUUID
  return `ctx_${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;
}

/**
 * Default global context for backwards compatibility.
 * WARNING: Using this context shares state across all callers and is not recommended
 * for production use with concurrent planning. It exists only for gradual migration.
 *
 * @deprecated Use createPlanningContext() for new code
 */
let _defaultContext: PlanningContext | null = null;

/**
 * Get the default (shared) planning context.
 * This maintains backwards compatibility with code using the global counter.
 *
 * @deprecated Use createPlanningContext() for isolated contexts
 */
export function getDefaultPlanningContext(): PlanningContext {
  if (!_defaultContext) {
    _defaultContext = new PlanningContext('default');
  }
  return _defaultContext;
}

/**
 * Reset the default planning context.
 * This is equivalent to the old resetPlanNodeIds() behavior.
 *
 * @deprecated Use isolated PlanningContext instances instead
 */
export function resetDefaultPlanningContext(): void {
  _defaultContext = null;
}
