/**
 * lake.do - Client Event Emitter
 *
 * This module provides a typed event emitter for the DoLakeClient.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

// =============================================================================
// Event Types
// =============================================================================

/**
 * Event types emitted by the DoLakeClient.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type LakeClientEventType = 'connected' | 'disconnected' | 'reconnecting' | 'reconnected' | 'error';

/**
 * Event handler function type for client events.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type LakeClientEventHandler<T = unknown> = (event?: T) => void;

/**
 * Event map for type-safe event handling.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface LakeClientEventMap {
  connected: void;
  disconnected: void;
  reconnecting: { attempt: number; delay: number };
  reconnected: { attempts: number };
  error: Error;
}

// =============================================================================
// Event Emitter Implementation
// =============================================================================

/**
 * Typed event emitter for the DoLakeClient.
 *
 * @description Provides a simple, type-safe event emitter implementation
 * using the EventEmitter pattern. This replaces manual Map-based event handling
 * with a reusable class.
 *
 * @example
 * ```typescript
 * const emitter = new LakeClientEventEmitter();
 *
 * emitter.on('connected', () => console.log('Connected!'));
 * emitter.on('error', (error) => console.error('Error:', error));
 *
 * emitter.emit('connected');
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class LakeClientEventEmitter {
  private readonly listeners = new Map<LakeClientEventType, Set<LakeClientEventHandler>>();

  /**
   * Registers an event listener for the specified event type.
   *
   * @description Adds a callback function that will be called when the
   * specified event is emitted. Multiple listeners can be registered for
   * the same event.
   *
   * @param event - The event type to listen for
   * @param handler - The callback function to invoke when the event occurs
   *
   * @example
   * ```typescript
   * emitter.on('connected', () => console.log('Connected!'));
   * emitter.on('error', (error) => console.error('Error:', error));
   * ```
   */
  on(event: LakeClientEventType, handler: LakeClientEventHandler): void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(handler);
  }

  /**
   * Removes an event listener for the specified event type.
   *
   * @description Removes a previously registered callback function.
   * If the handler was not registered, this method does nothing.
   *
   * @param event - The event type to remove the listener from
   * @param handler - The callback function to remove
   *
   * @example
   * ```typescript
   * const handler = () => console.log('Connected!');
   * emitter.on('connected', handler);
   * // Later...
   * emitter.off('connected', handler);
   * ```
   */
  off(event: LakeClientEventType, handler: LakeClientEventHandler): void {
    const handlers = this.listeners.get(event);
    if (handlers) {
      handlers.delete(handler);
    }
  }

  /**
   * Registers a one-time event listener for the specified event type.
   *
   * @description Adds a callback function that will be called only once
   * when the specified event is emitted, then automatically removed.
   *
   * @param event - The event type to listen for
   * @param handler - The callback function to invoke when the event occurs
   *
   * @example
   * ```typescript
   * emitter.once('connected', () => {
   *   console.log('First connection established!');
   * });
   * ```
   */
  once(event: LakeClientEventType, handler: LakeClientEventHandler): void {
    const onceHandler: LakeClientEventHandler = (data) => {
      this.off(event, onceHandler);
      handler(data);
    };
    this.on(event, onceHandler);
  }

  /**
   * Emits an event to all registered listeners.
   *
   * @param event - The event type to emit
   * @param data - Optional data to pass to the listeners
   *
   * @example
   * ```typescript
   * emitter.emit('connected');
   * emitter.emit('error', new Error('Connection failed'));
   * ```
   */
  emit(event: LakeClientEventType, data?: unknown): void {
    const handlers = this.listeners.get(event);
    if (handlers) {
      for (const handler of handlers) {
        try {
          handler(data);
        } catch (error) {
          console.error(`Error in ${event} event handler:`, error);
        }
      }
    }
  }

  /**
   * Removes all listeners for an event, or all listeners if no event specified.
   *
   * @param event - Optional event type to clear listeners for
   *
   * @example
   * ```typescript
   * // Remove all 'error' listeners
   * emitter.clear('error');
   *
   * // Remove all listeners
   * emitter.clear();
   * ```
   */
  clear(event?: LakeClientEventType): void {
    if (event) {
      this.listeners.delete(event);
    } else {
      this.listeners.clear();
    }
  }

  /**
   * Returns the number of listeners for an event.
   *
   * @param event - The event type
   * @returns Number of listeners
   *
   * @example
   * ```typescript
   * console.log(`${emitter.listenerCount('error')} error handlers registered`);
   * ```
   */
  listenerCount(event: LakeClientEventType): number {
    return this.listeners.get(event)?.size ?? 0;
  }

  /**
   * Checks if there are any listeners for an event.
   *
   * @param event - The event type
   * @returns True if there are listeners, false otherwise
   */
  hasListeners(event: LakeClientEventType): boolean {
    return this.listenerCount(event) > 0;
  }
}
