/**
 * lake.do - Event Emitter
 *
 * This module provides a simple typed event emitter for connection events.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

import type { ConnectionEventType, ConnectionEventHandler } from './types.js';

// =============================================================================
// Event Emitter Implementation
// =============================================================================

/**
 * Simple typed event emitter for connection events.
 *
 * @description Provides basic event registration and emission capabilities
 * with type safety for event handlers.
 *
 * @example
 * ```typescript
 * const emitter = new ConnectionEventEmitter();
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
export class ConnectionEventEmitter {
  private readonly listeners = new Map<ConnectionEventType, Set<ConnectionEventHandler>>();

  /**
   * Registers an event listener.
   *
   * @param event - The event type to listen for
   * @param handler - The callback function to invoke
   */
  on(event: ConnectionEventType, handler: ConnectionEventHandler): void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(handler);
  }

  /**
   * Removes an event listener.
   *
   * @param event - The event type to remove the listener from
   * @param handler - The callback function to remove
   */
  off(event: ConnectionEventType, handler: ConnectionEventHandler): void {
    const handlers = this.listeners.get(event);
    if (handlers) {
      handlers.delete(handler);
    }
  }

  /**
   * Registers a one-time event listener.
   *
   * @param event - The event type to listen for
   * @param handler - The callback function to invoke once
   */
  once(event: ConnectionEventType, handler: ConnectionEventHandler): void {
    const onceHandler: ConnectionEventHandler = (data) => {
      this.off(event, onceHandler);
      handler(data);
    };
    this.on(event, onceHandler);
  }

  /**
   * Emits an event to all registered listeners.
   *
   * @param event - The event type to emit
   * @param data - Optional data to pass to listeners
   */
  emit(event: ConnectionEventType, data?: unknown): void {
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
   */
  clear(event?: ConnectionEventType): void {
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
   */
  listenerCount(event: ConnectionEventType): number {
    return this.listeners.get(event)?.size ?? 0;
  }
}
