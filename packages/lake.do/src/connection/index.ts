/**
 * lake.do - Connection Management Module
 *
 * This module provides WebSocket connection management with:
 * - Event emission for connection lifecycle
 * - Pending request tracking
 * - Connection state management
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Connection types for WebSocket management.
 * @public
 * @stability stable
 */
export type {
  ConnectionEventType,
  ConnectionEventHandler,
  ConnectionConfig,
  PendingRequest,
} from './types.js';

// =============================================================================
// Event Emitter
// =============================================================================

/**
 * Event emitter for connection lifecycle events.
 * @public
 * @stability stable
 */
export { ConnectionEventEmitter } from './event-emitter.js';

// =============================================================================
// Connection Manager
// =============================================================================

/**
 * WebSocket connection manager and error types.
 * @public
 * @stability stable
 */
export {
  WebSocketConnectionManager,
  ConnectionError,
} from './manager.js';
