/**
 * @dotdo/lake.do - Connection Management Module
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

export type {
  ConnectionEventType,
  ConnectionEventHandler,
  ConnectionConfig,
  PendingRequest,
} from './types.js';

// =============================================================================
// Event Emitter
// =============================================================================

export { ConnectionEventEmitter } from './event-emitter.js';

// =============================================================================
// Connection Manager
// =============================================================================

export {
  WebSocketConnectionManager,
  ConnectionError,
} from './manager.js';
