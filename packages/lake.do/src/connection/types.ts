/**
 * lake.do - Connection Types
 *
 * This module provides types for WebSocket connection management.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

import type { RetryConfig } from '../types.js';

// =============================================================================
// Event Types
// =============================================================================

/**
 * Event types emitted by the connection manager.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type ConnectionEventType = 'connected' | 'disconnected' | 'reconnecting' | 'reconnected' | 'error' | 'message';

/**
 * Event handler function type for connection events.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type ConnectionEventHandler<T = unknown> = (data?: T) => void;

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Configuration options for connection management.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface ConnectionConfig {
  /** WebSocket URL (ws:// or wss://) */
  url: string;
  /** Authentication token */
  token?: string;
  /** Connection timeout in milliseconds */
  timeout: number;
  /** Retry configuration */
  retry: RetryConfig;
}

// =============================================================================
// Pending Request Types
// =============================================================================

/**
 * Pending RPC request entry.
 *
 * @internal
 */
export interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
}
