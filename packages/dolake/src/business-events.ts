/**
 * DoLake Business Events Ingestion
 *
 * Handles business event ingestion including Stripe webhooks with P0 durability.
 *
 * Features:
 * - HTTP POST endpoint for webhooks at /v1/webhooks/stripe
 * - Stripe signature verification (using stripe-signature header)
 * - Event normalization to CDC format
 * - P0 durability tier (dual-write to R2+KV)
 * - Idempotency via event ID deduplication
 *
 * Issue: do-d1isn.7 - Business events ingestion (Stripe webhooks)
 */

import type { R2Storage, KVStorage } from './durability.js';

// =============================================================================
// Stripe Event Types
// =============================================================================

/**
 * Stripe webhook event structure
 */
export interface StripeEvent {
  id: string;
  object: 'event';
  api_version: string;
  created: number;
  type: string;
  livemode: boolean;
  pending_webhooks: number;
  request: {
    id: string | null;
    idempotency_key: string | null;
  };
  data: {
    object: Record<string, unknown>;
    previous_attributes?: Record<string, unknown>;
  };
}

/**
 * Extended KV storage interface with TTL support
 */
export interface ExtendedKVStorage extends KVStorage {
  write(key: string, value: string, options?: { expirationTtl?: number }): Promise<void>;
  list(prefix: string): Promise<string[]>;
}

// =============================================================================
// CDC Event Types (Normalized from Stripe)
// =============================================================================

/**
 * Normalized CDC event from Stripe webhook
 */
export interface NormalizedCDCEvent {
  /** Event type marker */
  $type: 'stripe.webhook';
  /** Original Stripe event ID */
  eventId: string;
  /** Stripe event type (e.g., payment_intent.succeeded) */
  eventType: string;
  /** Timestamp (Unix seconds) */
  timestamp: number;
  /** CDC operation type */
  operation: 'INSERT';
  /** Target table */
  table: string;
  /** Row identifier (event ID) */
  rowId: string;
  /** Sequence number derived from event ID */
  sequence: number;
  /** Event payload (data.object from Stripe) */
  payload: Record<string, unknown>;
  /** Payload after the change (alias for payload) */
  after: Record<string, unknown>;
  /** Durability tier */
  durabilityTier: 'P0';
  /** Metadata */
  metadata: {
    source: 'stripe';
    durability: 'P0';
    idempotencyKey: string;
    livemode: boolean;
    apiVersion: string;
    stripeRequestId: string | null;
    stripeIdempotencyKey: string | null;
  };
}

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Configuration for Stripe webhook handler
 */
export interface StripeWebhookConfig {
  /** Stripe webhook signing secret */
  webhookSecret: string;
  /** KV storage backend */
  kv: ExtendedKVStorage;
  /** R2 storage backend */
  r2: R2Storage;
  /** Signature tolerance in seconds (default: 300 = 5 minutes) */
  signatureToleranceSeconds?: number;
  /** Maximum retries for R2 writes (default: 100) */
  maxR2Retries?: number;
  /** Base retry delay in ms (default: 100) */
  baseRetryDelayMs?: number;
  /** Maximum retry delay in ms (default: 30000) */
  maxRetryDelayMs?: number;
  /** DLQ path prefix (default: 'dlq/') */
  dlqPathPrefix?: string;
  /** Idempotency TTL in seconds (default: 86400 = 24 hours) */
  idempotencyTtlSeconds?: number;
}

/**
 * Default configuration values
 */
export const DEFAULT_STRIPE_WEBHOOK_CONFIG = {
  signatureToleranceSeconds: 300,
  maxR2Retries: 100,
  baseRetryDelayMs: 100,
  maxRetryDelayMs: 30000,
  dlqPathPrefix: 'dlq/',
  idempotencyTtlSeconds: 86400,
} as const;

// =============================================================================
// Result Types
// =============================================================================

/**
 * Result of webhook handling
 */
export interface StripeWebhookResult {
  /** HTTP status code */
  status: number;
  /** Whether event was received successfully */
  received: boolean;
  /** Event ID if available */
  eventId?: string | undefined;
  /** Event type if available */
  eventType?: string | undefined;
  /** Whether this was a duplicate event */
  duplicate: boolean;
  /** Whether written to R2 */
  writtenToR2: boolean;
  /** Whether written to KV */
  writtenToKV: boolean;
  /** Error message if any */
  error?: string | undefined;
  /** KV error if KV write failed */
  kvError?: string | undefined;
  /** Number of R2 retries */
  retryCount: number;
  /** Retry delays in ms */
  retryDelays?: number[] | undefined;
  /** Whether sent to DLQ */
  sentToDLQ: boolean;
  /** DLQ path if sent */
  dlqPath?: string | undefined;
  /** Processing time in ms */
  processingTimeMs: number;
}

/**
 * Result of signature verification
 */
export interface SignatureVerificationResult {
  /** Whether signature is valid */
  valid: boolean;
  /** Error message if invalid */
  error?: string | undefined;
  /** Timestamp from signature */
  timestamp?: number | undefined;
}

// =============================================================================
// Stripe Signature Verification
// =============================================================================

/**
 * Verify Stripe webhook signature
 *
 * Stripe signatures use HMAC-SHA256 and include a timestamp to prevent replay attacks.
 * Format: t=timestamp,v1=signature[,v1=signature...]
 *
 * @param payload - Raw request body
 * @param signatureHeader - stripe-signature header value
 * @param secret - Webhook signing secret
 * @param toleranceSeconds - Maximum age of timestamp (default: 300 = 5 minutes)
 */
export async function verifyStripeSignature(
  payload: string,
  signatureHeader: string,
  secret: string,
  toleranceSeconds: number = 300
): Promise<SignatureVerificationResult> {
  // Parse signature header
  const parts = signatureHeader.split(',');
  let timestamp: number | undefined;
  const signatures: string[] = [];

  for (const part of parts) {
    const [key, value] = part.split('=');
    if (!key || !value) continue;
    if (key === 't') {
      const parsedTimestamp = parseInt(value, 10);
      if (isNaN(parsedTimestamp)) {
        return { valid: false, error: 'Invalid timestamp in signature header' };
      }
      timestamp = parsedTimestamp;
    } else if (key === 'v1') {
      signatures.push(value);
    }
  }

  // Validate parsed values
  if (timestamp === undefined) {
    return { valid: false, error: 'Missing timestamp in signature header' };
  }

  if (signatures.length === 0) {
    return { valid: false, error: 'Missing v1 signature in signature header' };
  }

  // Check timestamp tolerance
  const now = Math.floor(Date.now() / 1000);
  const age = now - timestamp;
  if (age > toleranceSeconds) {
    return { valid: false, error: 'Signature timestamp expired', timestamp };
  }

  // Compute expected signature
  const signedPayload = `${timestamp}.${payload}`;
  const encoder = new TextEncoder();

  try {
    const cryptoKey = await crypto.subtle.importKey(
      'raw',
      encoder.encode(secret),
      { name: 'HMAC', hash: 'SHA-256' },
      false,
      ['sign']
    );

    const expectedSignatureBuffer = await crypto.subtle.sign(
      'HMAC',
      cryptoKey,
      encoder.encode(signedPayload)
    );

    const expectedSignatureArray = Array.from(new Uint8Array(expectedSignatureBuffer));
    const expectedSignatureHex = expectedSignatureArray
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');

    // Check if any signature matches (constant-time comparison)
    let valid = false;
    for (const sig of signatures) {
      if (constantTimeCompare(sig, expectedSignatureHex)) {
        valid = true;
        break;
      }
    }

    if (!valid) {
      return { valid: false, error: 'Invalid signature', timestamp };
    }

    return { valid: true, timestamp };
  } catch (error) {
    return {
      valid: false,
      error: `Signature verification error: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
}

/**
 * Constant-time string comparison to prevent timing attacks
 */
function constantTimeCompare(a: string, b: string): boolean {
  if (a.length !== b.length) {
    return false;
  }

  let result = 0;
  for (let i = 0; i < a.length; i++) {
    result |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }

  return result === 0;
}

// =============================================================================
// Event Normalization
// =============================================================================

/**
 * Normalize Stripe event to CDC format
 *
 * Converts a Stripe webhook event into a CDC-compatible event structure
 * suitable for ingestion into the lakehouse.
 */
export function normalizeStripeEventToCDC(stripeEvent: StripeEvent): NormalizedCDCEvent {
  // Generate sequence from event ID hash
  const sequence = hashToSequence(stripeEvent.id);

  const payload = stripeEvent.data.object;

  return {
    $type: 'stripe.webhook',
    eventId: stripeEvent.id,
    eventType: stripeEvent.type,
    timestamp: stripeEvent.created,
    operation: 'INSERT',
    table: 'stripe_webhooks',
    rowId: stripeEvent.id,
    sequence,
    payload,
    after: payload,
    durabilityTier: 'P0',
    metadata: {
      source: 'stripe',
      durability: 'P0',
      idempotencyKey: stripeEvent.id,
      livemode: stripeEvent.livemode,
      apiVersion: stripeEvent.api_version,
      stripeRequestId: stripeEvent.request?.id ?? null,
      stripeIdempotencyKey: stripeEvent.request?.idempotency_key ?? null,
    },
  };
}

/**
 * Convert string to a deterministic sequence number
 */
function hashToSequence(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return Math.abs(hash);
}

// =============================================================================
// Stripe Webhook Handler
// =============================================================================

/**
 * Handler for Stripe webhook events
 *
 * Provides:
 * - Signature verification
 * - Event normalization to CDC format
 * - P0 durability (dual-write to R2+KV)
 * - Idempotency via event ID deduplication
 */
export class StripeWebhookHandler {
  private webhookSecret: string;
  private kv: ExtendedKVStorage;
  private r2: R2Storage;
  private signatureToleranceSeconds: number;
  private maxR2Retries: number;
  private baseRetryDelayMs: number;
  private maxRetryDelayMs: number;
  private dlqPathPrefix: string;
  private idempotencyTtlSeconds: number;

  // In-memory lock for concurrent duplicate prevention
  private processingEvents = new Set<string>();

  constructor(config: StripeWebhookConfig) {
    this.webhookSecret = config.webhookSecret;
    this.kv = config.kv;
    this.r2 = config.r2;
    this.signatureToleranceSeconds =
      config.signatureToleranceSeconds ?? DEFAULT_STRIPE_WEBHOOK_CONFIG.signatureToleranceSeconds;
    this.maxR2Retries = config.maxR2Retries ?? DEFAULT_STRIPE_WEBHOOK_CONFIG.maxR2Retries;
    this.baseRetryDelayMs = config.baseRetryDelayMs ?? DEFAULT_STRIPE_WEBHOOK_CONFIG.baseRetryDelayMs;
    this.maxRetryDelayMs = config.maxRetryDelayMs ?? DEFAULT_STRIPE_WEBHOOK_CONFIG.maxRetryDelayMs;
    this.dlqPathPrefix = config.dlqPathPrefix ?? DEFAULT_STRIPE_WEBHOOK_CONFIG.dlqPathPrefix;
    this.idempotencyTtlSeconds =
      config.idempotencyTtlSeconds ?? DEFAULT_STRIPE_WEBHOOK_CONFIG.idempotencyTtlSeconds;
  }

  /**
   * Handle incoming webhook request
   */
  async handleRequest(request: Request): Promise<StripeWebhookResult> {
    const startTime = Date.now();
    const result: StripeWebhookResult = {
      status: 200,
      received: false,
      duplicate: false,
      writtenToR2: false,
      writtenToKV: false,
      retryCount: 0,
      sentToDLQ: false,
      processingTimeMs: 0,
    };

    try {
      // 1. Validate HTTP method
      if (request.method !== 'POST') {
        result.status = 405;
        result.error = 'Method not allowed. Use POST.';
        return this.finalizeResult(result, startTime);
      }

      // 2. Get request body
      const payload = await request.text();
      if (!payload) {
        result.status = 400;
        result.error = 'Empty request body';
        return this.finalizeResult(result, startTime);
      }

      // 3. Validate signature
      const signatureHeader = request.headers.get('stripe-signature');
      if (!signatureHeader) {
        result.status = 401;
        result.error = 'Missing stripe-signature header';
        return this.finalizeResult(result, startTime);
      }

      const signatureResult = await verifyStripeSignature(
        payload,
        signatureHeader,
        this.webhookSecret,
        this.signatureToleranceSeconds
      );

      if (!signatureResult.valid) {
        result.status = 401;
        result.error = signatureResult.error ?? 'Invalid signature';
        return this.finalizeResult(result, startTime);
      }

      // 4. Parse JSON
      let stripeEvent: StripeEvent;
      try {
        stripeEvent = JSON.parse(payload);
      } catch {
        result.status = 400;
        result.error = 'Invalid JSON body';
        return this.finalizeResult(result, startTime);
      }

      // 5. Validate required fields
      if (!stripeEvent.id) {
        result.status = 400;
        result.error = 'Missing event id field';
        return this.finalizeResult(result, startTime);
      }

      if (!stripeEvent.type) {
        result.status = 400;
        result.error = 'Missing event type field';
        return this.finalizeResult(result, startTime);
      }

      result.eventId = stripeEvent.id;
      result.eventType = stripeEvent.type;

      // 6. Check idempotency (deduplication)
      const isDuplicate = await this.checkIdempotency(stripeEvent.id);
      if (isDuplicate) {
        result.received = true;
        result.duplicate = true;
        return this.finalizeResult(result, startTime);
      }

      // 7. Check for concurrent processing (in-memory lock)
      if (this.processingEvents.has(stripeEvent.id)) {
        result.received = true;
        result.duplicate = true;
        return this.finalizeResult(result, startTime);
      }

      // Add to processing set
      this.processingEvents.add(stripeEvent.id);

      try {
        // 8. Normalize to CDC format
        const cdcEvent = normalizeStripeEventToCDC(stripeEvent);

        // 9. P0 Dual-write (R2 + KV)
        const writeResult = await this.dualWrite(cdcEvent, stripeEvent.id);

        result.writtenToR2 = writeResult.r2Success;
        result.writtenToKV = writeResult.kvSuccess;
        result.retryCount = writeResult.retryCount;
        result.retryDelays = writeResult.retryDelays;
        result.sentToDLQ = writeResult.sentToDLQ;
        result.dlqPath = writeResult.dlqPath;
        result.kvError = writeResult.kvError;

        // 10. Store idempotency key
        await this.storeIdempotency(stripeEvent.id);

        result.received = true;
      } finally {
        // Remove from processing set
        this.processingEvents.delete(stripeEvent.id);
      }

      return this.finalizeResult(result, startTime);
    } catch (error) {
      result.status = 500;
      result.error = `Internal error: ${error instanceof Error ? error.message : String(error)}`;
      return this.finalizeResult(result, startTime);
    }
  }

  /**
   * Check if event has already been processed (idempotency check)
   */
  private async checkIdempotency(eventId: string): Promise<boolean> {
    try {
      const existing = await this.kv.read(`idempotency:${eventId}`);
      return existing !== null;
    } catch {
      // If KV read fails, assume not duplicate (allow processing)
      return false;
    }
  }

  /**
   * Store idempotency key after successful processing
   */
  private async storeIdempotency(eventId: string): Promise<void> {
    try {
      await this.kv.write(`idempotency:${eventId}`, 'true', {
        expirationTtl: this.idempotencyTtlSeconds,
      });
    } catch {
      // Idempotency store failure is not fatal
    }
  }

  /**
   * Perform P0 dual-write to R2 and KV
   */
  private async dualWrite(
    cdcEvent: NormalizedCDCEvent,
    eventId: string
  ): Promise<{
    r2Success: boolean;
    kvSuccess: boolean;
    retryCount: number;
    retryDelays: number[];
    sentToDLQ: boolean;
    dlqPath?: string | undefined;
    kvError?: string | undefined;
  }> {
    const eventData = new TextEncoder().encode(JSON.stringify(cdcEvent));
    const eventPath = `p0/stripe_webhooks/${eventId}.json`;

    let r2Success = false;
    let kvSuccess = false;
    let retryCount = 0;
    const retryDelays: number[] = [];
    let sentToDLQ = false;
    let dlqPath: string | undefined;
    let kvError: string | undefined;

    // Write to KV (part of dual-write, but not retry-dependent on R2)
    try {
      await this.kv.write(eventPath, JSON.stringify(cdcEvent), {
        expirationTtl: this.idempotencyTtlSeconds,
      });
      kvSuccess = true;
    } catch (error) {
      kvError = error instanceof Error ? error.message : String(error);
    }

    // Write to R2 with retries
    while (!r2Success && retryCount < this.maxR2Retries) {
      try {
        await this.r2.write(eventPath, eventData);
        r2Success = true;
      } catch {
        retryCount++;
        if (retryCount < this.maxR2Retries) {
          const delay = this.calculateBackoff(retryCount);
          retryDelays.push(delay);
          await this.delay(delay);
        }
      }
    }

    // If R2 permanently failed, send to DLQ
    if (!r2Success) {
      dlqPath = await this.sendToDLQ(cdcEvent, eventId);
      sentToDLQ = true;
    }

    return {
      r2Success,
      kvSuccess,
      retryCount,
      retryDelays,
      sentToDLQ,
      dlqPath,
      kvError,
    };
  }

  /**
   * Send event to dead letter queue
   */
  private async sendToDLQ(cdcEvent: NormalizedCDCEvent, eventId: string): Promise<string> {
    const timestamp = Date.now();
    const dlqPath = `${this.dlqPathPrefix}stripe_webhooks/${timestamp}_${eventId}.json`;

    try {
      await this.kv.write(dlqPath, JSON.stringify(cdcEvent), {
        expirationTtl: 7 * 24 * 60 * 60, // 7 days
      });
    } catch {
      // DLQ write failure is logged but not fatal
      // The event was already written to KV in the main path
    }

    return dlqPath;
  }

  /**
   * Calculate exponential backoff delay
   */
  private calculateBackoff(retryCount: number): number {
    const delay = this.baseRetryDelayMs * Math.pow(2, retryCount - 1);
    return Math.min(delay, this.maxRetryDelayMs);
  }

  /**
   * Delay helper
   */
  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Finalize result with processing time
   */
  private finalizeResult(result: StripeWebhookResult, startTime: number): StripeWebhookResult {
    result.processingTimeMs = Date.now() - startTime;
    return result;
  }
}
