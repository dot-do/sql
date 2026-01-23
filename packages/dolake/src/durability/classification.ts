/**
 * Event Classification
 *
 * Single responsibility: Classifying CDC events into durability tiers.
 */

import type { CDCEvent } from '../types.js';
import { DurabilityTier } from './types.js';

// =============================================================================
// Classification Tables
// =============================================================================

/**
 * Tables that are classified as P0 (critical)
 */
const P0_TABLES = new Set([
  'stripe_webhooks',
  'payments',
  'payment_intents',
  'transactions',
  'orders',
  'subscriptions',
  'invoices',
  'refunds',
  'disputes',
]);

/**
 * Tables that are classified as P1 (important)
 */
const P1_TABLES = new Set([
  'users',
  'user_actions',
  'user_events',
  'accounts',
  'signups',
  'registrations',
  'authentications',
  'sessions',
  'api_keys',
  'permissions',
  'roles',
  'audit_logs',
]);

/**
 * Tables that are classified as P2 (standard)
 */
const P2_TABLES = new Set([
  'analytics_events',
  'analytics',
  'telemetry',
  'metrics',
  'logs',
  'events',
  'page_views',
  'clicks',
  'impressions',
]);

/**
 * Tables that are classified as P3 (best-effort)
 */
const P3_TABLES = new Set([
  'anonymous_visits',
  'anonymous_events',
  'tracking',
  'bot_visits',
  'health_checks',
  'ping',
]);

/**
 * Sources that imply P0 (critical)
 */
const P0_SOURCES = new Set([
  'stripe',
  'payment',
  'billing',
  'financial',
]);

/**
 * Sources that imply P1 (important)
 */
const P1_SOURCES = new Set([
  'auth',
  'authentication',
  'user',
  'account',
]);

/**
 * Sources that imply P3 (best-effort)
 */
const P3_SOURCES = new Set([
  'tracking',
  'anonymous',
  'bot',
]);

// =============================================================================
// Classification Functions
// =============================================================================

/**
 * Classify an event into a durability tier based on its properties.
 *
 * Classification priority:
 * 1. Explicit durability metadata (highest priority)
 * 2. Table name matching
 * 3. Source matching
 * 4. Default to P2 (standard)
 *
 * @param event - The CDC event to classify
 * @returns The durability tier for the event
 */
export function classifyEvent(event: CDCEvent): DurabilityTier {
  // 1. Check for explicit durability metadata (highest priority)
  const explicitDurability = event.metadata?.durability as string | undefined;
  if (explicitDurability) {
    const tier = explicitDurability.toUpperCase();
    if (tier === 'P0' || tier === 'P1' || tier === 'P2' || tier === 'P3') {
      return tier as DurabilityTier;
    }
  }

  // 2. Check table name
  const tableName = event.table.toLowerCase();

  if (P0_TABLES.has(tableName)) {
    return DurabilityTier.P0;
  }
  if (P1_TABLES.has(tableName)) {
    return DurabilityTier.P1;
  }
  if (P2_TABLES.has(tableName)) {
    return DurabilityTier.P2;
  }
  if (P3_TABLES.has(tableName)) {
    return DurabilityTier.P3;
  }

  // Check for partial table name matches
  if (tableName.includes('payment') || tableName.includes('stripe') || tableName.includes('transaction')) {
    return DurabilityTier.P0;
  }
  if (tableName.includes('user') || tableName.includes('auth') || tableName.includes('account')) {
    return DurabilityTier.P1;
  }
  if (tableName.includes('analytics') || tableName.includes('telemetry') || tableName.includes('metric')) {
    return DurabilityTier.P2;
  }
  if (tableName.includes('anonymous') || tableName.includes('tracking') || tableName.includes('visit')) {
    return DurabilityTier.P3;
  }

  // 3. Check source metadata
  const source = (event.metadata?.source as string | undefined)?.toLowerCase();
  if (source) {
    if (P0_SOURCES.has(source)) {
      return DurabilityTier.P0;
    }
    if (P1_SOURCES.has(source)) {
      return DurabilityTier.P1;
    }
    if (P3_SOURCES.has(source)) {
      return DurabilityTier.P3;
    }
  }

  // 4. Default to P2 (standard)
  return DurabilityTier.P2;
}

/**
 * Classify multiple events and return their classifications
 *
 * @param events - Array of CDC events to classify
 * @returns Array of classification results
 */
export function classifyEvents(events: CDCEvent[]): Array<{
  event: CDCEvent;
  tier: DurabilityTier;
}> {
  return events.map((event) => ({
    event,
    tier: classifyEvent(event),
  }));
}

// =============================================================================
// Classification Table Management
// =============================================================================

/**
 * Check if a table is in a specific tier's classification
 */
export function isTableInTier(tableName: string, tier: DurabilityTier): boolean {
  const normalized = tableName.toLowerCase();
  switch (tier) {
    case DurabilityTier.P0:
      return P0_TABLES.has(normalized);
    case DurabilityTier.P1:
      return P1_TABLES.has(normalized);
    case DurabilityTier.P2:
      return P2_TABLES.has(normalized);
    case DurabilityTier.P3:
      return P3_TABLES.has(normalized);
    default:
      return false;
  }
}

/**
 * Get all tables in a specific tier
 */
export function getTablesInTier(tier: DurabilityTier): string[] {
  switch (tier) {
    case DurabilityTier.P0:
      return Array.from(P0_TABLES);
    case DurabilityTier.P1:
      return Array.from(P1_TABLES);
    case DurabilityTier.P2:
      return Array.from(P2_TABLES);
    case DurabilityTier.P3:
      return Array.from(P3_TABLES);
    default:
      return [];
  }
}
