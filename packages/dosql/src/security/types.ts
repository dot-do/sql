/**
 * DoSQL Security Module - Types
 *
 * Defines interfaces for Role-Based Access Control (RBAC) and
 * Row-Level Security (RLS) policies.
 *
 * @packageDocumentation
 */

import type { Expression } from '../engine/types.js';

// =============================================================================
// PRIVILEGE TYPES
// =============================================================================

/**
 * Object-level privileges that can be granted to roles.
 * Follows PostgreSQL-style privilege naming.
 */
export type Privilege =
  | 'SELECT'
  | 'INSERT'
  | 'UPDATE'
  | 'DELETE'
  | 'CREATE'
  | 'DROP'
  | 'ALTER'
  | 'ALL';

/**
 * Expand ALL into individual privileges
 */
export function expandPrivileges(privileges: Privilege[]): Privilege[] {
  const expanded = new Set<Privilege>();
  for (const priv of privileges) {
    if (priv === 'ALL') {
      expanded.add('SELECT');
      expanded.add('INSERT');
      expanded.add('UPDATE');
      expanded.add('DELETE');
      expanded.add('CREATE');
      expanded.add('DROP');
      expanded.add('ALTER');
    } else {
      expanded.add(priv);
    }
  }
  return Array.from(expanded);
}

// =============================================================================
// ROLE TYPES
// =============================================================================

/**
 * A database role (user or group)
 */
export interface Role {
  /** Unique role name */
  name: string;
  /** Whether this is a superuser role (bypasses all checks) */
  superuser: boolean;
  /** Whether the role can log in (user vs group) */
  login: boolean;
  /** Whether the role can create databases */
  createDb: boolean;
  /** Whether the role can create other roles */
  createRole: boolean;
  /** Roles this role inherits privileges from */
  memberOf: string[];
  /** When the role was created */
  createdAt: Date;
}

/**
 * Options for creating a new role
 */
export interface CreateRoleOptions {
  name: string;
  superuser?: boolean;
  login?: boolean;
  createDb?: boolean;
  createRole?: boolean;
  password?: string;
}

// =============================================================================
// GRANT TYPES
// =============================================================================

/**
 * Scope of a privilege grant - what it applies to
 */
export type GrantTarget =
  | { type: 'table'; name: string }
  | { type: 'schema'; name: string }
  | { type: 'database' };

/**
 * A privilege grant record
 */
export interface Grant {
  /** The role receiving the privilege */
  role: string;
  /** The privilege being granted */
  privilege: Privilege;
  /** What the privilege applies to */
  target: GrantTarget;
  /** Whether the grantee can further grant this privilege */
  withGrantOption: boolean;
  /** Who granted this privilege */
  grantedBy: string;
  /** When the grant was made */
  grantedAt: Date;
}

// =============================================================================
// ROW-LEVEL SECURITY TYPES
// =============================================================================

/**
 * The command types a policy can apply to
 */
export type PolicyCommand = 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE' | 'ALL';

/**
 * Whether a policy is permissive or restrictive.
 * - PERMISSIVE: row is visible if ANY permissive policy matches (OR)
 * - RESTRICTIVE: row is visible only if ALL restrictive policies match (AND)
 */
export type PolicyType = 'PERMISSIVE' | 'RESTRICTIVE';

/**
 * A Row-Level Security policy (CREATE POLICY equivalent)
 */
export interface RLSPolicy {
  /** Unique policy name */
  name: string;
  /** Table the policy applies to */
  table: string;
  /** Which command types the policy applies to */
  command: PolicyCommand;
  /** Whether the policy is permissive or restrictive */
  policyType: PolicyType;
  /** Roles this policy applies to (empty = all roles) */
  roles: string[];
  /**
   * USING expression - filters rows for SELECT/UPDATE/DELETE visibility.
   * Stored as a string expression that references current_user and row columns.
   */
  using?: string;
  /**
   * WITH CHECK expression - validates new/modified rows for INSERT/UPDATE.
   * Stored as a string expression.
   */
  withCheck?: string;
  /** Whether the policy is enabled */
  enabled: boolean;
  /** When the policy was created */
  createdAt: Date;
}

/**
 * Options for creating a new RLS policy
 */
export interface CreatePolicyOptions {
  name: string;
  table: string;
  command?: PolicyCommand;
  policyType?: PolicyType;
  roles?: string[];
  using?: string;
  withCheck?: string;
}

/**
 * Table-level RLS enablement state
 */
export interface TableRLSState {
  /** The table name */
  table: string;
  /** Whether RLS is enabled on this table */
  enabled: boolean;
  /** Whether to force RLS for table owner too */
  forceRowSecurity: boolean;
}

// =============================================================================
// SECURITY CONTEXT
// =============================================================================

/**
 * Security context for query execution.
 * Passed alongside ExecutionContext to enforce security.
 */
export interface SecurityContext {
  /** The current authenticated role */
  currentRole: string;
  /** All effective roles (including inherited) */
  effectiveRoles: string[];
  /** Whether to bypass all security checks (superuser) */
  bypassSecurity: boolean;
}

// =============================================================================
// SECURITY ERROR TYPES
// =============================================================================

/**
 * Error codes specific to the security module
 */
export type SecurityErrorCode =
  | 'ROLE_NOT_FOUND'
  | 'ROLE_ALREADY_EXISTS'
  | 'PERMISSION_DENIED'
  | 'POLICY_NOT_FOUND'
  | 'POLICY_ALREADY_EXISTS'
  | 'RLS_VIOLATION'
  | 'INVALID_GRANT'
  | 'INSUFFICIENT_PRIVILEGE';
