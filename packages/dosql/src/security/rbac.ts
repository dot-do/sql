/**
 * DoSQL Security Module - Role-Based Access Control (RBAC)
 *
 * Implements role management, privilege grants, and authorization checks.
 * Follows PostgreSQL-style RBAC semantics:
 *
 * - CREATE ROLE: Create roles with configurable attributes
 * - GRANT/REVOKE: Manage object-level privileges
 * - Role inheritance: Roles can inherit privileges from other roles
 * - WITH GRANT OPTION: Delegated privilege management
 *
 * @packageDocumentation
 */

import type {
  Role,
  CreateRoleOptions,
  Grant,
  GrantTarget,
  Privilege,
  SecurityContext,
  SecurityErrorCode,
} from './types.js';
import { expandPrivileges } from './types.js';

// =============================================================================
// SECURITY ERROR
// =============================================================================

/**
 * Error thrown by the security module
 */
export class SecurityError extends Error {
  readonly code: SecurityErrorCode;

  constructor(code: SecurityErrorCode, message: string) {
    super(message);
    this.name = 'SecurityError';
    this.code = code;
  }
}

// =============================================================================
// RBAC MANAGER
// =============================================================================

/**
 * Role-Based Access Control Manager
 *
 * Manages roles, grants, and authorization checks for the database.
 * All state is in-memory; persistence is handled by the caller
 * (e.g., stored in DO storage or serialized to the catalog).
 */
export class RBACManager {
  /** All registered roles, keyed by role name */
  private roles = new Map<string, Role>();

  /** All privilege grants */
  private grants: Grant[] = [];

  constructor() {
    // Create the default public role that all roles inherit from
    this.roles.set('public', {
      name: 'public',
      superuser: false,
      login: false,
      createDb: false,
      createRole: false,
      memberOf: [],
      createdAt: new Date(),
    });
  }

  // ===========================================================================
  // ROLE MANAGEMENT
  // ===========================================================================

  /**
   * Create a new role
   *
   * @throws SecurityError if role already exists
   */
  createRole(options: CreateRoleOptions): Role {
    if (this.roles.has(options.name)) {
      throw new SecurityError(
        'ROLE_ALREADY_EXISTS',
        `Role '${options.name}' already exists`,
      );
    }

    const role: Role = {
      name: options.name,
      superuser: options.superuser ?? false,
      login: options.login ?? false,
      createDb: options.createDb ?? false,
      createRole: options.createRole ?? false,
      memberOf: ['public'],
      createdAt: new Date(),
    };

    this.roles.set(role.name, role);
    return role;
  }

  /**
   * Drop a role
   *
   * @throws SecurityError if role does not exist or is 'public'
   */
  dropRole(roleName: string): void {
    if (roleName === 'public') {
      throw new SecurityError(
        'PERMISSION_DENIED',
        "Cannot drop the 'public' role",
      );
    }

    if (!this.roles.has(roleName)) {
      throw new SecurityError(
        'ROLE_NOT_FOUND',
        `Role '${roleName}' does not exist`,
      );
    }

    // Remove grants involving this role
    this.grants = this.grants.filter(
      (g) => g.role !== roleName && g.grantedBy !== roleName,
    );

    // Remove from other roles' memberOf lists
    for (const role of this.roles.values()) {
      role.memberOf = role.memberOf.filter((m) => m !== roleName);
    }

    this.roles.delete(roleName);
  }

  /**
   * Get a role by name
   */
  getRole(roleName: string): Role | undefined {
    return this.roles.get(roleName);
  }

  /**
   * List all roles
   */
  listRoles(): Role[] {
    return Array.from(this.roles.values());
  }

  /**
   * Grant role membership (role inheritance)
   *
   * @param roleName - The role to add membership to
   * @param memberRole - The role that becomes a member
   * @throws SecurityError if either role doesn't exist
   */
  grantRoleMembership(roleName: string, memberRole: string): void {
    const role = this.roles.get(memberRole);
    if (!role) {
      throw new SecurityError(
        'ROLE_NOT_FOUND',
        `Role '${memberRole}' does not exist`,
      );
    }

    if (!this.roles.has(roleName)) {
      throw new SecurityError(
        'ROLE_NOT_FOUND',
        `Role '${roleName}' does not exist`,
      );
    }

    // Check for circular membership
    if (this.hasRoleMembership(roleName, memberRole)) {
      throw new SecurityError(
        'INVALID_GRANT',
        `Circular role membership: '${roleName}' is already a member of '${memberRole}'`,
      );
    }

    if (!role.memberOf.includes(roleName)) {
      role.memberOf.push(roleName);
    }
  }

  /**
   * Revoke role membership
   */
  revokeRoleMembership(roleName: string, memberRole: string): void {
    const role = this.roles.get(memberRole);
    if (!role) {
      throw new SecurityError(
        'ROLE_NOT_FOUND',
        `Role '${memberRole}' does not exist`,
      );
    }

    role.memberOf = role.memberOf.filter((m) => m !== roleName);
  }

  /**
   * Check if a role has transitive membership in another role
   */
  hasRoleMembership(targetRole: string, roleName: string, visited = new Set<string>()): boolean {
    if (visited.has(roleName)) return false;
    visited.add(roleName);

    const role = this.roles.get(roleName);
    if (!role) return false;

    if (role.memberOf.includes(targetRole)) return true;

    for (const parent of role.memberOf) {
      if (this.hasRoleMembership(targetRole, parent, visited)) return true;
    }

    return false;
  }

  /**
   * Get all effective roles for a given role (including inherited roles)
   */
  getEffectiveRoles(roleName: string): string[] {
    const effective = new Set<string>();
    this.collectEffectiveRoles(roleName, effective);
    return Array.from(effective);
  }

  private collectEffectiveRoles(roleName: string, effective: Set<string>): void {
    if (effective.has(roleName)) return;
    effective.add(roleName);

    const role = this.roles.get(roleName);
    if (!role) return;

    for (const parent of role.memberOf) {
      this.collectEffectiveRoles(parent, effective);
    }
  }

  // ===========================================================================
  // PRIVILEGE GRANTS
  // ===========================================================================

  /**
   * Grant privileges to a role
   *
   * @param privileges - The privileges to grant
   * @param target - What the privilege applies to (table, schema, database)
   * @param roleName - The role to grant to
   * @param grantedBy - Who is granting (for audit)
   * @param withGrantOption - Whether the grantee can further grant
   * @throws SecurityError if role doesn't exist
   */
  grant(
    privileges: Privilege[],
    target: GrantTarget,
    roleName: string,
    grantedBy: string,
    withGrantOption = false,
  ): void {
    if (!this.roles.has(roleName)) {
      throw new SecurityError(
        'ROLE_NOT_FOUND',
        `Role '${roleName}' does not exist`,
      );
    }

    const expanded = expandPrivileges(privileges);

    for (const privilege of expanded) {
      // Check if an identical grant already exists
      const exists = this.grants.some(
        (g) =>
          g.role === roleName &&
          g.privilege === privilege &&
          this.targetsEqual(g.target, target),
      );

      if (!exists) {
        this.grants.push({
          role: roleName,
          privilege,
          target,
          withGrantOption,
          grantedBy,
          grantedAt: new Date(),
        });
      }
    }
  }

  /**
   * Revoke privileges from a role
   */
  revoke(
    privileges: Privilege[],
    target: GrantTarget,
    roleName: string,
  ): void {
    const expanded = expandPrivileges(privileges);
    const expandedSet = new Set(expanded);

    this.grants = this.grants.filter(
      (g) =>
        !(
          g.role === roleName &&
          expandedSet.has(g.privilege) &&
          this.targetsEqual(g.target, target)
        ),
    );
  }

  /**
   * Check if a role has a specific privilege on a target
   */
  hasPrivilege(
    roleName: string,
    privilege: Privilege,
    target: GrantTarget,
  ): boolean {
    // Superusers bypass all checks
    const role = this.roles.get(roleName);
    if (role?.superuser) return true;

    // Get all effective roles
    const effectiveRoles = this.getEffectiveRoles(roleName);

    // Check grants for any effective role
    return this.grants.some(
      (g) =>
        effectiveRoles.includes(g.role) &&
        (g.privilege === privilege || g.privilege === 'ALL') &&
        (this.targetsEqual(g.target, target) || this.targetCovers(g.target, target)),
    );
  }

  /**
   * Check if a role has grant option for a privilege
   */
  hasGrantOption(
    roleName: string,
    privilege: Privilege,
    target: GrantTarget,
  ): boolean {
    const role = this.roles.get(roleName);
    if (role?.superuser) return true;

    const effectiveRoles = this.getEffectiveRoles(roleName);

    return this.grants.some(
      (g) =>
        effectiveRoles.includes(g.role) &&
        (g.privilege === privilege || g.privilege === 'ALL') &&
        (this.targetsEqual(g.target, target) || this.targetCovers(g.target, target)) &&
        g.withGrantOption,
    );
  }

  /**
   * Get all grants for a role (including inherited)
   */
  getGrantsForRole(roleName: string): Grant[] {
    const effectiveRoles = this.getEffectiveRoles(roleName);
    return this.grants.filter((g) => effectiveRoles.includes(g.role));
  }

  /**
   * Get all grants on a target
   */
  getGrantsOnTarget(target: GrantTarget): Grant[] {
    return this.grants.filter((g) => this.targetsEqual(g.target, target));
  }

  // ===========================================================================
  // AUTHORIZATION CHECK
  // ===========================================================================

  /**
   * Check authorization and throw if denied
   *
   * @throws SecurityError with PERMISSION_DENIED
   */
  authorize(
    roleName: string,
    privilege: Privilege,
    target: GrantTarget,
  ): void {
    if (!this.hasPrivilege(roleName, privilege, target)) {
      const targetDesc = this.describeTarget(target);
      throw new SecurityError(
        'PERMISSION_DENIED',
        `Permission denied: role '${roleName}' does not have ${privilege} privilege on ${targetDesc}`,
      );
    }
  }

  /**
   * Build a SecurityContext for a role
   */
  buildSecurityContext(roleName: string): SecurityContext {
    const role = this.roles.get(roleName);
    if (!role) {
      throw new SecurityError(
        'ROLE_NOT_FOUND',
        `Role '${roleName}' does not exist`,
      );
    }

    return {
      currentRole: roleName,
      effectiveRoles: this.getEffectiveRoles(roleName),
      bypassSecurity: role.superuser,
    };
  }

  // ===========================================================================
  // HELPERS
  // ===========================================================================

  private targetsEqual(a: GrantTarget, b: GrantTarget): boolean {
    if (a.type !== b.type) return false;
    if (a.type === 'database') return true;
    return (a as { name: string }).name === (b as { name: string }).name;
  }

  /**
   * Check if a broader target covers a more specific one.
   * e.g., a database grant covers all tables
   */
  private targetCovers(broader: GrantTarget, specific: GrantTarget): boolean {
    if (broader.type === 'database') return true;
    if (broader.type === 'schema' && specific.type === 'table') {
      // Schema-level grants cover tables within that schema
      return true;
    }
    return false;
  }

  private describeTarget(target: GrantTarget): string {
    switch (target.type) {
      case 'table':
        return `table '${target.name}'`;
      case 'schema':
        return `schema '${target.name}'`;
      case 'database':
        return 'database';
    }
  }
}
