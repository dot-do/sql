/**
 * DoSQL Security Module Tests
 *
 * Comprehensive tests for RBAC and Row-Level Security:
 * - Role management (CREATE/DROP/list)
 * - Role inheritance and effective roles
 * - Privilege grants and revocations
 * - Authorization checks
 * - RLS policy creation and management
 * - Row visibility filtering (PERMISSIVE/RESTRICTIVE)
 * - WITH CHECK validation for INSERT/UPDATE
 * - RLS expression evaluation
 * - SecurityContext building
 * - Edge cases and error paths
 *
 * Following the project's TDD with NO MOCKS philosophy.
 */

import { describe, it, expect, beforeEach } from 'vitest';

import { RBACManager, SecurityError } from '../rbac.js';
import { RLSManager, evaluateRLSExpression } from '../rls.js';
import { expandPrivileges } from '../types.js';
import type {
  Role,
  Grant,
  GrantTarget,
  Privilege,
  SecurityContext,
  RLSPolicy,
  CreatePolicyOptions,
} from '../types.js';

// =============================================================================
// RBAC TESTS
// =============================================================================

describe('security - RBAC', () => {
  let rbac: RBACManager;

  beforeEach(() => {
    rbac = new RBACManager();
  });

  // ===========================================================================
  // ROLE MANAGEMENT
  // ===========================================================================

  describe('role management', () => {
    it('should have a default public role', () => {
      const pub = rbac.getRole('public');
      expect(pub).toBeDefined();
      expect(pub!.name).toBe('public');
      expect(pub!.superuser).toBe(false);
      expect(pub!.login).toBe(false);
    });

    it('should create a basic role', () => {
      const role = rbac.createRole({ name: 'reader' });
      expect(role.name).toBe('reader');
      expect(role.superuser).toBe(false);
      expect(role.login).toBe(false);
      expect(role.createDb).toBe(false);
      expect(role.createRole).toBe(false);
      expect(role.memberOf).toContain('public');
    });

    it('should create a role with all options', () => {
      const role = rbac.createRole({
        name: 'admin',
        superuser: true,
        login: true,
        createDb: true,
        createRole: true,
      });
      expect(role.superuser).toBe(true);
      expect(role.login).toBe(true);
      expect(role.createDb).toBe(true);
      expect(role.createRole).toBe(true);
    });

    it('should throw when creating a duplicate role', () => {
      rbac.createRole({ name: 'reader' });
      expect(() => rbac.createRole({ name: 'reader' })).toThrow(SecurityError);
      expect(() => rbac.createRole({ name: 'reader' })).toThrow(
        "Role 'reader' already exists",
      );
    });

    it('should list all roles', () => {
      rbac.createRole({ name: 'reader' });
      rbac.createRole({ name: 'writer' });
      const roles = rbac.listRoles();
      const names = roles.map((r) => r.name);
      expect(names).toContain('public');
      expect(names).toContain('reader');
      expect(names).toContain('writer');
      expect(roles.length).toBe(3);
    });

    it('should drop a role', () => {
      rbac.createRole({ name: 'temp_role' });
      expect(rbac.getRole('temp_role')).toBeDefined();
      rbac.dropRole('temp_role');
      expect(rbac.getRole('temp_role')).toBeUndefined();
    });

    it('should throw when dropping non-existent role', () => {
      expect(() => rbac.dropRole('ghost')).toThrow(SecurityError);
      expect(() => rbac.dropRole('ghost')).toThrow(
        "Role 'ghost' does not exist",
      );
    });

    it('should not allow dropping the public role', () => {
      expect(() => rbac.dropRole('public')).toThrow(SecurityError);
      expect(() => rbac.dropRole('public')).toThrow(
        "Cannot drop the 'public' role",
      );
    });

    it('should clean up grants when dropping a role', () => {
      rbac.createRole({ name: 'temp' });
      rbac.grant(
        ['SELECT'],
        { type: 'table', name: 'users' },
        'temp',
        'system',
      );
      rbac.dropRole('temp');
      // Grants for 'temp' should be removed
      const grants = rbac.getGrantsOnTarget({ type: 'table', name: 'users' });
      expect(grants.length).toBe(0);
    });

    it('should clean up memberOf references when dropping a role', () => {
      rbac.createRole({ name: 'parent' });
      rbac.createRole({ name: 'child' });
      rbac.grantRoleMembership('parent', 'child');
      expect(rbac.getRole('child')!.memberOf).toContain('parent');

      rbac.dropRole('parent');
      expect(rbac.getRole('child')!.memberOf).not.toContain('parent');
    });
  });

  // ===========================================================================
  // ROLE INHERITANCE
  // ===========================================================================

  describe('role inheritance', () => {
    it('should grant role membership', () => {
      rbac.createRole({ name: 'readers' });
      rbac.createRole({ name: 'alice', login: true });
      rbac.grantRoleMembership('readers', 'alice');

      const alice = rbac.getRole('alice')!;
      expect(alice.memberOf).toContain('readers');
    });

    it('should not duplicate role membership', () => {
      rbac.createRole({ name: 'readers' });
      rbac.createRole({ name: 'alice', login: true });
      rbac.grantRoleMembership('readers', 'alice');
      rbac.grantRoleMembership('readers', 'alice');

      const alice = rbac.getRole('alice')!;
      const count = alice.memberOf.filter((m) => m === 'readers').length;
      expect(count).toBe(1);
    });

    it('should get effective roles including inherited', () => {
      rbac.createRole({ name: 'readers' });
      rbac.createRole({ name: 'writers' });
      rbac.createRole({ name: 'editors' });
      rbac.createRole({ name: 'alice', login: true });

      rbac.grantRoleMembership('readers', 'editors');
      rbac.grantRoleMembership('writers', 'editors');
      rbac.grantRoleMembership('editors', 'alice');

      const effective = rbac.getEffectiveRoles('alice');
      expect(effective).toContain('alice');
      expect(effective).toContain('editors');
      expect(effective).toContain('readers');
      expect(effective).toContain('writers');
      expect(effective).toContain('public');
    });

    it('should detect circular membership', () => {
      rbac.createRole({ name: 'a' });
      rbac.createRole({ name: 'b' });
      rbac.grantRoleMembership('a', 'b');

      expect(() => rbac.grantRoleMembership('b', 'a')).toThrow(SecurityError);
      expect(() => rbac.grantRoleMembership('b', 'a')).toThrow(
        'Circular role membership',
      );
    });

    it('should revoke role membership', () => {
      rbac.createRole({ name: 'readers' });
      rbac.createRole({ name: 'alice', login: true });
      rbac.grantRoleMembership('readers', 'alice');
      rbac.revokeRoleMembership('readers', 'alice');

      const alice = rbac.getRole('alice')!;
      expect(alice.memberOf).not.toContain('readers');
    });

    it('should throw when granting membership to non-existent role', () => {
      rbac.createRole({ name: 'readers' });
      expect(() =>
        rbac.grantRoleMembership('readers', 'ghost'),
      ).toThrow(SecurityError);
    });

    it('should throw when granting non-existent parent role', () => {
      rbac.createRole({ name: 'alice' });
      expect(() =>
        rbac.grantRoleMembership('ghost', 'alice'),
      ).toThrow(SecurityError);
    });

    it('should check transitive role membership', () => {
      rbac.createRole({ name: 'a' });
      rbac.createRole({ name: 'b' });
      rbac.createRole({ name: 'c' });
      rbac.grantRoleMembership('a', 'b');
      rbac.grantRoleMembership('b', 'c');

      expect(rbac.hasRoleMembership('a', 'c')).toBe(true);
      expect(rbac.hasRoleMembership('b', 'c')).toBe(true);
      expect(rbac.hasRoleMembership('c', 'a')).toBe(false);
    });
  });

  // ===========================================================================
  // PRIVILEGE GRANTS
  // ===========================================================================

  describe('privilege grants', () => {
    it('should grant a privilege on a table', () => {
      rbac.createRole({ name: 'reader' });
      rbac.grant(
        ['SELECT'],
        { type: 'table', name: 'users' },
        'reader',
        'admin',
      );

      expect(
        rbac.hasPrivilege('reader', 'SELECT', {
          type: 'table',
          name: 'users',
        }),
      ).toBe(true);
    });

    it('should not have ungranted privileges', () => {
      rbac.createRole({ name: 'reader' });
      rbac.grant(
        ['SELECT'],
        { type: 'table', name: 'users' },
        'reader',
        'admin',
      );

      expect(
        rbac.hasPrivilege('reader', 'INSERT', {
          type: 'table',
          name: 'users',
        }),
      ).toBe(false);
    });

    it('should expand ALL into individual privileges', () => {
      const expanded = expandPrivileges(['ALL']);
      expect(expanded).toContain('SELECT');
      expect(expanded).toContain('INSERT');
      expect(expanded).toContain('UPDATE');
      expect(expanded).toContain('DELETE');
      expect(expanded).toContain('CREATE');
      expect(expanded).toContain('DROP');
      expect(expanded).toContain('ALTER');
    });

    it('should grant ALL privileges', () => {
      rbac.createRole({ name: 'admin' });
      rbac.grant(
        ['ALL'],
        { type: 'table', name: 'users' },
        'admin',
        'system',
      );

      expect(
        rbac.hasPrivilege('admin', 'SELECT', {
          type: 'table',
          name: 'users',
        }),
      ).toBe(true);
      expect(
        rbac.hasPrivilege('admin', 'DELETE', {
          type: 'table',
          name: 'users',
        }),
      ).toBe(true);
    });

    it('should revoke a privilege', () => {
      rbac.createRole({ name: 'reader' });
      rbac.grant(
        ['SELECT', 'INSERT'],
        { type: 'table', name: 'users' },
        'reader',
        'admin',
      );
      rbac.revoke(
        ['INSERT'],
        { type: 'table', name: 'users' },
        'reader',
      );

      expect(
        rbac.hasPrivilege('reader', 'SELECT', {
          type: 'table',
          name: 'users',
        }),
      ).toBe(true);
      expect(
        rbac.hasPrivilege('reader', 'INSERT', {
          type: 'table',
          name: 'users',
        }),
      ).toBe(false);
    });

    it('should not duplicate grants', () => {
      rbac.createRole({ name: 'reader' });
      rbac.grant(
        ['SELECT'],
        { type: 'table', name: 'users' },
        'reader',
        'admin',
      );
      rbac.grant(
        ['SELECT'],
        { type: 'table', name: 'users' },
        'reader',
        'admin',
      );

      const grants = rbac.getGrantsForRole('reader');
      const selectGrants = grants.filter(
        (g) => g.privilege === 'SELECT' && g.target.type === 'table',
      );
      expect(selectGrants.length).toBe(1);
    });

    it('should inherit privileges through role membership', () => {
      rbac.createRole({ name: 'readers' });
      rbac.createRole({ name: 'alice', login: true });
      rbac.grantRoleMembership('readers', 'alice');

      rbac.grant(
        ['SELECT'],
        { type: 'table', name: 'users' },
        'readers',
        'admin',
      );

      expect(
        rbac.hasPrivilege('alice', 'SELECT', {
          type: 'table',
          name: 'users',
        }),
      ).toBe(true);
    });

    it('should handle database-level grants covering tables', () => {
      rbac.createRole({ name: 'dba' });
      rbac.grant(['SELECT'], { type: 'database' }, 'dba', 'system');

      expect(
        rbac.hasPrivilege('dba', 'SELECT', {
          type: 'table',
          name: 'any_table',
        }),
      ).toBe(true);
    });

    it('should handle schema-level grants covering tables', () => {
      rbac.createRole({ name: 'schema_reader' });
      rbac.grant(
        ['SELECT'],
        { type: 'schema', name: 'public' },
        'schema_reader',
        'system',
      );

      expect(
        rbac.hasPrivilege('schema_reader', 'SELECT', {
          type: 'table',
          name: 'users',
        }),
      ).toBe(true);
    });

    it('should throw when granting to non-existent role', () => {
      expect(() =>
        rbac.grant(
          ['SELECT'],
          { type: 'table', name: 'users' },
          'ghost',
          'admin',
        ),
      ).toThrow(SecurityError);
    });

    it('should support WITH GRANT OPTION', () => {
      rbac.createRole({ name: 'reader' });
      rbac.grant(
        ['SELECT'],
        { type: 'table', name: 'users' },
        'reader',
        'admin',
        true,
      );

      expect(
        rbac.hasGrantOption('reader', 'SELECT', {
          type: 'table',
          name: 'users',
        }),
      ).toBe(true);
    });

    it('should get grants on a target', () => {
      rbac.createRole({ name: 'reader' });
      rbac.createRole({ name: 'writer' });
      rbac.grant(
        ['SELECT'],
        { type: 'table', name: 'users' },
        'reader',
        'admin',
      );
      rbac.grant(
        ['INSERT'],
        { type: 'table', name: 'users' },
        'writer',
        'admin',
      );

      const grants = rbac.getGrantsOnTarget({ type: 'table', name: 'users' });
      expect(grants.length).toBe(2);
    });
  });

  // ===========================================================================
  // SUPERUSER
  // ===========================================================================

  describe('superuser', () => {
    it('should bypass all privilege checks', () => {
      rbac.createRole({ name: 'super', superuser: true });

      expect(
        rbac.hasPrivilege('super', 'DELETE', {
          type: 'table',
          name: 'any_table',
        }),
      ).toBe(true);
    });

    it('should have grant option on everything', () => {
      rbac.createRole({ name: 'super', superuser: true });

      expect(
        rbac.hasGrantOption('super', 'SELECT', {
          type: 'table',
          name: 'anything',
        }),
      ).toBe(true);
    });
  });

  // ===========================================================================
  // AUTHORIZATION
  // ===========================================================================

  describe('authorization', () => {
    it('should not throw when authorized', () => {
      rbac.createRole({ name: 'reader' });
      rbac.grant(
        ['SELECT'],
        { type: 'table', name: 'users' },
        'reader',
        'admin',
      );

      expect(() =>
        rbac.authorize('reader', 'SELECT', {
          type: 'table',
          name: 'users',
        }),
      ).not.toThrow();
    });

    it('should throw PERMISSION_DENIED when not authorized', () => {
      rbac.createRole({ name: 'nobody' });

      expect(() =>
        rbac.authorize('nobody', 'DELETE', {
          type: 'table',
          name: 'users',
        }),
      ).toThrow(SecurityError);

      try {
        rbac.authorize('nobody', 'DELETE', {
          type: 'table',
          name: 'users',
        });
      } catch (e) {
        expect((e as SecurityError).code).toBe('PERMISSION_DENIED');
      }
    });
  });

  // ===========================================================================
  // SECURITY CONTEXT
  // ===========================================================================

  describe('security context', () => {
    it('should build a security context for a role', () => {
      rbac.createRole({ name: 'readers' });
      rbac.createRole({ name: 'alice', login: true });
      rbac.grantRoleMembership('readers', 'alice');

      const ctx = rbac.buildSecurityContext('alice');
      expect(ctx.currentRole).toBe('alice');
      expect(ctx.effectiveRoles).toContain('alice');
      expect(ctx.effectiveRoles).toContain('readers');
      expect(ctx.effectiveRoles).toContain('public');
      expect(ctx.bypassSecurity).toBe(false);
    });

    it('should set bypassSecurity for superuser', () => {
      rbac.createRole({ name: 'super', superuser: true });
      const ctx = rbac.buildSecurityContext('super');
      expect(ctx.bypassSecurity).toBe(true);
    });

    it('should throw for non-existent role', () => {
      expect(() => rbac.buildSecurityContext('ghost')).toThrow(SecurityError);
    });
  });
});

// =============================================================================
// RLS TESTS
// =============================================================================

describe('security - RLS', () => {
  let rls: RLSManager;

  beforeEach(() => {
    rls = new RLSManager();
  });

  // ===========================================================================
  // TABLE RLS STATE
  // ===========================================================================

  describe('table RLS state', () => {
    it('should be disabled by default', () => {
      expect(rls.isRLSEnabled('users')).toBe(false);
    });

    it('should enable RLS on a table', () => {
      rls.enableRLS('users');
      expect(rls.isRLSEnabled('users')).toBe(true);
    });

    it('should disable RLS on a table', () => {
      rls.enableRLS('users');
      rls.disableRLS('users');
      expect(rls.isRLSEnabled('users')).toBe(false);
    });

    it('should support force row security', () => {
      rls.enableRLS('users', true);
      const state = rls.getRLSState('users');
      expect(state?.forceRowSecurity).toBe(true);
    });
  });

  // ===========================================================================
  // POLICY MANAGEMENT
  // ===========================================================================

  describe('policy management', () => {
    it('should create a policy', () => {
      const policy = rls.createPolicy({
        name: 'user_isolation',
        table: 'users',
        using: "owner = current_user",
      });

      expect(policy.name).toBe('user_isolation');
      expect(policy.table).toBe('users');
      expect(policy.command).toBe('ALL');
      expect(policy.policyType).toBe('PERMISSIVE');
      expect(policy.enabled).toBe(true);
    });

    it('should create a policy with all options', () => {
      const policy = rls.createPolicy({
        name: 'select_own',
        table: 'orders',
        command: 'SELECT',
        policyType: 'RESTRICTIVE',
        roles: ['customer'],
        using: "customer_id = current_user",
        withCheck: "customer_id = current_user",
      });

      expect(policy.command).toBe('SELECT');
      expect(policy.policyType).toBe('RESTRICTIVE');
      expect(policy.roles).toEqual(['customer']);
    });

    it('should throw when creating duplicate policy on same table', () => {
      rls.createPolicy({ name: 'p1', table: 'users' });
      expect(() =>
        rls.createPolicy({ name: 'p1', table: 'users' }),
      ).toThrow(SecurityError);
    });

    it('should allow same policy name on different tables', () => {
      rls.createPolicy({ name: 'isolation', table: 'users' });
      expect(() =>
        rls.createPolicy({ name: 'isolation', table: 'orders' }),
      ).not.toThrow();
    });

    it('should drop a policy', () => {
      rls.createPolicy({ name: 'p1', table: 'users' });
      rls.dropPolicy('p1', 'users');
      expect(rls.getPolicies('users').length).toBe(0);
    });

    it('should throw when dropping non-existent policy', () => {
      expect(() => rls.dropPolicy('ghost', 'users')).toThrow(SecurityError);
    });

    it('should enable/disable a policy', () => {
      rls.createPolicy({ name: 'p1', table: 'users' });
      rls.setPolicyEnabled('p1', 'users', false);
      const policies = rls.getPolicies('users');
      expect(policies[0].enabled).toBe(false);

      rls.setPolicyEnabled('p1', 'users', true);
      expect(rls.getPolicies('users')[0].enabled).toBe(true);
    });

    it('should throw when setting enabled on non-existent policy', () => {
      expect(() =>
        rls.setPolicyEnabled('ghost', 'users', true),
      ).toThrow(SecurityError);
    });

    it('should get applicable policies for command and role', () => {
      const ctx: SecurityContext = {
        currentRole: 'alice',
        effectiveRoles: ['alice', 'readers', 'public'],
        bypassSecurity: false,
      };

      rls.createPolicy({
        name: 'select_policy',
        table: 'users',
        command: 'SELECT',
        roles: ['readers'],
        using: 'TRUE',
      });

      rls.createPolicy({
        name: 'insert_policy',
        table: 'users',
        command: 'INSERT',
        roles: ['writers'],
        using: 'TRUE',
      });

      const selectPolicies = rls.getApplicablePolicies('users', 'SELECT', ctx);
      expect(selectPolicies.length).toBe(1);
      expect(selectPolicies[0].name).toBe('select_policy');

      // alice is not in 'writers', so no INSERT policies
      const insertPolicies = rls.getApplicablePolicies('users', 'INSERT', ctx);
      expect(insertPolicies.length).toBe(0);
    });

    it('should return no policies for bypass security context', () => {
      const ctx: SecurityContext = {
        currentRole: 'super',
        effectiveRoles: ['super'],
        bypassSecurity: true,
      };

      rls.createPolicy({
        name: 'p1',
        table: 'users',
        using: 'FALSE',
      });

      const policies = rls.getApplicablePolicies('users', 'SELECT', ctx);
      expect(policies.length).toBe(0);
    });

    it('should include ALL command policies for any command', () => {
      const ctx: SecurityContext = {
        currentRole: 'alice',
        effectiveRoles: ['alice', 'public'],
        bypassSecurity: false,
      };

      rls.createPolicy({
        name: 'all_policy',
        table: 'users',
        command: 'ALL',
        using: 'TRUE',
      });

      expect(
        rls.getApplicablePolicies('users', 'SELECT', ctx).length,
      ).toBe(1);
      expect(
        rls.getApplicablePolicies('users', 'INSERT', ctx).length,
      ).toBe(1);
      expect(
        rls.getApplicablePolicies('users', 'DELETE', ctx).length,
      ).toBe(1);
    });
  });

  // ===========================================================================
  // ROW VISIBILITY (USING clause)
  // ===========================================================================

  describe('row visibility', () => {
    const aliceCtx: SecurityContext = {
      currentRole: 'alice',
      effectiveRoles: ['alice', 'public'],
      bypassSecurity: false,
    };

    const bobCtx: SecurityContext = {
      currentRole: 'bob',
      effectiveRoles: ['bob', 'public'],
      bypassSecurity: false,
    };

    const superCtx: SecurityContext = {
      currentRole: 'super',
      effectiveRoles: ['super', 'public'],
      bypassSecurity: true,
    };

    it('should allow all rows when RLS is not enabled', () => {
      rls.createPolicy({
        name: 'deny_all',
        table: 'users',
        using: 'FALSE',
      });

      const row = { id: 1, name: 'test', owner: 'alice' };
      expect(rls.isRowVisible('users', row, 'SELECT', aliceCtx)).toBe(true);
    });

    it('should deny all rows when RLS enabled with no policies', () => {
      rls.enableRLS('users');

      const row = { id: 1, name: 'test', owner: 'alice' };
      expect(rls.isRowVisible('users', row, 'SELECT', aliceCtx)).toBe(false);
    });

    it('should filter rows based on permissive USING clause', () => {
      rls.enableRLS('users');
      rls.createPolicy({
        name: 'own_rows',
        table: 'users',
        using: "owner = current_user",
      });

      const aliceRow = { id: 1, name: 'Alice', owner: 'alice' };
      const bobRow = { id: 2, name: 'Bob', owner: 'bob' };

      expect(rls.isRowVisible('users', aliceRow, 'SELECT', aliceCtx)).toBe(true);
      expect(rls.isRowVisible('users', bobRow, 'SELECT', aliceCtx)).toBe(false);
      expect(rls.isRowVisible('users', bobRow, 'SELECT', bobCtx)).toBe(true);
    });

    it('should handle TRUE/FALSE boolean policies', () => {
      rls.enableRLS('users');
      rls.createPolicy({
        name: 'allow_all',
        table: 'users',
        using: 'TRUE',
      });

      const row = { id: 1, name: 'test' };
      expect(rls.isRowVisible('users', row, 'SELECT', aliceCtx)).toBe(true);
    });

    it('should allow superuser to bypass RLS', () => {
      rls.enableRLS('users');
      // No policies = deny all for regular users

      const row = { id: 1, name: 'test' };
      expect(rls.isRowVisible('users', row, 'SELECT', superCtx)).toBe(true);
    });

    it('should enforce RLS for superuser when force_row_security is set', () => {
      rls.enableRLS('users', true); // forceRowSecurity
      // No policies = deny all

      const row = { id: 1, name: 'test' };
      expect(rls.isRowVisible('users', row, 'SELECT', superCtx)).toBe(false);
    });

    it('should OR permissive policies together', () => {
      rls.enableRLS('docs');
      rls.createPolicy({
        name: 'own_docs',
        table: 'docs',
        policyType: 'PERMISSIVE',
        using: "author = current_user",
      });
      rls.createPolicy({
        name: 'public_docs',
        table: 'docs',
        policyType: 'PERMISSIVE',
        using: "is_public = 1",
      });

      // Alice's private doc
      expect(
        rls.isRowVisible(
          'docs',
          { id: 1, author: 'alice', is_public: 0 },
          'SELECT',
          aliceCtx,
        ),
      ).toBe(true);

      // Public doc by Bob
      expect(
        rls.isRowVisible(
          'docs',
          { id: 2, author: 'bob', is_public: 1 },
          'SELECT',
          aliceCtx,
        ),
      ).toBe(true);

      // Bob's private doc - not visible to Alice
      expect(
        rls.isRowVisible(
          'docs',
          { id: 3, author: 'bob', is_public: 0 },
          'SELECT',
          aliceCtx,
        ),
      ).toBe(false);
    });

    it('should AND restrictive policies', () => {
      rls.enableRLS('docs');

      // Base permissive policy that allows all
      rls.createPolicy({
        name: 'base',
        table: 'docs',
        policyType: 'PERMISSIVE',
        using: 'TRUE',
      });

      // Restrictive: must be active
      rls.createPolicy({
        name: 'active_only',
        table: 'docs',
        policyType: 'RESTRICTIVE',
        using: "status = 'active'",
      });

      // Restrictive: must be in correct department
      rls.createPolicy({
        name: 'dept_only',
        table: 'docs',
        policyType: 'RESTRICTIVE',
        using: "department = 'engineering'",
      });

      // Active + engineering = visible
      expect(
        rls.isRowVisible(
          'docs',
          { id: 1, status: 'active', department: 'engineering' },
          'SELECT',
          aliceCtx,
        ),
      ).toBe(true);

      // Active + sales = NOT visible (department restriction fails)
      expect(
        rls.isRowVisible(
          'docs',
          { id: 2, status: 'active', department: 'sales' },
          'SELECT',
          aliceCtx,
        ),
      ).toBe(false);

      // Inactive + engineering = NOT visible (status restriction fails)
      expect(
        rls.isRowVisible(
          'docs',
          { id: 3, status: 'archived', department: 'engineering' },
          'SELECT',
          aliceCtx,
        ),
      ).toBe(false);
    });

    it('should filter rows in batch', () => {
      rls.enableRLS('users');
      rls.createPolicy({
        name: 'own_rows',
        table: 'users',
        using: "owner = current_user",
      });

      const rows = [
        { id: 1, name: 'Alice', owner: 'alice' },
        { id: 2, name: 'Bob', owner: 'bob' },
        { id: 3, name: 'Alice2', owner: 'alice' },
      ];

      const filtered = rls.filterRows('users', rows, 'SELECT', aliceCtx);
      expect(filtered.length).toBe(2);
      expect(filtered[0].name).toBe('Alice');
      expect(filtered[1].name).toBe('Alice2');
    });

    it('should skip disabled policies', () => {
      rls.enableRLS('users');
      rls.createPolicy({
        name: 'allow_all',
        table: 'users',
        using: 'TRUE',
      });
      rls.setPolicyEnabled('allow_all', 'users', false);

      const row = { id: 1, name: 'test' };
      // Disabled policy = no applicable policies = deny
      expect(rls.isRowVisible('users', row, 'SELECT', aliceCtx)).toBe(false);
    });
  });

  // ===========================================================================
  // WITH CHECK (INSERT/UPDATE validation)
  // ===========================================================================

  describe('WITH CHECK validation', () => {
    const aliceCtx: SecurityContext = {
      currentRole: 'alice',
      effectiveRoles: ['alice', 'public'],
      bypassSecurity: false,
    };

    it('should validate INSERT with WITH CHECK', () => {
      rls.enableRLS('users');
      rls.createPolicy({
        name: 'own_insert',
        table: 'users',
        command: 'INSERT',
        withCheck: "owner = current_user",
      });

      // Alice inserting her own row
      expect(
        rls.checkRow(
          'users',
          { id: 1, name: 'New', owner: 'alice' },
          'INSERT',
          aliceCtx,
        ),
      ).toBe(true);

      // Alice trying to insert as Bob
      expect(
        rls.checkRow(
          'users',
          { id: 2, name: 'New', owner: 'bob' },
          'INSERT',
          aliceCtx,
        ),
      ).toBe(false);
    });

    it('should fall back to USING when no WITH CHECK', () => {
      rls.enableRLS('users');
      rls.createPolicy({
        name: 'own_rows',
        table: 'users',
        command: 'ALL',
        using: "owner = current_user",
        // No withCheck - should use 'using' for check too
      });

      expect(
        rls.checkRow(
          'users',
          { id: 1, owner: 'alice' },
          'INSERT',
          aliceCtx,
        ),
      ).toBe(true);

      expect(
        rls.checkRow(
          'users',
          { id: 1, owner: 'bob' },
          'INSERT',
          aliceCtx,
        ),
      ).toBe(false);
    });
  });

  // ===========================================================================
  // RLS EXPRESSION EVALUATOR
  // ===========================================================================

  describe('RLS expression evaluator', () => {
    const ctx: SecurityContext = {
      currentRole: 'alice',
      effectiveRoles: ['alice', 'public'],
      bypassSecurity: false,
    };

    it('should evaluate simple equality', () => {
      const row = { owner: 'alice', status: 'active' };
      expect(evaluateRLSExpression("owner = 'alice'", row, ctx)).toBe(true);
      expect(evaluateRLSExpression("owner = 'bob'", row, ctx)).toBe(false);
    });

    it('should evaluate current_user', () => {
      const row = { owner: 'alice' };
      expect(evaluateRLSExpression("owner = current_user", row, ctx)).toBe(true);
    });

    it('should evaluate current_role', () => {
      const row = { role_name: 'alice' };
      expect(evaluateRLSExpression("role_name = current_role", row, ctx)).toBe(true);
    });

    it('should evaluate numeric comparisons', () => {
      const row = { age: 25, score: 100 };
      expect(evaluateRLSExpression("age >= 18", row, ctx)).toBe(true);
      expect(evaluateRLSExpression("age < 18", row, ctx)).toBe(false);
      expect(evaluateRLSExpression("score = 100", row, ctx)).toBe(true);
    });

    it('should evaluate AND/OR/NOT', () => {
      const row = { owner: 'alice', is_public: 1 };
      expect(
        evaluateRLSExpression(
          "owner = current_user AND is_public = 1",
          row,
          ctx,
        ),
      ).toBe(true);
      expect(
        evaluateRLSExpression(
          "owner = 'bob' OR is_public = 1",
          row,
          ctx,
        ),
      ).toBe(true);
      expect(
        evaluateRLSExpression(
          "NOT owner = 'bob'",
          row,
          ctx,
        ),
      ).toBe(true);
    });

    it('should evaluate IS NULL / IS NOT NULL', () => {
      const row = { name: 'Alice', deleted_at: null };
      expect(evaluateRLSExpression("deleted_at IS NULL", row, ctx)).toBe(true);
      expect(evaluateRLSExpression("deleted_at IS NOT NULL", row, ctx)).toBe(false);
      expect(evaluateRLSExpression("name IS NOT NULL", row, ctx)).toBe(true);
      expect(evaluateRLSExpression("name IS NULL", row, ctx)).toBe(false);
    });

    it('should evaluate boolean keywords', () => {
      expect(evaluateRLSExpression("TRUE", { id: 1 }, ctx)).toBe(true);
      expect(evaluateRLSExpression("FALSE", { id: 1 }, ctx)).toBe(false);
    });

    it('should evaluate != and <> operators', () => {
      const row = { status: 'active' };
      expect(evaluateRLSExpression("status != 'deleted'", row, ctx)).toBe(true);
      expect(evaluateRLSExpression("status <> 'active'", row, ctx)).toBe(false);
    });

    it('should handle parenthesized expressions', () => {
      const row = { a: 1, b: 2, c: 3 };
      expect(
        evaluateRLSExpression("(a = 1 OR b = 99) AND c = 3", row, ctx),
      ).toBe(true);
      expect(
        evaluateRLSExpression("(a = 99 OR b = 99) AND c = 3", row, ctx),
      ).toBe(false);
    });

    it('should handle missing columns as NULL', () => {
      const row = { id: 1 };
      expect(evaluateRLSExpression("nonexistent IS NULL", row, ctx)).toBe(true);
      expect(evaluateRLSExpression("nonexistent = 'value'", row, ctx)).toBe(false);
    });
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('security - RBAC + RLS integration', () => {
  it('should combine RBAC authorization with RLS row filtering', () => {
    const rbac = new RBACManager();
    const rlsManager = new RLSManager();

    // Setup roles
    rbac.createRole({ name: 'customer', login: true });
    rbac.createRole({ name: 'admin', superuser: true, login: true });

    // Grant SELECT on orders to customer
    rbac.grant(
      ['SELECT'],
      { type: 'table', name: 'orders' },
      'customer',
      'system',
    );

    // Enable RLS on orders
    rlsManager.enableRLS('orders');
    rlsManager.createPolicy({
      name: 'own_orders',
      table: 'orders',
      command: 'SELECT',
      using: "customer_id = current_user",
    });

    const orders = [
      { id: 1, customer_id: 'alice', total: 100 },
      { id: 2, customer_id: 'bob', total: 200 },
      { id: 3, customer_id: 'alice', total: 50 },
    ];

    // Alice: has SELECT privilege and sees only her orders
    const aliceCtx = rbac.buildSecurityContext('customer');
    // Override currentRole for this scenario
    const aliceSecCtx: SecurityContext = {
      ...aliceCtx,
      currentRole: 'alice',
    };
    rbac.authorize('customer', 'SELECT', { type: 'table', name: 'orders' });
    const aliceOrders = rlsManager.filterRows(
      'orders',
      orders,
      'SELECT',
      aliceSecCtx,
    );
    expect(aliceOrders.length).toBe(2);

    // Admin (superuser): sees all rows (bypass RLS)
    const adminCtx = rbac.buildSecurityContext('admin');
    const adminOrders = rlsManager.filterRows(
      'orders',
      orders,
      'SELECT',
      adminCtx,
    );
    expect(adminOrders.length).toBe(3);
  });

  it('should enforce both RBAC privilege check and RLS policy on mutations', () => {
    const rbac = new RBACManager();
    const rlsManager = new RLSManager();

    rbac.createRole({ name: 'writer', login: true });
    rbac.grant(
      ['INSERT'],
      { type: 'table', name: 'posts' },
      'writer',
      'system',
    );

    rlsManager.enableRLS('posts');
    rlsManager.createPolicy({
      name: 'own_posts',
      table: 'posts',
      command: 'INSERT',
      withCheck: "author = current_user",
    });

    const writerCtx: SecurityContext = {
      currentRole: 'alice',
      effectiveRoles: ['alice', 'writer', 'public'],
      bypassSecurity: false,
    };

    // RBAC: has INSERT
    rbac.authorize('writer', 'INSERT', { type: 'table', name: 'posts' });

    // RLS: can insert own post
    expect(
      rlsManager.checkRow(
        'posts',
        { id: 1, title: 'Test', author: 'alice' },
        'INSERT',
        writerCtx,
      ),
    ).toBe(true);

    // RLS: cannot insert as another user
    expect(
      rlsManager.checkRow(
        'posts',
        { id: 2, title: 'Test', author: 'bob' },
        'INSERT',
        writerCtx,
      ),
    ).toBe(false);
  });
});
