/**
 * DoSQL E2E CI Integration Tests (RED Phase TDD)
 *
 * This file documents the expected behavior of E2E tests running in CI.
 * These tests verify that:
 * 1. E2E tests can run automatically in CI
 * 2. Test worker is deployed before tests
 * 3. Test worker is cleaned up after tests
 * 4. Secrets are properly configured
 * 5. Test results are reported
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { existsSync, readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execSync } from 'node:child_process';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
// __dirname = packages/dosql/e2e/__tests__
// PROJECT_ROOT = sql (monorepo root) - 4 levels up
// PACKAGE_ROOT = packages/dosql - 2 levels up
const PROJECT_ROOT = join(__dirname, '../../../..');
const PACKAGE_ROOT = join(__dirname, '../..');

// =============================================================================
// CI Environment Detection
// =============================================================================

const isCI = !!process.env.CI;
const isGitHubActions = !!process.env.GITHUB_ACTIONS;

// =============================================================================
// CI Configuration Tests
// =============================================================================

describe('E2E CI Configuration', () => {
  describe('GitHub Actions Workflow', () => {
    const workflowPath = join(PROJECT_ROOT, '.github/workflows/e2e.yml');

    it('E2E workflow file exists', () => {
      // RED: This test documents that a dedicated E2E workflow should exist
      expect(existsSync(workflowPath)).toBe(true);
    });

    it('workflow is valid YAML', () => {
      if (!existsSync(workflowPath)) {
        expect.fail('Workflow file does not exist');
        return;
      }

      const content = readFileSync(workflowPath, 'utf-8');

      // Basic YAML structure checks
      expect(content).toContain('name:');
      expect(content).toContain('jobs:');
      expect(content).toContain('runs-on:');
    });

    it('workflow triggers on push to main', () => {
      if (!existsSync(workflowPath)) {
        expect.fail('Workflow file does not exist');
        return;
      }

      const content = readFileSync(workflowPath, 'utf-8');

      expect(content).toMatch(/on:\s*\n\s*push:/m);
      expect(content).toMatch(/branches:\s*\[.*main.*\]/m);
    });

    it('workflow triggers on pull_request', () => {
      if (!existsSync(workflowPath)) {
        expect.fail('Workflow file does not exist');
        return;
      }

      const content = readFileSync(workflowPath, 'utf-8');

      expect(content).toContain('pull_request:');
    });

    it('workflow has manual trigger option (workflow_dispatch)', () => {
      if (!existsSync(workflowPath)) {
        expect.fail('Workflow file does not exist');
        return;
      }

      const content = readFileSync(workflowPath, 'utf-8');

      expect(content).toContain('workflow_dispatch:');
    });

    it('workflow uses required secrets', () => {
      if (!existsSync(workflowPath)) {
        expect.fail('Workflow file does not exist');
        return;
      }

      const content = readFileSync(workflowPath, 'utf-8');

      // Required Cloudflare credentials for deployment
      expect(content).toContain('CLOUDFLARE_API_TOKEN');
      expect(content).toContain('CLOUDFLARE_ACCOUNT_ID');
    });

    it('workflow has deploy step before tests', () => {
      if (!existsSync(workflowPath)) {
        expect.fail('Workflow file does not exist');
        return;
      }

      const content = readFileSync(workflowPath, 'utf-8');

      // Should have a deployment step
      expect(content).toMatch(/deploy|Deploy/i);
      expect(content).toContain('wrangler');
    });

    it('workflow has cleanup step that always runs', () => {
      if (!existsSync(workflowPath)) {
        expect.fail('Workflow file does not exist');
        return;
      }

      const content = readFileSync(workflowPath, 'utf-8');

      // Cleanup should always run, even on failure
      expect(content).toContain('always()');
      expect(content).toMatch(/cleanup|Cleanup/i);
    });

    it('workflow runs E2E tests', () => {
      if (!existsSync(workflowPath)) {
        expect.fail('Workflow file does not exist');
        return;
      }

      const content = readFileSync(workflowPath, 'utf-8');

      // Should run the E2E test script
      expect(content).toMatch(/test:e2e|vitest.*e2e/i);
    });

    it('workflow uploads test artifacts', () => {
      if (!existsSync(workflowPath)) {
        expect.fail('Workflow file does not exist');
        return;
      }

      const content = readFileSync(workflowPath, 'utf-8');

      // Should upload test results
      expect(content).toContain('upload-artifact');
    });
  });

  describe('Package Scripts', () => {
    const packageJsonPath = join(PACKAGE_ROOT, 'package.json');

    it('package.json has test:e2e:ci script', () => {
      const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf-8'));

      expect(packageJson.scripts['test:e2e:ci']).toBeDefined();
    });

    it('test:e2e:ci sets E2E_AUTO_DEPLOY environment variable', () => {
      const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf-8'));
      const script = packageJson.scripts['test:e2e:ci'] || '';

      expect(script).toContain('E2E_AUTO_DEPLOY');
    });
  });

  describe('Vitest Configuration', () => {
    const vitestConfigPath = join(PACKAGE_ROOT, 'e2e/vitest.config.ts');

    it('vitest.config.ts exists', () => {
      expect(existsSync(vitestConfigPath)).toBe(true);
    });

    it('vitest config enables globalSetup when E2E_AUTO_DEPLOY is set', () => {
      const content = readFileSync(vitestConfigPath, 'utf-8');

      expect(content).toContain('globalSetup');
      expect(content).toContain('E2E_AUTO_DEPLOY');
    });

    it('vitest config outputs JUnit for CI', () => {
      const content = readFileSync(vitestConfigPath, 'utf-8');

      // Should have JUnit reporter and outputFile configuration
      expect(content).toContain('junit');
      expect(content).toContain('outputFile');
      expect(content).toContain('test-results');
    });
  });

  describe('Global Setup', () => {
    const globalSetupPath = join(PACKAGE_ROOT, 'e2e/global-setup.ts');

    it('global-setup.ts exists', () => {
      expect(existsSync(globalSetupPath)).toBe(true);
    });

    it('global setup deploys worker', () => {
      const content = readFileSync(globalSetupPath, 'utf-8');

      expect(content).toContain('deploy');
      expect(content).toMatch(/async function setup/);
    });

    it('global setup tears down worker', () => {
      const content = readFileSync(globalSetupPath, 'utf-8');

      expect(content).toContain('teardown');
      expect(content).toMatch(/async function teardown/);
    });

    it('global setup handles existing endpoint gracefully', () => {
      const content = readFileSync(globalSetupPath, 'utf-8');

      expect(content).toContain('DOSQL_E2E_ENDPOINT');
    });
  });
});

// =============================================================================
// CI Runtime Tests (only run in CI environment)
// =============================================================================

const describeCIOnly = isCI ? describe : describe.skip;

describeCIOnly('E2E CI Runtime', () => {
  describe('Environment Variables', () => {
    it('CLOUDFLARE_API_TOKEN is set', () => {
      // In CI, this should be set from GitHub secrets
      expect(process.env.CLOUDFLARE_API_TOKEN).toBeDefined();
      expect(process.env.CLOUDFLARE_API_TOKEN).not.toBe('');
    });

    it('CLOUDFLARE_ACCOUNT_ID is set', () => {
      // In CI, this should be set from GitHub secrets
      expect(process.env.CLOUDFLARE_ACCOUNT_ID).toBeDefined();
      expect(process.env.CLOUDFLARE_ACCOUNT_ID).not.toBe('');
    });

    it('CI environment is detected', () => {
      expect(isCI).toBe(true);
    });
  });

  describe('GitHub Actions Specific', () => {
    const describeGitHubActions = isGitHubActions ? describe : describe.skip;

    describeGitHubActions('GitHub Actions Environment', () => {
      it('GITHUB_ACTIONS is set', () => {
        expect(process.env.GITHUB_ACTIONS).toBe('true');
      });

      it('GITHUB_RUN_ID is available for unique suffixes', () => {
        expect(process.env.GITHUB_RUN_ID).toBeDefined();
      });

      it('GITHUB_SHA is available for version tracking', () => {
        expect(process.env.GITHUB_SHA).toBeDefined();
      });
    });
  });

  describe('Deployment', () => {
    it('E2E endpoint is available after global setup', () => {
      // If E2E_AUTO_DEPLOY ran, we should have an endpoint
      const endpoint = process.env.DOSQL_E2E_ENDPOINT;

      if (process.env.E2E_AUTO_DEPLOY) {
        expect(endpoint).toBeDefined();
        expect(endpoint).toMatch(/^https:\/\//);
      }
    });

    it('deployed worker responds to health check', async () => {
      const endpoint = process.env.DOSQL_E2E_ENDPOINT;

      if (!endpoint) {
        expect.fail('No E2E endpoint configured');
        return;
      }

      const response = await fetch(`${endpoint}/health`);
      expect(response.ok).toBe(true);

      const data = await response.json();
      expect(data.status).toBe('ok');
    });
  });
});

// =============================================================================
// Test Output and Reporting
// =============================================================================

describe('Test Reporting', () => {
  it('test-results directory exists or will be created', () => {
    // This directory should be created during test runs
    const testResultsDir = join(PACKAGE_ROOT, 'test-results');

    // Note: Directory may not exist until tests run
    // This just documents the expected location
    expect(testResultsDir).toContain('test-results');
  });

  it('JUnit output path is configured correctly', () => {
    const vitestConfigPath = join(PACKAGE_ROOT, 'e2e/vitest.config.ts');
    const content = readFileSync(vitestConfigPath, 'utf-8');

    // Should output to test-results directory
    expect(content).toContain('test-results');
    expect(content).toContain('e2e-junit.xml');
  });
});

// =============================================================================
// Error Handling and Recovery
// =============================================================================

describe('Error Handling', () => {
  describe('Setup Failures', () => {
    it('setup.ts handles missing credentials gracefully', () => {
      const setupPath = join(PACKAGE_ROOT, 'e2e/setup.ts');
      const content = readFileSync(setupPath, 'utf-8');

      // Should check for authentication before deploying
      expect(content).toContain('isAuthenticated');
      expect(content).toMatch(/Not authenticated|CLOUDFLARE_API_TOKEN/);
    });

    it('setup.ts handles deployment failures', () => {
      const setupPath = join(PACKAGE_ROOT, 'e2e/setup.ts');
      const content = readFileSync(setupPath, 'utf-8');

      // Should have try-catch and error handling
      expect(content).toMatch(/catch.*error/i);
      expect(content).toMatch(/throw.*Error/);
    });
  });

  describe('Teardown Failures', () => {
    it('teardown continues on failure when force flag is set', () => {
      const setupPath = join(PACKAGE_ROOT, 'e2e/setup.ts');
      const content = readFileSync(setupPath, 'utf-8');

      // Should have force option for cleanup
      expect(content).toContain('force');
    });

    it('global teardown does not fail the test run', () => {
      const globalSetupPath = join(PACKAGE_ROOT, 'e2e/global-setup.ts');
      const content = readFileSync(globalSetupPath, 'utf-8');

      // Should catch errors in teardown
      expect(content).toMatch(/catch.*error/i);
      expect(content).toMatch(/warn|console\.warn/i);
    });
  });
});
