/**
 * Global Setup for E2E Tests
 *
 * Automatically deploys DoSQL to Cloudflare when E2E_AUTO_DEPLOY is set.
 * Used for CI/CD pipelines.
 */

import { deploy, teardown, PACKAGE_ROOT } from './setup.js';
import { writeFileSync, existsSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';

let deployedWorkerName: string | null = null;

export async function setup(): Promise<void> {
  console.log('[E2E Global Setup] Starting...');

  // Skip if endpoint already set
  if (process.env.DOSQL_E2E_ENDPOINT) {
    console.log('[E2E Global Setup] Using existing endpoint:', process.env.DOSQL_E2E_ENDPOINT);
    return;
  }

  // Skip if skip flag is set
  if (process.env.DOSQL_E2E_SKIP) {
    console.log('[E2E Global Setup] E2E tests skipped via DOSQL_E2E_SKIP');
    return;
  }

  try {
    const suffix = `ci-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

    console.log('[E2E Global Setup] Deploying DoSQL...');
    const deployment = await deploy({ suffix });

    deployedWorkerName = deployment.workerName;

    // Save endpoint for tests
    const endpointFile = join(PACKAGE_ROOT, '.e2e-endpoint');
    writeFileSync(endpointFile, deployment.url, 'utf-8');

    // Also set environment variable for current process
    process.env.DOSQL_E2E_ENDPOINT = deployment.url;

    console.log('[E2E Global Setup] Deployed to:', deployment.url);
    console.log('[E2E Global Setup] Worker name:', deployedWorkerName);
  } catch (error) {
    console.error('[E2E Global Setup] Deployment failed:', error);
    throw error;
  }
}

export async function teardown(): Promise<void> {
  console.log('[E2E Global Teardown] Starting...');

  // Clean up endpoint file
  const endpointFile = join(PACKAGE_ROOT, '.e2e-endpoint');
  if (existsSync(endpointFile)) {
    unlinkSync(endpointFile);
    console.log('[E2E Global Teardown] Removed endpoint file');
  }

  // Teardown deployed worker
  if (deployedWorkerName) {
    try {
      console.log('[E2E Global Teardown] Deleting worker:', deployedWorkerName);
      await teardown(deployedWorkerName, { force: true });
      console.log('[E2E Global Teardown] Worker deleted');
    } catch (error) {
      console.warn('[E2E Global Teardown] Failed to delete worker:', error);
      // Don't fail the teardown
    }
  }

  console.log('[E2E Global Teardown] Complete');
}

export default { setup, teardown };
