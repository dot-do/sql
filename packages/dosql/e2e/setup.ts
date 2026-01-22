/**
 * DoSQL E2E Test Setup
 *
 * Handles deployment and teardown of DoSQL to a real Cloudflare account.
 * Uses wrangler CLI for deployment operations.
 *
 * @packageDocumentation
 */

import { execSync, spawn, type ChildProcess } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync, unlinkSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

// =============================================================================
// Types
// =============================================================================

export interface DeploymentConfig {
  /** Worker name for the deployment */
  workerName: string;
  /** Account ID (optional, uses CLOUDFLARE_ACCOUNT_ID env var) */
  accountId?: string;
  /** API token (optional, uses CLOUDFLARE_API_TOKEN env var) */
  apiToken?: string;
  /** Region hint for deployment */
  region?: 'wnam' | 'enam' | 'weur' | 'eeur' | 'apac';
  /** Custom deployment suffix (for parallel test runs) */
  suffix?: string;
  /** Timeout for deployment operations (ms) */
  timeoutMs?: number;
}

export interface DeploymentResult {
  /** Deployed worker URL */
  url: string;
  /** Worker name */
  workerName: string;
  /** Durable Object namespace ID */
  doNamespaceId: string;
  /** Deployment timestamp */
  deployedAt: number;
  /** Version/revision */
  version: string;
}

export interface E2ETestContext {
  /** Base URL for the deployed worker */
  baseUrl: string;
  /** Unique test run ID */
  testRunId: string;
  /** Cleanup function */
  cleanup: () => Promise<void>;
}

// =============================================================================
// Constants
// =============================================================================

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PACKAGE_ROOT = join(__dirname, '..');

const DEFAULT_CONFIG: DeploymentConfig = {
  workerName: 'dosql-e2e-test',
  timeoutMs: 120_000,
};

// E2E wrangler configuration
const E2E_WRANGLER_CONFIG = {
  name: 'dosql-e2e-test',
  main: 'src/worker/index.ts',
  compatibility_date: '2026-01-01',
  durable_objects: {
    bindings: [
      { name: 'DOSQL_DB', class_name: 'DoSQLDatabase' },
    ],
  },
  migrations: [
    { tag: 'v1', new_classes: ['DoSQLDatabase'] },
  ],
};

// =============================================================================
// Wrangler CLI Helpers
// =============================================================================

/**
 * Execute a wrangler command and return the output
 */
function wranglerExec(
  command: string,
  options: { cwd?: string; timeout?: number; env?: Record<string, string> } = {}
): string {
  const { cwd = PACKAGE_ROOT, timeout = 60_000, env = {} } = options;

  const fullEnv = {
    ...process.env,
    ...env,
    CLOUDFLARE_ACCOUNT_ID: env.CLOUDFLARE_ACCOUNT_ID || process.env.CLOUDFLARE_ACCOUNT_ID,
    CLOUDFLARE_API_TOKEN: env.CLOUDFLARE_API_TOKEN || process.env.CLOUDFLARE_API_TOKEN,
  };

  try {
    const result = execSync(`npx wrangler ${command}`, {
      cwd,
      timeout,
      env: fullEnv,
      stdio: ['pipe', 'pipe', 'pipe'],
      encoding: 'utf-8',
    });
    return result.toString().trim();
  } catch (error) {
    const execError = error as { stderr?: string; stdout?: string; message: string };
    throw new Error(
      `Wrangler command failed: wrangler ${command}\n` +
      `stderr: ${execError.stderr || ''}\n` +
      `stdout: ${execError.stdout || ''}\n` +
      `error: ${execError.message}`
    );
  }
}

/**
 * Check if wrangler is authenticated
 */
export function isAuthenticated(): boolean {
  try {
    wranglerExec('whoami', { timeout: 10_000 });
    return true;
  } catch {
    return false;
  }
}

/**
 * Get current account info
 */
export function getAccountInfo(): { email: string; accountId: string } | null {
  try {
    const output = wranglerExec('whoami', { timeout: 10_000 });
    const emailMatch = output.match(/Logged in as: (.+)/);
    const accountMatch = output.match(/Account ID: (\w+)/);

    if (emailMatch && accountMatch) {
      return {
        email: emailMatch[1].trim(),
        accountId: accountMatch[1].trim(),
      };
    }
    return null;
  } catch {
    return null;
  }
}

// =============================================================================
// Deployment Functions
// =============================================================================

/**
 * Generate E2E wrangler configuration file
 */
function generateWranglerConfig(config: DeploymentConfig): string {
  const workerName = config.suffix
    ? `${config.workerName}-${config.suffix}`
    : config.workerName;

  const wranglerConfig = {
    ...E2E_WRANGLER_CONFIG,
    name: workerName,
  };

  return JSON.stringify(wranglerConfig, null, 2);
}

/**
 * Deploy DoSQL to Cloudflare Workers
 */
export async function deploy(
  config: Partial<DeploymentConfig> = {}
): Promise<DeploymentResult> {
  const fullConfig: DeploymentConfig = { ...DEFAULT_CONFIG, ...config };

  // Validate authentication
  if (!isAuthenticated()) {
    throw new Error(
      'Not authenticated with Cloudflare. Run `npx wrangler login` or set CLOUDFLARE_API_TOKEN.'
    );
  }

  const workerName = fullConfig.suffix
    ? `${fullConfig.workerName}-${fullConfig.suffix}`
    : fullConfig.workerName;

  console.log(`[E2E Setup] Deploying ${workerName}...`);

  // Generate temporary wrangler.json for E2E
  const e2eWranglerPath = join(PACKAGE_ROOT, 'wrangler.e2e.json');
  const wranglerConfigContent = generateWranglerConfig(fullConfig);
  writeFileSync(e2eWranglerPath, wranglerConfigContent, 'utf-8');

  try {
    // Deploy the worker
    const deployOutput = wranglerExec(
      `deploy --config wrangler.e2e.json`,
      { timeout: fullConfig.timeoutMs }
    );

    // Parse deployment output for URL
    const urlMatch = deployOutput.match(/https:\/\/[^\s]+\.workers\.dev/);
    if (!urlMatch) {
      throw new Error(`Failed to parse deployment URL from output: ${deployOutput}`);
    }

    const url = urlMatch[0];

    // Get Durable Object namespace ID
    let doNamespaceId = 'unknown';
    try {
      const tailOutput = wranglerExec(`d1 list --json`, { timeout: 30_000 });
      // Parse if needed - for now we just need the worker URL
    } catch {
      // D1 list might fail, continue anyway
    }

    console.log(`[E2E Setup] Deployed to ${url}`);

    return {
      url,
      workerName,
      doNamespaceId,
      deployedAt: Date.now(),
      version: `e2e-${Date.now()}`,
    };
  } finally {
    // Clean up temporary config
    if (existsSync(e2eWranglerPath)) {
      unlinkSync(e2eWranglerPath);
    }
  }
}

/**
 * Tear down an E2E deployment
 */
export async function teardown(
  workerName: string,
  options: { force?: boolean } = {}
): Promise<void> {
  console.log(`[E2E Setup] Tearing down ${workerName}...`);

  try {
    wranglerExec(`delete ${workerName} --yes`, { timeout: 60_000 });
    console.log(`[E2E Setup] Successfully deleted ${workerName}`);
  } catch (error) {
    if (options.force) {
      console.warn(`[E2E Setup] Failed to delete ${workerName}, continuing...`);
    } else {
      throw error;
    }
  }
}

// =============================================================================
// Test Context Management
// =============================================================================

/**
 * Create a unique test context for E2E testing
 */
export async function createTestContext(
  config: Partial<DeploymentConfig> = {}
): Promise<E2ETestContext> {
  const testRunId = `e2e-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const suffix = testRunId;

  const deployment = await deploy({ ...config, suffix });

  return {
    baseUrl: deployment.url,
    testRunId,
    cleanup: async () => {
      await teardown(deployment.workerName, { force: true });
    },
  };
}

/**
 * Get the E2E endpoint URL from environment or deployment
 */
export function getE2EEndpoint(): string {
  // Check for pre-deployed endpoint
  const envEndpoint = process.env.DOSQL_E2E_ENDPOINT;
  if (envEndpoint) {
    return envEndpoint;
  }

  // Check for local endpoint file (created by setup script)
  const endpointFile = join(PACKAGE_ROOT, '.e2e-endpoint');
  if (existsSync(endpointFile)) {
    return readFileSync(endpointFile, 'utf-8').trim();
  }

  throw new Error(
    'No E2E endpoint configured. Either set DOSQL_E2E_ENDPOINT or run `npm run test:e2e:deploy`'
  );
}

/**
 * Wait for worker to be ready (cold start + initialization)
 */
export async function waitForReady(
  baseUrl: string,
  options: { timeoutMs?: number; intervalMs?: number } = {}
): Promise<void> {
  const { timeoutMs = 30_000, intervalMs = 500 } = options;
  const startTime = Date.now();

  while (Date.now() - startTime < timeoutMs) {
    try {
      const response = await fetch(`${baseUrl}/health`);
      if (response.ok) {
        const data = await response.json() as { status: string };
        if (data.status === 'ok') {
          return;
        }
      }
    } catch {
      // Continue waiting
    }

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error(`Worker not ready after ${timeoutMs}ms`);
}

// =============================================================================
// CLI Entrypoint
// =============================================================================

/**
 * Run setup from command line
 */
export async function runSetup(action: 'deploy' | 'cleanup'): Promise<void> {
  if (action === 'deploy') {
    const deployment = await deploy({
      suffix: process.env.E2E_SUFFIX || undefined,
    });

    // Save endpoint for tests
    const endpointFile = join(PACKAGE_ROOT, '.e2e-endpoint');
    writeFileSync(endpointFile, deployment.url, 'utf-8');

    console.log(`\nE2E endpoint saved to ${endpointFile}`);
    console.log(`Run tests with: npm run test:e2e`);
  } else if (action === 'cleanup') {
    const suffix = process.env.E2E_SUFFIX || undefined;
    const workerName = suffix ? `dosql-e2e-test-${suffix}` : 'dosql-e2e-test';
    await teardown(workerName, { force: true });

    // Remove endpoint file
    const endpointFile = join(PACKAGE_ROOT, '.e2e-endpoint');
    if (existsSync(endpointFile)) {
      unlinkSync(endpointFile);
    }
  }
}

// Run if executed directly
if (process.argv[1]?.endsWith('setup.ts') || process.argv[1]?.endsWith('setup.js')) {
  const action = process.argv[2] as 'deploy' | 'cleanup';
  if (!action || !['deploy', 'cleanup'].includes(action)) {
    console.error('Usage: npx tsx e2e/setup.ts [deploy|cleanup]');
    process.exit(1);
  }

  runSetup(action).catch((error) => {
    console.error('Setup failed:', error);
    process.exit(1);
  });
}

// =============================================================================
// Exports
// =============================================================================

export {
  PACKAGE_ROOT,
  wranglerExec,
};
