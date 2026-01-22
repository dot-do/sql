/**
 * DoLake E2E Test Setup
 *
 * Handles deployment and teardown of DoLake to a real Cloudflare account.
 * Uses wrangler CLI for deployment operations.
 *
 * @packageDocumentation
 */

import { execSync } from 'node:child_process';
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
export const PACKAGE_ROOT = join(__dirname, '..');
export const TEST_WORKER_DIR = join(__dirname, 'test-worker');

const DEFAULT_CONFIG: DeploymentConfig = {
  workerName: 'dolake-e2e-test',
  timeoutMs: 120_000,
};

// =============================================================================
// Wrangler CLI Helpers
// =============================================================================

/**
 * Execute a wrangler command and return the output
 */
export function wranglerExec(
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
 * Generate a temporary wrangler.toml with custom worker name and bucket
 */
function generateWranglerToml(config: DeploymentConfig): string {
  const workerName = config.suffix
    ? `${config.workerName}-${config.suffix}`
    : config.workerName;

  const bucketName = `${workerName}-bucket`;

  return `# DoLake E2E Test Worker (Generated)
#
# Auto-generated wrangler configuration for E2E testing.
# Worker: ${workerName}
# Generated: ${new Date().toISOString()}

name = "${workerName}"
main = "src/index.ts"
compatibility_date = "2026-01-01"

# Durable Objects
[durable_objects]
bindings = [
  { name = "DOLAKE", class_name = "DoLake" }
]

# R2 Buckets
[[r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "${bucketName}"

# Migrations for Durable Object
[[migrations]]
tag = "v1"
new_classes = ["DoLake"]

# Environment variables
[vars]
ENVIRONMENT = "e2e-test"
LOG_LEVEL = "debug"
`;
}

/**
 * Ensure R2 bucket exists for E2E testing
 */
async function ensureR2Bucket(bucketName: string): Promise<void> {
  try {
    // Check if bucket exists
    wranglerExec(`r2 bucket list`, { timeout: 30_000 });
    // Try to create if it doesn't exist (will fail silently if exists)
    try {
      wranglerExec(`r2 bucket create ${bucketName}`, { timeout: 30_000 });
      console.log(`[DoLake E2E Setup] Created R2 bucket: ${bucketName}`);
    } catch {
      // Bucket likely already exists
      console.log(`[DoLake E2E Setup] R2 bucket ${bucketName} already exists or could not be created`);
    }
  } catch (error) {
    console.warn(`[DoLake E2E Setup] Could not verify R2 bucket: ${error}`);
  }
}

/**
 * Deploy DoLake to Cloudflare Workers using the test-worker
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

  const bucketName = `${workerName}-bucket`;

  console.log(`[DoLake E2E Setup] Deploying ${workerName}...`);
  console.log(`[DoLake E2E Setup] Test worker directory: ${TEST_WORKER_DIR}`);

  // Verify test-worker directory exists
  if (!existsSync(TEST_WORKER_DIR)) {
    throw new Error(
      `Test worker directory not found: ${TEST_WORKER_DIR}\n` +
      'Please ensure the e2e/test-worker directory exists with wrangler.toml and src/index.ts'
    );
  }

  // Ensure R2 bucket exists
  await ensureR2Bucket(bucketName);

  // Generate temporary wrangler.toml with custom name
  const tempWranglerPath = join(TEST_WORKER_DIR, 'wrangler.e2e.toml');
  const wranglerContent = generateWranglerToml(fullConfig);
  writeFileSync(tempWranglerPath, wranglerContent, 'utf-8');

  try {
    // Deploy the worker using the test-worker directory
    const deployOutput = wranglerExec(
      `deploy --config wrangler.e2e.toml`,
      {
        cwd: TEST_WORKER_DIR,
        timeout: fullConfig.timeoutMs,
      }
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
      // Parse DO namespace from wrangler output if available
      const doMatch = deployOutput.match(/DOLAKE.*?([a-f0-9-]{36})/i);
      if (doMatch) {
        doNamespaceId = doMatch[1];
      }
    } catch {
      // DO namespace parsing might fail, continue anyway
    }

    console.log(`[DoLake E2E Setup] Deployed to ${url}`);

    return {
      url,
      workerName,
      doNamespaceId,
      deployedAt: Date.now(),
      version: `e2e-${Date.now()}`,
    };
  } finally {
    // Clean up temporary config
    if (existsSync(tempWranglerPath)) {
      unlinkSync(tempWranglerPath);
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
  console.log(`[DoLake E2E Setup] Tearing down ${workerName}...`);

  try {
    wranglerExec(`delete ${workerName} --yes`, { timeout: 60_000 });
    console.log(`[DoLake E2E Setup] Successfully deleted ${workerName}`);
  } catch (error) {
    if (options.force) {
      console.warn(`[DoLake E2E Setup] Failed to delete ${workerName}, continuing...`);
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
  const envEndpoint = process.env.DOLAKE_E2E_ENDPOINT || process.env.DOLAKE_URL;
  if (envEndpoint) {
    return envEndpoint;
  }

  // Check for local endpoint file (created by setup script)
  const endpointFile = join(PACKAGE_ROOT, '.e2e-endpoint');
  if (existsSync(endpointFile)) {
    return readFileSync(endpointFile, 'utf-8').trim();
  }

  throw new Error(
    'No E2E endpoint configured. Either set DOLAKE_E2E_ENDPOINT or DOLAKE_URL, or run `npm run test:e2e:deploy`'
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

/**
 * Measure cold start latency
 */
export async function measureColdStart(
  baseUrl: string,
  options: { iterations?: number } = {}
): Promise<{ p50: number; p95: number; p99: number; min: number; max: number; avg: number }> {
  const { iterations = 5 } = options;
  const samples: number[] = [];

  for (let i = 0; i < iterations; i++) {
    // Use a unique path to ensure cold start
    const uniquePath = `/health?cold=${Date.now()}-${i}`;

    const start = performance.now();
    await fetch(`${baseUrl}${uniquePath}`);
    const elapsed = performance.now() - start;
    samples.push(elapsed);

    // Wait a bit between iterations
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  samples.sort((a, b) => a - b);

  const percentile = (p: number) => {
    const index = Math.ceil((p / 100) * samples.length) - 1;
    return samples[Math.max(0, index)];
  };

  return {
    p50: percentile(50),
    p95: percentile(95),
    p99: percentile(99),
    min: samples[0],
    max: samples[samples.length - 1],
    avg: samples.reduce((a, b) => a + b, 0) / samples.length,
  };
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
    const workerName = suffix ? `dolake-e2e-test-${suffix}` : 'dolake-e2e-test';
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
