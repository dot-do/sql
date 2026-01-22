/**
 * DoSQL Bundle Size Analysis Script
 *
 * This script bundles DoSQL using esbuild and measures:
 * - Unminified size
 * - Minified size
 * - Gzipped size
 * - Per-module breakdown
 *
 * Usage: node scripts/bundle-analysis.mjs
 */

import * as esbuild from 'esbuild';
import { createWriteStream, existsSync, mkdirSync, readFileSync, statSync, writeFileSync, unlinkSync } from 'fs';
import { gzipSync } from 'zlib';
import { dirname, join, relative } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = join(__dirname, '..');
const OUTPUT_DIR = join(ROOT_DIR, 'dist-bundle-analysis');

// Ensure output directory exists
if (!existsSync(OUTPUT_DIR)) {
  mkdirSync(OUTPUT_DIR, { recursive: true });
}

/**
 * Format bytes to human-readable string
 */
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

/**
 * Calculate gzip size
 */
function gzipSize(content) {
  return gzipSync(Buffer.from(content)).length;
}

/**
 * Bundle configuration presets
 */
const BUNDLE_CONFIGS = {
  // Worker-only bundle (what actually runs in CF Workers)
  worker: {
    entryPoint: 'src/worker/index.ts',
    name: 'DoSQL Worker',
    description: 'Minimal worker bundle with B-tree, WAL, FSX only',
    external: ['cloudflare:*', 'node:*'],
  },

  // Full library bundle (all exports) - excluding Node.js deps
  full: {
    entryPoint: 'src/index.ts',
    name: 'DoSQL Full Library',
    description: 'Complete library with all modules (excluding Node.js deps)',
    external: ['cloudflare:*', 'node:*', 'fs', 'os', 'path', 'child_process', 'crypto', 'ai-evaluate'],
  },

  // Full library with capnweb
  fullWithCapnweb: {
    entryPoint: 'src/index.ts',
    name: 'DoSQL + capnweb',
    description: 'Full library with capnweb RPC (excluding ai-evaluate)',
    external: ['cloudflare:*', 'node:*', 'fs', 'os', 'path', 'child_process', 'crypto', 'ai-evaluate'],
  },

  // Database module only (for testing individual module sizes)
  database: {
    entryPoint: 'src/database.ts',
    name: 'DoSQL Database',
    description: 'High-level database API only',
    external: ['cloudflare:*', 'node:*', 'fs', 'os', 'path', 'child_process', 'crypto', 'ai-evaluate', 'capnweb', 'iceberg-js'],
  },
};

/**
 * Run esbuild with metafile for analysis
 */
async function bundleWithMetafile(entryPoint, outputPath, minify = false, external = ['cloudflare:*', 'node:*']) {
  const result = await esbuild.build({
    entryPoints: [join(ROOT_DIR, entryPoint)],
    bundle: true,
    outfile: outputPath,
    format: 'esm',
    platform: 'browser', // Workers use browser-like environment
    target: 'es2022',
    minify,
    metafile: true,
    treeShaking: true,
    // External cloudflare modules
    external,
    // Suppress warnings for now
    logLevel: 'silent',
    // Don't resolve dependencies that won't work in Workers
    define: {
      'process.env.NODE_ENV': '"production"',
    },
  });

  return result;
}

/**
 * Analyze metafile to get per-module breakdown
 */
function analyzeMetafile(metafile, outputPath) {
  const output = metafile.outputs[outputPath] || Object.values(metafile.outputs)[0];
  if (!output) {
    return { modules: [], totalInputs: 0 };
  }

  const modules = [];
  let totalInputs = 0;

  for (const [path, info] of Object.entries(output.inputs)) {
    const size = info.bytesInOutput;
    totalInputs += size;

    // Categorize module
    let category = 'other';
    if (path.includes('node_modules/capnweb')) category = 'capnweb';
    else if (path.includes('node_modules/iceberg-js')) category = 'iceberg-js';
    else if (path.includes('node_modules/ai-evaluate')) category = 'ai-evaluate';
    else if (path.includes('node_modules/')) category = 'node_modules (other)';
    else if (path.includes('src/btree')) category = 'dosql/btree';
    else if (path.includes('src/fsx')) category = 'dosql/fsx';
    else if (path.includes('src/wal')) category = 'dosql/wal';
    else if (path.includes('src/worker')) category = 'dosql/worker';
    else if (path.includes('src/proc')) category = 'dosql/proc';
    else if (path.includes('src/columnar')) category = 'dosql/columnar';
    else if (path.includes('src/cdc')) category = 'dosql/cdc';
    else if (path.includes('src/sharding')) category = 'dosql/sharding';
    else if (path.includes('src/transaction')) category = 'dosql/transaction';
    else if (path.includes('src/virtual')) category = 'dosql/virtual';
    else if (path.includes('src/parser')) category = 'dosql/parser';
    else if (path.includes('src/schema')) category = 'dosql/schema';
    else if (path.includes('src/sources')) category = 'dosql/sources';
    else if (path.includes('src/')) category = 'dosql/core';

    modules.push({
      path,
      size,
      category,
    });
  }

  return { modules, totalInputs };
}

/**
 * Aggregate modules by category
 */
function aggregateByCategory(modules) {
  const categories = new Map();

  for (const mod of modules) {
    const current = categories.get(mod.category) || { size: 0, count: 0, files: [] };
    current.size += mod.size;
    current.count++;
    current.files.push(mod.path);
    categories.set(mod.category, current);
  }

  return Array.from(categories.entries())
    .map(([category, data]) => ({ category, ...data }))
    .sort((a, b) => b.size - a.size);
}

/**
 * Main analysis function
 */
async function runAnalysis() {
  console.log('DoSQL Bundle Size Analysis');
  console.log('='.repeat(60));
  console.log();

  const results = {};

  for (const [key, config] of Object.entries(BUNDLE_CONFIGS)) {
    console.log(`Analyzing: ${config.name}`);
    console.log(`Entry: ${config.entryPoint}`);
    console.log('-'.repeat(60));

    const unminifiedPath = join(OUTPUT_DIR, `${key}.js`);
    const minifiedPath = join(OUTPUT_DIR, `${key}.min.js`);

    const external = config.external || ['cloudflare:*', 'node:*'];

    // Bundle unminified
    let unminifiedResult;
    try {
      unminifiedResult = await bundleWithMetafile(config.entryPoint, unminifiedPath, false, external);
    } catch (err) {
      console.error(`Failed to bundle ${key}: ${err.message}`);
      results[key] = { error: err.message, config };
      continue;
    }

    // Bundle minified
    let minifiedResult;
    try {
      minifiedResult = await bundleWithMetafile(config.entryPoint, minifiedPath, true, external);
    } catch (err) {
      console.error(`Failed to minify ${key}: ${err.message}`);
      results[key] = { error: err.message, config };
      continue;
    }

    // Read file sizes
    const unminifiedContent = readFileSync(unminifiedPath, 'utf-8');
    const minifiedContent = readFileSync(minifiedPath, 'utf-8');

    const unminifiedSize = Buffer.byteLength(unminifiedContent, 'utf-8');
    const minifiedSize = Buffer.byteLength(minifiedContent, 'utf-8');
    const gzippedSize = gzipSize(minifiedContent);

    // Analyze metafile
    const { modules, totalInputs } = analyzeMetafile(unminifiedResult.metafile, unminifiedPath);
    const categoryBreakdown = aggregateByCategory(modules);

    results[key] = {
      config,
      sizes: {
        unminified: unminifiedSize,
        minified: minifiedSize,
        gzipped: gzippedSize,
      },
      categoryBreakdown,
      modules,
    };

    // Print summary
    console.log(`  Unminified: ${formatBytes(unminifiedSize)}`);
    console.log(`  Minified:   ${formatBytes(minifiedSize)}`);
    console.log(`  Gzipped:    ${formatBytes(gzippedSize)}`);
    console.log();
    console.log('  Category Breakdown:');
    for (const cat of categoryBreakdown.slice(0, 10)) {
      const pct = ((cat.size / unminifiedSize) * 100).toFixed(1);
      console.log(`    ${cat.category.padEnd(25)} ${formatBytes(cat.size).padStart(10)} (${pct}%, ${cat.count} files)`);
    }
    console.log();
  }

  // Write detailed JSON report
  const reportPath = join(OUTPUT_DIR, 'bundle-analysis.json');
  writeFileSync(reportPath, JSON.stringify(results, null, 2));
  console.log(`Detailed report written to: ${reportPath}`);

  // Generate markdown report
  const mdReport = generateMarkdownReport(results);
  const mdPath = join(ROOT_DIR, 'docs', 'bundle-analysis.md');

  // Ensure docs directory exists
  const docsDir = dirname(mdPath);
  if (!existsSync(docsDir)) {
    mkdirSync(docsDir, { recursive: true });
  }

  writeFileSync(mdPath, mdReport);
  console.log(`Markdown report written to: ${mdPath}`);

  return results;
}

/**
 * Generate markdown report
 */
function generateMarkdownReport(results) {
  const lines = [];

  lines.push('# DoSQL Bundle Size Analysis');
  lines.push('');
  lines.push('Generated: ' + new Date().toISOString());
  lines.push('');
  lines.push('## Executive Summary');
  lines.push('');
  lines.push('This analysis examines the bundle size of DoSQL when compiled for Cloudflare Workers using esbuild.');
  lines.push('');

  // Summary table
  lines.push('| Bundle | Unminified | Minified | Gzipped | CF Free Limit | CF Paid Limit |');
  lines.push('|--------|------------|----------|---------|---------------|---------------|');

  for (const [key, data] of Object.entries(results)) {
    if (data.error) {
      lines.push(`| ${data.config?.name || key} | Error: ${data.error} | - | - | - | - |`);
      continue;
    }

    const unmin = formatBytes(data.sizes.unminified);
    const min = formatBytes(data.sizes.minified);
    const gz = formatBytes(data.sizes.gzipped);
    const freePct = ((data.sizes.gzipped / (1024 * 1024)) * 100).toFixed(1);
    const paidPct = ((data.sizes.gzipped / (10 * 1024 * 1024)) * 100).toFixed(1);

    lines.push(`| ${data.config.name} | ${unmin} | ${min} | ${gz} | ${freePct}% of 1MB | ${paidPct}% of 10MB |`);
  }

  lines.push('');
  lines.push('## Cloudflare Worker Limits');
  lines.push('');
  lines.push('- **Free tier**: 1 MB compressed (gzipped)');
  lines.push('- **Paid tier**: 10 MB compressed (gzipped)');
  lines.push('');

  // Detailed breakdown for each bundle
  for (const [key, data] of Object.entries(results)) {
    if (data.error) continue;

    lines.push(`## ${data.config.name}`);
    lines.push('');
    lines.push(`**Entry Point**: \`${data.config.entryPoint}\``);
    lines.push('');
    lines.push(`**Description**: ${data.config.description}`);
    lines.push('');
    lines.push('### Size Metrics');
    lines.push('');
    lines.push(`- Unminified: **${formatBytes(data.sizes.unminified)}**`);
    lines.push(`- Minified: **${formatBytes(data.sizes.minified)}**`);
    lines.push(`- Gzipped: **${formatBytes(data.sizes.gzipped)}**`);
    lines.push('');
    lines.push('### Module Breakdown by Category');
    lines.push('');
    lines.push('| Category | Size | % of Total | File Count |');
    lines.push('|----------|------|------------|------------|');

    for (const cat of data.categoryBreakdown) {
      const pct = ((cat.size / data.sizes.unminified) * 100).toFixed(1);
      lines.push(`| ${cat.category} | ${formatBytes(cat.size)} | ${pct}% | ${cat.count} |`);
    }

    lines.push('');
  }

  // Recommendations
  lines.push('## Bundle Size Reduction Opportunities');
  lines.push('');
  lines.push('### 1. External Dependencies Analysis');
  lines.push('');

  const workerData = results.worker;
  if (workerData && !workerData.error) {
    const externalDeps = workerData.categoryBreakdown.filter(c =>
      c.category === 'capnweb' ||
      c.category === 'iceberg-js' ||
      c.category === 'ai-evaluate' ||
      c.category === 'node_modules (other)'
    );

    if (externalDeps.length === 0) {
      lines.push('Worker bundle successfully excludes heavy external dependencies!');
    } else {
      lines.push('External dependencies included in bundle:');
      lines.push('');
      for (const dep of externalDeps) {
        lines.push(`- **${dep.category}**: ${formatBytes(dep.size)} (${dep.count} files)`);
      }
    }
  }

  lines.push('');
  lines.push('### 2. Code Splitting Recommendations');
  lines.push('');
  lines.push('Consider splitting these modules into separate entry points:');
  lines.push('');
  lines.push('- **dosql/proc** - Stored procedures can be loaded on-demand');
  lines.push('- **dosql/sharding** - Only needed for multi-DO deployments');
  lines.push('- **dosql/columnar** - Optional columnar format support');
  lines.push('- **dosql/cdc** - Change data capture is optional');
  lines.push('');

  lines.push('### 3. Tree Shaking Improvements');
  lines.push('');
  lines.push('- Mark side-effect-free modules in package.json');
  lines.push('- Use explicit exports instead of `export *`');
  lines.push('- Consider `/* @__PURE__ */` annotations for factory functions');
  lines.push('');

  lines.push('### 4. Dependency Alternatives');
  lines.push('');
  lines.push('| Current | Alternative | Potential Savings |');
  lines.push('|---------|-------------|-------------------|');
  lines.push('| capnweb (full) | capnweb/lite | TBD |');
  lines.push('| iceberg-js | Custom minimal impl | TBD |');
  lines.push('| ai-evaluate | Exclude from worker | Full removal |');
  lines.push('');

  lines.push('### 5. Build-Time Optimizations');
  lines.push('');
  lines.push('- Use `esbuild --analyze` to identify dead code');
  lines.push('- Consider Terser for additional minification (2-5% smaller)');
  lines.push('- Use `define` to eliminate dead branches');
  lines.push('');

  lines.push('## Comparison to Limits');
  lines.push('');

  if (workerData && !workerData.error) {
    const gzipped = workerData.sizes.gzipped;
    const freeMB = 1024 * 1024;
    const paidMB = 10 * 1024 * 1024;

    if (gzipped < freeMB) {
      lines.push(`Worker bundle (${formatBytes(gzipped)} gzipped) fits within the **free tier** 1MB limit.`);
    } else if (gzipped < paidMB) {
      lines.push(`Worker bundle (${formatBytes(gzipped)} gzipped) requires **paid tier** but fits within 10MB limit.`);
    } else {
      lines.push(`WARNING: Worker bundle (${formatBytes(gzipped)} gzipped) EXCEEDS the 10MB paid tier limit!`);
    }

    lines.push('');
    lines.push(`Remaining capacity (free tier): ${formatBytes(freeMB - gzipped)}`);
    lines.push(`Remaining capacity (paid tier): ${formatBytes(paidMB - gzipped)}`);
  }

  lines.push('');
  lines.push('## Build Commands');
  lines.push('');
  lines.push('```bash');
  lines.push('# Generate this analysis');
  lines.push('node scripts/bundle-analysis.mjs');
  lines.push('');
  lines.push('# Build worker bundle manually');
  lines.push('npx esbuild src/worker/index.ts --bundle --minify --format=esm --outfile=dist-bundle/worker.min.js --external:cloudflare:*');
  lines.push('');
  lines.push('# Analyze with metafile');
  lines.push('npx esbuild src/worker/index.ts --bundle --metafile=meta.json --outfile=dist-bundle/worker.js --external:cloudflare:*');
  lines.push('```');
  lines.push('');

  return lines.join('\n');
}

// Run analysis
runAnalysis().catch(console.error);
