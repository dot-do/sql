#!/usr/bin/env npx tsx
/**
 * Benchmark CLI Entry Point
 *
 * Run benchmarks from the command line.
 *
 * Usage:
 *   npx tsx benchmark/src/cli.ts -a dosql -s simple-select
 *   npx tsx benchmark/src/cli.ts --adapter dosql --scenario insert --iterations 500
 */

import { main } from './runner.js';

// Run the CLI
main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
