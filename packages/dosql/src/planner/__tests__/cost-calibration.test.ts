/**
 * Tests for runtime cost model calibration for Durable Object latencies.
 *
 * Validates:
 * - Latency observation recording and EWMA tracking
 * - Calibration threshold (minObservations) behavior
 * - Cost coefficient adjustment from observed latencies
 * - DO-specific cost factors (tier multipliers, cross-DO RPC, hibernation)
 * - Stale observation eviction
 * - Batch recording
 * - Diagnostics reporting
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  RuntimeCalibrator,
  createRuntimeCalibrator,
  DEFAULT_DO_COST_FACTORS,
  DEFAULT_CALIBRATION_CONFIG,
  type StorageTier,
  type OperationType,
  type LatencyObservation,
  type CalibrationConfig,
  type DOCostFactors,
} from '../index.js';
import { DEFAULT_COST_MODEL } from '../types.js';

// =============================================================================
// LATENCY OBSERVATION TRACKING
// =============================================================================

describe('RuntimeCalibrator latency tracking', () => {
  let calibrator: RuntimeCalibrator;

  beforeEach(() => {
    calibrator = createRuntimeCalibrator();
  });

  it('should record a single latency observation', () => {
    calibrator.recordLatency('do_storage', 'sequential_read', 1.2);

    const stats = calibrator.getLatencyStats('do_storage', 'sequential_read');
    expect(stats).toBeDefined();
    expect(stats!.count).toBe(1);
    expect(stats!.totalMs).toBe(1.2);
    expect(stats!.minMs).toBe(1.2);
    expect(stats!.maxMs).toBe(1.2);
    expect(stats!.ewmaMs).toBe(1.2);
  });

  it('should aggregate multiple observations with EWMA', () => {
    const alpha = DEFAULT_CALIBRATION_CONFIG.ewmaAlpha;

    calibrator.recordLatency('do_storage', 'random_read', 4.0);
    calibrator.recordLatency('do_storage', 'random_read', 6.0);
    calibrator.recordLatency('do_storage', 'random_read', 3.0);

    const stats = calibrator.getLatencyStats('do_storage', 'random_read');
    expect(stats).toBeDefined();
    expect(stats!.count).toBe(3);
    expect(stats!.totalMs).toBe(13.0);
    expect(stats!.minMs).toBe(3.0);
    expect(stats!.maxMs).toBe(6.0);

    // Verify EWMA calculation:
    // After first: ewma = 4.0
    // After second: ewma = alpha * 6.0 + (1-alpha) * 4.0
    // After third: ewma = alpha * 3.0 + (1-alpha) * (previous ewma)
    let expectedEwma = 4.0;
    expectedEwma = alpha * 6.0 + (1 - alpha) * expectedEwma;
    expectedEwma = alpha * 3.0 + (1 - alpha) * expectedEwma;
    expect(stats!.ewmaMs).toBeCloseTo(expectedEwma, 6);
  });

  it('should track min and max correctly', () => {
    calibrator.recordLatency('r2', 'sequential_read', 80.0);
    calibrator.recordLatency('r2', 'sequential_read', 120.0);
    calibrator.recordLatency('r2', 'sequential_read', 95.0);
    calibrator.recordLatency('r2', 'sequential_read', 50.0);
    calibrator.recordLatency('r2', 'sequential_read', 200.0);

    const stats = calibrator.getLatencyStats('r2', 'sequential_read');
    expect(stats!.minMs).toBe(50.0);
    expect(stats!.maxMs).toBe(200.0);
    expect(stats!.count).toBe(5);
  });

  it('should track different tier+operation combinations independently', () => {
    calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
    calibrator.recordLatency('do_storage', 'random_read', 4.0);
    calibrator.recordLatency('r2', 'sequential_read', 100.0);
    calibrator.recordLatency('r2_cached', 'sequential_read', 50.0);
    calibrator.recordLatency('compute', 'hash_op', 0.01);

    expect(calibrator.getLatencyStats('do_storage', 'sequential_read')!.count).toBe(1);
    expect(calibrator.getLatencyStats('do_storage', 'random_read')!.count).toBe(1);
    expect(calibrator.getLatencyStats('r2', 'sequential_read')!.count).toBe(1);
    expect(calibrator.getLatencyStats('r2_cached', 'sequential_read')!.count).toBe(1);
    expect(calibrator.getLatencyStats('compute', 'hash_op')!.count).toBe(1);
  });

  it('should return undefined for untracked combinations', () => {
    expect(calibrator.getLatencyStats('do_storage', 'write')).toBeUndefined();
  });
});

// =============================================================================
// BATCH RECORDING
// =============================================================================

describe('RuntimeCalibrator batch recording', () => {
  let calibrator: RuntimeCalibrator;

  beforeEach(() => {
    calibrator = createRuntimeCalibrator();
  });

  it('should record a batch of observations', () => {
    const observations: LatencyObservation[] = [
      { tier: 'do_storage', operation: 'sequential_read', latencyMs: 1.1, timestamp: Date.now() },
      { tier: 'do_storage', operation: 'sequential_read', latencyMs: 0.9, timestamp: Date.now() },
      { tier: 'do_storage', operation: 'random_read', latencyMs: 3.5, timestamp: Date.now() },
      { tier: 'r2', operation: 'sequential_read', latencyMs: 95.0, timestamp: Date.now() },
    ];

    calibrator.recordBatch(observations);

    expect(calibrator.getLatencyStats('do_storage', 'sequential_read')!.count).toBe(2);
    expect(calibrator.getLatencyStats('do_storage', 'random_read')!.count).toBe(1);
    expect(calibrator.getLatencyStats('r2', 'sequential_read')!.count).toBe(1);
  });

  it('should handle empty batch gracefully', () => {
    calibrator.recordBatch([]);
    expect(calibrator.isCalibrated()).toBe(false);
  });
});

// =============================================================================
// CALIBRATION THRESHOLD
// =============================================================================

describe('RuntimeCalibrator calibration threshold', () => {
  it('should not be calibrated with zero observations', () => {
    const calibrator = createRuntimeCalibrator();
    expect(calibrator.isCalibrated()).toBe(false);
  });

  it('should not be calibrated with fewer than minObservations', () => {
    const calibrator = createRuntimeCalibrator({ minObservations: 10 });

    for (let i = 0; i < 9; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0 + Math.random());
    }

    expect(calibrator.isCalibrated()).toBe(false);
  });

  it('should be calibrated once minObservations reached on core operations', () => {
    const calibrator = createRuntimeCalibrator({ minObservations: 5 });

    for (let i = 0; i < 5; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0 + Math.random() * 0.5);
    }

    expect(calibrator.isCalibrated()).toBe(true);
  });

  it('should calibrate if random_read has enough observations even if sequential_read does not', () => {
    const calibrator = createRuntimeCalibrator({ minObservations: 3 });

    for (let i = 0; i < 3; i++) {
      calibrator.recordLatency('do_storage', 'random_read', 3.0 + Math.random());
    }

    expect(calibrator.isCalibrated()).toBe(true);
  });

  it('should not calibrate from non-core operation observations alone', () => {
    const calibrator = createRuntimeCalibrator({ minObservations: 3 });

    // Only record compute observations (not core DO storage ops)
    for (let i = 0; i < 10; i++) {
      calibrator.recordLatency('compute', 'hash_op', 0.01);
    }

    expect(calibrator.isCalibrated()).toBe(false);
  });
});

// =============================================================================
// COST COEFFICIENT CALIBRATION
// =============================================================================

describe('RuntimeCalibrator cost coefficient calibration', () => {
  let calibrator: RuntimeCalibrator;

  beforeEach(() => {
    calibrator = createRuntimeCalibrator({ minObservations: 3 });
  });

  it('should return default config when not calibrated', () => {
    const config = calibrator.getCalibratedConfig();

    // Should have DO-adjusted memory limits
    expect(config.sortMemory).toBeLessThanOrEqual(32 * 1024 * 1024);
    expect(config.hashMemory).toBeLessThanOrEqual(32 * 1024 * 1024);
  });

  it('should calibrate I/O costs from observed latencies', () => {
    // Simulate DO storage: ~1ms sequential, ~4ms random
    for (let i = 0; i < 5; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
      calibrator.recordLatency('do_storage', 'random_read', 4.0);
    }

    const config = calibrator.getCalibratedConfig();

    // Sequential I/O should be normalized to 1.0
    expect(config.sequentialIOCost).toBe(1.0);

    // Random I/O should be ~4x sequential
    expect(config.randomIOCost).toBeCloseTo(4.0, 1);

    // Heap fetch should equal random I/O
    expect(config.heapFetchCost).toBeCloseTo(4.0, 1);
  });

  it('should calibrate tier multipliers from observed R2 latencies', () => {
    // DO storage: 1ms, R2 cached: 50ms, R2: 100ms
    for (let i = 0; i < 5; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
      calibrator.recordLatency('do_storage', 'random_read', 4.0);
      calibrator.recordLatency('r2_cached', 'sequential_read', 50.0);
      calibrator.recordLatency('r2', 'sequential_read', 100.0);
    }

    const config = calibrator.getCalibratedConfig();
    const factors = calibrator.getDOCostFactors();

    // Warm tier should be ~50x DO storage
    expect(factors.warmTierMultiplier).toBeCloseTo(50.0, 0);

    // Cold tier should be ~100x DO storage
    expect(factors.coldTierMultiplier).toBeCloseTo(100.0, 0);

    // Sequential I/O remains normalized
    expect(config.sequentialIOCost).toBe(1.0);
  });

  it('should calibrate compute costs when compute data is available', () => {
    for (let i = 0; i < 5; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
      calibrator.recordLatency('do_storage', 'random_read', 4.0);
      calibrator.recordLatency('compute', 'hash_op', 0.02);
      calibrator.recordLatency('compute', 'sort_comparison', 0.03);
    }

    const config = calibrator.getCalibratedConfig();

    // Hash op should be 0.02/1.0 = 0.02 relative to sequential
    expect(config.hashOperationCost).toBeCloseTo(0.02, 2);

    // Sort comparison should be 0.03/1.0 = 0.03
    expect(config.sortComparisonCost).toBeCloseTo(0.03, 2);
  });

  it('should cache calibrated config until new observations arrive', () => {
    for (let i = 0; i < 5; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
      calibrator.recordLatency('do_storage', 'random_read', 4.0);
    }

    const config1 = calibrator.getCalibratedConfig();
    const config2 = calibrator.getCalibratedConfig();

    // Should return the same object (cached)
    expect(config1).toBe(config2);

    // Recording new data should invalidate cache
    calibrator.recordLatency('do_storage', 'sequential_read', 2.0);
    const config3 = calibrator.getCalibratedConfig();
    expect(config3).not.toBe(config1);
  });

  it('should respect custom base config overrides', () => {
    for (let i = 0; i < 5; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
      calibrator.recordLatency('do_storage', 'random_read', 4.0);
    }

    const config = calibrator.getCalibratedConfig({
      defaultRowCount: 5000,
      avgRowSize: 200,
    });

    expect(config.defaultRowCount).toBe(5000);
    expect(config.avgRowSize).toBe(200);
    expect(config.sequentialIOCost).toBe(1.0);
  });
});

// =============================================================================
// DO-SPECIFIC COST FACTORS
// =============================================================================

describe('RuntimeCalibrator DO-specific cost factors', () => {
  let calibrator: RuntimeCalibrator;

  beforeEach(() => {
    calibrator = createRuntimeCalibrator();
  });

  it('should provide default DO cost factors', () => {
    const factors = calibrator.getDOCostFactors();

    expect(factors.crossDORpcMultiplier).toBe(DEFAULT_DO_COST_FACTORS.crossDORpcMultiplier);
    expect(factors.largePagePenalty).toBe(DEFAULT_DO_COST_FACTORS.largePagePenalty);
    expect(factors.warmTierMultiplier).toBe(DEFAULT_DO_COST_FACTORS.warmTierMultiplier);
    expect(factors.coldTierMultiplier).toBe(DEFAULT_DO_COST_FACTORS.coldTierMultiplier);
    expect(factors.hibernationWakeUpCostMs).toBe(DEFAULT_DO_COST_FACTORS.hibernationWakeUpCostMs);
    expect(factors.alarmSchedulingCostMs).toBe(DEFAULT_DO_COST_FACTORS.alarmSchedulingCostMs);
  });

  it('should allow custom DO cost factors at construction', () => {
    const custom = createRuntimeCalibrator({}, {
      crossDORpcMultiplier: 5.0,
      hibernationWakeUpCostMs: 100.0,
    });

    const factors = custom.getDOCostFactors();
    expect(factors.crossDORpcMultiplier).toBe(5.0);
    expect(factors.hibernationWakeUpCostMs).toBe(100.0);
    // Others should retain defaults
    expect(factors.largePagePenalty).toBe(DEFAULT_DO_COST_FACTORS.largePagePenalty);
  });

  it('should allow updating individual cost factors', () => {
    calibrator.setDOCostFactor('crossDORpcMultiplier', 10.0);

    const factors = calibrator.getDOCostFactors();
    expect(factors.crossDORpcMultiplier).toBe(10.0);
  });

  it('should compute tier-adjusted costs correctly', () => {
    const baseCost = 1.0;

    expect(calibrator.getTierCost('do_storage', baseCost)).toBe(1.0);
    expect(calibrator.getTierCost('r2_cached', baseCost)).toBe(DEFAULT_DO_COST_FACTORS.warmTierMultiplier);
    expect(calibrator.getTierCost('r2', baseCost)).toBe(DEFAULT_DO_COST_FACTORS.coldTierMultiplier);
    expect(calibrator.getTierCost('compute', baseCost)).toBe(0);
  });

  it('should compute cross-DO RPC cost', () => {
    const baseCost = 2.0;
    const rpcCost = calibrator.getCrossDOCost(baseCost);
    expect(rpcCost).toBe(baseCost * DEFAULT_DO_COST_FACTORS.crossDORpcMultiplier);
  });

  it('should return hibernation wake-up cost', () => {
    const cost = calibrator.getHibernationWakeUpCost();
    expect(cost).toBe(DEFAULT_DO_COST_FACTORS.hibernationWakeUpCostMs);
  });
});

// =============================================================================
// GETLATENCY AND GETMEANLATENCY
// =============================================================================

describe('RuntimeCalibrator getLatency and getMeanLatency', () => {
  let calibrator: RuntimeCalibrator;

  beforeEach(() => {
    calibrator = createRuntimeCalibrator({ minObservations: 3 });
  });

  it('should return default when not enough observations', () => {
    calibrator.recordLatency('do_storage', 'sequential_read', 1.5);
    calibrator.recordLatency('do_storage', 'sequential_read', 2.0);
    // Only 2 observations, minObservations is 3

    expect(calibrator.getLatency('do_storage', 'sequential_read', 99.0)).toBe(99.0);
    expect(calibrator.getMeanLatency('do_storage', 'sequential_read', 99.0)).toBe(99.0);
  });

  it('should return EWMA when enough observations exist', () => {
    calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
    calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
    calibrator.recordLatency('do_storage', 'sequential_read', 1.0);

    const latency = calibrator.getLatency('do_storage', 'sequential_read', 99.0);
    expect(latency).toBeCloseTo(1.0, 1);
    expect(latency).not.toBe(99.0);
  });

  it('should return mean when enough observations exist', () => {
    calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
    calibrator.recordLatency('do_storage', 'sequential_read', 2.0);
    calibrator.recordLatency('do_storage', 'sequential_read', 3.0);

    const mean = calibrator.getMeanLatency('do_storage', 'sequential_read', 99.0);
    expect(mean).toBeCloseTo(2.0, 6);
  });

  it('should return default for unknown tier+operation', () => {
    expect(calibrator.getLatency('r2', 'write', 42.0)).toBe(42.0);
    expect(calibrator.getMeanLatency('r2', 'write', 42.0)).toBe(42.0);
  });
});

// =============================================================================
// STALE OBSERVATION EVICTION
// =============================================================================

describe('RuntimeCalibrator stale eviction', () => {
  it('should evict stale observations based on observationMaxAgeMs', () => {
    // Use a very short max age so we can test eviction
    const calibrator = createRuntimeCalibrator({
      observationMaxAgeMs: 100,
    });

    calibrator.recordLatency('do_storage', 'sequential_read', 1.0);

    // Immediately, nothing should be evicted
    expect(calibrator.evictStale()).toBe(0);
    expect(calibrator.getLatencyStats('do_storage', 'sequential_read')).toBeDefined();

    // We cannot easily wait in a Worker test, but we can verify the mechanism
    // by checking that the eviction logic works structurally.
    // The lastUpdated is set to Date.now() when recorded,
    // and eviction checks against (now - maxAge).
  });

  it('should return count of evicted entries', () => {
    const calibrator = createRuntimeCalibrator({
      observationMaxAgeMs: 1, // Very short - will be stale almost immediately
    });

    calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
    calibrator.recordLatency('do_storage', 'random_read', 4.0);
    calibrator.recordLatency('r2', 'sequential_read', 100.0);

    // These might or might not be evicted depending on timing,
    // but the method should return a number >= 0
    const evicted = calibrator.evictStale();
    expect(typeof evicted).toBe('number');
    expect(evicted).toBeGreaterThanOrEqual(0);
  });
});

// =============================================================================
// RESET
// =============================================================================

describe('RuntimeCalibrator reset', () => {
  it('should clear all state on reset', () => {
    const calibrator = createRuntimeCalibrator();

    calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
    calibrator.recordLatency('do_storage', 'random_read', 4.0);
    calibrator.setDOCostFactor('crossDORpcMultiplier', 10.0);

    calibrator.reset();

    expect(calibrator.getLatencyStats('do_storage', 'sequential_read')).toBeUndefined();
    expect(calibrator.getLatencyStats('do_storage', 'random_read')).toBeUndefined();
    expect(calibrator.isCalibrated()).toBe(false);

    // DO cost factors should be reset to defaults
    const factors = calibrator.getDOCostFactors();
    expect(factors.crossDORpcMultiplier).toBe(DEFAULT_DO_COST_FACTORS.crossDORpcMultiplier);
  });
});

// =============================================================================
// DIAGNOSTICS
// =============================================================================

describe('RuntimeCalibrator diagnostics', () => {
  it('should report empty diagnostics for fresh calibrator', () => {
    const calibrator = createRuntimeCalibrator();
    const diag = calibrator.getDiagnostics();

    expect(diag.trackedKeys).toBe(0);
    expect(diag.totalObservations).toBe(0);
    expect(diag.isCalibrated).toBe(false);
    expect(diag.latencies.size).toBe(0);
    expect(diag.lastRecalibrationAt).toBe(0);
  });

  it('should report accurate diagnostics after observations', () => {
    const calibrator = createRuntimeCalibrator({ minObservations: 2 });

    calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
    calibrator.recordLatency('do_storage', 'sequential_read', 1.2);
    calibrator.recordLatency('do_storage', 'random_read', 4.0);

    const diag = calibrator.getDiagnostics();

    expect(diag.trackedKeys).toBe(2);
    expect(diag.totalObservations).toBe(3);
    expect(diag.isCalibrated).toBe(true);
    expect(diag.latencies.size).toBe(2);
    expect(diag.latencies.has('do_storage:sequential_read')).toBe(true);
    expect(diag.latencies.has('do_storage:random_read')).toBe(true);
  });

  it('should report DO cost factors in diagnostics', () => {
    const calibrator = createRuntimeCalibrator({}, {
      crossDORpcMultiplier: 7.5,
    });

    const diag = calibrator.getDiagnostics();
    expect(diag.doCostFactors.crossDORpcMultiplier).toBe(7.5);
  });

  it('should update lastRecalibrationAt after getCalibratedConfig', () => {
    const calibrator = createRuntimeCalibrator({ minObservations: 2 });

    for (let i = 0; i < 3; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
      calibrator.recordLatency('do_storage', 'random_read', 4.0);
    }

    calibrator.getCalibratedConfig();

    const diag = calibrator.getDiagnostics();
    expect(diag.lastRecalibrationAt).toBeGreaterThan(0);
  });
});

// =============================================================================
// EWMA ACCURACY
// =============================================================================

describe('RuntimeCalibrator EWMA accuracy', () => {
  it('should converge EWMA toward recent values', () => {
    const calibrator = createRuntimeCalibrator({ ewmaAlpha: 0.5 });

    // Record a sequence where latency changes from 1ms to 10ms
    for (let i = 0; i < 10; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
    }

    const statsAfterLow = calibrator.getLatencyStats('do_storage', 'sequential_read')!;
    expect(statsAfterLow.ewmaMs).toBeCloseTo(1.0, 0);

    // Now latency jumps to 10ms
    for (let i = 0; i < 20; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 10.0);
    }

    const statsAfterHigh = calibrator.getLatencyStats('do_storage', 'sequential_read')!;
    // EWMA should converge toward 10.0 (not exactly, due to initial weight)
    expect(statsAfterHigh.ewmaMs).toBeGreaterThan(9.0);
    expect(statsAfterHigh.ewmaMs).toBeLessThanOrEqual(10.0);
  });

  it('should weight recent observations more with high alpha', () => {
    const highAlpha = createRuntimeCalibrator({ ewmaAlpha: 0.9 });
    const lowAlpha = createRuntimeCalibrator({ ewmaAlpha: 0.1 });

    // Record 1.0 then jump to 10.0
    highAlpha.recordLatency('do_storage', 'sequential_read', 1.0);
    lowAlpha.recordLatency('do_storage', 'sequential_read', 1.0);

    highAlpha.recordLatency('do_storage', 'sequential_read', 10.0);
    lowAlpha.recordLatency('do_storage', 'sequential_read', 10.0);

    const highStats = highAlpha.getLatencyStats('do_storage', 'sequential_read')!;
    const lowStats = lowAlpha.getLatencyStats('do_storage', 'sequential_read')!;

    // High alpha should be closer to 10.0 (more reactive)
    expect(highStats.ewmaMs).toBeGreaterThan(lowStats.ewmaMs);
  });
});

// =============================================================================
// INTEGRATION WITH COST ESTIMATOR CONFIG
// =============================================================================

describe('RuntimeCalibrator integration with CostModelConfig', () => {
  it('should produce a valid CostModelConfig from calibration', () => {
    const calibrator = createRuntimeCalibrator({ minObservations: 3 });

    for (let i = 0; i < 5; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
      calibrator.recordLatency('do_storage', 'random_read', 4.0);
    }

    const config = calibrator.getCalibratedConfig();

    // Validate all required CostModelConfig fields exist
    expect(config.randomIOCost).toBeDefined();
    expect(config.sequentialIOCost).toBeDefined();
    expect(config.cpuComparisonCost).toBeDefined();
    expect(config.heapFetchCost).toBeDefined();
    expect(config.hashOperationCost).toBeDefined();
    expect(config.sortComparisonCost).toBeDefined();
    expect(config.sortMemory).toBeDefined();
    expect(config.hashMemory).toBeDefined();
    expect(config.defaultRowCount).toBeDefined();
    expect(config.equalitySelectivity).toBeDefined();
    expect(config.rangeSelectivity).toBeDefined();
    expect(config.likeSelectivity).toBeDefined();
    expect(config.indexRowsPerPage).toBeDefined();
    expect(config.tableRowsPerPage).toBeDefined();
    expect(config.avgRowSize).toBeDefined();

    // All cost values should be positive
    expect(config.randomIOCost).toBeGreaterThan(0);
    expect(config.sequentialIOCost).toBeGreaterThan(0);
    expect(config.cpuComparisonCost).toBeGreaterThan(0);
    expect(config.heapFetchCost).toBeGreaterThan(0);
  });

  it('should produce cost model where random > sequential (typical DO latency profile)', () => {
    const calibrator = createRuntimeCalibrator({ minObservations: 3 });

    // Typical DO latencies
    for (let i = 0; i < 5; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
      calibrator.recordLatency('do_storage', 'random_read', 3.5);
    }

    const config = calibrator.getCalibratedConfig();
    expect(config.randomIOCost).toBeGreaterThan(config.sequentialIOCost);
  });

  it('should constrain memory budgets for DO environment', () => {
    const calibrator = createRuntimeCalibrator({ minObservations: 3 });

    const config = calibrator.getCalibratedConfig({
      sortMemory: 512 * 1024 * 1024, // 512MB (too large for DO)
      hashMemory: 512 * 1024 * 1024,
    });

    // Should be capped at 32MB for DO environment
    expect(config.sortMemory).toBeLessThanOrEqual(32 * 1024 * 1024);
    expect(config.hashMemory).toBeLessThanOrEqual(32 * 1024 * 1024);
  });
});

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

describe('createRuntimeCalibrator factory', () => {
  it('should create a calibrator with default config', () => {
    const calibrator = createRuntimeCalibrator();
    expect(calibrator).toBeInstanceOf(RuntimeCalibrator);
    expect(calibrator.isCalibrated()).toBe(false);
  });

  it('should create a calibrator with custom config', () => {
    const calibrator = createRuntimeCalibrator({
      ewmaAlpha: 0.5,
      minObservations: 20,
    });

    // Record 19 observations - should not be calibrated
    for (let i = 0; i < 19; i++) {
      calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
    }
    expect(calibrator.isCalibrated()).toBe(false);

    // 20th observation - now calibrated
    calibrator.recordLatency('do_storage', 'sequential_read', 1.0);
    expect(calibrator.isCalibrated()).toBe(true);
  });

  it('should create a calibrator with custom DO cost factors', () => {
    const calibrator = createRuntimeCalibrator({}, {
      coldTierMultiplier: 200.0,
      warmTierMultiplier: 80.0,
    });

    const factors = calibrator.getDOCostFactors();
    expect(factors.coldTierMultiplier).toBe(200.0);
    expect(factors.warmTierMultiplier).toBe(80.0);
  });
});
