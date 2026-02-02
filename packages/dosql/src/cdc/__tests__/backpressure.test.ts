/**
 * Backpressure Controller Tests
 *
 * Tests for CDC backpressure propagation from DoLake to DoSQL:
 * - Signal handling (pause/slow_down/resume)
 * - Adaptive batch sizing
 * - Delay calculation and throttling
 * - State transitions and metrics
 * - Integration with DoLake ACK/NACK responses
 *
 * Tests run using workers-vitest-pool (NO MOCKS).
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  BackpressureController,
  createBackpressureController,
  ackToBackpressureSignal,
  nackToBackpressureSignal,
  type BackpressureConfig,
  type BackpressureState,
  type LakehouseAckWithBackpressure,
  DEFAULT_BACKPRESSURE_CONFIG,
} from '../backpressure.js';
import type { BackpressureSignal } from '../types.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a backpressure signal
 */
function createSignal(
  type: BackpressureSignal['type'],
  bufferUtilization: number,
  options: Partial<BackpressureSignal> = {}
): BackpressureSignal {
  return {
    type,
    bufferUtilization,
    ...options,
  };
}

/**
 * Create a mock DoLake ACK response
 */
function createAck(overrides: Partial<LakehouseAckWithBackpressure> = {}): LakehouseAckWithBackpressure {
  return {
    status: 'ok',
    bufferUtilization: 0.5,
    ...overrides,
  };
}

// =============================================================================
// Test: Controller Initialization
// =============================================================================

describe('BackpressureController - Initialization', () => {
  it('should initialize with default config', () => {
    const controller = createBackpressureController();

    expect(controller.getState()).toBe('normal');
    const { delayMs, batchSize } = controller.getBatchParameters();
    expect(delayMs).toBe(DEFAULT_BACKPRESSURE_CONFIG.minDelayMs);
    expect(batchSize).toBe(DEFAULT_BACKPRESSURE_CONFIG.maxBatchSize);
  });

  it('should accept custom config', () => {
    const controller = createBackpressureController({
      maxBatchSize: 500,
      minDelayMs: 100,
    });

    const { batchSize } = controller.getBatchParameters();
    expect(batchSize).toBe(500);
  });

  it('should report initial metrics', () => {
    const controller = createBackpressureController();
    const metrics = controller.getMetrics();

    expect(metrics.state).toBe('normal');
    expect(metrics.totalPauseSignals).toBe(0);
    expect(metrics.totalSlowDownSignals).toBe(0);
    expect(metrics.totalResumeSignals).toBe(0);
    expect(metrics.lastSignalAt).toBeNull();
  });
});

// =============================================================================
// Test: Signal Handling - Pause
// =============================================================================

describe('BackpressureController - Pause Signal', () => {
  let controller: BackpressureController;

  beforeEach(() => {
    controller = createBackpressureController({
      maxDelayMs: 5000,
      minBatchSize: 10,
      maxBatchSize: 1000,
    });
  });

  it('should handle pause signal', () => {
    controller.handleSignal(createSignal('pause', 0.95, {
      reason: 'Buffer full',
    }));

    expect(controller.getState()).toBe('paused');
    expect(controller.shouldProceed()).toBe(false);
  });

  it('should set maximum delay on pause', () => {
    controller.handleSignal(createSignal('pause', 0.95));

    const { delayMs } = controller.getBatchParameters();
    expect(delayMs).toBe(5000); // maxDelayMs
  });

  it('should reduce batch size to minimum on pause', () => {
    controller.handleSignal(createSignal('pause', 0.95));

    const { batchSize } = controller.getBatchParameters();
    expect(batchSize).toBe(10); // minBatchSize
  });

  it('should track pause signals in metrics', () => {
    controller.handleSignal(createSignal('pause', 0.95));
    controller.handleSignal(createSignal('pause', 0.98));

    const metrics = controller.getMetrics();
    expect(metrics.totalPauseSignals).toBe(2);
  });
});

// =============================================================================
// Test: Signal Handling - Slow Down
// =============================================================================

describe('BackpressureController - Slow Down Signal', () => {
  let controller: BackpressureController;

  beforeEach(() => {
    controller = createBackpressureController({
      maxDelayMs: 5000,
      minBatchSize: 10,
      maxBatchSize: 1000,
      warningThreshold: 0.7,
      criticalThreshold: 0.9,
    });
  });

  it('should handle slow_down signal', () => {
    controller.handleSignal(createSignal('slow_down', 0.75, {
      suggestedDelayMs: 500,
    }));

    expect(controller.getState()).toBe('warning');
    expect(controller.shouldProceed()).toBe(true);
  });

  it('should use suggested delay', () => {
    controller.handleSignal(createSignal('slow_down', 0.75, {
      suggestedDelayMs: 500,
    }));

    const { delayMs } = controller.getBatchParameters();
    expect(delayMs).toBe(500);
  });

  it('should cap delay at maxDelayMs', () => {
    controller.handleSignal(createSignal('slow_down', 0.75, {
      suggestedDelayMs: 10000,
    }));

    const { delayMs } = controller.getBatchParameters();
    expect(delayMs).toBe(5000); // maxDelayMs
  });

  it('should reduce batch size based on utilization', () => {
    controller.handleSignal(createSignal('slow_down', 0.8));

    const { batchSize } = controller.getBatchParameters();
    // With 0.8 utilization, batch size should be ~20% of max (200)
    expect(batchSize).toBeLessThan(1000);
    expect(batchSize).toBeGreaterThan(10);
  });

  it('should transition to critical state at high utilization', () => {
    controller.handleSignal(createSignal('slow_down', 0.92));

    expect(controller.getState()).toBe('critical');
  });

  it('should track slow_down signals in metrics', () => {
    controller.handleSignal(createSignal('slow_down', 0.75));
    controller.handleSignal(createSignal('slow_down', 0.85));

    const metrics = controller.getMetrics();
    expect(metrics.totalSlowDownSignals).toBe(2);
  });
});

// =============================================================================
// Test: Signal Handling - Resume
// =============================================================================

describe('BackpressureController - Resume Signal', () => {
  let controller: BackpressureController;

  beforeEach(() => {
    controller = createBackpressureController({
      maxDelayMs: 5000,
      minDelayMs: 0,
      minBatchSize: 10,
      maxBatchSize: 1000,
      resumeDecayRate: 0.5,
    });

    // Put controller in slow_down state first
    controller.handleSignal(createSignal('slow_down', 0.8, {
      suggestedDelayMs: 1000,
    }));
  });

  it('should handle resume signal', () => {
    controller.handleSignal(createSignal('resume', 0.3, {
      reason: 'Buffer drained',
    }));

    expect(controller.getState()).toBe('normal');
    expect(controller.shouldProceed()).toBe(true);
  });

  it('should decay delay on resume', () => {
    const beforeDelay = controller.getBatchParameters().delayMs;

    controller.handleSignal(createSignal('resume', 0.3));

    const { delayMs } = controller.getBatchParameters();
    // Delay should be halved (resumeDecayRate = 0.5)
    expect(delayMs).toBe(Math.floor(beforeDelay * 0.5));
  });

  it('should increase batch size on resume', () => {
    const beforeBatch = controller.getBatchParameters().batchSize;

    controller.handleSignal(createSignal('resume', 0.3));

    const { batchSize } = controller.getBatchParameters();
    expect(batchSize).toBeGreaterThan(beforeBatch);
  });

  it('should track resume signals in metrics', () => {
    controller.handleSignal(createSignal('resume', 0.3));
    controller.handleSignal(createSignal('resume', 0.2));

    const metrics = controller.getMetrics();
    expect(metrics.totalResumeSignals).toBe(2);
  });

  it('should transition from paused to normal on resume', () => {
    // First pause
    controller.handleSignal(createSignal('pause', 0.95));
    expect(controller.getState()).toBe('paused');

    // Then resume
    controller.handleSignal(createSignal('resume', 0.3));
    expect(controller.getState()).toBe('normal');
    expect(controller.shouldProceed()).toBe(true);
  });
});

// =============================================================================
// Test: State Transitions
// =============================================================================

describe('BackpressureController - State Transitions', () => {
  let controller: BackpressureController;

  beforeEach(() => {
    controller = createBackpressureController({
      warningThreshold: 0.7,
      criticalThreshold: 0.9,
    });
  });

  it('should transition normal -> warning', () => {
    const stateChanges: BackpressureState[] = [];
    controller.onStateChange((newState) => stateChanges.push(newState));

    controller.handleSignal(createSignal('slow_down', 0.75));

    expect(stateChanges).toContain('warning');
    expect(controller.getState()).toBe('warning');
  });

  it('should transition normal -> critical', () => {
    const stateChanges: BackpressureState[] = [];
    controller.onStateChange((newState) => stateChanges.push(newState));

    controller.handleSignal(createSignal('slow_down', 0.92));

    expect(stateChanges).toContain('critical');
    expect(controller.getState()).toBe('critical');
  });

  it('should transition normal -> paused', () => {
    const stateChanges: BackpressureState[] = [];
    controller.onStateChange((newState) => stateChanges.push(newState));

    controller.handleSignal(createSignal('pause', 0.95));

    expect(stateChanges).toContain('paused');
    expect(controller.getState()).toBe('paused');
  });

  it('should transition warning -> critical', () => {
    controller.handleSignal(createSignal('slow_down', 0.75));
    expect(controller.getState()).toBe('warning');

    controller.handleSignal(createSignal('slow_down', 0.92));
    expect(controller.getState()).toBe('critical');
  });

  it('should transition critical -> warning', () => {
    controller.handleSignal(createSignal('slow_down', 0.92));
    expect(controller.getState()).toBe('critical');

    controller.handleSignal(createSignal('slow_down', 0.75));
    expect(controller.getState()).toBe('warning');
  });

  it('should transition warning -> normal on resume', () => {
    controller.handleSignal(createSignal('slow_down', 0.75));
    expect(controller.getState()).toBe('warning');

    controller.handleSignal(createSignal('resume', 0.3));
    expect(controller.getState()).toBe('normal');
  });
});

// =============================================================================
// Test: Callbacks
// =============================================================================

describe('BackpressureController - Callbacks', () => {
  let controller: BackpressureController;

  beforeEach(() => {
    controller = createBackpressureController();
  });

  it('should call state change callback', () => {
    const callback = vi.fn();
    controller.onStateChange(callback);

    controller.handleSignal(createSignal('slow_down', 0.75));

    expect(callback).toHaveBeenCalledWith(
      'warning',
      'normal',
      expect.objectContaining({ state: 'warning' })
    );
  });

  it('should call batch parameter callback', () => {
    const callback = vi.fn();
    controller.onBatchParameterChange(callback);

    controller.handleSignal(createSignal('slow_down', 0.75, {
      suggestedDelayMs: 500,
    }));

    expect(callback).toHaveBeenCalledWith(500, expect.any(Number));
  });

  it('should allow unsubscribing from callbacks', () => {
    const callback = vi.fn();
    const unsubscribe = controller.onStateChange(callback);

    unsubscribe();

    controller.handleSignal(createSignal('slow_down', 0.75));

    expect(callback).not.toHaveBeenCalled();
  });

  it('should handle callback errors gracefully', () => {
    const errorCallback = vi.fn(() => {
      throw new Error('Callback error');
    });
    const goodCallback = vi.fn();

    controller.onStateChange(errorCallback);
    controller.onStateChange(goodCallback);

    // Should not throw
    expect(() => {
      controller.handleSignal(createSignal('slow_down', 0.75));
    }).not.toThrow();

    // Good callback should still be called
    expect(goodCallback).toHaveBeenCalled();
  });
});

// =============================================================================
// Test: Reset
// =============================================================================

describe('BackpressureController - Reset', () => {
  it('should reset to normal state', () => {
    const controller = createBackpressureController();

    // Put in slow_down state
    controller.handleSignal(createSignal('slow_down', 0.85, {
      suggestedDelayMs: 1000,
    }));

    // Reset
    controller.reset();

    expect(controller.getState()).toBe('normal');
    expect(controller.shouldProceed()).toBe(true);
  });

  it('should reset batch parameters', () => {
    const controller = createBackpressureController({
      minDelayMs: 0,
      maxBatchSize: 1000,
    });

    // Put in slow_down state
    controller.handleSignal(createSignal('slow_down', 0.85, {
      suggestedDelayMs: 1000,
    }));

    // Reset
    controller.reset();

    const { delayMs, batchSize } = controller.getBatchParameters();
    expect(delayMs).toBe(0);
    expect(batchSize).toBe(1000);
  });

  it('should notify state change on reset', () => {
    const controller = createBackpressureController();
    const callback = vi.fn();
    controller.onStateChange(callback);

    // Put in warning state
    controller.handleSignal(createSignal('slow_down', 0.75));
    callback.mockClear();

    // Reset
    controller.reset();

    expect(callback).toHaveBeenCalledWith('normal', 'warning', expect.any(Object));
  });
});

// =============================================================================
// Test: Wait For Delay
// =============================================================================

describe('BackpressureController - Wait For Delay', () => {
  it('should wait for the configured delay', async () => {
    const controller = createBackpressureController();

    controller.handleSignal(createSignal('slow_down', 0.75, {
      suggestedDelayMs: 50,
    }));

    const start = Date.now();
    await controller.waitForDelay();
    const elapsed = Date.now() - start;

    expect(elapsed).toBeGreaterThanOrEqual(40); // Allow some variance
  });

  it('should not wait if delay is 0', async () => {
    const controller = createBackpressureController({
      minDelayMs: 0,
    });

    const start = Date.now();
    await controller.waitForDelay();
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(20);
  });
});

// =============================================================================
// Test: Metrics
// =============================================================================

describe('BackpressureController - Metrics', () => {
  it('should track buffer utilization', () => {
    const controller = createBackpressureController();

    controller.handleSignal(createSignal('slow_down', 0.82));

    const metrics = controller.getMetrics();
    expect(metrics.lastBufferUtilization).toBe(0.82);
  });

  it('should track last signal timestamp', () => {
    const controller = createBackpressureController();
    const before = Date.now();

    controller.handleSignal(createSignal('slow_down', 0.75));

    const metrics = controller.getMetrics();
    expect(metrics.lastSignalAt).toBeGreaterThanOrEqual(before);
  });

  it('should track time in states', async () => {
    const controller = createBackpressureController({
      warningThreshold: 0.7,
    });

    // Enter warning state
    controller.handleSignal(createSignal('slow_down', 0.75));

    // Wait a bit
    await new Promise(resolve => setTimeout(resolve, 50));

    const metrics = controller.getMetrics();
    expect(metrics.totalWarningTimeMs).toBeGreaterThanOrEqual(40);
  });
});

// =============================================================================
// Test: ACK to Signal Conversion
// =============================================================================

describe('ackToBackpressureSignal', () => {
  it('should return pause signal for circuit breaker open', () => {
    const signal = ackToBackpressureSignal(createAck({
      circuitBreakerState: 'open',
      bufferUtilization: 0.5,
    }));

    expect(signal).not.toBeNull();
    expect(signal!.type).toBe('pause');
    expect(signal!.reason).toContain('Circuit breaker');
  });

  it('should return pause signal for critical utilization', () => {
    const signal = ackToBackpressureSignal(createAck({
      bufferUtilization: 0.95,
    }));

    expect(signal).not.toBeNull();
    expect(signal!.type).toBe('pause');
    expect(signal!.bufferUtilization).toBe(0.95);
  });

  it('should return slow_down signal for warning utilization', () => {
    const signal = ackToBackpressureSignal(createAck({
      bufferUtilization: 0.75,
    }));

    expect(signal).not.toBeNull();
    expect(signal!.type).toBe('slow_down');
    expect(signal!.bufferUtilization).toBe(0.75);
  });

  it('should return slow_down signal for depleted rate limit tokens', () => {
    const signal = ackToBackpressureSignal(createAck({
      bufferUtilization: 0.5,
      remainingTokens: 5,
      bucketCapacity: 100,
    }));

    expect(signal).not.toBeNull();
    expect(signal!.type).toBe('slow_down');
    expect(signal!.reason).toContain('tokens');
  });

  it('should return resume signal for healthy buffer', () => {
    const signal = ackToBackpressureSignal(createAck({
      bufferUtilization: 0.3,
    }));

    expect(signal).not.toBeNull();
    expect(signal!.type).toBe('resume');
  });

  it('should return null for moderate utilization without pressure', () => {
    const signal = ackToBackpressureSignal(createAck({
      bufferUtilization: 0.55, // Between 0.5 and 0.7 - no signal needed
      remainingTokens: 80,
      bucketCapacity: 100,
    }));

    expect(signal).toBeNull();
  });
});

// =============================================================================
// Test: NACK to Signal Conversion
// =============================================================================

describe('nackToBackpressureSignal', () => {
  it('should return pause signal for buffer_full', () => {
    const signal = nackToBackpressureSignal('buffer_full', 5000);

    expect(signal.type).toBe('pause');
    expect(signal.bufferUtilization).toBe(1.0);
    expect(signal.suggestedDelayMs).toBe(5000);
  });

  it('should return slow_down signal for rate_limited', () => {
    const signal = nackToBackpressureSignal('rate_limited', 1000);

    expect(signal.type).toBe('slow_down');
    expect(signal.suggestedDelayMs).toBe(1000);
  });

  it('should return slow_down signal for load_shedding', () => {
    const signal = nackToBackpressureSignal('load_shedding', 2000);

    expect(signal.type).toBe('slow_down');
    expect(signal.bufferUtilization).toBe(0.9);
    expect(signal.suggestedDelayMs).toBe(2000);
  });

  it('should handle unknown NACK reasons', () => {
    const signal = nackToBackpressureSignal('unknown_reason', 500);

    expect(signal.type).toBe('slow_down');
    expect(signal.suggestedDelayMs).toBe(500);
    expect(signal.reason).toContain('unknown_reason');
  });

  it('should use default delay when not provided', () => {
    const signal = nackToBackpressureSignal('buffer_full');

    expect(signal.suggestedDelayMs).toBe(5000); // Default for buffer_full
  });
});

// =============================================================================
// Test: Integration Scenario
// =============================================================================

describe('BackpressureController - Integration Scenario', () => {
  it('should handle a typical backpressure cycle', async () => {
    const controller = createBackpressureController({
      maxDelayMs: 1000,
      minDelayMs: 0,
      minBatchSize: 10,
      maxBatchSize: 100,
      warningThreshold: 0.7,
      criticalThreshold: 0.9,
    });

    const stateHistory: BackpressureState[] = [];
    controller.onStateChange((newState) => stateHistory.push(newState));

    // 1. Normal operation
    expect(controller.getState()).toBe('normal');
    expect(controller.getBatchParameters().batchSize).toBe(100);

    // 2. Buffer starts filling - slow_down
    controller.handleSignal(createSignal('slow_down', 0.75, {
      suggestedDelayMs: 200,
    }));
    expect(controller.getState()).toBe('warning');
    expect(controller.getBatchParameters().delayMs).toBe(200);
    expect(controller.getBatchParameters().batchSize).toBeLessThan(100);

    // 3. Buffer critical - more slow_down
    controller.handleSignal(createSignal('slow_down', 0.92, {
      suggestedDelayMs: 500,
    }));
    expect(controller.getState()).toBe('critical');
    expect(controller.getBatchParameters().delayMs).toBe(500);

    // 4. Buffer full - pause
    controller.handleSignal(createSignal('pause', 0.98));
    expect(controller.getState()).toBe('paused');
    expect(controller.shouldProceed()).toBe(false);
    expect(controller.getBatchParameters().delayMs).toBe(1000);
    expect(controller.getBatchParameters().batchSize).toBe(10);

    // 5. Buffer drains - resume
    controller.handleSignal(createSignal('resume', 0.4));
    expect(controller.getState()).toBe('normal');
    expect(controller.shouldProceed()).toBe(true);

    // Check state history
    expect(stateHistory).toEqual(['warning', 'critical', 'paused', 'normal']);

    // Check final metrics
    const metrics = controller.getMetrics();
    expect(metrics.totalPauseSignals).toBe(1);
    expect(metrics.totalSlowDownSignals).toBe(2);
    expect(metrics.totalResumeSignals).toBe(1);
  });
});

// =============================================================================
// Test: Adaptive Batch Sizing
// =============================================================================

describe('BackpressureController - Adaptive Batch Sizing', () => {
  it('should disable adaptive batch sizing when configured', () => {
    const controller = createBackpressureController({
      adaptiveBatchSize: false,
      maxBatchSize: 1000,
    });

    controller.handleSignal(createSignal('slow_down', 0.9));

    const { batchSize } = controller.getBatchParameters();
    expect(batchSize).toBe(1000); // Should not change
  });

  it('should scale batch size with utilization', () => {
    const controller = createBackpressureController({
      adaptiveBatchSize: true,
      maxBatchSize: 1000,
      minBatchSize: 10,
    });

    // At 80% utilization, batch should be ~20% of max
    controller.handleSignal(createSignal('slow_down', 0.8));
    const { batchSize: batch80 } = controller.getBatchParameters();

    // Reset and test at 50% utilization
    controller.reset();
    controller.handleSignal(createSignal('slow_down', 0.5));
    const { batchSize: batch50 } = controller.getBatchParameters();

    // Higher utilization should result in smaller batch
    expect(batch80).toBeLessThan(batch50);
  });
});
