/**
 * Structured Logger Tests
 *
 * Tests for the structured logging system, focusing on:
 * - Error events include structured fields (timestamp, level, context)
 * - Logger can be configured (level, output)
 * - Errors include transaction ID, operation name
 * - Log output is JSON-serializable
 *
 * Issue: sql-rvph - Replace console.error with structured logging
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  createLogger,
  LogLevel,
  compareLogLevels,
  type LogEntry,
  type LogSink,
  type StructuredLogger,
  JsonSink,
  NoOpSink,
  MultiSink,
  FilteringSink,
  ConsoleSink,
  StandardContext,
  withTraceContext,
} from '../index.js';

// =============================================================================
// 1. Error Events Include Structured Fields
// =============================================================================

describe('Structured Logger - Error Events Include Structured Fields', () => {
  let logs: LogEntry[];
  let sink: LogSink;
  let logger: StructuredLogger;

  beforeEach(() => {
    logs = [];
    sink = {
      write(entry) {
        logs.push(entry);
      },
    };
    logger = createLogger({ sink, level: 'debug' });
  });

  it('should include timestamp in all log entries', () => {
    logger.error('test error');

    expect(logs).toHaveLength(1);
    expect(logs[0]).toHaveProperty('timestamp');
    expect(typeof logs[0].timestamp).toBe('string');
    // Verify ISO 8601 format
    expect(logs[0].timestamp).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
  });

  it('should include level in all log entries', () => {
    logger.debug('debug');
    logger.info('info');
    logger.warn('warn');
    logger.error('error');

    expect(logs).toHaveLength(4);
    expect(logs[0].level).toBe('debug');
    expect(logs[1].level).toBe('info');
    expect(logs[2].level).toBe('warn');
    expect(logs[3].level).toBe('error');
  });

  it('should include context with operation and txId', () => {
    const txId = 'txn-12345';
    const operation = 'ROLLBACK';

    logger.error('Failed to apply rollback operations', new Error('DB Error'), {
      txnId: txId,
      operation: operation,
    });

    expect(logs).toHaveLength(1);
    expect(logs[0].context).toBeDefined();
    expect(logs[0].context?.txnId).toBe(txId);
    expect(logs[0].context?.operation).toBe(operation);
  });

  it('should include error details with name, code, message, and stack', () => {
    const error = new Error('Something failed');
    error.name = 'TransactionError';
    (error as any).code = 'ROLLBACK_FAILED';

    logger.error('Operation failed', error, { operation: 'rollback' });

    expect(logs).toHaveLength(1);
    expect(logs[0].error).toBeDefined();
    expect(logs[0].error?.name).toBe('TransactionError');
    expect(logs[0].error?.code).toBe('ROLLBACK_FAILED');
    expect(logs[0].error?.message).toBe('Something failed');
    expect(logs[0].error?.stack).toBeDefined();
  });

  it('should include trace ID in all log entries', () => {
    logger.setTraceId('trace-abc-123');

    logger.debug('message 1');
    logger.info('message 2');
    logger.warn('message 3');
    logger.error('message 4');

    expect(logs).toHaveLength(4);
    logs.forEach(entry => {
      expect(entry.traceId).toBe('trace-abc-123');
    });
  });

  it('should propagate trace ID through child loggers', () => {
    logger.setTraceId('parent-trace');

    const childLogger = logger.child({ component: 'transaction' });
    childLogger.error('Child error');

    expect(logs).toHaveLength(1);
    expect(logs[0].traceId).toBe('parent-trace');
    expect(logs[0].context?.component).toBe('transaction');
  });
});

// =============================================================================
// 2. Logger Can Be Configured (Level, Output)
// =============================================================================

describe('Structured Logger - Configuration', () => {
  it('should filter logs below configured minimum level', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    // Set level to 'warn' - should filter debug and info
    const logger = createLogger({ sink, level: 'warn' });

    logger.debug('debug message');
    logger.info('info message');
    logger.warn('warn message');
    logger.error('error message');

    expect(logs).toHaveLength(2);
    expect(logs[0].level).toBe('warn');
    expect(logs[1].level).toBe('error');
  });

  it('should allow runtime level changes', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const logger = createLogger({ sink, level: 'error' });

    logger.info('should be filtered');
    expect(logs).toHaveLength(0);

    logger.setLevel('debug');

    logger.debug('now visible');
    expect(logs).toHaveLength(1);
    expect(logs[0].level).toBe('debug');
  });

  it('should support custom sink configuration', () => {
    const customOutput: string[] = [];
    const customSink: LogSink = {
      write(entry) {
        customOutput.push(`CUSTOM: ${entry.message}`);
      },
    };

    const logger = createLogger({ sink: customSink });
    logger.info('test message');

    expect(customOutput).toHaveLength(1);
    expect(customOutput[0]).toBe('CUSTOM: test message');
  });

  it('should support JsonSink for structured JSON output', () => {
    const jsonOutput: string[] = [];
    const jsonSink = new JsonSink({
      write: (json) => jsonOutput.push(json),
    });

    const logger = createLogger({ sink: jsonSink });
    logger.setTraceId('json-trace');
    logger.info('test message', { userId: 42 });

    expect(jsonOutput).toHaveLength(1);

    const parsed = JSON.parse(jsonOutput[0]);
    expect(parsed.traceId).toBe('json-trace');
    expect(parsed.message).toBe('test message');
    expect(parsed.context?.userId).toBe(42);
  });

  it('should support default context', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const logger = createLogger({
      sink,
      defaultContext: { service: 'dosql', version: '1.0' },
    });

    logger.info('message', { extra: 'data' });

    expect(logs).toHaveLength(1);
    expect(logs[0].context?.service).toBe('dosql');
    expect(logs[0].context?.version).toBe('1.0');
    expect(logs[0].context?.extra).toBe('data');
  });
});

// =============================================================================
// 3. Errors Include Transaction ID and Operation Name
// =============================================================================

describe('Structured Logger - Transaction Context', () => {
  it('should support transaction-specific logging context', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const logger = createLogger({ sink });

    // Use StandardContext for transaction logging
    const txContext = new StandardContext()
      .with('txnId', 'txn-abc-123')
      .with('operation', 'COMMIT')
      .with('state', 'ACTIVE')
      .build();

    logger.error('Transaction operation failed', new Error('DB Error'), txContext);

    expect(logs).toHaveLength(1);
    expect(logs[0].context?.txnId).toBe('txn-abc-123');
    expect(logs[0].context?.operation).toBe('COMMIT');
    expect(logs[0].context?.state).toBe('ACTIVE');
  });

  it('should support child logger for transaction-scoped logging', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const baseLogger = createLogger({ sink });

    // Create a transaction-scoped logger
    const txLogger = baseLogger.child({
      txnId: 'txn-xyz-789',
      isolationLevel: 'SERIALIZABLE',
    });

    txLogger.info('Transaction started');
    txLogger.error('Rollback failed', new Error('WAL write error'), {
      operation: 'ROLLBACK',
    });

    expect(logs).toHaveLength(2);

    // Both logs should have transaction context
    expect(logs[0].context?.txnId).toBe('txn-xyz-789');
    expect(logs[0].context?.isolationLevel).toBe('SERIALIZABLE');

    expect(logs[1].context?.txnId).toBe('txn-xyz-789');
    expect(logs[1].context?.isolationLevel).toBe('SERIALIZABLE');
    expect(logs[1].context?.operation).toBe('ROLLBACK');
  });

  it('should support async trace context for transaction correlation', async () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const logger = createLogger({ sink });

    await withTraceContext('txn-trace-456', async () => {
      logger.info('Before async operation');

      await new Promise(resolve => setTimeout(resolve, 10));

      logger.info('After async operation');
    });

    expect(logs).toHaveLength(2);
    expect(logs[0].traceId).toBe('txn-trace-456');
    expect(logs[1].traceId).toBe('txn-trace-456');
  });
});

// =============================================================================
// 4. Log Output is JSON-Serializable
// =============================================================================

describe('Structured Logger - JSON Serialization', () => {
  it('should produce valid JSON output', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const logger = createLogger({ sink });
    logger.info('test message', { nested: { value: 123 } });

    expect(logs).toHaveLength(1);

    // Should be JSON-serializable without error
    const json = JSON.stringify(logs[0]);
    const parsed = JSON.parse(json);

    expect(parsed.message).toBe('test message');
    expect(parsed.context?.nested?.value).toBe(123);
  });

  it('should handle circular references gracefully', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const logger = createLogger({ sink });

    // Create circular reference
    const obj: Record<string, unknown> = { name: 'test' };
    obj.self = obj;

    // Should not throw
    expect(() => logger.info('circular test', { data: obj })).not.toThrow();

    expect(logs).toHaveLength(1);

    // Should be JSON-serializable
    const json = JSON.stringify(logs[0]);
    const parsed = JSON.parse(json);
    expect(parsed.context?.data?.self).toBe('[Circular]');
  });

  it('should escape special characters properly', () => {
    const jsonOutput: string[] = [];
    const jsonSink = new JsonSink({
      write: (json) => jsonOutput.push(json),
    });

    const logger = createLogger({ sink: jsonSink });
    logger.info('message with "quotes" and \n newlines');

    expect(jsonOutput).toHaveLength(1);

    // Should be valid JSON
    const parsed = JSON.parse(jsonOutput[0]);
    expect(parsed.message).toBe('message with "quotes" and \n newlines');
  });

  it('should handle error objects with non-serializable properties', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const logger = createLogger({ sink });

    const error = new Error('Test error');
    // Add non-serializable property
    (error as any).fn = () => {};

    // Should not throw
    expect(() => logger.error('error test', error)).not.toThrow();

    expect(logs).toHaveLength(1);
    expect(logs[0].error?.message).toBe('Test error');

    // Should be JSON-serializable
    const json = JSON.stringify(logs[0]);
    expect(json).toBeDefined();
  });
});

// =============================================================================
// 5. Performance and Lazy Evaluation
// =============================================================================

describe('Structured Logger - Performance', () => {
  it('should support lazy message evaluation', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    let expensiveCallCount = 0;
    const expensiveOperation = () => {
      expensiveCallCount++;
      return 'expensive result';
    };

    // Set level to error - debug should be filtered
    const logger = createLogger({ sink, level: 'error' });

    // Using lazy evaluation - should not call expensiveOperation
    logger.debug(() => `expensive: ${expensiveOperation()}`);

    expect(expensiveCallCount).toBe(0);
    expect(logs).toHaveLength(0);

    // When logging is enabled, it should be called
    logger.error(() => `expensive: ${expensiveOperation()}`);
    expect(expensiveCallCount).toBe(1);
    expect(logs).toHaveLength(1);
  });

  it('should have minimal overhead when logging is disabled', () => {
    const logger = createLogger({ sink: new NoOpSink(), level: 'error' });

    const iterations = 10000;
    const start = performance.now();

    for (let i = 0; i < iterations; i++) {
      logger.debug('debug message', { i });
    }

    const elapsed = performance.now() - start;
    const avgPerCall = elapsed / iterations;

    // Should be very fast when filtered (< 0.01ms per call)
    expect(avgPerCall).toBeLessThan(0.01);
  });
});

// =============================================================================
// 6. Log Level Comparison
// =============================================================================

describe('Structured Logger - Log Level Utilities', () => {
  it('should compare log levels correctly', () => {
    expect(compareLogLevels('debug', 'info')).toBeLessThan(0);
    expect(compareLogLevels('error', 'warn')).toBeGreaterThan(0);
    expect(compareLogLevels('info', 'info')).toBe(0);
    expect(compareLogLevels('debug', 'error')).toBeLessThan(0);
  });

  it('should have numeric log level constants', () => {
    expect(LogLevel.DEBUG).toBe(0);
    expect(LogLevel.INFO).toBe(1);
    expect(LogLevel.WARN).toBe(2);
    expect(LogLevel.ERROR).toBe(3);
  });

  it('should get current log level', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const logger = createLogger({ sink, level: 'warn' });
    expect(logger.getLevel()).toBe('warn');

    logger.setLevel('debug');
    expect(logger.getLevel()).toBe('debug');
  });
});

// =============================================================================
// 7. Sink Types
// =============================================================================

describe('Structured Logger - Sink Types', () => {
  it('should support MultiSink for fan-out', () => {
    const sink1Logs: LogEntry[] = [];
    const sink2Logs: LogEntry[] = [];

    const sink1: LogSink = { write(entry) { sink1Logs.push(entry); } };
    const sink2: LogSink = { write(entry) { sink2Logs.push(entry); } };

    const multiSink = new MultiSink([sink1, sink2]);
    const logger = createLogger({ sink: multiSink });

    logger.info('test message');

    expect(sink1Logs).toHaveLength(1);
    expect(sink2Logs).toHaveLength(1);
  });

  it('should support FilteringSink', () => {
    const logs: LogEntry[] = [];
    const baseSink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    // Only pass error logs
    const filteringSink = new FilteringSink(baseSink, {
      filter: (entry) => entry.level === 'error',
    });

    const logger = createLogger({ sink: filteringSink, level: 'debug' });

    logger.info('info message');
    logger.warn('warn message');
    logger.error('error message');

    expect(logs).toHaveLength(1);
    expect(logs[0].level).toBe('error');
  });

  it('should support fallback sink on primary sink failure', () => {
    const fallbackLogs: LogEntry[] = [];

    const failingSink: LogSink = {
      write() {
        throw new Error('Sink failure');
      },
    };

    const fallbackSink: LogSink = {
      write(entry) {
        fallbackLogs.push(entry);
      },
    };

    const logger = createLogger({
      sink: failingSink,
      fallbackSink: fallbackSink,
    });

    // Should not throw
    expect(() => logger.info('test')).not.toThrow();

    // Should have used fallback
    expect(fallbackLogs).toHaveLength(1);
  });
});

// =============================================================================
// 8. Integration with Transaction Manager Use Cases
// =============================================================================

describe('Structured Logger - Transaction Manager Integration', () => {
  it('should log rollback failures with proper context', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const logger = createLogger({ sink });

    // Simulate what transaction manager would log
    const txnId = 'txn-12345';
    const error = new Error('WAL write failed');
    (error as any).code = 'WAL_FAILURE';

    logger.error('Failed to apply rollback operations', error, {
      txnId,
      operation: 'ROLLBACK',
      component: 'TransactionManager',
    });

    expect(logs).toHaveLength(1);
    expect(logs[0].level).toBe('error');
    expect(logs[0].message).toBe('Failed to apply rollback operations');
    expect(logs[0].error?.code).toBe('WAL_FAILURE');
    expect(logs[0].context?.txnId).toBe(txnId);
    expect(logs[0].context?.operation).toBe('ROLLBACK');
    expect(logs[0].context?.component).toBe('TransactionManager');

    // Verify JSON-serializable
    const json = JSON.stringify(logs[0]);
    expect(json).toBeDefined();
    expect(JSON.parse(json).context.txnId).toBe(txnId);
  });

  it('should log WAL write failures with proper context', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const logger = createLogger({ sink });

    const txnId = 'txn-67890';
    const error = new Error('Storage unavailable');

    logger.error('Failed to write ROLLBACK to WAL', error, {
      txnId,
      operation: 'WAL_WRITE',
      walOp: 'ROLLBACK',
    });

    expect(logs).toHaveLength(1);
    expect(logs[0].message).toBe('Failed to write ROLLBACK to WAL');
    expect(logs[0].context?.walOp).toBe('ROLLBACK');
  });

  it('should log savepoint rollback failures with proper context', () => {
    const logs: LogEntry[] = [];
    const sink: LogSink = {
      write(entry) {
        logs.push(entry);
      },
    };

    const logger = createLogger({ sink });

    const error = new Error('Savepoint not found');

    logger.error('Rollback to savepoint failed', error, {
      txnId: 'txn-11111',
      savepointName: 'sp1',
      operation: 'ROLLBACK_TO',
    });

    expect(logs).toHaveLength(1);
    expect(logs[0].context?.savepointName).toBe('sp1');
    expect(logs[0].context?.operation).toBe('ROLLBACK_TO');
  });
});
