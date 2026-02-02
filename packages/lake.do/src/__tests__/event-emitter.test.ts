/**
 * Tests for lake.do ConnectionEventEmitter
 *
 * @module lake.do/tests/event-emitter
 */

import { describe, it, expect, vi } from 'vitest';
import { ConnectionEventEmitter } from '../connection/event-emitter.js';

describe('ConnectionEventEmitter', () => {
  describe('on', () => {
    it('registers and calls event listener', () => {
      const emitter = new ConnectionEventEmitter();
      const handler = vi.fn();

      emitter.on('connected', handler);
      emitter.emit('connected');

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it('registers multiple listeners for same event', () => {
      const emitter = new ConnectionEventEmitter();
      const handler1 = vi.fn();
      const handler2 = vi.fn();

      emitter.on('connected', handler1);
      emitter.on('connected', handler2);
      emitter.emit('connected');

      expect(handler1).toHaveBeenCalledTimes(1);
      expect(handler2).toHaveBeenCalledTimes(1);
    });

    it('passes event data to handler', () => {
      const emitter = new ConnectionEventEmitter();
      const handler = vi.fn();
      const errorData = new Error('test error');

      emitter.on('error', handler);
      emitter.emit('error', errorData);

      expect(handler).toHaveBeenCalledWith(errorData);
    });

    it('supports all event types', () => {
      const emitter = new ConnectionEventEmitter();
      const events = ['connected', 'disconnected', 'reconnecting', 'reconnected', 'error'] as const;

      for (const event of events) {
        const handler = vi.fn();
        emitter.on(event, handler);
        emitter.emit(event);
        expect(handler).toHaveBeenCalled();
      }
    });
  });

  describe('off', () => {
    it('removes event listener', () => {
      const emitter = new ConnectionEventEmitter();
      const handler = vi.fn();

      emitter.on('connected', handler);
      emitter.emit('connected');
      expect(handler).toHaveBeenCalledTimes(1);

      emitter.off('connected', handler);
      emitter.emit('connected');
      expect(handler).toHaveBeenCalledTimes(1); // Still 1, not called again
    });

    it('only removes specified listener', () => {
      const emitter = new ConnectionEventEmitter();
      const handler1 = vi.fn();
      const handler2 = vi.fn();

      emitter.on('connected', handler1);
      emitter.on('connected', handler2);

      emitter.off('connected', handler1);
      emitter.emit('connected');

      expect(handler1).not.toHaveBeenCalled();
      expect(handler2).toHaveBeenCalledTimes(1);
    });

    it('handles removing non-existent listener gracefully', () => {
      const emitter = new ConnectionEventEmitter();
      const handler = vi.fn();

      // No listeners registered yet
      expect(() => emitter.off('connected', handler)).not.toThrow();

      // Register different listener
      emitter.on('connected', vi.fn());
      expect(() => emitter.off('connected', handler)).not.toThrow();
    });
  });

  describe('once', () => {
    it('calls listener only once', () => {
      const emitter = new ConnectionEventEmitter();
      const handler = vi.fn();

      emitter.once('connected', handler);

      emitter.emit('connected');
      emitter.emit('connected');
      emitter.emit('connected');

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it('passes data to once listener', () => {
      const emitter = new ConnectionEventEmitter();
      const handler = vi.fn();
      const data = { test: 'value' };

      emitter.once('error', handler);
      emitter.emit('error', data);

      expect(handler).toHaveBeenCalledWith(data);
    });

    it('works alongside regular listeners', () => {
      const emitter = new ConnectionEventEmitter();
      const onceHandler = vi.fn();
      const regularHandler = vi.fn();

      emitter.once('connected', onceHandler);
      emitter.on('connected', regularHandler);

      emitter.emit('connected');
      emitter.emit('connected');

      expect(onceHandler).toHaveBeenCalledTimes(1);
      expect(regularHandler).toHaveBeenCalledTimes(2);
    });
  });

  describe('emit', () => {
    it('does nothing when no listeners', () => {
      const emitter = new ConnectionEventEmitter();

      // Should not throw
      expect(() => emitter.emit('connected')).not.toThrow();
      expect(() => emitter.emit('disconnected', { reason: 'test' })).not.toThrow();
    });

    it('handles errors in handlers gracefully', () => {
      const emitter = new ConnectionEventEmitter();
      const errorHandler = vi.fn(() => {
        throw new Error('Handler error');
      });
      const normalHandler = vi.fn();

      // Spy on console.error
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      emitter.on('connected', errorHandler);
      emitter.on('connected', normalHandler);

      // Should not throw
      expect(() => emitter.emit('connected')).not.toThrow();

      // Error should be logged
      expect(consoleSpy).toHaveBeenCalled();

      // Other handler should still be called
      expect(normalHandler).toHaveBeenCalled();

      consoleSpy.mockRestore();
    });

    it('passes undefined when no data provided', () => {
      const emitter = new ConnectionEventEmitter();
      const handler = vi.fn();

      emitter.on('connected', handler);
      emitter.emit('connected');

      expect(handler).toHaveBeenCalledWith(undefined);
    });
  });

  describe('clear', () => {
    it('clears all listeners for specific event', () => {
      const emitter = new ConnectionEventEmitter();
      const connectedHandler = vi.fn();
      const disconnectedHandler = vi.fn();

      emitter.on('connected', connectedHandler);
      emitter.on('disconnected', disconnectedHandler);

      emitter.clear('connected');

      emitter.emit('connected');
      emitter.emit('disconnected');

      expect(connectedHandler).not.toHaveBeenCalled();
      expect(disconnectedHandler).toHaveBeenCalled();
    });

    it('clears all listeners when no event specified', () => {
      const emitter = new ConnectionEventEmitter();
      const connectedHandler = vi.fn();
      const disconnectedHandler = vi.fn();
      const errorHandler = vi.fn();

      emitter.on('connected', connectedHandler);
      emitter.on('disconnected', disconnectedHandler);
      emitter.on('error', errorHandler);

      emitter.clear();

      emitter.emit('connected');
      emitter.emit('disconnected');
      emitter.emit('error');

      expect(connectedHandler).not.toHaveBeenCalled();
      expect(disconnectedHandler).not.toHaveBeenCalled();
      expect(errorHandler).not.toHaveBeenCalled();
    });
  });

  describe('listenerCount', () => {
    it('returns 0 for event with no listeners', () => {
      const emitter = new ConnectionEventEmitter();

      expect(emitter.listenerCount('connected')).toBe(0);
    });

    it('returns correct count for registered listeners', () => {
      const emitter = new ConnectionEventEmitter();

      emitter.on('connected', vi.fn());
      emitter.on('connected', vi.fn());
      emitter.on('connected', vi.fn());

      expect(emitter.listenerCount('connected')).toBe(3);
    });

    it('updates count after removing listeners', () => {
      const emitter = new ConnectionEventEmitter();
      const handler = vi.fn();

      emitter.on('connected', handler);
      emitter.on('connected', vi.fn());

      expect(emitter.listenerCount('connected')).toBe(2);

      emitter.off('connected', handler);

      expect(emitter.listenerCount('connected')).toBe(1);
    });

    it('returns 0 after clearing listeners', () => {
      const emitter = new ConnectionEventEmitter();

      emitter.on('connected', vi.fn());
      emitter.on('connected', vi.fn());

      emitter.clear('connected');

      expect(emitter.listenerCount('connected')).toBe(0);
    });

    it('counts once listeners correctly', () => {
      const emitter = new ConnectionEventEmitter();

      emitter.on('connected', vi.fn());
      emitter.once('connected', vi.fn());

      expect(emitter.listenerCount('connected')).toBe(2);

      emitter.emit('connected');

      expect(emitter.listenerCount('connected')).toBe(1); // once listener removed
    });
  });
});
