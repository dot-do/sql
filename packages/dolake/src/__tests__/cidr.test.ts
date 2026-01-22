/**
 * CIDR Matching Tests
 *
 * RED Phase TDD: Test proper CIDR notation parsing and bitwise matching
 * for IP whitelisting in the rate limiter.
 *
 * The current implementation only handles hardcoded private ranges.
 * These tests verify that CIDR matching works for ANY CIDR notation.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { RateLimiter } from '../rate-limiter.js';

describe('CIDR Matching', () => {
  /**
   * Helper to test if an IP is whitelisted by checking if it bypasses rate limiting.
   * Creates a rate limiter with 0 connections per second, so only whitelisted IPs succeed.
   */
  function isIpWhitelisted(ip: string, whitelistedIps: string[]): boolean {
    const limiter = new RateLimiter({
      connectionsPerSecond: 1,
      whitelistedIps,
    });

    // First connection consumes the rate limit
    limiter.checkConnection('warmup', '1.1.1.1');

    // If this IP is whitelisted, it should still be allowed
    const result = limiter.checkConnection('test', ip);
    return result.allowed;
  }

  describe('/8 subnet matching', () => {
    it('should match IPs in 10.0.0.0/8 range', () => {
      // First IP in range
      expect(isIpWhitelisted('10.0.0.0', ['10.0.0.0/8'])).toBe(true);

      // Middle of range
      expect(isIpWhitelisted('10.128.50.25', ['10.0.0.0/8'])).toBe(true);

      // Last IP in range
      expect(isIpWhitelisted('10.255.255.255', ['10.0.0.0/8'])).toBe(true);
    });

    it('should NOT match IPs outside 10.0.0.0/8 range', () => {
      // Just before the range
      expect(isIpWhitelisted('9.255.255.255', ['10.0.0.0/8'])).toBe(false);

      // Just after the range
      expect(isIpWhitelisted('11.0.0.0', ['10.0.0.0/8'])).toBe(false);
    });
  });

  describe('/12 subnet matching (172.16.0.0/12)', () => {
    it('should match IPs at the start of 172.16.0.0/12 range', () => {
      expect(isIpWhitelisted('172.16.0.0', ['172.16.0.0/12'])).toBe(true);
    });

    it('should match IPs in the middle of 172.16.0.0/12 range', () => {
      // 172.24.x.x should be in range (172.16-31)
      expect(isIpWhitelisted('172.24.128.50', ['172.16.0.0/12'])).toBe(true);
    });

    it('should match IPs at the end of 172.16.0.0/12 range', () => {
      // 172.31.255.255 is the last IP in 172.16.0.0/12
      expect(isIpWhitelisted('172.31.255.255', ['172.16.0.0/12'])).toBe(true);
    });

    it('should NOT match IPs just before 172.16.0.0/12 range', () => {
      // 172.15.255.255 is outside the range
      expect(isIpWhitelisted('172.15.255.255', ['172.16.0.0/12'])).toBe(false);
    });

    it('should NOT match IPs just after 172.16.0.0/12 range', () => {
      // 172.32.0.0 is outside the range
      expect(isIpWhitelisted('172.32.0.0', ['172.16.0.0/12'])).toBe(false);
    });
  });

  describe('/16 subnet matching', () => {
    it('should match IPs in 192.168.0.0/16 range', () => {
      expect(isIpWhitelisted('192.168.0.0', ['192.168.0.0/16'])).toBe(true);
      expect(isIpWhitelisted('192.168.128.50', ['192.168.0.0/16'])).toBe(true);
      expect(isIpWhitelisted('192.168.255.255', ['192.168.0.0/16'])).toBe(true);
    });

    it('should NOT match IPs outside 192.168.0.0/16 range', () => {
      expect(isIpWhitelisted('192.167.255.255', ['192.168.0.0/16'])).toBe(false);
      expect(isIpWhitelisted('192.169.0.0', ['192.168.0.0/16'])).toBe(false);
    });
  });

  describe('/24 subnet matching (non-private ranges)', () => {
    // This test exposes the bug - the current implementation only handles
    // hardcoded private ranges, not arbitrary CIDRs like 203.0.113.0/24
    it('should match all IPs in 203.0.113.0/24 test range', () => {
      // First IP
      expect(isIpWhitelisted('203.0.113.0', ['203.0.113.0/24'])).toBe(true);

      // Middle IP
      expect(isIpWhitelisted('203.0.113.128', ['203.0.113.0/24'])).toBe(true);

      // Last IP
      expect(isIpWhitelisted('203.0.113.255', ['203.0.113.0/24'])).toBe(true);
    });

    it('should NOT match IPs outside 203.0.113.0/24 range', () => {
      // Adjacent subnets
      expect(isIpWhitelisted('203.0.112.255', ['203.0.113.0/24'])).toBe(false);
      expect(isIpWhitelisted('203.0.114.0', ['203.0.113.0/24'])).toBe(false);
    });

    it('should match IPs in 198.51.100.0/24 range', () => {
      expect(isIpWhitelisted('198.51.100.0', ['198.51.100.0/24'])).toBe(true);
      expect(isIpWhitelisted('198.51.100.128', ['198.51.100.0/24'])).toBe(true);
      expect(isIpWhitelisted('198.51.100.255', ['198.51.100.0/24'])).toBe(true);
    });
  });

  describe('/32 single IP matching', () => {
    it('should match exact IP for /32 CIDR', () => {
      expect(isIpWhitelisted('192.0.2.128', ['192.0.2.128/32'])).toBe(true);
    });

    it('should NOT match adjacent IPs for /32 CIDR', () => {
      // One IP before
      expect(isIpWhitelisted('192.0.2.127', ['192.0.2.128/32'])).toBe(false);

      // One IP after
      expect(isIpWhitelisted('192.0.2.129', ['192.0.2.128/32'])).toBe(false);
    });
  });

  describe('/10 subnet matching (100.64.0.0/10 CGNAT)', () => {
    // This test also exposes the bug - CGNAT is not a hardcoded private range
    it('should match IPs at the start of CGNAT range', () => {
      expect(isIpWhitelisted('100.64.0.0', ['100.64.0.0/10'])).toBe(true);
    });

    it('should match IPs in the middle of CGNAT range', () => {
      // 100.64.0.0/10 covers 100.64.0.0 - 100.127.255.255
      expect(isIpWhitelisted('100.100.50.25', ['100.64.0.0/10'])).toBe(true);
    });

    it('should match IPs at the end of CGNAT range', () => {
      expect(isIpWhitelisted('100.127.255.255', ['100.64.0.0/10'])).toBe(true);
    });

    it('should NOT match IPs outside CGNAT range', () => {
      // Just before
      expect(isIpWhitelisted('100.63.255.255', ['100.64.0.0/10'])).toBe(false);

      // Just after
      expect(isIpWhitelisted('100.128.0.0', ['100.64.0.0/10'])).toBe(false);
    });
  });

  describe('Edge cases', () => {
    it('should handle 0.0.0.0 IP address in /8 range', () => {
      expect(isIpWhitelisted('0.0.0.0', ['0.0.0.0/8'])).toBe(true);
      expect(isIpWhitelisted('0.255.255.255', ['0.0.0.0/8'])).toBe(true);
    });

    it('should handle 255.255.255.255 IP address', () => {
      expect(isIpWhitelisted('255.255.255.255', ['255.255.255.0/24'])).toBe(true);
      expect(isIpWhitelisted('255.255.255.0', ['255.255.255.0/24'])).toBe(true);
    });

    it('should handle /0 CIDR (all IPs)', () => {
      expect(isIpWhitelisted('1.2.3.4', ['0.0.0.0/0'])).toBe(true);
      expect(isIpWhitelisted('255.255.255.255', ['0.0.0.0/0'])).toBe(true);
      expect(isIpWhitelisted('100.100.100.100', ['0.0.0.0/0'])).toBe(true);
    });

    it('should handle boundary IPs correctly for /24', () => {
      // 192.168.1.0/24 covers 192.168.1.0 - 192.168.1.255
      expect(isIpWhitelisted('192.168.0.255', ['192.168.1.0/24'])).toBe(false);
      expect(isIpWhitelisted('192.168.1.0', ['192.168.1.0/24'])).toBe(true);
      expect(isIpWhitelisted('192.168.1.255', ['192.168.1.0/24'])).toBe(true);
      expect(isIpWhitelisted('192.168.2.0', ['192.168.1.0/24'])).toBe(false);
    });

    it('should handle exact IP match without CIDR notation', () => {
      expect(isIpWhitelisted('127.0.0.1', ['127.0.0.1'])).toBe(true);
      expect(isIpWhitelisted('127.0.0.2', ['127.0.0.1'])).toBe(false);
    });
  });

  describe('Whitelisted IPs bypass rate limiting', () => {
    it('should allow unlimited connections from whitelisted IP', () => {
      // Create limiter with very restrictive limits
      const limiter = new RateLimiter({
        connectionsPerSecond: 1,
        maxConnectionsPerIp: 1,
        whitelistedIps: ['10.0.0.0/8'],
      });

      // First connection from whitelisted IP
      const result1 = limiter.checkConnection('client1', '10.1.2.3');
      expect(result1.allowed).toBe(true);

      // Second connection should also be allowed (whitelisted)
      const result2 = limiter.checkConnection('client2', '10.1.2.4');
      expect(result2.allowed).toBe(true);

      // Third connection still allowed
      const result3 = limiter.checkConnection('client3', '10.1.2.5');
      expect(result3.allowed).toBe(true);
    });

    it('should rate limit non-whitelisted IPs', () => {
      const limiter = new RateLimiter({
        connectionsPerSecond: 1,
        whitelistedIps: ['10.0.0.0/8'],
      });

      // First connection from non-whitelisted IP
      const result1 = limiter.checkConnection('client1', '11.1.2.3');
      expect(result1.allowed).toBe(true);

      // Second connection should be rate limited
      const result2 = limiter.checkConnection('client2', '11.1.2.4');
      expect(result2.allowed).toBe(false);
      expect(result2.reason).toBe('rate_limited');
    });

    it('should whitelist non-private CIDR ranges (203.0.113.0/24)', () => {
      const limiter = new RateLimiter({
        connectionsPerSecond: 1,
        whitelistedIps: ['203.0.113.0/24'],
      });

      // First connection from whitelisted IP
      const result1 = limiter.checkConnection('client1', '203.0.113.50');
      expect(result1.allowed).toBe(true);

      // Second connection should also be allowed (whitelisted)
      const result2 = limiter.checkConnection('client2', '203.0.113.100');
      expect(result2.allowed).toBe(true);

      // Third connection still allowed
      const result3 = limiter.checkConnection('client3', '203.0.113.150');
      expect(result3.allowed).toBe(true);
    });
  });

  describe('Private range detection still works', () => {
    it('should whitelist RFC1918 10.x.x.x addresses', () => {
      expect(isIpWhitelisted('10.50.100.200', ['10.0.0.0/8'])).toBe(true);
    });

    it('should whitelist RFC1918 172.16-31.x.x addresses', () => {
      expect(isIpWhitelisted('172.16.0.1', ['172.16.0.0/12'])).toBe(true);
      expect(isIpWhitelisted('172.31.255.254', ['172.16.0.0/12'])).toBe(true);
    });

    it('should whitelist RFC1918 192.168.x.x addresses', () => {
      expect(isIpWhitelisted('192.168.1.100', ['192.168.0.0/16'])).toBe(true);
    });

    it('should whitelist localhost 127.0.0.1', () => {
      expect(isIpWhitelisted('127.0.0.1', ['127.0.0.1'])).toBe(true);
    });
  });

  describe('Invalid CIDR handling', () => {
    it('should handle malformed CIDR gracefully (no crash)', () => {
      // Should not throw, just not match
      expect(() => isIpWhitelisted('192.168.1.100', [
        'invalid',
        '192.168.1.0/',     // Missing mask
        '/24',              // Missing network
      ])).not.toThrow();
    });

    it('should not match IPs for invalid CIDR masks', () => {
      // Invalid mask > 32 should not match
      expect(isIpWhitelisted('192.168.1.100', ['192.168.1.0/33'])).toBe(false);

      // Negative mask should not match
      expect(isIpWhitelisted('192.168.1.100', ['192.168.1.0/-1'])).toBe(false);
    });

    it('should not match IPs for incomplete IP in CIDR', () => {
      expect(isIpWhitelisted('192.168.1.100', ['192.168.1/24'])).toBe(false);
    });

    it('should not match IPs for invalid octet values', () => {
      expect(isIpWhitelisted('192.168.1.100', ['256.0.0.0/8'])).toBe(false);
    });

    it('should handle empty whitelist', () => {
      const limiter = new RateLimiter({
        connectionsPerSecond: 1,
        whitelistedIps: [],
      });

      // First connection allowed (not from whitelist, just rate limit)
      const result1 = limiter.checkConnection('client1', '10.0.0.1');
      expect(result1.allowed).toBe(true);

      // Second connection should be rate limited (no whitelist)
      const result2 = limiter.checkConnection('client2', '10.0.0.2');
      expect(result2.allowed).toBe(false);
    });
  });

  describe('Multiple CIDR ranges', () => {
    it('should match IP if it falls within any whitelisted range', () => {
      const whitelist = [
        '10.0.0.0/8',
        '192.168.0.0/16',
        '203.0.113.0/24',
      ];

      expect(isIpWhitelisted('10.1.2.3', whitelist)).toBe(true);
      expect(isIpWhitelisted('192.168.1.1', whitelist)).toBe(true);
      expect(isIpWhitelisted('203.0.113.50', whitelist)).toBe(true);
    });

    it('should NOT match IP if it does not fall within any range', () => {
      const whitelist = [
        '10.0.0.0/8',
        '192.168.0.0/16',
        '203.0.113.0/24',
      ];

      expect(isIpWhitelisted('11.1.2.3', whitelist)).toBe(false);
      expect(isIpWhitelisted('192.169.1.1', whitelist)).toBe(false);
      expect(isIpWhitelisted('203.0.114.50', whitelist)).toBe(false);
    });
  });
});
