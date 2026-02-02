/**
 * Ranking Algorithm Tests
 *
 * Tests for FTS relevance ranking including:
 * - BM25 calculation
 * - TF-IDF calculation
 * - Score normalization
 * - DocumentScorer utility
 * - Column weight utilities
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  calculateBM25,
  calculateMultiTermBM25,
  calculateTFIDF,
  calculateMultiTermTFIDF,
  normalizeScores,
  logScale,
  DocumentScorer,
  calculateWeightedColumnScore,
  calculateFieldLengthNorm,
  DEFAULT_BM25_PARAMS,
} from '../ranking.js';
import type { RankingParams } from '../types.js';

// =============================================================================
// BM25 Calculation Tests
// =============================================================================

describe('calculateBM25', () => {
  const baseParams: RankingParams = {
    termFrequency: 2,
    documentLength: 100,
    averageDocumentLength: 100,
    numDocuments: 1000,
    documentFrequency: 50,
    k1: 1.2,
    b: 0.75,
  };

  describe('basic calculation', () => {
    it('should return positive score for valid parameters', () => {
      const score = calculateBM25(baseParams);
      expect(score).toBeGreaterThan(0);
    });

    it('should return 0 for zero term frequency', () => {
      const score = calculateBM25({
        ...baseParams,
        termFrequency: 0,
      });
      // With 0 term frequency, the numerator is 0
      expect(score).toBe(0);
    });

    it('should return 0 when documentFrequency is 0', () => {
      const score = calculateBM25({
        ...baseParams,
        documentFrequency: 0,
      });
      expect(score).toBe(0);
    });

    it('should return 0 when numDocuments is 0', () => {
      const score = calculateBM25({
        ...baseParams,
        numDocuments: 0,
      });
      expect(score).toBe(0);
    });

    it('should return 0 when averageDocumentLength is 0', () => {
      const score = calculateBM25({
        ...baseParams,
        averageDocumentLength: 0,
      });
      expect(score).toBe(0);
    });
  });

  describe('term frequency impact', () => {
    it('should increase score with higher term frequency', () => {
      const lowTf = calculateBM25({ ...baseParams, termFrequency: 1 });
      const highTf = calculateBM25({ ...baseParams, termFrequency: 5 });
      expect(highTf).toBeGreaterThan(lowTf);
    });

    it('should show diminishing returns for higher term frequency', () => {
      const tf1 = calculateBM25({ ...baseParams, termFrequency: 1 });
      const tf2 = calculateBM25({ ...baseParams, termFrequency: 2 });
      const tf10 = calculateBM25({ ...baseParams, termFrequency: 10 });
      const tf20 = calculateBM25({ ...baseParams, termFrequency: 20 });

      // Increase from 1->2 should be larger than 10->20 (relatively)
      const increase1to2 = tf2 - tf1;
      const increase10to20 = tf20 - tf10;
      expect(increase1to2).toBeGreaterThan(increase10to20 / 2);
    });
  });

  describe('IDF (Inverse Document Frequency) impact', () => {
    it('should score rare terms higher than common terms', () => {
      const rareTermScore = calculateBM25({
        ...baseParams,
        documentFrequency: 1,
      });
      const commonTermScore = calculateBM25({
        ...baseParams,
        documentFrequency: 500,
      });
      expect(rareTermScore).toBeGreaterThan(commonTermScore);
    });

    it('should handle term appearing in all documents', () => {
      const score = calculateBM25({
        ...baseParams,
        documentFrequency: 1000,
        numDocuments: 1000,
      });
      // IDF should be low but still positive due to the +1 in formula
      expect(score).toBeGreaterThanOrEqual(0);
    });

    it('should handle term appearing in half of documents', () => {
      const score = calculateBM25({
        ...baseParams,
        documentFrequency: 500,
        numDocuments: 1000,
      });
      expect(score).toBeGreaterThan(0);
    });
  });

  describe('document length normalization', () => {
    it('should score shorter documents higher (same TF)', () => {
      const shortDoc = calculateBM25({
        ...baseParams,
        documentLength: 50,
      });
      const longDoc = calculateBM25({
        ...baseParams,
        documentLength: 200,
      });
      expect(shortDoc).toBeGreaterThan(longDoc);
    });

    it('should score document at average length normally', () => {
      const score = calculateBM25({
        ...baseParams,
        documentLength: 100,
        averageDocumentLength: 100,
      });
      expect(score).toBeGreaterThan(0);
    });

    it('should handle very short documents', () => {
      const score = calculateBM25({
        ...baseParams,
        documentLength: 1,
        averageDocumentLength: 100,
      });
      expect(score).toBeGreaterThan(0);
    });

    it('should handle very long documents', () => {
      const score = calculateBM25({
        ...baseParams,
        documentLength: 10000,
        averageDocumentLength: 100,
      });
      expect(score).toBeGreaterThan(0);
    });
  });

  describe('k1 parameter effects', () => {
    it('should increase term frequency saturation with higher k1', () => {
      const lowK1 = calculateBM25({ ...baseParams, k1: 0.5 });
      const highK1 = calculateBM25({ ...baseParams, k1: 2.0 });
      // With higher k1, term frequency has more impact
      expect(lowK1).not.toBe(highK1);
    });

    it('should handle k1 = 0', () => {
      const score = calculateBM25({ ...baseParams, k1: 0 });
      expect(typeof score).toBe('number');
    });
  });

  describe('b parameter effects', () => {
    it('should increase length normalization with higher b', () => {
      const shortWithLowB = calculateBM25({
        ...baseParams,
        documentLength: 50,
        b: 0.25,
      });
      const shortWithHighB = calculateBM25({
        ...baseParams,
        documentLength: 50,
        b: 1.0,
      });
      // With higher b, short docs get more boost
      expect(shortWithHighB).toBeGreaterThan(shortWithLowB);
    });

    it('should handle b = 0 (no length normalization)', () => {
      const score = calculateBM25({ ...baseParams, b: 0 });
      expect(score).toBeGreaterThan(0);
    });

    it('should handle b = 1 (full length normalization)', () => {
      const score = calculateBM25({ ...baseParams, b: 1 });
      expect(score).toBeGreaterThan(0);
    });
  });
});

// =============================================================================
// Multi-Term BM25 Tests
// =============================================================================

describe('calculateMultiTermBM25', () => {
  const termParams: RankingParams[] = [
    {
      termFrequency: 2,
      documentLength: 100,
      averageDocumentLength: 100,
      numDocuments: 1000,
      documentFrequency: 50,
      k1: 1.2,
      b: 0.75,
    },
    {
      termFrequency: 1,
      documentLength: 100,
      averageDocumentLength: 100,
      numDocuments: 1000,
      documentFrequency: 100,
      k1: 1.2,
      b: 0.75,
    },
  ];

  it('should sum scores for multiple terms', () => {
    const multiScore = calculateMultiTermBM25(termParams);
    const singleScore1 = calculateBM25(termParams[0]);
    const singleScore2 = calculateBM25(termParams[1]);
    expect(multiScore).toBe(singleScore1 + singleScore2);
  });

  it('should apply weights to terms', () => {
    const unweighted = calculateMultiTermBM25(termParams);
    const weighted = calculateMultiTermBM25(termParams, [2.0, 0.5]);
    expect(weighted).not.toBe(unweighted);
  });

  it('should handle empty array', () => {
    const score = calculateMultiTermBM25([]);
    expect(score).toBe(0);
  });

  it('should handle single term', () => {
    const multiScore = calculateMultiTermBM25([termParams[0]]);
    const singleScore = calculateBM25(termParams[0]);
    expect(multiScore).toBe(singleScore);
  });
});

// =============================================================================
// TF-IDF Calculation Tests
// =============================================================================

describe('calculateTFIDF', () => {
  describe('basic calculation', () => {
    it('should return positive score for valid parameters', () => {
      const score = calculateTFIDF({
        termFrequency: 2,
        numDocuments: 1000,
        documentFrequency: 50,
      });
      expect(score).toBeGreaterThan(0);
    });

    it('should return 0 for zero term frequency with log TF', () => {
      const score = calculateTFIDF({
        termFrequency: 0,
        numDocuments: 1000,
        documentFrequency: 50,
        useLogTF: true,
      });
      expect(score).toBe(0);
    });

    it('should return 0 when documentFrequency is 0', () => {
      const score = calculateTFIDF({
        termFrequency: 2,
        numDocuments: 1000,
        documentFrequency: 0,
      });
      expect(score).toBe(0);
    });

    it('should return 0 when numDocuments is 0', () => {
      const score = calculateTFIDF({
        termFrequency: 2,
        numDocuments: 0,
        documentFrequency: 50,
      });
      expect(score).toBe(0);
    });
  });

  describe('log TF normalization', () => {
    it('should use log TF by default', () => {
      const withLog = calculateTFIDF({
        termFrequency: 10,
        numDocuments: 1000,
        documentFrequency: 50,
      });
      const withoutLog = calculateTFIDF({
        termFrequency: 10,
        numDocuments: 1000,
        documentFrequency: 50,
        useLogTF: false,
      });
      expect(withLog).not.toBe(withoutLog);
    });

    it('should have diminishing returns with log TF', () => {
      const tf1 = calculateTFIDF({
        termFrequency: 1,
        numDocuments: 1000,
        documentFrequency: 50,
        useLogTF: true,
      });
      const tf10 = calculateTFIDF({
        termFrequency: 10,
        numDocuments: 1000,
        documentFrequency: 50,
        useLogTF: true,
      });
      // Score should increase but not by 10x
      expect(tf10 / tf1).toBeLessThan(10);
    });

    it('should scale linearly without log TF', () => {
      const tf1 = calculateTFIDF({
        termFrequency: 1,
        numDocuments: 1000,
        documentFrequency: 50,
        useLogTF: false,
      });
      const tf10 = calculateTFIDF({
        termFrequency: 10,
        numDocuments: 1000,
        documentFrequency: 50,
        useLogTF: false,
      });
      expect(tf10 / tf1).toBe(10);
    });
  });

  describe('IDF calculation', () => {
    it('should score rare terms higher', () => {
      const rareScore = calculateTFIDF({
        termFrequency: 1,
        numDocuments: 1000,
        documentFrequency: 1,
      });
      const commonScore = calculateTFIDF({
        termFrequency: 1,
        numDocuments: 1000,
        documentFrequency: 500,
      });
      expect(rareScore).toBeGreaterThan(commonScore);
    });

    it('should return 0 IDF when term in all docs', () => {
      const score = calculateTFIDF({
        termFrequency: 1,
        numDocuments: 1000,
        documentFrequency: 1000,
      });
      expect(score).toBe(0);
    });
  });
});

// =============================================================================
// Multi-Term TF-IDF Tests
// =============================================================================

describe('calculateMultiTermTFIDF', () => {
  const termParams = [
    { termFrequency: 2, numDocuments: 1000, documentFrequency: 50 },
    { termFrequency: 1, numDocuments: 1000, documentFrequency: 100 },
  ];

  it('should sum scores for multiple terms', () => {
    const multiScore = calculateMultiTermTFIDF(termParams);
    const singleScore1 = calculateTFIDF(termParams[0]);
    const singleScore2 = calculateTFIDF(termParams[1]);
    expect(multiScore).toBeCloseTo(singleScore1 + singleScore2);
  });

  it('should apply weights to terms', () => {
    const unweighted = calculateMultiTermTFIDF(termParams);
    const weighted = calculateMultiTermTFIDF(termParams, [2.0, 0.5]);
    expect(weighted).not.toBe(unweighted);
  });

  it('should handle empty array', () => {
    const score = calculateMultiTermTFIDF([]);
    expect(score).toBe(0);
  });
});

// =============================================================================
// Score Normalization Tests
// =============================================================================

describe('normalizeScores', () => {
  it('should normalize scores to [0, 1] range', () => {
    const scores = [10, 20, 30, 40, 50];
    const normalized = normalizeScores(scores);
    expect(Math.max(...normalized)).toBe(1);
    expect(Math.min(...normalized)).toBe(10 / 50);
  });

  it('should handle all same scores', () => {
    const scores = [5, 5, 5, 5];
    const normalized = normalizeScores(scores);
    expect(normalized).toEqual([1, 1, 1, 1]);
  });

  it('should handle single score', () => {
    const normalized = normalizeScores([42]);
    expect(normalized).toEqual([1]);
  });

  it('should handle empty array', () => {
    const normalized = normalizeScores([]);
    expect(normalized).toEqual([]);
  });

  it('should handle all zeros', () => {
    const scores = [0, 0, 0];
    const normalized = normalizeScores(scores);
    expect(normalized).toEqual([0, 0, 0]);
  });

  it('should preserve relative ordering', () => {
    const scores = [10, 30, 20, 50, 40];
    const normalized = normalizeScores(scores);
    // Check relative ordering is preserved
    expect(normalized[3]).toBe(1); // 50 is max
    expect(normalized[0]).toBeLessThan(normalized[1]); // 10 < 30
  });
});

// =============================================================================
// Log Scale Tests
// =============================================================================

describe('logScale', () => {
  it('should apply log scaling to scores', () => {
    const scores = [0, 1, 10, 100];
    const scaled = logScale(scores);
    expect(scaled[0]).toBe(0); // log(1) = 0
    expect(scaled[1]).toBeCloseTo(Math.log(2)); // log(1 + 1)
    expect(scaled[2]).toBeCloseTo(Math.log(11)); // log(1 + 10)
  });

  it('should handle empty array', () => {
    const scaled = logScale([]);
    expect(scaled).toEqual([]);
  });

  it('should compress high values', () => {
    const scores = [1, 100, 10000];
    const scaled = logScale(scores);
    // Ratio between scaled values should be much smaller than original
    const originalRatio = scores[2] / scores[0];
    const scaledRatio = scaled[2] / scaled[0];
    expect(scaledRatio).toBeLessThan(originalRatio);
  });
});

// =============================================================================
// DocumentScorer Tests
// =============================================================================

describe('DocumentScorer', () => {
  let scorer: DocumentScorer;

  beforeEach(() => {
    scorer = new DocumentScorer({
      numDocuments: 1000,
      avgDocLength: 100,
    });
  });

  describe('addTermScore', () => {
    it('should accumulate scores for documents', () => {
      scorer.addTermScore(1, 2, 100, 50);
      expect(scorer.getScore(1)).toBeGreaterThan(0);
    });

    it('should accumulate multiple term scores', () => {
      scorer.addTermScore(1, 2, 100, 50);
      const score1 = scorer.getScore(1);
      scorer.addTermScore(1, 1, 100, 100);
      expect(scorer.getScore(1)).toBeGreaterThan(score1);
    });

    it('should apply weight to score', () => {
      scorer.addTermScore(1, 2, 100, 50, 1.0);
      const score1 = scorer.getScore(1);
      scorer.clear();
      scorer.addTermScore(1, 2, 100, 50, 2.0);
      const score2 = scorer.getScore(1);
      expect(score2).toBeCloseTo(score1 * 2);
    });

    it('should track multiple documents', () => {
      scorer.addTermScore(1, 2, 100, 50);
      scorer.addTermScore(2, 3, 150, 50);
      scorer.addTermScore(3, 1, 80, 50);

      expect(scorer.getScore(1)).toBeGreaterThan(0);
      expect(scorer.getScore(2)).toBeGreaterThan(0);
      expect(scorer.getScore(3)).toBeGreaterThan(0);
    });
  });

  describe('getRankedResults', () => {
    it('should return results sorted by score descending', () => {
      scorer.addTermScore(1, 1, 100, 50);
      scorer.addTermScore(2, 3, 100, 50);
      scorer.addTermScore(3, 2, 100, 50);

      const results = scorer.getRankedResults();
      expect(results[0].score).toBeGreaterThanOrEqual(results[1].score);
      expect(results[1].score).toBeGreaterThanOrEqual(results[2].score);
    });

    it('should return empty array for no scores', () => {
      const results = scorer.getRankedResults();
      expect(results).toEqual([]);
    });

    it('should return all scored documents', () => {
      scorer.addTermScore(1, 2, 100, 50);
      scorer.addTermScore(2, 3, 100, 50);

      const results = scorer.getRankedResults();
      expect(results.length).toBe(2);
    });
  });

  describe('getScore', () => {
    it('should return 0 for unscored document', () => {
      expect(scorer.getScore(999)).toBe(0);
    });

    it('should return correct score for scored document', () => {
      scorer.addTermScore(1, 2, 100, 50);
      expect(scorer.getScore(1)).toBeGreaterThan(0);
    });
  });

  describe('clear', () => {
    it('should clear all scores', () => {
      scorer.addTermScore(1, 2, 100, 50);
      scorer.addTermScore(2, 3, 100, 50);
      scorer.clear();

      expect(scorer.getScore(1)).toBe(0);
      expect(scorer.getScore(2)).toBe(0);
      expect(scorer.getRankedResults()).toEqual([]);
    });
  });

  describe('custom BM25 parameters', () => {
    it('should use custom k1', () => {
      const customScorer = new DocumentScorer({
        numDocuments: 1000,
        avgDocLength: 100,
        k1: 2.0,
      });
      customScorer.addTermScore(1, 2, 100, 50);
      const customScore = customScorer.getScore(1);

      scorer.addTermScore(1, 2, 100, 50);
      const defaultScore = scorer.getScore(1);

      expect(customScore).not.toBe(defaultScore);
    });

    it('should use custom b', () => {
      const customScorer = new DocumentScorer({
        numDocuments: 1000,
        avgDocLength: 100,
        b: 0.5,
      });
      customScorer.addTermScore(1, 2, 50, 50);
      const customScore = customScorer.getScore(1);

      scorer.addTermScore(1, 2, 50, 50);
      const defaultScore = scorer.getScore(1);

      expect(customScore).not.toBe(defaultScore);
    });
  });
});

// =============================================================================
// Column Weight Utilities Tests
// =============================================================================

describe('calculateWeightedColumnScore', () => {
  it('should apply weights to column scores', () => {
    const columnScores = new Map([
      ['title', 10],
      ['content', 5],
    ]);
    const weights = { title: 2.0, content: 1.0 };

    const score = calculateWeightedColumnScore(columnScores, weights);
    // (10 * 2 + 5 * 1) / (2 + 1) = 25 / 3
    expect(score).toBeCloseTo(25 / 3);
  });

  it('should use default weight of 1 for unspecified columns', () => {
    const columnScores = new Map([
      ['title', 10],
      ['content', 5],
    ]);
    const weights = { title: 2.0 };

    const score = calculateWeightedColumnScore(columnScores, weights);
    // (10 * 2 + 5 * 1) / (2 + 1) = 25 / 3
    expect(score).toBeCloseTo(25 / 3);
  });

  it('should return 0 for empty column scores', () => {
    const score = calculateWeightedColumnScore(new Map(), { title: 1.0 });
    expect(score).toBe(0);
  });

  it('should handle single column', () => {
    const columnScores = new Map([['content', 10]]);
    const weights = { content: 1.0 };

    const score = calculateWeightedColumnScore(columnScores, weights);
    expect(score).toBe(10);
  });
});

describe('calculateFieldLengthNorm', () => {
  it('should calculate length normalization for columns', () => {
    const columnLengths = new Map([
      ['title', 10],
      ['content', 200],
    ]);
    const avgColumnLengths = { title: 20, content: 100 };

    const norms = calculateFieldLengthNorm(columnLengths, avgColumnLengths);

    expect(norms.get('title')).toBeDefined();
    expect(norms.get('content')).toBeDefined();
  });

  it('should return 1 when length equals average', () => {
    const columnLengths = new Map([['content', 100]]);
    const avgColumnLengths = { content: 100 };

    const norms = calculateFieldLengthNorm(columnLengths, avgColumnLengths);
    expect(norms.get('content')).toBe(1);
  });

  it('should return lower value for shorter documents', () => {
    const columnLengths = new Map([['content', 50]]);
    const avgColumnLengths = { content: 100 };

    const norms = calculateFieldLengthNorm(columnLengths, avgColumnLengths);
    expect(norms.get('content')!).toBeLessThan(1);
  });

  it('should return higher value for longer documents', () => {
    const columnLengths = new Map([['content', 200]]);
    const avgColumnLengths = { content: 100 };

    const norms = calculateFieldLengthNorm(columnLengths, avgColumnLengths);
    expect(norms.get('content')!).toBeGreaterThan(1);
  });

  it('should respect b parameter', () => {
    const columnLengths = new Map([['content', 50]]);
    const avgColumnLengths = { content: 100 };

    const normsLowB = calculateFieldLengthNorm(columnLengths, avgColumnLengths, 0.25);
    const normsHighB = calculateFieldLengthNorm(columnLengths, avgColumnLengths, 0.75);

    // Lower b means less length normalization
    expect(normsLowB.get('content')!).toBeGreaterThan(normsHighB.get('content')!);
  });
});

// =============================================================================
// Default Parameters Tests
// =============================================================================

describe('DEFAULT_BM25_PARAMS', () => {
  it('should have k1 value of 1.2', () => {
    expect(DEFAULT_BM25_PARAMS.k1).toBe(1.2);
  });

  it('should have b value of 0.75', () => {
    expect(DEFAULT_BM25_PARAMS.b).toBe(0.75);
  });
});

// =============================================================================
// Edge Cases and Numerical Stability
// =============================================================================

describe('Numerical edge cases', () => {
  it('should handle very large term frequencies', () => {
    const score = calculateBM25({
      termFrequency: 1000000,
      documentLength: 100,
      averageDocumentLength: 100,
      numDocuments: 1000,
      documentFrequency: 50,
      k1: 1.2,
      b: 0.75,
    });
    expect(isFinite(score)).toBe(true);
    expect(score).toBeGreaterThan(0);
  });

  it('should handle very small term frequencies', () => {
    const score = calculateBM25({
      termFrequency: 0.001,
      documentLength: 100,
      averageDocumentLength: 100,
      numDocuments: 1000,
      documentFrequency: 50,
      k1: 1.2,
      b: 0.75,
    });
    expect(isFinite(score)).toBe(true);
  });

  it('should handle very large document counts', () => {
    const score = calculateBM25({
      termFrequency: 2,
      documentLength: 100,
      averageDocumentLength: 100,
      numDocuments: 1000000000,
      documentFrequency: 1,
      k1: 1.2,
      b: 0.75,
    });
    expect(isFinite(score)).toBe(true);
    expect(score).toBeGreaterThan(0);
  });

  it('should handle very long documents', () => {
    const score = calculateBM25({
      termFrequency: 2,
      documentLength: 1000000,
      averageDocumentLength: 100,
      numDocuments: 1000,
      documentFrequency: 50,
      k1: 1.2,
      b: 0.75,
    });
    expect(isFinite(score)).toBe(true);
    expect(score).toBeGreaterThan(0);
  });

  it('should produce consistent results', () => {
    const params: RankingParams = {
      termFrequency: 2,
      documentLength: 100,
      averageDocumentLength: 100,
      numDocuments: 1000,
      documentFrequency: 50,
      k1: 1.2,
      b: 0.75,
    };

    const score1 = calculateBM25(params);
    const score2 = calculateBM25(params);
    expect(score1).toBe(score2);
  });
});
