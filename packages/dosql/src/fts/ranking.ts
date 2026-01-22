/**
 * Ranking Algorithms for Full-Text Search
 *
 * Implements BM25 and TF-IDF ranking algorithms.
 */

import type { RankingParams } from './types.js';

// Re-export for convenience
export type { RankingParams };

// =============================================================================
// BM25 Ranking
// =============================================================================

/**
 * Default BM25 parameters
 */
export const DEFAULT_BM25_PARAMS = {
  k1: 1.2, // Term saturation parameter
  b: 0.75, // Length normalization parameter
};

/**
 * Calculate BM25 score for a term in a document
 *
 * BM25 formula:
 * score = IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl / avgdl)))
 *
 * Where:
 * - tf = term frequency in document
 * - dl = document length
 * - avgdl = average document length
 * - IDF = log((N - df + 0.5) / (df + 0.5))
 * - N = total number of documents
 * - df = document frequency (number of documents containing term)
 * - k1, b = tuning parameters
 */
export function calculateBM25(params: RankingParams): number {
  const {
    termFrequency,
    documentLength,
    averageDocumentLength,
    numDocuments,
    documentFrequency,
    k1,
    b,
  } = params;

  // Prevent division by zero
  if (documentFrequency === 0 || numDocuments === 0 || averageDocumentLength === 0) {
    return 0;
  }

  // Calculate IDF (Inverse Document Frequency)
  // Using the standard BM25 IDF formula
  const idf = Math.log(
    (numDocuments - documentFrequency + 0.5) / (documentFrequency + 0.5) + 1
  );

  // Calculate length normalization
  const lengthNorm = 1 - b + b * (documentLength / averageDocumentLength);

  // Calculate term frequency saturation
  const tfSaturation =
    (termFrequency * (k1 + 1)) / (termFrequency + k1 * lengthNorm);

  return idf * tfSaturation;
}

/**
 * Calculate BM25 score for multiple terms
 */
export function calculateMultiTermBM25(
  termParams: RankingParams[],
  weights?: number[]
): number {
  let score = 0;

  for (let i = 0; i < termParams.length; i++) {
    const termScore = calculateBM25(termParams[i]);
    const weight = weights?.[i] ?? 1;
    score += termScore * weight;
  }

  return score;
}

// =============================================================================
// TF-IDF Ranking
// =============================================================================

/**
 * Calculate TF-IDF score for a term in a document
 *
 * TF-IDF formula:
 * score = tf * idf
 *
 * Where:
 * - tf = term frequency (or log(1 + tf))
 * - idf = log(N / df)
 */
export function calculateTFIDF(params: {
  termFrequency: number;
  numDocuments: number;
  documentFrequency: number;
  useLogTF?: boolean;
}): number {
  const { termFrequency, numDocuments, documentFrequency, useLogTF = true } = params;

  if (documentFrequency === 0 || numDocuments === 0) {
    return 0;
  }

  // Term frequency (optionally with log normalization)
  const tf = useLogTF ? Math.log(1 + termFrequency) : termFrequency;

  // Inverse document frequency
  const idf = Math.log(numDocuments / documentFrequency);

  return tf * idf;
}

/**
 * Calculate TF-IDF score for multiple terms
 */
export function calculateMultiTermTFIDF(
  termParams: Array<{
    termFrequency: number;
    numDocuments: number;
    documentFrequency: number;
  }>,
  weights?: number[]
): number {
  let score = 0;

  for (let i = 0; i < termParams.length; i++) {
    const termScore = calculateTFIDF(termParams[i]);
    const weight = weights?.[i] ?? 1;
    score += termScore * weight;
  }

  return score;
}

// =============================================================================
// Score Normalization
// =============================================================================

/**
 * Normalize scores to [0, 1] range
 */
export function normalizeScores(scores: number[]): number[] {
  if (scores.length === 0) return [];

  const max = Math.max(...scores);
  if (max === 0) return scores.map(() => 0);

  return scores.map((s) => s / max);
}

/**
 * Apply logarithmic scaling to scores
 */
export function logScale(scores: number[]): number[] {
  return scores.map((s) => Math.log(1 + s));
}

// =============================================================================
// Document Scorer
// =============================================================================

/**
 * Document scorer that accumulates scores for ranking
 */
export class DocumentScorer {
  private scores: Map<number, number> = new Map();
  private numDocuments: number;
  private avgDocLength: number;
  private k1: number;
  private b: number;

  constructor(params: {
    numDocuments: number;
    avgDocLength: number;
    k1?: number;
    b?: number;
  }) {
    this.numDocuments = params.numDocuments;
    this.avgDocLength = params.avgDocLength;
    this.k1 = params.k1 ?? DEFAULT_BM25_PARAMS.k1;
    this.b = params.b ?? DEFAULT_BM25_PARAMS.b;
  }

  /**
   * Add a term score for a document
   */
  addTermScore(
    docId: number,
    termFrequency: number,
    documentLength: number,
    documentFrequency: number,
    weight: number = 1
  ): void {
    const score = calculateBM25({
      termFrequency,
      documentLength,
      averageDocumentLength: this.avgDocLength,
      numDocuments: this.numDocuments,
      documentFrequency,
      k1: this.k1,
      b: this.b,
    });

    const current = this.scores.get(docId) ?? 0;
    this.scores.set(docId, current + score * weight);
  }

  /**
   * Get final scores sorted by rank
   */
  getRankedResults(): Array<{ docId: number; score: number }> {
    const results = Array.from(this.scores.entries()).map(([docId, score]) => ({
      docId,
      score,
    }));

    // Sort by score descending
    results.sort((a, b) => b.score - a.score);

    return results;
  }

  /**
   * Get score for a specific document
   */
  getScore(docId: number): number {
    return this.scores.get(docId) ?? 0;
  }

  /**
   * Clear all scores
   */
  clear(): void {
    this.scores.clear();
  }
}

// =============================================================================
// Column Weight Utilities
// =============================================================================

/**
 * Calculate weighted score across columns
 */
export function calculateWeightedColumnScore(
  columnScores: Map<string, number>,
  weights: Record<string, number>
): number {
  let totalScore = 0;
  let totalWeight = 0;

  for (const [column, score] of columnScores) {
    const weight = weights[column] ?? 1;
    totalScore += score * weight;
    totalWeight += weight;
  }

  return totalWeight > 0 ? totalScore / totalWeight : 0;
}

/**
 * Apply field length normalization across columns
 */
export function calculateFieldLengthNorm(
  columnLengths: Map<string, number>,
  avgColumnLengths: Record<string, number>,
  b: number = DEFAULT_BM25_PARAMS.b
): Map<string, number> {
  const norms = new Map<string, number>();

  for (const [column, length] of columnLengths) {
    const avgLength = avgColumnLengths[column] ?? length;
    const norm = 1 - b + b * (length / avgLength);
    norms.set(column, norm);
  }

  return norms;
}
