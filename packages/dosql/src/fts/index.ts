/**
 * Full-Text Search (FTS5-style) Implementation
 *
 * Main entry point for the FTS module.
 */

import type {
  FTSConfig,
  FTSDocument,
  FTSSearchResult,
  FTSSearchOptions,
  FTSIndex,
  IndexStats,
  TokenizerType,
  ColumnWeights,
  Posting,
  PostingList,
  DocumentMeta,
  MatchQuery,
} from './types.js';

import { createTokenizer, type Tokenizer, type Token } from './tokenizer.js';
import { parseMatchQuery, extractTerms } from './query.js';
import { calculateBM25, DocumentScorer, DEFAULT_BM25_PARAMS } from './ranking.js';
import { highlight, snippet } from './auxiliary.js';

// Re-export types and functions
export * from './types.js';
export * from './tokenizer.js';
export * from './query.js';
export * from './ranking.js';
export * from './auxiliary.js';

// =============================================================================
// FTS Index Implementation
// =============================================================================

/**
 * In-memory FTS index implementation
 */
class FTSIndexImpl implements FTSIndex {
  readonly columns: string[];
  readonly tokenizer: TokenizerType;
  readonly columnWeights?: ColumnWeights;
  readonly prefixLengths?: number[];

  private _tokenizer: Tokenizer;
  private postingLists: Map<string, PostingList> = new Map();
  private prefixPostings: Map<string, Set<string>> = new Map(); // prefix -> terms
  private documents: Map<number, DocumentMeta> = new Map();
  private documentContent: Map<number, Record<string, string>> = new Map();
  private totalTokens: number = 0;
  private columnTokenTotals: number[] = [];

  constructor(config: FTSConfig) {
    this.columns = config.columns;
    this.tokenizer = config.tokenizer ?? 'simple';
    this.columnWeights = config.columnWeights;
    this.prefixLengths = config.prefix;
    this._tokenizer = createTokenizer(this.tokenizer);
    this.columnTokenTotals = new Array(config.columns.length).fill(0);
  }

  /**
   * Insert or update a document
   */
  async insert(doc: FTSDocument): Promise<void> {
    // If document exists, delete it first
    if (this.documents.has(doc.rowid)) {
      await this.delete(doc.rowid);
    }

    const columnLengths: number[] = [];
    let totalLength = 0;

    // Index each column
    for (let colIdx = 0; colIdx < this.columns.length; colIdx++) {
      const column = this.columns[colIdx];
      const content = doc.columns[column] ?? '';
      const tokens = this._tokenizer.tokenize(content);

      columnLengths.push(tokens.length);
      totalLength += tokens.length;
      this.columnTokenTotals[colIdx] += tokens.length;

      // Build term frequency map for this document/column
      const termFreqs = new Map<string, { freq: number; positions: number[] }>();

      for (const token of tokens) {
        const existing = termFreqs.get(token.term);
        if (existing) {
          existing.freq++;
          existing.positions.push(token.position);
        } else {
          termFreqs.set(token.term, { freq: 1, positions: [token.position] });
        }
      }

      // Add to posting lists
      for (const [term, { freq, positions }] of termFreqs) {
        let postingList = this.postingLists.get(term);
        if (!postingList) {
          postingList = { df: 0, postings: [] };
          this.postingLists.set(term, postingList);
        }

        // Check if document already in postings (shouldn't happen after delete)
        const existingPosting = postingList.postings.find(
          (p) => p.docId === doc.rowid && p.columnIndex === colIdx
        );
        if (!existingPosting) {
          postingList.df++;
          postingList.postings.push({
            docId: doc.rowid,
            frequency: freq,
            positions,
            columnIndex: colIdx,
          });
        }

        // Index prefixes
        if (this.prefixLengths) {
          for (const prefixLen of this.prefixLengths) {
            if (term.length >= prefixLen) {
              const prefix = term.slice(0, prefixLen);
              let prefixTerms = this.prefixPostings.get(prefix);
              if (!prefixTerms) {
                prefixTerms = new Set();
                this.prefixPostings.set(prefix, prefixTerms);
              }
              prefixTerms.add(term);
            }
          }
        }
      }
    }

    // Store document metadata
    this.documents.set(doc.rowid, {
      rowid: doc.rowid,
      columnLengths,
      totalLength,
    });

    // Store content for highlighting/snippets
    this.documentContent.set(doc.rowid, { ...doc.columns });

    this.totalTokens += totalLength;
  }

  /**
   * Delete a document
   */
  async delete(rowid: number): Promise<boolean> {
    const docMeta = this.documents.get(rowid);
    if (!docMeta) return false;

    // Remove from posting lists
    for (const [term, postingList] of this.postingLists) {
      const initialLength = postingList.postings.length;
      postingList.postings = postingList.postings.filter((p) => p.docId !== rowid);
      const removed = initialLength - postingList.postings.length;
      postingList.df -= removed;

      // Remove empty posting lists
      if (postingList.postings.length === 0) {
        this.postingLists.delete(term);
      }
    }

    // Update column totals
    for (let i = 0; i < docMeta.columnLengths.length; i++) {
      this.columnTokenTotals[i] -= docMeta.columnLengths[i];
    }

    this.totalTokens -= docMeta.totalLength;
    this.documents.delete(rowid);
    this.documentContent.delete(rowid);

    return true;
  }

  /**
   * Search the index
   */
  async search(queryStr: string, options?: FTSSearchOptions): Promise<FTSSearchResult[]> {
    if (!queryStr || queryStr.trim() === '') {
      return [];
    }

    // Parse the query
    const query = parseMatchQuery(queryStr);

    // Execute the query
    const matchingDocs = await this.executeQuery(query);

    // Calculate scores
    const results = this.rankResults(matchingDocs, query, options);

    // Apply highlighting and snippets
    this.applyAuxiliaryFunctions(results, query, options);

    // Apply limit and offset
    let finalResults = results;
    if (options?.offset) {
      finalResults = finalResults.slice(options.offset);
    }
    if (options?.limit) {
      finalResults = finalResults.slice(0, options.limit);
    }

    return finalResults;
  }

  /**
   * Execute a query and return matching document IDs
   */
  private async executeQuery(
    query: MatchQuery,
    columnFilter?: number
  ): Promise<Set<number>> {
    switch (query.type) {
      case 'term':
        return this.executeTermQuery(query.term, columnFilter);

      case 'phrase':
        return this.executePhraseQuery(query.terms, columnFilter);

      case 'prefix':
        return this.executePrefixQuery(query.prefix, columnFilter);

      case 'and': {
        const leftDocs = await this.executeQuery(query.left, columnFilter);
        const rightDocs = await this.executeQuery(query.right, columnFilter);
        return this.intersect(leftDocs, rightDocs);
      }

      case 'or': {
        const leftDocs = await this.executeQuery(query.left, columnFilter);
        const rightDocs = await this.executeQuery(query.right, columnFilter);
        return this.union(leftDocs, rightDocs);
      }

      case 'not': {
        const includeDocs = await this.executeQuery(query.include, columnFilter);
        const excludeDocs = await this.executeQuery(query.exclude, columnFilter);
        return this.difference(includeDocs, excludeDocs);
      }

      case 'near':
        return this.executeNearQuery(query.terms, query.distance, columnFilter);

      case 'column': {
        const colIdx = this.columns.indexOf(query.column);
        if (colIdx === -1) {
          throw new Error(`Unknown column: ${query.column}`);
        }
        return this.executeQuery(query.query, colIdx);
      }

      default:
        return new Set();
    }
  }

  /**
   * Execute a simple term query
   */
  private executeTermQuery(term: string, columnFilter?: number): Set<number> {
    if (!term) return new Set();

    const normalizedTerm = this._tokenizer.normalize(term);
    const postingList = this.postingLists.get(normalizedTerm);

    if (!postingList) return new Set();

    const docs = new Set<number>();
    for (const posting of postingList.postings) {
      if (columnFilter === undefined || posting.columnIndex === columnFilter) {
        docs.add(posting.docId);
      }
    }

    return docs;
  }

  /**
   * Execute a phrase query
   */
  private executePhraseQuery(terms: string[], columnFilter?: number): Set<number> {
    if (terms.length === 0) return new Set();
    if (terms.length === 1) return this.executeTermQuery(terms[0], columnFilter);

    const normalizedTerms = terms.map((t) => this._tokenizer.normalize(t));

    // Get posting lists for all terms
    const postingLists = normalizedTerms.map((t) => this.postingLists.get(t));

    // All terms must exist
    if (postingLists.some((p) => !p)) return new Set();

    // Find documents containing all terms
    const candidateDocs = new Set<number>();
    const firstPostings = postingLists[0]!;
    for (const posting of firstPostings.postings) {
      if (columnFilter === undefined || posting.columnIndex === columnFilter) {
        candidateDocs.add(posting.docId);
      }
    }

    for (let i = 1; i < postingLists.length; i++) {
      const termDocs = new Set<number>();
      for (const posting of postingLists[i]!.postings) {
        if (columnFilter === undefined || posting.columnIndex === columnFilter) {
          termDocs.add(posting.docId);
        }
      }
      // Intersect
      for (const doc of candidateDocs) {
        if (!termDocs.has(doc)) {
          candidateDocs.delete(doc);
        }
      }
    }

    // For each candidate, check phrase positions
    const phraseMatches = new Set<number>();

    for (const docId of candidateDocs) {
      // Get positions for each term in this document
      const termPositions: number[][] = [];
      let valid = true;

      for (let i = 0; i < normalizedTerms.length; i++) {
        const postingList = postingLists[i]!;
        const docPosting = postingList.postings.find(
          (p) =>
            p.docId === docId &&
            (columnFilter === undefined || p.columnIndex === columnFilter)
        );

        if (!docPosting) {
          valid = false;
          break;
        }

        termPositions.push(docPosting.positions);
      }

      if (!valid) continue;

      // Check if positions form a consecutive sequence
      if (this.checkPhrasePositions(termPositions)) {
        phraseMatches.add(docId);
      }
    }

    return phraseMatches;
  }

  /**
   * Check if term positions form a consecutive phrase
   */
  private checkPhrasePositions(termPositions: number[][]): boolean {
    if (termPositions.length === 0) return false;

    // For each starting position of the first term
    for (const startPos of termPositions[0]) {
      let matches = true;

      // Check if subsequent terms appear at consecutive positions
      for (let i = 1; i < termPositions.length; i++) {
        const expectedPos = startPos + i;
        if (!termPositions[i].includes(expectedPos)) {
          matches = false;
          break;
        }
      }

      if (matches) return true;
    }

    return false;
  }

  /**
   * Execute a prefix query
   */
  private executePrefixQuery(prefix: string, columnFilter?: number): Set<number> {
    const normalizedPrefix = this._tokenizer.normalize(prefix);
    const docs = new Set<number>();

    // Check if we have prefix index
    if (this.prefixLengths) {
      const matchingPrefixLen = this.prefixLengths.find(
        (len) => normalizedPrefix.length >= len
      );
      if (matchingPrefixLen) {
        const prefixKey = normalizedPrefix.slice(0, matchingPrefixLen);
        const matchingTerms = this.prefixPostings.get(prefixKey);
        if (matchingTerms) {
          for (const term of matchingTerms) {
            if (term.startsWith(normalizedPrefix)) {
              const termDocs = this.executeTermQuery(term, columnFilter);
              for (const doc of termDocs) {
                docs.add(doc);
              }
            }
          }
          return docs;
        }
      }
    }

    // Fallback: scan all terms
    for (const [term] of this.postingLists) {
      if (term.startsWith(normalizedPrefix)) {
        const termDocs = this.executeTermQuery(term, columnFilter);
        for (const doc of termDocs) {
          docs.add(doc);
        }
      }
    }

    return docs;
  }

  /**
   * Execute a NEAR proximity query
   */
  private executeNearQuery(
    terms: string[],
    distance: number,
    columnFilter?: number
  ): Set<number> {
    if (terms.length < 2) {
      return terms.length === 1 ? this.executeTermQuery(terms[0], columnFilter) : new Set();
    }

    const normalizedTerms = terms.map((t) => this._tokenizer.normalize(t));

    // Get posting lists for all terms
    const postingLists = normalizedTerms.map((t) => this.postingLists.get(t));

    // All terms must exist
    if (postingLists.some((p) => !p)) return new Set();

    // Find documents containing all terms
    let candidateDocs = new Set<number>();
    for (const posting of postingLists[0]!.postings) {
      if (columnFilter === undefined || posting.columnIndex === columnFilter) {
        candidateDocs.add(posting.docId);
      }
    }

    for (let i = 1; i < postingLists.length; i++) {
      const termDocs = new Set<number>();
      for (const posting of postingLists[i]!.postings) {
        if (columnFilter === undefined || posting.columnIndex === columnFilter) {
          termDocs.add(posting.docId);
        }
      }
      candidateDocs = this.intersect(candidateDocs, termDocs);
    }

    // Check proximity for each candidate
    const nearMatches = new Set<number>();

    for (const docId of candidateDocs) {
      // Get all positions for this document
      const allPositions: Array<{ pos: number; termIdx: number }> = [];

      for (let termIdx = 0; termIdx < normalizedTerms.length; termIdx++) {
        const postingList = postingLists[termIdx]!;
        const docPosting = postingList.postings.find(
          (p) =>
            p.docId === docId &&
            (columnFilter === undefined || p.columnIndex === columnFilter)
        );

        if (docPosting) {
          for (const pos of docPosting.positions) {
            allPositions.push({ pos, termIdx });
          }
        }
      }

      // Sort by position
      allPositions.sort((a, b) => a.pos - b.pos);

      // Check if all terms appear within distance
      if (this.checkNearProximity(allPositions, terms.length, distance)) {
        nearMatches.add(docId);
      }
    }

    return nearMatches;
  }

  /**
   * Check if all terms appear within the specified distance
   */
  private checkNearProximity(
    positions: Array<{ pos: number; termIdx: number }>,
    numTerms: number,
    distance: number
  ): boolean {
    // Sliding window approach
    for (let i = 0; i < positions.length; i++) {
      const windowTerms = new Set<number>();
      let windowEnd = i;

      for (let j = i; j < positions.length; j++) {
        if (positions[j].pos - positions[i].pos <= distance) {
          windowTerms.add(positions[j].termIdx);
          windowEnd = j;
        } else {
          break;
        }
      }

      if (windowTerms.size === numTerms) {
        return true;
      }
    }

    return false;
  }

  /**
   * Rank search results
   */
  private rankResults(
    matchingDocs: Set<number>,
    query: MatchQuery,
    options?: FTSSearchOptions
  ): FTSSearchResult[] {
    const results: FTSSearchResult[] = [];
    const numDocs = this.documents.size;
    const avgDocLength = numDocs > 0 ? this.totalTokens / numDocs : 0;

    const k1 = options?.bm25Params?.k1 ?? DEFAULT_BM25_PARAMS.k1;
    const b = options?.bm25Params?.b ?? DEFAULT_BM25_PARAMS.b;

    const searchTerms = extractTerms(query).map((t) => this._tokenizer.normalize(t));

    for (const docId of matchingDocs) {
      const docMeta = this.documents.get(docId);
      if (!docMeta) continue;

      let score = 0;

      // Calculate BM25 score for each term
      for (const term of searchTerms) {
        const postingList = this.postingLists.get(term);
        if (!postingList) continue;

        // Sum scores across all columns
        for (const posting of postingList.postings) {
          if (posting.docId !== docId) continue;

          const colWeight = this.columnWeights?.[this.columns[posting.columnIndex]] ?? 1;

          const termScore = calculateBM25({
            termFrequency: posting.frequency,
            documentLength: docMeta.totalLength,
            averageDocumentLength: avgDocLength,
            numDocuments: numDocs,
            documentFrequency: postingList.df,
            k1,
            b,
          });

          score += termScore * colWeight;
        }
      }

      results.push({
        rowid: docId,
        rank: score,
        bm25: options?.includeScore ? score : undefined,
      });
    }

    // Sort by score descending
    results.sort((a, b) => b.rank - a.rank);

    return results;
  }

  /**
   * Apply highlight and snippet functions to results
   */
  private applyAuxiliaryFunctions(
    results: FTSSearchResult[],
    query: MatchQuery,
    options?: FTSSearchOptions
  ): void {
    const terms = extractTerms(query);

    for (const result of results) {
      const content = this.documentContent.get(result.rowid);
      if (!content) continue;

      // Apply highlighting
      if (options?.highlight) {
        const { column, startTag, endTag } = options.highlight;
        const text = content[column] ?? '';
        result.highlight = highlight(text, terms, { startTag, endTag });
      }

      // Apply snippet
      if (options?.snippet) {
        const { column, maxLength, ellipsis, startTag, endTag } = options.snippet;
        const text = content[column] ?? '';
        result.snippet = snippet(text, terms, {
          maxLength,
          ellipsis,
          startTag,
          endTag,
        });
      }
    }
  }

  // Set operations
  private intersect(a: Set<number>, b: Set<number>): Set<number> {
    const result = new Set<number>();
    for (const item of a) {
      if (b.has(item)) {
        result.add(item);
      }
    }
    return result;
  }

  private union(a: Set<number>, b: Set<number>): Set<number> {
    const result = new Set(a);
    for (const item of b) {
      result.add(item);
    }
    return result;
  }

  private difference(a: Set<number>, b: Set<number>): Set<number> {
    const result = new Set<number>();
    for (const item of a) {
      if (!b.has(item)) {
        result.add(item);
      }
    }
    return result;
  }

  /**
   * Get number of indexed documents
   */
  async count(): Promise<number> {
    return this.documents.size;
  }

  /**
   * Get index statistics
   */
  async stats(): Promise<IndexStats> {
    const numDocs = this.documents.size;
    return {
      numDocuments: numDocs,
      totalTokens: this.totalTokens,
      avgDocLength: numDocs > 0 ? this.totalTokens / numDocs : 0,
      vocabularySize: this.postingLists.size,
      avgColumnLengths: this.columns.map((_, i) =>
        numDocs > 0 ? this.columnTokenTotals[i] / numDocs : 0
      ),
    };
  }

  /**
   * Clear all documents
   */
  async clear(): Promise<void> {
    this.postingLists.clear();
    this.prefixPostings.clear();
    this.documents.clear();
    this.documentContent.clear();
    this.totalTokens = 0;
    this.columnTokenTotals = new Array(this.columns.length).fill(0);
  }

  /**
   * Optimize/compact the index
   */
  async optimize(): Promise<void> {
    // Sort posting lists by doc ID for better cache locality
    for (const postingList of this.postingLists.values()) {
      postingList.postings.sort((a, b) => a.docId - b.docId);
    }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a new FTS index
 */
export function createFTSIndex(config: FTSConfig): FTSIndex {
  return new FTSIndexImpl(config);
}
