/**
 * Full-Text Search Type Definitions
 *
 * Core types for FTS5-style full-text search implementation.
 */

// =============================================================================
// Core Types
// =============================================================================

/**
 * Token produced by a tokenizer
 */
export interface Token {
  /** The normalized term */
  term: string;
  /** Position in token stream (0-indexed) */
  position: number;
  /** Byte offset of start in original text */
  start: number;
  /** Byte offset of end in original text */
  end: number;
}

/**
 * Tokenizer type identifiers
 */
export type TokenizerType = 'simple' | 'porter' | 'unicode61' | 'trigram';

/**
 * Column weights for ranking
 */
export type ColumnWeights = Record<string, number>;

/**
 * Configuration for creating an FTS index
 */
export interface FTSConfig {
  /** Column names to index */
  columns: string[];
  /** Tokenizer to use (default: 'simple') */
  tokenizer?: TokenizerType;
  /** Column weights for ranking (default: 1.0 for all) */
  columnWeights?: ColumnWeights;
  /** Prefix lengths to index for prefix queries */
  prefix?: number[];
  /** Content table for external content FTS */
  contentTable?: string;
  /** Rowid column name for external content */
  contentRowid?: string;
}

/**
 * Document to be indexed
 */
export interface FTSDocument {
  /** Unique row identifier */
  rowid: number;
  /** Column values keyed by column name */
  columns: Record<string, string>;
}

/**
 * Search result from FTS query
 */
export interface FTSSearchResult {
  /** Row identifier */
  rowid: number;
  /** Relevance rank score */
  rank: number;
  /** BM25 score (if requested) */
  bm25?: number;
  /** Highlighted content (if requested) */
  highlight?: string;
  /** Content snippet (if requested) */
  snippet?: string;
  /** Column that matched (for column-specific queries) */
  matchedColumn?: string;
  /** Term positions in the document */
  positions?: Map<string, number[]>;
}

/**
 * Options for search queries
 */
export interface FTSSearchOptions {
  /** Ranking algorithm ('bm25' or 'tfidf') */
  rank?: 'bm25' | 'tfidf';
  /** Include BM25 score in results */
  includeScore?: boolean;
  /** Highlight matched terms */
  highlight?: HighlightConfig;
  /** Generate content snippet */
  snippet?: SnippetConfig;
  /** Custom BM25 parameters */
  bm25Params?: BM25Params;
  /** Maximum results to return */
  limit?: number;
  /** Results to skip */
  offset?: number;
}

/**
 * Configuration for highlighting
 */
export interface HighlightConfig {
  /** Column to highlight */
  column: string;
  /** Tag to insert before match */
  startTag: string;
  /** Tag to insert after match */
  endTag: string;
}

/**
 * Configuration for snippets
 */
export interface SnippetConfig {
  /** Column to extract snippet from */
  column: string;
  /** Maximum snippet length in characters */
  maxLength: number;
  /** Ellipsis string for truncation */
  ellipsis?: string;
  /** Tag to insert before match */
  startTag?: string;
  /** Tag to insert after match */
  endTag?: string;
}

/**
 * BM25 algorithm parameters
 */
export interface BM25Params {
  /** Term saturation parameter (default: 1.2) */
  k1: number;
  /** Length normalization parameter (default: 0.75) */
  b: number;
}

// =============================================================================
// Inverted Index Types
// =============================================================================

/**
 * Posting list entry for a term in a document
 */
export interface Posting {
  /** Document rowid */
  docId: number;
  /** Term frequency in document */
  frequency: number;
  /** Positions where term appears */
  positions: number[];
  /** Column index where term appears */
  columnIndex: number;
}

/**
 * Posting list for a term
 */
export interface PostingList {
  /** Document frequency (number of documents containing term) */
  df: number;
  /** List of postings */
  postings: Posting[];
}

/**
 * Document metadata stored in index
 */
export interface DocumentMeta {
  /** Document rowid */
  rowid: number;
  /** Length of each column in tokens */
  columnLengths: number[];
  /** Total document length in tokens */
  totalLength: number;
}

/**
 * Index statistics
 */
export interface IndexStats {
  /** Total number of documents */
  numDocuments: number;
  /** Total number of tokens across all documents */
  totalTokens: number;
  /** Average document length in tokens */
  avgDocLength: number;
  /** Number of unique terms */
  vocabularySize: number;
  /** Average column lengths */
  avgColumnLengths: number[];
}

// =============================================================================
// Query AST Types
// =============================================================================

/**
 * Base query node type
 */
export type MatchQuery =
  | TermQuery
  | PhraseQuery
  | PrefixQuery
  | AndQuery
  | OrQuery
  | NotQuery
  | NearQuery
  | ColumnQuery;

/**
 * Simple term query
 */
export interface TermQuery {
  type: 'term';
  term: string;
}

/**
 * Phrase query (exact sequence of terms)
 */
export interface PhraseQuery {
  type: 'phrase';
  terms: string[];
}

/**
 * Prefix query (term*)
 */
export interface PrefixQuery {
  type: 'prefix';
  prefix: string;
}

/**
 * AND boolean query
 */
export interface AndQuery {
  type: 'and';
  left: MatchQuery;
  right: MatchQuery;
}

/**
 * OR boolean query
 */
export interface OrQuery {
  type: 'or';
  left: MatchQuery;
  right: MatchQuery;
}

/**
 * NOT boolean query
 */
export interface NotQuery {
  type: 'not';
  include: MatchQuery;
  exclude: MatchQuery;
}

/**
 * NEAR proximity query
 */
export interface NearQuery {
  type: 'near';
  terms: string[];
  distance: number;
}

/**
 * Column-filtered query
 */
export interface ColumnQuery {
  type: 'column';
  column: string;
  query: MatchQuery;
}

// =============================================================================
// FTS Index Interface
// =============================================================================

/**
 * Full-text search index interface
 */
export interface FTSIndex {
  /** Indexed columns */
  readonly columns: string[];
  /** Tokenizer type */
  readonly tokenizer: TokenizerType;
  /** Column weights */
  readonly columnWeights?: ColumnWeights;
  /** Prefix lengths for prefix queries */
  readonly prefixLengths?: number[];

  /**
   * Insert or update a document
   */
  insert(doc: FTSDocument): Promise<void>;

  /**
   * Delete a document by rowid
   */
  delete(rowid: number): Promise<boolean>;

  /**
   * Search the index
   */
  search(query: string, options?: FTSSearchOptions): Promise<FTSSearchResult[]>;

  /**
   * Get number of indexed documents
   */
  count(): Promise<number>;

  /**
   * Get index statistics
   */
  stats(): Promise<IndexStats>;

  /**
   * Clear all documents from index
   */
  clear(): Promise<void>;

  /**
   * Optimize/compact the index
   */
  optimize(): Promise<void>;
}

// =============================================================================
// Tokenizer Interface
// =============================================================================

/**
 * Tokenizer interface
 */
export interface Tokenizer {
  /** Tokenize input text into tokens */
  tokenize(text: string): Token[];
  /** Get normalized form of a term for matching */
  normalize(term: string): string;
}

// =============================================================================
// Auxiliary Function Types
// =============================================================================

/**
 * Options for highlight function
 */
export interface HighlightOptions {
  /** Tag to insert before match */
  startTag: string;
  /** Tag to insert after match */
  endTag: string;
  /** Case-sensitive matching */
  caseSensitive?: boolean;
}

/**
 * Options for snippet function
 */
export interface SnippetOptions {
  /** Maximum snippet length */
  maxLength: number;
  /** Ellipsis for truncation */
  ellipsis?: string;
  /** Tag to insert before match */
  startTag?: string;
  /** Tag to insert after match */
  endTag?: string;
}

// =============================================================================
// Ranking Types
// =============================================================================

/**
 * Parameters for BM25 calculation
 */
export interface RankingParams {
  /** Term frequency in document */
  termFrequency: number;
  /** Document length in tokens */
  documentLength: number;
  /** Average document length */
  averageDocumentLength: number;
  /** Total number of documents */
  numDocuments: number;
  /** Number of documents containing term */
  documentFrequency: number;
  /** BM25 k1 parameter */
  k1: number;
  /** BM25 b parameter */
  b: number;
}
