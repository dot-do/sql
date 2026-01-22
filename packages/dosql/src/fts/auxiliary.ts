/**
 * FTS5 Auxiliary Functions
 *
 * Implements highlight() and snippet() functions for full-text search results.
 */

import type { HighlightOptions, SnippetOptions } from './types.js';

// Re-export types
export type { HighlightOptions, SnippetOptions };

// =============================================================================
// Highlight Function
// =============================================================================

/**
 * Highlight matched terms in text
 *
 * @param text - Original text to highlight
 * @param terms - Terms to highlight
 * @param options - Highlight options
 * @returns Text with highlighted terms
 */
export function highlight(
  text: string,
  terms: string[],
  options: HighlightOptions
): string {
  if (!terms || terms.length === 0 || !text) {
    return text;
  }

  const { startTag, endTag, caseSensitive = false } = options;

  // Build regex pattern for all terms
  // Escape special regex characters in terms
  const escapedTerms = terms.map((t) =>
    t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  );

  // Match whole words only
  const pattern = new RegExp(
    `\\b(${escapedTerms.join('|')})\\b`,
    caseSensitive ? 'g' : 'gi'
  );

  // Replace matches with highlighted version
  return text.replace(pattern, `${startTag}$1${endTag}`);
}

/**
 * Highlight with multiple term groups (for phrases)
 */
export function highlightPhrases(
  text: string,
  phrases: string[][],
  terms: string[],
  options: HighlightOptions
): string {
  const { startTag, endTag, caseSensitive = false } = options;
  let result = text;

  // First highlight phrases
  for (const phrase of phrases) {
    if (phrase.length > 0) {
      const phrasePattern = phrase
        .map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
        .join('\\s+');
      const regex = new RegExp(
        `\\b(${phrasePattern})\\b`,
        caseSensitive ? 'g' : 'gi'
      );
      result = result.replace(regex, `${startTag}$1${endTag}`);
    }
  }

  // Then highlight individual terms (avoiding already highlighted regions)
  result = highlight(result, terms, options);

  return result;
}

// =============================================================================
// Snippet Function
// =============================================================================

/**
 * Extract a snippet from text around matched terms
 *
 * @param text - Original text
 * @param terms - Terms that were matched
 * @param options - Snippet options
 * @returns Extracted snippet with optional highlighting
 */
export function snippet(
  text: string,
  terms: string[],
  options: SnippetOptions
): string {
  if (!text || text.length === 0) {
    return '';
  }

  const {
    maxLength,
    ellipsis = '...',
    startTag = '',
    endTag = '',
  } = options;

  // If text is shorter than maxLength, return it (with highlighting)
  if (text.length <= maxLength) {
    if (startTag && endTag && terms.length > 0) {
      return highlight(text, terms, { startTag, endTag });
    }
    return text;
  }

  // Find the best snippet region
  const region = findBestSnippetRegion(text, terms, maxLength);

  // Extract the snippet
  let snippetText = text.slice(region.start, region.end);

  // Add ellipsis if truncated
  if (region.start > 0) {
    snippetText = ellipsis + snippetText;
  }
  if (region.end < text.length) {
    snippetText = snippetText + ellipsis;
  }

  // Apply highlighting if tags provided
  if (startTag && endTag && terms.length > 0) {
    snippetText = highlight(snippetText, terms, { startTag, endTag });
  }

  return snippetText;
}

/**
 * Find the best region for a snippet based on term density
 */
function findBestSnippetRegion(
  text: string,
  terms: string[],
  maxLength: number
): { start: number; end: number; score: number } {
  if (terms.length === 0) {
    return { start: 0, end: Math.min(text.length, maxLength), score: 0 };
  }

  // Find all term positions
  const positions: Array<{ pos: number; term: string }> = [];
  const lowerText = text.toLowerCase();

  for (const term of terms) {
    const lowerTerm = term.toLowerCase();
    let pos = 0;
    while ((pos = lowerText.indexOf(lowerTerm, pos)) !== -1) {
      // Check for word boundary
      const before = pos === 0 || /\W/.test(text[pos - 1]);
      const after = pos + term.length >= text.length || /\W/.test(text[pos + term.length]);
      if (before && after) {
        positions.push({ pos, term });
      }
      pos++;
    }
  }

  if (positions.length === 0) {
    return { start: 0, end: Math.min(text.length, maxLength), score: 0 };
  }

  // Sort positions
  positions.sort((a, b) => a.pos - b.pos);

  // Sliding window to find region with most matches
  let bestRegion = { start: 0, end: Math.min(text.length, maxLength), score: 0 };

  for (let i = 0; i < positions.length; i++) {
    // Center window on this position
    const centerPos = positions[i].pos;
    const halfWindow = Math.floor(maxLength / 2);

    let start = Math.max(0, centerPos - halfWindow);
    let end = Math.min(text.length, start + maxLength);

    // Adjust to word boundaries
    if (start > 0) {
      // Find previous space
      const spacePos = text.lastIndexOf(' ', start);
      if (spacePos > start - 20) {
        start = spacePos + 1;
      }
    }
    if (end < text.length) {
      // Find next space
      const spacePos = text.indexOf(' ', end);
      if (spacePos !== -1 && spacePos < end + 20) {
        end = spacePos;
      }
    }

    // Count matches in this region
    let score = 0;
    const uniqueTerms = new Set<string>();
    for (const { pos, term } of positions) {
      if (pos >= start && pos + term.length <= end) {
        score++;
        uniqueTerms.add(term);
      }
    }
    // Bonus for unique term coverage
    score += uniqueTerms.size * 2;

    if (score > bestRegion.score) {
      bestRegion = { start, end, score };
    }
  }

  return bestRegion;
}

/**
 * Extract multiple snippets from text
 */
export function snippetMultiple(
  text: string,
  terms: string[],
  options: SnippetOptions & { numSnippets: number; separator?: string }
): string {
  const { numSnippets, separator = ' ... ', ...snippetOptions } = options;

  if (!text || text.length === 0 || numSnippets <= 0) {
    return '';
  }

  // For single snippet, use regular snippet function
  if (numSnippets === 1) {
    return snippet(text, terms, snippetOptions);
  }

  const snippetLength = Math.floor(snippetOptions.maxLength / numSnippets);
  const snippets: string[] = [];
  const usedRegions: Array<{ start: number; end: number }> = [];

  // Find all term positions
  const positions: number[] = [];
  const lowerText = text.toLowerCase();

  for (const term of terms) {
    const lowerTerm = term.toLowerCase();
    let pos = 0;
    while ((pos = lowerText.indexOf(lowerTerm, pos)) !== -1) {
      positions.push(pos);
      pos++;
    }
  }

  positions.sort((a, b) => a - b);

  // Extract snippets around different positions
  for (const pos of positions) {
    if (snippets.length >= numSnippets) break;

    // Check if this position overlaps with existing regions
    const halfWindow = Math.floor(snippetLength / 2);
    const start = Math.max(0, pos - halfWindow);
    const end = Math.min(text.length, start + snippetLength);

    const overlaps = usedRegions.some(
      (r) => (start >= r.start && start < r.end) || (end > r.start && end <= r.end)
    );

    if (!overlaps) {
      const snippetText = snippet(
        text.slice(start, end),
        terms,
        { ...snippetOptions, maxLength: snippetLength }
      );
      snippets.push(snippetText);
      usedRegions.push({ start, end });
    }
  }

  // Fill remaining slots from beginning if needed
  if (snippets.length === 0) {
    return snippet(text, terms, snippetOptions);
  }

  return snippets.join(separator);
}

// =============================================================================
// Text Utilities
// =============================================================================

/**
 * Get surrounding context for a position in text
 */
export function getContext(
  text: string,
  position: number,
  contextLength: number = 50
): { before: string; after: string } {
  const halfLength = Math.floor(contextLength / 2);

  let beforeStart = Math.max(0, position - halfLength);
  let afterEnd = Math.min(text.length, position + halfLength);

  // Adjust to word boundaries
  if (beforeStart > 0) {
    const spacePos = text.lastIndexOf(' ', beforeStart);
    if (spacePos > beforeStart - 20) {
      beforeStart = spacePos + 1;
    }
  }
  if (afterEnd < text.length) {
    const spacePos = text.indexOf(' ', afterEnd);
    if (spacePos !== -1 && spacePos < afterEnd + 20) {
      afterEnd = spacePos;
    }
  }

  return {
    before: text.slice(beforeStart, position),
    after: text.slice(position, afterEnd),
  };
}

/**
 * Count term occurrences in text
 */
export function countOccurrences(text: string, term: string): number {
  const lowerText = text.toLowerCase();
  const lowerTerm = term.toLowerCase();
  let count = 0;
  let pos = 0;

  while ((pos = lowerText.indexOf(lowerTerm, pos)) !== -1) {
    // Check word boundaries
    const before = pos === 0 || /\W/.test(text[pos - 1]);
    const after = pos + term.length >= text.length || /\W/.test(text[pos + term.length]);
    if (before && after) {
      count++;
    }
    pos++;
  }

  return count;
}
