/**
 * Three-Way Merge Algorithm for DoSQL Branches
 *
 * Implements git-style three-way merge for file content:
 * - Line-based diff algorithm
 * - Conflict detection and markers
 * - Auto-resolution strategies
 *
 * @packageDocumentation
 */

import type {
  MergeStrategy,
  MergeConflict,
  ConflictVersion,
  CommitId,
} from './types.js';

// =============================================================================
// Diff Types
// =============================================================================

/**
 * A single diff operation
 */
export interface DiffOp {
  /** Type of operation */
  type: 'equal' | 'insert' | 'delete';
  /** Lines involved */
  lines: string[];
  /** Starting line number in original */
  originalStart?: number;
  /** Starting line number in modified */
  modifiedStart?: number;
}

/**
 * Result of diff operation between two texts
 */
export interface DiffResult {
  /** List of diff operations */
  ops: DiffOp[];
  /** Number of additions */
  additions: number;
  /** Number of deletions */
  deletions: number;
}

/**
 * A hunk in a patch
 */
export interface DiffHunk {
  /** Start line in original */
  originalStart: number;
  /** Number of lines in original */
  originalCount: number;
  /** Start line in modified */
  modifiedStart: number;
  /** Number of lines in modified */
  modifiedCount: number;
  /** Lines in the hunk (with +/-/space prefix) */
  lines: string[];
}

// =============================================================================
// Three-Way Merge Types
// =============================================================================

/**
 * Result of three-way merge
 */
export interface ThreeWayMergeResult {
  /** Whether merge was successful (no conflicts) */
  success: boolean;
  /** Merged content (may contain conflict markers if not successful) */
  content: string;
  /** Lines of merged content */
  lines: string[];
  /** List of conflicts */
  conflicts: MergeConflictRegion[];
  /** Statistics */
  stats: {
    baseLines: number;
    oursLines: number;
    theirsLines: number;
    resultLines: number;
    conflictRegions: number;
  };
}

/**
 * A conflict region in merged content
 */
export interface MergeConflictRegion {
  /** Starting line in result */
  startLine: number;
  /** Ending line in result */
  endLine: number;
  /** Our lines (target branch) */
  ours: string[];
  /** Their lines (source branch) */
  theirs: string[];
  /** Base lines (common ancestor) */
  base: string[];
}

/**
 * Options for merge operation
 */
export interface MergeOptions {
  /** Strategy for auto-resolution */
  strategy?: 'ours' | 'theirs' | 'union';
  /** Include diff3-style markers (show base) */
  diff3?: boolean;
  /** Conflict marker labels */
  labels?: {
    ours?: string;
    theirs?: string;
    base?: string;
  };
}

// =============================================================================
// Diff Algorithm
// =============================================================================

/**
 * Compute the diff between two texts using Myers algorithm
 */
export function diff(original: string, modified: string): DiffResult {
  const originalLines = splitLines(original);
  const modifiedLines = splitLines(modified);

  const ops: DiffOp[] = [];
  let additions = 0;
  let deletions = 0;

  // Compute LCS (Longest Common Subsequence)
  const lcs = computeLCS(originalLines, modifiedLines);

  // Build diff ops from LCS
  let origIdx = 0;
  let modIdx = 0;

  for (const [origMatch, modMatch] of lcs) {
    // Handle deletions (lines in original before match)
    if (origIdx < origMatch) {
      const deleted = originalLines.slice(origIdx, origMatch);
      ops.push({
        type: 'delete',
        lines: deleted,
        originalStart: origIdx,
      });
      deletions += deleted.length;
    }

    // Handle insertions (lines in modified before match)
    if (modIdx < modMatch) {
      const inserted = modifiedLines.slice(modIdx, modMatch);
      ops.push({
        type: 'insert',
        lines: inserted,
        modifiedStart: modIdx,
      });
      additions += inserted.length;
    }

    // Handle equal line
    ops.push({
      type: 'equal',
      lines: [originalLines[origMatch]],
      originalStart: origMatch,
      modifiedStart: modMatch,
    });

    origIdx = origMatch + 1;
    modIdx = modMatch + 1;
  }

  // Handle remaining lines
  if (origIdx < originalLines.length) {
    const deleted = originalLines.slice(origIdx);
    ops.push({
      type: 'delete',
      lines: deleted,
      originalStart: origIdx,
    });
    deletions += deleted.length;
  }

  if (modIdx < modifiedLines.length) {
    const inserted = modifiedLines.slice(modIdx);
    ops.push({
      type: 'insert',
      lines: inserted,
      modifiedStart: modIdx,
    });
    additions += inserted.length;
  }

  return {
    ops: coalesceOps(ops),
    additions,
    deletions,
  };
}

/**
 * Compute unified diff output
 */
export function unifiedDiff(
  original: string,
  modified: string,
  options: {
    contextLines?: number;
    originalName?: string;
    modifiedName?: string;
  } = {}
): string {
  const { contextLines = 3, originalName = 'a', modifiedName = 'b' } = options;
  const diffResult = diff(original, modified);

  if (diffResult.additions === 0 && diffResult.deletions === 0) {
    return '';
  }

  const originalLines = splitLines(original);
  const modifiedLines = splitLines(modified);
  const hunks = generateHunks(diffResult.ops, originalLines, modifiedLines, contextLines);

  const lines: string[] = [
    `--- ${originalName}`,
    `+++ ${modifiedName}`,
  ];

  for (const hunk of hunks) {
    lines.push(
      `@@ -${hunk.originalStart + 1},${hunk.originalCount} +${hunk.modifiedStart + 1},${hunk.modifiedCount} @@`
    );
    lines.push(...hunk.lines);
  }

  return lines.join('\n');
}

// =============================================================================
// Three-Way Merge Algorithm
// =============================================================================

/**
 * Perform three-way merge of text content
 *
 * @param base - Common ancestor content
 * @param ours - Our version (target branch)
 * @param theirs - Their version (source branch)
 * @param options - Merge options
 * @returns Merge result with content and conflicts
 */
export function threeWayMerge(
  base: string,
  ours: string,
  theirs: string,
  options: MergeOptions = {}
): ThreeWayMergeResult {
  const { strategy, diff3 = false, labels = {} } = options;

  const baseLines = splitLines(base);
  const oursLines = splitLines(ours);
  const theirsLines = splitLines(theirs);

  // Compute diffs from base
  const oursDiff = diff(base, ours);
  const theirsDiff = diff(base, theirs);

  // Build change maps
  const oursChanges = buildChangeMap(oursDiff.ops, baseLines);
  const theirsChanges = buildChangeMap(theirsDiff.ops, baseLines);

  // Merge changes
  const result: string[] = [];
  const conflicts: MergeConflictRegion[] = [];
  let baseIdx = 0;

  while (baseIdx <= baseLines.length) {
    const oursChange = oursChanges.get(baseIdx);
    const theirsChange = theirsChanges.get(baseIdx);

    if (!oursChange && !theirsChange) {
      // No changes at this position - keep base line
      if (baseIdx < baseLines.length) {
        result.push(baseLines[baseIdx]);
      }
      baseIdx++;
    } else if (oursChange && !theirsChange) {
      // Only ours changed
      result.push(...(oursChange.insert || []));
      if (oursChange.delete) {
        baseIdx += oursChange.delete;
      } else {
        baseIdx++;
      }
    } else if (!oursChange && theirsChange) {
      // Only theirs changed
      result.push(...(theirsChange.insert || []));
      if (theirsChange.delete) {
        baseIdx += theirsChange.delete;
      } else {
        baseIdx++;
      }
    } else if (oursChange && theirsChange) {
      // Both changed - check for conflict
      const oursInsert = oursChange.insert || [];
      const theirsInsert = theirsChange.insert || [];
      const oursDelete = oursChange.delete || 0;
      const theirsDelete = theirsChange.delete || 0;

      if (arraysEqual(oursInsert, theirsInsert) && oursDelete === theirsDelete) {
        // Same change - no conflict
        result.push(...oursInsert);
        baseIdx += Math.max(oursDelete, 1);
      } else {
        // Conflict!
        const baseContent = baseLines.slice(baseIdx, baseIdx + Math.max(oursDelete, theirsDelete, 1));

        if (strategy === 'ours') {
          result.push(...oursInsert);
        } else if (strategy === 'theirs') {
          result.push(...theirsInsert);
        } else if (strategy === 'union') {
          // Include both (unique lines only)
          const union = [...new Set([...oursInsert, ...theirsInsert])];
          result.push(...union);
        } else {
          // No auto-resolution - add conflict markers
          const startLine = result.length;
          result.push(`<<<<<<< ${labels.ours || 'ours'}`);
          result.push(...oursInsert);
          if (diff3) {
            result.push(`||||||| ${labels.base || 'base'}`);
            result.push(...baseContent);
          }
          result.push(`=======`);
          result.push(...theirsInsert);
          result.push(`>>>>>>> ${labels.theirs || 'theirs'}`);

          conflicts.push({
            startLine,
            endLine: result.length - 1,
            ours: oursInsert,
            theirs: theirsInsert,
            base: baseContent,
          });
        }

        baseIdx += Math.max(oursDelete, theirsDelete, 1);
      }
    }
  }

  return {
    success: conflicts.length === 0,
    content: result.join('\n'),
    lines: result,
    conflicts,
    stats: {
      baseLines: baseLines.length,
      oursLines: oursLines.length,
      theirsLines: theirsLines.length,
      resultLines: result.length,
      conflictRegions: conflicts.length,
    },
  };
}

/**
 * Resolve conflicts in merged content by choosing a resolution
 */
export function resolveConflicts(
  content: string,
  resolution: 'ours' | 'theirs' | 'base'
): string {
  const lines = splitLines(content);
  const result: string[] = [];
  let inConflict = false;
  let conflictSection: 'ours' | 'base' | 'theirs' | null = null;

  for (const line of lines) {
    if (line.startsWith('<<<<<<<')) {
      inConflict = true;
      conflictSection = 'ours';
      continue;
    }

    if (line.startsWith('|||||||')) {
      conflictSection = 'base';
      continue;
    }

    if (line === '=======') {
      conflictSection = 'theirs';
      continue;
    }

    if (line.startsWith('>>>>>>>')) {
      inConflict = false;
      conflictSection = null;
      continue;
    }

    if (inConflict) {
      if (conflictSection === resolution) {
        result.push(line);
      }
    } else {
      result.push(line);
    }
  }

  return result.join('\n');
}

/**
 * Check if content has unresolved conflicts
 */
export function hasConflictMarkers(content: string): boolean {
  return content.includes('<<<<<<<') &&
         content.includes('=======') &&
         content.includes('>>>>>>>');
}

/**
 * Extract conflict regions from content with markers
 */
export function extractConflicts(content: string): MergeConflictRegion[] {
  const lines = splitLines(content);
  const conflicts: MergeConflictRegion[] = [];

  let inConflict = false;
  let startLine = 0;
  let section: 'ours' | 'base' | 'theirs' | null = null;
  let ours: string[] = [];
  let base: string[] = [];
  let theirs: string[] = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    if (line.startsWith('<<<<<<<')) {
      inConflict = true;
      startLine = i;
      section = 'ours';
      ours = [];
      base = [];
      theirs = [];
      continue;
    }

    if (line.startsWith('|||||||')) {
      section = 'base';
      continue;
    }

    if (line === '=======') {
      section = 'theirs';
      continue;
    }

    if (line.startsWith('>>>>>>>')) {
      conflicts.push({
        startLine,
        endLine: i,
        ours,
        theirs,
        base,
      });
      inConflict = false;
      section = null;
      continue;
    }

    if (inConflict && section) {
      switch (section) {
        case 'ours':
          ours.push(line);
          break;
        case 'base':
          base.push(line);
          break;
        case 'theirs':
          theirs.push(line);
          break;
      }
    }
  }

  return conflicts;
}

// =============================================================================
// Binary File Handling
// =============================================================================

/**
 * Check if content appears to be binary
 */
export function isBinary(data: Uint8Array): boolean {
  // Check for null bytes in first 8KB
  const checkSize = Math.min(data.length, 8192);

  for (let i = 0; i < checkSize; i++) {
    if (data[i] === 0) {
      return true;
    }
  }

  // Check for high ratio of non-printable characters
  let nonPrintable = 0;
  for (let i = 0; i < checkSize; i++) {
    const byte = data[i];
    if (byte < 32 && byte !== 9 && byte !== 10 && byte !== 13) {
      nonPrintable++;
    }
  }

  return nonPrintable / checkSize > 0.3;
}

/**
 * Merge binary files (always conflicts)
 */
export function mergeBinary(
  base: Uint8Array | null,
  ours: Uint8Array,
  theirs: Uint8Array,
  resolution?: 'ours' | 'theirs' | 'base'
): { success: boolean; content: Uint8Array | null; conflict: boolean } {
  // Check if content is identical
  if (arraysEqualUint8(ours, theirs)) {
    return { success: true, content: ours, conflict: false };
  }

  // Binary files can't be auto-merged
  if (resolution === 'ours') {
    return { success: true, content: ours, conflict: false };
  } else if (resolution === 'theirs') {
    return { success: true, content: theirs, conflict: false };
  } else if (resolution === 'base' && base) {
    return { success: true, content: base, conflict: false };
  }

  // Conflict
  return { success: false, content: null, conflict: true };
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Split text into lines (preserving empty lines)
 */
export function splitLines(text: string): string[] {
  if (text === '') return [];
  return text.split(/\r?\n/);
}

/**
 * Compute LCS between two line arrays
 * Returns array of [original index, modified index] pairs
 */
function computeLCS(original: string[], modified: string[]): Array<[number, number]> {
  const m = original.length;
  const n = modified.length;

  // Build DP table
  const dp: number[][] = Array(m + 1)
    .fill(null)
    .map(() => Array(n + 1).fill(0));

  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      if (original[i - 1] === modified[j - 1]) {
        dp[i][j] = dp[i - 1][j - 1] + 1;
      } else {
        dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
      }
    }
  }

  // Backtrack to find LCS
  const result: Array<[number, number]> = [];
  let i = m;
  let j = n;

  while (i > 0 && j > 0) {
    if (original[i - 1] === modified[j - 1]) {
      result.unshift([i - 1, j - 1]);
      i--;
      j--;
    } else if (dp[i - 1][j] > dp[i][j - 1]) {
      i--;
    } else {
      j--;
    }
  }

  return result;
}

/**
 * Coalesce adjacent diff operations of the same type
 */
function coalesceOps(ops: DiffOp[]): DiffOp[] {
  const result: DiffOp[] = [];

  for (const op of ops) {
    const last = result[result.length - 1];

    if (last && last.type === op.type) {
      last.lines.push(...op.lines);
    } else {
      result.push({ ...op });
    }
  }

  return result;
}

/**
 * Generate unified diff hunks
 */
function generateHunks(
  ops: DiffOp[],
  originalLines: string[],
  modifiedLines: string[],
  contextLines: number
): DiffHunk[] {
  const hunks: DiffHunk[] = [];
  let currentHunk: DiffHunk | null = null;
  let lastChangeEnd = -1;

  let origLine = 0;
  let modLine = 0;

  for (const op of ops) {
    const needsNewHunk = !currentHunk ||
      (op.type === 'equal' && origLine - lastChangeEnd > contextLines * 2);

    if (needsNewHunk && currentHunk) {
      // Add trailing context to current hunk
      const contextStart = lastChangeEnd;
      const contextEnd = Math.min(contextStart + contextLines, originalLines.length);
      for (let i = contextStart; i < contextEnd; i++) {
        currentHunk.lines.push(' ' + originalLines[i]);
        currentHunk.originalCount++;
        currentHunk.modifiedCount++;
      }
      hunks.push(currentHunk);
      currentHunk = null;
    }

    if (op.type === 'equal') {
      origLine += op.lines.length;
      modLine += op.lines.length;
    } else {
      if (!currentHunk) {
        // Start new hunk with leading context
        const contextStart = Math.max(0, origLine - contextLines);
        currentHunk = {
          originalStart: contextStart,
          originalCount: 0,
          modifiedStart: Math.max(0, modLine - contextLines),
          modifiedCount: 0,
          lines: [],
        };

        // Add leading context
        for (let i = contextStart; i < origLine; i++) {
          currentHunk.lines.push(' ' + originalLines[i]);
          currentHunk.originalCount++;
          currentHunk.modifiedCount++;
        }
      }

      if (op.type === 'delete') {
        for (const line of op.lines) {
          currentHunk.lines.push('-' + line);
          currentHunk.originalCount++;
        }
        origLine += op.lines.length;
        lastChangeEnd = origLine;
      } else if (op.type === 'insert') {
        for (const line of op.lines) {
          currentHunk.lines.push('+' + line);
          currentHunk.modifiedCount++;
        }
        modLine += op.lines.length;
        lastChangeEnd = origLine;
      }
    }
  }

  // Finalize last hunk
  if (currentHunk) {
    const contextStart = lastChangeEnd;
    const contextEnd = Math.min(contextStart + contextLines, originalLines.length);
    for (let i = contextStart; i < contextEnd; i++) {
      currentHunk.lines.push(' ' + originalLines[i]);
      currentHunk.originalCount++;
      currentHunk.modifiedCount++;
    }
    hunks.push(currentHunk);
  }

  return hunks;
}

/**
 * Build a change map from diff operations
 */
interface Change {
  insert?: string[];
  delete?: number;
}

function buildChangeMap(ops: DiffOp[], baseLines: string[]): Map<number, Change> {
  const changes = new Map<number, Change>();
  let baseIdx = 0;

  for (const op of ops) {
    if (op.type === 'equal') {
      baseIdx += op.lines.length;
    } else if (op.type === 'delete') {
      const existing = changes.get(baseIdx) || {};
      existing.delete = (existing.delete || 0) + op.lines.length;
      changes.set(baseIdx, existing);
      baseIdx += op.lines.length;
    } else if (op.type === 'insert') {
      const existing = changes.get(baseIdx) || {};
      existing.insert = [...(existing.insert || []), ...op.lines];
      changes.set(baseIdx, existing);
    }
  }

  return changes;
}

/**
 * Check if two string arrays are equal
 */
function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

/**
 * Check if two Uint8Arrays are equal
 */
function arraysEqualUint8(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

// =============================================================================
// Merge Strategy Helpers
// =============================================================================

/**
 * Apply merge strategy to resolve all conflicts automatically
 */
export function applyMergeStrategy(
  mergeResult: ThreeWayMergeResult,
  strategy: 'ours' | 'theirs' | 'base' | 'union'
): string {
  if (mergeResult.success) {
    return mergeResult.content;
  }

  return resolveConflicts(mergeResult.content, strategy === 'union' ? 'ours' : strategy);
}

/**
 * Create conflict file with all versions
 */
export function createConflictFile(
  path: string,
  base: string | null,
  ours: string,
  theirs: string,
  labels?: { base?: string; ours?: string; theirs?: string }
): string {
  const result: string[] = [];

  result.push(`# Conflict in ${path}`);
  result.push(`# Resolve by editing and removing conflict markers`);
  result.push('');

  result.push(`<<<<<<< ${labels?.ours || 'ours (current branch)'}`);
  result.push(ours);

  if (base !== null) {
    result.push(`||||||| ${labels?.base || 'base (common ancestor)'}`);
    result.push(base);
  }

  result.push('=======');
  result.push(theirs);
  result.push(`>>>>>>> ${labels?.theirs || 'theirs (incoming branch)'}`);

  return result.join('\n');
}
