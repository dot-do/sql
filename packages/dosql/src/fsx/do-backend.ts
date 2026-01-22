/**
 * Durable Object Storage Backend for FSX
 *
 * Implements FSXBackend using Durable Object storage for hot/recent data.
 * Supports automatic chunking for data larger than 2MB (DO storage limit).
 *
 * Key features:
 * - 2MB max per blob (aligned with DO limits)
 * - Automatic chunking for larger data
 * - Range read support via chunk selection
 * - Metadata tracking for efficient operations
 */

import {
  type FSXBackend,
  type FSXBackendWithMeta,
  type FSXMetadata,
  type ByteRange,
  type ChunkConfig,
  DEFAULT_CHUNK_CONFIG,
  FSXError,
  FSXErrorCode,
} from './types.js';

// =============================================================================
// Durable Object Storage Interface (Cloudflare Workers Types)
// =============================================================================

/**
 * Minimal DurableObjectStorage interface
 * Compatible with Cloudflare Workers DurableObjectStorage binding
 */
export interface DurableObjectStorageLike {
  get<T = unknown>(key: string): Promise<T | undefined>;
  get<T = unknown>(keys: string[]): Promise<Map<string, T>>;
  put(key: string, value: unknown): Promise<void>;
  put(entries: Record<string, unknown>): Promise<void>;
  delete(key: string): Promise<boolean>;
  delete(keys: string[]): Promise<number>;
  list(options?: DOListOptions): Promise<Map<string, unknown>>;
}

export interface DOListOptions {
  prefix?: string;
  start?: string;
  end?: string;
  limit?: number;
  reverse?: boolean;
}

// =============================================================================
// Internal Types
// =============================================================================

/**
 * Metadata stored alongside chunked files
 */
interface ChunkedFileMetadata {
  /** Total size of the file */
  totalSize: number;
  /** Number of chunks */
  chunkCount: number;
  /** Size of each chunk (last chunk may be smaller) */
  chunkSize: number;
  /** Timestamp when file was created/updated */
  createdAt: number;
  /** Original path */
  path: string;
}

/**
 * Internal file entry stored in DO storage
 * For small files: contains data directly
 * For large files: contains metadata pointing to chunks
 */
interface FileEntry {
  /** File data (for non-chunked files) */
  data?: ArrayBuffer;
  /** Metadata for chunked files */
  chunked?: ChunkedFileMetadata;
  /** Creation timestamp */
  createdAt: number;
  /** Last modified timestamp */
  modifiedAt: number;
  /** File size in bytes */
  size: number;
}

// =============================================================================
// DO Backend Implementation
// =============================================================================

/**
 * Durable Object storage backend implementation
 *
 * Storage layout:
 * - `file:{path}` - FileEntry for the file (metadata or inline data)
 * - `_chunks/{path}/{chunkIndex}` - Chunk data for large files
 */
export class DOStorageBackend implements FSXBackendWithMeta {
  private readonly storage: DurableObjectStorageLike;
  private readonly config: ChunkConfig;
  private readonly filePrefix = 'file:';

  constructor(storage: DurableObjectStorageLike, config: Partial<ChunkConfig> = {}) {
    this.storage = storage;
    this.config = { ...DEFAULT_CHUNK_CONFIG, ...config };
  }

  // ===========================================================================
  // FSXBackend Implementation
  // ===========================================================================

  async read(path: string, range?: ByteRange): Promise<Uint8Array | null> {
    const entry = await this.getFileEntry(path);
    if (!entry) return null;

    // Handle non-chunked file
    if (entry.data) {
      const data = new Uint8Array(entry.data);
      if (range) {
        return this.extractRange(data, range);
      }
      return data;
    }

    // Handle chunked file
    if (entry.chunked) {
      return this.readChunkedFile(entry.chunked, range);
    }

    return null;
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    const now = Date.now();

    // Small file: store inline
    if (data.length <= this.config.maxChunkSize) {
      const entry: FileEntry = {
        data: data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer,
        createdAt: now,
        modifiedAt: now,
        size: data.length,
      };

      // Delete any existing chunks if upgrading from chunked to inline
      await this.deleteChunks(path);
      await this.storage.put(this.fileKey(path), entry);
      return;
    }

    // Large file: chunk it
    await this.writeChunkedFile(path, data, now);
  }

  async delete(path: string): Promise<void> {
    const entry = await this.getFileEntry(path);
    if (!entry) return;

    // Delete chunks if chunked file
    if (entry.chunked) {
      await this.deleteChunks(path);
    }

    // Delete the file entry
    await this.storage.delete(this.fileKey(path));
  }

  async list(prefix: string): Promise<string[]> {
    const searchPrefix = this.filePrefix + prefix;
    const entries = await this.storage.list({ prefix: searchPrefix });

    const paths: string[] = [];
    for (const key of entries.keys()) {
      // Extract path from file:path format
      const filePath = key.slice(this.filePrefix.length);
      paths.push(filePath);
    }

    return paths.sort();
  }

  async exists(path: string): Promise<boolean> {
    const entry = await this.storage.get(this.fileKey(path));
    return entry !== undefined;
  }

  // ===========================================================================
  // FSXBackendWithMeta Implementation
  // ===========================================================================

  async metadata(path: string): Promise<FSXMetadata | null> {
    const entry = await this.getFileEntry(path);
    if (!entry) return null;

    return {
      size: entry.size,
      lastModified: new Date(entry.modifiedAt),
    };
  }

  // ===========================================================================
  // Chunk Management
  // ===========================================================================

  private async writeChunkedFile(path: string, data: Uint8Array, timestamp: number): Promise<void> {
    const chunkSize = this.config.maxChunkSize;
    const chunkCount = Math.ceil(data.length / chunkSize);

    // Prepare chunk writes
    const chunkWrites: Record<string, ArrayBuffer> = {};

    for (let i = 0; i < chunkCount; i++) {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, data.length);
      const chunkData = data.slice(start, end);
      const chunkKey = this.chunkKey(path, i);

      // Convert to ArrayBuffer for DO storage
      chunkWrites[chunkKey] = chunkData.buffer.slice(
        chunkData.byteOffset,
        chunkData.byteOffset + chunkData.byteLength
      );
    }

    // Delete any old chunks beyond new chunk count
    await this.deleteChunks(path, chunkCount);

    // Write all chunks in one batch
    await this.storage.put(chunkWrites);

    // Write file entry with metadata
    const metadata: ChunkedFileMetadata = {
      totalSize: data.length,
      chunkCount,
      chunkSize,
      createdAt: timestamp,
      path,
    };

    const entry: FileEntry = {
      chunked: metadata,
      createdAt: timestamp,
      modifiedAt: timestamp,
      size: data.length,
    };

    await this.storage.put(this.fileKey(path), entry);
  }

  private async readChunkedFile(
    metadata: ChunkedFileMetadata,
    range?: ByteRange
  ): Promise<Uint8Array> {
    const { totalSize, chunkCount, chunkSize, path } = metadata;

    // Calculate which chunks we need
    let startByte = 0;
    let endByte = totalSize - 1;

    if (range) {
      [startByte, endByte] = range;
      // Validate range
      if (startByte < 0 || endByte >= totalSize || startByte > endByte) {
        throw new FSXError(
          FSXErrorCode.INVALID_RANGE,
          `Invalid range [${startByte}, ${endByte}] for file of size ${totalSize}`,
          path
        );
      }
    }

    const startChunk = Math.floor(startByte / chunkSize);
    const endChunk = Math.floor(endByte / chunkSize);

    // Fetch required chunks
    const chunkKeys = [];
    for (let i = startChunk; i <= endChunk; i++) {
      chunkKeys.push(this.chunkKey(path, i));
    }

    const chunks = await this.storage.get<ArrayBuffer>(chunkKeys);

    // Assemble result
    const resultSize = endByte - startByte + 1;
    const result = new Uint8Array(resultSize);
    let resultOffset = 0;

    for (let i = startChunk; i <= endChunk; i++) {
      const chunkKey = this.chunkKey(path, i);
      const chunkBuffer = chunks.get(chunkKey);

      if (!chunkBuffer) {
        throw new FSXError(
          FSXErrorCode.CHUNK_CORRUPTED,
          `Missing chunk ${i} for file ${path}`,
          path
        );
      }

      const chunkData = new Uint8Array(chunkBuffer);
      const chunkStart = i * chunkSize;

      // Calculate which part of this chunk we need
      const copyStart = Math.max(0, startByte - chunkStart);
      const copyEnd = Math.min(chunkData.length, endByte - chunkStart + 1);
      const copyLength = copyEnd - copyStart;

      result.set(chunkData.subarray(copyStart, copyEnd), resultOffset);
      resultOffset += copyLength;
    }

    return result;
  }

  private async deleteChunks(path: string, keepFrom?: number): Promise<void> {
    const prefix = `${this.config.chunkPrefix}${path}/`;
    const chunks = await this.storage.list({ prefix });

    const keysToDelete: string[] = [];
    for (const key of chunks.keys()) {
      if (keepFrom !== undefined) {
        // Parse chunk index from key
        const indexStr = key.slice(prefix.length);
        const index = parseInt(indexStr, 10);
        if (!isNaN(index) && index >= keepFrom) {
          keysToDelete.push(key);
        }
      } else {
        keysToDelete.push(key);
      }
    }

    if (keysToDelete.length > 0) {
      await this.storage.delete(keysToDelete);
    }
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  private fileKey(path: string): string {
    return `${this.filePrefix}${path}`;
  }

  private chunkKey(path: string, index: number): string {
    return `${this.config.chunkPrefix}${path}/${index.toString().padStart(6, '0')}`;
  }

  private async getFileEntry(path: string): Promise<FileEntry | null> {
    const entry = await this.storage.get<FileEntry>(this.fileKey(path));
    return entry ?? null;
  }

  private extractRange(data: Uint8Array, range: ByteRange): Uint8Array {
    const [start, end] = range;

    if (start < 0 || end >= data.length || start > end) {
      throw new FSXError(
        FSXErrorCode.INVALID_RANGE,
        `Invalid range [${start}, ${end}] for data of size ${data.length}`
      );
    }

    return data.slice(start, end + 1);
  }

  // ===========================================================================
  // Administrative Methods
  // ===========================================================================

  /**
   * Get storage statistics
   */
  async getStats(): Promise<{
    fileCount: number;
    totalSize: number;
    chunkedFileCount: number;
  }> {
    const entries = await this.storage.list({ prefix: this.filePrefix });

    let fileCount = 0;
    let totalSize = 0;
    let chunkedFileCount = 0;

    for (const value of entries.values()) {
      const entry = value as FileEntry;
      fileCount++;
      totalSize += entry.size;
      if (entry.chunked) {
        chunkedFileCount++;
      }
    }

    return { fileCount, totalSize, chunkedFileCount };
  }

  /**
   * Compact storage by removing orphaned chunks
   */
  async compact(): Promise<{ orphanedChunks: number; bytesReclaimed: number }> {
    // List all file entries to get valid chunk prefixes
    const fileEntries = await this.storage.list({ prefix: this.filePrefix });
    const validChunkPrefixes = new Set<string>();

    for (const value of fileEntries.values()) {
      const entry = value as FileEntry;
      if (entry.chunked) {
        validChunkPrefixes.add(`${this.config.chunkPrefix}${entry.chunked.path}/`);
      }
    }

    // List all chunks
    const allChunks = await this.storage.list({ prefix: this.config.chunkPrefix });

    const orphanedKeys: string[] = [];
    let bytesReclaimed = 0;

    for (const [key, value] of allChunks.entries()) {
      const isValid = [...validChunkPrefixes].some((prefix) => key.startsWith(prefix));
      if (!isValid) {
        orphanedKeys.push(key);
        if (value instanceof ArrayBuffer) {
          bytesReclaimed += value.byteLength;
        }
      }
    }

    if (orphanedKeys.length > 0) {
      await this.storage.delete(orphanedKeys);
    }

    return { orphanedChunks: orphanedKeys.length, bytesReclaimed };
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a DO storage backend
 * @param storage - DurableObjectStorage binding from Cloudflare Workers
 * @param config - Optional chunk configuration
 */
export function createDOBackend(
  storage: DurableObjectStorageLike,
  config?: Partial<ChunkConfig>
): DOStorageBackend {
  return new DOStorageBackend(storage, config);
}
