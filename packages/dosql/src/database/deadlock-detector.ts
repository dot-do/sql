/**
 * Deadlock Detection for DoSQL
 *
 * Implements comprehensive deadlock detection with:
 * - Wait-for graph construction and cycle detection
 * - Multiple victim selection policies
 * - Deadlock prevention schemes
 * - Statistics and history tracking
 *
 * @packageDocumentation
 */

import {
  TransactionError,
  TransactionErrorCode,
  type LockType,
} from '../transaction/types.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Victim selection policy for deadlock resolution
 */
export type VictimSelectionPolicy =
  | 'youngest' // Select the youngest transaction (by timestamp)
  | 'leastWork' // Select the transaction with least work done
  | 'roundRobin' // Rotate victims to prevent starvation
  | 'preferReadOnly' // Prefer read-only transactions as victims
  | 'priority'; // Select based on transaction priority

/**
 * Deadlock prevention scheme
 */
export type DeadlockPreventionScheme =
  | 'waitDie' // Older waits, younger dies (aborts)
  | 'woundWait' // Older wounds (forces abort) younger
  | 'lockOrdering' // Enforce lock acquisition order
  | 'noWait'; // Never wait, fail immediately

/**
 * Wait reason in the graph
 */
export type WaitReason =
  | 'exclusive' // Waiting for exclusive lock
  | 'shared' // Waiting for shared lock
  | 'lockUpgrade'; // Waiting to upgrade from shared to exclusive

/**
 * Edge in the wait-for graph
 */
export interface WaitForEdge {
  /** Transaction waiting */
  from: string;
  /** Transaction being waited on */
  to: string;
  /** Resource being waited for */
  resource: string;
  /** Reason for waiting */
  reason: WaitReason;
  /** When the wait started */
  startTime: number;
  /** Lock type being requested */
  requestedLockType: LockType;
}

/**
 * Deadlock event information
 */
export interface DeadlockInfo {
  /** Cycle of transaction IDs */
  cycle: string[];
  /** Resources involved */
  resources: string[];
  /** Selected victim transaction ID */
  victimTxnId: string;
  /** Wait times for each participant */
  waitTimes: Record<string, number>;
  /** Timestamp when deadlock was detected */
  timestamp: number;
  /** DOT graph visualization */
  graphDot: string;
  /** All participants in the cycle */
  participants: string[];
  /** Transactions that exceeded the deadlock timeout */
  exceededTimeout?: string[];
}

/**
 * Deadlock statistics
 */
export interface DeadlockStats {
  /** Total deadlocks detected */
  totalDeadlocks: number;
  /** Average cycle length */
  avgCycleLength: number;
  /** Total victims aborted by policy */
  victimsAborted: Record<VictimSelectionPolicy, number>;
  /** Prevention scheme activations */
  preventionActivations: number;
  /** Total upgrade deadlocks (deadlocks caused by lock upgrades) */
  upgradeDeadlocks: number;
}

/**
 * Transaction metadata for victim selection
 */
export interface TransactionMeta {
  /** Transaction start timestamp */
  startTime: number;
  /** Transaction cost (work done) */
  cost: number;
  /** Whether transaction is read-only */
  readOnly: boolean;
  /** Number of times selected as victim */
  victimCount: number;
  /** Transaction priority (higher = less likely to be victim) */
  priority: number;
}

/**
 * Auto retry configuration
 */
export interface AutoRetryConfig {
  /** Whether auto-retry is enabled */
  enabled: boolean;
  /** Maximum number of retries */
  maxRetries: number;
  /** Base backoff time in milliseconds */
  baseBackoffMs: number;
}

/**
 * Throttle configuration
 */
export interface ThrottleConfig {
  /** Maximum detections per second */
  maxDetectionsPerSecond: number;
  /** Cooldown time in milliseconds */
  cooldownMs: number;
}

/**
 * Throttle state
 */
export interface ThrottleState {
  /** Whether detection is currently throttled */
  isThrottled: boolean;
  /** Number of detections in current window */
  detectionsInWindow: number;
  /** Time when throttle will lift */
  throttleUntil?: number;
}

/**
 * Deadlock prediction result
 */
export interface DeadlockPrediction {
  /** Whether adding this wait would cause a deadlock */
  wouldCauseDeadlock: boolean;
  /** Predicted cycle if deadlock would occur */
  predictedCycle?: string[];
  /** Probability of deadlock (0-1) */
  probability?: number;
}

/**
 * Enhanced deadlock error with additional context
 */
export class DeadlockError extends TransactionError {
  public readonly victimTxnId: string;
  public readonly cycle: string[];
  public readonly resources: string[];
  public readonly waitTimes: Record<string, number>;
  public readonly graphDot: string;
  public readonly participants: string[];

  constructor(info: DeadlockInfo, txnId: string) {
    super(
      TransactionErrorCode.DEADLOCK,
      `Deadlock detected: cycle ${info.cycle.join(' -> ')}`,
      txnId
    );
    this.victimTxnId = info.victimTxnId;
    this.cycle = info.cycle;
    this.resources = info.resources;
    this.waitTimes = info.waitTimes;
    this.graphDot = info.graphDot;
    this.participants = info.participants;
    this.name = 'DeadlockError';
  }
}

// =============================================================================
// Wait-For Graph
// =============================================================================

/**
 * Wait-for graph for deadlock detection
 */
export class WaitForGraph {
  /** Adjacency list: from txn -> edges */
  private edges: Map<string, WaitForEdge[]> = new Map();
  /** Transaction metadata */
  private txnMeta: Map<string, TransactionMeta> = new Map();
  /** Round-robin counter for victim selection */
  private roundRobinCounter: number = 0;

  /**
   * Add an edge to the graph
   */
  addEdge(edge: Omit<WaitForEdge, 'startTime'>): void {
    const fullEdge: WaitForEdge = {
      ...edge,
      startTime: Date.now(),
    };

    let fromEdges = this.edges.get(edge.from);
    if (!fromEdges) {
      fromEdges = [];
      this.edges.set(edge.from, fromEdges);
    }

    // Don't add duplicate edges
    const exists = fromEdges.some(
      (e) => e.to === edge.to && e.resource === edge.resource
    );
    if (!exists) {
      fromEdges.push(fullEdge);
    }
  }

  /**
   * Remove an edge from the graph
   */
  removeEdge(from: string, to: string, resource?: string): void {
    const edges = this.edges.get(from);
    if (!edges) return;

    const filtered = edges.filter(
      (e) =>
        e.to !== to || (resource !== undefined && e.resource !== resource)
    );

    if (filtered.length === 0) {
      this.edges.delete(from);
    } else {
      this.edges.set(from, filtered);
    }
  }

  /**
   * Remove all edges for a transaction (both as source and target)
   */
  removeTransaction(txnId: string): void {
    // Remove outgoing edges
    this.edges.delete(txnId);

    // Remove incoming edges
    for (const [from, edges] of this.edges) {
      const filtered = edges.filter((e) => e.to !== txnId);
      if (filtered.length === 0) {
        this.edges.delete(from);
      } else {
        this.edges.set(from, filtered);
      }
    }

    // Remove metadata
    this.txnMeta.delete(txnId);
  }

  /**
   * Check if an edge exists
   */
  hasEdge(from: string, to: string): boolean {
    const edges = this.edges.get(from);
    return edges ? edges.some((e) => e.to === to) : false;
  }

  /**
   * Get the wait reason between two transactions
   */
  getWaitReason(from: string, to: string): WaitReason | undefined {
    const edges = this.edges.get(from);
    if (!edges) return undefined;
    const edge = edges.find((e) => e.to === to);
    return edge?.reason;
  }

  /**
   * Get total edge count
   */
  getEdgeCount(): number {
    let count = 0;
    for (const edges of this.edges.values()) {
      count += edges.length;
    }
    return count;
  }

  /**
   * Check if the graph has any cycle
   */
  hasCycle(): boolean {
    const cycle = this.findCycle();
    return cycle !== null;
  }

  /**
   * Find a cycle in the graph using DFS
   * Returns the cycle as an array of transaction IDs, or null if no cycle
   */
  findCycle(): string[] | null {
    const visited = new Set<string>();
    const recStack = new Set<string>();
    const parent = new Map<string, string>();

    // Get all nodes
    const nodes = new Set<string>();
    for (const from of this.edges.keys()) {
      nodes.add(from);
    }
    for (const edges of this.edges.values()) {
      for (const edge of edges) {
        nodes.add(edge.to);
      }
    }

    for (const node of nodes) {
      if (!visited.has(node)) {
        const cycle = this.dfsCycle(node, visited, recStack, parent);
        if (cycle) return cycle;
      }
    }

    return null;
  }

  private dfsCycle(
    node: string,
    visited: Set<string>,
    recStack: Set<string>,
    parent: Map<string, string>
  ): string[] | null {
    visited.add(node);
    recStack.add(node);

    const edges = this.edges.get(node) || [];
    for (const edge of edges) {
      const neighbor = edge.to;

      if (!visited.has(neighbor)) {
        parent.set(neighbor, node);
        const cycle = this.dfsCycle(neighbor, visited, recStack, parent);
        if (cycle) return cycle;
      } else if (recStack.has(neighbor)) {
        // Found cycle - reconstruct path
        const cyclePath: string[] = [neighbor];
        let current = node;
        while (current !== neighbor) {
          cyclePath.unshift(current);
          current = parent.get(current)!;
          if (!current) break;
        }
        cyclePath.push(neighbor); // Close the cycle
        return cyclePath;
      }
    }

    recStack.delete(node);
    return null;
  }

  /**
   * Find cycle starting from a specific transaction
   */
  findCycleFrom(startTxn: string): string[] | null {
    const visited = new Set<string>();
    const path: string[] = [];

    const dfs = (current: string): string[] | null => {
      if (visited.has(current)) {
        // Found cycle - extract the cycle portion
        const cycleStart = path.indexOf(current);
        if (cycleStart !== -1) {
          const cycle = path.slice(cycleStart);
          cycle.push(current); // Close the cycle
          return cycle;
        }
        return null;
      }

      visited.add(current);
      path.push(current);

      const edges = this.edges.get(current) || [];
      for (const edge of edges) {
        const result = dfs(edge.to);
        if (result) return result;
      }

      path.pop();
      return null;
    };

    return dfs(startTxn);
  }

  /**
   * Get resources involved in a cycle
   * Fixed to collect ALL resources by traversing edges in both directions around the cycle
   */
  getResourcesInCycle(cycle: string[]): string[] {
    const resources = new Set<string>();
    const participants = new Set(cycle);

    // Traverse edges between all participants to capture all resources
    for (let i = 0; i < cycle.length - 1; i++) {
      const from = cycle[i];
      const to = cycle[i + 1];

      // Get resources from from -> to edge
      const fromEdges = this.edges.get(from) || [];
      for (const edge of fromEdges) {
        if (edge.to === to) {
          resources.add(edge.resource);
        }
      }

      // Also check reverse direction (to -> from) for complete cycle coverage
      const toEdges = this.edges.get(to) || [];
      for (const edge of toEdges) {
        if (edge.to === from) {
          resources.add(edge.resource);
        }
      }
    }

    return Array.from(resources);
  }

  /**
   * Get wait times for transactions in a cycle
   */
  getWaitTimesInCycle(cycle: string[]): Record<string, number> {
    const now = Date.now();
    const waitTimes: Record<string, number> = {};

    for (let i = 0; i < cycle.length - 1; i++) {
      const from = cycle[i];
      const edges = this.edges.get(from) || [];
      let minStartTime = now;
      for (const edge of edges) {
        if (edge.startTime < minStartTime) {
          minStartTime = edge.startTime;
        }
      }
      waitTimes[from] = now - minStartTime;
    }

    return waitTimes;
  }

  /**
   * Get all edges from the graph
   */
  getAllEdges(): WaitForEdge[] {
    const allEdges: WaitForEdge[] = [];
    for (const edges of this.edges.values()) {
      allEdges.push(...edges);
    }
    return allEdges;
  }

  /**
   * Generate DOT graph visualization
   */
  toDot(): string {
    const lines: string[] = ['digraph WaitFor {'];
    lines.push('  node [shape=box];');

    for (const [from, edges] of this.edges) {
      for (const edge of edges) {
        const label = `${edge.resource}\\n(${edge.reason})`;
        lines.push(`  "${from}" -> "${edge.to}" [label="${label}"];`);
      }
    }

    lines.push('}');
    return lines.join('\n');
  }

  /**
   * Set transaction metadata
   */
  setTransactionMeta(txnId: string, meta: Partial<TransactionMeta>): void {
    const existing = this.txnMeta.get(txnId) || {
      startTime: Date.now(),
      cost: 0,
      readOnly: false,
      victimCount: 0,
      priority: 0,
    };
    this.txnMeta.set(txnId, { ...existing, ...meta });
  }

  /**
   * Get transaction metadata
   */
  getTransactionMeta(txnId: string): TransactionMeta | undefined {
    return this.txnMeta.get(txnId);
  }

  /**
   * Select a victim from a cycle based on policy
   */
  selectVictim(cycle: string[], policy: VictimSelectionPolicy): string {
    // Remove closing node from cycle for victim selection
    const participants = cycle.slice(0, -1);

    switch (policy) {
      case 'youngest':
        return this.selectYoungestVictim(participants);
      case 'leastWork':
        return this.selectLeastWorkVictim(participants);
      case 'roundRobin':
        return this.selectRoundRobinVictim(participants);
      case 'preferReadOnly':
        return this.selectPreferReadOnlyVictim(participants);
      case 'priority':
        return this.selectPriorityVictim(participants);
      default:
        return participants[participants.length - 1]; // Default: last in cycle
    }
  }

  private selectYoungestVictim(participants: string[]): string {
    let youngest = participants[0];
    let youngestTime = 0;

    for (const txnId of participants) {
      const meta = this.txnMeta.get(txnId);
      const startTime = meta?.startTime || 0;
      if (startTime > youngestTime) {
        youngestTime = startTime;
        youngest = txnId;
      }
    }

    return youngest;
  }

  private selectLeastWorkVictim(participants: string[]): string {
    let leastWork = participants[0];
    let minCost = Infinity;

    for (const txnId of participants) {
      const meta = this.txnMeta.get(txnId);
      const cost = meta?.cost ?? 0;
      if (cost < minCost) {
        minCost = cost;
        leastWork = txnId;
      }
    }

    return leastWork;
  }

  private selectRoundRobinVictim(participants: string[]): string {
    const index = this.roundRobinCounter % participants.length;
    this.roundRobinCounter++;
    return participants[index];
  }

  private selectPreferReadOnlyVictim(participants: string[]): string {
    // Prefer read-only transactions as victims
    for (const txnId of participants) {
      const meta = this.txnMeta.get(txnId);
      if (meta?.readOnly) {
        return txnId;
      }
    }
    // Fallback to youngest
    return this.selectYoungestVictim(participants);
  }

  private selectPriorityVictim(participants: string[]): string {
    // Select lowest priority transaction as victim
    let lowestPriority = participants[0];
    let minPriority = Infinity;

    for (const txnId of participants) {
      const meta = this.txnMeta.get(txnId);
      const priority = meta?.priority ?? 0;
      if (priority < minPriority) {
        minPriority = priority;
        lowestPriority = txnId;
      }
    }

    return lowestPriority;
  }

  /**
   * Clear all edges and metadata
   */
  clear(): void {
    this.edges.clear();
    this.txnMeta.clear();
  }
}

// =============================================================================
// Deadlock Detector
// =============================================================================

/**
 * Options for the deadlock detector
 */
export interface DeadlockDetectorOptions {
  /** Enable deadlock detection */
  enabled?: boolean;
  /** Victim selection policy */
  victimSelection?: VictimSelectionPolicy;
  /** Deadlock prevention scheme (if set, detection is disabled) */
  deadlockPrevention?: DeadlockPreventionScheme;
  /** Detection interval in ms (for periodic detection) */
  deadlockDetectionInterval?: number;
  /** Timeout for deadlock detection */
  deadlockTimeout?: number;
  /** Callback when deadlock is detected */
  onDeadlock?: (info: DeadlockInfo) => void;
  /** Maximum history entries to keep */
  maxHistorySize?: number;
  /** Auto-retry configuration */
  autoRetry?: AutoRetryConfig;
  /** Throttle configuration */
  throttle?: ThrottleConfig;
}

/**
 * Deadlock detector for transaction lock management
 */
export class DeadlockDetector {
  private graph: WaitForGraph;
  private options: Required<Omit<DeadlockDetectorOptions, 'autoRetry' | 'throttle'>> & {
    autoRetry?: AutoRetryConfig;
    throttle?: ThrottleConfig;
  };
  private stats: DeadlockStats;
  private history: DeadlockInfo[];
  private persistentStats: DeadlockStats;
  private persistentHistory: DeadlockInfo[];
  private txnStartTimes: Map<string, number>;
  private lockOrdering: Map<string, number>; // For lock ordering prevention
  private txnLockOrders: Map<string, number> = new Map();
  private txnPriorities: Map<string, number> = new Map();
  private throttleState: ThrottleState;
  private lastDetectionTime: number = 0;
  private detectionsInCurrentSecond: number = 0;

  constructor(options: DeadlockDetectorOptions = {}) {
    this.graph = new WaitForGraph();
    this.options = {
      enabled: options.enabled ?? true,
      victimSelection: options.victimSelection ?? 'youngest',
      deadlockPrevention: options.deadlockPrevention as DeadlockPreventionScheme,
      deadlockDetectionInterval: options.deadlockDetectionInterval ?? 100,
      deadlockTimeout: options.deadlockTimeout ?? 5000,
      onDeadlock: options.onDeadlock ?? (() => {}),
      maxHistorySize: options.maxHistorySize ?? 100,
      autoRetry: options.autoRetry,
      throttle: options.throttle,
    };
    this.stats = this.createEmptyStats();
    this.persistentStats = this.createEmptyStats();
    this.history = [];
    this.persistentHistory = [];
    this.txnStartTimes = new Map();
    this.lockOrdering = new Map();
    this.throttleState = {
      isThrottled: false,
      detectionsInWindow: 0,
    };
  }

  private createEmptyStats(): DeadlockStats {
    return {
      totalDeadlocks: 0,
      avgCycleLength: 0,
      victimsAborted: {
        youngest: 0,
        leastWork: 0,
        roundRobin: 0,
        preferReadOnly: 0,
        priority: 0,
      },
      preventionActivations: 0,
      upgradeDeadlocks: 0,
    };
  }

  /**
   * Register a transaction start
   * Only sets start time if not already registered (preserves original timestamp)
   */
  registerTransaction(txnId: string): void {
    // Only set start time if not already registered
    if (!this.txnStartTimes.has(txnId)) {
      const now = Date.now();
      this.txnStartTimes.set(txnId, now);
      this.graph.setTransactionMeta(txnId, {
        startTime: now,
        cost: 0,
        readOnly: false,
        victimCount: 0,
        priority: 0,
      });
    }
  }

  /**
   * Set transaction cost (for leastWork policy)
   */
  setTransactionCost(txnId: string, cost: number): void {
    this.graph.setTransactionMeta(txnId, { cost });
  }

  /**
   * Mark transaction as read-only
   */
  markReadOnly(txnId: string, readOnly: boolean): void {
    this.graph.setTransactionMeta(txnId, { readOnly });
  }

  /**
   * Set transaction priority (for priority victim selection)
   */
  setTransactionPriority(txnId: string, priority: number): void {
    this.txnPriorities.set(txnId, priority);
    this.graph.setTransactionMeta(txnId, { priority });
  }

  /**
   * Add a wait edge (txn waiting for resource held by holder)
   */
  addWait(
    waitingTxn: string,
    holderTxn: string,
    resource: string,
    lockType: LockType,
    reason: WaitReason = 'exclusive'
  ): void {
    if (!this.txnStartTimes.has(waitingTxn)) {
      this.registerTransaction(waitingTxn);
    }
    if (!this.txnStartTimes.has(holderTxn)) {
      this.registerTransaction(holderTxn);
    }

    this.graph.addEdge({
      from: waitingTxn,
      to: holderTxn,
      resource,
      reason,
      requestedLockType: lockType,
    });
  }

  /**
   * Remove wait edge when lock is granted or transaction releases
   */
  removeWait(waitingTxn: string, holderTxn?: string, resource?: string): void {
    if (holderTxn) {
      this.graph.removeEdge(waitingTxn, holderTxn, resource);
    } else {
      // Remove all waits for this transaction
      this.graph.removeTransaction(waitingTxn);
    }
  }

  /**
   * Unregister a transaction (cleanup)
   */
  unregisterTransaction(txnId: string): void {
    this.graph.removeTransaction(txnId);
    this.txnStartTimes.delete(txnId);
  }

  /**
   * Check for deadlock when adding a new wait
   * Returns DeadlockInfo if deadlock detected, null otherwise
   */
  checkDeadlock(waitingTxn: string): DeadlockInfo | null {
    if (!this.options.enabled) {
      return null;
    }

    // Check throttle
    if (this.isThrottled()) {
      return null;
    }

    const cycle = this.graph.findCycleFrom(waitingTxn);
    if (!cycle || cycle.length < 2) {
      return null;
    }

    // Deadlock detected - create info
    const resources = this.graph.getResourcesInCycle(cycle);
    const waitTimes = this.graph.getWaitTimesInCycle(cycle);
    const victimTxnId = this.graph.selectVictim(cycle, this.options.victimSelection);
    const graphDot = this.graph.toDot();
    const participants = cycle.slice(0, -1);

    // Check for exceeded timeout
    const exceededTimeout: string[] = [];
    const timeout = this.options.deadlockTimeout;
    for (const txnId of participants) {
      if (waitTimes[txnId] && waitTimes[txnId] > timeout) {
        exceededTimeout.push(txnId);
      }
    }

    const info: DeadlockInfo = {
      cycle,
      resources,
      victimTxnId,
      waitTimes,
      timestamp: Date.now(),
      graphDot,
      participants,
      exceededTimeout: exceededTimeout.length > 0 ? exceededTimeout : undefined,
    };

    // Check if this is an upgrade deadlock
    const isUpgradeDeadlock = this.checkIfUpgradeDeadlock(cycle);

    // Update statistics
    this.updateStats(info, isUpgradeDeadlock);

    // Add to history
    this.addToHistory(info);

    // Update throttle state
    this.updateThrottleState();

    // Invoke callback
    this.options.onDeadlock(info);

    return info;
  }

  /**
   * Async version of checkDeadlock with timeout support
   */
  async checkDeadlockAsync(
    waitingTxn: string,
    options?: { timeout?: number }
  ): Promise<DeadlockInfo | null> {
    const timeout = options?.timeout ?? this.options.deadlockTimeout;

    return new Promise((resolve) => {
      const timeoutId = setTimeout(() => {
        resolve(null);
      }, timeout);

      try {
        const result = this.checkDeadlock(waitingTxn);
        clearTimeout(timeoutId);
        resolve(result);
      } catch (error) {
        clearTimeout(timeoutId);
        resolve(null);
      }
    });
  }

  private checkIfUpgradeDeadlock(cycle: string[]): boolean {
    for (let i = 0; i < cycle.length - 1; i++) {
      const from = cycle[i];
      const reason = this.graph.getWaitReason(from, cycle[i + 1]);
      if (reason === 'lockUpgrade') {
        return true;
      }
    }
    return false;
  }

  private isThrottled(): boolean {
    if (!this.options.throttle) {
      return false;
    }

    const now = Date.now();

    // Check if we're in a cooldown period
    if (this.throttleState.throttleUntil && now < this.throttleState.throttleUntil) {
      return true;
    }

    // Reset counter if we're in a new second
    if (now - this.lastDetectionTime >= 1000) {
      this.detectionsInCurrentSecond = 0;
      this.lastDetectionTime = now;
    }

    // Check if we've exceeded max detections
    if (this.detectionsInCurrentSecond >= this.options.throttle.maxDetectionsPerSecond) {
      this.throttleState.isThrottled = true;
      this.throttleState.throttleUntil = now + this.options.throttle.cooldownMs;
      return true;
    }

    return false;
  }

  private updateThrottleState(): void {
    this.detectionsInCurrentSecond++;
    this.throttleState.detectionsInWindow = this.detectionsInCurrentSecond;
  }

  /**
   * Predict if adding a wait would cause a deadlock
   */
  predictDeadlock(
    waitingTxn: string,
    holderTxn: string,
    resource: string
  ): DeadlockPrediction {
    // Temporarily add the edge to check for cycle
    this.graph.addEdge({
      from: waitingTxn,
      to: holderTxn,
      resource,
      reason: 'exclusive',
      requestedLockType: 'EXCLUSIVE' as LockType,
    });

    const cycle = this.graph.findCycleFrom(waitingTxn);

    // Remove the temporary edge
    this.graph.removeEdge(waitingTxn, holderTxn, resource);

    if (cycle && cycle.length >= 2) {
      return {
        wouldCauseDeadlock: true,
        predictedCycle: cycle,
        probability: 1.0,
      };
    }

    return {
      wouldCauseDeadlock: false,
      probability: this.calculateDeadlockProbability(waitingTxn),
    };
  }

  /**
   * Calculate the probability of deadlock for a transaction
   */
  getDeadlockProbability(txnId: string): number {
    return this.calculateDeadlockProbability(txnId);
  }

  private calculateDeadlockProbability(txnId: string): number {
    // Simple heuristic based on graph structure
    const allEdges = this.graph.getAllEdges();
    if (allEdges.length === 0) {
      return 0;
    }

    // Count paths that could lead to deadlock
    let dangerouspaths = 0;
    const visited = new Set<string>();

    const countPaths = (current: string, depth: number): number => {
      if (depth > 10) return 0; // Limit depth
      if (visited.has(current)) return 1; // Potential cycle

      visited.add(current);
      let paths = 0;

      for (const edge of allEdges) {
        if (edge.from === current) {
          paths += countPaths(edge.to, depth + 1);
        }
      }

      visited.delete(current);
      return paths;
    };

    dangerouspaths = countPaths(txnId, 0);

    // Normalize to 0-1 range
    const maxPaths = allEdges.length * 2;
    return Math.min(1, dangerouspaths / maxPaths);
  }

  /**
   * Check for deadlock prevention violation
   * Returns error if prevention scheme would be violated
   */
  checkPrevention(
    waitingTxn: string,
    holderTxn: string,
    resource: string
  ): TransactionError | null {
    if (!this.options.deadlockPrevention) {
      return null;
    }

    const waitingStart = this.txnStartTimes.get(waitingTxn) || Date.now();
    const holderStart = this.txnStartTimes.get(holderTxn) || Date.now();
    const waitingIsOlder = waitingStart < holderStart;

    switch (this.options.deadlockPrevention) {
      case 'waitDie':
        // Older can wait for younger, younger must die
        if (!waitingIsOlder) {
          this.stats.preventionActivations++;
          this.persistentStats.preventionActivations++;
          return new TransactionError(
            TransactionErrorCode.ABORTED,
            `Wait-die: younger transaction ${waitingTxn} must abort when waiting for older ${holderTxn}`,
            waitingTxn
          );
        }
        break;

      case 'woundWait':
        // Older wounds (aborts) younger that holds lock
        // This is handled differently - older proceeds, younger is aborted
        if (waitingIsOlder) {
          this.stats.preventionActivations++;
          this.persistentStats.preventionActivations++;
          // Return special error to indicate holder should be aborted
          const error = new TransactionError(
            TransactionErrorCode.ABORTED,
            `Wound-wait: older transaction ${waitingTxn} wounds younger holder ${holderTxn}`,
            holderTxn
          );
          (error as any).woundTarget = holderTxn;
          return error;
        }
        break;

      case 'lockOrdering':
        // Check if acquiring out of order
        const lastOrder = this.getLastLockOrder(waitingTxn);
        const resourceOrder = this.getLockOrder(resource);
        if (lastOrder !== undefined && resourceOrder < lastOrder) {
          this.stats.preventionActivations++;
          this.persistentStats.preventionActivations++;
          return new TransactionError(
            TransactionErrorCode.LOCK_FAILED,
            `Lock ordering violation: ${waitingTxn} cannot acquire ${resource} (order ${resourceOrder}) after higher-ordered lock (${lastOrder})`,
            waitingTxn
          );
        }
        break;

      case 'noWait':
        // Never wait - immediate failure
        this.stats.preventionActivations++;
        this.persistentStats.preventionActivations++;
        return new TransactionError(
          TransactionErrorCode.LOCK_FAILED,
          `No-wait: ${waitingTxn} cannot wait for ${holderTxn}`,
          waitingTxn
        );
    }

    return null;
  }

  /**
   * Record lock acquisition for lock ordering
   */
  recordLockAcquisition(txnId: string, resource: string): void {
    if (this.options.deadlockPrevention === 'lockOrdering') {
      const order = this.getLockOrder(resource);
      const existing = this.txnLockOrders.get(txnId);
      if (!existing || order > existing) {
        this.txnLockOrders.set(txnId, order);
      }
    }
  }

  private getLockOrder(resource: string): number {
    let order = this.lockOrdering.get(resource);
    if (order === undefined) {
      // Assign order based on resource name (alphabetical)
      order = Array.from(resource).reduce((acc, char) => acc + char.charCodeAt(0), 0);
      this.lockOrdering.set(resource, order);
    }
    return order;
  }

  private getLastLockOrder(txnId: string): number | undefined {
    return this.txnLockOrders.get(txnId);
  }

  /**
   * Get the wait-for graph
   */
  getWaitForGraph(): WaitForGraph {
    return this.graph;
  }

  /**
   * Get deadlock statistics
   */
  getDeadlockStats(): DeadlockStats {
    return { ...this.stats };
  }

  /**
   * Get deadlock history
   */
  getDeadlockHistory(): DeadlockInfo[] {
    return [...this.history];
  }

  /**
   * Get persistent deadlock statistics (not reset by clear())
   */
  getPersistentStats(): DeadlockStats {
    return { ...this.persistentStats };
  }

  /**
   * Get persistent deadlock history (not reset by clear())
   */
  getPersistentHistory(): DeadlockInfo[] {
    return [...this.persistentHistory];
  }

  /**
   * Get auto-retry configuration
   */
  getRetryConfig(): AutoRetryConfig | undefined {
    return this.options.autoRetry;
  }

  /**
   * Get throttle state
   */
  getThrottleState(): ThrottleState {
    return { ...this.throttleState };
  }

  /**
   * Export deadlock report as JSON string
   */
  exportReport(): string {
    const report = {
      totalDeadlocks: this.persistentStats.totalDeadlocks,
      avgCycleLength: this.persistentStats.avgCycleLength,
      victimsAborted: this.persistentStats.victimsAborted,
      preventionActivations: this.persistentStats.preventionActivations,
      upgradeDeadlocks: this.persistentStats.upgradeDeadlocks,
      history: this.persistentHistory,
      exportedAt: new Date().toISOString(),
    };
    return JSON.stringify(report, null, 2);
  }

  private updateStats(info: DeadlockInfo, isUpgradeDeadlock: boolean): void {
    // Update current stats
    this.stats.totalDeadlocks++;
    const cycleLength = info.cycle.length - 1;
    this.stats.avgCycleLength =
      (this.stats.avgCycleLength * (this.stats.totalDeadlocks - 1) + cycleLength) /
      this.stats.totalDeadlocks;
    this.stats.victimsAborted[this.options.victimSelection]++;
    if (isUpgradeDeadlock) {
      this.stats.upgradeDeadlocks++;
    }

    // Update persistent stats
    this.persistentStats.totalDeadlocks++;
    this.persistentStats.avgCycleLength =
      (this.persistentStats.avgCycleLength * (this.persistentStats.totalDeadlocks - 1) + cycleLength) /
      this.persistentStats.totalDeadlocks;
    this.persistentStats.victimsAborted[this.options.victimSelection]++;
    if (isUpgradeDeadlock) {
      this.persistentStats.upgradeDeadlocks++;
    }
  }

  private addToHistory(info: DeadlockInfo): void {
    // Add to current history
    this.history.push(info);
    if (this.history.length > this.options.maxHistorySize) {
      this.history.shift();
    }

    // Add to persistent history
    this.persistentHistory.push(info);
    if (this.persistentHistory.length > this.options.maxHistorySize) {
      this.persistentHistory.shift();
    }
  }

  /**
   * Clear all state including stats and history
   */
  clear(): void {
    this.graph.clear();
    this.txnStartTimes.clear();
    this.txnLockOrders.clear();
    this.txnPriorities.clear();
    this.history = [];
    this.stats = this.createEmptyStats();
  }

  /**
   * Clear only the graph state, preserving stats and history
   */
  clearGraph(): void {
    this.graph.clear();
    this.txnStartTimes.clear();
    this.txnLockOrders.clear();
    this.txnPriorities.clear();
  }
}

/**
 * Create a deadlock detector
 */
export function createDeadlockDetector(
  options: DeadlockDetectorOptions = {}
): DeadlockDetector {
  return new DeadlockDetector(options);
}
