/**
 * View Registry for DoSQL
 *
 * Manages view definitions including:
 * - Registration and storage
 * - Dependency tracking
 * - INSTEAD OF trigger management
 * - Temporary view lifecycle
 */

import type {
  ViewDefinition,
  ParsedView,
  ViewRegistry,
  RegisterViewOptions,
  ListViewsOptions,
  InsteadOfTrigger,
  ViewDMLOperation,
  ViewType,
  DependencyAnalysis,
  ViewDependencyNode,
} from './types.js';
import { ViewError, ViewErrorCode } from './types.js';
import { extractReferences, validateViewDefinition } from './parser.js';

// =============================================================================
// In-Memory View Storage
// =============================================================================

/**
 * Storage interface for views
 */
export interface ViewStorage {
  /** Get a view by fully qualified name */
  get(name: string): ViewDefinition | undefined;

  /** Set a view */
  set(name: string, view: ViewDefinition): void;

  /** Delete a view */
  delete(name: string): boolean;

  /** Check if a view exists */
  has(name: string): boolean;

  /** Get all views */
  getAll(): ViewDefinition[];

  /** Clear all views */
  clear(): void;
}

/**
 * Create an in-memory view storage
 */
export function createInMemoryViewStorage(): ViewStorage {
  const views = new Map<string, ViewDefinition>();

  return {
    get(name: string): ViewDefinition | undefined {
      return views.get(name.toLowerCase());
    },

    set(name: string, view: ViewDefinition): void {
      views.set(name.toLowerCase(), view);
    },

    delete(name: string): boolean {
      return views.delete(name.toLowerCase());
    },

    has(name: string): boolean {
      return views.has(name.toLowerCase());
    },

    getAll(): ViewDefinition[] {
      return Array.from(views.values());
    },

    clear(): void {
      views.clear();
    },
  };
}

// =============================================================================
// View Registry Implementation
// =============================================================================

/**
 * Options for creating a view registry
 */
export interface ViewRegistryOptions {
  /** Custom storage (default: in-memory) */
  storage?: ViewStorage;

  /** Whether to validate views on registration */
  validateOnRegister?: boolean;

  /** Function to check if a table exists */
  tableExists?: (name: string) => boolean;
}

/**
 * Create a view registry
 */
export function createViewRegistry(options: ViewRegistryOptions = {}): ViewRegistry {
  const storage = options.storage ?? createInMemoryViewStorage();
  const validateOnRegister = options.validateOnRegister ?? true;
  const tableExists = options.tableExists ?? (() => true);

  // INSTEAD OF triggers stored separately
  const insteadOfTriggers = new Map<string, InsteadOfTrigger[]>();

  /**
   * Get fully qualified view name
   */
  function getFullName(name: string, schema?: string): string {
    return schema ? `${schema}.${name}` : name;
  }

  /**
   * Determine if a view is updatable
   */
  function isViewUpdatable(selectQuery: string): boolean {
    const upper = selectQuery.toUpperCase();

    // Not updatable if contains these
    if (
      /\bDISTINCT\b/.test(upper) ||
      /\bGROUP\s+BY\b/.test(upper) ||
      /\bHAVING\b/.test(upper) ||
      /\bUNION\b/.test(upper) ||
      /\bINTERSECT\b/.test(upper) ||
      /\bEXCEPT\b/.test(upper) ||
      /\bJOIN\b/.test(upper) ||
      /\bLIMIT\b/.test(upper) ||
      /\bOFFSET\b/.test(upper)
    ) {
      return false;
    }

    // Not updatable if has aggregate functions
    if (
      /\b(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT)\s*\(/i.test(selectQuery)
    ) {
      return false;
    }

    // Must have exactly one table in FROM clause
    const fromMatch = selectQuery.match(/\bFROM\s+(\w+)/i);
    if (!fromMatch) {
      return false;
    }

    return true;
  }

  /**
   * Separate tables from views in references
   */
  function categorizeReferences(refs: string[]): {
    tables: string[];
    views: string[];
  } {
    const tables: string[] = [];
    const views: string[] = [];

    for (const ref of refs) {
      if (storage.has(ref)) {
        views.push(ref);
      } else {
        tables.push(ref);
      }
    }

    return { tables, views };
  }

  /**
   * Check for circular dependencies
   */
  function hasCircularDependency(viewName: string, selectQuery: string): boolean {
    const visited = new Set<string>();
    const stack = new Set<string>();

    function dfs(name: string): boolean {
      if (stack.has(name)) {
        return true; // Circular dependency
      }
      if (visited.has(name)) {
        return false;
      }

      visited.add(name);
      stack.add(name);

      const view = storage.get(name);
      if (view) {
        const { possibleViews } = extractReferences(view.selectQuery);
        const { views: referencedViews } = categorizeReferences(possibleViews);

        for (const ref of [...view.referencedViews, ...referencedViews]) {
          if (dfs(ref)) {
            return true;
          }
        }
      }

      stack.delete(name);
      return false;
    }

    // Check if adding this view would create a cycle
    const { tables: refTables } = extractReferences(selectQuery);
    const { views: referencedViews } = categorizeReferences(refTables);

    if (referencedViews.includes(viewName)) {
      return true; // Self-reference
    }

    for (const ref of referencedViews) {
      visited.clear();
      stack.clear();
      stack.add(viewName); // Start with the new view in the stack
      if (dfs(ref)) {
        return true;
      }
    }

    return false;
  }

  return {
    register(parsed: ParsedView, opts?: RegisterViewOptions): ViewDefinition {
      const fullName = getFullName(parsed.name, parsed.schema);

      // Check if view already exists
      const existing = storage.get(fullName);
      if (existing && !parsed.orReplace && !opts?.replace) {
        if (parsed.ifNotExists) {
          return existing;
        }
        throw new ViewError(
          ViewErrorCode.DUPLICATE_VIEW,
          `View '${fullName}' already exists`,
          fullName
        );
      }

      // Validate the view definition
      if (validateOnRegister) {
        const validation = validateViewDefinition(parsed);
        if (!validation.valid) {
          throw new ViewError(
            ViewErrorCode.INVALID_DEFINITION,
            validation.errors.join('; '),
            fullName
          );
        }
      }

      // Check for circular dependencies
      if (hasCircularDependency(parsed.name, parsed.selectQuery)) {
        throw new ViewError(
          ViewErrorCode.CIRCULAR_DEPENDENCY,
          `Creating view '${fullName}' would create a circular dependency`,
          fullName
        );
      }

      // Extract references
      const { tables } = extractReferences(parsed.selectQuery);
      const { tables: referencedTables, views: referencedViews } = categorizeReferences(tables);

      // Validate referenced tables exist (if table checker provided)
      for (const table of referencedTables) {
        if (!tableExists(table) && !storage.has(table)) {
          throw new ViewError(
            ViewErrorCode.INVALID_REFERENCE,
            `View references non-existent table or view: ${table}`,
            fullName
          );
        }
      }

      // Create the view definition
      const now = new Date();
      const view: ViewDefinition = {
        ...parsed,
        id: `view_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        type: parsed.temporary ? 'temporary' : 'standard',
        updatable: isViewUpdatable(parsed.selectQuery),
        referencedTables,
        referencedViews,
        version: existing ? existing.version + 1 : 1,
        createdAt: existing?.createdAt ?? now,
        updatedAt: now,
        enabled: true,
        description: opts?.description,
        tags: opts?.tags,
      };

      storage.set(fullName, view);
      return view;
    },

    get(name: string, schema?: string): ViewDefinition | undefined {
      return storage.get(getFullName(name, schema));
    },

    exists(name: string, schema?: string): boolean {
      return storage.has(getFullName(name, schema));
    },

    list(opts?: ListViewsOptions): ViewDefinition[] {
      let views = storage.getAll();

      if (opts?.schema) {
        views = views.filter(v => v.schema === opts.schema);
      }

      if (opts?.type) {
        views = views.filter(v => v.type === opts.type);
      }

      if (opts?.referencedTable) {
        views = views.filter(v => v.referencedTables.includes(opts.referencedTable!));
      }

      if (opts?.enabled !== undefined) {
        views = views.filter(v => v.enabled === opts.enabled);
      }

      if (opts?.tag) {
        views = views.filter(v => v.tags?.includes(opts.tag!));
      }

      if (!opts?.includeTemporary) {
        views = views.filter(v => v.type !== 'temporary');
      }

      return views;
    },

    drop(name: string, opts?: { ifExists?: boolean; cascade?: boolean; schema?: string }): boolean {
      const fullName = getFullName(name, opts?.schema);
      const view = storage.get(fullName);

      if (!view) {
        if (opts?.ifExists) {
          return false;
        }
        throw new ViewError(
          ViewErrorCode.NOT_FOUND,
          `View '${fullName}' does not exist`,
          fullName
        );
      }

      // Check for dependents
      const dependents = this.getDependents(name);
      if (dependents.length > 0 && !opts?.cascade) {
        throw new ViewError(
          ViewErrorCode.HAS_DEPENDENTS,
          `Cannot drop view '${fullName}' because other views depend on it: ${dependents.map(d => d.name).join(', ')}`,
          fullName
        );
      }

      // If cascade, drop dependents first
      if (opts?.cascade) {
        for (const dependent of dependents) {
          this.drop(dependent.name, { cascade: true, schema: dependent.schema });
        }
      }

      // Remove INSTEAD OF triggers
      insteadOfTriggers.delete(fullName.toLowerCase());

      return storage.delete(fullName);
    },

    getDependents(name: string): ViewDefinition[] {
      const fullName = name.toLowerCase();
      return storage.getAll().filter(
        v => v.referencedViews.map(r => r.toLowerCase()).includes(fullName) ||
             v.referencedTables.map(r => r.toLowerCase()).includes(fullName)
      );
    },

    getDependencies(name: string): ViewDefinition[] {
      const view = storage.get(name);
      if (!view) {
        return [];
      }

      const deps: ViewDefinition[] = [];
      for (const ref of view.referencedViews) {
        const dep = storage.get(ref);
        if (dep) {
          deps.push(dep);
        }
      }
      return deps;
    },

    clear(opts?: { temporaryOnly?: boolean }): void {
      if (opts?.temporaryOnly) {
        const temps = storage.getAll().filter(v => v.type === 'temporary');
        for (const temp of temps) {
          storage.delete(getFullName(temp.name, temp.schema));
        }
      } else {
        storage.clear();
        insteadOfTriggers.clear();
      }
    },

    registerInsteadOfTrigger<T>(trigger: InsteadOfTrigger<T>): void {
      const key = trigger.viewName.toLowerCase();
      const existing = insteadOfTriggers.get(key) ?? [];

      // Check for duplicate name
      if (existing.some(t => t.name === trigger.name)) {
        throw new ViewError(
          ViewErrorCode.INVALID_DEFINITION,
          `INSTEAD OF trigger '${trigger.name}' already exists for view '${trigger.viewName}'`,
          trigger.viewName
        );
      }

      existing.push(trigger as InsteadOfTrigger);
      existing.sort((a, b) => a.priority - b.priority);
      insteadOfTriggers.set(key, existing);
    },

    getInsteadOfTriggers(viewName: string, operation: ViewDMLOperation): InsteadOfTrigger[] {
      const triggers = insteadOfTriggers.get(viewName.toLowerCase()) ?? [];
      return triggers.filter(
        t => t.enabled && t.operations.includes(operation)
      );
    },

    removeInsteadOfTrigger(name: string): boolean {
      for (const [key, triggers] of insteadOfTriggers.entries()) {
        const index = triggers.findIndex(t => t.name === name);
        if (index !== -1) {
          triggers.splice(index, 1);
          if (triggers.length === 0) {
            insteadOfTriggers.delete(key);
          }
          return true;
        }
      }
      return false;
    },
  };
}

// =============================================================================
// Dependency Analysis
// =============================================================================

/**
 * Analyze view dependencies in a registry
 */
export function analyzeDependencies(registry: ViewRegistry): DependencyAnalysis {
  const views = registry.list({ includeTemporary: true });
  const nodes = new Map<string, ViewDependencyNode>();
  const circularDependencies: string[][] = [];
  const orphanedViews: string[] = [];

  // Build initial nodes
  for (const view of views) {
    nodes.set(view.name.toLowerCase(), {
      name: view.name,
      dependencies: [...view.referencedTables, ...view.referencedViews],
      dependents: [],
      depth: -1,
    });
  }

  // Build dependent relationships
  for (const view of views) {
    const viewKey = view.name.toLowerCase();
    for (const dep of [...view.referencedTables, ...view.referencedViews]) {
      const depNode = nodes.get(dep.toLowerCase());
      if (depNode) {
        depNode.dependents.push(view.name);
      }
    }
  }

  // Calculate depths and detect cycles using Kahn's algorithm
  const inDegree = new Map<string, number>();
  const queue: string[] = [];

  for (const [name, node] of nodes) {
    // Count only view dependencies (not table dependencies)
    const viewDeps = node.dependencies.filter(d => nodes.has(d.toLowerCase()));
    inDegree.set(name, viewDeps.length);

    if (viewDeps.length === 0) {
      queue.push(name);
      node.depth = 0;
    }
  }

  const topologicalOrder: string[] = [];
  const processed = new Set<string>();

  while (queue.length > 0) {
    const current = queue.shift()!;
    topologicalOrder.push(nodes.get(current)!.name);
    processed.add(current);

    const currentNode = nodes.get(current)!;
    for (const dependent of currentNode.dependents) {
      const depKey = dependent.toLowerCase();
      const depNode = nodes.get(depKey);
      if (!depNode) continue;

      const newDegree = (inDegree.get(depKey) ?? 0) - 1;
      inDegree.set(depKey, newDegree);

      if (newDegree === 0) {
        queue.push(depKey);
        depNode.depth = currentNode.depth + 1;
      }
    }
  }

  // Check for circular dependencies (nodes not processed)
  for (const [name, node] of nodes) {
    if (!processed.has(name)) {
      // Find the cycle
      const cycle = findCycle(name, nodes);
      if (cycle.length > 0) {
        circularDependencies.push(cycle);
      }
    }
  }

  // Check for orphaned views (references that don't exist)
  for (const view of views) {
    for (const dep of view.referencedViews) {
      if (!nodes.has(dep.toLowerCase())) {
        orphanedViews.push(view.name);
        break;
      }
    }
  }

  return {
    nodes,
    topologicalOrder,
    circularDependencies,
    orphanedViews,
  };
}

/**
 * Find a cycle starting from a node
 */
function findCycle(start: string, nodes: Map<string, ViewDependencyNode>): string[] {
  const visited = new Set<string>();
  const path: string[] = [];

  function dfs(current: string): string[] | null {
    if (path.includes(current)) {
      // Found cycle
      const cycleStart = path.indexOf(current);
      return path.slice(cycleStart);
    }

    if (visited.has(current)) {
      return null;
    }

    visited.add(current);
    path.push(current);

    const node = nodes.get(current);
    if (node) {
      for (const dep of node.dependencies) {
        const depKey = dep.toLowerCase();
        if (nodes.has(depKey)) {
          const cycle = dfs(depKey);
          if (cycle) {
            return cycle;
          }
        }
      }
    }

    path.pop();
    return null;
  }

  return dfs(start) ?? [];
}

// =============================================================================
// View Builder
// =============================================================================

/**
 * Fluent builder for creating views
 */
export class ViewBuilder {
  private _name: string = '';
  private _schema?: string;
  private _columns?: string[];
  private _selectQuery: string = '';
  private _orReplace: boolean = false;
  private _ifNotExists: boolean = false;
  private _temporary: boolean = false;
  private _checkOption?: 'CASCADED' | 'LOCAL';
  private _description?: string;
  private _tags?: string[];

  /**
   * Set the view name
   */
  name(name: string): this {
    this._name = name;
    return this;
  }

  /**
   * Set the schema
   */
  schema(schema: string): this {
    this._schema = schema;
    return this;
  }

  /**
   * Set column aliases
   */
  columns(...columns: string[]): this {
    this._columns = columns;
    return this;
  }

  /**
   * Set the SELECT query
   */
  as(selectQuery: string): this {
    this._selectQuery = selectQuery;
    return this;
  }

  /**
   * Alias for as()
   */
  select(selectQuery: string): this {
    return this.as(selectQuery);
  }

  /**
   * Enable OR REPLACE
   */
  orReplace(): this {
    this._orReplace = true;
    return this;
  }

  /**
   * Enable IF NOT EXISTS
   */
  ifNotExists(): this {
    this._ifNotExists = true;
    return this;
  }

  /**
   * Make this a temporary view
   */
  temporary(): this {
    this._temporary = true;
    return this;
  }

  /**
   * Add WITH CHECK OPTION
   */
  withCheckOption(type: 'CASCADED' | 'LOCAL' = 'CASCADED'): this {
    this._checkOption = type;
    return this;
  }

  /**
   * Set description
   */
  description(desc: string): this {
    this._description = desc;
    return this;
  }

  /**
   * Add tags
   */
  tag(...tags: string[]): this {
    this._tags = [...(this._tags ?? []), ...tags];
    return this;
  }

  /**
   * Build the parsed view object
   */
  build(): ParsedView {
    if (!this._name) {
      throw new ViewError(
        ViewErrorCode.INVALID_DEFINITION,
        'View name is required'
      );
    }
    if (!this._selectQuery) {
      throw new ViewError(
        ViewErrorCode.INVALID_DEFINITION,
        'SELECT query is required'
      );
    }

    return {
      name: this._name,
      schema: this._schema,
      columns: this._columns,
      selectQuery: this._selectQuery,
      orReplace: this._orReplace,
      ifNotExists: this._ifNotExists,
      temporary: this._temporary,
      checkOption: this._checkOption,
      rawSql: '', // Will be generated if needed
    };
  }

  /**
   * Build and register with a registry
   */
  register(registry: ViewRegistry): ViewDefinition {
    return registry.register(this.build(), {
      description: this._description,
      tags: this._tags,
    });
  }
}

/**
 * Create a new view builder
 */
export function view(name?: string): ViewBuilder {
  const builder = new ViewBuilder();
  if (name) {
    builder.name(name);
  }
  return builder;
}
