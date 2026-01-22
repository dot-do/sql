/**
 * UNSPSC Database Benchmark Dataset
 *
 * United Nations Standard Products and Services Code classification system.
 * Hierarchical product taxonomy used for procurement and categorization.
 *
 * Dataset characteristics:
 * - ~50,000 rows total across all tables
 * - 4-level hierarchy: Segment > Family > Class > Commodity
 * - Numeric code-based identification (prefix queries)
 * - Parent-child relationships
 *
 * Query patterns tested:
 * - Hierarchical traversal (parent/child)
 * - Prefix queries on codes
 * - Tree aggregations
 * - Path queries
 * - Level-based filtering
 *
 * @packageDocumentation
 */

// =============================================================================
// Type Definitions
// =============================================================================

/**
 * Segment entity (Level 1 - 2 digits)
 * Example: 10 - Live Plant and Animal Material
 */
export interface UnspscSegment {
  /** Segment code (2 digits, e.g., "10") */
  segment_code: string;
  /** Segment name */
  name: string;
  /** Description */
  description: string;
  /** Whether segment is active */
  is_active: number;
  /** Sort order for display */
  sort_order: number;
}

/**
 * Family entity (Level 2 - 4 digits)
 * Example: 1010 - Live animals
 */
export interface UnspscFamily {
  /** Family code (4 digits, e.g., "1010") */
  family_code: string;
  /** Parent segment code */
  segment_code: string;
  /** Family name */
  name: string;
  /** Description */
  description: string;
  /** Whether family is active */
  is_active: number;
  /** Sort order within segment */
  sort_order: number;
}

/**
 * Class entity (Level 3 - 6 digits)
 * Example: 101015 - Livestock
 */
export interface UnspscClass {
  /** Class code (6 digits, e.g., "101015") */
  class_code: string;
  /** Parent family code */
  family_code: string;
  /** Class name */
  name: string;
  /** Description */
  description: string;
  /** Whether class is active */
  is_active: number;
  /** Sort order within family */
  sort_order: number;
}

/**
 * Commodity entity (Level 4 - 8 digits)
 * Example: 10101501 - Cats
 */
export interface UnspscCommodity {
  /** Commodity code (8 digits, e.g., "10101501") */
  commodity_code: string;
  /** Parent class code */
  class_code: string;
  /** Commodity name */
  name: string;
  /** Description */
  description: string;
  /** Whether commodity is active */
  is_active: number;
  /** Unit of measure (nullable) */
  unit_of_measure: string | null;
  /** Average unit price (nullable) */
  average_price: number | null;
  /** Keywords for search */
  keywords: string | null;
  /** Sort order within class */
  sort_order: number;
}

/**
 * Full path view for a commodity
 */
export interface UnspscFullPath {
  /** Commodity code */
  commodity_code: string;
  /** Commodity name */
  commodity_name: string;
  /** Class code */
  class_code: string;
  /** Class name */
  class_name: string;
  /** Family code */
  family_code: string;
  /** Family name */
  family_name: string;
  /** Segment code */
  segment_code: string;
  /** Segment name */
  segment_name: string;
  /** Full path string */
  full_path: string;
}

/**
 * Synonym/alias for search optimization
 */
export interface UnspscSynonym {
  /** Unique synonym identifier */
  synonym_id: number;
  /** Commodity code this synonym applies to */
  commodity_code: string;
  /** Synonym term */
  term: string;
  /** Language code (e.g., 'en', 'es') */
  language: string;
}

/**
 * Cross-reference to other classification systems
 */
export interface UnspscCrossRef {
  /** Unique cross-reference identifier */
  crossref_id: number;
  /** UNSPSC commodity code */
  commodity_code: string;
  /** External system name */
  external_system: string;
  /** External system code */
  external_code: string;
  /** Mapping confidence (0.0-1.0) */
  confidence: number;
}

// =============================================================================
// Schema Definitions
// =============================================================================

/**
 * SQL statements to create UNSPSC schema
 */
export const UNSPSC_SCHEMA = {
  segments: `
    CREATE TABLE IF NOT EXISTS segments (
      segment_code TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT NOT NULL,
      is_active INTEGER NOT NULL DEFAULT 1,
      sort_order INTEGER NOT NULL
    )
  `,

  families: `
    CREATE TABLE IF NOT EXISTS families (
      family_code TEXT PRIMARY KEY,
      segment_code TEXT NOT NULL REFERENCES segments(segment_code),
      name TEXT NOT NULL,
      description TEXT NOT NULL,
      is_active INTEGER NOT NULL DEFAULT 1,
      sort_order INTEGER NOT NULL
    )
  `,

  classes: `
    CREATE TABLE IF NOT EXISTS classes (
      class_code TEXT PRIMARY KEY,
      family_code TEXT NOT NULL REFERENCES families(family_code),
      name TEXT NOT NULL,
      description TEXT NOT NULL,
      is_active INTEGER NOT NULL DEFAULT 1,
      sort_order INTEGER NOT NULL
    )
  `,

  commodities: `
    CREATE TABLE IF NOT EXISTS commodities (
      commodity_code TEXT PRIMARY KEY,
      class_code TEXT NOT NULL REFERENCES classes(class_code),
      name TEXT NOT NULL,
      description TEXT NOT NULL,
      is_active INTEGER NOT NULL DEFAULT 1,
      unit_of_measure TEXT,
      average_price REAL,
      keywords TEXT,
      sort_order INTEGER NOT NULL
    )
  `,

  synonyms: `
    CREATE TABLE IF NOT EXISTS synonyms (
      synonym_id INTEGER PRIMARY KEY,
      commodity_code TEXT NOT NULL REFERENCES commodities(commodity_code),
      term TEXT NOT NULL,
      language TEXT NOT NULL DEFAULT 'en'
    )
  `,

  cross_references: `
    CREATE TABLE IF NOT EXISTS cross_references (
      crossref_id INTEGER PRIMARY KEY,
      commodity_code TEXT NOT NULL REFERENCES commodities(commodity_code),
      external_system TEXT NOT NULL,
      external_code TEXT NOT NULL,
      confidence REAL NOT NULL DEFAULT 1.0
    )
  `,
};

/**
 * Index creation statements for optimal query performance
 */
export const UNSPSC_INDEXES = [
  // Segment indexes
  'CREATE INDEX IF NOT EXISTS idx_segments_active ON segments(is_active)',
  'CREATE INDEX IF NOT EXISTS idx_segments_sort ON segments(sort_order)',

  // Family indexes
  'CREATE INDEX IF NOT EXISTS idx_families_segment ON families(segment_code)',
  'CREATE INDEX IF NOT EXISTS idx_families_active ON families(is_active)',

  // Class indexes
  'CREATE INDEX IF NOT EXISTS idx_classes_family ON classes(family_code)',
  'CREATE INDEX IF NOT EXISTS idx_classes_active ON classes(is_active)',

  // Commodity indexes
  'CREATE INDEX IF NOT EXISTS idx_commodities_class ON commodities(class_code)',
  'CREATE INDEX IF NOT EXISTS idx_commodities_active ON commodities(is_active)',
  'CREATE INDEX IF NOT EXISTS idx_commodities_price ON commodities(average_price)',
  'CREATE INDEX IF NOT EXISTS idx_commodities_unit ON commodities(unit_of_measure)',

  // Synonym indexes
  'CREATE INDEX IF NOT EXISTS idx_synonyms_commodity ON synonyms(commodity_code)',
  'CREATE INDEX IF NOT EXISTS idx_synonyms_term ON synonyms(term)',
  'CREATE INDEX IF NOT EXISTS idx_synonyms_language ON synonyms(language)',

  // Cross-reference indexes
  'CREATE INDEX IF NOT EXISTS idx_crossref_commodity ON cross_references(commodity_code)',
  'CREATE INDEX IF NOT EXISTS idx_crossref_system ON cross_references(external_system)',
  'CREATE INDEX IF NOT EXISTS idx_crossref_external ON cross_references(external_code)',
];

// =============================================================================
// Seeded Random Number Generator
// =============================================================================

/**
 * Simple seeded PRNG (Mulberry32)
 */
class SeededRandom {
  private state: number;

  constructor(seed: number) {
    this.state = seed;
  }

  next(): number {
    let t = (this.state += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  int(min: number, max: number): number {
    return Math.floor(this.next() * (max - min + 1)) + min;
  }

  pick<T>(array: T[]): T {
    return array[this.int(0, array.length - 1)];
  }

  decimal(min: number, max: number, decimals: number = 2): number {
    const value = this.next() * (max - min) + min;
    return Number(value.toFixed(decimals));
  }

  bool(probability: number = 0.5): boolean {
    return this.next() < probability;
  }

  pickMultiple<T>(array: T[], count: number): T[] {
    const shuffled = [...array].sort(() => this.next() - 0.5);
    return shuffled.slice(0, Math.min(count, array.length));
  }
}

// =============================================================================
// Reference Data
// =============================================================================

const SEGMENT_DATA: Array<{ code: string; name: string; description: string; families: Array<{ suffix: string; name: string; classes: string[] }> }> = [
  {
    code: '10',
    name: 'Live Plant and Animal Material and Accessories and Supplies',
    description: 'Living organisms and related supplies',
    families: [
      { suffix: '10', name: 'Live animals', classes: ['Livestock', 'Pets', 'Farm animals', 'Aquatic animals'] },
      { suffix: '11', name: 'Domestic pet products', classes: ['Pet food', 'Pet accessories', 'Pet care'] },
      { suffix: '12', name: 'Animal feed', classes: ['Livestock feed', 'Poultry feed', 'Fish feed'] },
      { suffix: '13', name: 'Live plants', classes: ['Trees', 'Shrubs', 'Flowers', 'Seeds'] },
      { suffix: '14', name: 'Plant products', classes: ['Fertilizers', 'Pesticides', 'Growing media'] },
    ],
  },
  {
    code: '11',
    name: 'Mineral and Textile and Inedible Plant and Animal Materials',
    description: 'Raw materials from natural sources',
    families: [
      { suffix: '10', name: 'Minerals and ores', classes: ['Metallic ores', 'Non-metallic minerals', 'Precious stones'] },
      { suffix: '11', name: 'Textile materials', classes: ['Natural fibers', 'Synthetic fibers', 'Yarns'] },
      { suffix: '12', name: 'Leather materials', classes: ['Raw hides', 'Processed leather', 'Leather products'] },
      { suffix: '13', name: 'Wood materials', classes: ['Lumber', 'Plywood', 'Timber'] },
    ],
  },
  {
    code: '12',
    name: 'Chemicals including Bio Chemicals and Gas Materials',
    description: 'Chemical compounds and gases',
    families: [
      { suffix: '10', name: 'Industrial chemicals', classes: ['Acids', 'Bases', 'Solvents', 'Catalysts'] },
      { suffix: '11', name: 'Biochemicals', classes: ['Enzymes', 'Proteins', 'Amino acids'] },
      { suffix: '12', name: 'Gases', classes: ['Industrial gases', 'Medical gases', 'Specialty gases'] },
      { suffix: '13', name: 'Adhesives and sealants', classes: ['Adhesives', 'Sealants', 'Bonding agents'] },
      { suffix: '14', name: 'Cleaning chemicals', classes: ['Detergents', 'Sanitizers', 'Degreasers'] },
    ],
  },
  {
    code: '13',
    name: 'Resin and Rosin and Rubber and Foam and Film and Elastomeric Materials',
    description: 'Polymer and elastomer materials',
    families: [
      { suffix: '10', name: 'Rubber materials', classes: ['Natural rubber', 'Synthetic rubber', 'Rubber compounds'] },
      { suffix: '11', name: 'Plastic materials', classes: ['Thermoplastics', 'Thermosets', 'Plastic films'] },
      { suffix: '12', name: 'Foam materials', classes: ['Polyurethane foam', 'Polystyrene foam', 'Rubber foam'] },
      { suffix: '13', name: 'Resins', classes: ['Epoxy resins', 'Polyester resins', 'Acrylic resins'] },
    ],
  },
  {
    code: '14',
    name: 'Paper Materials and Products',
    description: 'Paper and paper-based products',
    families: [
      { suffix: '10', name: 'Paper products', classes: ['Writing paper', 'Printing paper', 'Specialty paper'] },
      { suffix: '11', name: 'Paper materials', classes: ['Pulp', 'Newsprint', 'Cardboard'] },
      { suffix: '12', name: 'Packaging paper', classes: ['Kraft paper', 'Corrugated', 'Tissue paper'] },
    ],
  },
  {
    code: '15',
    name: 'Fuels and Fuel Additives and Lubricants and Anti corrosive Materials',
    description: 'Energy sources and protective materials',
    families: [
      { suffix: '10', name: 'Fuels', classes: ['Petroleum fuels', 'Biofuels', 'Coal', 'Natural gas'] },
      { suffix: '11', name: 'Lubricants', classes: ['Motor oils', 'Industrial lubricants', 'Greases'] },
      { suffix: '12', name: 'Fuel additives', classes: ['Octane boosters', 'Fuel stabilizers', 'Anti-freeze'] },
      { suffix: '13', name: 'Anti-corrosive materials', classes: ['Rust inhibitors', 'Coatings', 'Primers'] },
    ],
  },
  {
    code: '20',
    name: 'Mining and Well Drilling Machinery and Accessories',
    description: 'Equipment for extraction industries',
    families: [
      { suffix: '10', name: 'Mining equipment', classes: ['Excavators', 'Drilling machines', 'Loaders'] },
      { suffix: '11', name: 'Well drilling equipment', classes: ['Drill bits', 'Drill pipes', 'Casings'] },
      { suffix: '12', name: 'Mining accessories', classes: ['Conveyor systems', 'Crushers', 'Screens'] },
    ],
  },
  {
    code: '21',
    name: 'Farming and Fishing and Forestry and Wildlife Machinery and Accessories',
    description: 'Agricultural and natural resource equipment',
    families: [
      { suffix: '10', name: 'Agricultural machinery', classes: ['Tractors', 'Harvesters', 'Planters', 'Tillers'] },
      { suffix: '11', name: 'Fishing equipment', classes: ['Fishing boats', 'Nets', 'Processing equipment'] },
      { suffix: '12', name: 'Forestry equipment', classes: ['Chainsaws', 'Log splitters', 'Chippers'] },
    ],
  },
  {
    code: '22',
    name: 'Building and Construction Machinery and Accessories',
    description: 'Construction and building equipment',
    families: [
      { suffix: '10', name: 'Heavy construction equipment', classes: ['Cranes', 'Bulldozers', 'Excavators', 'Loaders'] },
      { suffix: '11', name: 'Concrete equipment', classes: ['Mixers', 'Pumps', 'Forms', 'Vibrators'] },
      { suffix: '12', name: 'Road building equipment', classes: ['Pavers', 'Rollers', 'Graders'] },
      { suffix: '13', name: 'Scaffolding and ladders', classes: ['Scaffolds', 'Ladders', 'Platforms'] },
    ],
  },
  {
    code: '23',
    name: 'Industrial Manufacturing and Processing Machinery and Accessories',
    description: 'Manufacturing equipment and machinery',
    families: [
      { suffix: '10', name: 'Metal working machinery', classes: ['Lathes', 'Mills', 'Grinders', 'Presses'] },
      { suffix: '11', name: 'Welding equipment', classes: ['Welders', 'Torches', 'Electrodes'] },
      { suffix: '12', name: 'Packaging machinery', classes: ['Filling machines', 'Sealing machines', 'Labelers'] },
      { suffix: '13', name: 'Textile machinery', classes: ['Looms', 'Spinning machines', 'Dyeing equipment'] },
      { suffix: '14', name: 'Food processing equipment', classes: ['Mixers', 'Ovens', 'Freezers', 'Slicers'] },
    ],
  },
  {
    code: '24',
    name: 'Material Handling and Conditioning and Storage Machinery and their Accessories and Supplies',
    description: 'Material handling and storage equipment',
    families: [
      { suffix: '10', name: 'Material handling equipment', classes: ['Forklifts', 'Pallet jacks', 'Hoists', 'Conveyors'] },
      { suffix: '11', name: 'Storage equipment', classes: ['Shelving', 'Racks', 'Bins', 'Cabinets'] },
      { suffix: '12', name: 'Packaging supplies', classes: ['Boxes', 'Tape', 'Stretch wrap', 'Cushioning'] },
    ],
  },
  {
    code: '25',
    name: 'Commercial and Military and Private Vehicles and their Accessories and Components',
    description: 'Vehicles and transportation equipment',
    families: [
      { suffix: '10', name: 'Motor vehicles', classes: ['Automobiles', 'Trucks', 'Vans', 'SUVs'] },
      { suffix: '11', name: 'Vehicle parts', classes: ['Engines', 'Transmissions', 'Brakes', 'Suspension'] },
      { suffix: '12', name: 'Vehicle accessories', classes: ['Tires', 'Batteries', 'Lights', 'Audio systems'] },
      { suffix: '13', name: 'Military vehicles', classes: ['Tanks', 'APCs', 'Military trucks'] },
      { suffix: '14', name: 'Aircraft', classes: ['Helicopters', 'Fixed-wing', 'Drones'] },
    ],
  },
  {
    code: '26',
    name: 'Power Generation and Distribution Machinery and Accessories',
    description: 'Power generation and electrical distribution',
    families: [
      { suffix: '10', name: 'Power generators', classes: ['Diesel generators', 'Gas generators', 'Solar panels'] },
      { suffix: '11', name: 'Transformers', classes: ['Distribution transformers', 'Power transformers'] },
      { suffix: '12', name: 'Electrical distribution', classes: ['Switchgear', 'Circuit breakers', 'Cables'] },
      { suffix: '13', name: 'Batteries and storage', classes: ['Lead-acid batteries', 'Lithium batteries', 'UPS systems'] },
    ],
  },
  {
    code: '27',
    name: 'Tools and General Machinery',
    description: 'Hand tools and general-purpose machinery',
    families: [
      { suffix: '10', name: 'Hand tools', classes: ['Wrenches', 'Screwdrivers', 'Pliers', 'Hammers'] },
      { suffix: '11', name: 'Power tools', classes: ['Drills', 'Saws', 'Sanders', 'Grinders'] },
      { suffix: '12', name: 'Measuring tools', classes: ['Calipers', 'Micrometers', 'Levels', 'Tape measures'] },
      { suffix: '13', name: 'Cutting tools', classes: ['Drill bits', 'Saw blades', 'Milling cutters'] },
    ],
  },
  {
    code: '30',
    name: 'Structures and Building and Construction and Manufacturing Components and Supplies',
    description: 'Building materials and components',
    families: [
      { suffix: '10', name: 'Structural materials', classes: ['Steel beams', 'Concrete blocks', 'Timber frames'] },
      { suffix: '11', name: 'Building components', classes: ['Doors', 'Windows', 'Roofing', 'Siding'] },
      { suffix: '12', name: 'Hardware', classes: ['Fasteners', 'Hinges', 'Locks', 'Brackets'] },
      { suffix: '13', name: 'Insulation', classes: ['Fiberglass', 'Foam board', 'Spray foam'] },
      { suffix: '14', name: 'Plumbing fixtures', classes: ['Faucets', 'Sinks', 'Toilets', 'Showers'] },
    ],
  },
  {
    code: '31',
    name: 'Manufacturing Components and Supplies',
    description: 'Components for manufacturing',
    families: [
      { suffix: '10', name: 'Bearings and bushings', classes: ['Ball bearings', 'Roller bearings', 'Bushings'] },
      { suffix: '11', name: 'Gears and sprockets', classes: ['Spur gears', 'Helical gears', 'Sprockets', 'Chains'] },
      { suffix: '12', name: 'Seals and gaskets', classes: ['O-rings', 'Gaskets', 'Oil seals'] },
      { suffix: '13', name: 'Springs', classes: ['Compression springs', 'Extension springs', 'Torsion springs'] },
    ],
  },
  {
    code: '32',
    name: 'Electronic Components and Supplies',
    description: 'Electronic parts and components',
    families: [
      { suffix: '10', name: 'Semiconductors', classes: ['Integrated circuits', 'Transistors', 'Diodes', 'LEDs'] },
      { suffix: '11', name: 'Passive components', classes: ['Resistors', 'Capacitors', 'Inductors'] },
      { suffix: '12', name: 'Connectors', classes: ['Wire connectors', 'PCB connectors', 'RF connectors'] },
      { suffix: '13', name: 'Displays', classes: ['LCD displays', 'LED displays', 'OLED displays'] },
    ],
  },
  {
    code: '39',
    name: 'Electrical Systems and Lighting and Components and Accessories and Supplies',
    description: 'Electrical and lighting systems',
    families: [
      { suffix: '10', name: 'Lighting equipment', classes: ['Light bulbs', 'Fixtures', 'Ballasts', 'LED lights'] },
      { suffix: '11', name: 'Electrical wire', classes: ['Building wire', 'Industrial cable', 'Fiber optic'] },
      { suffix: '12', name: 'Electrical components', classes: ['Switches', 'Outlets', 'Junction boxes'] },
    ],
  },
  {
    code: '40',
    name: 'Distribution and Conditioning Systems and Equipment and Components',
    description: 'HVAC and distribution systems',
    families: [
      { suffix: '10', name: 'HVAC equipment', classes: ['Air conditioners', 'Heaters', 'Ventilators', 'Heat pumps'] },
      { suffix: '11', name: 'Pumps and compressors', classes: ['Centrifugal pumps', 'Piston pumps', 'Compressors'] },
      { suffix: '12', name: 'Valves', classes: ['Ball valves', 'Gate valves', 'Check valves', 'Control valves'] },
      { suffix: '13', name: 'Piping', classes: ['Steel pipe', 'PVC pipe', 'Copper tubing', 'Fittings'] },
    ],
  },
  {
    code: '41',
    name: 'Laboratory and Measuring and Observing and Testing Equipment',
    description: 'Scientific and testing equipment',
    families: [
      { suffix: '10', name: 'Laboratory equipment', classes: ['Microscopes', 'Centrifuges', 'Spectrometers'] },
      { suffix: '11', name: 'Measuring equipment', classes: ['Scales', 'Thermometers', 'Pressure gauges'] },
      { suffix: '12', name: 'Testing equipment', classes: ['Hardness testers', 'Tensile testers', 'Analyzers'] },
    ],
  },
  {
    code: '42',
    name: 'Medical Equipment and Accessories and Supplies',
    description: 'Healthcare equipment and supplies',
    families: [
      { suffix: '10', name: 'Medical instruments', classes: ['Surgical instruments', 'Diagnostic tools', 'Monitors'] },
      { suffix: '11', name: 'Medical supplies', classes: ['Bandages', 'Syringes', 'Gloves', 'Masks'] },
      { suffix: '12', name: 'Medical equipment', classes: ['Hospital beds', 'Wheelchairs', 'Imaging equipment'] },
      { suffix: '13', name: 'Pharmaceuticals', classes: ['Prescription drugs', 'OTC medications', 'Vaccines'] },
    ],
  },
  {
    code: '43',
    name: 'Information Technology Broadcasting and Telecommunications',
    description: 'IT and communications equipment',
    families: [
      { suffix: '10', name: 'Computer hardware', classes: ['Desktops', 'Laptops', 'Servers', 'Tablets'] },
      { suffix: '11', name: 'Computer peripherals', classes: ['Monitors', 'Keyboards', 'Mice', 'Printers'] },
      { suffix: '12', name: 'Networking equipment', classes: ['Routers', 'Switches', 'Firewalls', 'Cables'] },
      { suffix: '13', name: 'Software', classes: ['Operating systems', 'Applications', 'Security software'] },
      { suffix: '14', name: 'Telecommunications', classes: ['Phones', 'Radio equipment', 'Satellite equipment'] },
    ],
  },
  {
    code: '44',
    name: 'Office Equipment and Accessories and Supplies',
    description: 'Office products and supplies',
    families: [
      { suffix: '10', name: 'Office machines', classes: ['Copiers', 'Fax machines', 'Shredders', 'Calculators'] },
      { suffix: '11', name: 'Office supplies', classes: ['Pens', 'Paper', 'Folders', 'Binders'] },
      { suffix: '12', name: 'Office furniture', classes: ['Desks', 'Chairs', 'File cabinets', 'Bookcases'] },
    ],
  },
  {
    code: '45',
    name: 'Printing and Photographic and Audio and Visual Equipment and Supplies',
    description: 'Media and printing equipment',
    families: [
      { suffix: '10', name: 'Printing equipment', classes: ['Printers', 'Plotters', 'Printing presses'] },
      { suffix: '11', name: 'Photography equipment', classes: ['Cameras', 'Lenses', 'Lighting', 'Tripods'] },
      { suffix: '12', name: 'Audio equipment', classes: ['Microphones', 'Speakers', 'Amplifiers', 'Mixers'] },
      { suffix: '13', name: 'Video equipment', classes: ['Cameras', 'Monitors', 'Projectors', 'Recorders'] },
    ],
  },
  {
    code: '46',
    name: 'Defense and Law Enforcement and Security and Safety Equipment and Supplies',
    description: 'Security and safety equipment',
    families: [
      { suffix: '10', name: 'Security equipment', classes: ['Cameras', 'Alarms', 'Access control', 'Locks'] },
      { suffix: '11', name: 'Safety equipment', classes: ['Fire extinguishers', 'Safety signs', 'First aid'] },
      { suffix: '12', name: 'Personal protective equipment', classes: ['Helmets', 'Gloves', 'Goggles', 'Vests'] },
      { suffix: '13', name: 'Law enforcement equipment', classes: ['Restraints', 'Batons', 'Body armor'] },
    ],
  },
  {
    code: '47',
    name: 'Cleaning Equipment and Supplies',
    description: 'Cleaning products and equipment',
    families: [
      { suffix: '10', name: 'Cleaning equipment', classes: ['Vacuum cleaners', 'Floor scrubbers', 'Pressure washers'] },
      { suffix: '11', name: 'Cleaning supplies', classes: ['Mops', 'Brooms', 'Buckets', 'Cloths'] },
      { suffix: '12', name: 'Cleaning chemicals', classes: ['Detergents', 'Disinfectants', 'Degreasers'] },
    ],
  },
  {
    code: '48',
    name: 'Service Industry Machinery and Equipment and Supplies',
    description: 'Service industry equipment',
    families: [
      { suffix: '10', name: 'Food service equipment', classes: ['Ovens', 'Refrigerators', 'Dishwashers', 'Grills'] },
      { suffix: '11', name: 'Hospitality equipment', classes: ['Hotel furniture', 'Linens', 'Amenities'] },
      { suffix: '12', name: 'Laundry equipment', classes: ['Washers', 'Dryers', 'Irons', 'Presses'] },
    ],
  },
  {
    code: '50',
    name: 'Food and Beverage Products',
    description: 'Food and beverage items',
    families: [
      { suffix: '10', name: 'Fresh foods', classes: ['Fruits', 'Vegetables', 'Meat', 'Seafood'] },
      { suffix: '11', name: 'Processed foods', classes: ['Canned goods', 'Frozen foods', 'Snacks'] },
      { suffix: '12', name: 'Beverages', classes: ['Water', 'Soft drinks', 'Juices', 'Alcoholic beverages'] },
      { suffix: '13', name: 'Dairy products', classes: ['Milk', 'Cheese', 'Yogurt', 'Butter'] },
      { suffix: '14', name: 'Bakery products', classes: ['Bread', 'Pastries', 'Cakes', 'Cookies'] },
    ],
  },
  {
    code: '51',
    name: 'Drugs and Pharmaceutical Products',
    description: 'Pharmaceutical products',
    families: [
      { suffix: '10', name: 'Prescription medications', classes: ['Antibiotics', 'Pain relievers', 'Cardiovascular', 'Neurological'] },
      { suffix: '11', name: 'OTC medications', classes: ['Pain relievers', 'Cold remedies', 'Antacids', 'Vitamins'] },
      { suffix: '12', name: 'Medical supplies', classes: ['Bandages', 'Syringes', 'Diagnostic supplies'] },
    ],
  },
  {
    code: '52',
    name: 'Domestic Appliances and Supplies and Consumer Electronic Products',
    description: 'Consumer appliances and electronics',
    families: [
      { suffix: '10', name: 'Major appliances', classes: ['Refrigerators', 'Washing machines', 'Dryers', 'Dishwashers'] },
      { suffix: '11', name: 'Small appliances', classes: ['Toasters', 'Blenders', 'Coffee makers', 'Microwaves'] },
      { suffix: '12', name: 'Consumer electronics', classes: ['Televisions', 'Audio systems', 'Gaming consoles'] },
    ],
  },
  {
    code: '53',
    name: 'Apparel and Luggage and Personal Care Products',
    description: 'Clothing and personal items',
    families: [
      { suffix: '10', name: 'Clothing', classes: ['Shirts', 'Pants', 'Dresses', 'Outerwear'] },
      { suffix: '11', name: 'Footwear', classes: ['Shoes', 'Boots', 'Sandals', 'Athletic footwear'] },
      { suffix: '12', name: 'Luggage', classes: ['Suitcases', 'Backpacks', 'Briefcases', 'Travel accessories'] },
      { suffix: '13', name: 'Personal care', classes: ['Toiletries', 'Cosmetics', 'Hair care', 'Skincare'] },
    ],
  },
  {
    code: '54',
    name: 'Timepieces and Jewelry and Gemstone Products',
    description: 'Watches, jewelry, and gemstones',
    families: [
      { suffix: '10', name: 'Timepieces', classes: ['Watches', 'Clocks', 'Watch accessories'] },
      { suffix: '11', name: 'Jewelry', classes: ['Rings', 'Necklaces', 'Bracelets', 'Earrings'] },
      { suffix: '12', name: 'Gemstones', classes: ['Diamonds', 'Rubies', 'Sapphires', 'Emeralds'] },
    ],
  },
];

const UNITS_OF_MEASURE = [
  'Each', 'Pack', 'Box', 'Case', 'Dozen', 'Hundred', 'Thousand',
  'Kilogram', 'Pound', 'Ounce', 'Ton', 'Liter', 'Gallon', 'Meter',
  'Foot', 'Square Meter', 'Square Foot', 'Cubic Meter', 'Hour', 'Day',
];

const EXTERNAL_SYSTEMS = ['HS Code', 'NAICS', 'SIC', 'CPV', 'GTIN', 'SKU'];

const LANGUAGES = ['en', 'es', 'fr', 'de', 'zh', 'ja'];

// =============================================================================
// Data Generator
// =============================================================================

/**
 * Configuration for UNSPSC data generation
 */
export interface UnspscGeneratorConfig {
  /** Random seed for reproducibility */
  seed?: number;
  /** Number of commodities per class */
  commoditiesPerClass?: number;
  /** Average synonyms per commodity */
  avgSynonymsPerCommodity?: number;
  /** Average cross-references per commodity */
  avgCrossRefsPerCommodity?: number;
}

/**
 * Default configuration
 */
const DEFAULT_CONFIG: Required<UnspscGeneratorConfig> = {
  seed: 42,
  commoditiesPerClass: 15,
  avgSynonymsPerCommodity: 2,
  avgCrossRefsPerCommodity: 1,
};

/**
 * Generated UNSPSC dataset
 */
export interface UnspscDataset {
  segments: UnspscSegment[];
  families: UnspscFamily[];
  classes: UnspscClass[];
  commodities: UnspscCommodity[];
  synonyms: UnspscSynonym[];
  crossReferences: UnspscCrossRef[];
}

/**
 * Generate a complete UNSPSC dataset with deterministic seeding
 *
 * @param config - Generation configuration
 * @returns Complete dataset with all entities
 *
 * @example
 * ```ts
 * const data = generateUnspscData({ seed: 12345 });
 * console.log(data.commodities.length); // ~50000
 * ```
 */
export function generateUnspscData(config: UnspscGeneratorConfig = {}): UnspscDataset {
  const cfg = { ...DEFAULT_CONFIG, ...config };
  const rng = new SeededRandom(cfg.seed);

  const segments: UnspscSegment[] = [];
  const families: UnspscFamily[] = [];
  const classes: UnspscClass[] = [];
  const commodities: UnspscCommodity[] = [];
  const synonyms: UnspscSynonym[] = [];
  const crossReferences: UnspscCrossRef[] = [];

  let synonymId = 1;
  let crossRefId = 1;

  // Process each segment from reference data
  for (let segIdx = 0; segIdx < SEGMENT_DATA.length; segIdx++) {
    const segmentData = SEGMENT_DATA[segIdx];

    // Create segment
    segments.push({
      segment_code: segmentData.code,
      name: segmentData.name,
      description: segmentData.description,
      is_active: rng.bool(0.95) ? 1 : 0,
      sort_order: segIdx + 1,
    });

    // Create families for this segment
    for (let famIdx = 0; famIdx < segmentData.families.length; famIdx++) {
      const familyData = segmentData.families[famIdx];
      const familyCode = `${segmentData.code}${familyData.suffix}`;

      families.push({
        family_code: familyCode,
        segment_code: segmentData.code,
        name: familyData.name,
        description: `${familyData.name} within ${segmentData.name}`,
        is_active: rng.bool(0.95) ? 1 : 0,
        sort_order: famIdx + 1,
      });

      // Create classes for this family
      for (let classIdx = 0; classIdx < familyData.classes.length; classIdx++) {
        const className = familyData.classes[classIdx];
        const classCode = `${familyCode}${String(classIdx + 10).padStart(2, '0')}`;

        classes.push({
          class_code: classCode,
          family_code: familyCode,
          name: className,
          description: `${className} products and services`,
          is_active: rng.bool(0.95) ? 1 : 0,
          sort_order: classIdx + 1,
        });

        // Create commodities for this class
        const commodityCount = rng.int(
          Math.floor(cfg.commoditiesPerClass * 0.5),
          Math.floor(cfg.commoditiesPerClass * 1.5)
        );

        for (let commIdx = 0; commIdx < commodityCount; commIdx++) {
          const commodityCode = `${classCode}${String(commIdx + 1).padStart(2, '0')}`;
          const commodityName = generateCommodityName(className, commIdx, rng);

          commodities.push({
            commodity_code: commodityCode,
            class_code: classCode,
            name: commodityName,
            description: `${commodityName} for ${className.toLowerCase()} applications`,
            is_active: rng.bool(0.95) ? 1 : 0,
            unit_of_measure: rng.bool(0.8) ? rng.pick(UNITS_OF_MEASURE) : null,
            average_price: rng.bool(0.6) ? rng.decimal(1, 10000, 2) : null,
            keywords: rng.bool(0.7) ? generateKeywords(commodityName, className, rng) : null,
            sort_order: commIdx + 1,
          });

          // Generate synonyms
          const synonymCount = rng.int(0, cfg.avgSynonymsPerCommodity * 2);
          for (let synIdx = 0; synIdx < synonymCount; synIdx++) {
            synonyms.push({
              synonym_id: synonymId++,
              commodity_code: commodityCode,
              term: generateSynonym(commodityName, rng),
              language: rng.bool(0.7) ? 'en' : rng.pick(LANGUAGES),
            });
          }

          // Generate cross-references
          const crossRefCount = rng.int(0, cfg.avgCrossRefsPerCommodity * 2);
          for (let crIdx = 0; crIdx < crossRefCount; crIdx++) {
            crossReferences.push({
              crossref_id: crossRefId++,
              commodity_code: commodityCode,
              external_system: rng.pick(EXTERNAL_SYSTEMS),
              external_code: generateExternalCode(rng),
              confidence: rng.decimal(0.7, 1.0, 2),
            });
          }
        }
      }
    }
  }

  return {
    segments,
    families,
    classes,
    commodities,
    synonyms,
    crossReferences,
  };
}

/**
 * Generate a commodity name based on class and index
 */
function generateCommodityName(className: string, index: number, rng: SeededRandom): string {
  const prefixes = ['Standard', 'Premium', 'Industrial', 'Commercial', 'Heavy-duty', 'Lightweight', 'Compact', 'Professional', 'Economy', 'Deluxe'];
  const suffixes = ['', ' Type A', ' Type B', ' Series', ' Model', ' Grade', '', '', '', ''];

  const prefix = index < 5 ? '' : rng.pick(prefixes) + ' ';
  const suffix = rng.pick(suffixes);
  const baseName = className.endsWith('s') ? className.slice(0, -1) : className;

  return `${prefix}${baseName}${suffix}`.trim();
}

/**
 * Generate keywords for a commodity
 */
function generateKeywords(name: string, className: string, rng: SeededRandom): string {
  const baseKeywords = name.toLowerCase().split(' ');
  const classKeywords = className.toLowerCase().split(' ');
  const additionalKeywords = ['product', 'supply', 'equipment', 'material', 'component'];

  const keywords = Array.from(new Set([...baseKeywords, ...classKeywords, rng.pick(additionalKeywords)]));
  return keywords.slice(0, rng.int(3, 6)).join(', ');
}

/**
 * Generate a synonym for a commodity name
 */
function generateSynonym(name: string, rng: SeededRandom): string {
  const modifications = [
    (n: string) => n.toLowerCase(),
    (n: string) => n.toUpperCase(),
    (n: string) => n.split(' ').reverse().join(' '),
    (n: string) => n.replace(/\s+/g, '-'),
    (n: string) => `${n} (generic)`,
    (n: string) => `${n.split(' ')[0]} item`,
  ];

  return rng.pick(modifications)(name);
}

/**
 * Generate an external code
 */
function generateExternalCode(rng: SeededRandom): string {
  const formats = [
    () => `${rng.int(1000, 9999)}.${rng.int(10, 99)}.${rng.int(10, 99)}`,
    () => `${rng.int(100000, 999999)}`,
    () => `${String.fromCharCode(rng.int(65, 90))}${rng.int(1000, 9999)}`,
    () => `${rng.int(10, 99)}-${rng.int(1000, 9999)}`,
  ];

  return rng.pick(formats)();
}

// =============================================================================
// Benchmark Queries
// =============================================================================

/**
 * Query category for classification
 */
export type UnspscQueryCategory =
  | 'point_lookup'
  | 'prefix'
  | 'hierarchical'
  | 'tree_traversal'
  | 'aggregate'
  | 'text_search'
  | 'path';

/**
 * Benchmark query definition
 */
export interface UnspscBenchmarkQuery {
  /** Unique query identifier */
  id: string;
  /** Human-readable name */
  name: string;
  /** Query category */
  category: UnspscQueryCategory;
  /** SQL statement */
  sql: string;
  /** Query description */
  description: string;
  /** Expected result characteristics */
  expectedResults?: {
    minRows?: number;
    maxRows?: number;
  };
}

/**
 * Standard benchmark queries for UNSPSC dataset
 */
export const UNSPSC_BENCHMARK_QUERIES: UnspscBenchmarkQuery[] = [
  // Point Lookups
  {
    id: 'unspsc_point_01',
    name: 'Segment by Code',
    category: 'point_lookup',
    sql: "SELECT * FROM segments WHERE segment_code = '43'",
    description: 'Single segment lookup by code',
    expectedResults: { maxRows: 1 },
  },
  {
    id: 'unspsc_point_02',
    name: 'Family by Code',
    category: 'point_lookup',
    sql: "SELECT * FROM families WHERE family_code = '4310'",
    description: 'Single family lookup by code',
    expectedResults: { maxRows: 1 },
  },
  {
    id: 'unspsc_point_03',
    name: 'Class by Code',
    category: 'point_lookup',
    sql: "SELECT * FROM classes WHERE class_code = '431010'",
    description: 'Single class lookup by code',
    expectedResults: { maxRows: 1 },
  },
  {
    id: 'unspsc_point_04',
    name: 'Commodity by Code',
    category: 'point_lookup',
    sql: "SELECT * FROM commodities WHERE commodity_code = '43101001'",
    description: 'Single commodity lookup by code',
    expectedResults: { maxRows: 1 },
  },

  // Prefix Queries
  {
    id: 'unspsc_prefix_01',
    name: 'Families in Segment',
    category: 'prefix',
    sql: "SELECT * FROM families WHERE family_code LIKE '43%' ORDER BY family_code",
    description: 'All families starting with segment 43',
  },
  {
    id: 'unspsc_prefix_02',
    name: 'Classes in Family',
    category: 'prefix',
    sql: "SELECT * FROM classes WHERE class_code LIKE '4310%' ORDER BY class_code",
    description: 'All classes starting with family 4310',
  },
  {
    id: 'unspsc_prefix_03',
    name: 'Commodities by Prefix',
    category: 'prefix',
    sql: "SELECT * FROM commodities WHERE commodity_code LIKE '431010%' ORDER BY commodity_code",
    description: 'All commodities in class 431010',
  },
  {
    id: 'unspsc_prefix_04',
    name: 'Multi-Segment Search',
    category: 'prefix',
    sql: "SELECT * FROM commodities WHERE commodity_code LIKE '43%' OR commodity_code LIKE '44%' ORDER BY commodity_code LIMIT 100",
    description: 'Commodities from IT or Office segments',
    expectedResults: { maxRows: 100 },
  },

  // Hierarchical Queries
  {
    id: 'unspsc_hier_01',
    name: 'Children of Segment',
    category: 'hierarchical',
    sql: `
      SELECT f.family_code, f.name, COUNT(cl.class_code) as class_count
      FROM families f
      LEFT JOIN classes cl ON f.family_code = cl.family_code
      WHERE f.segment_code = '43'
      GROUP BY f.family_code, f.name
      ORDER BY f.sort_order
    `,
    description: 'Families in a segment with class counts',
  },
  {
    id: 'unspsc_hier_02',
    name: 'Children of Family',
    category: 'hierarchical',
    sql: `
      SELECT cl.class_code, cl.name, COUNT(co.commodity_code) as commodity_count
      FROM classes cl
      LEFT JOIN commodities co ON cl.class_code = co.class_code
      WHERE cl.family_code = '4310'
      GROUP BY cl.class_code, cl.name
      ORDER BY cl.sort_order
    `,
    description: 'Classes in a family with commodity counts',
  },
  {
    id: 'unspsc_hier_03',
    name: 'Direct Children',
    category: 'hierarchical',
    sql: `
      SELECT commodity_code, name, average_price
      FROM commodities
      WHERE class_code = '431010'
      ORDER BY sort_order
    `,
    description: 'Commodities directly under a class',
  },
  {
    id: 'unspsc_hier_04',
    name: 'Level Statistics',
    category: 'hierarchical',
    sql: `
      SELECT
        (SELECT COUNT(*) FROM segments) as segment_count,
        (SELECT COUNT(*) FROM families) as family_count,
        (SELECT COUNT(*) FROM classes) as class_count,
        (SELECT COUNT(*) FROM commodities) as commodity_count
    `,
    description: 'Count of items at each hierarchy level',
    expectedResults: { maxRows: 1 },
  },

  // Tree Traversal Queries
  {
    id: 'unspsc_tree_01',
    name: 'Full Tree Under Segment',
    category: 'tree_traversal',
    sql: `
      SELECT s.segment_code, s.name as segment_name,
             f.family_code, f.name as family_name,
             cl.class_code, cl.name as class_name
      FROM segments s
      JOIN families f ON s.segment_code = f.segment_code
      JOIN classes cl ON f.family_code = cl.family_code
      WHERE s.segment_code = '43'
      ORDER BY f.sort_order, cl.sort_order
    `,
    description: 'Complete tree structure under a segment',
  },
  {
    id: 'unspsc_tree_02',
    name: 'Leaf Nodes Only',
    category: 'tree_traversal',
    sql: `
      SELECT co.commodity_code, co.name, co.average_price
      FROM commodities co
      WHERE co.class_code IN (
        SELECT cl.class_code
        FROM classes cl
        JOIN families f ON cl.family_code = f.family_code
        WHERE f.segment_code = '43'
      )
      ORDER BY co.commodity_code
      LIMIT 100
    `,
    description: 'All commodities (leaves) under a segment',
    expectedResults: { maxRows: 100 },
  },
  {
    id: 'unspsc_tree_03',
    name: 'Breadcrumb Path',
    category: 'tree_traversal',
    sql: `
      SELECT co.commodity_code, co.name as commodity,
             cl.name as class, f.name as family, s.name as segment
      FROM commodities co
      JOIN classes cl ON co.class_code = cl.class_code
      JOIN families f ON cl.family_code = f.family_code
      JOIN segments s ON f.segment_code = s.segment_code
      WHERE co.commodity_code = '43101001'
    `,
    description: 'Full path from commodity to root',
    expectedResults: { maxRows: 1 },
  },

  // Path Queries
  {
    id: 'unspsc_path_01',
    name: 'Full Path for Commodities',
    category: 'path',
    sql: `
      SELECT
        co.commodity_code,
        s.segment_code || '/' || f.family_code || '/' || cl.class_code || '/' || co.commodity_code as full_path,
        s.name || ' > ' || f.name || ' > ' || cl.name || ' > ' || co.name as path_name
      FROM commodities co
      JOIN classes cl ON co.class_code = cl.class_code
      JOIN families f ON cl.family_code = f.family_code
      JOIN segments s ON f.segment_code = s.segment_code
      LIMIT 50
    `,
    description: 'Full hierarchical path for commodities',
    expectedResults: { maxRows: 50 },
  },
  {
    id: 'unspsc_path_02',
    name: 'Ancestor Search',
    category: 'path',
    sql: `
      SELECT s.name as segment, f.name as family, cl.name as class,
             COUNT(co.commodity_code) as commodity_count
      FROM segments s
      JOIN families f ON s.segment_code = f.segment_code
      JOIN classes cl ON f.family_code = cl.family_code
      LEFT JOIN commodities co ON cl.class_code = co.class_code
      WHERE s.name LIKE '%Technology%'
      GROUP BY s.segment_code, s.name, f.family_code, f.name, cl.class_code, cl.name
    `,
    description: 'Find all items under segments matching pattern',
  },
  {
    id: 'unspsc_path_03',
    name: 'Sibling Items',
    category: 'path',
    sql: `
      SELECT co.commodity_code, co.name
      FROM commodities co
      WHERE co.class_code = (
        SELECT class_code FROM commodities WHERE commodity_code = '43101001'
      )
      ORDER BY co.sort_order
    `,
    description: 'Commodities in same class as given commodity',
  },

  // Aggregate Queries
  {
    id: 'unspsc_agg_01',
    name: 'Segment Summary',
    category: 'aggregate',
    sql: `
      SELECT s.segment_code, s.name,
             COUNT(DISTINCT f.family_code) as family_count,
             COUNT(DISTINCT cl.class_code) as class_count,
             COUNT(DISTINCT co.commodity_code) as commodity_count
      FROM segments s
      LEFT JOIN families f ON s.segment_code = f.segment_code
      LEFT JOIN classes cl ON f.family_code = cl.family_code
      LEFT JOIN commodities co ON cl.class_code = co.class_code
      GROUP BY s.segment_code, s.name
      ORDER BY commodity_count DESC
    `,
    description: 'Summary statistics by segment',
  },
  {
    id: 'unspsc_agg_02',
    name: 'Price Statistics by Segment',
    category: 'aggregate',
    sql: `
      SELECT s.name,
             COUNT(co.commodity_code) as priced_count,
             AVG(co.average_price) as avg_price,
             MIN(co.average_price) as min_price,
             MAX(co.average_price) as max_price
      FROM segments s
      JOIN families f ON s.segment_code = f.segment_code
      JOIN classes cl ON f.family_code = cl.family_code
      JOIN commodities co ON cl.class_code = co.class_code
      WHERE co.average_price IS NOT NULL
      GROUP BY s.segment_code, s.name
      ORDER BY avg_price DESC
    `,
    description: 'Price statistics by segment',
  },
  {
    id: 'unspsc_agg_03',
    name: 'Unit of Measure Distribution',
    category: 'aggregate',
    sql: `
      SELECT unit_of_measure, COUNT(*) as count
      FROM commodities
      WHERE unit_of_measure IS NOT NULL
      GROUP BY unit_of_measure
      ORDER BY count DESC
      LIMIT 10
    `,
    description: 'Most common units of measure',
    expectedResults: { maxRows: 10 },
  },
  {
    id: 'unspsc_agg_04',
    name: 'Active vs Inactive by Level',
    category: 'aggregate',
    sql: `
      SELECT 'Segments' as level,
             SUM(is_active) as active, COUNT(*) - SUM(is_active) as inactive
      FROM segments
      UNION ALL
      SELECT 'Families', SUM(is_active), COUNT(*) - SUM(is_active) FROM families
      UNION ALL
      SELECT 'Classes', SUM(is_active), COUNT(*) - SUM(is_active) FROM classes
      UNION ALL
      SELECT 'Commodities', SUM(is_active), COUNT(*) - SUM(is_active) FROM commodities
    `,
    description: 'Active/inactive counts by hierarchy level',
    expectedResults: { maxRows: 4 },
  },
  {
    id: 'unspsc_agg_05',
    name: 'Synonym Language Distribution',
    category: 'aggregate',
    sql: `
      SELECT language, COUNT(*) as synonym_count
      FROM synonyms
      GROUP BY language
      ORDER BY synonym_count DESC
    `,
    description: 'Synonym counts by language',
  },

  // Text Search Queries
  {
    id: 'unspsc_text_01',
    name: 'Search Commodities by Name',
    category: 'text_search',
    sql: "SELECT * FROM commodities WHERE name LIKE '%Computer%' ORDER BY commodity_code",
    description: 'Commodities with Computer in name',
  },
  {
    id: 'unspsc_text_02',
    name: 'Search by Keywords',
    category: 'text_search',
    sql: "SELECT * FROM commodities WHERE keywords LIKE '%electronic%' LIMIT 50",
    description: 'Commodities with electronic keyword',
    expectedResults: { maxRows: 50 },
  },
  {
    id: 'unspsc_text_03',
    name: 'Search by Synonym',
    category: 'text_search',
    sql: `
      SELECT co.commodity_code, co.name, sy.term as synonym
      FROM commodities co
      JOIN synonyms sy ON co.commodity_code = sy.commodity_code
      WHERE sy.term LIKE '%server%'
      LIMIT 20
    `,
    description: 'Commodities matching synonym term',
    expectedResults: { maxRows: 20 },
  },
  {
    id: 'unspsc_text_04',
    name: 'Multi-Level Search',
    category: 'text_search',
    sql: `
      SELECT s.name as segment, f.name as family, co.name as commodity
      FROM commodities co
      JOIN classes cl ON co.class_code = cl.class_code
      JOIN families f ON cl.family_code = f.family_code
      JOIN segments s ON f.segment_code = s.segment_code
      WHERE co.name LIKE '%Software%' OR cl.name LIKE '%Software%' OR f.name LIKE '%Software%'
      ORDER BY s.sort_order, f.sort_order, co.commodity_code
      LIMIT 30
    `,
    description: 'Search across multiple hierarchy levels',
    expectedResults: { maxRows: 30 },
  },
  {
    id: 'unspsc_text_05',
    name: 'Cross-Reference Search',
    category: 'text_search',
    sql: `
      SELECT co.commodity_code, co.name, cr.external_system, cr.external_code
      FROM commodities co
      JOIN cross_references cr ON co.commodity_code = cr.commodity_code
      WHERE cr.external_system = 'HS Code'
      LIMIT 25
    `,
    description: 'Find commodities by external system',
    expectedResults: { maxRows: 25 },
  },
];

// =============================================================================
// Dataset Information
// =============================================================================

/**
 * UNSPSC dataset metadata
 */
export const UNSPSC_METADATA = {
  name: 'UNSPSC',
  version: '1.0.0',
  description: 'United Nations Standard Products and Services Code classification',
  tables: [
    'segments',
    'families',
    'classes',
    'commodities',
    'synonyms',
    'cross_references',
  ],
  approximateRows: 50000,
  queryCategories: [
    'point_lookup',
    'prefix',
    'hierarchical',
    'tree_traversal',
    'aggregate',
    'text_search',
    'path',
  ],
  queryCount: UNSPSC_BENCHMARK_QUERIES.length,
};

/**
 * Get all schema creation statements in order
 */
export function getUnspscSchemaStatements(): string[] {
  return [
    UNSPSC_SCHEMA.segments,
    UNSPSC_SCHEMA.families,
    UNSPSC_SCHEMA.classes,
    UNSPSC_SCHEMA.commodities,
    UNSPSC_SCHEMA.synonyms,
    UNSPSC_SCHEMA.cross_references,
    ...UNSPSC_INDEXES,
  ];
}

/**
 * Get queries by category
 */
export function getUnspscQueriesByCategory(
  category: UnspscQueryCategory
): UnspscBenchmarkQuery[] {
  return UNSPSC_BENCHMARK_QUERIES.filter((q) => q.category === category);
}
