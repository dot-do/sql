/**
 * Northwind Database Benchmark Dataset
 *
 * Classic Microsoft sample database for testing OLTP workloads.
 * Contains typical business entities: customers, orders, products, etc.
 *
 * Dataset characteristics:
 * - ~8,000 rows total across all tables
 * - Relational structure with foreign keys
 * - Date-based queries, joins, aggregations
 *
 * Query patterns tested:
 * - Point lookups by primary key
 * - Range queries on dates
 * - Multi-table joins
 * - Aggregations (COUNT, SUM, AVG)
 * - Filtering with multiple conditions
 *
 * @packageDocumentation
 */

// =============================================================================
// Type Definitions
// =============================================================================

/**
 * Customer entity
 */
export interface NorthwindCustomer {
  /** Unique customer identifier (5-char code) */
  customer_id: string;
  /** Company name */
  company_name: string;
  /** Contact person name */
  contact_name: string;
  /** Contact person title */
  contact_title: string;
  /** Street address */
  address: string;
  /** City */
  city: string;
  /** Region/state (nullable) */
  region: string | null;
  /** Postal code */
  postal_code: string;
  /** Country */
  country: string;
  /** Phone number */
  phone: string;
  /** Fax number (nullable) */
  fax: string | null;
}

/**
 * Employee entity
 */
export interface NorthwindEmployee {
  /** Unique employee identifier */
  employee_id: number;
  /** Last name */
  last_name: string;
  /** First name */
  first_name: string;
  /** Job title */
  title: string;
  /** Title of courtesy (Mr., Ms., etc.) */
  title_of_courtesy: string;
  /** Birth date (Unix timestamp) */
  birth_date: number;
  /** Hire date (Unix timestamp) */
  hire_date: number;
  /** Street address */
  address: string;
  /** City */
  city: string;
  /** Region/state (nullable) */
  region: string | null;
  /** Postal code */
  postal_code: string;
  /** Country */
  country: string;
  /** Home phone */
  home_phone: string;
  /** Extension */
  extension: string;
  /** Notes about employee */
  notes: string;
  /** Reports to (employee_id, nullable) */
  reports_to: number | null;
}

/**
 * Category entity
 */
export interface NorthwindCategory {
  /** Unique category identifier */
  category_id: number;
  /** Category name */
  category_name: string;
  /** Category description */
  description: string;
}

/**
 * Supplier entity
 */
export interface NorthwindSupplier {
  /** Unique supplier identifier */
  supplier_id: number;
  /** Company name */
  company_name: string;
  /** Contact person name */
  contact_name: string;
  /** Contact person title */
  contact_title: string;
  /** Street address */
  address: string;
  /** City */
  city: string;
  /** Region/state (nullable) */
  region: string | null;
  /** Postal code */
  postal_code: string;
  /** Country */
  country: string;
  /** Phone number */
  phone: string;
  /** Fax number (nullable) */
  fax: string | null;
  /** Home page URL (nullable) */
  home_page: string | null;
}

/**
 * Product entity
 */
export interface NorthwindProduct {
  /** Unique product identifier */
  product_id: number;
  /** Product name */
  product_name: string;
  /** Supplier identifier */
  supplier_id: number;
  /** Category identifier */
  category_id: number;
  /** Quantity per unit description */
  quantity_per_unit: string;
  /** Unit price */
  unit_price: number;
  /** Units currently in stock */
  units_in_stock: number;
  /** Units on order */
  units_on_order: number;
  /** Reorder level threshold */
  reorder_level: number;
  /** Whether product is discontinued */
  discontinued: number;
}

/**
 * Order entity
 */
export interface NorthwindOrder {
  /** Unique order identifier */
  order_id: number;
  /** Customer identifier */
  customer_id: string;
  /** Employee identifier */
  employee_id: number;
  /** Order date (Unix timestamp) */
  order_date: number;
  /** Required delivery date (Unix timestamp, nullable) */
  required_date: number | null;
  /** Shipped date (Unix timestamp, nullable) */
  shipped_date: number | null;
  /** Shipping method identifier */
  ship_via: number;
  /** Freight cost */
  freight: number;
  /** Ship-to name */
  ship_name: string;
  /** Ship-to address */
  ship_address: string;
  /** Ship-to city */
  ship_city: string;
  /** Ship-to region (nullable) */
  ship_region: string | null;
  /** Ship-to postal code */
  ship_postal_code: string;
  /** Ship-to country */
  ship_country: string;
}

/**
 * Order detail entity (line items)
 */
export interface NorthwindOrderDetail {
  /** Order identifier */
  order_id: number;
  /** Product identifier */
  product_id: number;
  /** Unit price at time of order */
  unit_price: number;
  /** Quantity ordered */
  quantity: number;
  /** Discount percentage (0.0-1.0) */
  discount: number;
}

/**
 * Shipper entity
 */
export interface NorthwindShipper {
  /** Unique shipper identifier */
  shipper_id: number;
  /** Company name */
  company_name: string;
  /** Phone number */
  phone: string;
}

// =============================================================================
// Schema Definitions
// =============================================================================

/**
 * SQL statements to create Northwind schema
 */
export const NORTHWIND_SCHEMA = {
  customers: `
    CREATE TABLE IF NOT EXISTS customers (
      customer_id TEXT PRIMARY KEY,
      company_name TEXT NOT NULL,
      contact_name TEXT NOT NULL,
      contact_title TEXT NOT NULL,
      address TEXT NOT NULL,
      city TEXT NOT NULL,
      region TEXT,
      postal_code TEXT NOT NULL,
      country TEXT NOT NULL,
      phone TEXT NOT NULL,
      fax TEXT
    )
  `,

  employees: `
    CREATE TABLE IF NOT EXISTS employees (
      employee_id INTEGER PRIMARY KEY,
      last_name TEXT NOT NULL,
      first_name TEXT NOT NULL,
      title TEXT NOT NULL,
      title_of_courtesy TEXT NOT NULL,
      birth_date INTEGER NOT NULL,
      hire_date INTEGER NOT NULL,
      address TEXT NOT NULL,
      city TEXT NOT NULL,
      region TEXT,
      postal_code TEXT NOT NULL,
      country TEXT NOT NULL,
      home_phone TEXT NOT NULL,
      extension TEXT NOT NULL,
      notes TEXT NOT NULL,
      reports_to INTEGER REFERENCES employees(employee_id)
    )
  `,

  categories: `
    CREATE TABLE IF NOT EXISTS categories (
      category_id INTEGER PRIMARY KEY,
      category_name TEXT NOT NULL,
      description TEXT NOT NULL
    )
  `,

  suppliers: `
    CREATE TABLE IF NOT EXISTS suppliers (
      supplier_id INTEGER PRIMARY KEY,
      company_name TEXT NOT NULL,
      contact_name TEXT NOT NULL,
      contact_title TEXT NOT NULL,
      address TEXT NOT NULL,
      city TEXT NOT NULL,
      region TEXT,
      postal_code TEXT NOT NULL,
      country TEXT NOT NULL,
      phone TEXT NOT NULL,
      fax TEXT,
      home_page TEXT
    )
  `,

  shippers: `
    CREATE TABLE IF NOT EXISTS shippers (
      shipper_id INTEGER PRIMARY KEY,
      company_name TEXT NOT NULL,
      phone TEXT NOT NULL
    )
  `,

  products: `
    CREATE TABLE IF NOT EXISTS products (
      product_id INTEGER PRIMARY KEY,
      product_name TEXT NOT NULL,
      supplier_id INTEGER NOT NULL REFERENCES suppliers(supplier_id),
      category_id INTEGER NOT NULL REFERENCES categories(category_id),
      quantity_per_unit TEXT NOT NULL,
      unit_price REAL NOT NULL,
      units_in_stock INTEGER NOT NULL,
      units_on_order INTEGER NOT NULL,
      reorder_level INTEGER NOT NULL,
      discontinued INTEGER NOT NULL DEFAULT 0
    )
  `,

  orders: `
    CREATE TABLE IF NOT EXISTS orders (
      order_id INTEGER PRIMARY KEY,
      customer_id TEXT NOT NULL REFERENCES customers(customer_id),
      employee_id INTEGER NOT NULL REFERENCES employees(employee_id),
      order_date INTEGER NOT NULL,
      required_date INTEGER,
      shipped_date INTEGER,
      ship_via INTEGER NOT NULL REFERENCES shippers(shipper_id),
      freight REAL NOT NULL,
      ship_name TEXT NOT NULL,
      ship_address TEXT NOT NULL,
      ship_city TEXT NOT NULL,
      ship_region TEXT,
      ship_postal_code TEXT NOT NULL,
      ship_country TEXT NOT NULL
    )
  `,

  order_details: `
    CREATE TABLE IF NOT EXISTS order_details (
      order_id INTEGER NOT NULL REFERENCES orders(order_id),
      product_id INTEGER NOT NULL REFERENCES products(product_id),
      unit_price REAL NOT NULL,
      quantity INTEGER NOT NULL,
      discount REAL NOT NULL DEFAULT 0,
      PRIMARY KEY (order_id, product_id)
    )
  `,
};

/**
 * Index creation statements for optimal query performance
 */
export const NORTHWIND_INDEXES = [
  // Customer indexes
  'CREATE INDEX IF NOT EXISTS idx_customers_country ON customers(country)',
  'CREATE INDEX IF NOT EXISTS idx_customers_city ON customers(city)',

  // Employee indexes
  'CREATE INDEX IF NOT EXISTS idx_employees_reports_to ON employees(reports_to)',
  'CREATE INDEX IF NOT EXISTS idx_employees_hire_date ON employees(hire_date)',

  // Product indexes
  'CREATE INDEX IF NOT EXISTS idx_products_category ON products(category_id)',
  'CREATE INDEX IF NOT EXISTS idx_products_supplier ON products(supplier_id)',
  'CREATE INDEX IF NOT EXISTS idx_products_discontinued ON products(discontinued)',

  // Order indexes
  'CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id)',
  'CREATE INDEX IF NOT EXISTS idx_orders_employee ON orders(employee_id)',
  'CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date)',
  'CREATE INDEX IF NOT EXISTS idx_orders_shipped ON orders(shipped_date)',

  // Order details indexes
  'CREATE INDEX IF NOT EXISTS idx_order_details_product ON order_details(product_id)',
];

// =============================================================================
// Seeded Random Number Generator
// =============================================================================

/**
 * Simple seeded PRNG (Mulberry32)
 * Provides deterministic random values for reproducible data generation
 */
class SeededRandom {
  private state: number;

  constructor(seed: number) {
    this.state = seed;
  }

  /** Generate random number between 0 and 1 */
  next(): number {
    let t = (this.state += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  /** Generate random integer in range [min, max] */
  int(min: number, max: number): number {
    return Math.floor(this.next() * (max - min + 1)) + min;
  }

  /** Pick random element from array */
  pick<T>(array: T[]): T {
    return array[this.int(0, array.length - 1)];
  }

  /** Generate random date within range (Unix timestamp) */
  date(startYear: number, endYear: number): number {
    const start = new Date(startYear, 0, 1).getTime();
    const end = new Date(endYear, 11, 31).getTime();
    return Math.floor(this.next() * (end - start) + start);
  }

  /** Generate random decimal number */
  decimal(min: number, max: number, decimals: number = 2): number {
    const value = this.next() * (max - min) + min;
    return Number(value.toFixed(decimals));
  }
}

// =============================================================================
// Reference Data
// =============================================================================

const COUNTRIES = [
  'USA', 'UK', 'Germany', 'France', 'Italy', 'Spain', 'Canada',
  'Brazil', 'Mexico', 'Japan', 'Australia', 'Netherlands', 'Belgium',
  'Switzerland', 'Austria', 'Sweden', 'Norway', 'Denmark', 'Finland', 'Ireland',
];

const CITIES: Record<string, string[]> = {
  USA: ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Seattle', 'Boston'],
  UK: ['London', 'Manchester', 'Birmingham', 'Leeds', 'Glasgow', 'Liverpool'],
  Germany: ['Berlin', 'Munich', 'Hamburg', 'Frankfurt', 'Cologne', 'Stuttgart'],
  France: ['Paris', 'Lyon', 'Marseille', 'Toulouse', 'Nice', 'Bordeaux'],
  Italy: ['Rome', 'Milan', 'Naples', 'Turin', 'Florence', 'Venice'],
  Spain: ['Madrid', 'Barcelona', 'Valencia', 'Seville', 'Bilbao', 'Malaga'],
  Canada: ['Toronto', 'Vancouver', 'Montreal', 'Calgary', 'Ottawa', 'Edmonton'],
  Brazil: ['Sao Paulo', 'Rio de Janeiro', 'Brasilia', 'Salvador', 'Fortaleza'],
  Mexico: ['Mexico City', 'Guadalajara', 'Monterrey', 'Puebla', 'Tijuana'],
  Japan: ['Tokyo', 'Osaka', 'Kyoto', 'Yokohama', 'Nagoya', 'Sapporo'],
  Australia: ['Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide'],
  Netherlands: ['Amsterdam', 'Rotterdam', 'The Hague', 'Utrecht', 'Eindhoven'],
  Belgium: ['Brussels', 'Antwerp', 'Ghent', 'Bruges', 'Liege'],
  Switzerland: ['Zurich', 'Geneva', 'Basel', 'Bern', 'Lausanne'],
  Austria: ['Vienna', 'Salzburg', 'Innsbruck', 'Graz', 'Linz'],
  Sweden: ['Stockholm', 'Gothenburg', 'Malmo', 'Uppsala', 'Vasteras'],
  Norway: ['Oslo', 'Bergen', 'Trondheim', 'Stavanger', 'Drammen'],
  Denmark: ['Copenhagen', 'Aarhus', 'Odense', 'Aalborg', 'Esbjerg'],
  Finland: ['Helsinki', 'Espoo', 'Tampere', 'Oulu', 'Turku'],
  Ireland: ['Dublin', 'Cork', 'Galway', 'Limerick', 'Waterford'],
};

const FIRST_NAMES = [
  'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
  'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
  'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Nancy', 'Daniel', 'Lisa',
];

const LAST_NAMES = [
  'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
  'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
  'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White',
];

const COMPANY_SUFFIXES = ['Inc.', 'Corp.', 'LLC', 'Ltd.', 'Group', 'Industries', 'Enterprises', 'Co.'];

const JOB_TITLES = [
  'Owner', 'Purchasing Manager', 'Sales Representative', 'Marketing Manager',
  'Order Administrator', 'Accounting Manager', 'Sales Manager', 'Vice President',
];

const EMPLOYEE_TITLES = [
  'Sales Representative', 'Vice President of Sales', 'Sales Manager',
  'Inside Sales Coordinator', 'Marketing Manager', 'Assistant',
];

const COURTESY_TITLES = ['Mr.', 'Ms.', 'Mrs.', 'Dr.'];

const CATEGORIES = [
  { name: 'Beverages', description: 'Soft drinks, coffees, teas, beers, and ales' },
  { name: 'Condiments', description: 'Sweet and savory sauces, relishes, spreads, and seasonings' },
  { name: 'Confections', description: 'Desserts, candies, and sweet breads' },
  { name: 'Dairy Products', description: 'Cheeses, milk, and dairy derivatives' },
  { name: 'Grains/Cereals', description: 'Breads, crackers, pasta, and cereal' },
  { name: 'Meat/Poultry', description: 'Prepared meats' },
  { name: 'Produce', description: 'Dried fruit and bean curd' },
  { name: 'Seafood', description: 'Seaweed and fish' },
];

const PRODUCTS_BY_CATEGORY: Record<string, string[]> = {
  Beverages: ['Chai', 'Chang', 'Chartreuse', 'Cote de Blaye', 'Ipoh Coffee', 'Lager', 'Outback Lager', 'Steeleye Stout'],
  Condiments: ['Aniseed Syrup', 'Cajun Seasoning', 'Curry Sauce', 'Genen Shouyu', 'Hot Pepper Sauce', 'Mustard', 'Sweet Chili Sauce'],
  Confections: ['Chocolade', 'Gumbar', 'Maxilaku', 'Pavlova', 'Scottish Shortbread', 'Tarte', 'Teatime Biscuits', 'Zaanse Koeken'],
  'Dairy Products': ['Camembert', 'Flotemysost', 'Geitost', 'Gorgonzola', 'Mascarpone', 'Mozzarella', 'Queso Cabrales', 'Queso Manchego'],
  'Grains/Cereals': ['Filo Mix', 'Gnocchi', 'Gustaf Kansen', 'Ravioli', 'Tunnbrod', 'Wimmers'],
  'Meat/Poultry': ['Alice Mutton', 'Mishi Kobe', 'Pate', 'Perth Pasties', 'Thuringer', 'Tourtiere'],
  Produce: ['Longlife Tofu', 'Manjimup Apples', 'Uncle Bob Organic', 'Rosle Sauerkraut', 'Tofu'],
  Seafood: ['Boston Crab', 'Carnarvon Tigers', 'Escargots', 'Gravad Lax', 'Ikura', 'Inlagd Sill', 'Konbu', 'Nord-Ost Matjeshering'],
};

const SHIPPERS = [
  { name: 'Speedy Express', phone: '(503) 555-9831' },
  { name: 'United Package', phone: '(503) 555-3199' },
  { name: 'Federal Shipping', phone: '(503) 555-9931' },
];

// =============================================================================
// Data Generator
// =============================================================================

/**
 * Configuration for Northwind data generation
 */
export interface NorthwindGeneratorConfig {
  /** Random seed for reproducibility */
  seed?: number;
  /** Number of customers to generate */
  customerCount?: number;
  /** Number of employees to generate */
  employeeCount?: number;
  /** Number of suppliers to generate */
  supplierCount?: number;
  /** Number of orders to generate */
  orderCount?: number;
  /** Average order details per order */
  avgOrderDetails?: number;
}

/**
 * Default configuration
 */
const DEFAULT_CONFIG: Required<NorthwindGeneratorConfig> = {
  seed: 42,
  customerCount: 91,
  employeeCount: 9,
  supplierCount: 29,
  orderCount: 830,
  avgOrderDetails: 3,
};

/**
 * Generated Northwind dataset
 */
export interface NorthwindDataset {
  customers: NorthwindCustomer[];
  employees: NorthwindEmployee[];
  categories: NorthwindCategory[];
  suppliers: NorthwindSupplier[];
  shippers: NorthwindShipper[];
  products: NorthwindProduct[];
  orders: NorthwindOrder[];
  orderDetails: NorthwindOrderDetail[];
}

/**
 * Generate a complete Northwind dataset with deterministic seeding
 *
 * @param config - Generation configuration
 * @returns Complete dataset with all entities
 *
 * @example
 * ```ts
 * const data = generateNorthwindData({ seed: 12345 });
 * console.log(data.customers.length); // 91 (default)
 * ```
 */
export function generateNorthwindData(
  config: NorthwindGeneratorConfig = {}
): NorthwindDataset {
  const cfg = { ...DEFAULT_CONFIG, ...config };
  const rng = new SeededRandom(cfg.seed);

  // Generate categories (static data)
  const categories: NorthwindCategory[] = CATEGORIES.map((cat, i) => ({
    category_id: i + 1,
    category_name: cat.name,
    description: cat.description,
  }));

  // Generate shippers (static data)
  const shippers: NorthwindShipper[] = SHIPPERS.map((ship, i) => ({
    shipper_id: i + 1,
    company_name: ship.name,
    phone: ship.phone,
  }));

  // Generate suppliers
  const suppliers: NorthwindSupplier[] = [];
  for (let i = 0; i < cfg.supplierCount; i++) {
    const country = rng.pick(COUNTRIES);
    const city = rng.pick(CITIES[country] || CITIES.USA);
    suppliers.push({
      supplier_id: i + 1,
      company_name: `${rng.pick(LAST_NAMES)} ${rng.pick(COMPANY_SUFFIXES)}`,
      contact_name: `${rng.pick(FIRST_NAMES)} ${rng.pick(LAST_NAMES)}`,
      contact_title: rng.pick(JOB_TITLES),
      address: `${rng.int(1, 9999)} ${rng.pick(LAST_NAMES)} Street`,
      city,
      region: rng.next() > 0.7 ? null : `Region ${rng.int(1, 10)}`,
      postal_code: String(rng.int(10000, 99999)),
      country,
      phone: `(${rng.int(100, 999)}) ${rng.int(100, 999)}-${rng.int(1000, 9999)}`,
      fax: rng.next() > 0.5 ? `(${rng.int(100, 999)}) ${rng.int(100, 999)}-${rng.int(1000, 9999)}` : null,
      home_page: rng.next() > 0.7 ? `https://www.supplier${i + 1}.com` : null,
    });
  }

  // Generate products
  const products: NorthwindProduct[] = [];
  let productId = 1;
  for (const category of categories) {
    const productNames = PRODUCTS_BY_CATEGORY[category.category_name] || [];
    for (const productName of productNames) {
      products.push({
        product_id: productId++,
        product_name: productName,
        supplier_id: rng.int(1, suppliers.length),
        category_id: category.category_id,
        quantity_per_unit: `${rng.int(1, 48)} units`,
        unit_price: rng.decimal(5, 250, 2),
        units_in_stock: rng.int(0, 125),
        units_on_order: rng.int(0, 100),
        reorder_level: rng.int(5, 30),
        discontinued: rng.next() > 0.9 ? 1 : 0,
      });
    }
  }

  // Generate employees
  const employees: NorthwindEmployee[] = [];
  for (let i = 0; i < cfg.employeeCount; i++) {
    const country = rng.pick(['USA', 'UK']);
    const city = rng.pick(CITIES[country]);
    employees.push({
      employee_id: i + 1,
      last_name: rng.pick(LAST_NAMES),
      first_name: rng.pick(FIRST_NAMES),
      title: rng.pick(EMPLOYEE_TITLES),
      title_of_courtesy: rng.pick(COURTESY_TITLES),
      birth_date: rng.date(1950, 1975),
      hire_date: rng.date(2015, 2024),
      address: `${rng.int(1, 9999)} ${rng.pick(LAST_NAMES)} Ave`,
      city,
      region: rng.next() > 0.7 ? null : `Region ${rng.int(1, 5)}`,
      postal_code: String(rng.int(10000, 99999)),
      country,
      home_phone: `(${rng.int(100, 999)}) ${rng.int(100, 999)}-${rng.int(1000, 9999)}`,
      extension: String(rng.int(100, 999)),
      notes: `Employee ${i + 1} notes.`,
      reports_to: i === 0 ? null : rng.int(1, Math.min(i, 3)),
    });
  }

  // Generate customers
  const customers: NorthwindCustomer[] = [];
  for (let i = 0; i < cfg.customerCount; i++) {
    const country = rng.pick(COUNTRIES);
    const city = rng.pick(CITIES[country] || CITIES.USA);
    const lastName = rng.pick(LAST_NAMES);
    customers.push({
      customer_id: lastName.substring(0, 5).toUpperCase(),
      company_name: `${lastName} ${rng.pick(COMPANY_SUFFIXES)}`,
      contact_name: `${rng.pick(FIRST_NAMES)} ${lastName}`,
      contact_title: rng.pick(JOB_TITLES),
      address: `${rng.int(1, 9999)} ${rng.pick(LAST_NAMES)} Blvd`,
      city,
      region: rng.next() > 0.6 ? null : `Region ${rng.int(1, 10)}`,
      postal_code: String(rng.int(10000, 99999)),
      country,
      phone: `(${rng.int(100, 999)}) ${rng.int(100, 999)}-${rng.int(1000, 9999)}`,
      fax: rng.next() > 0.5 ? `(${rng.int(100, 999)}) ${rng.int(100, 999)}-${rng.int(1000, 9999)}` : null,
    });
  }

  // Ensure unique customer IDs
  const usedIds = new Set<string>();
  customers.forEach((c, i) => {
    let id = c.customer_id;
    let suffix = 0;
    while (usedIds.has(id)) {
      id = c.customer_id.substring(0, 4) + String(suffix++);
    }
    usedIds.add(id);
    customers[i].customer_id = id;
  });

  // Generate orders
  const orders: NorthwindOrder[] = [];
  const orderDetails: NorthwindOrderDetail[] = [];

  for (let i = 0; i < cfg.orderCount; i++) {
    const customer = rng.pick(customers);
    const orderDate = rng.date(2022, 2025);
    const requiredDate = orderDate + rng.int(7, 30) * 24 * 60 * 60 * 1000;
    const shipped = rng.next() > 0.1;
    const shippedDate = shipped ? orderDate + rng.int(1, 14) * 24 * 60 * 60 * 1000 : null;

    orders.push({
      order_id: 10248 + i,
      customer_id: customer.customer_id,
      employee_id: rng.int(1, employees.length),
      order_date: orderDate,
      required_date: requiredDate,
      shipped_date: shippedDate,
      ship_via: rng.int(1, shippers.length),
      freight: rng.decimal(5, 500, 2),
      ship_name: customer.company_name,
      ship_address: customer.address,
      ship_city: customer.city,
      ship_region: customer.region,
      ship_postal_code: customer.postal_code,
      ship_country: customer.country,
    });

    // Generate order details
    const detailCount = rng.int(1, cfg.avgOrderDetails * 2);
    const usedProducts = new Set<number>();

    for (let j = 0; j < detailCount; j++) {
      let product: NorthwindProduct;
      do {
        product = rng.pick(products);
      } while (usedProducts.has(product.product_id));
      usedProducts.add(product.product_id);

      orderDetails.push({
        order_id: 10248 + i,
        product_id: product.product_id,
        unit_price: product.unit_price,
        quantity: rng.int(1, 50),
        discount: rng.next() > 0.7 ? rng.decimal(0.05, 0.25, 2) : 0,
      });
    }
  }

  return {
    customers,
    employees,
    categories,
    suppliers,
    shippers,
    products,
    orders,
    orderDetails,
  };
}

// =============================================================================
// Benchmark Queries
// =============================================================================

/**
 * Query category for classification
 */
export type NorthwindQueryCategory =
  | 'point_lookup'
  | 'range'
  | 'join'
  | 'aggregate'
  | 'text_search'
  | 'date_filter'
  | 'subquery';

/**
 * Benchmark query definition
 */
export interface NorthwindBenchmarkQuery {
  /** Unique query identifier */
  id: string;
  /** Human-readable name */
  name: string;
  /** Query category */
  category: NorthwindQueryCategory;
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
 * Standard benchmark queries for Northwind dataset
 *
 * Contains 25+ queries covering:
 * - Point lookups
 * - Range queries
 * - Multi-table joins
 * - Aggregations
 * - Text search
 * - Date filtering
 * - Subqueries
 */
export const NORTHWIND_BENCHMARK_QUERIES: NorthwindBenchmarkQuery[] = [
  // Point Lookups
  {
    id: 'nw_point_01',
    name: 'Customer by ID',
    category: 'point_lookup',
    sql: "SELECT * FROM customers WHERE customer_id = 'ALFKI'",
    description: 'Single customer lookup by primary key',
    expectedResults: { minRows: 0, maxRows: 1 },
  },
  {
    id: 'nw_point_02',
    name: 'Product by ID',
    category: 'point_lookup',
    sql: 'SELECT * FROM products WHERE product_id = 1',
    description: 'Single product lookup by primary key',
    expectedResults: { minRows: 1, maxRows: 1 },
  },
  {
    id: 'nw_point_03',
    name: 'Order by ID',
    category: 'point_lookup',
    sql: 'SELECT * FROM orders WHERE order_id = 10248',
    description: 'Single order lookup by primary key',
    expectedResults: { minRows: 1, maxRows: 1 },
  },
  {
    id: 'nw_point_04',
    name: 'Employee by ID',
    category: 'point_lookup',
    sql: 'SELECT * FROM employees WHERE employee_id = 1',
    description: 'Single employee lookup by primary key',
    expectedResults: { minRows: 1, maxRows: 1 },
  },

  // Range Queries
  {
    id: 'nw_range_01',
    name: 'Products by Price Range',
    category: 'range',
    sql: 'SELECT * FROM products WHERE unit_price BETWEEN 10 AND 50 ORDER BY unit_price',
    description: 'Products within a price range',
    expectedResults: { minRows: 10 },
  },
  {
    id: 'nw_range_02',
    name: 'Orders by Date Range',
    category: 'range',
    sql: 'SELECT * FROM orders WHERE order_date BETWEEN 1672531200000 AND 1704067200000',
    description: 'Orders within 2023 date range',
    expectedResults: { minRows: 50 },
  },
  {
    id: 'nw_range_03',
    name: 'Low Stock Products',
    category: 'range',
    sql: 'SELECT * FROM products WHERE units_in_stock < reorder_level AND discontinued = 0',
    description: 'Products needing reorder',
  },
  {
    id: 'nw_range_04',
    name: 'High Value Orders',
    category: 'range',
    sql: 'SELECT * FROM orders WHERE freight > 100 ORDER BY freight DESC LIMIT 20',
    description: 'Top 20 orders by freight cost',
    expectedResults: { maxRows: 20 },
  },

  // Join Queries
  {
    id: 'nw_join_01',
    name: 'Orders with Customer Info',
    category: 'join',
    sql: `
      SELECT o.order_id, o.order_date, c.company_name, c.contact_name
      FROM orders o
      JOIN customers c ON o.customer_id = c.customer_id
      LIMIT 100
    `,
    description: 'Orders joined with customer details',
    expectedResults: { maxRows: 100 },
  },
  {
    id: 'nw_join_02',
    name: 'Order Details with Product',
    category: 'join',
    sql: `
      SELECT od.order_id, p.product_name, od.quantity, od.unit_price, od.discount
      FROM order_details od
      JOIN products p ON od.product_id = p.product_id
      WHERE od.order_id = 10248
    `,
    description: 'Line items with product details for specific order',
  },
  {
    id: 'nw_join_03',
    name: 'Products with Supplier and Category',
    category: 'join',
    sql: `
      SELECT p.product_name, s.company_name as supplier, c.category_name
      FROM products p
      JOIN suppliers s ON p.supplier_id = s.supplier_id
      JOIN categories c ON p.category_id = c.category_id
      ORDER BY c.category_name, p.product_name
    `,
    description: 'Three-way join: products, suppliers, categories',
  },
  {
    id: 'nw_join_04',
    name: 'Employee Hierarchy',
    category: 'join',
    sql: `
      SELECT e.employee_id, e.first_name || ' ' || e.last_name as employee,
             m.first_name || ' ' || m.last_name as manager
      FROM employees e
      LEFT JOIN employees m ON e.reports_to = m.employee_id
    `,
    description: 'Self-join for employee manager hierarchy',
  },
  {
    id: 'nw_join_05',
    name: 'Complete Order Information',
    category: 'join',
    sql: `
      SELECT o.order_id, c.company_name, e.first_name || ' ' || e.last_name as employee,
             sh.company_name as shipper, o.freight
      FROM orders o
      JOIN customers c ON o.customer_id = c.customer_id
      JOIN employees e ON o.employee_id = e.employee_id
      JOIN shippers sh ON o.ship_via = sh.shipper_id
      LIMIT 50
    `,
    description: 'Four-way join: orders, customers, employees, shippers',
    expectedResults: { maxRows: 50 },
  },

  // Aggregate Queries
  {
    id: 'nw_agg_01',
    name: 'Orders by Country',
    category: 'aggregate',
    sql: `
      SELECT ship_country, COUNT(*) as order_count
      FROM orders
      GROUP BY ship_country
      ORDER BY order_count DESC
    `,
    description: 'Order count by shipping country',
  },
  {
    id: 'nw_agg_02',
    name: 'Sales by Category',
    category: 'aggregate',
    sql: `
      SELECT c.category_name,
             SUM(od.quantity * od.unit_price * (1 - od.discount)) as total_sales
      FROM order_details od
      JOIN products p ON od.product_id = p.product_id
      JOIN categories c ON p.category_id = c.category_id
      GROUP BY c.category_name
      ORDER BY total_sales DESC
    `,
    description: 'Total sales by product category',
  },
  {
    id: 'nw_agg_03',
    name: 'Average Order Value by Customer',
    category: 'aggregate',
    sql: `
      SELECT c.company_name,
             COUNT(DISTINCT o.order_id) as order_count,
             AVG(od.quantity * od.unit_price) as avg_line_value
      FROM customers c
      JOIN orders o ON c.customer_id = o.customer_id
      JOIN order_details od ON o.order_id = od.order_id
      GROUP BY c.customer_id, c.company_name
      HAVING COUNT(DISTINCT o.order_id) > 5
      ORDER BY avg_line_value DESC
      LIMIT 10
    `,
    description: 'Top 10 customers by average order line value',
    expectedResults: { maxRows: 10 },
  },
  {
    id: 'nw_agg_04',
    name: 'Product Stock Summary',
    category: 'aggregate',
    sql: `
      SELECT c.category_name,
             COUNT(*) as product_count,
             SUM(p.units_in_stock) as total_stock,
             AVG(p.unit_price) as avg_price
      FROM products p
      JOIN categories c ON p.category_id = c.category_id
      GROUP BY c.category_name
    `,
    description: 'Stock summary by category',
  },
  {
    id: 'nw_agg_05',
    name: 'Employee Performance',
    category: 'aggregate',
    sql: `
      SELECT e.first_name || ' ' || e.last_name as employee,
             COUNT(o.order_id) as orders_handled,
             SUM(o.freight) as total_freight
      FROM employees e
      LEFT JOIN orders o ON e.employee_id = o.employee_id
      GROUP BY e.employee_id
      ORDER BY orders_handled DESC
    `,
    description: 'Order count and freight by employee',
  },

  // Text Search Queries
  {
    id: 'nw_text_01',
    name: 'Customers by Company Name',
    category: 'text_search',
    sql: "SELECT * FROM customers WHERE company_name LIKE '%Inc%'",
    description: 'Customers with Inc in company name',
  },
  {
    id: 'nw_text_02',
    name: 'Products Starting With',
    category: 'text_search',
    sql: "SELECT * FROM products WHERE product_name LIKE 'C%' ORDER BY product_name",
    description: 'Products starting with C',
  },
  {
    id: 'nw_text_03',
    name: 'Suppliers in City',
    category: 'text_search',
    sql: "SELECT * FROM suppliers WHERE city LIKE '%on%'",
    description: 'Suppliers in cities containing "on"',
  },

  // Date Filter Queries
  {
    id: 'nw_date_01',
    name: 'Recent Orders',
    category: 'date_filter',
    sql: `
      SELECT * FROM orders
      WHERE order_date > (SELECT MAX(order_date) - 30*24*60*60*1000 FROM orders)
      ORDER BY order_date DESC
    `,
    description: 'Orders from last 30 days of data',
  },
  {
    id: 'nw_date_02',
    name: 'Unshipped Orders',
    category: 'date_filter',
    sql: 'SELECT * FROM orders WHERE shipped_date IS NULL ORDER BY order_date',
    description: 'Orders that have not been shipped',
  },
  {
    id: 'nw_date_03',
    name: 'Late Shipments',
    category: 'date_filter',
    sql: `
      SELECT order_id, order_date, required_date, shipped_date
      FROM orders
      WHERE shipped_date > required_date
      ORDER BY shipped_date - required_date DESC
    `,
    description: 'Orders shipped after required date',
  },

  // Subqueries
  {
    id: 'nw_sub_01',
    name: 'Above Average Price Products',
    category: 'subquery',
    sql: `
      SELECT product_name, unit_price
      FROM products
      WHERE unit_price > (SELECT AVG(unit_price) FROM products)
      ORDER BY unit_price DESC
    `,
    description: 'Products priced above average',
  },
  {
    id: 'nw_sub_02',
    name: 'Customers With Orders',
    category: 'subquery',
    sql: `
      SELECT customer_id, company_name
      FROM customers
      WHERE customer_id IN (SELECT DISTINCT customer_id FROM orders)
    `,
    description: 'Customers who have placed orders',
  },
  {
    id: 'nw_sub_03',
    name: 'Top Selling Products',
    category: 'subquery',
    sql: `
      SELECT p.product_name,
             (SELECT SUM(quantity) FROM order_details WHERE product_id = p.product_id) as total_qty
      FROM products p
      ORDER BY total_qty DESC NULLS LAST
      LIMIT 10
    `,
    description: 'Top 10 products by quantity sold',
    expectedResults: { maxRows: 10 },
  },
];

// =============================================================================
// Dataset Information
// =============================================================================

/**
 * Northwind dataset metadata
 */
export const NORTHWIND_METADATA = {
  name: 'Northwind',
  version: '1.0.0',
  description: 'Classic Microsoft sample database for OLTP workloads',
  tables: [
    'customers',
    'employees',
    'categories',
    'suppliers',
    'shippers',
    'products',
    'orders',
    'order_details',
  ],
  approximateRows: 8000,
  queryCategories: [
    'point_lookup',
    'range',
    'join',
    'aggregate',
    'text_search',
    'date_filter',
    'subquery',
  ],
  queryCount: NORTHWIND_BENCHMARK_QUERIES.length,
};

/**
 * Get all schema creation statements in order
 */
export function getNorthwindSchemaStatements(): string[] {
  return [
    NORTHWIND_SCHEMA.categories,
    NORTHWIND_SCHEMA.suppliers,
    NORTHWIND_SCHEMA.shippers,
    NORTHWIND_SCHEMA.products,
    NORTHWIND_SCHEMA.employees,
    NORTHWIND_SCHEMA.customers,
    NORTHWIND_SCHEMA.orders,
    NORTHWIND_SCHEMA.order_details,
    ...NORTHWIND_INDEXES,
  ];
}

/**
 * Get queries by category
 */
export function getNorthwindQueriesByCategory(
  category: NorthwindQueryCategory
): NorthwindBenchmarkQuery[] {
  return NORTHWIND_BENCHMARK_QUERIES.filter((q) => q.category === category);
}
