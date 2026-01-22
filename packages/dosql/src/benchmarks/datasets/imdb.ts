/**
 * IMDB Database Benchmark Dataset
 *
 * Movie database subset for testing complex join patterns.
 * Contains movies, actors, directors, roles, and ratings.
 *
 * Dataset characteristics:
 * - ~100,000 rows total across all tables
 * - Many-to-many relationships (movies <-> actors)
 * - Self-referential data (sequels)
 * - Heavy aggregation patterns
 *
 * Query patterns tested:
 * - Many-to-many joins
 * - Top-N queries with rankings
 * - Complex aggregations
 * - Filtering with multiple conditions
 * - Self-joins
 *
 * @packageDocumentation
 */

// =============================================================================
// Type Definitions
// =============================================================================

/**
 * Movie entity
 */
export interface ImdbMovie {
  /** Unique movie identifier */
  movie_id: number;
  /** Movie title */
  title: string;
  /** Original title (if different) */
  original_title: string | null;
  /** Release year */
  release_year: number;
  /** Runtime in minutes */
  runtime_minutes: number;
  /** Primary genre */
  genre: string;
  /** Secondary genre (nullable) */
  genre_secondary: string | null;
  /** Budget in USD (nullable) */
  budget: number | null;
  /** Box office revenue in USD (nullable) */
  revenue: number | null;
  /** Movie tagline */
  tagline: string | null;
  /** Plot summary */
  overview: string;
  /** Production country */
  production_country: string;
  /** Original language */
  original_language: string;
  /** Release status */
  status: 'Released' | 'Post Production' | 'In Production' | 'Planned' | 'Canceled';
  /** Sequel to movie_id (nullable) */
  sequel_to: number | null;
}

/**
 * Person entity (actors, directors, crew)
 */
export interface ImdbPerson {
  /** Unique person identifier */
  person_id: number;
  /** Full name */
  name: string;
  /** Date of birth (Unix timestamp) */
  birth_date: number | null;
  /** Date of death (Unix timestamp, nullable) */
  death_date: number | null;
  /** Place of birth */
  birth_place: string | null;
  /** Biography */
  biography: string | null;
  /** Primary profession */
  primary_profession: 'Actor' | 'Director' | 'Producer' | 'Writer' | 'Cinematographer' | 'Composer' | 'Editor';
  /** Gender */
  gender: 'Male' | 'Female' | 'Other';
  /** Popularity score (0-100) */
  popularity: number;
}

/**
 * Movie-Actor relationship with role information
 */
export interface ImdbRole {
  /** Movie identifier */
  movie_id: number;
  /** Person identifier */
  person_id: number;
  /** Character name */
  character_name: string;
  /** Billing order (1 = lead) */
  billing_order: number;
  /** Role type */
  role_type: 'Lead' | 'Supporting' | 'Cameo' | 'Voice' | 'Uncredited';
}

/**
 * Movie-Director relationship
 */
export interface ImdbDirector {
  /** Movie identifier */
  movie_id: number;
  /** Person identifier (director) */
  person_id: number;
  /** Co-director indicator */
  is_co_director: number;
}

/**
 * Movie rating/review data
 */
export interface ImdbRating {
  /** Movie identifier */
  movie_id: number;
  /** Average rating (0.0-10.0) */
  rating: number;
  /** Number of votes */
  vote_count: number;
  /** Rating from professional critics (0.0-100.0, nullable) */
  critic_score: number | null;
  /** Rating from audience (0.0-100.0, nullable) */
  audience_score: number | null;
  /** Last updated (Unix timestamp) */
  updated_at: number;
}

/**
 * Award nomination/win record
 */
export interface ImdbAward {
  /** Unique award identifier */
  award_id: number;
  /** Movie identifier */
  movie_id: number;
  /** Person identifier (nullable - can be movie-level award) */
  person_id: number | null;
  /** Award ceremony name */
  ceremony: string;
  /** Award category */
  category: string;
  /** Year of ceremony */
  year: number;
  /** Whether won (1) or just nominated (0) */
  won: number;
}

/**
 * Genre reference table
 */
export interface ImdbGenre {
  /** Unique genre identifier */
  genre_id: number;
  /** Genre name */
  name: string;
  /** Genre description */
  description: string;
}

/**
 * Movie-Genre many-to-many relationship
 */
export interface ImdbMovieGenre {
  /** Movie identifier */
  movie_id: number;
  /** Genre identifier */
  genre_id: number;
  /** Whether primary genre for movie */
  is_primary: number;
}

// =============================================================================
// Schema Definitions
// =============================================================================

/**
 * SQL statements to create IMDB schema
 */
export const IMDB_SCHEMA = {
  genres: `
    CREATE TABLE IF NOT EXISTS genres (
      genre_id INTEGER PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      description TEXT NOT NULL
    )
  `,

  movies: `
    CREATE TABLE IF NOT EXISTS movies (
      movie_id INTEGER PRIMARY KEY,
      title TEXT NOT NULL,
      original_title TEXT,
      release_year INTEGER NOT NULL,
      runtime_minutes INTEGER NOT NULL,
      genre TEXT NOT NULL,
      genre_secondary TEXT,
      budget INTEGER,
      revenue INTEGER,
      tagline TEXT,
      overview TEXT NOT NULL,
      production_country TEXT NOT NULL,
      original_language TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'Released',
      sequel_to INTEGER REFERENCES movies(movie_id)
    )
  `,

  movie_genres: `
    CREATE TABLE IF NOT EXISTS movie_genres (
      movie_id INTEGER NOT NULL REFERENCES movies(movie_id),
      genre_id INTEGER NOT NULL REFERENCES genres(genre_id),
      is_primary INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (movie_id, genre_id)
    )
  `,

  persons: `
    CREATE TABLE IF NOT EXISTS persons (
      person_id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      birth_date INTEGER,
      death_date INTEGER,
      birth_place TEXT,
      biography TEXT,
      primary_profession TEXT NOT NULL,
      gender TEXT NOT NULL,
      popularity REAL NOT NULL DEFAULT 0
    )
  `,

  roles: `
    CREATE TABLE IF NOT EXISTS roles (
      movie_id INTEGER NOT NULL REFERENCES movies(movie_id),
      person_id INTEGER NOT NULL REFERENCES persons(person_id),
      character_name TEXT NOT NULL,
      billing_order INTEGER NOT NULL,
      role_type TEXT NOT NULL DEFAULT 'Supporting',
      PRIMARY KEY (movie_id, person_id, character_name)
    )
  `,

  directors: `
    CREATE TABLE IF NOT EXISTS directors (
      movie_id INTEGER NOT NULL REFERENCES movies(movie_id),
      person_id INTEGER NOT NULL REFERENCES persons(person_id),
      is_co_director INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (movie_id, person_id)
    )
  `,

  ratings: `
    CREATE TABLE IF NOT EXISTS ratings (
      movie_id INTEGER PRIMARY KEY REFERENCES movies(movie_id),
      rating REAL NOT NULL,
      vote_count INTEGER NOT NULL,
      critic_score REAL,
      audience_score REAL,
      updated_at INTEGER NOT NULL
    )
  `,

  awards: `
    CREATE TABLE IF NOT EXISTS awards (
      award_id INTEGER PRIMARY KEY,
      movie_id INTEGER NOT NULL REFERENCES movies(movie_id),
      person_id INTEGER REFERENCES persons(person_id),
      ceremony TEXT NOT NULL,
      category TEXT NOT NULL,
      year INTEGER NOT NULL,
      won INTEGER NOT NULL DEFAULT 0
    )
  `,
};

/**
 * Index creation statements for optimal query performance
 */
export const IMDB_INDEXES = [
  // Movie indexes
  'CREATE INDEX IF NOT EXISTS idx_movies_year ON movies(release_year)',
  'CREATE INDEX IF NOT EXISTS idx_movies_genre ON movies(genre)',
  'CREATE INDEX IF NOT EXISTS idx_movies_status ON movies(status)',
  'CREATE INDEX IF NOT EXISTS idx_movies_sequel ON movies(sequel_to)',
  'CREATE INDEX IF NOT EXISTS idx_movies_country ON movies(production_country)',
  'CREATE INDEX IF NOT EXISTS idx_movies_language ON movies(original_language)',

  // Person indexes
  'CREATE INDEX IF NOT EXISTS idx_persons_profession ON persons(primary_profession)',
  'CREATE INDEX IF NOT EXISTS idx_persons_popularity ON persons(popularity)',
  'CREATE INDEX IF NOT EXISTS idx_persons_gender ON persons(gender)',

  // Role indexes
  'CREATE INDEX IF NOT EXISTS idx_roles_person ON roles(person_id)',
  'CREATE INDEX IF NOT EXISTS idx_roles_billing ON roles(billing_order)',
  'CREATE INDEX IF NOT EXISTS idx_roles_type ON roles(role_type)',

  // Director indexes
  'CREATE INDEX IF NOT EXISTS idx_directors_person ON directors(person_id)',

  // Rating indexes
  'CREATE INDEX IF NOT EXISTS idx_ratings_rating ON ratings(rating)',
  'CREATE INDEX IF NOT EXISTS idx_ratings_votes ON ratings(vote_count)',

  // Award indexes
  'CREATE INDEX IF NOT EXISTS idx_awards_movie ON awards(movie_id)',
  'CREATE INDEX IF NOT EXISTS idx_awards_person ON awards(person_id)',
  'CREATE INDEX IF NOT EXISTS idx_awards_ceremony ON awards(ceremony)',
  'CREATE INDEX IF NOT EXISTS idx_awards_year ON awards(year)',
  'CREATE INDEX IF NOT EXISTS idx_awards_won ON awards(won)',

  // Movie-Genre indexes
  'CREATE INDEX IF NOT EXISTS idx_movie_genres_genre ON movie_genres(genre_id)',
  'CREATE INDEX IF NOT EXISTS idx_movie_genres_primary ON movie_genres(is_primary)',
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

  pickMultiple<T>(array: T[], count: number): T[] {
    const shuffled = [...array].sort(() => this.next() - 0.5);
    return shuffled.slice(0, Math.min(count, array.length));
  }

  date(startYear: number, endYear: number): number {
    const start = new Date(startYear, 0, 1).getTime();
    const end = new Date(endYear, 11, 31).getTime();
    return Math.floor(this.next() * (end - start) + start);
  }

  bool(probability: number = 0.5): boolean {
    return this.next() < probability;
  }
}

// =============================================================================
// Reference Data
// =============================================================================

const GENRES = [
  { name: 'Action', description: 'High-energy films with physical stunts, chases, and fights' },
  { name: 'Adventure', description: 'Films featuring exploration and exciting journeys' },
  { name: 'Animation', description: 'Films created using animation techniques' },
  { name: 'Comedy', description: 'Films intended to make audiences laugh' },
  { name: 'Crime', description: 'Films centered around criminal activities' },
  { name: 'Documentary', description: 'Non-fiction films about real events' },
  { name: 'Drama', description: 'Serious films focused on emotional themes' },
  { name: 'Family', description: 'Films suitable for all ages' },
  { name: 'Fantasy', description: 'Films with magical or supernatural elements' },
  { name: 'History', description: 'Films depicting historical events' },
  { name: 'Horror', description: 'Films intended to frighten audiences' },
  { name: 'Music', description: 'Films centered around music and musicians' },
  { name: 'Mystery', description: 'Films involving puzzles and investigations' },
  { name: 'Romance', description: 'Films focused on romantic relationships' },
  { name: 'Science Fiction', description: 'Films with futuristic or scientific themes' },
  { name: 'Thriller', description: 'Films designed to create suspense and tension' },
  { name: 'War', description: 'Films depicting warfare and its effects' },
  { name: 'Western', description: 'Films set in the American Old West' },
];

const FIRST_NAMES_MALE = [
  'James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Joseph',
  'Thomas', 'Christopher', 'Daniel', 'Matthew', 'Anthony', 'Mark', 'Steven',
  'Andrew', 'Paul', 'Joshua', 'Kenneth', 'Kevin', 'Brian', 'George', 'Timothy',
  'Ronald', 'Edward', 'Jason', 'Jeffrey', 'Ryan', 'Jacob', 'Nicholas',
];

const FIRST_NAMES_FEMALE = [
  'Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan',
  'Jessica', 'Sarah', 'Karen', 'Lisa', 'Nancy', 'Betty', 'Margaret', 'Sandra',
  'Ashley', 'Kimberly', 'Emily', 'Donna', 'Michelle', 'Dorothy', 'Carol',
  'Amanda', 'Melissa', 'Deborah', 'Stephanie', 'Rebecca', 'Sharon', 'Laura', 'Cynthia',
];

const LAST_NAMES = [
  'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
  'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
  'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
  'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson',
  'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen',
];

const MOVIE_TITLE_PREFIXES = [
  'The', 'A', 'An', '', '', '', 'Rise of', 'Fall of', 'Return of', 'Legend of',
  'Chronicles of', 'Journey to', 'Battle for', 'Quest for', 'Shadow of',
];

const MOVIE_TITLE_WORDS = [
  'Dawn', 'Dusk', 'Night', 'Day', 'Storm', 'Fire', 'Ice', 'Thunder', 'Lightning',
  'Shadow', 'Light', 'Dark', 'Red', 'Blue', 'Black', 'White', 'Golden', 'Silver',
  'Steel', 'Iron', 'Dragon', 'Phoenix', 'Wolf', 'Eagle', 'Lion', 'Serpent',
  'Crown', 'Sword', 'Shield', 'Heart', 'Soul', 'Spirit', 'Ghost', 'Dream',
  'Nightmare', 'Horizon', 'Frontier', 'Edge', 'Depths', 'Heights', 'Kingdom',
  'Empire', 'Republic', 'Alliance', 'Legacy', 'Destiny', 'Fate', 'Fortune',
];

const MOVIE_TITLE_SUFFIXES = [
  '', '', '', '', '', 'Rising', 'Falling', 'Awakening', 'Redemption',
  'Revelation', 'Revolution', 'Resurrection', 'Reckoning', 'Requiem',
  'Protocol', 'Conspiracy', 'Chronicles', 'Saga', 'Legacy',
];

const COUNTRIES = [
  'United States', 'United Kingdom', 'France', 'Germany', 'Japan', 'South Korea',
  'China', 'India', 'Canada', 'Australia', 'Italy', 'Spain', 'Brazil', 'Mexico',
  'Sweden', 'Denmark', 'Norway', 'Netherlands', 'New Zealand', 'Ireland',
];

const LANGUAGES = [
  'English', 'French', 'German', 'Japanese', 'Korean', 'Chinese', 'Spanish',
  'Portuguese', 'Italian', 'Hindi', 'Swedish', 'Danish', 'Norwegian', 'Dutch',
];

const BIRTH_PLACES = [
  'Los Angeles, California, USA', 'New York City, New York, USA',
  'London, England, UK', 'Paris, France', 'Tokyo, Japan',
  'Sydney, Australia', 'Toronto, Canada', 'Berlin, Germany',
  'Rome, Italy', 'Madrid, Spain', 'Mumbai, India', 'Seoul, South Korea',
  'Chicago, Illinois, USA', 'San Francisco, California, USA',
  'Atlanta, Georgia, USA', 'Miami, Florida, USA', 'Boston, Massachusetts, USA',
];

const CHARACTER_FIRST_NAMES = [
  'Jack', 'John', 'James', 'Max', 'Alex', 'Sam', 'Mike', 'Tom', 'Nick', 'Chris',
  'Sarah', 'Emma', 'Kate', 'Lisa', 'Anna', 'Maria', 'Jane', 'Diana', 'Laura', 'Amy',
];

const CHARACTER_LAST_NAMES = [
  'Stone', 'Hart', 'Black', 'White', 'Storm', 'Wolf', 'Hunter', 'Knight',
  'Chase', 'Cross', 'Pierce', 'Sharp', 'West', 'Drake', 'Blake', 'Cole',
];

const TAGLINES = [
  'The adventure begins.',
  'Some secrets are better left buried.',
  'In a world divided, one will rise.',
  'The truth will set you free.',
  'Every hero has a dark side.',
  'Prepare for the unexpected.',
  'Nothing is as it seems.',
  'One choice changes everything.',
  'Fear has a new name.',
  'The countdown has begun.',
  'Survival is just the beginning.',
  'Heroes are made, not born.',
];

const AWARD_CEREMONIES = [
  'Academy Awards', 'Golden Globe Awards', 'BAFTA Awards', 'Screen Actors Guild Awards',
  'Critics Choice Awards', 'Cannes Film Festival', 'Venice Film Festival', 'Berlin Film Festival',
];

const AWARD_CATEGORIES = [
  'Best Picture', 'Best Director', 'Best Actor', 'Best Actress',
  'Best Supporting Actor', 'Best Supporting Actress', 'Best Original Screenplay',
  'Best Adapted Screenplay', 'Best Cinematography', 'Best Film Editing',
  'Best Original Score', 'Best Visual Effects', 'Best Animated Feature',
  'Best Foreign Language Film', 'Best Documentary Feature',
];

// =============================================================================
// Data Generator
// =============================================================================

/**
 * Configuration for IMDB data generation
 */
export interface ImdbGeneratorConfig {
  /** Random seed for reproducibility */
  seed?: number;
  /** Number of movies to generate */
  movieCount?: number;
  /** Number of actors to generate */
  actorCount?: number;
  /** Number of directors to generate */
  directorCount?: number;
  /** Average cast size per movie */
  avgCastSize?: number;
  /** Percentage of movies with awards */
  awardPercentage?: number;
}

/**
 * Default configuration
 */
const DEFAULT_CONFIG: Required<ImdbGeneratorConfig> = {
  seed: 42,
  movieCount: 5000,
  actorCount: 8000,
  directorCount: 500,
  avgCastSize: 8,
  awardPercentage: 0.15,
};

/**
 * Generated IMDB dataset
 */
export interface ImdbDataset {
  genres: ImdbGenre[];
  movies: ImdbMovie[];
  movieGenres: ImdbMovieGenre[];
  persons: ImdbPerson[];
  roles: ImdbRole[];
  directors: ImdbDirector[];
  ratings: ImdbRating[];
  awards: ImdbAward[];
}

/**
 * Generate a complete IMDB dataset with deterministic seeding
 *
 * @param config - Generation configuration
 * @returns Complete dataset with all entities
 *
 * @example
 * ```ts
 * const data = generateImdbData({ seed: 12345 });
 * console.log(data.movies.length); // 5000 (default)
 * ```
 */
export function generateImdbData(config: ImdbGeneratorConfig = {}): ImdbDataset {
  const cfg = { ...DEFAULT_CONFIG, ...config };
  const rng = new SeededRandom(cfg.seed);

  // Generate genres (static data)
  const genres: ImdbGenre[] = GENRES.map((g, i) => ({
    genre_id: i + 1,
    name: g.name,
    description: g.description,
  }));

  // Generate persons (actors and directors)
  const persons: ImdbPerson[] = [];
  const professions: ImdbPerson['primary_profession'][] = [
    'Actor', 'Director', 'Producer', 'Writer', 'Cinematographer', 'Composer', 'Editor',
  ];

  // Generate directors first
  for (let i = 0; i < cfg.directorCount; i++) {
    const gender = rng.pick(['Male', 'Female'] as const);
    const firstName = rng.pick(gender === 'Male' ? FIRST_NAMES_MALE : FIRST_NAMES_FEMALE);
    const lastName = rng.pick(LAST_NAMES);
    const birthYear = rng.int(1940, 1995);
    const isDeceased = rng.bool(0.05);

    persons.push({
      person_id: i + 1,
      name: `${firstName} ${lastName}`,
      birth_date: rng.date(birthYear, birthYear),
      death_date: isDeceased ? rng.date(birthYear + 50, 2025) : null,
      birth_place: rng.pick(BIRTH_PLACES),
      biography: `${firstName} ${lastName} is a renowned director known for their distinctive visual style.`,
      primary_profession: 'Director',
      gender,
      popularity: rng.decimal(20, 100, 1),
    });
  }

  // Generate actors
  for (let i = 0; i < cfg.actorCount; i++) {
    const gender = rng.pick(['Male', 'Female'] as const);
    const firstName = rng.pick(gender === 'Male' ? FIRST_NAMES_MALE : FIRST_NAMES_FEMALE);
    const lastName = rng.pick(LAST_NAMES);
    const birthYear = rng.int(1950, 2005);
    const isDeceased = rng.bool(0.03);

    persons.push({
      person_id: cfg.directorCount + i + 1,
      name: `${firstName} ${lastName}`,
      birth_date: rng.date(birthYear, birthYear),
      death_date: isDeceased ? rng.date(birthYear + 40, 2025) : null,
      birth_place: rng.bool(0.8) ? rng.pick(BIRTH_PLACES) : null,
      biography: rng.bool(0.7) ? `${firstName} ${lastName} is an accomplished actor.` : null,
      primary_profession: rng.bool(0.95) ? 'Actor' : rng.pick(professions),
      gender,
      popularity: rng.decimal(1, 100, 1),
    });
  }

  // Generate movies
  const movies: ImdbMovie[] = [];
  const movieGenres: ImdbMovieGenre[] = [];
  const roles: ImdbRole[] = [];
  const directorsTable: ImdbDirector[] = [];
  const ratings: ImdbRating[] = [];
  const awards: ImdbAward[] = [];
  let awardId = 1;

  for (let i = 0; i < cfg.movieCount; i++) {
    const movieId = i + 1;
    const prefix = rng.pick(MOVIE_TITLE_PREFIXES);
    const word1 = rng.pick(MOVIE_TITLE_WORDS);
    const word2 = rng.bool(0.4) ? ` ${rng.pick(MOVIE_TITLE_WORDS)}` : '';
    const suffix = rng.pick(MOVIE_TITLE_SUFFIXES);
    const title = `${prefix}${prefix ? ' ' : ''}${word1}${word2}${suffix ? ': ' + suffix : ''}`.trim();

    const releaseYear = rng.int(1970, 2025);
    const primaryGenre = rng.pick(genres);
    const secondaryGenre = rng.bool(0.6) ? rng.pick(genres.filter((g) => g.genre_id !== primaryGenre.genre_id)) : null;

    const hasBudget = rng.bool(0.7);
    const budget = hasBudget ? rng.int(1, 300) * 1000000 : null;
    const hasRevenue = hasBudget && rng.bool(0.8);
    const revenue = hasRevenue ? rng.int(Math.max(1, (budget || 1000000) / 10000000), 1000) * 1000000 : null;

    // Sequels - 10% chance for recent movies
    let sequelTo: number | null = null;
    if (releaseYear > 2000 && rng.bool(0.1) && i > 10) {
      sequelTo = rng.int(1, Math.min(i - 1, 100));
    }

    movies.push({
      movie_id: movieId,
      title,
      original_title: rng.bool(0.1) ? `Original: ${title}` : null,
      release_year: releaseYear,
      runtime_minutes: rng.int(75, 210),
      genre: primaryGenre.name,
      genre_secondary: secondaryGenre?.name || null,
      budget,
      revenue,
      tagline: rng.bool(0.7) ? rng.pick(TAGLINES) : null,
      overview: `A ${primaryGenre.name.toLowerCase()} film about ${word1.toLowerCase()}.`,
      production_country: rng.pick(COUNTRIES),
      original_language: rng.pick(LANGUAGES),
      status: rng.bool(0.95) ? 'Released' : rng.pick(['Post Production', 'In Production', 'Planned', 'Canceled']),
      sequel_to: sequelTo,
    });

    // Add movie-genre relationships
    movieGenres.push({
      movie_id: movieId,
      genre_id: primaryGenre.genre_id,
      is_primary: 1,
    });

    if (secondaryGenre) {
      movieGenres.push({
        movie_id: movieId,
        genre_id: secondaryGenre.genre_id,
        is_primary: 0,
      });
    }

    // Add directors (1-2 per movie)
    const directorCount = rng.bool(0.1) ? 2 : 1;
    const movieDirectors = rng.pickMultiple(
      persons.filter((p) => p.primary_profession === 'Director'),
      directorCount
    );

    for (let j = 0; j < movieDirectors.length; j++) {
      directorsTable.push({
        movie_id: movieId,
        person_id: movieDirectors[j].person_id,
        is_co_director: j > 0 ? 1 : 0,
      });
    }

    // Add cast (roles)
    const castSize = rng.int(Math.floor(cfg.avgCastSize * 0.5), Math.floor(cfg.avgCastSize * 1.5));
    const movieActors = rng.pickMultiple(
      persons.filter((p) => p.primary_profession === 'Actor'),
      castSize
    );

    const roleTypes: ImdbRole['role_type'][] = ['Lead', 'Lead', 'Supporting', 'Supporting', 'Supporting', 'Cameo', 'Voice', 'Uncredited'];

    for (let j = 0; j < movieActors.length; j++) {
      const charFirstName = rng.pick(CHARACTER_FIRST_NAMES);
      const charLastName = rng.pick(CHARACTER_LAST_NAMES);

      roles.push({
        movie_id: movieId,
        person_id: movieActors[j].person_id,
        character_name: `${charFirstName} ${charLastName}`,
        billing_order: j + 1,
        role_type: j < 2 ? 'Lead' : rng.pick(roleTypes),
      });
    }

    // Add rating
    const rating = rng.decimal(3, 9.5, 1);
    const voteCount = Math.floor(Math.pow(rng.next(), 0.5) * 500000) + 100; // Power law distribution

    ratings.push({
      movie_id: movieId,
      rating,
      vote_count: voteCount,
      critic_score: rng.bool(0.6) ? rng.decimal(30, 100, 0) : null,
      audience_score: rng.bool(0.6) ? rng.decimal(40, 100, 0) : null,
      updated_at: Date.now() - rng.int(0, 365) * 24 * 60 * 60 * 1000,
    });

    // Add awards (for some movies)
    if (rng.bool(cfg.awardPercentage)) {
      const awardCount = rng.int(1, 6);
      const ceremonyYear = releaseYear < 2025 ? releaseYear + 1 : releaseYear;

      for (let a = 0; a < awardCount; a++) {
        const isPersonAward = rng.bool(0.6);
        awards.push({
          award_id: awardId++,
          movie_id: movieId,
          person_id: isPersonAward ? rng.pick(movieActors)?.person_id || null : null,
          ceremony: rng.pick(AWARD_CEREMONIES),
          category: rng.pick(AWARD_CATEGORIES),
          year: ceremonyYear,
          won: rng.bool(0.3) ? 1 : 0,
        });
      }
    }
  }

  return {
    genres,
    movies,
    movieGenres,
    persons,
    roles,
    directors: directorsTable,
    ratings,
    awards,
  };
}

// =============================================================================
// Benchmark Queries
// =============================================================================

/**
 * Query category for classification
 */
export type ImdbQueryCategory =
  | 'point_lookup'
  | 'range'
  | 'join'
  | 'aggregate'
  | 'top_n'
  | 'many_to_many'
  | 'self_join';

/**
 * Benchmark query definition
 */
export interface ImdbBenchmarkQuery {
  /** Unique query identifier */
  id: string;
  /** Human-readable name */
  name: string;
  /** Query category */
  category: ImdbQueryCategory;
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
 * Standard benchmark queries for IMDB dataset
 */
export const IMDB_BENCHMARK_QUERIES: ImdbBenchmarkQuery[] = [
  // Point Lookups
  {
    id: 'imdb_point_01',
    name: 'Movie by ID',
    category: 'point_lookup',
    sql: 'SELECT * FROM movies WHERE movie_id = 1',
    description: 'Single movie lookup by primary key',
    expectedResults: { maxRows: 1 },
  },
  {
    id: 'imdb_point_02',
    name: 'Person by ID',
    category: 'point_lookup',
    sql: 'SELECT * FROM persons WHERE person_id = 1',
    description: 'Single person lookup by primary key',
    expectedResults: { maxRows: 1 },
  },
  {
    id: 'imdb_point_03',
    name: 'Movie Rating',
    category: 'point_lookup',
    sql: 'SELECT * FROM ratings WHERE movie_id = 1',
    description: 'Rating lookup by movie ID',
    expectedResults: { maxRows: 1 },
  },
  {
    id: 'imdb_point_04',
    name: 'Genre by ID',
    category: 'point_lookup',
    sql: 'SELECT * FROM genres WHERE genre_id = 1',
    description: 'Genre lookup by primary key',
    expectedResults: { maxRows: 1 },
  },

  // Range Queries
  {
    id: 'imdb_range_01',
    name: 'Movies by Year Range',
    category: 'range',
    sql: 'SELECT * FROM movies WHERE release_year BETWEEN 2015 AND 2020 ORDER BY release_year',
    description: 'Movies released between 2015-2020',
  },
  {
    id: 'imdb_range_02',
    name: 'High Rated Movies',
    category: 'range',
    sql: 'SELECT m.*, r.rating FROM movies m JOIN ratings r ON m.movie_id = r.movie_id WHERE r.rating >= 8.0 ORDER BY r.rating DESC',
    description: 'Movies with rating 8.0 or higher',
  },
  {
    id: 'imdb_range_03',
    name: 'Blockbuster Movies',
    category: 'range',
    sql: 'SELECT * FROM movies WHERE revenue > 500000000 ORDER BY revenue DESC',
    description: 'Movies with over $500M revenue',
  },
  {
    id: 'imdb_range_04',
    name: 'Long Movies',
    category: 'range',
    sql: 'SELECT * FROM movies WHERE runtime_minutes > 180 ORDER BY runtime_minutes DESC',
    description: 'Movies over 3 hours runtime',
  },

  // Many-to-Many Join Queries
  {
    id: 'imdb_m2m_01',
    name: 'Movie Cast',
    category: 'many_to_many',
    sql: `
      SELECT m.title, p.name as actor, r.character_name, r.billing_order
      FROM movies m
      JOIN roles r ON m.movie_id = r.movie_id
      JOIN persons p ON r.person_id = p.person_id
      WHERE m.movie_id = 1
      ORDER BY r.billing_order
    `,
    description: 'Full cast for a specific movie',
  },
  {
    id: 'imdb_m2m_02',
    name: 'Actor Filmography',
    category: 'many_to_many',
    sql: `
      SELECT m.title, m.release_year, r.character_name, r.role_type
      FROM persons p
      JOIN roles r ON p.person_id = r.person_id
      JOIN movies m ON r.movie_id = m.movie_id
      WHERE p.person_id = 501
      ORDER BY m.release_year DESC
    `,
    description: 'All movies for a specific actor',
  },
  {
    id: 'imdb_m2m_03',
    name: 'Director Filmography',
    category: 'many_to_many',
    sql: `
      SELECT m.title, m.release_year, r.rating
      FROM persons p
      JOIN directors d ON p.person_id = d.person_id
      JOIN movies m ON d.movie_id = m.movie_id
      LEFT JOIN ratings r ON m.movie_id = r.movie_id
      WHERE p.person_id = 1
      ORDER BY m.release_year DESC
    `,
    description: 'All movies for a specific director with ratings',
  },
  {
    id: 'imdb_m2m_04',
    name: 'Movies by Genre',
    category: 'many_to_many',
    sql: `
      SELECT m.title, m.release_year, g.name as genre
      FROM movies m
      JOIN movie_genres mg ON m.movie_id = mg.movie_id
      JOIN genres g ON mg.genre_id = g.genre_id
      WHERE g.name = 'Action' AND mg.is_primary = 1
      ORDER BY m.release_year DESC
      LIMIT 50
    `,
    description: 'Action movies (primary genre)',
    expectedResults: { maxRows: 50 },
  },
  {
    id: 'imdb_m2m_05',
    name: 'Co-Stars Network',
    category: 'many_to_many',
    sql: `
      SELECT p2.name as co_star, COUNT(*) as movies_together
      FROM roles r1
      JOIN roles r2 ON r1.movie_id = r2.movie_id AND r1.person_id != r2.person_id
      JOIN persons p2 ON r2.person_id = p2.person_id
      WHERE r1.person_id = 501
      GROUP BY r2.person_id, p2.name
      ORDER BY movies_together DESC
      LIMIT 10
    `,
    description: 'Top co-stars for an actor',
    expectedResults: { maxRows: 10 },
  },

  // Join Queries
  {
    id: 'imdb_join_01',
    name: 'Movie with Director and Rating',
    category: 'join',
    sql: `
      SELECT m.title, m.release_year, p.name as director, r.rating, r.vote_count
      FROM movies m
      JOIN directors d ON m.movie_id = d.movie_id
      JOIN persons p ON d.person_id = p.person_id
      LEFT JOIN ratings r ON m.movie_id = r.movie_id
      ORDER BY r.rating DESC NULLS LAST
      LIMIT 100
    `,
    description: 'Top 100 movies with director and rating info',
    expectedResults: { maxRows: 100 },
  },
  {
    id: 'imdb_join_02',
    name: 'Award Winners',
    category: 'join',
    sql: `
      SELECT m.title, a.ceremony, a.category, a.year, p.name as recipient
      FROM awards a
      JOIN movies m ON a.movie_id = m.movie_id
      LEFT JOIN persons p ON a.person_id = p.person_id
      WHERE a.won = 1
      ORDER BY a.year DESC, a.ceremony
      LIMIT 50
    `,
    description: 'Recent award winners',
    expectedResults: { maxRows: 50 },
  },
  {
    id: 'imdb_join_03',
    name: 'Lead Actors by Genre',
    category: 'join',
    sql: `
      SELECT g.name as genre, p.name as lead_actor, COUNT(*) as lead_roles
      FROM movie_genres mg
      JOIN genres g ON mg.genre_id = g.genre_id
      JOIN roles r ON mg.movie_id = r.movie_id
      JOIN persons p ON r.person_id = p.person_id
      WHERE mg.is_primary = 1 AND r.role_type = 'Lead'
      GROUP BY g.genre_id, g.name, p.person_id, p.name
      HAVING COUNT(*) >= 3
      ORDER BY lead_roles DESC
      LIMIT 20
    `,
    description: 'Actors with most lead roles by genre',
    expectedResults: { maxRows: 20 },
  },

  // Self-Join Queries
  {
    id: 'imdb_self_01',
    name: 'Movie Sequels',
    category: 'self_join',
    sql: `
      SELECT m1.title as sequel, m2.title as original, m1.release_year, m2.release_year as original_year
      FROM movies m1
      JOIN movies m2 ON m1.sequel_to = m2.movie_id
      ORDER BY m1.release_year DESC
    `,
    description: 'Movies and their predecessors',
  },
  {
    id: 'imdb_self_02',
    name: 'Franchise Chain',
    category: 'self_join',
    sql: `
      SELECT m1.title, m2.title as sequel, m3.title as third
      FROM movies m1
      LEFT JOIN movies m2 ON m2.sequel_to = m1.movie_id
      LEFT JOIN movies m3 ON m3.sequel_to = m2.movie_id
      WHERE m1.sequel_to IS NULL AND m2.movie_id IS NOT NULL
      ORDER BY m1.release_year
      LIMIT 20
    `,
    description: 'Movie franchise chains (original + sequels)',
    expectedResults: { maxRows: 20 },
  },

  // Aggregate Queries
  {
    id: 'imdb_agg_01',
    name: 'Movies by Genre Count',
    category: 'aggregate',
    sql: `
      SELECT g.name, COUNT(*) as movie_count
      FROM genres g
      LEFT JOIN movie_genres mg ON g.genre_id = mg.genre_id
      GROUP BY g.genre_id, g.name
      ORDER BY movie_count DESC
    `,
    description: 'Movie count by genre',
  },
  {
    id: 'imdb_agg_02',
    name: 'Yearly Box Office',
    category: 'aggregate',
    sql: `
      SELECT release_year, COUNT(*) as movie_count,
             SUM(revenue) as total_revenue, AVG(revenue) as avg_revenue
      FROM movies
      WHERE revenue IS NOT NULL
      GROUP BY release_year
      ORDER BY release_year DESC
    `,
    description: 'Box office statistics by year',
  },
  {
    id: 'imdb_agg_03',
    name: 'Director Statistics',
    category: 'aggregate',
    sql: `
      SELECT p.name, COUNT(d.movie_id) as movie_count,
             AVG(r.rating) as avg_rating, SUM(m.revenue) as total_revenue
      FROM persons p
      JOIN directors d ON p.person_id = d.person_id
      JOIN movies m ON d.movie_id = m.movie_id
      LEFT JOIN ratings r ON m.movie_id = r.movie_id
      GROUP BY p.person_id, p.name
      HAVING COUNT(d.movie_id) >= 5
      ORDER BY avg_rating DESC NULLS LAST
      LIMIT 20
    `,
    description: 'Director performance statistics',
    expectedResults: { maxRows: 20 },
  },
  {
    id: 'imdb_agg_04',
    name: 'Actor Career Stats',
    category: 'aggregate',
    sql: `
      SELECT p.name, p.gender,
             COUNT(r.movie_id) as movie_count,
             SUM(CASE WHEN r.role_type = 'Lead' THEN 1 ELSE 0 END) as lead_roles,
             AVG(rt.rating) as avg_movie_rating
      FROM persons p
      JOIN roles r ON p.person_id = r.person_id
      JOIN movies m ON r.movie_id = m.movie_id
      LEFT JOIN ratings rt ON m.movie_id = rt.movie_id
      WHERE p.primary_profession = 'Actor'
      GROUP BY p.person_id, p.name, p.gender
      HAVING COUNT(r.movie_id) >= 10
      ORDER BY avg_movie_rating DESC NULLS LAST
      LIMIT 25
    `,
    description: 'Actor career statistics',
    expectedResults: { maxRows: 25 },
  },
  {
    id: 'imdb_agg_05',
    name: 'Genre Rating Analysis',
    category: 'aggregate',
    sql: `
      SELECT g.name,
             COUNT(*) as movie_count,
             AVG(r.rating) as avg_rating,
             MIN(r.rating) as min_rating,
             MAX(r.rating) as max_rating
      FROM genres g
      JOIN movie_genres mg ON g.genre_id = mg.genre_id
      JOIN ratings r ON mg.movie_id = r.movie_id
      WHERE mg.is_primary = 1
      GROUP BY g.genre_id, g.name
      ORDER BY avg_rating DESC
    `,
    description: 'Rating statistics by genre',
  },

  // Top-N Queries
  {
    id: 'imdb_topn_01',
    name: 'Top Rated Movies',
    category: 'top_n',
    sql: `
      SELECT m.title, m.release_year, r.rating, r.vote_count
      FROM movies m
      JOIN ratings r ON m.movie_id = r.movie_id
      WHERE r.vote_count >= 10000
      ORDER BY r.rating DESC, r.vote_count DESC
      LIMIT 25
    `,
    description: 'Top 25 highest rated movies (min 10k votes)',
    expectedResults: { maxRows: 25 },
  },
  {
    id: 'imdb_topn_02',
    name: 'Most Popular Actors',
    category: 'top_n',
    sql: `
      SELECT p.name, p.popularity, COUNT(r.movie_id) as movie_count
      FROM persons p
      LEFT JOIN roles r ON p.person_id = r.person_id
      WHERE p.primary_profession = 'Actor'
      GROUP BY p.person_id, p.name, p.popularity
      ORDER BY p.popularity DESC
      LIMIT 20
    `,
    description: 'Top 20 most popular actors',
    expectedResults: { maxRows: 20 },
  },
  {
    id: 'imdb_topn_03',
    name: 'Highest Grossing Movies',
    category: 'top_n',
    sql: `
      SELECT title, release_year, budget, revenue, (revenue - COALESCE(budget, 0)) as profit
      FROM movies
      WHERE revenue IS NOT NULL
      ORDER BY revenue DESC
      LIMIT 20
    `,
    description: 'Top 20 highest grossing movies',
    expectedResults: { maxRows: 20 },
  },
  {
    id: 'imdb_topn_04',
    name: 'Most Awarded People',
    category: 'top_n',
    sql: `
      SELECT p.name, p.primary_profession,
             SUM(a.won) as wins, COUNT(*) as nominations
      FROM persons p
      JOIN awards a ON p.person_id = a.person_id
      GROUP BY p.person_id, p.name, p.primary_profession
      ORDER BY wins DESC, nominations DESC
      LIMIT 15
    `,
    description: 'Top 15 most awarded people',
    expectedResults: { maxRows: 15 },
  },
  {
    id: 'imdb_topn_05',
    name: 'Best Director-Actor Pairs',
    category: 'top_n',
    sql: `
      SELECT dir.name as director, act.name as actor, COUNT(*) as collaborations
      FROM directors d
      JOIN persons dir ON d.person_id = dir.person_id
      JOIN roles r ON d.movie_id = r.movie_id
      JOIN persons act ON r.person_id = act.person_id
      GROUP BY d.person_id, dir.name, r.person_id, act.name
      HAVING COUNT(*) >= 2
      ORDER BY collaborations DESC
      LIMIT 10
    `,
    description: 'Most frequent director-actor collaborations',
    expectedResults: { maxRows: 10 },
  },
];

// =============================================================================
// Dataset Information
// =============================================================================

/**
 * IMDB dataset metadata
 */
export const IMDB_METADATA = {
  name: 'IMDB',
  version: '1.0.0',
  description: 'Movie database for testing complex join patterns',
  tables: [
    'genres',
    'movies',
    'movie_genres',
    'persons',
    'roles',
    'directors',
    'ratings',
    'awards',
  ],
  approximateRows: 100000,
  queryCategories: [
    'point_lookup',
    'range',
    'join',
    'aggregate',
    'top_n',
    'many_to_many',
    'self_join',
  ],
  queryCount: IMDB_BENCHMARK_QUERIES.length,
};

/**
 * Get all schema creation statements in order
 */
export function getImdbSchemaStatements(): string[] {
  return [
    IMDB_SCHEMA.genres,
    IMDB_SCHEMA.movies,
    IMDB_SCHEMA.movie_genres,
    IMDB_SCHEMA.persons,
    IMDB_SCHEMA.roles,
    IMDB_SCHEMA.directors,
    IMDB_SCHEMA.ratings,
    IMDB_SCHEMA.awards,
    ...IMDB_INDEXES,
  ];
}

/**
 * Get queries by category
 */
export function getImdbQueriesByCategory(
  category: ImdbQueryCategory
): ImdbBenchmarkQuery[] {
  return IMDB_BENCHMARK_QUERIES.filter((q) => q.category === category);
}
