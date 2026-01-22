/**
 * O*NET Database Benchmark Dataset
 *
 * Occupational Information Network database for workforce analysis.
 * Contains occupational data with skills, abilities, and knowledge requirements.
 *
 * Dataset characteristics:
 * - ~170,000 rows total across all tables
 * - Hierarchical occupation classification
 * - Many-to-many relationships
 * - Text-heavy data suitable for search benchmarks
 *
 * Query patterns tested:
 * - Text search (occupation titles, descriptions)
 * - Hierarchical queries (occupation families)
 * - Range queries (importance/level scores)
 * - Multi-table joins
 * - Aggregations on skill requirements
 *
 * @packageDocumentation
 */

// =============================================================================
// Type Definitions
// =============================================================================

/**
 * Occupation entity (SOC-based classification)
 */
export interface OnetOccupation {
  /** O*NET-SOC Code (e.g., "11-1011.00") */
  onetsoc_code: string;
  /** Occupation title */
  title: string;
  /** Detailed description */
  description: string;
  /** Major occupation group (first 2 digits) */
  major_group: string;
  /** Minor group (first 4 digits) */
  minor_group: string;
  /** Broad occupation (first 6 digits) */
  broad_occupation: string;
  /** Job zone (1-5, complexity level) */
  job_zone: number;
  /** Education level required (1-12) */
  education_level: number;
  /** Experience required in months */
  experience_months: number;
  /** On-the-job training required (1-9) */
  training_level: number;
}

/**
 * Skill entity
 */
export interface OnetSkill {
  /** Unique skill identifier */
  skill_id: string;
  /** Skill name */
  name: string;
  /** Skill description */
  description: string;
  /** Skill category */
  category: 'Basic' | 'Cross-Functional' | 'Complex Problem Solving' | 'Resource Management' | 'Social' | 'Systems' | 'Technical';
}

/**
 * Occupation-Skill relationship
 */
export interface OnetOccupationSkill {
  /** O*NET-SOC Code */
  onetsoc_code: string;
  /** Skill identifier */
  skill_id: string;
  /** Importance rating (1.0-5.0) */
  importance: number;
  /** Level rating (1.0-7.0) */
  level: number;
  /** Data source (e.g., "Analyst") */
  data_source: string;
}

/**
 * Ability entity
 */
export interface OnetAbility {
  /** Unique ability identifier */
  ability_id: string;
  /** Ability name */
  name: string;
  /** Ability description */
  description: string;
  /** Ability category */
  category: 'Cognitive' | 'Psychomotor' | 'Physical' | 'Sensory';
}

/**
 * Occupation-Ability relationship
 */
export interface OnetOccupationAbility {
  /** O*NET-SOC Code */
  onetsoc_code: string;
  /** Ability identifier */
  ability_id: string;
  /** Importance rating (1.0-5.0) */
  importance: number;
  /** Level rating (1.0-7.0) */
  level: number;
}

/**
 * Knowledge area entity
 */
export interface OnetKnowledge {
  /** Unique knowledge identifier */
  knowledge_id: string;
  /** Knowledge area name */
  name: string;
  /** Knowledge area description */
  description: string;
  /** Knowledge category */
  category: 'Business' | 'Engineering' | 'Health' | 'Arts' | 'Law' | 'Communications' | 'Education' | 'Mathematics';
}

/**
 * Occupation-Knowledge relationship
 */
export interface OnetOccupationKnowledge {
  /** O*NET-SOC Code */
  onetsoc_code: string;
  /** Knowledge identifier */
  knowledge_id: string;
  /** Importance rating (1.0-5.0) */
  importance: number;
  /** Level rating (1.0-7.0) */
  level: number;
}

/**
 * Work activity entity
 */
export interface OnetWorkActivity {
  /** Unique activity identifier */
  activity_id: string;
  /** Activity name */
  name: string;
  /** Activity description */
  description: string;
  /** Activity category */
  category: 'Information Input' | 'Work Output' | 'Mental Processes' | 'Interacting';
}

/**
 * Occupation-Work Activity relationship
 */
export interface OnetOccupationActivity {
  /** O*NET-SOC Code */
  onetsoc_code: string;
  /** Activity identifier */
  activity_id: string;
  /** Importance rating (1.0-5.0) */
  importance: number;
  /** Level rating (1.0-7.0) */
  level: number;
}

/**
 * Work context entity
 */
export interface OnetWorkContext {
  /** Unique context identifier */
  context_id: string;
  /** Context name */
  name: string;
  /** Context description */
  description: string;
  /** Context category */
  category: 'Interpersonal' | 'Physical' | 'Structural';
}

/**
 * Occupation-Work Context relationship
 */
export interface OnetOccupationContext {
  /** O*NET-SOC Code */
  onetsoc_code: string;
  /** Context identifier */
  context_id: string;
  /** Rating (1.0-5.0) */
  rating: number;
  /** Category-specific measure */
  category_description: string;
}

// =============================================================================
// Schema Definitions
// =============================================================================

/**
 * SQL statements to create O*NET schema
 */
export const ONET_SCHEMA = {
  occupations: `
    CREATE TABLE IF NOT EXISTS occupations (
      onetsoc_code TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      description TEXT NOT NULL,
      major_group TEXT NOT NULL,
      minor_group TEXT NOT NULL,
      broad_occupation TEXT NOT NULL,
      job_zone INTEGER NOT NULL,
      education_level INTEGER NOT NULL,
      experience_months INTEGER NOT NULL,
      training_level INTEGER NOT NULL
    )
  `,

  skills: `
    CREATE TABLE IF NOT EXISTS skills (
      skill_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT NOT NULL,
      category TEXT NOT NULL
    )
  `,

  occupation_skills: `
    CREATE TABLE IF NOT EXISTS occupation_skills (
      onetsoc_code TEXT NOT NULL REFERENCES occupations(onetsoc_code),
      skill_id TEXT NOT NULL REFERENCES skills(skill_id),
      importance REAL NOT NULL,
      level REAL NOT NULL,
      data_source TEXT NOT NULL,
      PRIMARY KEY (onetsoc_code, skill_id)
    )
  `,

  abilities: `
    CREATE TABLE IF NOT EXISTS abilities (
      ability_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT NOT NULL,
      category TEXT NOT NULL
    )
  `,

  occupation_abilities: `
    CREATE TABLE IF NOT EXISTS occupation_abilities (
      onetsoc_code TEXT NOT NULL REFERENCES occupations(onetsoc_code),
      ability_id TEXT NOT NULL REFERENCES abilities(ability_id),
      importance REAL NOT NULL,
      level REAL NOT NULL,
      PRIMARY KEY (onetsoc_code, ability_id)
    )
  `,

  knowledge: `
    CREATE TABLE IF NOT EXISTS knowledge (
      knowledge_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT NOT NULL,
      category TEXT NOT NULL
    )
  `,

  occupation_knowledge: `
    CREATE TABLE IF NOT EXISTS occupation_knowledge (
      onetsoc_code TEXT NOT NULL REFERENCES occupations(onetsoc_code),
      knowledge_id TEXT NOT NULL REFERENCES knowledge(knowledge_id),
      importance REAL NOT NULL,
      level REAL NOT NULL,
      PRIMARY KEY (onetsoc_code, knowledge_id)
    )
  `,

  work_activities: `
    CREATE TABLE IF NOT EXISTS work_activities (
      activity_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT NOT NULL,
      category TEXT NOT NULL
    )
  `,

  occupation_activities: `
    CREATE TABLE IF NOT EXISTS occupation_activities (
      onetsoc_code TEXT NOT NULL REFERENCES occupations(onetsoc_code),
      activity_id TEXT NOT NULL REFERENCES work_activities(activity_id),
      importance REAL NOT NULL,
      level REAL NOT NULL,
      PRIMARY KEY (onetsoc_code, activity_id)
    )
  `,

  work_contexts: `
    CREATE TABLE IF NOT EXISTS work_contexts (
      context_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT NOT NULL,
      category TEXT NOT NULL
    )
  `,

  occupation_contexts: `
    CREATE TABLE IF NOT EXISTS occupation_contexts (
      onetsoc_code TEXT NOT NULL REFERENCES occupations(onetsoc_code),
      context_id TEXT NOT NULL REFERENCES work_contexts(context_id),
      rating REAL NOT NULL,
      category_description TEXT NOT NULL,
      PRIMARY KEY (onetsoc_code, context_id)
    )
  `,
};

/**
 * Index creation statements for optimal query performance
 */
export const ONET_INDEXES = [
  // Occupation indexes
  'CREATE INDEX IF NOT EXISTS idx_occupations_major ON occupations(major_group)',
  'CREATE INDEX IF NOT EXISTS idx_occupations_minor ON occupations(minor_group)',
  'CREATE INDEX IF NOT EXISTS idx_occupations_broad ON occupations(broad_occupation)',
  'CREATE INDEX IF NOT EXISTS idx_occupations_job_zone ON occupations(job_zone)',
  'CREATE INDEX IF NOT EXISTS idx_occupations_education ON occupations(education_level)',

  // Skill indexes
  'CREATE INDEX IF NOT EXISTS idx_skills_category ON skills(category)',

  // Occupation-skill indexes
  'CREATE INDEX IF NOT EXISTS idx_occ_skills_skill ON occupation_skills(skill_id)',
  'CREATE INDEX IF NOT EXISTS idx_occ_skills_importance ON occupation_skills(importance)',
  'CREATE INDEX IF NOT EXISTS idx_occ_skills_level ON occupation_skills(level)',

  // Ability indexes
  'CREATE INDEX IF NOT EXISTS idx_abilities_category ON abilities(category)',

  // Occupation-ability indexes
  'CREATE INDEX IF NOT EXISTS idx_occ_abilities_ability ON occupation_abilities(ability_id)',
  'CREATE INDEX IF NOT EXISTS idx_occ_abilities_importance ON occupation_abilities(importance)',

  // Knowledge indexes
  'CREATE INDEX IF NOT EXISTS idx_knowledge_category ON knowledge(category)',

  // Occupation-knowledge indexes
  'CREATE INDEX IF NOT EXISTS idx_occ_knowledge_knowledge ON occupation_knowledge(knowledge_id)',
  'CREATE INDEX IF NOT EXISTS idx_occ_knowledge_importance ON occupation_knowledge(importance)',

  // Work activity indexes
  'CREATE INDEX IF NOT EXISTS idx_activities_category ON work_activities(category)',

  // Work context indexes
  'CREATE INDEX IF NOT EXISTS idx_contexts_category ON work_contexts(category)',
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
    return shuffled.slice(0, count);
  }
}

// =============================================================================
// Reference Data
// =============================================================================

const MAJOR_GROUPS = [
  { code: '11', name: 'Management', jobs: ['Chief Executives', 'General Managers', 'Operations Managers', 'Marketing Managers', 'Sales Managers', 'Financial Managers', 'HR Managers', 'IT Managers'] },
  { code: '13', name: 'Business and Financial Operations', jobs: ['Accountants', 'Auditors', 'Budget Analysts', 'Financial Analysts', 'Management Analysts', 'Compliance Officers', 'Cost Estimators'] },
  { code: '15', name: 'Computer and Mathematical', jobs: ['Software Developers', 'Data Scientists', 'Database Administrators', 'Network Architects', 'Security Analysts', 'Systems Analysts', 'Web Developers'] },
  { code: '17', name: 'Architecture and Engineering', jobs: ['Architects', 'Civil Engineers', 'Electrical Engineers', 'Mechanical Engineers', 'Industrial Engineers', 'Chemical Engineers', 'Environmental Engineers'] },
  { code: '19', name: 'Life, Physical, and Social Science', jobs: ['Biologists', 'Chemists', 'Physicists', 'Environmental Scientists', 'Sociologists', 'Psychologists', 'Economists'] },
  { code: '21', name: 'Community and Social Service', jobs: ['Social Workers', 'Counselors', 'Health Educators', 'Probation Officers', 'Community Service Managers', 'Mental Health Counselors'] },
  { code: '23', name: 'Legal', jobs: ['Lawyers', 'Judges', 'Paralegals', 'Legal Secretaries', 'Court Reporters', 'Arbitrators', 'Mediators'] },
  { code: '25', name: 'Educational Instruction and Library', jobs: ['Teachers', 'Professors', 'Librarians', 'Instructional Coordinators', 'Teaching Assistants', 'Tutors', 'Education Administrators'] },
  { code: '27', name: 'Arts, Design, Entertainment, Sports', jobs: ['Graphic Designers', 'Interior Designers', 'Musicians', 'Athletes', 'Coaches', 'Photographers', 'Video Editors'] },
  { code: '29', name: 'Healthcare Practitioners', jobs: ['Physicians', 'Surgeons', 'Dentists', 'Pharmacists', 'Registered Nurses', 'Physical Therapists', 'Optometrists'] },
  { code: '31', name: 'Healthcare Support', jobs: ['Nursing Assistants', 'Medical Assistants', 'Home Health Aides', 'Physical Therapy Aides', 'Dental Assistants', 'Pharmacy Technicians'] },
  { code: '33', name: 'Protective Service', jobs: ['Police Officers', 'Firefighters', 'Security Guards', 'Correctional Officers', 'Detectives', 'Lifeguards', 'TSA Officers'] },
  { code: '35', name: 'Food Preparation and Serving', jobs: ['Chefs', 'Cooks', 'Bartenders', 'Waiters', 'Food Service Managers', 'Bakers', 'Baristas'] },
  { code: '37', name: 'Building and Grounds Cleaning', jobs: ['Janitors', 'Landscapers', 'Pest Control Workers', 'Housekeepers', 'Groundskeepers', 'Tree Trimmers'] },
  { code: '39', name: 'Personal Care and Service', jobs: ['Hairdressers', 'Fitness Trainers', 'Childcare Workers', 'Recreation Workers', 'Tour Guides', 'Funeral Directors'] },
  { code: '41', name: 'Sales and Related', jobs: ['Retail Salespersons', 'Sales Representatives', 'Real Estate Agents', 'Insurance Agents', 'Telemarketers', 'Cashiers'] },
  { code: '43', name: 'Office and Administrative Support', jobs: ['Secretaries', 'Receptionists', 'Data Entry Clerks', 'Customer Service Reps', 'File Clerks', 'Mail Clerks'] },
  { code: '45', name: 'Farming, Fishing, and Forestry', jobs: ['Farmers', 'Fishers', 'Forest Workers', 'Agricultural Inspectors', 'Animal Breeders', 'Farm Workers'] },
  { code: '47', name: 'Construction and Extraction', jobs: ['Carpenters', 'Electricians', 'Plumbers', 'Welders', 'Roofers', 'Construction Laborers', 'Miners'] },
  { code: '49', name: 'Installation, Maintenance, Repair', jobs: ['Mechanics', 'HVAC Technicians', 'Maintenance Workers', 'Industrial Machinery Mechanics', 'Telecommunications Technicians'] },
  { code: '51', name: 'Production', jobs: ['Assemblers', 'Machine Operators', 'Quality Control Inspectors', 'Packaging Workers', 'Machinists', 'Welders'] },
  { code: '53', name: 'Transportation and Material Moving', jobs: ['Truck Drivers', 'Bus Drivers', 'Pilots', 'Ship Captains', 'Forklift Operators', 'Warehouse Workers'] },
];

const SKILLS_DATA: Array<{ id: string; name: string; description: string; category: OnetSkill['category'] }> = [
  // Basic Skills
  { id: 'SK01', name: 'Reading Comprehension', description: 'Understanding written sentences and paragraphs in work-related documents', category: 'Basic' },
  { id: 'SK02', name: 'Active Listening', description: 'Giving full attention to what others say, understanding points made', category: 'Basic' },
  { id: 'SK03', name: 'Writing', description: 'Communicating effectively in writing as appropriate for audience needs', category: 'Basic' },
  { id: 'SK04', name: 'Speaking', description: 'Talking to others to convey information effectively', category: 'Basic' },
  { id: 'SK05', name: 'Mathematics', description: 'Using mathematics to solve problems', category: 'Basic' },
  { id: 'SK06', name: 'Science', description: 'Using scientific rules and methods to solve problems', category: 'Basic' },
  // Cross-Functional Skills
  { id: 'SK07', name: 'Critical Thinking', description: 'Using logic and reasoning to identify strengths and weaknesses of solutions', category: 'Cross-Functional' },
  { id: 'SK08', name: 'Active Learning', description: 'Understanding implications of new information for problem-solving', category: 'Cross-Functional' },
  { id: 'SK09', name: 'Learning Strategies', description: 'Selecting and using training methods appropriate for the situation', category: 'Cross-Functional' },
  { id: 'SK10', name: 'Monitoring', description: 'Monitoring performance of yourself, others, or organizations', category: 'Cross-Functional' },
  // Social Skills
  { id: 'SK11', name: 'Social Perceptiveness', description: 'Being aware of others reactions and understanding why they react that way', category: 'Social' },
  { id: 'SK12', name: 'Coordination', description: 'Adjusting actions in relation to others actions', category: 'Social' },
  { id: 'SK13', name: 'Persuasion', description: 'Persuading others to change their minds or behavior', category: 'Social' },
  { id: 'SK14', name: 'Negotiation', description: 'Bringing others together and trying to reconcile differences', category: 'Social' },
  { id: 'SK15', name: 'Instructing', description: 'Teaching others how to do something', category: 'Social' },
  { id: 'SK16', name: 'Service Orientation', description: 'Actively looking for ways to help people', category: 'Social' },
  // Complex Problem Solving
  { id: 'SK17', name: 'Complex Problem Solving', description: 'Identifying complex problems and reviewing related information', category: 'Complex Problem Solving' },
  // Technical Skills
  { id: 'SK18', name: 'Operations Analysis', description: 'Analyzing needs and product requirements to create a design', category: 'Technical' },
  { id: 'SK19', name: 'Technology Design', description: 'Generating or adapting equipment and technology to serve user needs', category: 'Technical' },
  { id: 'SK20', name: 'Equipment Selection', description: 'Determining the kind of tools and equipment needed to do a job', category: 'Technical' },
  { id: 'SK21', name: 'Installation', description: 'Installing equipment, machines, wiring, or programs to meet specifications', category: 'Technical' },
  { id: 'SK22', name: 'Programming', description: 'Writing computer programs for various purposes', category: 'Technical' },
  { id: 'SK23', name: 'Operations Monitoring', description: 'Watching gauges, dials, or other indicators to make sure a machine is working properly', category: 'Technical' },
  { id: 'SK24', name: 'Quality Control Analysis', description: 'Conducting tests and inspections of products or services', category: 'Technical' },
  { id: 'SK25', name: 'Troubleshooting', description: 'Determining causes of operating errors and deciding what to do', category: 'Technical' },
  { id: 'SK26', name: 'Repairing', description: 'Repairing machines or systems using the needed tools', category: 'Technical' },
  // Systems Skills
  { id: 'SK27', name: 'Judgment and Decision Making', description: 'Considering costs and benefits of potential actions', category: 'Systems' },
  { id: 'SK28', name: 'Systems Analysis', description: 'Determining how a system should work and how changes will affect outcomes', category: 'Systems' },
  { id: 'SK29', name: 'Systems Evaluation', description: 'Identifying measures of system performance and actions to improve', category: 'Systems' },
  // Resource Management
  { id: 'SK30', name: 'Time Management', description: 'Managing ones own time and the time of others', category: 'Resource Management' },
  { id: 'SK31', name: 'Management of Financial Resources', description: 'Determining how money will be spent and accounting for expenses', category: 'Resource Management' },
  { id: 'SK32', name: 'Management of Material Resources', description: 'Obtaining and seeing to the appropriate use of equipment and materials', category: 'Resource Management' },
  { id: 'SK33', name: 'Management of Personnel Resources', description: 'Motivating, developing, and directing people as they work', category: 'Resource Management' },
];

const ABILITIES_DATA: Array<{ id: string; name: string; description: string; category: OnetAbility['category'] }> = [
  // Cognitive Abilities
  { id: 'AB01', name: 'Oral Comprehension', description: 'Ability to listen and understand information and ideas through spoken words', category: 'Cognitive' },
  { id: 'AB02', name: 'Written Comprehension', description: 'Ability to read and understand information and ideas in writing', category: 'Cognitive' },
  { id: 'AB03', name: 'Oral Expression', description: 'Ability to communicate information and ideas in speaking so others understand', category: 'Cognitive' },
  { id: 'AB04', name: 'Written Expression', description: 'Ability to communicate information and ideas in writing so others understand', category: 'Cognitive' },
  { id: 'AB05', name: 'Fluency of Ideas', description: 'Ability to come up with a number of ideas about a topic', category: 'Cognitive' },
  { id: 'AB06', name: 'Originality', description: 'Ability to come up with unusual or clever ideas about a given topic', category: 'Cognitive' },
  { id: 'AB07', name: 'Problem Sensitivity', description: 'Ability to tell when something is wrong or is likely to go wrong', category: 'Cognitive' },
  { id: 'AB08', name: 'Deductive Reasoning', description: 'Ability to apply general rules to specific problems to produce sensible answers', category: 'Cognitive' },
  { id: 'AB09', name: 'Inductive Reasoning', description: 'Ability to combine pieces of information to form general rules or conclusions', category: 'Cognitive' },
  { id: 'AB10', name: 'Information Ordering', description: 'Ability to arrange things or actions in a certain order or pattern', category: 'Cognitive' },
  { id: 'AB11', name: 'Mathematical Reasoning', description: 'Ability to choose the right mathematical methods to solve a problem', category: 'Cognitive' },
  { id: 'AB12', name: 'Number Facility', description: 'Ability to add, subtract, multiply, or divide quickly and correctly', category: 'Cognitive' },
  // Psychomotor Abilities
  { id: 'AB13', name: 'Arm-Hand Steadiness', description: 'Ability to keep hand and arm steady while moving or holding arm and hand', category: 'Psychomotor' },
  { id: 'AB14', name: 'Manual Dexterity', description: 'Ability to quickly move hand along with arm or two hands to grasp objects', category: 'Psychomotor' },
  { id: 'AB15', name: 'Finger Dexterity', description: 'Ability to make precisely coordinated movements of fingers for manipulating small objects', category: 'Psychomotor' },
  { id: 'AB16', name: 'Control Precision', description: 'Ability to quickly and repeatedly adjust controls of a machine', category: 'Psychomotor' },
  { id: 'AB17', name: 'Multilimb Coordination', description: 'Ability to coordinate two or more limbs while sitting, standing, or lying down', category: 'Psychomotor' },
  { id: 'AB18', name: 'Response Orientation', description: 'Ability to choose quickly between two or more movements', category: 'Psychomotor' },
  { id: 'AB19', name: 'Rate Control', description: 'Ability to time movements or control of equipment in anticipation of changes', category: 'Psychomotor' },
  { id: 'AB20', name: 'Reaction Time', description: 'Ability to quickly respond to a signal when it appears', category: 'Psychomotor' },
  // Physical Abilities
  { id: 'AB21', name: 'Static Strength', description: 'Ability to exert maximum muscle force to lift, push, pull, or carry objects', category: 'Physical' },
  { id: 'AB22', name: 'Explosive Strength', description: 'Ability to use short bursts of muscle force to propel oneself or objects', category: 'Physical' },
  { id: 'AB23', name: 'Dynamic Strength', description: 'Ability to exert muscle force repeatedly or continuously over time', category: 'Physical' },
  { id: 'AB24', name: 'Trunk Strength', description: 'Ability to use abdominal and lower back muscles to support part of body', category: 'Physical' },
  { id: 'AB25', name: 'Stamina', description: 'Ability to exert oneself physically over long periods without getting winded', category: 'Physical' },
  { id: 'AB26', name: 'Extent Flexibility', description: 'Ability to bend, stretch, twist, or reach with body, arms, or legs', category: 'Physical' },
  { id: 'AB27', name: 'Dynamic Flexibility', description: 'Ability to quickly and repeatedly bend, stretch, twist, or reach', category: 'Physical' },
  { id: 'AB28', name: 'Gross Body Coordination', description: 'Ability to coordinate the movement of arms, legs, and torso together', category: 'Physical' },
  { id: 'AB29', name: 'Gross Body Equilibrium', description: 'Ability to keep or regain body balance or stay upright when in unstable position', category: 'Physical' },
  // Sensory Abilities
  { id: 'AB30', name: 'Near Vision', description: 'Ability to see details at close range', category: 'Sensory' },
  { id: 'AB31', name: 'Far Vision', description: 'Ability to see details at a distance', category: 'Sensory' },
  { id: 'AB32', name: 'Visual Color Discrimination', description: 'Ability to match or detect differences between colors', category: 'Sensory' },
  { id: 'AB33', name: 'Night Vision', description: 'Ability to see under low light conditions', category: 'Sensory' },
  { id: 'AB34', name: 'Peripheral Vision', description: 'Ability to see objects or movement of objects to ones side', category: 'Sensory' },
  { id: 'AB35', name: 'Depth Perception', description: 'Ability to judge which of several objects is closer or farther away', category: 'Sensory' },
  { id: 'AB36', name: 'Hearing Sensitivity', description: 'Ability to detect or tell the differences between sounds', category: 'Sensory' },
  { id: 'AB37', name: 'Auditory Attention', description: 'Ability to focus on a single source of sound in presence of other sounds', category: 'Sensory' },
  { id: 'AB38', name: 'Speech Recognition', description: 'Ability to identify and understand the speech of another person', category: 'Sensory' },
  { id: 'AB39', name: 'Speech Clarity', description: 'Ability to speak clearly so others can understand you', category: 'Sensory' },
];

const KNOWLEDGE_DATA: Array<{ id: string; name: string; description: string; category: OnetKnowledge['category'] }> = [
  // Business
  { id: 'KN01', name: 'Administration and Management', description: 'Business and management principles', category: 'Business' },
  { id: 'KN02', name: 'Clerical', description: 'Administrative procedures and systems', category: 'Business' },
  { id: 'KN03', name: 'Economics and Accounting', description: 'Economic and accounting principles and practices', category: 'Business' },
  { id: 'KN04', name: 'Sales and Marketing', description: 'Principles and methods for showing and selling products', category: 'Business' },
  { id: 'KN05', name: 'Customer and Personal Service', description: 'Principles for providing customer and personal services', category: 'Business' },
  { id: 'KN06', name: 'Personnel and Human Resources', description: 'Principles for recruitment, selection, training, compensation', category: 'Business' },
  // Engineering
  { id: 'KN07', name: 'Computers and Electronics', description: 'Circuit boards, processors, electronic equipment, computer hardware and software', category: 'Engineering' },
  { id: 'KN08', name: 'Engineering and Technology', description: 'Practical application of engineering science and technology', category: 'Engineering' },
  { id: 'KN09', name: 'Design', description: 'Design techniques, tools, and principles in production of precision technical plans', category: 'Engineering' },
  { id: 'KN10', name: 'Building and Construction', description: 'Materials, methods, and tools involved in construction or repair', category: 'Engineering' },
  { id: 'KN11', name: 'Mechanical', description: 'Machines and tools, including their designs, uses, repair, and maintenance', category: 'Engineering' },
  // Health
  { id: 'KN12', name: 'Medicine and Dentistry', description: 'Information and techniques needed to diagnose and treat injuries and diseases', category: 'Health' },
  { id: 'KN13', name: 'Therapy and Counseling', description: 'Principles, methods, and procedures for diagnosis, treatment, and rehabilitation', category: 'Health' },
  { id: 'KN14', name: 'Biology', description: 'Plant and animal organisms, their tissues, cells, functions, interdependencies', category: 'Health' },
  { id: 'KN15', name: 'Psychology', description: 'Human behavior and performance; mental processes and assessment', category: 'Health' },
  // Law
  { id: 'KN16', name: 'Law and Government', description: 'Laws, legal codes, court procedures, government regulations, executive orders', category: 'Law' },
  { id: 'KN17', name: 'Public Safety and Security', description: 'Relevant equipment, policies, procedures, and strategies', category: 'Law' },
  // Communications
  { id: 'KN18', name: 'Telecommunications', description: 'Transmission, broadcasting, switching, control, and operation of systems', category: 'Communications' },
  { id: 'KN19', name: 'Communications and Media', description: 'Media production, communication, and dissemination techniques', category: 'Communications' },
  // Education
  { id: 'KN20', name: 'Education and Training', description: 'Principles and methods for curriculum design, teaching, and instruction', category: 'Education' },
  { id: 'KN21', name: 'English Language', description: 'Structure and content of the English language', category: 'Education' },
  { id: 'KN22', name: 'Foreign Language', description: 'Structure and content of a foreign language', category: 'Education' },
  { id: 'KN23', name: 'History and Archeology', description: 'Historical events and their causes, indicators, and effects', category: 'Education' },
  { id: 'KN24', name: 'Philosophy and Theology', description: 'Different philosophical systems and religions', category: 'Education' },
  // Arts
  { id: 'KN25', name: 'Fine Arts', description: 'Theory and techniques for composing, producing, and performing works', category: 'Arts' },
  // Mathematics
  { id: 'KN26', name: 'Mathematics', description: 'Arithmetic, algebra, geometry, calculus, statistics, and their applications', category: 'Mathematics' },
  { id: 'KN27', name: 'Chemistry', description: 'Chemical composition, structure, and properties of substances', category: 'Mathematics' },
  { id: 'KN28', name: 'Physics', description: 'Physical principles, laws, their interrelationships, and applications', category: 'Mathematics' },
  { id: 'KN29', name: 'Geography', description: 'Principles and methods for describing features of land, sea, and air masses', category: 'Mathematics' },
  { id: 'KN30', name: 'Sociology and Anthropology', description: 'Group behavior and dynamics, societal trends and influences', category: 'Mathematics' },
];

const WORK_ACTIVITIES_DATA: Array<{ id: string; name: string; description: string; category: OnetWorkActivity['category'] }> = [
  // Information Input
  { id: 'WA01', name: 'Getting Information', description: 'Observing, receiving, and obtaining information from all relevant sources', category: 'Information Input' },
  { id: 'WA02', name: 'Monitoring Processes', description: 'Monitoring and reviewing information from materials, events, or the environment', category: 'Information Input' },
  { id: 'WA03', name: 'Identifying Objects', description: 'Identifying information by categorizing, estimating, recognizing differences', category: 'Information Input' },
  { id: 'WA04', name: 'Inspecting Equipment', description: 'Inspecting equipment, structures, or materials to identify problems', category: 'Information Input' },
  { id: 'WA05', name: 'Estimating Quantities', description: 'Estimating sizes, distances, and quantities; determining time, costs, resources', category: 'Information Input' },
  // Work Output
  { id: 'WA06', name: 'Performing General Physical', description: 'Performing physical activities that require considerable use of arms and legs', category: 'Work Output' },
  { id: 'WA07', name: 'Handling Objects', description: 'Using hands and arms in handling, installing, positioning, and moving materials', category: 'Work Output' },
  { id: 'WA08', name: 'Controlling Machines', description: 'Using either control mechanisms or direct physical activity to operate machines', category: 'Work Output' },
  { id: 'WA09', name: 'Operating Vehicles', description: 'Running, maneuvering, navigating, or driving vehicles or mechanized equipment', category: 'Work Output' },
  { id: 'WA10', name: 'Interacting With Computers', description: 'Using computers and computer systems to program, write software, set up functions', category: 'Work Output' },
  { id: 'WA11', name: 'Drafting and Specifying', description: 'Providing documentation, detailed instructions, drawings, or specifications', category: 'Work Output' },
  { id: 'WA12', name: 'Repairing Machines', description: 'Repairing machines or systems using the needed tools', category: 'Work Output' },
  // Mental Processes
  { id: 'WA13', name: 'Judging Qualities', description: 'Assessing the value, importance, or quality of things or people', category: 'Mental Processes' },
  { id: 'WA14', name: 'Processing Information', description: 'Compiling, coding, categorizing, calculating, tabulating, auditing, or verifying information', category: 'Mental Processes' },
  { id: 'WA15', name: 'Analyzing Data', description: 'Identifying the underlying principles, reasons, or facts of information', category: 'Mental Processes' },
  { id: 'WA16', name: 'Making Decisions', description: 'Analyzing information and evaluating results to choose the best solution', category: 'Mental Processes' },
  { id: 'WA17', name: 'Thinking Creatively', description: 'Developing, designing, or creating new applications, ideas, relationships, systems', category: 'Mental Processes' },
  { id: 'WA18', name: 'Updating Knowledge', description: 'Keeping up-to-date technically and applying new knowledge to your job', category: 'Mental Processes' },
  { id: 'WA19', name: 'Developing Objectives', description: 'Establishing long-range objectives and specifying strategies and actions', category: 'Mental Processes' },
  { id: 'WA20', name: 'Scheduling Work', description: 'Scheduling events, programs, and activities, as well as the work of others', category: 'Mental Processes' },
  { id: 'WA21', name: 'Organizing and Planning', description: 'Developing specific goals and plans to prioritize, organize, and accomplish work', category: 'Mental Processes' },
  // Interacting
  { id: 'WA22', name: 'Interpreting Meaning', description: 'Translating or explaining what information means and how it can be used', category: 'Interacting' },
  { id: 'WA23', name: 'Communicating With Supervisors', description: 'Providing information to supervisors, co-workers, and subordinates', category: 'Interacting' },
  { id: 'WA24', name: 'Communicating Outside', description: 'Communicating with people outside the organization', category: 'Interacting' },
  { id: 'WA25', name: 'Establishing Relationships', description: 'Developing constructive and cooperative working relationships with others', category: 'Interacting' },
  { id: 'WA26', name: 'Assisting Others', description: 'Providing personal assistance, medical attention, emotional support', category: 'Interacting' },
  { id: 'WA27', name: 'Selling or Influencing', description: 'Convincing others to buy merchandise or to change their minds or actions', category: 'Interacting' },
  { id: 'WA28', name: 'Resolving Conflicts', description: 'Handling complaints, settling disputes, and resolving grievances', category: 'Interacting' },
  { id: 'WA29', name: 'Coordinating Work', description: 'Getting members of a group to work together to accomplish tasks', category: 'Interacting' },
  { id: 'WA30', name: 'Training Others', description: 'Identifying the educational needs of others and teaching', category: 'Interacting' },
  { id: 'WA31', name: 'Coaching and Developing', description: 'Identifying the developmental needs of others and coaching', category: 'Interacting' },
  { id: 'WA32', name: 'Guiding and Directing', description: 'Providing guidance and direction to subordinates', category: 'Interacting' },
];

const WORK_CONTEXTS_DATA: Array<{ id: string; name: string; description: string; category: OnetWorkContext['category'] }> = [
  // Interpersonal
  { id: 'WC01', name: 'Face-to-Face Discussions', description: 'How often do you have face-to-face discussions with individuals or teams', category: 'Interpersonal' },
  { id: 'WC02', name: 'Contact With Others', description: 'How much does this job require the worker to be in contact with others', category: 'Interpersonal' },
  { id: 'WC03', name: 'Work With Work Group', description: 'How important is it to work with others in a group or team', category: 'Interpersonal' },
  { id: 'WC04', name: 'Deal With External Customers', description: 'How important is it to work with external customers', category: 'Interpersonal' },
  { id: 'WC05', name: 'Coordinate or Lead Others', description: 'How important is it to coordinate or lead others in accomplishing work', category: 'Interpersonal' },
  { id: 'WC06', name: 'Responsible for Others Health', description: 'How responsible is the worker for the health and safety of others', category: 'Interpersonal' },
  { id: 'WC07', name: 'Frequency of Conflict', description: 'How often are there conflict situations the worker has to face', category: 'Interpersonal' },
  { id: 'WC08', name: 'Deal With Unpleasant People', description: 'How frequently does the worker have to deal with unpleasant people', category: 'Interpersonal' },
  { id: 'WC09', name: 'Deal With Angry People', description: 'How frequently does the worker have to deal with angry people', category: 'Interpersonal' },
  // Physical
  { id: 'WC10', name: 'Indoors, Environmentally Controlled', description: 'How often does this job require working indoors in environmentally controlled conditions', category: 'Physical' },
  { id: 'WC11', name: 'Indoors, Not Controlled', description: 'How often does this job require working indoors in non-controlled conditions', category: 'Physical' },
  { id: 'WC12', name: 'Outdoors, Exposed', description: 'How often does this job require working outdoors, exposed to all weather', category: 'Physical' },
  { id: 'WC13', name: 'Outdoors, Under Cover', description: 'How often does this job require working outdoors, under cover', category: 'Physical' },
  { id: 'WC14', name: 'Exposed to Hazardous Conditions', description: 'How often does this job require exposure to hazardous conditions', category: 'Physical' },
  { id: 'WC15', name: 'Exposed to Hazardous Equipment', description: 'How often does this job require exposure to hazardous equipment', category: 'Physical' },
  { id: 'WC16', name: 'Sounds, Noise Levels', description: 'How often does this job require working exposed to sounds and noise', category: 'Physical' },
  { id: 'WC17', name: 'Very Hot or Cold', description: 'How often does this job require working in very hot or cold temperatures', category: 'Physical' },
  { id: 'WC18', name: 'Bright or Inadequate Lighting', description: 'How often does this job require working in bright or inadequate lighting', category: 'Physical' },
  // Structural
  { id: 'WC19', name: 'Freedom to Make Decisions', description: 'How much decision making freedom does this job offer', category: 'Structural' },
  { id: 'WC20', name: 'Structured versus Unstructured', description: 'How structured is this job, how much latitude for determining tasks', category: 'Structural' },
  { id: 'WC21', name: 'Level of Competition', description: 'How competitive is this job in terms of achieving success', category: 'Structural' },
  { id: 'WC22', name: 'Time Pressure', description: 'How often does this job require the worker to meet strict deadlines', category: 'Structural' },
  { id: 'WC23', name: 'Pace Determined by Speed', description: 'How important is it to pace work to the speed of equipment or machinery', category: 'Structural' },
  { id: 'WC24', name: 'Duration of Typical Work Week', description: 'Number of hours typically worked in one week', category: 'Structural' },
  { id: 'WC25', name: 'Importance of Being Exact', description: 'How important is being very exact or highly accurate in performing this job', category: 'Structural' },
  { id: 'WC26', name: 'Importance of Repeating Tasks', description: 'How important is repeating the same physical or mental activities', category: 'Structural' },
  { id: 'WC27', name: 'Consequence of Error', description: 'How serious would the result usually be if the worker made a mistake', category: 'Structural' },
  { id: 'WC28', name: 'Impact of Decisions', description: 'What results do your decisions usually have on other people or company', category: 'Structural' },
];

// =============================================================================
// Data Generator
// =============================================================================

/**
 * Configuration for O*NET data generation
 */
export interface OnetGeneratorConfig {
  /** Random seed for reproducibility */
  seed?: number;
  /** Number of occupations to generate per major group */
  occupationsPerGroup?: number;
  /** Average skills per occupation */
  avgSkillsPerOccupation?: number;
  /** Average abilities per occupation */
  avgAbilitiesPerOccupation?: number;
  /** Average knowledge areas per occupation */
  avgKnowledgePerOccupation?: number;
  /** Average work activities per occupation */
  avgActivitiesPerOccupation?: number;
  /** Average work contexts per occupation */
  avgContextsPerOccupation?: number;
}

/**
 * Default configuration
 */
const DEFAULT_CONFIG: Required<OnetGeneratorConfig> = {
  seed: 42,
  occupationsPerGroup: 40,
  avgSkillsPerOccupation: 12,
  avgAbilitiesPerOccupation: 15,
  avgKnowledgePerOccupation: 10,
  avgActivitiesPerOccupation: 15,
  avgContextsPerOccupation: 12,
};

/**
 * Generated O*NET dataset
 */
export interface OnetDataset {
  occupations: OnetOccupation[];
  skills: OnetSkill[];
  occupationSkills: OnetOccupationSkill[];
  abilities: OnetAbility[];
  occupationAbilities: OnetOccupationAbility[];
  knowledge: OnetKnowledge[];
  occupationKnowledge: OnetOccupationKnowledge[];
  workActivities: OnetWorkActivity[];
  occupationActivities: OnetOccupationActivity[];
  workContexts: OnetWorkContext[];
  occupationContexts: OnetOccupationContext[];
}

/**
 * Generate a complete O*NET dataset with deterministic seeding
 *
 * @param config - Generation configuration
 * @returns Complete dataset with all entities
 *
 * @example
 * ```ts
 * const data = generateOnetData({ seed: 12345 });
 * console.log(data.occupations.length); // ~880
 * ```
 */
export function generateOnetData(config: OnetGeneratorConfig = {}): OnetDataset {
  const cfg = { ...DEFAULT_CONFIG, ...config };
  const rng = new SeededRandom(cfg.seed);

  // Generate static reference data
  const skills: OnetSkill[] = SKILLS_DATA.map((s) => ({
    skill_id: s.id,
    name: s.name,
    description: s.description,
    category: s.category,
  }));

  const abilities: OnetAbility[] = ABILITIES_DATA.map((a) => ({
    ability_id: a.id,
    name: a.name,
    description: a.description,
    category: a.category,
  }));

  const knowledge: OnetKnowledge[] = KNOWLEDGE_DATA.map((k) => ({
    knowledge_id: k.id,
    name: k.name,
    description: k.description,
    category: k.category,
  }));

  const workActivities: OnetWorkActivity[] = WORK_ACTIVITIES_DATA.map((w) => ({
    activity_id: w.id,
    name: w.name,
    description: w.description,
    category: w.category,
  }));

  const workContexts: OnetWorkContext[] = WORK_CONTEXTS_DATA.map((c) => ({
    context_id: c.id,
    name: c.name,
    description: c.description,
    category: c.category,
  }));

  // Generate occupations
  const occupations: OnetOccupation[] = [];
  const occupationSkills: OnetOccupationSkill[] = [];
  const occupationAbilities: OnetOccupationAbility[] = [];
  const occupationKnowledge: OnetOccupationKnowledge[] = [];
  const occupationActivities: OnetOccupationActivity[] = [];
  const occupationContexts: OnetOccupationContext[] = [];

  for (const majorGroup of MAJOR_GROUPS) {
    const occupationsToGenerate = Math.min(cfg.occupationsPerGroup, majorGroup.jobs.length * 5);

    for (let i = 0; i < occupationsToGenerate; i++) {
      const baseJob = majorGroup.jobs[i % majorGroup.jobs.length];
      const minorCode = `${majorGroup.code}${String(rng.int(10, 99)).padStart(2, '0')}`;
      const broadCode = `${minorCode}.${rng.int(10, 99)}`;
      const onetsocCode = `${broadCode}.${String(rng.int(0, 99)).padStart(2, '0')}`;

      const jobSuffix = i > 0 ? ` ${['Senior', 'Junior', 'Lead', 'Associate', 'Chief', 'Assistant', 'Deputy'][i % 7]}` : '';

      occupations.push({
        onetsoc_code: onetsocCode,
        title: `${baseJob}${jobSuffix}`,
        description: `Perform professional duties related to ${baseJob.toLowerCase()} in ${majorGroup.name.toLowerCase()} contexts. Requires specialized knowledge and skills.`,
        major_group: majorGroup.code,
        minor_group: minorCode,
        broad_occupation: broadCode,
        job_zone: rng.int(1, 5),
        education_level: rng.int(1, 12),
        experience_months: rng.int(0, 120),
        training_level: rng.int(1, 9),
      });

      // Generate skill associations
      const skillCount = rng.int(
        Math.floor(cfg.avgSkillsPerOccupation * 0.5),
        Math.floor(cfg.avgSkillsPerOccupation * 1.5)
      );
      const selectedSkills = rng.pickMultiple(skills, skillCount);
      for (const skill of selectedSkills) {
        occupationSkills.push({
          onetsoc_code: onetsocCode,
          skill_id: skill.skill_id,
          importance: rng.decimal(1, 5, 2),
          level: rng.decimal(1, 7, 2),
          data_source: 'Analyst',
        });
      }

      // Generate ability associations
      const abilityCount = rng.int(
        Math.floor(cfg.avgAbilitiesPerOccupation * 0.5),
        Math.floor(cfg.avgAbilitiesPerOccupation * 1.5)
      );
      const selectedAbilities = rng.pickMultiple(abilities, abilityCount);
      for (const ability of selectedAbilities) {
        occupationAbilities.push({
          onetsoc_code: onetsocCode,
          ability_id: ability.ability_id,
          importance: rng.decimal(1, 5, 2),
          level: rng.decimal(1, 7, 2),
        });
      }

      // Generate knowledge associations
      const knowledgeCount = rng.int(
        Math.floor(cfg.avgKnowledgePerOccupation * 0.5),
        Math.floor(cfg.avgKnowledgePerOccupation * 1.5)
      );
      const selectedKnowledge = rng.pickMultiple(knowledge, knowledgeCount);
      for (const k of selectedKnowledge) {
        occupationKnowledge.push({
          onetsoc_code: onetsocCode,
          knowledge_id: k.knowledge_id,
          importance: rng.decimal(1, 5, 2),
          level: rng.decimal(1, 7, 2),
        });
      }

      // Generate activity associations
      const activityCount = rng.int(
        Math.floor(cfg.avgActivitiesPerOccupation * 0.5),
        Math.floor(cfg.avgActivitiesPerOccupation * 1.5)
      );
      const selectedActivities = rng.pickMultiple(workActivities, activityCount);
      for (const activity of selectedActivities) {
        occupationActivities.push({
          onetsoc_code: onetsocCode,
          activity_id: activity.activity_id,
          importance: rng.decimal(1, 5, 2),
          level: rng.decimal(1, 7, 2),
        });
      }

      // Generate context associations
      const contextCount = rng.int(
        Math.floor(cfg.avgContextsPerOccupation * 0.5),
        Math.floor(cfg.avgContextsPerOccupation * 1.5)
      );
      const selectedContexts = rng.pickMultiple(workContexts, contextCount);
      for (const context of selectedContexts) {
        occupationContexts.push({
          onetsoc_code: onetsocCode,
          context_id: context.context_id,
          rating: rng.decimal(1, 5, 2),
          category_description: context.category,
        });
      }
    }
  }

  return {
    occupations,
    skills,
    occupationSkills,
    abilities,
    occupationAbilities,
    knowledge,
    occupationKnowledge,
    workActivities,
    occupationActivities,
    workContexts,
    occupationContexts,
  };
}

// =============================================================================
// Benchmark Queries
// =============================================================================

/**
 * Query category for classification
 */
export type OnetQueryCategory =
  | 'point_lookup'
  | 'range'
  | 'join'
  | 'aggregate'
  | 'text_search'
  | 'hierarchical'
  | 'top_n';

/**
 * Benchmark query definition
 */
export interface OnetBenchmarkQuery {
  /** Unique query identifier */
  id: string;
  /** Human-readable name */
  name: string;
  /** Query category */
  category: OnetQueryCategory;
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
 * Standard benchmark queries for O*NET dataset
 */
export const ONET_BENCHMARK_QUERIES: OnetBenchmarkQuery[] = [
  // Point Lookups
  {
    id: 'onet_point_01',
    name: 'Occupation by Code',
    category: 'point_lookup',
    sql: "SELECT * FROM occupations WHERE onetsoc_code = '11-1011.00'",
    description: 'Single occupation lookup by O*NET-SOC code',
    expectedResults: { minRows: 0, maxRows: 1 },
  },
  {
    id: 'onet_point_02',
    name: 'Skill by ID',
    category: 'point_lookup',
    sql: "SELECT * FROM skills WHERE skill_id = 'SK01'",
    description: 'Single skill lookup by identifier',
    expectedResults: { maxRows: 1 },
  },
  {
    id: 'onet_point_03',
    name: 'Ability by ID',
    category: 'point_lookup',
    sql: "SELECT * FROM abilities WHERE ability_id = 'AB01'",
    description: 'Single ability lookup by identifier',
    expectedResults: { maxRows: 1 },
  },
  {
    id: 'onet_point_04',
    name: 'Knowledge by ID',
    category: 'point_lookup',
    sql: "SELECT * FROM knowledge WHERE knowledge_id = 'KN01'",
    description: 'Single knowledge area lookup by identifier',
    expectedResults: { maxRows: 1 },
  },

  // Range Queries
  {
    id: 'onet_range_01',
    name: 'High Importance Skills',
    category: 'range',
    sql: 'SELECT * FROM occupation_skills WHERE importance >= 4.0 ORDER BY importance DESC',
    description: 'Skills with high importance rating',
  },
  {
    id: 'onet_range_02',
    name: 'Entry Level Jobs',
    category: 'range',
    sql: 'SELECT * FROM occupations WHERE job_zone <= 2 AND experience_months <= 12',
    description: 'Occupations requiring minimal experience',
  },
  {
    id: 'onet_range_03',
    name: 'Advanced Education Jobs',
    category: 'range',
    sql: 'SELECT * FROM occupations WHERE education_level >= 10 ORDER BY education_level DESC',
    description: 'Occupations requiring advanced degrees',
  },
  {
    id: 'onet_range_04',
    name: 'High Level Abilities',
    category: 'range',
    sql: 'SELECT * FROM occupation_abilities WHERE level >= 5.0 ORDER BY level DESC LIMIT 100',
    description: 'Top 100 ability requirements by level',
    expectedResults: { maxRows: 100 },
  },

  // Text Search Queries
  {
    id: 'onet_text_01',
    name: 'Manager Occupations',
    category: 'text_search',
    sql: "SELECT * FROM occupations WHERE title LIKE '%Manager%'",
    description: 'Occupations with Manager in title',
  },
  {
    id: 'onet_text_02',
    name: 'Computer Skills',
    category: 'text_search',
    sql: "SELECT * FROM skills WHERE description LIKE '%computer%'",
    description: 'Skills mentioning computers',
  },
  {
    id: 'onet_text_03',
    name: 'Engineering Knowledge',
    category: 'text_search',
    sql: "SELECT * FROM knowledge WHERE category = 'Engineering'",
    description: 'Knowledge areas in engineering category',
  },
  {
    id: 'onet_text_04',
    name: 'Analysis Occupations',
    category: 'text_search',
    sql: "SELECT * FROM occupations WHERE title LIKE '%Analyst%' OR description LIKE '%analysis%'",
    description: 'Occupations involving analysis',
  },

  // Hierarchical Queries
  {
    id: 'onet_hier_01',
    name: 'Occupations by Major Group',
    category: 'hierarchical',
    sql: "SELECT * FROM occupations WHERE major_group = '15'",
    description: 'All occupations in Computer and Mathematical group',
  },
  {
    id: 'onet_hier_02',
    name: 'Occupations by Minor Group',
    category: 'hierarchical',
    sql: "SELECT * FROM occupations WHERE minor_group LIKE '15-1%'",
    description: 'Occupations in Computer Occupations minor group',
  },
  {
    id: 'onet_hier_03',
    name: 'Count by Major Group',
    category: 'hierarchical',
    sql: `
      SELECT major_group, COUNT(*) as occupation_count
      FROM occupations
      GROUP BY major_group
      ORDER BY occupation_count DESC
    `,
    description: 'Occupation counts by major group',
  },
  {
    id: 'onet_hier_04',
    name: 'Job Zone Distribution',
    category: 'hierarchical',
    sql: `
      SELECT job_zone, COUNT(*) as count, AVG(education_level) as avg_education
      FROM occupations
      GROUP BY job_zone
      ORDER BY job_zone
    `,
    description: 'Distribution of occupations by job zone',
  },

  // Join Queries
  {
    id: 'onet_join_01',
    name: 'Occupation Skills Detail',
    category: 'join',
    sql: `
      SELECT o.title, s.name as skill_name, os.importance, os.level
      FROM occupations o
      JOIN occupation_skills os ON o.onetsoc_code = os.onetsoc_code
      JOIN skills s ON os.skill_id = s.skill_id
      WHERE o.major_group = '15'
      ORDER BY os.importance DESC
      LIMIT 50
    `,
    description: 'Computer occupations with their skill requirements',
    expectedResults: { maxRows: 50 },
  },
  {
    id: 'onet_join_02',
    name: 'Occupation Abilities Detail',
    category: 'join',
    sql: `
      SELECT o.title, a.name as ability_name, oa.importance, oa.level
      FROM occupations o
      JOIN occupation_abilities oa ON o.onetsoc_code = oa.onetsoc_code
      JOIN abilities a ON oa.ability_id = a.ability_id
      WHERE oa.importance >= 4.0
      LIMIT 100
    `,
    description: 'High-importance ability requirements',
    expectedResults: { maxRows: 100 },
  },
  {
    id: 'onet_join_03',
    name: 'Full Occupation Profile',
    category: 'join',
    sql: `
      SELECT o.onetsoc_code, o.title,
             COUNT(DISTINCT os.skill_id) as skill_count,
             COUNT(DISTINCT oa.ability_id) as ability_count,
             COUNT(DISTINCT ok.knowledge_id) as knowledge_count
      FROM occupations o
      LEFT JOIN occupation_skills os ON o.onetsoc_code = os.onetsoc_code
      LEFT JOIN occupation_abilities oa ON o.onetsoc_code = oa.onetsoc_code
      LEFT JOIN occupation_knowledge ok ON o.onetsoc_code = ok.onetsoc_code
      GROUP BY o.onetsoc_code, o.title
      LIMIT 50
    `,
    description: 'Occupation profiles with requirement counts',
    expectedResults: { maxRows: 50 },
  },
  {
    id: 'onet_join_04',
    name: 'Skill Usage Across Occupations',
    category: 'join',
    sql: `
      SELECT s.name, s.category,
             COUNT(os.onetsoc_code) as occupation_count,
             AVG(os.importance) as avg_importance
      FROM skills s
      JOIN occupation_skills os ON s.skill_id = os.skill_id
      GROUP BY s.skill_id, s.name, s.category
      ORDER BY occupation_count DESC
    `,
    description: 'Skills ranked by number of occupations requiring them',
  },
  {
    id: 'onet_join_05',
    name: 'Work Context by Occupation',
    category: 'join',
    sql: `
      SELECT o.title, wc.name as context, oc.rating
      FROM occupations o
      JOIN occupation_contexts oc ON o.onetsoc_code = oc.onetsoc_code
      JOIN work_contexts wc ON oc.context_id = wc.context_id
      WHERE o.job_zone >= 4
      ORDER BY oc.rating DESC
      LIMIT 100
    `,
    description: 'Work contexts for advanced occupations',
    expectedResults: { maxRows: 100 },
  },

  // Aggregate Queries
  {
    id: 'onet_agg_01',
    name: 'Skill Category Stats',
    category: 'aggregate',
    sql: `
      SELECT s.category,
             COUNT(DISTINCT s.skill_id) as skill_count,
             COUNT(os.onetsoc_code) as usage_count,
             AVG(os.importance) as avg_importance
      FROM skills s
      LEFT JOIN occupation_skills os ON s.skill_id = os.skill_id
      GROUP BY s.category
      ORDER BY usage_count DESC
    `,
    description: 'Statistics by skill category',
  },
  {
    id: 'onet_agg_02',
    name: 'Ability Category Distribution',
    category: 'aggregate',
    sql: `
      SELECT a.category,
             COUNT(DISTINCT a.ability_id) as ability_count,
             AVG(oa.level) as avg_level
      FROM abilities a
      LEFT JOIN occupation_abilities oa ON a.ability_id = oa.ability_id
      GROUP BY a.category
    `,
    description: 'Ability distribution by category',
  },
  {
    id: 'onet_agg_03',
    name: 'Education Requirements Summary',
    category: 'aggregate',
    sql: `
      SELECT education_level,
             COUNT(*) as occupation_count,
             AVG(experience_months) as avg_experience,
             AVG(job_zone) as avg_job_zone
      FROM occupations
      GROUP BY education_level
      ORDER BY education_level
    `,
    description: 'Summary statistics by education level',
  },
  {
    id: 'onet_agg_04',
    name: 'Most Common Skills',
    category: 'aggregate',
    sql: `
      SELECT s.name, COUNT(*) as usage_count
      FROM occupation_skills os
      JOIN skills s ON os.skill_id = s.skill_id
      GROUP BY os.skill_id, s.name
      ORDER BY usage_count DESC
      LIMIT 10
    `,
    description: 'Top 10 most commonly required skills',
    expectedResults: { maxRows: 10 },
  },

  // Top-N Queries
  {
    id: 'onet_topn_01',
    name: 'Top Cognitive Abilities',
    category: 'top_n',
    sql: `
      SELECT a.name, AVG(oa.importance) as avg_importance
      FROM abilities a
      JOIN occupation_abilities oa ON a.ability_id = oa.ability_id
      WHERE a.category = 'Cognitive'
      GROUP BY a.ability_id, a.name
      ORDER BY avg_importance DESC
      LIMIT 5
    `,
    description: 'Top 5 cognitive abilities by average importance',
    expectedResults: { maxRows: 5 },
  },
  {
    id: 'onet_topn_02',
    name: 'Top Knowledge Areas',
    category: 'top_n',
    sql: `
      SELECT k.name, k.category, COUNT(*) as occupation_count
      FROM knowledge k
      JOIN occupation_knowledge ok ON k.knowledge_id = ok.knowledge_id
      GROUP BY k.knowledge_id, k.name, k.category
      ORDER BY occupation_count DESC
      LIMIT 10
    `,
    description: 'Top 10 knowledge areas by occupation count',
    expectedResults: { maxRows: 10 },
  },
  {
    id: 'onet_topn_03',
    name: 'Highest Paid Major Groups',
    category: 'top_n',
    sql: `
      SELECT major_group, AVG(job_zone) as avg_complexity, COUNT(*) as occupation_count
      FROM occupations
      GROUP BY major_group
      ORDER BY avg_complexity DESC
      LIMIT 5
    `,
    description: 'Top 5 major groups by average job complexity',
    expectedResults: { maxRows: 5 },
  },
  {
    id: 'onet_topn_04',
    name: 'Most Demanding Occupations',
    category: 'top_n',
    sql: `
      SELECT o.title, o.job_zone, o.education_level,
             (SELECT COUNT(*) FROM occupation_skills WHERE onetsoc_code = o.onetsoc_code) as skill_count
      FROM occupations o
      WHERE o.job_zone = 5
      ORDER BY o.education_level DESC
      LIMIT 10
    `,
    description: 'Top 10 most demanding occupations',
    expectedResults: { maxRows: 10 },
  },
];

// =============================================================================
// Dataset Information
// =============================================================================

/**
 * O*NET dataset metadata
 */
export const ONET_METADATA = {
  name: 'O*NET',
  version: '1.0.0',
  description: 'Occupational Information Network database for workforce analysis',
  tables: [
    'occupations',
    'skills',
    'occupation_skills',
    'abilities',
    'occupation_abilities',
    'knowledge',
    'occupation_knowledge',
    'work_activities',
    'occupation_activities',
    'work_contexts',
    'occupation_contexts',
  ],
  approximateRows: 170000,
  queryCategories: [
    'point_lookup',
    'range',
    'join',
    'aggregate',
    'text_search',
    'hierarchical',
    'top_n',
  ],
  queryCount: ONET_BENCHMARK_QUERIES.length,
};

/**
 * Get all schema creation statements in order
 */
export function getOnetSchemaStatements(): string[] {
  return [
    ONET_SCHEMA.occupations,
    ONET_SCHEMA.skills,
    ONET_SCHEMA.occupation_skills,
    ONET_SCHEMA.abilities,
    ONET_SCHEMA.occupation_abilities,
    ONET_SCHEMA.knowledge,
    ONET_SCHEMA.occupation_knowledge,
    ONET_SCHEMA.work_activities,
    ONET_SCHEMA.occupation_activities,
    ONET_SCHEMA.work_contexts,
    ONET_SCHEMA.occupation_contexts,
    ...ONET_INDEXES,
  ];
}

/**
 * Get queries by category
 */
export function getOnetQueriesByCategory(
  category: OnetQueryCategory
): OnetBenchmarkQuery[] {
  return ONET_BENCHMARK_QUERIES.filter((q) => q.category === category);
}
