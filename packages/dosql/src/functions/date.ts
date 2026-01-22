/**
 * SQLite Date/Time Functions
 *
 * Implements SQLite-compatible date/time functions:
 * - date(timestring, modifiers...) - Returns date as YYYY-MM-DD
 * - time(timestring, modifiers...) - Returns time as HH:MM:SS
 * - datetime(timestring, modifiers...) - Returns datetime as YYYY-MM-DD HH:MM:SS
 * - julianday(timestring, modifiers...) - Returns Julian day number
 * - strftime(format, timestring, modifiers...) - Custom formatted datetime
 * - unixepoch(timestring, modifiers...) - Returns Unix timestamp (seconds)
 *
 * Time strings can be:
 * - YYYY-MM-DD
 * - YYYY-MM-DD HH:MM
 * - YYYY-MM-DD HH:MM:SS
 * - YYYY-MM-DD HH:MM:SS.SSS
 * - YYYY-MM-DDTHH:MM:SS
 * - HH:MM
 * - HH:MM:SS
 * - HH:MM:SS.SSS
 * - now
 * - DDDDDDDDDD (Julian day number)
 *
 * Modifiers:
 * - NNN days, NNN hours, NNN minutes, NNN seconds, NNN months, NNN years
 * - start of month, start of year, start of day
 * - weekday N (0=Sunday, 6=Saturday)
 * - localtime, utc
 */

import type { SqlValue } from '../engine/types.js';
import type { SqlFunction, FunctionSignature } from './registry.js';

// =============================================================================
// CONSTANTS
// =============================================================================

/** Julian day number for Unix epoch (Jan 1, 1970 00:00:00 UTC) */
const JULIAN_UNIX_EPOCH = 2440587.5;

/** Milliseconds per day */
const MS_PER_DAY = 86400000;

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Parse a time string into a Date object
 */
function parseTimeString(timestring: SqlValue): Date | null {
  if (timestring === null) return null;

  // Special value 'now'
  if (typeof timestring === 'string' && timestring.toLowerCase() === 'now') {
    return new Date();
  }

  // Already a Date
  if (timestring instanceof Date) {
    return new Date(timestring);
  }

  // Unix timestamp (number of seconds or milliseconds)
  if (typeof timestring === 'number') {
    // If it looks like seconds (before year 3000), multiply by 1000
    if (timestring < 32503680000) {
      return new Date(timestring * 1000);
    }
    return new Date(timestring);
  }

  if (typeof timestring === 'bigint') {
    const num = Number(timestring);
    if (num < 32503680000) {
      return new Date(num * 1000);
    }
    return new Date(num);
  }

  const str = String(timestring).trim();

  // Check if it's a Julian day number (pure number)
  const julianMatch = str.match(/^(\d+\.?\d*)$/);
  if (julianMatch) {
    const julian = parseFloat(julianMatch[1]);
    // Only treat as Julian if it's a reasonable Julian day (> year 0)
    if (julian > 0 && julian < 5373484) {
      return julianToDate(julian);
    }
  }

  // Try to parse ISO 8601 and SQLite date formats
  // YYYY-MM-DD
  // YYYY-MM-DD HH:MM
  // YYYY-MM-DD HH:MM:SS
  // YYYY-MM-DD HH:MM:SS.SSS
  // YYYY-MM-DDTHH:MM:SS

  // Replace 'T' with space for consistent parsing
  const normalized = str.replace('T', ' ');

  // Full datetime with optional subseconds
  const datetimeMatch = normalized.match(
    /^(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?)?$/
  );
  if (datetimeMatch) {
    const [, year, month, day, hour = '0', minute = '0', second = '0', ms = '0'] = datetimeMatch;
    const date = new Date(Date.UTC(
      parseInt(year, 10),
      parseInt(month, 10) - 1,
      parseInt(day, 10),
      parseInt(hour, 10),
      parseInt(minute, 10),
      parseInt(second, 10),
      parseInt(ms.padEnd(3, '0').slice(0, 3), 10)
    ));
    return date;
  }

  // Time only: HH:MM, HH:MM:SS, HH:MM:SS.SSS
  const timeMatch = str.match(/^(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?$/);
  if (timeMatch) {
    const [, hour, minute, second = '0', ms = '0'] = timeMatch;
    // Use epoch date (2000-01-01) as base
    const date = new Date(Date.UTC(
      2000, 0, 1,
      parseInt(hour, 10),
      parseInt(minute, 10),
      parseInt(second, 10),
      parseInt(ms.padEnd(3, '0').slice(0, 3), 10)
    ));
    return date;
  }

  // Try native Date parsing as fallback
  const parsed = new Date(str);
  if (!isNaN(parsed.getTime())) {
    return parsed;
  }

  return null;
}

/**
 * Convert a Date to Julian day number
 */
function dateToJulian(date: Date): number {
  const ms = date.getTime();
  return JULIAN_UNIX_EPOCH + ms / MS_PER_DAY;
}

/**
 * Convert a Julian day number to Date
 */
function julianToDate(julian: number): Date {
  const ms = (julian - JULIAN_UNIX_EPOCH) * MS_PER_DAY;
  return new Date(ms);
}

/**
 * Apply modifiers to a date
 */
function applyModifiers(date: Date, modifiers: SqlValue[]): Date | null {
  let d = new Date(date);

  for (const mod of modifiers) {
    if (mod === null) continue;

    const modifier = String(mod).trim().toLowerCase();

    // NNN days
    const daysMatch = modifier.match(/^([+-]?\d+(?:\.\d+)?)\s+days?$/);
    if (daysMatch) {
      const days = parseFloat(daysMatch[1]);
      d = new Date(d.getTime() + days * MS_PER_DAY);
      continue;
    }

    // NNN hours
    const hoursMatch = modifier.match(/^([+-]?\d+(?:\.\d+)?)\s+hours?$/);
    if (hoursMatch) {
      const hours = parseFloat(hoursMatch[1]);
      d = new Date(d.getTime() + hours * 3600000);
      continue;
    }

    // NNN minutes
    const minutesMatch = modifier.match(/^([+-]?\d+(?:\.\d+)?)\s+minutes?$/);
    if (minutesMatch) {
      const minutes = parseFloat(minutesMatch[1]);
      d = new Date(d.getTime() + minutes * 60000);
      continue;
    }

    // NNN seconds
    const secondsMatch = modifier.match(/^([+-]?\d+(?:\.\d+)?)\s+seconds?$/);
    if (secondsMatch) {
      const seconds = parseFloat(secondsMatch[1]);
      d = new Date(d.getTime() + seconds * 1000);
      continue;
    }

    // NNN months
    const monthsMatch = modifier.match(/^([+-]?\d+)\s+months?$/);
    if (monthsMatch) {
      const months = parseInt(monthsMatch[1], 10);
      d = new Date(Date.UTC(
        d.getUTCFullYear(),
        d.getUTCMonth() + months,
        d.getUTCDate(),
        d.getUTCHours(),
        d.getUTCMinutes(),
        d.getUTCSeconds(),
        d.getUTCMilliseconds()
      ));
      continue;
    }

    // NNN years
    const yearsMatch = modifier.match(/^([+-]?\d+)\s+years?$/);
    if (yearsMatch) {
      const years = parseInt(yearsMatch[1], 10);
      d = new Date(Date.UTC(
        d.getUTCFullYear() + years,
        d.getUTCMonth(),
        d.getUTCDate(),
        d.getUTCHours(),
        d.getUTCMinutes(),
        d.getUTCSeconds(),
        d.getUTCMilliseconds()
      ));
      continue;
    }

    // start of year
    if (modifier === 'start of year') {
      d = new Date(Date.UTC(d.getUTCFullYear(), 0, 1, 0, 0, 0, 0));
      continue;
    }

    // start of month
    if (modifier === 'start of month') {
      d = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1, 0, 0, 0, 0));
      continue;
    }

    // start of day
    if (modifier === 'start of day') {
      d = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 0, 0, 0, 0));
      continue;
    }

    // weekday N (0=Sunday through 6=Saturday)
    const weekdayMatch = modifier.match(/^weekday\s+(\d)$/);
    if (weekdayMatch) {
      const targetDay = parseInt(weekdayMatch[1], 10);
      if (targetDay < 0 || targetDay > 6) continue;

      const currentDay = d.getUTCDay();
      let daysToAdd = targetDay - currentDay;
      if (daysToAdd <= 0) daysToAdd += 7;

      d = new Date(d.getTime() + daysToAdd * MS_PER_DAY);
      continue;
    }

    // localtime - convert from UTC to local time
    if (modifier === 'localtime') {
      const offset = d.getTimezoneOffset();
      d = new Date(d.getTime() - offset * 60000);
      continue;
    }

    // utc - convert from local time to UTC
    if (modifier === 'utc') {
      const offset = d.getTimezoneOffset();
      d = new Date(d.getTime() + offset * 60000);
      continue;
    }
  }

  return d;
}

/**
 * Format date as YYYY-MM-DD
 */
function formatDate(date: Date): string {
  const year = date.getUTCFullYear().toString().padStart(4, '0');
  const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
  const day = date.getUTCDate().toString().padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * Format time as HH:MM:SS
 */
function formatTime(date: Date): string {
  const hours = date.getUTCHours().toString().padStart(2, '0');
  const minutes = date.getUTCMinutes().toString().padStart(2, '0');
  const seconds = date.getUTCSeconds().toString().padStart(2, '0');
  return `${hours}:${minutes}:${seconds}`;
}

/**
 * Format datetime as YYYY-MM-DD HH:MM:SS
 */
function formatDateTime(date: Date): string {
  return `${formatDate(date)} ${formatTime(date)}`;
}

// =============================================================================
// DATE/TIME FUNCTION IMPLEMENTATIONS
// =============================================================================

/**
 * date(timestring, modifiers...) - Returns date as YYYY-MM-DD
 */
export function date(timestring: SqlValue, ...modifiers: SqlValue[]): SqlValue {
  const d = parseTimeString(timestring);
  if (d === null) return null;

  const modified = applyModifiers(d, modifiers);
  if (modified === null) return null;

  return formatDate(modified);
}

/**
 * time(timestring, modifiers...) - Returns time as HH:MM:SS
 */
export function time(timestring: SqlValue, ...modifiers: SqlValue[]): SqlValue {
  const d = parseTimeString(timestring);
  if (d === null) return null;

  const modified = applyModifiers(d, modifiers);
  if (modified === null) return null;

  return formatTime(modified);
}

/**
 * datetime(timestring, modifiers...) - Returns datetime as YYYY-MM-DD HH:MM:SS
 */
export function datetime(timestring: SqlValue, ...modifiers: SqlValue[]): SqlValue {
  const d = parseTimeString(timestring);
  if (d === null) return null;

  const modified = applyModifiers(d, modifiers);
  if (modified === null) return null;

  return formatDateTime(modified);
}

/**
 * julianday(timestring, modifiers...) - Returns Julian day number
 */
export function julianday(timestring: SqlValue, ...modifiers: SqlValue[]): SqlValue {
  const d = parseTimeString(timestring);
  if (d === null) return null;

  const modified = applyModifiers(d, modifiers);
  if (modified === null) return null;

  return dateToJulian(modified);
}

/**
 * strftime(format, timestring, modifiers...) - Custom formatted datetime
 *
 * Format specifiers:
 * %d - day of month: 01-31
 * %f - fractional seconds: SS.SSS
 * %H - hour: 00-24
 * %j - day of year: 001-366
 * %J - Julian day number
 * %m - month: 01-12
 * %M - minute: 00-59
 * %s - seconds since 1970-01-01
 * %S - seconds: 00-59
 * %w - day of week: 0-6 (0=Sunday)
 * %W - week of year: 00-53
 * %Y - year: 0000-9999
 * %% - literal %
 */
export function strftime(
  format: SqlValue,
  timestring: SqlValue,
  ...modifiers: SqlValue[]
): SqlValue {
  if (format === null || timestring === null) return null;

  const fmt = String(format);
  const d = parseTimeString(timestring);
  if (d === null) return null;

  const modified = applyModifiers(d, modifiers);
  if (modified === null) return null;

  return fmt.replace(/%([dDfHIjJmMsSUwWYZ%])/g, (match, specifier) => {
    switch (specifier) {
      case 'd':
        return modified.getUTCDate().toString().padStart(2, '0');
      case 'f': {
        const sec = modified.getUTCSeconds();
        const ms = modified.getUTCMilliseconds();
        return `${sec.toString().padStart(2, '0')}.${ms.toString().padStart(3, '0')}`;
      }
      case 'H':
        return modified.getUTCHours().toString().padStart(2, '0');
      case 'I': {
        const h = modified.getUTCHours();
        return (h % 12 || 12).toString().padStart(2, '0');
      }
      case 'j': {
        const start = new Date(Date.UTC(modified.getUTCFullYear(), 0, 0));
        const diff = modified.getTime() - start.getTime();
        const day = Math.floor(diff / MS_PER_DAY);
        return day.toString().padStart(3, '0');
      }
      case 'J':
        return dateToJulian(modified).toFixed(6);
      case 'm':
        return (modified.getUTCMonth() + 1).toString().padStart(2, '0');
      case 'M':
        return modified.getUTCMinutes().toString().padStart(2, '0');
      case 's':
        return Math.floor(modified.getTime() / 1000).toString();
      case 'S':
        return modified.getUTCSeconds().toString().padStart(2, '0');
      case 'U': {
        // Week of year (Sunday = first day of week)
        const start = new Date(Date.UTC(modified.getUTCFullYear(), 0, 1));
        const diff = modified.getTime() - start.getTime();
        const dayOfYear = Math.floor(diff / MS_PER_DAY);
        const sundayOffset = (start.getUTCDay() + 6) % 7;
        const week = Math.floor((dayOfYear + sundayOffset) / 7);
        return week.toString().padStart(2, '0');
      }
      case 'w':
        return modified.getUTCDay().toString();
      case 'W': {
        // Week of year (Monday = first day of week)
        const start = new Date(Date.UTC(modified.getUTCFullYear(), 0, 1));
        const diff = modified.getTime() - start.getTime();
        const dayOfYear = Math.floor(diff / MS_PER_DAY);
        const mondayOffset = start.getUTCDay() === 0 ? 6 : start.getUTCDay() - 1;
        const week = Math.floor((dayOfYear + mondayOffset) / 7);
        return week.toString().padStart(2, '0');
      }
      case 'Y':
        return modified.getUTCFullYear().toString().padStart(4, '0');
      case 'Z':
        return 'UTC';
      case '%':
        return '%';
      default:
        return match;
    }
  });
}

/**
 * unixepoch(timestring, modifiers...) - Returns Unix timestamp (seconds)
 * With 'subsec' modifier, returns fractional seconds
 */
export function unixepoch(timestring: SqlValue, ...modifiers: SqlValue[]): SqlValue {
  const d = parseTimeString(timestring);
  if (d === null) return null;

  // Check for 'subsec' modifier
  const hasSubsec = modifiers.some(
    m => m !== null && String(m).toLowerCase() === 'subsec'
  );

  // Filter out 'subsec' from modifiers before applying
  const otherModifiers = modifiers.filter(
    m => m === null || String(m).toLowerCase() !== 'subsec'
  );

  const modified = applyModifiers(d, otherModifiers);
  if (modified === null) return null;

  if (hasSubsec) {
    return modified.getTime() / 1000;
  }

  return Math.floor(modified.getTime() / 1000);
}

/**
 * current_date - Returns current date as YYYY-MM-DD
 */
export function current_date(): SqlValue {
  return formatDate(new Date());
}

/**
 * current_time - Returns current time as HH:MM:SS
 */
export function current_time(): SqlValue {
  return formatTime(new Date());
}

/**
 * current_timestamp - Returns current datetime as YYYY-MM-DD HH:MM:SS
 */
export function current_timestamp(): SqlValue {
  return formatDateTime(new Date());
}

/**
 * timediff(time1, time2) - Returns difference between two times
 * Result is in format HH:MM:SS.SSS (can be negative)
 */
export function timediff(time1: SqlValue, time2: SqlValue): SqlValue {
  const d1 = parseTimeString(time1);
  const d2 = parseTimeString(time2);

  if (d1 === null || d2 === null) return null;

  let diffMs = d1.getTime() - d2.getTime();
  const negative = diffMs < 0;
  diffMs = Math.abs(diffMs);

  const hours = Math.floor(diffMs / 3600000);
  diffMs %= 3600000;
  const minutes = Math.floor(diffMs / 60000);
  diffMs %= 60000;
  const seconds = Math.floor(diffMs / 1000);
  const ms = diffMs % 1000;

  const sign = negative ? '-' : '';
  const time = `${sign}${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}.${ms.toString().padStart(3, '0')}`;

  return time;
}

// =============================================================================
// FUNCTION SIGNATURES FOR REGISTRY
// =============================================================================

export const dateFunctions: Record<string, SqlFunction> = {
  date: { fn: date, minArgs: 1, maxArgs: Infinity },
  time: { fn: time, minArgs: 1, maxArgs: Infinity },
  datetime: { fn: datetime, minArgs: 1, maxArgs: Infinity },
  julianday: { fn: julianday, minArgs: 1, maxArgs: Infinity },
  strftime: { fn: strftime, minArgs: 2, maxArgs: Infinity },
  unixepoch: { fn: unixepoch, minArgs: 1, maxArgs: Infinity },
  current_date: { fn: current_date, minArgs: 0, maxArgs: 0 },
  current_time: { fn: current_time, minArgs: 0, maxArgs: 0 },
  current_timestamp: { fn: current_timestamp, minArgs: 0, maxArgs: 0 },
  now: { fn: current_timestamp, minArgs: 0, maxArgs: 0 },
  timediff: { fn: timediff, minArgs: 2, maxArgs: 2 },
};

export const dateSignatures: Record<string, FunctionSignature> = {
  date: {
    name: 'date',
    params: [
      { name: 'timestring', type: 'string' },
      { name: 'modifiers', type: 'string', variadic: true },
    ],
    returnType: 'string',
    description: 'Returns date as YYYY-MM-DD',
  },
  time: {
    name: 'time',
    params: [
      { name: 'timestring', type: 'string' },
      { name: 'modifiers', type: 'string', variadic: true },
    ],
    returnType: 'string',
    description: 'Returns time as HH:MM:SS',
  },
  datetime: {
    name: 'datetime',
    params: [
      { name: 'timestring', type: 'string' },
      { name: 'modifiers', type: 'string', variadic: true },
    ],
    returnType: 'string',
    description: 'Returns datetime as YYYY-MM-DD HH:MM:SS',
  },
  julianday: {
    name: 'julianday',
    params: [
      { name: 'timestring', type: 'string' },
      { name: 'modifiers', type: 'string', variadic: true },
    ],
    returnType: 'number',
    description: 'Returns Julian day number',
  },
  strftime: {
    name: 'strftime',
    params: [
      { name: 'format', type: 'string' },
      { name: 'timestring', type: 'string' },
      { name: 'modifiers', type: 'string', variadic: true },
    ],
    returnType: 'string',
    description: 'Custom formatted datetime',
  },
  unixepoch: {
    name: 'unixepoch',
    params: [
      { name: 'timestring', type: 'string' },
      { name: 'modifiers', type: 'string', variadic: true },
    ],
    returnType: 'number',
    description: 'Returns Unix timestamp (seconds since 1970-01-01)',
  },
  current_date: {
    name: 'current_date',
    params: [],
    returnType: 'string',
    description: 'Returns current date as YYYY-MM-DD',
  },
  current_time: {
    name: 'current_time',
    params: [],
    returnType: 'string',
    description: 'Returns current time as HH:MM:SS',
  },
  current_timestamp: {
    name: 'current_timestamp',
    params: [],
    returnType: 'string',
    description: 'Returns current datetime as YYYY-MM-DD HH:MM:SS',
  },
};
