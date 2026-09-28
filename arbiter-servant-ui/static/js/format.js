// ---------------------------------------------------------------------------
// Pure utility functions
// ---------------------------------------------------------------------------

function truncate(str, len = 60) {
  if (!str) return '';
  const s = typeof str === 'string' ? str : JSON.stringify(str);
  return s.length > len ? s.substring(0, len) + '...' : s;
}

function formatJson(obj) {
  try {
    return JSON.stringify(obj, null, 2);
  } catch {
    return String(obj);
  }
}

// The placeholder for a value that is absent. Every table cell, tile and
// drawer field renders this rather than its own dash.
const EMPTY = '\u2014';

// Compact count: exact below 1000, then 1.2K / 15.2K / 1.5M.
const _compactNumFmt = new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 });
function formatCompact(n) {
  return n == null ? EMPTY : _compactNumFmt.format(n);
}

// Count noun for a total. Pass an explicit plural where adding "s" is wrong.
function pluralize(n, one, many) {
  return n === 1 ? one : (many || one + 's');
}

function formatTime(iso, fallback = '') {
  if (!iso) return fallback;
  try {
    return new Date(iso).toLocaleString(undefined, {
      year: 'numeric', month: 'numeric', day: 'numeric',
      hour: 'numeric', minute: '2-digit', second: '2-digit',
    });
  } catch {
    return iso;
  }
}

// Wall-clock time, for a live log where every row arrived seconds ago and a
// relative age would read the same on all of them.
function formatClock(iso, fallback = '') {
  if (!iso) return fallback;
  try {
    return new Date(iso).toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit', second: '2-digit' });
  } catch {
    return iso;
  }
}

const MS_PER_SECOND = 1000;
const SECONDS_PER_MINUTE = 60;
const SECONDS_PER_HOUR = 3600;
const SECONDS_PER_DAY = 86400;

// Duration units, largest first, and how many of them a humanized duration shows
// when each one leads.
const DURATION_UNITS = [['d', SECONDS_PER_DAY], ['h', SECONDS_PER_HOUR], ['m', SECONDS_PER_MINUTE], ['s', 1]];
const DURATION_PARTS = { d: 2, h: 2, m: 1, s: 1 };

// A second count as [value, unit] pairs from its leading unit down. It is rounded
// at the smallest unit shown, and the carry moves up: 3599 is 1h, never 60m.
function durationParts(secs, partsFor) {
  const smallest = DURATION_UNITS.length - 1;
  const leadAt = (n) => {
    const i = DURATION_UNITS.findIndex(([, size]) => n >= size);
    return i < 0 ? smallest : i;
  };
  const lastAt = (i) => Math.min(i + partsFor(DURATION_UNITS[i][0]) - 1, smallest);
  const raw = Math.max(0, secs);
  const step = DURATION_UNITS[lastAt(leadAt(raw))][1];
  let rest = Math.round(raw / step) * step;
  const lead = leadAt(rest);
  return DURATION_UNITS.slice(lead, lastAt(lead) + 1).map(([unit, size]) => {
    const value = Math.floor(rest / size);
    rest -= value * size;
    return [value, unit];
  });
}

function formatAge(iso, fallback = EMPTY) {
  if (!iso) return fallback;
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return iso;
  const [[value, unit]] = durationParts((Date.now() - t) / MS_PER_SECOND, () => 1);
  return `${value}${unit} ago`;
}

// Humanized duration from a second count: 45s / 12m / 3h 20m / 2d 4h.
function formatDurationSecs(secs, fallback = EMPTY) {
  if (secs == null || Number.isNaN(secs)) return fallback;
  return durationParts(secs, (unit) => DURATION_PARTS[unit])
    .filter(([value], i) => i === 0 || value)
    .map(([value, unit]) => value + unit)
    .join(' ');
}

// A clock field: two digits, zero-padded.
function padTwo(n) {
  return String(n).padStart(2, '0');
}

function formatCountdown(iso, fallback = '') {
  if (!iso) return fallback;
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return iso;
  const delta = Math.round((t - Date.now()) / MS_PER_SECOND);
  if (delta <= 0) return 'ready';
  const days = Math.floor(delta / SECONDS_PER_DAY);
  const h = Math.floor((delta % SECONDS_PER_DAY) / SECONDS_PER_HOUR);
  const m = Math.floor((delta % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
  const s = delta % SECONDS_PER_MINUTE;
  const hms = `${padTwo(h)}:${padTwo(m)}:${padTwo(s)}`;
  return days > 0 ? `${days}d ${hms}` : hms;
}

/**
 * Runs `worker(item, i)` over `items` with at most `limit` in flight at once.
 * Returns a Promise.allSettled-style array in input order.
 */
async function mapLimit(items, limit, worker) {
  const results = new Array(items.length);
  let next = 0;
  const run = async () => {
    while (next < items.length) {
      const i = next++;
      try {
        results[i] = { status: 'fulfilled', value: await worker(items[i], i) };
      } catch (reason) {
        results[i] = { status: 'rejected', reason };
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, run));
  return results;
}

// The leading run of a UUID, enough to tell two workers apart in a cell or a chip.
function shortId(id) {
  return String(id).slice(0, SHORT_ID_CHARS);
}

// Characters of a UUID a short form keeps: its first hyphen-delimited group.
const SHORT_ID_CHARS = 8;

// A datetime-local field's value as a UTC instant. The field carries local wall-clock
// with no zone, so the reader's own zone is what resolves it. Blank stays blank, and an
// unparseable value is dropped rather than sent as a filter nobody asked for.
function toIsoInstant(localValue) {
  if (!localValue) return undefined;
  const at = new Date(localValue);
  return Number.isNaN(at.getTime()) ? undefined : at.toISOString();
}

// An instant as a datetime-local field's value, in the reader's own zone: the
// inverse of toIsoInstant. An unparseable value comes back blank.
function toLocalInput(value) {
  if (!value) return '';
  const at = new Date(value);
  if (Number.isNaN(at.getTime())) return '';
  const date = `${at.getFullYear()}-${padTwo(at.getMonth() + 1)}-${padTwo(at.getDate())}`;
  const time = `${padTwo(at.getHours())}:${padTwo(at.getMinutes())}`;
  return `${date}T${time}` + (at.getSeconds() ? ':' + padTwo(at.getSeconds()) : '');
}
