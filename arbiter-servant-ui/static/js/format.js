// Pure helpers. Every export is also available to templates.
/** @import { Schema } from '../../types/client' */

// The placeholder for an absent value.
export const EMPTY = '—';

export const MS_PER_SECOND = 1000;
export const SECONDS_PER_MINUTE = 60;
export const SECONDS_PER_HOUR = 3600;
export const SECONDS_PER_DAY = 86400;

// Characters of a UUID a short form keeps: its first hyphen-delimited group.
const SHORT_ID_CHARS = 8;
const TRUNCATE_CHARS = 60;
const DATE_ONLY_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

// A row re-renders on every poll, so each object is serialized once.
// Caches by object identity. Rows are replaced whole on each poll.
export function memoByObject(f) {
  const cache = new WeakMap();
  return (v) => {
    if (v === null || typeof v !== 'object') return f(v);
    let s = cache.get(v);
    if (s === undefined) cache.set(v, (s = f(v)));
    return s;
  };
}

export const jsonText = memoByObject((v) => JSON.stringify(v));

export function truncate(v, len = TRUNCATE_CHARS) {
  if (v == null) return '';
  const s = typeof v === 'string' ? v : jsonText(v);
  return s.length > len ? s.slice(0, len) + '...' : s;
}

// A payload cell. A JSON null payload reads null.
export const truncatePayload = (v) => (v === null ? 'null' : truncate(v));

export const formatJson = memoByObject((v) => {
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v);
  }
});

const compactFmt = new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 });

// Exact below 1000, then 1.2K / 15.2K / 1.5M.
export function formatCompact(n) {
  return n == null ? EMPTY : compactFmt.format(n);
}

// Pass an explicit plural where adding "s" is wrong.
export function pluralize(n, one, many) {
  return n === 1 ? one : many || one + 's';
}

function dateString(iso, fallback, fmt) {
  if (!iso) return fallback;
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : fmt(d);
}

const timeFmt = new Intl.DateTimeFormat(undefined, {
  year: 'numeric',
  month: 'numeric',
  day: 'numeric',
  hour: 'numeric',
  minute: '2-digit',
  second: '2-digit',
});
const clockFmt = new Intl.DateTimeFormat(undefined, { hour: 'numeric', minute: '2-digit', second: '2-digit' });

export function formatTime(iso, fallback = '') {
  return dateString(iso, fallback, (d) => timeFmt.format(d));
}

// Wall-clock time, for a live log where every row arrived seconds ago.
export function formatClock(iso, fallback = '') {
  return dateString(iso, fallback, (d) => clockFmt.format(d));
}

// Units largest first, with how many parts a duration shows when each one leads.
/** @type {[string, number][]} */
const DURATION_UNITS = [
  ['d', SECONDS_PER_DAY],
  ['h', SECONDS_PER_HOUR],
  ['m', SECONDS_PER_MINUTE],
  ['s', 1],
];
const DURATION_PARTS = { d: 2, h: 2, m: 1, s: 1 };

// A second count as [value, unit] pairs from its leading unit down. It rounds at
// the smallest unit shown and carries upward: 3599 is 1h, never 60m.
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

export function formatAge(iso, fallback = EMPTY) {
  return dateString(iso, fallback, (d) => {
    const [[value, unit]] = durationParts((Date.now() - d.getTime()) / MS_PER_SECOND, () => 1);
    return `${value}${unit} ago`;
  });
}

// 45s / 12m / 3h 20m / 2d 4h.
export function formatDuration(secs, fallback = EMPTY) {
  if (secs == null || Number.isNaN(secs)) return fallback;
  return durationParts(secs, (unit) => DURATION_PARTS[unit])
    .filter(([value], i) => i === 0 || value)
    .map(([value, unit]) => `${value}${unit}`)
    .join(' ');
}

const padTwo = (n) => String(n).padStart(2, '0');

export function formatCountdown(iso, fallback = '') {
  return dateString(iso, fallback, (d) => {
    const delta = Math.round((d.getTime() - Date.now()) / MS_PER_SECOND);
    if (delta <= 0) return 'ready';
    const days = Math.floor(delta / SECONDS_PER_DAY);
    const hms = [
      Math.floor((delta % SECONDS_PER_DAY) / SECONDS_PER_HOUR),
      Math.floor((delta % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE),
      delta % SECONDS_PER_MINUTE,
    ]
      .map(padTwo)
      .join(':');
    return days > 0 ? `${days}d ${hms}` : hms;
  });
}

// The leading run of a UUID, enough to tell two workers apart.
export function shortId(id) {
  return String(id).slice(0, SHORT_ID_CHARS);
}

// A datetime-local value as a UTC instant, in the reader's zone. Unparseable is dropped.
export function toIsoInstant(local) {
  if (!local) return undefined;
  const at = new Date(DATE_ONLY_PATTERN.test(local) ? local + 'T00:00' : local);
  return Number.isNaN(at.getTime()) ? undefined : at.toISOString();
}

// An instant as a datetime-local value in the reader's zone. Unparseable is blank.
export function toLocalInput(value) {
  if (!value) return '';
  const at = new Date(value);
  if (Number.isNaN(at.getTime())) return '';
  const date = `${at.getFullYear()}-${padTwo(at.getMonth() + 1)}-${padTwo(at.getDate())}`;
  const time = `${padTwo(at.getHours())}:${padTwo(at.getMinutes())}`;
  return `${date}T${time}` + (at.getSeconds() ? ':' + padTwo(at.getSeconds()) : '');
}

// Runs worker over items with at most limit in flight. Settled results, in order.
export async function mapLimit(items, limit, worker) {
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

// An insert payload: any JSON value, or bare text as a string. Text that opens
// like a JSON string, object or array must parse.
export function parsePayload(raw) {
  try {
    return { value: JSON.parse(raw) };
  } catch (e) {
    return /^["[{]/.test(raw) ? { error: e.message } : { value: raw };
  }
}

// An optional whole-number field. Blank is null, invalid is { error: true }.
export function parseOptionalInt(v, min) {
  if (v === '' || v == null) return { value: null };
  const n = Number(v);
  if (!Number.isInteger(n) || (min != null && n < min)) return { error: true };
  return { value: n };
}

// An override field. Off is null (revert to default). On, the value must pass check.
export function parseOverride(on, v, check) {
  if (!on) return { value: null };
  const n = v === '' || v == null ? NaN : Number(v);
  return check(n) ? { value: n } : { error: true };
}

// Every status the server can filter jobs by.
/** @type {Schema<'JobStatus'>[]} */
export const JOB_STATUSES = ['ready', 'in_flight', 'backoff', 'scheduled', 'throttled', 'exhausted', 'suspended', 'cancelled'];

// The queue stats field that counts each status. Ready is waitingJobs().
/** @type {Record<Exclude<Schema<'JobStatus'>, 'ready'>, keyof Schema<'QueueStats'>>} */
const STATUS_COUNTS = {
  in_flight: 'inFlightJobs',
  scheduled: 'scheduledJobs',
  backoff: 'backoffJobs',
  exhausted: 'exhaustedJobs',
  throttled: 'throttledJobs',
  suspended: 'suspendedJobs',
  cancelled: 'cancelledJobs',
};

const STATUS_BADGES = {
  suspended: 'bg-warning-subtle text-warning-emphasis',
  cancelled: 'bg-dark-subtle text-dark-emphasis',
  in_flight: 'bg-primary-subtle text-primary-emphasis',
  backoff: 'bg-danger-subtle text-danger-emphasis',
  scheduled: 'bg-secondary-subtle text-secondary-emphasis',
  throttled: 'bg-info-subtle text-info-emphasis',
  exhausted: 'bg-danger-subtle text-danger-emphasis',
  ready: 'bg-success-subtle text-success-emphasis',
  blocked: 'blocked-badge',
};

export function statusBadgeClass(status) {
  return STATUS_BADGES[status] || 'bg-secondary-subtle text-secondary-emphasis';
}

// Tooltips for the queue measures on the queue list and the queue page.
export const WAITING_TITLE = 'Visible jobs waiting for a claim: ready plus blocked';
export const READY_TITLE = 'Jobs a claim takes now';
export const BLOCKED_TITLE = 'Visible jobs a claim skips for now: behind a group head, a full concurrency key, or an empty rate-limit bucket';
export const OLDEST_READY_TITLE = 'Age of the oldest ready or blocked job, since it became visible';
export const OLDEST_RUNNING_TITLE = 'How long the oldest in-flight job has been running. A handler that never returns keeps climbing.';

// Visible jobs waiting for a claim: ready plus blocked. Unknown without stats.
export function waitingJobs(stats) {
  return stats ? stats.readyJobs + stats.blockedJobs : undefined;
}

// A status's count in queue stats. Ready counts as waiting, and no status is the total.
export function statusCount(stats, status) {
  if (status === 'ready') return waitingJobs(stats);
  return stats?.[status ? STATUS_COUNTS[status] : 'totalJobs'];
}

export function gateLabel(g, empty = EMPTY) {
  return g ? g.prefix + ':' + g.suffix : empty;
}

export const PERCENT = 100;

// Fill percents where the color bands change.
const LOW_FILL_DANGER_PCT = 25;
const LOW_FILL_WARNING_PCT = 50;
const HIGH_FILL_WARNING_PCT = 75;
const HIGH_FILL_DANGER_PCT = 100;

// A fraction as an integer percent in 0..100. Null is empty.
export function pct(frac) {
  return frac == null ? 0 : Math.max(0, Math.min(PERCENT, Math.round(frac * PERCENT)));
}

// Color band where low fill is bad (remaining tokens).
export function lowFillClass(p) {
  return p < LOW_FILL_DANGER_PCT ? 'bg-danger' : p < LOW_FILL_WARNING_PCT ? 'bg-warning' : 'bg-success';
}

// Color band where high fill is bad (slots in use).
export function highFillClass(p) {
  return p >= HIGH_FILL_DANGER_PCT ? 'bg-danger' : p >= HIGH_FILL_WARNING_PCT ? 'bg-warning' : 'bg-success';
}

export function zeroClass(n) {
  return n ? '' : 'is-zero';
}

// Tree indent: one band per ancestor. The band colors repeat by depth. Empty at the root.
const TREE_BAND_PX = 12;
const TREE_BAND_FADE_PX = 6;
const TREE_BAND_TOKENS = ['--arb-teal-dim', '--arb-purple', '--arb-gold'];
const TREE_BAND_MIX_PCT = 35;

export function treeIndentStyle(depth) {
  if (!depth) return '';
  const stops = Array.from({ length: depth }, (_, level) => {
    const token = TREE_BAND_TOKENS[level % TREE_BAND_TOKENS.length];
    const from = level * TREE_BAND_PX;
    return `color-mix(in srgb, var(${token}) ${TREE_BAND_MIX_PCT}%, transparent) ${from}px ${from + TREE_BAND_PX}px`;
  });
  const indent = depth * TREE_BAND_PX + TREE_BAND_FADE_PX;
  stops.push(`transparent ${indent}px`);
  return `background: linear-gradient(to right, ${stops.join(', ')}); padding-left: calc(var(--bs-table-cell-padding-x, 0.5rem) + ${indent}px)`;
}
