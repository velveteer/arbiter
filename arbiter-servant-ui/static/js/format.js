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

function formatAge(iso, fallback = EMPTY) {
  if (!iso) return fallback;
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return iso;
  const ageSecs = Math.max(0, (Date.now() - t) / 1000);
  if (ageSecs < 60) return `${Math.round(ageSecs)}s ago`;
  if (ageSecs < 3600) return `${Math.round(ageSecs / 60)}m ago`;
  if (ageSecs < 86400) return `${Math.round(ageSecs / 3600)}h ago`;
  return `${Math.round(ageSecs / 86400)}d ago`;
}

// Humanized duration from a second count: 45s / 12m / 3h 20m / 2d 4h.
function formatDurationSecs(secs, fallback = EMPTY) {
  if (secs == null || Number.isNaN(secs)) return fallback;
  const s = Math.max(0, Math.round(secs));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  if (s < 86400) {
    const h = Math.floor(s / 3600);
    const m = Math.round((s % 3600) / 60);
    return m ? `${h}h ${m}m` : `${h}h`;
  }
  const d = Math.floor(s / 86400);
  const h = Math.round((s % 86400) / 3600);
  return h ? `${d}d ${h}h` : `${d}d`;
}

function formatCountdown(iso, fallback = '') {
  if (!iso) return fallback;
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return iso;
  const delta = Math.round((t - Date.now()) / 1000);
  if (delta <= 0) return 'ready';
  const days = Math.floor(delta / 86400);
  const h = Math.floor((delta % 86400) / 3600);
  const m = Math.floor((delta % 3600) / 60);
  const s = delta % 60;
  const pad = (n) => String(n).padStart(2, '0');
  const hms = `${pad(h)}:${pad(m)}:${pad(s)}`;
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
