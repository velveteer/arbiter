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

// Compact count: exact below 1000, then 1.2K / 15.2K / 1.5M. Null renders as an em dash.
const _compactNumFmt = new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 });
function formatCompact(n) {
  return n == null ? '—' : _compactNumFmt.format(n);
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

function formatAge(iso, fallback = '-') {
  if (!iso) return fallback;
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return iso;
  const ageSecs = Math.max(0, (Date.now() - t) / 1000);
  if (ageSecs < 60) return `${Math.round(ageSecs)}s ago`;
  if (ageSecs < 3600) return `${Math.round(ageSecs / 60)}m ago`;
  if (ageSecs < 86400) return `${Math.round(ageSecs / 3600)}h ago`;
  return `${Math.round(ageSecs / 86400)}d ago`;
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
