/**
 * Shared utilities for Alpine.js components.
 * Loaded before component scripts so functions are available globally.
 */

// Position dropdown menus with Popper's "fixed" strategy so they escape the
// horizontal-scroll clipping of .table-responsive, without forcing the
// container's overflow open (which would reset its scroll position).
if (window.bootstrap && bootstrap.Dropdown) {
  bootstrap.Dropdown.Default.popperConfig = (defaults) => ({ ...defaults, strategy: 'fixed' });
}

// Click-to-arm confirmation, a replacement for window.confirm on destructive
// actions. Spread into a component with ...confirmArm(), then guard the handler
// with `if (!this.confirmArmed(key)) return` and reflect isArmed(key) in the label.
function confirmArm() {
  return {
    _armed: null,
    _armedTimer: null,
    // First click arms the key and returns false. A second click on the same key
    // within the window confirms it and returns true. Arming a new key disarms
    // the previous one, and an unconfirmed key relaxes after a few seconds.
    confirmArmed(key) {
      if (this._armed === key) {
        clearTimeout(this._armedTimer);
        this._armed = null;
        return true;
      }
      this._armed = key;
      clearTimeout(this._armedTimer);
      this._armedTimer = setTimeout(() => { this._armed = null; }, 3000);
      return false;
    },
    isArmed(key) {
      return this._armed === key;
    },
  };
}

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

// ---------------------------------------------------------------------------
// Toast notifications
// ---------------------------------------------------------------------------

function showToast(message, type = 'danger') {
  const container = document.getElementById('toastContainer');
  const id = 'toast-' + Date.now();
  const bg = {
    danger: 'bg-danger-subtle text-danger-emphasis',
    success: 'bg-success-subtle text-success-emphasis',
    warning: 'bg-warning-subtle text-warning-emphasis',
    info: 'bg-info-subtle text-info-emphasis',
  }[type] || 'bg-danger-subtle text-danger-emphasis';
  const el = document.createElement('div');
  el.id = id;
  el.className = `toast ${bg}`;
  el.setAttribute('role', 'alert');
  el.innerHTML = `<div class="d-flex">
    <div class="toast-body"></div>
    <button type="button" class="btn-close me-2 m-auto" data-bs-dismiss="toast"></button>
  </div>`;
  el.querySelector('.toast-body').textContent = message;
  container.appendChild(el);
  const toast = new bootstrap.Toast(el, { delay: 5000 });
  el.addEventListener('hidden.bs.toast', () => el.remove());
  toast.show();
}

// ---------------------------------------------------------------------------
// Pagination mixin
// ---------------------------------------------------------------------------

/**
 * Adds limit/offset pagination properties and methods to a component object.
 *
 * The component must define `total` (data source count) and a load method
 * whose name is passed as `loadMethod` (e.g. 'loadJobs').
 *
 * Provided properties: limit, offset, loaded
 * Provided getters:    currentPage, totalPages
 * Provided methods:    goToPage, nextPage, prevPage
 */
function withPagination(component, loadMethod) {
  const pagination = {
    limit: 50,
    offset: 0,
    loaded: false,

    get currentPage() {
      return Math.floor(this.offset / this.limit) + 1;
    },

    get totalPages() {
      return Math.max(1, Math.ceil(this.total / this.limit));
    },

    goToPage(page) {
      const p = Math.max(1, Math.min(this.totalPages, parseInt(page, 10) || 1));
      this.offset = (p - 1) * this.limit;
      this[loadMethod]();
    },

    nextPage() {
      if (this.currentPage < this.totalPages) {
        this.offset += this.limit;
        this[loadMethod]();
      }
    },

    prevPage() {
      this.offset = Math.max(0, this.offset - this.limit);
      this[loadMethod]();
    },
  };

  // Use Object.defineProperties to preserve getter semantics through the merge
  const result = {};
  Object.defineProperties(result, Object.getOwnPropertyDescriptors(component));
  Object.defineProperties(result, Object.getOwnPropertyDescriptors(pagination));
  return result;
}

// ---------------------------------------------------------------------------
// Column show/hide preferences
// ---------------------------------------------------------------------------

// Persisted column visibility shared by table tabs. Pass an ordered registry of
// { key, label, weight, required? } and a localStorage key.
function columnPrefs(columns, storageKey) {
  return {
    columns,
    colVis: {},

    _loadColPrefs() {
      const def = {};
      columns.forEach((c) => { def[c.key] = true; });
      let saved = {};
      try { saved = JSON.parse(localStorage.getItem(storageKey)) || {}; } catch { saved = {}; }
      this.colVis = { ...def, ...saved };
    },

    colVisible(key) {
      return this.colVis[key] !== false;
    },

    toggleCol(key) {
      if (columns.find((c) => c.key === key)?.required) return;
      this.colVis = { ...this.colVis, [key]: !this.colVisible(key) };
      localStorage.setItem(storageKey, JSON.stringify(this.colVis));
    },

    // Width renormalized over visible columns, so the table fills 100% and
    // hiding a column lets the rest grow to fill the freed space.
    colPct(key) {
      const total = columns.reduce((sum, c) => sum + (this.colVisible(c.key) ? c.weight : 0), 0);
      const col = columns.find((c) => c.key === key);
      return total ? (col.weight / total) * 100 : 0;
    },

    resetColumns() {
      this.colVis = {};
      columns.forEach((c) => { this.colVis[c.key] = true; });
      localStorage.removeItem(storageKey);
    },
  };
}

// ---------------------------------------------------------------------------
// URL filter sync
// ---------------------------------------------------------------------------

// Filter keys cleared on tab switch.
const _filterKeys = ['group_key', 'parent_id', 'status', 'sort_by', 'sort_dir'];

function readFiltersFromUrl() {
  const p = new URLSearchParams(location.search);
  return {
    groupKey: p.get('group_key') || '',
    parentId: p.get('parent_id') || '',
    status: p.get('status') || '',
    sortBy: p.get('sort_by') || '',
    sortDir: p.get('sort_dir') || '',
  };
}

function writeFiltersToUrl(filters) {
  const url = new URL(location.href);
  for (const k of _filterKeys) url.searchParams.delete(k);
  if (filters.groupKey) url.searchParams.set('group_key', filters.groupKey);
  if (filters.parentId) url.searchParams.set('parent_id', filters.parentId);
  if (filters.status) url.searchParams.set('status', filters.status);
  if (filters.sortBy) url.searchParams.set('sort_by', filters.sortBy);
  if (filters.sortDir) url.searchParams.set('sort_dir', filters.sortDir);
  history.replaceState(null, '', url);
}

function clearFiltersFromUrl() {
  const url = new URL(location.href);
  for (const k of _filterKeys) url.searchParams.delete(k);
  history.replaceState(null, '', url);
}

// ---------------------------------------------------------------------------
// Tab-active tracking
// ---------------------------------------------------------------------------

/**
 * Sets `component.active` from DOM state and registers Bootstrap tab listeners.
 *
 * @param {Object}   component  - Alpine component (usually `this`)
 * @param {string}   tabTarget  - data-bs-target value, e.g. '#tab-events'
 * @param {Object}   [callbacks]
 * @param {Function} [callbacks.onShow] - called when tab becomes visible
 * @param {Function} [callbacks.onHide] - called when tab becomes hidden
 */
function trackTabActive(component, tabTarget, callbacks) {
  const tab = document.querySelector('[data-bs-target="' + tabTarget + '"]');
  component.active = !!(tab && tab.classList.contains('active'));

  if (component.active && callbacks && callbacks.onShow) {
    callbacks.onShow();
  }

  document.addEventListener('shown.bs.tab', (e) => {
    if (e.target.getAttribute('data-bs-target') === tabTarget) {
      component.active = true;
      if (callbacks && callbacks.onShow) callbacks.onShow();
    }
  });
  document.addEventListener('hidden.bs.tab', (e) => {
    if (e.target.getAttribute('data-bs-target') === tabTarget) {
      component.active = false;
      if (callbacks && callbacks.onHide) callbacks.onHide();
    }
  });
}
