/**
 * Shared utilities for Alpine.js components.
 * Loaded before component scripts so functions are available globally.
 */

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
// URL filter sync
// ---------------------------------------------------------------------------

/** Keys managed by tab components — cleared on tab switch. */
const _filterKeys = ['group_key', 'parent_id', 'suspended', 'in_flight'];

function readFiltersFromUrl() {
  const p = new URLSearchParams(location.search);
  return {
    groupKey: p.get('group_key') || '',
    parentId: p.get('parent_id') || '',
    suspended: p.get('suspended') || '',
    inFlight: p.get('in_flight') === 'true',
  };
}

function writeFiltersToUrl(filters) {
  const url = new URL(location.href);
  for (const k of _filterKeys) url.searchParams.delete(k);
  if (filters.groupKey) url.searchParams.set('group_key', filters.groupKey);
  if (filters.parentId) url.searchParams.set('parent_id', filters.parentId);
  if (filters.suspended) url.searchParams.set('suspended', filters.suspended);
  if (filters.inFlight) url.searchParams.set('in_flight', 'true');
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
