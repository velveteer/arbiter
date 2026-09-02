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

// Close the dropdown a clicked item lives in. Used for single-shot actions in
// menus that set data-bs-auto-close="outside" (so click-to-arm items survive).
function closeDropdown(el) {
  const toggle = el.closest('.dropdown')?.querySelector('[data-bs-toggle="dropdown"]');
  if (toggle) bootstrap.Dropdown.getOrCreateInstance(toggle).hide();
}

// Shared filter-builder markup (chips + field dropdown + value adder), stamped
// into each table toolbar with x-html. Its bindings resolve against the tableTab
// mixin, so every tab that spreads tableTab renders an identical builder.
const FILTER_BUILDER_HTML = `
  <template x-for="chip in activeFilterChips()" :key="chip.field">
    <span class="filter-chip">
      <span class="filter-chip-label" x-text="chip.label + ':'"></span>
      <span class="filter-chip-value" x-text="chip.value"></span>
      <button type="button" class="filter-chip-x" @click="removeFilter(chip.field)" :aria-label="'Remove ' + chip.label + ' filter'">&#x2715;</button>
    </span>
  </template>
  <div class="input-group input-group-sm" style="width: auto;">
    <button class="btn btn-outline-secondary dropdown-toggle" type="button" data-bs-toggle="dropdown" aria-expanded="false" title="Filter field" x-text="currentFilterField().label"></button>
    <ul class="dropdown-menu">
      <template x-for="f in filterFields" :key="f.field">
        <li><a class="dropdown-item" href="#" :class="{ active: newFilterField === f.field }" @click.prevent="newFilterField = f.field; newFilterValue = ''" x-text="f.label"></a></li>
      </template>
    </ul>
    <template x-if="!currentFilterOptions().length">
      <input :type="currentFilterField().type || 'text'" class="form-control" style="min-width: 150px;" :placeholder="currentFilterPlaceholder()" aria-label="Filter value" x-model="newFilterValue" @keyup.enter="addFilter()" @keyup.escape="newFilterValue = ''">
    </template>
    <template x-if="currentFilterOptions().length">
      <select class="form-select" style="min-width: 150px;" aria-label="Filter value" x-model="newFilterValue" @keyup.escape="newFilterValue = ''">
        <option value="" x-text="currentFilterPlaceholder()"></option>
        <template x-for="o in currentFilterOptions()" :key="o">
          <option :value="o" x-text="o"></option>
        </template>
      </select>
    </template>
    <button class="btn btn-outline-secondary" type="button" @click="addFilter()" :disabled="!newFilterValue" title="Add filter" aria-label="Add filter">+</button>
  </div>`;

// Refresh button plus auto-refresh interval, stamped into each table toolbar.
// Bindings resolve against the tableTab mixin, so every tab renders the same one.
const TABLE_REFRESH_HTML = `
  <div class="btn-group btn-group-sm">
    <button class="btn btn-outline-secondary" :disabled="polling" @click="refresh()" title="Refresh now" aria-label="Refresh now"><span :class="{ spin: polling }">&#x21bb;</span></button>
    <button class="btn btn-outline-secondary dropdown-toggle" type="button" data-bs-toggle="dropdown" aria-expanded="false" title="Auto-refresh interval">
      <span x-text="refreshMode === 'paused' ? 'Off' : refreshMode"></span>
    </button>
    <ul class="dropdown-menu dropdown-menu-end">
      <template x-for="m in Object.keys(ARB_TIMING.refreshModes)" :key="m">
        <li><a class="dropdown-item" :class="{ active: refreshMode === m }" href="#" @click.prevent="setRefreshMode(m)" x-text="'Every ' + m"></a></li>
      </template>
      <li><hr class="dropdown-divider"></li>
      <li><a class="dropdown-item" :class="{ active: refreshMode === 'paused' }" href="#" @click.prevent="setRefreshMode('paused')">Off</a></li>
    </ul>
  </div>`;

// Column show/hide menu. Reads the columnPrefs mixin.
const COLUMNS_MENU_HTML = `
  <div class="dropdown">
    <button class="btn btn-outline-secondary btn-sm dropdown-toggle" type="button" data-bs-toggle="dropdown" data-bs-auto-close="outside" title="Show/hide columns">Columns</button>
    <ul class="dropdown-menu p-2" style="min-width: 12rem;">
      <template x-for="c in togglableColumns()" :key="c.key">
        <li>
          <label class="dropdown-item d-flex align-items-center gap-2 mb-0">
            <input type="checkbox" class="form-check-input mt-0" :checked="colVisible(c.key)" @change="toggleCol(c.key)">
            <span x-text="c.label"></span>
          </label>
        </li>
      </template>
      <li><hr class="dropdown-divider"></li>
      <li><button class="dropdown-item" @click="resetColumns()">Reset to defaults</button></li>
    </ul>
  </div>`;

// Placeholder rows for a table's first load, stamped as a tbody of its own so it
// sits with the rows it stands in for. Later reloads keep the rows already shown.
const TABLE_SKELETON_HTML = `
  <template x-for="i in (loading && !loaded && slowLoad ? 5 : 0)" :key="'skeleton-' + i">
    <tr class="skeleton-row">
      <td :colspan="colCount()"><span class="skeleton-bar"></span></td>
    </tr>
  </template>`;

// The one panel every view puts in place of its table when a first load did not
// land. It reads the noun the view loads and the message the server gave, so the
// views answer a failure alike.
const LOAD_ERROR_HTML = `
<div class="empty-state" role="alert" x-show="loadFailed()">
  <svg class="empty-state-icon is-error" viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.3">
    <path d="M12 4.4 21.2 19.4H2.8z" stroke-linejoin="round"/>
    <path d="M12 10.2v3.6M12 16.6h.01" stroke-linecap="round"/>
  </svg>
  <p class="empty-state-title">Could not load <span x-text="loadNoun"></span></p>
  <p class="empty-state-note" x-text="_loadErrorMsg"></p>
  <button type="button" class="btn btn-outline-secondary btn-sm empty-state-action" :disabled="loading" @click="refresh()"
    ><span :class="loading ? 'spin' : 'd-none'" aria-hidden="true">&#x21bb;</span>
    <span x-text="loading ? ' Trying…' : 'Try again'"></span></button>
</div>`;

// Chrome a view only wears when it has rows: its roll-up strip, and the queue
// landing's toolbar. Whether the view had rows is known before the load answers,
// so the slot is held from the first frame and the numbers land in it instead of
// pushing the table down. A view that was empty holds nothing.
function summaryMemory(storageKey) {
  let seen = null;
  return {
    summaryExpected: localStorage.getItem(storageKey) === '1',

    summaryClass(has) {
      if (!this.loaded) return this.summaryExpected ? 'is-pending' : 'd-none';
      if (seen !== has) {
        seen = has;
        localStorage.setItem(storageKey, has ? '1' : '0');
      }
      return has ? '' : 'd-none';
    },
  };
}

// Pager. The head carries the count, page jump and page size; the foot repeats
// just the controls. rowNoun names what is being counted.
const PAGINATION_TOP_HTML = `
  <div class="d-flex align-items-center gap-2 mb-2" :class="{ invisible: !loaded }">
    <div class="btn-group btn-group-sm" x-show="totalPages > 1">
      <button class="btn btn-outline-secondary" :disabled="currentPage === 1" @click="prevPage()">Prev</button>
      <button class="btn btn-outline-secondary" :disabled="currentPage >= totalPages" @click="nextPage()">Next</button>
    </div>
    <span class="text-muted small" x-text="total + ' ' + pluralize(total, rowNoun, rowNounPlural) + (totalPages > 1 ? ' \u00b7 page ' + currentPage + ' of ' + totalPages : '')"></span>
    <input type="number" class="form-control form-control-sm" style="width: 70px;" min="1" :max="totalPages" x-show="totalPages > 1"
      :placeholder="currentPage" @keyup.enter="goToPage($el.value); $el.value = ''" title="Jump to page" aria-label="Jump to page">
    <div class="btn-group btn-group-sm ms-auto">
      <button class="btn btn-outline-secondary dropdown-toggle" type="button" data-bs-toggle="dropdown" title="Rows per page">
        <span x-text="limit"></span> / page
      </button>
      <ul class="dropdown-menu dropdown-menu-end">
        <template x-for="n in pageSizes" :key="n">
          <li><a class="dropdown-item" :class="{ active: n === limit }" href="#" @click.prevent="setLimit(n)" x-text="n + ' per page'"></a></li>
        </template>
      </ul>
    </div>
  </div>`;

const PAGINATION_BOTTOM_HTML = `
  <div class="d-flex align-items-center gap-2">
    <div class="btn-group btn-group-sm">
      <button class="btn btn-outline-secondary" :disabled="currentPage === 1" @click="prevPage()">Prev</button>
      <button class="btn btn-outline-secondary" :disabled="currentPage >= totalPages" @click="nextPage()">Next</button>
    </div>
    <span class="text-muted small" x-text="'Page ' + currentPage + ' of ' + totalPages"></span>
  </div>`;

// x-copyable="expr" turns its host into a copy-wrap: a copy button plus a
// payload <pre> bound to expr. Copies the rendered text, so expr appears once.
document.addEventListener('alpine:init', () => {
  Alpine.directive('copyable', (el, { expression }, { evaluateLater, effect }) => {
    el.classList.add('copy-wrap');
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'copy-btn';
    btn.title = 'Copy';
    btn.setAttribute('aria-label', 'Copy to clipboard');
    const pre = document.createElement('pre');
    pre.className = 'payload-display p-2 rounded';
    el.append(btn, pre);
    btn.addEventListener('click', () => copyText(pre.textContent, btn));
    const getText = evaluateLater(expression);
    effect(() => getText((v) => { pre.textContent = v == null ? '' : String(v); }));
  });
});

// Copy text to the clipboard, flashing the triggering button on success.
async function copyText(text, btn) {
  const value = text == null ? '' : String(text);
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(value);
    } else {
      const ta = document.createElement('textarea');
      ta.value = value;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      ta.remove();
    }
    if (btn) {
      btn.classList.add('copied');
      setTimeout(() => btn.classList.remove('copied'), 1200);
    }
  } catch (e) {
    showToast('Copy failed: ' + e.message);
  }
}

// Bootstrap warns when a hiding modal still holds focus, so hand it back first.
document.addEventListener('hide.bs.modal', (e) => {
  if (e.target.contains(document.activeElement)) document.activeElement.blur();
});

function showModal(id) {
  const el = document.getElementById(id);
  if (el && window.bootstrap) bootstrap.Modal.getOrCreateInstance(el).show();
}

function hideModal(id) {
  const el = document.getElementById(id);
  if (el && window.bootstrap) bootstrap.Modal.getInstance(el)?.hide();
}

// Wide enough to sit beside the list, the drawer stays backdrop-less so the list
// remains readable and clickable. Narrower than that it covers the list anyway,
// so it goes full width and takes a backdrop, reading as modal.
const ARB_DRAWER_MODAL_MQ = '(max-width: 1200px)';

// Drops the body scroll lock Bootstrap holds while a modal or drawer is open.
// The saved padding goes with it, so a later reset cannot restore a stale value.
function clearScrollLock() {
  document.body.classList.remove('modal-open', 'offcanvas-open');
  document.body.removeAttribute('data-bs-padding-right');
  document.body.style.removeProperty('overflow');
  document.body.style.removeProperty('padding-right');
}

function showDrawer(id) {
  const el = document.getElementById(id);
  if (!el || !window.bootstrap) return;
  attachDrawerResize(el);
  const asModal = window.matchMedia(ARB_DRAWER_MODAL_MQ).matches;
  const existing = bootstrap.Offcanvas.getInstance(el);
  // Config is read once per instance, so a viewport that crossed the breakpoint
  // needs a fresh one.
  if (existing && el._arbModal !== asModal) {
    existing.dispose();
    clearScrollLock();
  }
  el._arbModal = asModal;
  // Stepping re-enters while the drawer is open, and the stepper itself lives
  // inside it, so only the opening call names where focus came from.
  if (!el.classList.contains('show')) el._arbReturnFocus = document.activeElement;
  bootstrap.Offcanvas.getOrCreateInstance(el, { backdrop: asModal, scroll: !asModal }).show();
}

function hideDrawer(id) {
  const el = document.getElementById(id);
  if (el && window.bootstrap) bootstrap.Offcanvas.getInstance(el)?.hide();
}

// Bootstrap activates a focus trap for an offcanvas only when it locks the page
// or draws a backdrop, so the wide-viewport drawer gets neither focus nor the
// Escape handler it binds on the panel. Keep it non-modal, but hand it focus on
// open, give the key a home on the document, and put focus back on close.
document.addEventListener('shown.bs.offcanvas', (e) => {
  const el = e.target;
  if (!el.classList.contains('detail-drawer')) return;
  // The panel scrolls, so it needs a tab stop of its own. Without one the arrow
  // keys land on the drawer root, which is not what scrolls.
  el.querySelectorAll('.offcanvas-body').forEach((b) => { b.tabIndex = 0; });
  if (el.contains(document.activeElement)) return;
  (el.querySelector('.drawer-close') || el).focus();
});

document.addEventListener('hidden.bs.offcanvas', (e) => {
  const el = e.target;
  const back = el._arbReturnFocus;
  el._arbReturnFocus = null;
  // Only when closing would otherwise strand focus: the reader may have clicked
  // elsewhere, and yanking them back would be worse than leaving them be.
  const stranded = document.activeElement === document.body || el.contains(document.activeElement);
  if (stranded && back && document.contains(back)) back.focus();
});

document.addEventListener('keydown', (e) => {
  if (e.key !== 'Escape') return;
  const el = openDrawerEl();
  if (el && !el.contains(document.activeElement)) hideDrawer(el.id);
});

// ---------------------------------------------------------------------------
// Detail drawers: shared behaviour for every drawer on the page
// ---------------------------------------------------------------------------

const ARB_DRAWER_MIN = 320;
const ARB_DRAWER_WIDTH_KEY = 'arb.drawerWidth';

function openDrawerEl() {
  return document.querySelector('.detail-drawer.show');
}

// True when a row click should open its detail, rather than hit a control the
// row owns.
function rowDetailClick(e) {
  const target = e.target instanceof Element ? e.target : null;
  return !target?.closest('a, button, input, select, label, .dropdown, .format-toggle-cell');
}

function setDrawerWidth(px) {
  const w = Math.round(Math.max(ARB_DRAWER_MIN, Math.min(px, window.innerWidth * 0.9)));
  document.documentElement.style.setProperty('--arb-drawer-w', w + 'px');
  return w;
}

const _storedDrawerWidth = parseInt(localStorage.getItem(ARB_DRAWER_WIDTH_KEY), 10);
if (Number.isFinite(_storedDrawerWidth)) setDrawerWidth(_storedDrawerWidth);

// A drag handle on the drawer's leading edge. Added on first open, so every
// drawer gets one without repeating it in markup.
function attachDrawerResize(el) {
  if (el._arbResize) return;
  el._arbResize = true;
  const grip = document.createElement('div');
  grip.className = 'drawer-resize';
  grip.setAttribute('role', 'separator');
  grip.setAttribute('aria-orientation', 'vertical');
  grip.setAttribute('aria-label', 'Resize panel');
  grip.tabIndex = 0;
  el.prepend(grip);

  const onMove = (ev) => setDrawerWidth(window.innerWidth - ev.clientX);
  const onUp = () => {
    document.removeEventListener('pointermove', onMove);
    document.removeEventListener('pointerup', onUp);
    document.body.classList.remove('drawer-resizing');
    const w = parseInt(getComputedStyle(el).width, 10);
    if (Number.isFinite(w)) localStorage.setItem(ARB_DRAWER_WIDTH_KEY, String(w));
  };
  grip.addEventListener('pointerdown', (ev) => {
    ev.preventDefault();
    document.body.classList.add('drawer-resizing');
    document.addEventListener('pointermove', onMove);
    document.addEventListener('pointerup', onUp);
  });
  // Keyboard resize, since the grip is focusable.
  grip.addEventListener('keydown', (ev) => {
    const step = ev.shiftKey ? 64 : 16;
    if (ev.key !== 'ArrowLeft' && ev.key !== 'ArrowRight') return;
    ev.preventDefault();
    const cur = parseInt(getComputedStyle(el).width, 10);
    const w = setDrawerWidth(cur + (ev.key === 'ArrowLeft' ? step : -step));
    localStorage.setItem(ARB_DRAWER_WIDTH_KEY, String(w));
  });
}

// A drawer carries no backdrop, so an outside click closes it here. A click on
// a row that opens a drawer falls through, swapping the detail instead.
document.addEventListener('click', (e) => {
  const el = openDrawerEl();
  if (!el || el.contains(e.target)) return;
  // A control that removes itself on click leaves a detached target behind.
  if (e.target instanceof Element && !e.target.isConnected) return;
  if (el.querySelector('.drawer-edit')) return;
  if (e.target instanceof Element && e.target.closest('.detail-row')) return;
  bootstrap.Offcanvas.getInstance(el)?.hide();
});

// Step through rows while a drawer is open. Driving the header buttons keeps
// the disabled-at-the-ends behaviour in one place. The arrow keys stay with the
// drawer's own scrolling, which a wide non-modal drawer needs.
document.addEventListener('keydown', (e) => {
  if (e.metaKey || e.ctrlKey || e.altKey) return;
  const el = openDrawerEl();
  if (!el) return;
  const target = e.target instanceof Element ? e.target : null;
  if (target?.closest('input, textarea, select, [contenteditable]')) return;
  const back = e.key === 'k';
  const fwd = e.key === 'j';
  if (!back && !fwd) return;
  e.preventDefault();
  el.querySelectorAll('.drawer-nav-btn')[back ? 0 : 1]?.click();
});

// Dismiss any open modal before a view/queue change removes its x-if block from the
// DOM. Otherwise the .modal-backdrop and body scroll-lock are orphaned (Bootstrap's
// hide callback never fires on a torn-out node), leaving the page dimmed and locked.
function dismissOpenModals() {
  if (!window.bootstrap) return;
  document.querySelectorAll('.modal.show').forEach((el) => bootstrap.Modal.getInstance(el)?.hide());
  document.querySelectorAll('.offcanvas.show').forEach((el) => bootstrap.Offcanvas.getInstance(el)?.hide());
  // The teardown can drop the modal mid-transition, so sweep any stranded backdrop
  // and body lock on the next frame as a safety net.
  requestAnimationFrame(() => {
    document.querySelectorAll('.modal-backdrop, .offcanvas-backdrop').forEach((b) => b.remove());
    clearScrollLock();
  });
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
      this._armedTimer = setTimeout(() => { this._armed = null; }, ARB_TIMING.armWindowMs);
      return false;
    },
    isArmed(key) {
      return this._armed === key;
    },
    disarm() {
      clearTimeout(this._armedTimer);
      this._armed = null;
    },
  };
}

// Type-the-name confirmation, keyed on an ARB_CONFIG mode flag. Methods, not
// getters: a spread evaluates a getter once and copies the result as a value.
function typeToConfirm(configKey) {
  return {
    confirmTarget: '',
    confirmText: '',
    confirmMode() {
      return (typeof ARB_CONFIG !== 'undefined' && ARB_CONFIG[configKey]) || 'type';
    },
    confirmValid() {
      return this.confirmText === this.confirmTarget && this.confirmTarget !== '';
    },
    openConfirm(target) {
      this.confirmTarget = target;
      this.confirmText = '';
    },
    resetConfirm() {
      this.confirmTarget = '';
      this.confirmText = '';
    },
  };
}

// Shared save lifecycle for the override edit modals. buildBody returns { body }
// to save or { error } to reject. Owns the saving flag, modal close, and reload.
async function saveOverrides(edit, { apiFn, close, buildBody, reload }) {
  const built = buildBody(edit);
  if (built.error) { edit.error = built.error; return; }
  edit.error = '';
  edit.saving = true;
  try {
    await apiFn(edit.prefix, built.body);
    close?.();
    await reload();
  } catch (err) {
    edit.error = err.message;
  } finally {
    edit.saving = false;
  }
}

// Per-row single-flight guard, spread into the table and polling mixins.
function busyRows() {
  return {
    busyRows: {},
    isBusy(key) {
      return !!this.busyRows[key];
    },
    async withBusyRow(key, fn) {
      if (this.busyRows[key]) return;
      this.busyRows = { ...this.busyRows, [key]: true };
      try {
        await fn();
      } finally {
        const next = { ...this.busyRows };
        delete next[key];
        this.busyRows = next;
      }
    },
  };
}

// Drives the Refresh button spinner. True only for polls (not the first load,
// which the top bar covers) and held to a whole number of turns, so a fast poll
// finishes a rotation instead of snapping back part-way. Call _watchPolling once
// during the component's init.
function pollSpinner() {
  return {
    polling: false,
    _pollStart: 0,
    _pollTimer: null,
    _watchPolling() {
      this.$watch('loading', (v) => {
        if (v) {
          if (!this.loaded) return;
          this.polling = true;
          this._pollStart = Date.now();
          if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
        } else if (this.polling) {
          const elapsed = Date.now() - this._pollStart;
          const turns = Math.max(1, Math.ceil(elapsed / ARB_TIMING.spinPeriodMs));
          const remaining = turns * ARB_TIMING.spinPeriodMs - elapsed;
          if (this._pollTimer) clearTimeout(this._pollTimer);
          this._pollTimer = setTimeout(() => { this.polling = false; this._pollTimer = null; }, remaining);
        }
      });
    },
    _stopWatchPolling() {
      if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
    },
  };
}

// The per-component state guardedLoad owns. Alpine writes an undeclared property
// to a shared parent scope, so each slot is declared on the component itself.
// isEmpty reads whether the view has nothing on screen, which decides whether a
// failure takes the whole view or stays a toast over the rows already there.
function loadState(isEmpty = () => true) {
  return {
    loading: false,
    loaded: false,
    // A first load only earns placeholder rows once it outlasts the same delay
    // the top-bar loader waits out, so a quick one does not flash them.
    slowLoad: false,
    _slowTimer: null,
    _loadSeq: 0,
    _loadsInFlight: 0,
    _loadErrored: false,
    _loadErrorMsg: '',
    _loaderToken: null,

    // The view has nothing to show and its last load did not land.
    loadFailed() {
      return this._loadErrored && isEmpty(this);
    },

    // The toolbar and table wait out a quick first load, so a failure opens as
    // the panel rather than a table that appears and goes. A load slow enough to
    // earn placeholders shows them, and every later load keeps the rows it has.
    viewReady() {
      return !this.loadFailed() && (this.loaded || this.slowLoad);
    },
  };
}

// Runs a load body under the loading flag, a stale-response guard, and a
// one-shot error toast. body(seq, isStale) does the fetch + apply. opts.suppressToast,
// if it returns true at error time, skips the toast (still logs).
async function guardedLoad(self, body, opts) {
  const errorLabel = 'Could not load ' + self.loadNoun;
  // The first load of a view drives the global top-bar loader instead of an
  // in-table "Loading…" flash. Later polls only spin that view's Refresh button.
  // The release is idempotent, so a concurrent load must not claim twice.
  const store = self.$store && self.$store.app;
  if (!self.loaded && !!store && !self._loaderToken) {
    self._loaderToken = store.claimLoader();
  }
  if (!self.loaded && !self._slowTimer) {
    self._slowTimer = setTimeout(() => {
      self._slowTimer = null;
      if (!self.loaded) self.slowLoad = true;
    }, ARB_TIMING.loaderDelayMs);
  }
  self.loading = true;
  self._loadsInFlight = (self._loadsInFlight || 0) + 1;
  self._loadSeq = (self._loadSeq || 0) + 1;
  const seq = self._loadSeq;
  const isStale = () => seq !== self._loadSeq;
  try {
    await body(seq, isStale);
    if (isStale()) return;
    self._loadErrored = false;
  } catch (e) {
    if (isStale()) return;
    console.error(errorLabel + ':', e);
    if (opts && opts.suppressToast && opts.suppressToast()) return;
    const first = !self._loadErrored;
    self._loadErrored = true;
    self._loadErrorMsg = e.message;
    // The panel carries a failure the view can show. A toast is for the rest: a
    // poll that failed under rows already on screen.
    if (first && !self.loadFailed()) showToast(errorLabel + ': ' + e.message);
  } finally {
    // The winner owns the flag. An abandoned load clears it once nothing is outstanding.
    self._loadsInFlight--;
    if (seq === self._loadSeq || self._loadsInFlight === 0) self.loading = false;
    // Only the winning load releases the loader, so a superseded first load
    // can't drop it while the view is still empty. Teardown and an onHide seq
    // bump release the claim themselves.
    if (seq === self._loadSeq) {
      if (self._slowTimer) { clearTimeout(self._slowTimer); self._slowTimer = null; }
      self.slowLoad = false;
      self.loaded = true;
      releaseInitialLoad(self);
    }
  }
}

// Drops this component's claim on the top-bar loader. Idempotent: teardown may
// release a load whose fetch is still in flight.
function releaseInitialLoad(self) {
  const token = self._loaderToken;
  if (!token) return;
  self._loaderToken = null;
  const store = self.$store && self.$store.app;
  if (store) store.releaseLoader(token);
}

// Client-side sorting for a list the screen already holds, for the tables whose
// rows do not come back paged from the server. keys maps a column to a reader.
// Text columns open ascending, numeric ones descending, taken from the first row.
function clientSort(rowsProp, keys, defaultKey, tieBreakKey) {
  return {
    sortBy: defaultKey || '',
    sortDir: 'asc',

    toggleSort(key) {
      if (!keys[key]) return;
      if (this.sortBy === key) {
        this.sortDir = this.sortDir === 'asc' ? 'desc' : 'asc';
        return;
      }
      this.sortBy = key;
      const first = this[rowsProp][0];
      this.sortDir = typeof (first === undefined ? '' : keys[key](first)) === 'string' ? 'asc' : 'desc';
    },

    sortIndicator(key) {
      if (this.sortBy !== key) return '\u2195';
      return this.sortDir === 'asc' ? '\u25b2' : '\u25bc';
    },

    ariaSort(key) {
      if (this.sortBy !== key) return 'none';
      return this.sortDir === 'asc' ? 'ascending' : 'descending';
    },

    // Sorts a copy, so the order the server sent is still there underneath.
    sortRows(rows) {
      const read = keys[this.sortBy];
      if (!read) return rows;
      const dir = this.sortDir === 'asc' ? 1 : -1;
      const tie = tieBreakKey && keys[tieBreakKey];
      const cmp = (x, y) => {
        if (x == null && y == null) return 0;
        if (x == null) return 1;
        if (y == null) return -1;
        return typeof x === 'string' ? x.localeCompare(y) : x - y;
      };
      return rows.slice().sort((a, b) => {
        const c = cmp(read(a), read(b));
        if (c !== 0) return c * dir;
        return tie ? cmp(tie(a), tie(b)) : 0;
      });
    },
  };
}

function storedRefreshMode(storageKey, defaultMode) {
  const saved = storageKey && localStorage.getItem(storageKey);
  return saved === 'paused' || ARB_TIMING.refreshModes[saved] ? saved : defaultMode;
}

// Manual refresh plus a configurable auto-refresh interval: the members
// TABLE_REFRESH_HTML binds. Ticks only while the tab is active and idle.
function refreshControl(loadMethod, storageKey, defaultMode = '5s') {
  return {
    refreshMode: storedRefreshMode(storageKey, defaultMode),
    _refreshTimer: null,

    refresh() {
      this[loadMethod]();
    },

    setRefreshMode(mode) {
      this.refreshMode = mode;
      if (storageKey) localStorage.setItem(storageKey, mode);
      this._startTimer();
    },

    _startTimer() {
      this._stopTimer();
      if (this.refreshMode === 'paused') return;
      const ms = ARB_TIMING.refreshModes[this.refreshMode] || ARB_TIMING.refreshModes[defaultMode];
      this._refreshTimer = setInterval(() => {
        if (this.active && !this.loading) this[loadMethod]();
      }, ms);
    },

    _stopTimer() {
      if (this._refreshTimer) {
        clearInterval(this._refreshTimer);
        this._refreshTimer = null;
      }
    },
  };
}

// Shared jobs/dlq table mixin.
function tableTab(loadMethod, refreshStorageKey) {
  return {
    ...busyRows(),
    ...pollSpinner(),
    ...confirmArm(),
    ...tabActive(),
    ...refreshControl(loadMethod, refreshStorageKey),
    _onQueueChanged: null,
    _onSseReconnect: null,
    _onSseEvent: null,
    _onUrlChanged: null,
    _hashName: '',
    pendingChanges: 0,

    // Take the filters the URL names, when the URL names this tab. Called at init
    // for a deep link, and again on a history step, which lands on a URL this tab
    // is already mounted for.
    readUrlFilters(hashName) {
      this._hashName = hashName;
      if (location.hash.replace('#', '') !== hashName) return;
      const p = new URLSearchParams(location.search);
      this.filterFields.forEach((f) => {
        const v = p.get(f.param) || '';
        this[f.model] = v;
        this[f.applied] = v;
      });
      this.sortBy = p.get('sort_by') || '';
      this.sortDir = p.get('sort_dir') || '';
      if (this.stateFilter !== undefined) this.stateFilter = p.get('status') || '';
    },

    // Write this tab's applied filters into the address bar, so the view is a link.
    // Rewrites the current entry: a filter is a narrowing of where the reader already
    // is, not a place of its own.
    _syncFiltersToUrl() {
      // A load that resolves after the reader moved on belongs to a tab the URL no
      // longer names, and its filters are not the ones the address bar describes.
      if (location.hash.replace('#', '') !== this._hashName) return;
      const url = new URL(location.href);
      for (const k of _filterKeys) url.searchParams.delete(k);
      this.filterFields.forEach((f) => {
        if (this[f.applied]) url.searchParams.set(f.param, this[f.applied]);
      });
      if (this.stateFilter) url.searchParams.set('status', this.stateFilter);
      if (this.sortBy) url.searchParams.set('sort_by', this.sortBy);
      if (this.sortDir) url.searchParams.set('sort_dir', this.sortDir);
      history.replaceState(null, '', url);
    },

    // The selected queue's declared label set, for a Kind filter's options.
    kindOptions: [],
    async loadKinds() {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      try {
        this.kindOptions = (await ArbiterAPI.listKinds(queue)) || [];
      } catch {
        this.kindOptions = [];
      }
    },

    // Filter builder: one chip per applied filter, plus a "field + value" adder.
    // `param` names the field's query-string and API key, so a tab adds a filter by
    // declaring it here and reading it in its own loader.
    filterFields: [
      { field: 'group', label: 'Group', param: 'group_key', model: 'groupKeyFilter', applied: '_appliedGroupKey' },
      { field: 'parent', label: 'Parent ID', param: 'parent_id', model: 'parentIdFilter', applied: '_appliedParentId', numeric: true },
      // Job ID locates a single row, so it does not combine with the others.
      { field: 'job', label: 'Job ID', param: 'job_id', model: 'jobIdFilter', applied: '_appliedJobId', numeric: true, exclusive: true },
    ],
    newFilterField: 'group',
    newFilterValue: '',
    _lastInvalid: {},

    // The applied value of one filter, letting a caller's overrides stand in for it.
    // Loaders read their parameters through this, so a field is wired once.
    filterValue(field, overrides) {
      const f = this.filterFields.find((x) => x.field === field);
      if (!f) return '';
      return (overrides?.[field] ?? this[f.applied]) || '';
    },

    // Apply one filter and clear the rest. Backs the links that jump straight to a
    // narrowed list (a parent's children, a worker's jobs, a policy's throttled jobs).
    setOnlyFilter(field, value) {
      const overrides = {};
      this.filterFields.forEach((f) => {
        const v = f.field === field ? String(value) : '';
        this[f.model] = v;
        overrides[f.field] = v;
      });
      if (this.stateFilter !== undefined) this.stateFilter = '';
      this._resetView(overrides);
    },

    currentFilterField() {
      return this.filterFields.find((f) => f.field === this.newFilterField) || this.filterFields[0];
    },
    currentFilterPlaceholder() {
      return this.currentFilterField().label + '…';
    },
    // `options` names the component property holding a field's value list.
    currentFilterOptions() {
      const f = this.currentFilterField();
      return (f.options && this[f.options]) || [];
    },
    activeFilterChips() {
      return this.filterFields
        .filter((f) => (this[f.applied] || '') !== '')
        .map((f) => ({
          field: f.field,
          label: f.label,
          value: f.format ? f.format(this[f.applied]) : this[f.applied],
        }));
    },

    addFilter() {
      const f = this.currentFilterField();
      const v = (this.newFilterValue || '').trim();
      if (!v) return;
      if (f.numeric && !/^\d+$/.test(v)) {
        showToast(f.label + ' must be a positive integer', 'warning');
        return;
      }
      // An exclusive field (Job ID) clears every other filter. Any other field
      // clears the exclusive ones but coexists with its non-exclusive siblings.
      this.filterFields.forEach((x) => {
        if (x.field === f.field) return;
        if (f.exclusive || x.exclusive) this[x.model] = '';
      });
      this[f.model] = v;
      this.newFilterValue = '';
      this.applyFilter();
    },

    removeFilter(field) {
      const f = this.filterFields.find((x) => x.field === field);
      if (!f) return;
      this[f.model] = '';
      this.applyFilter();
    },

    _cycleSort(col) {
      if (this.sortBy !== col) {
        this.sortBy = col;
        this.sortDir = 'desc';
      } else if (this.sortDir === 'desc') {
        this.sortDir = 'asc';
      } else {
        this.sortBy = '';
        this.sortDir = '';
      }
    },

    sortIndicator(col) {
      if (this.sortBy !== col) return '↕';
      return this.sortDir === 'asc' ? '▲' : '▼';
    },

    // Default sort/reset/url-sync for a flat tab. Tabs with expansion state
    // (jobs) override these.
    toggleSort(col) {
      this._cycleSort(col);
      this._resetView();
    },

    _resetView(filterOverrides) {
      this.offset = 0;
      this[loadMethod](filterOverrides);
      this._startTimer();
    },

    applyFilter() {
      const next = {};
      for (const f of this.filterFields) {
        const v = (this[f.model] || '').trim();
        // Auto-apply fires from both Enter and change/blur. Only warn once per value.
        if (f.numeric && v && !/^\d+$/.test(v)) {
          if (this._lastInvalid[f.field] !== v) {
            showToast(f.label + ' must be a positive integer', 'warning');
            this._lastInvalid[f.field] = v;
          }
          return;
        }
        this._lastInvalid[f.field] = null;
        this[f.model] = v;
        next[f.field] = v;
      }
      if (this.filterFields.every((f) => next[f.field] === (this[f.applied] || ''))) return;
      this._resetView(next);
    },

    filterByParent(id) {
      this.setOnlyFilter('parent', id);
    },

    _bindTableEvents(opts) {
      this._watchPolling();
      // Clears what the old queue narrowed, then adopts whatever the new URL names.
      // Reading here rather than in a second event keeps a queue change to one load:
      // a history step lands on a URL that carries both a new queue and its filters.
      this._onQueueChanged = () => {
        this.disarm();
        this.filterFields.forEach((f) => { this[f.model] = ''; this[f.applied] = ''; });
        this._lastInvalid = {};
        this.newFilterValue = '';
        this.sortBy = '';
        this.sortDir = '';
        // Reset paging even while inactive, so the tab reopens on page 1 of the new
        // queue rather than a leftover offset (offset lives only in _resetView, which
        // is active-gated).
        this.offset = 0;
        if (opts.onQueueReset) opts.onQueueReset();
        this.readUrlFilters(opts.hashName);
        if (this.active) this._resetView();
      };
      this._onSseReconnect = () => {
        if (this.active) this[loadMethod]();
      };
      this._onSseEvent = (e) => {
        const count = opts.relevant(e.detail);
        if (count > 0) this.pendingChanges += count;
      };
      // A history step lands on a URL whose filters this tab has to adopt. It runs
      // after queueChanged, so a cross-queue step resets first and then reads.
      this._onUrlChanged = () => {
        this.readUrlFilters(opts.hashName);
        this.offset = 0;
        if (this.active) this._resetView();
      };
      window.addEventListener(ARB_EVENTS.queueChanged, this._onQueueChanged);
      window.addEventListener(ARB_EVENTS.sseReconnect, this._onSseReconnect);
      window.addEventListener(ARB_EVENTS.sseEvent, this._onSseEvent);
      window.addEventListener(ARB_EVENTS.urlChanged, this._onUrlChanged);
    },

    _unbindTableEvents() {
      window.removeEventListener(ARB_EVENTS.queueChanged, this._onQueueChanged);
      window.removeEventListener(ARB_EVENTS.sseReconnect, this._onSseReconnect);
      window.removeEventListener(ARB_EVENTS.sseEvent, this._onSseEvent);
      window.removeEventListener(ARB_EVENTS.urlChanged, this._onUrlChanged);
      this._stopWatchPolling();
      releaseInitialLoad(this);
    },
  };
}

// Shared cron/workers polling mixin (visibility-aware).
function pollingTab(loadMethod, intervalMs, refreshStorageKey) {
  // The screen's own cadence names the starting choice, so the control opens on
  // what it was already doing.
  const defaultMode = Object.keys(ARB_TIMING.refreshModes).find((k) => ARB_TIMING.refreshModes[k] === intervalMs) || '30s';
  return {
    ...busyRows(),
    ...pollSpinner(),
    ...tabActive(),
    refreshInterval: null,
    refreshMode: storedRefreshMode(refreshStorageKey, defaultMode),
    _visibilityHandler: null,

    refresh() {
      this[loadMethod]();
    },

    setRefreshMode(mode) {
      this.refreshMode = mode;
      if (refreshStorageKey) localStorage.setItem(refreshStorageKey, mode);
      if (this.active) this.startPolling();
      else this.stopPolling();
    },

    startPolling() {
      this.stopPolling();
      const ms = ARB_TIMING.refreshModes[this.refreshMode];
      if (!ms) return;
      this.refreshInterval = setInterval(() => { if (!this.loading) this[loadMethod](); }, ms);
    },

    stopPolling() {
      if (this.refreshInterval) {
        clearInterval(this.refreshInterval);
        this.refreshInterval = null;
      }
    },

    _bindVisibility() {
      this._visibilityHandler = () => {
        if (document.hidden) {
          this.stopPolling();
        } else if (this.active) {
          this[loadMethod]();
          this.startPolling();
        }
      };
      document.addEventListener('visibilitychange', this._visibilityHandler);
    },

    _unbindVisibility() {
      if (this._visibilityHandler) {
        document.removeEventListener('visibilitychange', this._visibilityHandler);
        this._visibilityHandler = null;
      }
    },

    // opts.onQueueChange() runs on a queue switch (before reload). opts.onHide()
    // runs when the tab is hidden (before polling stops).
    initPolling(tabTarget, opts = {}) {
      trackTabActive(this, tabTarget, {
        onShow: () => { this[loadMethod](); this.startPolling(); },
        onHide: () => { if (opts.onHide) opts.onHide(); this.stopPolling(); },
      });
      this.$watch('$store.app.selectedQueue', () => {
        if (opts.onQueueChange) opts.onQueueChange();
        if (this.active) this[loadMethod]();
      });
      this._watchPolling();
      this._bindVisibility();
    },

    // For views shown/hidden by mount (x-if) rather than a Bootstrap tab: active
    // for the component's whole lifetime, so load and poll start immediately.
    initPollingMounted() {
      this.active = true;
      this._watchPolling();
      this[loadMethod]();
      this.startPolling();
      this._bindVisibility();
    },

    teardownPolling() {
      untrackTabActive(this);
      this.stopPolling();
      this._unbindVisibility();
      this._stopWatchPolling();
      releaseInitialLoad(this);
    },
  };
}

// Window event-bus subscription mixin. _bindBus maps ARB_EVENTS keys to handlers
// and _unbindBus removes them all.
function eventBusTab() {
  return {
    _busHandlers: null,
    _bindBus(handlerMap) {
      this._busHandlers = Object.entries(handlerMap).map(([name, fn]) => {
        const event = ARB_EVENTS[name];
        window.addEventListener(event, fn);
        return [event, fn];
      });
    },
    _unbindBus() {
      if (!this._busHandlers) return;
      for (const [event, fn] of this._busHandlers) window.removeEventListener(event, fn);
      this._busHandlers = null;
    },
  };
}

// Label for a job's rate-limit or concurrency gate key.
function gateLabel(g, empty = EMPTY) {
  return g ? g.prefix + ':' + g.suffix : empty;
}

// Badge classes for a job status, shared by the tables and the detail drawers.
function statusBadgeClass(status) {
  return {
    suspended: 'bg-warning-subtle text-warning-emphasis',
    cancelled: 'bg-dark-subtle text-dark-emphasis',
    in_flight: 'bg-primary-subtle text-primary-emphasis',
    backoff: 'bg-danger-subtle text-danger-emphasis',
    scheduled: 'bg-secondary-subtle text-secondary-emphasis',
    throttled: 'bg-info-subtle text-info-emphasis',
    ready: 'bg-success-subtle text-success-emphasis',
  }[status] || 'bg-secondary-subtle text-secondary-emphasis';
}

// Prev/next stepping for a drawer over the rows on the page. opts.openWith maps
// a row to whatever the tab's viewDetail takes (a row, or an id to fetch).
// opts.drawer names the drawer element, for the tabs that close it themselves.
//
// A tab whose detail can fail calls captureDetailNeighbours() before it drops
// the selection, pinning them as rows rather than as positions in a live list.
function rowDetail(rowsProp, idField, selectedProp, opts = {}) {
  const argFor = opts.openWith || ((row) => row);
  return {
    detailNeighbours: null,

    closeDetail() {
      hideDrawer(opts.drawer);
    },

    // An action taken from the drawer leaves its content stale.
    // The panel on a job a destructive action just acted on has nothing to add.
    closeDetailIfOpen(id) {
      const cur = this[selectedProp];
      if (cur && String(cur[idField]) === String(id)) this.closeDetail();
    },

    _detailIndex() {
      const cur = this[selectedProp];
      if (!cur) return -1;
      return this[rowsProp].findIndex((r) => String(r[idField]) === String(cur[idField]));
    },

    // Pins the neighbours of aroundId, falling back to the selection, then to
    // whatever is already pinned.
    captureDetailNeighbours(aroundId) {
      const rows = this[rowsProp];
      let i = aroundId == null ? -1
        : rows.findIndex((r) => String(r[idField]) === String(aroundId));
      if (i < 0) i = this._detailIndex();
      if (i < 0) return;
      this.detailNeighbours = {
        prev: i > 0 ? rows[i - 1] : null,
        next: rows[i + 1] || null,
      };
    },

    clearDetailNeighbours() {
      this.detailNeighbours = null;
    },

    // The selection holds a row from a previous load. Re-point it at the fresh
    // row so the drawer and the list read the same values.
    resyncDetailSelection() {
      const cur = this[selectedProp];
      if (!cur) return;
      const fresh = this[rowsProp].find((r) => String(r[idField]) === String(cur[idField]));
      // Another page or a filter can drop a row that still exists, so a missing
      // selection is re-read by id rather than taken as gone.
      if (fresh) this[selectedProp] = fresh;
      else this.refreshOpenDetail?.();
    },

    _detailNeighbour(delta) {
      if (this.detailNeighbours) {
        return (delta < 0 ? this.detailNeighbours.prev : this.detailNeighbours.next) || null;
      }
      const i = this._detailIndex();
      return i < 0 ? null : this[rowsProp][i + delta] || null;
    },

    // A method, not a getter: a spread would evaluate a getter once, in the
    // mixin's own scope, and copy the result as a value. Pinned neighbours mean
    // the row is gone, so there is no position to report.
    detailPosition() {
      if (this.detailNeighbours) return '';
      const i = this._detailIndex();
      return i < 0 ? '' : (i + 1) + ' of ' + this[rowsProp].length;
    },

    detailStatusClass() {
      return statusBadgeClass(this.detailStatus);
    },

    hasDetailStep(delta) {
      return !!this._detailNeighbour(delta);
    },

    stepDetail(delta) {
      const next = this._detailNeighbour(delta);
      if (next) this.viewDetail(argFor(next));
    },

    rowClick(e, item) {
      if (rowDetailClick(e)) this.viewDetail(item);
    },
  };
}

// Shared drawer header. Bindings resolve against the hosting tab, which supplies
// detailTitle, detailStatus, detailPosition and the stepping methods.
const DETAIL_HEAD_HTML = `
  <div class="drawer-head">
    <div class="drawer-head-text">
      <span class="drawer-title" x-text="detailTitle"></span>
      <span class="drawer-sub">
        <template x-if="detailStatus">
          <span class="badge" :class="detailStatusClass()" x-text="detailStatus"></span>
        </template>
        <span x-show="detailPosition()" x-text="detailPosition()"></span>
      </span>
    </div>
    <div class="drawer-actions">
      <template x-for="job in detailRows" :key="job._id">
        <div class="dropdown">
          <button class="drawer-actions-btn dropdown-toggle" type="button" data-bs-toggle="dropdown"
            data-bs-auto-close="outside" aria-expanded="false" :disabled="isBusy(job._id)">Actions</button>
          <ul class="dropdown-menu dropdown-menu-end" x-html="detailActionsHtml"></ul>
        </div>
      </template>
      <div class="drawer-nav">
        <button type="button" class="drawer-nav-btn" :disabled="!hasDetailStep(-1)" @click="stepDetail(-1)"
          title="Previous (k)" aria-label="Previous">&#8593;</button>
        <button type="button" class="drawer-nav-btn" :disabled="!hasDetailStep(1)" @click="stepDetail(1)"
          title="Next (j)" aria-label="Next">&#8595;</button>
      </div>
      <button type="button" class="drawer-close" data-bs-dismiss="offcanvas" aria-label="Close">&#10005;</button>
    </div>
  </div>`;

// Tooltip for a gate badge, which itself shows only the prefix.
function gateTitle(gate, kind) {
  return kind + ' · ' + gateLabel(gate, '');
}

// ---------------------------------------------------------------------------
// Fill-bar helpers
// ---------------------------------------------------------------------------

// Clamp a fraction to an integer 0..100 percent.
function clampPct(frac) {
  return Math.max(0, Math.min(100, Math.round(frac * 100)));
}

// Fill percent for a possibly-null fraction (an absent fill renders as empty).
function fillPct(frac) {
  return frac == null ? 0 : clampPct(frac);
}

// Colour band where low fill is bad (e.g. remaining rate-limit tokens).
function lowFillClass(pct) {
  return pct < 25 ? 'bg-danger' : pct < 50 ? 'bg-warning' : 'bg-success';
}

// Colour band where high fill is bad (e.g. concurrency utilization).
function highFillClass(pct) {
  return pct >= 100 ? 'bg-danger' : pct >= 75 ? 'bg-warning' : 'bg-success';
}

// Parse an optional whole-number form field. Blank is null, invalid is { error: true }.
function parseOptionalInt(v, min) {
  if (!v) return { value: null };
  const n = Number(v);
  if (!Number.isInteger(n) || (min != null && n < min)) return { error: true };
  return { value: n };
}

// Parse an override input field. A blank field is null (revert to default), not 0.
// check is Number.isInteger for whole-number limits, Number.isFinite otherwise.
function parseOverride(v, check) {
  if (v === '' || v == null) return null;
  const n = Number(v);
  return check(n) ? n : null;
}

// Shared drawer lifecycle for the rate-limit and concurrency tabs. Owns the open
// policy and its capped child list, refreshes an open drawer on each poll without
// flashing the spinner, and closes a drawer whose policy vanished. cfg supplies the
// field/method names, fetchers, and labels that differ per tab.
function drillDownTab(cfg) {
  return {
    ...loadState((s) => s.policies.length === 0),
    policies: [],
    selectedPolicy: null,
    [cfg.listField]: [],
    [cfg.loadingField]: false,
    editing: false,
    _itemSeq: 0,
    _onDrillHidden: null,

    // Total items for the open policy, from its own row (the drawer list is capped
    // at cfg.itemLimit).
    itemTotal() {
      const p = this.selectedPolicy;
      return p ? p[cfg.countField] : this[cfg.listField].length;
    },

    // Truncation note reads the stable policy count and fixed cap, never the live
    // list, which is empty mid-load and would otherwise flash the note on open.
    itemCap: cfg.itemLimit,
    hasMoreItems() {
      return this.itemTotal() > cfg.itemLimit;
    },

    // Placeholder rows to stand in for the ones on the way, so the list opens at
    // the height it keeps. The policy row already counted them.
    expectedItems() {
      return Math.max(1, Math.min(this.itemTotal(), cfg.itemLimit));
    },

    // The drawer also closes on its own: the close button, Escape, a navigation.
    // Dropping the selection here covers every one of those.
    bindDrillDrawer() {
      this._onDrillHidden = (e) => {
        if (e.target.id === cfg.drawerId) this._clearDrill();
      };
      document.addEventListener('hidden.bs.offcanvas', this._onDrillHidden);
    },

    unbindDrillDrawer() {
      document.removeEventListener('hidden.bs.offcanvas', this._onDrillHidden);
      this._onDrillHidden = null;
    },

    _clearDrill() {
      this.selectedPolicy = null;
      this[cfg.listField] = [];
      this[cfg.loadingField] = false;
      this.editing = false;
    },

    // The drawer is the editor's home, so open it on this policy first.
    openEdit(p) {
      this.buildEdit(p);
      this.viewDetail(p);
      this.editing = true;
    },

    cancelEdit() {
      this.editing = false;
      this.edit.error = '';
    },

    async loadPolicies() {
      await guardedLoad(this, async (seq, isStale) => {
        const data = await cfg.fetchPolicies();
        if (isStale()) return;
        this.policies = data.policies || [];
        // A policy that vanished takes its drawer with it. The rest re-point the
        // open one at the fresh row, so the drawer reads the values the table does.
        if (this.selectedPolicy && !this.policies.some((p) => p.prefix === this.selectedPolicy.prefix)) {
          this.closeDetail();
          this._clearDrill();
        } else {
          this.resyncDetailSelection();
        }
      });
      // Keep an open drawer fresh on each poll, without flashing the spinner.
      if (this.selectedPolicy) await this[cfg.loadName](this.selectedPolicy.prefix, { silent: true });
      await this._openUrlPrefix();
    },

    _urlPrefixDone: false,

    // A ?prefix= deep link expands that policy once, after the list arrives.
    async _openUrlPrefix() {
      if (this._urlPrefixDone) return;
      const want = new URLSearchParams(location.search).get('prefix');
      if (!want) {
        this._urlPrefixDone = true;
        return;
      }
      const p = this.policies.find((x) => x.prefix === want);
      if (!p) return;
      this._urlPrefixDone = true;
      if (this.selectedPolicy?.prefix !== p.prefix) await this.viewDetail(p);
    },

    // A row already in the drawer stays there, so a second click never closes what
    // it just opened. The keyboard toggle is what closes it.
    async viewDetail(p) {
      const same = this.selectedPolicy?.prefix === p.prefix;
      this.selectedPolicy = p;
      this.editing = false;
      showDrawer(cfg.drawerId);
      if (same) return;
      // Drop the previous policy's rows so they never render under the new heading.
      this[cfg.listField] = [];
      await this[cfg.loadName](p.prefix);
    },

    async [cfg.toggleName](p) {
      if (this.selectedPolicy?.prefix === p.prefix) this.closeDetail();
      else await this.viewDetail(p);
    },

    async [cfg.loadName](prefix, { silent = false } = {}) {
      const seq = ++this._itemSeq;
      if (!silent) this[cfg.loadingField] = true;
      let data, err;
      try {
        data = await cfg.fetchItems(prefix, { limit: cfg.itemLimit });
      } catch (e) {
        err = e;
      }
      // A superseded fetch (or a closed/vanished drill-down) owns nothing anymore:
      // it must touch neither the list nor the spinner.
      if (seq !== this._itemSeq || this.selectedPolicy?.prefix !== prefix) return;
      // The latest fetch settles the spinner, even a silent one that superseded a
      // user-initiated load.
      this[cfg.loadingField] = false;
      if (err) {
        // On a background poll, keep the stale list rather than blanking it.
        if (silent) return;
        showToast(`Could not load ${cfg.itemLabel}: ${err.message}`);
        this[cfg.listField] = [];
      } else {
        this[cfg.listField] = data[cfg.listField] || [];
      }
    },
  };
}

// ---------------------------------------------------------------------------
// Toast notifications
// ---------------------------------------------------------------------------

function showToast(message, type = 'danger') {
  const container = document.getElementById('toastContainer');
  if (!container) return;
  const key = type + '\u0000' + message;

  const existing = Array.from(container.children).find((c) => c.dataset.toastKey === key);
  if (existing && bootstrap.Toast.getInstance(existing)) {
    const n = (parseInt(existing.dataset.toastCount, 10) || 1) + 1;
    existing.dataset.toastCount = String(n);
    const badge = existing.querySelector('.toast-count');
    if (badge) { badge.textContent = '×' + n; badge.classList.remove('d-none'); }
    return;
  }

  while (container.children.length >= ARB_TIMING.toastMaxVisible && container.firstElementChild) {
    const oldest = container.firstElementChild;
    bootstrap.Toast.getInstance(oldest)?.dispose();
    oldest.remove();
  }

  const bg = {
    danger: 'bg-danger-subtle text-danger-emphasis',
    success: 'bg-success-subtle text-success-emphasis',
    warning: 'bg-warning-subtle text-warning-emphasis',
    info: 'bg-info-subtle text-info-emphasis',
  }[type] || 'bg-danger-subtle text-danger-emphasis';
  const el = document.createElement('div');
  el.dataset.toastKey = key;
  el.dataset.toastCount = '1';
  el.className = `toast ${bg}`;
  el.setAttribute('role', type === 'danger' ? 'alert' : 'status');
  el.innerHTML = `<div class="d-flex">
    <div class="toast-body"></div>
    <span class="toast-count badge bg-secondary-subtle text-secondary-emphasis align-self-center me-2 d-none"></span>
    <button type="button" class="btn-close me-2 m-auto" data-bs-dismiss="toast" aria-label="Dismiss"></button>
  </div>`;
  el.querySelector('.toast-body').textContent = message;
  container.appendChild(el);
  const toast = new bootstrap.Toast(el, { delay: ARB_TIMING.toastDelays[type] ?? ARB_TIMING.toastDelays.danger });
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
function withPagination(component, loadMethod, sizeStorageKey) {
  const stored = sizeStorageKey ? parseInt(localStorage.getItem(sizeStorageKey), 10) : NaN;
  const pagination = {
    limit: ARB_TIMING.pageSizes.includes(stored) ? stored : ARB_TIMING.pageLimit,
    pageSizes: ARB_TIMING.pageSizes,
    offset: 0,
    loaded: false,

    // Keeps the current page's first row in view rather than the page number.
    setLimit(size) {
      const n = parseInt(size, 10);
      if (!ARB_TIMING.pageSizes.includes(n) || n === this.limit) return;
      const firstRow = this.offset;
      this.limit = n;
      this.offset = Math.floor(firstRow / n) * n;
      if (sizeStorageKey) localStorage.setItem(sizeStorageKey, String(n));
      this[loadMethod]();
    },

    get currentPage() {
      return Math.floor(this.offset / this.limit) + 1;
    },

    get totalPages() {
      return Math.max(1, Math.ceil(this.total / this.limit));
    },

    goToPage(page) {
      const n = parseInt(page, 10);
      if (!Number.isFinite(n)) return;
      const p = Math.max(1, Math.min(this.totalPages, n));
      const offset = (p - 1) * this.limit;
      if (offset === this.offset) return;
      this.offset = offset;
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

// Row-selection state for bulk actions: a `selected` map keyed by row id plus the
// select/toggle/all helpers. rowsProp names the component's row array, idField the
// per-row primary-key field. Wraps like withPagination to preserve getters.
function withSelection(component, rowsProp, idField) {
  const selection = {
    selected: {},

    isSelected(id) {
      return !!this.selected[id];
    },

    toggleSelect(id) {
      const next = { ...this.selected };
      if (next[id]) delete next[id]; else next[id] = true;
      this.selected = next;
    },

    get selectedIds() {
      return Object.keys(this.selected).filter(k => this.selected[k]).map(Number);
    },

    get selectedCount() {
      return this.selectedIds.length;
    },

    get allSelected() {
      const rows = this[rowsProp];
      return rows.length > 0 && rows.every(j => this.selected[j[idField]]);
    },

    toggleSelectAll() {
      if (this.allSelected) {
        this.selected = {};
      } else {
        const next = {};
        for (const j of this[rowsProp]) next[j[idField]] = true;
        this.selected = next;
      }
    },
  };

  const result = {};
  Object.defineProperties(result, Object.getOwnPropertyDescriptors(component));
  Object.defineProperties(result, Object.getOwnPropertyDescriptors(selection));
  return result;
}

// ---------------------------------------------------------------------------
// Column show/hide preferences
// ---------------------------------------------------------------------------

// Persisted column visibility shared by table tabs. Pass an ordered registry of
// { key, label, weight, required?, autoHide? } and a localStorage key. colVis
// holds the user's explicit choices; anything unset follows autoEmpty.
function columnPrefs(columns, storageKey) {
  return {
    columns,
    colVis: {},
    autoEmpty: {},
    _colSeen: {},

    _loadColPrefs() {
      try {
        const saved = JSON.parse(localStorage.getItem(storageKey)) || {};
        const kept = {};
        columns.forEach((c) => { if (c.key in saved) kept[c.key] = saved[c.key] !== false; });
        this.colVis = kept;
      } catch {
        this.colVis = {};
      }
    },

    // A { key: bool } map over the autoHide columns, recomputed on each load.
    // A column that has carried data stays put for the rest of the queue: page
    // content churns, and re-deciding every poll resizes the whole table.
    // An omitted key is no evidence either way and leaves that column as it is.
    setAutoEmpty(empty) {
      const next = { ...this.autoEmpty };
      const seen = { ...this._colSeen };
      columns.forEach((c) => {
        if (!c.autoHide || empty[c.key] === undefined) return;
        if (!empty[c.key]) seen[c.key] = true;
        next[c.key] = empty[c.key] && !seen[c.key];
      });
      this._colSeen = seen;
      this.autoEmpty = next;
    },

    // Called when the queue changes: a different queue uses different fields, so
    // the latch restarts. The map itself stays until the new rows are measured:
    // clearing it here reopens every auto-hidden column for the length of the
    // fetch, and the table visibly resizes twice. setAutoEmpty rewrites every
    // key at once, so the stale map never outlives the first load.
    resetAutoEmpty() {
      this._colSeen = {};
    },

    // The columns actually rendered, for a cell that has to span the table. A
    // method, not a getter: a spread of this mixin would freeze a getter's first
    // reading and copy it as a value.
    colCount() {
      return columns.filter((c) => this.colVisible(c.key)).length;
    },

    colVisible(key) {
      const col = columns.find((c) => c.key === key);
      // A column marked narrow: false is dropped on a phone whatever the saved
      // preference says. The preference is left untouched, so the desktop view is
      // unchanged, and the drawer still carries every field a tap away.
      if (col?.narrow === false && this.$store.app.narrow) return false;
      if (col?.required) return true;
      if (key in this.colVis) return this.colVis[key];
      return !this.autoEmpty[key];
    },

    // The columns the menu can offer here: one dropped by width is not togglable.
    togglableColumns() {
      return columns.filter((c) => !c.required && !(c.narrow === false && this.$store.app.narrow));
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
      localStorage.removeItem(storageKey);
    },
  };
}

// ---------------------------------------------------------------------------
// URL filter sync
// ---------------------------------------------------------------------------

// Every filter key any tab writes, cleared as a set when the view changes. A tab's
// own filterFields name which of them it owns.
const _filterKeys = [
  'group_key',
  'parent_id',
  'job_id',
  'claimed_by',
  'kind',
  'payload',
  'rate_limit_prefix',
  'concurrency_prefix',
  'completed_after',
  'completed_before',
  'status',
  'sort_by',
  'sort_dir',
];

// Relative URL to a queue's Jobs tab, narrowed by any of the filter keys. One source
// of truth for the deep-link shape used by the queue cards, the stat cards, the worker
// rows, the policy tables, and the store's in-app navigation.
function queueJobsUrl(queue, filters) {
  const p = new URLSearchParams({ queue });
  // A bare status keeps the older one-argument form the stat cards call.
  const named = typeof filters === 'string' ? { status: filters } : filters || {};
  for (const [k, v] of Object.entries(named)) {
    if (v) p.set(k, String(v));
  }
  return '?' + p.toString() + '#jobs';
}

// Relative URL to one job in a queue's Jobs tab, for the event log's job column.
function queueJobUrl(queue, jobId) {
  return queueJobsUrl(queue, { job_id: jobId });
}

// Anchor click guard: true if this is a plain left-click to handle as an SPA nav
// (and preventDefault). False for modifier/middle clicks, which fall through to the
// href so the browser opens it in a new tab.
function plainNavClick(e) {
  if (e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return false;
  e.preventDefault();
  return true;
}

// ---------------------------------------------------------------------------
// Tab-active tracking
// ---------------------------------------------------------------------------

// Listener slots for trackTabActive.
function tabActive() {
  return {
    _tabShownHandler: null,
    _tabHiddenHandler: null,
  };
}

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

  component._tabShownHandler = (e) => {
    if (e.target.getAttribute('data-bs-target') === tabTarget) {
      component.active = true;
      if (callbacks && callbacks.onShow) callbacks.onShow();
    }
  };
  component._tabHiddenHandler = (e) => {
    if (e.target.getAttribute('data-bs-target') === tabTarget) {
      component.active = false;
      if (callbacks && callbacks.onHide) callbacks.onHide();
    }
  };
  document.addEventListener('shown.bs.tab', component._tabShownHandler);
  document.addEventListener('hidden.bs.tab', component._tabHiddenHandler);
}

function untrackTabActive(component) {
  if (component._tabShownHandler) {
    document.removeEventListener('shown.bs.tab', component._tabShownHandler);
    component._tabShownHandler = null;
  }
  if (component._tabHiddenHandler) {
    document.removeEventListener('hidden.bs.tab', component._tabHiddenHandler);
    component._tabHiddenHandler = null;
  }
}

// Activate the sub-tab named in the URL hash (or the first tab if the hash is
// absent/unknown), so the correct pane is shown from the first paint rather than
// flashing the default tab first. Each area's tabs mount lazily via x-if, so this
// runs on the block's init. No tab carries a hardcoded active class.
function activateSubTabFromHash(valid) {
  const h = location.hash.replace('#', '');
  const target = h && valid.includes(h) ? h : valid[0];
  const btn = document.querySelector('[data-bs-target="#tab-' + target + '"]');
  if (btn) bootstrap.Tab.getOrCreateInstance(btn).show();
}

// Edge fades on the app's horizontal scrollers. The classes say which side has
// content left to reveal, so a clipped nav or table shows where it continues.
const SCROLL_EDGE_SELECTOR = '.nav-primary, .nav-tabs, .table-responsive';

function markScrollEdges(el) {
  // Above the table breakpoint these containers overflow visibly rather than
  // scrolling. Fading an edge there would hide content nothing can bring back.
  const overflow = getComputedStyle(el).overflowX;
  const slack = overflow === 'auto' || overflow === 'scroll' ? el.scrollWidth - el.clientWidth : 0;
  el.classList.toggle('scroll-edge-start', slack > 1 && el.scrollLeft > 1);
  el.classList.toggle('scroll-edge-end', slack > 1 && el.scrollLeft < slack - 1);
}

// The scroller and its content are both watched: the box can stay put while the
// table inside it grows a column.
const _scrollEdgeSizes = new ResizeObserver((entries) => entries.forEach((e) => {
  markScrollEdges(e.target.matches(SCROLL_EDGE_SELECTOR) ? e.target : e.target.parentElement);
}));

// Scrollers mount and unmount with their panes, so rescan whenever the DOM settles.
let _scrollEdgeScanQueued = false;
function refreshScrollEdges() {
  if (_scrollEdgeScanQueued) return;
  _scrollEdgeScanQueued = true;
  requestAnimationFrame(() => {
    _scrollEdgeScanQueued = false;
    document.querySelectorAll(SCROLL_EDGE_SELECTOR).forEach((el) => {
      _scrollEdgeSizes.observe(el);
      if (el.firstElementChild) _scrollEdgeSizes.observe(el.firstElementChild);
      markScrollEdges(el);
    });
  });
}

// Scroll events do not bubble, so listen in the capture phase.
document.addEventListener('scroll', (e) => {
  if (e.target instanceof Element && e.target.matches(SCROLL_EDGE_SELECTOR)) markScrollEdges(e.target);
}, true);
new MutationObserver(refreshScrollEdges).observe(document.documentElement, { childList: true, subtree: true });
document.addEventListener('DOMContentLoaded', refreshScrollEdges);

// Keep the newly shown sub-tab visible when the strip is scrolled off-screen.
document.addEventListener('shown.bs.tab', (e) => e.target.scrollIntoView({ block: 'nearest', inline: 'nearest' }));
