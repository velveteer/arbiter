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
        <li><a class="dropdown-item" href="#" :class="{ active: newFilterField === f.field }" @click.prevent="newFilterField = f.field" x-text="f.label"></a></li>
      </template>
    </ul>
    <input type="text" class="form-control" style="min-width: 150px;" :placeholder="currentFilterPlaceholder()" aria-label="Filter value" x-model="newFilterValue" @keyup.enter="addFilter()" @keyup.escape="newFilterValue = ''">
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

function showModal(id) {
  const el = document.getElementById(id);
  if (el && window.bootstrap) bootstrap.Modal.getOrCreateInstance(el).show();
}

function hideModal(id) {
  const el = document.getElementById(id);
  if (el && window.bootstrap) bootstrap.Modal.getInstance(el)?.hide();
}

// Dismiss any open modal before a view/queue change removes its x-if block from the
// DOM. Otherwise the .modal-backdrop and body scroll-lock are orphaned (Bootstrap's
// hide callback never fires on a torn-out node), leaving the page dimmed and locked.
function dismissOpenModals() {
  if (!window.bootstrap) return;
  document.querySelectorAll('.modal.show').forEach((el) => bootstrap.Modal.getInstance(el)?.hide());
  // The teardown can drop the modal mid-transition, so sweep any stranded backdrop
  // and body lock on the next frame as a safety net.
  requestAnimationFrame(() => {
    document.querySelectorAll('.modal-backdrop').forEach((b) => b.remove());
    document.body.classList.remove('modal-open');
    document.body.style.removeProperty('overflow');
    document.body.style.removeProperty('padding-right');
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
async function saveOverrides(edit, { apiFn, modalId, buildBody, reload }) {
  const built = buildBody(edit);
  if (built.error) { edit.error = built.error; return; }
  edit.error = '';
  edit.saving = true;
  try {
    await apiFn(edit.prefix, built.body);
    hideModal(modalId);
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

// Runs a load body under the loading flag, a stale-response guard, and a
// one-shot error toast. body(seq, isStale) does the fetch + apply. opts.suppressToast,
// if it returns true at error time, skips the toast (still logs).
async function guardedLoad(self, errorLabel, body, opts) {
  // The first load of a view drives the global top-bar loader instead of an
  // in-table "Loading…" flash. Later polls only spin that view's Refresh button.
  // The release is idempotent, so a concurrent load must not claim twice.
  const store = self.$store && self.$store.app;
  if (!self.loaded && !!store && !self._initialLoadHeld) {
    self._initialLoadHeld = true;
    store.beginInitialLoad();
  }
  self.loading = true;
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
    if (!self._loadErrored) { self._loadErrored = true; showToast(errorLabel + ': ' + e.message); }
  } finally {
    // Only the winning load releases the loader, so a superseded first load
    // can't drop it while the view is still empty. Teardown and an onHide seq
    // bump release the claim themselves.
    if (seq === self._loadSeq) {
      self.loading = false;
      self.loaded = true;
      releaseInitialLoad(self);
    }
  }
}

// Drops this component's claim on the top-bar loader. Idempotent: teardown may
// release a load whose fetch is still in flight.
function releaseInitialLoad(self) {
  if (!self._initialLoadHeld) return;
  self._initialLoadHeld = false;
  const store = self.$store && self.$store.app;
  if (store) store.endInitialLoad();
}

// Shared jobs/dlq table mixin.
function tableTab(loadMethod, refreshStorageKey) {
  return {
    ...busyRows(),
    ...pollSpinner(),
    ...confirmArm(),
    ...tabActive(),
    refreshMode: (refreshStorageKey && localStorage.getItem(refreshStorageKey)) || '5s',
    _refreshTimer: null,
    _lastInvalidParentId: null,
    _lastInvalidJobId: null,
    _onQueueChanged: null,
    _onSseReconnect: null,
    _onSseEvent: null,
    pendingChanges: 0,

    // Filter builder: the group/parent/job filters, surfaced as chips plus one
    // "field + value" adder. Each maps onto existing filter state, so applyFilter
    // and the URL sync are unchanged.
    filterFields: [
      { field: 'group', label: 'Group', model: 'groupKeyFilter', applied: '_appliedGroupKey', numeric: false },
      { field: 'parent', label: 'Parent ID', model: 'parentIdFilter', applied: '_appliedParentId', numeric: true },
      // Job ID locates a single row, so it does not combine with the others.
      { field: 'job', label: 'Job ID', model: 'jobIdFilter', applied: '_appliedJobId', numeric: true, exclusive: true },
    ],
    newFilterField: 'group',
    newFilterValue: '',

    currentFilterField() {
      return this.filterFields.find((f) => f.field === this.newFilterField) || this.filterFields[0];
    },
    currentFilterPlaceholder() {
      return this.currentFilterField().label + '…';
    },
    activeFilterChips() {
      return this.filterFields
        .filter((f) => (this[f.applied] || '') !== '')
        .map((f) => ({ field: f.field, label: f.label, value: this[f.applied] }));
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

    _startTimer() {
      this._stopTimer();
      if (this.refreshMode === 'paused') return;
      const ms = ARB_TIMING.refreshModes[this.refreshMode] || ARB_TIMING.refreshModes['5s'];
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

    setRefreshMode(mode) {
      this.refreshMode = mode;
      if (refreshStorageKey) localStorage.setItem(refreshStorageKey, mode);
      this._startTimer();
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
      if (this.sortBy !== col) return ' ↕';
      return this.sortDir === 'asc' ? ' ▲' : ' ▼';
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

    _syncFiltersToUrl() {
      writeFiltersToUrl({ groupKey: this._appliedGroupKey, parentId: this._appliedParentId, jobId: this._appliedJobId, sortBy: this.sortBy, sortDir: this.sortDir });
    },

    applyFilter() {
      const pid = this.parentIdFilter.trim();
      const jid = (this.jobIdFilter || '').trim();
      // Auto-apply fires this from both Enter and change/blur. Only warn once per value.
      if (pid && !/^\d+$/.test(pid)) {
        if (this._lastInvalidParentId !== pid) {
          showToast('Parent ID must be a positive integer', 'warning');
          this._lastInvalidParentId = pid;
        }
        return;
      }
      this._lastInvalidParentId = null;
      if (jid && !/^\d+$/.test(jid)) {
        if (this._lastInvalidJobId !== jid) {
          showToast('Job ID must be a positive integer', 'warning');
          this._lastInvalidJobId = jid;
        }
        return;
      }
      this._lastInvalidJobId = null;
      if (pid === this._appliedParentId && jid === (this._appliedJobId || '') && this.groupKeyFilter === this._appliedGroupKey) return;
      this.parentIdFilter = pid;
      this.jobIdFilter = jid;
      this._resetView({ groupKey: this.groupKeyFilter, parentId: pid, jobId: jid });
    },

    filterByParent(id) {
      this.parentIdFilter = String(id);
      this.groupKeyFilter = '';
      this.jobIdFilter = '';
      this._resetView({ groupKey: '', parentId: String(id), jobId: '' });
    },

    _bindTableEvents(opts) {
      this._watchPolling();
      this._onQueueChanged = () => {
        this.disarm();
        this.groupKeyFilter = '';
        this.parentIdFilter = '';
        this.jobIdFilter = '';
        this.newFilterValue = '';
        this._appliedGroupKey = '';
        this._appliedParentId = '';
        this._appliedJobId = '';
        this.sortBy = '';
        this.sortDir = '';
        // Reset paging even while inactive, so the tab reopens on page 1 of the new
        // queue rather than a leftover offset (offset lives only in _resetView, which
        // is active-gated).
        this.offset = 0;
        if (opts.onQueueReset) opts.onQueueReset();
        if (this.active) this._resetView();
      };
      this._onSseReconnect = () => {
        if (this.active) this[loadMethod]();
      };
      this._onSseEvent = (e) => {
        const count = opts.relevant(e.detail);
        if (count > 0) this.pendingChanges += count;
      };
      window.addEventListener(ARB_EVENTS.queueChanged, this._onQueueChanged);
      window.addEventListener(ARB_EVENTS.sseReconnect, this._onSseReconnect);
      window.addEventListener(ARB_EVENTS.sseEvent, this._onSseEvent);
    },

    _unbindTableEvents() {
      window.removeEventListener(ARB_EVENTS.queueChanged, this._onQueueChanged);
      window.removeEventListener(ARB_EVENTS.sseReconnect, this._onSseReconnect);
      window.removeEventListener(ARB_EVENTS.sseEvent, this._onSseEvent);
      this._stopWatchPolling();
      releaseInitialLoad(this);
    },
  };
}

// Shared cron/workers polling mixin (visibility-aware).
function pollingTab(loadMethod, intervalMs) {
  return {
    ...busyRows(),
    ...pollSpinner(),
    ...tabActive(),
    refreshInterval: null,
    _visibilityHandler: null,

    startPolling() {
      this.stopPolling();
      this.refreshInterval = setInterval(() => { if (!this.loading) this[loadMethod](); }, intervalMs);
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
function gateLabel(g, empty = '-') {
  return g ? g.prefix + ':' + g.suffix : empty;
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

// Shared drill-down lifecycle for the rate-limit and concurrency tabs. Owns the
// expanded prefix and its capped child list, refreshes an open drill-down on each
// poll without flashing the spinner, and closes a drill-down whose prefix vanished.
// cfg supplies the field/method names, fetchers, and labels that differ per tab.
function drillDownTab(cfg) {
  return {
    // Policy list and guardedLoad state, owned by loadPolicies below.
    policies: [],
    loading: false,
    loaded: false,
    _loadErrored: false,
    expandedPrefix: null,
    [cfg.listField]: [],
    [cfg.loadingField]: false,
    _itemSeq: 0,

    // Total items for the open prefix, from its policy row (the drill-down list is
    // capped at cfg.itemLimit).
    itemTotal() {
      const p = this.policies.find((x) => x.prefix === this.expandedPrefix);
      return p ? p[cfg.countField] : this[cfg.listField].length;
    },

    // Truncation note reads the stable policy count and fixed cap, never the live
    // list, which is empty mid-load and would otherwise flash the note on open.
    itemCap: cfg.itemLimit,
    hasMoreItems() {
      return this.itemTotal() > cfg.itemLimit;
    },

    async loadPolicies() {
      await guardedLoad(this, cfg.policyError, async (seq, isStale) => {
        const data = await cfg.fetchPolicies();
        if (isStale()) return;
        this.policies = data.policies || [];
        // Close a drill-down whose prefix vanished.
        if (this.expandedPrefix && !this.policies.some((p) => p.prefix === this.expandedPrefix)) {
          this.expandedPrefix = null;
          this[cfg.listField] = [];
        }
      });
      // Keep an open drill-down fresh on each poll, without flashing the spinner.
      if (this.expandedPrefix) await this[cfg.loadName](this.expandedPrefix, { silent: true });
    },

    async [cfg.toggleName](p) {
      if (this.expandedPrefix === p.prefix) {
        this.expandedPrefix = null;
        this[cfg.listField] = [];
        this[cfg.loadingField] = false;
        return;
      }
      // Drop the previous prefix's rows so they never render under the new heading.
      this.expandedPrefix = p.prefix;
      this[cfg.listField] = [];
      await this[cfg.loadName](p.prefix);
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
      if (seq !== this._itemSeq || this.expandedPrefix !== prefix) return;
      // The latest fetch settles the spinner, even a silent one that superseded a
      // user-initiated load.
      this[cfg.loadingField] = false;
      if (err) {
        // On a background poll, keep the stale list rather than blanking it.
        if (silent) return;
        showToast(`Failed to load ${cfg.itemLabel}: ${err.message}`);
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
    <span class="toast-count badge text-bg-secondary align-self-center me-2 d-none"></span>
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
function withPagination(component, loadMethod) {
  const pagination = {
    limit: ARB_TIMING.pageLimit,
    offset: 0,
    loaded: false,

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
// { key, label, weight, required? } and a localStorage key.
function columnPrefs(columns, storageKey) {
  return {
    columns,
    colVis: {},

    _loadColPrefs() {
      const def = {};
      columns.forEach((c) => { def[c.key] = true; });
      try {
        const saved = JSON.parse(localStorage.getItem(storageKey)) || {};
        columns.forEach((c) => { if (c.key in saved) def[c.key] = saved[c.key] !== false; });
      } catch { /* keep defaults */ }
      this.colVis = def;
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
const _filterKeys = ['group_key', 'parent_id', 'job_id', 'status', 'sort_by', 'sort_dir'];

function readFiltersFromUrl() {
  const p = new URLSearchParams(location.search);
  return {
    groupKey: p.get('group_key') || '',
    parentId: p.get('parent_id') || '',
    jobId: p.get('job_id') || '',
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
  if (filters.jobId) url.searchParams.set('job_id', filters.jobId);
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

// Relative URL to a queue's Jobs tab, optionally filtered by status. One source of
// truth for the deep-link shape used by the queue cards, the stat cards, and the
// store's in-app navigation.
function queueJobsUrl(queue, status) {
  const p = new URLSearchParams({ queue });
  if (status) p.set('status', status);
  return '?' + p.toString() + '#jobs';
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

// Slots for trackTabActive's listeners. Teardown only sees a property the data
// object declared, so every component calling trackTabActive must spread this.
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
