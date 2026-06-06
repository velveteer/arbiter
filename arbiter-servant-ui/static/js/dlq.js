/**
 * Alpine component: DLQ table + retry/delete
 */
// Ordered column registry. Order must match the table header and cell order.
const DLQ_COLUMNS = [
  { key: 'select', label: 'Select', weight: 4, required: true },
  { key: 'dlqid', label: 'DLQ ID', weight: 6 },
  { key: 'jobid', label: 'Job ID', weight: 6 },
  { key: 'parent', label: 'Parent', weight: 6 },
  { key: 'group', label: 'Group', weight: 8 },
  { key: 'payload', label: 'Payload', weight: 16 },
  { key: 'failed', label: 'Failed At', weight: 12 },
  { key: 'attempts', label: 'Attempts', weight: 8 },
  { key: 'error', label: 'Last Error', weight: 16 },
  { key: 'actions', label: 'Actions', weight: 18 },
];

document.addEventListener('alpine:init', () => {
  Alpine.data('dlqTab', () => withPagination({
    ...columnPrefs(DLQ_COLUMNS, 'arb.dlqCols'),
    dlqJobs: [],
    total: 0,
    loading: false,
    active: false,
    selectedDLQJob: null,
    selected: {},
    bulkBusy: false,
    parentIdFilter: '',
    groupKeyFilter: '',
    _appliedParentId: '',
    _appliedGroupKey: '',
    refreshMode: '5s',
    _refreshTimer: null,
    pendingChanges: 0,
    sortBy: '',
    sortDir: '',

    get hasUnappliedFilters() {
      return this.groupKeyFilter !== this._appliedGroupKey
          || this.parentIdFilter !== this._appliedParentId;
    },

    _syncFiltersToUrl() {
      writeFiltersToUrl({
        groupKey: this._appliedGroupKey,
        parentId: this._appliedParentId,
        sortBy: this.sortBy,
        sortDir: this.sortDir,
      });
    },

    init() {
      this._loadColPrefs();
      const f = readFiltersFromUrl();
      if (location.hash.replace('#', '') === 'dlq') {
        this.groupKeyFilter = f.groupKey;
        this._appliedGroupKey = f.groupKey;
        this.parentIdFilter = f.parentId;
        this._appliedParentId = f.parentId;
        this.sortBy = f.sortBy;
        this.sortDir = f.sortDir;
      }
      trackTabActive(this, '#tab-dlq', {
        onShow: () => { this.loadDLQ(); this._startTimer(); },
        onHide: () => {
          this._loadSeq = (this._loadSeq || 0) + 1;
          const modalEl = document.getElementById('dlqDetailModal');
          if (modalEl) bootstrap.Modal.getInstance(modalEl)?.hide();
          clearFiltersFromUrl();
          this.groupKeyFilter = '';
          this.parentIdFilter = '';
          this._appliedGroupKey = '';
          this._appliedParentId = '';
          this.sortBy = '';
          this.sortDir = '';
          this.selected = {};
          if (this._refreshTimer) { clearInterval(this._refreshTimer); this._refreshTimer = null; }
        },
      });
      window.addEventListener('queue-changed', () => {
        this.groupKeyFilter = '';
        this.parentIdFilter = '';
        this._appliedGroupKey = '';
        this._appliedParentId = '';
        this.sortBy = '';
        this.sortDir = '';
        this.selected = {};
        if (this.active) this._resetView();
      });
      window.addEventListener('sse-reconnect', () => {
        if (this.active) this.loadDLQ();
      });
      window.addEventListener('sse-event', (e) => {
        const queue = Alpine.store('app').selectedQueue;
        const count = e.detail.filter(evt =>
          evt.table === queue && evt.event === 'job_dlq'
        ).length;
        if (count > 0) this.pendingChanges += count;
      });
    },

    _startTimer() {
      if (this._refreshTimer) {
        clearInterval(this._refreshTimer);
        this._refreshTimer = null;
      }
      if (this.refreshMode === 'paused') return;
      const ms = { '1s': 1000, '5s': 5000, '10s': 10000, '30s': 30000 }[this.refreshMode] || 5000;
      this._refreshTimer = setInterval(() => {
        if (this.active && !this.loading) this.loadDLQ();
      }, ms);
    },

    setRefreshMode(mode) {
      this.refreshMode = mode;
      this._startTimer();
    },

    async loadDLQ(filterOverrides) {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      this.loading = true;
      this._loadSeq = (this._loadSeq || 0) + 1;
      const seq = this._loadSeq;
      const gk = filterOverrides?.groupKey ?? this._appliedGroupKey;
      const pid = filterOverrides?.parentId ?? this._appliedParentId;
      const startingPending = this.pendingChanges;
      try {
        const data = await ArbiterAPI.listDLQ(queue, {
          limit: this.limit,
          offset: this.offset,
          parentId: pid || undefined,
          groupKey: gk || undefined,
          sortBy: this.sortBy || undefined,
          sortDir: this.sortDir || undefined,
        });
        if (seq !== this._loadSeq) return;
        this._appliedGroupKey = gk;
        this._appliedParentId = pid;
        this.dlqJobs = data.dlqJobs || [];
        this.total = data.dlqTotal || 0;
        // Drop selections for rows no longer on the current page (deleted, retried, paged away).
        const present = new Set(this.dlqJobs.map(j => String(j.dlqPrimaryKey)));
        const pruned = {};
        for (const id of Object.keys(this.selected)) {
          if (this.selected[id] && present.has(id)) pruned[id] = true;
        }
        this.selected = pruned;
        this.pendingChanges = Math.max(0, this.pendingChanges - startingPending);
        this._syncFiltersToUrl();

        // Clamp offset if past last page (e.g. after a delete dropped the total).
        if (this.offset > 0 && this.offset >= this.total && this.total > 0) {
          this.offset = Math.max(0, (Math.ceil(this.total / this.limit) - 1) * this.limit);
          this.loadDLQ();
          return;
        }
      } catch (e) {
        if (seq !== this._loadSeq) return;
        console.error('Failed to load DLQ:', e);
      } finally {
        if (seq === this._loadSeq) {
          this.loading = false;
          this.loaded = true;
        }
      }
    },

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
      return this.dlqJobs.length > 0 && this.dlqJobs.every(j => this.selected[j.dlqPrimaryKey]);
    },

    toggleSelectAll() {
      if (this.allSelected) {
        this.selected = {};
      } else {
        const next = {};
        for (const j of this.dlqJobs) next[j.dlqPrimaryKey] = true;
        this.selected = next;
      }
    },

    async bulkRetry() {
      const ids = this.selectedIds;
      if (ids.length === 0 || this.bulkBusy) return;
      if (!confirm(`Retry ${ids.length} DLQ ${ids.length === 1 ? 'entry' : 'entries'}?`)) return;
      const queue = Alpine.store('app').selectedQueue;
      this.bulkBusy = true;
      // No batch-retry endpoint, so retry each entry individually, capped at 5
      // concurrent requests so a large selection doesn't flood the server.
      const results = await mapLimit(ids, 5, id => ArbiterAPI.retryFromDLQ(queue, id));
      this.bulkBusy = false;
      const failed = results.filter(r => r.status === 'rejected').length;
      this.selected = {};
      this.loadDLQ();
      if (failed > 0) showToast(`${failed} of ${ids.length} retries failed`);
      else showToast(`Retried ${ids.length} ${ids.length === 1 ? 'entry' : 'entries'}`, 'success');
    },

    async bulkDelete() {
      const ids = this.selectedIds;
      if (ids.length === 0 || this.bulkBusy) return;
      if (!confirm(`Permanently delete ${ids.length} DLQ ${ids.length === 1 ? 'entry' : 'entries'}?`)) return;
      const queue = Alpine.store('app').selectedQueue;
      this.bulkBusy = true;
      try {
        const res = await ArbiterAPI.deleteDLQBatch(queue, ids);
        this.selected = {};
        this.loadDLQ();
        showToast(`Deleted ${res?.deleted ?? ids.length} ${(res?.deleted ?? ids.length) === 1 ? 'entry' : 'entries'}`, 'success');
      } catch (e) {
        showToast('Failed to delete: ' + e.message);
      } finally {
        this.bulkBusy = false;
      }
    },

    async retryJob(id) {
      if (this._actionBusy) return;
      this._actionBusy = true;
      const queue = Alpine.store('app').selectedQueue;
      try {
        await ArbiterAPI.retryFromDLQ(queue, id);
        this.loadDLQ();
      } catch (e) {
        showToast('Failed to retry: ' + e.message);
      } finally {
        this._actionBusy = false;
      }
    },

    async deleteJob(id) {
      const queue = Alpine.store('app').selectedQueue;
      if (!confirm('Permanently delete this DLQ entry?')) return;
      try {
        await ArbiterAPI.deleteDLQ(queue, id);
        this.loadDLQ();
      } catch (e) {
        showToast('Failed to delete: ' + e.message);
      }
    },

    _resetView(filterOverrides) {
      this.offset = 0;
      this.loadDLQ(filterOverrides);
      this._startTimer();
    },

    applyFilter() {
      const trimmed = this.parentIdFilter.trim();
      if (trimmed && !/^\d+$/.test(trimmed)) {
        // Auto-apply fires this from both Enter and change/blur; only warn once per value.
        if (this._lastInvalidParentId !== trimmed) {
          showToast('Parent ID must be a positive integer', 'warning');
          this._lastInvalidParentId = trimmed;
        }
        return;
      }
      this._lastInvalidParentId = null;
      if (trimmed === this._appliedParentId && this.groupKeyFilter === this._appliedGroupKey) return;
      this.parentIdFilter = trimmed;
      this._resetView({ groupKey: this.groupKeyFilter, parentId: trimmed });
    },

    filterByParent(id) {
      this.parentIdFilter = String(id);
      this.groupKeyFilter = '';
      this._resetView({ groupKey: '', parentId: String(id) });
    },

    toggleSort(col) {
      if (this.sortBy !== col) {
        this.sortBy = col;
        this.sortDir = 'desc';
      } else if (this.sortDir === 'desc') {
        this.sortDir = 'asc';
      } else {
        this.sortBy = '';
        this.sortDir = '';
      }
      this._resetView();
    },

    sortIndicator(col) {
      if (this.sortBy !== col) return ' ↕';
      return this.sortDir === 'asc' ? ' ▲' : ' ▼';
    },

    viewDetail(job) {
      this.selectedDLQJob = job;
      bootstrap.Modal.getOrCreateInstance(document.getElementById('dlqDetailModal')).show();
    },
  }, 'loadDLQ'));
});
