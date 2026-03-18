/**
 * Alpine component: DLQ table + retry/delete
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('dlqTab', () => withPagination({
    dlqJobs: [],
    total: 0,
    loading: false,
    active: false,
    selectedDLQJob: null,
    parentIdFilter: '',
    groupKeyFilter: '',
    _appliedParentId: '',
    _appliedGroupKey: '',
    refreshMode: '5s',
    _refreshTimer: null,
    pendingChanges: 0,

    get hasUnappliedFilters() {
      return this.groupKeyFilter !== this._appliedGroupKey
          || this.parentIdFilter !== this._appliedParentId;
    },

    _syncFiltersToUrl() {
      writeFiltersToUrl({
        groupKey: this._appliedGroupKey,
        parentId: this._appliedParentId,
      });
    },

    init() {
      const f = readFiltersFromUrl();
      if (location.hash.replace('#', '') === 'dlq') {
        this.groupKeyFilter = f.groupKey;
        this._appliedGroupKey = f.groupKey;
        this.parentIdFilter = f.parentId;
        this._appliedParentId = f.parentId;
      }
      trackTabActive(this, '#tab-dlq', {
        onShow: () => { this.loadDLQ(); this._startTimer(); },
        onHide: () => {
          clearFiltersFromUrl();
          if (this._refreshTimer) { clearInterval(this._refreshTimer); this._refreshTimer = null; }
        },
      });
      window.addEventListener('queue-changed', () => {
        if (this.active) {
          this.groupKeyFilter = '';
          this.parentIdFilter = '';
          this._appliedGroupKey = '';
          this._appliedParentId = '';
          this._resetView();
        }
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
      const gk = filterOverrides?.groupKey ?? this._appliedGroupKey;
      const pid = filterOverrides?.parentId ?? this._appliedParentId;
      try {
        const data = await ArbiterAPI.listDLQ(queue, {
          limit: this.limit,
          offset: this.offset,
          parentId: pid || undefined,
          groupKey: gk || undefined,
        });
        this._appliedGroupKey = gk;
        this._appliedParentId = pid;
        this.dlqJobs = data.dlqJobs || [];
        this.total = data.dlqTotal || 0;
        this.pendingChanges = 0;
        this._syncFiltersToUrl();
      } catch (e) {
        console.error('Failed to load DLQ:', e);
      } finally {
        this.loading = false;
        this.loaded = true;
      }
    },

    async retryJob(id) {
      const queue = Alpine.store('app').selectedQueue;
      try {
        await ArbiterAPI.retryFromDLQ(queue, id);
        this.loadDLQ();
      } catch (e) {
        showToast('Failed to retry: ' + e.message);
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
      this._resetView({ groupKey: this.groupKeyFilter, parentId: this.parentIdFilter });
    },

    filterByParent(id) {
      this.parentIdFilter = String(id);
      this.groupKeyFilter = '';
      this._resetView({ groupKey: '', parentId: String(id) });
    },

    viewDetail(job) {
      this.selectedDLQJob = job;
      bootstrap.Modal.getOrCreateInstance(document.getElementById('dlqDetailModal')).show();
    },
  }, 'loadDLQ'));
});
