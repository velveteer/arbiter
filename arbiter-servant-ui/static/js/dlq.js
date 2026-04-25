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
          this._loadSeq = (this._loadSeq || 0) + 1;
          const modalEl = document.getElementById('dlqDetailModal');
          if (modalEl) bootstrap.Modal.getInstance(modalEl)?.hide();
          clearFiltersFromUrl();
          this.groupKeyFilter = '';
          this.parentIdFilter = '';
          this._appliedGroupKey = '';
          this._appliedParentId = '';
          if (this._refreshTimer) { clearInterval(this._refreshTimer); this._refreshTimer = null; }
        },
      });
      window.addEventListener('queue-changed', () => {
        this.groupKeyFilter = '';
        this.parentIdFilter = '';
        this._appliedGroupKey = '';
        this._appliedParentId = '';
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
        });
        if (seq !== this._loadSeq) return;
        this._appliedGroupKey = gk;
        this._appliedParentId = pid;
        this.dlqJobs = data.dlqJobs || [];
        this.total = data.dlqTotal || 0;
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
      const trimmed = this.parentIdFilter.trim();
      if (trimmed && !/^\d+$/.test(trimmed)) {
        showToast('Parent ID must be a positive integer', 'warning');
        return;
      }
      this.parentIdFilter = trimmed;
      this._resetView({ groupKey: this.groupKeyFilter, parentId: trimmed });
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
