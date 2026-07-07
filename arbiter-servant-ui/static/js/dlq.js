/**
 * Alpine component: DLQ table + retry/delete
 */
// Ordered column registry. Order must match the table header and cell order.
const DLQ_COLUMNS = [
  { key: 'select', label: 'Select', weight: 4, required: true },
  { key: 'dlqid', label: 'DLQ ID', weight: 7 },
  { key: 'jobid', label: 'Job ID', weight: 7 },
  { key: 'parent', label: 'Parent', weight: 7 },
  { key: 'group', label: 'Group', weight: 8 },
  { key: 'payload', label: 'Payload', weight: 13 },
  { key: 'failed', label: 'Failed At', weight: 12 },
  { key: 'attempts', label: 'Attempts', weight: 8 },
  { key: 'error', label: 'Last Error', weight: 13 },
  { key: 'ratelimit', label: 'Rate Limit', weight: 9 },
  { key: 'concurrency', label: 'Concurrency', weight: 10 },
  { key: 'actions', label: 'Actions', weight: 12 },
];

document.addEventListener('alpine:init', () => {
  Alpine.data('dlqTab', () => withPagination({
    ...columnPrefs(DLQ_COLUMNS, 'arb.dlqCols'),
    ...tableTab('loadDLQ', 'arb.dlqRefresh'),
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
    sortBy: '',
    sortDir: '',
    _loadErrored: false,

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
          hideModal('dlqDetailModal');
          this._stopTimer();
        },
      });
      this._bindTableEvents({
        onQueueReset: () => { this.selected = {}; },
        relevant: (events) => {
          const queue = Alpine.store('app').selectedQueue;
          return events.filter(evt => evt.table === queue && evt.event === 'job_dlq').length;
        },
      });
    },

    destroy() {
      untrackTabActive(this);
      this._unbindTableEvents();
      this._stopTimer();
    },

    async loadDLQ(filterOverrides) {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      const gk = filterOverrides?.groupKey ?? this._appliedGroupKey;
      const pid = filterOverrides?.parentId ?? this._appliedParentId;
      const startingPending = this.pendingChanges;
      await guardedLoad(this, 'Failed to load DLQ', async (seq, isStale) => {
        const data = await ArbiterAPI.listDLQ(queue, {
          limit: this.limit,
          offset: this.offset,
          parentId: pid || undefined,
          groupKey: gk || undefined,
          sortBy: this.sortBy || undefined,
          sortDir: this.sortDir || undefined,
        });
        if (isStale()) return;
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
        }
      });
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
      if (!this.confirmArmed('bulkRetry')) return;
      const queue = Alpine.store('app').selectedQueue;
      this.bulkBusy = true;
      // No batch-retry endpoint, so retry each entry individually, capped at 5
      // concurrent requests so a large selection doesn't flood the server.
      try {
        const results = await mapLimit(ids, ARB_TIMING.bulkConcurrency, id => ArbiterAPI.retryFromDLQ(queue, id));
        // Bail if the queue switched mid-op; _onQueueChanged already cleared selection.
        if (Alpine.store('app').selectedQueue !== queue) return;
        const failedIds = ids.filter((id, i) => results[i].status === 'rejected');
        const next = {};
        for (const id of failedIds) next[id] = true;
        this.selected = next;
        this.loadDLQ();
        if (failedIds.length > 0) showToast(`${failedIds.length} of ${ids.length} retries failed`);
        else showToast(`Retried ${ids.length} ${ids.length === 1 ? 'entry' : 'entries'}`, 'success');
      } finally {
        this.bulkBusy = false;
      }
    },

    async bulkDelete() {
      const ids = this.selectedIds;
      if (ids.length === 0 || this.bulkBusy) return;
      if (!this.confirmArmed('bulkDelete')) return;
      const queue = Alpine.store('app').selectedQueue;
      this.bulkBusy = true;
      try {
        const res = await ArbiterAPI.deleteDLQBatch(queue, ids);
        // Bail if the queue switched mid-op; _onQueueChanged already cleared selection.
        if (Alpine.store('app').selectedQueue !== queue) return;
        this.selected = {};
        this.loadDLQ();
        const deleted = res?.deleted ?? ids.length;
        if (deleted < ids.length) {
          showToast(`Deleted ${deleted} of ${ids.length}; ${ids.length - deleted} no longer present`, 'warning');
        } else {
          showToast(`Deleted ${deleted} ${deleted === 1 ? 'entry' : 'entries'}`, 'success');
        }
      } catch (e) {
        showToast('Failed to delete: ' + e.message);
      } finally {
        this.bulkBusy = false;
      }
    },

    async retryJob(id) {
      await this.withBusyRow(id, async () => {
        const queue = Alpine.store('app').selectedQueue;
        try {
          await ArbiterAPI.retryFromDLQ(queue, id);
          await this.loadDLQ();
        } catch (e) {
          showToast('Failed to retry: ' + e.message);
        }
      });
    },

    async deleteJob(id, el) {
      if (this.busyRows[id]) return;
      if (!this.confirmArmed('del:' + id)) return;
      closeDropdown(el);
      await this.withBusyRow(id, async () => {
        const queue = Alpine.store('app').selectedQueue;
        try {
          await ArbiterAPI.deleteDLQ(queue, id);
          await this.loadDLQ();
        } catch (e) {
          showToast('Failed to delete: ' + e.message);
        }
      });
    },

    _resetView(filterOverrides) {
      this.offset = 0;
      this.loadDLQ(filterOverrides);
      this._startTimer();
    },

    toggleSort(col) {
      this._cycleSort(col);
      this._resetView();
    },

    viewDetail(job) {
      this.selectedDLQJob = job;
      showModal('dlqDetailModal');
    },
  }, 'loadDLQ'));
});
