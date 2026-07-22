/**
 * Alpine component: completed-job archive with re-enqueue and purge actions.
 */
// Order must match the table header and cell order.
const ARCHIVE_COLUMNS = [
  { key: 'select', label: 'Select', weight: 4, required: true },
  { key: 'archiveid', label: 'Archive ID', weight: 6 },
  { key: 'jobid', label: 'Job ID', weight: 6 },
  { key: 'parent', label: 'Parent', weight: 6 },
  { key: 'group', label: 'Group', weight: 8 },
  { key: 'payload', label: 'Payload', weight: 18 },
  { key: 'hasresult', label: 'Results?', weight: 7 },
  { key: 'inserted', label: 'Inserted At', weight: 12 },
  { key: 'completed', label: 'Completed At', weight: 12 },
  { key: 'attempts', label: 'Attempts', weight: 8 },
  { key: 'actions', label: 'Actions', weight: 14 },
];

document.addEventListener('alpine:init', () => {
  Alpine.data('archiveTab', () => withPagination(withSelection({
    ...columnPrefs(ARCHIVE_COLUMNS, 'arb.archiveCols'),
    ...tableTab('loadArchive', 'arb.archiveRefresh'),
    archiveJobs: [],
    total: 0,
    loading: false,
    active: false,
    selectedArchiveJob: null,
    bulkBusy: false,
    parentIdFilter: '',
    groupKeyFilter: '',
    jobIdFilter: '',
    _appliedParentId: '',
    _appliedGroupKey: '',
    _appliedJobId: '',
    sortBy: '',
    sortDir: '',
    _loadErrored: false,

    _syncFiltersToUrl() {
      writeFiltersToUrl({ groupKey: this._appliedGroupKey, parentId: this._appliedParentId, jobId: this._appliedJobId, sortBy: this.sortBy, sortDir: this.sortDir });
    },

    toggleSort(col) {
      this._cycleSort(col);
      this._resetView();
    },

    init() {
      this._loadColPrefs();
      const f = readFiltersFromUrl();
      if (location.hash.replace('#', '') === 'archive') {
        this.groupKeyFilter = f.groupKey;
        this._appliedGroupKey = f.groupKey;
        this.parentIdFilter = f.parentId;
        this._appliedParentId = f.parentId;
        this.jobIdFilter = f.jobId;
        this._appliedJobId = f.jobId;
        this.sortBy = f.sortBy;
        this.sortDir = f.sortDir;
      }
      trackTabActive(this, '#tab-archive', {
        onShow: () => { this.loadArchive(); this._startTimer(); },
        onHide: () => {
          this._loadSeq = (this._loadSeq || 0) + 1;
          releaseInitialLoad(this);
          hideModal('archiveDetailModal');
          this._stopTimer();
        },
      });
      // No SSE event correlates with archival, so only queue switches and
      // reconnects trigger a reload. Routine updates ride the refresh timer.
      this._bindTableEvents({
        onQueueReset: () => { this.selected = {}; },
        relevant: () => 0,
      });
    },

    destroy() {
      untrackTabActive(this);
      this._unbindTableEvents();
      this._stopTimer();
    },

    async loadArchive(filterOverrides) {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      const gk = filterOverrides?.groupKey ?? this._appliedGroupKey;
      const pid = filterOverrides?.parentId ?? this._appliedParentId;
      const jid = filterOverrides?.jobId ?? this._appliedJobId;
      await guardedLoad(this, 'Failed to load archive', async (seq, isStale) => {
        const data = await ArbiterAPI.listArchive(queue, {
          limit: this.limit,
          offset: this.offset,
          parentId: pid || undefined,
          jobId: jid || undefined,
          groupKey: gk || undefined,
          sortBy: this.sortBy || undefined,
          sortDir: this.sortDir || undefined,
        });
        if (isStale()) return;
        this._appliedGroupKey = gk;
        this._appliedParentId = pid;
        this._appliedJobId = jid;
        this.archiveJobs = data.archiveJobs || [];
        this.total = data.archiveTotal || 0;
        // Drop selections for rows no longer on the current page.
        const present = new Set(this.archiveJobs.map(j => String(j.archivePrimaryKey)));
        const pruned = {};
        for (const id of Object.keys(this.selected)) {
          if (this.selected[id] && present.has(id)) pruned[id] = true;
        }
        this.selected = pruned;
        this._syncFiltersToUrl();

        // Clamp offset if past the last page (e.g. after a purge dropped the total).
        if (this.offset > 0 && this.offset >= this.total && this.total > 0) {
          this.offset = Math.max(0, (Math.ceil(this.total / this.limit) - 1) * this.limit);
          this.loadArchive();
        }
      });
    },

    async bulkReEnqueue() {
      const ids = this.selectedIds;
      if (ids.length === 0 || this.bulkBusy) return;
      if (!this.confirmArmed('bulkReEnqueue')) return;
      const queue = Alpine.store('app').selectedQueue;
      this.bulkBusy = true;
      try {
        const results = await mapLimit(ids, ARB_TIMING.bulkConcurrency, id => ArbiterAPI.reEnqueueArchive(queue, id));
        if (Alpine.store('app').selectedQueue !== queue) return;
        const failedIds = ids.filter((id, i) => results[i].status === 'rejected');
        const next = {};
        for (const id of failedIds) next[id] = true;
        this.selected = next;
        this.loadArchive();
        if (failedIds.length > 0) showToast(`${failedIds.length} of ${ids.length} re-enqueues failed`);
        else showToast(`Re-enqueued ${ids.length} ${ids.length === 1 ? 'job' : 'jobs'}`, 'success');
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
        const res = await ArbiterAPI.deleteArchiveBatch(queue, ids);
        if (Alpine.store('app').selectedQueue !== queue) return;
        this.selected = {};
        this.loadArchive();
        const deleted = res?.deleted ?? ids.length;
        showToast(`Purged ${deleted} ${deleted === 1 ? 'entry' : 'entries'}`, 'success');
      } catch (e) {
        showToast('Failed to purge: ' + e.message);
      } finally {
        this.bulkBusy = false;
      }
    },

    async reEnqueueJob(id, el) {
      if (this.busyRows[id]) return;
      if (!this.confirmArmed('reenq:' + id)) return;
      closeDropdown(el);
      await this.withBusyRow(id, async () => {
        const queue = Alpine.store('app').selectedQueue;
        try {
          await ArbiterAPI.reEnqueueArchive(queue, id);
          showToast('Re-enqueued', 'success');
        } catch (e) {
          showToast('Failed to re-enqueue: ' + e.message);
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
          await ArbiterAPI.deleteArchive(queue, id);
          await this.loadArchive();
        } catch (e) {
          showToast('Failed to purge: ' + e.message);
        }
      });
    },

    _resetView(filterOverrides) {
      this.offset = 0;
      this.loadArchive(filterOverrides);
      this._startTimer();
    },

    viewDetail(job) {
      this.selectedArchiveJob = job;
      showModal('archiveDetailModal');
    },
  }, 'archiveJobs', 'archivePrimaryKey'), 'loadArchive'));
});
