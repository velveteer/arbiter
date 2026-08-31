/**
 * Alpine component: completed-job archive with re-enqueue and purge actions.
 */
// Order must match the table header and cell order.
const ARCHIVE_COLUMNS = [
  { key: 'select', label: 'Select', weight: 4, required: true, narrow: false },
  { key: 'archiveid', label: 'Archive ID', weight: 6, narrow: false },
  { key: 'jobid', label: 'Job ID', weight: 6 },
  { key: 'parent', label: 'Parent', weight: 6, narrow: false },
  { key: 'group', label: 'Group', weight: 8, narrow: false },
  { key: 'payload', label: 'Payload', weight: 18 },
  { key: 'hasresult', label: 'Result', weight: 7, narrow: false },
  { key: 'inserted', label: 'Inserted', weight: 12, narrow: false },
  { key: 'completed', label: 'Completed', weight: 12 },
  { key: 'attempts', label: 'Attempts', weight: 8, narrow: false },
  { key: 'actions', label: 'Actions', weight: 5 },
];

// Row actions, stamped into both the row menu and the drawer header.
const ARCHIVE_ACTIONS_HTML = `
<li x-show="!job._inDrawer"><a class="dropdown-item" href="#" @click.prevent="viewDetail(job); closeDropdown($el)">Detail</a></li>
<li><a class="dropdown-item" href="#" @click.prevent="reEnqueueJob(job.archivePrimaryKey, $el)" :class="{ 'fw-semibold': isArmed('reenq:' + job.archivePrimaryKey) }" x-text="isArmed('reenq:' + job.archivePrimaryKey) ? 'Confirm re-enqueue' : 'Re-enqueue'"></a></li>
<li><a class="dropdown-item text-danger" href="#" @click.prevent="deleteJob(job.archivePrimaryKey, $el)" :class="{ 'fw-semibold': isArmed('del:' + job.archivePrimaryKey) }" x-text="isArmed('del:' + job.archivePrimaryKey) ? 'Confirm purge' : 'Purge'"></a></li>`;

document.addEventListener('alpine:init', () => {
  Alpine.data('archiveTab', () => withPagination(withSelection({
    ...columnPrefs(ARCHIVE_COLUMNS, 'arb.archiveCols'),
    ...rowDetail('archiveJobs', 'archivePrimaryKey', 'selectedArchiveJob', { drawer: 'archiveDetailDrawer' }),
    ...tableTab('loadArchive', 'arb.archiveRefresh'),
    archiveJobs: [],
    rowNoun: 'archived job',
    detailActionsHtml: ARCHIVE_ACTIONS_HTML,
    rowNounPlural: '',
    total: 0,
    ...loadState(),
    active: false,
    selectedArchiveJob: null,
    bulkBusy: false,
    parentIdFilter: '',
    groupKeyFilter: '',
    jobIdFilter: '',
    completedAfterFilter: '',
    completedBeforeFilter: '',
    _appliedParentId: '',
    _appliedGroupKey: '',
    _appliedJobId: '',
    _appliedCompletedAfter: '',
    _appliedCompletedBefore: '',
    sortBy: '',
    sortDir: '',

    // The shared three, plus the completion window, which is the only way to find
    // anything in an archive that has been accumulating for months.
    filterFields: [
      { field: 'group', label: 'Group', param: 'group_key', model: 'groupKeyFilter', applied: '_appliedGroupKey' },
      { field: 'parent', label: 'Parent ID', param: 'parent_id', model: 'parentIdFilter', applied: '_appliedParentId', numeric: true },
      { field: 'job', label: 'Job ID', param: 'job_id', model: 'jobIdFilter', applied: '_appliedJobId', numeric: true, exclusive: true },
      { field: 'after', label: 'Completed after', param: 'completed_after', model: 'completedAfterFilter', applied: '_appliedCompletedAfter', type: 'datetime-local', format: formatTime },
      { field: 'before', label: 'Completed before', param: 'completed_before', model: 'completedBeforeFilter', applied: '_appliedCompletedBefore', type: 'datetime-local', format: formatTime },
    ],

    init() {
      this._loadColPrefs();
      this.readUrlFilters('archive');
      trackTabActive(this, '#tab-archive', {
        onShow: () => { this.loadArchive(); this._startTimer(); },
        onHide: () => {
          this._loadSeq = (this._loadSeq || 0) + 1;
          releaseInitialLoad(this);
          this.closeDetail();
          this._stopTimer();
        },
      });
      // No SSE event correlates with archival, so only queue switches and
      // reconnects trigger a reload. Routine updates ride the refresh timer.
      this._bindTableEvents({
        hashName: 'archive',
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
      const gk = this.filterValue('group', filterOverrides);
      const pid = this.filterValue('parent', filterOverrides);
      const jid = this.filterValue('job', filterOverrides);
      const after = this.filterValue('after', filterOverrides);
      const before = this.filterValue('before', filterOverrides);
      await guardedLoad(this, 'Failed to load archive', async (seq, isStale) => {
        const data = await ArbiterAPI.listArchive(queue, {
          limit: this.limit,
          offset: this.offset,
          parentId: pid || undefined,
          jobId: jid || undefined,
          groupKey: gk || undefined,
          completedAfter: toIsoInstant(after),
          completedBefore: toIsoInstant(before),
          sortBy: this.sortBy || undefined,
          sortDir: this.sortDir || undefined,
        });
        if (isStale()) return;
        this._appliedGroupKey = gk;
        this._appliedParentId = pid;
        this._appliedJobId = jid;
        this._appliedCompletedAfter = after;
        this._appliedCompletedBefore = before;
        this.archiveJobs = data.archiveJobs || [];
        this.resyncDetailSelection();
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
          this.closeDetailIfOpen(id);
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
          this.closeDetailIfOpen(id);
          await this.loadArchive();
        } catch (e) {
          showToast('Failed to purge: ' + e.message);
        }
      });
    },

    get detailTitle() {
      return this.selectedArchiveJob ? 'Archived job ' + this.selectedArchiveJob.archivePrimaryKey : 'Archived job';
    },

    get detailStatus() {
      return '';
    },

    get detailRows() {
      const cur = this.selectedArchiveJob;
      return cur ? [Object.assign({}, cur, { _id: cur.archivePrimaryKey, _inDrawer: true })] : [];
    },

    viewDetail(job) {
      this.selectedArchiveJob = job;
      showDrawer('archiveDetailDrawer');
    },
  }, 'archiveJobs', 'archivePrimaryKey'), 'loadArchive', 'arb.archivePageSize'));
});
