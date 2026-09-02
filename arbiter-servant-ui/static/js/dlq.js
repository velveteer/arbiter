/**
 * Alpine component: DLQ table + retry/delete
 */
// Ordered column registry. Order must match the table header and cell order.
const DLQ_COLUMNS = [
  { key: 'select', label: 'Select', weight: 4, required: true, narrow: false },
  { key: 'dlqid', label: 'DLQ ID', weight: 7, narrow: false },
  { key: 'jobid', label: 'Job ID', weight: 7 },
  { key: 'parent', label: 'Parent', weight: 7, narrow: false },
  { key: 'group', label: 'Group', weight: 8, narrow: false },
  { key: 'kind', label: 'Kind', weight: 8, autoHide: true, narrow: false },
  { key: 'payload', label: 'Payload', weight: 13 },
  { key: 'failed', label: 'Failed', weight: 12 },
  { key: 'attempts', label: 'Attempts', weight: 8, narrow: false },
  { key: 'error', label: 'Last Error', weight: 13, narrow: false },
  { key: 'gates', label: 'Gates', weight: 13, autoHide: true, narrow: false },
  { key: 'actions', label: 'Actions', weight: 5 },
];

// Row actions, stamped into both the row menu and the drawer header.
const DLQ_ACTIONS_HTML = `
<li x-show="!job._inDrawer"><a class="dropdown-item" href="#" @click.prevent="viewDetail(job); closeDropdown($el)">Detail</a></li>
<li><a class="dropdown-item" href="#" @click.prevent="retryJob(job.dlqPrimaryKey); closeDropdown($el)">Retry</a></li>
<li><a class="dropdown-item text-danger" href="#" @click.prevent="deleteJob(job.dlqPrimaryKey, $el)" :class="{ 'fw-semibold': isArmed('del:' + job.dlqPrimaryKey) }" x-text="isArmed('del:' + job.dlqPrimaryKey) ? 'Confirm delete permanently' : 'Delete'"></a></li>`;

document.addEventListener('alpine:init', () => {
  Alpine.data('dlqTab', () => withPagination(withSelection({
    ...columnPrefs(DLQ_COLUMNS, 'arb.dlqCols.v2'),
    ...rowDetail('dlqJobs', 'dlqPrimaryKey', 'selectedDLQJob', { drawer: 'dlqDetailDrawer' }),
    ...tableTab('loadDLQ', 'arb.dlqRefresh'),
    dlqJobs: [],
    rowNoun: 'DLQ entry',
    detailActionsHtml: DLQ_ACTIONS_HTML,
    rowNounPlural: 'DLQ entries',
    total: 0,
    ...loadState(),
    active: false,
    selectedDLQJob: null,
    bulkBusy: false,
    parentIdFilter: '',
    groupKeyFilter: '',
    jobIdFilter: '',
    kindFilter: '',
    _appliedParentId: '',
    _appliedGroupKey: '',
    _appliedJobId: '',
    _appliedKind: '',
    sortBy: '',
    sortDir: '',

    filterFields: [
      { field: 'group', label: 'Group', param: 'group_key', model: 'groupKeyFilter', applied: '_appliedGroupKey' },
      { field: 'parent', label: 'Parent ID', param: 'parent_id', model: 'parentIdFilter', applied: '_appliedParentId', numeric: true },
      { field: 'job', label: 'Job ID', param: 'job_id', model: 'jobIdFilter', applied: '_appliedJobId', numeric: true, exclusive: true },
      { field: 'kind', label: 'Kind', param: 'kind', model: 'kindFilter', applied: '_appliedKind', options: 'kindOptions' },
    ],

    init() {
      this._loadColPrefs();
      this.readUrlFilters('dlq');
      this.loadKinds();
      trackTabActive(this, '#tab-dlq', {
        onShow: () => { this.loadDLQ(); this._startTimer(); },
        onHide: () => {
          this._loadSeq = (this._loadSeq || 0) + 1;
          releaseInitialLoad(this);
          this.closeDetail();
          this._stopTimer();
        },
      });
      this._bindTableEvents({
        hashName: 'dlq',
        onQueueReset: () => { this.selected = {}; this.resetAutoEmpty(); this.loadKinds(); },
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
      const gk = this.filterValue('group', filterOverrides);
      const pid = this.filterValue('parent', filterOverrides);
      const jid = this.filterValue('job', filterOverrides);
      const kind = this.filterValue('kind', filterOverrides);
      const startingPending = this.pendingChanges;
      await guardedLoad(this, 'Failed to load DLQ', async (seq, isStale) => {
        const data = await ArbiterAPI.listDLQ(queue, {
          limit: this.limit,
          offset: this.offset,
          parentId: pid || undefined,
          jobId: jid || undefined,
          groupKey: gk || undefined,
          kind: kind || undefined,
          sortBy: this.sortBy || undefined,
          sortDir: this.sortDir || undefined,
        });
        if (isStale()) return;
        this._appliedGroupKey = gk;
        this._appliedParentId = pid;
        this._appliedJobId = jid;
        this._appliedKind = kind;
        this.dlqJobs = data.dlqJobs || [];
        this.total = data.dlqTotal || 0;
        this.resyncDetailSelection();
        if (this.dlqJobs.length > 0) {
          this.setAutoEmpty({
            kind: this.dlqJobs.every((j) => !j.jobSnapshot?.kind),
            gates: this.dlqJobs.every((j) => !j.jobSnapshot?.rateLimit && !j.jobSnapshot?.concurrency),
          });
        }
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
        // Bail if the queue switched mid-op. _onQueueChanged already cleared selection.
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
        // Bail if the queue switched mid-op. _onQueueChanged already cleared selection.
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
          this.closeDetailIfOpen(id);
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
          this.closeDetailIfOpen(id);
          await this.loadDLQ();
        } catch (e) {
          showToast('Failed to delete: ' + e.message);
        }
      });
    },

    get detailTitle() {
      return this.selectedDLQJob ? 'DLQ entry ' + this.selectedDLQJob.dlqPrimaryKey : 'DLQ entry';
    },

    get detailStatus() {
      return '';
    },

    get detailRows() {
      const cur = this.selectedDLQJob;
      return cur ? [Object.assign({}, cur, { _id: cur.dlqPrimaryKey, _inDrawer: true })] : [];
    },

    viewDetail(job) {
      this.selectedDLQJob = job;
      showDrawer('dlqDetailDrawer');
    },
  }, 'dlqJobs', 'dlqPrimaryKey'), 'loadDLQ', 'arb.dlqPageSize'));
});
