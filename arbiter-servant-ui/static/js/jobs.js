/**
 * Alpine component: job table + pagination + actions + insert form + detail drawer
 */
// Ordered column registry. Order must match the table header and cell order.
// weight is a relative share, renormalized over the visible columns to fill 100%.
// autoHide columns drop out when no row on the page populates them.
const JOB_COLUMNS = [
  { key: 'select', label: '', weight: 3, required: true, narrow: false },
  { key: 'id', label: 'ID', weight: 4, required: true },
  { key: 'kind', label: 'Kind', weight: 8, autoHide: true, narrow: false },
  { key: 'payload', label: 'Payload', weight: 14 },
  { key: 'group', label: 'Group', weight: 7, autoHide: true, narrow: false },
  { key: 'parent', label: 'Parent', weight: 7, autoHide: true, narrow: false },
  { key: 'children', label: 'Children', weight: 9, autoHide: true, narrow: false },
  { key: 'priority', label: 'Priority', weight: 8, autoHide: true, narrow: false },
  { key: 'attempts', label: 'Attempts', weight: 8, autoHide: true, narrow: false },
  { key: 'status', label: 'Status', weight: 7 },
  { key: 'inserted', label: 'Inserted', weight: 8 },
  { key: 'visible', label: 'Visible', weight: 10, autoHide: true, narrow: false },
  { key: 'gates', label: 'Gates', weight: 10, autoHide: true, narrow: false },
  { key: 'actions', label: 'Actions', weight: 4 },
];

// Row actions, stamped into both the row menu and the drawer header so the two
// cannot drift. Binds `job`, which each site supplies.
const JOB_ACTIONS_HTML = `
<li x-show="!job._inDrawer"><a class="dropdown-item" href="#" @click.prevent="viewDetail(job.primaryKey); closeDropdown($el)">Detail</a></li>
<li x-show="job.status === 'scheduled' || job.status === 'backoff'">
  <a class="dropdown-item" href="#" @click.prevent="promoteJob(job.primaryKey); closeDropdown($el)">Promote</a>
</li>
<li x-show="canSuspend(job)">
  <a class="dropdown-item" href="#" @click.prevent="pauseAction(job); closeDropdown($el)" x-text="job._childCount > 0 ? 'Pause descendants' : 'Suspend'"></a>
</li>
<li x-show="canResume(job)">
  <a class="dropdown-item" href="#" @click.prevent="resumeAction(job); closeDropdown($el)" x-text="job._childCount > 0 ? 'Resume descendants' : 'Resume'"></a>
</li>
<li><hr class="dropdown-divider"></li>
<li>
  <a class="dropdown-item" href="#" @click.prevent="cancelJob(job.primaryKey, $el)" :class="{ 'text-warning fw-semibold': isArmed('cancel:' + job.primaryKey) }" x-text="isArmed('cancel:' + job.primaryKey) ? ('Confirm cancel' + (job._childCount ? ' (+' + job._childCount + ' children)' : '')) : 'Cancel'"></a>
</li>
<li x-show="job.status === 'in_flight'">
  <a class="dropdown-item text-danger" href="#" @click.prevent="forceCancelJob(job.primaryKey, $el)" :class="{ 'fw-semibold': isArmed('fcancel:' + job.primaryKey) }" x-text="isArmed('fcancel:' + job.primaryKey) ? 'Confirm force-cancel (interrupts handler)' : 'Force Cancel'"></a>
</li>
<li>
  <a class="dropdown-item" href="#" @click.prevent="moveToDLQ(job.primaryKey, $el)" :class="{ 'text-warning fw-semibold': isArmed('movedlq:' + job.primaryKey) }" x-text="isArmed('movedlq:' + job.primaryKey) ? ('Confirm move to DLQ' + (job._childCount ? ' (+' + job._childCount + ' children)' : '')) : 'Move to DLQ'"></a>
</li>`;

document.addEventListener('alpine:init', () => {
  Alpine.data('jobsTab', () => withPagination(withSelection({
    ...columnPrefs(JOB_COLUMNS, 'arb.jobCols.v2'),
    ...rowDetail('selectableJobs', 'primaryKey', 'selectedJob', { openWith: (row) => row.primaryKey, drawer: 'jobDetailDrawer' }),
    ...tableTab('loadJobs', 'arb.jobsRefresh'),
    bulkBusy: false,
    rowNoun: 'job',
    rowNounPlural: '',
    jobs: [],
    total: 0,
    groupKeyFilter: '',
    parentIdFilter: '',
    jobIdFilter: '',
    claimedByFilter: '',
    kindFilter: '',
    payloadFilter: '',
    ratePrefixFilter: '',
    concPrefixFilter: '',
    stateFilter: '',
    _appliedGroupKey: '',
    _appliedParentId: '',
    _appliedJobId: '',
    _appliedClaimedBy: '',
    _appliedKind: '',
    _appliedPayload: '',
    _appliedRatePrefix: '',
    _appliedConcPrefix: '',

    // The shared three, plus the ones only a job table can answer: which worker holds
    // a job, what its payload says, and which policy gates it.
    filterFields: [
      { field: 'group', label: 'Group', param: 'group_key', model: 'groupKeyFilter', applied: '_appliedGroupKey' },
      { field: 'parent', label: 'Parent ID', param: 'parent_id', model: 'parentIdFilter', applied: '_appliedParentId', numeric: true },
      { field: 'job', label: 'Job ID', param: 'job_id', model: 'jobIdFilter', applied: '_appliedJobId', numeric: true, exclusive: true },
      { field: 'worker', label: 'Worker', param: 'claimed_by', model: 'claimedByFilter', applied: '_appliedClaimedBy', format: shortId },
      { field: 'kind', label: 'Kind', param: 'kind', model: 'kindFilter', applied: '_appliedKind', options: 'kindOptions' },
      { field: 'payload', label: 'Payload', param: 'payload', model: 'payloadFilter', applied: '_appliedPayload' },
      { field: 'rate', label: 'Rate limit', param: 'rate_limit_prefix', model: 'ratePrefixFilter', applied: '_appliedRatePrefix' },
      { field: 'conc', label: 'Concurrency', param: 'concurrency_prefix', model: 'concPrefixFilter', applied: '_appliedConcPrefix' },
    ],
    _onFilterJobs: null,
    childCounts: {},
    dlqChildCounts: {},
    expandedParents: {},
    _expandSeq: {},
    viewMode: 'tree',
    ...loadState(),
    active: false,
    selectedJob: null,
    sortBy: '',
    sortDir: '',
    detailError: '',
    detailErrorDetail: '',
    detailErrorId: null,
    detailGone: false,
    _detailSeq: 0,
    notVisibleFormat: localStorage.getItem('arb.notVisibleFormat') || 'countdown',

    fmtAge: formatDurationSecs,

    // The sibling jobs the same worker holds: this queue's list, filtered to its lease.
    workerJobsUrl(workerId) {
      return queueJobsUrl(Alpine.store('app').selectedQueue, { claimed_by: workerId });
    },

    goToWorkerJobs(e, workerId) {
      if (!plainNavClick(e)) return;
      this.closeDetail();
      this.setOnlyFilter('worker', workerId);
    },

    toggleNotVisibleFormat() {
      this.notVisibleFormat = this.notVisibleFormat === 'countdown' ? 'absolute' : 'countdown';
      localStorage.setItem('arb.notVisibleFormat', this.notVisibleFormat);
    },

    formatNotVisible(iso) {
      if (!iso) return EMPTY;
      return this.notVisibleFormat === 'countdown' ? formatCountdown(iso) : formatTime(iso);
    },

    // A filter that can match a child renders flat: a matching child has no visible
    // parent to nest under. The parent filter is the exception, since it is asking
    // for one parent's children in the first place.
    get flatOnly() {
      return !!(this.stateFilter || this._appliedClaimedBy || this._appliedPayload
        || this._appliedRatePrefix || this._appliedConcPrefix);
    },

    get effectiveViewMode() {
      return this.flatOnly ? 'flat' : this.viewMode;
    },

    get displayJobs() {
      const result = [];
      const treeHideChildren = this.effectiveViewMode === 'tree';
      const rootKeys = new Set(this.jobs.map((j) => String(j.primaryKey)));
      // Rows an open expansion already renders. A truncated expansion lists only what
      // it shows, so a child past the cut still needs its own row.
      const nestedKeys = new Set();
      for (const expansion of Object.values(this.expandedParents)) {
        for (const child of expansion?.jobs || []) nestedKeys.add(String(child.primaryKey));
      }
      const flatten = (jobs, depth, parentCounts) => {
        for (const job of jobs) {
          if (depth === 0 && !this._appliedParentId && job.parentId) {
            // In tree view a child is reached by expanding its parent.
            const reachable = treeHideChildren && rootKeys.has(String(job.parentId));
            if (reachable || nestedKeys.has(String(job.primaryKey))) continue;
          }
          const key = job.primaryKey;
          const cc = parentCounts?.childCounts || this.childCounts;
          const dc = parentCounts?.dlqChildCounts || this.dlqChildCounts;
          const childCount = cc[key] || 0;
          result.push(Object.assign({}, job, {
            _depth: depth,
            _childCount: childCount,
            _dlqChildCount: dc[key] || 0,
          }));
          const expanded = this.expandedParents[key];
          if (expanded && expanded.jobs) {
            flatten(expanded.jobs, depth + 1, expanded);
            // Truncation is measured against the expansion's own filtered
            // total (jobsTotal), not the unfiltered child count, so an active
            // filter doesn't produce misleading "X of Y" labels.
            const expandedTotal = expanded.total ?? expanded.jobs.length;
            if (expanded.jobs.length > 0 && expandedTotal > expanded.jobs.length) {
              result.push({
                _isMoreRow: true,
                _depth: depth + 1,
                _parentKey: key,
                _shown: expanded.jobs.length,
                _total: expandedTotal,
                primaryKey: '__more_' + key,
              });
            }
          }
        }
      };
      flatten(this.jobs, 0, null);
      return result;
    },

    // The open job shaped like a table row, so the shared actions menu binds the
    // same way it does in the table. Empty when nothing is open.
    get detailRows() {
      if (!this.selectedJob) return [];
      const k = this.selectedJob.primaryKey;
      return [Object.assign({}, this.selectedJob, {
        _id: k,
        _childCount: this.childCounts[k] || 0,
        _dlqChildCount: this.dlqChildCounts[k] || 0,
        _inDrawer: true,
      })];
    },

    detailActionsHtml: JOB_ACTIONS_HTML,

    async refreshOpenDetail() {
      if (this._drawerOpen() && this.selectedJob) await this.viewDetail(this.selectedJob.primaryKey);
    },

    // Real job rows only: the "showing N of M children" markers are not selectable.
    get selectableJobs() {
      return this.displayJobs.filter((j) => !j._isMoreRow);
    },

    // Columns no row on the page fills.
    _refreshAutoEmpty(jobs) {
      if (jobs.length === 0) return;
      this.setAutoEmpty({
        kind: jobs.every((j) => !j.kind),
        group: jobs.every((j) => !j.groupKey),
        parent: jobs.every((j) => !j.parentId),
        children: jobs.every((j) => !this.childCounts[j.primaryKey] && !this.dlqChildCounts[j.primaryKey]),
        priority: jobs.every((j) => !j.priority),
        attempts: jobs.every((j) => !j.attempts),
        visible: jobs.every((j) => !j.notVisibleUntil),
        gates: jobs.every((j) => !j.rateLimit && !j.concurrency),
      });
    },

    isExpanded(id) {
      return !!this.expandedParents[id];
    },

    async toggleChildren(id) {
      const seq = (this._expandSeq[id] || 0) + 1;
      this._expandSeq[id] = seq;
      if (this.expandedParents[id]) {
        const copy = { ...this.expandedParents };
        delete copy[id];
        this.expandedParents = copy;
        return;
      }
      const queue = Alpine.store('app').selectedQueue;
      try {
        const data = await ArbiterAPI.listJobs(queue, {
          parentId: id, limit: ARB_TIMING.childPageLimit,
          sortBy: this.sortBy || undefined,
          sortDir: this.sortDir || undefined,
        });
        if (this._expandSeq[id] !== seq) return;
        this.expandedParents[id] = {
          jobs: data.jobs || [],
          total: data.jobsTotal || 0,
          childCounts: data.childCounts || {},
          dlqChildCounts: data.dlqChildCounts || {},
        };
      } catch (e) {
        if (this._expandSeq[id] !== seq) return;
        showToast('Failed to load children: ' + e.message);
      }
    },

    canSuspend(job) {
      return job._childCount > 0 ? true : job.status !== 'suspended';
    },
    canResume(job) {
      return job._childCount > 0 ? true : job.status === 'suspended';
    },

    async pauseAction(job) {
      await this.withBusyRow(job.primaryKey, async () => {
        const queue = Alpine.store('app').selectedQueue;
        try {
          if (job._childCount > 0) {
            await ArbiterAPI.pauseChildren(queue, job.primaryKey);
          } else {
            await ArbiterAPI.suspendJob(queue, job.primaryKey);
          }
          await this.loadJobs();
          await this.refreshOpenDetail();
        } catch (e) {
          showToast('Failed: ' + e.message);
        }
      });
    },

    async resumeAction(job) {
      await this.withBusyRow(job.primaryKey, async () => {
        const queue = Alpine.store('app').selectedQueue;
        try {
          if (job._childCount > 0) {
            await ArbiterAPI.resumeChildren(queue, job.primaryKey);
          } else {
            await ArbiterAPI.resumeJob(queue, job.primaryKey);
          }
          await this.loadJobs();
          await this.refreshOpenDetail();
        } catch (e) {
          showToast('Failed: ' + e.message);
        }
      });
    },

    // Insert form
    insertPayload: '',
    insertGroupKey: '',
    insertDedupKey: '',
    insertDedupStrategy: 'ignore',
    insertPriority: 0,
    insertNotVisibleUntil: '',
    insertMaxAttempts: '',
    insertError: '',
    inserting: false,

    get insertPayloadInvalid() {
      const raw = this.insertPayload.trim();
      if (!raw || !/^[\[{]/.test(raw)) return false;
      try { JSON.parse(raw); return false; } catch { return true; }
    },

    init() {
      this._loadColPrefs();
      this.readUrlFilters('jobs');
      this.loadKinds();
      trackTabActive(this, '#tab-jobs', {
        onShow: () => { this.loadJobs(); this._startTimer(); },
        onHide: () => {
          this._loadSeq = (this._loadSeq || 0) + 1;
          this._detailSeq = (this._detailSeq || 0) + 1;
          releaseInitialLoad(this);
          this.closeDetail();
          this._stopTimer();
        },
      });
      this._bindTableEvents({
        hashName: 'jobs',
        onQueueReset: () => { this.stateFilter = ''; this.selected = {}; this.resetAutoEmpty(); this.loadKinds(); },
        relevant: (events) => {
          const queue = Alpine.store('app').selectedQueue;
          // Inserts land as ready/scheduled (or suspended for rollup parents), but
          // never in_flight/backoff/throttled/cancelled, which require a prior claim.
          const insertsRelevant = !['in_flight', 'backoff', 'throttled', 'cancelled'].includes(this.stateFilter);
          const relevantTypes = insertsRelevant
            ? ['job_inserted', 'job_updated', 'job_deleted']
            : ['job_updated', 'job_deleted'];
          return events.filter(evt =>
            evt.table === queue && relevantTypes.includes(evt.event)
          ).length;
        },
      });
      // A stats card (same detail view) asks to show a filtered job list.
      this._onFilterJobs = (e) => this.showStatus(e.detail || '');
      window.addEventListener(ARB_EVENTS.filterJobs, this._onFilterJobs);
    },

    destroy() {
      untrackTabActive(this);
      this._unbindTableEvents();
      window.removeEventListener(ARB_EVENTS.filterJobs, this._onFilterJobs);
      this._stopTimer();
    },

    // Switch to the Jobs tab showing only `status` (clears other filters).
    // Exactly one of the two branches loads, so this stays a single fetch.
    showStatus(status) {
      this.disarm();
      this.stateFilter = status;
      this.groupKeyFilter = '';
      this._appliedGroupKey = '';
      this.parentIdFilter = '';
      this._appliedParentId = '';
      this.jobIdFilter = '';
      this._appliedJobId = '';
      this.offset = 0;
      this.expandedParents = {};
      this._expandSeq = {};
      // Branch on the tab's own class, the same fact Bootstrap checks before
      // deciding whether to fire shown.bs.tab. Reading the cached `active`
      // instead can leave a showing tab with no load: show() no-ops and onShow
      // never runs.
      const btn = document.querySelector('[data-bs-target="#tab-jobs"]');
      if (!btn || btn.classList.contains('active')) {
        this._resetView();
      } else {
        bootstrap.Tab.getOrCreateInstance(btn).show();
      }
    },

    async loadJobs(filterOverrides) {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      const gk = this.filterValue('group', filterOverrides);
      const pid = this.filterValue('parent', filterOverrides);
      const jid = this.filterValue('job', filterOverrides);
      const worker = this.filterValue('worker', filterOverrides);
      const kind = this.filterValue('kind', filterOverrides);
      const payload = this.filterValue('payload', filterOverrides);
      const rate = this.filterValue('rate', filterOverrides);
      const conc = this.filterValue('conc', filterOverrides);
      const startingPending = this.pendingChanges;
      await guardedLoad(this, 'Failed to load jobs', async (seq, isStale) => {
        // Any filter that can match a child renders flat, so a match is never hidden
        // behind a parent the filter itself excluded.
        const narrowed = !!(pid || gk || jid || worker || kind || payload || rate || conc);
        const rootsOnly = !this.stateFilter && this.viewMode === 'tree' && !narrowed;
        const data = await ArbiterAPI.listJobs(queue, {
          limit: this.limit,
          offset: this.offset,
          groupKey: gk || undefined,
          parentId: pid || undefined,
          jobId: jid || undefined,
          claimedBy: worker || undefined,
          kind: kind || undefined,
          payload: payload || undefined,
          ratePrefix: rate || undefined,
          concPrefix: conc || undefined,
          status: this.stateFilter || undefined,
          rootsOnly,
          sortBy: this.sortBy || undefined,
          sortDir: this.sortDir || undefined,
        });
        if (isStale()) return;
        const jobs = data.jobs || [];
        this._appliedGroupKey = gk;
        this._appliedParentId = pid;
        this._appliedJobId = jid;
        this._appliedClaimedBy = worker;
        this._appliedKind = kind;
        this._appliedPayload = payload;
        this._appliedRatePrefix = rate;
        this._appliedConcPrefix = conc;
        this.jobs = jobs;
        this.total = data.jobsTotal || 0;
        this.childCounts = data.childCounts || {};
        this.dlqChildCounts = data.dlqChildCounts || {};
        this._refreshAutoEmpty(jobs);
        this.resyncDetailSelection();
        this.pendingChanges = Math.max(0, this.pendingChanges - startingPending);
        this._syncFiltersToUrl();
        // Drop selections for rows this page no longer carries. Expanded children
        // are selectable too, so this spans every rendered row, not just the roots.
        const present = new Set(this.selectableJobs.map((j) => String(j.primaryKey)));
        const keptSel = {};
        for (const id of Object.keys(this.selected)) {
          if (present.has(id)) keptSel[id] = true;
        }
        this.selected = keptSel;

        if (this.offset > 0 && this.offset >= this.total && this.total > 0) {
          this.offset = Math.max(0, (Math.ceil(this.total / this.limit) - 1) * this.limit);
          this.loadJobs();
          return;
        }

        const reachable = new Set();
        const collect = (rows) => {
          for (const j of rows) {
            const k = String(j.primaryKey);
            reachable.add(k);
            const exp = this.expandedParents[j.primaryKey];
            if (exp?.jobs) collect(exp.jobs);
          }
        };
        collect(jobs);
        const kept = {};
        for (const [eid, eData] of Object.entries(this.expandedParents)) {
          if (reachable.has(String(eid))) kept[eid] = eData;
        }
        this.expandedParents = kept;

        const expandedIds = Object.keys(this.expandedParents);
        if (expandedIds.length > 0) {
          await mapLimit(expandedIds, ARB_TIMING.bulkConcurrency, async (id) => {
            try {
              const d = await ArbiterAPI.listJobs(queue, {
                parentId: id, limit: ARB_TIMING.childPageLimit,
                sortBy: this.sortBy || undefined,
                sortDir: this.sortDir || undefined,
              });
              // Drop a stale response, and only update if still expanded.
              if (seq !== this._loadSeq || !this.expandedParents[id]) return;
              this.expandedParents[id] = {
                jobs: d.jobs || [],
                total: d.jobsTotal || 0,
                childCounts: d.childCounts || {},
                dlqChildCounts: d.dlqChildCounts || {},
              };
            } catch (_) {
              // If parent no longer exists, collapse it
              if (seq !== this._loadSeq) return;
              const c = { ...this.expandedParents };
              delete c[id];
              this.expandedParents = c;
            }
          });
        }
      });
    },

    // State dropdown -> server-side status filter (bound via x-model to stateFilter).
    applyState() {
      this._resetView();
    },

    toggleViewMode() {
      this.viewMode = this.viewMode === 'tree' ? 'flat' : 'tree';
      this._resetView();
    },

    toggleSort(col) {
      this._cycleSort(col);
      // Sort changes don't invalidate which parents are expanded -- just
      // re-fetch top-level + each expansion under the new sort. Reset offset
      // so the user lands on page 1 of the new ordering.
      this.offset = 0;
      this.loadJobs();
      this._startTimer();
    },

    _resetView(filterOverrides) {
      this.offset = 0;
      this.expandedParents = {};
      this._expandSeq = {};
      this.loadJobs(filterOverrides);
      this._startTimer();
    },

    async cancelJob(id, el) {
      if (this.busyRows[id]) return;
      if (!this.confirmArmed('cancel:' + id)) return;
      closeDropdown(el);
      await this.withBusyRow(id, async () => {
        const queue = Alpine.store('app').selectedQueue;
        try {
          await ArbiterAPI.cancelJob(queue, id);
          this.closeDetailIfOpen(id);
          if (String(id) === this._appliedParentId) {
            this.parentIdFilter = '';
            this._resetView({ parent: '' });
          } else {
            await this.loadJobs();
          }
        } catch (e) {
          showToast('Failed to cancel: ' + e.message);
        }
      });
    },

    async forceCancelJob(id, el) {
      if (this.busyRows[id]) return;
      if (!this.confirmArmed('fcancel:' + id)) return;
      closeDropdown(el);
      await this.withBusyRow(id, async () => {
        const queue = Alpine.store('app').selectedQueue;
        try {
          await ArbiterAPI.forceCancelJob(queue, id);
          this.closeDetailIfOpen(id);
          if (String(id) === this._appliedParentId) {
            this.parentIdFilter = '';
            this._resetView({ parent: '' });
          } else {
            await this.loadJobs();
          }
        } catch (e) {
          showToast('Failed to force-cancel: ' + e.message);
        }
      });
    },

    async promoteJob(id) {
      await this.withBusyRow(id, async () => {
        const queue = Alpine.store('app').selectedQueue;
        try {
          await ArbiterAPI.promoteJob(queue, id);
          await this.loadJobs();
          await this.refreshOpenDetail();
        } catch (e) {
          showToast('Failed to promote: ' + e.message);
        }
      });
    },

    async moveToDLQ(id, el) {
      if (this.busyRows[id]) return;
      if (!this.confirmArmed('movedlq:' + id)) return;
      closeDropdown(el);
      await this.withBusyRow(id, async () => {
        const queue = Alpine.store('app').selectedQueue;
        try {
          await ArbiterAPI.moveToDLQ(queue, id);
          this.closeDetailIfOpen(id);
          await this.loadJobs();
        } catch (e) {
          showToast('Failed to move to DLQ: ' + e.message);
        }
      });
    },

    // Bulk actions over the checkbox selection. No batch endpoint exists for
    // these, so each id goes individually with the shared concurrency cap.
    async _bulkOver(key, verb, apiCall) {
      const ids = this.selectedIds;
      if (ids.length === 0 || this.bulkBusy) return;
      if (!this.confirmArmed(key)) return;
      const queue = Alpine.store('app').selectedQueue;
      this.bulkBusy = true;
      try {
        const results = await mapLimit(ids, ARB_TIMING.bulkConcurrency, (id) => apiCall(queue, id));
        // Bail if the queue switched mid-op. _onQueueChanged already cleared selection.
        if (Alpine.store('app').selectedQueue !== queue) return;
        const failed = ids.filter((id, i) => results[i].status === 'rejected');
        const next = {};
        for (const id of failed) next[id] = true;
        this.selected = next;
        await this.loadJobs();
        await this.refreshOpenDetail();
        if (failed.length > 0) showToast(`${failed.length} of ${ids.length} ${verb} failed`);
        else showToast(`${verb} ${ids.length} ${ids.length === 1 ? 'job' : 'jobs'}`, 'success');
      } finally {
        this.bulkBusy = false;
      }
    },

    bulkCancel() {
      return this._bulkOver('bulkCancel', 'Cancelled', (q, id) => ArbiterAPI.cancelJob(q, id));
    },

    bulkPromote() {
      return this._bulkOver('bulkPromote', 'Promoted', (q, id) => ArbiterAPI.promoteJob(q, id));
    },

    bulkMoveToDLQ() {
      return this._bulkOver('bulkDlq', 'Moved', (q, id) => ArbiterAPI.moveToDLQ(q, id));
    },

    _drawerOpen() {
      return !!document.getElementById('jobDetailDrawer')?.classList.contains('show');
    },

    // A failure while the drawer is open becomes an in-drawer message rather than
    // an open drawer emptied of its job. Its neighbours are pinned first, so a
    // job that vanished mid-browse does not strand the reader.
    _detailFailed(id, headline, detail, { gone = false } = {}) {
      if (!this._drawerOpen()) {
        this._clearDetailError();
        showToast(detail ? headline + ' ' + detail : headline);
        return;
      }
      this.captureDetailNeighbours(id);
      this.selectedJob = null;
      this.detailErrorId = id;
      this.detailError = headline;
      this.detailErrorDetail = detail || '';
      this.detailGone = gone;
    },

    _clearDetailError() {
      this.detailError = '';
      this.detailErrorDetail = '';
      this.detailErrorId = null;
      this.detailGone = false;
      this.clearDetailNeighbours();
    },

    async viewDetail(id) {
      const queue = Alpine.store('app').selectedQueue;
      this._detailSeq = (this._detailSeq || 0) + 1;
      const seq = this._detailSeq;
      try {
        const data = await ArbiterAPI.getJob(queue, id);
        if (seq !== this._detailSeq) return;
        if (!data || !data.job) {
          this._detailGone(id);
          return;
        }
        this.selectedJob = data.job;
        this._clearDetailError();
        showDrawer('jobDetailDrawer');
      } catch (e) {
        if (seq !== this._detailSeq) return;
        if (e.status === 404) this._detailGone(id);
        else this._detailFailed(id, 'Could not load this job.', e.message);
      }
    },

    // The job is not coming back, so the panel says so and offers no retry.
    _detailGone(id) {
      this._detailFailed(id, 'This job is no longer in the queue.',
        'It may have completed, been archived, or moved to the DLQ.', { gone: true });
    },

    get detailTitle() {
      return this.selectedJob ? 'Job ' + this.selectedJob.primaryKey
        : (this.detailErrorId != null ? 'Job ' + this.detailErrorId : 'Job');
    },

    get detailStatus() {
      return this.selectedJob?.status || (this.detailGone ? 'gone' : '');
    },

    retryDetail() {
      if (this.detailErrorId != null) this.viewDetail(this.detailErrorId);
    },

    // Overrides the mixin: a "showing N of M children" marker is not a job.
    rowClick(e, job) {
      if (job._isMoreRow || !rowDetailClick(e)) return;
      this.viewDetail(job.primaryKey);
    },

    // Insert opens as a modal, so the form starts clean each time it is asked for.
    openInsert() {
      this.insertError = '';
      showModal('insertJobModal');
    },

    async submitInsert() {
      if (this.inserting) return;
      const queue = Alpine.store('app').selectedQueue;
      this.insertError = '';

      const raw = this.insertPayload.trim();
      if (!raw) {
        this.insertError = 'Payload is required.';
        return;
      }

      let payload = raw;
      if (/^[\[{]/.test(raw)) {
        try {
          payload = JSON.parse(raw);
        } catch (e) {
          this.insertError = 'Invalid JSON: ' + e.message;
          return;
        }
      }

      // datetime-local is wall-clock in the browser's zone. Send it as UTC ISO.
      let notVisibleUntil = null;
      if (this.insertNotVisibleUntil) {
        const d = new Date(this.insertNotVisibleUntil);
        if (isNaN(d.getTime())) {
          this.insertError = 'Invalid "Scheduled for" time.';
          return;
        }
        notVisibleUntil = d.toISOString();
      }

      const priority = parseOptionalInt(this.insertPriority);
      if (priority.error) {
        this.insertError = 'Priority must be a whole number.';
        return;
      }

      const maxAttempts = parseOptionalInt(this.insertMaxAttempts, 1);
      if (maxAttempts.error) {
        this.insertError = 'Max attempts must be a whole number of at least 1.';
        return;
      }

      this.inserting = true;
      try {
        const body = { payload };
        if (this.insertGroupKey) body.groupKey = this.insertGroupKey;
        if (this.insertDedupKey) body.dedupKey = {key: this.insertDedupKey, strategy: this.insertDedupStrategy};
        if (priority.value != null) body.priority = priority.value;
        if (notVisibleUntil) body.notVisibleUntil = notVisibleUntil;
        if (maxAttempts.value != null) body.maxAttempts = maxAttempts.value;

        await ArbiterAPI.insertJob(queue, body);

        this.insertPayload = '';
        this.insertGroupKey = '';
        this.insertDedupKey = '';
        this.insertDedupStrategy = 'ignore';
        this.insertPriority = 0;
        this.insertNotVisibleUntil = '';
        this.insertMaxAttempts = '';
        hideModal('insertJobModal');
        this.loadJobs();
        showToast('Job inserted', 'success');
      } catch (e) {
        this.insertError = e.message;
      } finally {
        this.inserting = false;
      }
    },
  }, 'selectableJobs', 'primaryKey'), 'loadJobs', 'arb.jobsPageSize'));
});
