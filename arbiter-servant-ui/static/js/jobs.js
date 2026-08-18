/**
 * Alpine component: job table + pagination + actions + insert form + detail modal
 */
// Ordered column registry. Order must match the table header and cell order.
// weight is a relative share, renormalized over the visible columns to fill 100%.
const JOB_COLUMNS = [
  { key: 'id', label: 'ID', weight: 4, required: true },
  { key: 'payload', label: 'Payload', weight: 12 },
  { key: 'group', label: 'Group', weight: 7 },
  { key: 'parent', label: 'Parent', weight: 7 },
  { key: 'children', label: 'Children', weight: 9 },
  { key: 'priority', label: 'Priority', weight: 8 },
  { key: 'attempts', label: 'Attempts', weight: 8 },
  { key: 'status', label: 'Status', weight: 7 },
  { key: 'inserted', label: 'Inserted At', weight: 10 },
  { key: 'visible', label: 'Visible', weight: 12 },
  { key: 'ratelimit', label: 'Rate Limit', weight: 9 },
  { key: 'concurrency', label: 'Concurrency', weight: 10 },
  { key: 'actions', label: 'Actions', weight: 12 },
];

document.addEventListener('alpine:init', () => {
  Alpine.data('jobsTab', () => withPagination({
    ...columnPrefs(JOB_COLUMNS, 'arb.jobCols'),
    ...tableTab('loadJobs', 'arb.jobsRefresh'),
    jobs: [],
    total: 0,
    groupKeyFilter: '',
    parentIdFilter: '',
    jobIdFilter: '',
    stateFilter: '',
    _appliedGroupKey: '',
    _appliedParentId: '',
    _appliedJobId: '',
    _onFilterJobs: null,
    childCounts: {},
    dlqChildCounts: {},
    expandedParents: {},
    viewMode: 'tree',
    loading: false,
    active: false,
    selectedJob: null,
    sortBy: '',
    sortDir: '',
    _loadErrored: false,
    notVisibleFormat: localStorage.getItem('arb.notVisibleFormat') || 'countdown',

    toggleNotVisibleFormat() {
      this.notVisibleFormat = this.notVisibleFormat === 'countdown' ? 'absolute' : 'countdown';
      localStorage.setItem('arb.notVisibleFormat', this.notVisibleFormat);
    },

    formatNotVisible(iso) {
      if (!iso) return '-';
      return this.notVisibleFormat === 'countdown' ? formatCountdown(iso) : formatTime(iso);
    },

    get effectiveViewMode() {
      // A status filter spans the tree, so render flat (a matching child has no
      // visible parent to nest under).
      return this.stateFilter ? 'flat' : this.viewMode;
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

    isExpanded(id) {
      return !!this.expandedParents[id];
    },

    async toggleChildren(id) {
      this._expandSeq = this._expandSeq || {};
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
        } catch (e) {
          showToast('Failed: ' + e.message);
        }
      });
    },

    // Insert form
    showInsertForm: false,
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

    _syncFiltersToUrl() {
      writeFiltersToUrl({
        groupKey: this._appliedGroupKey,
        parentId: this._appliedParentId,
        jobId: this._appliedJobId,
        status: this.stateFilter,
        sortBy: this.sortBy,
        sortDir: this.sortDir,
      });
    },

    init() {
      this._loadColPrefs();
      const f = readFiltersFromUrl();
      if (location.hash.replace('#', '') === 'jobs') {
        this.groupKeyFilter = f.groupKey;
        this._appliedGroupKey = f.groupKey;
        this.parentIdFilter = f.parentId;
        this._appliedParentId = f.parentId;
        this.jobIdFilter = f.jobId;
        this._appliedJobId = f.jobId;
        this.stateFilter = f.status;
        this.sortBy = f.sortBy;
        this.sortDir = f.sortDir;
      }
      trackTabActive(this, '#tab-jobs', {
        onShow: () => { this.loadJobs(); this._startTimer(); },
        onHide: () => {
          this._loadSeq = (this._loadSeq || 0) + 1;
          this._detailSeq = (this._detailSeq || 0) + 1;
          releaseInitialLoad(this);
          hideModal('jobDetailModal');
          this._stopTimer();
        },
      });
      this._bindTableEvents({
        onQueueReset: () => { this.stateFilter = ''; },
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

    // Switch to the Jobs tab showing only `status` (clears other filters). The
    // tab's onShow handler does the load, so this stays a single fetch.
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
      if (this.active) {
        // Already on Jobs (no tab switch to trigger onShow), so reload directly.
        this._resetView();
      } else {
        const btn = document.querySelector('[data-bs-target="#tab-jobs"]');
        if (btn) bootstrap.Tab.getOrCreateInstance(btn).show();
      }
    },

    async loadJobs(filterOverrides) {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      const gk = filterOverrides?.groupKey ?? this._appliedGroupKey;
      const pid = filterOverrides?.parentId ?? this._appliedParentId;
      const jid = filterOverrides?.jobId ?? this._appliedJobId;
      const startingPending = this.pendingChanges;
      await guardedLoad(this, 'Failed to load jobs', async (seq, isStale) => {
        const rootsOnly = !this.stateFilter && this.viewMode === 'tree' && !pid && !gk && !jid;
        const data = await ArbiterAPI.listJobs(queue, {
          limit: this.limit,
          offset: this.offset,
          groupKey: gk || undefined,
          parentId: pid || undefined,
          jobId: jid || undefined,
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
        this.jobs = jobs;
        this.total = data.jobsTotal || 0;
        this.childCounts = data.childCounts || {};
        this.dlqChildCounts = data.dlqChildCounts || {};
        this.pendingChanges = Math.max(0, this.pendingChanges - startingPending);
        this._syncFiltersToUrl();

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
          if (String(id) === this._appliedParentId) {
            this.parentIdFilter = '';
            this._resetView({ groupKey: this._appliedGroupKey, parentId: '' });
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
          if (String(id) === this._appliedParentId) {
            this.parentIdFilter = '';
            this._resetView({ groupKey: this._appliedGroupKey, parentId: '' });
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
          await this.loadJobs();
        } catch (e) {
          showToast('Failed to move to DLQ: ' + e.message);
        }
      });
    },

    async viewDetail(id) {
      const queue = Alpine.store('app').selectedQueue;
      this._detailSeq = (this._detailSeq || 0) + 1;
      const seq = this._detailSeq;
      try {
        const data = await ArbiterAPI.getJob(queue, id);
        if (seq !== this._detailSeq) return;
        if (!data || !data.job) { showToast('Job not found'); return; }
        this.selectedJob = data.job;
        showModal('jobDetailModal');
      } catch (e) {
        if (seq !== this._detailSeq) return;
        this.selectedJob = null;
        showToast('Failed to load job: ' + e.message);
      }
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
        this.loadJobs();
        showToast('Job inserted', 'success');
      } catch (e) {
        this.insertError = e.message;
      } finally {
        this.inserting = false;
      }
    },
  }, 'loadJobs'));
});
