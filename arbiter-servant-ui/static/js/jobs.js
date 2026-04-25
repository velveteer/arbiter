/**
 * Alpine component: job table + pagination + actions + insert form + detail modal
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('jobsTab', () => withPagination({
    jobs: [],
    total: 0,
    groupKeyFilter: '',
    parentIdFilter: '',
    suspendedFilter: '',
    _appliedGroupKey: '',
    _appliedParentId: '',
    childCounts: {},
    dlqChildCounts: {},
    pausedParents: [],
    expandedParents: {},
    viewMode: 'tree',
    showInFlight: false,
    loading: false,
    active: false,
    selectedJob: null,
    refreshMode: '5s',
    _refreshTimer: null,
    pendingChanges: 0,

    get _topLevelHiddenCount() {
      if (this.viewMode !== 'tree' || this._appliedParentId) return 0;
      return this.jobs.filter(j => j.parentId).length;
    },

    get displayJobs() {
      const result = [];
      const flatten = (jobs, depth, parentCounts) => {
        for (const job of jobs) {
          // In tree mode, hide children at the top level — they show via expand
          if (depth === 0 && this.viewMode === 'tree' && job.parentId && !this._appliedParentId) {
            continue;
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
            if (childCount > expanded.jobs.length) {
              result.push({
                _isMoreRow: true,
                _depth: depth + 1,
                _parentKey: key,
                _shown: expanded.jobs.length,
                _total: childCount,
                primaryKey: '__more_' + key,
              });
            }
          }
        }
      };
      flatten(this.jobs, 0, null);
      return result;
    },

    get hasUnappliedFilters() {
      return this.groupKeyFilter !== this._appliedGroupKey
          || this.parentIdFilter !== this._appliedParentId;
    },

    get displayTotal() {
      return Math.max(0, this.total - this._topLevelHiddenCount);
    },

    isExpanded(id) {
      return !!this.expandedParents[id];
    },

    async toggleChildren(id) {
      if (this.expandedParents[id]) {
        const copy = { ...this.expandedParents };
        delete copy[id];
        this.expandedParents = copy;
        return;
      }
      const queue = Alpine.store('app').selectedQueue;
      try {
        const data = await ArbiterAPI.listJobs(queue, {
          parentId: id, limit: 50,
          suspended: this.suspendedFilter !== '' ? this.suspendedFilter : undefined,
        });
        this.expandedParents[id] = {
          jobs: data.jobs || [],
          childCounts: data.childCounts || {},
          dlqChildCounts: data.dlqChildCounts || {},
          pausedParents: data.pausedParents || [],
        };
      } catch (e) {
        showToast('Failed to load children: ' + e.message);
      }
    },

    async pauseAction(job) {
      const queue = Alpine.store('app').selectedQueue;
      try {
        if (job._childCount > 0) {
          await ArbiterAPI.pauseChildren(queue, job.primaryKey);
        } else {
          await ArbiterAPI.suspendJob(queue, job.primaryKey);
        }
        this.loadJobs();
      } catch (e) {
        showToast('Failed: ' + e.message);
      }
    },

    async resumeAction(job) {
      const queue = Alpine.store('app').selectedQueue;
      try {
        if (job._childCount > 0) {
          await ArbiterAPI.resumeChildren(queue, job.primaryKey);
        } else {
          await ArbiterAPI.resumeJob(queue, job.primaryKey);
        }
        this.loadJobs();
      } catch (e) {
        showToast('Failed: ' + e.message);
      }
    },

    // Insert form
    showInsertForm: false,
    insertPayload: '',
    insertGroupKey: '',
    insertDedupKey: '',
    insertDedupStrategy: 'ignore',
    insertPriority: 0,
    insertError: '',
    inserting: false,

    _syncFiltersToUrl() {
      writeFiltersToUrl({
        groupKey: this._appliedGroupKey,
        parentId: this._appliedParentId,
        suspended: this.suspendedFilter,
        inFlight: this.showInFlight,
      });
    },

    init() {
      const f = readFiltersFromUrl();
      if (location.hash.replace('#', '') === 'jobs') {
        this.groupKeyFilter = f.groupKey;
        this._appliedGroupKey = f.groupKey;
        this.parentIdFilter = f.parentId;
        this._appliedParentId = f.parentId;
        this.suspendedFilter = f.suspended;
        this.showInFlight = f.inFlight;
      }
      trackTabActive(this, '#tab-jobs', {
        onShow: () => { this.loadJobs(); this._startTimer(); },
        onHide: () => {
          this._loadSeq = (this._loadSeq || 0) + 1;
          this._detailSeq = (this._detailSeq || 0) + 1;
          const modalEl = document.getElementById('jobDetailModal');
          if (modalEl) bootstrap.Modal.getInstance(modalEl)?.hide();
          clearFiltersFromUrl();
          this.groupKeyFilter = '';
          this.parentIdFilter = '';
          this.suspendedFilter = '';
          this.showInFlight = false;
          this._appliedGroupKey = '';
          this._appliedParentId = '';
          if (this._refreshTimer) { clearInterval(this._refreshTimer); this._refreshTimer = null; }
        },
      });
      window.addEventListener('queue-changed', () => {
        this.groupKeyFilter = '';
        this.parentIdFilter = '';
        this.suspendedFilter = '';
        this.showInFlight = false;
        this._appliedGroupKey = '';
        this._appliedParentId = '';
        if (this.active) this._resetView();
      });
      window.addEventListener('sse-reconnect', () => {
        if (this.active) this.loadJobs();
      });
      window.addEventListener('sse-event', (e) => {
        const queue = Alpine.store('app').selectedQueue;
        const relevantTypes = this.showInFlight
          ? ['job_updated', 'job_deleted']
          : ['job_inserted', 'job_updated', 'job_deleted'];
        const count = e.detail.filter(evt =>
          evt.table === queue && relevantTypes.includes(evt.event)
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
        if (this.active && !this.loading) this.loadJobs();
      }, ms);
    },

    setRefreshMode(mode) {
      this.refreshMode = mode;
      this._startTimer();
    },

    async loadJobs(filterOverrides) {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      this.loading = true;
      this._loadSeq = (this._loadSeq || 0) + 1;
      const seq = this._loadSeq;
      const gk = filterOverrides?.groupKey ?? this._appliedGroupKey;
      const pid = filterOverrides?.parentId ?? this._appliedParentId;
      const startingPending = this.pendingChanges;
      try {
        let data;
        if (this.showInFlight) {
          data = await ArbiterAPI.getInFlightJobs(queue, {
            limit: this.limit,
            offset: this.offset,
            parentId: pid || undefined,
          });
        } else {
          const rootsOnly = this.viewMode === 'tree' && !pid;
          data = await ArbiterAPI.listJobs(queue, {
            limit: this.limit,
            offset: this.offset,
            groupKey: gk || undefined,
            parentId: pid || undefined,
            suspended: this.suspendedFilter !== '' ? this.suspendedFilter : undefined,
            rootsOnly,
          });
        }
        if (seq !== this._loadSeq) return;
        const jobs = data.jobs || [];
        this._appliedGroupKey = gk;
        this._appliedParentId = pid;
        this.jobs = jobs;
        this.total = data.jobsTotal || 0;
        this.childCounts = data.childCounts || {};
        this.dlqChildCounts = data.dlqChildCounts || {};
        this.pausedParents = data.pausedParents || [];
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
          const refreshes = expandedIds.map(async (id) => {
            try {
              const d = await ArbiterAPI.listJobs(queue, {
                parentId: id, limit: 50,
                suspended: this.suspendedFilter !== '' ? this.suspendedFilter : undefined,
              });
              // Only update if still expanded (user may have collapsed during fetch)
              if (this.expandedParents[id]) {
                this.expandedParents[id] = {
                  jobs: d.jobs || [],
                  childCounts: d.childCounts || {},
                  dlqChildCounts: d.dlqChildCounts || {},
                  pausedParents: d.pausedParents || [],
                };
              }
            } catch (_) {
              // If parent no longer exists, collapse it
              const c = { ...this.expandedParents };
              delete c[id];
              this.expandedParents = c;
            }
          });
          await Promise.all(refreshes);
        }
      } catch (e) {
        if (seq !== this._loadSeq) return;
        console.error('Failed to load jobs:', e);
      } finally {
        if (seq === this._loadSeq) {
          this.loading = false;
          this.loaded = true;
        }
      }
    },

    toggleInFlight() {
      this.showInFlight = !this.showInFlight;
      this._resetView();
    },

    toggleViewMode() {
      this.viewMode = this.viewMode === 'tree' ? 'flat' : 'tree';
      this._resetView();
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

    _resetView(filterOverrides) {
      this.offset = 0;
      this.expandedParents = {};
      this.loadJobs(filterOverrides);
      this._startTimer();
    },

    async cancelJob(id, childCount, dlqChildCount) {
      const queue = Alpine.store('app').selectedQueue;
      const parts = [];
      if (childCount > 0) parts.push(`${childCount} active children`);
      if (dlqChildCount > 0) parts.push(`${dlqChildCount} DLQ entries (will be orphaned)`);
      const msg = parts.length > 0
        ? `Cancel this job and ${parts.join(' + ')}?`
        : 'Cancel this job?';
      if (!confirm(msg)) return;
      try {
        await ArbiterAPI.cancelJob(queue, id);
        if (String(id) === this._appliedParentId) {
          this.parentIdFilter = '';
          this._resetView({ groupKey: this._appliedGroupKey, parentId: '' });
        } else {
          this.loadJobs();
        }
      } catch (e) {
        showToast('Failed to cancel: ' + e.message);
      }
    },

    async promoteJob(id) {
      const queue = Alpine.store('app').selectedQueue;
      try {
        await ArbiterAPI.promoteJob(queue, id);
        this.loadJobs();
      } catch (e) {
        showToast('Failed to promote: ' + e.message);
      }
    },

    async moveToDLQ(id, childCount) {
      const queue = Alpine.store('app').selectedQueue;
      const msg = childCount > 0
        ? `Move this job and its ${childCount} children to the DLQ?`
        : 'Move this job to DLQ?';
      if (!confirm(msg)) return;
      try {
        await ArbiterAPI.moveToDLQ(queue, id);
        this.loadJobs();
      } catch (e) {
        showToast('Failed to move to DLQ: ' + e.message);
      }
    },

    async viewDetail(id) {
      const queue = Alpine.store('app').selectedQueue;
      this._detailSeq = (this._detailSeq || 0) + 1;
      const seq = this._detailSeq;
      try {
        const data = await ArbiterAPI.getJob(queue, id);
        if (seq !== this._detailSeq) return;
        this.selectedJob = data.job;
        bootstrap.Modal.getOrCreateInstance(document.getElementById('jobDetailModal')).show();
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

      this.inserting = true;
      try {
        const body = { payload };
        if (this.insertGroupKey) body.groupKey = this.insertGroupKey;
        if (this.insertDedupKey) body.dedupKey = {key: this.insertDedupKey, strategy: this.insertDedupStrategy};
        if (this.insertPriority) body.priority = parseInt(this.insertPriority, 10);

        await ArbiterAPI.insertJob(queue, body);

        this.insertPayload = '';
        this.insertGroupKey = '';
        this.insertDedupKey = '';
        this.insertDedupStrategy = 'ignore';
        this.insertPriority = 0;
        this.loadJobs();
      } catch (e) {
        this.insertError = e.message;
      } finally {
        this.inserting = false;
      }
    },
  }, 'loadJobs'));
});
