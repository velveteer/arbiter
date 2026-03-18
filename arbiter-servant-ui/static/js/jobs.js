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
    _hiddenChildren: 0,

    get displayJobs() {
      const result = [];
      let hiddenCount = 0;
      const flatten = (jobs, depth, parentCounts) => {
        for (const job of jobs) {
          // In tree mode, hide children at the top level — they show via expand
          if (depth === 0 && this.viewMode === 'tree' && job.parentId && !this._appliedParentId) {
            hiddenCount++;
            continue;
          }
          const key = job.primaryKey;
          const cc = parentCounts?.childCounts || this.childCounts;
          const dc = parentCounts?.dlqChildCounts || this.dlqChildCounts;
          result.push(Object.assign({}, job, {
            _depth: depth,
            _childCount: cc[key] || 0,
            _dlqChildCount: dc[key] || 0,
          }));
          const expanded = this.expandedParents[key];
          if (expanded && expanded.jobs) {
            flatten(expanded.jobs, depth + 1, expanded);
          }
        }
      };
      flatten(this.jobs, 0, null);
      this._hiddenChildren = hiddenCount;
      return result;
    },

    get hasUnappliedFilters() {
      return this.groupKeyFilter !== this._appliedGroupKey
          || this.parentIdFilter !== this._appliedParentId;
    },

    get displayTotal() {
      return Math.max(0, this.total - this._hiddenChildren);
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
          parentId: id, limit: 200,
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

    isPaused(job) {
      if (job._childCount > 0) {
        return this._isPausedParent(job.primaryKey);
      }
      return job.suspended;
    },

    _isPausedParent(id) {
      // Check top-level pausedParents and any expanded parent's pausedParents
      if (this.pausedParents.includes(id)) return true;
      for (const exp of Object.values(this.expandedParents)) {
        if (exp.pausedParents && exp.pausedParents.includes(id)) return true;
      }
      return false;
    },

    async togglePause(job) {
      const queue = Alpine.store('app').selectedQueue;
      const id = job.primaryKey;
      try {
        if (job._childCount > 0) {
          if (this._isPausedParent(id)) {
            await ArbiterAPI.resumeChildren(queue, id);
          } else {
            await ArbiterAPI.pauseChildren(queue, id);
          }
        } else {
          if (job.suspended) {
            await ArbiterAPI.resumeJob(queue, id);
          } else {
            await ArbiterAPI.suspendJob(queue, id);
          }
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
          clearFiltersFromUrl();
          if (this._refreshTimer) { clearInterval(this._refreshTimer); this._refreshTimer = null; }
        },
      });
      window.addEventListener('queue-changed', () => {
        if (this.active) {
          this.groupKeyFilter = '';
          this.parentIdFilter = '';
          this.suspendedFilter = '';
          this.showInFlight = false;
          this._appliedGroupKey = '';
          this._appliedParentId = '';
          this._resetView();
        }
      });
      window.addEventListener('sse-reconnect', () => {
        if (this.active) this.loadJobs();
      });
      window.addEventListener('sse-event', (e) => {
        const queue = Alpine.store('app').selectedQueue;
        const count = e.detail.filter(evt =>
          evt.table === queue &&
          ['job_inserted', 'job_updated', 'job_deleted'].includes(evt.event)
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
      // Snapshot filters: explicit overrides (from applyFilter) or current applied (for auto-refresh)
      const gk = filterOverrides?.groupKey ?? this._appliedGroupKey;
      const pid = filterOverrides?.parentId ?? this._appliedParentId;
      try {
        let data;
        if (this.showInFlight) {
          data = await ArbiterAPI.getInFlightJobs(queue, {
            limit: this.limit,
            offset: this.offset,
          });
        } else {
          data = await ArbiterAPI.listJobs(queue, {
            limit: this.limit,
            offset: this.offset,
            groupKey: gk || undefined,
            parentId: pid || undefined,
            suspended: this.suspendedFilter !== '' ? this.suspendedFilter : undefined,
          });
        }
        let jobs = data.jobs || [];
        // Client-side parent filter for in-flight view
        if (this.showInFlight && pid) {
          jobs = jobs.filter(j => String(j.parentId) === pid);
        }
        // Update applied state + data atomically (no flash)
        this._appliedGroupKey = gk;
        this._appliedParentId = pid;
        this.jobs = jobs;
        this.total = this.showInFlight && pid ? jobs.length : (data.jobsTotal || 0);
        this.childCounts = data.childCounts || {};
        this.dlqChildCounts = data.dlqChildCounts || {};
        this.pausedParents = data.pausedParents || [];
        this.pendingChanges = 0;
        this._syncFiltersToUrl();

        // Refresh any currently expanded parents
        const expandedIds = Object.keys(this.expandedParents);
        if (expandedIds.length > 0) {
          const refreshes = expandedIds.map(async (id) => {
            try {
              const d = await ArbiterAPI.listJobs(queue, {
                parentId: id, limit: 200,
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
        console.error('Failed to load jobs:', e);
      } finally {
        this.loading = false;
        this.loaded = true;
      }
    },

    toggleInFlight() {
      this.showInFlight = !this.showInFlight;
      this._resetView();
    },

    applyFilter() {
      this._resetView({ groupKey: this.groupKeyFilter, parentId: this.parentIdFilter });
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

    async cancelJob(id, childCount) {
      const queue = Alpine.store('app').selectedQueue;
      const msg = childCount > 0
        ? `Cancel this job and its ${childCount} children?`
        : 'Cancel this job?';
      if (!confirm(msg)) return;
      try {
        await ArbiterAPI.cancelJob(queue, id);
        this.loadJobs();
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

    async moveToDLQ(id) {
      const queue = Alpine.store('app').selectedQueue;
      if (!confirm('Move this job to DLQ?')) return;
      try {
        await ArbiterAPI.moveToDLQ(queue, id);
        this.loadJobs();
      } catch (e) {
        showToast('Failed to move to DLQ: ' + e.message);
      }
    },

    async viewDetail(id) {
      const queue = Alpine.store('app').selectedQueue;
      try {
        const data = await ArbiterAPI.getJob(queue, id);
        this.selectedJob = data.job;
        bootstrap.Modal.getOrCreateInstance(document.getElementById('jobDetailModal')).show();
      } catch (e) {
        showToast('Failed to load job: ' + e.message);
      }
    },

    async submitInsert() {
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
      } catch (e) {
        this.insertError = e.message;
      }
    },
  }, 'loadJobs'));
});
