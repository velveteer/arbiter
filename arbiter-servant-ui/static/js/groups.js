/**
 * Alpine component: a queue's open groups, largest first.
 */
// Order must match the table header and cell order.
const GROUP_COLUMNS = [
  { key: 'group', label: 'Group', weight: 22, required: true },
  { key: 'jobs', label: 'Jobs', weight: 9 },
  { key: 'ready', label: 'Ready', weight: 9, narrow: false },
  { key: 'head', label: 'Head job', weight: 20 },
  { key: 'inflight', label: 'Held', weight: 20 },
  { key: 'due', label: 'Next due', weight: 20, narrow: false },
];

document.addEventListener('alpine:init', () => {
  Alpine.data('groupsTab', () => withPagination({
    ...columnPrefs(GROUP_COLUMNS, 'arb.groupCols'),
    ...tableTab('loadGroups', 'arb.groupsRefresh'),
    ...tableFilters('group'),
    loadNoun: 'groups',
    rowNoun: 'group',
    rowNounPlural: '',
    groups: [],
    total: 0,
    selected: {},
    ...loadState((s) => s.groups.length === 0),
    // Bumped every second while the tab shows, so the countdowns move.
    tick: 0,
    _tickTimer: null,

    init() {
      this._loadColPrefs();
      this.readUrlFilters('groups');
      trackTabActive(this, '#tab-groups', {
        onShow: () => { this.loadGroups(); this._startTimer(); this._startTick(); },
        onHide: () => {
          this._loadSeq = (this._loadSeq || 0) + 1;
          releaseInitialLoad(this);
          this._stopTimer();
          this._stopTick();
        },
      });
      this._bindTableEvents({
        hashName: 'groups',
        relevant: (events) => {
          const queue = Alpine.store('app').selectedQueue;
          return events.filter((evt) => evt.table === queue && !evt.dlq).length;
        },
      });
    },

    destroy() {
      untrackTabActive(this);
      this._unbindTableEvents();
      this._stopTimer();
      this._stopTick();
    },

    _startTick() {
      this._stopTick();
      this._tickTimer = setInterval(() => { this.tick++; }, ARB_TIMING.countdownTickMs);
    },

    _stopTick() {
      if (this._tickTimer) { clearInterval(this._tickTimer); this._tickTimer = null; }
    },

    // Time left until iso, read against the tick so it counts down.
    countdown(iso) {
      void this.tick;
      return formatCountdown(iso, EMPTY);
    },

    leaseLapsed(g) {
      void this.tick;
      return !!g.inFlightUntil && new Date(g.inFlightUntil) <= new Date();
    },

    async loadGroups(filterOverrides) {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      const f = this.filterValues(filterOverrides);
      const startingPending = this.pendingChanges;
      await guardedLoad(this, async (seq, isStale) => {
        const data = await ArbiterAPI.listGroups(queue, {
          limit: this.limit,
          offset: this.offset,
          groupKey: f.group || undefined,
        });
        if (isStale()) return;
        this._setAppliedFilters(f);
        this.groups = data.groups || [];
        this.total = data.total || 0;
        this.pendingChanges = Math.max(0, this.pendingChanges - startingPending);
        this._syncFiltersToUrl();
        if (this.offset > 0 && this.offset >= this.total && this.total > 0) {
          this.offset = Math.max(0, (Math.ceil(this.total / this.limit) - 1) * this.limit);
          this.loadGroups();
        }
      });
    },

    jobsUrl(filters) {
      return queueJobsUrl(Alpine.store('app').selectedQueue, filters);
    },

    openJobs(e, filters) {
      if (!plainNavClick(e)) return;
      Alpine.store('app').openQueueJobs(Alpine.store('app').selectedQueue, filters);
    },
  }, 'loadGroups', 'arb.groupsPageSize'));
});
