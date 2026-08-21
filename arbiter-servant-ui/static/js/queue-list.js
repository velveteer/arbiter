/**
 * Alpine component: the queue-list landing, as cards or as a list. One bulk
 * request fetches every queue's stats; each card or row drills into a queue.
 */
// Sort keys shared by both views. An absent age sorts below any present one.
const QUEUE_SORT_KEYS = {
  queue: (r) => r.queue,
  ready: (r) => r.stats?.readyJobs ?? 0,
  inFlight: (r) => r.stats?.inFlightJobs ?? 0,
  scheduled: (r) => r.stats?.scheduledJobs ?? 0,
  backoff: (r) => r.stats?.backoffJobs ?? 0,
  throttled: (r) => r.stats?.throttledJobs ?? 0,
  dlq: (r) => r.stats?.dlqJobs ?? 0,
  oldest: (r) => r.stats?.oldestReadyAgeSeconds ?? -1,
  oldestInFlight: (r) => r.stats?.oldestInFlightAgeSeconds ?? -1,
  workers: (r) => r.workersLive ?? 0,
};

document.addEventListener('alpine:init', () => {
  Alpine.data('queueListTab', () => ({
    ...pollingTab('load', ARB_TIMING.queueListPollMs, 'arb.queuesRefresh'),
    ...eventBusTab(),
    rows: [],
    ...loadState(),
    search: '',
    viewMode: localStorage.getItem('arb.queueView') || '',
    // The only ordering that holds still while the counts under it move.
    sortBy: 'queue',
    sortDir: 'asc',

    init() {
      this.initPollingMounted();
      this._bindBus({ sseReconnect: () => this.load() });
    },

    destroy() {
      this.teardownPolling();
      this._unbindBus();
    },

    async load() {
      await guardedLoad(this, 'Failed to load queues', async (seq, isStale) => {
        const data = await ArbiterAPI.getAllStats();
        if (isStale()) return;
        this.rows = data.queues || [];
        // First visit only: a long queue list opens as a list.
        if (!this.viewMode) {
          this.viewMode = this.rows.length > ARB_TIMING.queueListThreshold ? 'list' : 'cards';
        }
      });
    },

    setViewMode(mode) {
      this.viewMode = mode;
      localStorage.setItem('arb.queueView', mode);
    },

    get displayRows() {
      const needle = this.search.trim().toLowerCase();
      const rows = needle
        ? this.rows.filter((r) => r.queue.toLowerCase().includes(needle))
        : this.rows.slice();
      const read = QUEUE_SORT_KEYS[this.sortBy] || QUEUE_SORT_KEYS.queue;
      const dir = this.sortDir === 'asc' ? 1 : -1;
      return rows.sort((a, b) => {
        const x = read(a);
        const y = read(b);
        if (x === y) return a.queue.localeCompare(b.queue);
        return (typeof x === 'string' ? x.localeCompare(y) : x - y) * dir;
      });
    },

    // Instance-wide roll-up.
    get summary() {
      return this.rows.reduce((acc, r) => {
        const s = r.stats || {};
        acc.ready += s.readyJobs || 0;
        acc.inFlight += s.inFlightJobs || 0;
        acc.throttled += s.throttledJobs || 0;
        acc.dlq += s.dlqJobs || 0;
        acc.workersLive += r.workersLive || 0;
        acc.workersPaused += r.workersPaused || 0;
        if (r.paused) acc.queuesPaused += 1;
        return acc;
      }, { ready: 0, inFlight: 0, throttled: 0, dlq: 0, workersLive: 0, workersPaused: 0, queuesPaused: 0 });
    },

    // Text reads ascending first, counts descending first.
    defaultSortDir(key) {
      return key === 'queue' ? 'asc' : 'desc';
    },

    toggleSort(key) {
      if (this.sortBy === key) {
        this.sortDir = this.sortDir === 'asc' ? 'desc' : 'asc';
      } else {
        this.sortBy = key;
        this.sortDir = this.defaultSortDir(key);
      }
    },

    sortIndicator(key) {
      if (this.sortBy !== key) return '↕';
      return this.sortDir === 'asc' ? '▲' : '▼';
    },

    fmtCount: formatCompact,
    fmtAge: formatDurationSecs,

    zeroClass(n) {
      return n ? '' : 'is-zero';
    },

    dlqCount(r) {
      return r.stats?.dlqJobs || 0;
    },

    // Pause status shown on a card. A paused queue (holds all work) takes precedence
    // over worker pause; among live workers, all-paused reads stronger than some.
    // Returns null when nothing is paused.
    pauseState(r) {
      if (r.paused) return { key: 'queue', label: 'paused' };
      const live = r.workersLive || 0;
      const paused = r.workersPaused || 0;
      if (paused > 0 && paused >= live) return { key: 'workers', label: 'workers paused' };
      if (paused > 0) return { key: 'some', label: `${paused}/${live} paused` };
      return null;
    },

    // Card link href: the queue overview, or its Jobs tab filtered by status.
    queueUrl(queue, status) {
      return status ? queueJobsUrl(queue, status) : '?' + new URLSearchParams({ queue }).toString();
    },

    dlqUrl(queue) {
      return '?' + new URLSearchParams({ queue }).toString() + '#dlq';
    },

    navTo(e, queue, status) {
      if (!plainNavClick(e)) return;
      if (status) Alpine.store('app').openQueueJobs(queue, status);
      else Alpine.store('app').openQueue(queue);
    },

    // The whole row drills in. The cells that carry their own link keep it.
    rowClick(e, r) {
      if (rowDetailClick(e)) Alpine.store('app').openQueueTab(r.queue, 'jobs');
    },

    navToJobs(e, queue) {
      if (!plainNavClick(e)) return;
      Alpine.store('app').openQueueTab(queue, 'jobs');
    },

    navToDLQ(e, queue) {
      if (!plainNavClick(e)) return;
      Alpine.store('app').openQueueTab(queue, 'dlq');
    },
  }));
});
