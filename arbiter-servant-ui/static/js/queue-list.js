/**
 * Alpine component: the queue-list landing. One bulk request fetches every
 * queue's stats; each card drills into a queue (or its filtered Jobs tab).
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('queueListTab', () => ({
    ...pollingTab('load', ARB_TIMING.queueListPollMs),
    ...eventBusTab(),
    rows: [],
    loading: false,
    loaded: false,
    _loadErrored: false,

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
      });
    },

    fmtCount: formatCompact,

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

    navTo(e, queue, status) {
      if (!plainNavClick(e)) return;
      if (status) Alpine.store('app').openQueueJobs(queue, status);
      else Alpine.store('app').openQueue(queue);
    },
  }));
});
