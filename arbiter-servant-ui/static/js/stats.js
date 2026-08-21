/**
 * Alpine component: the per-queue stat cards.
 *
 * The stats query aggregates over the whole queue table, so the refresh interval
 * is the only thing that schedules it, as it is for the job tables. A queue's
 * event stream does not reload it: at a busy queue's event rate that outpaced any
 * interval the reader picked.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('statsTab', () => ({
    ...eventBusTab(),
    ...tabActive(),
    ...pollSpinner(),
    ...refreshControl('loadStats', 'arb.statsRefresh', '30s'),
    stats: null,
    ...loadState(),
    active: false,

    init() {
      this._watchPolling();
      trackTabActive(this, '#tab-stats', {
        onShow: () => { this.loadStats(); this._startTimer(); },
        onHide: () => this._stopTimer(),
      });
      this._bindBus({
        queueChanged: () => { this.stats = null; if (this.active) this.loadStats(); },
        sseReconnect: () => { if (this.active) this.loadStats(); },
      });
    },

    destroy() {
      untrackTabActive(this);
      this._stopTimer();
      this._stopWatchPolling();
      this._unbindBus();
      releaseInitialLoad(this);
    },

    // Card link href: this queue's Jobs tab filtered by status (empty = all).
    jobsUrl(status) {
      return queueJobsUrl(Alpine.store('app').selectedQueue, status);
    },

    goToJobs(e, status) {
      if (!plainNavClick(e)) return;
      window.dispatchEvent(new CustomEvent(ARB_EVENTS.filterJobs, { detail: status }));
    },

    goToDLQ() {
      const btn = document.querySelector('[data-bs-target="#tab-dlq"]');
      if (btn) bootstrap.Tab.getOrCreateInstance(btn).show();
    },

    fmtAge: formatDurationSecs,

    zeroClass(n) {
      return n ? '' : 'is-zero';
    },

    async loadStats() {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      await guardedLoad(this, 'Failed to load stats', async (seq, isStale) => {
        const data = await ArbiterAPI.getStats(queue);
        if (isStale()) return;
        this.stats = data.stats;
      }, {
        // Suppress a stats toast for a queue we've already navigated away from
        // mid-fetch: the in-flight load for the old queue is no longer relevant.
        suppressToast: () => Alpine.store('app').selectedQueue !== queue,
      });
    },
  }));
});
