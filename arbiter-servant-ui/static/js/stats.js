/**
 * Alpine component: stat cards (total/ready/in-flight/scheduled/backoff/suspended/oldest-ready)
 *
 * Refreshes on SSE events matching the selected queue instead of polling.
 * The 30s sse-refresh timer keeps time-dependent values (like oldest job age) fresh.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('statsTab', () => ({
    ...eventBusTab(),
    ...tabActive(),
    stats: null,
    loading: false,
    active: false,
    _statsDebounce: null,
    _statsPending: false,
    _loadErrored: false,

    init() {
      trackTabActive(this, '#tab-stats', {
        onShow: () => this.loadStats(),
      });
      const refreshTick = () => { if (this.active && !this.loading) this.loadStats(); };
      this._bindBus({
        queueChanged: () => { this.stats = null; if (this.active) this.loadStats(); },
        sseEvent: (e) => {
          if (!this.active) return;
          const queue = Alpine.store('app').selectedQueue;
          if (e.detail.some(evt => evt.table === queue)) this._debouncedLoadStats();
        },
        sseReconnect: () => { if (this.active) this.loadStats(); },
        sseRefresh: refreshTick,
        pollTick: refreshTick,
      });
    },

    destroy() {
      untrackTabActive(this);
      if (this._statsDebounce) { clearTimeout(this._statsDebounce); this._statsDebounce = null; }
      this._statsPending = false;
      this._unbindBus();
      releaseInitialLoad(this);
    },

    _debouncedLoadStats() {
      if (this._statsDebounce) { this._statsPending = true; return; }
      this._statsDebounce = setTimeout(() => {
        this._statsPending = false;
        this.loadStats().finally(() => {
          this._statsDebounce = null;
          if (this._statsPending) this._debouncedLoadStats();
        });
      }, ARB_TIMING.statsDebounceMs);
    },

    // Card link href: this queue's Jobs tab filtered by status (empty = all).
    jobsUrl(status) {
      return queueJobsUrl(Alpine.store('app').selectedQueue, status);
    },

    goToJobs(e, status) {
      if (!plainNavClick(e)) return;
      window.dispatchEvent(new CustomEvent(ARB_EVENTS.filterJobs, { detail: status }));
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
